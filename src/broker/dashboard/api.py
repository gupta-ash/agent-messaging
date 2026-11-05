"""Broker dashboard — read-only API router + static serve.

This module is a READ-ONLY consumer of the broker's coordination.db. It opens
the database with a `mode=ro` SQLite URI (the engine rejects any write at the
connection level) and never imports the broker's read-write `connect_db()` —
which also avoids a circular import, since `server.py` imports this package.

One `APIRouter()` with explicit full route paths (no prefix) so the broker
mount is a single `app.include_router(router)` line. Routes:

    GET /api/dashboard/agents     — liveness-first agent feed
    GET /api/dashboard/pipelines  — in-flight-default pipeline list
    GET /api/dashboard/messages   — newest-first paginated agent messages
    GET /dashboard                — single-file HTML frontend

The destructive POST /api/admin/prune is broker-owned (uses the broker's
read-write connection). The dashboard frontend only CALLS it; it is NOT
implemented here.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

router = APIRouter()

# Path to this package directory (dashboard.html sibling lives here).
_PKG_DIR = Path(__file__).parent

# Relay registry path is a MODULE CONSTANT so tests can monkeypatch it.
# ~/.claude/relay/registry.json maps {session_id: {name, last_seen_ts, ...}}.
_REGISTRY_PATH = Path.home() / ".claude" / "relay" / "registry.json"

# Liveness thresholds (ms).
_LIVE_MS = 5 * 60 * 1000          # < 5m  -> live
_RECENT_MS = 60 * 60 * 1000       # < 1h  -> recent
_IDLE_MS = 24 * 60 * 60 * 1000    # < 24h -> idle ; else stale

# Stored statuses that imply "should be doing work right now". When the derived
# liveness contradicts (idle/stale), the agent gets a ⚠ flag.
_ACTIVE_STORED_STATUSES = {"running", "queued", "blocked"}

# In-flight pipeline statuses (the enum has 'open', not 'queued').
_IN_FLIGHT_STATUSES = ("open", "in_progress", "blocked")

# Kinds the broker is known to write into message_log. /api/dashboard/messages
# accepts any of them — the shipped UI only ever asks for agent_message, but
# curl-ing another kind is how you debug this thing. message_log.kind carries NO
# CHECK constraint (unlike agent_status.status and pipelines_view.status), so the
# database cannot be consulted as the source of truth; this tuple MIRRORS the
# broker's own write-side validator, server.py VALID_EVENT_KINDS, which is
# enforced on POST /api/event. Keep the two in sync: if the broker widens
# VALID_EVENT_KINDS, widen this.
#
# Deliberately EXCLUDES `message_logged`. That is an SSE WIRE kind — the
# fallthrough return of server.py sse_kind() — and is not a storage kind: it is
# absent from VALID_EVENT_KINDS, no writer can emit it, and message_log holds
# zero such rows (verified 2026-08-04 over event_id 1..6759). Accepting it would
# return 200 + [] forever, which is exactly the silent empty page the 422 below
# exists to prevent. Storage kinds and wire kinds are disjoint namespaces —
# storage `status` maps to wire `agent_status_change` — so do not merge them.
_MESSAGE_KINDS = (
    "agent_message", "agent_reply", "status", "handoff", "error",
    "pipeline_event", "permission_reply", "user_msg", "tool_call",
)

# mtime-cache for the relay registry: {"mtime": <ns|None>, "data": {...}}.
_registry_cache: dict[str, Any] = {"mtime": None, "data": {}}


# -------------------------------------------------------------------- helpers

def _now_ms() -> int:
    return int(time.time() * 1000)


def _db_path() -> str:
    """Read BROKER_DB_PATH at call time (not import time) so tests can set it."""
    return os.environ.get("BROKER_DB_PATH", "coordination.db")


def _norm_ts(ts: Any) -> Optional[int]:
    """Normalize a stored timestamp to epoch-ms, or None if unusable.

    The broker stores mixed-unit timestamps: pipelines_view.started_at /
    completed_at are 10-digit epoch-seconds for some producers and 13-digit
    epoch-ms for others, plus a few 20-digit anomalies. This collapses them:

      - <= 0       -> None
      - < 1e12     -> epoch-seconds, multiply by 1000
      - > 1e14     -> anomalous (20-digit) -> None (clamp)
      - otherwise  -> already epoch-ms
    """
    if ts is None:
        return None
    try:
        v = int(ts)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if v < 1_000_000_000_000:   # < 1e12 -> epoch-seconds
        v *= 1000
    if v > 100_000_000_000_000:  # > 1e14 -> 20-digit anomaly
        return None
    return v


def _derive_liveness(effective_ts: Optional[int], now: int) -> str:
    """Bucket an effective timestamp into live/recent/idle/stale."""
    if effective_ts is None:
        return "stale"
    age = now - effective_ts
    if age < _LIVE_MS:
        return "live"
    if age < _RECENT_MS:
        return "recent"
    if age < _IDLE_MS:
        return "idle"
    return "stale"


def _load_relay_registry() -> dict[str, dict[str, Any]]:
    """Load ~/.claude/relay/registry.json read-only, mtime-cached.

    Returns {session_id: {"name": <str|None>, "last_seen_ts": <int|None>}}.
    Degrades to {} on absent/unreadable/malformed file (never raises).
    """
    try:
        mtime = _REGISTRY_PATH.stat().st_mtime_ns
    except OSError:
        # File absent/unreadable -> reset cache and degrade.
        _registry_cache["mtime"] = None
        _registry_cache["data"] = {}
        return {}

    if _registry_cache["mtime"] == mtime:
        return _registry_cache["data"]

    try:
        raw = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        _registry_cache["mtime"] = mtime
        _registry_cache["data"] = {}
        return {}

    out: dict[str, dict[str, Any]] = {}
    if isinstance(raw, dict):
        for sid, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            last_seen = entry.get("last_seen_ts")
            out[sid] = {
                "name": name if isinstance(name, str) else None,
                "last_seen_ts": last_seen if isinstance(last_seen, int) else None,
            }
    _registry_cache["mtime"] = mtime
    _registry_cache["data"] = out
    return out


@asynccontextmanager
async def _connect_ro() -> AsyncIterator[aiosqlite.Connection]:
    """Open coordination.db read-only as an async context manager.

    `mode=ro` makes SQLite reject every write at the engine level — the
    read-only guarantee is enforced in the database, not just by convention.
    Only a read-side busy_timeout PRAGMA is applied (no WAL/FK writes, which a
    ro connection would reject).
    """
    uri = f"file:{_db_path()}?mode=ro"
    conn = await aiosqlite.connect(uri, uri=True)
    try:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA busy_timeout=5000")
        yield conn
    finally:
        await conn.close()


# -------------------------------------------------------------------- routes

@router.get("/api/dashboard/agents")
async def get_dashboard_agents(within_ms: Optional[int] = Query(None, ge=0)):
    """Liveness-first agent feed.

    Counts {live,recent,idle,stale} are computed over the FULL agent set;
    `within_ms` filters only the returned `agents` array (by effective age).

    The SELECT is deliberately unordered and unbounded — read the comment on
    the query below before adding an ORDER BY or a LIMIT to it.
    """
    now = _now_ms()
    registry = _load_relay_registry()

    try:
        async with _connect_ro() as conn:
            # Columns are exactly the ones the loop below reads. `capabilities`
            # and `registered_at` are stored by the broker but this endpoint has
            # never surfaced either, so they are not fetched.
            #
            # No ORDER BY: the sort key is effective_ts = max(last_activity_ts,
            # relay-registry last_seen_ts), and the registry is a JSON file
            # outside this database (_REGISTRY_PATH). SQL cannot compute that
            # key, so the Python sort below is authoritative and an SQL ordering
            # would only mislead the next reader into trusting a row order that
            # gets overwritten.
            #
            # No LIMIT: `counts` is computed over every row (see the docstring,
            # pinned by test_agents_within_ms_filters_array_not_counts) and
            # `within_ms` is applied in Python AFTER this fetch, so a LIMIT would
            # silently corrupt both. It would also cut the WRONG rows — an agent
            # stale in last_activity_ts but fresh in the registry sorts last in
            # any SQL order and is dropped first, while being one of the most
            # live agents there is (test_agents_effective_ts_is_max_of_activity_
            # and_registry constructs exactly that row). agent_status is keyed by
            # agent id, so it grows with distinct agents ever seen (131 today),
            # not with events; the broker's admin prune is the bound here, not a
            # LIMIT. test_agents_uncapped_250 pins this.
            cur = await conn.execute(
                "SELECT id, kind, channel, thread_id, status, current_task, "
                "last_activity_ts, display_name "
                "FROM agent_status"
            )
            rows = await cur.fetchall()
            await cur.close()
    except (sqlite3.Error, OSError):
        return {
            "now": now,
            "counts": {"live": 0, "recent": 0, "idle": 0, "stale": 0},
            "agents": [],
            "degraded": True,
        }

    counts = {"live": 0, "recent": 0, "idle": 0, "stale": 0}
    agents: list[dict[str, Any]] = []
    for row in rows:
        sid = row["id"]
        last_act = _norm_ts(row["last_activity_ts"])
        reg = registry.get(sid) or {}
        reg_seen = _norm_ts(reg.get("last_seen_ts"))
        # effective_ts = max(last_activity_ts, registry.last_seen_ts); None-safe.
        candidates = [t for t in (last_act, reg_seen) if t is not None]
        effective_ts = max(candidates) if candidates else None

        liveness = _derive_liveness(effective_ts, now)
        counts[liveness] += 1

        reg_name = reg.get("name")
        name = row["display_name"] or (reg_name if reg_name else None) or sid[:8]

        age_ms = (now - effective_ts) if effective_ts is not None else None
        contradiction = (
            row["status"] in _ACTIVE_STORED_STATUSES and liveness in ("idle", "stale")
        )

        agents.append({
            "id": sid,
            "name": name,
            "kind": row["kind"],
            "channel": row["channel"],
            "thread_id": row["thread_id"],
            "status": row["status"],
            "current_task": row["current_task"],
            "effective_ts": effective_ts,
            "last_activity_ts": last_act,
            "age_ms": age_ms,
            "liveness": liveness,
            "flag": "⚠" if contradiction else None,
        })

    # effective_ts DESC (None last); `id` breaks ties so two identical requests
    # always return the same sequence. Without it the order of tied rows is
    # SQLite's unspecified scan order, leaking through Python's stable sort.
    agents.sort(
        key=lambda a: (a["effective_ts"] is not None,
                       a["effective_ts"] or 0,
                       a["id"] or ""),
        reverse=True,
    )

    if within_ms is not None:
        agents = [
            a for a in agents
            if a["age_ms"] is not None and a["age_ms"] <= within_ms
        ]

    return {"now": now, "counts": counts, "agents": agents}


@router.get("/api/dashboard/pipelines")
async def get_dashboard_pipelines(
    status: str = Query("in_flight"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """In-flight-default pipeline list.

    `status=in_flight` (default) filters to open/in_progress/blocked;
    `status=all` returns every row. Mixed-unit timestamps are normalized in
    Python and sorted started_at DESC (None last) — the SQL sort can't be
    trusted across mixed units. `in_flight_count` and `total` are always
    reported (separate COUNTs over the unfiltered table).
    """
    if status not in ("in_flight", "all"):
        raise HTTPException(status_code=422, detail="status must be 'in_flight' or 'all'")

    base_cols = (
        "project_name, pipeline_id, shape, started_at, completed_at, "
        "status, step_count, steps_done, last_refresh_ts"
    )
    placeholders = ",".join("?" for _ in _IN_FLIGHT_STATUSES)

    try:
        async with _connect_ro() as conn:
            if status == "in_flight":
                cur = await conn.execute(
                    f"SELECT {base_cols} FROM pipelines_view "
                    f"WHERE status IN ({placeholders})",
                    _IN_FLIGHT_STATUSES,
                )
            else:
                cur = await conn.execute(f"SELECT {base_cols} FROM pipelines_view")
            rows = await cur.fetchall()
            await cur.close()

            cur = await conn.execute("SELECT COUNT(*) AS n FROM pipelines_view")
            total = (await cur.fetchone())["n"]
            await cur.close()

            cur = await conn.execute(
                f"SELECT COUNT(*) AS n FROM pipelines_view WHERE status IN ({placeholders})",
                _IN_FLIGHT_STATUSES,
            )
            in_flight_count = (await cur.fetchone())["n"]
            await cur.close()
    except (sqlite3.Error, OSError):
        return {
            "now": _now_ms(),
            "total": 0,
            "in_flight_count": 0,
            "returned": 0,
            "offset": offset,
            "limit": limit,
            "pipelines": [],
            "degraded": True,
        }

    now = _now_ms()
    pipelines: list[dict[str, Any]] = []
    for row in rows:
        started = _norm_ts(row["started_at"])
        completed = _norm_ts(row["completed_at"])
        pipelines.append({
            "project_name": row["project_name"],
            "pipeline_id": row["pipeline_id"],
            "shape": row["shape"],
            "status": row["status"],
            "step_count": row["step_count"],
            "steps_done": row["steps_done"],
            "started_at": started,
            "completed_at": completed,
            "age_ms": (now - started) if started is not None else None,
            "last_refresh_ts": _norm_ts(row["last_refresh_ts"]),
        })

    # Sort started_at DESC, None last; then slice offset/limit.
    pipelines.sort(key=lambda p: (p["started_at"] is not None, p["started_at"] or 0),
                   reverse=True)
    page = pipelines[offset:offset + limit]

    return {
        "now": now,
        "total": total,
        "in_flight_count": in_flight_count,
        "returned": len(page),
        "offset": offset,
        "limit": limit,
        "pipelines": page,
    }


@router.get("/api/dashboard/messages")
async def get_dashboard_messages(
    kind: str = Query("agent_message"),
    within_ms: Optional[int] = Query(None, ge=0),
    before: Optional[int] = Query(None, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    """Newest-first paginated agent messages.

    `before` is an exclusive event_id cursor (for "load older"). `next_before`
    is the oldest returned event_id, or null when fewer than `limit` rows came
    back (no more pages).

    `kind` defaults to agent_message (all the shipped UI asks for) but any
    `_MESSAGE_KINDS` member is accepted; an unknown kind is a 422, not an empty
    page that reads as "no messages".
    """
    if kind not in _MESSAGE_KINDS:
        raise HTTPException(
            status_code=422,
            detail="kind must be one of: " + ", ".join(_MESSAGE_KINDS),
        )

    clauses = ["kind = :kind"]
    params: dict[str, Any] = {"kind": kind, "limit": limit}

    if within_ms is not None:
        params["floor"] = _now_ms() - within_ms
        clauses.append("ts >= :floor")
    if before is not None:
        params["before"] = before
        clauses.append("event_id < :before")

    where = " AND ".join(clauses)
    try:
        async with _connect_ro() as conn:
            cur = await conn.execute(
                "SELECT event_id, client_event_id, from_agent, to_agent, channel, "
                "thread_id, kind, content, ts "
                f"FROM message_log WHERE {where} ORDER BY event_id DESC LIMIT :limit",
                params,
            )
            rows = await cur.fetchall()
            await cur.close()

            # Global SSE high-water mark (broker SSE cursor is global event_id
            # space). MAX returns NULL on an empty table -> None.
            cur = await conn.execute("SELECT MAX(event_id) AS m FROM message_log")
            latest_event_id = (await cur.fetchone())["m"]
            await cur.close()
    except (sqlite3.Error, OSError):
        return {
            "now": _now_ms(),
            "messages": [],
            "next_before": None,
            "latest_event_id": None,
            "degraded": True,
        }

    messages = [{
        "event_id": r["event_id"],
        "client_event_id": r["client_event_id"],
        "from_agent": r["from_agent"],
        "to_agent": r["to_agent"],
        "channel": r["channel"],
        "thread_id": r["thread_id"],
        "kind": r["kind"],
        "content": r["content"],
        "ts": _norm_ts(r["ts"]),
    } for r in rows]

    next_before = messages[-1]["event_id"] if len(messages) == limit else None

    return {
        "now": _now_ms(),
        "messages": messages,
        "next_before": next_before,
        "latest_event_id": latest_event_id,
    }


@router.get("/dashboard")
async def serve_dashboard():
    """Serve the single-file dashboard HTML (fail-loud 500 if missing)."""
    html_path = _PKG_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(
            status_code=500,
            detail="dashboard.html not found in dashboard package; reinstall",
        )
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"), status_code=200)
