"""Iter-4 cross-family discriminator fix tests:
- HIGH-1: POST /agent/status response.registered_at returns broker-time (stored), not client last_activity_ts
- HIGH-2: registered_at is monotonic across rows (max(now, MAX+1)) — concurrent inserts can't collide
- HIGH-3: next_cursor null on exact-multiple-of-limit (peek-ahead LIMIT+1)
- HIGH-4: pipelines collection NOT bounded by agent limit
- MED: cursor decoder strict on empty/malformed inputs
- LOW: contract.md cursor section trimmed (manual review; no test)
"""

from __future__ import annotations

import asyncio
import os

import aiosqlite
import pytest

pytestmark = pytest.mark.asyncio


# ===== HIGH-1: response.registered_at = broker-time, not client last_activity_ts =====

async def test_response_registered_at_is_broker_time_not_client(client):
    """Client posts last_activity_ts=2020 (way in the past); response should NOT echo it."""
    client_ts = 1577836800000  # 2020-01-01 UTC in epoch-ms
    r = await client.post("/api/agent/h1-1/status", json={
        "kind": "demo_research", "channel": "#x",
        "status": "idle", "last_activity_ts": client_ts,
    })
    assert r.status_code == 200
    body = r.json()
    # broker-time registered_at MUST be much greater than 2020 client value
    assert body["registered_at"] > client_ts
    assert body["registered_at"] > 1700000000000  # > 2023


async def test_response_registered_at_matches_db(client):
    """Response.registered_at must equal what's stored in agent_status."""
    r = await client.post("/api/agent/h1-2/status", json={
        "kind": "demo_research", "channel": "#x",
        "status": "idle", "last_activity_ts": 100,
    })
    response_ts = r.json()["registered_at"]
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT registered_at FROM agent_status WHERE id = 'h1-2'"
        )
        row = await cur.fetchone()
    assert response_ts == row["registered_at"]


async def test_idempotent_response_registered_at_is_stored_value(client):
    """Idempotency fast path also returns stored registered_at, not client last_act."""
    body = {
        "kind": "demo_research", "channel": "#x", "status": "idle",
        "last_activity_ts": 100,
        "client_event_id": "h1-idem-uuid",
    }
    r1 = await client.post("/api/agent/h1-3/status", json=body)
    r2 = await client.post("/api/agent/h1-3/status", json=body)
    # Both return same event_id (idempotent) AND same broker registered_at
    assert r1.json()["registered_at"] == r2.json()["registered_at"]
    assert r1.json()["registered_at"] > 100  # broker-time, not client's 100
    assert r1.json()["event_id"] == r2.json()["event_id"]


# ===== HIGH-2: registered_at monotonic across rows =====

async def test_registered_at_strictly_monotonic_under_same_clock(client, monkeypatch):
    """When now_ms() returns same value across multiple inserts, registered_at must
    still be strictly increasing per row (max(now, MAX+1) trick)."""
    from broker import server
    fixed_ts = 1746000000000
    monkeypatch.setattr(server, "now_ms", lambda: fixed_ts)

    for i in range(5):
        await client.post(f"/api/agent/mono-{i}/status", json={
            "kind": "demo_research", "channel": "#x", "status": "idle",
            "last_activity_ts": 1,
        })

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT id, registered_at FROM agent_status "
            "WHERE id LIKE 'mono-%' ORDER BY registered_at"
        )
        rows = await cur.fetchall()
    # All 5 rows MUST have distinct registered_at, strictly increasing
    timestamps = [r["registered_at"] for r in rows]
    assert len(timestamps) == 5
    assert len(set(timestamps)) == 5  # distinct
    assert timestamps == sorted(timestamps)  # increasing
    # First one >= fixed_ts; subsequent ones > prior
    assert timestamps[0] >= fixed_ts
    for i in range(1, 5):
        assert timestamps[i] > timestamps[i - 1]


async def test_registered_at_concurrent_insert_during_pagination_no_skip(client, monkeypatch):
    """Iter-4 HIGH-2 fix verification: simulate page1-fetch → concurrent insert → page2-fetch.
    The concurrent insert MUST appear in page 2 (not silently skipped).

    Seed 4 agents with limit=3 so peek-ahead emits a real next_cursor (iter-5 medium fix:
    earlier 3-seed-3-limit version was structurally inert because next_cursor was null
    on the first page, causing the late-insert assertion to be silently skipped).
    """
    from broker import server
    fixed_ts = 1746000000000
    monkeypatch.setattr(server, "now_ms", lambda: fixed_ts)

    # Register 4 agents — page 1 (limit=3) sees 3, page 2 sees 4th
    for i in range(4):
        await client.post(f"/api/agent/conc-{i}/status", json={
            "kind": "demo_coder", "channel": "#x", "status": "idle",
            "last_activity_ts": 1,
        })

    # Fetch page 1 with limit=3
    r1 = await client.get("/api/state", params={"limit": 3})
    d1 = r1.json()
    page1_ids = [a["id"] for a in d1["agents"] if a["id"].startswith("conc-")]
    assert len(page1_ids) == 3
    assert d1["next_cursor"] is not None, "page 1 must emit a cursor (4 agents > limit 3)"

    # Concurrent insert — same fixed_ts (monotonic enforcement still applies)
    await client.post("/api/agent/conc-LATE/status", json={
        "kind": "demo_coder", "channel": "#x", "status": "idle",
        "last_activity_ts": 1,
    })

    # Fetch page 2 with cursor from page 1
    r2 = await client.get("/api/state", params={"cursor": d1["next_cursor"]})
    d2 = r2.json()
    page2_ids = [a["id"] for a in d2["agents"] if a["id"].startswith("conc-")]
    # Both the original 4th (conc-3) AND the late inserter MUST appear in page 2
    # (iter-6 LOW fix: previously asserted only conc-LATE; a cursor bug skipping
    # conc-3 but returning conc-LATE would have passed the old test silently).
    assert "conc-3" in page2_ids, \
        f"original 4th agent (conc-3) MUST appear in page 2; got {page2_ids}"
    assert "conc-LATE" in page2_ids, \
        f"late-inserted agent MUST appear in page 2; got {page2_ids}"


# ===== HIGH-3: next_cursor null on exact final page =====

async def test_next_cursor_null_when_results_equal_limit_exactly(client):
    """Register exactly N agents; fetch with limit=N. next_cursor MUST be null."""
    for i in range(3):
        await client.post(f"/api/agent/exact-{i}/status", json={
            "kind": "demo_research", "channel": "#x", "status": "idle",
            "last_activity_ts": 1,
        })
        await asyncio.sleep(0.001)
    r = await client.get("/api/state", params={"limit": 3})
    d = r.json()
    assert len([a for a in d["agents"] if a["id"].startswith("exact-")]) == 3
    # No additional pages — next_cursor MUST be null even though len == limit
    assert d["next_cursor"] is None


async def test_next_cursor_set_when_more_pages_exist(client):
    for i in range(5):
        await client.post(f"/api/agent/more-{i}/status", json={
            "kind": "demo_research", "channel": "#x", "status": "idle",
            "last_activity_ts": 1,
        })
        await asyncio.sleep(0.001)
    r = await client.get("/api/state", params={"limit": 3})
    assert r.json()["next_cursor"] is not None  # 2 more pages


# ===== HIGH-4: pipelines NOT bounded by agent limit =====

async def test_pipelines_not_bounded_by_agent_limit(client, project_state_db, tmp_path):
    """Register multiple projects → pipelines_view has many rows; agent ?limit=1 should
    NOT truncate pipelines."""
    # Register first project (1 pipeline from fixture)
    await client.post("/api/project/register", json=project_state_db)

    # Add 2 more projects with their own state.dbs
    import sqlite3
    for i in range(2):
        proj_dir = tmp_path / f"extra-{i}"
        proj_dir.mkdir()
        sdb = proj_dir / "state.db"
        conn = sqlite3.connect(str(sdb))
        conn.executescript("""
            CREATE TABLE pipelines (id TEXT PRIMARY KEY, shape TEXT NOT NULL,
              started_at INTEGER NOT NULL, completed_at INTEGER,
              status TEXT NOT NULL CHECK (status IN ('open','in_progress','blocked','done','failed','cancelled')));
            CREATE TABLE pipeline_steps (pipeline_id TEXT NOT NULL REFERENCES pipelines(id),
              step_index INTEGER NOT NULL, agent_type TEXT NOT NULL,
              status TEXT NOT NULL, started_at INTEGER NOT NULL, completed_at INTEGER,
              output_path TEXT, last_task_id TEXT, inbox_path TEXT, output_artifact_paths TEXT,
              PRIMARY KEY (pipeline_id, step_index));
            CREATE TABLE pipeline_dependencies (pipeline_id TEXT NOT NULL,
              depends_on_pipeline_id TEXT NOT NULL,
              PRIMARY KEY (pipeline_id, depends_on_pipeline_id));
        """)
        conn.execute(
            "INSERT INTO pipelines (id, shape, started_at, status) VALUES (?, ?, 1, 'in_progress')",
            (f"px-{i}", "demo_pipeline"),
        )
        conn.execute(
            "INSERT INTO pipeline_steps (pipeline_id, step_index, agent_type, status, started_at) "
            "VALUES (?, 0, 'demo_research', 'in_progress', 1)",
            (f"px-{i}",),
        )
        conn.commit()
        conn.close()
        await client.post("/api/project/register", json={
            "name": f"extra-{i}", "root_path": str(proj_dir), "state_db_path": str(sdb),
        })

    # Verify 3 pipelines in DB
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT COUNT(*) AS n FROM pipelines_view")
        assert (await cur.fetchone())["n"] == 3

    # GET /api/state with limit=1 — pipelines MUST still all appear
    r = await client.get("/api/state", params={"limit": 1})
    d = r.json()
    assert len(d["pipelines"]) == 3, "pipelines must NOT be truncated by agent limit"


# ===== MED: cursor decoder strict =====

async def test_cursor_empty_string_returns_422(client):
    r = await client.get("/api/state", params={"cursor": ""})
    assert r.status_code == 422


async def test_cursor_missing_colon_returns_422(client):
    r = await client.get("/api/state", params={"cursor": "no-colon-here"})
    assert r.status_code == 422


async def test_cursor_empty_id_segment_returns_422(client):
    r = await client.get("/api/state", params={"cursor": "1746000000000:"})
    assert r.status_code == 422


async def test_cursor_empty_ts_segment_returns_422(client):
    r = await client.get("/api/state", params={"cursor": ":agent-x"})
    assert r.status_code == 422


async def test_cursor_non_int_ts_returns_422(client):
    r = await client.get("/api/state", params={"cursor": "abc:agent-x"})
    assert r.status_code == 422


async def test_cursor_negative_ts_returns_422(client):
    r = await client.get("/api/state", params={"cursor": "-1:agent-x"})
    assert r.status_code == 422


async def test_cursor_huge_ts_accepted_no_upper_bound(client):
    """Iter-5 LOW fix: decoder accepts post-year-2100 timestamps (no artificial cap).
    Iter-6 HIGH fix tightened: still rejects > SQLite i64 max."""
    # 9999999999999 (year 2316-ish) is well within i64 max
    r = await client.get("/api/state", params={"cursor": "9999999999999:agent-x"})
    assert r.status_code == 200
    assert r.json()["agents"] == []


async def test_cursor_at_i64_max_accepted(client):
    """SQLite i64 max (9223372036854775807) is the largest valid INTEGER bind. Accept."""
    r = await client.get("/api/state", params={"cursor": "9223372036854775807:agent-x"})
    assert r.status_code == 200


async def test_cursor_above_i64_max_returns_422(client):
    """Iter-6 HIGH fix: i64 max + 1 must 422 (not propagate to SQLite as 500)."""
    r = await client.get("/api/state", params={"cursor": "9223372036854775808:agent-x"})
    assert r.status_code == 422


# (iter-7 cleanup: removed duplicate `test_cursor_overflow_unparseable_returns_422`;
# `test_cursor_non_int_ts_returns_422` above already covers the unparseable-int path.)


async def test_cursor_id_with_colon_in_value_handled(client):
    """split(':', 1) ensures id can contain literal colons (e.g., user:123)."""
    # Register an agent with a colon in id
    await client.post("/api/agent/user:123/status", json={
        "kind": "demo_research", "channel": "#x", "status": "idle", "last_activity_ts": 1,
    })
    await client.post("/api/agent/zzz/status", json={
        "kind": "demo_research", "channel": "#x", "status": "idle", "last_activity_ts": 1,
    })
    await asyncio.sleep(0.005)
    # Fetch with limit=1; cursor should encode as "<ts>:user:123"
    r1 = await client.get("/api/state", params={"limit": 1})
    cursor = r1.json()["next_cursor"]
    assert cursor is not None
    # Round-trip the cursor
    r2 = await client.get("/api/state", params={"cursor": cursor})
    assert r2.status_code == 200


# ===== Composite index =====

async def test_composite_index_present(client):
    """Verify schema bootstrap created idx_agent_status_registered_at_id."""
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_agent_status_registered_at_id'"
        )
        row = await cur.fetchone()
    assert row is not None


# ===== iter-7 HIGH: i64 cap on ALL int inputs that bind to SQLite INTEGER =====

I64_MAX = 9223372036854775807


async def test_event_ts_above_i64_max_returns_422(client):
    """POST /api/event ts > i64 max must 422 (not 500 from aiosqlite overflow)."""
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": I64_MAX + 1,
    })
    assert r.status_code == 422


async def test_event_ts_at_i64_max_accepted(client):
    """ts at exact i64 max is valid."""
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": I64_MAX,
    })
    assert r.status_code == 200


async def test_agent_status_last_activity_ts_above_i64_max_returns_422(client):
    r = await client.post("/api/agent/big1/status", json={
        "kind": "demo_research", "channel": "#x", "status": "idle",
        "last_activity_ts": I64_MAX + 1,
    })
    assert r.status_code == 422


async def test_discord_cursor_last_check_ts_above_i64_max_returns_422(client):
    r = await client.post("/api/discord/cursors", json={
        "channel_id": "c", "last_seen_message_id": "1",
        "last_check_ts": I64_MAX + 1,
    })
    assert r.status_code == 422


async def test_events_cursor_above_i64_max_returns_422(client):
    """GET /api/events ?cursor= > i64 max must 422 via FastAPI Query(le=)."""
    r = await client.get("/api/events", params={"cursor": str(I64_MAX + 1)})
    assert r.status_code == 422


async def test_events_last_event_id_above_i64_max_returns_422(client):
    r = await client.get("/api/events", headers={"Last-Event-ID": str(I64_MAX + 1)})
    assert r.status_code == 422


async def test_events_last_event_id_negative_returns_422(client):
    r = await client.get("/api/events", headers={"Last-Event-ID": "-5"})
    assert r.status_code == 422


async def test_events_last_event_id_non_int_returns_422(client):
    r = await client.get("/api/events", headers={"Last-Event-ID": "garbage"})
    assert r.status_code == 422


async def test_audit_snapshot_window_ts_above_i64_max_returns_422(client, monkeypatch):
    monkeypatch.setenv("BROKER_ADMIN_ENABLED", "true")
    r = await client.post("/api/admin/audit-snapshot",
                          json={"snapshot_window_start_ts": I64_MAX + 1})
    assert r.status_code == 422


async def test_audit_snapshot_window_ts_close_to_max_returns_422(client, monkeypatch):
    """+1h end_ms would overflow; reject preemptively."""
    monkeypatch.setenv("BROKER_ADMIN_ENABLED", "true")
    too_close = I64_MAX - 1000  # +3_600_000ms would overflow
    r = await client.post("/api/admin/audit-snapshot",
                          json={"snapshot_window_start_ts": too_close})
    assert r.status_code == 422


async def test_normalize_ts_negative_int_returns_422(client):
    """ts as negative int — caught by normalize_ts (not just ISO-8601 path)."""
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": -1,
    })
    assert r.status_code == 422
