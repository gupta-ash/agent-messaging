# -*- coding: utf-8 -*-
"""shared.py — common library for cc-session-relay v2 receive hooks.

Per task spec `task-specs cc-session-relay-build.md` and
plan `plans_tmp crispy-hugging-brooks.md`.

Functions exposed:
- read_hook_stdin() -> dict
- resolve_session_id(stdin_payload) -> str
- read_cursor(session_id) -> int
- write_cursor(session_id, event_id) -> None     (atomic via tempfile + rename)
- fetch_pending_messages(session_id, since, kind, limit, broker_base_url) -> dict
- resolve_sender_name(from_agent_id, registry_path) -> str
- format_messages_for_injection(messages) -> str
- emit_hook_response(hook_event_name, additional_context) -> None
- do_receive(hook_event_name) -> None              (canonical entry routine)

Internal helpers (broker-known-down cache, v1.3.1 follow-up):
- _broker_down_cache_s() -> float                  (env-tunable BROKER_DOWN_CACHE_S; default 30s)
- _broker_down_marker_path() -> Path               (STATE_DIR / broker-down.json)
- _is_broker_known_down() -> bool                  (cache pre-check; silent-degrade)
- _mark_broker_down(reason) -> None                (atomic marker write; silent-degrade)
- _clear_broker_down_marker() -> None              (best-effort unlink on 2xx success)

Receive-side discipline (per task spec § Anti-patterns):
- Do NOT block in receive hooks (5s broker timeout, then degrade silently).
- Do NOT crash on missing registry/cursor files (first-run absence is normal).
- Do NOT swallow exceptions silently in send.py — but receive-side hooks MUST
  silent-degrade (never break a CC session because of a hook bug).
- Do NOT log conversation content to stderr — only metadata.
- Do NOT use print(); use sys.stdout.write / sys.stderr.write to avoid
  trailing-newline parser issues.

Cross-platform (Windows + POSIX): all paths via pathlib.Path; no hard-coded
home dir. UTF-8 stdout/stderr reconfigured in main entry-points.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Resolve the selected public installation before importing the env-required
# HTTP client. A malformed installed selection must not become a silent fallback.
_RELAY_DIR = str(Path(__file__).resolve().parent)
if _RELAY_DIR not in sys.path:
    sys.path.insert(0, _RELAY_DIR)
from broker_connection import resolve_broker_base_url

DEFAULT_BROKER_BASE_URL = resolve_broker_base_url()
os.environ.setdefault("BROKER_BASE_URL", DEFAULT_BROKER_BASE_URL)
try:
    import broker_client  # type: ignore  # hooks/relay/broker_client.py (sibling)
except Exception:  # ImportError, or its import-time BROKER_BASE_URL guard
    broker_client = None  # type: ignore

# ---------------------------------------------------------------------------
# Constants — paths + broker defaults
# ---------------------------------------------------------------------------

HOME = Path.home()
CLAUDE_DIR = HOME / ".claude"
RELAY_DIR = CLAUDE_DIR / "relay"
CURSORS_DIR = RELAY_DIR / "cursors"
REGISTRY_PATH = RELAY_DIR / "registry.json"
STATE_DIR = RELAY_DIR / "state"
SESSIONS_DIR = CLAUDE_DIR / "sessions"

# LOSSLESS-DELIVERY refactor (plan quizzical-launching-fox): per-recipient-
# SESSION confirmed-set lives at CONFIRMED_DIR / f"{sid}.json". The hook reads
# its own NOT from a module-level Path constant but via _confirmed_path() so a
# monkeypatch of RELAY_DIR in tests is honored. Telemetry counters live at
# CONFIRMED_DIR / "telemetry-counters.json" (B4).
# NOTE: these are derived dynamically (functions below) rather than as
# module-level constants so tests can monkeypatch shared.RELAY_DIR and have the
# sub-paths follow — same discipline as CONFIRMED_* below.

# Send-side defaults (existing behavior).
BROKER_TIMEOUT_S = 5.0
RETRY_BACKOFF_MS = (50, 100, 200, 400, 800)  # 5 attempts

# Receive-side defaults — fast-fail since silent-degrade is acceptable.
# Tunable via env: BROKER_RECEIVE_TIMEOUT_S (default 1.0) and
# BROKER_RECEIVE_ATTEMPTS (default 2).
DEFAULT_RECEIVE_TIMEOUT_S = 1.0
DEFAULT_RECEIVE_ATTEMPTS = 2
DEFAULT_POST_TIMEOUT_S = 2.0

# Broker-known-down cache (v1.3.1 follow-up) — short-circuits broker calls during
# outages so receive-hook latency stays low when broker has been down recently.
# Cache TTL tunable via BROKER_DOWN_CACHE_S env (default 30.0s). Marker file lives
# at STATE_DIR / BROKER_DOWN_MARKER_FILENAME and is written atomically by the
# degrade paths in fetch_pending_messages + post_status_with_display_name.
DEFAULT_BROKER_DOWN_CACHE_S = 30.0
BROKER_DOWN_MARKER_FILENAME = "broker-down.json"

# Terminal CC daemon job states — a holder in any of these is NOT live (not a
# routing conflict, GC-demotable). Anything else (e.g. "running") is live.
# Canonical single source of truth (ADR D54); imported by set_name.py and
# registry_gc.py so the liveness denylist can never drift across the three.
_TERMINAL_JOB_STATES = {
    "stopped", "done", "failed", "cancelled", "canceled", "error", "killed",
}


def _receive_timeout_s() -> float:
    raw = os.environ.get("BROKER_RECEIVE_TIMEOUT_S")
    if raw:
        try:
            v = float(raw)
            if v > 0:
                return v
        except ValueError:
            pass
    return DEFAULT_RECEIVE_TIMEOUT_S


def _receive_attempts() -> int:
    raw = os.environ.get("BROKER_RECEIVE_ATTEMPTS")
    if raw:
        try:
            v = int(raw)
            if v >= 1:
                return v
        except ValueError:
            pass
    return DEFAULT_RECEIVE_ATTEMPTS


def _post_timeout_s() -> float:
    """Resolve POST timeout in seconds for the SessionStart status helper.

    Tunable via BROKER_POST_TIMEOUT_S env var. Default 2.0s.
    Receive-side defaults to 1.0s (low-latency requirement); send-side defaults
    to 5.0s (must-succeed retry path). The status POST is a best-effort
    cosmetic one-shot fired on every panel boot — we want it to silent-degrade
    fast on broker outage so SessionStart hook latency stays under a couple
    seconds even when the broker is down.
    """
    raw = os.environ.get("BROKER_POST_TIMEOUT_S")
    if raw:
        try:
            v = float(raw)
            if v > 0:
                return v
        except ValueError:
            pass
    return DEFAULT_POST_TIMEOUT_S



# ---------------------------------------------------------------------------
# Broker-known-down cache helpers (v1.3.1 follow-up)
# ---------------------------------------------------------------------------
# Per spec `task-specs v131-followups-broker-helper-and-down-cache.md`
# § Item 2: short-circuit broker calls during outages so hook latency stays low
# when the broker has been down recently. Marker file at STATE_DIR /
# BROKER_DOWN_MARKER_FILENAME persists `last_unreachable_ts` (epoch ms),
# `consecutive_failures` (int), `last_reason` (str truncated to 200 chars).
# All helpers MUST silent-degrade per relay discipline (lines 21-23): cache
# correctness is best-effort optimization, broker-call correctness is the
# invariant. A marker write failure must NEVER crash a CC session.

def _broker_down_cache_s() -> float:
    """Resolve broker-known-down cache TTL in seconds.

    Tunable via BROKER_DOWN_CACHE_S env var. Default 30.0s. Mirrors the
    _post_timeout_s() resolver shape: positive float required, else default.
    """
    raw = os.environ.get("BROKER_DOWN_CACHE_S")
    if raw:
        try:
            v = float(raw)
            if v > 0:
                return v
        except ValueError:
            pass
    return DEFAULT_BROKER_DOWN_CACHE_S


def _broker_down_marker_path() -> Path:
    """Return the marker file path: STATE_DIR / BROKER_DOWN_MARKER_FILENAME."""
    return STATE_DIR / BROKER_DOWN_MARKER_FILENAME


def _is_broker_known_down() -> bool:
    """Return True iff the broker-down marker is present AND within the TTL window.

    Returns False on any of: marker absent, JSON parse failure, missing
    `last_unreachable_ts` field, non-int timestamp, OS read error, or marker
    older than `_broker_down_cache_s()`. Silent-degrade per relay discipline:
    on ANY error treat as no-marker (caller will retry the broker).
    """
    try:
        marker_path = _broker_down_marker_path()
        if not marker_path.exists():
            return False
        data = json.loads(marker_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return False
        last_ts = data.get("last_unreachable_ts")
        if not isinstance(last_ts, int) or isinstance(last_ts, bool):
            return False
        now_ms = int(time.time() * 1000)
        # Future-timestamp guard (discriminator iter-1 M1 fix): a corrupt or
        # clock-skewed marker with last_ts > now would otherwise be treated as
        # 'recent unreachable' indefinitely (negative diff is < positive ttl).
        # 5s tolerance for benign clock jitter (NTP step). Beyond that, treat
        # as no-marker so caller will retry the broker.
        if last_ts > now_ms + 5000:
            return False
        ttl_ms = _broker_down_cache_s() * 1000
        return (now_ms - last_ts) < ttl_ms
    except Exception:
        # Discriminator iter-1 M2 fix: broaden to bare Exception to honor R3.3
        # NON-NEGOTIABLE silent-degrade contract. Sibling helpers
        # (_mark_broker_down line ~243, _clear_broker_down_marker line ~271)
        # already use bare Exception; this matches their discipline. Pathological
        # JSON (RecursionError, MemoryError) or other unexpected errors must
        # NEVER propagate to callers — _is_broker_known_down is called PRE-try
        # in post_status_with_display_name (line ~538) where SessionStart hook
        # and /relay name skill callers may not wrap in try/except.
        return False


def _mark_broker_down(reason: str) -> None:
    """Write/update the broker-down marker atomically (tempfile + os.replace).

    Increments `consecutive_failures` on each call (read existing marker if
    present; start at 1 if absent or unreadable). Truncates `reason` to 200
    chars to avoid pathological log-injection writes.

    Silent-degrade per relay discipline: any failure (file I/O, JSON, OS
    permission) writes one stderr line and returns — never raises, never
    crashes the calling hook. The cache is best-effort optimization; broker-
    call correctness is the invariant.

    Concurrency note (discriminator iter-1 M3 documented limitation):
    multiple hook subprocesses writing the marker simultaneously are race-safe
    at the atomic-rename layer (last writer wins; all writers see a recent
    timestamp). HOWEVER, a mark-after-clear race is possible: Hook A's
    successful broker fire calls _clear_broker_down_marker() (unlinks the
    marker); concurrently Hook B's failure mid-write os.replace lands AFTER
    A's unlink, leaving B's stale failure marker present despite A's recent
    success. Recovery is bounded by the cache TTL (BROKER_DOWN_CACHE_S; default
    30s) — subsequent fires resume normal operation once the marker ages out.
    Likewise, consecutive_failures is read-then-write per process and may
    undercount under concurrent failure storms (two parallel marks both read 0,
    both write 1). The field is informational only; _is_broker_known_down only
    consults last_unreachable_ts. If accurate counting matters later, switch
    to file-locked read-modify-write or move telemetry broker-side.
    """
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        marker_path = _broker_down_marker_path()

        # Read existing marker for consecutive_failures increment; tolerate any
        # error (start at 1 if unreadable, missing, or malformed).
        prev_failures = 0
        if marker_path.exists():
            try:
                prev_data = json.loads(marker_path.read_text(encoding="utf-8"))
                if isinstance(prev_data, dict):
                    pv = prev_data.get("consecutive_failures")
                    if isinstance(pv, int) and not isinstance(pv, bool) and pv >= 0:
                        prev_failures = pv
            except (OSError, json.JSONDecodeError, ValueError):
                prev_failures = 0

        truncated_reason = (reason or "")[:200]
        payload = {
            "last_unreachable_ts": int(time.time() * 1000),
            "consecutive_failures": prev_failures + 1,
            "last_reason": truncated_reason,
        }

        # Atomic write via tempfile + os.replace (mirrors write_cursor pattern).
        fd, tmp_path = tempfile.mkstemp(
            prefix=".broker-down.",
            suffix=".json.tmp",
            dir=str(STATE_DIR),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, marker_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as e:
        # Silent-degrade: cache write failure must NOT crash CC. Surface metadata
        # so a cluster of these is greppable, but never propagate.
        sys.stderr.write(
            f"relay shared._mark_broker_down: marker write failed ({type(e).__name__}); "
            f"cache disabled this fire\n"
        )
        # Explicit flush
        # after critical-path stderr write — Windows pipe buffering can otherwise
        # lose the message if CC crashes between this write + buffer drain.
        try:
            sys.stderr.flush()
        except Exception:
            pass


def _clear_broker_down_marker() -> None:
    """Best-effort removal of the broker-down marker file.

    Called on successful 2xx broker responses to re-enable broker calls
    immediately. Silent on FileNotFoundError (already absent — the cleared
    state is the desired state). Logs other OSError to stderr but never
    raises. Wraps anything else silently per relay discipline.
    """
    try:
        marker_path = _broker_down_marker_path()
        try:
            os.unlink(marker_path)
        except FileNotFoundError:
            return
        except OSError as e:
            sys.stderr.write(
                f"relay shared._clear_broker_down_marker: unlink failed "
                f"({type(e).__name__}: {e}); marker may persist until TTL\n"
            )
            # Explicit flush
            # paired with the _mark_broker_down sibling above; same Windows pipe-
            # buffering rationale.
            try:
                sys.stderr.flush()
            except Exception:
                pass
    except Exception:
        # Last-ditch silent-degrade — never crash CC on a marker cleanup bug.
        pass


# ---------------------------------------------------------------------------
# #24 (C) sender-outbox advisory + recovery flush
# ---------------------------------------------------------------------------
# The receive hook is the natural per-turn place to (a) DRAIN the sender-outbox
# once the broker is reachable again (send.py persists a failed send there — the
# generic broker_client outbox), and (b) surface any queued sends inline so the
# sending agent/user knows they exist. Gated on UserPromptSubmit (once per user
# turn) + a cheap non-empty stat, so the idle path pays ~one getsize() and NEVER
# a glob/fsync sweep (preserves the #18/F2 win — flush only runs when there IS a
# queued send to drain). Best-effort throughout; never raises out of do_receive.

# Bounded recovery-flush on the receive hot path: at most K entries, single-shot
# each, so a large post-outage backlog OR a health-OK-but-/api/event-slow broker
# can never stall the UserPromptSubmit turn (bounded foreground work). The remainder
# drains over subsequent turns + the off-hot-path MCP-reconnect background flush.
_RELAY_FLUSH_MAX = 3
_RELAY_FLUSH_TIMEOUT_S = 1.0


def _broker_quick_healthy(timeout_s: float = 1.5) -> bool:
    """Cheap, BOUNDED GET /api/health so the receive-hook recovery flush never
    stalls the user turn on a WEDGED broker (listening but black-holing). True
    only on a fast 2xx; never raises. This bounds the hook's broker interaction
    regardless of the down-marker state — a DOWN broker refuses instantly, a
    WEDGED one is capped at timeout_s (H2)."""
    try:
        req = Request(_broker_base_url() + "/api/health", method="GET")
        with urlopen(req, timeout=timeout_s) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def _count_relay_sends_in_outbox(outbox_path) -> int:
    """Count outbox entries that are relay sends (endpoint == '/api/event').
    O(N) parse; only called when the outbox is non-empty (rare). Never raises."""
    n = 0
    try:
        with open(outbox_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if isinstance(entry, dict) and entry.get("endpoint") == "/api/event":
                    n += 1
    except Exception:
        return n
    return n


def _sender_outbox_advisory(hook_event_name: str) -> str:
    """Once per user turn, if relay sends are queued in the outbox: flush them
    when the broker is reachable (recovery trigger) and return a one-line
    advisory. Returns "" (no advisory) on the idle/empty path or any failure."""
    try:
        if hook_event_name != "UserPromptSubmit" or broker_client is None:
            return ""
        outbox = broker_client._outbox_path()
        try:
            if os.path.getsize(str(outbox)) <= 0:
                return ""
        except OSError:
            return ""
        n = _count_relay_sends_in_outbox(outbox)
        if n <= 0:
            return ""
        # Flush ONLY when the broker is confirmed reachable this fire: the marker
        # gate (fast, cached) AND a bounded health probe (H2 — a wedged broker can
        # otherwise stall the turn through flush_outbox's multi-attempt POSTs).
        if (not _is_broker_known_down()) and _broker_quick_healthy():
            try:
                broker_client.flush_outbox(
                    max_entries=_RELAY_FLUSH_MAX, attempts=1, timeout_s=_RELAY_FLUSH_TIMEOUT_S)
            except Exception:
                pass
            # #24 follow-up (1) (D78): do NOT re-scan the outbox post-flush. An entry
            # can leave by being SENT *or* dropped (TTL-stale/corrupt/permanent-4xx),
            # and flush_outbox returns only TOTAL-across-endpoints counts (a relay-
            # specific "m pending" is not derivable), so a 2nd O(N) scan/turn is the
            # redundant work. Emit an honest banner from the
            # single pre-flush `n` — never claim delivery (M1).
            return (
                f"⚠ {n} relay send(s) were queued while the broker was unreachable; "
                f"a recovery retry has run — any still-pending will keep retrying automatically."
            )
        return (
            f"⚠ Broker unreachable — your last {n} relay send(s) are queued "
            f"locally and will retry automatically on recovery."
        )
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Registry duplicate-name GC throttle (ADR D54)
# ---------------------------------------------------------------------------
# The SessionStart register hook runs an auto duplicate-name GC at most once per
# REGISTRY_GC_INTERVAL_S (default 6h) across ALL sessions, gated by a shared
# timestamp marker at RELAY_DIR / REGISTRY_GC_MARKER_FILENAME. Same silent-degrade
# + future-timestamp-guard discipline as the broker-down marker above, but
# FAIL-OPEN (missing/malformed/future marker -> due): an extra cheap GC scan is
# harmless, a suppressed GC leaves stale rows. registry_gc.py holds the sweep
# logic; these two helpers only gate + stamp.
DEFAULT_REGISTRY_GC_INTERVAL_S = 6 * 60 * 60.0  # 6h
REGISTRY_GC_MARKER_FILENAME = "registry-gc.json"


def _registry_gc_interval_s() -> float:
    raw = os.environ.get("REGISTRY_GC_INTERVAL_S")
    if raw:
        try:
            v = float(raw)
            if v > 0:
                return v
        except ValueError:
            pass
    return DEFAULT_REGISTRY_GC_INTERVAL_S


def _registry_gc_marker_path() -> Path:
    return RELAY_DIR / REGISTRY_GC_MARKER_FILENAME


def _registry_gc_due(now_ms: int) -> bool:
    """True iff a duplicate-name GC is due (>= interval since last run, or no/
    invalid marker). FAIL-OPEN: marker absent / unparseable / non-int ts / future
    ts -> due. Silent-degrade: any error -> due (an extra scan is harmless)."""
    try:
        marker_path = _registry_gc_marker_path()
        if not marker_path.exists():
            return True
        data = json.loads(marker_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return True
        last_ts = data.get("last_run_ts")
        if not isinstance(last_ts, int) or isinstance(last_ts, bool):
            return True
        if last_ts > now_ms + 5000:  # future-ts guard (clock skew/corrupt) -> due
            return True
        interval_ms = _registry_gc_interval_s() * 1000
        return (now_ms - last_ts) >= interval_ms
    except Exception:
        return True


def _stamp_registry_gc(now_ms: int) -> None:
    """Record now_ms as the last GC run (atomic tempfile + os.replace).
    Best-effort: any failure writes one stderr line and returns — never raises.
    Caller stamps ONLY after a successful GC + registry write."""
    try:
        RELAY_DIR.mkdir(parents=True, exist_ok=True)
        marker_path = _registry_gc_marker_path()
        payload = {"last_run_ts": int(now_ms)}
        fd, tmp_path = tempfile.mkstemp(
            prefix=".registry-gc.", suffix=".json.tmp", dir=str(RELAY_DIR),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, marker_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as e:
        sys.stderr.write(
            f"relay shared._stamp_registry_gc: marker write failed "
            f"({type(e).__name__}); GC throttle not advanced this fire\n"
        )
        try:
            sys.stderr.flush()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# stdin / hook-input
# ---------------------------------------------------------------------------

def read_hook_stdin() -> dict:
    """Read all of stdin, parse JSON, return dict.

    Raises ValueError with a clear diagnostic on parse failure.
    """
    raw = sys.stdin.read()
    if not raw:
        raise ValueError("hook stdin was empty (CC always sends JSON; missing payload indicates a wiring bug)")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"hook stdin is not valid JSON: {e}; first 200 chars: {raw[:200]!r}") from e
    if not isinstance(payload, dict):
        raise ValueError(f"hook stdin JSON must be an object; got {type(payload).__name__}")
    return payload


# ---------------------------------------------------------------------------
# session_id resolution
# ---------------------------------------------------------------------------

def is_codex_hook_payload(stdin_payload: dict) -> bool:
    """Return whether *stdin_payload* came from Codex's hook runtime.

    ``model`` is a documented Codex-specific common hook field. Claude payloads
    do not carry it, so this avoids guessing from a UUID-shaped identifier that
    both products use.
    """
    return (
        isinstance(stdin_payload, dict)
        and isinstance(stdin_payload.get("model"), str)
        and bool(stdin_payload.get("model"))
    )


def _broker_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    bearer = os.environ.get("BROKER_BEARER_TOKEN")
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    return headers


def _load_broker_state_agents() -> list[dict]:
    """Read the existing broker registry that binds Codex threads to adapters."""
    agents: list[dict] = []
    cursor: Optional[str] = None
    while True:
        params: dict[str, Any] = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        url = f"{_broker_base_url()}/api/state?{urlencode(params)}"
        req = Request(url, headers=_broker_headers(), method="GET")
        try:
            with urlopen(req, timeout=_receive_timeout_s()) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"could not read broker state for Codex relay identity: {type(exc).__name__}"
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("agents"), list):
            raise RuntimeError("broker state returned an invalid agents collection")
        agents.extend(row for row in payload["agents"] if isinstance(row, dict))
        next_cursor = payload.get("next_cursor")
        if next_cursor is None:
            return agents
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
            raise RuntimeError("broker state returned an invalid pagination cursor")
        cursor = next_cursor


def resolve_codex_registration(stdin_payload: dict) -> dict:
    """Resolve a Codex thread to one existing stable R3B/R5B adapter row."""
    thread_id = stdin_payload.get("session_id")
    if not isinstance(thread_id, str) or not thread_id:
        raise RuntimeError("Codex hook payload has no session_id thread binding")
    matches = [
        row
        for row in _load_broker_state_agents()
        if row.get("thread_id") == thread_id
        and row.get("kind") == "codex-session"
        and row.get("transport") == "codex-app-server"
        and isinstance(row.get("id"), str)
        and bool(row.get("id"))
    ]
    if len(matches) != 1:
        state = "missing" if not matches else f"ambiguous ({len(matches)} matches)"
        raise RuntimeError(f"Codex relay adapter binding is {state}")
    return matches[0]

def resolve_session_id(stdin_payload: dict) -> str:
    """Return the session_id. Priority: stdin -> ~/.claude/sessions/{ppid}.json."""
    if is_codex_hook_payload(stdin_payload):
        return str(resolve_codex_registration(stdin_payload)["id"])
    sid = stdin_payload.get("session_id")
    if isinstance(sid, str) and sid:
        return sid

    ppid = os.getppid()
    sessions_file = SESSIONS_DIR / f"{ppid}.json"
    tried = [f"stdin_payload['session_id'] (absent or empty)", f"{sessions_file}"]
    if sessions_file.exists():
        try:
            data = json.loads(sessions_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                file_sid = data.get("sessionId")
                if isinstance(file_sid, str) and file_sid:
                    return file_sid
                tried.append(f"{sessions_file}#sessionId (key absent or empty)")
            else:
                tried.append(f"{sessions_file} (not a JSON object)")
        except (OSError, json.JSONDecodeError) as e:
            tried.append(f"{sessions_file} ({type(e).__name__}: {e})")
    raise RuntimeError(
        "could not resolve session_id; tried: " + " ; ".join(tried)
    )


# ---------------------------------------------------------------------------
# Parent process chain walking — for session_id resolution from subprocess depth
# ---------------------------------------------------------------------------
# session_id resolution (post-C-13: env-var-only)
# ---------------------------------------------------------------------------
# Per C-13 (no auto-derive code) + F-CC11-F-CC16 (research.md): CC injects
# CLAUDE_CODE_SESSION_ID into every subprocess env at process-spawn time
# (Node.js child_process.spawn). Verified empirically on Windows Git Bash via
# 9-test deep-inheritance probe (T1-T7 PASS including heartbeat-style
# fully-detached subprocess pattern).
#
# Pre-C-13 used `resolve_session_id_via_pid_chain` + `_walk_parent_pids*` +
# `_try_pid_for_session_id` (~210 LOC of pid-walk infrastructure). DELETED:
#   - pid-walk: undocumented + broken on Windows ($PPID=1, orphaned subprocess
#     per F-CC11); was confabulated invention (claude-relay's implementation
#     was MINE; circular self-citation per F-CC5/F-CC8)
#   - State files at ~/.claude/relay/state/{ppid}.json — fully removed
#     2026-05-11 (post-D35 follow-up; F-CR8 fix shape (A) reframed to
#     deletion). Were write-only diagnostic markers; no consumer post-D35
#     pid-walk removal. STATE_DIR retained for the broker-down marker
#     (still load-bearing per F-CR6 ~330x speedup).

def resolve_cc_session_id() -> Optional[str]:
    """Resolve current CC session_id via CLAUDE_CODE_SESSION_ID env var.

    Returns CLAUDE_CODE_SESSION_ID env var value (str), or None if unset.
    No pid-walk, no fallback (per C-13: explicit-source-only).

    For HOOK scripts, prefer reading session_id from the hook stdin payload
    directly (per F-CC1: payload['session_id'] is canonical for hook context).
    This helper is for slash-command-body scripts + library helpers that don't
    receive stdin payload.
    """
    return os.environ.get("CLAUDE_CODE_SESSION_ID")


# ---------------------------------------------------------------------------
# cursor I/O
# ---------------------------------------------------------------------------

def read_cursor(session_id: str) -> int:
    """Return last_seen_event_id from the cursor file. Missing/corrupt -> 0."""
    cursor_path = CURSORS_DIR / f"{session_id}.json"
    if not cursor_path.exists():
        return 0
    try:
        data = json.loads(cursor_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            v = data.get("last_seen_event_id", 0)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        sys.stderr.write(
            f"relay shared.read_cursor: cursor file {cursor_path} has unexpected shape; treating as 0\n"
        )
        return 0
    except (OSError, json.JSONDecodeError) as e:
        sys.stderr.write(
            f"relay shared.read_cursor: {cursor_path} corrupt ({type(e).__name__}: {e}); treating as 0\n"
        )
        return 0


def write_cursor(session_id: str, event_id: int) -> None:
    """Atomic + MONOTONIC + LOCKED write of the agent_message cursor file.

    v1.8 (F-CC26 fix): adds OS file lock + monotonic re-read around write to
    eliminate the inter-process race that allowed concurrent hook fires
    (UserPromptSubmit + SessionStart racing on /restore + /compact) to regress
    the cursor backward, triggering silent message re-delivery. Mirrors the
    discipline write_error_cursor adopted in v1.7.2 (D38 iter-2 MEDIUM fix).

    Lock strategy:
      - Windows: msvcrt.locking on a sentinel lockfile (advisory; stdlib).
      - POSIX: fcntl.flock on the same sentinel.
    Lock is held across the entire read -> compare -> write critical section.
    Best-effort: if locking is unsupported, falls through to monotonic-without-
    lock with a stderr warning (still prevents the most common regression).
    """
    CURSORS_DIR.mkdir(parents=True, exist_ok=True)
    cursor_path = CURSORS_DIR / f"{session_id}.json"
    lock_path = CURSORS_DIR / f"{session_id}.lock"
    new_event_id = int(event_id)

    lock_fh = None
    try:
        # Create-or-open the lockfile; Windows requires file to exist before locking.
        lock_fh = open(lock_path, "a+", encoding="utf-8")
        _locked = _acquire_exclusive_lock(lock_fh)
        try:
            # Critical section: read current cursor, take max, write.
            if cursor_path.exists():
                try:
                    current = json.loads(cursor_path.read_text(encoding="utf-8"))
                    current_eid = current.get("last_seen_event_id", 0) if isinstance(current, dict) else 0
                    if isinstance(current_eid, int) and current_eid > new_event_id:
                        new_event_id = current_eid
                except (OSError, json.JSONDecodeError):
                    pass  # unreadable treated as 0
            payload = {"last_seen_event_id": new_event_id, "updated_at": int(time.time() * 1000)}
            fd, tmp_path = tempfile.mkstemp(prefix=f".{session_id}.", suffix=".json.tmp", dir=str(CURSORS_DIR))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, cursor_path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        finally:
            if _locked:
                _release_exclusive_lock(lock_fh)
    finally:
        if lock_fh is not None:
            try:
                lock_fh.close()
            except Exception:
                pass


# Inter-session message kinds the receive hook MUST poll. v1.8 (F-CC26 fix):
# the hook used to hardcode only "agent_message" at do_receive line ~929, so
# kind="agent_reply" + kind="handoff" rows addressed by to_agent never got
# auto-injected. Senders (subagents/ peer) emitted these legitimately per
# v1.0 SSE event kinds spec; recipients silently lost them. Backfill: after
# this ship, the next hook fire fetches all 3 kinds from each recipient's
# cursor (broker re-delivers via event_id > since). 12 historically orphaned
# messages (10 agent_reply + 2 handoff) become deliverable.
INTER_SESSION_MESSAGE_KINDS: tuple[str, ...] = ("agent_message", "agent_reply", "handoff")


def fetch_pending_inter_session_messages(
    session_id: str,
    since: int,
    limit_per_kind: int = 20,
    broker_base_url: Optional[str] = None,
) -> list[dict]:
    """Fetch all inter-session message kinds (agent_message + agent_reply + handoff)
    addressed to this session_id, since the given cursor, merged + de-duped by
    event_id + sorted ASC.

    Per-kind fetch shape mirrors fetch_pending_messages (each call is a separate
    broker round-trip). Returned list is the union; caller uses max(event_id) to
    advance the single shared cursor (event_id is one global AUTOINCREMENT counter
    per broker schema, so a single cursor works for all kinds).

    Silent-degrade per kind: if one kind's fetch fails (broker down, 4xx, etc.),
    its messages array is empty for this fire; other kinds still deliver. Mirrors
    the v1.7 receive_errors pattern (independent failure domains).
    """
    seen_event_ids: set[int] = set()
    merged: list[dict] = []
    for kind in INTER_SESSION_MESSAGE_KINDS:
        try:
            result = fetch_pending_messages(
                session_id,
                since=since,
                kind=kind,
                limit=int(limit_per_kind),
                broker_base_url=broker_base_url,
            )
        except Exception as e:
            sys.stderr.write(
                f"relay fetch_pending_inter_session_messages: kind={kind} fetch raised "
                f"({type(e).__name__}: {e}); continuing other kinds\n"
            )
            continue
        for m in (result.get("messages") or []):
            if not isinstance(m, dict):
                continue
            eid = m.get("event_id")
            if not isinstance(eid, int):
                continue
            if eid in seen_event_ids:
                continue
            seen_event_ids.add(eid)
            merged.append(m)
    merged.sort(key=lambda m: m.get("event_id", 0))
    return merged


# ---------------------------------------------------------------------------
# Broker GET — fetch pending messages
# ---------------------------------------------------------------------------

def _broker_base_url(override: Optional[str] = None) -> str:
    if override:
        return override.rstrip("/")
    return os.environ.get("BROKER_BASE_URL", DEFAULT_BROKER_BASE_URL).rstrip("/")


def fetch_pending_messages(
    session_id: str,
    since: int,
    kind: str = "agent_message",
    limit: int = 20,
    broker_base_url: Optional[str] = None,
) -> dict:
    """GET /api/agent/{session_id}/messages?since=...&kind=...&limit=...

    Returns parsed JSON dict on success. On HTTP error / connection error,
    logs metadata to stderr and returns {"messages": [], "next_cursor": None}
    (silent-degrade: missing broker MUST NOT break CC sessions).

    FETCH-FAILED SIGNAL (multi-kind paged-sweep fix): every returned dict carries
    a `fetch_ok` bool. It is True ONLY on the genuine broker-success path (2xx +
    parsed dict body). EVERY degrade branch (broker-known-down short-circuit, 4xx,
    non-object body, invalid JSON, retry-exhaustion) sets `fetch_ok=False`. This
    lets the paged sweep distinguish a FAILED kind (broker/HTTP/JSON error → the
    kind may have un-fetched rows) from a genuinely-EMPTY kind (success, no rows
    above `since`). A failed kind MUST NOT advance the floor past its un-fetched
    rows; an empty-but-ok kind is provably drained above `since`.

    5s timeout per attempt. Bounded retry 50/100/200/400/800 ms only on 5xx
    and connection errors (NOT 4xx).
    """
    base = _broker_base_url(broker_base_url)
    qs = urlencode({"since": int(since), "kind": kind, "limit": int(limit)})
    url = f"{base}/api/agent/{session_id}/messages?{qs}"

    # Broker-known-down cache pre-check (v1.3.1 follow-up): if a recent fire
    # already established the broker is unreachable, short-circuit BEFORE
    # entering the urlopen retry loop. Worst-case 2.05s wall time × 4 hooks
    # × every tool use becomes a 30s amortized free pass during outages.
    # Per spec § R2.7: pre-check happens ONCE before the loop; cache hit
    # returns the canonical degraded shape (same as exhaustion-degrade below).
    if _is_broker_known_down():
        sys.stderr.write(
            f"relay shared.fetch_pending_messages: broker known down within last "
            f"{_broker_down_cache_s()}s; skipping fire for sid={session_id}\n"
        )
        return {"messages": [], "next_cursor": None, "fetch_ok": False}

    bearer = os.environ.get("BROKER_BEARER_TOKEN")
    headers = {"Accept": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    # Receive-side: env-tunable attempts + timeout. Defaults are fast-fail since
    # silent-degrade is acceptable on this path. Worst-case wall time with defaults
    # (1.0s timeout, 2 attempts, 50ms backoff): ~2.05s.
    attempts = _receive_attempts()
    sleeps_ms = [0] + list(RETRY_BACKOFF_MS[: max(attempts - 1, 0)])
    timeout_s = _receive_timeout_s()
    last_error: Optional[str] = None
    # Track the LAST error category (not 'ever saw'). Spec § Anti-Patterns:
    # 'Do NOT mark broker as down on HTTPError' — so a transient URLError
    # followed by HTTPError 5xx (broker now responding) MUST NOT mark down.
    # Discriminator iter-1 HIGH finding fix.
    last_error_was_unreachable = False
    for attempt_idx, backoff_ms in enumerate(sleeps_ms):
        if backoff_ms > 0:
            time.sleep(backoff_ms / 1000.0)
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout_s) as resp:
                body = resp.read()
                data = json.loads(body.decode("utf-8"))
                if not isinstance(data, dict):
                    sys.stderr.write(
                        f"relay shared.fetch_pending_messages: broker returned non-object body; degrading\n"
                    )
                    return {"messages": [], "next_cursor": None, "fetch_ok": False}
                # Normalize: ensure messages key exists + is a list
                if "messages" not in data or not isinstance(data["messages"], list):
                    data["messages"] = []
                if "next_cursor" not in data:
                    data["next_cursor"] = None
                # Genuine broker success (2xx + parsed dict body): mark fetch_ok so
                # the paged sweep can treat an empty messages array as provably
                # DRAINED (not a silent failure). Overwrite any broker-sent value.
                data["fetch_ok"] = True
                # Broker responded successfully (200 + dict body) — clear any stale
                # broker-down marker so subsequent fires resume immediately. Per
                # spec § R2.7: clear ONLY on 2xx success with parsed dict body;
                # NOT on 4xx, non-object, or invalid-JSON degrade branches (those
                # do not prove the broker is healthy).
                _clear_broker_down_marker()
                return data
        except HTTPError as e:
            if 400 <= e.code < 500:
                # 4xx: do NOT retry
                sys.stderr.write(
                    f"relay shared.fetch_pending_messages: 4xx {e.code} for sid={session_id} since={since}; degrading\n"
                )
                return {"messages": [], "next_cursor": None, "fetch_ok": False}
            last_error = f"HTTP {e.code}"
            # Broker IS responding (HTTPError raises on 5xx with body). Per
            # spec anti-pattern: do NOT mark down on HTTPError. Reset flag so
            # a prior URLError does not cause a stale mark on loop exit.
            last_error_was_unreachable = False
        except (URLError, ConnectionError, TimeoutError, OSError) as e:
            last_error = f"{type(e).__name__}: {e}"
            last_error_was_unreachable = True
        except (json.JSONDecodeError, ValueError) as e:
            sys.stderr.write(
                f"relay shared.fetch_pending_messages: broker returned invalid JSON ({type(e).__name__}); degrading\n"
            )
            return {"messages": [], "next_cursor": None, "fetch_ok": False}
        except Exception as e:  # pragma: no cover — defensive
            last_error = f"{type(e).__name__}: {e}"
            # Bare-Exception is unknown cause; do NOT mark down per spec
            # anti-pattern. Could be programmer error, OOM, etc — none of
            # these signal 'broker unreachable'.
            last_error_was_unreachable = False

    # Loop exhausted. Mark broker down ONLY if the LAST attempt's error was
    # URL/Conn/Timeout/OSError (per spec § R2.7 + § Anti-Patterns: HTTPError
    # 5xx means broker is responding right now, so a prior URLError followed
    # by HTTPError 5xx must NOT trigger a mark — broker recovered mid-loop).
    if last_error_was_unreachable:
        _mark_broker_down(last_error or "unreachable")
    sys.stderr.write(
        f"relay shared.fetch_pending_messages: exhausted {len(sleeps_ms)} attempts for sid={session_id} "
        f"since={since}; last_error={last_error}; degrading\n"
    )
    return {"messages": [], "next_cursor": None, "fetch_ok": False}


# ---------------------------------------------------------------------------
# Broker POST — propagate display_name (v1.3.1 / D29)
# ---------------------------------------------------------------------------

def post_status_with_display_name(
    session_id: str,
    friendly_name: Optional[str],
    broker_base_url: Optional[str] = None,
    *,
    event_label: str = "register",
) -> None:
    """POST /api/agent/{session_id}/status with display_name.

    No-op when friendly_name is None or empty (peer spec recommendation (a):
    skip the POST entirely if no name yet, rather than POST display_name=null).

    Silent-degrade on broker failure (URLError / OSError / TimeoutError /
    HTTPError): emit metadata to stderr, return None. Callers (SessionStart
    hook + set_name skill) MUST NOT crash CC sessions on broker downtime.

    Uses _broker_base_url() to resolve broker URL (override > BROKER_BASE_URL
    env > DEFAULT_BROKER_BASE_URL). Honors BROKER_BEARER_TOKEN env if set.

    Payload (per peer broker v1.3.1 / D29):
      {
        "kind": "cc-session",
        "channel": "#cc-relay",
        "status": "idle",
        "last_activity_ts": <epoch_ms>,
        "display_name": <friendly_name>,
        "client_event_id": f"relay-{event_label}-{session_id}-{ts_ms}"
      }

    Timeout: tunable via BROKER_POST_TIMEOUT_S env (default 2.0s) — fast-fail
    so SessionStart hook latency stays low when the broker is down.
    No retry — SessionStart fires once; if the broker is down we drop and the
    next /relay name (or next session boot) will re-attempt.
    """
    if not friendly_name:
        return

    # Broker-known-down cache pre-check (v1.3.1 follow-up): if a recent fire
    # already established the broker is unreachable, short-circuit BEFORE
    # entering the urlopen path. SessionStart fires this on every panel boot
    # and `/relay name` fires it on every rename — eating the full POST timeout
    # × every panel × every fire during outages would dwarf the cache TTL.
    if _is_broker_known_down():
        sys.stderr.write(
            f"relay shared.post_status_with_display_name: broker known down within last "
            f"{_broker_down_cache_s()}s; skipping fire for sid={session_id}\n"
        )
        return

    try:
        base = _broker_base_url(broker_base_url)
        url = f"{base}/api/agent/{session_id}/status"
        ts_ms = int(time.time() * 1000)
        payload = {
            "kind": "cc-session",
            "channel": "#cc-relay",
            "status": "idle",
            "last_activity_ts": ts_ms,
            "display_name": friendly_name,
            "client_event_id": f"relay-{event_label}-{session_id}-{ts_ms}",
        }
        body = json.dumps(payload).encode("utf-8")

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        bearer = os.environ.get("BROKER_BEARER_TOKEN")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"

        req = Request(url, data=body, headers=headers, method="POST")
        with urlopen(req, timeout=_post_timeout_s()) as resp:
            # Drain body but ignore content.
            try:
                resp.read()
            except Exception:
                pass
        # Broker accepted the POST (urlopen did not raise) — clear any stale
        # broker-down marker so subsequent fires resume immediately. Per spec
        # § R2.8: clear ONLY on 2xx success (urlopen raises HTTPError on 4xx/5xx,
        # so reaching this line means the response was 2xx).
        _clear_broker_down_marker()
        return
    except HTTPError as e:
        if 400 <= e.code < 500:
            sys.stderr.write(
                f"relay shared.post_status_with_display_name: 4xx {e.code} for sid={session_id}; degrading\n"
            )
            return
        sys.stderr.write(
            f"relay shared.post_status_with_display_name: 5xx {e.code} for sid={session_id}; degrading\n"
        )
        return
    except (URLError, ConnectionError, TimeoutError, OSError) as e:
        # Broker unreachable — mark down for cache short-circuit on subsequent
        # fires. Per spec § R2.8: mark BEFORE the stderr write; preserve
        # existing degrade behavior. NOT marked on HTTPError (broker responded)
        # nor on bare-Exception (could be anything; defensive).
        _mark_broker_down(f"{type(e).__name__}: {e}")
        sys.stderr.write(
            f"relay shared.post_status_with_display_name: {type(e).__name__} for sid={session_id} "
            f"({e}); degrading\n"
        )
        return
    except Exception as e:  # pragma: no cover — defensive
        sys.stderr.write(
            f"relay shared.post_status_with_display_name: unexpected {type(e).__name__} for sid={session_id} "
            f"({e}); degrading\n"
        )
        return


# ---------------------------------------------------------------------------
# Sender-name resolution
# ---------------------------------------------------------------------------

# Wire senders can be friendly names or legacy session IDs. An unresolved name
# may already be a reply address; an unresolved UUID is an unknown identity.
# SYNC: server.py::_UUID_RE, same pattern, same flag.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def resolve_sender_name(
    from_agent_id: str,
    registry_path: Optional[str] = None,
    *,
    registry: Optional[dict] = None,
) -> str:
    """Look up from_agent_id in the registry; return the friendly name.

    On a MISS the answer depends on which key-space the input is in: a
    session-id-shaped value that the registry does not know is `"<unknown>"`;
    anything else is already an address and is returned unchanged. Never a
    truncation — the value is rendered as a reply address, and a truncated
    identifier addresses nobody in either key-space.

    If `registry` is provided (a pre-loaded dict), use it directly — avoids
    redundant disk reads when called in a tight loop (Fix #5).
    """
    if not from_agent_id:
        return "<unknown>"
    if registry is None:
        path = Path(registry_path) if registry_path else REGISTRY_PATH
        registry = {}
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    registry = data
            except (OSError, json.JSONDecodeError) as e:
                sys.stderr.write(
                    f"relay shared.resolve_sender_name: registry {path} unreadable ({type(e).__name__}); treating as a miss\n"
                )
    entry = registry.get(from_agent_id)
    if isinstance(entry, dict):
        name = entry.get("name")
        if isinstance(name, str) and name:
            # ENTRY-POINT FOLD (C-48), SENDER SIDE. C-48 says the fold applies
            # "sender AND recipient", and this is the sender half: the string
            # returned here is rendered as `from <sender>` in the receive hook's
            # message block, and the reader is a model that is instructed to
            # answer by addressing that name. So it is an ADDRESS, not decoration.
            #
            # Nothing misroutes if it is missed — send.py folds `--to` at its own
            # entry — but leaving it raw would print one casing while every
            # mailbox uses another, which is the "folds at some sites and not
            # others" state C-48 calls the worst outcome available.
            #
            # The fallback below is deliberately NOT a session_id prefix any more.
            return normalize_name(name)
    # This display value is also a reply address. Never truncate it: shortened
    # values resolve in neither the friendly-name nor session-id namespace.
    # A name may be absent from the SID-keyed registry while already being a
    # valid wire address. An unresolved UUID instead means the identity is unknown.
    if _UUID_RE.match(from_agent_id):
        return "<unknown>"
    return from_agent_id


def _load_registry_for_format() -> dict:
    """One-shot registry load for hot-path callers (format_messages_for_injection).
    Returns empty dict on any read error."""
    if not REGISTRY_PATH.exists():
        return {}
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _format_ts_local(ts_ms: Any) -> str:
    """Format epoch-ms timestamp as local-time YYYY-MM-DD HH:MM:SS. Falls back to '<unknown ts>'."""
    if not isinstance(ts_ms, (int, float)):
        return "<unknown ts>"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000.0))
    except (OverflowError, OSError, ValueError):
        return "<unknown ts>"


def _extract_text(content: Any) -> str:
    """If content is a JSON envelope ({text, sender_name, ...}), return inner text; else as-is."""
    if not isinstance(content, str):
        return str(content)
    stripped = content.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            obj = json.loads(stripped)
            if isinstance(obj, dict) and isinstance(obj.get("text"), str):
                return obj["text"]
        except json.JSONDecodeError:
            pass
    return content


def format_messages_for_injection(messages: list[dict]) -> str:
    """Render messages list as a Markdown block for additionalContext injection."""
    if not messages:
        return ""
    # Fix #5: load registry ONCE before the loop instead of N disk reads.
    registry = _load_registry_for_format()
    lines = ["**Inter-session messages received:**", ""]
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        ts_str = _format_ts_local(msg.get("ts"))
        from_id = msg.get("from_agent") or ""
        sender = resolve_sender_name(from_id, registry=registry) if from_id else "<unknown>"
        event_id = msg.get("event_id", "?")
        text = _extract_text(msg.get("content", ""))
        lines.append(f"[{ts_str} from {sender}] (event {event_id})")
        # Indent each line of body with '> ' (markdown blockquote)
        body_lines = text.splitlines() or [""]
        for bl in body_lines:
            lines.append(f"> {bl}")
        lines.append("")
    # ACK-DRIVEN: hook injections are invisible to the render-grep (they are NOT
    # origin.kind==channel records), so an explicit broker_ack is their ONLY
    # confirmation signal. Tell the model it MUST ack these ids or they re-deliver
    # next fire (bounded duplicate; never a silent loss).
    ack_ids = [m.get("event_id") for m in messages
               if isinstance(m, dict) and isinstance(m.get("event_id"), int)]
    if ack_ids:
        lines.append(
            f"→ You MUST confirm receipt or these RE-DELIVER next turn: "
            f"broker_ack(event_ids={ack_ids})"
        )
        lines.append(
            "→ If you are a SUBAGENT: do NOT act on or broker_ack these — they "
            "are the orchestrator's; surface them and let the orchestrator ack."
        )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Hook response emission
# ---------------------------------------------------------------------------

def emit_hook_response(hook_event_name: str, additional_context: Optional[str]) -> None:
    """Write the JSON hook response to stdout.

    With non-empty additional_context, emit:
      {"hookSpecificOutput": {"hookEventName": <name>, "additionalContext": <text>}}
    Else emit `{}` (CC accepts empty response).

    Uses sys.stdout.write (not print) to avoid trailing-newline parser issues.
    """
    if additional_context:
        payload = {
            "hookSpecificOutput": {
                "hookEventName": hook_event_name,
                "additionalContext": additional_context,
            }
        }
    else:
        payload = {}
    sys.stdout.write(json.dumps(payload))
    sys.stdout.flush()


def _fetch_agent_messages_aggregate(
    *, since: int, kind: str, limit: int = 200
) -> dict:
    """Read one broker aggregate page without entering a legacy mailbox."""
    params = urlencode({"since": int(since), "kind": kind, "limit": int(limit)})
    url = f"{_broker_base_url()}/api/agent-messages?{params}"
    req = Request(url, headers=_broker_headers(), method="GET")
    try:
        with urlopen(req, timeout=_receive_timeout_s()) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"could not read Codex error stream: {type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
        raise RuntimeError("Codex error stream returned an invalid message page")
    return payload


def fetch_pending_codex_errors(
    registration: dict, *, since: int, limit: int = 10
) -> dict:
    """Read addressed errors without racing the adapter's ordered inbox."""
    agent_id = registration.get("id")
    relay_name = registration.get("relay_name")
    recipients = {
        value.casefold()
        for value in (agent_id, relay_name)
        if isinstance(value, str) and value
    }
    if not recipients:
        raise RuntimeError("Codex relay adapter row has no delivery identity")

    cursor = int(since)
    selected: list[dict] = []
    while len(selected) < int(limit):
        page = _fetch_agent_messages_aggregate(since=cursor, kind="error", limit=200)
        rows = page.get("messages") or []
        progressed = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            event_id = row.get("event_id")
            if not isinstance(event_id, int) or event_id <= cursor:
                continue
            cursor = event_id
            progressed = True
            to_agent = row.get("to_agent")
            if isinstance(to_agent, str) and to_agent.casefold() in recipients:
                selected.append(row)
                if len(selected) >= int(limit):
                    break
        if len(selected) >= int(limit) or page.get("next_cursor") is None:
            break
        if not progressed:
            next_cursor = page.get("next_cursor")
            if not isinstance(next_cursor, int) or next_cursor <= cursor:
                raise RuntimeError("Codex error stream did not advance")
            cursor = next_cursor
    return {"messages": selected, "next_cursor": cursor}


# ---------------------------------------------------------------------------
# Canonical receive routine
# ---------------------------------------------------------------------------

# ACTIVE PLAN (friendly-name primitive) helpers — used by do_receive for the
# dedup + advisory + auto-rename-sync paths.

# ---------------------------------------------------------------------------
# Relay-name normalisation (C-48) — THE definition; every other site imports it
# ---------------------------------------------------------------------------

_NAME_FOLD = {c: c + 32 for c in range(ord("A"), ord("Z") + 1)}


def normalize_name(name):
    """Fold a relay friendly_name to its canonical form: ASCII A-Z -> a-z.

    WHY THIS EXISTS. A friendly_name is a KEY addressed across machines, and the
    fleet spans a case-preserving-but-case-INSENSITIVE filesystem (NTFS) and a
    case-SENSITIVE one (ext4). Without a fold, `Alpha` and `alpha` are ONE
    mailbox on Windows and TWO on Linux — the same code with two behaviours,
    which is precisely what C-48 forbids. C-47's shape applies: rather than
    detect the collision and compensate, make it unrepresentable.

    WHERE TO CALL IT — AT ENTRY, NOT AT RESOLUTION. A name reaches a process
    from exactly two files (the CC daemon job-state, the relay registry) and two
    arguments (`--name`, `--to`). Fold there and every downstream value is
    canonical by construction: the broker poll identity, the delivery-key
    filename, the reclaim comparisons, the posted `to_agent`. Folding at
    RESOLVERS instead was designed, reviewed and KILLED — the MCP plugin has TWO
    independent identity-producing resolvers, so folding "one site per
    component" left it subscribing under one casing while filing its cursor
    under another. Entry points can be counted; resolvers cannot.

    WHY ASCII TRANSLATE AND NOT `.lower()` / `.casefold()`. Both are
    Unicode-aware and map characters from OUTSIDE the validated name alphabet
    INTO it — `.casefold()` sends the Kelvin sign U+212A to 'k', `.lower()`
    sends the Turkish dotted capital U+0130 to 'i' + U+0307. That would let two
    genuinely distinct strings land in one mailbox, inventing the very collision
    this fold exists to remove. The validated alphabet is [A-Za-z0-9._-]
    (`NAME_RE`, mirrored in set_name.py / register_session.py / the plugin), so
    an A-Z-only translate collapses exactly the case-variants it is meant to and
    nothing else. Every character outside A-Z passes through untouched, so this
    is the identity function on any name already lowercase — including the CC
    job-state names, which no validator guards.

    Non-str input is returned UNCHANGED. Callers pass Optional[str]; a resolver
    that returns None must keep returning None rather than raise, and the
    plugin's copy of this function must agree — a key that differs between the
    writer and the reader is the whole defect being closed here.
    """
    if not isinstance(name, str):
        return name
    return name.translate(_NAME_FOLD)


def resolve_daemon_job_name(session_id: str) -> Optional[str]:
    """Background-agent durable name from the CC Agent-View daemon job-state.

    CC's background-agents daemon writes `~/.claude/jobs/<sid>/state.json` with
    the user-set name it durably tracks across respawns:
        {"name": "test_a", "nameSource": "user", "template": "bg", ...}
    The daemon re-issues the session_id on each respawn but re-writes this file
    with the SAME name under the new sid — so this is the drift-immune identity
    for a background agent. Honored ONLY when nameSource == "user" (the user's
    explicit name, per C-13) AND template == "bg" (foreground jobs store a
    formatted display title in `name`, not the relay friendly_name).

    Mirrors `local-relay-channel/server.py:_read_daemon_job_name` (kept as a
    separate copy there because that MCP server is intentionally standalone and
    does not import this module). Returns the name NORMALISED per C-48
    (`normalize_name`), or None — the plugin's copy folds at the same point, and
    the two must not drift.
    """
    jobs_dir = CLAUDE_DIR / "jobs"
    # The daemon names the job dir by the 8-char "daemonShort" (== sessionId[:8]),
    # NOT the full session_id. Try the full id first (forward-compat), then the
    # short prefix; validate the state.json's own `sessionId` matches to guard
    # against an 8-hex prefix collision between two sessions.
    for candidate in (session_id, session_id[:8]):
        try:
            s = json.loads(
                (jobs_dir / candidate / "state.json").read_text(encoding="utf-8")
            )
        except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(s, dict):
            continue
        sj = s.get("sessionId")
        if candidate == session_id:
            # Full-id dir: the dir name already proves ownership. If a sessionId
            # is present it must still match (defensive).
            if isinstance(sj, str) and sj and sj != session_id:
                continue
        else:
            # Short-prefix (8-hex) dir: REQUIRE an exact sessionId match — the
            # 8-char prefix could collide with a different session
            # (discriminator iter-1 LOW).
            if not (isinstance(sj, str) and sj == session_id):
                continue
        if s.get("nameSource") != "user" or s.get("template") != "bg":
            continue
        name = s.get("name")
        if isinstance(name, str) and name.strip():
            # THE LEAF FOLD (C-48), and it is load-bearing well beyond this
            # function. This is the ONLY job-state name source on the hook side,
            # and its three callers are the router (_resolve_name_from_registry),
            # set_name.py's reclaim scan (_live_bg_jobstate_holders) and
            # register_session.py's display path. Folding the RETURN makes all
            # three canonical with no edit at any of them — in particular the
            # reclaim scan, whose folded-claim-against-RAW-stored comparison is
            # exactly what got the predecessor change reverted.
            #
            # It is also the only place this value CAN be canonicalised: the CC
            # daemon owns `~/.claude/jobs/<sid>/state.json`, set_name.py cannot
            # write it, and no validator guards its `name`. Left raw, a bg agent
            # named `Alpha` polls `Alpha` while every sender posts `alpha`.
            return normalize_name(name.strip())
    return None


def resolve_daemon_job_state(session_id: str) -> Optional[str]:
    """Return the CC daemon job 'state' for a sid (e.g. 'running', 'stopped',
    'done'), or None when there is no job-state dir / it is unreadable.

    Mirrors resolve_daemon_job_name's candidate-dir (full id then 8-char
    prefix) + sessionId-match discipline (guards an 8-hex prefix collision).
    Used to surface a background agent's liveness in the relay-name conflict
    diagnostic (set_name.py). Unlike resolve_daemon_job_name this does NOT gate
    on nameSource/template — liveness applies to any job-state row."""
    jobs_dir = CLAUDE_DIR / "jobs"
    for candidate in (session_id, session_id[:8]):
        try:
            s = json.loads(
                (jobs_dir / candidate / "state.json").read_text(encoding="utf-8")
            )
        except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(s, dict):
            continue
        sj = s.get("sessionId")
        if candidate == session_id:
            if isinstance(sj, str) and sj and sj != session_id:
                continue
        else:
            if not (isinstance(sj, str) and sj == session_id):
                continue
        st = s.get("state")
        if isinstance(st, str) and st:
            return st
        return None
    return None


def _sid_liveness(sid: str) -> str:
    """Best-effort authoritative liveness for a session_id. Returns one of
    "live" | "dead" | "unknown".

    Returns "live" on a POSITIVE life signal:
      (a) a CC daemon job-state with an OWNING, non-terminal 'state' (background
          agents) — aggregated over BOTH candidate dirs (jobs/<sid> and
          jobs/<sid[:8]>) directly, so a stale terminal candidate cannot mask a
          live one, OR
      (b) a live CC foreground process maps the sid in ~/.claude/sessions/*.json
          (CC writes <pid>.json keyed by 'sessionId'; older schema 'session_id').

    Returns "dead" only when BOTH probes complete CLEANLY with no signal (a
    genuinely-gone session has no non-terminal job-state AND no live session file).
    Any probe that RAISES, or finds a file PRESENT-BUT-UNREADABLE (truncated mid
    daemon/CC write, transient lock), is treated as UNCERTAIN => "unknown" so a
    caller never deletes or steals a name from a session that might
    still be alive. A cleanly-ABSENT file (FileNotFoundError) is a clean negative,
    not uncertainty. Both consumers — set_name reclaim-cleanup and register_session
    auto-inherit — want this identical conservative direction. Canonical single
    source of truth (imported by both); supersedes the copy-pasted sessions/*.json
    + job-state probes that had drifted across set_name.py / send.py.

    NOTE: only ever called on OTHER sids (both callers exclude own_session_id),
    so the own-session file never masks a predecessor's death.
    """
    if not sid:
        return "dead"
    uncertain = False
    # (a) background-agent daemon job-state. Aggregate BOTH candidate dirs
    # OURSELVES rather than trusting resolve_daemon_job_state's first-match: that
    # helper returns the FIRST owning candidate's state, so a stale OWNING terminal
    # jobs/<full-sid> file would mask a live (or unreadable) jobs/<sid[:8]> file for
    # the same holder (F1). Ownership mirrors resolve_daemon_job_state: the full-id
    # candidate owns unless its sessionId is a DIFFERENT non-empty string; the
    # sid[:8] candidate owns only if sessionId == sid. Any owning NON-terminal
    # state -> live; unreadable/malformed -> uncertain; non-owning / terminal /
    # absent -> clean negative for THAT candidate (keep checking the other one).
    try:
        jobs_dir = CLAUDE_DIR / "jobs"
        for cand in (sid, sid[:8]):
            try:
                raw = (jobs_dir / cand / "state.json").read_text(encoding="utf-8")
            except FileNotFoundError:
                continue                       # clean-absent
            except OSError:
                uncertain = True               # present-but-unreadable
                continue
            try:
                obj = json.loads(raw)
            except ValueError:
                uncertain = True               # present-but-malformed (mid-write)
                continue
            if not isinstance(obj, dict):
                uncertain = True               # present-but-malformed shape -> uncertain
                continue
            sj = obj.get("sessionId")
            if cand == sid:
                owns = not (isinstance(sj, str) and sj and sj != sid)
            else:
                owns = isinstance(sj, str) and sj == sid
            if not owns:
                continue                       # a different session's dir
            st = obj.get("state")
            if isinstance(st, str) and st and st.lower() not in _TERMINAL_JOB_STATES:
                return "live"                   # live background agent
            if not (isinstance(st, str) and st):
                # owning but NO usable state -> uncertain -> fail toward live (parity
                # with registry_gc._live_bg_jobstate; the set_name reclaim DELETE
                # path uses this probe, so the bias must be under-delete).
                uncertain = True
            # else owning + cleanly-terminal -> dead per THIS candidate; the other
            # candidate may still be live or unreadable, so keep checking.
    except Exception:
        uncertain = True
    # (b) live foreground process mapping (accept BOTH 'sessionId' (current CC
    # schema) and 'session_id' (legacy)).
    try:
        if SESSIONS_DIR.is_dir():
            for sf in SESSIONS_DIR.glob("*.json"):
                try:
                    d = json.loads(sf.read_text(encoding="utf-8"))
                except FileNotFoundError:
                    continue                       # vanished between glob and read
                except (OSError, ValueError):
                    uncertain = True               # present-but-unreadable -> MIGHT be sid
                    continue
                if isinstance(d, dict) and sid in (d.get("sessionId"), d.get("session_id")):
                    return "live"
    except Exception:
        uncertain = True
    # No positive signal: DEAD, unless a probe was uncertain (then fail toward live).
    return "unknown" if uncertain else "dead"


def _is_sid_live(sid: str) -> bool:
    """Back-compat wrapper: True iff NOT provably dead (LIVE or UNKNOWN) -- identical
    semantics to the pre-refactor _is_sid_live. Fail-toward-live so the destructive
    consumers (set_name reclaim-delete, register auto-inherit) never delete/steal a
    name from a session that might be alive. Callers that must distinguish a positive
    live signal from an uncertain probe (e.g. the send path) call _sid_liveness and
    handle "unknown" explicitly.
    """
    return _sid_liveness(sid) != "dead"


# Sentinel for "the registry could not be READ", as distinct from "the registry
# was read and this session has no name". A unique object rather than a string:
# every string is a candidate delivery key, so a string sentinel could itself be
# mistaken for an answer — the exact class of defect this removes. `is`
# comparison only; never truth-tested.
# SYNC: server.py::_NAME_UNKNOWN. The two sides must agree on WHICH registry
# states are facts about this session and which are faults, or they compute
# different keys and it is not a key (D139).
_NAME_UNKNOWN = object()


def _resolve_name_from_registry(session_id: str) -> Any:
    """Best-effort: return friendly_name for this session_id, or None.
    Used to drive the friendly-name-keyed broker poll in do_receive AND, via
    `_delivery_key`, to name the confirmed-set file itself (D130) — the broker
    addresses a recipient by name, so the cursor recording what that recipient
    has seen must be filed under the same name. (This docstring previously said
    the confirmed-set is keyed by session_id; that was true before D130 and is
    now the opposite of the code.)

    (Pre-lossless-refactor this ALSO constructed the channel-cursors MCP dedup
    floor (Site R); that boost is removed — dedup is now the per-sid
    confirmed-set + transcript-grep backstop. The function survives only to
    resolve the broker poll identity.)

    Resolution order MIRRORS server.py:_resolve_my_name so the receive-hook
    backup path routes identically to the MCP push path:
      1. CC daemon job-state (background agents; nameSource=="user" + template=="bg")
         — authoritative for a bg agent's identity; checked FIRST so an inherited
         or stale registry name cannot override it (discriminator iter-1 HIGH).
      2. Relay registry: explicit user-set name ONLY (naming_status=="user_set").
         Legacy/default non-user registry names are NOT treated as routing
         identities (parity with server.py + C-13; discriminator iter-1 MEDIUM).

    Without this, a backgrounded agent's hook path would poll the broker by raw
    session_id instead of its friendly_name and miss name-addressed rows.

    THREE OUTCOMES, NOT TWO, AND THE THIRD IS WHY THIS FUNCTION CHANGED.
      * a `str`          — this session's name.
      * `None`           — ESTABLISHED nameless: the registry was read and holds
                           no user-set name for this session.
      * `_NAME_UNKNOWN`  — COULD NOT ESTABLISH: the registry was absent,
                           unreadable, corrupt, non-UTF-8, or not a JSON object.
    Those last two used to be the same return value, and merging them is what let
    a transient fault be recorded as a fact about this session — `_delivery_key`
    turned it into `confirmed/<sid>.json` while the plugin wrote
    `confirmed/<name>.json`, and every ack was refused for two days (D181). A
    caller cannot recover a distinction its callee threw away, so the split has
    to happen here rather than one frame up.
    """
    # 1. Background-agent durable name from the daemon job-state.
    #
    # WRAPPED, AND THE PLUGIN IS WHY. Cross-family review found this call sitting
    # OUTSIDE the try below while the plugin's mirror wraps its own daemon read in
    # `except Exception: pass` and falls through to the registry. So a raising
    # daemon helper made the hook abort the fire while the plugin still computed a
    # key -- a DIVERGENCE, and a delivery key that differs between the writer and
    # the reader is not a key (D139). Matching the plugin outranks my own view of
    # what a failed job-state read ought to mean, for the same reason the
    # `REGISTRY_PATH.exists()` pre-check was removed above.
    #
    # Falling through is also right on its own terms: a job-state read that FAILED
    # is not evidence of namelessness, and the registry is a second, independent
    # source that may well answer. Refusing here would discard it unread.
    try:
        bg_name = resolve_daemon_job_name(session_id)
    except Exception:
        bg_name = None
    if bg_name:
        return bg_name
    # 2. Relay registry: explicit user-set name only.
    #
    # NO `REGISTRY_PATH.exists()` PRE-CHECK, DELIBERATELY. It used to sit here and
    # sent a missing file to `return None` — "this session has no name" — where the
    # plugin's mirror lets FileNotFoundError (an OSError) reach its except arm and
    # answers UNKNOWN. MEASURED before this change, registry.json ABSENT:
    # plugin `_delivery_key()` -> None, hook `_delivery_key()` -> the sid. Letting
    # the open raise makes the two identical by construction rather than by a
    # pre-check kept in step with an except tuple.
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        # `data` must be an OBJECT. A registry whose top level is a list,
        # string, number or null is not a readable registry — it is a corrupt
        # one, and saying "no name" about it is the merge this function exists
        # to stop. (It also makes `.get` raise AttributeError, which is NOT in
        # the except tuple below — MEASURED raising on all three shapes.)
        if not isinstance(data, dict):
            return _NAME_UNKNOWN
        # AN ABSENT ROW AND A PRESENT-BUT-CORRUPT ROW ARE DIFFERENT ANSWERS.
        # `entry = data.get(session_id) or {}` stood here and collapsed them:
        # a row holding a string, list, number, bool or explicit null fell
        # through the `isinstance(dict)` test to `return None` — reported as
        # ESTABLISHED NAMELESS, which `_delivery_key` then turns into a
        # session-id key. That is the outage's own failure shape surviving
        # inside its fix; cross-family review found it in the plugin's first
        # draft, where 5 of 5 malformed shapes produced a sid key while the
        # absent-row and valid-row controls behaved correctly.
        #
        # Membership is tested rather than truthiness because `.get()` cannot
        # tell an absent key from one whose value is null, and those two need
        # OPPOSITE answers: absent is a genuinely unregistered session
        # (nameless, key by sid); null is a corrupt row (refuse).
        if session_id not in data:
            return None                 # no row at all — genuinely nameless
        entry = data[session_id]
        if not isinstance(entry, dict):
            return _NAME_UNKNOWN        # row present and corrupt — refuse
        name = entry.get("name")
        ns = entry.get("naming_status")
        if isinstance(name, str) and name and ns == "user_set":
            # ENTRY-POINT FOLD (C-48): the registry file is the second of
            # the two places a name enters this process. set_name.py folds
            # before it WRITES, so a post-fix registry already holds
            # lowercase and this is the identity function; it is here for
            # the rows written BEFORE the fold shipped, which must resolve
            # to the same key their senders now address.
            return normalize_name(name)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        # THE ARM THAT USED TO LIE. It fell through to `return None`, which
        # `_delivery_key` turned into a session-id key — so an unreadable
        # registry minted a permanent orphan confirmed-set that silently
        # converts a fail-closed refusal into a stale allow-list.
        # ValueError covers UnicodeDecodeError on a non-UTF-8 registry; OSError
        # covers FileNotFoundError. The plugin's mirror catches the same tuple
        # and returns the same sentinel — the two must agree on degradation as
        # well as on the name they return.
        return _NAME_UNKNOWN
    return None


# ---------------------------------------------------------------------------
# LOSSLESS-DELIVERY refactor (plan quizzical-launching-fox) — per-session
# confirmed-set + transcript-grep backstop + telemetry.
# ---------------------------------------------------------------------------
# Each recipient SESSION keeps a confirmed-set at RELAY_DIR/confirmed/{sid}.json
#     {"floor": int, "acked": [ids], "updated_at": ms}
# FIX 1 (cross-family review) redefines `floor`: "every message ADDRESSED TO ME
# with event_id ≤ floor is confirmed/handled" — NOT a GLOBAL-id contiguous
# high-water. `acked` = confirmed to-me ids ABOVE the floor (e.g. an isolated
# relay_ack above an unfilled gap). An event is "confirmed for me" iff
# `id <= floor OR id in acked`.
#
# Two distinct floor-advance paths, by writer:
#   - relay_ack (server.py) + the emit-FAILURE path here use _collapse_confirmed:
#     floor advances only over a CONTIGUOUS run of acked ids (floor+1 in acked).
#     This is correct for ISOLATED acks/renders where we have NOT swept the inbox
#     — a gapped id must never fold into floor.
#   - the do_receive emit-SUCCESS path uses _advance_floor_locked: after PAGING
#     the recipient inbox and handling every to-me row up to `examined_max`, floor
#     jumps directly to `examined_max` (the broker returns to-me rows contiguously
#     from `since`, so nothing ≤ examined_max is left unhandled). This is what
#     fixes the original starvation bug (floor pinned because floor+1 was some
#     OTHER session's global id, never in this recipient's acked).
#
# This file is WRITTEN by BOTH the relay_ack MCP tool (server.py) and this hook
# (do_receive), so every write MUST hold the sentinel lock + write atomically.
# Locking + atomic-write convention copied verbatim from
# relay-name/scripts/set_name.py.

# Parse the first well-formed channel tag's double-quoted attributes, never the
# peer-authored body: a message can quote event_id="NNNN" without receiving NNNN.
# Claude Code XML-escapes attribute values, but not body text. The cardinality
# check is additional defence against tampered tags, not a substitute for that
# harness boundary. A genuine tag precedes any tag quoted inside its body.
#
# A leading prefix is allowed so a harness wrapper does not strand a real
# delivery. Residual: if a render has no genuine tag, a body-quoted tag may be
# selected. A missing/malformed id instead fails toward re-delivery: the floor
# remains below the message, which can be injected on every later hook fire.
# Single-quoted attributes are not accepted. The plugin's unquoted body header
# is not authoritative, even when it contains a peer-supplied quoted id.
_RENDERED_OPEN_TAG_RE = re.compile(
    r'<channel\b((?:\s+[A-Za-z_][A-Za-z0-9_]*="[^"]*")*)\s*/?>'
)
_RENDERED_ATTR_RE = re.compile(r'\s+([A-Za-z_][A-Za-z0-9_]*)="([^"]*)"')

# Accept exactly this MCP key or this plugin-manifest/MCP-key pair. A suffix
# match could admit another plugin backed by a different event-id namespace,
# confirming mail this session never received. Guard the type before frozenset
# membership because legal JSON lists/dicts are unhashable.
_RENDER_ACCEPTED_SERVERS = frozenset((
    "local-relay-channel",
    "plugin:local-relay-channel:local-relay-channel",
))

_TELEMETRY_FILENAME = "delivery-counters.json"

# FIX 1 (cross-family review) — paged-sweep bounds for the gap-aware backstop.
# The broker returns to-me rows contiguously from `since` ascending, capped at
# SWEEP_PAGE_LIMIT per kind. We page (re-query with since=max-returned-id) until
# a page is short (no more rows) OR we hit the per-fire work cap. The cap bounds
# a pathological backlog (e.g. a floor=0 migration sweep) so a single hook fire
# can never block CC on an unbounded broker scan. If the cap is hit, floor is
# NOT advanced past the last FULLY-examined page boundary (and we log it) — so
# the remaining tail rolls forward to the next fire; never silently dropped.
SWEEP_PAGE_LIMIT = 100          # broker max `limit` per kind (server.py le=100)
SWEEP_MAX_PAGES = 10            # ≤10 broker round-trips per fire
SWEEP_MAX_MESSAGES = 1000       # ≤1000 to-me rows examined per fire


def _confirmed_dir() -> Path:
    return RELAY_DIR / "confirmed"


def _confirmed_path(sid: str) -> Path:
    return _confirmed_dir() / f"{sid}.json"


def _confirmed_lock_path(sid: str) -> Path:
    # Sentinel lock file (NOT the confirmed json itself) — on Windows
    # os.replace() refuses to overwrite a file held open in the same process.
    return _confirmed_dir() / f"{sid}.json.lock"


# --- delivery key (D130) ---------------------------------------------------
# THE DEFECT THIS CLOSES, in one sentence: the broker addresses a recipient by
# NAME and the client filed its delivery cursor under SESSION_ID, so a rename
# moved the name and orphaned the cursor. The broker inbox query is
#     WHERE to_agent = ? AND event_id > ? AND kind = ?
# (server.py get_agent_messages) — `event_id > since` is the ONLY re-delivery
# suppression there is, and `since` comes from this cursor. So the cursor MUST
# be keyed by whatever `to_agent` is keyed by. That is the whole fix; everything
# the epoch machinery used to do was an attempt to survive the mismatch.
_KEY_RE = re.compile(r"^[A-Za-z0-9._\-]{1,64}$")
# Sentinel distinguishing "caller did not resolve a name" from "caller resolved
# it and the answer was None". A plain None default conflates the two and would
# silently re-resolve, reintroducing the double-read race.
_UNRESOLVED = object()
# Windows refuses these basenames with ANY extension. One of them passing through
# as a delivery key would fail every cursor write and replay forever, silently —
# so it must fall back to the sid rather than be trusted.
_RESERVED_BASENAMES = frozenset(
    ["CON", "PRN", "AUX", "NUL"]
    + ["COM%d" % i for i in range(1, 10)]
    + ["LPT%d" % i for i in range(1, 10)]
    # The telemetry counters file lives in the SAME directory the confirmed-sets
    # now occupy, and its stem passes _KEY_RE. Before name-keying that namespace
    # was UUID-only and could not collide; it can now. MEASURED: a session keyed
    # `delivery-counters` reads the counters file as its confirmed-set (both
    # readers survive it), so `_seed_name_key_from_sid` takes its "migrated
    # already" early return on it and `_gate_broker_ack` then refuses every id
    # forever, while writes clobber the telemetry. Derived from the constant
    # rather than spelled out, so renaming the file cannot silently un-reserve it.
    + [_TELEMETRY_FILENAME.split(".")[0].upper()]
)
# NOT handled here, deliberately: NTFS is case-insensitive, so `Alpha` and `alpha`
# would share one cursor file while the broker treats them as two distinct
# mailboxes (its SQL `to_agent = ?` is case-SENSITIVE). Case-folding the key here
# would MERGE two real mailboxes, which is the loss, not the fix. The correct
# place is set_name.py's holder scan: refuse to claim a case-variant of a name
# somebody already holds.


def _delivery_key(session_id: str, name: Any = _UNRESOLVED) -> Optional[str]:
    """The identity the broker ADDRESSES this session by — hence the identity
    its delivery cursor must be FILED under. `None` means REFUSE: we could not
    establish who we are, and no key may be invented.

    Falls back to the session_id for a NAMELESS session, which is correct and is
    deliberately kept: nothing is addressed to such a session by name, so it
    cannot be renamed-into and cannot replay, and the plugin's `_delivery_key`
    does exactly the same. C-37 forbids the session id BECOMING the key when a
    name exists; it has never forbidden a nameless session from using its own id.
    Refusing there would be a regression dressed as a fix.

    WHAT CHANGED, AND IT IS ONE DISTINCTION RATHER THAN A NEW MECHANISM. Several
    registry states used to collapse into one return value — MEASURED on the
    pre-fix bytes, `_delivery_key(sid)` returned the sid for ALL of: registry
    absent, registry empty, corrupt JSON, row=null, row=string, top-level list.
    Only two of those are namelessness. The rest are "I could not check", and
    answering them with a well-formed filename is what made a total identity
    failure indistinguishable from an ordinary miss for two days across three
    silent alarms (D181). `_NAME_UNKNOWN` now carries that distinction out of
    `_resolve_name_from_registry`, and this function refuses on it.

    THE `session_id` PARAMETER STAYS, and the asymmetry with the plugin — whose
    `_delivery_key()` takes none — is deliberate rather than an oversight. This
    hook is a fresh subprocess handed the true id on stdin every fire, so its id
    is current BY CONSTRUCTION. Only the plugin, a process that outlives the
    value, can be handed a stale one.

    Pass `name` when the caller has ALREADY resolved it, so the identity is read
    from one snapshot instead of twice. Resolving twice in one fire is a real
    race: a rename landing between the two reads makes the caller poll the broker
    as one identity and file the answer under another. `_NAME_UNKNOWN` is an
    ACCEPTED value for that parameter — the caller must be able to hand its one
    snapshot straight through, refusal included, or the saved read reintroduces
    the very race it was saved to avoid.

    The returned value becomes a FILENAME. `_resolve_name_from_registry` may
    return a string from CC's daemon job-state (`resolve_daemon_job_name`), which
    nothing in this codebase validates — set_name.py's NAME_RE only ever gated
    the /relay-name path. So re-validate at the point of use. A name that
    cannot be a safe filename is not a usable delivery key; falling back to the
    sid preserves exactly today's behaviour rather than inventing a new one.
    """
    if name is _UNRESOLVED:
        try:
            name = _resolve_name_from_registry(session_id)
        except Exception:
            # WAS `return session_id`. A resolver that RAISED told us nothing
            # about this session; answering with a filename anyway IS the root
            # cause, not a degradation of it.
            return None
    if name is _NAME_UNKNOWN:
        return None
    if not name or not isinstance(name, str):
        return session_id
    if not _KEY_RE.match(name) or name in (".", ".."):
        return session_id
    # Windows basenames that cannot be created regardless of extension. A name
    # that passes _KEY_RE but cannot be written would fail every cursor write and
    # replay forever, silently.
    if name.split(".")[0].upper() in _RESERVED_BASENAMES:
        return session_id
    return name


def _seed_name_key_from_sid(key: str, session_id: str) -> None:
    """ONE-TIME migration, per name, at the first name-keyed read.

    A session that already accrued a floor under the sid-keyed scheme keeps it —
    but ONLY when the old file's `epoch_name` PROVES that floor accrued under
    this same name. That is the one job epoch_name was ever genuinely good for,
    and it is the last one: nothing writes it any more, so this reader drains a
    fixed population and can be deleted once the fleet has rolled over.

    An unprovable floor is dropped, not adopted. A floor accrued under `alpha`
    applied to `beta` would permanently suppress beta-addressed ids below it.
    Re-delivery is safer than silently claiming receipt under another identity.
    """
    if key == session_id:
        return                      # nameless: already the same file
    try:
        if _confirmed_path(key).exists():
            return                  # migrated already; never re-run
        old = _confirmed_path(session_id)
        if not old.exists():
            return                  # nothing to carry
        data = json.loads(old.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return
        # Provenance must be ESTABLISHED, not assumed. Both conditions, and the
        # sticky broken-flag counts as unprovable (it means the floor conflates
        # two or more name epochs — OQ-RELAY-8).
        if data.get("epoch_name") != key or bool(data.get("epoch_broken", False)):
            return
        floor = data.get("floor")
        if not isinstance(floor, int) or isinstance(floor, bool) or floor <= 0:
            return
        # Carry the floor only. `acked` holds ids parked above a gap; they are
        # globally-unique point cross-offs that re-confirm harmlessly, and
        # carrying them would widen what this migration asserts.
        # _advance_floor_locked is the correct writer here: it is monotonic
        # (floor = max(prev_floor, target_floor)) and with awaiting_ack_ids=None
        # it clobbers nothing.
        _advance_floor_locked(key, floor)
        sys.stderr.write(
            f"relay shared: migrated delivery cursor sid={session_id[:8]}… -> "
            f"name={key!r} at floor {floor} (epoch-proven)\n"
        )
    except Exception as e:
        # Fail toward "no seed": the session starts at 0 and re-delivers. Bounded
        # duplicate, never loss.
        sys.stderr.write(
            f"relay shared._seed_name_key_from_sid: {key!r} skipped "
            f"({type(e).__name__}: {e}); cursor starts fresh\n"
        )


def _acquire_confirmed_lock(fh) -> bool:
    """NB-retry exclusive lock on the sentinel handle. Copied from
    set_name.py:_acquire_lock (msvcrt / fcntl, ×10 @ 50ms)."""
    try:
        if os.name == "nt":
            import msvcrt
            for _ in range(10):
                try:
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    return True
                except OSError:
                    time.sleep(0.05)
            return False
        else:
            import fcntl
            for _ in range(10):
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return True
                except (BlockingIOError, OSError):
                    time.sleep(0.05)
            return False
    except Exception:
        return False


def _release_confirmed_lock(fh) -> None:
    """Release the sentinel lock. Copied from set_name.py:_release_lock."""
    try:
        if os.name == "nt":
            import msvcrt
            try:
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass


def _read_confirmed_set(session_id: str, default_floor: int = 0) -> dict:
    """Read this session's confirmed-set. Returns normalized
    {"floor": int, "acked": list[int]}.

    Default when absent (B2): floor = `default_floor` (the caller passes the
    current cursors/{sid}.json value so migration is seamless — a session that
    has been advancing the plain cursor inherits it as its starting floor),
    acked = []. Silent-degrade: on corrupt/unreadable file, return the default.
    """
    p = _confirmed_path(session_id)
    if not p.exists():
        return {"floor": int(default_floor), "acked": [],
                "epoch_name": None, "epoch_broken": False}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"floor": int(default_floor), "acked": [],
                    "epoch_name": None, "epoch_broken": False}
        floor = data.get("floor", default_floor)
        floor_corrupt = False
        if not isinstance(floor, int) or isinstance(floor, bool) or floor < 0:
            floor = int(default_floor)
            floor_corrupt = True   # PRESENT-but-invalid floor => provenance untrusted
        raw_acked = data.get("acked", [])
        acked: list[int] = []
        if isinstance(raw_acked, list):
            for v in raw_acked:
                if isinstance(v, int) and not isinstance(v, bool) and v > floor:
                    acked.append(v)
        # awaiting_ack (P2, subagent-safe): the ids the HOOK injected-to-MAIN that
        # are not yet confirmed — the ONLY ids broker_ack (server.py) may fold.
        # None == ABSENT (pre-upgrade file / hook not fired since upgrade) =>
        # broker_ack falls back to permissive (transition window). A list (even
        # empty) is the authoritative allow-list. Pruned > floor (self-cleaning).
        aw_raw = data.get("awaiting_ack", None)
        if aw_raw is None:
            awaiting = None
        elif isinstance(aw_raw, list):
            awaiting = sorted({v for v in aw_raw
                               if isinstance(v, int) and not isinstance(v, bool)
                               and v > floor})
        else:
            awaiting = None
        # epoch tag (OQ-RELAY-6 / D78): identity the floor STARTED accruing under +
        # sticky cross-epoch-break flag. STAMPED only by _advance_floor_locked; parsed
        # here so every reader/writer round-trips them. MUST match the plugin
        # local-relay-channel/server.py:_read_confirmed_set verbatim.
        # FAIL-CLOSED on malformed provenance (D78 "corrupt epoch => suppress"):
        #  - epoch_name wrong-type => None (suppress);
        #  - epoch_broken PRESENT but non-bool => True/broken (suppress); ABSENT => False
        #    (the omit-when-default common path is unaffected);
        #  - epoch_name paired with a corrupt (present-but-invalid) floor is decoupled
        #    from its accrual range => drop epoch_name (suppress).
        en_raw = data.get("epoch_name", None)
        epoch_name = en_raw if isinstance(en_raw, str) else None
        eb_raw = data.get("epoch_broken", False)
        epoch_broken = eb_raw if isinstance(eb_raw, bool) else True
        if floor_corrupt:
            epoch_name = None
        return {"floor": floor, "acked": sorted(set(acked)),
                "awaiting_ack": awaiting,
                "epoch_name": epoch_name, "epoch_broken": epoch_broken}
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {"floor": int(default_floor), "acked": [],
                "epoch_name": None, "epoch_broken": False}


def _collapse_confirmed(floor: int, acked: set[int]) -> tuple[int, list[int]]:
    """Collapse the contiguous run of acked ids immediately above `floor` INTO
    `floor`. Isolated acks above a gap stay in `acked`. THE per-event invariant.
    Returns (new_floor, sorted_remaining_acked).
    """
    acked = {a for a in acked if a > floor}
    while (floor + 1) in acked:
        floor += 1
        acked.discard(floor)
    return floor, sorted(acked)


# ---------------------------------------------------------------------------
# #3 repost rate-limit marker (OQ-RELAY-6 / D78)
# ---------------------------------------------------------------------------
# The receive-hook mirror (do_receive) posts the confirmed-set {floor, acked} to
# the broker every UserPromptSubmit. This per-sid marker records the LAST
# SUCCESSFULLY-posted (name, floor, acked) so an unchanged turn skips the POST
# (kills the redundant per-turn repost). Holds ONLY benign last-posted state, so a
# write race is at worst one extra idempotent repost => NO lock. Fail-OPEN: an
# unreadable/absent marker => post.
#
# WHY FAIL-OPEN IS SAFE (corrected 2026-08-09). This used to read "the over-mark
# protection is the epoch gate" — a mechanism DELETED at D130 — so the sole stated
# rationale for a live fail-open branch named something that no longer exists. The
# protection was never the gate: the FLOOR cannot pass an unconfirmed id, because
# _collapse_confirmed advances only across a CONTIGUOUS run and D142's clamp holds the
# barrier at the sweep's own examined boundary when a fetch did not drain. An extra
# POST therefore re-sends an already-safe floor, and the server scopes point cross-offs
# by to_agent with a status guard. This marker is a bandwidth optimisation, never a
# safety device — which is what makes failing open correct here.
def _mirror_state_dir() -> Path:
    return RELAY_DIR / "mirror-state"


def _mirror_state_path(sid: str) -> Path:
    return _mirror_state_dir() / f"{sid}.json"


def _read_mirror_state(sid: str) -> Optional[dict]:
    try:
        p = _mirror_state_path(sid)
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _mirror_state_changed(sid: str, name: str, floor, acked) -> bool:
    """True (=> POST) iff (name, floor, acked) differs from the last successful
    mirror. Fail-OPEN: absent/unreadable marker => changed => post."""
    last = _read_mirror_state(sid)
    if last is None:
        return True
    return (last.get("name") != name
            or last.get("floor") != floor
            or list(last.get("acked") or []) != list(acked))


def _write_mirror_state(sid: str, name: str, floor, acked) -> None:
    """Record the last SUCCESSFUL mirror (atomic tempfile + os.replace, modeled on
    _stamp_registry_gc). Best-effort: any failure writes one stderr line, never
    raises. `floor` may be None — do_receive posts None when the floor is 0 (nothing
    contiguously confirmed yet). The epoch gate this line used to name was deleted at
    D130."""
    try:
        d = _mirror_state_dir()
        d.mkdir(parents=True, exist_ok=True)
        payload = {"name": name, "floor": floor, "acked": list(acked)}
        fd, tmp_path = tempfile.mkstemp(
            prefix=".mirror-state.", suffix=".json.tmp", dir=str(d),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, _mirror_state_path(sid))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as e:
        sys.stderr.write(
            f"relay _write_mirror_state: write failed for sid={sid[:8]}… "
            f"({type(e).__name__}: {e}); next turn reconciles\n"
        )


def _write_confirmed_set_locked(
    session_id: str, new_ids, default_floor: int = 0
) -> dict:
    """Add `new_ids` (iterable of int) to this session's confirmed-set under the
    sentinel lock + atomic write, collapsing the contiguous run into `floor`.
    Returns the resulting {"floor", "acked"}.

    Shared with server.py:relay_ack — both writers take this same lock. On
    lock-failure / write error: silent-degrade, return the current on-disk
    state without writing (next fire reconciles).
    """
    cdir = _confirmed_dir()
    try:
        cdir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        sys.stderr.write(
            f"relay shared._write_confirmed_set_locked: mkdir failed "
            f"({type(e).__name__}: {e}); skipping confirmed-set write\n"
        )
        return _read_confirmed_set(session_id, default_floor)

    lock_path = _confirmed_lock_path(session_id)
    try:
        lock_path.touch(exist_ok=True)
    except OSError:
        pass
    try:
        with open(lock_path, "r+", encoding="utf-8") as lock_fh:
            locked = _acquire_confirmed_lock(lock_fh)
            if not locked:
                sys.stderr.write(
                    f"relay shared._write_confirmed_set_locked: could not lock "
                    f"confirmed-set for sid={session_id[:8]}…; skipping write\n"
                )
                return _read_confirmed_set(session_id, default_floor)
            try:
                cur = _read_confirmed_set(session_id, default_floor)
                floor = cur["floor"]
                acked = set(cur["acked"])
                for nid in new_ids:
                    if isinstance(nid, int) and not isinstance(nid, bool) and nid > 0:
                        if nid > floor:
                            acked.add(nid)
                floor, acked_list = _collapse_confirmed(floor, acked)
                # epoch tag (OQ-RELAY-6 / D78): PRESERVE verbatim from `cur` (this
                # writer never accrues the floor under a new identity — only
                # _advance_floor_locked stamps). Omit-when-default matches the plugin.
                epoch_name = cur.get("epoch_name")
                epoch_broken = bool(cur.get("epoch_broken", False))
                payload = {
                    "floor": floor,
                    "acked": acked_list,
                    "updated_at": int(time.time() * 1000),
                }
                if epoch_name is not None:
                    payload["epoch_name"] = epoch_name
                if epoch_broken:
                    payload["epoch_broken"] = epoch_broken
                p = _confirmed_path(session_id)
                fd, tmp_str = tempfile.mkstemp(
                    prefix=".confirmed.", suffix=".json.tmp", dir=str(cdir)
                )
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(payload, f)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_str, p)
                except Exception:
                    try:
                        os.unlink(tmp_str)
                    except OSError:
                        pass
                    raise
                return {"floor": floor, "acked": acked_list}
            finally:
                if locked:
                    _release_confirmed_lock(lock_fh)
    except Exception as e:
        sys.stderr.write(
            f"relay shared._write_confirmed_set_locked: write failed for "
            f"sid={session_id[:8]}… ({type(e).__name__}: {e}); next fire reconciles\n"
        )
        return _read_confirmed_set(session_id, default_floor)


def _advance_floor_locked(
    session_id: str,
    target_floor: int,
    extra_acked_ids=(),
    default_floor: int = 0,
    awaiting_ack_ids=None,
) -> dict:
    """FIX 1 (cross-family review): advance this session's confirmed-set floor
    DIRECTLY to `target_floor` — the high-water of TO-ME messages fully examined
    (swept + handled) this fire — then fold `extra_acked_ids` (ids > the new
    floor) into `acked` and prune `acked` to ids strictly above the new floor.

    WHY this replaces the `floor+1 in acked` contiguity advance for do_receive:
    `floor` no longer means "every GLOBAL event_id ≤ floor is confirmed" — it
    means "every message ADDRESSED TO ME with event_id ≤ floor is confirmed/
    handled." The broker returns to-me rows CONTIGUOUSLY from `since` ascending
    (WHERE to_agent=? AND event_id>? ORDER BY event_id ASC), so once we have
    paged through and HANDLED every to-me row up to `target_floor` (injected the
    genuine drops, suppressed the rendered/acked ones), advancing floor to
    `target_floor` cannot bury an unconfirmed to-me message — there is none ≤
    `target_floor` left unhandled. Isolated higher acks (above a future gap) are
    preserved in `extra_acked_ids`.

    Caller contract (CRITICAL for losslessness): `target_floor` MUST be the
    highest to-me event_id whose DELIVERY IS CONFIRMED this fire (rendered, acked,
    OR successfully emitted). If an injected id ≤ target_floor failed to emit, the
    caller MUST lower `target_floor` below it (or skip this call) so the next fire
    re-sweeps + re-delivers it. See do_receive's emit-then-confirm ordering.

    Monotonic: never lowers floor below the on-disk value (max with current).
    Same lock + atomic-write discipline as _write_confirmed_set_locked. On
    lock-failure / write error: silent-degrade, return current on-disk state.
    """
    cdir = _confirmed_dir()
    try:
        cdir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        sys.stderr.write(
            f"relay shared._advance_floor_locked: mkdir failed "
            f"({type(e).__name__}: {e}); skipping confirmed-set write\n"
        )
        return _read_confirmed_set(session_id, default_floor)

    lock_path = _confirmed_lock_path(session_id)
    try:
        lock_path.touch(exist_ok=True)
    except OSError:
        pass
    try:
        with open(lock_path, "r+", encoding="utf-8") as lock_fh:
            locked = _acquire_confirmed_lock(lock_fh)
            if not locked:
                sys.stderr.write(
                    f"relay shared._advance_floor_locked: could not lock "
                    f"confirmed-set for sid={session_id[:8]}…; skipping write\n"
                )
                return _read_confirmed_set(session_id, default_floor)
            try:
                cur = _read_confirmed_set(session_id, default_floor)
                prev_floor = cur["floor"]
                # Monotonic: never regress below the stored floor.
                floor = max(prev_floor, int(target_floor))
                acked = {a for a in cur["acked"] if a > floor}
                for nid in extra_acked_ids:
                    if isinstance(nid, int) and not isinstance(nid, bool) and nid > floor:
                        acked.add(nid)
                # An ack contiguous with the just-advanced floor still collapses
                # in (keeps acked minimal); gapped acks stay above.
                floor, acked_list = _collapse_confirmed(floor, acked)
                # epoch tag: DELETED (D130). It existed to answer "which name did
                # this floor accrue under?" — unanswerable for a file named after a
                # session_id, which is why it needed a stamp, a sticky corruption
                # flag, and a laundering guard. This file is now named after the
                # NAME, so the question is answered by the filename and cannot be
                # gotten wrong. The one case the stamp genuinely served — carrying a
                # pre-existing sid-keyed floor across the migration — is handled once
                # by _seed_name_key_from_sid, which READS the old stamp and never
                # writes a new one.
                # P2 (subagent-safe): record the hook-injected-to-MAIN unconfirmed
                # ids as the broker_ack allow-list, pruned > the new floor (an id
                # <= floor is already confirmed). Written under THIS lock so the
                # floor/acked/awaiting_ack triple is always mutually consistent.
                # awaiting_ack_ids is None at legacy call sites (no awaiting arg) =>
                # PRESERVE the on-disk value (cur, read above under this lock)
                # rather than clobber it.
                confirmed_now = set(acked_list)   # MED1: never leave an id in BOTH
                if awaiting_ack_ids is None:
                    aw_keep = cur.get("awaiting_ack")
                    awaiting_out = ([a for a in aw_keep
                                     if a > floor and a not in confirmed_now]
                                    if isinstance(aw_keep, list) else None)
                else:
                    awaiting_out = sorted({
                        a for a in awaiting_ack_ids
                        if isinstance(a, int) and not isinstance(a, bool)
                        and a > floor and a not in confirmed_now
                    })
                payload = {
                    "floor": floor,
                    "acked": acked_list,
                    "updated_at": int(time.time() * 1000),
                }
                if awaiting_out is not None:
                    payload["awaiting_ack"] = awaiting_out
                p = _confirmed_path(session_id)
                fd, tmp_str = tempfile.mkstemp(
                    prefix=".confirmed.", suffix=".json.tmp", dir=str(cdir)
                )
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(payload, f)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_str, p)
                except Exception:
                    try:
                        os.unlink(tmp_str)
                    except OSError:
                        pass
                    raise
                return {"floor": floor, "acked": acked_list}
            finally:
                if locked:
                    _release_confirmed_lock(lock_fh)
    except Exception as e:
        sys.stderr.write(
            f"relay shared._advance_floor_locked: write failed for "
            f"sid={session_id[:8]}… ({type(e).__name__}: {e}); next fire reconciles\n"
        )
        return _read_confirmed_set(session_id, default_floor)


def _find_own_transcript(session_id: str) -> Optional[Path]:
    """Glob the unique transcript for this session:
    ~/.claude/projects/*/{session_id}.jsonl. Returns the path or None.

    Per the verified probe: a session finds its own transcript via this glob
    (unique match; do NOT hand-derive the dir encoding). Silent-degrade: any
    error → None (caller fails toward delivery).
    """
    try:
        projects_dir = CLAUDE_DIR / "projects"
        matches = list(projects_dir.glob(f"*/{session_id}.jsonl"))
        if matches:
            # Unique by contract; if >1 (shouldn't happen) take the most recent.
            if len(matches) == 1:
                return matches[0]
            return max(matches, key=lambda p: p.stat().st_mtime)
    except OSError:
        pass
    return None


def _rendered_event_ids_in_transcript(session_id: str) -> tuple[set[int], bool]:
    """Scan this session's transcript for channel messages that RENDERED into
    context and collect their event_ids.

    A rendered channel message appears as a JSONL line whose parsed JSON has
    type=="user" AND origin.kind=="channel"; the rendered event id is in
    message.content (a string) as event_id="NNNN". PARSE WITH json.loads PER
    LINE (a naive regex on the escaped text fails).

    Returns (set_of_rendered_event_ids, read_ok).

    read_ok is False at EXACTLY TWO sites, both whole-file, both enumerated with
    ast rather than by reading: the transcript could not be located (path is
    None), or an OSError was raised opening/reading it. There is no other
    `return (set(), False)` in this function and read_ok is never assigned. The
    caller then FAILS TOWARD DELIVERY (treat unknowns as NOT rendered →
    re-inject).

    A PER-ENTRY failure never sets read_ok False -- but the per-entry failures do
    NOT all behave alike, and that distinction is where two earlier versions of
    this docstring were wrong, in opposite directions:
      * a line that is not valid JSON is caught and SKIPPED WITHOUT incrementing
        `_unparsed` (the handler is a bare `continue`);
      * an entry whose message or content is the wrong TYPE, or a channel render
        whose tag yields no usable id, DOES increment `_unparsed`.
    So `_unparsed` counts unusable CHANNEL RENDERS, not unparseable lines. It is
    a diagnostic emitted once per fire and never affects read_ok.

    A UnicodeDecodeError raised while iterating the file is NOT caught here (the
    handler is `except OSError`) and propagates. Said explicitly because the
    previous wording claimed "cannot be ITERATED" returns False, which is broader
    than the code, and the version before THAT claimed False on "ANY read/parse"
    error, which is broader still.
    """
    path = _find_own_transcript(session_id)
    if path is None:
        return set(), False
    rendered: set[int] = set()
    # Count local-relay renders that yielded NO usable
    # event_id. Without this a zero-confirmation return is indistinguishable from "no
    # renders at all" -- both produce (set(), True), `read_ok` stays True so the
    # caller does NOT take the safe unknown branch, and nothing anywhere is logged.
    # Reported ONCE per fire below, never per entry: this runs on every user turn.
    _unparsed = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Cheap pre-filter to skip non-channel lines fast.
                if '"channel"' not in line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    # A single bad line must not poison the whole scan; skip it.
                    continue
                if not isinstance(obj, dict):
                    continue
                if obj.get("type") != "user":
                    continue
                # MAIN-CONTEXT ONLY (cross-family GAN, subagent-safety): a channel
                # render inside a SUBAGENT turn is NOT the orchestrator seeing the
                # message. Grep-confirming it would advance the cursor with no main
                # delivery → silent loss. Skip sidechain (subagent) entries; fail
                # toward delivery (the hook re-injects into main + tracks it in
                # awaiting_ack). Empirically main renders are isSidechain=false and
                # subagent turns write to a SEPARATE transcript file (so this is
                # currently defense-in-depth), but the invariant is made explicit.
                if obj.get("isSidechain") is True:
                    continue
                origin = obj.get("origin")
                if not isinstance(origin, dict) or origin.get("kind") != "channel":
                    continue
                # Only this relay plugin's channel renders are authoritative.
                # Another channel plugin may use unrelated message-id semantics,
                # even if it later adds an event_id attribute.
                #
                # A render with no `server`, a null `server`, or a differently-spelled
                # one is skipped and re-injected. That fails toward DELIVERY rather
                # than loss, which is the direction we want -- but "benign" would be
                # an overstatement: it is benign only if acting twice on the message
                # is harmless. Plausible triggers are a plugin rename or a transcript
                # migration, not peer behaviour.
                #
                # BOTH REGISTRATION FORMS ARE ACCEPTED (see _RENDER_ACCEPTED_SERVERS
                # for why the test is exact membership and not a suffix match). A
                # bare-equality guard against the mcp key alone would skip EVERY
                # render on a box where the relay is enabled as a plugin, and confirm
                # nothing, silently. That is not a lost duplicate: the broker sweep is
                # `WHERE event_id > ?` with NO delivery-status filter, so a floor
                # pinned below an unconfirmed render re-serves and re-injects the same
                # row on EVERY hook fire (measured: 12 injections in 12 fires, driving
                # the real do_receive). One `/plugin` click is the entire trigger.
                _srv = origin.get("server")
                if not isinstance(_srv, str) or _srv not in _RENDER_ACCEPTED_SERVERS:
                    continue
                # COUNTED, not silently skipped (iteration 3 MED-2). These two shapes
                # are accepted relay renders that yield no usable id, which is exactly
                # what the counter below claims to measure -- but they used to `continue`
                # in front of it, so the counter's own promise ("a non-zero count means
                # something changed") would have read ZERO through precisely the change
                # most worth detecting: a transcript-format migration making `content` a
                # structured block list, which would skip every render and pin the floor.
                msg = obj.get("message")
                if not isinstance(msg, dict):
                    _unparsed += 1
                    continue
                content = msg.get("content")
                if not isinstance(content, str):
                    _unparsed += 1
                    continue
                # TAG ONLY (D10): parse the opening tag's ATTRIBUTE LIST and read the
                # event id by KEY. This IS a search for the first well-formed tag (see
                # the OFFSET paragraph at the regex) -- an earlier version of this
                # comment said "not a search" on the line above a .search() call. What
                # makes it safe is not the search-vs-match choice and not the quoting:
                # it is that the tag must carry EXACTLY ONE event_id attribute.
                #
                # The old code took the FIRST event_id and broke. Because every
                # peer-supplied attribute renders BEFORE event_id, an injected pair won
                # that race and the genuine id was discarded (measured: ids=[99001], 2920
                # dropped). Requiring cardinality 1 makes a tampered tag yield NOTHING,
                # which re-delivers -- the safe direction -- instead of confirming an id
                # that was never delivered, which mutes the reaper.
                #
                # The old finditer admitted unboundedly many ids from peer-authored body
                # text (measured: 21 from one crafted message); that stays closed.
                tag = _RENDERED_OPEN_TAG_RE.search(content)
                _got = False
                if tag is not None:
                    _ids = [_v for _k, _v in _RENDERED_ATTR_RE.findall(tag.group(1))
                            if _k == "event_id"]
                    if len(_ids) == 1:
                        try:
                            rendered.add(int(_ids[0]))
                            _got = True
                        except (ValueError, TypeError):
                            pass
                if not _got:
                    # Missing/malformed tags, ids, attribute cardinality or
                    # content types fail toward re-delivery. Count that path so
                    # a changed harness rendering shape is not invisible.
                    _unparsed += 1
    except OSError:
        # Transcript exists but unreadable → fail toward delivery.
        return set(), False
    if _unparsed:
        # Metadata only, per this module's docstring: two counts and a filename,
        # never message content. One line per fire regardless of entry count.
        #
        # THE WRITE IS INSIDE THE TRY, not just the flush, because this site sits on
        # the SUCCESS path immediately before the safe return: a raise here (closed or
        # broken stderr) would replace a correct under-confirming result with an
        # exception, in a receive hook the module docstring requires to silent-degrade.
        try:
            sys.stderr.write(
                f"relay shared._rendered_event_ids_in_transcript: {_unparsed} "
                f"local-relay render(s) yielded no usable event_id "
                f"({len(rendered)} confirmed) in {path.name}\n"
            )
            sys.stderr.flush()
        except Exception:
            pass
    return rendered, True


def _bump_delivery_counters(deltas: dict) -> None:
    """B4 telemetry: increment per-source confirmation counters. Sources:
    `acked` (already confirmed by relay_ack), `grep_found` (state #3 found in
    transcript), `redelivered` (state #2/#3 injected). Enables measuring
    ack-miss rate + dup rate.

    Best-effort + locked (the counter file could be touched by concurrent hook
    fires). Silent-degrade: a telemetry write failure must NEVER affect delivery.
    """
    if not deltas:
        return
    try:
        cdir = _confirmed_dir()
        cdir.mkdir(parents=True, exist_ok=True)
        counter_path = cdir / _TELEMETRY_FILENAME
        lock_path = cdir / f"{_TELEMETRY_FILENAME}.lock"
        try:
            lock_path.touch(exist_ok=True)
        except OSError:
            pass
        with open(lock_path, "r+", encoding="utf-8") as lock_fh:
            locked = _acquire_confirmed_lock(lock_fh)
            try:
                cur = {}
                if counter_path.exists():
                    try:
                        loaded = json.loads(counter_path.read_text(encoding="utf-8"))
                        if isinstance(loaded, dict):
                            cur = loaded
                    except (OSError, json.JSONDecodeError, ValueError):
                        cur = {}
                for k, v in deltas.items():
                    if not isinstance(v, int) or v <= 0:
                        continue
                    prev = cur.get(k)
                    cur[k] = (prev + v) if isinstance(prev, int) and not isinstance(prev, bool) else v
                cur["updated_at"] = int(time.time() * 1000)
                fd, tmp_str = tempfile.mkstemp(
                    prefix=".delivery-counters.", suffix=".json.tmp", dir=str(cdir)
                )
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(cur, f)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_str, counter_path)
                except Exception:
                    try:
                        os.unlink(tmp_str)
                    except OSError:
                        pass
                    raise
            finally:
                if locked:
                    _release_confirmed_lock(lock_fh)
    except Exception:
        # Telemetry is non-load-bearing; never propagate.
        pass


def _build_pending_mcp_reconnect_advisory(session_id: str) -> Optional[str]:
    """One-line advisory if /relay-name was run but /mcp reconnect not yet.
    Marker file written by set_name.py (Site N); cleared by SessionStart hook
    (Site T) OR auto-stale after 1h. Returns None when no advisory needed.
    """
    try:
        marker = RELAY_DIR / "pending-mcp-reconnect" / f"{session_id}.flag"
        if not marker.exists():
            return None
        try:
            data = json.loads(marker.read_text(encoding="utf-8"))
            new_name = data.get("new_name", "<unknown>")
            ts = int(data.get("ts", 0))
        except (json.JSONDecodeError, OSError, TypeError, ValueError):
            return None
        # Auto-stale after 1h.
        if (int(time.time() * 1000) - ts) > 3_600_000:
            try:
                marker.unlink()
            except OSError:
                pass
            return None
        return (
            f"[relay] You renamed this session to '{new_name}' but haven't "
            f"run `/mcp reconnect` yet. Push notifications addressed to "
            f"'{new_name}' won't arrive until you do."
        )
    except Exception:
        return None


# Strict /rename detection: line-start, exact slash-rename, valid name shape
# only (matches set_name.py NAME_RE + NAME_MAX). Anchored at line-start to
# avoid matching `/rename` mid-content. Multi-line mode for prompts that
# carry leading whitespace lines.
_RENAME_RE = re.compile(r"^\s*/rename\s+([A-Za-z0-9._\-]{1,64})\s*$", re.MULTILINE)


def _maybe_auto_sync_rename(session_id: str, prompt: str) -> None:
    """Detect `/rename foo` in user prompt → spawn set_name.py to sync registry
    + broker display_name + pending-mcp-reconnect marker. Non-blocking
    (detached subprocess) so receive-hook never waits on broker latency.

    Side-effect only; emits nothing back into the prompt context. The pending-
    mcp-reconnect marker, written by the spawned set_name.py, becomes the
    advisory signal injected on the NEXT UserPromptSubmit by
    _build_pending_mcp_reconnect_advisory.
    """
    if not prompt:
        return
    m = _RENAME_RE.search(prompt)
    if not m:
        return
    new_name = m.group(1)
    skill = Path.home() / ".agents" / "skills" / "relay-name" / "scripts" / "set_name.py"
    if not skill.exists():
        return
    try:
        env = os.environ.copy()
        env["CLAUDE_CODE_SESSION_ID"] = session_id  # explicit; defense-in-depth
        log_path = RELAY_DIR / "auto-rename.log"
        with open(log_path, "ab") as logf:
            kwargs = {"stdout": logf, "stderr": logf, "env": env}
            if os.name == "nt":
                # Windows: detach so receive-hook return doesn't wait on child.
                kwargs["creationflags"] = 0x00000008 | 0x00000200  # DETACHED | NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            subprocess.Popen([sys.executable, str(skill), "--name", new_name], **kwargs)
    except Exception:
        pass  # advisory only; failure non-fatal


def _build_naming_nudge(session_id: str) -> Optional[str]:
    """Naming UX: build a one-line additionalContext nudge until user claims a
    friendly name via `/relay-name`.

    Per APPENDIX 3 + 4 of plan: nudge fires AT MOST ONCE per session-lifecycle
    (cursor file at CURSORS_DIR / f"{session_id}-naming-nudged.txt" suppresses
    subsequent calls; cursor cleared by register_session.py on every SessionStart
    so fresh sessions / VSCode restarts / /compact + /restore each get one nudge).

    Suppression rule (post-cwd-derive-revert): suppress only when
    `naming_status == 'user_set'` (post-`/relay-name`). Default name
    `<reponame>-<short-sid-8>` does NOT suppress — user is encouraged to claim
    a friendly name for cross-session messaging.

    Returns None when:
      - cursor file already exists (already nudged this lifecycle)
      - registry entry missing OR registry unreadable
      - naming_status is 'user_set' (user has claimed a name)
      - cursor file write fails (avoid re-fire spam)

    On fire: atomic-create the cursor file BEFORE returning the nudge string,
    so a crash mid-emit doesn't produce a re-fire next prompt. Slight bias
    toward under-nudging vs double-nudging. Silent-degrade per receive-hook
    discipline — never raise.
    """
    try:
        cursor_path = CURSORS_DIR / f"{session_id}-naming-nudged.txt"
        if cursor_path.exists():
            return None
        if not REGISTRY_PATH.exists():
            return None
        try:
            data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        entry = data.get(session_id) if isinstance(data, dict) else None
        if not isinstance(entry, dict):
            return None
        if entry.get("naming_status") == "user_set":
            return None
        name = entry.get("name")
        try:
            CURSORS_DIR.mkdir(parents=True, exist_ok=True)
            cursor_path.touch(exist_ok=True)
        except OSError:
            return None
        # `name or session_id[:8]` STOOD HERE, and it made the one message whose
        # entire purpose is to say "you have no name" assert a name — a
        # fabricated eight-character session-id prefix, which is an identifier in
        # neither key-space. A reader told `Session named 'a0000000'` has been
        # told something false about itself in the same breath as being asked to
        # fix it, and the string is close enough to an address to be quoted back.
        # A row with no `name` is exactly the unnamed case this nudge exists for,
        # so it now SAYS unnamed.
        if isinstance(name, str) and name:
            lead = f"[relay] Session named '{name}'."
        else:
            lead = "[relay] This session has no relay name."
        return (
            f"{lead} "
            f"Run `/relay-name <new-name>` to set a friendly name "
            f"for cross-session messaging (or skip)."
        )
    except Exception:
        # Silent-degrade — never raise from a hook helper.
        return None


def _sweep_kind(
    identity: str, floor: int, kind: str,
) -> tuple[list[dict], str, int]:
    """MULTI-KIND PAGED-SWEEP FIX — page a SINGLE inbox `kind` for `identity`
    above `floor`, on that kind's OWN scan cursor.

    The broker returns to-me rows of one kind CONTIGUOUSLY from `since` ascending
    (WHERE to_agent=? AND kind=? AND event_id>? ORDER BY event_id ASC LIMIT N).
    We page by re-querying with since=max-returned-id-of-this-kind until the kind
    returns a SHORT page (< SWEEP_PAGE_LIMIT ⇒ exhausted) or we hit the per-kind
    work cap (SWEEP_MAX_PAGES / SWEEP_MAX_MESSAGES). Calls `fetch_pending_messages`
    PER KIND directly (NOT the 3-in-1 merge) so cursor advancement is per-kind —
    a high event_id of a DIFFERENT kind can never advance this kind's cursor past
    its own un-returned tail.

    Returns (messages, status, max_fetched_id):
      - messages:        unique to-me rows of THIS kind swept (event_id-deduped, ASC).
      - status:          one of:
          'exhausted' — a short page (no failure) proved no row of this kind
                         remains un-fetched above `floor`. Safe boundary = +inf.
          'capped'     — hit the page/message cap with full pages; more rows of
                         this kind MAY remain above max_fetched_id. We have the
                         CONTIGUOUS prefix floor+1..max_fetched_id; safe boundary
                         = max_fetched_id.
          'failed'     — a page fetch FAILED (broker/HTTP/JSON error: fetch_ok
                         False). This kind may have un-fetched rows ABOVE floor we
                         never saw → cannot advance. Safe boundary = floor.
      - max_fetched_id:  highest event_id of THIS kind actually fetched this sweep
                         (>= floor; == floor when nothing fetched).

    Silent-degrade: a fetch raising is caught here and treated as 'failed' (never
    re-raises out — preserves do_receive's overall silent-degrade discipline).
    """
    messages: list[dict] = []
    seen: set[int] = set()
    scan_cursor = floor
    max_fetched = floor

    for _page in range(SWEEP_MAX_PAGES):
        try:
            result = fetch_pending_messages(
                identity, since=scan_cursor, kind=kind, limit=SWEEP_PAGE_LIMIT
            )
        except Exception as e:
            # Defensive — fetch_pending_messages itself silent-degrades, but a
            # caller-level guard keeps the sweep from ever raising out.
            sys.stderr.write(
                f"relay shared._sweep_kind: kind={kind} fetch raised "
                f"({type(e).__name__}: {e}); treating as FAILED\n"
            )
            return messages, "failed", max_fetched

        # A FAILED fetch (broker/HTTP/JSON error) is NOT a drained kind. Stop and
        # signal failure so the caller caps this kind's boundary at `floor`.
        if not result.get("fetch_ok", False):
            sys.stderr.write(
                f"relay shared._sweep_kind: kind={kind} fetch failed "
                f"(fetch_ok False) for identity={identity!r} since={scan_cursor}; "
                f"capping boundary at floor={floor}\n"
            )
            return messages, "failed", max_fetched

        rows = result.get("messages") or []
        page_max = scan_cursor
        page_rows = 0
        for m in rows:
            if not isinstance(m, dict):
                continue
            eid = m.get("event_id")
            if not isinstance(eid, int):
                continue
            page_rows += 1
            if eid > page_max:
                page_max = eid
            if eid in seen:
                continue
            seen.add(eid)
            messages.append(m)

        if page_max > max_fetched:
            max_fetched = page_max

        # Short page (< per-kind limit) ⇒ broker had no more rows of this kind
        # above `since` ⇒ this kind is EXHAUSTED. A non-advancing page (broker
        # echoed only already-seen rows / empty) is also exhausted (no new tail).
        if page_rows < SWEEP_PAGE_LIMIT or page_max <= scan_cursor:
            return messages, "exhausted", max_fetched

        scan_cursor = page_max

        # Per-kind message cap: stop before the next round-trip. The contiguous
        # prefix floor+1..max_fetched is drained; beyond is unknown ⇒ capped.
        if len(messages) >= SWEEP_MAX_MESSAGES:
            return messages, "capped", max_fetched
    # Page cap exhausted with full pages every time ⇒ more may remain ⇒ capped.
    return messages, "capped", max_fetched


# Sentinel: a kind that is EXHAUSTED (short page, no failure) imposes no ceiling
# on the floor — nothing of that kind remains un-fetched above floor.
_KIND_BOUNDARY_INF = float("inf")


def _sweep_to_me_messages(
    identity: str, floor: int,
) -> tuple[list[dict], int, bool]:
    """MULTI-KIND PAGED-SWEEP FIX (cross-family review, confidence 0.9) — page
    EACH inbox kind on its OWN cursor and advance the floor only to the boundary
    provably-drained for EVERY kind.

    THE BUG THIS FIXES: `event_id` is a GLOBAL counter; the inbox is queried PER
    KIND. The prior sweep merged 3 kinds into one list and advanced ONE cursor to
    the merged page_max. Two silent-loss triggers:
      (a) per-kind truncation: kind A has >limit to-me rows (truncated at its
          LIMIT) while kind B has a much higher single event_id → the high B id
          advanced the cursor past A's un-returned tail → after floor=examined_max,
          A's tail is buried (id < floor, never injected/acked).
      (b) per-kind degrade: a kind's fetch fails + silently returns empty → looks
          "drained" → floor advances over its real un-fetched rows.

    THE FIX: for each kind, `_sweep_kind` pages that kind ALONE (its own cursor)
    and reports a per-kind safe boundary:
      - FAILED    → boundary = floor   (cannot advance for this kind).
      - CAPPED    → boundary = that kind's max fetched id (contiguous floor+1..max).
      - EXHAUSTED → boundary = +inf    (nothing of this kind remains above floor).
    examined_max = MIN(boundaries). If all kinds exhausted (all +inf) →
    examined_max = max fetched id across all kinds (or floor if nothing fetched).
    Always >= floor (monotonic). The net invariant: floor never advances past an
    un-fetched to-me message of ANY kind; any kind that failed or truncated this
    fire caps the advance.

    Returns (messages, examined_max, complete):
      - messages:     all unique to-me rows across kinds (event_id-deduped, ASC).
      - examined_max: highest event_id fully examined for EVERY kind this sweep
                      (the per-kind-min boundary). floor may advance to this; no
                      to-me row of any kind ≤ it is left un-fetched.
      - complete:     True iff EVERY kind drained (all exhausted). False if any
                      kind capped or failed (tail rolls to next fire).

    HANDLED-ABOVE-examined_max: a message handled this fire whose id > examined_max
    (handled because some OTHER kind reached higher, or this kind capped above the
    min) is returned in `messages` but is NOT covered by the floor advance — the
    caller MUST ack it (see do_receive: ids > examined_max that were injected /
    grep-confirmed go into `acked`) so the next fire does not re-inject it.

    Silent-degrade: never raises out (per-kind failures are flagged, not raised).
    """
    messages: list[dict] = []
    seen: set[int] = set()
    boundaries: list[float] = []
    max_fetched_any = floor
    any_fetched = False
    complete = True

    for kind in INTER_SESSION_MESSAGE_KINDS:
        kind_msgs, status, kind_max = _sweep_kind(identity, floor, kind)

        # Merge this kind's rows (dedupe by event_id across kinds — defensive;
        # event_id is globally unique so kinds never overlap, but the prior code
        # deduped and a belt-and-braces guard is cheap).
        for m in kind_msgs:
            eid = m.get("event_id")
            if not isinstance(eid, int):
                continue
            if eid in seen:
                continue
            seen.add(eid)
            messages.append(m)

        if kind_max > max_fetched_any:
            max_fetched_any = kind_max
        if kind_max > floor:
            any_fetched = True

        if status == "failed":
            # Cannot advance for this kind: its un-fetched rows above floor are
            # unknown. Boundary = floor caps the min at floor for this fire.
            boundaries.append(float(floor))
            complete = False
        elif status == "capped":
            # Contiguous floor+1..kind_max drained for this kind; beyond unknown.
            boundaries.append(float(kind_max))
            complete = False
        else:  # 'exhausted'
            # Nothing of this kind remains un-fetched above floor → no ceiling.
            boundaries.append(_KIND_BOUNDARY_INF)

    # examined_max = MIN across kinds of each kind's safe boundary.
    min_boundary = min(boundaries) if boundaries else float(floor)
    if min_boundary == _KIND_BOUNDARY_INF:
        # Every kind exhausted (no ceiling from any). The provably-drained
        # high-water is the max id we actually fetched (every to-me row ≤ it of
        # every kind was returned). Nothing fetched → stay at floor.
        examined_max = max_fetched_any if any_fetched else floor
    else:
        # At least one kind failed/capped. The min finite boundary is the highest
        # id below which EVERY kind is provably drained. Never below floor.
        examined_max = int(min_boundary)
        if examined_max < floor:
            examined_max = floor

    messages.sort(key=lambda m: m.get("event_id", 0))
    return messages, examined_max, complete


def do_receive(hook_event_name: str) -> None:
    """Canonical receive routine called by all four receive_*.py entry-points.

    Silent-degrade on any unexpected exception: log full traceback to stderr,
    emit `{}` to stdout, exit 0. NEVER break the CC session because of a hook bug.
    """
    # UTF-8 stdout/stderr per CLAUDE.md Windows note.
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:  # pragma: no cover — defensive on weird streams
            pass

    try:
        payload = read_hook_stdin()
    except Exception as e:
        sys.stderr.write(f"relay do_receive[{hook_event_name}]: stdin parse failed ({type(e).__name__}: {e}); degrading\n")
        emit_hook_response(hook_event_name, None)
        return

    # --- SUBAGENT GATE (D137) ------------------------------------------------
    # D57 item 1 asserts, parenthetically, that this hook "runs main-only --
    # subagent turns fire SubagentStart, unused by relay". THAT PREMISE IS
    # MEASURABLY FALSE: PostToolUse fires inside subagents too (23 captured
    # instances). So this function polls the broker, renders a message into the
    # SUBAGENT's transcript, and records that id in `awaiting_ack` -- a list
    # whose entire meaning is "MAIN has been shown this". The orchestrator then
    # never sees the message while every layer believes it was delivered.
    #
    # WHY D57's EXISTING LAYERS CANNOT COVER THIS, and it is the load-bearing
    # distinction: its 4th layer instructs subagents not to ack. Here no subagent
    # misbehaves -- it need not call broker_ack at all. THE PLUMBING creates the
    # bad state. An instruction to the caller cannot stop this hook from writing
    # a wrong list, which is precisely why D57 chose to "key on what is
    # legitimately ackable, not who-called". The mechanism is sound; its INPUT
    # was corrupted.
    #
    # THEREFORE THE FIX IS A SUBTRACTION, NOT A FIFTH LAYER: make D57's stated
    # premise true. Return immediately on a subagent fire -- no poll, no
    # injection, no bookkeeping. MAIN collects the message on its own next tool
    # boundary. Bounded delay, in the safe direction, and it DELETES behaviour
    # rather than adding machinery (C-39; D129's "I built scaffolding to survive
    # a wrong key instead of changing the key").
    #
    # THE DISCRIMINATOR IS `agent_id`, BY PRESENCE -- and that is Claude Code's
    # OWN documented contract for the field, read verbatim out of the binary's
    # hook schema rather than inferred from a sample:
    #     "Subagent identifier. Present only when the hook fires from within a
    #      subagent (e.g., a tool called by an AgentTool worker). Absent for the
    #      main thread, even in --agent sessions. Use this field (not agent_type)
    #      to distinguish subagent calls from main-thread calls."
    # agent_type describes a category that a main agent and its workers may share;
    # agent_id identifies a subagent instance and is absent on main-thread hooks.
    #
    # FAIL-OPEN, deliberately: a malformed payload or an absent key means "main",
    # which is exactly today's behaviour. Guessing "subagent" wrongly would stop
    # MAIN receiving mail entirely -- far worse than the bounded duplicate this
    # direction can cost.
    try:
        if isinstance(payload, dict) and payload.get("agent_id"):
            emit_hook_response(hook_event_name, None)
            return
    except Exception:
        pass  # fail-open to the main-context path

    # R3B/R5B already owns Codex registration and ordered message delivery via
    # the app-server adapter. A second poll here would race that adapter, and the
    # broker deliberately rejects Claude's legacy inbox for Codex-owned rows.
    if is_codex_hook_payload(payload):
        emit_hook_response(hook_event_name, None)
        return

    try:
        session_id = resolve_session_id(payload)
    except Exception as e:
        sys.stderr.write(f"relay do_receive[{hook_event_name}]: session_id unresolved ({type(e).__name__}: {e}); degrading\n")
        emit_hook_response(hook_event_name, None)
        return

    emitted = False
    # Naming UX Option C: build one-line nudge if applicable. Only fires on
    # UserPromptSubmit (other receive hooks are PostToolUse / Notification /
    # SessionStart, none of which inject extra context per discipline).
    # Decoupled from broker fetch so nudge still emits even on broker outage.
    nudge: Optional[str] = None
    if hook_event_name == "UserPromptSubmit":
        try:
            nudge = _build_naming_nudge(session_id)
        except Exception:
            pass  # silent-degrade — defense in depth (helper already wraps)

        # ACTIVE PLAN Site Q: /rename auto-sync. Detect `/rename foo` in user
        # prompt → spawn set_name.py to update registry + broker + marker.
        # Non-blocking; advisory surfaces NEXT turn via marker-file path.
        try:
            _maybe_auto_sync_rename(session_id, payload.get("prompt", ""))
        except Exception:
            pass

        # ACTIVE PLAN Site O: pending-mcp-reconnect advisory. Fires when user
        # ran /relay-name but hasn't /mcp reconnected yet (marker file
        # written by set_name.py). One-line append to nudge.
        try:
            adv = _build_pending_mcp_reconnect_advisory(session_id)
            if adv:
                nudge = (nudge + "\n" + adv) if nudge else adv
        except Exception:
            pass

    try:
        # LOSSLESS-DELIVERY refactor (plan quizzical-launching-fox, B1+B3) +
        # CROSS-FAMILY REVIEW FIXES 1+2.
        # ============================================================
        # The receive-hook is the AUTHORITATIVE backstop. Push (MCP) delivers
        # full content but advances NOTHING the hook reads.
        #
        # Authority is a per-SESSION confirmed-set {floor, acked}. FIX 1 redefines
        # `floor`: "every message ADDRESSED TO ME with event_id ≤ floor is
        # confirmed/handled" — NOT a global-id contiguous high-water. We SWEEP
        # the recipient inbox (page through ALL to-me rows above floor, bounded),
        # transcript-grep the unconfirmed (fail toward delivery), inject the
        # genuine drops, then advance floor to the highest to-me id fully examined.
        #
        # FIX 2: EMIT BEFORE CONFIRM. The confirmed-set floor advance + injected-id
        # confirmation happen AFTER a successful emit, so a failure between
        # confirm + emit can never mark-confirmed-but-undelivered (bias to a
        # re-delivered duplicate, never to silent loss).
        plain_cursor = read_cursor(session_id)
        # D130: THE IDENTITY WE ASK THE BROKER WITH MUST BE THE IDENTITY WE FILE
        # THE ANSWER UNDER. `primary_identity` below has always been name-or-sid —
        # the code already knew the right identity to ASK with, and then remembered
        # the answer under `session_id`. Those two being different variables IS the
        # replay bug: a rename moves the name and orphans the cursor, the cursor
        # reads 0, and the broker (whose only suppression is `event_id > since`)
        # re-serves the entire history.
        #
        # RESOLVE ONCE PER FIRE. An earlier draft resolved the identity here AND
        # again ~15 lines below as `my_name_for_dedup`, independently, from mutable
        # on-disk state. A rename landing between the two reads makes the sweep poll
        # as `beta` while the floor write lands in `confirmed/alpha.json` — the exact
        # cross-name laundering that got the predecessor patch rejected, arriving
        # through a race instead of through a migration. One read, one snapshot,
        # everything derived from it.
        resolved_name = _resolve_name_from_registry(session_id)
        dkey = _delivery_key(session_id, resolved_name)
        if dkey is None:
            # WE DO NOT KNOW WHO WE ARE. Every value derived from `dkey` below is
            # either a FILENAME (the confirmed-set) or a broker mailbox (the
            # sweep identity, the ack mirror). Inventing either is C-13's
            # prohibition and D181's two-day outage, so this fire delivers
            # NOTHING and writes NOTHING rather than guessing.
            #
            # THIS IS A DELAY, NOT A LOSS, and that is the whole reason refusing
            # is affordable here. Nothing is acked, no floor moves, and no cursor
            # advances, so every pending row stays pending at the broker; the
            # next fire (PostToolUse fires constantly) re-reads the registry and
            # delivers normally. The bias is toward a duplicate, never toward a
            # silent drop — the one direction this file never trades.
            #
            # Previously this state was unreachable-by-construction only because
            # the resolver manufactured a well-formed WRONG answer instead, which
            # is exactly what made a total identity failure indistinguishable
            # from an ordinary miss. `nudge` is passed through rather than
            # dropped so the refusal costs nothing it does not have to; note it
            # is USUALLY None here, since `_build_naming_nudge` reads the same
            # registry and returns None when it cannot — MEASURED end to end, the
            # corrupt-registry fire emits a bare `{}`.
            sys.stderr.write(
                f"relay do_receive[{hook_event_name}]: delivery key UNRESOLVABLE "
                f"for sid={session_id[:8]}… (registry absent/unreadable/corrupt, "
                f"or its row for this session is not an object). REFUSING to "
                f"deliver this fire rather than key by the session id — messages "
                f"stay pending at the broker and re-deliver on the next fire.\n"
            )
            emit_hook_response(hook_event_name, nudge)
            emitted = True
            return
        # One-time, per name, epoch-proven. No-op once migrated.
        _seed_name_key_from_sid(dkey, session_id)
        # `plain_cursor` is SID-scoped (cursors/<sid>.json), so it is a legitimate
        # default ONLY for the sid-keyed store. Feeding it to the NAME store installs
        # a floor of unproved provenance and silently defeats the guard in
        # _seed_name_key_from_sid immediately above: that function refuses any floor
        # it cannot prove, and this argument used to install one regardless.
        cs_default = plain_cursor if dkey == session_id else 0
        confirmed = _read_confirmed_set(dkey, default_floor=cs_default)
        floor = confirmed["floor"]
        acked_set = set(confirmed["acked"])

        # The legacy UUID side-poll still uses the UNBOOSTED plain cursor (MCP
        # never delivered UUID-addressed rows; the confirmed-set is friendly-name
        # scoped). Preserved verbatim from the prior implementation.
        receive_cursor_unboosted = plain_cursor

        # ONE IDENTITY DECISION, USED EVERYWHERE. `dkey` has already applied the
        # filename validation to `resolved_name`; if it REJECTED the name, `dkey`
        # fell back to the sid. Carrying the rejected name onward as the poll and
        # mirror identity is the split this whole change exists to remove:
        #
        #   MEASURED, name -> (poll identity, dkey):
        #     'normalname'        -> ('normalname', 'normalname')   agree
        #     'delivery-counters' -> ('delivery-counters', SID)     DIVERGE
        #     'CON' / 65-chars    -> (name,          SID)           DIVERGE
        #
        # With the old binding the hook SWEPT the broker as the NAME while reading
        # and writing confirmed/<sid>.json, and then posted a SID-derived floor
        # UNDER the name -- so a name-addressed row the session never saw could be
        # crossed off. That is silent loss, the one direction this file never
        # trades. The class is pre-existing (CON, 65-char names always did it), and
        # reserving the telemetry stem WIDENED it by moving a valid string into the
        # rejected set. Live population measured 0 of 38 user_set names; fixed
        # because it is latent, not because it is firing.
        #
        # So: a name that cannot be the delivery key is not a usable identity at
        # all, and this session behaves as NAMELESS -- it polls by sid, skips the
        # legacy side-poll (redundant once the primary poll IS the sid), and posts
        # no mirror. The rows stay `outstanding` and the reaper pages. Noisy, not
        # silent, which is the correct direction.
        my_name_for_dedup = resolved_name if dkey != session_id else None

        # FIX 1 (gap-aware PAGED sweep): page the broker inbox by friendly_name
        # (or session_id when unnamed) from since=floor. since=floor is EXCLUSIVE
        # at the broker (event_id > since); the broker returns to-me rows
        # contiguously ascending, so paging to the inbox tail guarantees no newest
        # to-me row is starved outside a single 20-row page (the prior bug).
        primary_identity = my_name_for_dedup or session_id
        messages, examined_max, sweep_complete = _sweep_to_me_messages(
            primary_identity, floor
        )
        if not sweep_complete:
            sys.stderr.write(
                f"relay do_receive[{hook_event_name}]: to-me sweep hit the "
                f"per-fire cap (pages={SWEEP_MAX_PAGES}/msgs={SWEEP_MAX_MESSAGES}) "
                f"for identity={primary_identity!r}; examined up to id={examined_max}; "
                f"floor advances only on broker_ack/render, so the tail simply "
                f"rolls to the next fire\n"
            )

        # Partition the swept friendly-name returns by confirmed-state, then
        # transcript-grep the unconfirmed ones (state #3 dedup). FAIL TOWARD
        # DELIVERY: read_ok == False ⇒ treat ALL as NOT rendered ⇒ inject.
        unconfirmed: list[dict] = []
        n_acked = 0       # telemetry: already confirmed (relay_ack OR floor)
        for m in messages:
            if not isinstance(m, dict):
                continue
            eid = m.get("event_id")
            if not isinstance(eid, int):
                continue
            if eid <= floor or eid in acked_set:
                # Already confirmed for me (state #1: model acked, OR below floor).
                n_acked += 1
                continue
            unconfirmed.append(m)

        to_inject: list[dict] = []
        grep_confirmed_ids: set[int] = set()   # rendered → confirm, no emit needed
        injected_ids: set[int] = set()         # genuine drops → confirm AFTER emit
        n_grep_found = 0      # telemetry: state #3 had-it-didn't-ack
        n_redelivered = 0     # telemetry: state #2/#3 genuine miss → injected
        if unconfirmed:
            rendered_ids, grep_ok = _rendered_event_ids_in_transcript(session_id)
            for m in unconfirmed:
                eid = m["event_id"]  # guaranteed int by the partition above
                # FAIL TOWARD DELIVERY: if the grep could not read/parse the
                # transcript (grep_ok False), rendered_ids is empty → NOT
                # rendered → inject. A genuine render → suppress.
                if grep_ok and eid in rendered_ids:
                    grep_confirmed_ids.add(eid)
                    n_grep_found += 1
                else:
                    to_inject.append(m)
                    injected_ids.add(eid)
                    n_redelivered += 1

        # Legacy UUID-addressed backlog (iter-2/iter-3 M4): pre-friendly-name
        # senders POSTed with to_agent=<sid>. Those rows never match the
        # friendly_name poll above. Side-poll by session_id ONLY when we have a
        # friendly_name (else sid IS primary_identity). Uses the UNBOOSTED plain
        # cursor + the SAME per-kind-safe paged sweep (MULTI-KIND FIX: the legacy
        # tail is never starved AND the plain cursor never over-advances past an
        # un-fetched / truncated / failed kind — _sweep_to_me_messages caps
        # legacy_examined_max at the per-kind-min boundary). These are NOT
        # confirmed-set members (the confirmed-set is friendly-name scoped); they
        # advance the plain cursor instead. Injected as before; deduped by
        # event_id against what we're already injecting.
        legacy_to_inject: list[dict] = []
        legacy_injected_ids: set[int] = set()  # genuine legacy drops injected this fire
        legacy_max_event_id = receive_cursor_unboosted
        legacy_ran = False            # did the legacy UUID side-poll run this fire?
        legacy_examined_boundary = 0  # legacy sweep's examined_max (if it ran)
        legacy_complete = True        # legacy sweep fully drained (not capped/failed)?
        if my_name_for_dedup and session_id and session_id != my_name_for_dedup:
            # D130 CRITICAL-A: this stream's rows are addressed to the raw SID, so
            # it must be filtered by the SID's confirmed-set — NOT the name's.
            # `floor`/`acked_set` above now come from confirmed/<name>.json. Applying
            # a name-scoped floor to SID-addressed rows silently discards this
            # session's own never-delivered legacy mail, and write_cursor() below
            # then advances past it permanently. There is no server-side backstop:
            # the broker inbox has no delivery-status predicate, so a dropped id is
            # unrecoverable by any means. Read the sid store once, here.
            legacy_cs = (confirmed if dkey == session_id
                         else _read_confirmed_set(session_id,
                                                  default_floor=plain_cursor))
            legacy_floor = legacy_cs.get("floor", 0) or 0
            legacy_acked = set(legacy_cs.get("acked", []) or [])
            try:
                legacy_messages, legacy_examined_max, _legacy_complete = \
                    _sweep_to_me_messages(session_id, receive_cursor_unboosted)
                legacy_ran = True
                legacy_examined_boundary = legacy_examined_max
                legacy_complete = _legacy_complete
                if legacy_examined_max > legacy_max_event_id:
                    legacy_max_event_id = legacy_examined_max
                if legacy_messages:
                    already = set(injected_ids) | grep_confirmed_ids
                    already |= {mm.get("event_id") for mm in to_inject
                                if isinstance(mm, dict)}
                    for legacy_msg in legacy_messages:
                        if not isinstance(legacy_msg, dict):
                            continue
                        eid = legacy_msg.get("event_id")
                        if not isinstance(eid, int):
                            continue
                        # ACK-DRIVEN (GAN finding #1): a legacy UUID row is ALSO
                        # confirmable via broker_ack — broker_ack writes THIS sid's
                        # confirmed-set and legacy rows are addressed to this sid.
                        # Honor it here so a broker_acked legacy id does NOT
                        # re-inject. (Legacy rows are hook-injected, hence invisible
                        # to the render-grep, so broker_ack is their confirmation.)
                        # D130: reads the SID store (legacy_floor/legacy_acked), which
                        # is what "THIS sid's confirmed-set" above has always meant.
                        if eid <= legacy_floor or eid in legacy_acked:
                            continue
                        if eid not in already:
                            legacy_to_inject.append(legacy_msg)
                            legacy_injected_ids.add(eid)
                            already.add(eid)
            except Exception:
                # Silent-degrade — primary-identity poll already succeeded; legacy
                # UUID-addressed rows just won't recover this fire. BUT the legacy
                # stream is now UNCERTAIN (we may not have examined its tail), so it
                # MUST constrain the shared confirmed-set floor: mark it ran +
                # incomplete with the boundary at the legacy confirmed watermark
                # (`receive_cursor_unboosted`). Else the confirm block would treat
                # legacy as unconstrained and the floor could compact from the
                # primary stream alone, burying an un-examined legacy id (GAN iter-3).
                legacy_ran = True
                legacy_complete = False
                legacy_examined_boundary = receive_cursor_unboosted
                sys.stderr.write(
                    f"relay do_receive[{hook_event_name}]: legacy UUID side-poll "
                    f"raised; treating legacy stream as incomplete (floor barrier at "
                    f"{receive_cursor_unboosted}) so it cannot bury an un-examined "
                    f"legacy row\n"
                )

        # Assemble the injection set: genuine drops (confirmed-set path) +
        # legacy UUID rows. Sort by event_id ASC (matches the prior contract).
        inject_messages = to_inject + legacy_to_inject
        inject_messages.sort(
            key=lambda m: m.get("event_id", 0) if isinstance(m, dict) else 0
        )

        # ---- FIX 2: EMIT FIRST (before confirming injected ids) --------------
        # The injection response is all-or-nothing (one combined hook response),
        # so a successful emit means EVERY injected id was delivered; a failed
        # emit means NONE were. We only advance floor over / confirm injected ids
        # AFTER a successful emit (below). emit_hook_response writes+flushes stdout.
        # #24 (C) sender-outbox advisory: once per user turn, flush any relay
        # sends that were queued while the broker was down (recovery trigger) and
        # surface them inline. Prepends the naming nudge; helper never raises.
        advisory = _sender_outbox_advisory(hook_event_name)
        eff_nudge = (advisory + "\n\n" + nudge) if (advisory and nudge) else (advisory or nudge)

        emit_ok = False
        if inject_messages:
            try:
                formatted = format_messages_for_injection(inject_messages)
                combined = (eff_nudge + "\n\n" + formatted) if eff_nudge else formatted
                emit_hook_response(hook_event_name, combined)
                emitted = True
                emit_ok = True
            except Exception as e:
                # Emit failed → do NOT confirm the injected ids; they re-deliver
                # next fire (accepted duplicate, never a silent loss). Never raise
                # out of do_receive (silent-degrade discipline).
                sys.stderr.write(
                    f"relay do_receive[{hook_event_name}]: inject emit failed "
                    f"({type(e).__name__}: {e}); NOT confirming injected ids; "
                    f"next fire re-delivers\n"
                )
        else:
            # Nothing to inject → emit the nudge + advisory (or {}). No ids to gate.
            emit_hook_response(hook_event_name, eff_nudge)
            emitted = True
            emit_ok = True

        # ---- ACK-DRIVEN confirm (restored 2026-06-11; revised per GAN review) --
        # The confirmed-set advances ONLY on proven receipt — an explicit model
        # `broker_ack` (written by server.py) or a grep-PROVEN render (type==user +
        # origin.kind==channel; `grep_confirmed_ids`). It NEVER advances on a bare
        # emit: emit-success is not proof the message rendered (that assumption
        # silently suppressed 33 to-me messages — F-CC40). injected_ids are NEVER
        # folded into the floor — they re-deliver next fire until acked/rendered.
        #
        # BUT the floor MUST still COMPACT over the RECIPIENT stream so it doesn't
        # stick: event_id is a GLOBAL counter while floor is the per-recipient
        # `since`, so floor+1 is almost always ANOTHER recipient's id — a naive
        # `floor+1 in acked` collapse would pin the floor forever and grow `acked`
        # unboundedly until the sweep cap starves new messages (GAN finding #2).
        # The floor advances to the lowest per-stream BARRIER (computed below over
        # BOTH the primary and legacy sweeps), compacting past global event-id gaps
        # but NEVER crossing an injected-unconfirmed id of either stream — loss-free.
        try:
            # Per-stream confirmation BARRIER = the highest id with NO unconfirmed
            # or un-examined to-me id at or below it. The single per-sid floor gates
            # BOTH the primary (friendly-name) AND the legacy (UUID) dedup checks
            # (`eid <= floor`), so it must respect BOTH streams — advancing it on
            # the primary stream alone can bury an unconfirmed legacy id (GAN iter-2).
            #   - injected (emitted, NOT acked/rendered) => stop just below the
            #     lowest injected id (it must re-deliver, never be confirmed by emit).
            #   - capped/failed sweep => stop at examined_max (unknown tail above it).
            #   - fully drained, nothing injected => no constraint (compact freely).
            # THE FIRST TWO ARE CONJUNCTIVE, and an `if/elif` made them EXCLUSIVE.
            # Measured: with one kind's fetch failing while another kind injects
            # above the gap, `min(injected_ids) - 1` won outright, the incomplete-
            # sweep clamp never ran, and the floor advanced 9 ids PAST examined_max
            # — over the failed kind's un-fetched to-me rows. Re-sweeping from the
            # new floor with every kind healthy did NOT return them: the broker
            # inbox has no delivery-status predicate, so `event_id > since` is the
            # only suppression and a crossed row is unrecoverable by any means.
            # A failed fetch does NOT require the broker to be down — a 4xx, a 5xx,
            # a non-object body or invalid JSON each set fetch_ok=False while the
            # other kinds still succeed, which is exactly when this fires.
            if injected_ids:
                prim_barrier = min(injected_ids) - 1
                if not sweep_complete:
                    prim_barrier = min(prim_barrier, examined_max)
            elif not sweep_complete:
                prim_barrier = examined_max
            else:
                prim_barrier = None
            if legacy_injected_ids:
                leg_barrier = min(legacy_injected_ids) - 1
                if legacy_ran and not legacy_complete:
                    leg_barrier = min(leg_barrier, legacy_examined_boundary)
            elif legacy_ran and not legacy_complete:
                leg_barrier = legacy_examined_boundary
            else:
                leg_barrier = None
            barriers = [b for b in (prim_barrier, leg_barrier) if b is not None]
            # No barrier anywhere => every examined to-me id (both streams) is
            # confirmed => compact to examined_max (past GLOBAL event-id gaps so the
            # floor never sticks + `acked` stays bounded). Otherwise stop at the
            # lowest barrier. NEVER fold injected_ids; only grep-rendered ids park
            # in acked. broker_ack (server.py) folds explicit acks via the same store.
            safe_floor = min(barriers) if barriers else examined_max
            # P2 (subagent-safe): the broker_ack allow-list = every id the hook
            # DELIVERED into MAIN this fire that is NOT yet confirmed (primary
            # friendly-name stream + legacy UUID stream — both are hook-injected,
            # hence invisible to the render-grep, so broker_ack is their ONLY
            # confirmation signal). These are exactly the ids a legitimate
            # MAIN-context broker_ack carries; a leaked-to-subagent PUSH id is
            # never here, so the server refuses a subagent ack of it (no loss).
            #
            # CRITICAL (cross-family GAN iter-2): membership is gated on emit_ok.
            # On emit FAILURE the injected ids did NOT render into main (they
            # re-deliver next fire) — adding them would let a subagent ack a
            # NEVER-DELIVERED id and silently advance the cursor. So on failure add
            # NOTHING new: PRESERVE a present allow-list (aw_arg=None), or close an
            # ABSENT transition window with [] so the gate engages (refuse) rather
            # than stay permissive across a failed-delivery fire.
            awaiting_now = sorted(injected_ids | legacy_injected_ids) if emit_ok else []
            if emit_ok:
                aw_arg = awaiting_now
            elif confirmed.get("awaiting_ack") is None:
                aw_arg = []
            else:
                aw_arg = None
            # Write whenever the floor/acked changes, OR there is a delivered
            # allow-list to record, OR awaiting_ack is still ABSENT (close the
            # transition window). Idle fires with the key present skip the write.
            if (safe_floor > floor or grep_confirmed_ids or awaiting_now
                    or confirmed.get("awaiting_ack") is None):
                _advance_floor_locked(
                    dkey,
                    target_floor=safe_floor,
                    extra_acked_ids=sorted(grep_confirmed_ids),
                    default_floor=cs_default,
                    awaiting_ack_ids=aw_arg,
                )
        except Exception as e:
            sys.stderr.write(
                f"relay do_receive[{hook_event_name}]: confirmed-set write "
                f"failed ({type(e).__name__}: {e}); next fire re-checks\n"
            )

        # WS-B-broker-2 (#18 mirror fix): mirror this recipient's gap-aware
        # confirmed-set {floor, acked} to the broker delivery ledger so its rows
        # flip 'outstanding' -> 'acked' and the escalation reaper (#18) can tell a
        # processed message from a genuinely-stuck one. We mirror the FLOOR (the
        # contiguous-confirmed high-water — advances only past a CONTIGUOUS run, so
        # it never passes an unread/gap id) PLUS the parked `acked` ids (confirmed
        # ids sitting above a gap). Together they are EXACTLY the ids this recipient
        # has confirmed; an unread gap id is in NEITHER -> its broker row stays
        # outstanding (reaper alerts + the hook keeps re-delivering it). This
        # SUBSUMES the old grep-only mirror whenever the floor is actually POSTED.
        # "grep-confirmed ids are <= floor or in acked" is a property of the LOCAL
        # confirmed-set, and the POST below sends `post_floor`, NOT the local floor.
        # `post_floor` is None whenever the floor is 0, and in that case the
        # subsumption stops holding for every id that folded into the floor -- which
        # is why the suppressed branch carries THIS FIRE's grep-confirmed ids as
        # explicit point ids. [Corrected 2026-08-09: this said the EPOCH GATE nils out
        # post_floor. That gate was deleted at D130; floor==0 is the only suppression
        # left, and this branch gates on "the range is suppressed" rather than on any
        # epoch, which is what makes it cover OQ-RELAY-8 too (D111).]
        #
        # An id confirmed through the MCP broker_ack
        # path that ALREADY folded into the floor on an EARLIER fire is in neither
        # `mirror_acked` (it collapsed into the floor) nor `grep_confirmed_ids` (that
        # is this fire's render scan), so it does NOT ride here. The gap is real --
        # it is the name-degraded-MCP case (D74) -- and it is NOT closed by carrying
        # below-floor ids: the floor is a RANGE assertion, so re-asserting ids that
        # already folded into it says nothing new about the ones that did not.
        # It requires separate reconciliation rather than an invented floor claim.
        #
        # Cadence: once per user turn (UserPromptSubmit) OR whenever we just
        # grep-confirmed a push render this fire (low-latency for the healthy case) —
        # NOT on every PostToolUse/Notification fire. Re-read the POST-write
        # confirmed-set so `floor` reflects both this fire's hook advance AND any MCP
        # broker_ack fold between fires. Self-healing: re-posts the current floor each
        # turn, so a transient miss is caught up next turn. fast_fail keeps it off the
        # turn critical path; a miss enqueues to the outbox + replays on reconnect.
        # Best-effort: the LOCAL confirmed-set is the delivery authority; this only
        # mirrors it. Never raises out of do_receive.
        #
        # A legacy-seeded range has unknown identity provenance and cannot be
        # certified by adopting its floor. If one inbox fetch fails while
        # another injects above the gap, a floor can cross unfetched rows.
        # The sweep-barrier clamp prevents that; the seed itself cannot.
        if (broker_client is not None and my_name_for_dedup
                and (hook_event_name == "UserPromptSubmit" or grep_confirmed_ids)):
            try:
                final_cs = _read_confirmed_set(dkey, default_floor=cs_default)
                mirror_floor = final_cs.get("floor", 0)
                mirror_acked = sorted(final_cs.get("acked", []))
                # epoch gate: DELETED (D130). It asked whether this floor accrued
                # under the identity we are about to mirror it as — a real question
                # when the file was named after a session_id. The confirmed-set is
                # now FILED under that identity, so the answer is yes by
                # construction and the gate had degenerated to `name == name`.
                # Deleting it is what closes OQ-RELAY-7: an epoch-less session could
                # never mirror its floor, so the broker never learned its receipts
                # and the escalation reaper paged a human about messages that had
                # been delivered, rendered and read.
                post_floor = mirror_floor if mirror_floor > 0 else None
                # D1: when the floor is SUPPRESSED (post_floor None, i.e. floor 0),
                # the ids that folded INTO the floor lose their ONLY carrier.
                # `acked` holds just the ids parked
                # ABOVE a gap, so a contiguously-confirmed id is in NEITHER the (nil)
                # floor NOR the posted list, and its delivery row can never flip -- it
                # strands outstanding and eventually pages the operator for a message
                # that was delivered, rendered and read. Carry THIS fire's grep-confirmed
                # ids explicitly instead. They are POINT cross-offs, and the server scopes
                # them by to_agent with a status guard (server.py post_agent_ack:
                # `WHERE to_agent=? AND event_id IN (...) AND status!='acked'`), so they
                # cannot reach another recipient's row or double-count it. This
                # branch leaves the floor range unchanged and carries only IDs
                # confirmed on this fire;
                # confirmations folded on earlier fires require reconciliation.
                # The soundness of these point acknowledgements depends on the
                # transcript parser admitting only genuinely rendered event IDs.
                post_ids = (sorted(set(mirror_acked) | grep_confirmed_ids)
                            if post_floor is None else mirror_acked)
                if post_floor is not None or post_ids:
                    # #3 rate-limit: skip when (name, floor, ids) is unchanged since
                    # the last SUCCESSFUL mirror; record only on post_ack non-None.
                    if _mirror_state_changed(dkey, my_name_for_dedup,
                                             post_floor, post_ids):
                        res = broker_client.post_ack(
                            my_name_for_dedup,
                            post_ids,
                            floor=post_floor,
                            fast_fail=True,
                        )
                        if res is not None:
                            _write_mirror_state(dkey, my_name_for_dedup,
                                                post_floor, post_ids)
            except Exception as e:
                sys.stderr.write(
                    f"relay do_receive[{hook_event_name}]: post_ack(mirror) failed "
                    f"({type(e).__name__}: {e}); local confirmed-set already advanced\n"
                )

        # B4 telemetry: record confirmation source counts (ack-miss + dup rate).
        # `redelivered` only counts on a successful emit (else the dup happens
        # next fire, not this one — counting here would double-count).
        try:
            _bump_delivery_counters({
                "acked": n_acked,
                "grep_found": n_grep_found,
                "redelivered": n_redelivered if emit_ok else 0,
            })
        except Exception:
            pass

        # Advance the plain cursor (the legacy side-poll floor + migration seed).
        # ACK-DRIVEN (GAN finding #1): advance over the RECIPIENT stream but NEVER
        # past an injected-unconfirmed legacy id, and NOT on a bare emit (emit !=
        # render). Highest safe boundary = legacy_examined_max (nothing injected)
        # else min(legacy_examined_max, min(legacy_injected_ids)-1). Confirmed
        # legacy rows below the first drop fold in; unconfirmed ones re-inject
        # until broker_acked (suppressed via the confirmed-set check above).
        # write_cursor is monotonic.
        if legacy_injected_ids:
            legacy_safe = min(legacy_max_event_id, min(legacy_injected_ids) - 1)
        else:
            legacy_safe = legacy_max_event_id
        if legacy_safe > plain_cursor:
            try:
                write_cursor(session_id, legacy_safe)
            except Exception as e:
                sys.stderr.write(
                    f"relay do_receive[{hook_event_name}]: cursor write failed ({type(e).__name__}: {e}); "
                    f"next fire will re-deliver\n"
                )
    except Exception:
        # Guard diagnostics so a broken stderr does not prevent the response
        # fallback. Emitting an empty response still requires usable stdout.
        try:
            sys.stderr.write(
                f"relay do_receive[{hook_event_name}]: unexpected exception; full traceback:\n"
                + traceback.format_exc()
            )
        except Exception:
            pass
        # Only emit fallback if primary emit hasn't fired — prevents double-JSON-write
        # to stdout (Fix #1: CC's parser rejects `{...}{}` as invalid).
        # Still try to emit nudge if computed (broker fetch may have failed but
        # nudge is independent).
        if not emitted:
            try:
                emit_hook_response(hook_event_name, nudge)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# v1.7 (Wave 6 Track 4) — universal error envelope receive path
# ---------------------------------------------------------------------------

ERROR_CURSORS_DIR = RELAY_DIR / "error_cursors"


def read_error_cursor(session_id: str) -> int:
    """Return last_seen_error_event_id from the error cursor file. Missing/corrupt -> 0.

    Mirrors read_cursor() but uses a separate ERROR_CURSORS_DIR so error and
    agent_message polls maintain independent cursors (per v1.7 design — they
    advance at different rates and a corrupted error cursor must not poison
    agent_message delivery).
    """
    cursor_path = ERROR_CURSORS_DIR / f"{session_id}.json"
    if not cursor_path.exists():
        return 0
    try:
        data = json.loads(cursor_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            v = data.get("last_seen_event_id", 0)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        sys.stderr.write(
            f"relay shared.read_error_cursor: cursor file {cursor_path} has unexpected shape; treating as 0\n"
        )
        return 0
    except (OSError, json.JSONDecodeError) as e:
        sys.stderr.write(
            f"relay shared.read_error_cursor: {cursor_path} corrupt ({type(e).__name__}: {e}); treating as 0\n"
        )
        return 0


def write_error_cursor(session_id: str, event_id: int) -> None:
    """Atomic + MONOTONIC + LOCKED write of the error cursor file.

    v1.7.2 (D38 iter-2 MEDIUM fix): adds OS file lock around read-modify-replace
    to eliminate the inter-process TOCTOU race. Previously v1.7.1 re-read the
    cursor before write but had no lock, so concurrent writers could still
    regress (A reads 0+event 5, B reads 0+event 3, A replaces 5, B replaces 3
    → final cursor 3).

    Lock strategy:
      - Windows: msvcrt.locking on a sentinel lockfile (advisory; stdlib).
      - POSIX: fcntl.flock on the same sentinel.
    Lock is held across the entire read → compare → write critical section.
    Best-effort: if locking is unsupported (very old Python / weird FS), falls
    through to the previous re-read-only monotonic path with a stderr warning.
    """
    ERROR_CURSORS_DIR.mkdir(parents=True, exist_ok=True)
    cursor_path = ERROR_CURSORS_DIR / f"{session_id}.json"
    lock_path = ERROR_CURSORS_DIR / f"{session_id}.lock"
    new_event_id = int(event_id)

    # Open (or create) the lockfile and acquire an exclusive lock on it.
    # Lock is released when the file handle closes.
    lock_fh = None
    try:
        # Create-or-open the lockfile; Windows requires file to exist before locking.
        lock_fh = open(lock_path, "a+", encoding="utf-8")
        _locked = _acquire_exclusive_lock(lock_fh)
        try:
            # Critical section: read current cursor, take max, write.
            if cursor_path.exists():
                try:
                    current = json.loads(cursor_path.read_text(encoding="utf-8"))
                    current_eid = current.get("last_seen_event_id", 0) if isinstance(current, dict) else 0
                    if isinstance(current_eid, int) and current_eid > new_event_id:
                        new_event_id = current_eid
                except (OSError, json.JSONDecodeError):
                    pass  # unreadable treated as 0
            payload = {"last_seen_event_id": new_event_id, "updated_at": int(time.time() * 1000)}
            fd, tmp_path = tempfile.mkstemp(prefix=f".{session_id}.", suffix=".json.tmp", dir=str(ERROR_CURSORS_DIR))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, cursor_path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        finally:
            if _locked:
                _release_exclusive_lock(lock_fh)
    finally:
        if lock_fh is not None:
            try:
                lock_fh.close()
            except Exception:
                pass


def _acquire_exclusive_lock(fh) -> bool:
    """Acquire an exclusive OS lock on fh. Returns True on success, False on
    unsupported platform (logs stderr warning + falls through to lockless path).
    """
    try:
        if sys.platform == "win32":
            import msvcrt
            # Lock first byte exclusively, blocking. Locks > 1 byte may fail on
            # zero-length files (sentinel is empty), so write a marker byte first
            # if needed.
            try:
                fh.seek(0, os.SEEK_END)
                if fh.tell() == 0:
                    fh.write("L")  # marker so we have a byte to lock
                    fh.flush()
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)
                return True
            except OSError as e:
                sys.stderr.write(
                    f"relay write_error_cursor: msvcrt lock failed "
                    f"({type(e).__name__}: {e}); falling through to lockless monotonic write\n"
                )
                return False
        else:
            import fcntl
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                return True
            except OSError as e:
                sys.stderr.write(
                    f"relay write_error_cursor: fcntl lock failed "
                    f"({type(e).__name__}: {e}); falling through to lockless monotonic write\n"
                )
                return False
    except ImportError as e:
        sys.stderr.write(
            f"relay write_error_cursor: lock module unavailable "
            f"({type(e).__name__}: {e}); falling through to lockless monotonic write\n"
        )
        return False


def _release_exclusive_lock(fh) -> None:
    """Release exclusive OS lock. Silent-degrade on error."""
    try:
        if sys.platform == "win32":
            import msvcrt
            fh.seek(0)
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass


def format_errors_for_injection(error_messages: list[dict]) -> str:
    """Render v1.7 error envelopes for inject-as-additional-context.

    Each error event's `content` is a JSON envelope per v1.7 contract.
    Output format leads with severity + type for routing, then enumerates
    each sub-error. During the v1.7 transition window, legacy string-format
    content (from un-migrated producers like state_transition.py) is rendered
    as-is with a [LEGACY] marker so the orchestrator still sees it.

    v1.7.1 (D38 discriminator MEDIUM fix): bounded output. Defensive caps protect
    against malformed producers bloating additionalContext on every prompt fire:
      - MAX_ERROR_EVENTS_PER_INJECTION events rendered (excess truncated)
      - MAX_SUB_ERRORS_PER_EVENT sub-errors per event (excess summarized)
      - MAX_CHARS_PER_DETAIL chars per sub-error.detail
      - MAX_CHARS_PER_CONTEXT_VALUE chars per context value
      - MAX_TOTAL_INJECTION_BYTES hard ceiling on returned string
    """
    MAX_ERROR_EVENTS_PER_INJECTION = 10
    MAX_SUB_ERRORS_PER_EVENT = 8
    MAX_CHARS_PER_DETAIL = 300
    MAX_CHARS_PER_CONTEXT_VALUE = 200
    # v1.7.2 (D38 iter-2 MEDIUM fix): keep under CC's ~10000-char inline-hook
    # threshold so the truncation marker stays visible inline (above the threshold,
    # CC writes output to a file and only passes a path + preview, hiding the marker).
    MAX_TOTAL_INJECTION_BYTES = 9000

    def _trunc(s, n):
        if not isinstance(s, str):
            s = str(s)
        return s if len(s) <= n else s[:n] + "...[TRUNC]"

    out_lines = ["**Inter-session errors received (v1.7 envelope channel):**\n"]
    total_events = len(error_messages)
    rendered_events = error_messages[:MAX_ERROR_EVENTS_PER_INJECTION]
    truncated_event_count = max(0, total_events - len(rendered_events))

    for m in rendered_events:
        if not isinstance(m, dict):
            continue
        eid = m.get("event_id")
        from_agent = m.get("from_agent") or m.get("agent") or "unknown"
        content = m.get("content") or ""
        try:
            env = json.loads(content)
            if not isinstance(env, dict) or "type" not in env:
                raise ValueError("not an envelope")
            etype = env.get("type", "unknown")
            sev = env.get("severity", "error")
            errors = env.get("errors") or []
            ctx = env.get("context") or {}
            sev_upper = sev.upper() if isinstance(sev, str) else "ERROR"
            out_lines.append(
                f"\n[{sev_upper}] type={etype} from={from_agent} event_id={eid}"
            )
            rendered_errs = errors[:MAX_SUB_ERRORS_PER_EVENT] if isinstance(errors, list) else []
            for e in rendered_errs:
                if not isinstance(e, dict):
                    out_lines.append(f"  - <malformed sub-error: {type(e).__name__}>")
                    continue
                ek = e.get("kind", "?")
                ed = _trunc(e.get("detail", ""), MAX_CHARS_PER_DETAIL)
                rec = e.get("auto_recovered", False)
                rec_str = " [auto-recovered]" if rec else ""
                out_lines.append(f"  - {ek}: {ed}{rec_str}")
            if isinstance(errors, list) and len(errors) > MAX_SUB_ERRORS_PER_EVENT:
                out_lines.append(
                    f"  - ... [{len(errors) - MAX_SUB_ERRORS_PER_EVENT} more sub-errors truncated]"
                )
            if ctx and isinstance(ctx, dict):
                ctx_items = [(k, _trunc(v, MAX_CHARS_PER_CONTEXT_VALUE)) for k, v in list(ctx.items())[:6]]
                ctx_str = ", ".join(f"{k}={v}" for k, v in ctx_items)
                out_lines.append(f"  context: {ctx_str}")
        except (json.JSONDecodeError, ValueError):
            # Legacy string-format content (pre-v1.7 producers during transition window).
            out_lines.append(
                f"\n[LEGACY] from={from_agent} event_id={eid}: {_trunc(content, MAX_CHARS_PER_DETAIL)}"
            )

    if truncated_event_count > 0:
        out_lines.append(
            f"\n[... {truncated_event_count} more error events truncated; raise MAX_ERROR_EVENTS_PER_INJECTION to see all]"
        )

    out = "\n".join(out_lines)
    # Hard ceiling on returned string
    if len(out.encode("utf-8")) > MAX_TOTAL_INJECTION_BYTES:
        # Truncate to byte ceiling, then re-encode safely
        encoded = out.encode("utf-8")[:MAX_TOTAL_INJECTION_BYTES - 60]
        # Avoid splitting mid-multibyte char
        out = encoded.decode("utf-8", errors="ignore") + "\n\n[... INJECTION TRUNCATED at byte ceiling]"
    return out


def do_receive_errors(hook_event_name: str) -> None:
    """v1.7 (Wave 6 Track 4) — receive routine for the universal error envelope channel.

    Polls broker for kind=error events addressed to this session_id, injects
    them as additionalContext for the agent's next turn. Mirrors do_receive()
    shape but uses separate error cursor + error formatter.

    Silent-degrade on any unexpected exception: log full traceback to stderr,
    emit `{}` to stdout, exit 0. NEVER break the CC session because of a hook bug.
    """
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    try:
        payload = read_hook_stdin()
    except Exception as e:
        sys.stderr.write(
            f"relay do_receive_errors[{hook_event_name}]: stdin parse failed "
            f"({type(e).__name__}: {e}); degrading\n"
        )
        emit_hook_response(hook_event_name, None)
        return

    codex_registration: Optional[dict] = None
    try:
        if is_codex_hook_payload(payload):
            codex_registration = resolve_codex_registration(payload)
            session_id = str(codex_registration["id"])
        else:
            session_id = resolve_session_id(payload)
    except Exception as e:
        sys.stderr.write(
            f"relay do_receive_errors[{hook_event_name}]: session_id unresolved "
            f"({type(e).__name__}: {e}); degrading\n"
        )
        emit_hook_response(hook_event_name, None)
        return

    emitted = False
    try:
        cursor = read_error_cursor(session_id)
        # v1.7.2 (D38 iter-2 HIGH fix): fetch limit MUST equal the render cap.
        # Previously: fetch 20 + render 10 + cursor-advance to all-fetched = events
        # 11-20 silently lost. Now: fetch only what we can render; trailing events
        # roll forward to the next hook fire.
        if codex_registration is not None:
            result = fetch_pending_codex_errors(codex_registration, since=cursor, limit=10)
        else:
            result = fetch_pending_messages(session_id, since=cursor, kind="error", limit=10)
        messages = result.get("messages") or []

        max_event_id = cursor
        for m in messages:
            if not isinstance(m, dict):
                continue
            eid = m.get("event_id")
            if isinstance(eid, int) and eid > max_event_id:
                max_event_id = eid

        next_cursor = (
            result.get("next_cursor")
            if codex_registration is not None
            else max_event_id
        )
        if not isinstance(next_cursor, int) or next_cursor < cursor:
            next_cursor = max_event_id

        if not messages:
            emit_hook_response(hook_event_name, None)
            emitted = True
            if codex_registration is not None and next_cursor > cursor:
                write_error_cursor(session_id, next_cursor)
            return

        formatted = format_errors_for_injection(messages)
        emit_hook_response(hook_event_name, formatted)
        emitted = True

        if next_cursor > cursor:
            try:
                write_error_cursor(session_id, next_cursor)
            except Exception as e:
                sys.stderr.write(
                    f"relay do_receive_errors[{hook_event_name}]: cursor write failed "
                    f"({type(e).__name__}: {e}); next fire will re-deliver\n"
                )
    except Exception:
        # A diagnostic failure must not prevent the response fallback.
        try:
            sys.stderr.write(
                f"relay do_receive_errors[{hook_event_name}]: unexpected exception; full traceback:\n"
                + traceback.format_exc()
            )
        except Exception:
            pass
        if not emitted:
            try:
                emit_hook_response(hook_event_name, None)
            except Exception:
                pass
