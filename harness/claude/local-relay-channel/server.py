# -*- coding: utf-8 -*-
"""Claude Code channel adapter for the agent-messaging broker.

Streams directed agent_message/agent_reply/handoff events from broker SSE into
notifications/claude/channel. The installed plugin contains the manifest, MCP
configuration, this server and optional cc_channel launchers.

Identity comes from ~/.claude/sessions/<parent-pid>.json, written by the owning
Claude Code process, with CLAUDE_CODE_SESSION_ID as a fallback. Only the direct
parent is considered: an ancestor walk could adopt a reused PID from another
session. The process record's conversation title is never a routing identity.
Names must instead be explicitly set in the relay registry or daemon job state.

BROKER_BASE_URL overrides the installed nonsecret connection selection; only an
absent selection defaults to http://127.0.0.1:8787. All diagnostics use stderr;
stdout belongs to MCP JSON-RPC. Custom channel messages use raw notifications.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

# --- Stdlib sanity: stderr must be UTF-8 (CC plugin logs are utf-8) ----------
# Windows console stderr defaults to cp1252; reconfigure if available.
try:  # pragma: no cover — best-effort; non-fatal if reconfigure unavailable
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass


# --- broker_client import (reused for `peer_reply` tool POST) -----------------
# broker_client.py is shared by relay-send, relay-check and the hook library.
# Prefer the installed relay library over the optional generic scripts directory.
# Each directory is MOVED to the front, not merely added when absent: if the
# primary were already somewhere on sys.path and only the fallback were
# inserted, the fallback would land ahead of it and a stale copy would win.
_AGENTS_SCRIPTS = os.path.expanduser("~/.agents/scripts")
_RELAY_HOOKS = os.path.expanduser("~/.claude/hooks/relay")
for _p in (_AGENTS_SCRIPTS, _RELAY_HOOKS):  # the last one moved to the front is searched first
    while _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

from broker_connection import resolve_broker_base_url


def _eprint(msg: str) -> None:
    """Write a line to stderr (CC plugin logs). NEVER stdout (= MCP wire)."""
    sys.stderr.write(msg.rstrip("\n") + "\n")
    sys.stderr.flush()


# --- Identity resolution (C-13 / D35 / F-CC12) -------------------------------
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# The MCP subprocess's environment identity can differ from its owning session.
# Read the authoritative direct-parent session record, without PID-table walks
# or a cached identity that could outlive a session transition.
_SESSION_DIR = Path.home() / ".claude" / "sessions"

def _resolve_session_file():
    """Locate the CC session file for the process that owns this subprocess.

    HOP 0 ONLY. The direct parent, one `stat`, no process-table snapshot, no
    ancestor walk, no memoisation, no module state.

    Only the direct parent is authoritative. Walking an unpinned process table
    could adopt a reused PID from another live session. The launch wrappers wrap
    Claude Code, not this MCP child, so no wrapper hop is needed here.
    """
    try:
        p = _SESSION_DIR / f"{os.getppid()}.json"
        return p if p.is_file() else None
    except Exception:
        return None


def _read_session_identity():
    """The sessionId, read FRESH from the owning CC process's session file.

    Returns None when unresolvable. EVERY caller falls back rather than
    failing: an unresolvable identity must degrade exactly as it did before this
    change and must never take the server down.

    Returns only sessionId. The record's name is a mutable conversation title,
    not a user-set routing identity; callers must use the relay registry or
    daemon job state for a name. Never infer one from a workspace or title.
    """
    p = _resolve_session_file()
    if p is None:
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(d, dict):
        return None
    sid = d.get("sessionId")
    return sid if isinstance(sid, str) and _UUID_RE.match(sid) else None


def _resolve_session_id() -> str:
    """Resolve this session's TRUE session_id.

    SOURCE 1 (authoritative): the owning CC process's session file, above.
    SOURCE 2 (fallback): the `CLAUDE_CODE_SESSION_ID` env var.

    THE ORDER IS THE FIX (D133). The env var is not stale, it is WRONG for this
    subprocess, so it cannot be primary. It stays as the fallback because it is
    the documented interface and is the only source available for a manual
    out-of-CC launch -- the sole remaining reason the fail-loud below exists.

    NOT a C-13 violation: nothing here derives, guesses or invents an identity.
    It reads an id CC itself wrote to disk -- the same file
    `register_session.py` and `shared.py` already read. C-13 forbids inventing a
    session id (uuid4 / pid-as-identity); this reads the authoritative record.
    """
    sid = _read_session_identity()
    if sid:
        return sid
    sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if not sid:
        _eprint(
            "local-relay-channel: CLAUDE_CODE_SESSION_ID env var not set. "
            "This MCP server must run inside a Claude Code session (CC injects "
            "this env var into every MCP subprocess at spawn time per F-CC12). "
            "If launching manually outside CC, set "
            "CLAUDE_CODE_SESSION_ID=<sid> explicitly before invoking."
        )
        sys.exit(1)
    if not _UUID_RE.match(sid):
        _eprint(
            f"local-relay-channel: CLAUDE_CODE_SESSION_ID={sid!r} is not "
            "UUID-shaped (expected 8-4-4-4-12 lowercase hex). Refusing to "
            "start; fail-loud per C-13."
        )
        sys.exit(1)
    return sid


def _broker_base_url() -> str:
    return resolve_broker_base_url()


# --- Friendly-name resolution (ACTIVE PLAN: friendly-name routing primitive) --
_NAME_FOLD = {c: c + 32 for c in range(ord("A"), ord("Z") + 1)}


def _normalize_name(name):
    """Fold a relay friendly_name to its canonical form: ASCII A-Z -> a-z.

    SYNC — THE DEFINITION OF RECORD IS `shared.py::normalize_name`, and this is a
    deliberate COPY, not an import. This MCP server is intentionally standalone
    and lives in a different repo shipping on a different channel (the broker
    Release; `shared.py` ships via agent-config), so it cannot import from the
    hooks package. Same reason `_read_daemon_job_name` below is a copy. **If the
    fold rule ever changes, BOTH must change in the same wave** — a key that
    differs between the writer and the reader is the entire defect this closes.

    WHY THIS EXISTS. A friendly_name is a KEY addressed across machines, and the
    fleet spans a case-INSENSITIVE filesystem (NTFS) and a case-SENSITIVE one
    (ext4), so unfolded `Alpha`/`alpha` are ONE mailbox on Windows and TWO on
    Linux — same code, two behaviours, which C-48 forbids.

    WHY ASCII TRANSLATE AND NOT `.lower()` / `.casefold()`: both map characters
    from OUTSIDE the validated alphabet INTO it (`.casefold()` sends U+212A to
    'k', `.lower()` sends U+0130 to 'i' + U+0307), which would collapse two
    genuinely distinct names into one mailbox — inventing the collision this
    removes. `_KEY_RE`'s alphabet is [A-Za-z0-9._-], so an A-Z-only translate
    collapses exactly the case-variants intended and nothing else. Everything
    outside A-Z passes through untouched, which matters because the CC job-state
    name is guarded by no validator at all.

    Non-str returns unchanged: the resolvers here are Optional[str] and must
    keep returning None rather than raise — `_resolve_my_name`'s only call site
    sits inside ZERO enclosing try blocks, so a raise here is an MCP startup
    death, which is the failure class this file was rewritten to avoid.
    """
    if not isinstance(name, str):
        return name
    return name.translate(_NAME_FOLD)


def _read_daemon_job_name(session_id: str) -> str | None:
    """Background-agent durable name source (CC Agent-View daemon).

    CC's background-agents daemon (`claude agents`) writes per-job state at
    `~/.claude/jobs/<sid>/state.json` carrying the user-set name it durably
    tracks for that agent across respawns:

        {"name": "test_a", "nameSource": "user", "template": "bg",
         "sessionId": "<sid>", "resumeSessionId": "<sid>", ...}

    Each daemon respawn re-issues the session_id but re-writes this file with the
    SAME name under the new sid — so this is the DRIFT-IMMUNE identity for a
    background agent (empirically: two distinct sids both carried name='test_a').
    Honored ONLY when:
      - nameSource == "user"  → the user's explicit name (per C-13 we never read
        an auto-assigned name), AND
      - template == "bg"      → foreground jobs store a formatted display title
        ("Active: …") in `name`, which is NOT the relay friendly_name.

    This is the precise per-sid linkage that fixes the background-agents
    disconnect: the daemon spawns workers under fresh, unnamed session_ids the
    relay registry has no entry for, but the daemon's own job-state still carries
    the user-set name. Returns the clean name string, or None.
    """
    jobs_dir = Path.home() / ".claude" / "jobs"
    # The daemon names the job dir by the 8-char "daemonShort" (== sessionId[:8]),
    # NOT the full session_id. Try the full id first (forward-compat), then the
    # short prefix; validate the state.json's own `sessionId` matches to guard
    # against an 8-hex prefix collision between two sessions.
    for candidate in (session_id, session_id[:8]):
        try:
            s = json.loads(
                (jobs_dir / candidate / "state.json").read_text(encoding="utf-8")
            )
        except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
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
            # THE LEAF FOLD (C-48) — and in THIS file it is the load-bearing one,
            # because this function is the ONLY name source SHARED by the two
            # independent identity-producing resolvers below:
            #
            #   _routing_name -> _resolve_my_name    -> subscription id, SSE
            #                                           filter, from_agent, resume
            #   _delivery_key -> _delivery_key_name  -> confirmed/<X>.json name,
            #                                           POST /api/agent/<X>/ack
            #
            # Folding at ONE of those resolvers instead of here was the design
            # that adversarial review killed: it makes this server subscribe as
            # 'alpha' while filing its cursor as 'Alpha'. On NTFS those two
            # filenames collide into one, so the split is INVISIBLE on every
            # case-insensitive filesystem and breaks on a case-sensitive filesystem.
            # Folding the leaf gives both resolvers the same value by
            # construction, which is why there is nothing to keep in step.
            #
            # Mirrors shared.py::resolve_daemon_job_name, which folds at exactly
            # this point.
            return _normalize_name(name.strip())
    return None


def _resolve_my_name(session_id: str) -> str | None:
    """Resolve the routing friendly_name for this session, or None if unnamed.

    Resolution order (most-authoritative first):
      1. `~/.claude/jobs/<sid>/state.json` daemon job-state (nameSource == "user"
         and template == "bg") → that name (background/daemon agents; authoritative
         + drift-immune across respawns; see `_read_daemon_job_name`). Checked
         FIRST so a stale or inherited registry entry cannot override a bg agent's
         durable Agent-View name.
      2. `registry[session_id]` with naming_status == "user_set" and non-empty
         name → that name (foreground sessions that ran `/relay-name`).

    Returns None when neither source yields a user-set name.

    TWO SOURCES, AND THAT IS THE SAME TWO `_delivery_key_name` READS, IN THE SAME
    ORDER. It is what makes the routing identity and the delivery key equal by
    construction rather than by a check: there is no source here the receive hook
    cannot also see, so the two cannot return different strings for one session.

    The workspace's remembered name file is deliberately not a source: it can
    outlive a registry name reclamation and is invisible to the receive hook.
    If the registry is unreadable, remain push-inactive rather than subscribing
    under a name for which this session cannot acknowledge delivery.

    The caller MUST NOT treat None as fatal. Per C-8 (session_id is the foolproof
    primitive; friendly_name is the routing overlay) + the background-agents
    daemon fix: an unnamed session starts in DEGRADED "push-inactive" mode rather
    than exiting. Pre-fix this fail-loud-exited (sys.exit(1)), which killed the
    MCP server for EVERY daemon-spawned worker — the daemon assigns fresh,
    unnamed session_ids the user never ran `/relay-name` against, so the
    registry-only lookup always missed → exit → "Connection closed" storm. The
    friendly_name routing identity is unchanged (NO session_id regression); we
    just added the daemon job-state as a second name source and made the no-name
    case degrade instead of crash.

    Eliminates the session_id-drift wedge class (F-CC30): friendly_name doesn't
    drift across CC lifecycle events; the daemon re-writes it under each new sid.
    """
    # 1. CC daemon job-state (background agents): the daemon is the source of
    #    truth for a backgrounded agent's identity. Checked FIRST so a bg agent's
    #    durable Agent-View name cannot be overridden by a stale or inherited
    #    registry entry (discriminator iter-1 HIGH: register_session.py could
    #    write an inherited workspace name for a bg sid). `_read_daemon_job_name`
    #    only matches template=="bg" jobs, so FOREGROUND sessions return None here
    #    and fall through to the registry path below — no foreground regression.
    bg_name = _read_daemon_job_name(session_id)
    if bg_name:
        return bg_name
    # A session record's name is a mutable conversation title, not a routing
    # identity. Its sessionId is authoritative; names come only from the two
    # explicit user-set sources above and below.
    # 2. Relay registry: explicit user-set name (foreground /relay-name).
    try:
        p = Path.home() / ".claude" / "relay" / "registry.json"
        with open(p, "r", encoding="utf-8") as f:
            reg = json.load(f)
        # `reg` must be an OBJECT. A registry whose top level is a list, string,
        # number or null makes `.get` raise AttributeError, which is NOT in the
        # except tuple below -- MEASURED raising on all three shapes.
        # A GUARD RATHER THAN AN EARLY RETURN, kept deliberately: it costs this
        # source and nothing else, and `_delivery_key_name` treats the same shape
        # the same way. The two resolvers must agree on degradation as well as on
        # the name they return.
        if isinstance(reg, dict):
            entry = reg.get(session_id) or {}
            if isinstance(entry, dict):
                name = entry.get("name")
                ns = entry.get("naming_status")
                if isinstance(name, str) and name and ns == "user_set":
                    # ENTRY-POINT FOLD (C-48), source 2 of 2 in this resolver.
                    # `_delivery_key_name` folds the SAME read independently —
                    # the two must return the same string for the same sid or the
                    # subscription and the cursor filename part company.
                    return _normalize_name(name)
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
        # ValueError covers UnicodeDecodeError on a non-UTF-8 registry.json.
        # MEASURED: without it this raises, and the ONLY call site of this
        # function -- `my_name = _resolve_my_name(my_sid)` in `_build_and_run` --
        # sits inside ZERO enclosing try blocks, so the exception takes the whole
        # MCP server down AT STARTUP. That is the "Connection closed" class this
        # function's own docstring says it was rewritten to avoid, and it fires
        # for every session on the box at once because they all read the same
        # file. The hook's mirror of this resolver already catches ValueError;
        # the two must agree on degradation as well as on the name they return.
        pass
    return None


# --- Cursor persistence (resume from last-seen event_id on reconnect) --------
# Per D42 (default-tail discipline): on FIRST start with no cursor, prime
# from broker's current next_cursor — DO NOT replay full history. User just
# enabled channels; doesn't want an inception dump.
#
# Iter-3 fix H3 (cap-5): TWO SEPARATE CURSORS.
#   1. DELIVERED-DEDUP cursor at `channel-cursors/{name}.json` —
#      advances ONLY when emit_status == "delivered". Receive-hook reads this
#      as its dedup floor (shared.py:Site R). A failed emit leaves it
#      UNTOUCHED so the backup-path can still re-fetch + inject.
#   2. SSE-RESUME cursor at `channel-resume/{name}.json` —
#      advances on every event seen on wire (regardless of emit outcome).
#      Used as Last-Event-ID on SSE reconnect to avoid re-receiving events
#      we already pulled (even if we failed to deliver them; the backup-path
#      will get them via the lower delivered-dedup cursor).
#
# Pre-iter-3, both purposes shared one cursor — a failed emit advanced it,
# the next reconnect persisted that value as the dedup floor, and the
# receive-hook backup-path skipped it forever (silent loss class).
def _delivered_cursor_path(name: str) -> Path:
    """DELIVERED-DEDUP cursor. Receive-hook reads this as dedup floor.
    Advances only on emit_status == "delivered". friendly-name keyed.
    """
    return Path.home() / ".claude" / "relay" / "channel-cursors" / f"{name}.json"


def _resume_cursor_path(name: str) -> Path:
    """SSE-RESUME cursor. Used as Last-Event-ID on broker reconnect.
    Advances on every event seen on wire (regardless of emit success).
    Distinct file so a failed emit doesn't poison the dedup floor.
    """
    return Path.home() / ".claude" / "relay" / "channel-resume" / f"{name}.json"


# Legacy alias preserved for any external readers (helper smoke tests, ops
# scripts) that reference `_cursor_path`. New code SHOULD use the explicit
# `_delivered_cursor_path` / `_resume_cursor_path`.
_cursor_path = _delivered_cursor_path


def _read_delivered_cursor(name: str) -> int | None:
    """Return the persisted DELIVERED dedup cursor for this name, or None."""
    p = _delivered_cursor_path(name)
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        v = data.get("last_event_id")
        if isinstance(v, int) and v >= 0:
            return v
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError):
        pass
    return None


def _read_resume_cursor(name: str) -> int | None:
    """Return the persisted SSE-RESUME cursor for this name, or None.

    Silent-tolerate corrupt/missing files; fresh-start handling is the
    caller's job (prime from broker's next_cursor per D42).
    """
    p = _resume_cursor_path(name)
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        v = data.get("last_event_id")
        if isinstance(v, int) and v >= 0:
            return v
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError):
        pass
    return None


# Legacy alias: pre-iter-3 callers expecting _read_cursor get the RESUME
# cursor (it's the higher of the two; safe for SSE Last-Event-ID purposes).
def _read_cursor(name: str) -> int | None:
    """DEPRECATED: prefer _read_resume_cursor (for SSE resume) or
    _read_delivered_cursor (for dedup floor). Returns RESUME cursor for
    back-compat with code that hasn't migrated; falls back to delivered
    cursor if resume file doesn't exist (first start after iter-3 fix).
    """
    v = _read_resume_cursor(name)
    if v is not None:
        return v
    return _read_delivered_cursor(name)


def _write_cursor_atomic(p: Path, event_id: int, name: str) -> None:
    """Shared atomic-write helper for both delivered + resume cursor files.
    Per M2-R1 (discriminator iter-2): refuses to persist the SQLITE_I64_MAX
    first-start sentinel; a fresh cursor file appears only AFTER the first
    real event_id has been observed, restoring R3.5 reconnect-resume.
    """
    if event_id >= 9223372036854775807:  # SQLITE_I64_MAX first-start sentinel
        return
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".cursor-", suffix=".tmp", dir=str(p.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"last_event_id": event_id, "name": name,
                           "ts": int(time.time() * 1000)}, f)
            os.replace(tmp_path, p)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as e:
        _eprint(f"local-relay-channel: cursor write failed at {p}: "
                f"{type(e).__name__}: {e}")


def _write_delivered_cursor(name: str, event_id: int) -> None:
    """Persist DELIVERED dedup cursor (receive-hook reads as floor).
    Advances ONLY on emit_status == "delivered" per iter-3 H3 fix.
    """
    _write_cursor_atomic(_delivered_cursor_path(name), event_id, name)


def _write_resume_cursor(name: str, event_id: int) -> None:
    """Persist SSE-RESUME cursor (used as Last-Event-ID on reconnect).
    Advances on every event seen on wire regardless of emit outcome.
    """
    _write_cursor_atomic(_resume_cursor_path(name), event_id, name)


# Legacy alias for any external caller — writes BOTH cursor files
# (pre-iter-3 semantics where both were the same file). Should NOT be called
# from new code; use _write_delivered_cursor / _write_resume_cursor explicitly.
def _write_cursor(name: str, event_id: int) -> None:
    """DEPRECATED legacy: writes both delivered + resume cursors. Pre-iter-3
    callers got both purposes from one file; this preserves that contract for
    any external code paths that haven't migrated to the explicit form.
    New code in this module uses _write_delivered_cursor / _write_resume_cursor
    explicitly so the per-purpose semantics are clear.
    """
    _write_delivered_cursor(name, event_id)
    _write_resume_cursor(name, event_id)


# --- Per-session confirmed-set (LOSSLESS-DELIVERY refactor; Phase 2 / A2) ------
# Each recipient SESSION (keyed by session_id, NOT friendly_name) keeps a
# confirmed-set at `~/.claude/relay/confirmed/{sid}.json`:
#     {"floor": int, "acked": [ids], "updated_at": ms,
#      "epoch_name": str|absent, "epoch_broken": bool}
# where `floor` is the contiguous-confirmed high-water and `acked` carries
# confirmed ids ABOVE a gap. `epoch_name`/`epoch_broken` (OQ-RELAY-6/D78) tag the
# identity the floor accrued under; STAMPED only by shared.py:_advance_floor_locked,
# PRESERVED verbatim here (the broker_ack fold never accrues). An event is
# "confirmed for me" iff
# `id <= floor OR id in acked`. This file is shared between the broker_ack tool
# (here) and the receive-hook backstop (shared.py:do_receive), so EVERY write
# MUST hold the sentinel lock + write atomically. Locking convention copied
# verbatim from `relay-name/scripts/set_name.py` (sentinel `.lock` file;
# msvcrt on Windows / fcntl on POSIX; NB retry ×10 @ 50ms; atomic write = temp
# file in same dir + os.replace).
_BROKER_ACK_MAX_IDS = 4096  # defensive cap on a single broker_ack batch.


def _confirmed_dir() -> Path:
    return Path.home() / ".claude" / "relay" / "confirmed"


# --- delivery key (D130) ----------------------------------------------------
# SYNC: this MUST agree, value-for-value, with `shared.py::_delivery_key`. The
# broker addresses a recipient by NAME and its inbox query is
#     WHERE to_agent = ? AND event_id > ?
# so the cursor has to be filed under whatever `to_agent` is keyed by. The hook
# writes that file; this process reads it. If the two ever compute a DIFFERENT
# key for the same session, they open different files -- which is exactly the
# defect this whole change exists to close, wearing a new coat.
#
# THE KEY STILL HAS ITS OWN RESOLVER, and that is now a REDUNDANCY rather than a
# compensation. Measured: `shared.py::_resolve_name_from_registry` consults
# exactly TWO sources -- daemon job-state, then registry `naming_status ==
# "user_set"`. `_resolve_my_name` above consults THOSE SAME TWO, in that order,
# since the workspace-file source was removed (see its docstring's REJECTED
# note). So the routing name and the delivery key cannot be different strings for
# one session: there is no source on either side the other cannot see. Keeping
# the two functions separate is what holds the plugin's key to the HOOK's
# definition -- a name source added to routing alone would re-open the split, and
# a separate function is where that gets noticed.
#
# Note this is only safe because `_resolve_session_id` now returns the TRUE sid
# (D133): the registry lookup below reads the same row the hook reads, so the
# two sides agree BY CONSTRUCTION rather than by luck.
_KEY_RE = re.compile(r"^[A-Za-z0-9._\-]{1,64}$")
_RESERVED_BASENAMES = frozenset(
    ["CON", "PRN", "AUX", "NUL"]
    + ["COM%d" % i for i in range(1, 10)]
    + ["LPT%d" % i for i in range(1, 10)]
    # SYNC with shared.py's set, which derives this entry from its own
    # `_TELEMETRY_FILENAME`. The counters file lives in the SAME directory the
    # confirmed-sets now occupy and its stem passes _KEY_RE, so before name-keying
    # that namespace was UUID-only and could not collide -- it can now. MEASURED:
    # a session keyed `delivery-counters` reads the counters file as its
    # confirmed-set, so the seed takes its "migrated already" early return on it
    # and this gate then refuses every id forever. Spelled out here because this
    # file does not define the telemetry constant; if that filename ever changes,
    # BOTH sides must change or the two keys diverge.
    + ["DELIVERY-COUNTERS"]
)


# Sentinel for "the registry could not be read", as distinct from "the registry
# was read and this session has no name". A unique object rather than a string:
# every string is a candidate delivery key, so a string sentinel could itself be
# mistaken for an answer -- which is the exact class of defect this whole change
# removes. `is` comparison only; never truth-tested.
_NAME_UNKNOWN = object()


def _delivery_key_name(session_id: str) -> "str | None":
    """The routing name AS THE HOOK COMPUTES IT. Mirror of
    `shared.py::_resolve_name_from_registry` -- two sources, same order, same
    `user_set` gate. Do not add sources here without adding them there.

    THREE OUTCOMES, NOT TWO, AND THE THIRD IS WHY THIS FUNCTION CHANGED.
      * a `str`          -- this session's name.
      * `None`           -- ESTABLISHED nameless: the registry was read and
                            holds no user-set name for this session.
      * `_NAME_UNKNOWN`  -- COULD NOT ESTABLISH: the registry was unreadable,
                            corrupt, non-UTF-8, or not a JSON object.
    Those last two used to be the same return value, and merging them is what
    let a transient fault be recorded as a fact about this session. A caller
    cannot recover a distinction its callee threw away, so the split has to
    happen here rather than one frame up.

    BOTH SOURCES ARE FOLDED (C-48): the job-state one at the leaf in
    `_read_daemon_job_name`, the registry one inline below. That is what makes
    this function and `_resolve_my_name` agree by construction rather than by
    two folds kept in step -- the split between those two is what got the
    predecessor change reverted, and on NTFS it would have been invisible.

    THE ASYMMETRY THIS DOCSTRING USED TO DESCRIBE IS GONE. `_resolve_my_name`
    once had a THIRD source (`<workspace>/.relay-name.json`) that this function
    deliberately did not mirror, because the hook cannot see that file and adding
    it here would SPLIT the key rather than close a gap. It was removed from the
    routing side instead, 2026-08-15, so the two resolvers now read the same two
    records in the same order and a disagreement is not constructible. Do not add
    a source to either side alone; the hook is the third party both must match.
    """
    try:
        bg = _read_daemon_job_name(session_id)
        if bg:
            return bg
    except Exception:
        # A job-state read that fails is NOT evidence of namelessness either --
        # but it is not evidence of a broken registry, so fall through to the
        # registry rather than refuse. A foreground session has no job-state at
        # all and must not be refused for its absence.
        pass
    try:
        p = Path.home() / ".claude" / "relay" / "registry.json"
        with open(p, "r", encoding="utf-8") as f:
            reg = json.load(f)
        # `reg` must be an OBJECT. A registry whose top level is a list, string,
        # number or null is not a readable registry -- it is a corrupt one, and
        # saying "no name" about it is the merge this function exists to stop.
        if not isinstance(reg, dict):
            return _NAME_UNKNOWN
        # AN ABSENT ROW AND A PRESENT-BUT-CORRUPT ROW ARE DIFFERENT ANSWERS, and
        # collapsing them is how this defect survived INSIDE its own fix. The
        # first draft wrote `entry = reg.get(session_id) or {}`, so a row holding
        # a string, list, number, bool or explicit null fell through the
        # `isinstance(dict)` test to `return None` -- reported as ESTABLISHED
        # NAMELESS, which the caller then turns into a session-id key. Measured
        # on the draft: 5 of 5 malformed shapes produced a sid key, with the
        # absent-row and valid-row controls both behaving correctly, so the
        # collapse was invisible to every test that did not corrupt a row.
        # Caught by cross-family review, reproduced by me before acting.
        #
        # Membership is tested rather than truthiness because `.get()` cannot
        # tell an absent key from one whose value is null, and those two need
        # OPPOSITE answers: absent is a genuinely unregistered session (nameless,
        # key by sid); null is a corrupt row (refuse).
        if session_id not in reg:
            return None                     # no row at all -- genuinely nameless
        entry = reg[session_id]
        if not isinstance(entry, dict):
            return _NAME_UNKNOWN            # row present and corrupt -- refuse
        name = entry.get("name")
        ns = entry.get("naming_status")
        if isinstance(name, str) and name and ns == "user_set":
            # ENTRY-POINT FOLD (C-48). Same read, same fold, as
            # `_resolve_my_name`'s source 2 above -- deliberately identical
            # so the subscription identity and the cursor filename cannot
            # part company for the same sid.
            return _normalize_name(name)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        # THE ARM THAT USED TO LIE. It previously fell through to `return None`,
        # which the caller then turned into a session-id key -- so an unreadable
        # registry minted a permanent orphan cursor file that silently converts a
        # fail-closed refusal into a stale allow-list.
        return _NAME_UNKNOWN
    return None


def _delivery_key() -> "str | None":
    """The identity the broker ADDRESSES this session by -- hence the identity
    its confirmed-set is FILED under. `None` means REFUSE: we could not establish
    who we are, and no key may be invented.

    Resolve the current owning-session record inside this function rather than
    accepting a caller's boot-time session ID. Claude Code can rotate that ID
    while this MCP child remains alive; filing under a stale ID would split the
    plugin's confirmed set from the hook's name-keyed one.

    A genuinely nameless session is still keyed by its current session ID, as
    on the hook side. An unavailable identity returns None and never invents a
    cursor key. Only the current parent record or documented fallback resolves
    identity; no ancestor walk or arbitrary-session entrypoint is allowed.
    """
    sid = _read_session_identity()
    if not sid:
        # Cannot establish the current identity. Everything downstream of this
        # value is a FILENAME and a broker mailbox; inventing either is C-13's
        # prohibition and D143's production incident.
        return None
    name = _delivery_key_name(sid)
    if name is _NAME_UNKNOWN:
        return None
    if name is None:
        return sid          # established nameless -- the sid is the right key
    # A name that cannot be a safe filename is treated as nameless, identically
    # to the hook (`_routing_name`'s measured table), so the two sides agree.
    return name if _key_accepts(name) else sid


def _key_accepts(name) -> bool:
    """Would `name` survive as a delivery-key FILENAME?

    Factored out of `_delivery_key` so the SUBSCRIPTION path can ask the same
    question the CURSOR path asks, from the same code, rather than from a second
    copy of the rules that would drift. The rule set is unchanged -- verified by
    re-running the full hook-vs-plugin differential after the refactor.
    """
    if not name or not isinstance(name, str):
        return False
    if not _KEY_RE.match(name) or name in (".", ".."):
        return False
    if name.split(".")[0].upper() in _RESERVED_BASENAMES:
        return False
    return True


def _routing_name(session_id: str) -> "str | None":
    """The name push may SUBSCRIBE under -- `_resolve_my_name` GATED by the
    delivery-key validator.

    THE SPLIT THIS CLOSES, and it is across the file boundary rather than inside
    either file. The hook now treats a key-rejected name as NAMELESS end to end
    (it polls by sid and posts no mirror). Without this gate the plugin would
    still SUBSCRIBE under that rejected name -- so the session goes on ACCEPTING
    mail addressed to a mailbox that nothing will ever sweep or acknowledge:
    `broker_ack` gates and posts under the sid, and the hook no longer polls that
    name at all.

    MEASURED, name -> (hook poll, plugin subscription) BEFORE this gate:
        'normalname'        -> ('normalname', 'normalname')   agree
        'delivery-counters' -> (SID,          'delivery-counters')   SPLIT
        'CON' / 'NUL' / 65-char / '.' -> (SID, the rejected name)    SPLIT

    Note what the hook fix traded, because it decides why this gate is the right
    close rather than a revert: BEFORE it, the hook polled as the NAME, so a
    push-missed CON-addressed row was still recoverable by the hook backstop.
    Removing that backstop was correct -- it eliminated a path where a
    sid-derived floor got mirrored UNDER the name and crossed off rows the
    session had never seen, which is silent loss. What it left behind is this
    subscription, and the fix is to stop subscribing, NOT to restore the poll.

    Degrading to None is deliberate and is the existing, well-trodden path: push
    goes inactive, the receive hook keeps delivering, and nothing is silently
    dropped. Substituting some OTHER identity here would be inventing one, which
    is what C-13 forbids and what D143 caught in production.

    THERE IS NO AGREEMENT GATE HERE, AND THERE IS NOTHING LEFT FOR ONE TO DO.
    A second half of this function once compared the routing name against the
    delivery key and adopted the key when they differed. It existed because
    `_resolve_my_name` read a THIRD name source (`<workspace>/.relay-name.json`)
    that `_delivery_key_name` could not -- the hook does not read that file -- so
    the two could answer with different strings and the session would accept mail
    it could never file a receipt for. That source was removed instead
    (2026-08-15): both resolvers now read the same two records in the same order,
    so the disagreement is not constructible and a check for it could only ever
    report false. Measured over 240 registry/job-state/file-shape combinations:
    75 disagreements with the third source, 0 without it.

    WHAT THE GATE DID THAT MUST NOT COME BACK: on a disagreement it returned the
    DELIVERY KEY, which for a demoted session is a raw session id -- putting a
    machine-generated identifier on the wire as this session's sender name. And
    its diagnostic had no latch, so a permanently-split session re-printed the
    same stderr paragraph every poll tick for the life of the process.

    THE FIRST GATE, on the other hand, STAYS: a resolved name that cannot be a
    delivery-key FILENAME still degrades to None. That one is about the hook's
    treatment of reserved/invalid names, not about a disagreement between two
    resolvers, and the measured table above is its evidence.
    """
    name = _resolve_my_name(session_id)
    if name and not _key_accepts(name):
        _eprint(
            f"local-relay-channel: resolved name {name!r} is NOT a usable delivery "
            f"key (reserved/invalid as a filename); starting DEGRADED rather than "
            f"subscribing to a mailbox the receive hook will never poll. Rename via "
            f"/relay-name to activate push."
        )
        return None
    return name


def _legacy_floor_for(key: str) -> int:
    """Create-floor seed for a confirmed-set, given a DELIVERY KEY.

    The legacy plain cursor lives at `cursors/<SESSION_ID>.json` -- a genuinely
    sid-scoped family. It is a legitimate seed ONLY when the delivery key IS the
    session id (a nameless session). Under a NAME key it must be 0.

    Why this matters rather than being tidy-up: seeding a NAME-keyed
    confirmed-set from `cursors/<NAME>.json` installs a floor of unproven
    provenance, which is exactly what `shared.py::_seed_name_key_from_sid`
    refuses to do and what its `cs_default` guard exists to prevent. A floor that
    is too HIGH suppresses ids the session never received -- silent loss, the
    one direction this codebase never trades for.

    In practice `cursors/<NAME>.json` usually does not exist and the read returns
    0 anyway. That is luck, not a guarantee: any friendly name that collides with
    a filename already in `cursors/` would inherit a foreign floor.
    """
    return _read_legacy_cursor_floor(key) if _UUID_RE.match(key) else 0


def _confirmed_path(sid: str) -> Path:
    return _confirmed_dir() / f"{sid}.json"


def _confirmed_lock_path(sid: str) -> Path:
    # Sentinel lock file (NOT the confirmed json itself) — on Windows
    # os.replace() refuses to overwrite a file held open in the same process.
    return _confirmed_dir() / f"{sid}.json.lock"


def _read_legacy_cursor_floor(sid: str) -> int:
    """Seed value for a NEWLY-CREATED confirmed-set: the legacy plain cursor
    `~/.claude/relay/cursors/{sid}.json` `last_seen_event_id` (default 0).

    FIX 3 (cross-family review, MED-1 migration): a session that had been
    advancing the plain cursor before the lossless refactor must inherit that
    value as its confirmed-set floor the FIRST time a confirmed-set is created
    for it — otherwise a floor=0 set is created and the receive-hook re-delivers
    all old history (and with the FIX-1 paged sweep, a floor=0 sweep is huge).

    Both writers (this broker_ack tool + shared.py:do_receive) MUST apply IDENTICAL
    seeding so whichever runs first on a migrated session creates the SAME floor.
    shared.py reads the same cursor file via `read_cursor()`; this mirror keeps
    server.py standalone (no shared-module import). Silent-degrade: any read /
    parse error → 0 (fail toward re-delivery, never toward silent loss).
    """
    p = Path.home() / ".claude" / "relay" / "cursors" / f"{sid}.json"
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            v = data.get("last_seen_event_id", 0)
            if isinstance(v, int) and not isinstance(v, bool) and v >= 0:
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
        pass
    return 0


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


def _read_confirmed_set(sid: str, default_floor: int = 0) -> dict:
    """Read the confirmed-set for a sid. Returns a normalized dict
    {"floor": int, "acked": list[int]} — default when absent/corrupt.

    FIX 3 (cross-family review): `default_floor` is the floor used when the
    confirmed-set file is ABSENT — seeded by the caller from the legacy plain
    cursor (`_read_legacy_cursor_floor`) so a migrated session that has only the
    plain cursor inherits it as its floor on first create (parity with
    shared.py:_read_confirmed_set's `default_floor`). When the file EXISTS, its
    own stored floor wins (a malformed/negative floor falls back to default_floor).

    NOTE: caller must hold the lock when this read is part of a
    read-modify-write. Standalone reads tolerate any error (return default).
    """
    p = _confirmed_path(sid)
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
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
        # awaiting_ack (P2, subagent-safe): hook-injected-to-MAIN allow-list.
        # None == ABSENT (pre-upgrade file) => broker_ack permissive fallback.
        # A list (even empty) is the authoritative gate. MUST match
        # shared.py:_read_confirmed_set semantics. Pruned > floor.
        aw_raw = data.get("awaiting_ack", None)
        if aw_raw is None:
            awaiting = None
        elif isinstance(aw_raw, list):
            awaiting = sorted({v for v in aw_raw
                               if isinstance(v, int) and not isinstance(v, bool)
                               and v > floor})
        else:
            awaiting = None
        # epoch tag (OQ-RELAY-6 / D78): identity the floor STARTED accruing under
        # + sticky cross-epoch-break flag. The PLUGIN never stamps these (only
        # shared.py:_advance_floor_locked accrues on floor-advance); here we
        # parse+return so the broker_ack fold PRESERVES them verbatim. MUST match
        # shared.py:_read_confirmed_set verbatim.
        # FAIL-CLOSED on malformed provenance (D78 "corrupt epoch => suppress"):
        # epoch_name wrong-type => None; epoch_broken PRESENT but non-bool => True
        # (broken); an epoch_name paired with a corrupt floor => drop it.
        en_raw = data.get("epoch_name", None)
        epoch_name = en_raw if isinstance(en_raw, str) else None
        eb_raw = data.get("epoch_broken", False)
        epoch_broken = eb_raw if isinstance(eb_raw, bool) else True
        if floor_corrupt:
            epoch_name = None
        return {"floor": floor, "acked": sorted(set(acked)),
                "awaiting_ack": awaiting,
                "epoch_name": epoch_name, "epoch_broken": epoch_broken}
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
        return {"floor": int(default_floor), "acked": [],
                "epoch_name": None, "epoch_broken": False}


def _collapse_confirmed(floor: int, acked: set[int]) -> tuple[int, list[int]]:
    """Collapse the contiguous run of acked ids immediately above `floor` INTO
    `floor`. Isolated acks above a gap stay in `acked`. THE per-event invariant:
    the floor only advances over a CONTIGUOUS confirmed run.

    Returns (new_floor, sorted_remaining_acked).
    """
    # Drop anything already <= floor.
    acked = {a for a in acked if a > floor}
    while (floor + 1) in acked:
        floor += 1
        acked.discard(floor)
    return floor, sorted(acked)


def _write_confirmed_set_locked(sid: str, new_ids: list[int]) -> dict:
    """Add `new_ids` to the sid's confirmed-set under the sentinel lock +
    atomic write, collapsing the contiguous run into `floor`. Returns the
    resulting {"floor", "acked"} (best-effort; on lock-failure returns the
    current on-disk state without writing).

    Shared with shared.py:do_receive — both writers MUST take this lock.

    FIX 3 (cross-family review): when the confirmed-set does not yet exist, the
    floor is seeded from the legacy plain cursor (`_read_legacy_cursor_floor`)
    so a migrated session inherits its plain-cursor high-water as its floor at
    create time — preventing a floor=0 set from being created here before the
    hook ever runs (which would re-deliver all old history). The seed is read
    INSIDE the lock so it reflects the same legacy-cursor value the hook sees.
    """
    cdir = _confirmed_dir()
    try:
        cdir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        _eprint(f"local-relay-channel: confirmed-set mkdir failed: "
                f"{type(e).__name__}: {e}")
        return _read_confirmed_set(sid, _legacy_floor_for(sid))

    lock_path = _confirmed_lock_path(sid)
    try:
        lock_path.touch(exist_ok=True)
    except OSError:
        pass
    try:
        with open(lock_path, "r+", encoding="utf-8") as lock_fh:
            locked = _acquire_confirmed_lock(lock_fh)
            if not locked:
                _eprint(
                    f"local-relay-channel: could not lock confirmed-set for "
                    f"sid={sid[:8]}…; skipping ack write (next hook fire will "
                    f"reconcile via transcript-grep)"
                )
                return _read_confirmed_set(sid, _legacy_floor_for(sid))
            try:
                # FIX 3: seed the create-floor from the legacy plain cursor under
                # the lock (matches shared.py:do_receive's default_floor=plain_cursor)
                # -- but ONLY when the key IS the session id. Under a NAME key the
                # legacy sid-cursor is not this store's history and seeding from it
                # would install an unproven floor. See _legacy_floor_for.
                default_floor = _legacy_floor_for(sid)
                cur = _read_confirmed_set(sid, default_floor)
                floor = cur["floor"]
                acked = set(cur["acked"])
                for nid in new_ids:
                    if isinstance(nid, int) and not isinstance(nid, bool) and nid > 0:
                        if nid > floor:
                            acked.add(nid)
                floor, acked_list = _collapse_confirmed(floor, acked)
                # P2 (subagent-safe): PRESERVE the hook-written awaiting_ack
                # allow-list, minus any id we just confirmed (now <= floor or in
                # acked) and minus ids <= floor. None (absent) STAYS None — only
                # the HOOK (shared.py) creates the key, keeping the "has the
                # no-loss authority spoken?" signal hook-owned. cur was read under
                # THIS lock (above).
                aw_prev = cur.get("awaiting_ack")
                if isinstance(aw_prev, list):
                    confirmed_now = set(acked_list)
                    awaiting_out = sorted(
                        a for a in aw_prev
                        if a > floor and a not in confirmed_now
                    )
                else:
                    awaiting_out = None
                # epoch tag (OQ-RELAY-6 / D78): PRESERVE verbatim from `cur` (read
                # under THIS lock, above). The broker_ack fold NEVER accrues the
                # floor under a new identity, so it must not stamp/alter the epoch —
                # only shared.py:_advance_floor_locked does. Omit-when-default
                # (epoch_name None / epoch_broken False) mirrors awaiting_ack's
                # convention; both channels serialize IDENTICALLY.
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
                if awaiting_out is not None:
                    payload["awaiting_ack"] = awaiting_out
                p = _confirmed_path(sid)
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
        _eprint(f"local-relay-channel: confirmed-set write failed for "
                f"sid={sid[:8]}…: {type(e).__name__}: {e}")
        return _read_confirmed_set(sid)


def _gate_broker_ack(dkey: str,
                     clean_ids: list[int],
                     diag: "dict | None" = None,
                     ) -> tuple[list[int], list[int], list[int]]:
    """P2 (subagent-safe): partition broker_ack's clean_ids into THREE lists --
    (foldable, already, refused). The middle one is explained by the "THREE
    outcomes, not two" note in the body below.

    The return stays a three-tuple for all callers. Optional `diag` receives
    delivery_key, floor and allowlist (present/absent) without changing the
    partitions or the behavior of callers that do not request diagnostics.
    Fold ONLY ids the HOOK injected-to-MAIN -- the `awaiting_ack`
    allow-list in the confirmed-set (written by shared.py:do_receive). Rationale:
      - a push-rendered-in-MAIN id is confirmed by the hook's transcript scan,
        not this allow-list. A refusal here does not undo that local receipt,
        but the broker row remains outstanding until a mirror carries it;
      - a hook-injected-to-MAIN id IS in awaiting_ack — broker_ack is its only
        confirmation signal — so it folds;
      - a leaked-to-SUBAGENT push id was NEVER hook-injected → NOT in awaiting_ack
        → a (subagent) ack of it is REFUSED → the hook re-delivers it to MAIN → no
        silent loss.
    The gate keys on the allow-list, NOT the caller identity, because an MCP tool
    call carries no subagent/session marker (the server cannot tell a subagent
    from the orchestrator). `dkey` is this session's DELIVERY KEY -- its friendly
    name, or its session_id when nameless -- and a subagent shares it, which is
    exactly why the allow-list, not who-called, is the gate.

    TRANSITION / back-compat: `awaiting_ack` ABSENT (None) means a pre-upgrade
    file OR a session whose hook has not fired since upgrade — the allow-list is
    unknown. We FAIL-CLOSED (refuse all) rather than permissively fold: a
    permissive absent-fallback is itself a silent-loss window (a subagent could
    fold a never-main-delivered id before / in the TOCTOU gap around the hook's
    first awaiting_ack write — cross-family GAN iter-2/iter-3). Refusing is
    loss-free — "refuse until the next hook fire", NOT "refuse forever": the hook
    writes awaiting_ack on its very next fire, an unconfirmed hook-injected id
    re-delivers (bounded dup), and push-rendered ids are grep-confirmed regardless.
    """
    # D130: `dkey` is the DELIVERY KEY (name, or sid when nameless) -- the same
    # key `shared.py::do_receive` files the confirmed-set under. Reading it by
    # sid is what made this gate fail-closed forever (D133).
    #
    # The legacy plain cursor is SID-scoped (`cursors/<sid>.json`), so it is a
    # legitimate default ONLY for the sid-keyed store. Passing it under a NAME
    # key would install a floor of unproved provenance -- the same defeat of the
    # migration guard that `shared.py` calls out at its own `cs_default`.
    cur = _read_confirmed_set(dkey, _legacy_floor_for(dkey))
    awaiting = cur.get("awaiting_ack")
    # READ THE FLOOR BEFORE THE FAIL-CLOSED RETURN, NOT AFTER IT. These three
    # lines used to sit BELOW the `awaiting is None` arm, so on the fail-closed
    # path an id at or below this session's OWN high-water mark -- an id it had
    # already confirmed here -- came back labelled "not in this session's
    # allow-list". That is not vague, it is wrong, and it was wrong for exactly
    # the population that needs a truthful answer most.
    #
    # THIS DOES NOT WIDEN THE FOLD. `foldable` is still EMPTY on that path: an
    # at-or-below-floor id has nothing left to fold, and no id absent from the
    # allow-list becomes foldable here or anywhere below. The only thing that
    # moves is which of the two NON-folding buckets such an id lands in, and
    # therefore what the caller is TOLD about it.
    floor = cur.get("floor", 0) or 0
    confirmed = set(cur.get("acked", []) or [])
    if diag is not None:
        # Written BEFORE the early return so the caller can name the mailbox it
        # read on BOTH refusal paths. A refusal that cannot say which mailbox it
        # opened is the thing that made a permanent fault look like a race.
        diag["delivery_key"] = dkey
        diag["floor"] = floor
        diag["allowlist"] = "absent" if awaiting is None else "present"
    if awaiting is None:
        # Absent allow-list (pre-upgrade file / hook has not fired since upgrade):
        # the no-loss authority (the hook) has NOT yet declared what it delivered
        # into MAIN, so we cannot safely fold anything. FAIL-CLOSED — REFUSE all.
        # (cross-family GAN iter-2/iter-3: a permissive absent-fallback is a
        # silent-loss window — a subagent could fold a never-main-delivered id
        # before, or in the TOCTOU gap around, the hook's first write.) Refusing is
        # LOSS-FREE: a hook-injected id simply re-delivers on the hook's next fire
        # (a bounded dup), push-rendered ids are auto-confirmed by the render-grep
        # regardless, and the hook writes awaiting_ack on its very next fire
        # (SessionStart/PostToolUse/…), after which legitimate acks fold normally.
        #
        # "REFUSE all" now means "refuse everything there was still something to
        # refuse ABOUT". An id at or below the floor, or already in this session's
        # explicit `acked` list, was confirmed here earlier by the render-grep;
        # nothing was denied it and nothing can be folded for it, so it is
        # RECONCILABLE exactly as it is on the allow-list-present path below --
        # same bucket, same reason code. The two paths now agree about the one
        # case where the truthful answer does not depend on the allow-list at all.
        #
        # NOT THE SAME BROKER POST, AND THIS SENTENCE USED TO SAY IT WAS. The
        # caller does NOT post the `already` ids it gets from THIS path. That is
        # deliberate and measured, not an oversight: HEAD posted nothing here
        # (this arm returned `([], [], clean_ids)`), and on this path the floor
        # can be seeded for a nameless session from the legacy sid-scoped plain
        # cursor -- provenance that proves sid-addressed receipt, not that
        # anything addressed to this mailbox arrived. Posting on that basis would
        # close delivery rows that may never have been delivered. This arm may
        # change what the caller is TOLD; it may not change what is WRITTEN or
        # POSTED. See the `post_ids` comment in `call_tool`.
        already, refused = [], []
        for i in clean_ids:
            if i <= floor or i in confirmed:
                already.append(i)
            else:
                refused.append(i)
        _eprint(
            f"local-relay-channel: broker_ack awaiting_ack ABSENT for "
            f"key={dkey}; REFUSE {sorted(set(refused))} until the hook "
            f"records the allow-list (next fire) — ids re-deliver if still pending"
            + (f"; RECONCILING {sorted(set(already))} already confirmed here "
               f"(<= floor {floor} or in acked)" if already else "")
        )
        return [], already, refused
    allow = set(awaiting)
    # THREE outcomes, not two. An id at-or-below THIS session's own floor (or
    # already in its own `acked` list) was ALREADY confirmed here: the render-grep
    # folded it when the message rendered, and _read_confirmed_set then pruned it
    # out of awaiting_ack via `v > floor`. It is therefore absent from `allow` and
    # used to land in `refused` -- reporting a denial for the one case where the
    # only truthful answer is "already done".
    # It is NOT foldable (nothing is left to fold) and NOT refused (nothing was
    # denied). It is RECONCILABLE: the caller posts it to the broker so a delivery
    # row that never learned of the local fold can be closed.
    # WHY THIS CANNOT OVER-MARK, which is the only property that matters here: the
    # floor is advanced solely by the gap-aware sweep, which stops BELOW any
    # unconfirmed id, so `i <= floor` is a fact this session established about
    # itself -- not an inference about another identity and not a range borrowed
    # from a predecessor. D57's subagent guard is not weakened either: a subagent
    # acking such an id causes no state change, because the fold it would have
    # produced has already happened.
    # NOT MEASURED, stated so it is not mistaken for proven: this changes what the
    # CALLER is told (`already` ids now report as acked). No caller is known to
    # branch on the refused list, but "no caller branches on it" is a claim about
    # code I do not own and have not enumerated.
    foldable, already, refused = [], [], []
    for i in clean_ids:
        if i in allow:
            foldable.append(i)
        elif i <= floor or i in confirmed:
            already.append(i)
        else:
            refused.append(i)
    if refused:
        _eprint(
            f"local-relay-channel: broker_ack REFUSED {sorted(set(refused))} for "
            f"key={dkey} (not in awaiting_ack={sorted(allow)}, not <= floor "
            f"{floor}) — likely a subagent ack of an orchestrator-owned message; "
            f"the receive hook will re-deliver it to the main context"
        )
    if already:
        _eprint(
            f"local-relay-channel: broker_ack RECONCILING {sorted(set(already))} for "
            f"key={dkey} (already confirmed locally: <= floor {floor} or in "
            f"acked) — posting to the broker so the delivery row can close"
        )
    return foldable, already, refused


# --- Registry lookup (optional display_name in meta) -------------------------
def _registry_lookup(sid: str) -> str | None:
    """Best-effort: return friendly display_name for a session_id, or None."""
    try:
        p = Path.home() / ".claude" / "relay" / "registry.json"
        with open(p, "r", encoding="utf-8") as f:
            reg = json.load(f)
        entry = reg.get(sid)
        if isinstance(entry, dict):
            name = entry.get("name")
            if isinstance(name, str) and name:
                # FOLDED (C-48), sender side. This value is surfaced to the model
                # as `from_name` on a legacy UUID-addressed message, and the
                # model is instructed to reply by addressing it — so it is an
                # ADDRESS handed to a consumer, not decoration.
                return _normalize_name(name)
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError,
            AttributeError, ValueError):
        # AttributeError: a registry whose top level is a list/string/number/null
        # makes `.get` raise. ValueError: UnicodeDecodeError on a non-UTF-8 file.
        # Same superset `_registry_names` below already uses. Caught by my own
        # poison test AFTER the sibling resolvers were fixed — this reader had
        # been left behind, which is why the test enumerates every resolver
        # rather than the one that was just edited.
        pass
    return None


def _broker_relay_name_status(to: str) -> bool | None:
    """Return broker-confirmed registration, or None when authority is unavailable."""
    client = globals().get("broker_client")
    if client is None:
        return None
    result = client._get("/api/relay-name/" + quote(to, safe=""))
    if not isinstance(result, dict):
        return None
    if result.get("relay_name") != to or not isinstance(
            result.get("registered"), bool):
        return None
    return result["registered"]


def _resolve_relay_recipient(to: str) -> tuple[str | None, bool]:
    """Resolve locally first, then ask the broker-owned relay-name authority.

    The boolean reports whether an authority verified the result. Broker
    unavailability retains the existing fail-open send behavior explicitly.
    """
    try:
        with open(Path.home() / ".claude" / "relay" / "registry.json", "r",
                  encoding="utf-8") as f:
            reg = json.load(f)
        entry = reg.get(to) if isinstance(reg, dict) else None
    except (OSError, ValueError, TypeError):
        reg = None
        entry = None
    if isinstance(entry, dict):
        return to, True
    if isinstance(reg, dict):
        for e in reg.values():
            if isinstance(e, dict) and _normalize_name(e.get("name")) == to:
                return to, True

    registered = _broker_relay_name_status(to)
    if registered is True:
        return to, True
    if registered is False:
        return None, True
    return to, False


def _registry_names() -> list[str]:
    """Every friendly_name claimed in the registry, sorted. NOT liveness-filtered --
    this file has no liveness probe -- so ended and test sessions appear too. It is a
    SPELLING AID for the refusal message, NOT a roster of reachable peers. Same
    ValueError-superset rationale as _resolve_relay_recipient above.
    """
    try:
        with open(Path.home() / ".claude" / "relay" / "registry.json", "r",
                  encoding="utf-8") as f:
            reg = json.load(f)
        # FOLDED (C-48). This list is handed to the MODEL as the addresses to
        # retry with after a refusal, so it must quote names in the form the
        # comparison above actually accepts. Folding also de-duplicates
        # case-variant rows into the ONE mailbox they now denote, which is the
        # honest count rather than a cosmetic one.
        return sorted({_normalize_name(e["name"]) for e in reg.values()
                       if isinstance(e, dict) and isinstance(e.get("name"), str)
                       and e["name"]})
    except (OSError, ValueError, AttributeError):
        return []

# --- Body unwrap + timestamp formatting (DISPLAY-ONLY; storage untouched) -----
# SYNC: keep identical to hooks/relay/shared.py:_extract_text
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


def _format_time_sent(ts_raw: Any) -> str:
    """Epoch-ms → human-readable local time, matching the receive-hook's exact
    format (shared.py:_format_ts_local → local time, "%Y-%m-%d %H:%M:%S") so the
    channel, receive-hook, and relay-check show identical timestamps for the
    same event. Fail-safe: on ANY failure return str(ts_raw); never raises.
    """
    try:
        return datetime.fromtimestamp(int(ts_raw) / 1000.0).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except Exception:
        return str(ts_raw)


# --- Filter constants (per ~/.claude/hooks/relay/shared.py:505) --------------
# INTER_SESSION_MESSAGE_KINDS — copied verbatim, NOT imported, to keep this
# MCP server standalone-runnable outside the relay-hooks path.
INTER_SESSION_MESSAGE_KINDS = ("agent_message", "agent_reply", "handoff")


# --- MCP server construction --------------------------------------------------
# Imports gated under __main__ so a `import server` smoke (per Acceptance
# Criteria item #2) doesn't pay the dep-import cost (and so this file is
# importable even if mcp/httpx temporarily absent).
def _build_and_run() -> None:
    """Construct MCP server, register handlers, and run the stdio loop."""
    import anyio
    import httpx
    import httpx_sse
    from mcp.server.lowlevel import Server, NotificationOptions
    from mcp.server.stdio import stdio_server

    # Initialize the env-required client from the same selected URL as SSE.
    # Existing environment configuration wins; malformed installed files raise.
    os.environ.setdefault("BROKER_BASE_URL", _broker_base_url())
    from mcp.server.session import ServerSession
    from mcp.shared.message import SessionMessage
    from mcp.types import (
        JSONRPCMessage,
        JSONRPCNotification,
        TextContent,
        Tool,
    )

    global broker_client
    import broker_client  # type: ignore  # ~/.claude/hooks/relay/, else ~/.agents/scripts/

    # WS-B-client (#19): mirror LOCAL acks to the broker delivery ledger so the
    # escalation reaper (#18) can distinguish a processed message from an
    # outstanding one. Fire-and-forget helpers used by call_tool (broker_ack ->
    # post foldable) + the SSE reconnect (outbox drain). Held in _ack_tasks because
    # the event loop keeps only WEAK refs to bare tasks and would GC-cancel them.
    _ack_tasks: set = set()

    def _spawn_bg(coro) -> None:
        """Fire-and-forget a coroutine, GC-safe. Best-effort; never raises."""
        try:
            t = asyncio.create_task(coro)
        except RuntimeError:
            return  # no running loop (shouldn't happen inside the async server)
        _ack_tasks.add(t)
        t.add_done_callback(_ack_tasks.discard)

    def _safe_post_ack(name: str, ids: list) -> None:
        # Default (5-attempt) path; runs on a worker thread => latency irrelevant.
        try:
            broker_client.post_ack(name, ids)
        except Exception as e:
            _eprint(f"local-relay-channel: post_ack(broker_ack) failed: "
                    f"{type(e).__name__}: {e}")

    def _safe_flush_outbox() -> None:
        try:
            broker_client.flush_outbox()
        except Exception as e:
            _eprint(f"local-relay-channel: flush_outbox failed: "
                    f"{type(e).__name__}: {e}")

    my_sid = _resolve_session_id()
    my_name = _routing_name(my_sid)  # Routing identity; None => degraded (push-inactive).
    # _routing_name = _resolve_my_name gated by the delivery-key FILENAME validator,
    # so push never subscribes to a mailbox the receive hook will not poll
    # (round-3 residual). The two resolvers themselves read the same two records,
    # so no further agreement check stands between them.
    broker_url = _broker_base_url()
    # Cold-start-race fix: cadence for re-resolving a not-yet-set friendly_name
    # while DEGRADED, then activating push. Only runs while nameless; stops once
    # resolved. Env-tunable; sane default 3s; guards against bad values.
    try:
        _degraded_poll_s = float(os.environ.get("RELAY_DEGRADED_NAME_POLL_S", "3"))
        if _degraded_poll_s <= 0:
            _degraded_poll_s = 3.0
    except (TypeError, ValueError):
        _degraded_poll_s = 3.0

    if my_name is None:
        # Forensic context for diagnosing a stuck-degraded session in the field
        # (C-17 instrument-before-fix): capture what the name sources saw so a
        # degraded case is explainable from the log alone.
        #
        # `.relay-name.json` IS NO LONGER A NAME SOURCE and the line below says
        # so, because a reader who saw the old wording would conclude a name was
        # available and not used. It is still worth printing: the file is where
        # /relay-name recorded the name the user last claimed here, so its
        # presence beside a nameless registry row is the signature of a row that
        # was removed or demoted -- the state whose remedy is to re-claim.
        try:
            _reg_p = Path.home() / ".claude" / "relay" / "registry.json"
            _reg = json.loads(_reg_p.read_text(encoding="utf-8")) if _reg_p.exists() else {}
            _row = _reg.get(my_sid) if isinstance(_reg, dict) else None
            _ws_diag = (_row or {}).get("workspace") if isinstance(_row, dict) else None
        except Exception:
            _row, _ws_diag = None, None
        try:
            _cwd_diag = os.getcwd()
        except Exception:
            _cwd_diag = None
        try:
            _ws_has = bool(
                isinstance(_ws_diag, str) and _ws_diag
                and (Path(_ws_diag) / ".relay-name.json").exists()
            )
            _cwd_has = bool(
                isinstance(_cwd_diag, str) and _cwd_diag
                and (Path(_cwd_diag) / ".relay-name.json").exists()
            )
            _eprint(
                f"local-relay-channel: DEGRADED diag sid={my_sid[:8]}… "
                f"registry_row={'present' if _row else 'ABSENT'} "
                f"registry_workspace={_ws_diag!r} cwd={_cwd_diag!r} "
                f"(not a name source) .relay-name.json(ws)="
                f"{'yes' if _ws_has else 'no'} "
                f".relay-name.json(cwd)={'yes' if _cwd_has else 'no'}"
            )
        except Exception:
            pass  # diagnostics must NEVER crash the degraded path (C-17)
        # DEGRADED mode (background-agents daemon fix): no user-set name resolved
        # from the relay registry OR the CC daemon job-state. Do NOT exit — that
        # was the old fail-loud behavior that killed the MCP server for every
        # daemon-spawned worker (fresh unnamed sids). Stay alive so CC reports
        # "connected" (no Connection-failed storm); skip the broker SSE push
        # subscription below (no routing identity to filter on). The receive-hook
        # backup path still delivers via registry/job-state polling.
        _eprint(
            f"local-relay-channel: session {my_sid[:8]}… has no user-set "
            f"friendly_name (checked relay registry + daemon job-state). "
            f"Starting DEGRADED: MCP handshake up, channel PUSH inactive until a "
            f"name is set. Will AUTO-ACTIVATE push within ~{_degraded_poll_s}s of "
            f"a name being set (/relay-name or SessionStart inherit) — NO "
            f"/mcp reconnect needed. Background agents inherit their Agent-View "
            f"name automatically once assigned."
        )
    else:
        _eprint(
            f"local-relay-channel: starting; session={my_sid[:8]}… "
            f"name={my_name!r}, broker={broker_url}"
        )

    # Iter-2 fix L5 (cap-5): clear pending-mcp-reconnect marker on successful
    # MCP startup. set_name.py wrote the marker; the operator ran /mcp reconnect
    # which respawned this subprocess; we successfully resolved a friendly_name
    # → the rename took effect. Marker should stop firing the advisory now even
    # if no SessionStart fires (which is the L5 case: /mcp reconnect alone does
    # NOT fire SessionStart). Silent-degrade on file-system errors.
    try:
        marker = Path.home() / ".claude" / "relay" / "pending-mcp-reconnect" / f"{my_sid}.flag"
        marker.unlink(missing_ok=True)
    except Exception:
        pass

    # ----- Custom-method notification emit (per R3.3) ------------------------
    async def emit_channel_notification(
        session: ServerSession, content: str, meta: dict[str, str]
    ) -> None:
        """Emit a raw `notifications/claude/channel` JSON-RPC notification.

        Bypasses ServerSession.send_notification (typed against the known
        ServerNotification union; rejects unknown methods). Wire shape per
        Discord plugin server.ts:868-884 + verified via model_dump_json:
            {"method":"notifications/claude/channel",
             "params":{"content":"...","meta":{...}},
             "jsonrpc":"2.0"}
        """
        # JSONRPCNotification accepts params: dict[str, Any] | None per
        # mcp.types (verified inline). Pydantic-serializes to the wire shape
        # above.
        notif = JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/claude/channel",
            params={"content": content, "meta": meta},
        )
        # JSONRPCMessage is a RootModel union — positional construction routes
        # to the matching variant (verified inline).
        msg = SessionMessage(message=JSONRPCMessage(notif))
        await session._write_stream.send(msg)

    # ----- SSE subscriber loop (per R3.4 + R3.5) -----------------------------
    # Exponential backoff: mirrors broker_client.RETRY_BACKOFF_MS shape per
    # spec (50/100/200/400/800 then cap at 800). After 5 failures: WARN once
    # but continue retrying at 800ms cap (MCP subprocess lifetime = CC session
    # lifetime; broker outages must not kill the channel — C-17 root-cause:
    # broker outage is transient, channel teardown is irrecoverable).
    BACKOFF_MS = (50, 100, 200, 400, 800)
    BACKOFF_CAP_MS = 800

    async def get_initial_cursor() -> int:
        """Resolve starting cursor:
           - SSE-RESUME cursor file if present (resume; advances on every event)
           - else SQLITE_I64_MAX (M2 fix per discriminator iter-1; D42 default-tail)
        """
        # Iter-3 H3: read the SSE-RESUME cursor (advances on every event seen
        # on wire) — NOT the DELIVERED cursor (which only advances on emit
        # success). Resume cursor is the correct floor for SSE Last-Event-ID
        # so we don't re-receive events we already pulled.
        persisted = _read_resume_cursor(my_name)
        if persisted is not None:
            _eprint(f"local-relay-channel: resuming from cursor={persisted}")
            return persisted
        # First-start path: prime to SQLITE_I64_MAX so broker's SSE replay
        # query `WHERE event_id > cursor` returns ZERO rows; subscribe-live
        # then delivers only new events from this moment forward (D42 default-
        # tail discipline; matches contract `?cursor=` validation `le=SQLITE_I64_MAX`).
        # M2 supersedes the iter-0 attempt that queried /api/state.next_cursor,
        # which is an opaque agent-pagination cursor (string `f"{registered_at}:{id}"`)
        # not an event_id; isinstance(nc,int) failed silently → replay-from-0
        # → inception dump. Zero extra HTTP call; no race window.
        SQLITE_I64_MAX = 9223372036854775807  # matches broker/server.py:75
        _eprint(f"local-relay-channel: first start; priming "
                f"cursor={SQLITE_I64_MAX} (SQLITE_I64_MAX; live-only)")
        return SQLITE_I64_MAX

    async def sse_subscribe_loop(session: ServerSession) -> None:
        """Long-lived task: subscribe to broker SSE, filter, emit notifications.

        Per spec R3.4: GET {broker}/api/events with Last-Event-ID header for
        resume. Client-side filter (broker has no to_agent SSE filter today).
        On drop: exponential backoff per R3.5; never exits.
        """
        last_event_id = await get_initial_cursor()
        consecutive_failures = 0
        attempt_idx = 0

        _eprint(
            f"local-relay-channel: subscribing to broker SSE at "
            f"{broker_url}/api/events for name={my_name!r}"
        )

        while True:
            try:
                # Iter-3 H3 fix: persist SSE-RESUME cursor (NOT delivered cursor)
                # BEFORE each connect so an outage doesn't lose resume progress
                # (per R3.5). The DELIVERED cursor is unaffected here — it only
                # advances on emit success per per-event branch below.
                _write_resume_cursor(my_name, last_event_id)

                headers = {"Last-Event-ID": str(last_event_id)}
                # SSE is long-lived but NOT silent-tolerant: Windows TCP
                # keepalive defaults to 2h, so a silently-dead peer (NIC
                # sleep/wake, broker process accept-queue wedge per F-CC22,
                # VPN reconnect, etc.) leaves us stuck in aiter_sse() forever.
                # Broker emits sse_starlette ping every 15s (DEFAULT_PING_INTERVAL)
                # + broker-level heartbeat event every 15s (interleaved at
                # ~7-8s effective cadence) → bytes-on-wire guaranteed at ≤15s
                # in steady state. Set read=30.0s (2× heartbeat) so silent
                # dead-peer raises httpx.ReadTimeout within 30s → caught
                # below → backoff + reconnect. NEVER false-positives in steady
                # state. A read deadline also catches a silent dead connection
                # that would otherwise leave the subscription idle indefinitely.
                timeout = httpx.Timeout(connect=5.0, read=30.0,
                                        write=5.0, pool=5.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with httpx_sse.aconnect_sse(
                        client, "GET", f"{broker_url}/api/events",
                        headers=headers,
                    ) as event_source:
                        # Successful connect → reset backoff.
                        # Log per-(re)connect so silent wedges leave forensics
                        # (mirrors F-CC21 / D40 broker observability — instrument
                        # before next diagnosis per C-17). Logs ALL connects
                        # including the first one for symmetry (compact line).
                        if consecutive_failures > 0:
                            _eprint(
                                f"local-relay-channel: SSE reconnected after "
                                f"{consecutive_failures} failure(s); "
                                f"last_event_id={last_event_id}"
                            )
                        else:
                            _eprint(
                                f"local-relay-channel: SSE connected; "
                                f"last_event_id={last_event_id}"
                            )
                        consecutive_failures = 0
                        attempt_idx = 0
                        # WS-B-client (#19): broker reachable again — replay any
                        # outboxed acks (+ other outboxed events) stranded during
                        # the outage. flush_outbox is sync urllib -> worker thread;
                        # cheap no-op when the outbox is empty. First live caller.
                        _spawn_bg(asyncio.to_thread(_safe_flush_outbox))
                        async for sse in event_source.aiter_sse():
                            if sse.event == "heartbeat":
                                continue
                            if not sse.data:
                                continue
                            try:
                                payload = json.loads(sse.data)
                            except json.JSONDecodeError as e:
                                _eprint(f"local-relay-channel: SSE event JSON "
                                        f"decode failed: {e}")
                                continue

                            # Advance in-memory cursor opportunistically (covers
                            # events we filter out so we don't re-receive them on
                            # reconnect). M2-R1 fix (discriminator iter-2):
                            # when cursor is the SQLITE_I64_MAX first-start
                            # sentinel, ANY real event_id is smaller — so the >
                            # guard would never fire and the in-memory cursor
                            # would stay stuck at max. Special-case: if currently
                            # at sentinel, take the first real event_id
                            # unconditionally.
                            evt_id_str = sse.id
                            evt_id_int: int | None = None
                            if evt_id_str:
                                try:
                                    evt_id_int = int(evt_id_str)
                                    if last_event_id >= 9223372036854775807:
                                        last_event_id = evt_id_int  # M2-R1
                                    elif evt_id_int > last_event_id:
                                        last_event_id = evt_id_int
                                except ValueError:
                                    pass

                            # Iter-2 fix H3 + iter-3 H3 reopen (cap-5):
                            # _maybe_emit returns 3-value status ("delivered",
                            # "skipped", "failed"). Iter-3 splits the cursors:
                            #   - DELIVERED cursor (channel-cursors/{name}.json)
                            #     advances ONLY on emit_status == "delivered".
                            #     Receive-hook reads this as dedup floor; a
                            #     failed/skipped event MUST NOT poison it or
                            #     backup-path skips it forever (silent loss).
                            #   - RESUME cursor (channel-resume/{name}.json)
                            #     advances on every event seen on wire — used
                            #     as Last-Event-ID on reconnect so we don't
                            #     re-receive events we already pulled.
                            emit_status = await _maybe_emit(session, payload, evt_id_str)

                            # Always persist RESUME cursor so reconnect doesn't
                            # re-pull events we've already seen.
                            if evt_id_int is not None:
                                _write_resume_cursor(my_name, evt_id_int)
                            # LOSSLESS-DELIVERY refactor (plan quizzical-launching-fox,
                            # Phase 1 / change A1): push no longer writes a
                            # suppression (DELIVERED) cursor. A transport-write
                            # "delivered" is NOT a render ACK (CC gives none), so
                            # advancing a name-keyed suppression floor here let the
                            # receive-hook skip a push-missed event forever (silent
                            # loss class, repro: 2950/2962). Authority now lives in
                            # the receive-hook's per-sid confirmed-set +
                            # transcript-grep backstop (shared.py:do_receive) +
                            # the broker_ack tool. We KEEP the RESUME cursor write
                            # above (still needed for SSE reconnect Last-Event-ID).

            except httpx.ReadTimeout:
                # The PRIMARY signal that the broker connection silently went
                # dead-peer. broker steady-state emits bytes ≤15s cadence; a
                # 30s read timeout means the wire actually went silent — NOT a
                # transient. Log distinctly for forensics, then fall through to
                # backoff (counted as a failure so the backoff schedule kicks
                # in normally).
                consecutive_failures += 1
                _eprint(
                    f"local-relay-channel: SSE read-timeout (30s) — silent "
                    f"dead-peer detected at {broker_url}/api/events; "
                    f"reconnecting (last_event_id={last_event_id}, "
                    f"failure#={consecutive_failures})"
                )
                if attempt_idx < len(BACKOFF_MS):
                    delay_ms = BACKOFF_MS[attempt_idx]
                else:
                    delay_ms = BACKOFF_CAP_MS
                attempt_idx += 1
                await asyncio.sleep(delay_ms / 1000.0)
                continue
            except (httpx.HTTPError, httpx_sse.SSEError, ConnectionError,
                    asyncio.TimeoutError, OSError) as e:
                consecutive_failures += 1
                # Pick backoff: BACKOFF_MS by index, then cap.
                if attempt_idx < len(BACKOFF_MS):
                    delay_ms = BACKOFF_MS[attempt_idx]
                else:
                    delay_ms = BACKOFF_CAP_MS
                attempt_idx += 1

                if consecutive_failures == 5:
                    # WARN after 5 consecutive failures; continue forever.
                    _eprint(
                        f"local-relay-channel: SSE reconnect failed "
                        f"{consecutive_failures}× to {broker_url}/api/events "
                        f"({type(e).__name__}: {e}); continuing at "
                        f"{BACKOFF_CAP_MS}ms cap. MCP subprocess stays alive "
                        f"per C-17 (broker outage must not kill the channel)."
                    )

                await asyncio.sleep(delay_ms / 1000.0)

    async def _maybe_emit(session: ServerSession, payload: dict,
                           evt_id_str: str | None) -> str:
        """Apply client-side filter; emit notification if event survives.

        Returns one of:
          - "delivered" — notification was successfully emitted via the channel
          - "skipped"   — event filtered out (wrong kind or not addressed to us)
          - "failed"    — emit raised an exception (logged but not propagated)

        Iter-2 fix H3 (cap-5): the return value gates the on-disk DEDUP CURSOR
        write at the caller. Pre-fix, the caller advanced the cursor regardless
        of emit outcome, causing receive-hook's backup-path dedup to skip events
        that channel push failed to deliver (silent loss). Only "delivered"
        advances the dedup cursor; "skipped" + "failed" leave it untouched so
        the receive-hook backup path can re-fetch + inject.

        SSE payload shape per broker/server.py:595-601:
            {"ts": int_ms, "kind": str, "agent": from_agent,
             "data": {"to_agent":..., "content":..., "channel":..., "thread_id":...}}
        """
        kind = payload.get("kind")
        if kind not in INTER_SESSION_MESSAGE_KINDS:
            return "skipped"
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return "skipped"
        # Filter by friendly_name (post-ACTIVE PLAN). Senders POST with
        # to_agent=<friendly_name> via send.py; broker stores opaquely; we
        # match here. session_id-based filtering eliminated to fix the F-CC30
        # session-id-drift wedge class.
        if data.get("to_agent") != my_name:
            return "skipped"

        from_agent = payload.get("agent") or "unknown"
        to_agent = data.get("to_agent") or ""
        ts_raw = payload.get("ts", "")
        time_sent = _format_time_sent(ts_raw)   # human-readable; matches receive-hook
        content = data.get("content") or ""
        if not isinstance(content, str):
            # broker normally stores content as str; defensively coerce.
            content = json.dumps(content, ensure_ascii=False)
        # Unwrap the broker's {"text":...} envelope for DISPLAY ONLY (storage
        # untouched). Reuses the receive-hook's exact _extract_text semantics so
        # channel display == hook display; tolerates relay-send envelopes,
        # bare-string replies, and non-envelope JSON via safe fallback (never raises).
        body = _extract_text(content)

        # Meta keys must be identifiers (letters/digits/underscore, NO hyphens —
        # silently dropped per S1). Each becomes a <channel> attribute (CC y68).
        meta: dict[str, str] = {
            "from": str(from_agent),
            "to": str(to_agent),
            "kind": str(kind),
            "ts": str(ts_raw),
            "time_sent": time_sent,
            "channel": str(data.get("channel") or ""),
        }
        if evt_id_str:
            meta["event_id"] = str(evt_id_str)

        # Legacy compat: UUID-shaped from_agent (old in-flight message) → surface
        # the friendly name as from_name for the recipient's benefit.
        if _UUID_RE.match(str(from_agent)):
            friendly = _registry_lookup(str(from_agent))
            if friendly:
                meta["from_name"] = friendly

        # Robust-visibility header prepended to the body: survives the DEGRADED
        # render (CC R68 gate skip → plain `server: {json}`, no attributes).
        # ASCII-only single line, separated from the body by a blank line so
        # multi-line / special-char / JSON bodies stay intact and clearly distinct.
        header_from = meta.get("from_name") or meta["from"]
        # Per-message ack directive carrying the actual event id (~10 tokens):
        # tells the model to call broker_ack([id]) to CONFIRM RECEIPT (local
        # cursor advance; delivery is deterministic — an un-acked message
        # re-delivers until acked). This is NOT a reply: a response the peer
        # should see is a separate peer_reply.
        evt_id_for_ack = meta.get("event_id")
        ack_directive = (
            f"  ack=broker_ack([{evt_id_for_ack}])" if evt_id_for_ack else ""
        )
        header = (
            f"[relay] from={header_from} to={meta['to']} "
            f"event_id={meta.get('event_id', '?')} sent={time_sent} "
            f"kind={meta['kind']}{ack_directive}"
            f" · subagents: don't act/ack, surface to orchestrator"
        )
        display = f"{header}\n\n{body}"

        try:
            await emit_channel_notification(session, display, meta)
            # LOSSLESS-DELIVERY refactor (Phase 4 / A5): drop-observability —
            # the forensics this incident exposed as missing. One compact line
            # per emit so a later silent-loss diagnosis has name + event_id +
            # status to grep (mirrors the SSE-connect logging discipline).
            _eprint(
                f"local-relay-channel: emit name={my_name!r} "
                f"event_id={evt_id_str} status=delivered"
            )
            return "delivered"
        except Exception as e:
            _eprint(f"local-relay-channel: emit notification failed for "
                    f"event_id={evt_id_str}: {type(e).__name__}: {e}")
            _eprint(
                f"local-relay-channel: emit name={my_name!r} "
                f"event_id={evt_id_str} status=failed"
            )
            return "failed"

    # ----- MCP Server + tool registration ------------------------------------
    instructions = (
        "Inbound inter-agent messages arrive as <channel source=\"local-relay\" "
        "...> tags between turns. Each tag's attributes carry: from (sender "
        "friendly_name), to (recipient friendly_name), event_id, kind "
        "(agent_message|agent_reply|handoff), ts (epoch-ms), time_sent "
        "(human-readable), channel, optionally from_name. The body begins with a "
        "`[relay]` header line (from/to/event_id/sent/kind), a blank line, then "
        "the message text — the header restates the metadata so it stays visible "
        "even if the channel render degrades. "
        "TWO DISTINCT TOOLS — do not confuse them. "
        "(1) `broker_ack` = RECEIPT CONFIRMATION — a SEPARATE, DECOUPLED act from "
        "your `peer_reply` response; never couple them. You MUST call `broker_ack` "
        "with the inbound event_ids IMMEDIATELY on seeing an inbound <channel> "
        "message, BEFORE doing any work (e.g. broker_ack(event_ids=[2967, 2969]); "
        "each `[relay]` header shows the exact call as `ack=broker_ack([<id>])`). It "
        "is LOCAL: it advances YOUR receive cursor so the broker stops re-delivering "
        "— it sends NOTHING to the sender, the sender never sees it, and it is NOT "
        "your answer to the peer nor a commitment to anything. Your substantive "
        "`peer_reply` (see (2)) is INDEPENDENT and MAY take several turns — research, "
        "tool calls, a multi-turn investigation — and that is expected and fine; do "
        "NOT withhold the fast receipt-ack while you work out a thoughtful answer. "
        "ACK RECEIPT NOW, respond thoughtfully whenever your work is done. Delivery "
        "is DETERMINISTIC — an un-acked message is RE-DELIVERED every turn until you "
        "broker_ack it, and past a grace window the broker ESCALATES to the human (a "
        "#relay-alerts ping). ACK FIRST TO AVOID RETRY + ESCALATION. "
        "SUBAGENTS: if you are a sub-agent/Task, a relay <channel> message that "
        "reached you is the ORCHESTRATOR's — do NOT act on it and do NOT "
        "broker_ack it (your ack is REFUSED for ids the orchestrator's receive-"
        "hook did not inject); surface it to the orchestrator and let it ack. "
        "(2) `peer_reply` = SEND A MESSAGE. To send a response the peer will "
        "actually receive, call `peer_reply` with to_session_id=<the from "
        "attribute>, content=<your text> (or use the relay-send skill). "
        "broker_ack is NOT a reply: if a peer expects a response you MUST "
        "peer_reply. The two are independent — you often do both (broker_ack to "
        "confirm receipt, peer_reply to respond). "
        "Channel push is ADDITIVE to the existing relay-check / receive-hook "
        "path; both work."
    )

    server = Server(
        name="local-relay-channel",
        version="0.2.0",
        instructions=instructions,
    )

    # Tool surface: `peer_reply(to_session_id, content, channel?)` (send a real
    # message a peer receives) + `broker_ack(event_ids:[int])` (LOCAL receipt
    # confirmation / cursor advance — sends nothing). Old names `reply`/`relay_ack`
    # stay accepted as unadvertised transition aliases in call_tool.
    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="peer_reply",
                description=(
                    "Send an actual reply MESSAGE to a peer: POSTs an agent_reply "
                    "event to the local broker, addressed to the specified peer, "
                    "who receives it as a <channel source=\"local-relay\" ...> tag "
                    "within ~1-2s. Use whenever a peer expects a response or a "
                    "content-level acknowledgement. This is NOT `broker_ack`: "
                    "peer_reply SENDS a message but does not advance your receive "
                    "cursor; broker_ack advances your cursor but sends no message. "
                    "They are independent — you often do both (broker_ack to "
                    "confirm receipt, peer_reply to respond). If `to` matches "
                    "NEITHER a registered session_id NOR a friendly_name RECORDED "
                    "IN THE RELAY REGISTRY, "
                    "NOTHING is sent and the result is {ok:false, error, "
                    "known_names} -- fix the name and retry; do not read it as "
                    "delivered. BEST-EFFORT: while the registry cannot be read the "
                    "send proceeds unchecked, so ok:true is not proof the recipient "
                    "exists."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "to_session_id": {
                            "type": "string",
                            "description": (
                                "Recipient friendly_name (e.g., 'trading_infra'). "
                                "Use the `from` attribute of the inbound channel "
                                "tag, which carries the sender's friendly_name. "
                                "A registered raw session_id is also accepted, "
                                "but PREFER THE NAME: a session_id-keyed message "
                                "is excluded from the push channel (which matches "
                                "on name) and its delivery row can never be acked, "
                                "so it strands outstanding and eventually pages a "
                                "human. "
                                "MUST resolve to one of those two key-spaces AS "
                                "RECORDED IN THE RELAY REGISTRY, or the send is "
                                "REFUSED and nothing is posted. A name held ONLY in "
                                "CC daemon job-state does not resolve here (same as "
                                "send.py, which is registry-only too); "
                                "liveness is NOT required. (Argument name kept as "
                                "'to_session_id' for backward-compat with existing "
                                "model prompts; semantically now a friendly_name "
                                "string.)"
                            ),
                        },
                        "content": {
                            "type": "string",
                            "description": "Reply text.",
                        },
                        "channel": {
                            "type": "string",
                            "description": (
                                "Broker channel (default '#cc-session-relay')."
                            ),
                            "default": "#cc-session-relay",
                        },
                    },
                    "required": ["to_session_id", "content"],
                },
            ),
            Tool(
                name="broker_ack",
                description=(
                    "Confirm you RECEIVED inbound <channel source=\"local-relay\"> "
                    "message(s) so they are not re-delivered. Call this IMMEDIATELY "
                    "on seeing an inbound channel message, BEFORE doing any work — "
                    "with their event_ids (the `[relay]` header of each message shows "
                    "the exact call as `ack=broker_ack([<id>])`). This is LOCAL "
                    "bookkeeping: it advances YOUR receive cursor only; it sends "
                    "NOTHING to the sender and is NOT your reply. Receipt-ack is "
                    "DECOUPLED from your substantive `peer_reply`, which is "
                    "independent and MAY take several turns (research, multi-turn "
                    "work) — do NOT withhold the ack until your answer is ready. "
                    "Delivery is DETERMINISTIC — an un-acked message is RE-DELIVERED "
                    "every turn until you ack it, and past a grace window the broker "
                    "ESCALATES to the human (a #relay-alerts ping). Ack first to "
                    "AVOID RETRY + escalation. If a peer expects a response, ALSO use "
                    "`peer_reply`. SUBAGENTS must NOT broker_ack relay messages — "
                    "they belong to the orchestrator (acks for ids the orchestrator "
                    "did not receive-hook-inject are refused). The result carries a "
                    "per-id `reasons` map, plus `delivery_key` (the mailbox this "
                    "session files receipts into) and `floor` (its high-water mark "
                    "there) so a refusal can be read without running anything else. "
                    "IN_CONFIRMED_SET = this session's "
                    "confirmed-set already covers the id; benign, do nothing. "
                    "NOT_AWAITED = the allow-list WAS READ and does not list this "
                    "id. Per-id. For a HOOK-INJECTED id this is transient and "
                    "benign: the hook records it on its next fire and the id "
                    "re-delivers until it does, so do nothing. FOR A PUSH-RENDERED "
                    "id it is PERMANENT and re-acking will never clear it -- such "
                    "an id is confirmed HERE by the hook's transcript render-grep "
                    "and is NEVER written to the allow-list, so it refuses "
                    "identically every time. Do not retry it. That local "
                    "confirmation reaches the SENDER only through the hook's floor "
                    "mirror, and when the mirror does not carry it the sender's "
                    "delivery row stays outstanding and eventually pages a human. "
                    "So if a peer tells you a message "
                    "you have already seen is still unacknowledged, SAY SO to the "
                    "user instead of re-acking -- that is the one case with no "
                    "in-session remedy. "
                    "ALLOWLIST_UNAVAILABLE = this session's mailbox carried no "
                    "usable allow-list, so every id still OPEN in the call was "
                    "refused together (ids at or below `floor` are reported "
                    "IN_CONFIRMED_SET instead, so a mixed result is normal). "
                    "TRANSIENT and benign, per-call: the receive hook writes the "
                    "allow-list on its next fire. Do nothing; the ids re-deliver "
                    "until then. "
                    "NO_IDENTITY = this session cannot establish who it is; NOTHING "
                    "was recorded and every ack will fail until its session record "
                    "or relay registry is readable again -- always surface this one, "
                    "and see the tool's own error text for the remedy. "
                    "WRITE_FAILED = this session could not confirm its receipt file "
                    "was written; the message re-delivers, so nothing is lost. Every "
                    "code is a LOCAL claim about this session's confirmed-set and "
                    "says nothing about whether the sender's delivery row closed. "
                    "Returns instantly."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_ids": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": (
                                "The event_ids you received this turn (integers). "
                                "Batch all ids seen into one call."
                            ),
                        },
                    },
                    "required": ["event_ids"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name in ("broker_ack", "relay_ack"):  # relay_ack = transition alias (unadvertised)
            # broker_ack: confirm RECEIPT of the given event_ids into THIS
            # session's confirmed-set (keyed by session_id, NOT friendly_name — a
            # session acks for itself; two sids sharing a name keep independent
            # confirmed-sets). LOCAL only — advances this session's receive cursor;
            # sends nothing to the sender. Returns immediately with a tiny ok
            # result; the locked+atomic write is kept fast. Validate input: ints
            # only, skip non-ints, cap list length defensively.
            raw_ids = arguments.get("event_ids")
            if not isinstance(raw_ids, list):
                # THE FULL ACKNOWLEDGEMENT SHAPE, ON THE ARM THAT ANSWERS FIRST.
                # Every other return in this arm carries `acked`, `refused`,
                # `reasons`, `delivery_key` and `floor`; this one carried none of
                # them, so a consumer had to special-case the one response it
                # gets by having sent the wrong thing. The tool description
                # promises a per-id `reasons` map and names the mailbox and the
                # floor, and a description that does not match behaviour is the
                # defect this whole change exists to remove -- leaving a known
                # instance of it inside the fix would be the same error one
                # layer up.
                #
                # EVERY VALUE HERE IS HONEST, NOT A PLACEHOLDER. The call was
                # rejected before any id existed: nothing was acked, nothing was
                # refused, no id has a reason, and `_delivery_key()` was never
                # consulted -- so no mailbox was established and no floor was
                # read. `null` for the latter pair says exactly that, where an
                # omission would leave the caller unable to tell "the server
                # does not report this" from "the server could not determine
                # it". Same values and same reasoning as the NO_IDENTITY return
                # below.
                return [TextContent(
                    type="text",
                    text=json.dumps({"ok": False,
                                     "error": "event_ids must be an array of integers",
                                     "acked": [],
                                     "refused": [],
                                     "reasons": {},
                                     "delivery_key": None,
                                     "floor": None}),
                )]
            clean_ids: list[int] = []
            for v in raw_ids[:_BROKER_ACK_MAX_IDS]:
                # bool is an int subclass; exclude it. Skip non-ints silently.
                if isinstance(v, bool):
                    continue
                if isinstance(v, int) and v > 0:
                    clean_ids.append(v)
            if not clean_ids:
                # Same three CORE keys as the main return below (`acked`,
                # `refused`, `reasons`), so a caller never has to branch on which
                # ok:true shape it got for the answer itself. (These ids were
                # dropped by the sanitiser above and get NO reason code -- a
                # pre-existing gap this change does not close.)
                #
                # EXPLICIT NULLS FOR THE OTHER TWO, AND THIS COMMENT USED TO
                # ARGUE THE OPPOSITE. It read that `delivery_key` and `floor`
                # were "deliberately absent here", on the grounds that naming a
                # mailbox this call never opened would be a claim it did not
                # establish. The premise is right and the conclusion was wrong:
                # `null` names no mailbox. It says there is none -- which is
                # exactly the fact this path established. OMITTING the pair is
                # the option that misleads, because the tool description tells
                # every agent that every result carries both, and a reader who
                # finds nothing cannot tell a server that does not report the
                # field from one that could not determine it. Same reasoning,
                # same values, as the NO_IDENTITY return below.
                return [TextContent(
                    type="text",
                    text=json.dumps({"ok": True, "acked": [], "refused": [],
                                     "reasons": {},
                                     "delivery_key": None,
                                     "floor": None}),
                )]
            # P2 (subagent-safe): fold ONLY ids the HOOK injected-to-MAIN
            # (awaiting_ack). A leaked-to-subagent PUSH id was never hook-injected,
            # so a (subagent) ack of it is REFUSED here -> the hook re-delivers it
            # to MAIN -> no silent loss. The gate keys on the allow-list, NOT the
            # caller, because an MCP tool call carries no subagent/session marker.
            # D130: OPEN THE FILE THE HOOK ACTUALLY WROTE. Both of these used to
            # be keyed by `my_sid`, so the gate read `confirmed/<plugin sid>.json`
            # while the hook wrote `confirmed/<hook sid>.json` -- a file that, for
            # this process, never exists. `awaiting_ack` was therefore always
            # absent, the fail-closed branch fired on every id, and `broker_ack`
            # returned NOT_AWAITED forever. That is the whole user-visible bug:
            # the peer message re-delivers every turn because the receipt can
            # never be recorded.
            #
            # Resolved ONCE per call and reused for both the read and the write:
            # resolving twice would let a rename land between them and send the
            # gate to one file and the fold to another.
            #
            # Resolve current identity inside _delivery_key; never pass the boot
            # session ID. Direct-parent-only resolution avoids ancestor PID reuse.
            dkey = _delivery_key()
            if dkey is None:
                # WE DO NOT KNOW WHO WE ARE. Refuse the whole call, loudly, and
                # write nothing. Previously this state was unreachable-by-
                # construction only because the resolver manufactured a
                # well-formed wrong answer instead -- which is what made a
                # total identity failure indistinguishable from an ordinary
                # miss for two days, across three silent alarms.
                # TWO CAUSES, TWO REMEDIES, AND ONE OF THEM IS NOT OURS TO FIX.
                # This branch is reached from `_delivery_key` in exactly two
                # ways, and the text below must not merge them the way the
                # previous wording did ("re-claiming a name ... rewrites both
                # records"):
                #   (1) NO READABLE SESSION RECORD -- `_read_session_identity`
                #       could not resolve `~/.claude/sessions/<parent pid>.json`.
                #       set_name.py NEVER writes that file; Claude Code does. So
                #       `/relay-name` cannot repair it, and neither can a
                #       reconnect: the respawned subprocess has the same parent
                #       and reads the same missing file. Only a fresh Claude Code
                #       session produces a fresh record.
                #   (2) THE RELAY REGISTRY COULD NOT BE READ (`_NAME_UNKNOWN`:
                #       unreadable/corrupt file, or this session's row present and
                #       not an object). Re-claiming a name DOES fix this one --
                #       measured in set_name.py, whose `_read_registry` returns
                #       `{}` on an unparseable file and whose write then REPLACES
                #       the file wholesale, so the row lands either way. (That
                #       wholesale replace also drops other sessions' rows; it is
                #       a repair, not a merge.)
                _eprint(
                    "local-relay-channel: broker_ack REFUSED ALL "
                    f"{sorted(set(clean_ids))} -- this session cannot establish "
                    "its own identity. NOTHING was written: filing an "
                    "ack under a guessed key is what caused the 2026-08-05 "
                    "outage. If the RELAY REGISTRY is the unreadable record, "
                    "re-claiming a name in THIS session (/relay-name <name>) "
                    "rewrites it. If the CC SESSION RECORD "
                    f"({_SESSION_DIR}/<parent pid>.json) is missing or "
                    "unparseable, nothing in the relay writes that file -- "
                    "Claude Code does -- so it takes a fresh Claude Code session; "
                    "reconnecting re-reads the same file. Messages re-deliver "
                    "until then, so nothing is lost."
                )
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "ok": False,
                        "error": (
                            "no_identity: this session could not establish its own "
                            "relay identity, so no acknowledgement was recorded. "
                            "This is NOT benign and NOT a per-message condition -- "
                            "every ack from this session will fail until it is "
                            "fixed. SURFACE THIS TO THE USER, and give them BOTH "
                            "branches, because the two causes have different "
                            "remedies. If the RELAY REGISTRY is the record that "
                            "could not be read, re-claiming a name in this session "
                            "(/relay-name <name>) rewrites it. If the CLAUDE "
                            "CODE SESSION RECORD is missing or unparseable, nothing "
                            "in the relay writes that file -- Claude Code does -- so "
                            "re-claiming a name cannot fix it and a fresh Claude "
                            "Code session is what does. Reloading the connection "
                            "fixes NEITHER: it re-reads the same records and returns "
                            "this same result. Messages re-deliver until then, so "
                            "nothing is lost."
                        ),
                        "acked": [],
                        "refused": sorted(set(clean_ids)),
                        "reasons": {str(i): "NO_IDENTITY"
                                    for i in sorted(set(clean_ids))},
                        # EXPLICIT NULLS, NOT AN OMITTED PAIR. The tool
                        # description promises every result carries
                        # `delivery_key` and `floor` so a refusal can be read
                        # without running anything else -- and a description
                        # that matches behaviour is the whole point of this
                        # change, so this path cannot be the exception that
                        # proves the reader wrong. `null` is also the HONEST
                        # value here rather than a placeholder: this branch is
                        # reached precisely because the mailbox could not be
                        # established, so there is no key to name and no floor
                        # was read. Omitting them would leave the caller unable
                        # to tell "the server does not report this" from "the
                        # server could not determine it".
                        "delivery_key": None,
                        "floor": None,
                    }),
                )]
            # THE GATE'S ARITY IS THREE AND STAYS THREE (seven fixed-arity unpacks
            # of it live in another peer's test tree, which imports the DEPLOYED
            # plugin by path -- a fourth return value breaks them from a Release
            # alone). Anything else it learns comes back through this optional
            # mapping, which a two-argument caller never sees.
            gate_diag: dict = {}
            foldable, already, refused = _gate_broker_ack(dkey, clean_ids, gate_diag)
            res = None
            if foldable:
                try:
                    res = _write_confirmed_set_locked(dkey, foldable)
                except Exception as e:
                    _eprint(f"local-relay-channel: broker_ack write raised "
                            f"{type(e).__name__}: {e}")
                    # Best-effort: never fail the tool call on a confirmed-set
                    # write error; the receive-hook backstop reconciles next fire.
                    res = None
            # WS-B-client (#19): mirror the gate-passed acks to the broker delivery
            # ledger. foldable ONLY — a subagent-leaked id was refused by
            # _gate_broker_ack and must NOT flip a broker row (D57). Keyed by this
            # session's CURRENT friendly name. RESIDUAL (rare): if the recipient was
            # RENAMED after a message was addressed to its OLD name but before this
            # ack, the broker row (keyed by the old to_agent) won't match and stays
            # 'outstanding' — the reaper (#18) must reconcile rows whose to_agent no
            # longer resolves to a live holder (see plan). NOT a silent loss (the
            # LOCAL confirmed-set still advanced; delivery is unaffected).
            # Fire-and-forget so broker_ack keeps its "returns instantly" contract.
            # D130: POST UNDER THE SAME IDENTITY WE JUST FILED UNDER. This read
            # `_resolve_my_name(my_sid) or my_name` -- the ROUTING resolver --
            # while the fold above used `_delivery_key`. At the time those two
            # read different source sets and could disagree, so the confirmed-set
            # landed under the sid while the broker ack posted under the name:
            # one call, two identities, and the local record then contradicted the
            # ledger. The source sets were made identical in 2026-08-15's
            # subtraction, so the two now agree by construction -- but `dkey` stays
            # the value posted here, because the ledger key must follow the FILING
            # identity even if the resolvers are ever split again.
            #
            # `dkey` is by definition "the identity the broker addresses this
            # session by", which is exactly what `/api/agent/{name}/ack` wants.
            # NARROWING, stated: a session the broker knows by a name that the
            # registry does not carry now posts under its sid instead of that
            # name. That is the SAFE direction -- the row stays outstanding and
            # the reaper eventually pages -- whereas the reverse silently closes a
            # row whose receipt was never recorded locally.
            ack_name = dkey
            # Reconcile locally confirmed point IDs even after they fold into
            # the floor: the explicit acked set contains only IDs above a gap,
            # so a suppressed floor would otherwise strand their broker rows.
            # Broker acknowledgements are idempotent and recipient-scoped.
            #
            # WHICH REFUSAL PATH THE GATE TOOK. Read from the gate's own
            # diagnostics rather than re-derived here, so the label below and the
            # partition above can never disagree; defaults to the
            # allow-list-PRESENT reading, which is the pre-existing behaviour.
            # Needed HERE, above the post, and not only at the reason loop -- the
            # post is the other thing this distinction decides.
            allowlist_absent = gate_diag.get("allowlist") == "absent"
            # THE POST IS NARROWER THAN `already`, AND IT MUST STAY EXACTLY WHERE
            # IT WAS. Reading the floor before the fail-closed return corrected a
            # LABEL: an at-or-below-floor id on the allow-list-ABSENT path now
            # reports IN_CONFIRMED_SET instead of NOT_AWAITED. That id also
            # arrives here in `already` -- and posting it would be a BEHAVIOUR
            # change, not a reporting one, on a path that posted NOTHING before.
            # MEASURED over 1,008 generated cases against the HEAD bytes: with the
            # unconditional union, 320 of them POST a set HEAD did not post; with
            # this guard, 0 do, while the fold stays identical in all 1,008.
            #
            # AND THE WIDENING WOULD HAVE BEEN UNSOUND, not merely different. On
            # this path the floor can be SEEDED, for a nameless session, from the
            # legacy sid-scoped plain cursor (`_legacy_floor_for`, which returns a
            # real value exactly when the key is a UUID). A floor of that
            # provenance proves sid-addressed receipt, not that anything addressed
            # to this mailbox was ever delivered -- so posting it would close
            # delivery rows on the strength of a cursor that never saw them. That
            # is silent loss, the one direction this codebase does not trade.
            # The allow-list-PRESENT path keeps posting `already` exactly as it
            # always has; this changes nothing there.
            post_ids = [] if allowlist_absent else sorted(set(foldable) | set(already))
            if post_ids and ack_name:
                _spawn_bg(asyncio.to_thread(_safe_post_ack, ack_name, post_ids))
            # Claim-8 contract: `acked` reflects ONLY ids actually confirmed by the
            # locked write (id <= floor OR in acked). On a lock-timeout / write
            # failure the writer folds nothing (returns the current on-disk state),
            # so those ids are reported NOT acked — the receive hook re-delivers
            # them (no loss), but `acked` never over-reports a fold that did not
            # happen. Write-deferred and gate-refused ids land in `refused`.
            # CHANGED: `already` ids are reported ACKED, because they ARE confirmed
            # here -- the fold happened earlier, via the render-grep. Reporting them
            # as refused was the single most misleading thing this tool did: it is
            # what sent four agent sessions hunting a delivery failure that had not
            # occurred, and it is indistinguishable at the call site from the
            # fail-closed refusal that IS a real fault.
            if res is not None:
                rfloor = res.get("floor", 0)
                racked = set(res.get("acked", []))
                acked_out = sorted(set(already) | {i for i in set(foldable)
                                                   if i <= rfloor or i in racked})
            else:
                acked_out = sorted(set(already))
            refused_out = sorted(set(clean_ids) - set(acked_out))
            # PER-ID REASON CODES. Additive: `acked` and `refused` are unchanged in
            # both presence and content (verified over 1,152 generated cases against
            # the un-patched tail). BENIGN vs FAULT is the only distinction a caller
            # can act on, so there are three codes and no more:
            #   BENIGN  IN_CONFIRMED_SET -- this session's confirmed-set already
            #             covers the id: i <= floor, OR i is in the explicit `acked`
            #             list. _collapse_confirmed keeps in `acked` ONLY ids
            #             strictly ABOVE the floor (:661), so a "below the floor"
            #             label would be false for 100% of that second arm -- which
            #             is why this code is named for the SET, not the position.
            #             It asserts NO provenance: the floor spans the GLOBAL
            #             event-id range, including other recipients. Broker ACK
            #             updates are recipient-scoped, so those IDs match no row.
            #   SPLIT   NOT_AWAITED -- this session's allow-list WAS READ and does
            #             not hold this id. TWO POPULATIONS, and only one of them is
            #             benign; this bullet claimed both were until 2026-08-14.
            #             A HOOK-INJECTED id is genuinely transient: it lands in
            #             `awaiting_ack` on the hook's next fire and re-delivers
            #             until it does. A PUSH-RENDERED id never lands there at all
            #             -- shared.py's `do_receive` builds the allow-list from
            #             `injected_ids | legacy_injected_ids`, and the render-grep
            #             set is the OTHER arm of that same if/else, so the two are
            #             disjoint by construction. Such an id refuses identically
            #             forever, and re-acking it can never change that. Locally
            #             that costs nothing (the grep already confirmed it), but
            #             the sender learns receipt through the hook's mirror;
            #             where that mirror does not carry it, the sender's row
            #             remains outstanding. Repeating this same refusal is
            #             not evidence that the broker row was reconciled.
            #   BENIGN  ALLOWLIST_UNAVAILABLE -- this session's mailbox carried NO
            #             usable allow-list, so every id still OPEN in the call was
            #             refused together. NOT necessarily every id in the call:
            #             the floor is read BEFORE the fail-closed return now, so an
            #             at-or-below-floor id in the same call is reported
            #             IN_CONFIRMED_SET and a MIXED result is normal (measured:
            #             floor 10, ids [5,20] -> acked [5], refused [20]). Do
            #             nothing: the receive hook writes the allow-list on its very
            #             next fire and the open ids re-deliver until then.
            #             NOTHING FROM THIS PATH IS POSTED TO THE BROKER, including
            #             the IN_CONFIRMED_SET ids -- HEAD posted nothing here and
            #             the floor behind those ids can be legacy-seeded. See the
            #             `post_ids` comment below.
            #   FAULT   WRITE_FAILED -- the gate PASSED the id but the confirmed-set
            #             write did not land. No loss (it re-delivers), but this
            #             session's local write is broken.
            # WHY THE SPLIT EXISTS NOW AND DID NOT BEFORE. `NOT_AWAITED` used to
            # cover THREE causes, and the third one never recovered on its own: a
            # session whose routing name and delivery key had parted company was
            # filing receipts into a mailbox nothing addressed, permanently, and
            # said the same single word about it that an ordinary race says. That
            # third cause is now UNCONSTRUCTIBLE -- `_resolve_my_name` and
            # `_delivery_key_name` read the SAME two records in the same order, so
            # there is no state in which routing answers with a name the delivery
            # side does not -- so the PER-CALL fail-closed cause now has its own
            # code and the remaining NOT_AWAITED is per-id.
            # WHAT THAT DOES NOT MEAN, and the first draft of this comment got it
            # wrong: it does NOT make every surviving NOT_AWAITED transient. The
            # push-rendered arm above is permanent and has no in-session remedy, and
            # it predates this change -- closing the identity split removed a cause,
            # it did not make the tail benign. Describing a permanent condition as
            # transient is the exact defect this change exists to remove; asserting
            # it one layer up in the explanation would have re-shipped it.
            # A plain successful fold gets NO entry: it is in `acked`, which is the
            # whole story. Keyed by id, NOT positional -- duplicate ids collapse in
            # the set arithmetic above and a positional list would mis-align.
            # WHY NOT `NO_EPOCH` AS THE FAULT ARM: it would be false per-call. The
            # epoch gate lives in shared.py and suppresses only the FLOOR argument of
            # post_ack; explicit event_ids are never suppressed, and the
            # _safe_post_ack above is epoch-blind. Epoch absence alone therefore
            # cannot establish a per-call fault. A push-rendered tail can remain
            # permanent even when other rows acknowledge successfully; an
            # acked-only denominator does not establish complete delivery.
            # WHAT THESE CODES DO NOT DISTINGUISH, so they are not over-read:
            #  - WRITE_FAILED merges the write RAISING with the write returning without
            #    the id (lock timeout / mkdir failure). Both mean "the fold did not
            #    happen"; which one is NOT reported. It can also fire for an id that IS
            #    on disk if the hook advanced the floor concurrently -- measured in a
            #    reproducer, reachability on this fleet UNMEASURED. That is why the
            #    tool description says "could not confirm" rather than "did not write".
            #  - ids dropped by the sanitiser above (non-int, <= 0, bool, or past the
            #    _BROKER_ACK_MAX_IDS cap) still appear in NO list and get NO code.
            #    Pre-existing hole, deliberately not closed here.
            # NO CODE BELOW ASSERTS ANYTHING ABOUT THE BROKER ROW: the post above is
            # fire-and-forget and is skipped entirely when ack_name is falsy.
            already_set, foldable_set = set(already), set(foldable)
            acked_set = set(acked_out)
            # Which of the two refusal codes applies is a property of the CALL,
            # not of the id: either the mailbox carried an allow-list or it did
            # not. ("Transient" is a property of neither -- see the NOT_AWAITED
            # bullet above, whose push-rendered arm is permanent.) `allowlist_absent`
            # is computed once, at the post above, and reused here so the code the
            # caller reads and the set the broker receives are decided by the same
            # value rather than by two reads that could drift apart.
            reasons: dict[str, str] = {}
            for i in sorted(set(clean_ids)):
                if i in already_set:
                    reasons[str(i)] = "IN_CONFIRMED_SET"
                elif i in foldable_set and i not in acked_set:
                    reasons[str(i)] = "WRITE_FAILED"
                elif i not in acked_set:
                    reasons[str(i)] = ("ALLOWLIST_UNAVAILABLE" if allowlist_absent
                                       else "NOT_AWAITED")
            # NAME THE MAILBOX AND THE FLOOR ON EVERY ANSWER, not just on refusals.
            # A refusal that cannot say which mailbox it opened is what made a
            # permanent identity fault indistinguishable from a race for eleven
            # hours; and reporting the pair only on the failing path would mean the
            # reader has no baseline to compare it against.
            #
            # `floor` IS THE FLOOR THE GATE READ, not the post-write floor. It is
            # the value every reason code above was computed against, so it
            # explains the answer; a floor read after the fold would not.
            #
            # THIS IS ALSO THE ONLY THING THAT WOULD EXPOSE A PLUGIN/HOOK KEY
            # DIVERGENCE. The two sides compute this key independently and are held
            # in step by a comment rather than by a test, so the key stays in the
            # response even now that the plugin-side split is closed.
            return [TextContent(
                type="text",
                text=json.dumps({"ok": True,
                                 "acked": acked_out,
                                 "refused": refused_out,
                                 "reasons": reasons,
                                 "delivery_key": gate_diag.get("delivery_key", dkey),
                                 "floor": gate_diag.get("floor", 0)}),
            )]

        if name not in ("peer_reply", "reply"):  # reply = transition alias (unadvertised)
            raise ValueError(f"Unknown tool: {name}")
        if my_name is None:
            # Degraded mode: no friendly_name → no sender identity. Refuse
            # rather than POST from_agent=None (or leak a session_id). The model
            # is told how to enable replies.
            #
            # IT NO LONGER SAYS "then /mcp reconnect", AND THE SAME COMMIT THAT
            # WROTE THAT SENTENCE ASSERTS THE OPPOSITE ~500 LINES BELOW: the
            # degraded supervisor re-resolves the name every ~3s and activates
            # push itself ("no /mcp reconnect needed"), and the rename watcher
            # keeps doing so afterwards. Prescribing a reconnect here told the
            # model to ask the operator for a step the process performs on its
            # own.
            # THE SECOND CLAUSE IS NOT PADDING. `_routing_name` also returns None
            # for a name that IS set but cannot be a delivery key (reserved or
            # invalid as a filename); and a session whose registry row LOST its
            # name resolves no name at all, because that row is one of only two
            # sources and the workspace-file source that used to cover for it was
            # removed. In both of those the supervisor keeps polling forever and
            # the user needs to know that a different / re-claimed name is the
            # move, not more waiting.
            return [TextContent(
                type="text",
                text=json.dumps({
                    "ok": False,
                    "error": ("this session has no usable relay identity; cannot "
                              "send replies. Run /relay-name <name> -- this "
                              "server picks the name up by itself within a few "
                              "seconds and activates push; do NOT ask the user to "
                              "reconnect. If replies still fail after that, the "
                              "name resolved but is not usable as a delivery key "
                              "(reserved or invalid as a filename), or this "
                              "session's registry row lost it: claim a different "
                              "name."),
                }),
            )]
        to = arguments["to_session_id"]
        # ENTRY-POINT FOLD (C-48). `to` is one of the two ARGUMENTS a relay name
        # enters a process through, and it is folded HERE — at the point of
        # binding, before ANY consumer — rather than inside
        # `_resolve_relay_recipient`.
        #
        # THAT PLACEMENT IS MEASURED, NOT STYLISTIC. The guard's RETURN VALUE IS
        # DISCARDED at its call site below (it is used purely as a
        # resolves/does-not-resolve predicate) and the POST passes `to` straight
        # through as `to_agent`. So folding inside the guard would have been a
        # NO-OP: a change that looks correct, reads correct, reviews clean, and
        # alters nothing that ships.
        #
        # UUID-SHAPED VALUES ARE EXEMPT. A session_id is a separate key-space and
        # sid addressing is out of scope for this change; the isinstance check
        # keeps a non-string argument from reaching the regex.
        if isinstance(to, str) and not _UUID_RE.match(to):
            to = _normalize_name(to)
        content = arguments["content"]
        channel = arguments.get("channel", "#cc-session-relay")
        # Refuse unresolved recipients before sending. Resolving a registry name
        # or exact registered session ID is not a liveness or reachability proof,
        # and a successful POST is not proof of receipt. This guard never alters
        # previously stored messages or outstanding delivery rows.
        resolved_to, recipient_verified = _resolve_relay_recipient(to)
        if resolved_to is None:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "ok": False,
                    "error": f"no relay session resolves '{to}'; nothing was sent",
                    "known_names": _registry_names(),
                }),
            )]
        to = resolved_to
        # post_event auto-enqueues on broker unavailability. The result below
        # preserves whether recipient authority verified this send.
        event_id = broker_client.post_event(
            from_agent=my_name,
            to_agent=to,
            channel=channel,
            kind="agent_reply",
            content=content,
        )
        if event_id is None:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "ok": False,
                    "error": "broker unreachable; reply enqueued to outbox",
                }),
            )]
        return [TextContent(
            type="text",
            text=json.dumps({"ok": True,
                             "event_id": event_id,
                             "recipient_verified": recipient_verified}),
        )]

    # ----- Initialization options + run loop ---------------------------------
    # NOTE: We reimplement Server.run inline (rather than calling it) so we
    # keep a reference to `session` for the SSE-driven notification emit path.
    # Server.run constructs the session internally and never exposes it; the
    # only way to push a notification through the session's _write_stream is
    # to own the construction here.
    init_options = server.create_initialization_options(
        notification_options=NotificationOptions(),
        experimental_capabilities={"claude/channel": {}},
    )

    async def name_aware_subscribe_supervisor(session: ServerSession) -> None:
        """Self-healing push activation (fixes the cold-start race).

        my_name is resolved once at boot. On a fresh session the friendly name
        is usually set a moment AFTER boot (/relay-name or the SessionStart
        register-hook inherit), so the server starts DEGRADED and — pre-fix —
        never re-checked, forcing a manual /mcp reconnect every startup. This
        task re-resolves the name in the background while degraded and activates
        the SSE push subscription as soon as a name appears. If a name is already
        set at boot it activates immediately (pre-fix behaviour).
        """
        nonlocal my_name, my_sid
        _degraded_polls = 0
        while my_name is None:
            # RE-RESOLVE THE SESSION ID FIRST. my_sid is bound ONCE at boot and
            # every door of _resolve_my_name is keyed by it, so a boot that
            # captured the wrong id could NEVER converge however long this ran.
            # Measured: 60s+ of polling, zero subscriptions, sid frozen wrong.
            #
            # _read_session_identity, NOT _resolve_session_id: the latter exits
            # the PROCESS when it finds nothing, and on a miss it falls back to
            # CLAUDE_CODE_SESSION_ID -- the value that is WRONG for this
            # subprocess and is the whole reason this fix exists. Adopting it
            # would overwrite a good id with a known-bad one. Do not "simplify"
            # this into the other function.
            #
            # NO NEW MACHINERY: _resolve_session_file already refuses to memoise
            # a MISS, and its docstring says it does so precisely so that a
            # session id written after we started is still picked up. That
            # capability was built and then never called a second time. This is
            # the missing call site.
            try:
                fresh_sid = _read_session_identity()
            except Exception as e:
                _eprint(f"local-relay-channel: degraded sid re-check failed: "
                        f"{type(e).__name__}: {e}")
                fresh_sid = None
            if fresh_sid is not None and fresh_sid != my_sid:
                _eprint(f"local-relay-channel: session id corrected "
                        f"{my_sid[:8]}… -> {fresh_sid[:8]}… after degraded "
                        f"start; re-checking the name against the TRUE id")
                my_sid = fresh_sid
            try:
                resolved = _routing_name(my_sid)   # same gate as the boot path
            except Exception as e:
                _eprint(f"local-relay-channel: degraded name re-check failed: "
                        f"{type(e).__name__}: {e}")
                resolved = None
            if resolved is not None:
                my_name = resolved
                _eprint(f"local-relay-channel: friendly_name resolved to "
                        f"{my_name!r} after degraded start; activating channel "
                        f"PUSH (no /mcp reconnect needed)")
                try:
                    marker = (Path.home() / ".claude" / "relay"
                              / "pending-mcp-reconnect" / f"{my_sid}.flag")
                    marker.unlink(missing_ok=True)
                except Exception:
                    pass
                break   # activate NOW -- do not fall through to another sleep
            # SLEEP LAST. This body used to sleep FIRST, so a process that died
            # inside one interval ran ZERO iterations -- and most degraded boots
            # are that short. Checking first also cuts typical activation from a
            # full interval to ~0.
            await anyio.sleep(_degraded_poll_s)
            _degraded_polls += 1
            # Heartbeat ~every 60s while stuck degraded so a live in-the-field
            # case is visible in the log (C-17). Low-volume; stops on resolve.
            # HONEST LIMIT, measured 2026-08-01: Claude Code captures NO plugin
            # stderr after the MCP handshake, so this line is INVISIBLE in the
            # field. Kept because it is visible on a manual/foreground launch.
            if _degraded_polls % max(1, int(round(60.0 / _degraded_poll_s))) == 0:
                _eprint(
                    f"local-relay-channel: still DEGRADED after "
                    f"~{int(_degraded_polls * _degraded_poll_s)}s "
                    f"(sid={my_sid[:8]}…, still no user-set name resolvable) — "
                    f"push inactive; receive-hook backup still delivering"
                )

        # A NAME IS ACTIVE. KEEP WATCHING FOR A RENAME -- this loop used to end
        # here, permanently, and that was the second half of the same root cause.
        #
        # WHY IT MATTERS: `my_name` is not merely a label. It is the client-side
        # filter every inbound event is matched against, and the `from_agent` of
        # every reply. Once this loop exited, a session that was renamed went on
        # filtering for its OLD mailbox forever -- mail to the new name silently
        # never emitted -- and the only recovery was a human typing
        # `/mcp reconnect`. That is exactly the state C-40 forbids and the scale
        # bar C-54 sets ("100s of peers; we cannot manually restart mcp servers
        # for every agent") rules out.
        #
        # THE WATCHER IS SMALLER THAN THE REFERENCE IMPLEMENTATION, and the
        # reason is a measurement rather than a shortcut. `vildanbina/claude-relay`
        # needs an `fs.watch` plus a re-REGISTRATION because its transport is
        # bound to the peer name. Ours is not: `sse_subscribe_loop` issues a bare
        # `GET {broker}/api/events` with NO name in the URL and filters
        # CLIENT-SIDE against this closure variable, which every reader re-reads
        # live. So a rename needs no re-subscribe, no reconnect and no new
        # machinery -- reassigning the variable is the whole mechanism.
        #
        # IT NEVER BLANKS A LIVE NAME. `_routing_name` returning None means
        # "could not resolve", which a transient unreadable registry produces as
        # readily as a real de-naming. Adopting that None would deactivate push
        # on a fault -- turning a blip into an outage -- so only a DIFFERENT,
        # non-None name is adopted. Same distinction as `_delivery_key`'s: a
        # failure to check is not a fact.
        async def rename_watch_loop() -> None:
            nonlocal my_name, my_sid
            while True:
                await anyio.sleep(_degraded_poll_s)
                try:
                    fresh_sid = _read_session_identity()
                    if fresh_sid and fresh_sid != my_sid:
                        _eprint(f"local-relay-channel: session id changed "
                                f"{my_sid[:8]}… -> {fresh_sid[:8]}… while active; "
                                f"re-checking the name against the TRUE id")
                        my_sid = fresh_sid
                    resolved = _routing_name(my_sid)
                except Exception as e:
                    _eprint(f"local-relay-channel: rename watch failed: "
                            f"{type(e).__name__}: {e}")
                    continue
                if resolved is not None and resolved != my_name:
                    _eprint(f"local-relay-channel: relay name changed "
                            f"{my_name!r} -> {resolved!r}; push now filters for "
                            f"the new mailbox (no /mcp reconnect needed)")
                    my_name = resolved

        async with anyio.create_task_group() as active_tg:
            active_tg.start_soon(rename_watch_loop)
            await sse_subscribe_loop(session)

    async def main() -> None:
        async with stdio_server() as (read_stream, write_stream):
            from contextlib import AsyncExitStack
            async with AsyncExitStack() as stack:
                lifespan_ctx = await stack.enter_async_context(
                    server.lifespan(server)
                )
                session = await stack.enter_async_context(
                    ServerSession(read_stream, write_stream, init_options)
                )
                async with anyio.create_task_group() as tg:
                    # Background: a supervisor that activates the SSE subscriber
                    # (which pumps notifications into session._write_stream) as
                    # soon as a friendly_name is resolvable. Cold-start-race fix:
                    # ALWAYS start it. Name set at boot → activates push
                    # immediately; degraded (no name yet) → it watches for the
                    # name and activates push with NO /mcp reconnect. While
                    # degraded the MCP server still serves the handshake + tool
                    # surface so CC sees it connected (background-agents daemon
                    # fix); the supervisor's poll loop is cancelled cleanly by
                    # the `finally` below on stdin EOF / client disconnect.
                    tg.start_soon(name_aware_subscribe_supervisor, session)
                    # Foreground: handle inbound MCP messages
                    # (initialize, tool calls, etc.).
                    try:
                        async for message in session.incoming_messages:
                            tg.start_soon(
                                server._handle_message,
                                message, session, lifespan_ctx, False,
                            )
                    finally:
                        # M3 fix (discriminator iter-1): when the inbound
                        # message loop ends (stdin EOF / client disconnect), the
                        # name_aware_subscribe_supervisor (polling while degraded,
                        # or running the infinite sse_subscribe_loop once active)
                        # would otherwise hold the task-group open forever —
                        # preventing AsyncExitStack cleanup and orphaning the
                        # subprocess. Explicit cancel of the task-group's scope
                        # releases it so the `async with` block can exit cleanly.
                        tg.cancel_scope.cancel()

    try:
        anyio.run(main)
    except KeyboardInterrupt:
        _eprint("local-relay-channel: shutdown (SIGINT)")
    except Exception as e:
        # Last-word log before subprocess exit; CC plugin logs capture this.
        _eprint(f"local-relay-channel: fatal: {type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    _build_and_run()
