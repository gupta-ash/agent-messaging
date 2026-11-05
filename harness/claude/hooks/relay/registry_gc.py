# -*- coding: utf-8 -*-
"""registry_gc.py — auto duplicate-name garbage collection for the relay registry.

Demotes STALE DUPLICATE-name rows in ~/.claude/relay/registry.json so a friendly
name stops accumulating dead holder rows across CC's session_id churn (/compact,
/restore, VSCode reload, UI-freeze-recovery each mint a new sid + a new row).

Stale-state CLEANUP, never a watchdog/auto-restart (CONSTRAINTS C-17). Called by
register_session.py INSIDE its held registry.json.lock critical section, AFTER the
own-row mutation/inherit and BEFORE the single atomic write, gated by a ~6h
throttle (shared._registry_gc_due). The pure function here does NOT lock or write
— the caller owns the lock + the write.

Design:
  * DEMOTE = the D52 mechanism (entry["name"]=None; pop naming_status). Frees the
    name, KEEPS the row, never deletes. A demoted row == a never-named row.
  * Scope = DUPLICATE NAMES ONLY: only act on a name carried by >=2 user_set rows.
  * Liveness = positive ALLOW-LIST. A holder is demotable iff ALL false:
      (1) sid == own_sid (the session registering now — always protected)
      (2) live foreground process-file (~/.claude/sessions/*.json with
          sessionId|session_id == sid — file-presence ONLY, NO os.kill). A
          STALE lingering file over-reports liveness (safe: GC under-cleans).
          A LIVE session's file that is transiently UNREADABLE under-reports
          liveness (unsafe), so if the foreground scan hits ANY read/parse
          error the whole GC run is skipped (fail closed).
      (3) live background job-state (jobs/<sid>/state.json state not terminal;
          a present-but-unreadable state.json fails toward LIVE)
    last_seen_ts is IGNORED for liveness (D52); only the per-workspace-retention
    tie-break uses it.
  * Per-workspace retention: for a duplicated name with ZERO live holders, keep
    the single most-recently-seen dead row PER WORKSPACE and demote the rest.
    This retention does not authorize name inheritance by a later session.

Cardinal rule: NEVER demote a live row (that flips its self-poll identity to raw
sid in _resolve_name_from_registry = message loss). Bias: under-clean, never over.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional

# shared.py lives next to this file
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shared import (  # noqa: E402
    CLAUDE_DIR,
    SESSIONS_DIR,
    _TERMINAL_JOB_STATES,
)


def _norm(p: Optional[str]) -> str:
    """Normalize a workspace path (normcase+normpath) so GC groups workspaces
    consistently regardless of case / separator differences on Windows."""
    if not isinstance(p, str) or not p:
        return ""
    try:
        return os.path.normcase(os.path.normpath(p))
    except Exception:
        return p.lower() if os.name == "nt" else p


def _live_foreground_sids(sessions_dir: Optional[Path] = None) -> tuple:
    """Return (live_sids, scan_clean). live_sids = sids with a live foreground
    process-file; a live CC process writes ~/.claude/sessions/<pid>.json keyed by
    'sessionId' (camelCase) or legacy 'session_id' — accept BOTH (correct form per
    set_name.py:108; NOT the buggy send.py:84 which reads only session_id).
    File-presence ONLY (no os.kill).

    scan_clean is False if ANY session file could not be cleanly read/parsed, OR
    the directory could not be LISTED (a hidden live session's sid would then be
    missing from live_sids). The caller MUST fail closed (skip GC) on
    scan_clean=False — otherwise a transiently-unreadable live session's row could
    be demoted (= message loss). A legitimately ABSENT sessions dir is CLEAN.

    IMPORTANT: list via os.listdir (NOT Path.glob/iterdir), because pathlib's
    glob/iterdir SWALLOW the scandir-time OSError on CPython 3.12 and yield an
    empty result with no exception — which would masquerade as a clean empty dir
    and re-open the live-row-demotion path. os.listdir RAISES the listing OSError
    so we can fail closed on it."""
    out: set = set()
    clean = True
    if sessions_dir is None:
        sessions_dir = SESSIONS_DIR
    try:
        names = os.listdir(sessions_dir)
    except (FileNotFoundError, NotADirectoryError):
        return out, True   # legitimately no foreground sessions dir -> CLEAN
    except OSError:
        return out, False  # dir present but unlistable -> untrustworthy
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            d = json.loads((sessions_dir / name).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            clean = False  # may hide a live session's sid -> untrustworthy
            continue
        if not isinstance(d, dict):
            clean = False
            continue
        for key in ("sessionId", "session_id"):
            v = d.get(key)
            if isinstance(v, str) and v:
                out.add(v)
    return out, clean


def _live_bg_jobstate(sid: str, jobs_dir: Path) -> bool:
    """True iff sid is a LIVE background-agent holder. AGGREGATES both candidate
    dirs (full-sid, then sid[:8]): returns True on ANY owning NON-terminal state,
    or on any present-but-unreadable/malformed/no-state file (FAIL TOWARD LIVE on
    uncertainty — never demote/delete a row whose liveness we could not determine).
    Returns False ONLY after ALL candidates are a CLEAN signal of death: absent,
    non-owning, or a cleanly-read terminal state.

    A stale OWNING terminal jobs/<full-sid> must NOT short-circuit and mask a
    running jobs/<sid[:8]> for the same holder — so a terminal owning candidate
    CONTINUES to the next candidate instead of returning False (mirrors
    shared._is_sid_live; the masking class that drove a live-row DELETE otherwise).

    Ownership mirrors shared.resolve_daemon_job_state: the full-sid candidate owns
    unless its sessionId is a DIFFERENT non-empty string; the sid[:8] candidate
    owns ONLY on an exact sessionId match (guards 8-hex prefix collisions). Honors
    an injected jobs_dir (the global resolver hardcodes the path, defeating the
    test seam) and does NOT gate on nameSource/template (liveness applies to any
    job-state row)."""
    if not sid:
        return False
    candidates = [sid]
    if len(sid) > 8:
        candidates.append(sid[:8])
    for i, cand in enumerate(candidates):
        sj = jobs_dir / cand / "state.json"
        try:
            present = sj.is_file()
        except OSError:
            return True  # uncertain -> live
        if not present:
            continue
        try:
            s = json.loads(sj.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            return True  # present but unreadable -> uncertain -> live
        if not isinstance(s, dict):
            return True  # present but malformed -> uncertain -> live
        owner = s.get("sessionId")
        if i == 0:
            # full-sid dir owns unless its sessionId is a DIFFERENT non-empty string
            if isinstance(owner, str) and owner and owner != sid:
                continue  # a different session's dir -> not ours
        else:
            # sid[:8] prefix dir owns ONLY on an exact sessionId match
            if owner != sid:
                continue
        st = s.get("state")
        if not isinstance(st, str):
            return True  # owning but no usable state -> uncertain -> live
        if st.lower() not in _TERMINAL_JOB_STATES:
            return True  # owning NON-terminal -> LIVE
        # owning but TERMINAL -> dead per THIS candidate; keep checking the other
        # (a stale terminal full-sid must not mask a running sid[:8]).
    return False  # all candidates clean-dead / absent / non-owning -> not a live bg holder


def gc_stale_duplicate_names(
    registry: dict,
    own_sid: str,
    *,
    now_ms: int,
    jobs_dir: Optional[Path] = None,
    sessions_dir: Optional[Path] = None,
) -> list:
    """Demote dead duplicate-name rows IN PLACE; return the list of demoted sids.

    Pure (no lock, no write, no broker IO) — the caller (register_session.py) owns
    the registry lock + the atomic write. Hermetic: jobs_dir/sessions_dir
    injectable for tests. See module docstring for the full contract.
    """
    if not isinstance(registry, dict):
        return []
    if jobs_dir is None:
        jobs_dir = CLAUDE_DIR / "jobs"

    live_fg, fg_clean = _live_foreground_sids(sessions_dir)
    if not fg_clean:
        # A foreground session file could not be cleanly read -> a LIVE session's
        # sid may be missing from live_fg. Fail CLOSED: skip this entire GC run
        # rather than risk demoting a live row (the next SessionStart retries).
        return []

    def _is_live(sid: str) -> bool:
        if sid == own_sid:
            return True
        if sid in live_fg:
            return True
        try:
            return _live_bg_jobstate(sid, jobs_dir)
        except Exception:
            # Fail toward LIVE (never demote on a liveness-probe error) —
            # bias under-clean, never over-clean.
            return True

    # Group user_set holders by EXACT name (case-sensitive — names are
    # user-chosen + case-meaningful). Only user_set rows are routing
    # holders (matches _resolve_name_from_registry + the reclaim conflict gate).
    holders_by_name: dict = {}
    for sid, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            continue
        if entry.get("naming_status") != "user_set":
            continue
        holders_by_name.setdefault(name, []).append(sid)

    demoted: list = []

    def _demote(sid: str) -> None:
        entry = registry.get(sid)
        if isinstance(entry, dict):
            entry["name"] = None
            entry.pop("naming_status", None)
            demoted.append(sid)

    def _last_seen_key(sid: str):
        # Total order over any input: a non-numeric / bool / missing last_seen_ts
        # sorts as 0 so a malformed row can never raise TypeError inside max().
        v = (registry.get(sid) or {}).get("last_seen_ts")
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0

    for name, holders in holders_by_name.items():
        if len(holders) < 2:
            continue  # not a duplicate — leave single holders alone
        dead = [s for s in holders if not _is_live(s)]
        if not dead:
            continue
        if len(dead) < len(holders):
            # >=1 LIVE holder -> it is the route + inherit source; demote ALL
            # dead (inherit's uniqueness check already refuses any other
            # workspace while a live holder exists).
            for s in dead:
                _demote(s)
        else:
            # ZERO live holders -> keep ONE most-recent dead row per workspace,
            # demote the rest. (Once the C-19 inheritance anchor; inheritance was
            # deleted per D60/C-26, so this retained row is now vestigial -- see the
            # module docstring's "Per-workspace retention" note.)
            by_ws: dict = {}
            for s in dead:
                ws = _norm((registry.get(s) or {}).get("workspace"))
                by_ws.setdefault(ws, []).append(s)
            for ws, ws_dead in by_ws.items():
                anchor = max(ws_dead, key=_last_seen_key)
                for s in ws_dead:
                    if s != anchor:
                        _demote(s)

    return demoted


def gc_dead_rows(
    registry: dict,
    own_sid: str,
    *,
    now_ms: int,
    age_ms: Optional[int] = None,
    jobs_dir: Optional[Path] = None,
    sessions_dir: Optional[Path] = None,
) -> list:
    """DELETE dead, aged, NON-user_set rows IN PLACE; return the deleted sids.

    Bounds registry.json growth: every CC session that ever ran leaves a row, and
    gc_stale_duplicate_names only DEMOTES (never deletes), so nameless dead rows
    accumulate unboundedly (~30KB/day per F-CR8). This deletes rows that are ALL of:
      - not own_sid;
      - NOT user_set — a user_set row is a routing/inherit identity; the single
        dead user_set anchor per (name, workspace) is the C-19 inheritance source
        and MUST survive (demotion of duplicates is gc_stale_duplicate_names' job;
        a demoted row becomes nameless and this function reaps it on a later run);
      - provably DEAD (not _is_live; same positive-allow-list + fail-toward-live
        discipline as gc_stale_duplicate_names — a live row is NEVER deleted);
      - AGED (most-recent of last_seen_ts/registered_at older than age_ms; default
        1 day, env RELAY_ROW_GC_AGE_DAYS) — a small floor so a very-recently-
        registered row is never reaped; a row with no usable timestamp is KEPT.

    Pure (no lock/write/broker IO) — the caller (register_session.py) owns the
    registry lock + the single atomic write, and calls this AFTER
    gc_stale_duplicate_names within the same held-lock critical section. Fails
    CLOSED on a foreground-scan uncertainty (skip the whole run), exactly like
    gc_stale_duplicate_names. Stale-state CLEANUP, never a watchdog (C-17). Bias:
    under-clean, never over-clean.
    """
    if not isinstance(registry, dict):
        return []
    if jobs_dir is None:
        jobs_dir = CLAUDE_DIR / "jobs"
    if age_ms is None:
        try:
            days = float(os.environ.get("RELAY_ROW_GC_AGE_DAYS", "1"))
        except (TypeError, ValueError):
            days = 1.0
        if days < 0:
            days = 1.0
        age_ms = int(days * 24 * 3600 * 1000)

    live_fg, fg_clean = _live_foreground_sids(sessions_dir)
    if not fg_clean:
        # A live session's sid may be hidden -> fail CLOSED (delete nothing this run).
        return []

    def _is_live(sid: str) -> bool:
        if sid == own_sid:
            return True
        if sid in live_fg:
            return True
        try:
            return _live_bg_jobstate(sid, jobs_dir)
        except Exception:
            return True  # fail toward LIVE on any probe error (never over-delete)

    def _recency_ms(entry: dict):
        # Most-recent of last_seen_ts / registered_at; None if neither is numeric.
        best = None
        for k in ("last_seen_ts", "registered_at"):
            v = entry.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                best = v if best is None else max(best, v)
        return best

    to_delete: list = []
    for sid, entry in registry.items():
        if sid == own_sid:
            continue
        if not isinstance(entry, dict):
            continue
        if entry.get("naming_status") == "user_set":
            continue  # routing/inherit identity -> leave to gc_stale_duplicate_names
        ts = _recency_ms(entry)
        if ts is None:
            continue  # no usable timestamp -> can't prove aged -> KEEP (conservative)
        if (now_ms - ts) < age_ms:
            continue  # too recent -> KEEP
        if _is_live(sid):
            continue  # live (or maybe-live) -> KEEP
        to_delete.append(sid)

    for sid in to_delete:
        registry.pop(sid, None)
    return to_delete
