# -*- coding: utf-8 -*-
"""set_name.py — cc-session-relay name override skill entry-point.

Per task spec § relay-name.

Updates the friendly name for the current session_id in
`~/.claude/relay/registry.json`. Acquires a file-lock to serialize concurrent
writers (matches register_session.py discipline).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

# Import shared broker helper from the relay hooks package — same urllib boilerplate
# is centralized there so register_session.py and this skill stay in sync (v1.3.1).
sys.path.insert(0, str(Path.home() / ".claude" / "hooks" / "relay"))
from shared import (  # noqa: E402
    SESSIONS_DIR,
    _is_sid_live,
    normalize_name,
    post_status_with_display_name,
    resolve_daemon_job_name,
    resolve_daemon_job_state,
)

# APPENDIX 2 (post-cwd-derive-revert): derive_project family removed from shared.py.
# Tier-3 state.db re-bootstrap dropped; this skill is now strictly a friendly-name
# update (registry write + broker display_name propagation). Project tagging is
# handled at pipeline-dispatch time by peer's helper, not at name-claim time.

HOME = Path.home()
RELAY_DIR = HOME / ".claude" / "relay"
REGISTRY_PATH = RELAY_DIR / "registry.json"

NAME_RE = re.compile(r"^[A-Za-z0-9._\-]+$")
NAME_MAX = 64

# Remember an explicitly claimed name in the current project directory.
# This file is a reminder for future explicit claims, not an automatic identity
# source derived from a folder, slug or workspace.
RELAY_NAME_FILE = ".relay-name.json"


def _validate_name(name: str) -> Optional[str]:
    """Return None if valid, else error string."""
    if not name:
        return "name is empty"
    if len(name) > NAME_MAX:
        return f"name exceeds {NAME_MAX} chars (got {len(name)})"
    if not NAME_RE.fullmatch(name):
        return "name must contain only alphanumeric, '.', '_', '-' (no spaces, newlines, or other punctuation)"
    return None


def _resolve_session_id() -> str:
    """Resolve current CC session_id via CLAUDE_CODE_SESSION_ID env var.

    Per C-13 (no auto-derive) + F-CC11-F-CC16 (research.md): CC injects
    CLAUDE_CODE_SESSION_ID into every subprocess env at process-spawn time.
    Verified empirically on Windows Git Bash via 9-test deep-inheritance probe;
    inferred HIGH confidence on Linux/macOS via binary symmetry.

    Pre-C-13 used pid-walk (resolve_session_id_via_pid_chain). Removed:
    - pid-walk: undocumented + broken on Windows ($PPID=1, orphaned subprocess)
      per F-CC11; was confabulated invention per F-CC5/F-CC8.

    Fail loud if env var unset (script must run inside a CC session, OR caller
    must set CLAUDE_CODE_SESSION_ID explicitly).
    """
    sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if sid:
        return sid
    sys.stderr.write(
        "relay-name: CLAUDE_CODE_SESSION_ID env var not set. This script must "
        "run inside a Claude Code session (CC injects this env var into every "
        "subprocess at spawn time per F-CC12). If running outside CC, set "
        "CLAUDE_CODE_SESSION_ID=<sid> explicitly before invoking.\n"
    )
    sys.exit(1)


def _holder_liveness_label(sid: str, last_seen_ts: object, now_ms: int) -> str:
    """Human-readable liveness for a conflicting name-holder (DIAGNOSTIC ONLY —
    not a gate). Background agents: the CC daemon job-state 'state'
    (stopped/done/running). Foreground: a live process mapping the sid in
    ~/.claude/sessions/*.json. Else an idle-age hint from last_seen_ts."""
    try:
        st = resolve_daemon_job_state(sid)
    except Exception:
        st = None
    if st:
        return f"job-state={st}"
    # Foreground live-process probe: a live CC process writes
    # ~/.claude/sessions/<pid>.json. CC's current schema keys the sid as
    # "sessionId" (camelCase, verified v2.1.x); older readers used "session_id"
    # (e.g. send.py:_is_session_active) — accept BOTH so a live foreground
    # holder is not mislabeled "idle".
    try:
        if SESSIONS_DIR.is_dir():
            for sf in SESSIONS_DIR.glob("*.json"):
                try:
                    d = json.loads(sf.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if isinstance(d, dict) and sid in (d.get("sessionId"), d.get("session_id")):
                    return "live-process"
    except OSError:
        pass
    try:
        age_h = (now_ms - int(last_seen_ts or 0)) / 3_600_000
        return f"idle {age_h:.1f}h (no live signal)"
    except (TypeError, ValueError):
        return "unknown"


# _TERMINAL_JOB_STATES lives in shared (canonical single source, ADR D54); this
# skill no longer references it directly (dead import removed).


def _live_bg_jobstate_holders(name: str, own_sid: str, jobs_dir=None) -> list:
    """Enumerate LIVE background-agent holders of `name` (other than own_sid)
    that the CANONICAL router `shared.py:resolve_daemon_job_name` would route to
    `name`. Background agents resolve their routing name job-state-FIRST
    (shared.py:_resolve_name_from_registry), so a live bg holder keeps routing to
    `name` even after its relay-registry row is demoted, and a bg agent named
    only via job-state has no registry row at all — we surface these so the
    conflict diagnostic is honest. They are NOT relay-registry-demotable (the CC
    daemon owns that file and re-asserts the name on respawn).

    Attribution is delegated to resolve_daemon_job_name / resolve_daemon_job_state
    so the scanner can never diverge from the actual router (no under- OR
    over-detection): we enumerate CANDIDATE sids from the jobs dir, then confirm
    each via the resolver. `jobs_dir` is injectable for tests; default is the
    live layout. Returns list of (sid, state)."""
    holders = []
    if jobs_dir is None:
        jobs_dir = Path.home() / ".claude" / "jobs"
    try:
        if not jobs_dir.is_dir():
            return holders
        seen = set()
        for d in jobs_dir.iterdir():
            try:
                s = json.loads((d / "state.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, ValueError):
                continue
            if not isinstance(s, dict):
                continue
            # Candidate owner sid: an explicit sessionId, else a full-id dir name
            # (a full-id dir proves ownership even without a sessionId field). A
            # short-prefix dir without a sessionId cannot be owned by the resolver.
            sj = s.get("sessionId")
            dirname = d.name
            if isinstance(sj, str) and sj:
                cand = sj
            elif len(dirname) >= 32:
                cand = dirname
            else:
                continue
            if cand == own_sid or cand in seen:
                continue
            seen.add(cand)
            # Confirm via the canonical router: include ONLY if the resolver would
            # route this sid to `name` (handles strip, nameSource/template gating,
            # and jobs/<sid>-before-jobs/<sid[:8]> precedence identically).
            if resolve_daemon_job_name(cand) != name:
                continue
            # Liveness via the corrected AGGREGATING probe (_is_sid_live checks
            # BOTH job-state candidate dirs), NOT resolve_daemon_job_state's
            # first-match — a stale OWNING terminal jobs/<full-sid> must not mask a
            # running jobs/<sid[:8]> for the same holder (closes the D61 warning-
            # fidelity LOW; the delete/demote decision already used _is_sid_live).
            if not _is_sid_live(cand):
                continue
            st = resolve_daemon_job_state(cand)
            holders.append((cand, st if isinstance(st, str) else "unknown"))
    except OSError:
        pass
    return holders




def _acquire_lock(fh) -> bool:
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
            # Fix #4: use LOCK_NB in retry loop matching Windows discipline.
            # Plain LOCK_EX blocks forever on POSIX — asymmetric worst case.
            for _ in range(10):
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return True
                except (BlockingIOError, OSError):
                    time.sleep(0.05)
            return False
    except Exception:
        return False


def _release_lock(fh) -> None:
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


def _read_registry() -> dict:
    if not REGISTRY_PATH.exists():
        return {}
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _write_registry_atomic(data: dict) -> None:
    """Atomic write: temp file in same dir + os.replace. Unique tmp name avoids
    collisions when the sentinel lock acquisition fails (Fix #2)."""
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        prefix=".registry.", suffix=".tmp", dir=str(REGISTRY_PATH.parent)
    )
    tmp_path = Path(tmp_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, REGISTRY_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _write_relay_name_file(name: str, sid: str, now_ms: int) -> Optional[Path]:
    """Persist {name, set_by_sid, set_at} to <cwd>/.relay-name.json.

    Future sessions can display the remembered name and ask the user to claim
    it explicitly. A write failure warns but never fails the current registry
    claim. Atomic replacement prevents concurrent readers seeing a torn file.
    Returns the written path, or None on failure.
    """
    try:
        target_dir = Path(os.getcwd())
        target = target_dir / RELAY_NAME_FILE
        payload = {"name": name, "set_by_sid": sid, "set_at": now_ms}
        fd, tmp_str = tempfile.mkstemp(
            prefix=".relay-name.", suffix=".tmp", dir=str(target_dir)
        )
        tmp_path = Path(tmp_str)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, target)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        return target
    except Exception as e:
        sys.stderr.write(
            f"set_name.py: WARNING - could not persist {RELAY_NAME_FILE} to cwd "
            f"({type(e).__name__}: {e}). The name is set in the registry for THIS "
            "session, but future sessions cannot read the remembered name from "
            "this directory. The registry claim itself succeeded.\n"
        )
        return None


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    p = argparse.ArgumentParser(
        prog="relay-name",
        description="Set or update the friendly name for the current CC session.",
    )
    p.add_argument("--name", required=True, help=f"New name (max {NAME_MAX} chars; alphanumeric + . _ -)")
    p.add_argument(
        "--reclaim", "--force", dest="reclaim", action="store_true",
        help="Take over the name even if another session holds it (prior "
             "holder demoted to name=None). Use only after confirming the "
             "takeover with the user.",
    )
    args = p.parse_args()

    err = _validate_name(args.name)
    if err:
        sys.stderr.write(f"set_name.py: invalid --name: {err}\n")
        sys.exit(2)

    # ENTRY-POINT FOLD (C-48). `--name` is one of the two ARGUMENTS a relay name
    # enters a process through, and this script is the only writer of the relay
    # registry's `name` field — so folding here is what makes the stored value
    # canonical for every reader (the receive hook, the MCP plugin, send.py)
    # without any of them having to stay in step with the others.
    #
    # AFTER validation, deliberately: `_validate_name` reports against the string
    # the user actually typed, so a rejected name is quoted back as entered. The
    # fold cannot turn a valid name invalid — NAME_RE's alphabet is closed under
    # A-Z -> a-z — so the order costs nothing and keeps the diagnostic honest.
    #
    # Everything downstream inherits the canonical name: the project reminder,
    # broker display_name and holder scans, whose operands are also folded.
    args.name = normalize_name(args.name)

    session_id = _resolve_session_id()
    now_ms = int(time.time() * 1000)
    relay_name_file: Optional[Path] = None  # set inside the lock, after the registry write

    RELAY_DIR.mkdir(parents=True, exist_ok=True)
    # Sentinel lock file (NOT registry.json itself) — on Windows os.replace()
    # refuses to overwrite a file held open in the same process. See
    # register_session.py for the full discipline.
    lock_path = REGISTRY_PATH.with_suffix(".json.lock")
    lock_path.touch(exist_ok=True)
    with lock_path.open("r+", encoding="utf-8") as lock_fh:
        locked = _acquire_lock(lock_fh)
        if not locked:
            sys.stderr.write(
                "set_name.py: could not acquire the registry lock after retries. "
                "Refusing to read-modify-write the name registry unlocked — a "
                "concurrent claim could race the conflict check and let two live "
                "sessions silently hold the same name. Re-run in a moment.\n"
            )
            sys.exit(4)
        try:
            registry = _read_registry()

            # Two routing-identity sources can hold a name: the relay registry
            # (user_set rows — demotable here) and the CC daemon job-state (live
            # background agents — the daemon owns that file and re-asserts the
            # name on respawn, so its job-state route is NOT releasable by us).
            # Routing resolution is job-state-FIRST (shared.py:
            # _resolve_name_from_registry), so the conflict set mirrors BOTH
            # sources. A sid can appear in both. Liveness is informational.
            # ----- C-25 reclaim-by-default -----------------------------------
            # An explicit /relay-name IS the user's authorization to own this
            # name: claim it here and CLEAN UP every
            # other row holding it. Scope = the NAME field (NOT just user_set
            # rows) so stale non-user_set duplicates are removed too. Per holder:
            #   - PROVABLY DEAD (not _is_sid_live)  -> DELETE the row entirely
            #     ("mark all other sessions with that name as stale and remove
            #     them"); kills the duplicate-row accumulation at the source.
            #   - LIVE (foreground or bg)           -> demote (name=None) + WARN.
            #     Never silently break a maybe-live session; a live bg daemon
            #     re-asserts its name on respawn and is not registry-deletable.
            # _is_sid_live fails toward LIVE on a probe error, so uncertainty ->
            # demote+warn (conservative), never delete. No exit-3 / no --reclaim
            # required (flag kept as a no-op back-compat alias); exit-4 (lock
            # failure) above is unchanged.
            bg_holders = _live_bg_jobstate_holders(args.name, session_id)
            bg_live_sids = {s for s, _ in bg_holders}
            # Candidate holder sids: any OTHER registry row named args.name, plus
            # any live bg job-state holder (which may have no registry row at all).
            holder_sids: list[str] = []
            for other_sid, other_entry in registry.items():
                if other_sid == session_id:
                    continue  # own entry — rename is allowed
                # BOTH OPERANDS FOLDED. `args.name` was folded at its entry point
                # above; the STORED name is folded here because rows written
                # before this change shipped still carry their original casing,
                # and `_read_registry` returns the raw JSON.
                #
                # THIS EXACT LINE IS WHY THE PREDECESSOR WAS REVERTED. That
                # version folded only the request, so claiming a name stored as
                # `Alpha` compared 'alpha' against 'Alpha', matched nothing, and
                # left TWO rows holding the name — strictly worse than the
                # unfolded code, which at least disposed of the one it found.
                if (isinstance(other_entry, dict)
                        and normalize_name(other_entry.get("name")) == args.name):
                    holder_sids.append(other_sid)
            for s in bg_live_sids:
                if s != session_id and s not in holder_sids:
                    holder_sids.append(s)

            removed_dead: list[str] = []
            demoted_live: list[tuple[str, str, bool, bool, bool]] = []  # sid,label,is_bg,had_row,row_demotable
            reclaimed_from: list[str] = []  # sids whose registry ROW we mutated
            for h_sid in holder_sids:
                entry = registry.get(h_sid)
                had_row = isinstance(entry, dict)
                # A holder reaches this loop by EITHER scan above, and only ONE
                # of them proves the row holds the claimed name: the registry
                # scan matched on the row's own folded `name`, while the bg scan
                # matched on the CC daemon's job-state name -- which the daemon
                # owns and this script cannot write. So a background agent that
                # renamed itself carries job-state 'alpha' and registry 'gamma'
                # at once, and disposing its row on the strength of the job-state
                # match destroys a name nobody reclaimed, leaving that session
                # unaddressable. Gate on what the ROW says, not on whether a row
                # exists. (T52)
                row_demotable = had_row and (
                    normalize_name(entry.get("name")) == args.name
                )
                live = (h_sid in bg_live_sids) or _is_sid_live(h_sid)
                if live:
                    label = _holder_liveness_label(
                        h_sid,
                        (entry or {}).get("last_seen_ts"),
                        now_ms,
                    )
                    # Demote a row that actually HOLDS the name, so no stale
                    # routing name survives; a live bg daemon route (no such
                    # row) is warned only.
                    if row_demotable:
                        entry["name"] = None
                        entry.pop("naming_status", None)
                        registry[h_sid] = entry
                        reclaimed_from.append(h_sid)
                    demoted_live.append(
                        (h_sid, label, h_sid in bg_live_sids, had_row, row_demotable)
                    )
                else:
                    # Provably dead -> remove the row entirely (C-25 "remove
                    # them"). A non-matching row cannot reach here today: the bg
                    # scan pre-filters on liveness, so its holders are forced
                    # live above. Gated on row_demotable anyway, so "never touch
                    # a row that does not hold the claimed name" holds by
                    # construction rather than by an accident of the other scan.
                    if row_demotable:
                        del registry[h_sid]
                        removed_dead.append(h_sid)
                        reclaimed_from.append(h_sid)

            for h_sid, label, _is_bg, had_row, row_demotable in demoted_live:
                if row_demotable:
                    tail = "its registry row was demoted to no-name."
                elif had_row:
                    tail = ("its registry row holds a DIFFERENT name and was "
                            "left alone; only the daemon route is affected.")
                else:
                    tail = "no registry row to demote (bg daemon route only)."
                sys.stderr.write(
                    f"set_name.py: WARNING - reclaimed {args.name!r} from a LIVE "
                    f"holder sid={h_sid} [{label}]; {tail}\n"
                )
            bg_note_sids = [s for s, _l, is_bg, _hr, _rd in demoted_live if is_bg]
            if bg_note_sids:
                sys.stderr.write(
                    "set_name.py: NOTE - "
                    f"{len(bg_note_sids)} live background-agent holder(s) keep "
                    "routing via the CC daemon until stopped (not registry-"
                    "deletable): "
                    + ", ".join(bg_note_sids) + "\n"
                )
            if removed_dead:
                sys.stderr.write(
                    f"set_name.py: removed {len(removed_dead)} stale dead row(s) "
                    f"holding {args.name!r}.\n"
                )

            existing = registry.get(session_id)
            old_name: Optional[str] = None
            if isinstance(existing, dict):
                old_name = existing.get("name")
                existing["name"] = args.name
                existing["last_seen_ts"] = now_ms
                # User explicitly named this session: lock naming_status to
                # 'user_set'. Subsequent SessionStart fires (e.g., /compact +
                # /restore) preserve this — register_session.py honors the
                # user_set guard. Also suppresses the naming-nudge per APPENDIX
                # 3 + 4 (_build_naming_nudge filters on naming_status='user_set').
                existing["naming_status"] = "user_set"
                registry[session_id] = existing
            else:
                registry[session_id] = {
                    "name": args.name,
                    "workspace": os.getcwd(),
                    "kind": "unknown",
                    "entrypoint": "unknown",
                    "registered_at": now_ms,
                    "last_seen_ts": now_ms,
                    "naming_status": "user_set",
                }
            _write_registry_atomic(registry)
            # C-26 + concurrency (MEDIUM-1 fix): persist the authoritative per-
            # project name file INSIDE the registry lock so a concurrent same-cwd
            # claim cannot land its file write out of order. The session that writes
            # the registry LAST also writes the file last, so the registry name and
            # the restore file always agree (no lost-update where registry=beta but
            # .relay-name.json=alpha). cwd = the project dir under the CLI workflow.
            # Best-effort: warn-on-fail, never fails the claim (registry already
            # written). Local file I/O only — no broker/network I/O under the lock.
            relay_name_file = _write_relay_name_file(args.name, session_id, now_ms)
        finally:
            if locked:
                _release_lock(lock_fh)

    # Lock is released — propagate display_name to broker (v1.3.1 / D29).
    # Broker IO MUST happen outside the lock so other registry writers aren't
    # blocked by network latency. The helper itself silent-degrades; this
    # try/except is defense-in-depth.
    try:
        post_status_with_display_name(session_id, args.name, event_label="rename")
    except Exception:
        sys.stderr.write(
            "set_name.py: post_status_with_display_name raised unexpectedly; continuing\n"
        )

    # ACTIVE PLAN Site N: write pending-mcp-reconnect marker file.
    # Receive-hook reads this in shared.py:_build_pending_mcp_reconnect_advisory
    # and injects a one-line nudge until user runs /mcp reconnect (which respawns
    # MCP subprocess → new env var → new friendly_name → push works under new
    # name). Marker is auto-cleared by register_session.py on next SessionStart
    # OR auto-stale after 1h (advisory helper). Best-effort; non-fatal.
    # Write only when an existing name actually changed. Re-claiming the same
    # name is a no-op for the running MCP process and must not emit a false
    # reconnect advisory.
    # THE PREDICATE IS `old_name is not None and ...`, DELIBERATELY, NOT the naive
    # `old_name != args.name`: old_name is None for a DEGRADED session (no registry
    # row, or a row whose name is None), and `None != "broker"` is True -- so the
    # naive form would keep firing the false banner for exactly the case the plugin
    # already self-heals ("activating channel PUSH (no /mcp reconnect needed)").
    name_changed = old_name is not None and old_name != args.name
    if name_changed:
        try:
            pending_dir = RELAY_DIR / "pending-mcp-reconnect"
            pending_dir.mkdir(parents=True, exist_ok=True)
            marker = pending_dir / f"{session_id}.flag"
            marker.write_text(
                json.dumps({"new_name": args.name, "ts": now_ms}, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass  # advisory only; failure non-fatal

    receipt = {
        "session_id": session_id,
        "old_name": old_name,
        "new_name": args.name,
        "relay_name_file": str(relay_name_file) if relay_name_file else None,
        "reclaimed_from": reclaimed_from,
        # Same gate as the marker: an unconditional reconnect demand on a no-op
        # re-claim is the stdout half of the same false alarm.
        "next_step": (
            (
                "Run `/mcp reconnect` to refresh the local-relay-channel "
                "subscription under the new friendly_name. Push notifications "
                "addressed to the new name will not arrive until you reconnect."
            )
            if name_changed else
            (
                "No action needed: this session already held that name, so the "
                "local-relay-channel subscription is unchanged and push keeps "
                "working. Do NOT run /mcp reconnect on account of this receipt."
            )
        ),
    }
    sys.stdout.write(json.dumps(receipt))
    sys.stdout.write("\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
