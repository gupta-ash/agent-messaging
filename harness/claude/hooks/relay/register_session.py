# -*- coding: utf-8 -*-
"""register_session.py — SessionStart hook: register/refresh registry entry.

Per task spec § register_session.py + plan § Component D.

On SessionStart fire:
1. Read stdin JSON; resolve session_id + cwd.
2. Best-effort enrich kind/entrypoint/startedAt from ~/.claude/sessions/{ppid}.json.
3. Lock-and-update ~/.claude/relay/registry.json:
   - new entry if session_id absent
   - update last_seen_ts otherwise
4. Emit a DISPLAY-ONLY identity notice (session_id + the current registry
   name + the project's relay-name from <cwd>/.relay-name.json) as
   hookSpecificOutput.additionalContext, plus sessionTitle for the UI tab.
   NEVER sets or derives a name (C-13/C-26).
   (state-marker write at ~/.claude/relay/state/{ppid}.json removed
    2026-05-11; was orphan post-D35 pid-walk deletion — no consumer.)

Silent-degrade discipline: any unexpected exception logs full traceback to
stderr, emits `{}`, exits 0 — never break a CC session because of a hook bug.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Optional

# shared.py lives next to this file
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shared import (  # noqa: E402  (post-sys.path mutation)
    CURSORS_DIR,
    REGISTRY_PATH,
    RELAY_DIR,
    SESSIONS_DIR,
    _registry_gc_due,
    _stamp_registry_gc,
    emit_hook_response,
    is_codex_hook_payload,
    post_status_with_display_name,
    read_hook_stdin,
    resolve_codex_registration,
    resolve_daemon_job_name,
    resolve_session_id,
)

# New registry rows are nameless. Only an explicit /relay-name claim sets the
# name; the per-project .relay-name.json is a reminder, never an identity source
# automatically applied by this hook. naming_status is absent or 'user_set'.

HOOK_EVENT_NAME = "SessionStart"

# Untrusted-input guard (HIGH-2): a name read from <cwd>/.relay-name.json is
# repo-supplied — any directory you cd into can ship one. Validate it with the SAME
# canonical policy as set_name.py (fullmatch of NAME_RE + NAME_MAX) BEFORE it is
# ever spliced into the SessionStart additionalContext (model context), to block
# prompt-injection / context-bloat. Literals intentionally mirror set_name.py (kept
# in sync by comment; consolidate into a shared constant after the WS-A/WS0 merge).
_NAME_RE = re.compile(r"^[A-Za-z0-9._\-]+$")
_NAME_MAX = 64


def _is_valid_relay_name(s) -> bool:
    """True iff s passes the canonical friendly-name policy (fullmatch of NAME_RE +
    NAME_MAX). EVERY name string spliced into the SessionStart additionalContext
    (model context) is guarded by this: the project-file name, the daemon job-state
    name, AND the registry-row name (which may be a legacy/poisoned value persisted
    before the writer switched to fullmatch — HIGH-2 completeness)."""
    return isinstance(s, str) and bool(s) and len(s) <= _NAME_MAX and bool(_NAME_RE.fullmatch(s))


# ---------------------------------------------------------------------------
# Registry locking — Windows uses msvcrt.locking; POSIX uses fcntl.flock.
# Falls back to no-lock RMW (still atomic-rename) if neither available.
# ---------------------------------------------------------------------------

def _acquire_lock(fh) -> bool:
    """Try to acquire an exclusive lock on the open file handle. Best-effort."""
    try:
        if os.name == "nt":
            import msvcrt
            # Lock the first byte of the file. msvcrt.locking is blocking with LK_LOCK.
            # Use LK_NBLCK in a small retry loop to avoid hanging the hook.
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
        if isinstance(data, dict):
            return data
        sys.stderr.write("relay register_session: registry not an object; treating as empty\n")
        return {}
    except (OSError, json.JSONDecodeError) as e:
        sys.stderr.write(
            f"relay register_session: registry corrupt ({type(e).__name__}: {e}); treating as empty\n"
        )
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


def _read_session_meta(ppid: int) -> dict:
    """Best-effort: read kind/entrypoint/startedAt from ~/.claude/sessions/{ppid}.json."""
    sessions_file = SESSIONS_DIR / f"{ppid}.json"
    if not sessions_file.exists():
        return {}
    try:
        data = json.loads(sessions_file.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError) as e:
        sys.stderr.write(
            f"relay register_session: sessions file {sessions_file} unreadable ({type(e).__name__}); skipping enrichment\n"
        )
    return {}


def _read_relay_name_file(cwd: str) -> Optional[str]:
    """C-26 DISPLAY support: read the project's remembered friendly name from
    <cwd>/.relay-name.json (written by /relay-name). Returns the name string,
    or None if cwd is empty / the file is absent / unreadable / malformed. This
    hook is DISPLAY-ONLY: it NEVER writes or auto-applies the name (C-13/C-26).
    The ".relay-name.json" literal is intentionally duplicated with set_name.py's
    RELAY_NAME_FILE to keep the WS-A branch disjoint from WS0's shared.py edits;
    consolidate into a shared constant after both branches merge.
    """
    if not cwd:
        return None
    try:
        f = Path(cwd) / ".relay-name.json"
        if not f.exists():
            return None
        data = json.loads(f.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            n = data.get("name")
            # HIGH-2: validate the untrusted repo-supplied name with the canonical
            # policy BEFORE returning it for display. An invalid / oversized /
            # injection-shaped name is treated as ABSENT (return None) — never
            # spliced raw into the model's additionalContext.
            if _is_valid_relay_name(n):
                return n
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    return None


# Session-id changes never inherit a name from a workspace or a sibling row.
# A remembered project name may be shown, but must be claimed explicitly with
# /relay-name. This avoids assigning a sibling project's mailbox by accident.


def _build_codex_session_start_output(payload: dict, registration: dict) -> dict:
    """Build context from the existing R3B/R5B Codex broker row.

    Codex does not support Claude's ``sessionTitle`` output field. Its hook
    ``session_id`` is the replaceable thread, while the broker row's ``id`` is
    the stable relay identity shared with the adapter and MCP tools.
    """
    thread_id = payload.get("session_id")
    agent_id = registration.get("id")
    relay_name = registration.get("relay_name")
    context = (
        f"[relay identity] stable adapter id: {agent_id} | "
        f"Codex thread_id: {thread_id} | "
        f"broker relay name: {relay_name if relay_name else '(unset)'}"
    )
    return {
        "hookSpecificOutput": {
            "hookEventName": HOOK_EVENT_NAME,
            "additionalContext": context,
        }
    }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    try:
        payload = read_hook_stdin()
    except Exception as e:
        sys.stderr.write(f"relay register_session: stdin parse failed ({type(e).__name__}: {e}); degrading\n")
        emit_hook_response(HOOK_EVENT_NAME, None)
        return

    # R3B/R5B registers the stable Codex mailbox before the remote TUI starts.
    # Reuse that row. Do not create a second row under the replaceable thread id
    # or overwrite the adapter's codex-session fields with Claude's status shape.
    if is_codex_hook_payload(payload):
        try:
            registration = resolve_codex_registration(payload)
            hook_output = _build_codex_session_start_output(payload, registration)
        except Exception as e:
            sys.stderr.write(
                "relay register_session: Codex adapter identity unresolved "
                f"({type(e).__name__}: {e}); degrading\n"
            )
            hook_output = {}
        sys.stdout.write(json.dumps(hook_output))
        sys.stdout.flush()
        return

    try:
        session_id = resolve_session_id(payload)
    except Exception as e:
        sys.stderr.write(f"relay register_session: session_id unresolved ({type(e).__name__}: {e}); degrading\n")
        emit_hook_response(HOOK_EVENT_NAME, None)
        return

    cwd = payload.get("cwd")
    if not isinstance(cwd, str):
        cwd = ""

    ppid = os.getppid()
    meta = _read_session_meta(ppid)
    now_ms = int(time.time() * 1000)

    display_name: Optional[str] = None
    registry_name: Optional[str] = None  # honest registry-row name (never the daemon fallback); for the DISPLAY notice
    try:
        RELAY_DIR.mkdir(parents=True, exist_ok=True)
        # Use a SEPARATE sentinel lock file (not the registry file itself) because
        # on Windows os.replace() refuses to overwrite a file that is currently
        # open by ANY handle in the same process — even our own. Holding the lock
        # on registry.json while trying to atomically replace it would deadlock-
        # by-EACCES. The sentinel pattern: lock registry.json.lock; modify
        # registry.json freely via tempfile + os.replace.
        lock_path = REGISTRY_PATH.with_suffix(".json.lock")
        lock_path.touch(exist_ok=True)
        with lock_path.open("r+", encoding="utf-8") as lock_fh:
            locked = _acquire_lock(lock_fh)
            if not locked:
                sys.stderr.write(
                    "relay register_session: could not acquire registry lock "
                    "after retries; SKIPPING the registry write this fire to avoid "
                    "clobbering a concurrent locked writer (a stale full-file "
                    "overwrite could revert another session's just-claimed name). "
                    "Best-effort: this session registers on the next locked "
                    "SessionStart.\n"
                )
            try:
                registry = _read_registry()
                existing = registry.get(session_id)
                if isinstance(existing, dict):
                    existing["last_seen_ts"] = now_ms
                    # Refresh cwd / kind / entrypoint best-effort if previously unknown.
                    if (not existing.get("workspace")) and cwd:
                        existing["workspace"] = cwd
                    if existing.get("kind") in (None, "unknown") and meta.get("kind"):
                        existing["kind"] = meta.get("kind")
                    if existing.get("entrypoint") in (None, "unknown") and meta.get("entrypoint"):
                        existing["entrypoint"] = meta.get("entrypoint")
                    # Preserve explicit names across session lifecycle changes.
                    pass  # name + naming_status preserved as-is for non-user_set entries
                    registry[session_id] = existing
                else:
                    # C-13/C-26: a fresh registry row is created NAMELESS. There
                    # is NO name derivation of any kind - not folder, not project-
                    # slug, not cwd-leaf, not a dead same-workspace predecessor.
                    # The project name file is display-only in this hook;
                    # an explicit /relay-name claim is the sole name source.
                    # naming-nudge in shared.py fires while name is None.
                    registry[session_id] = {
                        "name": None,
                        "workspace": cwd,
                        "kind": meta.get("kind") or "unknown",
                        "entrypoint": meta.get("entrypoint") or "unknown",
                        "registered_at": now_ms,
                        "last_seen_ts": now_ms,
                    }
                # --- AUTO duplicate-name GC (ADR D54) ------------------------
                # Demote dead DUPLICATE-name rows so friendly names stop
                # accumulating stale holders across CC's sid churn. Runs ONLY
                # under the held lock (never RMW other sids' rows unlocked) and
                # AFTER the own-row mutation above (own_sid row is final). Own
                # try/except: a GC bug degrades to "no GC this run",
                # never blocks the own-row write or aborts the hook. Throttled to
                # ~once / REGISTRY_GC_INTERVAL_S across all sessions.
                gc_ran = False
                if locked and _registry_gc_due(now_ms):
                    try:
                        from registry_gc import gc_stale_duplicate_names, gc_dead_rows
                        demoted = gc_stale_duplicate_names(
                            registry, session_id, now_ms=now_ms
                        )
                        # Row-count GC: after D54 demotes dead dup rows to nameless,
                        # delete dead + aged NON-user_set rows to bound file growth
                        # (user_set anchors survive for C-19). Same held lock.
                        deleted = gc_dead_rows(
                            registry, session_id, now_ms=now_ms
                        )
                        gc_ran = True
                        if demoted:
                            sys.stderr.write(
                                "relay register_session: registry GC demoted "
                                f"{len(demoted)} stale duplicate-name row(s)\n"
                            )
                        if deleted:
                            sys.stderr.write(
                                "relay register_session: registry GC deleted "
                                f"{len(deleted)} dead aged nameless row(s)\n"
                            )
                    except Exception:
                        sys.stderr.write(
                            "relay register_session: registry GC failed "
                            "(non-fatal); full traceback:\n"
                            + traceback.format_exc()
                        )
                # F2: only WRITE under a held lock. The GC is already lock-gated, so
                # the atomic write is the last unlocked mutation that
                # could reach disk; skipping it on lock failure prevents a stale
                # full-file overwrite from reverting a concurrent locked name claim.
                if locked:
                    _write_registry_atomic(registry)
                # Stamp the GC throttle AFTER a successful write (never before),
                # still under the lock; best-effort (must not raise).
                if gc_ran:
                    try:
                        _stamp_registry_gc(now_ms)
                    except Exception:
                        pass
                # Capture display_name AFTER write succeeds, BEFORE releasing lock.
                entry = registry.get(session_id)
                if isinstance(entry, dict):
                    name_val = entry.get("name")
                    # HIGH-2 completeness: the registry-row name is also spliced into
                    # additionalContext / sessionTitle / broker display. A poisoned
                    # row (a name persisted by the pre-fullmatch writer, or by other
                    # tooling) must NOT reach model context — guard it here and treat
                    # an invalid value as nameless. Row cleanup is left to
                    # /relay-name reclaim + registry GC (not mutated here, to keep
                    # this hook read-only w.r.t. other sids' concerns).
                    if _is_valid_relay_name(name_val):
                        display_name = name_val
                        registry_name = name_val
                    elif isinstance(name_val, str) and name_val:
                        sys.stderr.write(
                            "relay register_session: this sid's registry name failed "
                            "the name guard (poisoned/legacy); treating as nameless for "
                            "display. Run /relay-name to reset it.\n"
                        )
            finally:
                if locked:
                    _release_lock(lock_fh)
    except Exception:
        sys.stderr.write(
            "relay register_session: registry update failed; full traceback:\n"
            + traceback.format_exc()
        )
        # Continue to display_name propagation despite registry write failure.

    # Background-agent dashboard label (cosmetic): if no relay-registry name
    # resolved (the daemon assigns fresh, unnamed sids per respawn), fall back to
    # the durable name CC's daemon tracks in its job-state for this sid. Used
    # ONLY for the broker display_name + sessionTitle below — we deliberately do
    # NOT write a user_set registry entry from it (that would pollute the
    # workspace name-inheritance pool with one entry per bg respawn). MCP push
    # routing (server.py) + the receive-hook backup path (shared.py) read the
    # job-state directly at use-time, so functional routing does not depend on
    # this cosmetic step.
    if not display_name:
        try:
            _dn = resolve_daemon_job_name(session_id)
            if _dn:
                display_name = _dn
        except Exception:
            pass

    # After registry write, propagate display_name to broker
    # (v1.3.1 / D29). Silent-degrade matches existing receive-hook discipline;
    # SessionStart MUST NOT crash CC sessions on broker failure. The helper
    # itself already catches everything; this try/except is defense-in-depth
    # against unexpected exceptions (e.g., import-time errors on the call path).
    try:
        post_status_with_display_name(session_id, display_name)
    except Exception:
        sys.stderr.write(
            "relay register_session: post_status_with_display_name raised unexpectedly; "
            "full traceback:\n" + traceback.format_exc()
        )

    # Naming UX: clear the naming-nudge cursor on every SessionStart so the
    # next UserPromptSubmit re-fires the nudge if naming_status is not
    # 'user_set'; the nudge fires until the user runs /relay-name.
    # Fresh sessions, restarts and session-id changes all clear this cursor.
    # clear the cursor, all get one nudge on the next user prompt. Sessions
    # with naming_status='user_set' are filtered inside _build_naming_nudge
    # regardless of cursor state. Silent-degrade per existing relay discipline.
    try:
        CURSORS_DIR.mkdir(parents=True, exist_ok=True)
        (CURSORS_DIR / f"{session_id}-naming-nudged.txt").unlink(missing_ok=True)
    except Exception:
        pass

    # ACTIVE PLAN Site T: clear pending-mcp-reconnect marker on SessionStart.
    # If user ran /relay-name → set_name.py wrote the marker → user later
    # ran /mcp reconnect → MCP respawned → SessionStart fires → marker should
    # clear so the advisory stops firing. Silent-degrade.
    try:
        pending_dir = RELAY_DIR / "pending-mcp-reconnect"
        (pending_dir / f"{session_id}.flag").unlink(missing_ok=True)
    except Exception:
        pass

    # SessionStart hook output (C-26): DISPLAY-ONLY identity notice + UI tab title.
    # NEVER sets or derives a name (C-13/C-26) — it only shows what it can read:
    #   - session_id (always),
    #   - the current registry name for this sid (display_name; may be unset),
    #   - the project's remembered relay-name from <cwd>/.relay-name.json.
    # If the session is unnamed but the project file has a name, it NUDGES the user
    # to re-apply it with /relay-name — it does NOT auto-apply.
    # additionalContext is injected into the model context (SUPPORTED for
    # SessionStart on CC >=2.1.x; C-14-verified against the installed binary +
    # changelog); sessionTitle sets the UI tab (honored on startup/resume, ignored
    # on clear/compact). Silent-degrade: on any error, emit `{}`.
    try:
        project_relay_name = _read_relay_name_file(cwd)
        # HIGH-1: the identity notice shows the HONEST registry name only, never the
        # background-daemon fallback masquerading as the registry name. reg_name is
        # the registry row's name (None if the row is nameless).
        reg_name = registry_name
        # If display_name was filled from the CC daemon job-state (a bg agent's
        # durable name, D46) rather than the registry, surface it on a SEPARATE,
        # clearly-labeled line — validated with the same untrusted-input guard.
        daemon_name = None
        if display_name and display_name != reg_name and _is_valid_relay_name(display_name):
            daemon_name = display_name
        ctx = [
            f"session_id: {session_id}",
            f"registry name: {reg_name if reg_name else '(unset)'}",
            f"project relay-name (.relay-name.json): {project_relay_name if project_relay_name else '(none)'}",
        ]
        if daemon_name:
            ctx.append(f"background-agent name (daemon job-state): {daemon_name}")
        # Nudges fire only for a truly-nameless FOREGROUND session (no registry name
        # and no daemon name). A bg agent is daemon-named and needs no nudge.
        if reg_name and project_relay_name and reg_name != project_relay_name:
            ctx.append(
                f"NOTE: MISMATCH - registry name '{reg_name}' != project relay-name "
                f"'{project_relay_name}'. If this session should use the project "
                f"name, run /relay-name {project_relay_name}."
            )
        elif not reg_name and not daemon_name and project_relay_name:
            ctx.append(
                f"NOTE: this session is UNNAMED but this project remembers "
                f"'{project_relay_name}' - run /relay-name "
                f"{project_relay_name} to re-apply it for relay messaging."
            )
        elif not reg_name and not daemon_name and not project_relay_name:
            ctx.append(
                "NOTE: this session is UNNAMED - run /relay-name <name> to set "
                "a friendly name for relay messaging."
            )
        hook_specific = {
            "hookEventName": HOOK_EVENT_NAME,
            "additionalContext": "[relay identity] " + " | ".join(ctx),
        }
        # sessionTitle (UI tab only) keeps the existing behavior: the registry name
        # for a foreground session, or the daemon name for a bg agent (D46).
        if display_name:
            hook_specific["sessionTitle"] = display_name
        hook_output = {"hookSpecificOutput": hook_specific}
    except Exception:
        sys.stderr.write(
            "relay register_session: SessionStart display build failed "
            "(non-fatal); emitting empty output. " + traceback.format_exc()
        )
        hook_output = {}
    sys.stdout.write(json.dumps(hook_output))
    sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.stderr.write(
            "relay register_session: top-level exception; full traceback:\n"
            + traceback.format_exc()
        )
        try:
            emit_hook_response(HOOK_EVENT_NAME, None)
        except Exception:
            pass
