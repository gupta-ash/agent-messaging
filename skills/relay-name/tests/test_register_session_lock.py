# -*- coding: utf-8 -*-
"""register_session.main() lock-failure safety (HIGH#2): on lock-acquisition
failure, main() must WRITE NOTHING. An unlocked full-file registry RMW could
clobber a concurrent locked writer that just claimed a name (a real race at
single-user/home scale, C-28: several CC sessions hit SessionStart near-
simultaneously). The seeded registry is left byte-UNCHANGED and the new session
is NOT added; it registers on a later locked SessionStart.

Restored + reworked for post-D60/C-26 code: the prior version lived in
test_inherit_and_liveness.py and went through a `_wire_main` that patched
register_session._is_sid_live -- a symbol removed with the deleted inherit path --
so it ERRORED rather than covering this branch. main() no longer calls _is_sid_live,
so this focused version drops that seam and exercises only the lock-skip-write path.

Imports the DEPLOYED relay lib (~/.claude/hooks -> ~/.agents/hooks symlink);
REGISTRY_PATH / RELAY_DIR / CURSORS_DIR / probes are redirected to tmp so the real
~/.claude is never touched.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))

import register_session  # noqa: E402

WS = "C:/workspaces/proj"
NEW = "new-sid-0000"
PRED = "pred-sid-1111"


def _entry(name, ns, ws, last_seen=1):
    e = {"name": name, "workspace": ws, "last_seen_ts": last_seen,
         "kind": "unknown", "entrypoint": "unknown", "registered_at": last_seen}
    if ns is not None:
        e["naming_status"] = ns
    return e


def _wire_main(monkeypatch, tmp_path, reg_seed):
    """Redirect every external seam main() touches to tmp/no-op so the run is
    hermetic. Deliberately does NOT patch _acquire_lock -- the test sets that."""
    reg_path = tmp_path / "registry.json"
    reg_path.write_text(json.dumps(reg_seed), encoding="utf-8")
    monkeypatch.setattr(register_session, "REGISTRY_PATH", reg_path)
    monkeypatch.setattr(register_session, "RELAY_DIR", tmp_path)
    monkeypatch.setattr(register_session, "CURSORS_DIR", tmp_path / "cursors")
    monkeypatch.setattr(register_session, "read_hook_stdin", lambda: {"cwd": WS})
    monkeypatch.setattr(register_session, "resolve_session_id", lambda payload: NEW)
    monkeypatch.setattr(register_session, "_read_session_meta", lambda ppid: {})
    monkeypatch.setattr(register_session, "resolve_daemon_job_name", lambda sid: None)
    monkeypatch.setattr(register_session, "post_status_with_display_name", lambda *a, **k: None)
    monkeypatch.setattr(register_session, "emit_hook_response", lambda *a, **k: None)
    monkeypatch.setattr(register_session, "_registry_gc_due", lambda now: False)
    return reg_path


def test_main_lock_failure_skips_registry_write(monkeypatch, tmp_path):
    """HIGH#2: lock-acquire failure -> main() writes NOTHING (registry byte-identical;
    predecessor NOT deleted, new session NOT added)."""
    seed = {PRED: _entry("broker", "user_set", WS)}
    reg_path = _wire_main(monkeypatch, tmp_path, seed)
    before = reg_path.read_text(encoding="utf-8")
    monkeypatch.setattr(register_session, "_acquire_lock", lambda fh: False)

    register_session.main()

    after = reg_path.read_text(encoding="utf-8")
    # No write happened -> registry byte-identical; predecessor NOT deleted, new
    # session NOT added.
    assert after == before
    reg = json.loads(after)
    assert NEW not in reg
    assert PRED in reg and reg[PRED]["name"] == "broker"


def test_main_locked_writes_new_session(monkeypatch, tmp_path):
    """Positive control: with the lock HELD, main() DOES persist the new nameless
    row (proves the skip above is the lock-failure path, not a wiring artifact)."""
    reg_path = _wire_main(monkeypatch, tmp_path, {})
    monkeypatch.setattr(register_session, "_acquire_lock", lambda fh: True)
    monkeypatch.setattr(register_session, "_release_lock", lambda fh: None)

    register_session.main()

    reg = json.loads(reg_path.read_text(encoding="utf-8"))
    assert NEW in reg                       # locked -> the write happened
    assert reg[NEW].get("name") is None     # C-13/C-26: fresh row is NAMELESS
