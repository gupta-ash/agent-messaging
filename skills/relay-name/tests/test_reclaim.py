# -*- coding: utf-8 -*-
"""Unit tests for set_name.main() reclaim semantics.

set_name.py resolves several names at IMPORT time:
  - module-level Paths: REGISTRY_PATH, RELAY_DIR (derived from Path.home()).
  - `from shared import (SESSIONS_DIR, post_status_with_display_name,
    resolve_daemon_job_state)` — these become module-level names on `set_name`
    once imported, so they are monkeypatchable as `set_name.<name>`.

Strategy per test:
  - monkeypatch set_name.REGISTRY_PATH + set_name.RELAY_DIR -> tmp_path,
  - monkeypatch set_name.SESSIONS_DIR -> an empty tmp dir (no live-process hits),
  - monkeypatch set_name.post_status_with_display_name -> no-op,
  - monkeypatch set_name.resolve_daemon_job_state -> controllable stub,
  - set env CLAUDE_CODE_SESSION_ID, set sys.argv, call set_name.main() inside
    pytest.raises(SystemExit), assert .code + inspect the written registry.

These tests are fully self-contained; they never touch the real
~/.claude/relay/registry.json (REGISTRY_PATH is redirected to tmp_path).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Insert the scripts dir on sys.path so `import set_name` resolves. set_name.py
# itself inserts ~/.claude/hooks/relay so its `from shared import ...` works.
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "harness" / "claude"
sys.path.insert(0, str(SCRIPTS_DIR))

# Also insert ~/.claude/hooks/relay so the REAL-helper parity tests can import
# `shared` and monkeypatch shared.CLAUDE_DIR (the canonical resolver the helper
# now delegates to reads <CLAUDE_DIR>/jobs). Mirrors test_job_state.py.
RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))

import set_name  # noqa: E402
import shared  # noqa: E402


MY_SID = "11111111-aaaa-4bbb-8ccc-111111111111"
OTHER_SID = "22222222-dddd-4eee-8fff-222222222222"
OTHER_SID_2 = "33333333-1111-4222-8333-333333333333"
BG_SID = "bgsid-1111"


def _seed_registry(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def test_liveness_label_live_process_both_field_names(tmp_path, monkeypatch):
    """_holder_liveness_label labels a live foreground holder 'live-process'
    whether the sessions file keys the sid as 'sessionId' (CC v2.1.x schema) or
    the legacy 'session_id' — and falls back to 'idle ...' when neither matches."""
    sessions = tmp_path / "sessions"
    sessions.mkdir()
    (sessions / "1.json").write_text(json.dumps({"pid": 1, "sessionId": "live-cc-sid"}), encoding="utf-8")
    (sessions / "2.json").write_text(json.dumps({"pid": 2, "session_id": "legacy-sid"}), encoding="utf-8")
    monkeypatch.setattr(set_name, "SESSIONS_DIR", sessions)
    monkeypatch.setattr(set_name, "resolve_daemon_job_state", lambda s: None)
    assert set_name._holder_liveness_label("live-cc-sid", 0, 9_999_999_999_999) == "live-process"
    assert set_name._holder_liveness_label("legacy-sid", 0, 9_999_999_999_999) == "live-process"
    assert set_name._holder_liveness_label("ghost-sid", 0, 9_999_999_999_999).startswith("idle ")


def _read_registry(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Wire up an isolated set_name environment. Returns the registry Path."""
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir(parents=True, exist_ok=True)
    registry_path = relay_dir / "registry.json"
    sessions_dir = tmp_path / "sessions"  # empty -> no live-process matches
    sessions_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(set_name, "RELAY_DIR", relay_dir)
    monkeypatch.setattr(set_name, "REGISTRY_PATH", registry_path)
    monkeypatch.setattr(set_name, "SESSIONS_DIR", sessions_dir)
    monkeypatch.setattr(
        set_name, "post_status_with_display_name",
        lambda *a, **k: None,
    )
    # Default: no daemon job-state for any sid (label falls through to idle).
    monkeypatch.setattr(set_name, "resolve_daemon_job_state", lambda sid: None)
    # Default: no LIVE background-agent job-state holders. This isolates the
    # existing registry-only tests from the real ~/.claude/jobs dir; the F1
    # tests below override this stub to inject bg holders.
    monkeypatch.setattr(
        set_name, "_live_bg_jobstate_holders", lambda *a, **k: []
    )
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", MY_SID)
    return registry_path


def _run(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exc:
        set_name.main()
    return exc.value.code


def test_no_conflict_claim(env, monkeypatch):
    """Empty registry -> claim succeeds (exit 0), entry user_set."""
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"


def test_dead_holder_no_flag_reclaims_and_deletes(env, monkeypatch):
    """C-25: a DEAD user_set holder of X, no --reclaim -> exit 0, my_sid claims
    X user_set, and the dead holder's row is REMOVED (not merely demoted). No
    exit-3 / no --reclaim friction for the user's explicit command."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X",
            "naming_status": "user_set",
            "workspace": "C:/workspaces/other",
            "last_seen_ts": 1,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: False)
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"
    # Dead holder removed entirely (C-25 "remove them").
    assert OTHER_SID not in reg


def test_dead_holder_with_reclaim_also_deletes(env, monkeypatch):
    """--reclaim is a redundant alias post-C-25: a DEAD holder is removed either
    way; the claiming sid takes X user_set."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X",
            "naming_status": "user_set",
            "workspace": "C:/workspaces/other",
            "kind": "background",
            "last_seen_ts": 42,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: False)
    code = _run(monkeypatch, ["set_name.py", "--name", "X", "--reclaim"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"
    assert OTHER_SID not in reg


def test_force_alias_also_deletes_dead_holder(env, monkeypatch):
    """--force is an alias for --reclaim; dead holder removed."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X",
            "naming_status": "user_set",
            "workspace": "C:/workspaces/other",
            "last_seen_ts": 7,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: False)
    code = _run(monkeypatch, ["set_name.py", "--name", "X", "--force"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert OTHER_SID not in reg


def test_two_dead_holders_both_removed(env, monkeypatch):
    """Two dead sids both hold X -> both rows REMOVED in one run; my_sid claims."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X", "naming_status": "user_set",
            "workspace": "E:/a", "last_seen_ts": 1,
        },
        OTHER_SID_2: {
            "name": "X", "naming_status": "user_set",
            "workspace": "E:/b", "last_seen_ts": 2,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: False)
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert OTHER_SID not in reg
    assert OTHER_SID_2 not in reg


def test_live_holder_demoted_and_warned_not_deleted(env, monkeypatch, capsys):
    """A LIVE user_set holder of X -> its row is DEMOTED (name=None), NOT deleted
    (a live session might still be routing), a WARNING is printed, my_sid claims
    X, and the demoted sid appears in reclaimed_from."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X", "naming_status": "user_set",
            "workspace": "C:/workspaces/other", "kind": "foreground", "last_seen_ts": 5,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: sid == OTHER_SID)
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    out = capsys.readouterr()
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    # Live holder demoted, NOT deleted; other fields preserved.
    assert OTHER_SID in reg
    assert reg[OTHER_SID]["name"] is None
    assert "naming_status" not in reg[OTHER_SID]
    assert reg[OTHER_SID]["workspace"] == "C:/workspaces/other"
    assert "WARNING" in out.err and OTHER_SID[:14] in out.err
    receipt = json.loads(out.out.strip())
    assert receipt["reclaimed_from"] == [OTHER_SID]


def test_own_sid_resets_same_name_no_conflict(env, monkeypatch):
    """Own sid already holds X (no other holder) -> re-set X exits 0, own entry
    is not a conflict against itself."""
    _seed_registry(env, {
        MY_SID: {
            "name": "X", "naming_status": "user_set",
            "workspace": "E:/mine", "last_seen_ts": 5,
        },
    })
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"


def test_duplicate_name_not_user_set_dead_is_cleaned(env, monkeypatch):
    """C-25 scope is the NAME field: DEAD holders of X whose naming_status is
    'default' or absent (non-user_set) are ALSO removed (they are the visible
    duplicate rows the user complained about)."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X",
            "naming_status": "default",
            "workspace": "C:/workspaces/other",
            "last_seen_ts": 1,
        },
        OTHER_SID_2: {
            "name": "X",  # naming_status ABSENT entirely
            "workspace": "C:/workspaces/other2",
            "last_seen_ts": 1,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: False)
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"
    # Dead non-user_set duplicates removed.
    assert OTHER_SID not in reg
    assert OTHER_SID_2 not in reg


# --- FIX F2: fail closed on lock-acquisition failure -----------------------

def test_lock_acquire_failure_exits_4_registry_unchanged(env, monkeypatch):
    """Lock cannot be acquired -> main() exits 4 (NEW exit code) and the
    registry file is left untouched (no read-modify-write performed)."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "Y", "naming_status": "user_set",
            "workspace": "C:/workspaces/other", "last_seen_ts": 1,
        },
    })
    before = _read_registry(env)
    monkeypatch.setattr(set_name, "_acquire_lock", lambda fh: False)
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 4
    after = _read_registry(env)
    assert after == before
    assert MY_SID not in after


# --- FIX F1: conflict detection covers live bg job-state holders -----------

def test_bg_jobstate_holder_no_flag_proceeds_and_warns(
    env, monkeypatch, capsys
):
    """C-25: a LIVE background-agent job-state holder of X (no registry row) no
    longer BLOCKS the claim (pre-C-25 this exited 3). my_sid claims X user_set, a
    warning naming the bg holder is printed, and the bg sid is NOT in
    reclaimed_from (no registry row was mutated)."""
    _seed_registry(env, {})
    monkeypatch.setattr(
        set_name, "_live_bg_jobstate_holders",
        lambda name, own_sid: [(BG_SID, "running")] if name == "x" else [],
    )
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    out = capsys.readouterr()
    assert BG_SID[:14] in out.err
    assert "background-agent" in out.err  # the daemon-route NOTE
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"
    receipt = json.loads(out.out.strip())
    assert BG_SID not in receipt["reclaimed_from"]


def test_bg_jobstate_holder_reclaim_flag_still_exit0_bg_excluded(
    env, monkeypatch, capsys
):
    """--reclaim is redundant post-C-25 but still accepted: bg holder + --reclaim
    -> exit 0, claiming sid written user_set, a WARNING about the non-demotable bg
    holder printed, and reclaimed_from == [] (bg has no demotable registry row)."""
    _seed_registry(env, {})
    monkeypatch.setattr(
        set_name, "_live_bg_jobstate_holders",
        lambda name, own_sid: [(BG_SID, "running")] if name == "x" else [],
    )
    code = _run(monkeypatch, ["set_name.py", "--name", "X", "--reclaim"])
    assert code == 0
    out = capsys.readouterr()
    # Claiming sid took the name.
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"
    # Warning about the non-demotable live bg holder.
    assert "WARNING" in out.err
    assert BG_SID[:14] in out.err
    # reclaimed_from receipt (stdout JSON) excludes the bg sid.
    receipt = json.loads(out.out.strip())
    assert BG_SID not in receipt["reclaimed_from"]
    assert receipt["reclaimed_from"] == []


def test_mixed_dead_registry_and_live_bg_holder(
    env, monkeypatch, capsys
):
    """A DEAD registry holder + a LIVE bg job-state holder -> the dead registry
    row is REMOVED (appears in reclaimed_from); the live bg holder is warned about
    and left alone (not in reclaimed_from)."""
    _seed_registry(env, {
        OTHER_SID: {
            "name": "X", "naming_status": "user_set",
            "workspace": "C:/workspaces/other", "last_seen_ts": 9,
        },
    })
    monkeypatch.setattr(set_name, "_is_sid_live", lambda sid: False)
    monkeypatch.setattr(
        set_name, "_live_bg_jobstate_holders",
        lambda name, own_sid: [(BG_SID, "running")] if name == "x" else [],
    )
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    out = capsys.readouterr()
    reg = _read_registry(env)
    # New holder claimed.
    assert reg[MY_SID]["name"] == "x"
    assert reg[MY_SID]["naming_status"] == "user_set"
    # Dead registry holder REMOVED (not demoted).
    assert OTHER_SID not in reg
    # Warning about the live bg holder.
    assert "WARNING" in out.err
    assert BG_SID[:14] in out.err
    # reclaimed_from contains the removed registry sid but NOT the bg sid.
    receipt = json.loads(out.out.strip())
    assert OTHER_SID in receipt["reclaimed_from"]
    assert BG_SID not in receipt["reclaimed_from"]
    assert receipt["reclaimed_from"] == [OTHER_SID]


# --- FIX F1 parity: REAL _live_bg_jobstate_holders mirrors resolver ---------
# These call the non-stubbed helper directly with an injected tmp jobs_dir,
# proving the conflict scan's normalization + ownership mirror
# shared.py:resolve_daemon_job_name so a routable holder is never missed.

def _seed_jobstate(jobs_dir: Path, dirname: str, state_obj: dict) -> None:
    """Create tmp/jobs/<dirname>/state.json with the given object."""
    d = jobs_dir / dirname
    d.mkdir(parents=True, exist_ok=True)
    (d / "state.json").write_text(
        json.dumps(state_obj), encoding="utf-8"
    )


FULL_SID = "aabbccdd-1111-2222-3333-444455556666"


def test_real_helper_whitespace_parity(tmp_path, monkeypatch):
    """Short-prefix dir + whitespace-padded name -> the name is .strip()ed
    before comparison (mirrors resolver), so the holder IS found under its
    full sessionId with state 'running'."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, "aabbccdd", {
        "name": "  broker  ",
        "nameSource": "user",
        "template": "bg",
        "sessionId": FULL_SID,
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert holders == [(FULL_SID, "running")]


def test_real_helper_full_id_dir_without_sessionid(tmp_path, monkeypatch):
    """Full-UUID dir (len >= 32) with NO sessionId -> the dir name proves
    ownership, so the holder is returned with sid == the dir name."""
    full_dir = "11112222-3333-4444-5555-666677778888"
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, full_dir, {
        "name": "broker",
        "nameSource": "user",
        "template": "bg",
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert holders == [(full_dir, "running")]


def test_real_helper_short_prefix_without_sessionid_skipped(tmp_path, monkeypatch):
    """Short-prefix dir (len < 32) WITHOUT a sessionId -> skipped: a bare
    8-hex prefix cannot prove ownership, so an 8-hex collision can't
    mis-attribute the holder."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, "deadbeef", {
        "name": "broker",
        "nameSource": "user",
        "template": "bg",
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert holders == []


def test_real_helper_short_prefix_dirname_sessionid_mismatch_skipped(tmp_path, monkeypatch):
    """Short-prefix dir whose basename != sessionId[:8] -> skipped: the resolver
    only ever reads jobs/<sid[:8]>, so a stale/mislabeled dir (basename
    '99998888' but sessionId 'aabbccdd-...') must NOT fabricate a phantom holder
    the resolver would never route to."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, "99998888", {
        "name": "broker",
        "nameSource": "user",
        "template": "bg",
        "sessionId": FULL_SID,  # sj[:8] == "aabbccdd" != "99998888"
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert holders == []


def test_real_helper_terminal_state_excluded(tmp_path, monkeypatch):
    """Same shape as the whitespace-parity case but state 'stopped' -> the
    holder is terminal/retired, not a live conflict -> excluded."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, "aabbccdd", {
        "name": "  broker  ",
        "nameSource": "user",
        "template": "bg",
        "sessionId": FULL_SID,
        "state": "stopped",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert holders == []


def test_real_helper_terminal_fullid_masks_running_prefix_still_included(tmp_path, monkeypatch):
    """D61 warning-fidelity LOW (#1): a bg holder whose jobs/<full-sid> is a stale
    OWNING terminal state but jobs/<sid[:8]> is running must STILL be reported as a
    live holder. Liveness now goes through the aggregating _is_sid_live, not
    resolve_daemon_job_state's first-match (which returned the terminal full-id)."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, FULL_SID, {
        "name": "broker", "nameSource": "user", "template": "bg",
        "sessionId": FULL_SID, "state": "stopped",
    })
    _seed_jobstate(jobs_dir, FULL_SID[:8], {
        "name": "broker", "nameSource": "user", "template": "bg",
        "sessionId": FULL_SID, "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    monkeypatch.setattr(shared, "SESSIONS_DIR", tmp_path / "sessions")  # no foreground
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert [h[0] for h in holders] == [FULL_SID]


def test_real_helper_own_sid_excluded(tmp_path, monkeypatch):
    """Querying with own_sid == the holder's resolved sid -> the helper omits
    the caller's own session from the conflict set."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, "aabbccdd", {
        "name": "  broker  ",
        "nameSource": "user",
        "template": "bg",
        "sessionId": FULL_SID,
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", FULL_SID, jobs_dir=jobs_dir
    )
    assert holders == []


def test_real_helper_namesource_template_gating(tmp_path, monkeypatch):
    """nameSource != 'user' OR template != 'bg' -> not a user-named bg holder,
    so it is gated out even if the name matches."""
    jobs_dir = tmp_path / "jobs"
    # nameSource is 'system', not 'user'.
    _seed_jobstate(jobs_dir, "aabbccdd", {
        "name": "broker",
        "nameSource": "system",
        "template": "bg",
        "sessionId": FULL_SID,
        "state": "running",
    })
    # template is a non-'bg' template.
    _seed_jobstate(jobs_dir, "11112222-3333-4444-5555-666677778888", {
        "name": "broker",
        "nameSource": "user",
        "template": "example-agent",
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "other", jobs_dir=jobs_dir
    )
    assert holders == []


# --- FIX1 dual-dir precedence: resolver routes full-id dir, not short prefix --

def test_real_helper_dual_dir_full_id_precedence_empty(tmp_path, monkeypatch):
    """BOTH jobs/<FULL_SID>/ (name='other') and jobs/<FULL_SID[:8]>/
    (name='broker') exist, each with sessionId=FULL_SID. The canonical resolver
    reads jobs/<FULL_SID> FIRST (full-id dir precedence), so FULL_SID routes to
    'other' — NOT 'broker'. Delegating to the resolver, the helper must NOT
    report a 'broker' holder: the short-prefix dir is shadowed by routing, so a
    pre-delegation scanner that matched on the short dir's name would have
    over-detected. Returns EMPTY."""
    jobs_dir = tmp_path / "jobs"
    _seed_jobstate(jobs_dir, FULL_SID, {
        "name": "other",
        "nameSource": "user",
        "template": "bg",
        "sessionId": FULL_SID,
        "state": "running",
    })
    _seed_jobstate(jobs_dir, FULL_SID[:8], {
        "name": "broker",
        "nameSource": "user",
        "template": "bg",
        "sessionId": FULL_SID,
        "state": "running",
    })
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    holders = set_name._live_bg_jobstate_holders(
        "broker", "someoneelse", jobs_dir=jobs_dir
    )
    assert holders == []


# --- FIX2 dual-source reclaim: a registry+bg holder still gets its row demoted --

DUAL_SID = "44444444-5555-4666-8777-888899990000"


def test_reclaim_demotes_registry_row_even_when_also_bg_holder(
    env, monkeypatch, capsys
):
    """A sid that is BOTH a user_set registry holder AND a live bg job-state
    holder -> --reclaim STILL demotes its registry row (no stale user_set row
    survives), reclaimed_from INCLUDES it, AND a WARNING about its live daemon
    route is printed. Proves FIX2: registry demotion is no longer gated on the
    sid being absent from the bg-holder set."""
    _seed_registry(env, {
        DUAL_SID: {
            "name": "broker", "naming_status": "user_set",
            "workspace": "C:/workspaces/dual", "last_seen_ts": 11,
        },
    })
    # DUAL_SID is ALSO a live bg job-state holder (both sources point at it).
    monkeypatch.setattr(
        set_name, "_live_bg_jobstate_holders",
        lambda name, own_sid: [(DUAL_SID, "running")] if name == "broker" else [],
    )
    code = _run(monkeypatch, ["set_name.py", "--name", "broker", "--reclaim"])
    assert code == 0
    out = capsys.readouterr()
    reg = _read_registry(env)
    # Claiming sid took the name user_set.
    assert reg[MY_SID]["name"] == "broker"
    assert reg[MY_SID]["naming_status"] == "user_set"
    # The dual-source holder's registry row IS demoted (FIX2 core).
    assert reg[DUAL_SID]["name"] is None
    assert "naming_status" not in reg[DUAL_SID]
    assert reg[DUAL_SID]["workspace"] == "C:/workspaces/dual"
    # WARNING about the live bg daemon route for DUAL_SID.
    assert "WARNING" in out.err
    assert DUAL_SID[:14] in out.err
    # reclaimed_from INCLUDES the dual-source sid (its registry row was demoted).
    receipt = json.loads(out.out.strip())
    assert DUAL_SID in receipt["reclaimed_from"]
def test_bg_holder_row_named_something_else_is_left_alone(env, monkeypatch, capsys):
    """T52: a LIVE background-agent holder is matched on its CC daemon job-state
    name, which need not be the name its registry row carries -- a bg agent that
    renamed itself has job-state 'x' and registry 'gamma' at once. Claiming 'x'
    must dispose of rows holding 'x' and MUST NOT touch that agent's unrelated
    'gamma' row. The live daemon route is still surfaced (C-25: never hide a
    live residual)."""
    _seed_registry(env, {
        BG_SID: {
            "name": "gamma",
            "naming_status": "user_set",
            "workspace": "C:/workspaces/bg",
            "last_seen_ts": 1,
        },
    })
    monkeypatch.setattr(
        set_name, "_live_bg_jobstate_holders",
        lambda name, own_sid: [(BG_SID, "running")] if name == "x" else [],
    )
    code = _run(monkeypatch, ["set_name.py", "--name", "X"])
    assert code == 0
    reg = _read_registry(env)
    assert reg[MY_SID]["name"] == "x"
    # THE PROPERTY UNDER TEST: the unrelated name survives intact.
    assert reg[BG_SID]["name"] == "gamma"
    assert reg[BG_SID]["naming_status"] == "user_set"
    out = capsys.readouterr()
    assert BG_SID[:14] in out.err          # C-25: live residual still disclosed
    assert "background-agent" in out.err
    assert "DIFFERENT name" in out.err     # and disclosed HONESTLY
    receipt = json.loads(out.out.strip())
    assert BG_SID not in receipt["reclaimed_from"]
