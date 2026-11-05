# -*- coding: utf-8 -*-
"""Tests for shared._is_sid_live(): authoritative session liveness (background
job-state OR live foreground process file), failing toward LIVE on a probe error
OR a present-but-unreadable state/session file.

The repository fixtures install the source relay library into an isolated
Claude-layout home before imports. Session/job directories and process probes
are replaced per test, so no real user state is read or written. Registry
lock-failure behavior is covered separately in test_register_session_lock.py.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "harness" / "claude"
sys.path.insert(0, str(SCRIPTS_DIR))
RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))

import shared  # noqa: E402


# --------------------------------------------------------------------------- #
# shared._is_sid_live                                                          #
# --------------------------------------------------------------------------- #

def test_is_sid_live_bg_running(monkeypatch, tmp_path):
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    jd = tmp_path / "jobs" / "s1"
    jd.mkdir(parents=True)
    (jd / "state.json").write_text(
        json.dumps({"sessionId": "s1", "state": "running"}), encoding="utf-8")
    monkeypatch.setattr(shared, "SESSIONS_DIR", tmp_path / "nope")
    assert shared._is_sid_live("s1") is True


def test_is_sid_live_bg_terminal_no_fg_is_dead(monkeypatch, tmp_path):
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    jd = tmp_path / "jobs" / "s1"
    jd.mkdir(parents=True)
    (jd / "state.json").write_text(
        json.dumps({"sessionId": "s1", "state": "stopped"}), encoding="utf-8")
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("s1") is False


def test_is_sid_live_foreground_sessionid(monkeypatch, tmp_path):
    monkeypatch.setattr(shared, "resolve_daemon_job_state", lambda s: None)
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sd = tmp_path / "sessions"
    sd.mkdir()
    (sd / "123.json").write_text(json.dumps({"pid": 123, "sessionId": "s1"}), encoding="utf-8")
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("s1") is True


def test_is_sid_live_foreground_legacy_field(monkeypatch, tmp_path):
    monkeypatch.setattr(shared, "resolve_daemon_job_state", lambda s: None)
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sd = tmp_path / "sessions"
    sd.mkdir()
    (sd / "9.json").write_text(json.dumps({"session_id": "legacy1"}), encoding="utf-8")
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("legacy1") is True


def test_is_sid_live_dead_all_probes(monkeypatch, tmp_path):
    monkeypatch.setattr(shared, "resolve_daemon_job_state", lambda s: None)
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)  # no jobs dir -> clean absent
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("ghost") is False


def test_is_sid_live_empty_sid():
    assert shared._is_sid_live("") is False


def test_is_sid_live_unexpected_probe_error_fails_toward_live(monkeypatch, tmp_path):
    """The outer per-probe `except Exception` fails toward LIVE. Simulate an
    unexpected error in the foreground probe (SESSIONS_DIR.is_dir raises)."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)  # no jobs -> job probe clean

    class _Boom:
        def is_dir(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(shared, "SESSIONS_DIR", _Boom())
    assert shared._is_sid_live("s1") is True


def test_is_sid_live_unreadable_foreground_file_fails_toward_live(monkeypatch, tmp_path):
    """HIGH#1: a PRESENT-but-unreadable session file (truncated mid-write) MIGHT
    be the sid we're checking -> uncertain -> fail toward LIVE (never delete)."""
    monkeypatch.setattr(shared, "resolve_daemon_job_state", lambda s: None)
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sd = tmp_path / "sessions"
    sd.mkdir()
    (sd / "7.json").write_text("{ this is not valid json", encoding="utf-8")  # malformed
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("someone") is True


def test_is_sid_live_absent_foreground_file_is_clean_negative(monkeypatch, tmp_path):
    """A cleanly-ABSENT sessions dir (no files) is a clean negative, NOT uncertain."""
    monkeypatch.setattr(shared, "resolve_daemon_job_state", lambda s: None)
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sd = tmp_path / "sessions"
    sd.mkdir()  # exists but empty
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("ghost") is False


def test_is_sid_live_unreadable_jobstate_fails_toward_live(monkeypatch, tmp_path):
    """HIGH#1: resolve_daemon_job_state swallows a malformed state.json into None;
    the re-probe finds the file PRESENT-but-malformed -> uncertain -> live."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    jd = tmp_path / "jobs" / "sidX"
    jd.mkdir(parents=True)
    (jd / "state.json").write_text("{ truncated", encoding="utf-8")  # malformed
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    # real resolve_daemon_job_state reads the malformed file -> None; re-probe -> uncertain
    assert shared._is_sid_live("sidX") is True


def test_is_sid_live_oserror_foreground_file_fails_toward_live(monkeypatch, tmp_path):
    """F6: an OSError on a session-file read (here a directory where a file is
    expected) is present-but-unreadable -> uncertain -> live (not just ValueError)."""
    monkeypatch.setattr(shared, "resolve_daemon_job_state", lambda s: None)
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sd = tmp_path / "sessions"
    sd.mkdir()
    (sd / "5.json").mkdir()  # a DIRECTORY named like a session file -> read_text raises OSError
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live("nobody") is True


def test_is_sid_live_jobstate_candidate_masking_fails_toward_live(monkeypatch, tmp_path):
    """F1: a valid-but-NON-owning full-id job-state candidate must NOT mask a
    present-but-malformed sid[:8] candidate for the same (maybe-live) holder.
    Without the break->continue fix this returned False (would delete a live name)."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sid = "abcdefgh-1111-2222-3333-444444444444"
    # full-id candidate: valid JSON but owned by a DIFFERENT session -> resolve None.
    full = tmp_path / "jobs" / sid
    full.mkdir(parents=True)
    (full / "state.json").write_text(
        json.dumps({"sessionId": "someone-else", "state": "running"}), encoding="utf-8")
    # sid[:8] candidate: present but malformed (mid-write) -> uncertain.
    pref = tmp_path / "jobs" / sid[:8]
    pref.mkdir(parents=True)
    (pref / "state.json").write_text("{ truncated", encoding="utf-8")
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live(sid) is True


def test_is_sid_live_terminal_fullid_does_not_mask_live_prefix(monkeypatch, tmp_path):
    """F1 (iter-3): a stale OWNING terminal jobs/<full-sid> must NOT mask a LIVE
    jobs/<sid[:8]> for the SAME sid. resolve_daemon_job_state's first-match would
    return 'stopped' and never see the running prefix candidate."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sid = "abcdefgh-9999-2222-3333-444444444444"
    full = tmp_path / "jobs" / sid
    full.mkdir(parents=True)
    (full / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "stopped"}), encoding="utf-8")
    pref = tmp_path / "jobs" / sid[:8]
    pref.mkdir(parents=True)
    (pref / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "running"}), encoding="utf-8")
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live(sid) is True


def test_is_sid_live_terminal_fullid_plus_unreadable_prefix_uncertain(monkeypatch, tmp_path):
    """F1 (iter-3): terminal owning full-id + present-but-malformed sid[:8] -> the
    prefix uncertainty must survive (not be masked by the terminal full-id) -> live."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sid = "abcdefgh-8888-2222-3333-444444444444"
    full = tmp_path / "jobs" / sid
    full.mkdir(parents=True)
    (full / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "done"}), encoding="utf-8")
    pref = tmp_path / "jobs" / sid[:8]
    pref.mkdir(parents=True)
    (pref / "state.json").write_text("{ truncated", encoding="utf-8")
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live(sid) is True


def test_is_sid_live_owning_no_state_jobfile_fails_toward_live(monkeypatch, tmp_path):
    """Parity with registry_gc._live_bg_jobstate: an OWNING job-state file with no
    usable 'state' (schema-incomplete) is uncertain -> live (the reclaim DELETE
    path uses this probe, so the bias must be under-delete)."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sid = "sidY-1111"
    jd = tmp_path / "jobs" / sid
    jd.mkdir(parents=True)
    (jd / "state.json").write_text(
        json.dumps({"sessionId": sid, "name": "broker"}), encoding="utf-8")  # no 'state'
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live(sid) is True


def test_is_sid_live_nondict_jobfile_fails_toward_live(monkeypatch, tmp_path):
    """A valid-JSON but non-dict job-state file is malformed-shape -> uncertain -> live."""
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    sid = "sidZ-2222"
    jd = tmp_path / "jobs" / sid
    jd.mkdir(parents=True)
    (jd / "state.json").write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    sd = tmp_path / "sessions"
    sd.mkdir()
    monkeypatch.setattr(shared, "SESSIONS_DIR", sd)
    assert shared._is_sid_live(sid) is True
