# -*- coding: utf-8 -*-
"""Unit tests for shared.resolve_daemon_job_state().

Tests the CC daemon job-state liveness probe used by set_name.py's conflict
diagnostic. resolve_daemon_job_state reads `<CLAUDE_DIR>/jobs/<candidate>/
state.json`, trying the full session_id then the 8-char prefix, with a
sessionId-match collision guard on the prefix arm.

Strategy: insert ~/.claude/hooks/relay on sys.path, import shared, monkeypatch
shared.CLAUDE_DIR -> tmp_path, fabricate jobs/<dir>/state.json files.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))

import shared  # noqa: E402


FULL_SID = "abcdef12-3456-4789-8abc-def012345678"
SID8 = FULL_SID[:8]  # "abcdef12"


def _write_state(claude_dir: Path, dir_name: str, payload) -> None:
    job_dir = claude_dir / "jobs" / dir_name
    job_dir.mkdir(parents=True, exist_ok=True)
    state_path = job_dir / "state.json"
    if isinstance(payload, str):
        state_path.write_text(payload, encoding="utf-8")
    else:
        state_path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture
def claude_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "CLAUDE_DIR", tmp_path)
    return tmp_path


def test_full_id_dir_match(claude_dir):
    """Full-session-id dir with matching/consistent sessionId -> returns state."""
    _write_state(claude_dir, FULL_SID, {"sessionId": FULL_SID, "state": "stopped"})
    assert shared.resolve_daemon_job_state(FULL_SID) == "stopped"


def test_8prefix_dir_matching_sessionid_returns_state(claude_dir):
    """8-char prefix dir whose state.json sessionId matches the queried full sid
    -> returns the state."""
    _write_state(claude_dir, SID8, {"sessionId": FULL_SID, "state": "running"})
    assert shared.resolve_daemon_job_state(FULL_SID) == "running"


def test_8prefix_dir_nonmatching_sessionid_returns_none(claude_dir):
    """8-prefix dir whose sessionId does NOT match the queried sid -> None
    (collision guard; the prefix arm requires an exact sessionId match)."""
    other_full = SID8 + "-9999-4999-8999-999999999999"
    _write_state(claude_dir, SID8, {"sessionId": other_full, "state": "running"})
    assert shared.resolve_daemon_job_state(FULL_SID) is None


def test_missing_dir_returns_none(claude_dir):
    """No jobs dir / no candidate dir -> None."""
    assert shared.resolve_daemon_job_state(FULL_SID) is None


def test_malformed_json_returns_none(claude_dir):
    """Unparseable state.json -> None (json.JSONDecodeError caught)."""
    _write_state(claude_dir, FULL_SID, "{not valid json")
    assert shared.resolve_daemon_job_state(FULL_SID) is None


def test_state_field_missing_returns_none(claude_dir):
    """state.json present + sessionId consistent but no 'state' key -> None."""
    _write_state(claude_dir, FULL_SID, {"sessionId": FULL_SID, "name": "foo"})
    assert shared.resolve_daemon_job_state(FULL_SID) is None


def test_full_id_dir_mismatched_sessionid_skips_to_prefix(claude_dir):
    """Full-id dir whose embedded sessionId contradicts the dir name is skipped
    (continue); with no valid prefix dir the result is None."""
    _write_state(
        claude_dir, FULL_SID,
        {"sessionId": "ffffffff-0000-4000-8000-000000000000", "state": "done"},
    )
    assert shared.resolve_daemon_job_state(FULL_SID) is None


def test_full_id_dir_no_sessionid_field_returns_state(claude_dir):
    """Full-id dir with no sessionId field: dir name proves ownership, defensive
    check passes -> returns state."""
    _write_state(claude_dir, FULL_SID, {"state": "stopped"})
    assert shared.resolve_daemon_job_state(FULL_SID) == "stopped"
