"""OQ-RELAY-6 / D78 Phase A — local-relay-channel plugin epoch-preserve regression.

The plugin's broker_ack FOLD path (`_write_confirmed_set_locked`) must PRESERVE the
confirmed-set accrual-epoch tag (`epoch_name` / `epoch_broken`) verbatim. The plugin
NEVER accrues the floor under a new identity — only shared.py:_advance_floor_locked
stamps the epoch. If a fold dropped the epoch, shared.py's mirror gate would either
fail-closed (suppress a legitimate floor mirror) or, worse, re-open the over-mark by
wiping a sticky `epoch_broken` marker.

Loads the source plugin from this checkout. The confirmed-set directory and
legacy cursor reader are monkeypatched, so no installed user state is touched.

Serialization convention under test (must match shared.py Phase B verbatim):
  - epoch_name omitted from payload when None; epoch_broken omitted when False.
  - _read_confirmed_set ALWAYS returns both keys (str|None / bool) on every path.
"""
import importlib.util
import json
from pathlib import Path

import pytest

PLUGIN = Path(__file__).resolve().parents[1] / "harness/claude/local-relay-channel/server.py"


def _load_server():
    spec = importlib.util.spec_from_file_location("lrc_server_under_test", str(PLUGIN))
    mod = importlib.util.module_from_spec(spec)
    # import-safe: mcp imports + server construction are gated under __main__ (server.py L808/L1611)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def server(tmp_path, monkeypatch):
    mod = _load_server()
    cdir = tmp_path / "confirmed"
    cdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(mod, "_confirmed_dir", lambda: cdir)
    # hermetic: a random test sid has no legacy plain cursor on disk -> seed floor 0
    monkeypatch.setattr(mod, "_read_legacy_cursor_floor", lambda sid: 0)
    return mod


def _seed(mod, sid, payload):
    mod._confirmed_path(sid).write_text(json.dumps(payload), encoding="utf-8")


def _read_raw(mod, sid):
    return json.loads(mod._confirmed_path(sid).read_text(encoding="utf-8"))


def test_fold_preserves_epoch_name_and_broken(server):
    """The core Phase-A guarantee: a broker_ack fold keeps epoch_name + a sticky
    epoch_broken=True intact (the cross-epoch floor the reaper must NOT trust)."""
    sid = "epoch-preserve-1"
    _seed(server, sid, {"floor": 100, "acked": [102],
                        "epoch_name": "alpha", "epoch_broken": True})
    out = server._write_confirmed_set_locked(sid, [101])  # 101 collapses run 100->102
    assert out["floor"] == 102
    raw = _read_raw(server, sid)
    assert raw["epoch_name"] == "alpha"
    assert raw["epoch_broken"] is True
    got = server._read_confirmed_set(sid)
    assert got["epoch_name"] == "alpha"
    assert got["epoch_broken"] is True


def test_fold_preserves_name_omits_default_broken(server):
    """epoch_name preserved; epoch_broken=False is OMITTED (omit-when-default)."""
    sid = "epoch-preserve-2"
    _seed(server, sid, {"floor": 10, "acked": [], "epoch_name": "beta"})
    server._write_confirmed_set_locked(sid, [11])
    raw = _read_raw(server, sid)
    assert raw["epoch_name"] == "beta"
    assert "epoch_broken" not in raw
    got = server._read_confirmed_set(sid)
    assert got["epoch_name"] == "beta"
    assert got["epoch_broken"] is False


def test_fold_no_epoch_is_failsafe(server):
    """Migration/no-epoch file: fold writes NO epoch keys; reader defaults None/False
    (=> shared.py mirror gate suppresses the floor = fail-safe under-mark)."""
    sid = "epoch-preserve-3"
    _seed(server, sid, {"floor": 5, "acked": []})
    server._write_confirmed_set_locked(sid, [6])
    raw = _read_raw(server, sid)
    assert "epoch_name" not in raw
    assert "epoch_broken" not in raw
    got = server._read_confirmed_set(sid)
    assert got["epoch_name"] is None
    assert got["epoch_broken"] is False


def test_read_wrong_types_fail_closed(server):
    """Defensive parse (fail-CLOSED, D78): non-str epoch_name -> None; a PRESENT
    non-bool epoch_broken -> True (broken) so shared.py's mirror gate suppresses.
    MUST match shared.py:_read_confirmed_set verbatim."""
    sid = "epoch-preserve-4"
    _seed(server, sid, {"floor": 0, "acked": [],
                        "epoch_name": 123, "epoch_broken": "yes"})
    got = server._read_confirmed_set(sid)
    assert got["epoch_name"] is None
    assert got["epoch_broken"] is True


def test_read_corrupt_floor_drops_epoch(server):
    """A PRESENT-but-invalid floor decouples the epoch from its accrual range ->
    drop epoch_name (fail-CLOSED); floor falls back to the legacy default."""
    sid = "epoch-preserve-7"
    _seed(server, sid, {"floor": "bad", "acked": [], "epoch_name": "beta"})
    got = server._read_confirmed_set(sid)
    assert got["epoch_name"] is None


def test_read_absent_file_returns_epoch_defaults(server):
    """Absent confirmed-set: default return path still carries the epoch keys."""
    got = server._read_confirmed_set("no-such-sid")
    assert got["epoch_name"] is None
    assert got["epoch_broken"] is False


def test_read_corrupt_file_returns_epoch_defaults(server):
    """Corrupt JSON: except-path default return still carries the epoch keys."""
    sid = "epoch-preserve-5"
    server._confirmed_path(sid).write_text("{not valid json", encoding="utf-8")
    got = server._read_confirmed_set(sid)
    assert got["epoch_name"] is None
    assert got["epoch_broken"] is False


def test_fold_preserves_epoch_when_no_new_advance(server):
    """A fold that advances nothing (id already <= floor) still round-trips the epoch."""
    sid = "epoch-preserve-6"
    _seed(server, sid, {"floor": 50, "acked": [],
                        "epoch_name": "gamma", "epoch_broken": True})
    server._write_confirmed_set_locked(sid, [50])  # <= floor, no advance
    raw = _read_raw(server, sid)
    assert raw["floor"] == 50
    assert raw["epoch_name"] == "gamma"
    assert raw["epoch_broken"] is True
