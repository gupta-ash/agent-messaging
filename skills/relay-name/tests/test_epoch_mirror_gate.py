# -*- coding: utf-8 -*-
"""Confirmed-set legacy serialization and per-session mirror rate limiting.

Root conftest installs this repository's hook/plugin sources into an isolated
HOME before collection; each test further isolates its state. Name-keyed
migration is covered by tests/test_c37_seed_migration.py. Superseded epoch
stamping and mirror gating are preserved only in docs/history.
"""
from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))
import shared  # noqa: E402

_SERVER_PATH = Path.home() / ".claude" / "plugins" / "local-relay-channel" / "server.py"
_spec = importlib.util.spec_from_file_location("lrc_server_epoch", str(_SERVER_PATH))
lrc_server = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lrc_server)

MY_SID = "aaaaaaaa-1111-4222-8333-444444444444"
MY_NAME = "trading_visual"
OTHER_NAME = "trading_research"


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def relay_env(tmp_path, monkeypatch):
    """Redirect ALL relay state (shared + plugin) into tmp_path."""
    claude_dir = tmp_path / ".claude"
    relay_dir = claude_dir / "relay"
    for d in (relay_dir, relay_dir / "cursors", relay_dir / "confirmed",
              relay_dir / "mirror-state", claude_dir / "projects"):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(shared, "RELAY_DIR", relay_dir)
    monkeypatch.setattr(shared, "CURSORS_DIR", relay_dir / "cursors")
    monkeypatch.setattr(shared, "CLAUDE_DIR", claude_dir)
    monkeypatch.setattr(shared, "REGISTRY_PATH", relay_dir / "registry.json")
    # The plugin resolves confirmed paths from Path.home() — redirect it so its
    # _confirmed_dir() == shared's (cross-writer round-trip is faithful).
    monkeypatch.setattr(lrc_server.Path, "home", staticmethod(lambda: tmp_path),
                        raising=False)
    return relay_dir


def _seed_confirmed(relay_dir, sid, floor, acked=(), *,
                    epoch_name="__absent__", epoch_broken="__absent__"):
    """Write a confirmed-set file. epoch_name/epoch_broken are OMITTED unless
    explicitly provided (so we can simulate a migration/pre-upgrade file)."""
    p = relay_dir / "confirmed" / f"{sid}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    obj = {"floor": floor, "acked": list(acked)}
    if epoch_name != "__absent__":
        obj["epoch_name"] = epoch_name
    if epoch_broken != "__absent__":
        obj["epoch_broken"] = epoch_broken
    p.write_text(json.dumps(obj), encoding="utf-8")


def _read_confirmed_raw(relay_dir, sid) -> dict:
    p = relay_dir / "confirmed" / f"{sid}.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


class _FakeBrokerClient:
    """Records post_ack calls; return value is configurable (None => failure)."""
    def __init__(self, ret=object()):
        self.calls = []
        self._ret = {"acked_now": 1} if ret is None else ret

    def post_ack(self, agent_id, event_ids, *, floor=None, fast_fail=False,
                 broker_url=None):
        self.calls.append({"name": agent_id, "ids": list(event_ids),
                           "floor": floor, "fast_fail": fast_fail})
        return None if self._ret == "FAIL" else {"acked_now": len(event_ids)}


def _run_do_receive(monkeypatch, sid, broker_rows_by_identity, *,
                    name=MY_NAME, hook_event="UserPromptSubmit"):
    monkeypatch.setattr(shared, "_resolve_name_from_registry", lambda s: name)

    def fake_fetch_kind(identity, since, kind="agent_message", limit=20,
                        broker_base_url=None):
        rows = broker_rows_by_identity.get(identity, [])
        above = [r for r in rows if r.get("event_id", 0) > since
                 and r.get("kind", "agent_message") == kind]
        above.sort(key=lambda r: r.get("event_id", 0))
        return {"messages": above[:int(limit)], "next_cursor": None, "fetch_ok": True}

    monkeypatch.setattr(shared, "fetch_pending_messages", fake_fetch_kind)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"session_id": sid, "prompt": ""})))
    monkeypatch.setattr(sys, "stdout", io.StringIO())
    shared.do_receive(hook_event)


# ---------------------------------------------------------------------------
# _read_confirmed_set — epoch parse on all paths
# ---------------------------------------------------------------------------

def test_read_absent_returns_epoch_defaults(relay_env):
    r = shared._read_confirmed_set(MY_SID, default_floor=5)
    assert r["floor"] == 5 and r["epoch_name"] is None and r["epoch_broken"] is False


def test_read_not_a_dict_returns_epoch_defaults(relay_env):
    (relay_env / "confirmed" / f"{MY_SID}.json").write_text("[1,2,3]", encoding="utf-8")
    r = shared._read_confirmed_set(MY_SID, default_floor=7)
    assert r["floor"] == 7 and r["epoch_name"] is None and r["epoch_broken"] is False


def test_read_corrupt_returns_epoch_defaults(relay_env):
    (relay_env / "confirmed" / f"{MY_SID}.json").write_text("{not json", encoding="utf-8")
    r = shared._read_confirmed_set(MY_SID, default_floor=9)
    assert r["floor"] == 9 and r["epoch_name"] is None and r["epoch_broken"] is False


def test_read_valid_parses_epoch(relay_env):
    _seed_confirmed(relay_env, MY_SID, 100, epoch_name="alpha", epoch_broken=True)
    r = shared._read_confirmed_set(MY_SID)
    assert r["epoch_name"] == "alpha" and r["epoch_broken"] is True


def test_read_wrong_type_epoch_fail_closed(relay_env):
    # PRESENT non-bool epoch_broken -> True (fail-closed); wrong-type epoch_name -> None.
    p = relay_env / "confirmed" / f"{MY_SID}.json"
    p.write_text(json.dumps({"floor": 1, "epoch_name": 123, "epoch_broken": "yes"}),
                 encoding="utf-8")
    r = shared._read_confirmed_set(MY_SID)
    assert r["epoch_name"] is None and r["epoch_broken"] is True


def test_read_corrupt_floor_drops_epoch(relay_env):
    # a present-but-invalid floor decouples the epoch from its range -> drop epoch_name.
    p = relay_env / "confirmed" / f"{MY_SID}.json"
    p.write_text(json.dumps({"floor": "bad", "epoch_name": "beta"}), encoding="utf-8")
    r = shared._read_confirmed_set(MY_SID, default_floor=120)
    assert r["floor"] == 120 and r["epoch_name"] is None


# ---------------------------------------------------------------------------
# _write_confirmed_set_locked — epoch PRESERVE (defensive parity)
# ---------------------------------------------------------------------------

def test_write_confirmed_set_locked_preserves_epoch(relay_env):
    _seed_confirmed(relay_env, MY_SID, 100, epoch_name="alpha", epoch_broken=True)
    shared._write_confirmed_set_locked(MY_SID, [101])
    raw = _read_confirmed_raw(relay_env, MY_SID)
    assert raw["epoch_name"] == "alpha" and raw["epoch_broken"] is True


# ---------------------------------------------------------------------------
# #3 rate-limit marker
# ---------------------------------------------------------------------------

def test_mirror_state_changed_absent_true(relay_env):
    assert shared._mirror_state_changed(MY_SID, MY_NAME, 100, [101]) is True


def test_mirror_state_roundtrip_unchanged_false(relay_env):
    shared._write_mirror_state(MY_SID, MY_NAME, 100, [101])
    assert shared._mirror_state_changed(MY_SID, MY_NAME, 100, [101]) is False


def test_mirror_state_changed_on_any_field(relay_env):
    shared._write_mirror_state(MY_SID, MY_NAME, 100, [101])
    assert shared._mirror_state_changed(MY_SID, OTHER_NAME, 100, [101]) is True
    assert shared._mirror_state_changed(MY_SID, MY_NAME, 200, [101]) is True
    assert shared._mirror_state_changed(MY_SID, MY_NAME, 100, [101, 102]) is True


def test_mirror_state_floor_none_roundtrip(relay_env):
    shared._write_mirror_state(MY_SID, MY_NAME, None, [101])
    assert shared._mirror_state_changed(MY_SID, MY_NAME, None, [101]) is False
    assert shared._mirror_state_changed(MY_SID, MY_NAME, 100, [101]) is True


def test_rate_limit_skips_unchanged_turn(relay_env, monkeypatch):
    _seed_confirmed(relay_env, MY_NAME, 2965)
    fake = _FakeBrokerClient()
    monkeypatch.setattr(shared, "broker_client", fake)
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []})
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []})   # identical state
    assert len(fake.calls) == 1                            # 2nd turn skipped


def test_rate_limit_reposts_on_floor_advance(relay_env, monkeypatch):
    _seed_confirmed(relay_env, MY_NAME, 2965)
    fake = _FakeBrokerClient()
    monkeypatch.setattr(shared, "broker_client", fake)
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []})
    assert len(fake.calls) == 1 and fake.calls[0]["floor"] == 2965
    # advance the floor out-of-band, then re-run -> a fresh (name,floor) -> reposts.
    _seed_confirmed(relay_env, MY_NAME, 2980)
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []})
    assert len(fake.calls) == 2 and fake.calls[1]["floor"] == 2980


def test_rate_limit_records_only_on_success(relay_env, monkeypatch):
    _seed_confirmed(relay_env, MY_NAME, 2965)
    fake = _FakeBrokerClient(ret="FAIL")   # post_ack returns None (miss/outboxed)
    monkeypatch.setattr(shared, "broker_client", fake)
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []})
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []})   # marker NOT written -> retries
    assert len(fake.calls) == 2


