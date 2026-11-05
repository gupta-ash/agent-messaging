# -*- coding: utf-8 -*-
"""Unit tests for the #24 (C) sender-outbox advisory + recovery flush in the
relay receive hook (shared.py `_sender_outbox_advisory` + `_count_relay_sends_in_outbox`).

Imports the DEPLOYED relay shared lib (~/.claude/hooks/relay/shared.py) per the
fleet "faithful deploy preview" convention — sync repo source -> ~/.claude/hooks/relay
before running. Never touches the live broker/outbox (all paths monkeypatched).
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

_RELAY_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(_RELAY_DIR))
import shared  # noqa: E402


def _write_outbox(tmp_path, lines):
    ob = tmp_path / "outbox.jsonl"
    ob.write_text("".join(l + "\n" for l in lines), encoding="utf-8")
    return ob


def test_count_relay_sends_filters_endpoint(tmp_path):
    ob = _write_outbox(tmp_path, [
        '{"endpoint":"/api/event"}',
        '{"endpoint":"/api/agent/x/ack"}',
        "garbage-not-json",
        '{"endpoint":"/api/event"}',
        '{"no_endpoint":true}',
    ])
    assert shared._count_relay_sends_in_outbox(ob) == 2


def test_count_relay_sends_missing_file(tmp_path):
    assert shared._count_relay_sends_in_outbox(tmp_path / "nope.jsonl") == 0


def test_advisory_empty_outbox_no_flush_no_banner(monkeypatch, tmp_path):
    assert shared.broker_client is not None
    ob = _write_outbox(tmp_path, [])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    flushed = []
    monkeypatch.setattr(shared.broker_client, "flush_outbox", lambda *a, **k: flushed.append(1))
    assert shared._sender_outbox_advisory("UserPromptSubmit") == ""
    assert flushed == []


def test_advisory_flushes_and_banners_when_reachable(monkeypatch, tmp_path):
    ob = _write_outbox(tmp_path, ['{"endpoint":"/api/event"}', '{"endpoint":"/api/agent/x/ack"}'])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    monkeypatch.setattr(shared, "_is_broker_known_down", lambda: False)
    monkeypatch.setattr(shared, "_broker_quick_healthy", lambda *a, **k: True)

    flush_kwargs = {}

    def fake_flush(*a, **k):
        flush_kwargs.update(k)
        ob.write_text("", encoding="utf-8")  # simulate a full drain

    monkeypatch.setattr(shared.broker_client, "flush_outbox", fake_flush)
    msg = shared._sender_outbox_advisory("UserPromptSubmit")
    assert "1 relay send" in msg
    assert "recovery retry has run" in msg   # D78 #24-follow-up(1): honest "a retry ran" banner (replaced "drained"; post-flush re-scan removed)
    assert "sent" not in msg         # must NOT claim delivery for entries that merely left the outbox
    # HIGH fix: the hot-path flush is BOUNDED (<=K entries, single-shot)
    assert flush_kwargs.get("max_entries") == shared._RELAY_FLUSH_MAX
    assert flush_kwargs.get("attempts") == 1
    assert flush_kwargs.get("timeout_s") == shared._RELAY_FLUSH_TIMEOUT_S


def test_flush_outbox_bounded_max_entries(monkeypatch, tmp_path):
    # broker_client.flush_outbox(max_entries=K) replays at most K entries and
    # re-queues the rest FIFO (bounded hot-path drain).
    bc = shared.broker_client
    ob = tmp_path / "outbox.jsonl"
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(ob))
    now = int(time.time() * 1000)
    for i in range(5):
        bc._enqueue_outbox("/api/event", {"kind": "agent_message", "n": i}, f"cid-{i}", now)
    posted = []
    monkeypatch.setattr(bc, "_post", lambda **k: (posted.append(k.get("idempotency_key")) or {"ok": 1}))

    counts = bc.flush_outbox(max_entries=2, attempts=1, timeout_s=1.0)
    assert counts["flushed"] == 2 and len(posted) == 2
    assert posted == ["cid-0", "cid-1"]  # FIFO prefix drained first
    remaining = [l for l in ob.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(remaining) == 3           # the rest re-queued for the next call

    counts2 = bc.flush_outbox(max_entries=2, attempts=1, timeout_s=1.0)
    assert counts2["flushed"] == 2       # a second bounded call drains 2 more
    remaining2 = [l for l in ob.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(remaining2) == 1


def test_advisory_partial_drain_reports_preflush_count(monkeypatch, tmp_path):
    """Post-D78 (#24 follow-up 1): the banner reports the PRE-flush relay-send count
    + a generic auto-retry line; it does NOT re-scan post-flush for a specific
    'N still pending' count (that redundant O(N) scan was removed)."""
    ob = _write_outbox(tmp_path, ['{"endpoint":"/api/event"}', '{"endpoint":"/api/event"}'])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    monkeypatch.setattr(shared, "_is_broker_known_down", lambda: False)
    monkeypatch.setattr(shared, "_broker_quick_healthy", lambda *a, **k: True)

    def fake_flush(*a, **k):
        ob.write_text('{"endpoint":"/api/event"}\n', encoding="utf-8")  # one still pending

    monkeypatch.setattr(shared.broker_client, "flush_outbox", fake_flush)
    msg = shared._sender_outbox_advisory("UserPromptSubmit")
    assert "2 relay send" in msg              # pre-flush count reported
    assert "recovery retry has run" in msg    # D78 removed the redundant post-flush "N still pending" re-scan


def test_advisory_wedged_broker_no_flush(monkeypatch, tmp_path):
    # marker not known-down, but the bounded health probe fails (WEDGED broker)
    # -> NO flush (H2: never stall the user turn) + the queued banner.
    ob = _write_outbox(tmp_path, ['{"endpoint":"/api/event"}'])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    monkeypatch.setattr(shared, "_is_broker_known_down", lambda: False)
    monkeypatch.setattr(shared, "_broker_quick_healthy", lambda *a, **k: False)
    flushed = []
    monkeypatch.setattr(shared.broker_client, "flush_outbox", lambda *a, **k: flushed.append(1))
    msg = shared._sender_outbox_advisory("UserPromptSubmit")
    assert "queued" in msg.lower() and "recovery" in msg.lower()
    assert flushed == []


def test_advisory_broker_down_no_flush_queued_banner(monkeypatch, tmp_path):
    ob = _write_outbox(tmp_path, ['{"endpoint":"/api/event"}'])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    monkeypatch.setattr(shared, "_is_broker_known_down", lambda: True)
    flushed = []
    monkeypatch.setattr(shared.broker_client, "flush_outbox", lambda *a, **k: flushed.append(1))
    msg = shared._sender_outbox_advisory("UserPromptSubmit")
    assert "queued" in msg.lower() and "recovery" in msg.lower()
    assert flushed == []  # never flush while the broker is known-down


def test_advisory_turn_gate_non_userprompt(monkeypatch, tmp_path):
    ob = _write_outbox(tmp_path, ['{"endpoint":"/api/event"}'])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    flushed = []
    monkeypatch.setattr(shared.broker_client, "flush_outbox", lambda *a, **k: flushed.append(1))
    # PostToolUse / Notification fires must NOT flush or banner (turn gate)
    assert shared._sender_outbox_advisory("PostToolUse") == ""
    assert shared._sender_outbox_advisory("Notification") == ""
    assert flushed == []


def test_advisory_only_acks_queued_no_relay_banner(monkeypatch, tmp_path):
    # outbox has only ack entries (no /api/event) -> N==0 -> no advisory, no flush
    ob = _write_outbox(tmp_path, ['{"endpoint":"/api/agent/x/ack"}'])
    monkeypatch.setattr(shared.broker_client, "_outbox_path", lambda: ob)
    monkeypatch.setattr(shared, "_is_broker_known_down", lambda: False)
    flushed = []
    monkeypatch.setattr(shared.broker_client, "flush_outbox", lambda *a, **k: flushed.append(1))
    assert shared._sender_outbox_advisory("UserPromptSubmit") == ""
    assert flushed == []
