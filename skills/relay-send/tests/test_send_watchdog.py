# -*- coding: utf-8 -*-
"""Unit tests for the #24 sender-side broker-down watchdog in send.py.

Covers the new self-contained helpers (Discord token/channel/cooldown/sanitize/
alert POST/stamp) and the URLError-arm wiring (persist -> mark-down -> deduped
Discord alert -> queued_broker_down receipt -> exit 4) + the success-path
recovery flush + the import-time BROKER_BASE_URL crash-guard.

Loads the source entrypoint with an isolated home and relay library supplied by
the repository test fixtures. Never touches a live broker, real Discord, or real
user state; all external calls and state paths are monkeypatched.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.error import URLError

import pytest

_SEND_PATH = Path(__file__).resolve().parents[1] / "harness/claude/send.py"


def _load_send():
    spec = importlib.util.spec_from_file_location("relay-send_under_test", _SEND_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


send = _load_send()


# --------------------------- helpers: sanitize ---------------------------

def test_sanitize_printable_ascii_only():
    out = send._sanitize_alert_field("héllo `code`  \x07 tail", 200)
    assert all(0x20 <= ord(c) <= 0x7e for c in out)  # printable ASCII only
    assert "`" not in out
    assert "hllo" in out.replace(" ", "") or "h llo" in out  # accented char -> space


def test_sanitize_collapses_whitespace_and_truncates():
    assert send._sanitize_alert_field("a\n\n   b\tc", 200) == "a b c"
    assert len(send._sanitize_alert_field("x" * 500, 50)) == 50


def test_sanitize_non_str_never_raises():
    class Bad:
        def __str__(self):
            raise ValueError("boom")
    assert send._sanitize_alert_field(Bad()) == "?"


def test_sanitize_neutralizes_quote_and_markdown():
    out = send._sanitize_alert_field('a "q" *b* _i_ ~s~ |x| \\e `c`')
    for ch in ('"', "`", "*", "_", "~", "|", "\\"):
        assert ch not in out


# --------------------------- helpers: channel + cooldown ---------------------------

def test_alert_channel_requires_explicit_configuration(monkeypatch):
    monkeypatch.delenv("BROKER_ALERT_DISCORD_CHANNEL_ID", raising=False)
    assert send._alert_channel_id() == ""


def test_alert_channel_env_override(monkeypatch):
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "999")
    assert send._alert_channel_id() == "999"


def test_alert_channel_trims_configuration(monkeypatch):
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", " 999 ")
    assert send._alert_channel_id() == "999"


def test_cooldown_default_and_override(monkeypatch):
    monkeypatch.delenv("BROKER_SENDER_ALERT_COOLDOWN_S", raising=False)
    assert send._sender_alert_cooldown_s() == send.DEFAULT_SENDER_ALERT_COOLDOWN_S
    monkeypatch.setenv("BROKER_SENDER_ALERT_COOLDOWN_S", "42")
    assert send._sender_alert_cooldown_s() == 42.0
    monkeypatch.setenv("BROKER_SENDER_ALERT_COOLDOWN_S", "notanumber")
    assert send._sender_alert_cooldown_s() == send.DEFAULT_SENDER_ALERT_COOLDOWN_S


# --------------------------- helpers: token ---------------------------

def test_discord_token_env_wins(monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "envtok")
    assert send._discord_token() == "envtok"


def test_discord_token_from_env_file(monkeypatch, tmp_path):
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    d = tmp_path / "discord"
    d.mkdir()
    (d / ".env").write_text("FOO=bar\nDISCORD_BOT_TOKEN=filetok\nBAZ=x\n", encoding="utf-8")
    monkeypatch.setenv("DISCORD_STATE_DIR", str(d))
    assert send._discord_token() == "filetok"


def test_discord_token_absent(monkeypatch, tmp_path):
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DISCORD_STATE_DIR", str(tmp_path / "nope"))
    assert send._discord_token() is None


def test_discord_token_malformed_env_returns_none(monkeypatch, tmp_path):
    # H1 unit-level: a non-UTF-8 .env raises UnicodeDecodeError inside read_text;
    # _discord_token must swallow it (broadened except) and return None, not crash.
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    d = tmp_path / "discord"
    d.mkdir()
    (d / ".env").write_bytes(b"\xff\xfe\x00 not-utf8")
    monkeypatch.setenv("DISCORD_STATE_DIR", str(d))
    assert send._discord_token() is None


# --------------------------- helpers: cooldown stamp ---------------------------

def test_alert_due_when_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(send, "SENDER_ALERT_MARKER_PATH", tmp_path / "state" / "sender-alert.json")
    assert send._sender_alert_due() is True


def test_stamp_then_not_due_then_due(monkeypatch, tmp_path):
    marker = tmp_path / "state" / "sender-alert.json"
    monkeypatch.setattr(send, "SENDER_ALERT_MARKER_PATH", marker)
    monkeypatch.setenv("BROKER_SENDER_ALERT_COOLDOWN_S", "300")
    send._stamp_sender_alert()
    assert marker.exists()
    assert send._sender_alert_due() is False          # just stamped -> within cooldown
    marker.write_text(json.dumps({"last_sender_alert_ts": 1}), encoding="utf-8")
    assert send._sender_alert_due() is True            # ancient stamp -> due again


# --------------------------- helpers: Discord POST ---------------------------

class _FakeResp:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_post_discord_alert_payload(monkeypatch):
    captured = {}
    monkeypatch.setattr(send, "_discord_token", lambda: "tok")
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "999")

    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["headers"] = {k.lower(): v for k, v in req.header_items()}
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _FakeResp(200)

    monkeypatch.setattr(send, "urlopen", fake_urlopen)
    assert send._post_discord_alert("hello") is True
    assert captured["url"] == "https://discord.com/api/v10/channels/999/messages"
    assert captured["headers"]["authorization"] == "Bot tok"
    assert captured["body"]["allowed_mentions"] == {"parse": []}
    assert captured["body"]["flags"] == 4
    assert len(captured["body"]["content"]) <= 1900


@pytest.mark.parametrize("destination", [None, "", "  \t "])
def test_post_discord_alert_no_destination_never_reads_token_or_posts(monkeypatch, destination):
    if destination is None:
        monkeypatch.delenv("BROKER_ALERT_DISCORD_CHANNEL_ID", raising=False)
    else:
        monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", destination)

    def unexpected(*args, **kwargs):
        pytest.fail("unconfigured alerts must not read credentials or make HTTP requests")

    monkeypatch.setattr(send, "_discord_token", unexpected)
    monkeypatch.setattr(send, "urlopen", unexpected)
    assert send._post_discord_alert("x") is False


def test_post_discord_alert_no_token(monkeypatch):
    monkeypatch.setattr(send, "_discord_token", lambda: None)
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "999")
    assert send._post_discord_alert("x") is False


def test_post_discord_alert_never_raises(monkeypatch):
    monkeypatch.setattr(send, "_discord_token", lambda: "tok")
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "999")

    def boom(*a, **k):
        raise OSError("net down")

    monkeypatch.setattr(send, "urlopen", boom)
    assert send._post_discord_alert("x") is False


# --------------------------- main(): URLError arm wiring ---------------------------

def _patch_resolvers(monkeypatch):
    reg = {"bob-sid": {"name": "bob", "naming_status": "user_set", "last_seen_ts": 9_999_999_999_999}}
    monkeypatch.setattr(send, "_load_registry", lambda: reg)
    monkeypatch.setattr(send, "_resolve_sender_session_id", lambda: "sender-sid")
    monkeypatch.setattr(send, "_check_sender_gate", lambda sid, r: None)
    monkeypatch.setattr(send, "_resolve_recipient", lambda to, r, sid: ("bob", "bob-sid"))
    monkeypatch.setattr(send, "_resolve_sender_name", lambda sid, r: "alice")


def test_urlerror_persists_alerts_and_exits_4(monkeypatch, capsys):
    assert send.broker_client is not None  # import-guard held (proves setdefault worked)
    calls = {"enqueue": [], "mark": [], "alert": [], "stamp": 0, "clear": 0}
    _patch_resolvers(monkeypatch)
    monkeypatch.setattr(send, "_post_event",
                        lambda payload: (_ for _ in ()).throw(URLError("connection refused")))
    monkeypatch.setattr(send, "_mark_broker_down", lambda reason: calls["mark"].append(reason))
    monkeypatch.setattr(send, "_sender_alert_due", lambda: True)
    monkeypatch.setattr(send, "_post_discord_alert",
                        lambda content: (calls["alert"].append(content) or True))
    monkeypatch.setattr(send, "_stamp_sender_alert",
                        lambda: calls.__setitem__("stamp", calls["stamp"] + 1))
    monkeypatch.setattr(send, "_clear_sender_alert_stamp",
                        lambda: calls.__setitem__("clear", calls["clear"] + 1))
    monkeypatch.setattr(send.broker_client, "_enqueue_outbox",
                        lambda ep, payload, cid, ts, **k: calls["enqueue"].append((ep, cid, ts)))
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "hi there"])

    with pytest.raises(SystemExit) as ex:
        send.main()
    assert ex.value.code == 4
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["status"] == "queued_broker_down"
    assert out["delivered"] is False and out["queued"] is True
    assert out["to_name"] == "bob" and out["from_name"] == "alice"
    assert out["discord_alerted"] is True
    assert len(calls["enqueue"]) == 1 and calls["enqueue"][0][0] == "/api/event"
    assert calls["mark"] and "URLError" in calls["mark"][0]
    assert calls["alert"] and calls["stamp"] == 1
    assert calls["clear"] == 0                # claim held on success (no rollback)
    # the alert content is single-line + carries the distinguishable label
    assert "relay send undelivered" in calls["alert"][0]


def test_urlerror_post_fails_releases_claim(monkeypatch, capsys):
    # M2: claim the cooldown BEFORE posting; on a FAILED post, release it so the
    # next send is due again (fail toward alerting) — never a silent suppression.
    calls = {"stamp": 0, "clear": 0, "enqueue": 0}
    _patch_resolvers(monkeypatch)
    monkeypatch.setattr(send, "_post_event",
                        lambda payload: (_ for _ in ()).throw(URLError("refused")))
    monkeypatch.setattr(send, "_mark_broker_down", lambda reason: None)
    monkeypatch.setattr(send, "_sender_alert_due", lambda: True)
    monkeypatch.setattr(send, "_post_discord_alert", lambda content: False)  # Discord post FAILS
    monkeypatch.setattr(send, "_stamp_sender_alert",
                        lambda: calls.__setitem__("stamp", calls["stamp"] + 1))
    monkeypatch.setattr(send, "_clear_sender_alert_stamp",
                        lambda: calls.__setitem__("clear", calls["clear"] + 1))
    monkeypatch.setattr(send.broker_client, "_enqueue_outbox",
                        lambda *a, **k: calls.__setitem__("enqueue", calls["enqueue"] + 1))
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "hi"])

    with pytest.raises(SystemExit) as ex:
        send.main()
    assert ex.value.code == 4
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["discord_alerted"] is False
    assert calls["stamp"] == 1 and calls["clear"] == 1   # claimed then released
    assert calls["enqueue"] == 1                          # still persisted


def test_urlerror_malformed_env_does_not_crash(monkeypatch, capsys, tmp_path):
    # H1: a non-UTF-8 ~/.claude/channels/discord/.env must NOT crash the
    # broker-down path — the receipt is still emitted and exit code is 4.
    _patch_resolvers(monkeypatch)
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "999")
    monkeypatch.setattr(send, "_post_event",
                        lambda payload: (_ for _ in ()).throw(URLError("refused")))
    monkeypatch.setattr(send, "_mark_broker_down", lambda reason: None)
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    d = tmp_path / "discord"
    d.mkdir()
    (d / ".env").write_bytes(b"DISCORD_BOT_TOKEN=\xff\xfe not utf8\n")  # invalid UTF-8
    monkeypatch.setenv("DISCORD_STATE_DIR", str(d))
    monkeypatch.setattr(send, "SENDER_ALERT_MARKER_PATH", tmp_path / "state" / "sender-alert.json")
    monkeypatch.setattr(send.broker_client, "_enqueue_outbox", lambda *a, **k: None)
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "hi"])

    with pytest.raises(SystemExit) as ex:
        send.main()  # would traceback before the fix (UnicodeDecodeError uncaught)
    assert ex.value.code == 4
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["status"] == "queued_broker_down"
    assert out["discord_alerted"] is False   # no usable token -> no ping, but receipt emitted


def test_urlerror_cooldown_suppresses_alert_but_still_persists(monkeypatch, capsys):
    calls = {"enqueue": [], "alert": []}
    _patch_resolvers(monkeypatch)
    monkeypatch.setattr(send, "_post_event",
                        lambda payload: (_ for _ in ()).throw(URLError("refused")))
    monkeypatch.setattr(send, "_mark_broker_down", lambda reason: None)
    monkeypatch.setattr(send, "_sender_alert_due", lambda: False)  # cooldown active
    monkeypatch.setattr(send, "_post_discord_alert",
                        lambda content: (calls["alert"].append(content) or True))
    monkeypatch.setattr(send.broker_client, "_enqueue_outbox",
                        lambda ep, payload, cid, ts, **k: calls["enqueue"].append(ep))
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "again"])

    with pytest.raises(SystemExit) as ex:
        send.main()
    assert ex.value.code == 4
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["discord_alerted"] is False       # cooldown suppressed the ping
    assert calls["alert"] == []                  # _post_discord_alert never called
    assert len(calls["enqueue"]) == 1            # ...but the message is still persisted


def test_urlerror_no_broker_client_still_alerts_and_exits_4(monkeypatch, capsys):
    _patch_resolvers(monkeypatch)
    monkeypatch.setattr(send, "broker_client", None)  # simulate outbox unavailable
    monkeypatch.setattr(send, "_post_event",
                        lambda payload: (_ for _ in ()).throw(URLError("refused")))
    monkeypatch.setattr(send, "_mark_broker_down", lambda reason: None)
    monkeypatch.setattr(send, "_sender_alert_due", lambda: True)
    monkeypatch.setattr(send, "_stamp_sender_alert", lambda: None)
    monkeypatch.setattr(send, "_clear_sender_alert_stamp", lambda: None)
    monkeypatch.setattr(send, "_post_discord_alert", lambda content: True)
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "hi"])

    with pytest.raises(SystemExit) as ex:
        send.main()
    assert ex.value.code == 4
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["queued"] is False                # not persisted (no broker_client)
    assert "NOT persisted" in out["hint"]


# --------------------------- main(): success-path recovery flush ---------------------------

def test_success_path_flushes_when_outbox_nonempty(monkeypatch, capsys, tmp_path):
    _patch_resolvers(monkeypatch)
    monkeypatch.setattr(send, "_post_event", lambda payload: (200, {"event_id": 42}))
    ob = tmp_path / "outbox.jsonl"
    ob.write_text('{"endpoint":"/api/event"}\n', encoding="utf-8")
    monkeypatch.setattr(send.broker_client, "_outbox_path", lambda: ob)
    flushed = []
    flush_kwargs = {}
    monkeypatch.setattr(send.broker_client, "flush_outbox",
                        lambda *a, **k: (flush_kwargs.update(k), flushed.append(1)))
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "ok"])

    with pytest.raises(SystemExit) as ex:
        send.main()
    assert ex.value.code == 0
    assert flushed == [1]
    # HIGH fix: the recovery flush is BOUNDED (<=K entries, single-shot)
    assert flush_kwargs.get("max_entries") == send.SENDER_FLUSH_MAX
    assert flush_kwargs.get("attempts") == 1


def test_success_path_no_flush_when_outbox_empty(monkeypatch, capsys, tmp_path):
    _patch_resolvers(monkeypatch)
    monkeypatch.setattr(send, "_post_event", lambda payload: (200, {"event_id": 43}))
    ob = tmp_path / "outbox.jsonl"
    ob.write_text("", encoding="utf-8")
    monkeypatch.setattr(send.broker_client, "_outbox_path", lambda: ob)
    flushed = []
    monkeypatch.setattr(send.broker_client, "flush_outbox", lambda *a, **k: flushed.append(1))
    monkeypatch.setattr(sys, "argv", ["send.py", "--to", "bob", "--text", "ok"])

    with pytest.raises(SystemExit) as ex:
        send.main()
    assert ex.value.code == 0
    assert flushed == []


# --------------------------- import-time BROKER_BASE_URL crash-guard ---------------------------

def test_import_guard_broker_base_url_unset(tmp_path):
    """send.py must import even with BROKER_BASE_URL unset (it setdefaults BEFORE
    importing broker_client, which otherwise RuntimeErrors at import)."""
    env = dict(os.environ)
    env.pop("BROKER_BASE_URL", None)
    code = (
        "import importlib.util, os, sys;"
        "from pathlib import Path;"
        "p = Path(os.environ['SEND_PATH']);"
        "s = importlib.util.spec_from_file_location('s', p);"
        "m = importlib.util.module_from_spec(s);"
        "s.loader.exec_module(m);"
        "ok = bool(os.environ.get('BROKER_BASE_URL')) and (m.broker_client is not None);"
        "sys.exit(0 if ok else 1)"
    )
    env["SEND_PATH"] = str(_SEND_PATH)
    r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert r.returncode == 0, f"import-guard failed: {r.stderr}"
