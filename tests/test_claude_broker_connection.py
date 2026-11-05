"""Selected Claude broker ports reach real request-construction paths, without network I/O."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HOOKS = ROOT / "harness/claude/hooks/relay"
PLUGIN = ROOT / "harness/claude/local-relay-channel/server.py"
CHECK = ROOT / "skills/relay-check/harness/claude/check.py"
SEND = ROOT / "skills/relay-send/harness/claude/send.py"
SID = "a0000000-0000-4000-8000-000000000001"


@pytest.fixture
def home(tmp_path, monkeypatch):
    user_home = tmp_path / "user-home"
    user_home.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.chdir(workspace)
    for key in ("HOME", "USERPROFILE"):
        monkeypatch.setenv(key, str(user_home))
    monkeypatch.delenv("BROKER_BASE_URL", raising=False)
    monkeypatch.syspath_prepend(str(HOOKS))
    names = ("shared", "broker_client", "broker_connection")
    previous = {name: sys.modules.pop(name, None) for name in names}
    try:
        yield user_home
    finally:
        for name in names:
            sys.modules.pop(name, None)
            if previous[name] is not None:
                sys.modules[name] = previous[name]


def _connection_path(home):
    return home / ".agents/installations/agent-messaging/connection.json"


def _install_connection(home, text):
    target = _connection_path(home)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return target


def _select_port(home, port):
    url = f"http://127.0.0.1:{port}"
    _install_connection(home, json.dumps({"schema_version": 1, "broker_url": url}))
    return url


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Response:
    def __init__(self, body=b'{"messages": [], "next_cursor": null}'):
        self.body = body
        self.status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.body


@pytest.mark.parametrize("port", [18491, 65535])
def test_shared_initializes_client_and_fetches_selected_port(home, monkeypatch, port):
    selected = _select_port(home, port)
    shared = importlib.import_module("shared")
    assert shared.DEFAULT_BROKER_BASE_URL == selected
    assert os.environ["BROKER_BASE_URL"] == selected
    assert shared.broker_client is not None
    monkeypatch.delenv("BROKER_BASE_URL")
    targets = []

    def capture(request, timeout):
        targets.append((request.get_method(), request.full_url))
        return _Response()

    monkeypatch.setattr(shared, "urlopen", capture)
    monkeypatch.setattr(shared, "_is_broker_known_down", lambda: False)
    result = shared.fetch_pending_messages(SID, since=7, limit=3)
    assert result["fetch_ok"] is True
    assert targets == [("GET", f"{selected}/api/agent/{SID}/messages?since=7&kind=agent_message&limit=3")]

    # The env-required client was initialized from that same URL, not port 8787.
    targets.clear()
    monkeypatch.setattr(shared.broker_client, "urlopen", capture)
    result = shared.broker_client.get_agent_messages(SID, since=7, limit=3)
    assert result["messages"] == []
    assert targets == [("GET", f"{selected}/api/agent/{SID}/messages?since=7&kind=agent_message&limit=3")]


@pytest.mark.parametrize("port", [18491, 65535])
@pytest.mark.asyncio
async def test_plugin_startup_and_sse_request_use_selected_port(home, monkeypatch, port):
    import anyio
    import httpx_sse

    selected = _select_port(home, port)
    plugin = _load(PLUGIN, "selected_connection_channel")
    assert plugin._broker_base_url() == selected
    assert "BROKER_BASE_URL" not in os.environ
    monkeypatch.setattr(plugin, "_resolve_session_id", lambda: SID)
    monkeypatch.setattr(plugin, "_routing_name", lambda sid: "sample")
    main_functions = []
    monkeypatch.setattr(anyio, "run", main_functions.append)
    plugin._build_and_run()
    assert os.environ["BROKER_BASE_URL"] == selected
    assert len(main_functions) == 1

    # Exercise the subscriber constructed by real plugin startup, not a copied
    # URL formula. Stop at the transport boundary before any socket is opened.
    supervisor = inspect.getclosurevars(main_functions[0]).nonlocals["name_aware_subscribe_supervisor"]
    subscriber = inspect.getclosurevars(supervisor).nonlocals["sse_subscribe_loop"]
    requests = []

    class RequestCaptured(BaseException):
        pass

    class CaptureStream:
        async def __aenter__(self):
            raise RequestCaptured

        async def __aexit__(self, *args):
            return False

    def capture(client, method, url, *, headers):
        requests.append((method, url, headers))
        return CaptureStream()

    monkeypatch.setattr(httpx_sse, "aconnect_sse", capture)
    with pytest.raises(RequestCaptured):
        await subscriber(object())
    assert requests == [("GET", selected + "/api/events", {"Last-Event-ID": "9223372036854775807"})]

    # The plugin's reply client must use the same initialized request target.
    targets = []

    def capture_post(request, timeout):
        targets.append(request.full_url)
        return _Response(b'{"event_id": 9}')

    monkeypatch.setattr(plugin.broker_client, "urlopen", capture_post)
    assert plugin.broker_client.post_event("sample", "peer", "#relay", "agent_reply", "hello") == 9
    assert targets == [selected + "/api/event"]


def test_check_helper_initializes_its_direct_client_from_selected_port(home, monkeypatch):
    selected = _select_port(home, 18491)
    check = _load(CHECK, "selected_connection_check")
    assert check._broker_base_url() == selected
    client = sys.modules["broker_client"]
    targets = []

    def capture(request, timeout):
        targets.append(request.full_url)
        return _Response()

    monkeypatch.setattr(client, "urlopen", capture)
    assert check.get_agent_messages(SID)["messages"] == []
    assert len(targets) == 1
    assert targets[0].startswith(selected + f"/api/agent/{SID}/messages?")


@pytest.mark.parametrize("consumer", ["shared", "plugin", "check"])
def test_absent_selection_uses_portable_default_and_ignores_cwd_file(home, consumer):
    # A release/workspace copy is not the selected user's installation record.
    _install_connection(Path.cwd(), "not valid JSON")
    if consumer == "shared":
        module = importlib.import_module("shared")
    elif consumer == "plugin":
        module = _load(PLUGIN, "portable_connection_channel")
    else:
        module = _load(CHECK, "portable_connection_check")
    assert module._broker_base_url() == "http://127.0.0.1:8787"


def test_send_helper_posts_to_selected_port_without_environment(home, monkeypatch):
    selected = _select_port(home, 18491)
    send = _load(SEND, "selected_connection_send")
    assert send.DEFAULT_BROKER_BASE_URL == selected
    monkeypatch.delenv("BROKER_BASE_URL")
    targets = []

    def capture(request, timeout):
        targets.append((request.get_method(), request.full_url))
        return _Response(b'{"event_id": 9}')

    monkeypatch.setattr(send, "urlopen", capture)
    payload = {"from_agent": "sample", "to_agent": "peer", "client_event_id": SID}
    assert send._post_event(payload) == (200, {"event_id": 9})
    assert targets == [("POST", selected + "/api/event")]


@pytest.mark.parametrize("consumer", ["shared", "plugin", "check"])
def test_explicit_environment_wins_even_over_malformed_selection(home, monkeypatch, consumer):
    _install_connection(home, "not valid JSON")
    monkeypatch.setenv("BROKER_BASE_URL", "http://127.0.0.1:29491/")
    if consumer == "shared":
        module = importlib.import_module("shared")
    elif consumer == "plugin":
        module = _load(PLUGIN, "override_connection_channel")
    else:
        module = _load(CHECK, "override_connection_check")
    assert module._broker_base_url() == "http://127.0.0.1:29491"
    assert os.environ["BROKER_BASE_URL"] == "http://127.0.0.1:29491/"


_INVALID_SELECTIONS = [
    "not valid JSON",
    "[]",
    "{}",
    '{"schema_version":true,"broker_url":"http://127.0.0.1:18491"}',
    '{"schema_version":2,"broker_url":"http://127.0.0.1:18491"}',
    '{"schema_version":1,"broker_url":"http://127.0.0.1:18491","extra":true}',
    '{"schema_version":1,"schema_version":1,"broker_url":"http://127.0.0.1:18491"}',
    *[json.dumps({"schema_version": 1, "broker_url": url}) for url in (
        None,
        "http://127.0.0.1:0",
        "http://127.0.0.1:65536",
        "http://127.0.0.1:018491",
        "http://localhost:18491",
        "http://192.0.2.1:18491",
        "https://127.0.0.1:18491",
        "http://user@127.0.0.1:18491",
        "http://127.0.0.1:18491/",
        "http://127.0.0.1:18491?other=1",
    )],
]


@pytest.mark.parametrize("consumer", ["shared", "plugin", "check"])
@pytest.mark.parametrize("text", _INVALID_SELECTIONS)
def test_malformed_existing_selection_fails_without_fallback(home, consumer, text):
    _install_connection(home, text)
    with pytest.raises(RuntimeError, match="Invalid installed broker connection"):
        if consumer == "shared":
            importlib.import_module("shared")
        elif consumer == "plugin":
            _load(PLUGIN, "invalid_connection_channel")._broker_base_url()
        else:
            _load(CHECK, "invalid_connection_check")
    assert "BROKER_BASE_URL" not in os.environ


def test_unreadable_existing_selection_fails_without_fallback(home, monkeypatch):
    _select_port(home, 18491)
    target = _connection_path(home)
    resolver = importlib.import_module("broker_connection")
    read_text = Path.read_text

    def denied(path, *args, **kwargs):
        if path == target:
            raise PermissionError("synthetic unreadable connection")
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(RuntimeError, match="Cannot read installed broker connection"):
        resolver.resolve_broker_base_url()
    assert "BROKER_BASE_URL" not in os.environ
