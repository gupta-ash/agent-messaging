"""Codex-facing MCP operations backed directly by the broker HTTP API."""

from __future__ import annotations

import builtins
import importlib
import json
import sys
import time
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from mcp.shared.memory import create_connected_server_and_client_session


MODULE_PATH = Path(__file__).resolve().parents[1] / "harness" / "codex" / "codex_relay_mcp.py"


def test_codex_relay_mcp_module_exists():
    assert MODULE_PATH.is_file(), "R3A must add the standalone Codex relay MCP server"


@pytest.fixture
def relay_module():
    sys.modules.pop("codex_relay_mcp", None)
    return importlib.import_module("codex_relay_mcp")


def _payload(result):
    assert result.isError is False, result
    assert len(result.content) == 1
    return json.loads(result.content[0].text)


async def _call(server, tool_name: str, arguments: dict):
    async with create_connected_server_and_client_session(server) as session:
        return await session.call_tool(tool_name, arguments)


@pytest.mark.asyncio
async def test_lists_session_bound_receipt_ack_with_existing_operations(relay_module):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    server = relay_module.create_server(
        agent_id="stable-codex-id",
        broker_url="http://broker.invalid",
        http_client=client,
    )
    async with create_connected_server_and_client_session(server) as session:
        listed = await session.list_tools()

    assert {tool.name for tool in listed.tools} == {
        "ack",
        "send",
        "check",
        "name",
        "register",
    }
    assert all("agent_id" not in tool.inputSchema.get("properties", {}) for tool in listed.tools)
    assert all("state" not in tool.inputSchema.get("properties", {}) for tool in listed.tools)
    await client.aclose()


@pytest.mark.asyncio
async def test_name_collision_requires_explicit_takeover(relay_module, client):
    first = relay_module.create_server(
        agent_id="codex-a", broker_url="http://test", http_client=client
    )
    second = relay_module.create_server(
        agent_id="codex-b", broker_url="http://test", http_client=client
    )
    for server in (first, second):
        result = await _call(
            server, "register", {"thread_id": "thread", "status": "running"}
        )
        assert not result.isError

    assert not (await _call(first, "name", {"relay_name": "shared"})).isError
    collision = await _call(second, "name", {"relay_name": "shared"})
    assert collision.isError
    assert "HTTP 409" in collision.content[0].text
    takeover = _payload(
        await _call(second, "name", {"relay_name": "shared", "takeover": True})
    )
    assert takeover == {
        "id": "codex-b",
        "relay_name": "shared",
        "displaced_id": "codex-a",
    }


@pytest.mark.asyncio
async def test_check_is_read_only_and_never_acknowledges(relay_module, client):
    recipient = relay_module.create_server(
        agent_id="codex-recipient", broker_url="http://test", http_client=client
    )
    sender = relay_module.create_server(
        agent_id="codex-sender", broker_url="http://test", http_client=client
    )
    assert not (
        await _call(
            recipient,
            "register",
            {"thread_id": "thread-r", "status": "running"},
        )
    ).isError
    assert not (
        await _call(recipient, "name", {"relay_name": "recipient"})
    ).isError
    assert not (
        await _call(
            sender,
            "register",
            {"thread_id": "thread-s", "status": "running"},
        )
    ).isError
    assert not (await _call(sender, "name", {"relay_name": "sender"})).isError
    event_id = _payload(
        await _call(
            sender,
            "send",
            {"to_agent": "recipient", "content": "pending"},
        )
    )["event_id"]

    first = _payload(
        await _call(recipient, "check", {"after": 0, "limit": 100})
    )
    second = _payload(
        await _call(recipient, "check", {"after": 0, "limit": 100})
    )
    assert [row["event_id"] for row in first["deliveries"]] == [event_id]
    assert [row["event_id"] for row in second["deliveries"]] == [event_id]


@pytest.mark.asyncio
async def test_ack_posts_one_exact_event_for_bound_recipient(relay_module):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"acked_now": 1, "requested": 1})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://broker.test"
    )
    server = relay_module.create_server(
        agent_id="stable-codex-id",
        broker_url="http://broker.test",
        http_client=client,
    )

    payload = _payload(await _call(server, "ack", {"event_id": 73}))

    assert payload == {"acked_now": 1, "requested": 1}
    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/api/agent/stable-codex-id/delivery-ack")
    ]
    assert json.loads(requests[0].content) == {"event_ids": [73]}
    await client.aclose()


@pytest.mark.asyncio
async def test_reply_to_send_from_agent_returns_through_stable_endpoint(
    relay_module, client
):
    sender = relay_module.create_server(
        agent_id="codex-sender", broker_url="http://test", http_client=client
    )
    recipient = relay_module.create_server(
        agent_id="codex-recipient", broker_url="http://test", http_client=client
    )
    for server, thread_id, relay_name in (
        (sender, "thread-s", "sender"),
        (recipient, "thread-r", "recipient"),
    ):
        assert not (
            await _call(
                server,
                "register",
                {"thread_id": thread_id, "status": "running"},
            )
        ).isError
        assert not (await _call(server, "name", {"relay_name": relay_name})).isError

    sent_id = _payload(
        await _call(
            sender,
            "send",
            {"to_agent": "recipient", "content": "outbound"},
        )
    )["event_id"]
    received = _payload(await _call(recipient, "check", {}))["deliveries"]
    outbound = next(row for row in received if row["event_id"] == sent_id)

    assert outbound["from_agent"] == "sender"
    reply_id = _payload(
        await _call(
            recipient,
            "send",
            {
                "to_agent": outbound["from_agent"],
                "content": "reply",
                "kind": "agent_reply",
            },
        )
    )["event_id"]
    replies = _payload(await _call(sender, "check", {}))["deliveries"]
    assert [row["event_id"] for row in replies] == [reply_id]
    assert replies[0]["content"] == "reply"


@pytest.mark.asyncio
async def test_broker_http_failure_is_an_actionable_tool_error(relay_module):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                503, json={"detail": "maintenance window"}
            )
        ),
        base_url="http://broker.test",
    )
    server = relay_module.create_server(
        agent_id="stable-id",
        broker_url="http://broker.test",
        http_client=client,
    )
    result = await _call(
        server, "send", {"to_agent": "peer", "content": "hello"}
    )
    assert result.isError
    assert "HTTP 503" in result.content[0].text
    assert "maintenance window" in result.content[0].text
    await client.aclose()


@pytest.mark.asyncio
async def test_send_requires_the_identity_to_own_a_relay_name(relay_module):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"id": "stable-id", "relay_name": None},
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://broker.test"
    )
    server = relay_module.create_server(
        agent_id="stable-id",
        broker_url="http://broker.test",
        http_client=client,
    )

    result = await _call(
        server, "send", {"to_agent": "peer", "content": "hello"}
    )

    assert result.isError
    assert "no relay name" in result.content[0].text
    assert "call name before send" in result.content[0].text
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/agent/stable-id/status")
    ]
    await client.aclose()


@pytest.mark.asyncio
async def test_mcp_argument_validation_rejects_invalid_kind_and_delivery_page(
    relay_module,
):
    requests: list[httpx.Request] = []
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: requests.append(request)
            or httpx.Response(200, json={})
        ),
        base_url="http://broker.test",
    )
    server = relay_module.create_server(
        agent_id="stable-id",
        broker_url="http://broker.test",
        http_client=client,
    )
    invalid = [
        await _call(
            server,
            "send",
            {"to_agent": "peer", "content": "x", "kind": "status"},
        ),
        await _call(server, "check", {"after": -1}),
        await _call(server, "check", {"limit": 101}),
        await _call(server, "ack", {"event_id": 0}),
    ]
    assert all(result.isError for result in invalid)
    assert requests == []
    await client.aclose()


def test_cli_requires_explicit_agent_identity_and_uses_broker_env_defaults(
    relay_module, monkeypatch
):
    monkeypatch.setenv("BROKER_URL", "http://broker.from.env:9000")
    monkeypatch.setenv("BROKER_BEARER_TOKEN", "env-token")
    parser = relay_module.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    parsed = parser.parse_args(["--agent-id", "stable-id"])
    assert parsed.agent_id == "stable-id"
    assert parsed.broker_url == "http://broker.from.env:9000"
    assert parsed.bearer_token == "env-token"


@pytest.mark.asyncio
async def test_tools_never_open_or_write_adapter_state(
    relay_module, monkeypatch, tmp_path
):
    state_path = tmp_path / "adapter-state.json"
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"id": "stable-id", "event_id": 1}
            )
        ),
        base_url="http://broker.test",
    )
    server = relay_module.create_server(
        agent_id="stable-id",
        broker_url="http://broker.test",
        http_client=client,
    )

    def forbidden_open(*args, **kwargs):
        raise AssertionError(f"relay MCP attempted state-file access: {args!r}")

    monkeypatch.setattr(builtins, "open", forbidden_open)
    result = await _call(
        server,
        "register",
        {"thread_id": "thread-1", "status": "running"},
    )
    assert not result.isError
    assert not state_path.exists()
    await client.aclose()


@pytest.mark.asyncio
async def test_operations_use_stable_identity_and_exact_http_contract(
    relay_module, monkeypatch
):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST" and request.url.path.endswith("/status"):
            return httpx.Response(200, json={"id": "stable-codex-id", "event_id": 1})
        if request.method == "PUT":
            return httpx.Response(200, json={"relay_name": "codex.one"})
        if request.url.path.endswith("/status"):
            return httpx.Response(
                200,
                json={
                    "id": "stable-codex-id",
                    "status": "active",
                    "relay_name": "codex.one",
                },
            )
        if request.url.path.endswith("/deliveries"):
            return httpx.Response(200, json={"deliveries": [], "next_cursor": None})
        if request.url.path == "/api/event":
            return httpx.Response(200, json={"event_id": 2})
        return httpx.Response(500, json={"detail": "unexpected request"})

    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "must-not-be-used")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://broker.test"
    )
    server = relay_module.create_server(
        agent_id="stable-codex-id",
        broker_url="http://broker.test",
        bearer_token="secret-token",
        http_client=client,
    )
    before = int(time.time() * 1000)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool(
            "register", {"thread_id": "thread-1", "status": "running", "current_task": "R3A"}
        )
        await session.call_tool("name", {"relay_name": "codex.one"})
        checked = _payload(await session.call_tool("check", {"after": 7, "limit": 9}))
        assert checked["status"]["id"] == "stable-codex-id"
        assert checked["deliveries"] == []
        await session.call_tool(
            "send",
            {"to_agent": "peer.one", "content": "hello", "channel": "#custom", "kind": "handoff"},
        )
        await session.call_tool("ack", {"event_id": 73})
    after = int(time.time() * 1000)

    assert [(r.method, r.url.path) for r in requests] == [
        ("POST", "/api/agent/stable-codex-id/status"),
        ("PUT", "/api/agent/stable-codex-id/relay-name"),
        ("GET", "/api/agent/stable-codex-id/status"),
        ("GET", "/api/agent/stable-codex-id/deliveries"),
        ("GET", "/api/agent/stable-codex-id/status"),
        ("POST", "/api/event"),
        ("POST", "/api/agent/stable-codex-id/delivery-ack"),
    ]
    assert all(r.headers["authorization"] == "Bearer secret-token" for r in requests)
    assert all("RelayControl" not in r.headers["authorization"] for r in requests)
    assert all("/api/operator/" not in r.url.path for r in requests)
    assert all("delivery-offer" not in r.url.path for r in requests)
    assert all("/delivery/" not in r.url.path for r in requests)
    register_body = json.loads(requests[0].content)
    register_ts = register_body.pop("last_activity_ts")
    assert before <= register_ts <= after
    assert register_body == {
        "kind": "codex-session",
        "channel": "#codex-relay",
        "thread_id": "thread-1",
        "status": "running",
        "current_task": "R3A",
        "transport": "codex-app-server",
    }
    assert json.loads(requests[1].content) == {"relay_name": "codex.one", "takeover": False}
    assert requests[3].url.query.decode() == "after=7&limit=9"
    send_body = json.loads(requests[5].content)
    assert before <= send_body.pop("ts") <= after
    UUID(send_body.pop("client_event_id"))
    assert send_body == {
        "from_agent": "codex.one",
        "to_agent": "peer.one",
        "channel": "#custom",
        "kind": "handoff",
        "content": "hello",
    }
    assert json.loads(requests[6].content) == {"event_ids": [73]}
    await client.aclose()
