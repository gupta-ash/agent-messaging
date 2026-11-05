"""Focused public-build relay receipt and transparent-proxy contract."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from codex_adapter import (
    AdapterState,
    AppServerProtocol,
    CodexAdapter,
    JsonRpcIdRouter,
    RelayProxyHost,
    StateStore,
    TransparentJsonRpcConnection,
)


class FakeRpc:
    def __init__(self, responses: dict[str, object]):
        self.responses = responses
        self.requests: list[tuple[str, dict]] = []

    async def request(self, method: str, params: dict, *, timeout=None):
        self.requests.append((method, params))
        response = self.responses[method]
        return response(params) if callable(response) else response


class FakeBroker:
    def __init__(self, deliveries: list[dict]):
        self.deliveries = deliveries
        self.delivery_reads = 0
        self.acknowledge_calls: list[object] = []

    async def get_deliveries(self, *, agent_id: str):
        self.delivery_reads += 1
        return list(self.deliveries)

    async def acknowledge(self, **kwargs):
        self.acknowledge_calls.append(kwargs)
        raise AssertionError("adapter must never acknowledge app-server acceptance")


class QueueWebSocket:
    _STOP = object()

    def __init__(self):
        self.incoming: asyncio.Queue = asyncio.Queue()
        self.sent: asyncio.Queue = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.incoming.get()
        if message is self._STOP:
            raise StopAsyncIteration
        return message

    async def send(self, raw: str):
        await self.sent.put(raw)

    async def feed(self, message: dict):
        await self.incoming.put(json.dumps(message))

    async def take_sent(self) -> dict:
        return json.loads(await asyncio.wait_for(self.sent.get(), timeout=1))

    async def finish(self):
        await self.incoming.put(self._STOP)


async def allow_thread_request(method: str, params: dict) -> None:
    return None


async def ignore_thread_response(
    protocol: AppServerProtocol, method: str, params: dict, result: dict
) -> None:
    return None


def transparent_connection():
    downstream = QueueWebSocket()
    upstream = QueueWebSocket()
    connection = TransparentJsonRpcConnection(
        downstream,
        upstream,
        thread_guard=allow_thread_request,
        thread_observer=ignore_thread_response,
    )
    return connection, downstream, upstream


def delivery(event_id: int, content: str = "work") -> dict:
    return {
        "event_id": event_id,
        "from_agent": "peer.one",
        "to_agent": "codex.one",
        "channel": "#codex-relay",
        "kind": "agent_message",
        "content": content,
        "ts": 1,
    }


@pytest.mark.asyncio
async def test_identity_state_migrates_legacy_journal_to_identity_only(tmp_path: Path):
    path = tmp_path / "session.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "agent_id": "11111111-1111-4111-8111-111111111111",
                "thread_id": "thread-1",
                "working_directory": str(tmp_path),
                "accepted_unacked": [
                    {"event_id": 9, "turn_id": "turn-9", "method": "turn/start"}
                ],
            }
        ),
        encoding="utf-8",
    )
    store = StateStore(path)

    state = await store.load()
    assert state is not None
    assert state.version == 3
    assert state.last_broker_confirmed_name is None
    assert state.sandbox is None
    assert state.approval_policy is None
    assert state.approvals_reviewer is None
    assert not hasattr(state, "accepted_unacked")
    await store.save(state)

    assert set(json.loads(path.read_text(encoding="utf-8"))) == {
        "agent_id",
        "approval_policy",
        "approvals_reviewer",
        "last_broker_confirmed_name",
        "sandbox",
        "thread_id",
        "version",
        "working_directory",
    }


def test_identity_state_migrates_v2_with_permissions_unset(tmp_path: Path):
    state = AdapterState.from_dict(
        {
            "version": 2,
            "agent_id": "11111111-1111-4111-8111-111111111111",
            "thread_id": "thread-2",
            "working_directory": str(tmp_path),
            "last_broker_confirmed_name": "codex.two",
        }
    )

    assert state.version == 3
    assert state.last_broker_confirmed_name == "codex.two"
    assert state.sandbox is None
    assert state.approval_policy is None
    assert state.approvals_reviewer is None


@pytest.mark.asyncio
async def test_app_server_acceptance_stays_outstanding_and_instructs_model_to_ack_first(
    tmp_path: Path,
):
    rpc = FakeRpc(
        {"turn/start": {"turn": {"id": "turn-71", "status": "inProgress"}}}
    )
    protocol = AppServerProtocol(rpc)
    protocol.bind_thread("thread-1", status="idle")
    broker = FakeBroker([delivery(71)])
    state = AdapterState.new(
        working_directory=str(tmp_path),
        agent_id="11111111-1111-4111-8111-111111111111",
    )
    state.thread_id = "thread-1"
    adapter = CodexAdapter(
        protocol,
        broker,
        StateStore(tmp_path / "state.json"),
        state,
        relay_name="codex.one",
    )

    assert await adapter.drain_once() == 1

    assert broker.acknowledge_calls == []
    assert [row["event_id"] for row in broker.deliveries] == [71]
    method, params = rpc.requests[0]
    assert method == "turn/start"
    rendered = params["input"][0]["text"]
    assert "event_id=71" in rendered
    assert "Call the relay `ack` tool for event 71 immediately on receipt" in rendered
    assert "before doing the requested work" in rendered


@pytest.mark.asyncio
async def test_retry_rereads_broker_and_waits_thirty_seconds_per_event(tmp_path: Path):
    now = [100.0]
    rpc = FakeRpc(
        {
            "turn/start": {"turn": {"id": "turn-72", "status": "inProgress"}},
            "turn/steer": {"turnId": "turn-72"},
        }
    )
    protocol = AppServerProtocol(rpc)
    protocol.bind_thread("thread-1", status="idle")
    broker = FakeBroker([delivery(72)])
    state = AdapterState.new(
        working_directory=str(tmp_path),
        agent_id="11111111-1111-4111-8111-111111111111",
    )
    state.thread_id = "thread-1"
    adapter = CodexAdapter(
        protocol,
        broker,
        StateStore(tmp_path / "state.json"),
        state,
        relay_name="codex.one",
        monotonic=lambda: now[0],
    )

    assert await adapter.drain_once() == 1
    now[0] += 29.999
    assert await adapter.drain_once() == 0
    now[0] += 0.001
    assert await adapter.drain_once() == 1
    assert broker.delivery_reads == 3
    assert [method for method, _ in rpc.requests] == ["turn/start", "turn/steer"]

    broker.deliveries.clear()  # the model's MCP ack changed broker authority
    now[0] += 30
    assert await adapter.drain_once() == 0
    assert len(rpc.requests) == 2


@pytest.mark.asyncio
async def test_retry_interval_starts_after_delivery_attempt_completes(tmp_path: Path):
    now = [100.0]

    class LatentProtocol:
        def __init__(self):
            self.attempt_times = []

        async def accept_delivery(self, delivery):
            self.attempt_times.append(now[0])
            now[0] += 5.0

    protocol = LatentProtocol()
    broker = FakeBroker([delivery(73)])
    state = AdapterState.new(
        working_directory=str(tmp_path),
        agent_id="11111111-1111-4111-8111-111111111111",
    )
    adapter = CodexAdapter(
        protocol,
        broker,
        StateStore(tmp_path / "state.json"),
        state,
        relay_name="codex.one",
        monotonic=lambda: now[0],
    )

    assert await adapter.drain_once() == 1
    assert now[0] == 105.0

    now[0] = 134.999
    assert await adapter.drain_once() == 0

    now[0] = 135.0
    assert await adapter.drain_once() == 1
    assert protocol.attempt_times == [100.0, 135.0]


def test_request_id_router_preserves_tui_and_server_request_ids_without_collision():
    router = JsonRpcIdRouter()

    upstream = router.client_to_upstream(
        {"id": 7, "method": "turn/start", "params": {"threadId": "thread-1"}}
    )
    injection_id = router.reserve_adapter_request()
    assert upstream["id"] != 7
    assert injection_id not in {7, upstream["id"]}
    downstream = router.upstream_to_client(
        {"id": upstream["id"], "result": {"turn": {"id": "turn-1"}}}
    )
    assert downstream["id"] == 7

    approval = router.upstream_to_client(
        {
            "id": "app-server-approval",
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "thread-1", "turnId": "turn-1"},
        }
    )
    assert approval["id"] != "app-server-approval"
    decision = router.client_to_upstream(
        {"id": approval["id"], "result": {"decision": "decline"}}
    )
    assert decision == {
        "id": "app-server-approval",
        "result": {"decision": "decline"},
    }


@pytest.mark.asyncio
async def test_proxy_routes_concurrent_tui_and_adapter_requests_by_original_id():
    connection, downstream, upstream = transparent_connection()
    proxy_task = asyncio.create_task(connection.run())

    await downstream.feed(
        {"id": "tui-7", "method": "model/list", "params": {"source": "tui"}}
    )
    adapter_task = asyncio.create_task(
        connection.request("turn/start", {"source": "adapter"}, timeout=1)
    )
    first = await upstream.take_sent()
    second = await upstream.take_sent()
    requests = {message["method"]: message for message in (first, second)}

    assert requests["model/list"]["id"] != requests["turn/start"]["id"]
    await upstream.feed(
        {"id": requests["turn/start"]["id"], "result": {"turnId": "turn-1"}}
    )
    await upstream.feed(
        {"id": requests["model/list"]["id"], "result": {"models": ["gpt"]}}
    )

    assert await adapter_task == {"turnId": "turn-1"}
    assert await downstream.take_sent() == {
        "id": "tui-7",
        "result": {"models": ["gpt"]},
    }
    await downstream.finish()
    await asyncio.wait_for(proxy_task, timeout=1)


@pytest.mark.asyncio
async def test_proxy_forwards_app_server_approval_to_tui_and_returns_decision():
    connection, downstream, upstream = transparent_connection()
    proxy_task = asyncio.create_task(connection.run())

    await upstream.feed(
        {
            "id": "approval-9",
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "thread-1", "turnId": "turn-1"},
        }
    )
    approval = await downstream.take_sent()
    assert approval["id"] != "approval-9"

    await downstream.feed(
        {"id": approval["id"], "result": {"decision": "decline"}}
    )
    assert await upstream.take_sent() == {
        "id": "approval-9",
        "result": {"decision": "decline"},
    }
    await downstream.finish()
    await asyncio.wait_for(proxy_task, timeout=1)


@pytest.mark.parametrize(
    ("rpc_request", "error_text"),
    [
        ({"id": 21, "method": "thread/start"}, "already owns a thread"),
        (
            {"id": 22, "method": "thread/resume", "params": []},
            "different thread",
        ),
        (
            {"id": 23, "method": "thread/fork", "params": "not-an-object"},
            "switching is disabled",
        ),
    ],
)
@pytest.mark.asyncio
async def test_proxy_fails_closed_for_thread_requests_without_object_params(
    tmp_path: Path, rpc_request: dict, error_text: str
):
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-owned"
    state.sandbox = "workspace-write"
    state.approval_policy = "on-request"
    state.approvals_reviewer = "user"
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=FakeBroker([]),
        store=StateStore(tmp_path / "state.json"),
        state=state,
        relay_name=None,
        allow_new_thread=False,
    )
    downstream = QueueWebSocket()
    upstream = QueueWebSocket()
    connection = TransparentJsonRpcConnection(
        downstream,
        upstream,
        thread_guard=host.guard_thread_request,
        thread_observer=ignore_thread_response,
    )
    proxy_task = asyncio.create_task(connection.run())

    try:
        await downstream.feed(rpc_request)
        response = await downstream.take_sent()
        assert response["id"] == rpc_request["id"]
        assert error_text in response["error"]["message"]
        assert upstream.sent.empty()
    finally:
        await downstream.finish()
        await asyncio.wait_for(proxy_task, timeout=1)


@pytest.mark.asyncio
async def test_proxy_reapplies_saved_permissions_to_owned_thread_resume(
    tmp_path: Path,
):
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-owned"
    state.sandbox = "workspace-write"
    state.approval_policy = "on-request"
    state.approvals_reviewer = "user"
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=FakeBroker([]),
        store=StateStore(tmp_path / "state.json"),
        state=state,
        relay_name=None,
        allow_new_thread=False,
    )
    downstream = QueueWebSocket()
    upstream = QueueWebSocket()
    connection = TransparentJsonRpcConnection(
        downstream,
        upstream,
        thread_guard=host.guard_thread_request,
        thread_observer=ignore_thread_response,
    )
    proxy_task = asyncio.create_task(connection.run())

    await downstream.feed(
        {
            "id": 17,
            "method": "thread/resume",
            "params": {
                "threadId": "thread-owned",
                "sandbox": "danger-full-access",
                "approvalPolicy": "never",
            },
        }
    )
    routed = await upstream.take_sent()

    assert routed["method"] == "thread/resume"
    assert routed["params"] == {
        "threadId": "thread-owned",
        "sandbox": "workspace-write",
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
    }

    await downstream.finish()
    await asyncio.wait_for(proxy_task, timeout=1)


class BlockingRegistrationBroker:
    def __init__(self):
        self.register_started = asyncio.Event()
        self.allow_register = asyncio.Event()
        self.register_finished = asyncio.Event()
        self.wait_forever = asyncio.Event()

    async def get_status(self, *, agent_id: str):
        return None

    async def register(self, **kwargs):
        self.register_started.set()
        await self.allow_register.wait()
        self.register_finished.set()

    async def get_deliveries(self, *, agent_id: str):
        return []

    async def wake_events(self, *, after: int):
        await self.wait_forever.wait()
        yield after


@pytest.mark.asyncio
async def test_thread_registration_finishes_before_response_reaches_tui(tmp_path: Path):
    broker = BlockingRegistrationBroker()
    structured_policy = {"granular": {}}
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-old"
    state.sandbox = "read-only"
    state.approval_policy = "never"
    state.approvals_reviewer = "auto"
    store = StateStore(tmp_path / "state.json")
    await store.save(state)
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=broker,
        store=store,
        state=state,
        relay_name=None,
        allow_new_thread=True,
    )
    downstream = QueueWebSocket()
    upstream = QueueWebSocket()
    connection = TransparentJsonRpcConnection(
        downstream,
        upstream,
        thread_guard=host.guard_thread_request,
        thread_observer=host.observe_thread_response,
    )
    proxy_task = asyncio.create_task(connection.run())

    await downstream.feed(
        {
            "id": 7,
            "method": "thread/start",
            "params": {
                "sandbox": "workspace-write",
                "approvalPolicy": structured_policy,
                "approvalsReviewer": "user",
            },
        }
    )
    routed_request = await upstream.take_sent()
    assert routed_request["params"] == {
        "sandbox": "workspace-write",
        "approvalPolicy": structured_policy,
        "approvalsReviewer": "user",
    }
    before_response = await store.load()
    assert before_response is not None
    assert before_response.thread_id == "thread-old"
    assert before_response.sandbox == "read-only"
    assert before_response.approval_policy == "never"
    assert before_response.approvals_reviewer == "auto"
    await upstream.feed(
        {
            "id": routed_request["id"],
            "result": {
                "thread": {"id": "thread-1", "status": {"type": "idle"}, "turns": []}
            },
        }
    )
    await asyncio.wait_for(broker.register_started.wait(), timeout=1)
    assert downstream.sent.empty()
    persisted = await store.load()
    assert persisted is not None
    assert persisted.thread_id == "thread-1"
    assert persisted.sandbox == "workspace-write"
    assert persisted.approval_policy == structured_policy
    assert persisted.approvals_reviewer == "user"

    broker.allow_register.set()
    await asyncio.wait_for(broker.register_finished.wait(), timeout=1)
    response = await downstream.take_sent()
    assert response["id"] == 7
    assert response["result"]["thread"]["id"] == "thread-1"
    assert state.thread_id == "thread-1"

    await downstream.finish()
    await asyncio.wait_for(proxy_task, timeout=1)
    assert host.adapter_task is not None
    host.adapter_task.cancel()
    await asyncio.gather(host.adapter_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_late_adapter_response_after_timeout_is_swallowed():
    connection, downstream, upstream = transparent_connection()
    proxy_task = asyncio.create_task(connection.run())
    request_task = asyncio.create_task(
        connection.request("turn/start", {"source": "adapter"}, timeout=0.01)
    )
    routed_request = await upstream.take_sent()

    with pytest.raises(asyncio.TimeoutError):
        await request_task
    await upstream.feed(
        {"id": routed_request["id"], "result": {"turn": {"id": "too-late"}}}
    )
    await upstream.finish()
    await asyncio.wait_for(proxy_task, timeout=1)

    assert downstream.sent.empty()


class RecoveringBroker:
    def __init__(self):
        self.status_reads = 0
        self.register_calls = 0
        self.drain_succeeded = asyncio.Event()
        self.wait_forever = asyncio.Event()

    async def get_status(self, *, agent_id: str):
        self.status_reads += 1
        if self.status_reads == 1:
            raise OSError("broker temporarily unavailable")
        return None

    async def register(self, **kwargs):
        self.register_calls += 1

    async def get_deliveries(self, *, agent_id: str):
        self.drain_succeeded.set()
        return []

    async def wake_events(self, *, after: int):
        await self.wait_forever.wait()
        yield after


@pytest.mark.asyncio
async def test_adapter_run_continues_after_broker_failure(tmp_path: Path):
    protocol = AppServerProtocol(FakeRpc({}))
    protocol.bind_thread("thread-1", status="idle")
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-1"
    broker = RecoveringBroker()
    adapter = CodexAdapter(
        protocol,
        broker,
        StateStore(tmp_path / "state.json"),
        state,
        relay_name=None,
        retry_delay=0.01,
    )
    run_task = asyncio.create_task(adapter.run())

    await asyncio.wait_for(broker.drain_succeeded.wait(), timeout=1)
    assert broker.status_reads == 2
    assert broker.register_calls == 1
    assert adapter.registered is True

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task
