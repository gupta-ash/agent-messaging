"""Public-build relay adapter unit and disposable protocol tests."""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path

import httpx
import pytest
from websockets.asyncio.client import connect as websocket_connect
from websockets.asyncio.server import serve as websocket_serve

from codex_adapter import (
    AdapterState,
    AppServerProtocol,
    BrokerHttpClient,
    CodexAdapter,
    DeliveryBlocked,
    RelayProxyHost,
    StateLock,
    StateStore,
    _run_from_args,
    build_arg_parser,
    validate_loopback_websocket_url,
)


class FakeRpc:
    def __init__(self, responses: dict[str, object]):
        self.responses = responses
        self.requests: list[tuple[str, dict]] = []

    async def request(self, method: str, params: dict, *, timeout=None):
        self.requests.append((method, params))
        value = self.responses[method]
        return value(params) if callable(value) else value


@pytest.mark.parametrize(
    "url",
    ["ws://127.0.0.1:8765", "ws://localhost:8765", "ws://[::1]:8765"],
)
def test_loopback_websocket_urls_are_accepted(url: str):
    assert validate_loopback_websocket_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "wss://127.0.0.1:8765",
        "ws://0.0.0.0:8765",
        "ws://example.com:8765",
        "ws://user@127.0.0.1:8765",
        "ws://127.0.0.1:8765/path",
    ],
)
def test_non_loopback_or_credentialed_websocket_urls_are_rejected(url: str):
    with pytest.raises(ValueError, match="loopback"):
        validate_loopback_websocket_url(url)


def test_state_lock_refuses_a_second_owner(tmp_path: Path):
    first = StateLock(tmp_path / "state.json")
    second = StateLock(tmp_path / "state.json")
    first.acquire()
    try:
        with pytest.raises(RuntimeError, match="already active"):
            second.acquire()
    finally:
        first.release()


@pytest.mark.asyncio
async def test_state_round_trip_keeps_identity_and_permission_selections_only(
    tmp_path: Path,
):
    state = AdapterState.new(
        working_directory=str(tmp_path),
        agent_id="11111111-1111-4111-8111-111111111111",
    )
    state.thread_id = "thread-1"
    state.last_broker_confirmed_name = "codex.one"
    state.sandbox = "workspace-write"
    state.approval_policy = "on-request"
    state.approvals_reviewer = "user"
    store = StateStore(tmp_path / "state.json")
    await store.save(state)

    assert await store.load() == state
    persisted = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert persisted == {
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "approval_policy": "on-request",
        "approvals_reviewer": "user",
        "last_broker_confirmed_name": "codex.one",
        "sandbox": "workspace-write",
        "thread_id": "thread-1",
        "version": 3,
        "working_directory": str(tmp_path),
    }


@pytest.mark.asyncio
async def test_structured_approval_policy_round_trips_and_reapplies_to_resume(
    tmp_path: Path,
):
    structured_policy = {"granular": {}}
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-owned"
    state.sandbox = "workspace-write"
    state.approval_policy = structured_policy
    state.approvals_reviewer = "user"
    store = StateStore(tmp_path / "state.json")
    await store.save(state)

    loaded = await store.load()
    assert loaded is not None
    assert loaded.approval_policy == structured_policy
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=object(),
        store=store,
        state=loaded,
        relay_name=None,
        allow_new_thread=False,
    )
    params = {"threadId": "thread-owned", "approvalPolicy": "never"}

    await host.guard_thread_request("thread/resume", params)

    assert params["approvalPolicy"] == structured_policy


@pytest.mark.asyncio
async def test_idle_and_active_delivery_use_public_protocol_fields():
    rpc = FakeRpc(
        {
            "turn/start": {"turn": {"id": "turn-1", "status": "inProgress"}},
            "turn/steer": {"turnId": "turn-1"},
        }
    )
    protocol = AppServerProtocol(rpc)
    protocol.bind_thread("thread-1", status="idle")
    base = {
        "from_agent": "peer",
        "to_agent": "codex.one",
        "channel": "#codex-relay",
        "kind": "agent_message",
        "content": "hello",
    }

    idle = await protocol.accept_delivery({**base, "event_id": 1})
    active = await protocol.accept_delivery({**base, "event_id": 2})

    assert idle.method == "turn/start"
    assert active.method == "turn/steer"
    assert rpc.requests[1][1]["expectedTurnId"] == "turn-1"
    assert rpc.requests[1][1]["threadId"] == "thread-1"


@pytest.mark.asyncio
async def test_notifications_supply_and_clear_observed_active_turn():
    protocol = AppServerProtocol(FakeRpc({}))
    protocol.bind_thread("thread-1", status="idle")

    await protocol.handle_notification(
        "turn/started",
        {"threadId": "thread-1", "turn": {"id": "turn-current"}},
    )
    assert protocol.active_turn_id == "turn-current"
    await protocol.handle_notification(
        "turn/completed",
        {"threadId": "thread-1", "turn": {"id": "turn-current"}},
    )
    assert protocol.thread_status == "idle"
    assert protocol.active_turn_id is None


@pytest.mark.asyncio
async def test_broker_client_uses_stable_delivery_contract_without_adapter_ack():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/status") and request.method == "GET":
            return httpx.Response(404, json={"detail": "not found"})
        if request.url.path.endswith("/deliveries"):
            return httpx.Response(
                200,
                json={"deliveries": [{"event_id": 8}], "next_cursor": None},
            )
        if request.url.path.endswith("/relay-name"):
            return httpx.Response(200, json={"relay_name": "codex.one"})
        return httpx.Response(200, json={"ok": True})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://broker.test"
    )
    broker = BrokerHttpClient("http://broker.test", client=client)
    assert await broker.get_status(agent_id="stable") is None
    await broker.register(
        agent_id="stable", thread_id="thread-1", status="idle"
    )
    assert (await broker.claim_name(agent_id="stable", relay_name="codex.one"))[
        "relay_name"
    ] == "codex.one"
    assert await broker.get_deliveries(agent_id="stable") == [{"event_id": 8}]
    assert not any(request.url.path.endswith("/delivery-ack") for request in requests)
    await client.aclose()


@pytest.mark.asyncio
async def test_thread_guard_keeps_one_invocation_on_one_thread(tmp_path: Path):
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-owned"
    state.sandbox = "workspace-write"
    state.approval_policy = "on-request"
    state.approvals_reviewer = "user"
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500)),
        base_url="http://broker.test",
    )
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=BrokerHttpClient("http://broker.test", client=client),
        store=StateStore(tmp_path / "state.json"),
        state=state,
        relay_name=None,
        allow_new_thread=False,
    )

    resume_params = {
        "threadId": "thread-owned",
        "sandbox": "danger-full-access",
        "approvalPolicy": "never",
        "approvalsReviewer": "auto",
    }
    await host.guard_thread_request("thread/resume", resume_params)
    assert resume_params == {
        "threadId": "thread-owned",
        "sandbox": "workspace-write",
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
    }
    with pytest.raises(DeliveryBlocked, match="different thread"):
        await host.guard_thread_request(
            "thread/resume", {"threadId": "thread-other"}
        )
    with pytest.raises(DeliveryBlocked, match="switching"):
        await host.guard_thread_request("thread/fork", {})
    await client.aclose()


@pytest.mark.asyncio
async def test_new_thread_permission_does_not_authorize_resuming_another_thread(
    tmp_path: Path,
):
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-owned"
    state.sandbox = "workspace-write"
    state.approval_policy = "on-request"
    state.approvals_reviewer = "user"
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=object(),
        store=StateStore(tmp_path / "state.json"),
        state=state,
        relay_name=None,
        allow_new_thread=True,
    )
    params = {"threadId": "thread-other"}

    with pytest.raises(DeliveryBlocked, match="different thread"):
        await host.guard_thread_request("thread/resume", params)

    assert params == {"threadId": "thread-other"}
    assert state.thread_id == "thread-owned"
    assert state.sandbox == "workspace-write"
    assert state.approval_policy == "on-request"
    assert state.approvals_reviewer == "user"


@pytest.mark.asyncio
async def test_thread_guard_rejects_incomplete_saved_resume_permissions(tmp_path: Path):
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-owned"
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=object(),
        store=StateStore(tmp_path / "state.json"),
        state=state,
        relay_name=None,
        allow_new_thread=False,
    )

    with pytest.raises(DeliveryBlocked, match="start a new relay thread"):
        await host.guard_thread_request(
            "thread/resume", {"threadId": "thread-owned"}
        )


@pytest.mark.asyncio
async def test_thread_start_requires_all_public_permission_selections(tmp_path: Path):
    state = AdapterState.new(working_directory=str(tmp_path))
    host = RelayProxyHost(
        app_server_url="ws://127.0.0.1:8765",
        broker=object(),
        store=StateStore(tmp_path / "state.json"),
        state=state,
        relay_name=None,
        allow_new_thread=True,
    )

    with pytest.raises(DeliveryBlocked, match="thread/start requires non-empty"):
        await host.guard_thread_request(
            "thread/start",
            {
                "sandbox": "workspace-write",
                "approvalPolicy": "on-request",
                "approvalsReviewer": "",
            },
        )

    assert not (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_cli_requires_distinct_proxy_and_app_server_urls(tmp_path: Path):
    args = build_arg_parser().parse_args(
        [
            "--listen-url",
            "ws://127.0.0.1:8766",
            "--app-server-url",
            "ws://127.0.0.1:8765",
            "--state",
            "state.json",
        ]
    )
    assert args.listen_url.endswith(":8766")
    assert args.app_server_url.endswith(":8765")
    assert args.relay_name is None

    same_endpoint = build_arg_parser().parse_args(
        [
            "--listen-url",
            "ws://localhost:8765",
            "--app-server-url",
            "ws://127.0.0.1:8765",
            "--state",
            str(tmp_path / "state.json"),
        ]
    )
    with pytest.raises(DeliveryBlocked, match="ports must differ"):
        await asyncio.wait_for(_run_from_args(same_endpoint), timeout=0.1)


@pytest.mark.asyncio
async def test_proxy_round_trip_accepts_a_frame_larger_than_one_mib(
    tmp_path: Path, unused_tcp_port: int
):
    async def echo(websocket):
        async for message in websocket:
            await websocket.send(message)

    readiness_path = tmp_path / "ready.json"
    async with websocket_serve(echo, "127.0.0.1", 0, max_size=None) as upstream_server:
        upstream_port = upstream_server.sockets[0].getsockname()[1]
        args = build_arg_parser().parse_args(
            [
                "--listen-url",
                f"ws://127.0.0.1:{unused_tcp_port}",
                "--app-server-url",
                f"ws://127.0.0.1:{upstream_port}",
                "--broker-url",
                "http://127.0.0.1:1",
                "--state",
                str(tmp_path / "state.json"),
                "--readiness-file",
                str(readiness_path),
                "--working-directory",
                str(tmp_path),
            ]
        )
        proxy_task = asyncio.create_task(_run_from_args(args))
        try:
            for _ in range(200):
                if readiness_path.exists():
                    break
                if proxy_task.done():
                    await proxy_task
                await asyncio.sleep(0.01)
            assert readiness_path.exists()

            payload = {
                "method": "test/large-frame",
                "params": {"body": "x" * (1024 * 1024 + 1024)},
            }
            async with websocket_connect(
                f"ws://127.0.0.1:{unused_tcp_port}", max_size=None
            ) as tui:
                await tui.send(json.dumps(payload))
                echoed = json.loads(await asyncio.wait_for(tui.recv(), timeout=2))

            assert echoed == payload
        finally:
            proxy_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await proxy_task


class _EventStreamBroker:
    """In-memory stand-in for the broker's ``/api/events`` contract.

    Mirrors ``server.get_events``: reads ONLY ``?cursor=`` (or ``Last-Event-ID``),
    replays rows with ``event_id > cursor`` capped at 1000, then holds the stream
    open for live events the test pushes (or ends it, with ``close_after_replay``).
    """

    def __init__(
        self, head: int, *, close_after_replay: bool = False, pace: bool = False
    ):
        self.head = head
        self.close_after_replay = close_after_replay
        self.pace = pace  # one frame per event-loop turn, like separate network reads
        self.event_cursors: list[int] = []
        self.delivery_reads = 0
        self.replayed = asyncio.Event()
        self.live: asyncio.Queue[int] = asyncio.Queue()

    @staticmethod
    def _frame(event_id: int) -> bytes:
        return f"id: {event_id}\nevent: message\ndata: {{}}\n\n".encode()

    async def _body(self, cursor: int):
        for event_id in range(cursor + 1, min(self.head, cursor + 1000) + 1):
            yield self._frame(event_id)
            if self.pace:
                await asyncio.sleep(0)
        self.replayed.set()
        if self.close_after_replay:
            return
        while True:
            event_id = await self.live.get()
            self.head = max(self.head, event_id)
            yield self._frame(event_id)

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/events":
            raw = (
                request.headers.get("Last-Event-ID")
                or request.url.params.get("cursor")
                or "0"
            )
            self.event_cursors.append(int(raw))
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=self._body(int(raw)),
            )
        if path.endswith("/deliveries"):
            self.delivery_reads += 1
            return httpx.Response(200, json={"deliveries": [], "next_cursor": None})
        if path.endswith("/status") and request.method == "GET":
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(200, json={"ok": True})


def _adapter_over(fake: _EventStreamBroker, tmp_path: Path, *, retry_delay: float = 1.0):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(fake.handler), base_url="http://broker.test"
    )
    protocol = AppServerProtocol(FakeRpc({}))
    protocol.bind_thread("thread-1", status="idle")
    state = AdapterState.new(working_directory=str(tmp_path))
    state.thread_id = "thread-1"
    adapter = CodexAdapter(
        protocol,
        BrokerHttpClient("http://broker.test", client=client),
        StateStore(tmp_path / "state.json"),
        state,
        relay_name=None,
        retry_delay=retry_delay,
    )
    return adapter, client


async def _stop(run_task: asyncio.Task, client: httpx.AsyncClient) -> None:
    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("pace", [False, True], ids=["one-read", "paced"])
async def test_wake_stream_uses_one_connection_and_goes_quiet_after_replay(
    tmp_path: Path, pace: bool
):
    # Regression: the adapter used to reopen /api/events on every wake with a
    # parameter the server ignores, so each connection replayed from event 0 and
    # returned at once (~120 requests/s per Codex session, forever). Renaming the
    # parameter alone would instead walk the log one event per reconnect.
    # Guaranteed: one connection, and no requests once the stream is quiet. A
    # paced replay may cost several drains; frames that arrive together cost one.
    fake = _EventStreamBroker(head=2500, pace=pace)
    adapter, client = _adapter_over(fake, tmp_path)
    run_task = asyncio.create_task(adapter.run())
    try:
        await asyncio.wait_for(fake.replayed.wait(), timeout=10)
        await asyncio.sleep(0.2)
        assert fake.event_cursors == [0]
        if not pace:
            assert fake.delivery_reads <= 3
        settled = fake.delivery_reads
        await asyncio.sleep(0.3)  # quiet stream: the adapter must make no requests
        assert fake.delivery_reads == settled
        assert fake.event_cursors == [0]
        await fake.live.put(2501)  # positive control: a live event still wakes a drain
        for _ in range(200):
            if fake.delivery_reads > settled:
                break
            await asyncio.sleep(0.01)
        assert fake.delivery_reads > settled
        assert fake.event_cursors == [0]
    finally:
        await _stop(run_task, client)


@pytest.mark.asyncio
async def test_wake_stream_reconnects_from_the_last_event_seen_not_from_zero(
    tmp_path: Path,
):
    fake = _EventStreamBroker(head=2500, close_after_replay=True)
    adapter, client = _adapter_over(fake, tmp_path, retry_delay=0.01)
    run_task = asyncio.create_task(adapter.run())
    try:
        for _ in range(500):
            if len(fake.event_cursors) >= 4:
                break
            await asyncio.sleep(0.01)
        assert fake.event_cursors[:4] == [0, 1000, 2000, 2500]
    finally:
        await _stop(run_task, client)


@pytest.mark.asyncio
async def test_wake_stream_sends_only_parameters_the_server_events_route_reads():
    from broker import server

    route = next(
        r for r in server.app.routes if getattr(r, "path", None) == "/api/events"
    )
    server_params = {p.alias or p.name for p in route.dependant.query_params}
    assert "cursor" in server_params  # control: the instrument reads the real route

    sent: list[set[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(set(request.url.params.keys()))
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=b"id: 5\n\n"
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://broker.test"
    )
    broker = BrokerHttpClient("http://broker.test", client=client)
    assert [event_id async for event_id in broker.wake_events(after=4)] == [5]
    await client.aclose()
    assert sent == [{"cursor"}]
    assert sent[0] <= server_params
