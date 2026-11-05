"""Transparent public-Codex relay adapter with broker-owned receipt status.

The adapter proxies one normal TUI WebSocket connection to the official Codex
app-server. It injects outstanding broker messages on that same connection and
never treats app-server acceptance as a broker acknowledgement.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx
from websockets.asyncio.client import connect as websocket_connect
from websockets.asyncio.server import serve as websocket_serve


LOGGER = logging.getLogger("codex_adapter")
STATE_VERSION = 3
READINESS_VERSION = 1
APP_SERVER_TIMEOUT_SECONDS = 30.0
RETRY_INTERVAL_SECONDS = 30.0


class JsonRpcDisconnected(ConnectionError):
    """The proxied app-server connection closed before a request completed."""


class AppServerRejected(RuntimeError):
    """An app-server JSON-RPC request returned an error response."""

    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(f"app-server rejected request ({code}): {message}")
        self.code = code
        self.message = message
        self.data = data


class DeliveryBlocked(RuntimeError):
    """A relay operation could not proceed without losing its broker authority."""

    def __init__(self, message: str, *, transient: bool = False):
        super().__init__(message)
        self.transient = transient


@dataclass(frozen=True)
class AcceptedDelivery:
    event_id: int
    turn_id: str
    method: str


@dataclass
class AdapterState:
    """The complete durable state: identity and permissions, never message state."""

    version: int
    agent_id: str
    thread_id: str | None
    working_directory: str
    last_broker_confirmed_name: str | None = None
    sandbox: str | None = None
    approval_policy: str | dict[str, Any] | None = None
    approvals_reviewer: str | None = None

    @classmethod
    def new(
        cls, *, working_directory: str, agent_id: str | None = None
    ) -> "AdapterState":
        canonical_agent_id = str(UUID(agent_id)) if agent_id is not None else str(uuid4())
        return cls(
            version=STATE_VERSION,
            agent_id=canonical_agent_id,
            thread_id=None,
            working_directory=str(Path(working_directory).resolve()),
        )

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "AdapterState":
        version = raw.get("version")
        if version == 1:
            allowed = {
                "version",
                "agent_id",
                "thread_id",
                "working_directory",
                "accepted_unacked",
            }
            confirmed_name = None
            sandbox = None
            approval_policy = None
            approvals_reviewer = None
        elif version == 2:
            allowed = {
                "version",
                "agent_id",
                "thread_id",
                "working_directory",
                "last_broker_confirmed_name",
            }
            confirmed_name = raw.get("last_broker_confirmed_name")
            sandbox = None
            approval_policy = None
            approvals_reviewer = None
        elif version == STATE_VERSION:
            allowed = {
                "version",
                "agent_id",
                "thread_id",
                "working_directory",
                "last_broker_confirmed_name",
                "sandbox",
                "approval_policy",
                "approvals_reviewer",
            }
            confirmed_name = raw.get("last_broker_confirmed_name")
            sandbox = raw.get("sandbox")
            approval_policy = raw.get("approval_policy")
            approvals_reviewer = raw.get("approvals_reviewer")
        else:
            raise ValueError("unsupported adapter state version")
        if set(raw) - allowed:
            raise ValueError("adapter state contains unsupported fields")
        agent_id = raw.get("agent_id")
        try:
            agent_id = str(UUID(agent_id))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("adapter state agent_id must be a UUID") from exc
        thread_id = raw.get("thread_id")
        working_directory = raw.get("working_directory")
        if thread_id is not None and (
            not isinstance(thread_id, str) or not thread_id.strip()
        ):
            raise ValueError("adapter state thread_id must be a non-empty string or null")
        if not isinstance(working_directory, str) or not working_directory:
            raise ValueError("adapter state working_directory must be a non-empty string")
        if confirmed_name is not None and (
            not isinstance(confirmed_name, str) or not confirmed_name
        ):
            raise ValueError(
                "adapter state last_broker_confirmed_name must be a non-empty string or null"
            )
        for name, value in (
            ("sandbox", sandbox),
            ("approvals_reviewer", approvals_reviewer),
        ):
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(
                    f"adapter state {name} must be a non-empty string or null"
                )
        if approval_policy is not None and not (
            isinstance(approval_policy, dict)
            or (isinstance(approval_policy, str) and approval_policy.strip())
        ):
            raise ValueError(
                "adapter state approval_policy must be a non-empty string, object, or null"
            )
        return cls(
            version=STATE_VERSION,
            agent_id=agent_id,
            thread_id=thread_id,
            working_directory=working_directory,
            last_broker_confirmed_name=confirmed_name,
            sandbox=sandbox,
            approval_policy=approval_policy,
            approvals_reviewer=approvals_reviewer,
        )


class StateStore:
    """Atomically replace one identity document."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    async def load(self) -> AdapterState | None:
        if not self.path.exists():
            return None
        raw = await asyncio.to_thread(self.path.read_text, encoding="utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("adapter state must be a JSON object")
        return AdapterState.from_dict(parsed)

    async def save(self, state: AdapterState) -> None:
        payload = json.dumps(asdict(state), indent=2, sort_keys=True) + "\n"
        await asyncio.to_thread(self._replace, payload)

    def _replace(self, payload: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f".{self.path.name}.tmp-{os.getpid()}-{uuid4().hex}"
        )
        try:
            temporary.write_text(payload, encoding="utf-8", newline="\n")
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


class StateLock:
    """Hold an exclusive sibling lock for one adapter process lifetime."""

    def __init__(self, state_path: str | Path):
        state_path = Path(state_path)
        self.path = state_path.with_name(f"{state_path.name}.lock")
        self._handle = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"0")
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError("an adapter is already active for this state file") from exc
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        handle, self._handle = self._handle, None
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def validate_loopback_websocket_url(value: str) -> str:
    parsed = urlparse(value)
    host = parsed.hostname
    try:
        loopback = host == "localhost" or bool(
            host and ipaddress.ip_address(host).is_loopback
        )
    except ValueError:
        loopback = False
    if (
        parsed.scheme != "ws"
        or not loopback
        or parsed.port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("relay WebSocket URL must be an unauthenticated loopback ws:// URL")
    return value


def _bind_address(value: str) -> tuple[str, int]:
    parsed = urlparse(validate_loopback_websocket_url(value))
    return parsed.hostname or "127.0.0.1", int(parsed.port)


async def reset_readiness_file(path: str | Path | None) -> None:
    if path is not None:
        await asyncio.to_thread(Path(path).unlink, missing_ok=True)


async def publish_readiness_file(
    path: str | Path | None, state: AdapterState
) -> None:
    if path is None:
        return
    payload = json.dumps(
        {"version": READINESS_VERSION, "agent_id": state.agent_id, "status": "ready"},
        indent=2,
        sort_keys=True,
    ) + "\n"
    await asyncio.to_thread(StateStore(path)._replace, payload)


class JsonRpcIdRouter:
    """Translate both JSON-RPC directions so proxy and TUI IDs never collide."""

    def __init__(self) -> None:
        self._next_upstream_id = 1
        self._next_server_id = 1
        self._client_requests: dict[int, tuple[Any, str, dict[str, Any]]] = {}
        self._server_requests: dict[str, Any] = {}
        self._adapter_requests: set[int] = set()

    def _upstream_id(self) -> int:
        value = self._next_upstream_id
        self._next_upstream_id += 1
        return value

    def reserve_adapter_request(self) -> int:
        value = self._upstream_id()
        self._adapter_requests.add(value)
        return value

    def complete_adapter_request(self, request_id: Any) -> bool:
        if request_id not in self._adapter_requests:
            return False
        self._adapter_requests.remove(request_id)
        return True

    def client_request_context(
        self, request_id: Any
    ) -> tuple[Any, str, dict[str, Any]] | None:
        return self._client_requests.get(request_id)

    def client_to_upstream(self, message: dict[str, Any]) -> dict[str, Any]:
        routed = dict(message)
        if "method" in message and "id" in message:
            request_id = self._upstream_id()
            params = message.get("params")
            self._client_requests[request_id] = (
                message["id"],
                str(message["method"]),
                dict(params) if isinstance(params, dict) else {},
            )
            routed["id"] = request_id
        elif "method" not in message and "id" in message:
            original = self._server_requests.pop(str(message["id"]), None)
            if original is None:
                raise DeliveryBlocked("TUI returned an unknown server-request id")
            routed["id"] = original
        return routed

    def upstream_to_client(self, message: dict[str, Any]) -> dict[str, Any] | None:
        routed = dict(message)
        if "method" in message and "id" in message:
            request_id = f"relay-server-{self._next_server_id}"
            self._next_server_id += 1
            self._server_requests[request_id] = message["id"]
            routed["id"] = request_id
            return routed
        if "method" not in message and "id" in message:
            if self.complete_adapter_request(message["id"]):
                return None
            context = self._client_requests.pop(message["id"], None)
            if context is None:
                raise DeliveryBlocked("app-server returned an unknown response id")
            routed["id"] = context[0]
        return routed


class RpcClient(Protocol):
    async def request(
        self, method: str, params: dict[str, Any], *, timeout: float | None = None
    ) -> Any: ...


class AppServerProtocol:
    """Observed public app-server thread state plus relay injection operations."""

    def __init__(self, rpc: RpcClient):
        self.rpc = rpc
        self.thread_id: str | None = None
        self.thread_status: str | None = None
        self.active_turn_id: str | None = None

    def bind_thread(
        self,
        thread_id: str,
        *,
        status: str,
        active_turn_id: str | None = None,
    ) -> None:
        if status not in {"idle", "active"}:
            raise DeliveryBlocked("app-server returned an invalid thread status")
        if status == "active" and not active_turn_id:
            raise DeliveryBlocked("active thread does not expose an in-progress turn")
        self.thread_id = thread_id
        self.thread_status = status
        self.active_turn_id = active_turn_id if status == "active" else None

    def bind_thread_result(self, thread: dict[str, Any]) -> str:
        thread_id = thread.get("id")
        status = thread.get("status")
        status_type = status.get("type") if isinstance(status, dict) else status
        if not isinstance(thread_id, str) or not thread_id:
            raise DeliveryBlocked("app-server returned no thread identity")
        active_turns = [
            turn.get("id")
            for turn in thread.get("turns", [])
            if isinstance(turn, dict)
            and turn.get("status") == "inProgress"
            and isinstance(turn.get("id"), str)
        ]
        active_turn_id = active_turns[0] if len(active_turns) == 1 else None
        self.bind_thread(
            thread_id,
            status=str(status_type),
            active_turn_id=active_turn_id,
        )
        return thread_id

    async def handle_notification(self, method: str, params: dict[str, Any]) -> None:
        if params.get("threadId") != self.thread_id:
            return
        if method == "thread/status/changed":
            status = params.get("status")
            status_type = status.get("type") if isinstance(status, dict) else None
            if status_type in {"idle", "active"}:
                self.thread_status = status_type
                if status_type == "idle":
                    self.active_turn_id = None
        elif method == "turn/started":
            turn = params.get("turn")
            if isinstance(turn, dict) and isinstance(turn.get("id"), str):
                self.thread_status = "active"
                self.active_turn_id = turn["id"]
        elif method == "turn/completed":
            turn = params.get("turn")
            if isinstance(turn, dict) and turn.get("id") == self.active_turn_id:
                self.thread_status = "idle"
                self.active_turn_id = None

    @staticmethod
    def _render_delivery(delivery: dict[str, Any]) -> str:
        event_id = delivery["event_id"]
        return (
            f"[relay] from={delivery['from_agent']} event_id={event_id} "
            f"channel={delivery['channel']} kind={delivery['kind']}\n\n"
            f"Call the relay `ack` tool for event {event_id} immediately on receipt, "
            "before doing the requested work. ACK means received, not completed.\n\n"
            f"{delivery['content']}"
        )

    async def accept_delivery(self, delivery: dict[str, Any]) -> AcceptedDelivery:
        event_id = delivery.get("event_id")
        if not isinstance(event_id, int) or event_id < 1:
            raise DeliveryBlocked("broker delivery has an invalid event id")
        if self.thread_id is None:
            raise DeliveryBlocked("no app-server thread is attached", transient=True)
        base_params = {
            "threadId": self.thread_id,
            "input": [{"type": "text", "text": self._render_delivery(delivery)}],
            "clientUserMessageId": f"broker-event-{event_id}-{uuid4().hex}",
        }
        if self.thread_status == "idle":
            method = "turn/start"
            params = base_params
        elif self.thread_status == "active" and self.active_turn_id:
            method = "turn/steer"
            params = {**base_params, "expectedTurnId": self.active_turn_id}
        else:
            raise DeliveryBlocked(
                f"thread cannot accept broker event {event_id}: status={self.thread_status}",
                transient=True,
            )
        try:
            result = await self.rpc.request(
                method, params, timeout=APP_SERVER_TIMEOUT_SECONDS
            )
        except AppServerRejected as exc:
            raise DeliveryBlocked(
                f"{method} rejected for broker event {event_id}: {exc.message}",
                transient=True,
            ) from exc
        except (JsonRpcDisconnected, asyncio.TimeoutError) as exc:
            raise DeliveryBlocked(
                f"{method} did not accept broker event {event_id}: {exc}",
                transient=True,
            ) from exc
        if method == "turn/start":
            turn = result.get("turn") if isinstance(result, dict) else None
            turn_id = turn.get("id") if isinstance(turn, dict) else None
        else:
            turn_id = result.get("turnId") if isinstance(result, dict) else None
            if turn_id != self.active_turn_id:
                raise DeliveryBlocked(
                    f"turn/steer accepted a different turn for broker event {event_id}"
                )
        if not isinstance(turn_id, str) or not turn_id:
            raise DeliveryBlocked(f"{method} returned no accepted turn id")
        self.thread_status = "active"
        self.active_turn_id = turn_id
        return AcceptedDelivery(event_id=event_id, turn_id=turn_id, method=method)


class BrokerHttpClient:
    """Narrow broker API client used by the app-server adapter."""

    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        bearer_token: str | None = None,
    ):
        headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else None
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=30.0, headers=headers
        )

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    @staticmethod
    def _raise(response: httpx.Response) -> None:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise DeliveryBlocked(
                f"broker request failed with HTTP {response.status_code}",
                transient=response.status_code == 429 or response.status_code >= 500,
            ) from exc

    async def register(
        self,
        *,
        agent_id: str,
        thread_id: str,
        status: str,
        current_task: str | None = None,
    ) -> None:
        response = await self.client.post(
            f"/api/agent/{agent_id}/status",
            json={
                "kind": "codex-session",
                "channel": "#codex-relay",
                "thread_id": thread_id,
                "status": status,
                "current_task": current_task,
                "last_activity_ts": int(time.time() * 1000),
                "transport": "codex-app-server",
            },
        )
        self._raise(response)

    async def claim_name(self, *, agent_id: str, relay_name: str) -> dict[str, Any]:
        response = await self.client.put(
            f"/api/agent/{agent_id}/relay-name",
            json={"relay_name": relay_name, "takeover": False},
        )
        self._raise(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise DeliveryBlocked("broker returned an invalid relay-name response")
        return payload

    async def get_status(self, *, agent_id: str) -> dict[str, Any] | None:
        response = await self.client.get(f"/api/agent/{agent_id}/status")
        if response.status_code == 404:
            return None
        self._raise(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise DeliveryBlocked("broker returned an invalid status document")
        return payload

    async def set_status(
        self,
        *,
        agent_id: str,
        thread_id: str,
        status: str,
        current_task: str | None = None,
    ) -> None:
        await self.register(
            agent_id=agent_id,
            thread_id=thread_id,
            status=status,
            current_task=current_task,
        )

    async def get_deliveries(self, *, agent_id: str) -> list[dict[str, Any]]:
        response = await self.client.get(
            f"/api/agent/{agent_id}/deliveries", params={"after": 0, "limit": 100}
        )
        self._raise(response)
        payload = response.json()
        deliveries = payload.get("deliveries") if isinstance(payload, dict) else None
        if not isinstance(deliveries, list):
            raise DeliveryBlocked("broker returned an invalid delivery page")
        return deliveries

    async def wake_events(self, *, after: int) -> AsyncIterator[int]:
        """Yield the id of every event the broker streams after ``after``.

        ``/api/events`` replays rows with ``event_id > cursor`` and then stays open
        for live events, so ONE connection serves the whole wait. The parameter is
        ``cursor`` because it is the only one the server reads: an unread name is
        silently ignored and every connection replays from event 0.
        """
        try:
            async with self.client.stream(
                "GET", "/api/events", params={"cursor": after}
            ) as response:
                self._raise(response)
                async for line in response.aiter_lines():
                    if line.startswith("id:"):
                        value = line[3:].strip()
                        if value.isascii() and value.isdigit():
                            yield int(value)
        except httpx.ReadTimeout:
            return


class CodexAdapter:
    """Inject broker-outstanding events serially without acknowledging them."""

    def __init__(
        self,
        protocol: AppServerProtocol,
        broker: Any,
        state_store: StateStore,
        state: AdapterState,
        *,
        relay_name: str | None,
        retry_interval_seconds: float = RETRY_INTERVAL_SECONDS,
        retry_delay: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        if retry_interval_seconds < RETRY_INTERVAL_SECONDS:
            raise ValueError("per-event retry interval must be at least 30 seconds")
        self.protocol = protocol
        self.broker = broker
        self.state_store = state_store
        self.state = state
        self.relay_name = relay_name
        self.retry_interval_seconds = retry_interval_seconds
        self.retry_delay = retry_delay
        self.monotonic = monotonic
        self.registered = False
        self._last_attempt: dict[int, float] = {}
        self._injection_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._wake_cursor = 0

    async def register(self) -> None:
        if self.state.thread_id is None or self.protocol.thread_id != self.state.thread_id:
            raise DeliveryBlocked("adapter identity and observed TUI thread do not match")
        prior = await self.broker.get_status(agent_id=self.state.agent_id)
        broker_status = "running" if self.protocol.thread_status == "active" else "idle"
        await self.broker.register(
            agent_id=self.state.agent_id,
            thread_id=self.state.thread_id,
            status=broker_status,
            current_task=None,
        )
        confirmed_name = prior.get("relay_name") if isinstance(prior, dict) else None
        if prior is None and self.relay_name:
            claimed = await self.broker.claim_name(
                agent_id=self.state.agent_id, relay_name=self.relay_name
            )
            confirmed_name = claimed.get("relay_name")
        self.state.last_broker_confirmed_name = (
            confirmed_name if isinstance(confirmed_name, str) and confirmed_name else None
        )
        await self.state_store.save(self.state)
        self.registered = True

    async def refresh_confirmed_name(self) -> None:
        status = await self.broker.get_status(agent_id=self.state.agent_id)
        if status is None:
            self.registered = False
            return
        name = status.get("relay_name")
        confirmed = name if isinstance(name, str) and name else None
        if confirmed != self.state.last_broker_confirmed_name:
            self.state.last_broker_confirmed_name = confirmed
            await self.state_store.save(self.state)

    async def drain_once(self) -> int:
        deliveries = await self.broker.get_deliveries(agent_id=self.state.agent_id)
        outstanding = {
            row.get("event_id")
            for row in deliveries
            if isinstance(row, dict) and isinstance(row.get("event_id"), int)
        }
        for event_id in tuple(self._last_attempt):
            if event_id not in outstanding:
                self._last_attempt.pop(event_id, None)
        delivered = 0
        for delivery in deliveries:
            event_id = delivery.get("event_id") if isinstance(delivery, dict) else None
            if not isinstance(event_id, int) or event_id < 1:
                raise DeliveryBlocked("broker delivery has an invalid event id")
            now = self.monotonic()
            last_attempt = self._last_attempt.get(event_id)
            if (
                last_attempt is not None
                and now - last_attempt < self.retry_interval_seconds
            ):
                continue
            async with self._injection_lock:
                try:
                    await self.protocol.accept_delivery(delivery)
                except DeliveryBlocked as exc:
                    await self.broker.set_status(
                        agent_id=self.state.agent_id,
                        thread_id=self.state.thread_id,
                        status="queued" if exc.transient else "blocked",
                        current_task=f"broker event {event_id}: {exc}",
                    )
                    if not exc.transient:
                        raise
                    continue
                finally:
                    self._last_attempt[event_id] = self.monotonic()
            delivered += 1
        return delivered

    def _next_retry_delay(self) -> float:
        if not self._last_attempt:
            return RETRY_INTERVAL_SECONDS
        due = min(
            attempted + self.retry_interval_seconds
            for attempted in self._last_attempt.values()
        )
        return max(0.05, due - self.monotonic())

    async def _listen_for_wakes(self) -> None:
        """Hold one broker event stream open and turn its events into a wake flag.

        Every streamed event, replayed or live, only SETS the flag. Drains are
        therefore bounded by how often a drain completes while events stream --
        frames that arrive together collapse into one -- never by the number of
        events or by reconnecting, and a quiet stream causes no requests at all.
        A dropped stream resumes from the last id seen, never from 0, so a
        reconnect cannot replay the log again; reconnects are paced by
        ``retry_delay``.
        """
        while True:
            try:
                async for event_id in self.broker.wake_events(after=self._wake_cursor):
                    self._wake_cursor = max(self._wake_cursor, event_id)
                    self._wake.set()
            except (DeliveryBlocked, httpx.HTTPError, OSError) as exc:
                LOGGER.warning("broker wake stream failed; inbox remains authoritative: %s", exc)
            await asyncio.sleep(self.retry_delay)

    async def run(self) -> None:
        listener = asyncio.create_task(
            self._listen_for_wakes(), name="codex-relay-broker-wake"
        )
        try:
            while True:
                # Clear BEFORE draining: an event that lands during the drain sets
                # the flag again, so the next wait returns at once instead of losing it.
                self._wake.clear()
                try:
                    if not self.registered:
                        await self.register()
                    else:
                        await self.refresh_confirmed_name()
                    await self.drain_once()
                except (DeliveryBlocked, httpx.HTTPError, OSError) as exc:
                    LOGGER.warning("relay broker operation failed; retrying: %s", exc)
                    self.registered = False
                    await asyncio.sleep(self.retry_delay)
                    continue
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._wake.wait(),
                        timeout=min(self._next_retry_delay(), RETRY_INTERVAL_SECONDS),
                    )
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)


def _decode_jsonrpc(raw: Any) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        message = json.loads(raw)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeliveryBlocked("relay proxy received invalid JSON") from exc
    if not isinstance(message, dict):
        raise DeliveryBlocked("relay proxy received a non-object JSON-RPC message")
    return message


class TransparentJsonRpcConnection:
    """One TUI-to-app-server connection with adapter requests multiplexed into it."""

    def __init__(
        self,
        downstream: Any,
        upstream: Any,
        *,
        thread_guard: Callable[[str, dict[str, Any]], Awaitable[None]],
        thread_observer: Callable[
            [AppServerProtocol, str, dict[str, Any], dict[str, Any]], Awaitable[None]
        ],
    ) -> None:
        self.downstream = downstream
        self.upstream = upstream
        self.thread_guard = thread_guard
        self.thread_observer = thread_observer
        self.router = JsonRpcIdRouter()
        self.protocol = AppServerProtocol(self)
        self._upstream_send_lock = asyncio.Lock()
        self._downstream_send_lock = asyncio.Lock()
        self._adapter_futures: dict[int, asyncio.Future] = {}

    async def _send_upstream(self, message: dict[str, Any]) -> None:
        async with self._upstream_send_lock:
            await self.upstream.send(json.dumps(message, separators=(",", ":")))

    async def _send_downstream(self, message: dict[str, Any]) -> None:
        async with self._downstream_send_lock:
            await self.downstream.send(json.dumps(message, separators=(",", ":")))

    async def request(
        self, method: str, params: dict[str, Any], *, timeout: float | None = None
    ) -> Any:
        request_id = self.router.reserve_adapter_request()
        future = asyncio.get_running_loop().create_future()
        self._adapter_futures[request_id] = future
        try:
            await self._send_upstream(
                {"id": request_id, "method": method, "params": params}
            )
            if timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._adapter_futures.pop(request_id, None)

    async def _client_to_server(self) -> None:
        async for raw in self.downstream:
            message = _decode_jsonrpc(raw)
            method = message.get("method")
            params = message.get("params")
            if (
                isinstance(method, str)
                and "id" in message
                and (
                    isinstance(params, dict)
                    or method in {"thread/start", "thread/resume", "thread/fork"}
                )
            ):
                try:
                    await self.thread_guard(
                        method, params if isinstance(params, dict) else {}
                    )
                except DeliveryBlocked as exc:
                    await self._send_downstream(
                        {
                            "id": message["id"],
                            "error": {"code": -32000, "message": str(exc)},
                        }
                    )
                    continue
            await self._send_upstream(self.router.client_to_upstream(message))

    async def _server_to_client(self) -> None:
        async for raw in self.upstream:
            message = _decode_jsonrpc(raw)
            method = message.get("method")
            params = message.get("params")
            if isinstance(method, str) and isinstance(params, dict) and "id" not in message:
                await self.protocol.handle_notification(method, params)
            response_id = message.get("id") if "method" not in message else None
            if response_id in self._adapter_futures:
                future = self._adapter_futures[response_id]
                self.router.complete_adapter_request(response_id)
                error = message.get("error")
                if isinstance(error, dict):
                    future.set_exception(
                        AppServerRejected(
                            int(error.get("code", -32603)),
                            str(error.get("message", "unknown JSON-RPC error")),
                            error.get("data"),
                        )
                    )
                elif "result" in message:
                    future.set_result(message["result"])
                else:
                    future.set_exception(
                        AppServerRejected(-32603, "response has neither result nor error")
                    )
                continue
            context = (
                self.router.client_request_context(response_id)
                if response_id is not None
                else None
            )
            if context is not None and "result" in message:
                _, request_method, request_params = context
                if request_method in {"thread/start", "thread/resume"}:
                    result = message.get("result")
                    if isinstance(result, dict):
                        await self.thread_observer(
                            self.protocol, request_method, request_params, result
                        )
            routed = self.router.upstream_to_client(message)
            if routed is not None:
                await self._send_downstream(routed)

    async def run(self) -> None:
        client_task = asyncio.create_task(
            self._client_to_server(), name="codex-relay-tui-to-app-server"
        )
        server_task = asyncio.create_task(
            self._server_to_client(), name="codex-relay-app-server-to-tui"
        )
        try:
            done, pending = await asyncio.wait(
                {client_task, server_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                exception = task.exception()
                if exception is not None:
                    raise exception
        finally:
            for future in self._adapter_futures.values():
                if not future.done():
                    future.set_exception(JsonRpcDisconnected("proxied connection closed"))


class RelayProxyHost:
    """Own one normal TUI connection and its memory-only delivery scheduler."""

    def __init__(
        self,
        *,
        app_server_url: str,
        broker: BrokerHttpClient,
        store: StateStore,
        state: AdapterState,
        relay_name: str | None,
        allow_new_thread: bool,
    ) -> None:
        self.app_server_url = validate_loopback_websocket_url(app_server_url)
        self.broker = broker
        self.store = store
        self.state = state
        self.relay_name = relay_name
        self.allow_new_thread = allow_new_thread
        self.connection_active = False
        self.adapter: CodexAdapter | None = None
        self.adapter_task: asyncio.Task | None = None

    async def guard_thread_request(self, method: str, params: dict[str, Any]) -> None:
        if method == "thread/fork":
            raise DeliveryBlocked("in-TUI cross-thread switching is disabled")
        if method == "thread/start" and self.state.thread_id is not None:
            if not self.allow_new_thread:
                raise DeliveryBlocked(
                    "this relay invocation already owns a thread; start a new invocation"
                )
        if method == "thread/start":
            approval_policy = params.get("approvalPolicy")
            if (
                any(
                    not isinstance(params.get(name), str) or not params[name].strip()
                    for name in ("sandbox", "approvalsReviewer")
                )
                or not (
                    isinstance(approval_policy, dict)
                    or (
                        isinstance(approval_policy, str)
                        and approval_policy.strip()
                    )
                )
            ):
                raise DeliveryBlocked(
                    "thread/start requires non-empty sandbox and approvalsReviewer "
                    "strings and an approvalPolicy string or object"
                )
        if method == "thread/resume":
            requested = params.get("threadId")
            if requested != self.state.thread_id:
                raise DeliveryBlocked(
                    "this relay invocation cannot switch to a different thread"
                )
            if requested == self.state.thread_id:
                approval_policy = self.state.approval_policy
                if (
                    any(
                        not isinstance(value, str) or not value.strip()
                        for value in (
                            self.state.sandbox,
                            self.state.approvals_reviewer,
                        )
                    )
                    or not (
                        isinstance(approval_policy, dict)
                        or (
                            isinstance(approval_policy, str)
                            and approval_policy.strip()
                        )
                    )
                ):
                    raise DeliveryBlocked(
                        "saved relay permission selections are incomplete; "
                        "start a new relay thread"
                    )
                params.update(
                    {
                        "sandbox": self.state.sandbox,
                        "approvalPolicy": self.state.approval_policy,
                        "approvalsReviewer": self.state.approvals_reviewer,
                    }
                )

    async def observe_thread_response(
        self,
        protocol: AppServerProtocol,
        method: str,
        params: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        thread = result.get("thread")
        if not isinstance(thread, dict):
            raise DeliveryBlocked(f"{method} returned no thread document")
        thread_id = protocol.bind_thread_result(thread)
        if (
            self.state.thread_id is not None
            and self.state.thread_id != thread_id
            and not self.allow_new_thread
        ):
            raise DeliveryBlocked("app-server returned a different thread than requested")
        self.state.thread_id = thread_id
        if method == "thread/start":
            self.state.sandbox = params["sandbox"]
            self.state.approval_policy = params["approvalPolicy"]
            self.state.approvals_reviewer = params["approvalsReviewer"]
        self.allow_new_thread = False
        await self.store.save(self.state)
        if self.adapter_task is not None:
            return
        self.adapter = CodexAdapter(
            protocol,
            self.broker,
            self.store,
            self.state,
            relay_name=self.relay_name,
        )
        try:
            await self.adapter.register()
        except (DeliveryBlocked, httpx.HTTPError, OSError) as exc:
            LOGGER.warning(
                "broker unavailable during TUI thread attachment; ordinary Codex continues: %s",
                exc,
            )
        self.adapter_task = asyncio.create_task(
            self.adapter.run(), name="codex-relay-delivery-loop"
        )

    async def handle_connection(self, downstream: Any) -> None:
        if self.connection_active:
            await downstream.close(code=1013, reason="relay thread already has a TUI")
            return
        self.connection_active = True
        try:
            async with websocket_connect(self.app_server_url, max_size=None) as upstream:
                connection = TransparentJsonRpcConnection(
                    downstream,
                    upstream,
                    thread_guard=self.guard_thread_request,
                    thread_observer=self.observe_thread_response,
                )
                await connection.run()
        finally:
            self.connection_active = False
            if self.adapter_task is not None:
                self.adapter_task.cancel()
                await asyncio.gather(self.adapter_task, return_exceptions=True)
                self.adapter_task = None
                self.adapter = None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Proxy one normal Codex TUI connection and inject broker-outstanding "
            "messages without acknowledging app-server acceptance."
        )
    )
    parser.add_argument("--listen-url", required=True)
    parser.add_argument("--app-server-url", required=True)
    parser.add_argument(
        "--broker-url", default=os.environ.get("BROKER_URL", "http://127.0.0.1:8787")
    )
    parser.add_argument("--state", required=True)
    parser.add_argument("--agent-id", type=lambda value: str(UUID(value)))
    parser.add_argument("--readiness-file")
    parser.add_argument("--relay-name")
    parser.add_argument("--working-directory", default=os.getcwd())
    parser.add_argument("--new-thread", action="store_true")
    parser.add_argument("--bearer-token", default=os.environ.get("BROKER_BEARER_TOKEN"))
    parser.add_argument("--log-level", default="INFO")
    return parser


async def _run_from_args(args: argparse.Namespace) -> None:
    listen_host, listen_port = _bind_address(args.listen_url)
    app_server_url = validate_loopback_websocket_url(args.app_server_url)
    _, app_server_port = _bind_address(app_server_url)
    if listen_port == app_server_port:
        raise DeliveryBlocked("proxy and app-server ports must differ")
    working_directory = str(Path(args.working_directory).resolve())
    if not Path(working_directory).is_dir():
        raise DeliveryBlocked("working directory does not exist")
    store = StateStore(args.state)
    lock = StateLock(args.state)
    lock.acquire()
    broker: BrokerHttpClient | None = None
    try:
        await reset_readiness_file(args.readiness_file)
        state = await store.load()
        if state is None:
            state = AdapterState.new(
                working_directory=working_directory,
                agent_id=args.agent_id,
            )
        elif state.working_directory != working_directory:
            raise DeliveryBlocked(
                "persisted adapter working directory differs from this launch"
            )
        elif args.agent_id is not None and state.agent_id != args.agent_id:
            raise DeliveryBlocked(
                "persisted adapter agent identity differs from this launch"
            )
        await store.save(state)  # also removes any accepted v1 message journal
        broker = BrokerHttpClient(
            args.broker_url, bearer_token=args.bearer_token
        )
        host = RelayProxyHost(
            app_server_url=app_server_url,
            broker=broker,
            store=store,
            state=state,
            relay_name=args.relay_name,
            allow_new_thread=args.new_thread or state.thread_id is None,
        )
        async with websocket_serve(
            host.handle_connection, listen_host, listen_port, max_size=None
        ):
            await publish_readiness_file(args.readiness_file, state)
            await asyncio.Future()
    finally:
        if broker is not None:
            await broker.close()
        lock.release()


def _configure_console_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, str(log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    _configure_console_logging(args.log_level)
    try:
        asyncio.run(_run_from_args(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        LOGGER.error("adapter stopped: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
