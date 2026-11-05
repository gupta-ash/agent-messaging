"""Standalone stdio MCP operations for a stable Codex broker identity."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Annotated, Any, Literal
from uuid import uuid4

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import Field


SQLITE_I64_MAX = 9_223_372_036_854_775_807
DEFAULT_BROKER_URL = "http://127.0.0.1:8787"
After = Annotated[int, Field(ge=0, le=SQLITE_I64_MAX)]
Limit = Annotated[int, Field(ge=1, le=100)]
AgentName = Annotated[str, Field(min_length=1, max_length=256)]
RelayName = Annotated[str, Field(min_length=1, max_length=64)]
Channel = Annotated[str, Field(min_length=1)]
EventKind = Literal["agent_message", "agent_reply", "handoff"]
EventId = Annotated[int, Field(ge=1, le=SQLITE_I64_MAX)]


def _epoch_ms() -> int:
    return int(time.time() * 1000)


class _BrokerRelayHttp:
    """Minimal HTTP bridge; the broker remains the contract authority."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str | None,
        client: httpx.AsyncClient | None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = (
            {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}
        )
        self.client = client

    @staticmethod
    def _failure_detail(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text.strip() or "broker returned no error detail"
        detail = payload.get("detail", payload) if isinstance(payload, dict) else payload
        if isinstance(detail, str):
            return detail
        return json.dumps(detail, sort_keys=True)

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, int] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        async def _send(client: httpx.AsyncClient) -> httpx.Response:
            return await client.request(
                method,
                path,
                params=params,
                json=json_body,
                headers=self.headers,
            )

        try:
            if self.client is not None:
                response = await _send(self.client)
            else:
                async with httpx.AsyncClient(
                    base_url=self.base_url,
                    timeout=30.0,
                ) as client:
                    response = await _send(client)
        except httpx.RequestError as exc:
            raise ToolError(f"broker request failed: {exc}") from exc

        if response.is_error:
            detail = self._failure_detail(response)
            raise ToolError(f"broker request failed with HTTP {response.status_code}: {detail}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise ToolError(
                f"broker returned invalid JSON with HTTP {response.status_code}"
            ) from exc
        if not isinstance(payload, dict):
            raise ToolError(
                f"broker returned a non-object JSON document with HTTP {response.status_code}"
            )
        return payload


def _validate_agent_id(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("agent identity must not be empty")
    if len(value) > 256:
        raise argparse.ArgumentTypeError("agent identity exceeds broker limit of 256 characters")
    return value


def create_server(
    *,
    agent_id: str,
    broker_url: str = DEFAULT_BROKER_URL,
    bearer_token: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastMCP:
    """Create the relay MCP server bound to one immutable agent identity."""

    try:
        stable_agent_id = _validate_agent_id(agent_id)
    except argparse.ArgumentTypeError as exc:
        raise ValueError(str(exc)) from exc
    broker = _BrokerRelayHttp(
        broker_url,
        bearer_token=bearer_token,
        client=http_client,
    )
    server = FastMCP("codex-relay")

    @server.tool(name="register")
    async def register(
        thread_id: str,
        status: str,
        current_task: str | None = None,
    ) -> dict[str, Any]:
        """Register or refresh this Codex session at the broker."""

        return await broker.request(
            "POST",
            f"/api/agent/{stable_agent_id}/status",
            json_body={
                "kind": "codex-session",
                "channel": "#codex-relay",
                "thread_id": thread_id,
                "status": status,
                "current_task": current_task,
                "last_activity_ts": _epoch_ms(),
                "transport": "codex-app-server",
            },
        )

    @server.tool(name="name")
    async def name(
        relay_name: RelayName,
        takeover: bool = False,
    ) -> dict[str, Any]:
        """Claim or rename this session's broker mailbox."""

        return await broker.request(
            "PUT",
            f"/api/agent/{stable_agent_id}/relay-name",
            json_body={"relay_name": relay_name, "takeover": takeover},
        )

    @server.tool(name="check")
    async def check(after: After = 0, limit: Limit = 20) -> dict[str, Any]:
        """Read status and outstanding deliveries without acknowledging them."""

        status = await broker.request(
            "GET", f"/api/agent/{stable_agent_id}/status"
        )
        page = await broker.request(
            "GET",
            f"/api/agent/{stable_agent_id}/deliveries",
            params={"after": after, "limit": limit},
        )
        return {"status": status, **page}

    @server.tool(name="ack")
    async def ack(event_id: EventId) -> dict[str, Any]:
        """Acknowledge receipt of one relay event before doing its requested work."""

        return await broker.request(
            "POST",
            f"/api/agent/{stable_agent_id}/delivery-ack",
            json_body={"event_ids": [event_id]},
        )

    @server.tool(name="send")
    async def send(
        to_agent: AgentName,
        content: str,
        channel: Channel = "#codex-relay",
        kind: EventKind = "agent_message",
    ) -> dict[str, Any]:
        """Send one directed broker event from this Codex identity's mailbox."""

        status = await broker.request(
            "GET", f"/api/agent/{stable_agent_id}/status"
        )
        relay_name = status.get("relay_name")
        if not isinstance(relay_name, str) or not relay_name:
            raise ToolError(
                "broker identity has no relay name; call name before send"
            )

        return await broker.request(
            "POST",
            "/api/event",
            json_body={
                "from_agent": relay_name,
                "to_agent": to_agent,
                "channel": channel,
                "kind": kind,
                "content": content,
                "ts": _epoch_ms(),
                "client_event_id": str(uuid4()),
            },
        )

    return server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-id", required=True, type=_validate_agent_id)
    parser.add_argument(
        "--broker-url",
        default=os.environ.get("BROKER_URL", DEFAULT_BROKER_URL),
    )
    parser.add_argument(
        "--bearer-token",
        default=os.environ.get("BROKER_BEARER_TOKEN"),
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    server = create_server(
        agent_id=args.agent_id,
        broker_url=args.broker_url,
        bearer_token=args.bearer_token,
    )
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
