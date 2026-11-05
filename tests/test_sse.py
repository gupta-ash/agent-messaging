"""SSE tests: replay, live broadcast, backpressure / slow-consumer eviction.

These tests drive the SSE generator directly rather than via the HTTP layer,
because ASGITransport (httpx test transport) does not reliably propagate
client-disconnect signals to a long-lived SSE generator. Direct testing covers
the same logic with deterministic teardown via `aclose()`.
"""

from __future__ import annotations

import asyncio
import json

import pytest

pytestmark = pytest.mark.asyncio


class _MockRequest:
    """Minimal Request stand-in for the SSE generator. Only is_disconnected matters."""

    def __init__(self):
        self._disconnected = False

    async def is_disconnected(self) -> bool:
        return self._disconnected

    def disconnect(self):
        self._disconnected = True


async def _post_n_events(client, n, channel="#cc-demo_research"):
    ids = []
    for i in range(n):
        r = await client.post("/api/event", json={
            "from_agent": "ag1", "channel": channel,
            "kind": "user_msg", "content": f"msg-{i}", "ts": 1746400000000 + i,
        })
        assert r.status_code == 200
        ids.append(r.json()["event_id"])
    return ids


async def _drain(gen, n, timeout=2.0):
    """Pull up to n events from the generator, then aclose() it."""
    results = []
    try:
        async with asyncio.timeout(timeout):
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                results.append(evt)
                if len(results) >= n:
                    break
    except asyncio.TimeoutError:
        pass
    finally:
        await gen.aclose()
    return results


async def test_sse_replay_from_cursor_query(client):
    from broker import server
    ids = await _post_n_events(client, 5)
    req = _MockRequest()
    gen = server.sse_event_generator(req, ids[1])
    msgs = await _drain(gen, n=3)
    # Should receive ids[2], ids[3], ids[4]
    assert len(msgs) == 3
    assert msgs[0]["id"] == str(ids[2])
    assert msgs[2]["id"] == str(ids[4])


async def test_sse_replay_full_history(client):
    from broker import server
    ids = await _post_n_events(client, 4)
    req = _MockRequest()
    gen = server.sse_event_generator(req, 0)
    msgs = await _drain(gen, n=4)
    assert len(msgs) == 4
    assert [m["id"] for m in msgs] == [str(i) for i in ids]


async def test_sse_replay_id_field_matches_message_log_event_id(client):
    from broker import server
    ids = await _post_n_events(client, 2)
    req = _MockRequest()
    gen = server.sse_event_generator(req, 0)
    msgs = await _drain(gen, n=2)
    assert msgs[0]["id"] == str(ids[0])
    assert msgs[1]["id"] == str(ids[1])


async def test_sse_event_payload_shape(client):
    from broker import server
    await _post_n_events(client, 1)
    req = _MockRequest()
    gen = server.sse_event_generator(req, 0)
    msgs = await _drain(gen, n=1)
    assert len(msgs) == 1
    payload = json.loads(msgs[0]["data"])
    assert "ts" in payload
    assert "kind" in payload
    assert "agent" in payload
    assert "data" in payload


async def test_sse_live_broadcast(client):
    """Subscribe with no replay (no events yet), then broadcast → should receive live."""
    from broker import server
    req = _MockRequest()
    gen = server.sse_event_generator(req, 0)

    received = asyncio.Event()
    payload_holder = {}

    async def reader():
        try:
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                payload_holder["evt"] = evt
                received.set()
                return
        finally:
            pass

    reader_task = asyncio.create_task(reader())
    await asyncio.sleep(0.2)  # let reader subscribe + finish replay (empty)

    # Broadcast directly (don't go through HTTP — generator is already subscribed)
    await server.broadcast_event({
        "event_id": 9999, "ts": 1, "kind": "user_msg",
        "agent": "live-test", "data": {"content": "live"},
    })

    try:
        await asyncio.wait_for(received.wait(), timeout=2.0)
        assert payload_holder["evt"]["id"] == "9999"
    finally:
        reader_task.cancel()
        try:
            await reader_task
        except (asyncio.CancelledError, Exception):
            pass
        await gen.aclose()


async def test_sse_disconnect_terminates_generator(client):
    """Setting is_disconnected=True should let the generator exit on its next loop iteration."""
    from broker import server
    req = _MockRequest()
    gen = server.sse_event_generator(req, 0)

    async def reader():
        events = []
        async for evt in gen:
            events.append(evt)
            if len(events) >= 5:  # generous cap
                break
        return events

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.1)
    req.disconnect()
    # Push one event so the queue.get unblocks
    await server.broadcast_event({
        "event_id": 1, "ts": 1, "kind": "user_msg",
        "agent": "x", "data": {},
    })
    try:
        events = await asyncio.wait_for(task, timeout=3.0)
        # generator may have yielded 0 or 1 event before noticing disconnect
        assert len(events) <= 5
    finally:
        await gen.aclose()


async def test_sse_backpressure_marks_slow_consumer_dead(client):
    """Saturate a subscriber's queue → next broadcast marks it dead."""
    from broker import server

    sub = server._Subscriber(0)
    async with server._subs_lock:
        server._subscribers.append(sub)
    try:
        # Saturate the queue (maxsize 1000)
        for i in range(1000):
            sub.queue.put_nowait({
                "event_id": i, "ts": i, "kind": "user_msg",
                "agent": "test", "data": {},
            })

        # Now broadcast — should mark sub dead
        await server.broadcast_event({
            "event_id": 1001, "ts": 1, "kind": "user_msg",
            "agent": "test", "data": {},
        })
        assert sub.dead is True
    finally:
        async with server._subs_lock:
            if sub in server._subscribers:
                server._subscribers.remove(sub)


async def test_sse_other_subscribers_unaffected_by_slow_consumer(client):
    """When one consumer is dead, broadcast still reaches the live one."""
    from broker import server

    slow = server._Subscriber(0)
    fast = server._Subscriber(0)
    async with server._subs_lock:
        server._subscribers.extend([slow, fast])
    try:
        # Saturate slow
        for i in range(1000):
            slow.queue.put_nowait({"event_id": i, "ts": 0, "kind": "x", "agent": "x", "data": {}})

        # Broadcast — slow gets marked dead, fast gets the event
        await server.broadcast_event({
            "event_id": 5000, "ts": 1, "kind": "user_msg",
            "agent": "test", "data": {"content": "live"},
        })

        assert slow.dead is True
        assert fast.dead is False
        # fast.queue should have one item
        assert fast.queue.qsize() == 1
        item = fast.queue.get_nowait()
        assert item["event_id"] == 5000
    finally:
        async with server._subs_lock:
            for s in (slow, fast):
                if s in server._subscribers:
                    server._subscribers.remove(s)


# ---------- F2 (iter1 discriminator): SSE kind preservation for permission_reply + error ----------

async def _post_event_with_kind(client, kind: str, content: str = "x"):
    """Bypass Pydantic by writing directly to message_log so we can use raw kind
    values that may not be in VALID_EVENT_KINDS at the API boundary. For the
    F2 regression we want to test the SSE replay path's kind mapping for the
    real M2 kinds (permission_reply, error) end-to-end."""
    from broker import server
    async with server.connect_db() as conn:
        cur = await conn.execute(
            "INSERT INTO message_log "
            "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
            "VALUES (NULL, ?, NULL, '#test', NULL, ?, ?, ?)",
            ("test-agent", kind, content, 1746400000000),
        )
        eid = cur.lastrowid
        await cur.close()
        await conn.commit()
    return eid


async def _read_one_sse_event(client, cursor: int = 0, timeout: float = 2.0):
    from broker import server
    req = _MockRequest()
    gen = server.sse_event_generator(req, cursor)
    try:
        async with asyncio.timeout(timeout):
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                return json.loads(evt["data"])
    except asyncio.TimeoutError:
        return None
    finally:
        await gen.aclose()


async def test_sse_replay_preserves_permission_reply_kind(client):
    """F2: a message_log row with kind='permission_reply' MUST surface to SSE
    consumers under kind='permission_reply' (NOT collapsed to 'message_logged').
    D16 § (b) requires consumers to be able to filter on this kind."""
    await _post_event_with_kind(
        client, "permission_reply",
        '{"request_id": "abcde", "behavior": "allow"}',
    )
    payload = await _read_one_sse_event(client)
    assert payload is not None, "SSE consumer never received the event"
    assert payload["kind"] == "permission_reply", (
        f"expected kind='permission_reply' on the wire, got {payload['kind']!r} "
        f"(this is the F2 bug — sse_kind() must preserve permission_reply)"
    )


async def test_sse_replay_preserves_error_kind(client):
    """F2: D16 error events (catchup_truncated, persistent-open) MUST surface
    to SSE consumers under kind='error', not 'message_logged'."""
    await _post_event_with_kind(
        client, "error",
        "catchup_truncated=true; gap may exceed 100 messages",
    )
    payload = await _read_one_sse_event(client)
    assert payload is not None
    assert payload["kind"] == "error"


async def test_sse_live_broadcast_preserves_permission_reply_kind(client):
    """F2 live path: broadcast_event with kind='permission_reply' must NOT be
    remapped to 'message_logged' on the live SSE path either."""
    from broker import server
    req = _MockRequest()
    gen = server.sse_event_generator(req, 999_999)  # cursor past everything → no replay

    received = asyncio.Event()
    holder = {}

    async def reader():
        async for evt in gen:
            if evt.get("event") == "heartbeat":
                continue
            holder["evt"] = json.loads(evt["data"])
            received.set()
            return

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.2)
    await server.broadcast_event({
        "event_id": 12345, "ts": 1, "kind": "permission_reply",
        "agent": "user:abc",
        "data": {"to_agent": None, "content": "{}", "channel": "#x", "thread_id": None},
    })
    try:
        await asyncio.wait_for(received.wait(), timeout=2.0)
        assert holder["evt"]["kind"] == "permission_reply"
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        await gen.aclose()


async def test_sse_live_broadcast_preserves_error_kind(client):
    """F2 live path: kind='error' preserved on the live SSE path."""
    from broker import server
    req = _MockRequest()
    gen = server.sse_event_generator(req, 999_999)

    received = asyncio.Event()
    holder = {}

    async def reader():
        async for evt in gen:
            if evt.get("event") == "heartbeat":
                continue
            holder["evt"] = json.loads(evt["data"])
            received.set()
            return

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.2)
    await server.broadcast_event({
        "event_id": 12346, "ts": 1, "kind": "error",
        "agent": "system:discord_poller",
        "data": {"to_agent": None, "content": "boom", "channel": "#x", "thread_id": None},
    })
    try:
        await asyncio.wait_for(received.wait(), timeout=2.0)
        assert holder["evt"]["kind"] == "error"
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        await gen.aclose()


async def test_sse_kind_preserves_permission_reply_unit():
    """F2 unit-level: sse_kind('permission_reply') == 'permission_reply'."""
    from broker import server
    assert server.sse_kind("permission_reply") == "permission_reply"


async def test_sse_kind_preserves_error_unit():
    """F2 unit-level: sse_kind('error') == 'error'."""
    from broker import server
    assert server.sse_kind("error") == "error"


async def test_sse_kind_still_maps_user_msg_to_message_logged():
    """F2 no-regression + F-CC30 (local-relay-channel iter-1 M1): user_msg still
    collapses to message_logged; agent_reply + handoff now PRESERVED (per F-CC30
    additive expansion — matches agent_message precedent so SSE consumers like
    the local-relay-channel MCP server can route per-kind without losing them).
    """
    from broker import server
    assert server.sse_kind("user_msg") == "message_logged"
    assert server.sse_kind("agent_reply") == "agent_reply"  # F-CC30 M1
    assert server.sse_kind("handoff") == "handoff"  # F-CC30 M1


# ---------- v1.3 (D28): agent_message SSE kind preservation ----------


async def test_sse_replay_preserves_agent_message_kind(client):
    """v1.3 / D28: a POSTed agent_message event MUST surface to SSE consumers
    under kind='agent_message' (NOT collapsed to 'message_logged'). Mirrors the
    F2 pattern for permission_reply / error: the dashboard subscribes to the
    distinct kind for live filtering. Also validates the sse_kind unit
    mapping end-to-end (POST → message_log → replay → wire kind)."""
    r = await client.post("/api/event", json={
        "from_agent": "session-A", "to_agent": "session-B", "channel": "#cc-relay",
        "kind": "agent_message", "content": "test", "ts": 1746404783000,
        "client_event_id": "sse-am-1",
    })
    assert r.status_code == 200, r.text
    payload = await _read_one_sse_event(client)
    assert payload is not None, "SSE consumer never received the event"
    assert payload["kind"] == "agent_message", (
        f"expected kind='agent_message' on the wire, got {payload['kind']!r}"
    )
    # Sanity: the unit-level mapper agrees with the wire shape.
    from broker import server
    assert server.sse_kind("agent_message") == "agent_message"
