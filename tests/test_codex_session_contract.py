"""R1 product-neutral session identity and delivery contract.

These tests deliberately exercise the HTTP surface and the persisted SQLite rows.
Codex registration never supplies or fabricates ``CLAUDE_CODE_SESSION_ID``.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from uuid import uuid4

import aiosqlite
import pytest


pytestmark = pytest.mark.asyncio


async def _register(
    client,
    agent_id: str,
    *,
    transport: str | None = "codex-app-server",
    kind: str = "codex-session",
    channel: str = "#codex-relay",
    display_name: str | None = None,
    thread_id: str | None = "thread-1",
    client_event_id: str | None = None,
):
    body = {
        "kind": kind,
        "channel": channel,
        "thread_id": thread_id,
        "status": "idle",
        "last_activity_ts": 1,
    }
    if transport is not None:
        body["transport"] = transport
    if display_name is not None:
        body["display_name"] = display_name
    if client_event_id is not None:
        body["client_event_id"] = client_event_id
    return await client.post(f"/api/agent/{agent_id}/status", json=body)


async def _claim(client, agent_id: str, relay_name: str, *, takeover: bool = False):
    return await client.put(
        f"/api/agent/{agent_id}/relay-name",
        json={"relay_name": relay_name, "takeover": takeover},
    )


async def _row(agent_id: str):
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT * FROM agent_status WHERE id = ?", (agent_id,))
        row = await cur.fetchone()
        await cur.close()
        return dict(row) if row else None


async def _send(client, to_agent: str, kind: str, suffix: str):
    response = await client.post(
        "/api/event",
        json={
            "from_agent": "sender",
            "to_agent": to_agent,
            "channel": "#cc-relay",
            "kind": kind,
            "content": suffix,
            "ts": 10,
            "client_event_id": f"r1-{suffix}",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["event_id"]


async def _seed_pre_r1_delivery(to_agent: str, suffix: str) -> int:
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        cur = await conn.execute(
            "INSERT INTO message_log "
            "(client_event_id, from_agent, to_agent, channel, kind, content, ts) "
            "VALUES (?, 'legacy-sender', ?, '#cc-relay', 'agent_message', ?, 9)",
            (f"pre-r1-{suffix}", to_agent, suffix),
        )
        event_id = cur.lastrowid
        await cur.close()
        await conn.execute(
            "INSERT INTO delivery "
            "(to_agent, event_id, status, attempts, created_ts, updated_ts) "
            "VALUES (?, ?, 'outstanding', 0, 9, 9)",
            (to_agent, event_id),
        )
        await conn.commit()
        return event_id


async def test_codex_registers_without_claude_session_environment(client, monkeypatch):
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    agent_id = str(uuid4())

    registered = await _register(client, agent_id)

    assert registered.status_code == 200, registered.text
    status = await client.get(f"/api/agent/{agent_id}/status")
    assert status.status_code == 200, status.text
    assert status.json()["id"] == agent_id
    assert status.json()["transport"] == "codex-app-server"
    assert status.json()["relay_name"] is None
    assert "CLAUDE_CODE_SESSION_ID" not in os.environ


async def test_relay_name_probe_reports_codex_claude_unknown_and_invalid(client):
    codex_id = str(uuid4())
    assert (await _register(client, codex_id)).status_code == 200
    assert (await _claim(client, codex_id, "R4B.CX.RECV")).status_code == 200

    claude_id = str(uuid4())
    claude = await _register(
        client,
        claude_id,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="R4B.CC.RECV",
        client_event_id="relay-register-r4b-claude",
    )
    assert claude.status_code == 200, claude.text

    known = await client.get("/api/relay-name/R4B.CX.RECV")
    assert known.status_code == 200
    assert known.json() == {
        "relay_name": "r4b.cx.recv",
        "registered": True,
        "status": "idle",
        "transport": "codex-app-server",
    }

    known_claude = await client.get("/api/relay-name/r4b.cc.recv")
    assert known_claude.status_code == 200
    assert known_claude.json() == {
        "relay_name": "r4b.cc.recv",
        "registered": True,
        "status": "idle",
        "transport": "claude-code",
    }

    unknown = await client.get("/api/relay-name/r4b.unknown")
    assert unknown.status_code == 200
    assert unknown.json() == {
        "relay_name": "r4b.unknown",
        "registered": False,
        "status": None,
        "transport": None,
    }

    invalid = await client.get("/api/relay-name/bad%20name")
    assert invalid.status_code == 422


async def test_transport_is_optional_but_nonempty_and_bounded(client):
    legacy = await _register(client, "legacy-no-transport", transport=None)
    assert legacy.status_code == 200, legacy.text

    empty = await _register(client, "empty-transport", transport="")
    assert empty.status_code == 422
    overlong = await _register(client, "long-transport", transport="x" * 65)
    assert overlong.status_code == 422


async def test_stable_identity_refreshes_mutable_thread_and_preserves_claim(client):
    agent_id = str(uuid4())
    assert (await _register(client, agent_id)).status_code == 200
    claim = await _claim(client, agent_id, "CoDeX.One")
    assert claim.status_code == 200, claim.text
    assert claim.json() == {
        "id": agent_id,
        "relay_name": "codex.one",
        "displaced_id": None,
    }

    refreshed = await _register(client, agent_id, thread_id="thread-2")
    assert refreshed.status_code == 200, refreshed.text
    status = (await client.get(f"/api/agent/{agent_id}/status")).json()
    assert status["current_thread"] == "thread-2"
    assert status["relay_name"] == "codex.one"
    assert status["transport"] == "codex-app-server"


async def test_named_status_counts_handoffs_for_owned_name_and_stable_id(client):
    agent_id = str(uuid4())
    assert (await _register(client, agent_id)).status_code == 200
    assert (await _claim(client, agent_id, "MiXeD.Box")).status_code == 200
    await _send(client, "MIXED.BOX", "handoff", "status-queue-depth")
    await _send(client, agent_id, "handoff", "status-stable-id-queue-depth")

    status = await client.get(f"/api/agent/{agent_id}/status")

    assert status.status_code == 200, status.text
    assert status.json()["queue_depth"] == 2


@pytest.mark.parametrize("relay_name", ["", "bad name", "x" * 65, "cafe\N{LATIN SMALL LETTER E WITH ACUTE}"])
async def test_relay_name_claim_rejects_invalid_names(client, relay_name):
    agent_id = str(uuid4())
    assert (await _register(client, agent_id)).status_code == 200
    response = await _claim(client, agent_id, relay_name)
    assert response.status_code == 422
    assert (await _row(agent_id))["relay_name"] is None


async def test_collision_without_takeover_is_409_and_changes_neither_owner(client):
    first, second = str(uuid4()), str(uuid4())
    assert (await _register(client, first)).status_code == 200
    assert (await _register(client, second)).status_code == 200
    assert (await _claim(client, first, "Shared.Box")).status_code == 200

    before_first = await _row(first)
    before_second = await _row(second)
    collision = await _claim(client, second, "SHARED.BOX")

    assert collision.status_code == 409
    assert await _row(first) == before_first
    assert await _row(second) == before_second
    refresh = await _claim(client, first, "shared.box")
    assert refresh.status_code == 200, refresh.text


async def test_same_owner_rename_keeps_legacy_rollback_mailbox_safe(client):
    agent_id = str(uuid4())
    assert (await _register(client, agent_id)).status_code == 200
    assert (await _claim(client, agent_id, "oldbox")).status_code == 200
    acked_id = await _send(client, "OLDBOX", "agent_message", "rename-acked")
    outstanding_id = await _send(client, "oldbox", "handoff", "rename-outstanding")
    acked = await client.post(
        f"/api/agent/{agent_id}/delivery-ack", json={"event_ids": [acked_id]}
    )
    assert acked.status_code == 200, acked.text

    renamed = await _claim(client, agent_id, "NewBox")
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["relay_name"] == "newbox"

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT to_agent, event_id, status FROM delivery ORDER BY event_id"
        )
        deliveries = [dict(row) for row in await cur.fetchall()]
        await cur.close()
        cur = await conn.execute(
            "SELECT to_agent, event_id FROM message_log WHERE event_id IN (?, ?) "
            "ORDER BY event_id",
            (acked_id, outstanding_id),
        )
        messages = [dict(row) for row in await cur.fetchall()]
        await cur.close()

    assert deliveries == [
        {"to_agent": "oldbox", "event_id": acked_id, "status": "acked"},
        {"to_agent": "newbox", "event_id": outstanding_id, "status": "outstanding"},
    ]
    assert messages == [
        {"to_agent": "oldbox", "event_id": acked_id},
        {"to_agent": "newbox", "event_id": outstanding_id},
    ]
    stable = await client.get(f"/api/agent/{agent_id}/deliveries")
    assert [row["event_id"] for row in stable.json()["deliveries"]] == [outstanding_id]

    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        pre_r1_rows = conn.execute(
            "SELECT d.event_id FROM delivery d JOIN message_log m "
            "ON m.event_id=d.event_id AND m.to_agent=d.to_agent "
            "WHERE d.to_agent='newbox' AND d.status='outstanding'"
        ).fetchall()
        assert pre_r1_rows == [(outstanding_id,)]
        other_id = str(uuid4())
        conn.execute(
            "INSERT INTO agent_status "
            "(id,kind,channel,status,last_activity_ts,registered_at,transport) "
            "VALUES (?,'codex-session','#c','idle',1,1,'codex-app-server')",
            (other_id,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="stable_relay_namespace_retained"):
            conn.execute(
                "UPDATE agent_status SET relay_name='oldbox' WHERE id=?", (other_id,)
            )
        conn.rollback()
    finally:
        conn.close()


async def test_explicit_takeover_keeps_stored_rows_with_original_recipient(client):
    claude_id, codex_id = str(uuid4()), str(uuid4())
    claude = await _register(
        client,
        claude_id,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="Shared",
        client_event_id="relay-register-claude",
    )
    assert claude.status_code == 200, claude.text
    legacy_alias_id = await _seed_pre_r1_delivery(
        "Shared", "takeover-pre-r1-mixed"
    )
    event_id = await _send(client, "SHARED", "agent_message", "takeover")
    assert (await _register(client, codex_id)).status_code == 200

    taken = await _claim(client, codex_id, "shared", takeover=True)

    assert taken.status_code == 200, taken.text
    assert taken.json() == {
        "id": codex_id,
        "relay_name": "shared",
        "displaced_id": claude_id,
    }
    assert (await _row(claude_id))["relay_name"] == claude_id
    assert (await _row(claude_id))["status"] == "blocked"
    assert (await _row(codex_id))["relay_name"] == "shared"
    new_owner_deliveries = await client.get(f"/api/agent/{codex_id}/deliveries")
    assert new_owner_deliveries.json()["deliveries"] == []
    original_deliveries = await client.get(f"/api/agent/{claude_id}/deliveries")
    assert [row["event_id"] for row in original_deliveries.json()["deliveries"]] == [
        legacy_alias_id,
        event_id,
    ]
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        stored = conn.execute(
            "SELECT d.event_id,d.recipient_id,d.to_agent,m.to_agent "
            "FROM delivery d JOIN message_log m ON m.event_id=d.event_id "
            "WHERE d.event_id IN (?,?) ORDER BY d.event_id",
            (legacy_alias_id, event_id),
        ).fetchall()
        assert stored == [
            (legacy_alias_id, claude_id, claude_id, claude_id),
            (event_id, claude_id, claude_id, claude_id),
        ]
        pre_r1_original = conn.execute(
            "SELECT d.event_id FROM delivery d JOIN message_log m "
            "ON m.event_id=d.event_id AND m.to_agent=d.to_agent "
            "WHERE d.to_agent=? AND d.status='outstanding' ORDER BY d.event_id",
            (claude_id,),
        ).fetchall()
        assert pre_r1_original == [(legacy_alias_id,), (event_id,)]
        wrong_owner_ack = conn.execute(
            "UPDATE delivery SET status='acked',acked_ts=2,updated_ts=2 "
            "WHERE to_agent='shared' AND event_id=? AND status!='acked'",
            (event_id,),
        )
        assert wrong_owner_ack.rowcount == 0
        pre_r1_original_ack = conn.execute(
            "UPDATE delivery SET status='acked',acked_ts=2,updated_ts=2 "
            "WHERE to_agent=? AND event_id=? AND status!='acked'",
            (claude_id, legacy_alias_id),
        )
        assert pre_r1_original_ack.rowcount == 1
        conn.commit()
    finally:
        conn.close()
    stable_ack = await client.post(
        f"/api/agent/{claude_id}/delivery-ack",
        json={"event_ids": [event_id]},
    )
    assert stable_ack.status_code == 200, stable_ack.text
    assert stable_ack.json() == {"acked_now": 1, "requested": 1}

    legacy_read = await client.get("/api/agent/SHARED/messages")
    legacy_ack = await client.post(
        "/api/agent/SHARED/ack", json={"event_ids": [event_id]}
    )
    assert legacy_read.status_code == 409
    assert legacy_ack.status_code == 409


async def test_claude_status_derives_claim_and_preserves_it_on_invalid_display(client):
    agent_id = str(uuid4())
    registered = await _register(
        client,
        agent_id,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="Claude.One",
        client_event_id="relay-register-one",
    )
    assert registered.status_code == 200, registered.text
    assert (await _row(agent_id))["relay_name"] == "claude.one"
    assert (await _row(agent_id))["transport"] == "claude-code"

    invalid_refresh = await _register(
        client,
        agent_id,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="invalid name",
        client_event_id="relay-register-two",
    )
    assert invalid_refresh.status_code == 200, invalid_refresh.text
    assert (await _row(agent_id))["relay_name"] == "claude.one"
    assert (await _row(agent_id))["transport"] == "claude-code"


async def test_claude_register_collision_is_nonfatal_but_rename_prefix_takes_over(client):
    first, second = str(uuid4()), str(uuid4())
    assert (
        await _register(
            client,
            first,
            transport=None,
            kind="cc-session",
            channel="#cc-relay",
            display_name="Taken",
            client_event_id="relay-register-first",
        )
    ).status_code == 200

    automatic = await _register(
        client,
        second,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="TAKEN",
        client_event_id="relay-register-second",
    )
    assert automatic.status_code == 200, automatic.text
    assert (await _row(second))["relay_name"] is None
    assert (await _row(second))["transport"] == "claude-code"
    assert (await _row(first))["relay_name"] == "taken"
    replay = await _register(
        client,
        second,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="TAKEN",
        client_event_id="relay-register-second",
    )
    assert replay.status_code == 200, replay.text
    assert replay.json() == automatic.json()
    assert (await _row(second))["relay_name"] is None

    takeover = await _register(
        client,
        second,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="TAKEN",
        client_event_id=f"relay-rename-{second}-123",
    )
    assert takeover.status_code == 200, takeover.text
    assert (await _row(first))["relay_name"] is None
    assert (await _row(first))["status"] == "blocked"
    assert (await _row(second))["relay_name"] == "taken"
    assert (await _row(second))["transport"] == "claude-code"


async def test_header_only_rename_idempotency_key_does_not_authorize_takeover(client):
    first, second = str(uuid4()), str(uuid4())
    assert (
        await _register(
            client,
            first,
            transport=None,
            kind="cc-session",
            channel="#cc-relay",
            display_name="Taken",
            client_event_id="relay-register-header-owner",
        )
    ).status_code == 200

    collision = await client.post(
        f"/api/agent/{second}/status",
        headers={"Idempotency-Key": "relay-rename-header-only"},
        json={
            "kind": "cc-session",
            "channel": "#cc-relay",
            "status": "idle",
            "last_activity_ts": 1,
            "display_name": "TAKEN",
        },
    )

    assert collision.status_code == 200, collision.text
    assert (await _row(first))["relay_name"] == "taken"
    assert (await _row(second))["relay_name"] is None


async def test_foreign_body_rename_marker_does_not_authorize_takeover(client):
    first, second = str(uuid4()), str(uuid4())
    assert (
        await _register(
            client,
            first,
            transport=None,
            kind="cc-session",
            channel="#cc-relay",
            display_name="Taken",
            client_event_id="relay-register-foreign-marker-owner",
        )
    ).status_code == 200

    collision = await _register(
        client,
        second,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="TAKEN",
        client_event_id="relay-rename-different-session-123",
    )

    assert collision.status_code == 200, collision.text
    assert (await _row(first))["relay_name"] == "taken"
    assert (await _row(second))["relay_name"] is None


async def test_automatic_claude_collision_preserves_existing_prior_name(client):
    first, second = str(uuid4()), str(uuid4())
    assert (
        await _register(
            client,
            first,
            transport=None,
            kind="cc-session",
            channel="#cc-relay",
            display_name="Taken",
            client_event_id="relay-register-prior-first",
        )
    ).status_code == 200
    assert (
        await _register(
            client,
            second,
            transport=None,
            kind="cc-session",
            channel="#cc-relay",
            display_name="Second",
            client_event_id="relay-register-prior-second",
        )
    ).status_code == 200

    collision = await _register(
        client,
        second,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="TAKEN",
        client_event_id="relay-register-prior-collision",
    )

    assert collision.status_code == 200, collision.text
    assert (await _row(first))["relay_name"] == "taken"
    second_row = await _row(second)
    assert second_row["relay_name"] == "second"
    assert second_row["display_name"] == "TAKEN"


async def test_new_claude_row_with_missing_or_invalid_display_stays_unnamed(client):
    missing = await _register(
        client,
        "claude-missing",
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
    )
    invalid = await _register(
        client,
        "claude-invalid",
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="not valid",
    )
    assert missing.status_code == 200
    assert invalid.status_code == 200
    assert (await _row("claude-missing"))["relay_name"] is None
    assert (await _row("claude-invalid"))["relay_name"] is None


async def test_codex_delivery_stream_is_outstanding_ordered_and_stable_id_scoped(client):
    agent_id, other_id = str(uuid4()), str(uuid4())
    assert (await _register(client, agent_id)).status_code == 200
    assert (await _claim(client, agent_id, "CodexBox")).status_code == 200
    assert (await _register(client, other_id)).status_code == 200
    assert (await _claim(client, other_id, "otherbox")).status_code == 200

    first = await _send(client, "CODEXBOX", "agent_reply", "stream-1")
    second = await _send(client, "codexbox", "handoff", "stream-2")
    third = await _send(client, "CodexBox", "agent_message", "stream-3")
    await _send(client, "otherbox", "agent_message", "stream-other")
    ack = await client.post(
        f"/api/agent/{agent_id}/delivery-ack", json={"event_ids": [second]}
    )
    assert ack.status_code == 200, ack.text

    first_page = await client.get(
        f"/api/agent/{agent_id}/deliveries?after=0&limit=1"
    )
    assert first_page.status_code == 200, first_page.text
    assert [row["event_id"] for row in first_page.json()["deliveries"]] == [first]
    assert first_page.json()["next_cursor"] == first
    second_page = await client.get(
        f"/api/agent/{agent_id}/deliveries?after={first}&limit=10"
    )
    assert second_page.status_code == 200, second_page.text
    assert [row["event_id"] for row in second_page.json()["deliveries"]] == [third]
    assert second_page.json()["next_cursor"] is None


async def test_stable_delivery_ack_accepts_explicit_ids_only(client):
    agent_id = str(uuid4())
    assert (await _register(client, agent_id)).status_code == 200
    assert (await _claim(client, agent_id, "ackbox")).status_code == 200
    event_id = await _send(client, "ackbox", "agent_message", "explicit-ack")

    floor = await client.post(
        f"/api/agent/{agent_id}/delivery-ack", json={"floor": event_id}
    )
    named = await client.post(
        f"/api/agent/{agent_id}/delivery-ack",
        json={"event_ids": [event_id], "relay_name": "ackbox"},
    )
    empty = await client.post(f"/api/agent/{agent_id}/delivery-ack", json={})
    assert floor.status_code == 422
    assert named.status_code == 422
    assert empty.status_code == 422

    explicit = await client.post(
        f"/api/agent/{agent_id}/delivery-ack", json={"event_ids": [event_id]}
    )
    assert explicit.status_code == 200, explicit.text
    assert explicit.json() == {"acked_now": 1, "requested": 1}
    repeated = await client.post(
        f"/api/agent/{agent_id}/delivery-ack", json={"event_ids": [event_id]}
    )
    assert repeated.json() == {"acked_now": 0, "requested": 1}


async def test_stable_delivery_endpoints_require_registered_identity_not_name(client):
    unknown = str(uuid4())
    missing_get = await client.get(f"/api/agent/{unknown}/deliveries")
    missing_ack = await client.post(
        f"/api/agent/{unknown}/delivery-ack", json={"event_ids": [1]}
    )
    assert missing_get.status_code == 404
    assert missing_ack.status_code == 404

    unnamed = str(uuid4())
    assert (await _register(client, unnamed)).status_code == 200
    current = await _send(client, unnamed, "agent_message", "stable-id-current")
    rollback = await _seed_pre_r1_delivery(unnamed, "stable-id-rollback")

    unnamed_get = await client.get(f"/api/agent/{unnamed}/deliveries")
    assert unnamed_get.status_code == 200
    assert [row["event_id"] for row in unnamed_get.json()["deliveries"]] == [
        current,
        rollback,
    ]

    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    bound = conn.execute(
        "SELECT recipient_id FROM delivery WHERE event_id IN (?,?) ORDER BY event_id",
        (current, rollback),
    ).fetchall()
    conn.close()
    assert bound == [(unnamed,), (unnamed,)]

    claimed = await _claim(client, unnamed, "stable.id-fallback")
    assert claimed.status_code == 200, claimed.text
    renamed_get = await client.get(f"/api/agent/{unnamed}/deliveries")
    assert [row["event_id"] for row in renamed_get.json()["deliveries"]] == [
        current,
        rollback,
    ]
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    moved = conn.execute(
        "SELECT to_agent,recipient_id FROM delivery "
        "WHERE event_id IN (?,?) ORDER BY event_id",
        (current, rollback),
    ).fetchall()
    conn.close()
    assert moved == [
        ("stable.id-fallback", unnamed),
        ("stable.id-fallback", unnamed),
    ]

    unnamed_ack = await client.post(
        f"/api/agent/{unnamed}/delivery-ack",
        json={"event_ids": [current, rollback]},
    )
    assert unnamed_ack.json() == {"acked_now": 2, "requested": 2}
    drained = await client.get(f"/api/agent/{unnamed}/deliveries")
    assert drained.json() == {"deliveries": [], "next_cursor": None}


async def test_legacy_mixed_case_send_read_and_ack_use_canonical_mailbox(client):
    event_id = await _send(client, "Alpha", "agent_message", "mixed-case-round-trip")

    inbox = await client.get("/api/agent/Alpha/messages")

    assert inbox.status_code == 200, inbox.text
    assert [row["event_id"] for row in inbox.json()["messages"]] == [event_id]
    ack = await client.post("/api/agent/Alpha/ack", json={"event_ids": [event_id]})
    assert ack.status_code == 200, ack.text
    assert ack.json() == {"acked_now": 1, "requested": 1}


async def test_legacy_aliases_keep_pre_r1_exact_rows_and_follow_ingress_folding(client):
    pre_r1_mixed = await _seed_pre_r1_delivery("Alpha", "pre-r1-mixed")
    invalid_exact = await _seed_pre_r1_delivery("Legacy Box", "invalid-exact")
    invalid_folded = await _seed_pre_r1_delivery("legacy box", "invalid-folded")
    current = await _send(client, "Alpha", "agent_message", "current-canonical")
    invalid_current = await _send(
        client, "Legacy Box", "agent_message", "invalid-current-folded"
    )

    mixed_inbox = await client.get("/api/agent/Alpha/messages")
    assert mixed_inbox.status_code == 200, mixed_inbox.text
    assert [row["event_id"] for row in mixed_inbox.json()["messages"]] == [
        pre_r1_mixed,
        current,
    ]
    mixed_ack = await client.post(
        "/api/agent/Alpha/ack", json={"event_ids": [pre_r1_mixed, current]}
    )
    assert mixed_ack.status_code == 200, mixed_ack.text
    assert mixed_ack.json() == {"acked_now": 2, "requested": 2}

    invalid_inbox = await client.get("/api/agent/Legacy Box/messages")
    assert invalid_inbox.status_code == 200, invalid_inbox.text
    assert [row["event_id"] for row in invalid_inbox.json()["messages"]] == [
        invalid_exact,
        invalid_folded,
        invalid_current,
    ]
    invalid_ack = await client.post(
        "/api/agent/Legacy Box/ack",
        json={"event_ids": [invalid_exact, invalid_folded, invalid_current]},
    )
    assert invalid_ack.status_code == 200, invalid_ack.text
    assert invalid_ack.json() == {"acked_now": 3, "requested": 3}


async def test_stable_delivery_ownership_does_not_move_during_takeover(
    client, monkeypatch
):
    from broker import server

    original_id, takeover_id = str(uuid4()), str(uuid4())
    assert (await _register(client, original_id)).status_code == 200
    assert (await _claim(client, original_id, "snapshot-box")).status_code == 200
    assert (await _register(client, takeover_id)).status_code == 200
    before_takeover = await _send(
        client, "snapshot-box", "agent_message", "snapshot-before-takeover"
    )

    ownership_checked = asyncio.Event()
    continue_read = asyncio.Event()
    original_begin = server._begin_read_snapshot

    async def pause_after_snapshot(conn):
        await original_begin(conn)
        cur = await conn.execute("SELECT COUNT(*) FROM agent_status")
        await cur.fetchone()
        await cur.close()
        ownership_checked.set()
        await continue_read.wait()

    monkeypatch.setattr(server, "_begin_read_snapshot", pause_after_snapshot)
    read_task = asyncio.create_task(
        client.get(f"/api/agent/{original_id}/deliveries?after=0&limit=20")
    )
    await asyncio.wait_for(ownership_checked.wait(), timeout=2)
    try:
        takeover = await _claim(client, takeover_id, "snapshot-box", takeover=True)
        assert takeover.status_code == 200, takeover.text
        after_takeover = await _send(
            client, "snapshot-box", "agent_message", "snapshot-after-takeover"
        )
    finally:
        continue_read.set()

    response = await asyncio.wait_for(read_task, timeout=2)
    assert response.status_code == 200, response.text
    assert [row["event_id"] for row in response.json()["deliveries"]] == [
        before_takeover,
    ]
    new_owner = await client.get(f"/api/agent/{takeover_id}/deliveries")
    assert [row["event_id"] for row in new_owner.json()["deliveries"]] == [
        after_takeover,
    ]


async def test_legacy_message_read_keeps_ownership_snapshot_during_takeover(
    client, monkeypatch
):
    from broker import server

    claude_id, codex_id = str(uuid4()), str(uuid4())
    registered = await _register(
        client,
        claude_id,
        transport=None,
        kind="cc-session",
        channel="#cc-relay",
        display_name="legacy-snapshot",
        client_event_id="relay-register-legacy-snapshot",
    )
    assert registered.status_code == 200, registered.text
    assert (await _register(client, codex_id)).status_code == 200
    before_takeover = await _send(
        client, "legacy-snapshot", "agent_message", "legacy-before-takeover"
    )

    ownership_checked = asyncio.Event()
    continue_read = asyncio.Event()
    original_owner = server._legacy_mailbox_owner_id

    async def pause_after_ownership_check(conn, mailbox):
        owner_id = await original_owner(conn, mailbox)
        if mailbox == "legacy-snapshot":
            ownership_checked.set()
            await continue_read.wait()
        return owner_id

    monkeypatch.setattr(
        server, "_legacy_mailbox_owner_id", pause_after_ownership_check
    )
    read_task = asyncio.create_task(
        client.get(
            "/api/agent/legacy-snapshot/messages"
            f"?since={before_takeover}&kind=agent_message&limit=20"
        )
    )
    await asyncio.wait_for(ownership_checked.wait(), timeout=2)
    try:
        takeover = await _claim(client, codex_id, "legacy-snapshot", takeover=True)
        assert takeover.status_code == 200, takeover.text
        await _send(
            client, "legacy-snapshot", "agent_message", "legacy-after-takeover"
        )
    finally:
        continue_read.set()

    response = await asyncio.wait_for(read_task, timeout=2)
    assert response.status_code == 200, response.text
    assert response.json()["messages"] == []
    assert (await client.get("/api/agent/legacy-snapshot/messages")).status_code == 409


async def test_existing_database_migration_selects_deterministic_claude_winners(
    tmp_path, monkeypatch
):
    from broker import server

    warnings = []
    monkeypatch.setattr(
        server.logger,
        "warning",
        lambda message, *args: warnings.append(message % args),
    )

    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE agent_status (
          id TEXT PRIMARY KEY,
          kind TEXT NOT NULL,
          channel TEXT NOT NULL,
          thread_id TEXT,
          status TEXT NOT NULL,
          current_task TEXT,
          capabilities TEXT,
          last_activity_ts INTEGER NOT NULL,
          registered_at INTEGER NOT NULL,
          display_name TEXT
        );
        """
    )
    rows = [
        ("alpha-old", "cc-session", "#cc-relay", "idle", 10, 10, "Alpha"),
        ("alpha-new", "cc-session", "#cc-relay", "idle", 20, 5, "ALPHA"),
        ("beta-a", "cc-session", "#cc-relay", "idle", 30, 30, "Beta"),
        ("beta-z", "cc-session", "#cc-relay", "idle", 30, 30, "BETA"),
        ("invalid", "cc-session", "#cc-relay", "idle", 40, 40, "bad name"),
        ("wrong-kind", "worker", "#cc-relay", "idle", 50, 50, "Worker"),
        ("wrong-channel", "cc-session", "#other", "idle", 60, 60, "Other"),
    ]
    conn.executemany(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at,display_name) "
        "VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("BROKER_DB_PATH", str(db))

    await server.ensure_schema()
    await server.ensure_schema()

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    migrated = {
        row["id"]: (row["relay_name"], row["transport"])
        for row in conn.execute(
            "SELECT id, relay_name, transport FROM agent_status ORDER BY id"
        )
    }
    index_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' "
        "AND name='idx_agent_status_relay_name'"
    ).fetchone()[0]
    conn.close()

    assert migrated["alpha-new"] == ("alpha", "claude-code")
    assert migrated["beta-a"] == ("beta", "claude-code")
    for loser in (
        "alpha-old",
        "beta-z",
        "invalid",
        "wrong-kind",
        "wrong-channel",
    ):
        assert migrated[loser] == (None, None)
    assert "WHERE relay_name IS NOT NULL" in index_sql
    assert "alpha-old" in "\n".join(warnings)
    assert "beta-z" in "\n".join(warnings)


async def test_fresh_schema_has_nullable_session_columns_and_partial_unique_index(db_path):
    conn = sqlite3.connect(db_path)
    columns = {
        row[1]: row[3]
        for row in conn.execute("PRAGMA table_info(agent_status)").fetchall()
    }
    indexes = {
        row[1]: row[2]
        for row in conn.execute("PRAGMA index_list(agent_status)").fetchall()
    }
    conn.close()

    assert columns["relay_name"] == 0
    assert columns["transport"] == 0
    assert indexes["idx_agent_status_relay_name"] == 1
