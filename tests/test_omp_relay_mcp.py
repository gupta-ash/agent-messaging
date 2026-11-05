"""R2 OMP stdio MCP adapter wire, authorization, and broker integration tests."""

from __future__ import annotations

import asyncio
import base64
import errno
import hashlib
import json
import os
import time
import subprocess
from pathlib import Path
from urllib.parse import unquote
from uuid import uuid4

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from mcp.shared.memory import create_connected_server_and_client_session

import omp_relay_mcp as relay


pytestmark = pytest.mark.asyncio


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _identity(runtime: relay.OmpRelayRuntime) -> tuple[dict, Ed25519PrivateKey]:
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    identity = {
        "schema_version": 1,
        "ok": True,
        "instance_id": runtime.instance_id,
        "control_epoch": runtime.control_epoch,
        "mcp_process": runtime.process_witness,
        "bind_attempt_id": runtime.bind_attempt_id,
        "binding_epoch": runtime.new_binding_epoch,
        "omp_process_epoch": str(uuid4()),
        "omp_pid": 771,
        "omp_process_start_ms": 1_780_001_000_000,
        "session_id": str(uuid4()),
        "mode": "tui",
        "agent_kind": "main",
        "session_name": "r2-disposable",
        "control_public_key_b64url": _b64(public),
        "written_at_ms": int(time.time() * 1000),
    }
    return identity, private


def _runtime(tmp_path: Path, client: httpx.AsyncClient | None = None) -> relay.OmpRelayRuntime:
    tmp_path.mkdir(parents=True, exist_ok=True)
    state_root = tmp_path / "state"
    runtime_root = tmp_path / "runtime"
    state_root.mkdir()
    runtime_root.mkdir()
    runtime = relay.OmpRelayRuntime(
        broker_url="http://test",
        state_root=state_root,
        runtime_root=runtime_root,
        process_witness={"pid": 901, "start_key": "9001", "sequence_number": "7001", "boot_id": 52},
        parent_pid=771,
        validate_interpreter=False,
        http_client=client,
    )
    (runtime.rendezvous_dir / "control" / "requests").mkdir(parents=True)
    (runtime.rendezvous_dir / "control" / "results").mkdir()
    return runtime


def _authorization(
    runtime: relay.OmpRelayRuntime,
    identity: dict,
    private: Ed25519PrivateKey,
    *, issued_at_ms: int | None = None,
) -> dict:
    unsigned = {
        "schema_version": 1,
        "command_id": runtime.bind_attempt_id,
        "instance_id": runtime.instance_id,
        "control_epoch": runtime.control_epoch,
        "binding_epoch": runtime.new_binding_epoch,
        "omp_process_epoch": identity["omp_process_epoch"],
        "session_id": identity["session_id"],
        "issued_at_ms": issued_at_ms if issued_at_ms is not None else int(time.time() * 1000),
        "action": "AUTHORIZE_BIND",
        "args": {
            "bind_attempt_id": runtime.bind_attempt_id,
            "expected_binding_epoch": runtime.expected_binding_epoch,
            "gate_generation": str(uuid4()),
            "claim_generation": str(uuid4()),
            "mcp_process": runtime.process_witness,
            "control_token_hash": runtime.control_token_hash,
        },
    }
    return {**unsigned, "signature_b64url": _b64(private.sign(relay._canonical_json(unsigned)))}


async def _bind(runtime: relay.OmpRelayRuntime, notifications: list[tuple[str, dict]]) -> tuple[dict, Ed25519PrivateKey]:
    identity, private = _identity(runtime)
    runtime.identity = identity

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime.notifier = notify
    await runtime._bind(_authorization(runtime, identity, private))
    return identity, private


def _native_directory_symlink(target: Path, link: Path) -> None:
    try:
        os.symlink(target, link, target_is_directory=True)
    except NotImplementedError as exc:
        pytest.skip(f"physical directory symlink unavailable: {exc}")
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314 or exc.errno in (
            errno.ENOSYS,
            errno.EOPNOTSUPP,
            errno.ENOTSUP,
        ):
            pytest.skip(f"physical directory symlink unavailable: {exc}")
        raise


async def test_closed_wire_types_reject_unknowns_and_preserve_large_event_ids(tmp_path):
    runtime = _runtime(tmp_path)
    delivery = relay._validate_delivery(
        {
            "event_id": "9007199254740993",
            "client_event_id": None,
            "from_agent": "sender",
            "to_agent": "receiver",
            "channel": "#agents",
            "thread_id": None,
            "kind": "agent_message",
            "content": "payload",
            "ts": 1_780_001_000_000,
        }
    )
    assert delivery["event_id"] == "9007199254740993"
    assert relay.render_display(delivery) == (
        "[relay] from=sender to=receiver event_id=9007199254740993 kind=agent_message\n\npayload"
    )
    with pytest.raises(relay.RelayProtocolError, match="PROTOCOL_INVALID"):
        relay._closed({"schema_version": 1, "extra": True}, {"schema_version"})
    with pytest.raises(relay.RelayProtocolError, match="PROTOCOL_INVALID"):
        relay._event_id(9_007_199_254_740_993)
    await runtime.stop()


async def test_identity_and_signed_authorization_reject_wrong_pair_attempt_and_expiry(tmp_path):
    runtime = _runtime(tmp_path)
    identity, private = _identity(runtime)
    identity_path = runtime.rendezvous_dir / "identity.json"
    identity_path.write_text(json.dumps(identity), encoding="utf-8")
    assert runtime._read_identity(identity_path)["bind_attempt_id"] == runtime.bind_attempt_id

    for field, value in [
        ("instance_id", str(uuid4())),
        ("control_epoch", str(uuid4())),
        ("bind_attempt_id", str(uuid4())),
        ("mcp_process", {**runtime.process_witness, "sequence_number": "7002"}),
    ]:
        identity_path.write_text(json.dumps({**identity, field: value}), encoding="utf-8")
        with pytest.raises(relay.RelayProtocolError, match="IDENTITY_INVALID"):
            runtime._read_identity(identity_path)

    authorization_path = runtime.rendezvous_dir / "control" / "requests" / f"{runtime.bind_attempt_id}.json"
    authorization_path.write_text(json.dumps(_authorization(runtime, identity, private)), encoding="utf-8")
    parsed = runtime._read_bind_authorization(authorization_path, identity)
    assert parsed["args"]["mcp_process"] == runtime.process_witness
    stale = _authorization(
        runtime,
        identity,
        private,
        issued_at_ms=int(time.time() * 1000) - relay.APPROVAL_MAX_AGE_MS - 1,
    )
    authorization_path.write_text(json.dumps(stale), encoding="utf-8")
    with pytest.raises(relay.RelayProtocolError, match="CONTROL_CHANNEL_INVALID"):
        runtime._read_bind_authorization(authorization_path, identity)
    await runtime.stop()


@pytest.mark.skipif(os.name != "nt", reason="Windows canonical-case contract")
async def test_case_alias_roots_are_accepted_and_canonicalized(tmp_path):
    state_root = tmp_path / "CaseState"
    runtime_root = tmp_path / "CaseRuntime"
    state_root.mkdir()
    runtime_root.mkdir()

    def alias(value: Path) -> Path:
        return Path("".join(character.swapcase() if character.isalpha() else character for character in str(value)))

    runtime = relay.OmpRelayRuntime(
        broker_url="http://test",
        state_root=alias(state_root),
        runtime_root=alias(runtime_root),
        process_witness={"pid": 901, "start_key": "9001", "sequence_number": "7001", "boot_id": 52},
        parent_pid=771,
        validate_interpreter=False,
    )
    assert str(runtime.state_root) == str(state_root.resolve(strict=True))
    assert str(runtime.runtime_root) == str(runtime_root.resolve(strict=True))
    await runtime.stop()


@pytest.mark.skipif(os.name != "nt", reason="native Windows reparse contract")
async def test_native_case_sensitive_and_reparse_roots_fail_closed(tmp_path):
    sensitive = tmp_path / "sensitive"
    sensitive.mkdir()
    enabled = subprocess.run(
        ["fsutil.exe", "file", "SetCaseSensitiveInfo", str(sensitive), "enable"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert enabled.returncode == 0, f"native case-sensitive directory unavailable: {enabled.stderr or enabled.stdout}"
    upper = sensitive / "Root"
    lower = sensitive / "root"
    upper.mkdir()
    lower.mkdir()
    upper_resolved = str(upper.resolve(strict=True))
    lower_resolved = str(lower.resolve(strict=True))
    assert str(relay._canonical_root(str(upper), "STATE_ROOT")) == upper_resolved
    assert str(relay._canonical_root(str(lower), "STATE_ROOT")) == lower_resolved
    assert upper_resolved != lower_resolved
    with pytest.raises(relay.RelayProtocolError, match="STATE_ROOT_INVALID"):
        relay._canonical_root(str(sensitive / "ROOT"), "STATE_ROOT")

    target = tmp_path / "target"
    target.mkdir()
    junction = tmp_path / "junction"
    linked = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(target)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert linked.returncode == 0, f"native junction unavailable: {linked.stderr or linked.stdout}"

    real = tmp_path / "real"
    real.mkdir()
    nested = target / "nested"
    nested.mkdir()
    hidden = junction / "nested" / ".." / ".." / "real"
    for label in ("STATE_ROOT", "RUNTIME_ROOT"):
        for candidate in (junction, hidden):
            with pytest.raises(relay.RelayProtocolError, match=f"{label}_INVALID"):
                relay._canonical_root(str(candidate), label)


@pytest.mark.skipif(os.name != "nt", reason="native Windows directory symlink contract")
@pytest.mark.parametrize("label", ["STATE_ROOT", "RUNTIME_ROOT"])
async def test_native_directory_symlink_roots_fail_closed(tmp_path, label):
    target = tmp_path / "target"
    target.mkdir()
    symlink = tmp_path / "symlink"
    _native_directory_symlink(target, symlink)
    with pytest.raises(relay.RelayProtocolError, match=f"{label}_INVALID"):
        relay._canonical_root(str(symlink), label)


async def test_signed_control_attempt_is_immutable(tmp_path):
    runtime = _runtime(tmp_path)
    identity, private = _identity(runtime)
    runtime.identity = identity
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]
    runtime.session_id = identity["session_id"]
    command_id = str(uuid4())

    def command(name: str) -> dict:
        unsigned = {
            "schema_version": 1,
            "command_id": command_id,
            "instance_id": runtime.instance_id,
            "control_epoch": runtime.control_epoch,
            "binding_epoch": runtime.binding_epoch,
            "omp_process_epoch": runtime.omp_process_epoch,
            "session_id": runtime.session_id,
            "issued_at_ms": int(time.time() * 1000),
            "action": "TAKEOVER_NAME",
            "args": {"name": name},
        }
        return {**unsigned, "signature_b64url": _b64(private.sign(relay._canonical_json(unsigned)))}

    request_path = runtime.rendezvous_dir / "control" / "requests" / f"{command_id}.json"
    request_path.write_text(json.dumps(command("first.name")), encoding="utf-8")
    runtime._read_user_control(request_path)
    request_path.write_text(json.dumps(command("second.name")), encoding="utf-8")
    with pytest.raises(relay.RelayProtocolError, match="ATTEMPT_CONFLICT"):
        runtime._read_user_control(request_path)
    await runtime.stop()


async def test_native_name_takes_over_previous_session(client, tmp_path):
    first = _runtime(tmp_path / "first", client)
    second = _runtime(tmp_path / "second", client)
    try:
        first_identity, _ = await _bind(first, [])
        second_identity, _ = await _bind(second, [])
        first_id = first_identity["session_id"]
        second_id = second_identity["session_id"]
        assert first_id != second_id
        first_server = relay.create_server(first)
        second_server = relay.create_server(second)
        relay_name = "omp.takeover.test"

        content, receipt = await first_server.call_tool("name", {"relay_name": relay_name})
        assert receipt == {
            "schema_version": 1, "id": first_id, "relay_name": relay_name, "displaced_id": None,
        }
        await asyncio.wait_for(asyncio.gather(*tuple(first._tasks)), timeout=10)
        before = await client.post(
            "/api/event",
            json={
                "from_agent": "sender.takeover",
                "to_agent": relay_name,
                "channel": "#agents",
                "kind": "agent_message",
                "content": "queued before takeover",
                "ts": int(time.time() * 1000),
                "client_event_id": f"takeover-before-{uuid4()}",
            },
        )
        assert before.status_code == 200, before.text
        before_event = before.json()["event_id"]

        content, receipt = await second_server.call_tool("name", {"relay_name": relay_name})
        assert receipt == {
            "schema_version": 1, "id": second_id, "relay_name": relay_name, "displaced_id": first_id,
        }
        await asyncio.wait_for(asyncio.gather(*tuple(second._tasks)), timeout=10)
        first_status = await client.get(f"/api/agent/{first_id}/status")
        second_status = await client.get(f"/api/agent/{second_id}/status")
        assert first_status.status_code == 200, first_status.text
        assert second_status.status_code == 200, second_status.text
        assert first_status.json()["relay_name"] is None
        assert first_status.json()["status"] == "blocked"
        assert second_status.json()["relay_name"] == relay_name

        content, receipt = await second_server.call_tool("name", {"relay_name": relay_name})
        assert receipt == {
            "schema_version": 1, "id": second_id, "relay_name": relay_name, "displaced_id": None,
        }
        await asyncio.wait_for(asyncio.gather(*tuple(second._tasks)), timeout=10)
        after = await client.post(
            "/api/event",
            json={
                "from_agent": "sender.takeover",
                "to_agent": relay_name,
                "channel": "#agents",
                "kind": "agent_message",
                "content": "queued after takeover",
                "ts": int(time.time() * 1000),
                "client_event_id": f"takeover-after-{uuid4()}",
            },
        )
        assert after.status_code == 200, after.text
        after_event = after.json()["event_id"]
        assert before_event != after_event

        # Outstanding queues prove both stable recipient ownership and the absence of an ACK.
        for session_id, expected in ((first_id, [before_event]), (second_id, [after_event])):
            deliveries = await client.get(f"/api/agent/{session_id}/deliveries")
            assert deliveries.status_code == 200, deliveries.text
            assert [row["event_id"] for row in deliveries.json()["deliveries"]] == expected

        await first.tool_register("idle")
        first_status = await client.get(f"/api/agent/{first_id}/status")
        second_status = await client.get(f"/api/agent/{second_id}/status")
        assert first_status.status_code == 200, first_status.text
        assert second_status.status_code == 200, second_status.text
        assert first_status.json()["relay_name"] is None
        assert first_status.json()["status"] == "idle"
        assert second_status.json()["relay_name"] == relay_name
    finally:
        await asyncio.gather(first.stop(), second.stop())


async def test_native_name_takes_over_retained_namespace(client, tmp_path):
    first = _runtime(tmp_path / "first", client)
    second = _runtime(tmp_path / "second", client)
    try:
        first_identity, _ = await _bind(first, [])
        second_identity, _ = await _bind(second, [])
        first_id = first_identity["session_id"]
        second_id = second_identity["session_id"]
        assert first_id != second_id
        first_server = relay.create_server(first)
        second_server = relay.create_server(second)
        retained_name = "omp.takeover.retained"
        moved_name = "omp.takeover.moved"

        content, receipt = await first_server.call_tool("name", {"relay_name": retained_name})
        assert receipt == {
            "schema_version": 1, "id": first_id, "relay_name": retained_name, "displaced_id": None,
        }
        await asyncio.wait_for(asyncio.gather(*tuple(first._tasks)), timeout=10)
        before = await client.post(
            "/api/event",
            json={
                "from_agent": "sender.takeover",
                "to_agent": retained_name,
                "channel": "#agents",
                "kind": "agent_message",
                "content": "queued before rename",
                "ts": int(time.time() * 1000),
                "client_event_id": f"retained-before-{uuid4()}",
            },
        )
        assert before.status_code == 200, before.text
        before_event = before.json()["event_id"]

        content, receipt = await first_server.call_tool("name", {"relay_name": moved_name})
        assert receipt == {
            "schema_version": 1, "id": first_id, "relay_name": moved_name, "displaced_id": None,
        }
        await asyncio.wait_for(asyncio.gather(*tuple(first._tasks)), timeout=10)
        content, receipt = await second_server.call_tool("name", {"relay_name": retained_name})
        assert receipt == {
            "schema_version": 1, "id": second_id, "relay_name": retained_name, "displaced_id": None,
        }
        await asyncio.wait_for(asyncio.gather(*tuple(second._tasks)), timeout=10)
        first_status = await client.get(f"/api/agent/{first_id}/status")
        second_status = await client.get(f"/api/agent/{second_id}/status")
        assert first_status.status_code == 200, first_status.text
        assert second_status.status_code == 200, second_status.text
        assert first_status.json()["relay_name"] == moved_name
        assert second_status.json()["relay_name"] == retained_name

        after = await client.post(
            "/api/event",
            json={
                "from_agent": "sender.takeover",
                "to_agent": retained_name,
                "channel": "#agents",
                "kind": "agent_message",
                "content": "queued after retained-name takeover",
                "ts": int(time.time() * 1000),
                "client_event_id": f"retained-after-{uuid4()}",
            },
        )
        assert after.status_code == 200, after.text
        after_event = after.json()["event_id"]
        assert before_event != after_event
        for session_id, expected in ((first_id, [before_event]), (second_id, [after_event])):
            deliveries = await client.get(f"/api/agent/{session_id}/deliveries")
            assert deliveries.status_code == 200, deliveries.text
            assert [row["event_id"] for row in deliveries.json()["deliveries"]] == expected
    finally:
        await asyncio.gather(first.stop(), second.stop())


async def test_one_notification_requires_explicit_receipt_before_broker_ack(client, tmp_path):
    notifications: list[tuple[str, dict]] = []
    runtime = _runtime(tmp_path, client)
    identity, _ = await _bind(runtime, notifications)
    assert (await runtime.tool_name("omp.r2.test"))["relay_name"] == "omp.r2.test"

    sent = await client.post(
        "/api/event",
        json={
            "from_agent": "sender.explicit",
            "to_agent": "omp.r2.test",
            "channel": "#agents",
            "kind": "agent_message",
            "content": "one payload",
            "ts": int(time.time() * 1000),
            "client_event_id": f"r2-{uuid4()}",
        },
    )
    assert sent.status_code == 200, sent.text
    event_id = str(sent.json()["event_id"])
    await asyncio.gather(runtime.reconcile_once(), runtime.reconcile_once())
    messages = [params for method, params in notifications if method == relay.MESSAGE_METHOD]
    assert len(messages) == 1
    assert messages[0]["delivery"]["event_id"] == event_id
    assert messages[0]["display_text"] == (
        f"[relay] from=sender.explicit to=omp.r2.test event_id={event_id} kind=agent_message\n\none payload"
    )

    await runtime.reconcile_once()
    assert len([method for method, _ in notifications if method == relay.MESSAGE_METHOD]) == 1
    runtime._next_delivery_notification = 0.0
    await runtime.reconcile_once()
    assert len([method for method, _ in notifications if method == relay.MESSAGE_METHOD]) == 2
    still_outstanding = await client.get(f"/api/agent/{identity['session_id']}/deliveries")
    assert [row["event_id"] for row in still_outstanding.json()["deliveries"]] == [int(event_id)]

    offer_token = messages[0]["offer_token"]
    mirror = {
        "schema_version": 1,
        "session_id": identity["session_id"],
        "recipient_id": identity["session_id"],
        "event_id": event_id,
        "offer_token": offer_token,
        "omp_process_epoch": runtime.omp_process_epoch,
        "state": "accepted",
        "broker_terminal_state": "none",
        "identity_acknowledged": False,
        "health_reason": "NONE",
        "reject_reason_code": None,
        "application_acceptance_observed": True,
        "updated_at_ms": int(time.time() * 1000),
    }
    relay._atomic_write_json(runtime._offer_path(event_id), mirror)
    await runtime.reconcile_once()
    assert [params["type"] for method, params in notifications if method == relay.CONTROL_METHOD][-1] == "offer_accepted"

    mirror["broker_terminal_state"] = "accepted"
    relay._atomic_write_json(runtime._offer_path(event_id), mirror)
    await runtime.reconcile_once()
    assert [params["type"] for method, params in notifications if method == relay.CONTROL_METHOD][-1] == "identity_acked"
    mirror["identity_acknowledged"] = True
    relay._atomic_write_json(runtime._offer_path(event_id), mirror)
    await runtime.reconcile_once()
    assert runtime.held is None
    drained = await client.get(f"/api/agent/{identity['session_id']}/deliveries")
    assert drained.json()["deliveries"] == []
    await runtime.stop()


async def test_missing_mirror_and_empty_inbox_never_prove_acceptance(client, tmp_path):
    notifications: list[tuple[str, dict]] = []
    runtime = _runtime(tmp_path, client)
    identity, _ = await _bind(runtime, notifications)
    await runtime.tool_name("omp.no.inference")
    sent = await client.post(
        "/api/event",
        json={
            "from_agent": "sender",
            "to_agent": "omp.no.inference",
            "channel": "#agents",
            "kind": "agent_message",
            "content": "must remain",
            "ts": int(time.time() * 1000),
        },
    )
    event_id = str(sent.json()["event_id"])
    await runtime.reconcile_once()
    await runtime.reconcile_once()
    assert runtime.held is not None
    assert not runtime._offer_path(event_id).exists()
    rows = (await client.get(f"/api/agent/{identity['session_id']}/deliveries")).json()["deliveries"]
    assert [str(row["event_id"]) for row in rows] == [event_id]
    await runtime.stop()


async def test_restart_bind_conflict_refreshes_from_public_current_epoch(client, tmp_path):
    first = _runtime(tmp_path / "first", client)
    first_notifications: list[tuple[str, dict]] = []
    first_identity, _ = await _bind(first, first_notifications)
    replacement = _runtime(tmp_path / "replacement", client)

    async def replacement_notify(_method: str, _params: dict) -> None:
        return None

    replacement.notifier = replacement_notify
    replacement_identity, replacement_key = _identity(replacement)
    replacement_identity["session_id"] = first_identity["session_id"]
    replacement.identity = replacement_identity
    with pytest.raises(relay.RelayProtocolError, match="BINDING_CONFLICT") as conflict:
        await replacement._bind(_authorization(replacement, replacement_identity, replacement_key))
    current_epoch = conflict.value.details["current_binding_epoch"]
    assert current_epoch == first.binding_epoch

    replacement._prepare_bind_attempt(current_epoch)
    replacement_identity, replacement_key = _identity(replacement)
    replacement_identity["session_id"] = first_identity["session_id"]
    replacement.identity = replacement_identity
    await replacement._bind(_authorization(replacement, replacement_identity, replacement_key))
    assert replacement.binding_epoch == replacement.new_binding_epoch
    assert replacement.expected_binding_epoch == current_epoch
    await first.stop()
    await replacement.stop()


async def test_transient_start_failure_is_retryable_and_cleans_candidate(tmp_path):
    state_root = tmp_path / "state"
    runtime_root = tmp_path / "runtime"
    state_root.mkdir()
    runtime_root.mkdir()
    attempts = 0

    def health(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("offline", request=request)
        return httpx.Response(200, json={"checks": {"omp_relay_contract": 1}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(health), base_url="http://test")
    runtime = relay.OmpRelayRuntime(
        broker_url="http://test",
        state_root=state_root,
        runtime_root=runtime_root,
        process_witness={"pid": 901, "start_key": "9001", "sequence_number": "7001", "boot_id": 52},
        parent_pid=771,
        validate_interpreter=False,
        http_client=client,
    )
    notifications: list[tuple[str, dict]] = []

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime.notifier = notify
    await runtime.start()
    assert runtime._started
    deadline = time.monotonic() + 2.0
    while len(notifications) < 2 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert attempts >= 2
    assert [params["type"] for _, params in notifications[:2]] == ["identity_request", "rebind_reconcile"]
    await runtime.stop()
    await client.aclose()


async def test_unnamed_stale_control_failure_preserves_requested_action(client, tmp_path):
    runtime = _runtime(tmp_path, client)
    notifications: list[tuple[str, dict]] = []
    identity, private = await _bind(runtime, notifications)
    assert runtime.relay_name is None
    command_id = str(uuid4())
    unsigned = {
        "schema_version": 1,
        "command_id": command_id,
        "instance_id": runtime.instance_id,
        "control_epoch": runtime.control_epoch,
        "binding_epoch": str(uuid4()),
        "omp_process_epoch": runtime.omp_process_epoch,
        "session_id": identity["session_id"],
        "issued_at_ms": int(time.time() * 1000),
        "action": "TAKEOVER_NAME",
        "args": {"name": "occupied.name"},
    }
    request = {**unsigned, "signature_b64url": _b64(private.sign(relay._canonical_json(unsigned)))}
    request_path = runtime.rendezvous_dir / "control" / "requests" / f"{command_id}.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")

    await runtime._process_user_controls()
    result = json.loads((runtime.rendezvous_dir / "control" / "results" / f"{command_id}.json").read_text())
    assert result["action"] == "TAKEOVER_NAME"
    assert result["code"] == "CONTROL_CHANNEL_INVALID"
    normalized_id = str(uuid4())
    await runtime._write_user_result(normalized_id, "TAKEOVER_NAME", False, "PERSISTENCE_STALLED", None)
    normalized = json.loads(
        (runtime.rendezvous_dir / "control" / "results" / f"{normalized_id}.json").read_text()
    )
    assert normalized["code"] == "CONTROL_CHANNEL_INVALID"
    await runtime.stop()


async def test_binding_driver_conflict_refresh_and_session_cutover(client, tmp_path):
    runtime = _runtime(tmp_path, client)
    target_session = str(uuid4())
    expectations: list[str | None] = []

    def publish_authorization() -> None:
        identity, private = _identity(runtime)
        identity["session_id"] = target_session
        runtime.rendezvous_dir.joinpath("identity.json").write_text(json.dumps(identity), encoding="utf-8")
        request_path = runtime.rendezvous_dir / "control" / "requests" / f"{runtime.bind_attempt_id}.json"
        request_path.write_text(json.dumps(_authorization(runtime, identity, private)), encoding="utf-8")

    async def notify(_method: str, params: dict) -> None:
        if params.get("type") == "identity_request":
            expectations.append(params["expected_binding_epoch"])
            publish_authorization()
        elif params.get("type") == "identity_bound":
            relay._atomic_write_json(
                runtime.rendezvous_dir / "health.json",
                {
                    "schema_version": 1,
                    "instance_id": runtime.instance_id,
                    "binding_epoch": params["binding_epoch"],
                    "state": "connected",
                    "code": "CONNECTED",
                    "since_ms": int(time.time() * 1000),
                    "retry_in_ms": None,
                },
            )

    runtime.notifier = notify
    publish_authorization()
    driver = asyncio.create_task(runtime._complete_identity_handshake())

    async def wait_for(predicate) -> None:
        deadline = time.monotonic() + 3.0
        while not predicate() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert predicate()

    await wait_for(lambda: runtime.bound)
    initial_epoch = runtime.binding_epoch
    initial_session = runtime.session_id
    relay._atomic_write_json(
        runtime.rendezvous_dir / "health.json",
        {
            "schema_version": 1,
            "instance_id": runtime.instance_id,
            "binding_epoch": None,
            "state": "rebinding",
            "code": "REBINDING",
            "since_ms": int(time.time() * 1000),
            "retry_in_ms": None,
        },
    )
    await wait_for(lambda: runtime.bound and runtime.binding_epoch != initial_epoch)
    assert runtime.session_id == initial_session
    assert expectations[:2] == [None, initial_epoch]

    same_session_epoch = runtime.binding_epoch
    target_session = str(uuid4())
    relay._atomic_write_json(
        runtime.rendezvous_dir / "health.json",
        {
            "schema_version": 1,
            "instance_id": runtime.instance_id,
            "binding_epoch": None,
            "state": "rebinding",
            "code": "REBINDING",
            "since_ms": int(time.time() * 1000),
            "retry_in_ms": None,
        },
    )
    await wait_for(lambda: runtime.bound and runtime.session_id == target_session)
    assert runtime.binding_epoch != same_session_epoch
    assert expectations[-1] is None
    assert not driver.done()
    driver.cancel()
    await asyncio.gather(driver, return_exceptions=True)
    await runtime.stop()


async def test_bind_backoff_reuses_exact_body(tmp_path):
    runtime = _runtime(tmp_path)
    identity, private = _identity(runtime)
    runtime.identity = identity
    authorization = _authorization(runtime, identity, private)
    bodies: list[bytes] = []

    async def flaky_bind(_authorization: dict, body: dict | None = None) -> None:
        assert body is not None
        bodies.append(relay._canonical_json(body))
        if len(bodies) == 1:
            raise relay.RelayProtocolError("BROKER_UNREACHABLE")

    runtime._bind = flaky_bind  # type: ignore[method-assign]
    await runtime._bind_with_backoff(authorization)
    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    await runtime.stop()


async def test_non_json_server_error_is_transient_broker_failure(tmp_path):
    async def unavailable(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="temporarily unavailable")

    client = httpx.AsyncClient(transport=httpx.MockTransport(unavailable), base_url="http://test")
    runtime = _runtime(tmp_path, client)
    with pytest.raises(relay.RelayProtocolError, match="BROKER_UNREACHABLE"):
        await runtime._request("GET", "/api/health")
    assert runtime.health_code == "REBINDING"
    await runtime.stop()
    await client.aclose()


async def test_held_recovery_polls_local_mirror_between_private_status_ticks(tmp_path):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]
    runtime.session_id = identity["session_id"]
    runtime.relay_name = "omp.recovery.tick"
    event_id = "9007199254740993"
    offer_token = str(uuid4())
    status = {
        "recipient_id": runtime.session_id,
        "event_id": event_id,
        "offer_token": offer_token,
        "offer_generation": 1,
        "binding_epoch": runtime.binding_epoch,
        "omp_process_epoch": runtime.omp_process_epoch,
        "offer_state": "offered",
        "delivery_status": "outstanding",
        "reject_reason_code": None,
    }
    delivery = {
        "event_id": event_id,
        "client_event_id": None,
        "from_agent": "recovery.peer",
        "to_agent": runtime.relay_name,
        "channel": "#agents",
        "thread_id": None,
        "kind": "agent_message",
        "content": "recover on the safety tick",
        "ts": int(time.time() * 1000),
    }
    runtime._hold_event(status, delivery)
    relay._atomic_write_json(
        runtime._offer_path(event_id),
        {
            "schema_version": 1,
            "session_id": runtime.session_id,
            "recipient_id": runtime.session_id,
            "event_id": event_id,
            "offer_token": offer_token,
            "omp_process_epoch": str(uuid4()),
            "state": "received",
            "broker_terminal_state": "none",
            "identity_acknowledged": False,
            "health_reason": "NONE",
            "reject_reason_code": None,
            "application_acceptance_observed": False,
            "updated_at_ms": int(time.time() * 1000),
        },
    )
    status_reads = 0
    notifications: list[tuple[str, dict]] = []

    async def read_status(_event_id: str) -> dict:
        nonlocal status_reads
        status_reads += 1
        return status

    async def claim_head() -> dict:
        return {"schema_version": 1, "code": "HEAD", "status": status, "delivery": delivery}

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime._status = read_status  # type: ignore[method-assign]
    runtime._claim_head = claim_head  # type: ignore[method-assign]
    runtime.notifier = notify
    runtime._held_status_due = 0.0
    await runtime.reconcile_once()
    await runtime.reconcile_once()

    recovery = [
        params
        for method, params in notifications
        if method == relay.CONTROL_METHOD and params.get("type") == "offer_recovery_status"
    ]
    assert status_reads == 1
    assert len(recovery) == 1
    assert recovery[0]["delivery"] == delivery

    current = json.loads(runtime._offer_path(event_id).read_text())
    current.update(omp_process_epoch=runtime.omp_process_epoch, state="injected", updated_at_ms=current["updated_at_ms"] + 1)
    relay._atomic_write_json(runtime._offer_path(event_id), current)
    cas_calls = 0

    async def cas_without_status(
        _event_id: str, _offer_token: str, _target: str, _reason: str | None = None
    ) -> dict:
        nonlocal cas_calls
        cas_calls += 1
        return {**status, "offer_state": "injected"}

    runtime._cas = cas_without_status  # type: ignore[method-assign]
    await runtime.reconcile_once()
    assert status_reads == 1
    assert cas_calls == 1
    await runtime.stop()


async def test_background_loops_survive_binding_tool_errors(tmp_path):
    runtime = _runtime(tmp_path)
    runtime.held = ("1", str(uuid4()))
    reconcile_calls = 0

    async def reconcile() -> None:
        nonlocal reconcile_calls
        reconcile_calls += 1
        if reconcile_calls == 1:
            raise relay.ToolError("REBINDING")
        runtime._stopping = True

    runtime.reconcile_once = reconcile  # type: ignore[method-assign]
    runtime._reconcile_wake.set()
    await asyncio.wait_for(runtime._reconciliation_loop(), timeout=1.0)
    assert reconcile_calls == 2

    runtime._stopping = False
    control_calls = 0

    async def controls() -> None:
        nonlocal control_calls
        control_calls += 1
        if control_calls == 1:
            raise relay.ToolError("REBINDING")
        runtime._stopping = True

    runtime._process_user_controls = controls  # type: ignore[method-assign]
    await asyncio.wait_for(runtime._control_loop(), timeout=1.0)
    assert control_calls == 2
    await runtime.stop()


async def test_user_reject_refuses_non_rejected_broker_winner(client, tmp_path):
    runtime = _runtime(tmp_path, client)
    notifications: list[tuple[str, dict]] = []
    identity, _ = await _bind(runtime, notifications)

    async def accepted_winner(*_args, **_kwargs) -> dict:
        return {
            "recipient_id": identity["session_id"],
            "event_id": "1",
            "offer_token": str(uuid4()),
            "offer_generation": 1,
            "binding_epoch": runtime.binding_epoch,
            "omp_process_epoch": runtime.omp_process_epoch,
            "offer_state": "accepted",
            "delivery_status": "outstanding",
            "reject_reason_code": None,
        }

    runtime._cas = accepted_winner  # type: ignore[method-assign]
    with pytest.raises(relay.RelayProtocolError, match="TERMINAL_CONFLICT"):
        await runtime._execute_user_control(
            {
                "command_id": str(uuid4()),
                "action": "REJECT_DELIVERY",
                "args": {"event_id": "1", "offer_token": str(uuid4()), "reason_code": "USER_REJECTED"},
                # A real request is the full signed envelope (_read_user_control), which names its binding.
                "session_id": runtime.session_id,
                "binding_epoch": runtime.binding_epoch,
                "omp_process_epoch": runtime.omp_process_epoch,
            }
        )
    await runtime.stop()



async def test_stdio_server_exposes_only_four_named_tools(tmp_path):
    runtime = _runtime(tmp_path)
    server = relay.create_server(runtime)
    names = set(server._tool_manager._tools)
    assert names == {"register", "name", "check", "send"}
    assert server.name == "omp-relay"
    await runtime.stop()

async def test_first_tools_list_starts_runtime_with_a_live_server_session(tmp_path):
    state_root = tmp_path / "startup-state"
    runtime_root = tmp_path / "startup-runtime"
    state_root.mkdir()
    runtime_root.mkdir()
    health_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"checks": {"omp_relay_contract": 1}})
        ),
        base_url="http://test",
    )
    runtime = relay.OmpRelayRuntime(
        broker_url="http://test",
        state_root=state_root,
        runtime_root=runtime_root,
        process_witness={"pid": 901, "start_key": "9001", "sequence_number": "7001", "boot_id": 52},
        parent_pid=771,
        validate_interpreter=False,
        http_client=health_client,
    )
    notifications: list[tuple[str, dict]] = []

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime.notifier = notify
    server = relay.create_server(runtime)
    async with create_connected_server_and_client_session(server) as session:
        await session.initialize()
        tools = await session.list_tools()
        assert [tool.name for tool in tools.tools] == ["register", "name", "check", "send"]
        assert runtime._started is True
        assert [(method, params["type"]) for method, params in notifications] == [
            (relay.CONTROL_METHOD, "identity_request"),
            (relay.CONTROL_METHOD, "rebind_reconcile"),
        ]
    await runtime.stop()
    await health_client.aclose()


async def test_recovery_status_includes_authenticated_head_and_public_reads_ignore_prior_epoch(client, tmp_path):
    notifications: list[tuple[str, dict]] = []
    runtime = _runtime(tmp_path, client)
    identity, _ = await _bind(runtime, notifications)
    await runtime.tool_name("omp.recovery")
    await asyncio.sleep(0.01)
    sent = await client.post(
        "/api/event",
        json={
            "from_agent": "recovery.peer",
            "to_agent": "omp.recovery",
            "channel": "#agents",
            "kind": "agent_message",
            "content": "recover exactly",
            "ts": int(time.time() * 1000),
        },
    )
    event_id = str(sent.json()["event_id"])
    head = await runtime._claim_head()
    offer_token = head["status"]["offer_token"]
    prior = {
        "schema_version": 1,
        "session_id": identity["session_id"],
        "recipient_id": identity["session_id"],
        "event_id": event_id,
        "offer_token": offer_token,
        "omp_process_epoch": str(uuid4()),
        "state": "received",
        "broker_terminal_state": "none",
        "identity_acknowledged": False,
        "health_reason": "NONE",
        "reject_reason_code": None,
        "application_acceptance_observed": False,
        "updated_at_ms": int(time.time() * 1000),
    }
    relay._atomic_write_json(runtime._offer_path(event_id), prior)
    await runtime._reconcile_prior_candidates(head)
    recovery = [
        params
        for method, params in notifications
        if method == relay.CONTROL_METHOD and params.get("type") == "offer_recovery_status"
    ]
    assert len(recovery) == 1
    assert recovery[0]["status"]["event_id"] == event_id
    assert recovery[0]["delivery"] == head["delivery"]
    assert relay.render_display(recovery[0]["delivery"]) == (
        f"[relay] from=recovery.peer to=omp.recovery event_id={event_id} kind=agent_message\n\nrecover exactly"
    )

    current = {**prior, "event_id": str(int(event_id) + 1), "omp_process_epoch": runtime.omp_process_epoch, "state": "rejected", "broker_terminal_state": "rejected", "reject_reason_code": "USER_REJECTED"}
    prior_rejected = {**prior, "event_id": str(int(event_id) + 2), "state": "rejected", "broker_terminal_state": "rejected", "reject_reason_code": "USER_REJECTED"}
    relay._atomic_write_json(runtime._offer_path(current["event_id"]), current)
    relay._atomic_write_json(runtime._offer_path(prior_rejected["event_id"]), prior_rejected)
    check = await runtime.tool_check()
    assert check["queue"]["rejected_count"] == 1
    await runtime.stop()


@pytest.mark.parametrize(
    ("offer_state", "delivery_status", "reject_reason", "expect_identity_ack"),
    [
        ("rejected", "rejected", "USER_REJECTED", False),
        ("accepted", "acked", None, True),
    ],
)
async def test_empty_head_recovers_terminal_malformed_prior_mirror(
    tmp_path, offer_state, delivery_status, reject_reason, expect_identity_ack
):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    runtime.session_id = identity["session_id"]
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]
    runtime.relay_name = "omp.malformed"
    event_id = "77"
    offer_path = runtime._offer_path(event_id)
    offer_path.parent.mkdir(parents=True)
    offer_path.write_text("{malformed\n", encoding="utf-8")
    offer_token = str(uuid4())
    recovered: list[dict] = []
    identity_acks: list[dict] = []

    async def empty_head() -> dict:
        return {"schema_version": 1, "code": "EMPTY", "status": None, "delivery": None}

    async def terminal_status(requested_event_id: str) -> dict:
        assert requested_event_id == event_id
        return {
            "recipient_id": runtime.session_id,
            "event_id": event_id,
            "offer_token": offer_token,
            "offer_generation": 1,
            "binding_epoch": runtime.binding_epoch,
            "omp_process_epoch": runtime.omp_process_epoch,
            "offer_state": offer_state,
            "delivery_status": delivery_status,
            "reject_reason_code": reject_reason,
        }

    async def terminal_recovery(status: dict) -> None:
        recovered.append(status)

    async def identity_acked(status: dict) -> None:
        identity_acks.append(status)

    runtime._claim_head = empty_head  # type: ignore[method-assign]
    runtime._status = terminal_status  # type: ignore[method-assign]
    runtime._notify_terminal_recovery = terminal_recovery  # type: ignore[method-assign]
    runtime._notify_identity_acked = identity_acked  # type: ignore[method-assign]
    await runtime.reconcile_once()
    assert [(item["event_id"], item["offer_token"], item["offer_state"]) for item in recovered] == [
        (event_id, offer_token, offer_state)
    ]
    assert bool(identity_acks) is expect_identity_ack
    if identity_acks:
        assert identity_acks[0]["delivery_status"] == "acked"
    await runtime.stop()


@pytest.mark.parametrize(("initial_delivery_status", "expected_ack_calls"), [("acked", 0), ("outstanding", 2)])
async def test_queued_head_waits_for_current_process_identity_ack_redrive(
    tmp_path, initial_delivery_status, expected_ack_calls
):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    runtime.session_id = identity["session_id"]
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]
    runtime.relay_name = "omp.ack-recovery"
    event_id = "78"
    offer_token = str(uuid4())
    relay._atomic_write_json(
        runtime._offer_path(event_id),
        {
            "schema_version": 1,
            "session_id": runtime.session_id,
            "recipient_id": runtime.session_id,
            "event_id": event_id,
            "offer_token": offer_token,
            "omp_process_epoch": runtime.omp_process_epoch,
            "state": "accepted",
            "broker_terminal_state": "accepted",
            "identity_acknowledged": False,
            "health_reason": "NONE",
            "reject_reason_code": None,
            "application_acceptance_observed": True,
            "updated_at_ms": int(time.time() * 1000),
        },
    )
    notifications: list[tuple[str, dict]] = []
    ack_calls = 0
    claim_calls = 0

    def status(delivery_status: str) -> dict:
        return {
            "recipient_id": runtime.session_id,
            "event_id": event_id,
            "offer_token": offer_token,
            "offer_generation": 1,
            "binding_epoch": runtime.binding_epoch,
            "omp_process_epoch": runtime.omp_process_epoch,
            "offer_state": "accepted",
            "delivery_status": delivery_status,
            "reject_reason_code": None,
        }

    next_event_id = str(int(event_id) + 1)
    next_status = {
        **status("outstanding"),
        "event_id": next_event_id,
        "offer_token": str(uuid4()),
        "offer_state": "offered",
    }
    next_delivery = relay._validate_delivery(
        {
            "event_id": next_event_id,
            "client_event_id": None,
            "from_agent": "queued.sender",
            "to_agent": runtime.relay_name,
            "channel": "#agents",
            "thread_id": None,
            "kind": "agent_message",
            "content": "queued behind acknowledgement barrier",
            "ts": int(time.time() * 1000),
        }
    )

    async def queued_head() -> dict:
        nonlocal claim_calls
        claim_calls += 1
        return {"schema_version": 1, "code": "HEAD", "status": next_status, "delivery": next_delivery}

    async def current_status(requested_event_id: str) -> dict:
        assert requested_event_id == event_id
        return status(initial_delivery_status)

    async def acknowledge(requested_event_id: str, requested_token: str) -> dict:
        nonlocal ack_calls
        assert (requested_event_id, requested_token) == (event_id, offer_token)
        ack_calls += 1
        return status("acked")

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime._claim_head = queued_head  # type: ignore[method-assign]
    runtime._status = current_status  # type: ignore[method-assign]
    runtime._ack = acknowledge  # type: ignore[method-assign]
    runtime.notifier = notify
    await runtime.reconcile_once()
    await runtime.reconcile_once()
    assert claim_calls == 0
    assert ack_calls == expected_ack_calls
    identity_acks = [
        params
        for method, params in notifications
        if method == "notifications/omp/relay/control" and params.get("type") == "identity_acked"
    ]
    assert [item["event_id"] for item in identity_acks] == [event_id, event_id]

    kind, acknowledged = runtime._read_mirror(event_id)
    assert kind == "valid" and acknowledged is not None
    relay._atomic_write_json(runtime._offer_path(event_id), {**acknowledged, "identity_acknowledged": True})
    await runtime.reconcile_once()
    assert claim_calls == 1
    assert runtime.held == (next_event_id, next_status["offer_token"])
    message_notifications = [
        params for method, params in notifications if method == "notifications/omp/relay/message"
    ]
    assert [item["delivery"]["event_id"] for item in message_notifications] == [next_event_id]
    await runtime.stop()


async def test_queued_head_waits_for_current_process_rejection_redrive(tmp_path):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    runtime.session_id = identity["session_id"]
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]
    runtime.relay_name = "omp.reject-recovery"
    event_id = "80"
    offer_token = str(uuid4())
    mirror = {
        "schema_version": 1,
        "session_id": runtime.session_id,
        "recipient_id": runtime.session_id,
        "event_id": event_id,
        "offer_token": offer_token,
        "omp_process_epoch": runtime.omp_process_epoch,
        "state": "injected",
        "broker_terminal_state": "none",
        "identity_acknowledged": False,
        "health_reason": "NONE",
        "reject_reason_code": None,
        "application_acceptance_observed": False,
        "updated_at_ms": int(time.time() * 1000),
    }
    relay._atomic_write_json(runtime._offer_path(event_id), mirror)
    rejected_status = {
        "recipient_id": runtime.session_id,
        "event_id": event_id,
        "offer_token": offer_token,
        "offer_generation": 1,
        "binding_epoch": runtime.binding_epoch,
        "omp_process_epoch": runtime.omp_process_epoch,
        "offer_state": "rejected",
        "delivery_status": "rejected",
        "reject_reason_code": "USER_REJECTED",
    }
    next_event_id = str(int(event_id) + 1)
    next_status = {
        **rejected_status,
        "event_id": next_event_id,
        "offer_token": str(uuid4()),
        "offer_state": "offered",
        "delivery_status": "outstanding",
        "reject_reason_code": None,
    }
    next_delivery = relay._validate_delivery(
        {
            "event_id": next_event_id,
            "client_event_id": None,
            "from_agent": "queued.sender",
            "to_agent": runtime.relay_name,
            "channel": "#agents",
            "thread_id": None,
            "kind": "agent_message",
            "content": "queued behind rejection barrier",
            "ts": int(time.time() * 1000),
        }
    )
    notifications: list[tuple[str, dict]] = []
    claim_calls = 0

    async def status(requested_event_id: str) -> dict:
        assert requested_event_id == event_id
        return rejected_status

    async def queued_head() -> dict:
        nonlocal claim_calls
        claim_calls += 1
        return {"schema_version": 1, "code": "HEAD", "status": next_status, "delivery": next_delivery}

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime._status = status  # type: ignore[method-assign]
    runtime._claim_head = queued_head  # type: ignore[method-assign]
    runtime.notifier = notify
    await runtime.reconcile_once()
    await runtime.reconcile_once()
    assert claim_calls == 0
    rejections = [
        params
        for method, params in notifications
        if method == "notifications/omp/relay/control" and params.get("type") == "offer_rejected"
    ]
    assert [item["event_id"] for item in rejections] == [event_id, event_id]

    relay._atomic_write_json(
        runtime._offer_path(event_id),
        {**mirror, "state": "rejected", "broker_terminal_state": "rejected", "reject_reason_code": "USER_REJECTED"},
    )
    await runtime.reconcile_once()
    assert claim_calls == 1
    assert runtime.held == (next_event_id, next_status["offer_token"])
    messages = [params for method, params in notifications if method == "notifications/omp/relay/message"]
    assert [item["delivery"]["event_id"] for item in messages] == [next_event_id]
    await runtime.stop()


@pytest.mark.parametrize("candidate_kind", ["prior", "malformed"])
async def test_status_failure_keeps_prior_barrier_ahead_of_queued_head(tmp_path, candidate_kind):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    runtime.session_id = identity["session_id"]
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]
    runtime.relay_name = "omp.status-barrier"
    event_id = "82"
    offer_token = str(uuid4())
    prior = {
        "schema_version": 1,
        "session_id": runtime.session_id,
        "recipient_id": runtime.session_id,
        "event_id": event_id,
        "offer_token": offer_token,
        "omp_process_epoch": str(uuid4()),
        "state": "injected",
        "broker_terminal_state": "none",
        "identity_acknowledged": False,
        "health_reason": "NONE",
        "reject_reason_code": None,
        "application_acceptance_observed": False,
        "updated_at_ms": int(time.time() * 1000),
    }
    if candidate_kind == "prior":
        relay._atomic_write_json(runtime._offer_path(event_id), prior)
    else:
        runtime._offer_path(event_id).parent.mkdir(parents=True)
        runtime._offer_path(event_id).write_text("{malformed\n", encoding="utf-8")
    rejected_status = {
        "recipient_id": runtime.session_id,
        "event_id": event_id,
        "offer_token": offer_token,
        "offer_generation": 1,
        "binding_epoch": runtime.binding_epoch,
        "omp_process_epoch": runtime.omp_process_epoch,
        "offer_state": "rejected",
        "delivery_status": "rejected",
        "reject_reason_code": "USER_REJECTED",
    }
    next_event_id = str(int(event_id) + 1)
    next_status = {
        **rejected_status,
        "event_id": next_event_id,
        "offer_token": str(uuid4()),
        "offer_state": "offered",
        "delivery_status": "outstanding",
        "reject_reason_code": None,
    }
    next_delivery = relay._validate_delivery(
        {
            "event_id": next_event_id,
            "client_event_id": None,
            "from_agent": "queued.sender",
            "to_agent": runtime.relay_name,
            "channel": "#agents",
            "thread_id": None,
            "kind": "agent_message",
            "content": "queued behind unreadable status barrier",
            "ts": int(time.time() * 1000),
        }
    )
    status_calls = 0
    claim_calls = 0
    notifications: list[tuple[str, dict]] = []

    async def status(requested_event_id: str) -> dict:
        nonlocal status_calls
        assert requested_event_id == event_id
        status_calls += 1
        if status_calls == 1:
            raise relay.RelayProtocolError("BROKER_UNREACHABLE")
        return rejected_status

    async def queued_head() -> dict:
        nonlocal claim_calls
        claim_calls += 1
        return {"schema_version": 1, "code": "HEAD", "status": next_status, "delivery": next_delivery}

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime._status = status  # type: ignore[method-assign]
    runtime._claim_head = queued_head  # type: ignore[method-assign]
    runtime.notifier = notify
    with pytest.raises(relay.RelayProtocolError, match="BROKER_UNREACHABLE"):
        await runtime.reconcile_once()
    assert claim_calls == 0
    await runtime.reconcile_once()
    assert claim_calls == 0
    expected_type = "offer_recovery_status" if candidate_kind == "prior" else "offer_terminal_recovery"
    controls = [
        params
        for method, params in notifications
        if method == "notifications/omp/relay/control" and params.get("type") == expected_type
    ]
    assert [item["status"]["event_id"] if expected_type == "offer_recovery_status" else item["event_id"] for item in controls] == [event_id]

    relay._atomic_write_json(
        runtime._offer_path(event_id),
        {
            **prior,
            "omp_process_epoch": runtime.omp_process_epoch,
            "state": "rejected",
            "broker_terminal_state": "rejected",
            "reject_reason_code": "USER_REJECTED",
        },
    )
    await runtime.reconcile_once()
    assert claim_calls == 1
    assert runtime.held == (next_event_id, next_status["offer_token"])
    await runtime.stop()


async def test_check_surfaces_private_status_failure_instead_of_unoffered_projection(tmp_path):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    runtime.session_id = identity["session_id"]
    runtime.binding_epoch = runtime.new_binding_epoch
    runtime.omp_process_epoch = identity["omp_process_epoch"]

    async def request(method: str, path: str, **kwargs) -> dict:
        assert method == "GET"
        if path.endswith("/status"):
            return {"relay_name": "omp.check-failure"}
        assert path.endswith("/deliveries")
        return {"deliveries": [{"event_id": "1", "content": "unread status"}], "next_cursor": None}

    async def unavailable_status(event_id: str) -> dict:
        assert event_id == "1"
        raise relay.RelayProtocolError("BROKER_UNREACHABLE")

    runtime._request = request  # type: ignore[method-assign]
    runtime._status = unavailable_status  # type: ignore[method-assign]
    with pytest.raises(relay.ToolError, match="BROKER_UNREACHABLE"):
        await runtime.tool_check()
    await runtime.stop()


async def test_adapter_refuses_missing_broker_capability_and_never_imports_operator_secret(tmp_path, monkeypatch):
    sentinel = "operator-secret-must-not-enter-adapter"
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", sentinel)
    monkeypatch.setenv("BROKER_OPERATOR_ID", "operator-id-must-not-enter-adapter")

    async def health(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/health"
        return httpx.Response(200, json={"checks": {"omp_relay_contract": 0}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(health), base_url="http://test")
    runtime = _runtime(tmp_path, client)
    with pytest.raises(relay.RelayProtocolError, match="BROKER_CAPABILITY_MISSING"):
        await runtime._require_capability()
    server = relay.create_server(runtime)
    assert set(server._tool_manager._tools) == {"register", "name", "check", "send"}
    assert sentinel not in repr(runtime.__dict__)
    assert "operator-id-must-not-enter-adapter" not in repr(runtime.__dict__)
    await runtime.stop()
    await client.aclose()



# F2, producer side (2026-10-09). The child rebinds to another session while a request is in flight.
# Every control or result built from that request's answer must still name the session and binding
# the request was made under, and any mirror it consults must be that session's. The fake broker
# answers as server.py's _offer_status does: a status echoes the recipient in the request path and the
# binding epochs the request authenticated with.
CROSSING = {
    "held-ack": "delivery-ack",
    "held-cas": "delivery-offer-status",
    "event-terminal-recovery": "status",
    "event-recovery-status": "status",
    "retry-message": "delivery-offer",
    "user-reject": "delivery-offer-status",
    "user-takeover": "relay-name",
    "event-retry-message": "status",
    "prior-recovery-status": "status",
}
CROSSING_STATE = {
    "held-ack": ("accepted", "acked", None),
    "held-cas": ("accepted", "outstanding", None),
    "event-terminal-recovery": ("accepted", "acked", None),
    "event-recovery-status": ("offered", "outstanding", None),
    "retry-message": ("offered", "outstanding", None),
    "user-reject": ("rejected", "rejected", "USER_REJECTED"),
    "user-takeover": ("offered", "outstanding", None),
    "event-retry-message": ("offered", "outstanding", None),
    "prior-recovery-status": ("offered", "outstanding", None),
}
CROSSING_SENDS = {
    "held-ack": "identity_acked",
    "held-cas": "offer_accepted",
    "event-terminal-recovery": "offer_terminal_recovery",
    "event-recovery-status": "offer_recovery_status",
    "retry-message": "message",
    "user-reject": "offer_rejected",
    "user-takeover": None,
    # Since the final review a claim made after the rebind still goes to the first session (its pinned
    # binding), so the head is the first session's own and its delivery is used under its attribution.
    "event-retry-message": "message",
    "prior-recovery-status": "offer_recovery_status",
}


@pytest.mark.parametrize("case", sorted(CROSSING))
async def test_control_built_after_an_await_keeps_its_operation_attribution(tmp_path, case):
    runtime = _runtime(tmp_path)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    first = (identity["session_id"], runtime.new_binding_epoch, identity["omp_process_epoch"])
    second = (str(uuid4()), str(uuid4()), identity["omp_process_epoch"])
    runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch = first
    runtime.relay_name = "omp.first"
    event_id = "41"
    offer_token = str(uuid4())
    offer_state, delivery_status, reason = CROSSING_STATE[case]

    def delivery() -> dict:
        return {
            "event_id": event_id, "client_event_id": None, "from_agent": "peer.agent", "to_agent": "omp.first",
            "channel": "#agents", "thread_id": None, "kind": "agent_message", "content": "crossing",
            "ts": int(time.time() * 1000),
        }

    def mirror(**fields) -> dict:
        value = {
            "schema_version": 1, "session_id": first[0], "recipient_id": first[0], "event_id": event_id,
            "offer_token": offer_token, "omp_process_epoch": first[2], "state": "accepted",
            "broker_terminal_state": "none", "identity_acknowledged": False, "health_reason": "NONE",
            "reject_reason_code": None, "application_acceptance_observed": True,
            "updated_at_ms": int(time.time() * 1000),
        }
        value.update(fields)
        return value

    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(method, url, *, params=None, json_body=None, private=False):
        parts = url.split("/")
        operation = parts[-1]
        if operation == CROSSING[case] and not entered.is_set():
            entered.set()
            await release.wait()
        if operation == "relay-name":
            return {"relay_name": json_body["relay_name"], "displaced_id": None}
        authenticated = json_body if json_body is not None else params
        status = {
            "recipient_id": unquote(parts[3]), "event_id": event_id, "offer_token": offer_token,
            "offer_generation": 1, "binding_epoch": authenticated["binding_epoch"],
            "omp_process_epoch": authenticated["omp_process_epoch"], "offer_state": offer_state,
            "delivery_status": delivery_status, "reject_reason_code": reason,
        }
        if operation == "delivery-ack":
            return {"schema_version": 1, "code": "ACKED", "status": status}
        if operation == "delivery-offer-status":
            return {"schema_version": 1, "code": "UPDATED", "status": status}
        if operation == "delivery-offer":
            return {"schema_version": 1, "code": "HEAD", "status": status, "delivery": delivery()}
        return {"schema_version": 1, "status": status}

    notifications: list[tuple[str, dict]] = []

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime._request = request  # type: ignore[method-assign]
    runtime.notifier = notify
    held_status = {
        "recipient_id": first[0], "event_id": event_id, "offer_token": offer_token, "offer_generation": 1,
        "binding_epoch": first[1], "omp_process_epoch": first[2], "offer_state": "offered",
        "delivery_status": "outstanding", "reject_reason_code": None,
    }
    command_id = str(uuid4())
    attribution = {"session_id": first[0], "binding_epoch": first[1], "omp_process_epoch": first[2]}
    if case == "held-ack":
        relay._atomic_write_json(runtime._offer_path(event_id), mirror(broker_terminal_state="accepted"))
        runtime._hold_event({**held_status, "offer_state": "accepted"}, delivery())
        operation = runtime._reconcile_held_local()
    elif case == "held-cas":
        relay._atomic_write_json(runtime._offer_path(event_id), mirror())
        runtime._hold_event(held_status, delivery())
        operation = runtime._reconcile_held_local()
    elif case == "event-terminal-recovery":
        runtime._offer_path(event_id).parent.mkdir(parents=True, exist_ok=True)
        runtime._offer_path(event_id).write_text("{\n", encoding="utf-8")
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case == "event-recovery-status":
        relay._atomic_write_json(runtime._offer_path(event_id), mirror(omp_process_epoch=str(uuid4()), state="received", application_acceptance_observed=False))
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case == "retry-message":
        operation = runtime._retry_message_notification(held_status, None)
    elif case == "event-retry-message":
        # No mirror yet for the first session: the answer leads to a retry that claims a head.
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case == "prior-recovery-status":
        relay._atomic_write_json(runtime._offer_path(event_id), mirror(omp_process_epoch=str(uuid4()), state="received", application_acceptance_observed=False))
        operation = runtime._reconcile_prior_candidates(None)
    elif case == "user-reject":
        operation = runtime._execute_user_control({
            "command_id": command_id, "action": "REJECT_DELIVERY",
            "args": {"event_id": event_id, "offer_token": offer_token, "reason_code": "USER_REJECTED"}, **attribution,
        })
    else:
        operation = runtime._execute_user_control({
            "command_id": command_id, "action": "TAKEOVER_NAME", "args": {"name": "omp.claimed"}, **attribution,
        })

    async def rebind() -> None:
        # What _complete_identity_handshake does when health names another epoch, completed.
        await entered.wait()
        runtime._clear_held_event()
        runtime.pending_ack.clear()
        runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch = second
        runtime.relay_name = "omp.second"
        release.set()

    await asyncio.wait_for(asyncio.gather(operation, rebind()), timeout=10)
    violations = []
    sends = CROSSING_SENDS[case]
    built = [
        params for method, params in notifications
        if (sends == "message" and method == relay.MESSAGE_METHOD) or (method == relay.CONTROL_METHOD and params.get("type") == sends)
    ]
    if sends is not None and len(built) != 1:
        violations.append(f"expected one {sends} built from the first session's answer, got {notifications}")
    for params in built:
        if sends == "offer_recovery_status":
            got = (params["status"]["recipient_id"], params["binding_epoch"], params["omp_process_epoch"])
            if got != first:
                violations.append(f"{sends} was relabelled: {got} instead of {first}")
            if params["delivery"] is None or params["delivery"]["event_id"] != event_id:
                violations.append(f"the recovery status lacks the first session's own delivery: {params['delivery']}")
        elif (params["recipient_id"], params["binding_epoch"]) != first[:2]:
            violations.append(f"{sends} was relabelled: {(params['recipient_id'], params['binding_epoch'])} instead of {first[:2]}")
    others = [p for m, p in notifications if p not in built]
    if others:
        violations.append(f"other notifications were sent: {others}")
    if case.startswith("user-"):
        result_file = runtime.rendezvous_dir / "control" / "results" / f"{command_id}.json"
        result = json.loads(result_file.read_text()) if result_file.exists() else None
        if result is None:
            violations.append("the user command's result was not written")
        elif (result["session_id"], result["binding_epoch"], result["omp_process_epoch"]) != first:
            violations.append(f"the user command's result was relabelled: {(result['session_id'], result['binding_epoch'])}")
    if case == "user-takeover" and runtime.relay_name != "omp.second":
        violations.append(f"the first session's claimed name became the second session's: {runtime.relay_name}")
    await runtime.stop()
    assert violations == []



# Final review, finding 2 (2026-10-09). An operation that already has its attribution keeps it for every
# broker request after its first await: session, binding, process epoch and control credential. The child
# rebinds to a second session while the operation's first request is in flight. The fake broker
# authenticates each private request the way server.py's _require_current_control does (token hash,
# binding epoch and process epoch against the recipient's own row), and a rebind replaces only the
# second session's row, as server.py's bind does. Every request made after the rebind must go to the
# first session's endpoint under the first session's credentials, and be accepted there.
FOLLOW_ON = {
    # path: (first request, follow-on requests)
    "event-ack": ("status", ["ack"]),
    "event-ack-fallback": ("status", ["ack", "status"]),
    "event-cas-injected": ("status", ["cas"]),
    "event-cas-accepted": ("status", ["cas"]),
    "event-cas-fallback": ("status", ["cas", "status"]),
    "event-retry-claim": ("status", ["claim"]),
    "event-prior-claim": ("status", ["claim"]),
    "prior-candidates-claim": ("status", ["claim"]),
    "reconcile-once-head": ("claim", ["status"]),
    "tool-check": ("identity", ["deliveries", "status"]),
}


@pytest.mark.parametrize("case", sorted(FOLLOW_ON))
async def test_follow_on_broker_requests_keep_the_operation_attribution(tmp_path, case):
    first_request, expected_follow_on = FOLLOW_ON[case]
    event_id = "41"
    offer_token = str(uuid4())
    rows: dict[str, tuple[str, str, str]] = {}
    calls: list[dict] = []
    rebound = False
    entered = asyncio.Event()
    release = asyncio.Event()
    offer_state, delivery_status = {
        "event-ack": ("accepted", "outstanding"), "event-ack-fallback": ("accepted", "outstanding"),
        "event-cas-injected": ("offered", "outstanding"), "event-cas-accepted": ("offered", "outstanding"),
        "event-cas-fallback": ("offered", "outstanding"), "event-retry-claim": ("offered", "outstanding"),
        "event-prior-claim": ("offered", "outstanding"), "prior-candidates-claim": ("offered", "outstanding"),
        "reconcile-once-head": ("accepted", "outstanding"), "tool-check": ("offered", "outstanding"),
    }[case]
    delivery = {
        "event_id": event_id, "client_event_id": None, "from_agent": "peer.agent", "to_agent": "omp.first",
        "channel": "#agents", "thread_id": None, "kind": "agent_message", "content": "follow-on",
        "ts": int(time.time() * 1000),
    }

    def reply(code: int, body: dict) -> httpx.Response:
        return httpx.Response(code, json=body)

    async def broker(request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        session = unquote(parts[3])
        tail = "/".join(parts[4:])
        kind = {"delivery-ack": "ack", "delivery-offer-status": "cas", "delivery-offer": "claim",
                "deliveries": "deliveries", "status": "identity"}.get(tail, "status" if tail.endswith("/status") else tail)
        body = json.loads(request.content) if request.content else {}
        presented = request.headers.get("authorization", "")
        private = presented.startswith("RelayControl ")
        call = {"after_rebind": rebound, "kind": kind, "session": session, "private": private}
        if private:
            encoded = presented.split(" ", 1)[1]
            token = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            call.update(
                binding=body.get("binding_epoch", request.url.params.get("binding_epoch")),
                process=body.get("omp_process_epoch", request.url.params.get("omp_process_epoch")),
                token_hash=hashlib.sha256(token).hexdigest(),
            )
        calls.append(call)
        if kind == first_request and not entered.is_set():
            entered.set()
            await release.wait()
        if private:
            row = rows.get(session)
            if row is None or (call["binding"], call["process"], call["token_hash"]) != row:
                call["answer"] = 403
                return reply(403, {"error": {"code": "CONTROL_INVALID"}})
            binding, process, _ = row
        call["answer"] = 200
        if session != first[0] and kind in ("ack", "cas", "status"):
            call["answer"] = 404
            return reply(404, {"error": {"code": "DELIVERY_NOT_FOUND"}})
        if kind == "identity":
            return reply(200, {"relay_name": "omp.first"})
        if kind == "deliveries":
            return reply(200, {"deliveries": [{"event_id": int(event_id), "from_agent": "peer.agent"}], "next_cursor": None})
        if kind == "claim" and session != first[0]:
            return reply(200, {"schema_version": 1, "code": "EMPTY", "status": None, "delivery": None})
        state, acked = offer_state, delivery_status
        if kind == "ack":
            if case == "event-ack-fallback":
                call["answer"] = 409
                return reply(409, {"error": {"code": "TERMINAL_CONFLICT"}})
            state, acked = "accepted", "acked"
        if kind == "cas":
            if case == "event-cas-fallback":
                call["answer"] = 409
                return reply(409, {"error": {"code": "TERMINAL_CONFLICT"}})
            state = body["target_state"]
        status = {
            "recipient_id": session, "event_id": event_id, "offer_token": offer_token, "offer_generation": 1,
            "binding_epoch": binding, "omp_process_epoch": process, "offer_state": state,
            "delivery_status": acked, "reject_reason_code": None,
        }
        if kind == "ack":
            return reply(200, {"schema_version": 1, "code": "ACKED", "status": status})
        if kind == "cas":
            return reply(200, {"schema_version": 1, "code": "UPDATED", "status": status})
        if kind == "claim":
            return reply(200, {"schema_version": 1, "code": "HEAD", "status": status, "delivery": delivery})
        return reply(200, {"schema_version": 1, "status": status})

    client = httpx.AsyncClient(transport=httpx.MockTransport(broker), base_url="http://test")
    runtime = _runtime(tmp_path, client)
    identity, _ = _identity(runtime)
    runtime.identity = identity
    first = (identity["session_id"], runtime.new_binding_epoch, identity["omp_process_epoch"])
    runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch = first
    runtime.relay_name = "omp.first"
    rows[first[0]] = (first[1], first[2], runtime.control_token_hash)
    notifications: list[tuple[str, dict]] = []

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime.notifier = notify

    def mirror(**fields) -> dict:
        value = {
            "schema_version": 1, "session_id": first[0], "recipient_id": first[0], "event_id": event_id,
            "offer_token": offer_token, "omp_process_epoch": first[2], "state": "accepted",
            "broker_terminal_state": "none", "identity_acknowledged": False, "health_reason": "NONE",
            "reject_reason_code": None, "application_acceptance_observed": True,
            "updated_at_ms": int(time.time() * 1000),
        }
        value.update(fields)
        return value

    if case in ("event-ack", "event-ack-fallback"):
        relay._atomic_write_json(runtime._offer_path(event_id), mirror(broker_terminal_state="accepted"))
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case == "event-cas-injected":
        relay._atomic_write_json(runtime._offer_path(event_id), mirror(state="injected", application_acceptance_observed=False))
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case in ("event-cas-accepted", "event-cas-fallback"):
        relay._atomic_write_json(runtime._offer_path(event_id), mirror())
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case == "event-retry-claim":
        operation = runtime._reconcile_event(event_id, offer_token, None)
    elif case in ("event-prior-claim", "prior-candidates-claim"):
        relay._atomic_write_json(runtime._offer_path(event_id), mirror(omp_process_epoch=str(uuid4()), state="received", application_acceptance_observed=False))
        operation = (runtime._reconcile_event(event_id, offer_token, None) if case == "event-prior-claim"
                     else runtime._reconcile_prior_candidates(None))
    elif case == "reconcile-once-head":
        operation = runtime.reconcile_once()
    else:
        operation = runtime.tool_check()

    async def rebind() -> None:
        # _complete_identity_handshake's rebind to a second session, completed: a new control credential
        # and binding, and the broker's row for the second session (the first session's row is untouched).
        nonlocal rebound
        await entered.wait()
        runtime._clear_held_event()
        runtime.pending_ack.clear()
        runtime._prepare_bind_attempt(None)
        second = (str(uuid4()), runtime.new_binding_epoch, str(uuid4()))
        runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch = second
        runtime.identity = {**identity, "session_id": second[0]}
        runtime.relay_name = "omp.second"
        rows[second[0]] = (second[1], second[2], runtime.control_token_hash)
        rebound = True
        release.set()

    outcome = await asyncio.wait_for(asyncio.gather(operation, rebind(), return_exceptions=True), timeout=10)
    violations = []
    if isinstance(outcome[0], BaseException):
        violations.append(f"the operation failed: {outcome[0]!r}")
    follow_on = [call for call in calls if call["after_rebind"]]
    if [call["kind"] for call in follow_on] != expected_follow_on:
        violations.append(f"follow-on requests were {[(c['kind'], c['session'][:8]) for c in follow_on]}, expected {expected_follow_on}")
    for call in follow_on:
        if call["session"] != first[0]:
            violations.append(f"{call['kind']} went to another session's endpoint")
        if call["private"] and (call["binding"], call["process"], call["token_hash"]) != rows[first[0]]:
            violations.append(f"{call['kind']} carried another binding or control credential")
        if call["answer"] == 403:
            violations.append(f"{call['kind']} was refused by the broker's authentication")
    await runtime.stop()
    await client.aclose()
    assert violations == []


# The same, measured against server.py itself (in-process, temporary database): S1's offer is accepted
# but not yet acknowledged; the child rebinds to S2 while _reconcile_event's first status request is in
# flight. The broker's answer to the follow-on ACK decides whether S1's completion is recorded.
async def test_rebind_during_status_still_acknowledges_at_the_first_sessions_endpoint(client, tmp_path):
    notifications: list[tuple[str, dict]] = []
    runtime = _runtime(tmp_path, client)
    identity, _ = await _bind(runtime, notifications)
    first_session = identity["session_id"]
    assert (await runtime.tool_name("omp.final.ack"))["relay_name"] == "omp.final.ack"
    sent = await client.post(
        "/api/event",
        json={
            "from_agent": "sender.final", "to_agent": "omp.final.ack", "channel": "#agents",
            "kind": "agent_message", "content": "ack across a rebind", "ts": int(time.time() * 1000),
            "client_event_id": f"final-{uuid4()}",
        },
    )
    assert sent.status_code == 200, sent.text
    event_id = str(sent.json()["event_id"])
    await runtime.reconcile_once()
    offer_token = [p for m, p in notifications if m == relay.MESSAGE_METHOD][0]["offer_token"]
    mirror = {
        "schema_version": 1, "session_id": first_session, "recipient_id": first_session, "event_id": event_id,
        "offer_token": offer_token, "omp_process_epoch": runtime.omp_process_epoch, "state": "accepted",
        "broker_terminal_state": "none", "identity_acknowledged": False, "health_reason": "NONE",
        "reject_reason_code": None, "application_acceptance_observed": True, "updated_at_ms": int(time.time() * 1000),
    }
    relay._atomic_write_json(runtime._offer_path(event_id), mirror)
    await runtime.reconcile_once()
    mirror["broker_terminal_state"] = "accepted"
    relay._atomic_write_json(runtime._offer_path(event_id), mirror)
    runtime._clear_held_event()

    entered = asyncio.Event()
    release = asyncio.Event()
    answers: list[tuple[str, int, str]] = []

    class InFlight:
        # The pause sits where the real await does: after _request has built the request and its
        # credentials, while the response is outstanding.
        async def request(self, method, url, **kwargs):
            if url.endswith(f"/delivery/{event_id}/status") and not entered.is_set():
                entered.set()
                await release.wait()
            response = await client.request(method, url, **kwargs)
            error = response.json().get("error") if response.is_error else None
            session, _, tail = url.split("/api/agent/")[-1].partition("/")
            who = "first" if session == first_session else "other"
            answers.append((f"{method} {who}/{tail}", response.status_code,
                            error.get("code") if isinstance(error, dict) else "OK"))
            return response

    runtime.http_client = InFlight()  # type: ignore[assignment]

    async def rebind() -> None:
        await entered.wait()
        runtime._prepare_bind_attempt(None)
        second_identity, second_private = _identity(runtime)
        runtime.identity = second_identity
        await runtime._bind(_authorization(runtime, second_identity, second_private))
        release.set()

    outcome = await asyncio.wait_for(
        asyncio.gather(runtime._reconcile_event(event_id, offer_token, None), rebind(), return_exceptions=True), timeout=10
    )
    second_session = runtime.session_id
    outstanding = await client.get(f"/api/agent/{first_session}/deliveries")
    acks = [p for m, p in notifications if m == relay.CONTROL_METHOD and p.get("type") == "identity_acked"]
    violations = []
    if second_session == first_session:
        violations.append("the rebind did not move the child to a second session")
    if isinstance(outcome[0], BaseException):
        violations.append(f"the first session's reconcile failed: {outcome[0]!r}; broker answers {answers}")
    if outstanding.json()["deliveries"] != []:
        violations.append(f"the first session's offer is still outstanding at the broker; answers {answers}")
    if [(p["recipient_id"], p["binding_epoch"]) for p in acks] != [(first_session, identity["binding_epoch"])]:
        violations.append(f"identity_acked for the first session: {acks}")
    print("BROKER ANSWERS " + json.dumps(answers))
    await runtime.stop()
    assert violations == []


# A late answer must preserve the state of the binding that superseded it.
# under the first binding finishes after the child has bound a second. The second binding's own state --
# its held offer and the hold's companions, its relay name, its health, and the local half of a check --
# must stay its own. A same-session rebind keeps the session and the omp process and changes only the
# binding epoch, so a guard comparing the session (or the session and process) cannot pass it.
REBINDS = ["other-session", "same-session"]


def _bound(runtime: relay.OmpRelayRuntime) -> tuple[dict, tuple[str, str, str]]:
    identity, _ = _identity(runtime)
    runtime.identity = identity
    first = (identity["session_id"], runtime.new_binding_epoch, identity["omp_process_epoch"])
    runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch = first
    runtime.relay_name = "omp.first"
    runtime.health_state, runtime.health_code = "connected", "CONNECTED"
    return identity, first


def _rebind(runtime: relay.OmpRelayRuntime, identity: dict, kind: str) -> tuple[str, str, str]:
    # _complete_identity_handshake's rebind, completed: the barrier is dropped and a new binding installed.
    runtime._clear_held_event()
    runtime.pending_ack.clear()
    same = kind == "same-session"
    runtime._prepare_bind_attempt(runtime.binding_epoch if same else None)
    second = (
        runtime.session_id if same else str(uuid4()),
        runtime.new_binding_epoch,
        runtime.omp_process_epoch if same else str(uuid4()),
    )
    runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch = second
    runtime.identity = {**identity, "session_id": second[0], "omp_process_epoch": second[2]}
    return second


def _offer_status(session_id: str, event_id: str, offer_token: str, authenticated: dict, **fields) -> dict:
    # As server.py's _offer_status: the recipient in the request path, the binding the request authenticated with.
    value = {
        "recipient_id": session_id, "event_id": event_id, "offer_token": offer_token, "offer_generation": 1,
        "binding_epoch": authenticated["binding_epoch"], "omp_process_epoch": authenticated["omp_process_epoch"],
        "offer_state": "offered", "delivery_status": "outstanding", "reject_reason_code": None,
    }
    value.update(fields)
    return value


def _delivery(event_id: str) -> dict:
    return {
        "event_id": event_id, "client_event_id": None, "from_agent": "peer.agent", "to_agent": "omp.first",
        "channel": "#agents", "thread_id": None, "kind": "agent_message", "content": f"event {event_id}",
        "ts": int(time.time() * 1000),
    }


def _mirror(session_id: str, process_epoch: str, event_id: str, offer_token: str, **fields) -> dict:
    value = {
        "schema_version": 1, "session_id": session_id, "recipient_id": session_id, "event_id": event_id,
        "offer_token": offer_token, "omp_process_epoch": process_epoch, "state": "received",
        "broker_terminal_state": "none", "identity_acknowledged": False, "health_reason": "NONE",
        "reject_reason_code": None, "application_acceptance_observed": False,
        "updated_at_ms": int(time.time() * 1000),
    }
    value.update(fields)
    return value


# Finding 0, measured against server.py: the first binding's claim reaches the broker, which records the
# offer, and its answer is still in flight when the child binds again. An "accepted" head was accepted under
# the first binding and has no mirror left to complete it from (with one, the prior-candidate scan would
# take it before any claim); the step still hands it to _reconcile_event under the first binding.
@pytest.mark.parametrize("head", ["offered", "accepted"])
@pytest.mark.parametrize("rebind_kind", REBINDS)
async def test_a_late_claim_is_not_held_for_the_next_binding(client, tmp_path, rebind_kind, head):
    notifications: list[tuple[str, dict]] = []
    runtime = _runtime(tmp_path, client)
    await _bind(runtime, notifications)
    first = (runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch)
    first_credential = runtime.control_token
    await runtime.tool_name("omp.late.first")
    await asyncio.gather(*tuple(runtime._tasks))
    same = rebind_kind == "same-session"

    async def post(to_agent: str) -> str:
        sent = await client.post(
            "/api/event",
            json={
                "from_agent": "sender.late", "to_agent": to_agent, "channel": "#agents", "kind": "agent_message",
                "content": f"for {to_agent}", "ts": int(time.time() * 1000), "client_event_id": f"late-{uuid4()}",
            },
        )
        assert sent.status_code == 200, sent.text
        return str(sent.json()["event_id"])

    first_event = await post("omp.late.first")
    if head == "accepted":
        claimed = await runtime._claim_head()
        await runtime._cas(first_event, claimed["status"]["offer_token"], "accepted")
    second_event = first_event
    entered = asyncio.Event()
    release = asyncio.Event()
    late_claims: list[dict] = []

    class InFlight:
        async def request(self, method, url, **kwargs):
            response = await client.request(method, url, **kwargs)
            if url.endswith("/delivery-offer") and not entered.is_set():
                late_claims.append(response.json())
                entered.set()
                await release.wait()
            return response

    runtime.http_client = InFlight()  # type: ignore[assignment]

    async def rebind() -> None:
        nonlocal second_event
        await entered.wait()
        runtime._clear_held_event()
        runtime.pending_ack.clear()
        runtime.binding_epoch = runtime.omp_process_epoch = runtime.session_id = runtime.relay_name = None
        runtime._prepare_bind_attempt(first[1] if same else None)
        second_identity, second_private = _identity(runtime)
        if same:
            second_identity.update(session_id=first[0], omp_process_epoch=first[2])
        runtime.identity = second_identity
        await runtime._bind(_authorization(runtime, second_identity, second_private))
        if not same:
            named = await client.put(
                f"/api/agent/{runtime.session_id}/relay-name",
                json={
                    "schema_version": 1, "relay_name": "omp.late.second", "takeover": False,
                    "binding_epoch": runtime.binding_epoch, "omp_process_epoch": runtime.omp_process_epoch,
                },
                headers=runtime._private_headers(),
            )
            assert named.status_code == 200, named.text
            runtime.relay_name = "omp.late.second"
            runtime.health_state, runtime.health_code = "connected", "CONNECTED"
            second_event = await post("omp.late.second")
        release.set()

    outcome = await asyncio.wait_for(
        asyncio.gather(runtime.reconcile_once(), rebind(), return_exceptions=True), timeout=10
    )
    second = (runtime.session_id, runtime.binding_epoch, runtime.omp_process_epoch)
    violations = []
    if second[1] == first[1] or (second[0] == first[0]) != same:
        violations.append(f"the rebind did not produce the intended second binding: {first} -> {second}")
    if late_claims[0]["status"]["offer_state"] != head:
        violations.append(f"the late claim's head is {late_claims[0]['status']['offer_state']}, not {head}")
    if head == "accepted" and same:
        # Binding the session again replaced its binding row, so the broker refuses the first binding's
        # follow-on status; the session's next current claim takes the offer below.
        if not (isinstance(outcome[0], relay.RelayProtocolError) and outcome[0].code == "CONTROL_INVALID"):
            violations.append(f"the superseded binding's follow-on was not refused by the broker: {outcome[0]!r}")
    elif isinstance(outcome[0], BaseException):
        violations.append(f"the first binding's reconcile failed: {outcome[0]!r}")
    late_token = late_claims[0]["status"]["offer_token"]
    if runtime.held is not None:
        violations.append(f"the first binding's late claim became the second binding's hold: {runtime.held}")
    late_messages = [params for method, params in notifications if method == relay.MESSAGE_METHOD]
    if late_messages:
        violations.append(f"the first binding's late claim was offered: {late_messages}")

    await runtime.reconcile_once()
    # An accepted head is never offered as a message.
    messages = [params for method, params in notifications if method == relay.MESSAGE_METHOD]
    offered = [(p["recipient_id"], p["binding_epoch"], p["delivery"]["event_id"]) for p in messages]
    if offered != ([] if head == "accepted" and same else [(second[0], second[1], second_event)]):
        violations.append(f"the second binding did not offer its own head {second_event}: {offered}")
    held_binding = (runtime._held_status or {}).get("binding_epoch")
    if runtime.held is None or runtime.held[0] != second_event or held_binding != second[1]:
        violations.append(f"the second binding does not hold its own head: {runtime.held}, binding {held_binding}")
    if same:
        # The offer stays on its session; the session's next current claim is served the same token.
        if runtime.held is None or runtime.held[1] != late_token:
            violations.append(f"the session's next claim was not served the late claim's token {late_token}")
    else:
        outstanding = (await client.get(f"/api/agent/{first[0]}/deliveries")).json()["deliveries"]
        if [str(row["event_id"]) for row in outstanding] != [first_event]:
            violations.append(f"the first session's offer is no longer outstanding: {outstanding}")
        reclaim = await client.post(
            f"/api/agent/{first[0]}/delivery-offer",
            json={"schema_version": 1, "binding_epoch": first[1], "omp_process_epoch": first[2]},
            headers={"Authorization": f"RelayControl {_b64(first_credential)}"},
        )
        body = reclaim.json()
        if reclaim.status_code != 200 or body.get("code") != "HEAD" or body["status"]["offer_token"] != late_token:
            violations.append(f"the first session's own claim no longer recovers its offer: {reclaim.status_code} {body}")
    await runtime.stop()
    assert violations == []


# The hold's companions. "step-notify": the rebind lands while the claimed head's message notification is
# in flight. "event-retry": it lands while _reconcile_event's status is in flight, and the retry that follows
# claims the head again. Afterwards the second binding's queue head is a different event, and the next retry
# (the prior-candidate path) must go out at once with that event's own delivery.
@pytest.mark.parametrize("rebind_kind", ["none", *REBINDS])
@pytest.mark.parametrize("late", ["step-notify", "event-retry"])
async def test_a_late_message_notification_leaves_the_next_bindings_retry_state_alone(tmp_path, late, rebind_kind):
    runtime = _runtime(tmp_path)
    identity, first = _bound(runtime)
    tokens = {"41": str(uuid4()), "42": str(uuid4())}
    head = {"event_id": "41"}
    entered = asyncio.Event()
    release = asyncio.Event()
    notifications: list[tuple[str, dict]] = []

    async def request(method, url, *, params=None, json_body=None, private=False):
        parts = url.split("/")
        session = unquote(parts[3])
        authenticated = json_body if json_body is not None else params
        if parts[4] == "delivery":
            if late == "event-retry" and not entered.is_set():
                entered.set()
                await release.wait()
            return {"schema_version": 1, "status": _offer_status(session, parts[5], tokens[parts[5]], authenticated)}
        assert parts[4] == "delivery-offer", url
        event_id = head["event_id"]
        status = _offer_status(session, event_id, tokens[event_id], authenticated)
        return {"schema_version": 1, "code": "HEAD", "status": status, "delivery": _delivery(event_id)}

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))
        if late == "step-notify" and method == relay.MESSAGE_METHOD and not entered.is_set():
            entered.set()
            await release.wait()

    runtime._request = request  # type: ignore[method-assign]
    runtime.notifier = notify
    operation = runtime.reconcile_once() if late == "step-notify" else runtime._reconcile_event("41", tokens["41"], None)
    second = first

    async def rebind() -> None:
        nonlocal second
        await entered.wait()
        if rebind_kind != "none":
            second = _rebind(runtime, identity, rebind_kind)
            runtime.relay_name = "omp.second"
        release.set()

    outcome = await asyncio.wait_for(asyncio.gather(operation, rebind(), return_exceptions=True), timeout=10)
    violations = []
    if isinstance(outcome[0], BaseException):
        violations.append(f"the first binding's operation failed: {outcome[0]!r}")
    if rebind_kind == "none":
        # Control: the binding the operation ran under is still live, so its own retry state is written.
        if runtime._next_delivery_notification <= time.monotonic():
            violations.append("the operation did not throttle its own binding's next notification")
        if late == "event-retry" and (runtime._held_delivery or {}).get("event_id") != "41":
            violations.append(f"the operation did not keep its own binding's delivery: {runtime._held_delivery}")
    else:
        head["event_id"] = "42"
        authenticated = {"binding_epoch": second[1], "omp_process_epoch": second[2]}
        before = len(notifications)
        await runtime._retry_message_notification(_offer_status(second[0], "42", tokens["42"], authenticated), None)
        sent = [(p["binding_epoch"], p["delivery"]["event_id"]) for m, p in notifications[before:] if m == relay.MESSAGE_METHOD]
        if sent != [(second[1], "42")]:
            violations.append(f"the second binding's retry was not sent at once with its own delivery: {sent}")
    await runtime.stop()
    assert violations == []


# Finding 1. The second binding is named (and can send) or unnamed; "none" is the control in which the
# naming binding is still the live one.
@pytest.mark.parametrize("live", ["named", "unnamed"])
@pytest.mark.parametrize("rebind_kind", ["none", *REBINDS])
async def test_a_late_name_result_leaves_the_next_bindings_name_health_and_sender_alone(tmp_path, rebind_kind, live):
    runtime = _runtime(tmp_path)
    identity, _ = _bound(runtime)
    state = ("omp.live", "connected", "CONNECTED") if live == "named" else (None, "unnamed", "NAME_REQUIRED")
    if rebind_kind == "none":
        runtime.relay_name, runtime.health_state, runtime.health_code = state
    entered = asyncio.Event()
    release = asyncio.Event()
    events: list[dict] = []

    async def request(method, url, *, params=None, json_body=None, private=False):
        if url.endswith("/relay-name"):
            if not entered.is_set():
                entered.set()
                await release.wait()
            return {"relay_name": json_body["relay_name"], "displaced_id": None}
        if url == "/api/event":
            events.append(json_body)
            return {"event_id": 9}
        assert url.endswith("/delivery-offer"), url  # the reconcile tool_name wakes
        return {"schema_version": 1, "code": "EMPTY", "status": None, "delivery": None}

    runtime._request = request  # type: ignore[method-assign]

    async def rebind() -> None:
        await entered.wait()
        if rebind_kind != "none":
            _rebind(runtime, identity, rebind_kind)
            runtime.relay_name, runtime.health_state, runtime.health_code = state
        release.set()

    outcome = await asyncio.wait_for(
        asyncio.gather(runtime.tool_name("omp.late"), rebind(), return_exceptions=True), timeout=10
    )
    woken = await asyncio.gather(*tuple(runtime._tasks), return_exceptions=True)
    expected = ("omp.late", "connected", "CONNECTED") if rebind_kind == "none" else state
    violations = []
    if not isinstance(outcome[0], dict) or outcome[0].get("relay_name") != "omp.late":
        violations.append(f"the name result did not reach its caller: {outcome[0]!r}")
    if [item for item in woken if isinstance(item, BaseException)]:
        violations.append(f"the reconcile tool_name woke failed: {woken}")
    if (runtime.relay_name, runtime.health_state, runtime.health_code) != expected:
        violations.append(f"name and health are {(runtime.relay_name, runtime.health_state, runtime.health_code)}, expected {expected}")
    if expected[0] is not None:
        await runtime.tool_send("peer.agent", "after the name result")
        if [event["from_agent"] for event in events] != [expected[0]]:
            violations.append(f"the next send went out as {[event['from_agent'] for event in events]}, expected {expected[0]}")
    await runtime.stop()
    assert violations == []


# Finding 2, _request's three failure arms. "register" is unpinned: its only request is in flight at the
# rebind. "reconcile-follow-on" is pinned: _reconcile_event's status is in flight at the rebind, and the CAS
# after it fails. "none" is the control: a failure under the live binding still degrades it.
@pytest.mark.parametrize("rebind_kind", ["none", *REBINDS])
@pytest.mark.parametrize("arm", ["request-error", "non-json-5xx", "non-dict-5xx"])
@pytest.mark.parametrize("operation", ["register", "reconcile-follow-on"])
async def test_a_failed_request_degrades_only_the_binding_it_was_made_under(tmp_path, operation, arm, rebind_kind):
    offer_token = str(uuid4())
    entered = asyncio.Event()
    release = asyncio.Event()

    async def broker(request: httpx.Request) -> httpx.Response:
        if not entered.is_set():
            entered.set()
            await release.wait()
            if operation == "reconcile-follow-on":
                session = unquote(request.url.path.split("/")[3])
                status = _offer_status(session, "41", offer_token, dict(request.url.params))
                return httpx.Response(200, json={"schema_version": 1, "status": status})
        if arm == "request-error":
            raise httpx.ConnectError("offline", request=request)
        if arm == "non-json-5xx":
            return httpx.Response(503, text="temporarily unavailable")
        return httpx.Response(503, json=["temporarily unavailable"])

    client = httpx.AsyncClient(transport=httpx.MockTransport(broker), base_url="http://test")
    runtime = _runtime(tmp_path, client)
    identity, first = _bound(runtime)
    if operation == "register":
        running = runtime.tool_register("running")
    else:
        relay._atomic_write_json(runtime._offer_path("41"), _mirror(first[0], first[2], "41", offer_token, state="injected"))
        running = runtime._reconcile_event("41", offer_token, None)

    async def rebind() -> None:
        await entered.wait()
        if rebind_kind != "none":
            _rebind(runtime, identity, rebind_kind)
            runtime.health_state, runtime.health_code = "connected", "CONNECTED"
        release.set()

    outcome = await asyncio.wait_for(asyncio.gather(running, rebind(), return_exceptions=True), timeout=10)
    expected = ("degraded", "BROKER_UNREACHABLE") if rebind_kind == "none" else ("connected", "CONNECTED")
    violations = []
    if not (isinstance(outcome[0], relay.RelayProtocolError) and outcome[0].code == "BROKER_UNREACHABLE"):
        violations.append(f"the failure did not reach the operation that made the request: {outcome[0]!r}")
    if (runtime.health_state, runtime.health_code) != expected:
        violations.append(f"health is {(runtime.health_state, runtime.health_code)}, expected {expected}")
    await runtime.stop()
    await client.aclose()
    assert violations == []


# Finding 2, the reconciliation loop: one pass whose step fails after the rebind.
@pytest.mark.parametrize("rebind_kind", ["none", *REBINDS])
async def test_the_reconciliation_loop_reports_a_failure_only_on_the_binding_it_ran_under(tmp_path, rebind_kind):
    runtime = _runtime(tmp_path)
    identity, _ = _bound(runtime)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(method, url, *, params=None, json_body=None, private=False):
        assert url.endswith("/delivery-offer"), url
        entered.set()
        await release.wait()
        raise relay.RelayProtocolError("CONTROL_INVALID")

    runtime._request = request  # type: ignore[method-assign]

    async def rebind() -> None:
        await entered.wait()
        if rebind_kind != "none":
            _rebind(runtime, identity, rebind_kind)
            runtime.health_state, runtime.health_code = "connected", "CONNECTED"
        runtime._stopping = True  # this pass is the last
        runtime._reconcile_wake.set()
        release.set()

    await asyncio.wait_for(asyncio.gather(runtime._reconciliation_loop(), rebind()), timeout=10)
    expected = ("degraded", "CONTROL_INVALID") if rebind_kind == "none" else ("connected", "CONNECTED")
    health = (runtime.health_state, runtime.health_code)
    await runtime.stop()
    assert health == expected


# Finding 3. The second binding has its own health (in the child and in health.json) and, for another
# session, its own rejected mirrors. A same-session rebind in the same omp process shares the mirrors, so
# there only the health differs.
@pytest.mark.parametrize("rebind_kind", ["none", *REBINDS])
async def test_check_reports_one_bindings_broker_data_mirror_counts_and_health(tmp_path, rebind_kind):
    runtime = _runtime(tmp_path)
    identity, first = _bound(runtime)
    rejected = {"state": "rejected", "broker_terminal_state": "rejected", "reject_reason_code": "USER_REJECTED"}
    relay._atomic_write_json(runtime._offer_path("61"), _mirror(first[0], first[2], "61", str(uuid4()), **rejected))
    offer_token = str(uuid4())
    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(method, url, *, params=None, json_body=None, private=False):
        parts = url.split("/")
        session = unquote(parts[3])
        if parts[4] == "status":
            entered.set()
            await release.wait()
            return {"relay_name": "omp.first"}
        if parts[4] == "deliveries":
            return {"deliveries": [{"event_id": 41, "from_agent": "peer.agent"}], "next_cursor": None}
        assert parts[4] == "delivery", url
        return {"schema_version": 1, "status": _offer_status(session, "41", offer_token, params)}

    runtime._request = request  # type: ignore[method-assign]

    async def rebind() -> None:
        await entered.wait()
        if rebind_kind != "none":
            second = _rebind(runtime, identity, rebind_kind)
            runtime.health_state, runtime.health_code = "degraded", "BROKER_UNREACHABLE"
            relay._atomic_write_json(
                runtime.rendezvous_dir / "health.json",
                {
                    "schema_version": 1, "instance_id": runtime.instance_id, "binding_epoch": second[1],
                    "state": "degraded", "code": "BROKER_UNREACHABLE", "since_ms": int(time.time() * 1000),
                    "retry_in_ms": None,
                },
            )
            if rebind_kind == "other-session":
                for event_id in ("62", "63"):
                    relay._atomic_write_json(
                        runtime._offer_path(event_id, second[0]),
                        _mirror(second[0], second[2], event_id, str(uuid4()), **rejected),
                    )
        release.set()

    outcome = await asyncio.wait_for(asyncio.gather(runtime.tool_check(), rebind(), return_exceptions=True), timeout=10)
    await runtime.stop()
    assert not isinstance(outcome[0], BaseException), repr(outcome[0])
    result = outcome[0]
    assert {
        "session_id": result["identity"]["session_id"],
        "ok": result["ok"],
        "code": result["code"],
        "health": (result["health"]["state"], result["health"]["code"]),
        "rejected_count": result["queue"]["rejected_count"],
        "deliveries": [(item["event_id"], item["recipient_id"]) for item in result["deliveries"]],
    } == {
        "session_id": first[0],
        "ok": True,
        "code": "CONNECTED",
        "health": ("connected", "CONNECTED"),
        "rejected_count": 1,
        "deliveries": [("41", first[0])],
    }


# health.json records the health of one specific child binding, not any prior binding.
# binding of this child, named by instance and binding epoch. check reads it before its first await, so a record
# already there for another binding epoch, for no binding, or for another instance -- or one that cannot be read --
# is not the health of the binding check is pinned to, and check refuses before it asks the broker anything.
def _health_record(runtime: relay.OmpRelayRuntime, binding_epoch: str | None, state: str, code: str, **fields) -> dict:
    # As omp_relay_extension.ts's publishHealth writes it.
    value = {
        "schema_version": 1, "instance_id": runtime.instance_id, "binding_epoch": binding_epoch, "state": state,
        "code": code, "since_ms": int(time.time() * 1000), "retry_in_ms": None,
    }
    value.update(fields)
    return value


@pytest.mark.parametrize("foreign", ["other-binding-epoch", "null-binding-epoch", "other-instance", "malformed"])
async def test_check_refuses_shared_health_of_another_binding_before_any_broker_request(tmp_path, foreign):
    runtime = _runtime(tmp_path)
    _, first = _bound(runtime)
    health_path = runtime.rendezvous_dir / "health.json"
    if foreign == "malformed":
        health_path.write_text("{\n", encoding="utf-8")
    else:
        relay._atomic_write_json(
            health_path,
            {
                "other-binding-epoch": _health_record(runtime, str(uuid4()), "degraded", "BROKER_UNREACHABLE"),
                "null-binding-epoch": _health_record(runtime, None, "rebinding", "REBINDING"),
                "other-instance": _health_record(runtime, first[1], "connected", "CONNECTED", instance_id=str(uuid4())),
            }[foreign],
        )
    offer_token = str(uuid4())
    asked: list[str] = []

    async def request(method, url, *, params=None, json_body=None, private=False):
        asked.append(url)
        parts = url.split("/")
        if parts[4] == "status":
            return {"relay_name": "omp.first"}
        if parts[4] == "deliveries":
            return {"deliveries": [{"event_id": 41, "from_agent": "peer.agent"}], "next_cursor": None}
        assert parts[4] == "delivery", url
        return {"schema_version": 1, "status": _offer_status(unquote(parts[3]), "41", offer_token, params)}

    runtime._request = request  # type: ignore[method-assign]
    try:
        with pytest.raises(relay.ToolError, match="BINDING_CONFLICT"):
            await runtime.tool_check()
    finally:
        await runtime.stop()
    assert asked == []


# The record for check's own binding is that binding's health: check reports it, not the child's in-memory health.
async def test_check_reports_the_shared_health_recorded_for_its_own_binding(tmp_path):
    runtime = _runtime(tmp_path)
    _, first = _bound(runtime)
    relay._atomic_write_json(
        runtime.rendezvous_dir / "health.json",
        _health_record(runtime, first[1], "blocked", "DELIVERY_PENDING", since_ms=1_780_000_000_123),
    )
    offer_token = str(uuid4())

    async def request(method, url, *, params=None, json_body=None, private=False):
        parts = url.split("/")
        if parts[4] == "status":
            return {"relay_name": "omp.first"}
        if parts[4] == "deliveries":
            return {"deliveries": [{"event_id": 41, "from_agent": "peer.agent"}], "next_cursor": None}
        assert parts[4] == "delivery", url
        return {"schema_version": 1, "status": _offer_status(unquote(parts[3]), "41", offer_token, params)}

    runtime._request = request  # type: ignore[method-assign]
    result = await runtime.tool_check()
    await runtime.stop()
    assert {
        "session_id": result["identity"]["session_id"],
        "code": result["code"],
        "health": (result["health"]["state"], result["health"]["code"], result["health"]["since_ms"]),
        "deliveries": [(item["event_id"], item["recipient_id"]) for item in result["deliveries"]],
    } == {
        "session_id": first[0],
        "code": "DELIVERY_PENDING",
        "health": ("blocked", "DELIVERY_PENDING", 1_780_000_000_123),
        "deliveries": [("41", first[0])],
    }


# The first binding's own record, read before check's first await, stays the result's health when the child binds
# again during the broker requests and the second binding's record replaces the file: check still completes.
@pytest.mark.parametrize("rebind_kind", REBINDS)
async def test_check_keeps_its_own_bindings_shared_health_when_a_rebind_follows(tmp_path, rebind_kind):
    runtime = _runtime(tmp_path)
    identity, first = _bound(runtime)
    relay._atomic_write_json(
        runtime.rendezvous_dir / "health.json",
        _health_record(runtime, first[1], "blocked", "DELIVERY_PENDING", since_ms=1_780_000_000_123),
    )
    offer_token = str(uuid4())
    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(method, url, *, params=None, json_body=None, private=False):
        parts = url.split("/")
        session = unquote(parts[3])
        if parts[4] == "status":
            entered.set()
            await release.wait()
            return {"relay_name": "omp.first"}
        if parts[4] == "deliveries":
            return {"deliveries": [{"event_id": 41, "from_agent": "peer.agent"}], "next_cursor": None}
        assert parts[4] == "delivery", url
        return {"schema_version": 1, "status": _offer_status(session, "41", offer_token, params)}

    runtime._request = request  # type: ignore[method-assign]

    async def rebind() -> None:
        await entered.wait()
        second = _rebind(runtime, identity, rebind_kind)
        runtime.health_state, runtime.health_code = "degraded", "BROKER_UNREACHABLE"
        relay._atomic_write_json(
            runtime.rendezvous_dir / "health.json",
            _health_record(runtime, second[1], "degraded", "BROKER_UNREACHABLE"),
        )
        release.set()

    outcome = await asyncio.wait_for(asyncio.gather(runtime.tool_check(), rebind(), return_exceptions=True), timeout=10)
    await runtime.stop()
    assert not isinstance(outcome[0], BaseException), repr(outcome[0])
    result = outcome[0]
    assert {
        "session_id": result["identity"]["session_id"],
        "code": result["code"],
        "health": (result["health"]["state"], result["health"]["code"], result["health"]["since_ms"]),
        "deliveries": [(item["event_id"], item["recipient_id"]) for item in result["deliveries"]],
    } == {
        "session_id": first[0],
        "code": "DELIVERY_PENDING",
        "health": ("blocked", "DELIVERY_PENDING", 1_780_000_000_123),
        "deliveries": [("41", first[0])],
    }


# The binding driver reads the same file and must still see a record naming another binding epoch: that is how
# it learns the binding it holds has ended. test_binding_driver_conflict_refresh_and_session_cutover covers a
# null epoch; this is the non-null other epoch that check refuses.
async def test_binding_driver_leaves_a_binding_whose_shared_health_names_another_epoch(client, tmp_path):
    runtime = _runtime(tmp_path, client)
    target_session = str(uuid4())
    expectations: list[str | None] = []

    def publish_authorization() -> None:
        identity, private = _identity(runtime)
        identity["session_id"] = target_session
        runtime.rendezvous_dir.joinpath("identity.json").write_text(json.dumps(identity), encoding="utf-8")
        request_path = runtime.rendezvous_dir / "control" / "requests" / f"{runtime.bind_attempt_id}.json"
        request_path.write_text(json.dumps(_authorization(runtime, identity, private)), encoding="utf-8")

    async def notify(_method: str, params: dict) -> None:
        if params.get("type") == "identity_request":
            expectations.append(params["expected_binding_epoch"])
            publish_authorization()
        elif params.get("type") == "identity_bound":
            relay._atomic_write_json(
                runtime.rendezvous_dir / "health.json",
                _health_record(runtime, params["binding_epoch"], "connected", "CONNECTED"),
            )

    runtime.notifier = notify
    publish_authorization()
    driver = asyncio.create_task(runtime._complete_identity_handshake())

    async def wait_for(predicate) -> None:
        deadline = time.monotonic() + 3.0
        while not predicate() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert predicate()

    await wait_for(lambda: runtime.bound)
    initial_epoch = runtime.binding_epoch
    initial_session = runtime.session_id
    relay._atomic_write_json(
        runtime.rendezvous_dir / "health.json",
        _health_record(runtime, str(uuid4()), "degraded", "BROKER_UNREACHABLE"),
    )
    await wait_for(lambda: runtime.bound and runtime.binding_epoch != initial_epoch)
    assert runtime.session_id == initial_session
    assert expectations[:2] == [None, initial_epoch]
    assert not driver.done()
    driver.cancel()
    await asyncio.gather(driver, return_exceptions=True)
    await runtime.stop()


# The prior-candidate scan reads mirrors after awaiting a status for each malformed one. When the rebind
# lands during that status, the scan must go on over the first binding's own mirrors, not the second's.
# Here the first session's mirror 52 is from its own omp process, so it is current for that binding.
# A same-session rebind in the same omp process reads the same directory and process epoch, so only another
# session can tell the two apart.
@pytest.mark.parametrize("rebind_kind", ["none", "other-session"])
async def test_prior_candidates_finish_on_the_mirrors_of_the_binding_they_started_under(tmp_path, rebind_kind):
    runtime = _runtime(tmp_path)
    identity, first = _bound(runtime)
    tokens = {"51": str(uuid4()), "52": str(uuid4()), "53": str(uuid4())}
    runtime._offer_path("51").parent.mkdir(parents=True, exist_ok=True)
    runtime._offer_path("51").write_text("{\n", encoding="utf-8")
    relay._atomic_write_json(runtime._offer_path("52"), _mirror(first[0], first[2], "52", tokens["52"]))
    entered = asyncio.Event()
    release = asyncio.Event()
    asked: list[tuple[str, str]] = []
    notifications: list[tuple[str, dict]] = []

    async def request(method, url, *, params=None, json_body=None, private=False):
        parts = url.split("/")
        session = unquote(parts[3])
        authenticated = json_body if json_body is not None else params
        event_id = parts[5] if parts[4] == "delivery" else "52"
        asked.append((session, event_id))
        if session != first[0] or event_id == "53":
            raise relay.RelayProtocolError("DELIVERY_NOT_FOUND")
        if event_id == "51" and not entered.is_set():
            entered.set()
            await release.wait()
        status = _offer_status(session, event_id, tokens[event_id], authenticated)
        if parts[4] == "delivery-offer":
            return {"schema_version": 1, "code": "HEAD", "status": status, "delivery": _delivery(event_id)}
        return {"schema_version": 1, "status": status}

    async def notify(method: str, params: dict) -> None:
        notifications.append((method, params))

    runtime._request = request  # type: ignore[method-assign]
    runtime.notifier = notify

    async def rebind() -> None:
        await entered.wait()
        if rebind_kind != "none":
            second = _rebind(runtime, identity, rebind_kind)
            relay._atomic_write_json(runtime._offer_path("53", second[0]), _mirror(second[0], second[2], "53", tokens["53"]))
        release.set()

    outcome = await asyncio.wait_for(
        asyncio.gather(runtime._reconcile_prior_candidates(None), rebind(), return_exceptions=True), timeout=10
    )
    violations = []
    if outcome[0] is not True:
        violations.append(f"the scan did not finish on a candidate: {outcome[0]!r}")
    if [item for item in asked if item != (first[0], "51") and item != (first[0], "52")]:
        violations.append(f"the scan asked about another binding's offers: {asked}")
    sent = [
        (method == relay.MESSAGE_METHOD, params.get("recipient_id"), params.get("binding_epoch"), (params.get("delivery") or {}).get("event_id"))
        for method, params in notifications
    ]
    if sent != [(True, first[0], first[1], "52")]:
        violations.append(f"expected one message for the first binding's own mirror 52, got {sent}")
    await runtime.stop()
    assert violations == []
