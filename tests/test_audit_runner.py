"""M4 Pattern 4 — audit-snapshot background runner tests.

Covers the periodic asyncio background task added in M4:
  - emits closed-window snapshots to BROKER_AUDIT_DIR/{window_ms}.json
  - skips emit when file already exists (idempotent)
  - emits empty windows (file existence = "broker checked")
  - one-shot startup backfill of the just-completed hour
  - never snapshots the current open hour
  - graceful shutdown from lifespan
  - manual POST endpoint still works after helper extraction
  - manual POST is non-idempotent (always overwrites)
  - window math floors correctly to the just-completed hour boundary

Fixture conventions mirror tests/test_admin_and_auth.py: tmp_path-isolated
BROKER_AUDIT_DIR via monkeypatch.setenv; lifespan invoked directly via
`async with server.lifespan(server.app)` for runner-side tests; `client`
fixture from conftest.py for HTTP-side tests.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

pytestmark = pytest.mark.asyncio


# --------------------------------------------------------------------- helpers


async def _seed_event(server, ts_ms: int, content: str = "evt",
                      from_agent: str = "a1", channel: str = "#x",
                      kind: str = "user_msg") -> int:
    """Insert one row into message_log directly. Returns event_id."""
    async with server.connect_db() as conn:
        cur = await conn.execute(
            "INSERT INTO message_log "
            "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
            "VALUES (NULL, ?, NULL, ?, NULL, ?, ?, ?)",
            (from_agent, channel, kind, content, ts_ms),
        )
        await conn.commit()
        eid = cur.lastrowid
        await cur.close()
    return eid


def _expected_window_ms_now(server) -> int:
    """The just-completed hour's window_ms for the live clock — same formula
    the runner uses. Tests sample this immediately before runner sleeps so the
    hour boundary doesn't shift mid-test (1h boundary; safe in practice)."""
    return ((server.now_ms() // 1000 - 3600) // 3600) * 3600 * 1000


async def _poll_until(predicate, timeout: float = 2.0, interval: float = 0.05) -> bool:
    """Poll `predicate()` until truthy or `timeout` seconds elapse.

    Returns True when predicate becomes truthy mid-poll. On timeout, performs
    one final check of `predicate()` and returns its result — this preserves
    the prior fixed-sleep + single-shot-assert semantics where a transition
    between last poll and deadline still resolves correctly. Uses
    `asyncio.get_running_loop().time()` for monotonic deadline tracking.
    The interval=0.05s default matches the prior inline poll-loop in test 8.
    The timeout=2.0s default is comfortably above the prior fixed-sleep
    durations (0.15-0.4s) so slow CI machines have headroom while fast
    machines exit early.

    `predicate` is a zero-arg callable (sync). For Path.exists checks
    pass `f.exists` directly. For more complex checks wrap in a lambda.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()  # one final check after the deadline


@pytest_asyncio.fixture
async def isolated_env(tmp_path, monkeypatch):
    """Fresh DB + fresh audit dir + loopback bind (no auth). Returns
    (server_module, audit_dir: Path)."""
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    audit_dir = tmp_path / "audit-runs"
    monkeypatch.setenv("BROKER_AUDIT_DIR", str(audit_dir))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_ADMIN_ENABLED", raising=False)
    from broker import server
    await server.ensure_schema()
    return server, audit_dir


# --------------------------------------------------------------------- 1


async def test_audit_runner_emits_file_for_closed_window(isolated_env, monkeypatch):
    """Backfill path: 2 events in just-closed hour → file with event_count=2."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.1")
    window_ms = _expected_window_ms_now(server)

    await _seed_event(server, window_ms + 1, content="first")
    await _seed_event(server, window_ms + 2, content="second")

    async with server.lifespan(server.app):
        # Backfill is synchronous to startup — but mkdir + emit happen inside
        # the task's first event-loop turn. Poll until the file appears.
        f = audit_dir / f"{window_ms}.json"
        assert await _poll_until(f.exists, timeout=2.0), (
            f"expected {f} to exist after backfill"
        )
        data = json.loads(f.read_text(encoding="utf-8"))
        assert data["snapshot_window_start_ts"] == window_ms
        assert data["event_count"] == 2
        contents = {e["content"] for e in data["events"]}
        assert contents == {"first", "second"}


# --------------------------------------------------------------------- 2


async def test_audit_runner_emits_empty_window(isolated_env, monkeypatch):
    """No events in window → file still emitted with event_count=0."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.1")
    window_ms = _expected_window_ms_now(server)

    async with server.lifespan(server.app):
        f = audit_dir / f"{window_ms}.json"
        assert await _poll_until(f.exists, timeout=2.0)
        data = json.loads(f.read_text(encoding="utf-8"))
        assert data["snapshot_window_start_ts"] == window_ms
        assert data["event_count"] == 0
        assert data["events"] == []


# --------------------------------------------------------------------- 3


async def test_audit_runner_idempotent_skip_when_file_exists(isolated_env, monkeypatch):
    """Pre-create {window_ms}.json with sentinel; runner must NOT overwrite."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.1")
    window_ms = _expected_window_ms_now(server)

    audit_dir.mkdir(parents=True, exist_ok=True)
    sentinel = {"sentinel": True, "marker": "must-not-overwrite"}
    (audit_dir / f"{window_ms}.json").write_text(json.dumps(sentinel), encoding="utf-8")

    # Seed events that WOULD land in this window if the runner emitted.
    await _seed_event(server, window_ms + 1, content="should-not-appear")

    async with server.lifespan(server.app):
        # Wait for backfill + at least one tick of the loop.
        # Negative assertion (file must NOT be overwritten); fixed wait is
        # correct here — there's no positive predicate to poll for.
        await asyncio.sleep(0.3)
        data = json.loads((audit_dir / f"{window_ms}.json").read_text(encoding="utf-8"))
        assert data == sentinel, "runner overwrote a pre-existing snapshot file"


# --------------------------------------------------------------------- 4


async def test_audit_runner_startup_backfill_emits_last_closed_hour(isolated_env, monkeypatch):
    """Fresh start with no file → backfill produces a file BEFORE the first
    sleep elapses. We use a long interval (10s) so any emission inside the
    first 0.3s of lifespan is necessarily from backfill, not the loop."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "10")
    window_ms = _expected_window_ms_now(server)

    async with server.lifespan(server.app):
        f = audit_dir / f"{window_ms}.json"
        assert await _poll_until(f.exists, timeout=2.0), (
            "backfill did not emit before first sleep tick — startup backfill "
            "is supposed to be synchronous to task entry"
        )


# --------------------------------------------------------------------- 5


async def test_audit_runner_startup_backfill_skips_if_file_exists(isolated_env, monkeypatch):
    """Pre-create the just-closed-hour file; backfill must NOT overwrite."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "10")  # loop won't fire
    window_ms = _expected_window_ms_now(server)

    audit_dir.mkdir(parents=True, exist_ok=True)
    sentinel = {"sentinel": True, "phase": "backfill-skip"}
    (audit_dir / f"{window_ms}.json").write_text(json.dumps(sentinel), encoding="utf-8")

    async with server.lifespan(server.app):
        # Negative assertion (file must NOT be overwritten); fixed wait is
        # correct here — there's no positive predicate to poll for.
        await asyncio.sleep(0.3)
        data = json.loads((audit_dir / f"{window_ms}.json").read_text(encoding="utf-8"))
        assert data == sentinel


# --------------------------------------------------------------------- 6


async def test_audit_runner_does_not_snapshot_current_open_hour(isolated_env, monkeypatch):
    """Insert event with ts in the CURRENT (open) hour. Runner must NOT write
    a file for the current hour's window_ms — only the previous closed hour."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.1")

    closed_window_ms = _expected_window_ms_now(server)
    current_open_window_ms = closed_window_ms + 3_600_000

    # Event in the current incomplete hour.
    await _seed_event(server, server.now_ms(), content="in-open-hour")

    async with server.lifespan(server.app):
        closed_file = audit_dir / f"{closed_window_ms}.json"
        open_file = audit_dir / f"{current_open_window_ms}.json"
        # Step 1: confirm backfill emitted closed_file (synchronous to task entry).
        assert await _poll_until(closed_file.exists, timeout=2.0), (
            "closed-window file did not emit (backfill)"
        )
        # Step 2: delete closed_file and confirm a periodic-loop tick re-emits it.
        # This proves the loop fired at least once. Without this, the negative
        # assertion below would only prove the BACKFILL path doesn't snapshot the
        # open hour — not the periodic path the test name implies.
        closed_file.unlink()
        assert await _poll_until(closed_file.exists, timeout=2.0), (
            "closed-window file did not re-emit on periodic tick"
        )
        # Step 3: now that a confirmed periodic tick has run, the absence assertion
        # is meaningful: a buggy runner that snapshotted the open hour during a
        # tick would have written open_file by now.
        assert not open_file.exists(), (
            "runner snapshotted the current incomplete hour — must only emit closed windows"
        )


# --------------------------------------------------------------------- 7


async def test_audit_runner_uses_env_var_for_audit_dir(tmp_path, monkeypatch):
    """Custom BROKER_AUDIT_DIR must be respected by the runner."""
    custom_dir = tmp_path / "deep" / "nested" / "audit-target"
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    monkeypatch.setenv("BROKER_AUDIT_DIR", str(custom_dir))
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.1")
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_ADMIN_ENABLED", raising=False)

    from broker import server
    await server.ensure_schema()
    window_ms = _expected_window_ms_now(server)

    async with server.lifespan(server.app):
        f = custom_dir / f"{window_ms}.json"
        assert await _poll_until(f.exists, timeout=2.0), (
            f"runner did not honor BROKER_AUDIT_DIR; expected file at {f}"
        )


# --------------------------------------------------------------------- 8


async def test_audit_runner_uses_env_var_for_interval(isolated_env, monkeypatch, caplog):
    """Set tiny interval; verify the runner wakes up multiple times.

    Pragmatic: pre-create the closed-hour file (so backfill skips), then watch
    the loop. With BROKER_AUDIT_INTERVAL_SEC=0.05 over 0.3s, the loop should
    re-evaluate at least 3-4 times. We verify by clearing the file mid-run
    once and watching it get re-created — the second emit is the proof of a
    second tick after backfill."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.05")
    window_ms = _expected_window_ms_now(server)

    async with server.lifespan(server.app):
        # First file from backfill
        first = audit_dir / f"{window_ms}.json"
        assert await _poll_until(first.exists, timeout=1.0), "backfill emit missing"

        # Delete and watch the loop re-emit on a subsequent tick.
        first.unlink()
        # With interval=0.05, give the loop multiple intervals to re-fire.
        assert await _poll_until(first.exists, timeout=1.0), (
            "loop did not re-emit after file deletion — runner is not ticking"
        )


# --------------------------------------------------------------------- 9


async def test_audit_runner_graceful_shutdown(isolated_env, monkeypatch):
    """Lifespan exit must cancel + await the audit task cleanly AND clear the
    app.state mirror. Capture the live Task handle inside the lifespan block so
    the post-exit `.done()` check is load-bearing (the mirror is cleared to None
    in the lifespan finally, so `app.state.audit_task is None` would trivially
    pass without the captured-handle check).

    Asyncio coverage caveat: `.done()` validates *eventual* completion, not the
    *explicit* `await audit_task` step. Because subsequent shutdown steps
    (`await ttl_task`, discord_poller stop, drain_subscribers) yield to the
    loop, a cancelled audit task will reach `done()` even if the explicit await
    were dropped. Catching that specific regression class would require probing
    internals (e.g., monkeypatching `_audit_snapshot_runner`); we accept the
    limitation rather than couple the test to implementation details."""
    server, _audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "10")  # long sleep so it's parked

    async with server.lifespan(server.app):
        # Sanity: task spawned
        assert server.app.state.audit_task is not None
        assert not server.app.state.audit_task.done()
        captured_task = server.app.state.audit_task  # retain handle for post-exit check

    # After lifespan exit:
    # (a) the Task we captured must be done (cancelled-and-awaited counts as done).
    assert captured_task.done(), "audit task not awaited cleanly after lifespan exit"
    # (b) the app.state mirror must be cleared.
    assert server.app.state.audit_task is None, "lifespan finally did not clear app.state mirror"


# --------------------------------------------------------------------- 10


async def test_post_admin_audit_snapshot_endpoint_still_works_after_refactor(
    client, monkeypatch, tmp_path
):
    """Smoke that the manual POST endpoint preserves the full response shape
    `{output_path, event_count, snapshot_window_start_ts}` after the helper
    extraction."""
    monkeypatch.setenv("BROKER_ADMIN_ENABLED", "true")
    monkeypatch.setenv("BROKER_AUDIT_DIR", str(tmp_path / "audit-shape"))

    # Seed an event so event_count is non-trivial.
    await client.post("/api/event", json={
        "from_agent": "a1", "channel": "#x", "kind": "user_msg",
        "content": "shape-check", "ts": 1746400000000,
    })
    r = await client.post("/api/admin/audit-snapshot",
                          json={"snapshot_window_start_ts": 1746400000000})
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"output_path", "event_count", "snapshot_window_start_ts"}
    assert isinstance(body["output_path"], str)
    assert isinstance(body["event_count"], int)
    assert isinstance(body["snapshot_window_start_ts"], int)
    assert Path(body["output_path"]).exists()


# --------------------------------------------------------------------- 11


async def test_post_admin_audit_snapshot_overwrites_existing_file(
    client, monkeypatch, tmp_path
):
    """Manual POST is NON-idempotent by design — must overwrite a pre-existing
    snapshot file for the targeted window. Only the runner is idempotent."""
    audit_dir = tmp_path / "audit-overwrite"
    monkeypatch.setenv("BROKER_ADMIN_ENABLED", "true")
    monkeypatch.setenv("BROKER_AUDIT_DIR", str(audit_dir))
    audit_dir.mkdir(parents=True, exist_ok=True)

    target_window_ms = 1746400000000
    sentinel = {"sentinel": True, "scope": "manual-overwrite"}
    sentinel_path = audit_dir / f"{target_window_ms}.json"
    sentinel_path.write_text(json.dumps(sentinel), encoding="utf-8")

    # Seed an event in the targeted window.
    await client.post("/api/event", json={
        "from_agent": "a1", "channel": "#x", "kind": "user_msg",
        "content": "real-snapshot-content", "ts": target_window_ms + 1,
    })

    r = await client.post("/api/admin/audit-snapshot",
                          json={"snapshot_window_start_ts": target_window_ms})
    assert r.status_code == 200

    # Sentinel must be gone — replaced with the real snapshot.
    new = json.loads(sentinel_path.read_text(encoding="utf-8"))
    assert "sentinel" not in new, "manual POST did not overwrite — non-idempotent contract broken"
    assert new["snapshot_window_start_ts"] == target_window_ms
    assert new["event_count"] == 1
    assert new["events"][0]["content"] == "real-snapshot-content"


# --------------------------------------------------------------------- 12


async def test_audit_runner_window_math_floors_correctly(monkeypatch):
    """Window math: ((now_ms // 1000 - 3600) // 3600) * 3600 * 1000.
    Mock now_ms() to a known value mid-hour and verify the closed-window
    boundary the runner would compute matches expectation."""
    from broker import server

    # Pick a known instant: 2026-05-05 14:37:42 UTC = epoch 1778078262 sec.
    # Just-closed hour started at 13:00 UTC = epoch 1778072400 sec.
    fixed_now_ms = 1778078262_000
    expected_window_ms = 1778072400_000

    monkeypatch.setattr(server, "now_ms", lambda: fixed_now_ms)
    assert server._closed_hour_window_ms() == expected_window_ms

    # And on an hour boundary itself: 14:00 UTC exactly should yield 13:00.
    fixed_now_ms_2 = 1778076000_000  # 14:00:00 UTC
    expected_2 = 1778072400_000      # 13:00:00 UTC
    monkeypatch.setattr(server, "now_ms", lambda: fixed_now_ms_2)
    assert server._closed_hour_window_ms() == expected_2

    # And one millisecond before a boundary: 13:59:59.999 UTC should still
    # close to the 12:00 window (the 13:00 hour is the CURRENT incomplete one).
    fixed_now_ms_3 = 1778075999_999
    expected_3 = 1778068800_000  # 12:00:00 UTC
    monkeypatch.setattr(server, "now_ms", lambda: fixed_now_ms_3)
    assert server._closed_hour_window_ms() == expected_3


# --------------------------------------------------------------------- 13 (M-3 fix)


async def test_audit_runner_falls_back_to_default_on_invalid_interval(
    isolated_env, monkeypatch, caplog
):
    """M-3 fix: invalid BROKER_AUDIT_INTERVAL_SEC must NOT crash the runner.

    Set the env var to a non-numeric string; assert (a) lifespan startup
    succeeds (no crash), (b) a WARNING log is captured naming the bogus
    value, (c) backfill still emits the closed-window file (proving the
    runner stayed alive on the default 3600s interval). Note: the loop
    won't tick within our short observation window when the fallback is
    3600s — that's the point. Backfill is enough to prove liveness.

    The broker logger has `propagate=False` (server.py L80) so caplog's
    root handler doesn't see records. We attach caplog.handler directly
    to the named logger to capture WARNINGs from this module."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "not-a-number")
    window_ms = _expected_window_ms_now(server)

    broker_logger = logging.getLogger("broker")
    caplog.set_level(logging.WARNING, logger="broker")
    broker_logger.addHandler(caplog.handler)
    try:
        async with server.lifespan(server.app):
            # Backfill happens synchronously to runner task entry; poll until file appears.
            f = audit_dir / f"{window_ms}.json"
            assert await _poll_until(f.exists, timeout=2.0), (
                "backfill did not emit the closed-window file — runner did not "
                "fall back to the default interval (or crashed on the bad env var)"
            )
    finally:
        broker_logger.removeHandler(caplog.handler)

    # WARNING log must mention the bogus value AND the env var name.
    matching = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and "BROKER_AUDIT_INTERVAL_SEC" in r.getMessage()
        and "not-a-number" in r.getMessage()
    ]
    assert matching, (
        f"expected a WARNING log naming BROKER_AUDIT_INTERVAL_SEC and the bogus "
        f"value 'not-a-number'; captured records: "
        f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
    )


# --------------------------------------------------------------------- 14 (L-1 fix)


async def test_closed_hour_window_ms_clamps_negative_to_zero(monkeypatch):
    """L-1 fix: when wall-clock is before epoch+3600s the raw formula
    `((now_ms // 1000 - 3600) // 3600) * 3600 * 1000` goes negative.
    `_closed_hour_window_ms()` must clamp to 0 (not return a negative
    number, not raise). Pure unit test — no broker startup required.
    (Declared async only to satisfy the module-level asyncio mark; no
    awaits are needed inside.)"""
    from broker import server

    # 100s after epoch (well below the 3600s threshold). Raw formula gives:
    #   ((100 - 3600) // 3600) * 3600 * 1000 == (-1) * 3_600_000 == -3_600_000.
    # Clamp must promote that to 0.
    monkeypatch.setattr(server, "now_ms", lambda: 100_000)
    assert server._closed_hour_window_ms() == 0, (
        "L-1 clamp missing: helper returned a negative window for a sub-3600s clock"
    )

    # Exactly at the boundary (now == 3600s): raw == 0, clamp is a no-op.
    monkeypatch.setattr(server, "now_ms", lambda: 3_600_000)
    assert server._closed_hour_window_ms() == 0

    # One ms before the boundary: raw == -3_600_000, clamp must return 0.
    monkeypatch.setattr(server, "now_ms", lambda: 3_599_999)
    assert server._closed_hour_window_ms() == 0

    # Zero (epoch): raw is negative, clamp returns 0.
    monkeypatch.setattr(server, "now_ms", lambda: 0)
    assert server._closed_hour_window_ms() == 0


# --------------------------------------------------------------------- 15 (M-1 fix)


async def test_audit_runner_does_not_overwrite_when_manual_post_creates_file_first(
    isolated_env, monkeypatch
):
    """M-1 atomic-write contract: pre-create the closed-window file with
    sentinel content (simulating the manual POST endpoint having raced ahead
    of the runner). With BROKER_AUDIT_INTERVAL_SEC=0.1 and a 0.3s observation
    window the runner gets multiple chances to fire — backfill plus loop
    iterations. The runner's atomic O_EXCL write must FAIL on the existing
    file and return None; the sentinel content MUST still be present at the
    end. This proves the OS-level create-or-fail check (M-1 fix) prevents
    overwrite without a TOCTOU-racy `Path.exists()` pre-check."""
    server, audit_dir = isolated_env
    monkeypatch.setenv("BROKER_AUDIT_INTERVAL_SEC", "0.1")
    window_ms = _expected_window_ms_now(server)

    audit_dir.mkdir(parents=True, exist_ok=True)
    sentinel = {"sentinel": "manual_post"}
    sentinel_path = audit_dir / f"{window_ms}.json"
    sentinel_path.write_text(json.dumps(sentinel), encoding="utf-8")

    # Seed events that WOULD land in this window if the runner had been first.
    await _seed_event(server, window_ms + 1, content="runner-must-not-write-this")
    await _seed_event(server, window_ms + 2, content="or-this")

    async with server.lifespan(server.app):
        # Long enough for backfill + at least 2 loop ticks (interval=0.1s).
        # Negative assertion (M-1 O_EXCL atomic write must FAIL on existing file);
        # fixed wait is correct here — there's no positive predicate to poll for.
        await asyncio.sleep(0.3)
        data = json.loads(sentinel_path.read_text(encoding="utf-8"))
        assert data == sentinel, (
            "runner overwrote a pre-existing snapshot file — O_EXCL atomic "
            "create-or-fail contract broken (M-1 fix regressed)"
        )
