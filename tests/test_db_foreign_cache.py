"""F-CC36 follow-up — unit tests for the cached foreign-DB connection helper.

`connect_foreign_cached` caches ONE long-lived aiosqlite connection per path so the
per-project pipelines_view refresh stops spawning a fresh OS worker thread on every
debounced refresh (the un-timeout-able Thread.start() that wedged the loop under
memory pressure). These tests exercise the helper, the `_foreign_conns` cache dict,
and `close_foreign_conns` directly against a temp sqlite file — no lifespan, no HTTP.

Style mirrors tests/test_db_pool.py: module-level `pytestmark = pytest.mark.asyncio`,
`import server`, tmp_path-isolated DB path.
"""

from __future__ import annotations

import aiosqlite
import pytest
import pytest_asyncio

from broker import server

pytestmark = pytest.mark.asyncio


@pytest.fixture
def foreign_db(tmp_path):
    """A temp sqlite file with a tiny table so SELECTs work, plus guaranteed cleanup.

    Resets the module-level `_foreign_conns` cache before the test and tears down
    every cached connection after, so cached conns never leak across tests.
    """
    import sqlite3

    db = tmp_path / "foreign.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t (id, v) VALUES (1, 'hello')")
    conn.commit()
    conn.close()

    # Start from a clean cache so a leak from a prior test can't mask a bug here.
    server._foreign_conns.clear()
    yield str(db)


@pytest_asyncio.fixture(autouse=True)
async def _teardown_foreign_conns():
    """Always close + clear cached foreign conns after each test (no leak)."""
    yield
    await server.close_foreign_conns()


async def test_second_acquire_reuses_same_connection(foreign_db):
    """First acquire opens + caches; the second acquire yields the SAME object."""
    key = server._foreign_key(foreign_db)
    async with server.connect_foreign_cached(foreign_db) as conn1:
        # Cached on first open, keyed by the canonical path key.
        assert key in server._foreign_conns
        assert server._foreign_conns[key] is conn1
        first_id = id(conn1)
        cur = await conn1.execute("SELECT v FROM t WHERE id = 1")
        row = await cur.fetchone()
        await cur.close()
        assert tuple(row) == ("hello",)

    # After the first block the connection remains cached (not reopened next time).
    assert key in server._foreign_conns

    async with server.connect_foreign_cached(foreign_db) as conn2:
        # SAME connection object reused — not reopened.
        assert conn2 is conn1
        assert id(conn2) == first_id


async def test_connection_remains_cached_and_usable_after_block(foreign_db):
    """After a normal `async with` exit the conn stays cached AND still queries."""
    key = server._foreign_key(foreign_db)
    async with server.connect_foreign_cached(foreign_db) as conn:
        cached = conn

    # Still cached after the block.
    assert server._foreign_conns.get(key) is cached

    # Re-enter and run another query against the still-live cached connection.
    async with server.connect_foreign_cached(foreign_db) as conn2:
        assert conn2 is cached
        cur = await conn2.execute("SELECT COUNT(*) FROM t")
        row = await cur.fetchone()
        await cur.close()
        assert tuple(row) == (1,)


async def test_app_exception_does_not_evict_healthy_connection(foreign_db):
    """Raising an app-level exception inside the block must NOT evict a healthy
    connection: the finally-rollback succeeds, so the conn stays cached + reusable."""
    key = server._foreign_key(foreign_db)
    async with server.connect_foreign_cached(foreign_db) as conn:
        healthy = conn

    with pytest.raises(ValueError):
        async with server.connect_foreign_cached(foreign_db) as conn2:
            assert conn2 is healthy
            raise ValueError("app-level failure")

    # Healthy connection survives the app exception — still cached and the SAME object.
    assert server._foreign_conns.get(key) is healthy
    async with server.connect_foreign_cached(foreign_db) as conn3:
        assert conn3 is healthy
        cur = await conn3.execute("SELECT 1")
        row = await cur.fetchone()
        await cur.close()
        assert tuple(row) == (1,)


async def test_broken_connection_is_evicted(foreign_db):
    """A broken connection (rollback fails on the finally path) is closed + evicted
    so the path is removed from `_foreign_conns` and the next call reopens fresh."""
    key = server._foreign_key(foreign_db)
    # Prime the cache with a real connection.
    async with server.connect_foreign_cached(foreign_db) as conn:
        broken = conn
    assert server._foreign_conns.get(key) is broken

    # Force the return-path rollback to raise so _foreign_cached treats the conn as
    # broken: evict from the cache + close it.
    async def _boom():
        raise RuntimeError("rollback failed -> conn is broken")

    closed = {"v": False}
    real_close = broken.close

    async def _track_close():
        closed["v"] = True
        await real_close()

    async with server.connect_foreign_cached(foreign_db) as conn2:
        assert conn2 is broken
        conn2.rollback = _boom  # type: ignore[method-assign]
        conn2.close = _track_close  # type: ignore[method-assign]
        # Exit normally: finally-rollback raises -> evict + close.

    # The broken conn was evicted from the cache and closed.
    assert key not in server._foreign_conns
    assert closed["v"] is True

    # The next call reopens a FRESH connection (different object) and works.
    async with server.connect_foreign_cached(foreign_db) as conn3:
        assert conn3 is not broken
        assert server._foreign_conns.get(key) is conn3
        cur = await conn3.execute("SELECT v FROM t WHERE id = 1")
        row = await cur.fetchone()
        await cur.close()
        assert tuple(row) == ("hello",)


async def test_close_foreign_conns_closes_all_and_clears(foreign_db, tmp_path):
    """close_foreign_conns() closes every cached connection and empties the dict."""
    import sqlite3

    # Second foreign DB so the cache holds more than one entry.
    db2 = tmp_path / "foreign2.db"
    c = sqlite3.connect(str(db2))
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    c.commit()
    c.close()

    async with server.connect_foreign_cached(foreign_db) as conn_a:
        pass
    async with server.connect_foreign_cached(str(db2)) as conn_b:
        pass

    assert set(server._foreign_conns) == {
        server._foreign_key(foreign_db),
        server._foreign_key(str(db2)),
    }

    await server.close_foreign_conns()

    # Dict cleared and every connection actually closed (worker thread reclaimed).
    assert server._foreign_conns == {}
    for conn in (conn_a, conn_b):
        assert conn._connection is None
        assert conn._running is False


async def test_open_failure_does_not_cache(tmp_path, monkeypatch):
    """If _apply_pragmas raises after aiosqlite.connect spawned the worker thread,
    the helper closes that conn before re-raising and nothing is cached (no leak)."""
    db = tmp_path / "fresh.db"

    captured = {}
    real_connect = aiosqlite.connect

    def _tracking_connect(*args, **kwargs):
        cm = real_connect(*args, **kwargs)

        async def _await_and_capture():
            conn = await cm
            captured["conn"] = conn
            return conn

        return _await_and_capture()

    monkeypatch.setattr(server.aiosqlite, "connect", _tracking_connect)

    async def _boom_pragmas(_conn):
        raise RuntimeError("pragma application blew up")

    monkeypatch.setattr(server, "_apply_pragmas", _boom_pragmas)

    with pytest.raises(RuntimeError, match="pragma application blew up"):
        async with server.connect_foreign_cached(str(db)):
            pass  # pragma: no cover — open must fail before the body runs

    # Nothing cached (under either path spelling), and the connection that connect()
    # produced was closed.
    assert str(db) not in server._foreign_conns
    assert server._foreign_key(str(db)) not in server._foreign_conns
    leaked = captured["conn"]
    assert leaked._connection is None
    assert leaked._running is False


async def test_canonical_key_collapses_alternate_spellings(foreign_db):
    """Two different spellings of the SAME file (here a case-variant the OS's norm_path
    collapses) yield the SAME cached connection + exactly ONE cache entry — proves the
    cache is keyed by the canonical path, not the raw caller string. (Discriminator HIGH:
    state_db_path is not UNIQUE in the registry, so distinct project rows can hand in
    differently-spelled paths that resolve to one file.)"""
    # A differently-spelled path that norm_path collapses onto the same canonical key.
    alt = foreign_db.lower()
    # Guard the premise: on this OS the two strings differ but canonicalize identically.
    # (If a platform ever made them identical the test is vacuous, not wrong — skip it.)
    if alt == foreign_db or server._foreign_key(alt) != server._foreign_key(foreign_db):
        pytest.skip("OS does not collapse this spelling pair; canonical-key test N/A here")

    async with server.connect_foreign_cached(foreign_db) as conn1:
        first = conn1
        assert len(server._foreign_conns) == 1

    # Acquire via the ALTERNATE spelling: must reuse the SAME connection, not open a new one.
    async with server.connect_foreign_cached(alt) as conn2:
        assert conn2 is first
        # Still exactly one cached entry (canonical keying, no duplicate under `alt`).
        assert len(server._foreign_conns) == 1
        assert server._foreign_conns[server._foreign_key(foreign_db)] is first


async def test_concurrent_acquire_opens_only_one_connection(foreign_db):
    """TWO concurrent acquires of the same path each run a SELECT; the per-path lock
    serializes them so only ONE connection is opened (no double-open / no concurrent use
    of a single-worker-thread aiosqlite connection)."""
    import asyncio

    key = server._foreign_key(foreign_db)
    open_count = {"n": 0}
    real_connect = server.aiosqlite.connect

    def _counting_connect(*args, **kwargs):
        open_count["n"] += 1
        return real_connect(*args, **kwargs)

    # Count how many real connections get opened across the concurrent acquires.
    server.aiosqlite.connect = _counting_connect  # type: ignore[assignment]
    try:
        async def _worker():
            async with server.connect_foreign_cached(foreign_db) as conn:
                cur = await conn.execute("SELECT v FROM t WHERE id = 1")
                row = await cur.fetchone()
                await cur.close()
                return tuple(row)

        results = await asyncio.gather(_worker(), _worker())
    finally:
        server.aiosqlite.connect = real_connect  # type: ignore[assignment]

    # Both coroutines completed successfully against the shared cached connection...
    assert results == [("hello",), ("hello",)]
    # ...and the per-path lock meant only ONE connection was ever opened (no double-open).
    assert open_count["n"] == 1
    # Exactly one cached entry at rest, under the canonical key.
    assert len(server._foreign_conns) == 1
    assert key in server._foreign_conns


async def test_evict_foreign_conn_closes_and_allows_reopen(foreign_db):
    """evict_foreign_conn(path) closes + drops the cached connection; a subsequent
    connect_foreign_cached(path) reopens a FRESH one. Evicting a never-cached path is a
    no-op (no error)."""
    key = server._foreign_key(foreign_db)

    # Prime the cache.
    async with server.connect_foreign_cached(foreign_db) as conn:
        cached = conn
    assert server._foreign_conns.get(key) is cached

    # Evict: removed from the cache AND the underlying connection is closed.
    await server.evict_foreign_conn(foreign_db)
    assert key not in server._foreign_conns
    assert cached._connection is None
    assert cached._running is False

    # A subsequent acquire reopens a FRESH connection (different object) that still works.
    async with server.connect_foreign_cached(foreign_db) as conn2:
        assert conn2 is not cached
        assert server._foreign_conns.get(key) is conn2
        cur = await conn2.execute("SELECT v FROM t WHERE id = 1")
        row = await cur.fetchone()
        await cur.close()
        assert tuple(row) == ("hello",)


async def test_evict_foreign_conn_never_cached_is_noop(tmp_path):
    """evict_foreign_conn on a path that was never cached (no lock, no conn) is a no-op:
    no error, nothing added to the cache."""
    never = str(tmp_path / "never-opened.db")
    before = dict(server._foreign_conns)
    # Must not raise even though the path has no lock and no cached connection.
    await server.evict_foreign_conn(never)
    assert server._foreign_key(never) not in server._foreign_conns
    assert server._foreign_conns == before


async def test_close_foreign_conns_also_clears_locks(foreign_db):
    """close_foreign_conns() clears the per-path lock dict too, not just the conn cache —
    so a re-entered lifespan (tests) starts from a clean slate."""
    async with server.connect_foreign_cached(foreign_db) as conn:
        pass
    # Opening created both a cached conn and a per-path lock.
    assert server._foreign_conns
    assert server._foreign_conn_locks

    await server.close_foreign_conns()

    # Both dicts are empty after teardown.
    assert server._foreign_conns == {}
    assert server._foreign_conn_locks == {}


@pytest_asyncio.fixture
async def reset_refresh_tasks():
    """Reset the debounced-refresh globals before the test and guarantee NO refresh task
    leaks across tests: cancel every live task (current + orphaned) and clear all three
    module globals on teardown — even if an assert fails."""
    import asyncio

    server._refresh_tasks.clear()
    server._pending_refreshes.clear()
    server._refreshes_shutting_down = False
    try:
        yield
    finally:
        # Cancel + await every still-live task so none survives into the next test, then
        # clear the globals back to a pristine slate.
        live = list(server._refresh_tasks)
        for t in live:
            t.cancel()
        if live:
            await asyncio.gather(*live, return_exceptions=True)
        server._refresh_tasks.clear()
        server._pending_refreshes.clear()
        server._refreshes_shutting_down = False


async def test_orphaned_refresh_task_is_tracked_for_shutdown_drain(reset_refresh_tasks, monkeypatch):
    """A second schedule for the same project cancels+REPLACES the first task in
    _pending_refreshes, orphaning the first. The orphan must still be tracked in
    server._refresh_tasks so lifespan shutdown can cancel+await it before closing foreign
    conns (a replaced-but-unwinding task may sit inside connect_foreign_cached).
    (Discriminator MEDIUM iter-2.)"""
    import asyncio

    # Tiny debounce so _delayed() is cheap; we still assert tracking BEFORE the loop runs,
    # so the tasks are guaranteed live (parked in asyncio.sleep, not yet replaced/cancelled).
    monkeypatch.setattr(server, "_refresh_debounce_sec", 0.01)

    # First schedule: one tracked task, recorded as the current per-project task.
    await server.schedule_pipelines_view_refresh("p")
    assert len(server._refresh_tasks) == 1
    first = server._pending_refreshes["p"]
    assert first in server._refresh_tasks

    # Second schedule for the SAME project: cancels + REPLACES `first` in _pending_refreshes,
    # orphaning it. BOTH the orphan and the new current task must be tracked in _refresh_tasks
    # (the per-name dict only holds the current one — that was the leak the fix closes).
    await server.schedule_pipelines_view_refresh("p")
    current = server._pending_refreshes["p"]
    assert current is not first  # the dict entry was overwritten
    assert len(server._refresh_tasks) == 2  # orphan + current both tracked
    assert first in server._refresh_tasks
    assert current in server._refresh_tasks
    # The per-name dict, by contrast, only knows about the current task — proving the set
    # is what makes the orphan reachable at shutdown.
    assert set(server._pending_refreshes.values()) == {current}

    # Cancel both and let the event loop run: the add_done_callback(_refresh_tasks.discard)
    # self-discards each finished task, draining the set back toward empty (no leak).
    first.cancel()
    current.cancel()
    await asyncio.gather(first, current, return_exceptions=True)
    # Done-callbacks run as scheduled callbacks; yield once so they fire.
    await asyncio.sleep(0)
    assert server._refresh_tasks == set()


async def test_shutdown_guard_skips_already_cancelling_task():
    """Predicate-level test for the lifespan-shutdown cancel guard
    `(not task.done() and task.cancelling() == 0)`:

      - A task a prior re-schedule already cancelled (cancelling() == 1) must FAIL the
        guard, so shutdown does NOT deliver a SECOND cancel. (A 2nd CancelledError could
        land on the unshielded `await conn.rollback()` in connect_foreign_cached's finally,
        which `except Exception` won't catch, aborting cleanup before close_foreign_conns().)
      - A fresh, un-cancelled, not-done task must PASS the guard, so shutdown cancels it
        exactly once.

    Scope note: the full shutdown race (a replaced task mid-rollback receiving a 2nd
    cancel) is impractical to make deterministic, so this asserts the guard predicate
    directly — the cancel decision is the unit the fix changes. Relies on
    asyncio.Task.cancelling() (Python 3.12)."""
    import asyncio

    async def _sleep_forever():
        await asyncio.sleep(3600)

    already = asyncio.ensure_future(_sleep_forever())
    fresh = asyncio.ensure_future(_sleep_forever())
    try:
        # Let both tasks reach their `await asyncio.sleep(...)` so neither is done yet.
        await asyncio.sleep(0)

        # Simulate the re-schedule cancel on `already`: now a cancel is pending on it.
        already.cancel()
        assert already.cancelling() == 1
        assert not already.done()  # cancellation not yet delivered (loop hasn't run it)

        # The shutdown guard would NOT re-cancel a task that is already cancelling.
        assert (not already.done() and already.cancelling() == 0) is False

        # A fresh, un-cancelled, not-done task: the guard WOULD cancel it (exactly once).
        assert fresh.cancelling() == 0
        assert not fresh.done()
        assert (not fresh.done() and fresh.cancelling() == 0) is True
    finally:
        # Clean up both tasks so nothing leaks (gather swallows the CancelledErrors).
        fresh.cancel()
        if already.cancelling() == 0:
            already.cancel()
        await asyncio.gather(already, fresh, return_exceptions=True)
