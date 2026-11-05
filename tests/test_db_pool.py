"""F-CC36 — unit tests for the `_ConnectionPool` (root-cause fix for the
2026-06-08 event-loop wedge).

The pool opens a fixed number of long-lived aiosqlite connections ONCE (so the
per-request OS-thread spawn that wedged the loop never happens in steady state).
These tests exercise the pool class directly — no lifespan, no HTTP, no schema
(SELECT 1 needs none). Style mirrors tests/test_audit_runner.py:
module-level `pytestmark = pytest.mark.asyncio`, tmp_path-isolated DB path.
"""

from __future__ import annotations

import asyncio

import pytest

from broker import server

pytestmark = pytest.mark.asyncio


def _qsize(pool) -> int:
    """Available (idle) connections currently in the pool's queue."""
    return pool._queue.qsize()


async def test_open_creates_exactly_size_connections(tmp_path):
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 3)
    await pool.open()
    try:
        # open() creates exactly `size` connections; both the tracking list and
        # the idle queue hold `size` items at rest.
        assert len(pool._all) == 3
        assert _qsize(pool) == 3
    finally:
        await pool.close()


async def test_acquire_round_trips(tmp_path):
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 2)
    await pool.open()
    try:
        assert _qsize(pool) == 2
        async with pool.acquire() as conn:
            # While held, that connection is checked out of the queue.
            assert _qsize(pool) == 1
            cur = await conn.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
        # Returned at exit — queue back to full.
        assert _qsize(pool) == 2
        assert len(pool._all) == 2
    finally:
        await pool.close()


async def test_concurrency_above_size_serializes_gracefully(tmp_path):
    db = tmp_path / "pool.db"
    size = 3
    pool = server._ConnectionPool(str(db), size)
    await pool.open()
    try:
        completed = []

        async def worker(i):
            async with pool.acquire() as conn:
                cur = await conn.execute("SELECT 1")
                await cur.fetchone()
                await cur.close()
                await asyncio.sleep(0)  # force interleaving
            completed.append(i)

        # size + 5 concurrent borrowers contend for `size` connections. The
        # extra borrowers AWAIT an empty queue (yield to the loop) rather than
        # blocking it — graceful degradation, not a wedge.
        await asyncio.gather(*(worker(i) for i in range(size + 5)))

        assert len(completed) == size + 5
        # At rest the pool is full again and the connection set is unchanged
        # (no thread re-spawn under contention).
        assert _qsize(pool) == size
        assert len(pool._all) == size
    finally:
        await pool.close()


async def test_exclusive_checkout_not_handed_out_twice(tmp_path):
    db = tmp_path / "pool.db"
    size = 3
    pool = server._ConnectionPool(str(db), size)
    await pool.open()
    try:
        async with pool.acquire() as held:
            # The held connection is NOT in the idle queue — exactly size-1 left.
            assert _qsize(pool) == size - 1
            # A second concurrent borrower gets a DIFFERENT connection object.
            async with pool.acquire() as other:
                assert other is not held
                assert _qsize(pool) == size - 2
        assert _qsize(pool) == size
    finally:
        await pool.close()


async def test_app_level_exception_rolls_back_and_returns_connection(tmp_path):
    db = tmp_path / "pool.db"
    size = 2
    pool = server._ConnectionPool(str(db), size)
    await pool.open()
    try:
        before_all = list(pool._all)

        # An app-level ValueError inside the `async with` must NOT break or drop
        # the connection: it is rolled back (no-op when no txn) and RETURNED.
        with pytest.raises(ValueError):
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
                raise ValueError("app-level failure")

        # Pool size preserved; SAME underlying connections (no replacement, no
        # extra worker thread spawned).
        assert _qsize(pool) == size
        assert len(pool._all) == size
        assert pool._all == before_all

        # Pool is still usable afterward.
        async with pool.acquire() as conn:
            cur = await conn.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
        assert _qsize(pool) == size
    finally:
        await pool.close()


async def test_close_closes_all_and_is_idempotent(tmp_path):
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 3)
    await pool.open()
    assert len(pool._all) == 3

    await pool.close()
    assert pool._all == []
    assert pool._closed is True

    # Second close is a no-op (idempotent) — does not raise.
    await pool.close()
    assert pool._all == []


async def test_partial_open_failure_cleans_up_no_leak(tmp_path, monkeypatch):
    """A connection that fails partway through open() must not leak the
    already-opened connections: open() raises, `_all` is empty, the idle queue
    is empty, and every connection that WAS created has been closed."""
    db = tmp_path / "pool.db"
    size = 5
    fail_after = 3  # K < size successful opens, then raise on the (K+1)th
    pool = server._ConnectionPool(str(db), size)

    closed_count = 0

    class _TrackingConn:
        """Stand-in connection that only counts close() calls."""

        def __init__(self):
            self.closed = False

        async def close(self):
            nonlocal closed_count
            self.closed = True
            closed_count += 1

    calls = {"n": 0}
    opened = []

    async def _fake_new_conn():
        if calls["n"] >= fail_after:
            raise RuntimeError("connect blew up mid-open")
        calls["n"] += 1
        conn = _TrackingConn()
        opened.append(conn)
        return conn

    monkeypatch.setattr(pool, "_new_conn", _fake_new_conn)

    with pytest.raises(RuntimeError, match="connect blew up mid-open"):
        await pool.open()

    # Nothing leaks: tracking list empty, idle queue empty.
    assert pool._all == []
    assert _qsize(pool) == 0
    # All K connections that were successfully opened got closed.
    assert len(opened) == fail_after
    assert closed_count == fail_after
    assert all(c.closed for c in opened)


async def test_acquire_after_close_raises_not_hang(tmp_path):
    """A late acquire() after close() began must RAISE (pool closed) instead of
    parking forever on the queue."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 2)
    await pool.open()
    await pool.close()

    with pytest.raises(RuntimeError, match="closed"):
        async with pool.acquire():
            pass  # pragma: no cover — entry must raise before the body runs


async def test_close_wakes_parked_waiter(tmp_path):
    """close() must wake a coroutine already parked on an empty queue inside
    acquire() so it raises (pool closed) instead of hanging through shutdown."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()
    try:
        hold_entered = asyncio.Event()
        release = asyncio.Event()

        async def holder():
            # Take the single connection and HOLD it (don't exit) until released,
            # leaving the queue empty so the second acquire must park.
            async with pool.acquire():
                hold_entered.set()
                await release.wait()

        holder_task = asyncio.create_task(holder())
        await asyncio.wait_for(hold_entered.wait(), timeout=2.0)

        async def parked():
            # Queue is empty (single conn is held) -> this parks on queue.get().
            async with pool.acquire():
                pass  # pragma: no cover — must be woken to raise, not enter

        parked_task = asyncio.create_task(parked())
        # Give the parked task a tick to actually reach queue.get().
        await asyncio.sleep(0.05)

        # close() drops a sentinel that wakes the parked waiter, which must raise
        # RuntimeError within the timeout (proving it did not hang).
        await pool.close()
        with pytest.raises(RuntimeError, match="closed"):
            await asyncio.wait_for(parked_task, timeout=2.0)
    finally:
        # Release the holder so its task can unwind cleanly.
        release.set()
        await asyncio.wait_for(holder_task, timeout=2.0)


async def test_normal_exit_sanitizes_open_transaction(tmp_path):
    """A connection returned with a transaction still open (borrower exited
    NORMALLY without committing/rolling back) must be sanitized by _return's
    rollback so the next borrower starts clean."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()
    try:
        async with pool.acquire() as conn:
            await conn.execute("BEGIN")
            assert conn.in_transaction is True
            # Exit the `async with` NORMALLY — no commit, no rollback, no raise.

        # size=1 -> re-acquire hands back the SAME connection. _return's rollback
        # must have sanitized the leaked transaction.
        async with pool.acquire() as conn2:
            assert conn2.in_transaction is False
            cur = await conn2.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
    finally:
        await pool.close()


async def test_broken_conn_replace_keeps_all_accurate(tmp_path):
    """If a borrowed connection cannot be rolled back on return, _return closes
    and REPLACES it: `_all` stays exactly `size`, the dead conn is gone from
    `_all`, and the pool remains usable."""
    db = tmp_path / "pool.db"
    size = 1
    pool = server._ConnectionPool(str(db), size)
    await pool.open()
    try:
        before = list(pool._all)
        assert len(before) == size

        async with pool.acquire() as conn:
            dead = conn

            # Force the return-path rollback to raise ONCE so _return treats this
            # connection as broken and replaces it.
            async def _boom():
                raise RuntimeError("rollback failed -> conn is broken")

            conn.rollback = _boom  # type: ignore[method-assign]
            # Exit normally — _return calls rollback() (raises) -> close+replace.

        # Pool size unchanged; the dead conn is gone and a replacement is present.
        assert len(pool._all) == size
        assert dead not in pool._all
        assert _qsize(pool) == size

        # Pool still usable: the replacement serves a real query.
        async with pool.acquire() as conn2:
            assert conn2 is not dead
            cur = await conn2.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
        assert _qsize(pool) == size
        assert len(pool._all) == size
    finally:
        await pool.close()


async def test_new_conn_closes_conn_on_pragma_failure_no_leak(tmp_path, monkeypatch):
    """If `_apply_pragmas` raises AFTER aiosqlite.connect() spawned the worker
    thread, `_new_conn` must close that connection before re-raising so the
    thread does not leak (the very class of bug this fix targets)."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)

    captured = {}
    real_connect = server.aiosqlite.connect

    def _tracking_connect(*args, **kwargs):
        # aiosqlite.connect returns a Connection that is itself awaitable; wrap
        # the await so we capture the real connection object the pool will use.
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
        await pool._new_conn()

    # The connection that connect() produced was captured AND closed before the
    # exception propagated — no dangling aiosqlite worker thread.
    assert "conn" in captured, "expected aiosqlite.connect to have produced a conn"
    leaked = captured["conn"]
    # aiosqlite.close() tears down the worker thread and nulls `_connection`;
    # both observable flags confirm the conn was really closed (thread reclaimed).
    assert leaked._connection is None
    assert leaked._running is False
    # A second close() must be safe (already closed) — proves idempotent close.
    await leaked.close()


async def test_close_wakes_more_than_size_parked_waiters(tmp_path):
    """close() must wake EVERY parked waiter, even when more than `_size`
    coroutines are parked. The replenishing poison pill (acquire re-enqueues the
    sentinel before raising) makes a single sentinel wake any number of waiters,
    where the old fixed `_size` sentinels would have left extra waiters hung."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()
    holder_task = None
    release = asyncio.Event()
    try:
        hold_entered = asyncio.Event()

        async def holder():
            # Hold the single connection so the queue stays empty and the other
            # acquirers must park.
            async with pool.acquire():
                hold_entered.set()
                await release.wait()

        holder_task = asyncio.create_task(holder())
        await asyncio.wait_for(hold_entered.wait(), timeout=2.0)

        async def parked():
            async with pool.acquire():
                pass  # pragma: no cover — must be woken to raise, not enter

        # THREE parked waiters with size=1 -> well above `_size`. The old code
        # enqueued only `_size` (==1) sentinels and would wake just one of them.
        parked_tasks = [asyncio.create_task(parked()) for _ in range(3)]
        await asyncio.sleep(0.05)  # let all three reach queue.get()

        await pool.close()

        # ALL three parked waiters must raise (none left hanging) within timeout.
        results = await asyncio.wait_for(
            asyncio.gather(*parked_tasks, return_exceptions=True), timeout=2.0
        )
        assert len(results) == 3
        for r in results:
            assert isinstance(r, RuntimeError)
            assert "closed" in str(r)
    finally:
        release.set()
        if holder_task is not None:
            await asyncio.wait_for(holder_task, timeout=2.0)


async def test_return_after_concurrent_close_does_not_requeue(tmp_path):
    """The post-rollback recheck: if close() runs concurrently and flips
    `_closed` while the borrower's rollback await is in flight, _return must
    close the connection instead of requeuing it into a now-dead pool."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()

    closed_flag = {"closed": False}

    async with pool.acquire() as conn:
        real_rollback = conn.rollback
        real_close = conn.close

        async def _rollback_then_close_pool():
            # Simulate close() racing during the rollback await: rollback succeeds
            # but the pool becomes closed by the time the await returns.
            await real_rollback()
            pool._closed = True

        async def _track_close():
            closed_flag["closed"] = True
            await real_close()

        conn.rollback = _rollback_then_close_pool  # type: ignore[method-assign]
        conn.close = _track_close  # type: ignore[method-assign]
        # Exit normally: _return rolls back (flips _closed), then the recheck
        # sees _closed and must close instead of requeueing.

    # The connection was NOT requeued (queue stays empty) and WAS closed.
    assert _qsize(pool) == 0
    assert closed_flag["closed"] is True

    # Direct unit check of the up-front close branch: with _closed True before
    # the call, _return closes the conn and enqueues nothing.
    pool2 = server._ConnectionPool(str(tmp_path / "pool2.db"), 1)
    await pool2.open()
    direct_conn = await pool2._queue.get()  # take the one idle conn out
    pool2._closed = True
    direct_closed = {"closed": False}
    real_direct_close = direct_conn.close

    async def _direct_track_close():
        direct_closed["closed"] = True
        await real_direct_close()

    direct_conn.close = _direct_track_close  # type: ignore[method-assign]
    await pool2._return(direct_conn)
    assert direct_closed["closed"] is True
    assert pool2._queue.qsize() == 0


async def test_replacement_then_concurrent_close_not_enqueued(tmp_path):
    """If close() races and flips `_closed` WHILE `_new_conn()` is producing the
    replacement for a broken connection, the freshly-created replacement must be
    CLOSED and NOT enqueued into the now-dead pool (no leak, no stale queue
    entry)."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()

    real_new_conn = pool._new_conn
    replacement = {"conn": None, "closed": False}

    async def _new_conn_that_closes_pool_first():
        # Simulate close() racing: flip `_closed` BEFORE the replacement conn is
        # handed back, exactly as a concurrent close() would.
        pool._closed = True
        conn = await real_new_conn()
        # Wrap close() so the test can observe the replacement being torn down.
        real_close = conn.close

        async def _track_close():
            replacement["closed"] = True
            await real_close()

        conn.close = _track_close  # type: ignore[method-assign]
        replacement["conn"] = conn
        return conn

    async with pool.acquire() as conn:
        async def _boom():
            raise RuntimeError("rollback failed -> conn is broken")

        conn.rollback = _boom  # type: ignore[method-assign]
        pool._new_conn = _new_conn_that_closes_pool_first  # type: ignore[method-assign]
        # Exit normally: _return rolls back (raises) -> close+remove -> replace.
        # _new_conn flips _closed, so the new conn must be closed, not enqueued.

    # A replacement WAS created, then CLOSED, and never placed in the queue.
    assert replacement["conn"] is not None
    assert replacement["closed"] is True
    assert pool._queue.qsize() == 0
    # The replacement is not tracked in `_all` either (never appended on this path).
    assert replacement["conn"] not in pool._all


async def test_replacement_failure_empties_pool_fails_closed_no_hang(tmp_path):
    """size=1: a broken connection whose replacement ALSO fails empties the pool.
    With no live connections left, _return must fail the pool closed and enqueue
    the sentinel so a subsequent acquire() raises RuntimeError FAST (proving it
    does not hang forever waiting on an empty queue)."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()

    async def _new_conn_fails():
        raise RuntimeError("replacement connect failed")

    async with pool.acquire() as conn:
        async def _boom():
            raise RuntimeError("rollback failed -> conn is broken")

        conn.rollback = _boom  # type: ignore[method-assign]
        pool._new_conn = _new_conn_fails  # type: ignore[method-assign]
        # Exit: _return closes the broken conn, removes it (_all now empty), tries
        # to replace (fails) -> pool has no live conns -> fail closed + sentinel.

    # Pool failed closed with a poison pill enqueued; no live connections remain.
    assert pool._closed is True
    assert pool._all == []
    assert pool._queue.qsize() == 1  # the sentinel

    # A subsequent acquire must raise RuntimeError FAST (no hang). acquire() rejects
    # at entry because `_closed` is True; wrap in wait_for to prove no hang.
    async def _attempt():
        async with pool.acquire():
            pass  # pragma: no cover — entry must raise before the body runs

    with pytest.raises(RuntimeError, match="closed"):
        await asyncio.wait_for(_attempt(), timeout=2.0)


async def test_replacement_failure_with_survivor_degrades_not_closed(tmp_path):
    """Graceful-degradation counterpart: size=2. Break ONE connection with its
    replacement failing while the OTHER connection is still live in `_all`/queue.
    The pool must NOT be failed closed (a survivor remains) — it degrades to fewer
    slots and still serves from the surviving connection."""
    db = tmp_path / "pool.db"
    size = 2
    pool = server._ConnectionPool(str(db), size)
    await pool.open()
    try:
        async def _new_conn_fails():
            raise RuntimeError("replacement connect failed")

        # Borrow ONE of the two connections; the OTHER stays live in `_all` and in
        # the idle queue. Breaking + failing-to-replace the borrowed one leaves a
        # survivor, so the pool must degrade gracefully (not fail closed).
        async with pool.acquire() as conn:
            broken = conn

            async def _boom():
                raise RuntimeError("rollback failed -> conn is broken")

            conn.rollback = _boom  # type: ignore[method-assign]
            pool._new_conn = _new_conn_fails  # type: ignore[method-assign]
            # Exit: broken conn closed+removed; replacement fails; survivor remains
            # so the pool is NOT failed closed.

        # NOT failed closed — a live connection survives; degraded to size-1.
        assert pool._closed is False
        assert broken not in pool._all
        assert len(pool._all) == size - 1
        # No sentinel was enqueued; only the surviving idle connection is queued.
        assert pool._queue.qsize() == size - 1

        # The pool still serves from the survivor.
        async with pool.acquire() as conn2:
            assert conn2 is not broken
            cur = await conn2.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
        assert pool._queue.qsize() == size - 1
    finally:
        await pool.close()


async def test_inflight_replacement_guard_blocks_premature_fail_closed(tmp_path):
    """The fail-closed guard must respect an in-flight replacement counter.

    Concurrency race: two simultaneous broken-conn returns. If one replacement
    fails first while the other is still resolving, the pool must NOT fail closed
    on the empty-`_all` check alone — a pending replacement may yet restore
    service. `_return` tracks in-flight attempts in `pool._replacing` and only
    fails closed when it is 0.

    Deterministic proof (size=1): set up the broken+empty fail-closed scenario
    (rollback raises -> broken; `_new_conn` raises -> replacement fails; `_all`
    empty after removal). With `_replacing` pre-set to 1 (another replacement in
    flight), the return must NOT fail closed. With `_replacing` back to 0, an
    equivalent broken+empty return MUST fail closed.
    """
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()

    async def _new_conn_fails():
        raise RuntimeError("replacement connect failed")

    async def _boom_rollback():
        raise RuntimeError("rollback failed -> conn is broken")

    # --- Phase 1: a replacement is in flight (_replacing == 1) -> NO fail closed.
    conn = await pool._queue.get()  # take the single idle conn out of the queue
    conn.rollback = _boom_rollback  # type: ignore[method-assign]
    pool._new_conn = _new_conn_fails  # type: ignore[method-assign]
    # Simulate ANOTHER broken-conn replacement already in flight. _return will
    # bump this to 2 around its own _new_conn() await, then back to 1 in the
    # finally — so when the fail-closed check runs, _replacing == 1 (not 0).
    pool._replacing = 1

    await pool._return(conn)

    # `_all` is empty (the broken conn was removed) but a replacement is pending,
    # so the pool must NOT have failed closed and NO sentinel was enqueued.
    assert pool._all == []
    assert pool._closed is False
    assert pool._queue.qsize() == 0  # no sentinel

    # --- Phase 2: no replacement in flight (_replacing == 0) -> MUST fail closed.
    # Re-open a fresh single-conn pool for an equivalent broken+empty return.
    pool2 = server._ConnectionPool(str(tmp_path / "pool2.db"), 1)
    await pool2.open()
    conn2 = await pool2._queue.get()
    conn2.rollback = _boom_rollback  # type: ignore[method-assign]
    pool2._new_conn = _new_conn_fails  # type: ignore[method-assign]
    assert pool2._replacing == 0

    await pool2._return(conn2)

    # No live conns AND no replacement pending -> failed closed with the sentinel.
    assert pool2._all == []
    assert pool2._closed is True
    assert pool2._queue.qsize() == 1  # the sentinel
    got = await pool2._queue.get()
    assert got is server._POOL_SENTINEL


async def test_concurrent_broken_returns_recover_not_fail_closed(tmp_path):
    """Concurrency counterpart (size=2): two borrowed connections both rollback-
    fail and return concurrently. The FIRST replacement attempt fails; the SECOND
    succeeds. Interleaved via an asyncio.Event so BOTH increment `_replacing`
    before either resolves, the pool must NOT fail closed (the surviving in-flight
    replacement restores service) and recovers with >=1 usable connection.
    """
    db = tmp_path / "pool.db"
    size = 2
    pool = server._ConnectionPool(str(db), size)
    await pool.open()
    try:
        real_new_conn = pool._new_conn
        both_in_flight = asyncio.Event()
        call_count = {"n": 0}

        async def _interleaved_new_conn():
            call_count["n"] += 1
            n = call_count["n"]
            if n == 1:
                # First replacement: wait until the SECOND return has also bumped
                # _replacing (both in flight), then fail. This is the return that
                # empties `_all` first and would prematurely fail closed without
                # the counter guard.
                await asyncio.wait_for(both_in_flight.wait(), timeout=2.0)
                raise RuntimeError("first replacement failed")
            # Second replacement: signal that both are now in flight, then succeed
            # with a real connection.
            both_in_flight.set()
            return await real_new_conn()

        pool._new_conn = _interleaved_new_conn  # type: ignore[method-assign]

        async def _boom_rollback():
            raise RuntimeError("rollback failed -> conn is broken")

        # Borrow BOTH connections, break both, and return them concurrently.
        conn_a = await pool._queue.get()
        conn_b = await pool._queue.get()
        conn_a.rollback = _boom_rollback  # type: ignore[method-assign]
        conn_b.rollback = _boom_rollback  # type: ignore[method-assign]
        # Both conns are still tracked in `_all` (only removed inside _return).

        ret_a = asyncio.create_task(pool._return(conn_a))
        ret_b = asyncio.create_task(pool._return(conn_b))
        await asyncio.wait_for(asyncio.gather(ret_a, ret_b), timeout=3.0)

        # The pool must NOT have failed closed: the second replacement succeeded
        # while the first was in flight, so service is restored.
        assert pool._closed is False
        # At least one usable connection remains (the successful replacement).
        assert len(pool._all) >= 1
        assert pool._queue.qsize() >= 1

        # The survivor serves a real query.
        async with pool.acquire() as conn:
            cur = await conn.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
    finally:
        await pool.close()


async def test_cancellation_mid_borrow_does_not_shrink_pool(tmp_path):
    """A cancellation while a connection is borrowed must NOT drop the pool slot:
    `asyncio.shield(self._return(conn))` keeps the return atomic so the conn is
    always requeued and the pool returns to full."""
    db = tmp_path / "pool.db"
    pool = server._ConnectionPool(str(db), 1)
    await pool.open()
    try:
        entered = asyncio.Event()

        async def borrower():
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
                entered.set()
                # Hold the connection across a long sleep so we can cancel mid-borrow.
                await asyncio.sleep(3600)

        task = asyncio.create_task(borrower())
        # Wait until the task is genuinely inside the acquire block holding the conn.
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        assert _qsize(pool) == 0  # the single conn is checked out

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The shield guarantees the return completed: pool is full again and the
        # connection is reusable for a subsequent query.
        assert _qsize(pool) == 1
        assert len(pool._all) == 1
        async with pool.acquire() as conn2:
            cur = await conn2.execute("SELECT 1")
            row = await cur.fetchone()
            await cur.close()
            assert tuple(row) == (1,)
        assert _qsize(pool) == 1
    finally:
        await pool.close()
