"""F-CC36 follow-up — unit tests for the single shared watchdog Observer.

`start_watcher_for_project` schedules every project on ONE shared Observer (one OS
emitter thread), replacing the prior one-Observer-per-project model that leaked a
whole Observer + thread on every re-register.

FU2 (2026-06-08): multiple projects CAN share a parent dir (verified live: 4 such
pairs), and watchdog attaches every project's handler to ONE shared watch/emitter per
directory. So the bookkeeping now tracks ONE `ObservedWatch` per normalized dir
(`_dir_watch`) plus one handler per project (`_watches`, whose values are
`(norm_dir, handler)` tuples). On re-register we remove ONLY this project's prior
handler (never the whole watch) so co-located projects keep receiving events, and we
free a dir's emitter only when no project still watches it. These tests exercise
`start_watcher_for_project`, `stop_all_watchers`, and the
`_shared_observer` / `_watches` / `_dir_watch` module globals against temp dirs.

`start_watcher_for_project` calls `Observer().start()` (a real daemon thread), so
teardown MUST call `stop_all_watchers()` even when asserts fail. Because the tests
mutate module globals, a fixture resets `_shared_observer`/`_watches`/`_dir_watch`
before AND after each test so state never bleeds into other tests.

Style mirrors tests/test_db_pool.py: module-level `pytestmark`, `import server`,
tmp_path-isolated dirs.
"""

from __future__ import annotations

import asyncio

import pytest

from broker import server

# The functions under test are synchronous, but the suite is async-marked elsewhere;
# these tests don't need the event loop, so no module-level asyncio mark.


@pytest.fixture
def reset_watchers():
    """Reset the shared-observer globals before the test and guarantee the daemon
    observer thread is stopped + globals cleared after (even if an assert fails)."""
    # Pre-test: ensure a clean slate (stop any stray observer from a prior test).
    server.stop_all_watchers()
    assert server._shared_observer is None
    assert server._watches == {}
    assert server._dir_watch == {}
    try:
        yield
    finally:
        # Post-test: stop the daemon observer thread and clear globals.
        server.stop_all_watchers()


def _make_project(tmp_path, name: str, *, subdir: str | None = None):
    """Create a temp dir with a dummy state.db so parent_dir exists.

    When `subdir` is given, the state.db is placed inside that SHARED subdir so
    multiple projects can deliberately co-locate in one directory.
    """
    project_dir = tmp_path / (subdir if subdir is not None else name)
    project_dir.mkdir(exist_ok=True)
    state_db = project_dir / f"{name}.db" if subdir is not None else project_dir / "state.db"
    state_db.write_text("")  # presence is all that's checked
    return str(state_db)


def _dummy_loop():
    """A placeholder loop object: the handler only STORES it (never invoked here)."""
    return asyncio.new_event_loop()


def test_two_projects_distinct_dirs_two_watches_two_dirs(tmp_path, reset_watchers):
    """Two projects in DISTINCT dirs -> one shared Observer, two watches, two emitters."""
    loop = _dummy_loop()
    try:
        db_a = _make_project(tmp_path, "proj_a")
        db_b = _make_project(tmp_path, "proj_b")

        server.start_watcher_for_project("proj_a", db_a, loop)
        server.start_watcher_for_project("proj_b", db_b, loop)

        assert server._shared_observer is not None
        assert len(server._watches) == 2
        assert set(server._watches) == {"proj_a", "proj_b"}
        # Distinct dirs -> two separate emitters (one ObservedWatch each).
        assert len(server._dir_watch) == 2
    finally:
        loop.close()


def test_shared_dir_reregister_does_not_disturb_colocated_project(tmp_path, reset_watchers):
    """REGRESSION (FU2): two projects sharing ONE dir keep one emitter, and
    re-registering one does NOT drop the other's handler.

    Both projects' state.db live in the SAME directory, so watchdog hands them ONE
    shared ObservedWatch/emitter. The old code's unschedule(prior) would have dropped
    the whole watch — killing B's handler when A re-registers. With per-handler
    removal, B must be left untouched.
    """
    loop = _dummy_loop()
    try:
        # proj_a and proj_b co-located in the SAME dir (distinct .db filenames).
        db_a = _make_project(tmp_path, "proj_a", subdir="shared")
        db_b = _make_project(tmp_path, "proj_b", subdir="shared")

        server.start_watcher_for_project("proj_a", db_a, loop)
        server.start_watcher_for_project("proj_b", db_b, loop)

        # Two project handlers, but ONE shared emitter for the shared dir.
        assert len(server._watches) == 2
        assert len(server._dir_watch) == 1

        shared_norm_dir, b_handler = server._watches["proj_b"]
        a_norm_dir, a_old_handler = server._watches["proj_a"]
        assert a_norm_dir == shared_norm_dir  # both keyed to the same normalized dir
        shared_watch = server._dir_watch[shared_norm_dir]

        b_tuple_before = server._watches["proj_b"]

        # Watchdog-level: both projects' handlers are attached to the SAME shared watch.
        # `observer._handlers` is a watchdog-internal dict {watch -> set(handlers)} — the
        # only accessor for the per-watch handler set. Documented as internal-attribute use.
        handlers_before = set(server._shared_observer._handlers[shared_watch])
        assert a_old_handler in handlers_before
        assert b_handler in handlers_before

        # Re-register proj_a (same shared dir): must remove ONLY proj_a's old handler.
        server.start_watcher_for_project("proj_a", db_a, loop)

        # Bookkeeping: B untouched, still one emitter, still two projects.
        assert server._watches["proj_b"] is b_tuple_before  # B's tuple UNCHANGED
        assert len(server._dir_watch) == 1
        assert len(server._watches) == 2

        # A's handler object changed (new handler constructed on re-register).
        a_new_dir, a_new_handler = server._watches["proj_a"]
        assert a_new_dir == shared_norm_dir
        assert a_new_handler is not a_old_handler

        # Watchdog-level: B's handler still registered on the shared watch, A's OLD
        # handler gone, A's NEW handler present.
        handlers_after = set(server._shared_observer._handlers[shared_watch])
        assert b_handler in handlers_after          # B undisturbed
        assert a_old_handler not in handlers_after  # A's stale handler removed
        assert a_new_handler in handlers_after      # A's fresh handler attached
    finally:
        loop.close()


def test_reregister_move_to_new_dir_frees_old_emitter(tmp_path, reset_watchers):
    """Re-register a project that MOVES to a NEW dir where it was the ONLY watcher of
    the old dir -> old dir removed from _dir_watch, new dir present, watch count stable."""
    loop = _dummy_loop()
    try:
        db_a_old = _make_project(tmp_path, "proj_a")  # tmp_path/proj_a/state.db
        server.start_watcher_for_project("proj_a", db_a_old, loop)
        assert len(server._watches) == 1
        assert len(server._dir_watch) == 1
        old_norm_dir = server._watches["proj_a"][0]

        # Move proj_a's state.db to a brand-new directory and re-register.
        new_dir = tmp_path / "proj_a_moved"
        new_dir.mkdir()
        db_a_new = new_dir / "state.db"
        db_a_new.write_text("")
        server.start_watcher_for_project("proj_a", str(db_a_new), loop)

        new_norm_dir = server._watches["proj_a"][0]
        assert new_norm_dir != old_norm_dir
        # Old dir's emitter freed (proj_a was its only watcher); new dir present.
        assert old_norm_dir not in server._dir_watch
        assert new_norm_dir in server._dir_watch
        assert len(server._dir_watch) == 1
        # Still exactly one project tracked.
        assert len(server._watches) == 1
    finally:
        loop.close()


def test_stop_all_watchers_resets_globals(tmp_path, reset_watchers):
    """stop_all_watchers() sets `_shared_observer` to None and clears `_watches`
    AND `_dir_watch`."""
    loop = _dummy_loop()
    try:
        db_a = _make_project(tmp_path, "proj_a")
        server.start_watcher_for_project("proj_a", db_a, loop)
        assert server._shared_observer is not None
        assert len(server._watches) == 1
        assert len(server._dir_watch) == 1

        server.stop_all_watchers()

        assert server._shared_observer is None
        assert server._watches == {}
        assert server._dir_watch == {}
    finally:
        loop.close()


def test_missing_parent_dir_does_not_schedule(tmp_path, reset_watchers):
    """A project whose parent dir is missing is skipped: no Observer, no watch, no dir."""
    loop = _dummy_loop()
    try:
        missing = str(tmp_path / "nonexistent" / "state.db")
        server.start_watcher_for_project("ghost", missing, loop)

        assert server._shared_observer is None
        assert server._watches == {}
        assert server._dir_watch == {}
    finally:
        loop.close()
