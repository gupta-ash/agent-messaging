# -*- coding: utf-8 -*-
"""Unit tests for the relay broker_client ack-mirror — post_ack + the _post
fast-fail/outbox machinery that backs the receive-hook's critical-path mirror
(task #19) and its #18/F1 respin (outbox_on_4xx + the flush_outbox replay half).

Covers:
  - post_ack: path-traversal reject; int/bool/dedup/sort; empty -> None (no POST);
    <=500 chunking; agent_id URL-quoting; fast_fail kwargs threaded to _post.
  - _post overrides: attempts=1 => ONE urlopen attempt then outbox (loss-free);
    default 4xx dropped (non-ack); outbox_on_4xx => a 4xx IS enqueued (post_ack).
  - flush_outbox replay (#18/F1): a transient /ack 404/405 is RETAINED for the 24h
    TTL (self-heals post broker-upgrade); a permanent 4xx (422) / a non-ack 404 drops.
  - orphan-recovery is flush-gated, not import-time (#18/F2).

Self-contained; monkeypatches broker_client._post / .urlopen / ._enqueue_outbox.
Never touches the live broker or ~/.claude state.

NOTE (fleet convention): these import broker_client from the DEPLOYED artifact
(~/.claude/hooks/relay) — the tilde path is the Channel-B deployed target, and this
suite deliberately exercises what actually runs (D64 portability forbids a
build-host repo path here). So local dev MUST sync the repo source ->
~/.claude/hooks/relay before running (the "faithful deploy preview"); CI/fleet re-runs
this after deploying the merge. (Cross-family LOW, accepted.)
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

_RELAY_HOOKS = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(_RELAY_HOOKS))
import broker_client  # noqa: E402


# --------------------------- post_ack: validation ---------------------------

def test_post_ack_rejects_path_traversal():
    for bad in ("..", ".", "a/b", "a\\b"):
        with pytest.raises(ValueError):
            broker_client.post_ack(bad, [1])


def test_post_ack_rejects_non_string_agent_id():
    with pytest.raises(ValueError):
        broker_client.post_ack("", [1])


def test_post_ack_empty_returns_none_no_post(monkeypatch):
    calls = []
    monkeypatch.setattr(broker_client, "_post", lambda *a, **k: calls.append((a, k)))
    assert broker_client.post_ack("name", []) is None
    # non-ints, non-positive, and bool are all filtered out -> empty -> None
    assert broker_client.post_ack("name", [0, -1, True, False, "x", 1.5, None]) is None
    assert calls == []


def test_post_ack_dedup_sort_ints_only(monkeypatch):
    seen = {}

    def fake_post(endpoint, body, **k):
        seen["ids"] = body["event_ids"]
        return {"acked_now": len(body["event_ids"])}

    monkeypatch.setattr(broker_client, "_post", fake_post)
    broker_client.post_ack("name", [3, 1, 2, 2, True, 3])  # bool True excluded; deduped; sorted
    assert seen["ids"] == [1, 2, 3]


def test_post_ack_quotes_agent_id(monkeypatch):
    seen = {}
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: seen.update(endpoint=endpoint) or {})
    broker_client.post_ack("coder@team", [5])
    assert seen["endpoint"] == "/api/agent/coder%40team/ack"


def test_post_ack_chunks_over_500(monkeypatch):
    chunks = []
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: chunks.append(len(body["event_ids"])) or {})
    broker_client.post_ack("name", list(range(1, 1201)))  # 1200 -> 500 + 500 + 200
    assert chunks == [500, 500, 200]


def test_post_ack_fast_fail_passes_overrides(monkeypatch):
    seen = {}
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: seen.update(k) or {})
    broker_client.post_ack("name", [1], fast_fail=True)
    assert seen.get("attempts") == 1
    assert seen.get("timeout_s") == 1.0
    assert seen.get("skip_outbox_count") is True   # hook path skips the O(N) count


def test_post_ack_default_omits_overrides(monkeypatch):
    seen = {}
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: seen.update(k) or {})
    broker_client.post_ack("name", [1])
    assert "attempts" not in seen
    assert "timeout_s" not in seen
    assert "skip_outbox_count" not in seen


def test_post_ack_fast_fail_breaks_and_outboxes_remainder(monkeypatch):
    """(#19 iter-2) fast_fail bounds the hook to ~ONE chunk of HTTP: on the first
    failed chunk, stop the network loop and outbox the remainder directly
    (loss-free, count skipped)."""
    post_sizes = []
    enq = []
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: post_sizes.append(len(body["event_ids"])) or None)  # always fail
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda endpoint, payload, cid, ts, **k: enq.append((len(payload["event_ids"]), k.get("update_count"))))
    broker_client.post_ack("name", list(range(1, 1201)), fast_fail=True)  # 1200 -> 500/500/200
    assert post_sizes == [500]                       # only the FIRST chunk hit the network
    assert enq == [(500, False), (200, False)]       # remainder outboxed directly, count skipped


def test_post_ack_default_no_break_no_direct_outbox(monkeypatch):
    """Non-fast_fail: every chunk is attempted (each _post self-outboxes); post_ack
    itself does NOT enqueue directly."""
    post_sizes = []
    enq = []
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: post_sizes.append(len(body["event_ids"])) or None)
    monkeypatch.setattr(broker_client, "_enqueue_outbox", lambda *a, **k: enq.append(1))
    broker_client.post_ack("name", list(range(1, 1201)))  # default (not fast_fail)
    assert post_sizes == [500, 500, 200]             # all chunks attempted
    assert enq == []                                  # no direct enqueue by post_ack


def test_enqueue_outbox_skip_count_avoids_on_path_read(monkeypatch, tmp_path):
    """(#19 iter-2) update_count=False appends the entry but skips the lazy O(N)
    line-count (which stays uninitialized), so a broker-down hook turn pays no
    whole-file read on the critical path."""
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(tmp_path / "outbox.jsonl"))
    monkeypatch.setattr(broker_client, "_outbox_line_count", -1, raising=False)
    broker_client._enqueue_outbox("/api/agent/x/ack", {"event_ids": [1]}, "cid", 123,
                                  update_count=False)
    assert broker_client._outbox_line_count == -1    # count never initialized (no O(N) read)
    assert (tmp_path / "outbox.jsonl").exists()       # entry still durably appended
    # a subsequent counted enqueue lazy-inits over the real file (2 lines now)
    broker_client._enqueue_outbox("/api/agent/x/ack", {"event_ids": [2]}, "cid2", 124,
                                  update_count=True)
    assert broker_client._outbox_line_count == 2


def test_enqueue_outbox_skip_count_still_increments_when_initialized(monkeypatch, tmp_path):
    """(#19 iter-2 LOW) update_count=False skips only the EXPENSIVE O(N) re-read; if
    the counter is already initialized, the cheap O(1) increment still runs so the
    >1000 'visibility degraded' warning stays accurate for long-lived callers."""
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(tmp_path / "outbox.jsonl"))
    monkeypatch.setattr(broker_client, "_outbox_line_count", 5, raising=False)  # already initialized
    broker_client._enqueue_outbox("/api/agent/x/ack", {"event_ids": [1]}, "cid", 123,
                                  update_count=False)
    assert broker_client._outbox_line_count == 6      # O(1) increment kept, no O(N) read


# --------------------------- post_ack: floor mirror (#18/#32) ---------------

def test_post_ack_floor_only_posts(monkeypatch):
    # floor-only (no event_ids) sends ONE post with an empty list + the floor.
    seen = {}
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: seen.update(endpoint=endpoint, body=body) or {})
    broker_client.post_ack("broker", [], floor=4293)
    assert seen["endpoint"] == "/api/agent/broker/ack"
    assert seen["body"] == {"event_ids": [], "floor": 4293}


def test_post_ack_floor_attached_to_first_chunk_only(monkeypatch):
    # with ids + floor, the floor rides ONLY the first chunk (idempotent broker-side).
    bodies = []
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: bodies.append(dict(body)) or {})
    broker_client.post_ack("name", list(range(1, 1201)), floor=999)  # 500/500/200
    assert [len(b["event_ids"]) for b in bodies] == [500, 500, 200]
    assert bodies[0].get("floor") == 999
    assert "floor" not in bodies[1]
    assert "floor" not in bodies[2]


def test_post_ack_no_ids_no_floor_returns_none(monkeypatch):
    calls = []
    monkeypatch.setattr(broker_client, "_post", lambda *a, **k: calls.append(1))
    assert broker_client.post_ack("name", [], floor=None) is None
    assert calls == []


def test_post_ack_invalid_floor_ignored(monkeypatch):
    # a bad floor is dropped (must never black-hole a legit event_ids ack). Includes
    # an out-of-i64 floor (would 422 the whole body server-side + outbox-replay it
    # forever with the valid ids trapped) — cross-family iter-1 MEDIUM.
    bodies = []
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: bodies.append(dict(body)) or {})
    for bad in (0, -1, True, 1.5, "x", 9223372036854775808):
        bodies.clear()
        broker_client.post_ack("name", [5], floor=bad)   # ids still posted, floor dropped
        assert bodies == [{"event_ids": [5]}]             # no floor key
    # floor-only with an invalid floor -> nothing to post -> None (no POST at all)
    calls = []
    monkeypatch.setattr(broker_client, "_post", lambda *a, **k: calls.append(1))
    assert broker_client.post_ack("name", [], floor=0) is None
    assert broker_client.post_ack("name", [], floor=True) is None
    assert broker_client.post_ack("name", [], floor=9223372036854775808) is None
    assert calls == []


def test_post_ack_floor_only_fast_fail_outboxes(monkeypatch):
    # a floor-only fast_fail post against a 404 broker outboxes WITH the floor,
    # so the floor mirror self-heals on the next reconnect flush.
    from urllib.error import HTTPError
    enq = []

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda endpoint, payload, cid, ts, **k: enq.append((endpoint, dict(payload))))
    broker_client.post_ack("broker", [], floor=4293, fast_fail=True)
    assert enq == [("/api/agent/broker/ack", {"event_ids": [], "floor": 4293})]


# --------------------------- _post overrides --------------------------------

def test_post_ack_drops_out_of_range_event_ids(monkeypatch):
    # (#32 iter-2) an out-of-i64 event_id is dropped client-side (mirrors the floor
    # guard + server AckBody) so it can't 422 the whole body and black-hole the valid
    # ids via outbox replay. A mixed valid + max+1 batch ships only the valid ids.
    seen = {}
    monkeypatch.setattr(broker_client, "_post",
                        lambda endpoint, body, **k: seen.update(body=dict(body)) or {})
    broker_client.post_ack("name", [5, 9223372036854775808, 7])
    assert seen["body"] == {"event_ids": [5, 7]}   # max+1 dropped, valid ids posted


def test_post_attempts_override_single_shot(monkeypatch):
    """attempts=1 => exactly ONE urlopen attempt, then outbox (loss-free)."""
    from urllib.error import URLError
    calls = {"open": 0, "outbox": 0}

    def fake_urlopen(req, timeout=None):
        calls["open"] += 1
        raise URLError("refused")

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda *a, **k: calls.__setitem__("outbox", calls["outbox"] + 1))
    r = broker_client._post("/api/agent/x/ack", {"event_ids": [1]}, attempts=1, timeout_s=1.0)
    assert r is None
    assert calls["open"] == 1     # single-shot (no 5-attempt retry storm on the hook path)
    assert calls["outbox"] == 1   # enqueued on failure => replays on MCP reconnect


def test_post_default_attempts_is_five(monkeypatch):
    """Unset attempts keeps the 5-attempt contract for the async MCP path."""
    from urllib.error import URLError
    calls = {"open": 0}

    def fake_urlopen(req, timeout=None):
        calls["open"] += 1
        raise URLError("refused")

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox", lambda *a, **k: None)
    # tiny sleeps so the retry backoff doesn't slow the test
    monkeypatch.setattr(broker_client.time, "sleep", lambda *_: None)
    broker_client._post("/api/agent/x/ack", {"event_ids": [1]})
    assert calls["open"] == 5


def test_post_4xx_dropped_no_outbox(monkeypatch):
    """A 4xx (e.g. a >500 batch 422) is DROPPED and never outboxed — the reason
    post_ack MUST chunk <=500 before calling _post."""
    from urllib.error import HTTPError
    calls = {"outbox": 0}

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 422, "unprocessable", {}, io.BytesIO(b"too many ids"))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda *a, **k: calls.__setitem__("outbox", calls["outbox"] + 1))
    r = broker_client._post("/api/agent/x/ack", {"event_ids": [1]}, attempts=1)
    assert r is None
    assert calls["outbox"] == 0   # 4xx never enqueued


# --------------------- F1 (task #18 prereq): outbox_on_4xx -------------------

def test_post_outbox_on_4xx_enqueues(monkeypatch):
    """With outbox_on_4xx=True, a 4xx (404 from a broker predating /ack, or a
    rolling-deploy skew) is ENQUEUED — it self-heals on the next reconnect flush
    once the broker is upgraded — instead of silently dropping the ledger mirror."""
    from urllib.error import HTTPError
    calls = {"outbox": 0}

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b"no such endpoint"))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda *a, **k: calls.__setitem__("outbox", calls["outbox"] + 1))
    r = broker_client._post("/api/agent/x/ack", {"event_ids": [1]}, attempts=1, outbox_on_4xx=True)
    assert r is None
    assert calls["outbox"] == 1   # 4xx enqueued (F1 fix)


def test_post_outbox_on_4xx_respects_no_outbox_on_failure(monkeypatch):
    """The flusher (no_outbox_on_failure=True) must NOT re-enqueue on a 4xx even with
    outbox_on_4xx set — a line it is replaying must not be re-added by _post."""
    from urllib.error import HTTPError
    calls = {"outbox": 0}

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda *a, **k: calls.__setitem__("outbox", calls["outbox"] + 1))
    r = broker_client._post("/api/agent/x/ack", {"event_ids": [1]},
                            attempts=1, outbox_on_4xx=True, no_outbox_on_failure=True)
    assert r is None
    assert calls["outbox"] == 0


def test_post_default_4xx_still_dropped_for_non_ack(monkeypatch):
    """Unchanged for every non-ack caller: without outbox_on_4xx, a 4xx is still
    dropped (register_project/post_status/etc. keep today's behavior)."""
    from urllib.error import HTTPError
    calls = {"outbox": 0}

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 422, "bad", {}, io.BytesIO(b""))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda *a, **k: calls.__setitem__("outbox", calls["outbox"] + 1))
    r = broker_client._post("/api/agent/x/status", {"x": 1}, attempts=1)
    assert r is None
    assert calls["outbox"] == 0


def test_post_ack_single_chunk_404_outboxes(monkeypatch):
    """F1 CORE: the common hook case (single chunk, fast_fail) against a broker-404
    now OUTBOXES the ack instead of silently dropping the mirror, so the delivery row
    is not left falsely 'outstanding' for the #18 reaper to false-alert on."""
    from urllib.error import HTTPError
    enq = []

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda endpoint, payload, cid, ts, **k: enq.append((endpoint, payload["event_ids"])))
    broker_client.post_ack("broker", [4276], fast_fail=True)
    assert enq == [("/api/agent/broker/ack", [4276])]   # outboxed, not dropped


def test_post_ack_multichunk_404_outboxes_all(monkeypatch):
    """F1 multi-chunk: a 404 on the first chunk outboxes THAT chunk (via _post's
    outbox_on_4xx) AND the remainder (via post_ack's break-and-outbox) — all 1200
    ids preserved, none lost."""
    from urllib.error import HTTPError
    enq = []

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(broker_client, "_enqueue_outbox",
                        lambda endpoint, payload, cid, ts, **k: enq.append(len(payload["event_ids"])))
    broker_client.post_ack("broker", list(range(1, 1201)), fast_fail=True)  # 500/500/200
    assert enq == [500, 500, 200]   # 1st chunk by _post, remainder by post_ack


# --------------------- F2: orphan-recovery is flush-gated, not import --------

def test_flush_outbox_runs_orphan_recovery(monkeypatch, tmp_path):
    """F2: recovery moved from module-import to the FIRST statement of flush_outbox(),
    so only flushers pay the sweep (the per-fire receive hook never flushes)."""
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(tmp_path / "outbox.jsonl"))
    called = {"n": 0}
    monkeypatch.setattr(broker_client, "_recover_orphan_processing_outboxes",
                        lambda: called.__setitem__("n", called["n"] + 1))
    broker_client.flush_outbox()   # empty outbox -> early return, but recovery ran first
    assert called["n"] == 1


def test_flush_outbox_recovers_real_orphan(monkeypatch, tmp_path):
    """End-to-end F2: a stale .processing-<uuid> orphan is claimed + merged back into
    the live outbox by flush_outbox()'s recovery sweep."""
    import os
    import time as _t
    from urllib.error import URLError
    outbox = tmp_path / "outbox.jsonl"
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(outbox))
    orphan = tmp_path / "outbox.jsonl.processing-deadbeef"
    orphan.write_text(
        '{"client_event_id":"c1","endpoint":"/api/agent/x/ack",'
        '"payload":{"event_ids":[9]},"enqueued_ts":%d}\n' % int(_t.time() * 1000),
        encoding="utf-8")
    old = _t.time() - 120                       # older than ORPHAN_AGE_S (60s)
    os.utime(orphan, (old, old))

    def _boom(*a, **k):                          # POST fails -> recovered line preserved in outbox
        raise URLError("down")

    monkeypatch.setattr(broker_client, "urlopen", _boom)
    monkeypatch.setattr(broker_client.time, "sleep", lambda *_: None)
    broker_client.flush_outbox()
    assert not orphan.exists()                   # orphan claimed + merged + deleted
    assert "c1" in outbox.read_text(encoding="utf-8")   # merged into the live outbox


def test_import_alone_does_not_recover_orphan(tmp_path):
    """F2 (removal side): merely IMPORTING broker_client must NOT run the sweep — a
    stale orphan is left untouched by import alone (proves recovery is no longer at
    module top-level). Uses a fresh subprocess so the already-imported module here
    doesn't mask it."""
    import os
    import subprocess
    import sys as _sys
    import time as _t
    outbox = tmp_path / "outbox.jsonl"
    orphan = tmp_path / "outbox.jsonl.processing-cafe"
    orphan.write_text(
        '{"client_event_id":"c","endpoint":"/api/agent/x/ack",'
        '"payload":{"event_ids":[1]},"enqueued_ts":1}\n', encoding="utf-8")
    old = _t.time() - 120
    os.utime(orphan, (old, old))
    env = dict(os.environ, BROKER_OUTBOX_PATH=str(outbox),
               BROKER_BASE_URL="http://127.0.0.1:8787")
    code = "import sys; sys.path.insert(0, r'%s'); import broker_client" % str(_RELAY_HOOKS)
    subprocess.run([_sys.executable, "-c", code], env=env, check=True,
                   capture_output=True, timeout=30)
    assert orphan.exists()   # import did NOT sweep it (F2: recovery is flush-gated)


# ------------- F1 replay half (#18 HIGH): flush_outbox 4xx handling ----------

def _seed_outbox(tmp_path, monkeypatch, endpoint, cid):
    import time as _t
    outbox = tmp_path / "outbox.jsonl"
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(outbox))
    outbox.write_text(
        '{"client_event_id":"%s","endpoint":"%s","payload":{"event_ids":[7]},'
        '"enqueued_ts":%d}\n' % (cid, endpoint, int(_t.time() * 1000)),
        encoding="utf-8")
    monkeypatch.setattr(broker_client.time, "sleep", lambda *_: None)
    return outbox


def _raise_http(code):
    from urllib.error import HTTPError

    def fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, code, "x", {}, io.BytesIO(b"body"))
    return fake_urlopen


def test_flush_ack_404_retained_through_ttl(monkeypatch, tmp_path):
    """#18 HIGH: an outboxed /ack line replayed against a broker that still predates
    /ack (404) is RETAINED (survivors) for the 24h TTL window, NOT dropped — so the
    mirror self-heals once the broker is upgraded (was dropped_corrupted on 1st flush)."""
    outbox = _seed_outbox(tmp_path, monkeypatch, "/api/agent/broker/ack", "c1")
    monkeypatch.setattr(broker_client, "urlopen", _raise_http(404))
    counts = broker_client.flush_outbox()
    assert counts["dropped_corrupted"] == 0
    assert counts["failed"] == 1
    assert "c1" in outbox.read_text(encoding="utf-8")     # retained for retry


def test_flush_ack_405_retained(monkeypatch, tmp_path):
    """405 (method absent on an old broker) is treated the same transient way as 404."""
    outbox = _seed_outbox(tmp_path, monkeypatch, "/api/agent/broker/ack", "c1b")
    monkeypatch.setattr(broker_client, "urlopen", _raise_http(405))
    counts = broker_client.flush_outbox()
    assert counts["dropped_corrupted"] == 0
    assert counts["failed"] == 1
    assert "c1b" in outbox.read_text(encoding="utf-8")


def test_flush_ack_422_dropped(monkeypatch, tmp_path):
    """A PERMANENT 4xx (422 malformed) on an /ack replay drops immediately (bounded),
    not retained — so a genuinely bad body can't churn the outbox for 24h."""
    outbox = _seed_outbox(tmp_path, monkeypatch, "/api/agent/broker/ack", "c2")
    monkeypatch.setattr(broker_client, "urlopen", _raise_http(422))
    counts = broker_client.flush_outbox()
    assert counts["dropped_corrupted"] == 1
    assert counts["failed"] == 0
    assert (not outbox.exists()) or "c2" not in outbox.read_text(encoding="utf-8")


def test_flush_ack_floor_422_retained_skew(monkeypatch, tmp_path):
    """Regression: a FLOOR-BEARING /ack body that 422s during a
    NEW-hook -> OLD-broker deploy skew (the old broker has /ack from WS-B-broker-1 but
    predates the floor field, so a floor-only {event_ids:[], floor} body 422s on its
    event_ids min_length=1) is RETAINED (self-heals once the broker upgrades), NOT dropped.
    Contrast test_flush_ack_422_dropped: a NON-floor 422 (genuine malformed body) still drops."""
    import time as _t
    outbox = tmp_path / "outbox.jsonl"
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(outbox))
    outbox.write_text(
        '{"client_event_id":"cf","endpoint":"/api/agent/broker/ack",'
        '"payload":{"event_ids":[],"floor":100},"enqueued_ts":%d}\n' % int(_t.time() * 1000),
        encoding="utf-8")
    monkeypatch.setattr(broker_client.time, "sleep", lambda *_: None)
    monkeypatch.setattr(broker_client, "urlopen", _raise_http(422))
    counts = broker_client.flush_outbox()
    assert counts["dropped_corrupted"] == 0
    assert counts["failed"] == 1
    assert "cf" in outbox.read_text(encoding="utf-8")     # retained for retry post-upgrade


def test_flush_nonack_404_dropped(monkeypatch, tmp_path):
    """Only /ack 404/405 is retained; a 404 on a NON-ack endpoint still drops
    (the transient-retain carve-out is scoped to the ack mirror)."""
    outbox = _seed_outbox(tmp_path, monkeypatch, "/api/event", "c3")
    monkeypatch.setattr(broker_client, "urlopen", _raise_http(404))
    counts = broker_client.flush_outbox()
    assert counts["dropped_corrupted"] == 1
    assert counts["failed"] == 0


def test_flush_ack_404_then_200_self_heals(monkeypatch, tmp_path):
    """End-to-end self-heal: /ack line retained on a 404 flush, then flushed clean
    once the broker is upgraded (200) — the deploy-skew window closes on its own."""
    outbox = _seed_outbox(tmp_path, monkeypatch, "/api/agent/broker/ack", "c4")
    state = {"up": False}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"acked_now":1,"requested":1}'

    from urllib.error import HTTPError

    def fake_urlopen(req, timeout=None):
        if not state["up"]:
            raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))
        return _Resp()

    monkeypatch.setattr(broker_client, "urlopen", fake_urlopen)
    c1 = broker_client.flush_outbox()
    assert c1["failed"] == 1 and "c4" in outbox.read_text(encoding="utf-8")   # retained
    state["up"] = True
    c2 = broker_client.flush_outbox()
    assert c2["flushed"] == 1
    assert (not outbox.exists()) or "c4" not in outbox.read_text(encoding="utf-8")  # drained


def test_flush_ack_stale_dropped_without_post(monkeypatch, tmp_path):
    """The retained /ack line ages on its ORIGINAL enqueued_ts: once older than the
    24h TTL it drops as dropped_stale BEFORE any POST — proving the retain is bounded
    (guards a future refactor that refreshes enqueued_ts on retain)."""
    import time as _t
    outbox = tmp_path / "outbox.jsonl"
    monkeypatch.setenv("BROKER_OUTBOX_PATH", str(outbox))
    stale_ts = int(_t.time() * 1000) - broker_client.TTL_MS - 60_000    # well past 24h
    outbox.write_text(
        '{"client_event_id":"cs","endpoint":"/api/agent/broker/ack",'
        '"payload":{"event_ids":[1]},"enqueued_ts":%d}\n' % stale_ts, encoding="utf-8")

    def _boom(*a, **k):
        raise AssertionError("stale entry must NOT be POSTed")

    monkeypatch.setattr(broker_client, "urlopen", _boom)
    monkeypatch.setattr(broker_client.time, "sleep", lambda *_: None)
    counts = broker_client.flush_outbox()
    assert counts["dropped_stale"] == 1
    assert counts["failed"] == 0 and counts["flushed"] == 0
    assert (not outbox.exists()) or "cs" not in outbox.read_text(encoding="utf-8")


def test_flush_ack_404_repeat_retain_no_duplicate(monkeypatch, tmp_path):
    """Repeated 404 flushes retain EXACTLY ONE copy of the ack (no outbox growth
    before the broker upgrades) — the retained line is re-appended once per flush,
    not duplicated."""
    outbox = _seed_outbox(tmp_path, monkeypatch, "/api/agent/broker/ack", "cr")
    monkeypatch.setattr(broker_client, "urlopen", _raise_http(404))
    broker_client.flush_outbox()
    broker_client.flush_outbox()
    broker_client.flush_outbox()
    lines = [ln for ln in outbox.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 1
    assert lines[0].count('"client_event_id":"cr"') == 1
