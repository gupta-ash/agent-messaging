# -*- coding: utf-8 -*-
"""Unit tests for the lossless inter-agent delivery refactor (plan
quizzical-launching-fox).

Covers:
  - The confirmed-set primitive (floor + acked-above-gap, per-event collapse).
  - The relay_ack confirmed-set write (via server.py's _write_confirmed_set_locked).
  - The do_receive gap-aware backstop: transcript-grep dedup, fail-toward-delivery,
    and the CRITICAL per-event invariant (an isolated ack above a gap must NOT
    confirm the gapped id; the backstop must still deliver it).

Root conftest exports this repository's relay library and plugin into an isolated
HOME before collection. Per-test fixtures isolate state and stub inbox fetches;
mirror-specific tests supply their own broker client. Named delivery state uses
MY_NAME; transcript/session inputs continue to use MY_SID. No live HOME or broker
is used.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

# --- import shared (receive-hook library) ------------------------------------
RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))
import shared  # noqa: E402

# --- load server.py (the MCP push server) as a module ------------------------
_SERVER_PATH = Path.home() / ".claude" / "plugins" / "local-relay-channel" / "server.py"
_spec = importlib.util.spec_from_file_location("lrc_server", str(_SERVER_PATH))
lrc_server = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lrc_server)


MY_SID = "aaaaaaaa-1111-4222-8333-444444444444"
MY_SID_2 = "bbbbbbbb-5555-4666-8777-888888888888"
MY_NAME = "trading_visual"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def relay_env(tmp_path, monkeypatch):
    """Redirect ALL relay state (shared + server) into tmp_path. Returns the
    tmp relay dir.

    CRITICAL: relay_ack (server.py) and the hook (shared.py) write/read the SAME
    confirmed-set file across processes. server.py resolves its paths from
    Path.home()/.claude/relay/..., shared.py from shared.RELAY_DIR. We pin BOTH
    to tmp_path/.claude/relay so the cross-writer contract is faithfully tested
    (server's _confirmed_dir() == shared's _confirmed_dir())."""
    claude_dir = tmp_path / ".claude"
    relay_dir = claude_dir / "relay"
    cursors_dir = relay_dir / "cursors"
    confirmed_dir = relay_dir / "confirmed"
    projects_dir = claude_dir / "projects"
    for d in (relay_dir, cursors_dir, confirmed_dir, projects_dir):
        d.mkdir(parents=True, exist_ok=True)

    # shared.py module-level paths.
    monkeypatch.setattr(shared, "RELAY_DIR", relay_dir)
    monkeypatch.setattr(shared, "CURSORS_DIR", cursors_dir)
    monkeypatch.setattr(shared, "CLAUDE_DIR", claude_dir)
    monkeypatch.setattr(shared, "REGISTRY_PATH", relay_dir / "registry.json")
    monkeypatch.setattr(shared, "broker_client", None)

    # server.py resolves confirmed paths from Path.home() — redirect HOME so its
    # _confirmed_dir() (Path.home()/.claude/relay/confirmed) == shared's
    # confirmed dir. Both now land in tmp_path/.claude/relay/confirmed.
    monkeypatch.setattr(lrc_server.Path, "home", staticmethod(lambda: tmp_path),
                        raising=False)
    return relay_dir


def _seed_cursor(relay_dir: Path, sid: str, value: int) -> None:
    p = relay_dir / "cursors" / f"{sid}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"last_seen_event_id": value}), encoding="utf-8")


def _seed_confirmed(relay_dir: Path, sid: str, floor: int, acked: list,
                    *, epoch_name="__absent__", epoch_broken="__absent__") -> None:
    p = relay_dir / "confirmed" / f"{sid}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    obj = {"floor": floor, "acked": acked}
    # Legacy epoch fields are optional; named delivery no longer stamps them.
    if epoch_name != "__absent__":
        obj["epoch_name"] = epoch_name
    if epoch_broken != "__absent__":
        obj["epoch_broken"] = epoch_broken
    p.write_text(json.dumps(obj), encoding="utf-8")


def _read_confirmed_file(relay_dir: Path, sid: str) -> dict:
    p = relay_dir / "confirmed" / f"{sid}.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _write_transcript(claude_dir: Path, sid: str, rendered_event_ids: list) -> Path:
    """Write a realistic transcript JSONL containing channel-rendered lines for
    each event id in `rendered_event_ids`. Returns the path."""
    proj = claude_dir / "projects" / "E--some-encoded-project"
    proj.mkdir(parents=True, exist_ok=True)
    path = proj / f"{sid}.jsonl"
    lines = []
    # A non-channel user line (should be ignored by the grep).
    lines.append(json.dumps({"type": "user", "message": {"content": "hello"}}))
    for eid in rendered_event_ids:
        content = (
            f'<channel source="local-relay-channel" from="sender" to="{MY_NAME}" '
            f'kind="agent_message" ts="1780000000000" '
            f'channel="#cc-session-relay" event_id="{eid}">\n'
            f'[relay] from=sender to={MY_NAME} event_id={eid} sent=now '
            f'kind=agent_message\n\nbody for {eid}'
        )
        lines.append(json.dumps({
            "type": "user",
            "origin": {"kind": "channel", "server": "local-relay-channel"},
            "message": {"content": content},
        }))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _broker_msg(eid: int) -> dict:
    return {
        "event_id": eid,
        "from_agent": "sender",
        "content": f"body for {eid}",
        "ts": 1780000000000,
        "kind": "agent_message",
    }


def _broker_msg_kind(eid: int, kind: str) -> dict:
    """Like _broker_msg but with an explicit `kind` (agent_message / agent_reply
    / handoff) — for the multi-kind paged-sweep repro tests."""
    m = _broker_msg(eid)
    m["kind"] = kind
    return m


def _run_do_receive(monkeypatch, sid: str, broker_rows_by_identity: dict,
                    name: str = MY_NAME, hook_event="PostToolUse") -> str:
    """Drive shared.do_receive with a fake stdin payload + stubbed broker.
    `broker_rows_by_identity` maps the identity arg (name or sid) -> list[dict].
    Returns the captured stdout (the hook JSON response).

    MULTI-KIND FIX: the sweep now pages EACH kind via fetch_pending_messages
    (per-kind primitive), so we stub THAT (not the 3-in-1 merge). The stub
    filters by `kind` (rows carry kind=agent_message by default via _broker_msg),
    honors the EXCLUSIVE `since`, and returns the canonical dict shape with
    fetch_ok=True (genuine-success signal)."""
    # Stub name resolution so the friendly-name poll is deterministic.
    monkeypatch.setattr(shared, "_resolve_name_from_registry", lambda s: name)

    def fake_fetch_kind(identity, since, kind="agent_message", limit=20,
                        broker_base_url=None):
        rows = broker_rows_by_identity.get(identity, [])
        above = [r for r in rows
                 if r.get("event_id", 0) > since and r.get("kind", "agent_message") == kind]
        above.sort(key=lambda r: r.get("event_id", 0))
        return {"messages": above[:int(limit)], "next_cursor": None, "fetch_ok": True}

    monkeypatch.setattr(shared, "fetch_pending_messages", fake_fetch_kind)

    payload = json.dumps({"session_id": sid, "prompt": ""})
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)
    shared.do_receive(hook_event)
    return buf.getvalue()


def _injected_event_ids(hook_output: str) -> set:
    """Parse the hook response JSON and extract the event ids injected via
    format_messages_for_injection ('(event NNNN)')."""
    try:
        obj = json.loads(hook_output)
    except json.JSONDecodeError:
        return set()
    ctx = (obj.get("hookSpecificOutput") or {}).get("additionalContext") or ""
    import re
    return {int(m) for m in re.findall(r"\(event (\d+)\)", ctx)}


# ---------------------------------------------------------------------------
# Pure confirmed-set / collapse unit tests (no I/O)
# ---------------------------------------------------------------------------

def test_collapse_contiguous_advances_floor():
    """A contiguous run above floor collapses INTO floor; gapped ids stay in acked."""
    assert shared._collapse_confirmed(2961, {2962}) == (2962, [])
    assert shared._collapse_confirmed(2961, {2962, 2963, 2964}) == (2964, [])
    assert shared._collapse_confirmed(2961, {2962, 2963, 2967}) == (2963, [2967])


def test_collapse_isolated_ack_above_gap_does_not_advance_floor():
    """CRITICAL per-event invariant: ack 2967/2969 with floor 2961 (gap at 2962)
    keeps floor at 2961; both gapped acks stay in acked."""
    assert shared._collapse_confirmed(2961, {2967, 2969}) == (2961, [2967, 2969])


def test_server_collapse_matches_shared_collapse():
    """server.py + shared.py must implement IDENTICAL collapse semantics (they
    write the same shared file)."""
    for floor, acked in [(2961, {2962}), (2961, {2967, 2969}),
                         (0, {1, 2, 3, 7}), (100, set())]:
        assert (lrc_server._collapse_confirmed(floor, set(acked))
                == shared._collapse_confirmed(floor, set(acked)))


# ---------------------------------------------------------------------------
# Test 1 — GAP-CASE (primary: the lossless invariant)
# ---------------------------------------------------------------------------

def test_gap_case_backstop_delivers_only_the_genuine_drop(relay_env, monkeypatch):
    """confirmed-set floor=2961, acked={2967,2969}; broker has 2962/2967/2969 to
    me; transcript renders 2967/2969 but NOT 2962 -> backstop injects ONLY 2962;
    2967/2969 not re-injected; floor/acked updated correctly."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent  # tmp_path/.claude

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[2967, 2969])
    # Transcript renders 2967 + 2969 (model saw them) but NOT 2962 (the drop).
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[2967, 2969])

    broker = {
        MY_NAME: [_broker_msg(2962), _broker_msg(2967), _broker_msg(2969)],
        MY_SID: [],  # legacy side-poll returns nothing
    }
    out = _run_do_receive(monkeypatch, MY_SID, broker)

    injected = _injected_event_ids(out)
    assert injected == {2962}, f"expected only 2962 injected, got {injected}"

    # ACK-DRIVEN: 2962 was injected (emitted) but NOT rendered and NOT acked, so
    # it is NOT confirmed — the floor does NOT advance on a bare emit. 2967/2969
    # were already in acked (seeded); nothing new is grep-confirmed this fire, so
    # the confirmed-set is unchanged. 2962 (the genuine drop) re-injects next fire
    # until the model broker_acks it or it renders.
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs["floor"] == 2961, f"floor must NOT advance on bare emit, got {cs['floor']}"
    assert set(cs["acked"]) == {2967, 2969}, f"acked unchanged (already-acked ids), got {cs['acked']}"


# ---------------------------------------------------------------------------
# Test 2 — State #1: relay_ack -> confirmed -> hook stays silent
# ---------------------------------------------------------------------------

def test_state1_relay_ack_marks_confirmed_and_hook_does_not_inject(relay_env, monkeypatch):
    """relay_ack([2967]) writes the confirmed-set; a subsequent hook fire that
    sees 2967 from the broker does NOT inject it (already confirmed)."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent  # tmp_path/.claude

    # Floor starts at 2966 so 2967 is the next contiguous id.
    _seed_confirmed(relay_dir, MY_NAME, floor=2966, acked=[])
    # Model acks 2967 via the relay_ack tool path (server.py helper, shared file).
    res = lrc_server._write_confirmed_set_locked(MY_NAME, [2967])
    assert res["floor"] == 2967, res  # contiguous -> collapses into floor

    # No transcript needed (it's already confirmed; grep should not even matter).
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])

    broker = {MY_NAME: [_broker_msg(2967)], MY_SID: []}
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == set(), "2967 was acked; must not inject"


# ---------------------------------------------------------------------------
# Test 3 — State #2: no push, not acked, not in transcript -> injected
# ---------------------------------------------------------------------------

def test_state2_drop_not_in_transcript_is_injected(relay_env, monkeypatch):
    """2962 not acked + not in transcript -> injected (genuine drop). ACK-DRIVEN:
    a bare emit does NOT confirm it — the floor stays put and 2962 re-injects on
    the next fire until the model broker_acks it or it renders."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent  # tmp_path/.claude

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # empty transcript

    broker = {MY_NAME: [_broker_msg(2962)], MY_SID: []}
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == {2962}
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    # ACK-DRIVEN: emitted but NOT rendered/acked → NOT confirmed; floor unchanged.
    assert cs.get("floor", 2961) == 2961, f"floor must NOT advance on bare emit, got {cs.get('floor')}"
    assert 2962 not in set(cs.get("acked", [])), "emitted-but-unconfirmed id must not be acked"


# ---------------------------------------------------------------------------
# Test 3b — ACK-DRIVEN contract: unconfirmed re-delivers until broker_ack
# ---------------------------------------------------------------------------

def test_unconfirmed_redelivers_until_broker_ack(relay_env, monkeypatch):
    """The agreed deterministic contract, end-to-end over the do_receive hook:
    a delivered-but-unconfirmed message RE-DELIVERS every fire (a bare emit never
    confirms it) and STOPS only once the model broker_acks it. Never a silent loss,
    and no cross-agent traffic — broker_ack is a local cursor advance."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # never rendered
    broker = {MY_NAME: [_broker_msg(2962)], MY_SID: []}

    # Fire 1: injected; the bare emit does NOT confirm -> floor unchanged.
    assert _injected_event_ids(_run_do_receive(monkeypatch, MY_SID, broker)) == {2962}
    assert _read_confirmed_file(relay_dir, MY_NAME).get("floor") == 2961

    # Fire 2: STILL unconfirmed -> RE-injected (deterministic re-delivery).
    assert _injected_event_ids(_run_do_receive(monkeypatch, MY_SID, broker)) == {2962}
    assert _read_confirmed_file(relay_dir, MY_NAME).get("floor") == 2961

    # Model broker_acks 2962 (contiguous with floor 2961 -> collapses into floor).
    lrc_server._write_confirmed_set_locked(MY_NAME, [2962])
    assert _read_confirmed_file(relay_dir, MY_NAME)["floor"] == 2962

    # Fire 3: confirmed -> NOT re-injected. The re-delivery loop terminates on ack.
    assert _injected_event_ids(_run_do_receive(monkeypatch, MY_SID, broker)) == set()


# ---------------------------------------------------------------------------
# Test 4 — State #3-found: in transcript -> confirmed, not injected
# ---------------------------------------------------------------------------

def test_state3_found_in_transcript_confirmed_not_injected(relay_env, monkeypatch):
    """2962 IS in transcript (push delivered + rendered, model just didn't ack)
    -> confirmed, NOT re-injected."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent  # tmp_path/.claude

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[2962])

    broker = {MY_NAME: [_broker_msg(2962)], MY_SID: []}
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == set(), "2962 rendered; must not re-inject"
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs["floor"] == 2962, "grep-found id is still confirmed (floor advances)"


# ---------------------------------------------------------------------------
# Test 5 — grep-fail (unreadable transcript) -> inject (fail toward delivery)
# ---------------------------------------------------------------------------

def test_grep_fail_injects_fail_toward_delivery(relay_env, monkeypatch):
    """Transcript unreadable / missing -> read_ok False -> treat as NOT rendered
    -> inject (the accepted duplicate; never a silent loss)."""
    relay_dir = relay_env

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    # NO transcript written -> _find_own_transcript returns None -> read_ok False.
    # Defensive double-guard: also force the grep to report failure.
    monkeypatch.setattr(shared, "_rendered_event_ids_in_transcript",
                        lambda s: (set(), False))

    broker = {MY_NAME: [_broker_msg(2962)], MY_SID: []}
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == {2962}, "grep-fail must inject (fail toward delivery)"


def test_grep_fail_via_missing_transcript_injects(relay_env, monkeypatch):
    """Same fail-toward-delivery, but exercising the REAL _find_own_transcript
    miss path (no transcript file on disk) rather than a monkeypatch."""
    relay_dir = relay_env
    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    # Deliberately do NOT write any transcript file.
    broker = {MY_NAME: [_broker_msg(2962)], MY_SID: []}
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == {2962}


# ---------------------------------------------------------------------------
# Test 6 — relay_ack per-event: ack above a gap does NOT advance floor
# ---------------------------------------------------------------------------

def test_relay_ack_per_event_keeps_gap(relay_env, monkeypatch):
    """ack([2969]) with floor=2961 -> acked={2969}, floor STAYS 2961
    (gap at 2962-2968 preserved). Then the backstop still delivers 2962."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent  # tmp_path/.claude

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    res = lrc_server._write_confirmed_set_locked(MY_NAME, [2969])
    assert res["floor"] == 2961, "isolated ack above gap must NOT advance floor"
    assert res["acked"] == [2969]

    cs = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs["floor"] == 2961 and cs["acked"] == [2969]

    # Backstop: broker has 2962 (dropped) + 2969 (acked); transcript renders
    # neither (2962 is a genuine drop; 2969 was acked but transcript-less here).
    # 2962 is below the gap and unconfirmed -> delivered; 2969 already in acked.
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])
    broker = {MY_NAME: [_broker_msg(2962), _broker_msg(2969)], MY_SID: []}
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == {2962}, "2962 (gap) delivered; 2969 (acked) not"


# ---------------------------------------------------------------------------
# relay_ack input validation (server.py) — ints only, skip non-ints, cap
# ---------------------------------------------------------------------------

def test_relay_ack_skips_non_ints_and_bools(relay_env):
    """_write_confirmed_set_locked ignores non-int + bool + non-positive ids."""
    relay_dir = relay_env
    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    res = lrc_server._write_confirmed_set_locked(MY_NAME, [2962, "2963", None, True, -5, 0, 2964])
    # Only 2962 + 2964 valid; 2962 contiguous -> floor 2962; 2964 gapped -> acked.
    assert res["floor"] == 2962
    assert res["acked"] == [2964]


def test_real_transcript_grep_parses_quoted_event_ids(relay_env):
    """The REAL _rendered_event_ids_in_transcript parses event_id="NNNN" from
    channel-rendered lines (json.loads per line) and ignores non-channel lines."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent  # tmp_path/.claude
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[2967, 2969, 3001])
    rendered, ok = shared._rendered_event_ids_in_transcript(MY_SID)
    assert ok is True
    assert rendered == {2967, 2969, 3001}


# ===========================================================================
# CROSS-FAMILY REVIEW REGRESSIONS — FIX 1 (CRIT-1), FIX 2 (HIGH-1), FIX 3 (MED-1)
# ===========================================================================

def _run_do_receive_paged(monkeypatch, sid, broker_rows_by_identity,
                          page_limit, name=MY_NAME, hook_event="PostToolUse"):
    """Like _run_do_receive but pins the sweep's per-page limit to `page_limit`
    (via shared.SWEEP_PAGE_LIMIT) and the fake per-kind fetch faithfully honors
    that limit + the EXCLUSIVE `since` + the requested `kind` — so the sweep MUST
    page (re-query with since=max-returned-id) to drain a backlog larger than one
    page, and a short page (< limit) correctly signals exhaustion. Returns
    (stdout, n_fetch_calls)."""
    monkeypatch.setattr(shared, "_resolve_name_from_registry", lambda s: name)
    # Pin the production page size down so a modest backlog forces real paging.
    monkeypatch.setattr(shared, "SWEEP_PAGE_LIMIT", page_limit)
    calls = {"n": 0}

    def fake_fetch_kind(identity, since, kind="agent_message", limit=20,
                        broker_base_url=None):
        calls["n"] += 1
        rows = broker_rows_by_identity.get(identity, [])
        # Broker contract: to-me rows of THIS kind with event_id > since, ASC,
        # capped at `limit`. Honoring the requested limit exactly is what makes
        # "page shorter than limit => exhausted" hold. fetch_ok=True = success.
        above = sorted(
            (r for r in rows
             if r.get("event_id", 0) > since and r.get("kind", "agent_message") == kind),
            key=lambda r: r["event_id"])
        return {"messages": above[:int(limit)], "next_cursor": None, "fetch_ok": True}

    monkeypatch.setattr(shared, "fetch_pending_messages", fake_fetch_kind)
    payload = json.dumps({"session_id": sid, "prompt": ""})
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)
    shared.do_receive(hook_event)
    return buf.getvalue(), calls["n"]


def test_crit1_floor_pinned_low_30_sparse_to_me_all_delivered(relay_env, monkeypatch):
    """FIX 1 (CRIT-1): floor pinned LOW (2961) with 30 to-me messages at SPARSE
    global ids (interleaved in the global stream with OTHER sessions' ids that
    will never be in this recipient's acked). The old `floor+1 in acked` advance
    pinned floor at the first to-me id and a single limit=20 page starved the
    newest. Now the paged sweep delivers ALL 30 across one fire (none starved);
    ACK-DRIVEN, the floor advances only when the model acks/renders, never on a
    bare emit."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # none rendered

    # 30 sparse to-me ids well above floor, none contiguous with floor+1 (so the
    # OLD collapse would never advance). Spread across a wide global range; the
    # gaps stand for OTHER sessions' global events the recipient never sees.
    to_me_ids = [2962, 2967, 2970, 2999, 3050, 3051, 3120, 3200, 3333, 3400,
                 3500, 3600, 3601, 3700, 3800, 3900, 4000, 4100, 4200, 4300,
                 4400, 4500, 4600, 4700, 4800, 4900, 5000, 5100, 5200, 5300]
    assert len(to_me_ids) == 30
    broker = {MY_NAME: [_broker_msg(e) for e in to_me_ids], MY_SID: []}

    # Use a small page_limit (7) so the 30-row backlog forces multiple pages —
    # proves the sweep PAGES + advances instead of starving the tail at >limit.
    out, n_calls = _run_do_receive_paged(monkeypatch, MY_SID, broker, page_limit=7)

    injected = _injected_event_ids(out)
    assert injected == set(to_me_ids), (
        f"ALL 30 sparse to-me ids must be delivered (none starved); "
        f"missing={set(to_me_ids) - injected}, extra={injected - set(to_me_ids)}"
    )
    # The primary-identity sweep alone needed multiple round-trips (30 rows / 7
    # per page => >=5 fetch calls just for MY_NAME). Proves paging engaged.
    assert n_calls >= 5, f"expected multi-page sweep; only {n_calls} fetch calls"

    # floor advanced to the max examined id; acked pruned (all <= floor).
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    # ACK-DRIVEN: all 30 were injected (delivered this fire, none starved) but none
    # rendered/acked → NONE confirmed; the floor stays pinned at the seed. They
    # re-inject next fire until the model broker_acks them.
    assert cs.get("floor", 2961) == 2961, f"floor must NOT advance on bare emit, got {cs.get('floor')}"
    assert set(cs.get("acked", [])) == set(), f"acked stays empty (nothing confirmed), got {cs.get('acked')}"


def test_successive_fires_drain_backlog_via_ack(relay_env, monkeypatch):
    """ACK-DRIVEN drain: a >limit backlog above a pinned floor is fully DELIVERED
    in one fire (no starvation), but only CONFIRMED as the model acks it. Fire 1
    injects all 25 (floor stays pinned, nothing confirmed); the model broker_acks
    them; fire 2 re-injects NONE (all confirmed)."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent

    _seed_confirmed(relay_dir, MY_NAME, floor=100, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])

    # 25 to-me ids (> the old hardcoded limit=20) at sparse global positions.
    to_me_ids = [101 + i * 13 for i in range(25)]  # 101,114,127,...,413
    assert len(to_me_ids) == 25
    broker = {MY_NAME: [_broker_msg(e) for e in to_me_ids], MY_SID: []}

    # FIRE 1: all 25 delivered in one fire; ACK-DRIVEN -> none confirmed yet.
    out = _run_do_receive(monkeypatch, MY_SID, broker)
    injected = _injected_event_ids(out)
    assert injected == set(to_me_ids), (
        f">limit backlog must all deliver in one fire; missing={set(to_me_ids) - injected}"
    )
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs.get("floor", 100) == 100, "floor must NOT advance on bare emit"
    assert set(cs.get("acked", [])) == set(), "nothing confirmed until acked"

    # The model broker_acks all 25 (server.py writer == the broker_ack tool path).
    lrc_server._write_confirmed_set_locked(MY_NAME, to_me_ids)

    # FIRE 2: every id is now confirmed (id <= floor OR in acked) -> NONE re-injects.
    out2 = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out2) == set(), (
        f"acked backlog must NOT re-inject; got {_injected_event_ids(out2)}"
    )
    cs2 = _read_confirmed_file(relay_dir, MY_NAME)
    acked2 = set(cs2.get("acked", []))
    assert all(e <= cs2["floor"] or e in acked2 for e in to_me_ids), (
        f"all 25 must be confirmed after ack; floor={cs2['floor']} acked={cs2['acked']}"
    )


def test_high1_emit_failure_does_not_record_injected_id(relay_env, monkeypatch):
    """FIX 2 (HIGH-1): emit fails for a to_inject message -> the confirmed-set
    does NOT record that id (floor stays at the seed; id not in acked), so the
    NEXT fire (with a working emit) re-delivers it. Bias to duplicate, never loss."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent

    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # not rendered -> inject

    # Monkeypatch emit to RAISE when there is content to inject (non-empty
    # additionalContext). do_receive must catch it + NOT confirm the injected id.
    real_emit = shared.emit_hook_response
    emit_calls = {"with_content": 0}

    def failing_emit(hook_event_name, additional_context):
        if additional_context:
            emit_calls["with_content"] += 1
            raise RuntimeError("simulated stdout/emit failure")
        return real_emit(hook_event_name, additional_context)

    monkeypatch.setattr(shared, "emit_hook_response", failing_emit)

    broker = {MY_NAME: [_broker_msg(2962)], MY_SID: []}
    # Drive do_receive directly (emit raises internally; do_receive degrades).
    monkeypatch.setattr(shared, "_resolve_name_from_registry", lambda s: MY_NAME)

    def fake_fetch_kind(identity, since, kind="agent_message", limit=20,
                        broker_base_url=None):
        rows = broker.get(identity, [])
        above = [r for r in rows
                 if r.get("event_id", 0) > since and r.get("kind", "agent_message") == kind]
        above.sort(key=lambda r: r.get("event_id", 0))
        return {"messages": above[:int(limit)], "next_cursor": None, "fetch_ok": True}

    monkeypatch.setattr(shared, "fetch_pending_messages", fake_fetch_kind)
    monkeypatch.setattr(sys, "stdin",
                        io.StringIO(json.dumps({"session_id": MY_SID, "prompt": ""})))
    monkeypatch.setattr(sys, "stdout", io.StringIO())
    shared.do_receive("PostToolUse")  # must NOT raise (silent-degrade)

    # NON-VACUITY GUARD: fire 1 must have ACTUALLY attempted to emit injected
    # content (i.e. 2962 was fetched + queued to inject), so the failing-emit
    # path was genuinely exercised — not silently skipped by an empty sweep.
    assert emit_calls["with_content"] >= 1, (
        "fire 1 must have attempted to emit injected content (2962); "
        "an empty sweep would make this test vacuous"
    )

    # The injected id 2962 must NOT have been confirmed (emit failed).
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    floor_after = cs.get("floor", 2961) if cs else 2961
    acked_after = set(cs.get("acked", [])) if cs else set()
    assert floor_after < 2962, f"floor must NOT advance past undelivered 2962, got {floor_after}"
    assert 2962 not in acked_after, f"2962 must NOT be recorded as confirmed, got acked={acked_after}"

    # NEXT fire with a WORKING emit re-delivers 2962 (proves no silent loss).
    monkeypatch.setattr(shared, "emit_hook_response", real_emit)
    out2 = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out2) == {2962}, "2962 must re-deliver on the next fire"




def test_nameless_ack_seeds_legacy_cursor(relay_env, monkeypatch):
    """A nameless session retains its legacy floor while parking a gapped ack."""
    relay_dir = relay_env
    _seed_cursor(relay_dir, MY_SID_2, 2900)
    # Ack a GAPPED id well above the cursor; floor stays at the seeded 2900,
    # the gapped id parks in acked (NOT folded into a floor=0 contiguous run).
    res = lrc_server._write_confirmed_set_locked(MY_SID_2, [2950])
    assert res["floor"] == 2900, f"floor must remain seeded at 2900 (NOT 0), got {res['floor']}"
    assert res["acked"] == [2950], f"gapped ack parks in acked above the seeded floor, got {res['acked']}"


# ===========================================================================
# MULTI-KIND PAGED-SWEEP FIX — (a) per-kind truncation + (b) per-kind degrade
# event_id is a GLOBAL counter; the inbox is queried PER KIND. The floor may
# only advance to the boundary provably-drained for EVERY kind.
# ===========================================================================

def _run_do_receive_multikind(monkeypatch, sid, rows_by_kind, *,
                              page_limit=None, max_pages=None,
                              fetch_override=None, name=MY_NAME,
                              hook_event="PostToolUse"):
    """Drive do_receive with a PER-KIND broker stub.

    `rows_by_kind`: {kind -> list[dict]} for the PRIMARY identity (name). The
    legacy side-poll (by sid) always returns empty.

    The per-kind fetch (shared.fetch_pending_messages) honors the requested
    `kind`, the EXCLUSIVE `since`, and the `limit`; returns the canonical dict
    with fetch_ok=True. `fetch_override(identity, since, kind, limit)` — when
    given — may return a custom dict (e.g. a FAILED fetch: fetch_ok=False) for
    specific kinds, else None to fall through to the default.

    Returns (stdout, injected_ids_set)."""
    monkeypatch.setattr(shared, "_resolve_name_from_registry", lambda s: name)
    if page_limit is not None:
        monkeypatch.setattr(shared, "SWEEP_PAGE_LIMIT", page_limit)
    if max_pages is not None:
        monkeypatch.setattr(shared, "SWEEP_MAX_PAGES", max_pages)

    def fake_fetch_kind(identity, since, kind="agent_message", limit=20,
                        broker_base_url=None):
        if fetch_override is not None:
            ov = fetch_override(identity, since, kind, limit)
            if ov is not None:
                return ov
        rows = rows_by_kind.get(kind, []) if identity == name else []
        above = sorted((r for r in rows if r.get("event_id", 0) > since),
                       key=lambda r: r["event_id"])
        return {"messages": above[:int(limit)], "next_cursor": None, "fetch_ok": True}

    monkeypatch.setattr(shared, "fetch_pending_messages", fake_fetch_kind)
    payload = json.dumps({"session_id": sid, "prompt": ""})
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)
    shared.do_receive(hook_event)
    out = buf.getvalue()
    return out, _injected_event_ids(out)


def test_multikind_truncation_high_other_kind_does_not_bury_truncated_tail(
        relay_env, monkeypatch):
    """MULTI-KIND FIX (a) — per-kind truncation. From floor=0:
      - kind agent_message has ids 1..150 (>> the per-page limit, so it TRUNCATES
        and CAPS under a low SWEEP_MAX_PAGES).
      - kind handoff has a SINGLE high id 10000.
    The OLD merge+single-cursor sweep would compute one page_max across the merged
    rows; the high handoff id 10000 advanced the floor PAST agent_message's
    un-returned tail (101..150 buried, id < floor, never injected/acked).

    The FIX pages each kind on its own cursor and sets examined_max = MIN across
    kinds of each kind's safe boundary: agent_message is CAPPED (boundary = its
    max fetched id), handoff is EXHAUSTED (boundary = +inf). examined_max =
    agent_message's max, so the floor NEVER jumps to 10000. handoff 10000 IS
    delivered (and acked above examined_max so it is not re-injected). Across
    successive fires EVERY agent_message 1..150 AND handoff 10000 are delivered."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=0, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # none rendered

    am_ids = list(range(1, 151))  # 1..150
    rows_by_kind = {
        "agent_message": [_broker_msg_kind(e, "agent_message") for e in am_ids],
        "agent_reply": [],
        "handoff": [_broker_msg_kind(10000, "handoff")],
    }

    # page_limit=7, max_pages=3 => agent_message fetches at most 21 rows/fire.
    # ACK-DRIVEN drain: the model broker_acks what it received each fire, so the
    # floor walks the contiguous agent_message run; the gapped handoff id 10000
    # stays in `acked` (never collapses into floor) until the agent_message gap
    # would fill — which it never does (no to-me ids between 150 and 10000), so
    # the floor NEVER reaches 10000. That is the no-burial guarantee, now provided
    # by the gapped-ack collapse rather than examined_max.
    delivered: set[int] = set()
    saw_10000_parked_while_am_pending = False
    MAX_FIRES = 60  # generous; 150/21 ~= 8 fires for agent_message
    for _ in range(MAX_FIRES):
        _out, injected = _run_do_receive_multikind(
            monkeypatch, MY_SID, rows_by_kind, page_limit=7, max_pages=3)
        delivered |= injected
        # ACK-DRIVEN: confirm exactly what was delivered this fire (broker_ack).
        if injected:
            lrc_server._write_confirmed_set_locked(MY_NAME, sorted(injected))
        cs = _read_confirmed_file(relay_dir, MY_NAME)
        acked_now = set(cs.get("acked", []))
        # NO-BURIAL INVARIANT: the floor must NEVER reach 10000 — the gapped
        # handoff id stays parked in `acked`, the floor only walks agent_message.
        assert cs.get("floor", 0) < 10000, (
            f"floor must never reach the gapped handoff id 10000; got {cs.get('floor')}"
        )
        if 10000 in acked_now and am_ids[-1] not in delivered:
            saw_10000_parked_while_am_pending = True
        if am_ids[-1] in delivered and 10000 in delivered:
            break

    # Every agent_message 1..150 AND handoff 10000 delivered across the fires.
    missing = (set(am_ids) | {10000}) - delivered
    assert not missing, f"messages lost (never delivered): {sorted(missing)}"
    # 10000 was confirmed-above-the-gap (parked in acked) while the agent_message
    # backlog was still draining — proving the high handoff id never buried the tail.
    assert saw_10000_parked_while_am_pending, (
        "expected handoff 10000 parked in acked (gapped) while agent_message pending"
    )
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs["floor"] == am_ids[-1], f"floor should walk to agent_message max 150, got {cs['floor']}"
    assert 10000 in set(cs["acked"]), "handoff 10000 stays confirmed-above-gap in acked"


def test_multikind_degrade_failed_kind_does_not_advance_floor_over_it(
        relay_env, monkeypatch):
    """MULTI-KIND per-kind degrade, ACK-DRIVEN. The `handoff` kind's fetch FAILS
    (fetch_ok=False) while `agent_message` returns id 500. A failed kind is simply
    not fetched this fire, so its rows are neither injected nor (wrongly) confirmed.
    The floor never advances on a bare emit, so a failed kind can NEVER bury its
    un-fetched rows. The model broker_acks what it received; the recovered handoff
    rows deliver + are acked on the next healthy fire."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=100, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])

    rows_by_kind = {
        "agent_message": [_broker_msg_kind(500, "agent_message")],
        "agent_reply": [],
        # handoff rows exist on the broker (ids 200, 300) but fire-1 fetch FAILS.
        "handoff": [_broker_msg_kind(200, "handoff"),
                    _broker_msg_kind(300, "handoff")],
    }

    # FIRE 1: handoff fetch FAILS (fetch_ok=False). agent_message id 500 returns
    # fine and is injected. ACK-DRIVEN: a failed kind simply isn't fetched (so its
    # rows aren't injected and can't be wrongly confirmed); 500 is emitted but is
    # NOT confirmed by the bare emit. The model then broker_acks 500.
    def fail_handoff(identity, since, kind, limit):
        if kind == "handoff":
            return {"messages": [], "next_cursor": None, "fetch_ok": False}
        return None

    _out1, injected1 = _run_do_receive_multikind(
        monkeypatch, MY_SID, rows_by_kind, fetch_override=fail_handoff)

    cs1 = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs1.get("floor", 100) == 100, (
        f"floor must NOT advance (nothing acked/rendered); got {cs1.get('floor')}"
    )
    # The handoff rows 200/300 were never fetched (failed) -> not injected.
    assert 200 not in injected1 and 300 not in injected1, (
        f"failed-kind rows must not be injected this fire; got {injected1}"
    )
    assert 500 in injected1, f"agent_message 500 should be injected fire 1; got {injected1}"
    # ACK-DRIVEN: 500 is NOT confirmed by the bare emit — it must be broker_acked.
    assert 500 not in set(cs1.get("acked", [])), (
        f"emitted-but-unacked 500 must NOT be confirmed on emit; acked={cs1.get('acked')}"
    )
    # The model broker_acks 500 (gapped above floor 100 -> parked in acked).
    lrc_server._write_confirmed_set_locked(MY_NAME, [500])
    assert 500 in set(_read_confirmed_file(relay_dir, MY_NAME).get("acked", []))

    # FIRE 2: handoff healthy now -> rows 200/300 (> floor 100, not acked) are
    # delivered; agent_message 500 already broker_acked -> NOT re-injected.
    _out2, injected2 = _run_do_receive_multikind(
        monkeypatch, MY_SID, rows_by_kind)  # no override => all kinds healthy
    assert {200, 300}.issubset(injected2), (
        f"recovered handoff rows must deliver on the next healthy fire; got {injected2}"
    )
    assert 500 not in injected2, (
        f"already-acked agent_message 500 must NOT re-inject; got {injected2}"
    )

    # Model broker_acks the recovered handoff rows. All three are now CONFIRMED
    # (id <= floor OR in acked). ACK-DRIVEN + recipient-stream compaction: the
    # floor compacts forward over confirmed ids (past the global gap 101..199),
    # proving the floor does NOT stick and `acked` stays bounded (GAN finding #2).
    lrc_server._write_confirmed_set_locked(MY_NAME, [200, 300])
    cs2 = _read_confirmed_file(relay_dir, MY_NAME)
    fl2, acked2 = cs2.get("floor", 0), set(cs2.get("acked", []))
    assert all(e <= fl2 or e in acked2 for e in (200, 300, 500)), (
        f"all delivered+acked rows must be confirmed; floor={fl2} acked={acked2}"
    )


# ---------------------------------------------------------------------------
# GAN finding #2 regression — floor compacts past GLOBAL event-id gaps
# ---------------------------------------------------------------------------

def test_floor_compacts_over_global_gaps_keeps_acked_bounded(relay_env, monkeypatch):
    """event_id is a GLOBAL counter; floor is the per-recipient `since`, so floor+1
    is usually ANOTHER recipient's id. A naive collapse-only floor would stick and
    grow `acked` unboundedly (-> eventual sweep-cap starvation). The recipient-stream
    safe-floor compaction advances the floor past those global gaps once the to-me
    rows are confirmed, keeping `acked` bounded."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent

    _seed_confirmed(relay_dir, MY_NAME, floor=1000, acked=[])
    # Sparse to-me ids with big GLOBAL gaps; the ids in between are other
    # recipients' and will never be in MY acked.
    to_me = [1100, 1500, 2000]
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=to_me)  # all rendered
    broker = {MY_NAME: [_broker_msg(e) for e in to_me], MY_SID: []}

    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert _injected_event_ids(out) == set(), "all rendered -> none injected"
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    # Floor COMPACTED to the highest examined id (2000) despite the global gaps;
    # acked is EMPTY. Collapse-only would have stuck floor=1000, acked={1100,1500,2000}.
    assert cs["floor"] == 2000, f"floor must compact past global gaps to 2000, got {cs['floor']}"
    assert set(cs["acked"]) == set(), f"acked must stay bounded (empty), got {cs['acked']}"


# ---------------------------------------------------------------------------
# GAN finding #1 regression — legacy UUID cursor does not advance on bare emit
# ---------------------------------------------------------------------------

def test_legacy_uuid_cursor_does_not_advance_on_bare_emit(relay_env, monkeypatch):
    """A legacy UUID-addressed row (to_agent=<sid>), injected but NOT rendered/acked,
    must NOT advance the plain cursor on a bare emit (emit != render) — else a
    non-rendering fire silently loses it. It re-injects until broker_acked; a
    broker_ack (which writes THIS sid's confirmed-set) then suppresses it."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=0, acked=[])
    _seed_cursor(relay_dir, MY_SID, 1000)  # legacy plain cursor
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # nothing renders
    # Legacy row addressed to the SID (not the friendly name).
    broker = {MY_NAME: [], MY_SID: [_broker_msg(1100)]}

    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert 1100 in _injected_event_ids(out), "legacy row should be injected fire 1"
    cur = json.loads(
        (relay_dir / "cursors" / f"{MY_SID}.json").read_text(encoding="utf-8")
    )["last_seen_event_id"]
    assert cur < 1100, f"plain cursor must NOT advance past unconfirmed legacy 1100, got {cur}"

    # broker_ack 1100 -> confirmed-set -> legacy dedup honors it -> not re-injected.
    lrc_server._write_confirmed_set_locked(MY_SID, [1100])
    out2 = _run_do_receive(monkeypatch, MY_SID, broker)
    assert 1100 not in _injected_event_ids(out2), "broker_acked legacy row must not re-inject"


# ---------------------------------------------------------------------------
# GAN iter-2 regression — primary floor must NOT bury an unacked legacy id
# ---------------------------------------------------------------------------

def test_primary_floor_does_not_bury_unacked_legacy_id(relay_env, monkeypatch):
    """The single per-sid confirmed-set floor gates BOTH the primary (friendly-name)
    and legacy (UUID) dedup. A primary-stream floor advance must NOT cross an
    unacked LEGACY id (id < the primary id) or the next fire skips it via
    `eid <= floor` despite no broker_ack/render — a silent loss. Scenario from the
    iter-2 GAN review: legacy id 5, primary id 10, neither rendered/acked."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=0, acked=[])
    _seed_cursor(relay_dir, MY_SID, 0)  # legacy plain cursor at 0
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])  # nothing renders
    broker = {MY_NAME: [_broker_msg(10)], MY_SID: [_broker_msg(5)]}

    out = _run_do_receive(monkeypatch, MY_SID, broker)
    assert {5, 10}.issubset(_injected_event_ids(out)), (
        f"both legacy 5 and primary 10 should inject fire 1; got {_injected_event_ids(out)}"
    )
    # The floor must stop BELOW the lowest unconfirmed id across BOTH streams (5),
    # i.e. floor < 5 — so neither id is buried.
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    assert cs.get("floor", 0) < 5, f"floor must not cross unacked legacy id 5; got {cs.get('floor')}"

    # Fire 2 (still nothing acked/rendered): BOTH must re-deliver — no silent loss.
    out2 = _run_do_receive(monkeypatch, MY_SID, broker)
    assert {5, 10}.issubset(_injected_event_ids(out2)), (
        f"unconfirmed legacy 5 AND primary 10 must re-deliver, not be buried; got {_injected_event_ids(out2)}"
    )


def test_capped_legacy_sweep_does_not_let_floor_skip_legacy_tail(relay_env, monkeypatch):
    """If the legacy sweep is CAPPED/incomplete (an unknown legacy tail remains
    un-fetched), the single floor must not advance to a high PRIMARY id on the
    strength of the primary stream alone — else the un-fetched legacy tail is
    buried. The legacy barrier bounds the floor below the legacy ids."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=0, acked=[])
    _seed_cursor(relay_dir, MY_SID, 0)
    # Primary: a single high id 9000, RENDERED (confirmed) -> primary barrier None.
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[9000])
    legacy_ids = list(range(10, 10 + 60))  # 10..69
    broker = {MY_NAME: [_broker_msg(9000)], MY_SID: [_broker_msg(e) for e in legacy_ids]}

    # Force the legacy sweep to CAP (incomplete): only a prefix of the 60 legacy
    # ids is fetched, leaving an unknown tail.
    monkeypatch.setattr(shared, "SWEEP_PAGE_LIMIT", 5)
    monkeypatch.setattr(shared, "SWEEP_MAX_PAGES", 2)

    out = _run_do_receive(monkeypatch, MY_SID, broker)
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    # The floor must NOT jump to 9000 (primary) — that would bury the legacy ids
    # (the fetched prefix is unconfirmed; the tail is un-fetched). It stays bounded
    # below the legacy range.
    assert cs.get("floor", 0) < 10, (
        f"floor must not skip past the capped/unconfirmed legacy ids to 9000; got {cs.get('floor')}"
    )


def test_legacy_sweep_exception_does_not_let_floor_bury_legacy(relay_env, monkeypatch):
    """If the legacy UUID side-poll RAISES (broker error mid-sweep) while the primary
    sweep succeeds with a high confirmed id, the shared per-sid floor must NOT compact
    from the primary stream alone — the legacy stream is uncertain and bounds the
    floor at its confirmed watermark, so an un-examined legacy id is never buried
    (GAN iter-3)."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=0, acked=[])
    _seed_cursor(relay_dir, MY_SID, 0)  # legacy confirmed watermark at 0
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[10])  # primary 10 rendered
    broker = {MY_NAME: [_broker_msg(10)], MY_SID: [_broker_msg(5)]}  # legacy id 5 exists

    # Make the legacy side-poll RAISE (identity == session_id), primary unaffected.
    real_sweep = shared._sweep_to_me_messages
    def sweep_raises_for_legacy(identity, since, *a, **k):
        if identity == MY_SID:
            raise RuntimeError("simulated legacy sweep broker error")
        return real_sweep(identity, since, *a, **k)
    monkeypatch.setattr(shared, "_sweep_to_me_messages", sweep_raises_for_legacy)

    _run_do_receive(monkeypatch, MY_SID, broker)
    cs = _read_confirmed_file(relay_dir, MY_NAME)
    # Floor stays at the legacy watermark (0), NOT the primary's 10 — so legacy id 5
    # (un-examined this fire) re-delivers next fire instead of being buried.
    assert cs.get("floor", 0) <= 0, (
        f"floor must not compact past the legacy watermark on a failed legacy sweep; got {cs.get('floor')}"
    )


# ---------------------------------------------------------------------------
# P2: awaiting_ack gate (subagent-safe broker_ack) — plan quizzical-launching-fox
#
# broker_ack folds an id ONLY if the HOOK injected it into MAIN (the awaiting_ack
# allow-list). A leaked-to-subagent PUSH id was never hook-injected, so a subagent
# ack of it is refused and the hook re-delivers it to main — killing the
# subagent-ack silent loss WITHOUT being able to see the caller's identity.
# ---------------------------------------------------------------------------

def _seed_confirmed_aw(relay_dir: Path, sid: str, floor: int, acked: list,
                       awaiting: list) -> None:
    """Seed a confirmed-set WITH a present awaiting_ack allow-list."""
    p = relay_dir / "confirmed" / f"{sid}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"floor": floor, "acked": acked,
                             "awaiting_ack": awaiting}), encoding="utf-8")


def test_gate_subagent_leak_refused(relay_env, monkeypatch):
    """(a) HEADLINE: an id NOT in awaiting_ack (a push that leaked into a subagent,
    never hook-injected) is REFUSED by broker_ack -> the confirmed-set does NOT
    advance -> the hook re-delivers it to MAIN. Proves the silent loss is killed."""
    relay_dir = relay_env
    _seed_confirmed_aw(relay_dir, MY_NAME, floor=2961, acked=[], awaiting=[])
    foldable, already, refused = lrc_server._gate_broker_ack(MY_NAME, [2962])
    assert foldable == []
    assert already == []          # 2962 > floor 2961 and not in acked -> not ALREADY
    assert refused == [2962]
    f = _read_confirmed_file(relay_dir, MY_NAME)
    assert f["floor"] == 2961
    assert f.get("awaiting_ack") == []          # untouched by the refused ack
    # The hook still re-delivers 2962 to MAIN (no transcript render -> injected).
    out = _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2962)]})
    assert 2962 in _injected_event_ids(out)


def test_gate_hook_injected_id_folds(relay_env):
    """(b) An id IN awaiting_ack (hook-injected-to-main) folds + advances; the
    folded id is removed from awaiting_ack (self-cleaning)."""
    relay_dir = relay_env
    _seed_confirmed_aw(relay_dir, MY_NAME, floor=2961, acked=[], awaiting=[2962])
    foldable, already, refused = lrc_server._gate_broker_ack(MY_NAME, [2962])
    assert foldable == [2962]
    assert already == []          # in awaiting_ack -> FOLDABLE, never ALREADY
    assert refused == []
    lrc_server._write_confirmed_set_locked(MY_NAME, foldable)
    f = _read_confirmed_file(relay_dir, MY_NAME)
    assert f["floor"] == 2962                    # collapsed contiguously into floor
    assert f.get("awaiting_ack") == []           # folded id pruned


def test_gate_push_render_autoconfirm_untouched(relay_env, monkeypatch):
    """(c) A push-rendered-in-MAIN id is auto-confirmed by the hook's render-grep
    WITHOUT broker_ack; awaiting_ack reflects nothing-injected ([])."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    _write_transcript(claude_dir, MY_SID, [2962])     # 2962 rendered in main transcript
    out = _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2962)]})
    assert 2962 not in _injected_event_ids(out)       # grep-confirmed -> not injected
    f = _read_confirmed_file(relay_dir, MY_NAME)
    assert f["floor"] >= 2962 or 2962 in f.get("acked", [])
    assert f.get("awaiting_ack") == []                # nothing hook-injected this fire


def test_gate_absent_awaiting_refuses_then_engages(relay_env, monkeypatch):
    """(d) An ABSENT awaiting_ack key (pre-upgrade file / hook-not-fired-yet) is
    FAIL-CLOSED: broker_ack refuses all (loss-free — a hook-injected id re-delivers
    and push ids are grep-confirmed regardless). After a delivering do_receive fire
    the key is present with the delivered id, and a main ack folds it."""
    relay_dir = relay_env
    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])   # NO awaiting_ack key
    foldable, already, refused = lrc_server._gate_broker_ack(MY_NAME, [2962])
    assert foldable == []                       # absent -> REFUSE (fail-closed)
    assert already == []                        # the absent-key early return is ([], [], ids)
    assert refused == [2962]
    f0 = _read_confirmed_file(relay_dir, MY_NAME)
    assert f0["floor"] == 2961                  # nothing folded
    # A do_receive fire that DELIVERS 2962 records it in awaiting_ack.
    out = _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2962)]})
    assert 2962 in _injected_event_ids(out)
    f1 = _read_confirmed_file(relay_dir, MY_NAME)
    assert "awaiting_ack" in f1 and 2962 in f1["awaiting_ack"]
    # Now a MAIN ack of the delivered id folds.
    foldable2, already2, refused2 = lrc_server._gate_broker_ack(MY_NAME, [2962])
    assert foldable2 == [2962]
    assert already2 == []
    assert refused2 == []


def test_gate_concurrent_writers_consistent(relay_env):
    """(e) The hook's awaiting_ack write + broker_ack's fold interleave under the
    shared sentinel lock -> the confirmed-set is internally consistent on every
    run (no awaiting id <= floor, awaiting disjoint from acked, all acked > floor,
    valid JSON)."""
    import threading
    relay_dir = relay_env
    for _ in range(40):
        _seed_confirmed_aw(relay_dir, MY_NAME, floor=2961, acked=[],
                           awaiting=[2962, 2963])

        # A raise inside EITHER thread must fail this test. Thread.join() does not
        # re-raise, and the assertions below only inspect what the OTHER thread wrote --
        # so before this guard the test reported PASS while `ack` raised on every one of
        # the 40 iterations (MEASURED 2026-07-30: 40 pytest unhandled-thread-exception
        # warnings, and the summary line still said 1 passed). `hook` is not raising
        # today and carries the IDENTICAL latent shape, so BOTH targets are wrapped, not
        # just the one that broke. Per-iteration accumulator: a failure is reported on
        # the iteration it happened, not aggregated at the end.
        #
        # Deliberately NOT quoting the literal runtime strings here: a harness that
        # counts them in pytest output matches this comment when pytest echoes the
        # source, and reports a swallow in a run that had none.
        errors = []

        def _capture(fn):
            """Run fn in a thread, recording anything it raises for the joiner."""
            def _run():
                try:
                    fn()
                except BaseException as exc:      # noqa: BLE001 - re-raised below
                    errors.append(exc)            # list.append is atomic under the GIL
            return _run

        def hook():
            shared._advance_floor_locked(MY_NAME, target_floor=2961,
                                         extra_acked_ids=(), default_floor=0,
                                         awaiting_ack_ids=[2962, 2963])

        def ack():
            foldable, _, _ = lrc_server._gate_broker_ack(MY_NAME, [2962])
            if foldable:
                lrc_server._write_confirmed_set_locked(MY_NAME, foldable)

        ta = threading.Thread(target=_capture(hook))
        tb = threading.Thread(target=_capture(ack))
        ta.start(); tb.start(); ta.join(); tb.join()
        # Re-raise the ORIGINAL exception, not a wrapper: pytest must report the real
        # exception type, because a generic AssertionError would hide WHICH subject
        # failed -- the same blindness being fixed here.
        if errors:
            raise errors[0]

        f = _read_confirmed_file(relay_dir, MY_NAME)        # valid JSON (atomic write)
        floor = f["floor"]
        acked = set(f.get("acked", []))
        aw = set(f.get("awaiting_ack", []))
        assert all(a > floor for a in aw), (floor, aw)     # no awaiting id <= floor
        assert not (aw & acked), (aw, acked)               # awaiting disjoint acked
        assert all(a > floor for a in acked), (floor, acked)


def test_awaiting_ack_schema_additive(relay_env):
    """(f) The new optional awaiting_ack key is schema-additive: both readers
    surface the same floor/acked as before, plus the pruned allow-list."""
    relay_dir = relay_env
    _seed_confirmed_aw(relay_dir, MY_NAME, floor=100, acked=[105], awaiting=[106, 107])
    s = shared._read_confirmed_set(MY_NAME, default_floor=0)
    v = lrc_server._read_confirmed_set(MY_NAME, default_floor=0)
    assert s["floor"] == 100 and s["acked"] == [105]
    assert v["floor"] == 100 and v["acked"] == [105]
    assert s["awaiting_ack"] == [106, 107]
    assert v["awaiting_ack"] == [106, 107]


def test_advance_floor_prunes_awaiting_already_acked(relay_env):
    """(g) MED1 (cross-family): an id folded into `acked` above a gap must be
    removed from awaiting_ack on the next hook write — never leave an id in BOTH
    acked and awaiting_ack."""
    relay_dir = relay_env
    _seed_confirmed_aw(relay_dir, MY_NAME, floor=69, acked=[], awaiting=[70, 100])
    # broker_ack folds 100 (gapped: 70 still unconfirmed so floor stays 69, 100
    # lands in acked).
    foldable, already, refused = lrc_server._gate_broker_ack(MY_NAME, [100])
    assert foldable == [100]
    assert already == [] and refused == []      # 100 is in awaiting_ack, so neither
    lrc_server._write_confirmed_set_locked(MY_NAME, foldable)
    f1 = _read_confirmed_file(relay_dir, MY_NAME)
    assert f1["floor"] == 69 and 100 in f1["acked"]
    assert 100 not in f1.get("awaiting_ack", [])      # server writer prunes it
    # The hook re-writes awaiting (70 still injected, 100 now acked) — it must NOT
    # re-introduce 100 into awaiting_ack.
    shared._advance_floor_locked(MY_NAME, target_floor=69, extra_acked_ids=(),
                                 default_floor=0, awaiting_ack_ids=[70, 100])
    f2 = _read_confirmed_file(relay_dir, MY_NAME)
    aw = set(f2.get("awaiting_ack", []))
    acked = set(f2.get("acked", []))
    assert 100 in acked
    assert 100 not in aw                              # MED1 fix
    assert not (aw & acked)
    assert aw == {70}


def test_grep_skips_sidechain_channel_render(relay_env, monkeypatch):
    """(h) HIGH (cross-family): a channel render inside a SUBAGENT (isSidechain
    true) turn must NOT be grep-confirmed — that would advance the cursor without
    main delivery (silent loss). The hook must re-inject it into MAIN instead."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=2961, acked=[])
    proj = claude_dir / "projects" / "E--some-encoded-project"
    proj.mkdir(parents=True, exist_ok=True)
    content = (
        f'<channel source="local-relay-channel" from="sender" to="{MY_NAME}" '
        f'kind="agent_message" channel="#cc-session-relay" event_id="2962">\n'
        f'[relay] from=sender to={MY_NAME} event_id=2962 kind=agent_message\n\nbody'
    )
    line = json.dumps({
        "type": "user",
        "isSidechain": True,        # <- subagent turn
        "origin": {"kind": "channel", "server": "local-relay-channel"},
        "message": {"content": content},
    })
    (proj / f"{MY_SID}.jsonl").write_text(line + "\n", encoding="utf-8")
    rendered, ok = shared._rendered_event_ids_in_transcript(MY_SID)
    assert ok is True
    assert 2962 not in rendered                       # sidechain render NOT confirmed
    # consequently the hook re-injects 2962 into MAIN
    out = _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2962)]})
    assert 2962 in _injected_event_ids(out)


def test_grep_confirms_main_channel_render(relay_env):
    """(h-control) a NON-sidechain (main) channel render IS still grep-confirmed —
    the isSidechain guard must not break legitimate main-context confirmation."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _write_transcript(claude_dir, MY_SID, [2962])     # isSidechain absent/false
    rendered, ok = shared._rendered_event_ids_in_transcript(MY_SID)
    assert ok is True
    assert 2962 in rendered


def test_emit_failure_does_not_add_to_awaiting_ack(relay_env, monkeypatch):
    """(i) HIGH (cross-family iter-2): if the hook's injection emit FAILS, the
    attempted-but-undelivered ids must NOT enter awaiting_ack — otherwise a
    subagent could ack a never-delivered id and silently advance the cursor. The
    id must stay ack-refused and re-deliverable."""
    relay_dir = relay_env
    _seed_confirmed_aw(relay_dir, MY_NAME, floor=2961, acked=[], awaiting=[])

    real_emit = shared.emit_hook_response

    def boom(hook_event_name, ctx):
        # Fail ONLY the message-injection emit (non-empty additionalContext); let
        # an empty/nudge emit through so do_receive completes normally.
        if ctx and "2962" in ctx:
            raise RuntimeError("simulated emit failure")
        return real_emit(hook_event_name, ctx)

    monkeypatch.setattr(shared, "emit_hook_response", boom)
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2962)]})

    f = _read_confirmed_file(relay_dir, MY_NAME)
    assert 2962 not in f.get("awaiting_ack", [])      # un-delivered -> NOT ack-able
    assert f["floor"] == 2961                          # cursor did not advance
    # A (subagent) ack of 2962 is therefore refused -> the hook re-delivers it.
    foldable, already, refused = lrc_server._gate_broker_ack(MY_NAME, [2962])
    assert foldable == []
    assert already == []          # emit failed -> 2962 never entered awaiting_ack,
    assert refused == [2962]      # and floor stayed 2961, so it is refused not already


# ---------------------------------------------------------------------------
# WS-B-client (#19) — hook mirrors render-confirmed acks to the broker /ack
# ---------------------------------------------------------------------------

class _FakeBrokerClient:
    """Records post_ack calls; stands in for the broker_client module."""
    def __init__(self):
        self.calls = []

    def post_ack(self, agent_id, event_ids, *, floor=None, fast_fail=False, broker_url=None):
        self.calls.append({"name": agent_id, "ids": list(event_ids),
                           "floor": floor, "fast_fail": fast_fail})
        return {"acked_now": len(event_ids)}


def test_hook_mirrors_confirmed_set_on_grep_confirm(relay_env, monkeypatch):
    """(#18/#32) after a push render, do_receive mirrors the WHOLE gap-aware
    confirmed-set {floor, acked} to the broker (not just the grep ids) — keyed by
    this recipient's own name, fast_fail=True. Here 2961 renders + folds into the
    floor; the injected drop 2962 stays > floor and is NOT crossed off."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    # OQ-RELAY-6 (D78): clean session — epoch established under MY_NAME so the
    # post-render floor mirror PASSES the gate (a no-epoch floor would suppress).
    _seed_confirmed(relay_dir, MY_NAME, floor=2960, acked=[], epoch_name=MY_NAME)
    # broker has 2961 (rendered) + 2962 (NOT rendered) addressed to MY_NAME.
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[2961])
    fake = _FakeBrokerClient()
    monkeypatch.setattr(shared, "broker_client", fake)

    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2961), _broker_msg(2962)]})

    assert len(fake.calls) == 1
    c = fake.calls[0]
    assert c["name"] == MY_NAME
    assert c["floor"] == 2961            # 2961 rendered -> folded into the floor
    assert c["ids"] == []                # nothing parked above the floor
    assert 2962 > c["floor"]             # injected drop stays > floor -> NOT crossed off
    assert c["fast_fail"] is True        # off the turn critical path


def test_hook_mirrors_floor_on_user_prompt_even_without_grep(relay_env, monkeypatch):
    """(#18/#32) THE fix: on a USER TURN the hook mirrors its confirmed-set floor
    even when nothing was grep-confirmed this fire — this is what carries broker_ack'd
    folds (which the sid-drift-degraded MCP mirror misses, the D74 blocker). Here the
    floor already stands at 2965 from a prior fire; a UserPromptSubmit re-posts it so
    the broker rows flip. Self-healing (re-posts each turn)."""
    relay_dir = relay_env
    # OQ-RELAY-6 (D78): the floor mirrors only under a matching, unbroken epoch —
    # seed one (the floor was accrued as MY_NAME). An absent epoch fail-closes.
    _seed_confirmed(relay_dir, MY_NAME, floor=2965, acked=[], epoch_name=MY_NAME)
    fake = _FakeBrokerClient()
    monkeypatch.setattr(shared, "broker_client", fake)
    # no new broker rows + nothing rendered -> NO grep_confirmed this fire.
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: []}, hook_event="UserPromptSubmit")
    assert len(fake.calls) == 1
    c = fake.calls[0]
    assert c["floor"] == 2965            # the standing floor is mirrored
    assert c["ids"] == []
    assert c["fast_fail"] is True


def test_hook_mirror_preserves_gap_id(relay_env, monkeypatch):
    """(#18/#32) hook-layer wedge (an unread-gap scenario): an UNREAD gap id (injected, not
    rendered) is in NEITHER the floor NOR `acked`, so the mirror never crosses it
    off — its broker row stays outstanding + it keeps re-delivering. Here 2961 is the
    unread gap and 2962 renders (parked above the gap)."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    # OQ-RELAY-6 (D78): floor accrued as MY_NAME -> gate PASSES; the unread gap id
    # 2961 still stays outstanding (in neither the floor nor acked).
    _seed_confirmed(relay_dir, MY_NAME, floor=2960, acked=[], epoch_name=MY_NAME)
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[2962])  # 2962 rendered, 2961 NOT
    fake = _FakeBrokerClient()
    monkeypatch.setattr(shared, "broker_client", fake)

    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2961), _broker_msg(2962)]})

    assert len(fake.calls) == 1
    c = fake.calls[0]
    assert c["floor"] == 2960            # floor pinned BELOW the unread 2961
    assert c["ids"] == [2962]            # 2962 parked above the gap -> crossed off
    assert 2961 not in c["ids"]          # the UNREAD gap id is NOT crossed off
    assert 2961 > c["floor"]             # and it is above the floor -> stays outstanding


def test_hook_no_ack_post_when_nothing_rendered(relay_env, monkeypatch):
    """(#18/#32) on a NON-user-turn fire (PostToolUse) with nothing rendered, the
    mirror does not fire at all — no grep_confirmed and not a UserPromptSubmit — so a
    genuine drop's injected id is never crossed off (it re-delivers until confirmed)."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=2960, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[])   # nothing rendered
    fake = _FakeBrokerClient()
    monkeypatch.setattr(shared, "broker_client", fake)

    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2961)]})  # PostToolUse (default)

    assert fake.calls == []               # not a user turn + no grep => no mirror POST


def test_hook_survives_broker_client_import_failure(relay_env, monkeypatch):
    """(#19) if broker_client failed to import (sentinel None), the mirror silently
    no-ops and do_receive still completes (the hook must never crash a session)."""
    relay_dir = relay_env
    claude_dir = relay_dir.parent
    _seed_confirmed(relay_dir, MY_NAME, floor=2960, acked=[])
    _write_transcript(claude_dir, MY_SID, rendered_event_ids=[2961])
    monkeypatch.setattr(shared, "broker_client", None)   # import-failed sentinel

    # Must not raise; the local floor still advances over the rendered id.
    _run_do_receive(monkeypatch, MY_SID, {MY_NAME: [_broker_msg(2961)]})
    f = _read_confirmed_file(relay_dir, MY_NAME)
    assert 2961 <= f["floor"]            # local confirmed-set still advanced
