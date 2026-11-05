# -*- coding: utf-8 -*-
"""ACCEPTANCE TEST for the delivery-key identity fix and its refusal reporting.

Routing and receipt filing must share the same identity sources. The routing
resolver reads daemon job state and the relay registry, never a workspace's
remembered name file. A remembered file can outlive a registry name reclamation
and would create a mailbox that the receive hook cannot acknowledge.

THE PROPERTY UNDER TEST IS UNCHANGED, in one sentence so it can fail:

    A session must never ACCEPT mail for one mailbox while FILING receipts into
    another -- and when it refuses a receipt it must say which mailbox it read
    and which floor it read there, so the reader can tell a transient miss from
    a permanent one.

An absent allow-list and a present allow-list missing an id can both occur when
an identity was reclaimed. Assert the mailbox identity and structured refusal
reporting, not just a diagnostic string for one of those shapes.

THREE INVARIANTS THIS FILE MUST NOT BE USED TO BREAK. They outrank the feature.

  INV-1  THE FOLD MUST NOT MOVE. Which event ids get folded into the
         confirmed-set must be byte-for-byte what it is today. Nothing below
         asserts anything about WHICH ids fold; T2 and T3 constrain only what
         the caller is TOLD. Widening the fold reopens the hole where a subagent
         confirms messages the orchestrator never saw -- silent loss.
  INV-2  `_gate_broker_ack` RETURNS EXACTLY THREE VALUES. The lossless-delivery
         tests also unpack foldable/already/refused. Optional diagnostics travel
         through the mutable diagnostics dictionary, never a fourth return value.
  INV-3  "COULD NOT CHECK" IS NOT "CHECKED, AND DIFFERENT". Written for the
         agreement gate -- it must not deactivate push because the registry was
         momentarily unreadable -- and it OUTLIVED the gate in the one place
         with a durable consequence: `_delivery_key_name`'s three-outcome
         vocabulary (`_NAME_UNKNOWN`), which stops a transient read failure
         being RECORDED as a fact and minting a session-id-keyed confirmed-set.
         `test_control_unreadable_registry_is_not_a_disagreement` pins that; it
         passes today and must keep passing.
         `test_inv3_unreadable_registry_degrades_quietly_rather_than_splitting`
         records what the subtraction traded: with no third source, an
         unreadable registry now leaves push inactive for the duration instead
         of active-but-unacknowledgeable. That is the ratified cost.

EVERY TEST BUILDS ITS OWN SYNTHETIC HOME. On Windows `Path.home()` follows
USERPROFILE and ignores the HOME env var, so an env-only redirect does NOT move
it and an uncontained test would read -- and could write -- the operator's real
relay state. `_load` replaces the module's `Path` symbol and re-points the one
path the plugin freezes at import (`_SESSION_DIR`), then `_assert_contained`
interrogates the symbols the CODE reads rather than `Path.home()` itself.

THE INSTRUMENT CONTROLS ARE LOAD-BEARING, not decoration. Two assertions here
are of the form "substring X is absent" (T4, T5) and two read a response shape
(T2, T3). Every one of those passes vacuously if the extractor silently returns
nothing. So each extractor carries a positive control that must fire on a token
the fix cannot remove, plus a negative control on a token that was never there.
An absence claim from an instrument that could not have found the
counter-example is the most portable error there is.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import json
import os
import pathlib
import textwrap

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
PLUGIN = REPO / "harness" / "claude" / "local-relay-channel" / "server.py"

# Two distinct ids so a test can tell "returned the argument" apart from
# "returned some other session's id". Lowercase 8-4-4-4-12, the shape CC writes.
SID_A = "a0000000-0000-4000-8000-000000000001"
# The id a rotation lands on. Used only by T8, which needs the CC session record
# and the id `_routing_name` is asked about to be DIFFERENT strings -- the whole
# point being that an answer about one session is not an answer about the other.
SID_B = "0f1e2d3c-4b5a-4968-8776-655443332211"

NAME = "broker"


# ---------------------------------------------------------------------------
# Harness: import the plugin against a synthetic home
# ---------------------------------------------------------------------------

def _load(home: pathlib.Path):
    """Import the plugin with its home redirected into a scratch tree."""
    spec = importlib.util.spec_from_file_location("lrc_agreement_under_test",
                                                  str(PLUGIN))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    real_path_cls = mod.Path

    class HomePath(real_path_cls):
        @classmethod
        def home(cls):
            return cls(str(home))

    mod.Path = HomePath
    # The plugin freezes exactly one path at import time; every other path is
    # built at call time from `Path.home()` and is therefore already contained.
    mod._SESSION_DIR = real_path_cls(str(home)) / ".claude" / "sessions"
    _assert_contained(mod, home)
    return mod


def _within(child, parent) -> bool:
    """True iff `child` IS `parent` or lives under it, both taken RESOLVED.

    RESOLVED, AND VIA `commonpath` RATHER THAN A STRING PREFIX, BECAUSE THAT IS
    THE ENTIRE DIFFERENCE BETWEEN THIS AND THE CHECK IT REPLACES. A prefix test
    answers a question about SPELLING; the thing being guarded against is a path
    SPELLED inside the scratch tree that LANDS outside it. Measured on this box:
    with a scratch home of `<home>/zz_scratch`, the probe
    `<home>/zz_scratch/../.claude/sessions` starts with the scratch path and
    does not start with the real relay-state path -- so the prefix form reported
    CONTAINED for a probe resolving to the operator's own relay state, i.e. for
    precisely the escape it exists to reject.

    `normcase` because two Windows paths differ in case and separator without
    differing in meaning. `ValueError` because `commonpath` refuses a
    cross-drive pair -- which is a correct NO, not an error worth propagating;
    on this box pytest's temp root is on another drive from the profile, so it
    is the ordinary answer rather than an edge case.

    Neither argument has to exist, and `resolve()` is non-strict, so a probe
    naming a directory the plugin has not created yet is answered rather than
    raising -- which is required, since containment must be decidable BEFORE
    anything is written.

    IT IS NOT A PURELY TEXTUAL NORMALISATION, and calling it one would
    understate what it buys. `resolve()` READS the filesystem for the
    components that DO exist, so a symlink or an NTFS junction partway along
    the path is followed to its target; only the non-existent tail is folded
    textually. That is exactly why this beats a spelling test -- a junction
    pointing out of the scratch tree is invisible to a prefix comparison and
    visible here. It reads only: nothing is created and nothing is written.
    """
    c = os.path.normcase(str(pathlib.Path(child).resolve()))
    p = os.path.normcase(str(pathlib.Path(parent).resolve()))
    try:
        return os.path.commonpath([c, p]) == p
    except ValueError:
        return False


def _assert_contained(mod, home: pathlib.Path) -> None:
    """Prove the module cannot reach the operator's real relay state.

    Checks a CALL-TIME probe as well as the frozen constant, because the
    call-time paths are the ones that would actually be written.

    BOTH DIRECTIONS ARE KEPT AND BOTH ARE ANSWERED ON RESOLVED PATHS: the probe
    must live inside the scratch tree, and it must not live inside real relay
    state. See `_within` for why the resolution is the load-bearing part;
    `test_containment_helper_rejects_a_traversal_probe` is the standing proof
    that a traversal fails and an ordinary probe passes.

    THE SECOND ASSERTION COMPARES AGAINST THE REAL RELAY-STATE DIRECTORY
    (`~/.claude`), NOT AGAINST THE WHOLE USER PROFILE, and the difference is a
    portability break rather than a nicety. On a DEFAULT Windows box the
    temporary directory pytest hands out lives UNDER the profile
    (`C:\\Users\\<user>\\AppData\\Local\\Temp\\...`), so a perfectly contained
    synthetic home is inside `Path.home()` and a profile-wide comparison
    aborts the whole suite with a containment failure that is pure instrument.
    A redirected TEMP directory can conceal that portability defect.
    Comparing against `Path.home()/".claude"` keeps the guard that matters --
    a probe that genuinely leaked into the operator's relay state still fails
    -- while a temp dir under the profile passes.
    """
    real_state = pathlib.Path.home() / ".claude"
    probes = {
        "_SESSION_DIR": mod._SESSION_DIR,
        "_confirmed_dir()": mod._confirmed_dir(),
        "Path.home()": mod.Path.home(),
    }
    for label, value in probes.items():
        resolved = pathlib.Path(value).resolve()
        assert _within(value, home), (
            f"CONTAINMENT FAILURE: {label} = {str(value)!r} resolves to "
            f"{str(resolved)!r}, which is outside the scratch tree {home}. This "
            f"test would read the operator's real relay state and any result it "
            f"produced would be meaningless."
        )
        assert not _within(value, real_state), (
            f"CONTAINMENT FAILURE: {label} = {str(value)!r} resolves to "
            f"{str(resolved)!r}, which points into the REAL relay state "
            f"{real_state}."
        )


def _assert_registry_contained(registry_path, home: pathlib.Path) -> None:
    """The same containment semantic, for the HOOK's one frozen path.

    ONE HELPER, ONE SEMANTIC. `_assert_contained` guards the plugin's three
    paths and this guards the hook's single one, and both now answer on RESOLVED
    paths via `_within` rather than on string prefixes. This used to be an
    unnormalized `startswith` written inline in `_load_hook` -- the same defect
    as the one `_within`'s docstring records, in the same file, in a check with
    the same job. It was not reachable through `_load_hook` itself (the path is
    built as `home / ".claude" / ...`, so a `..` in `home` traverses BOTH sides
    identically and containment still holds), which is exactly why it needed
    extracting: an assertion no control can reach is not a verified one.
    `test_registry_containment_helper_rejects_a_traversal_probe` reaches it.
    """
    resolved = pathlib.Path(registry_path).resolve()
    assert _within(registry_path, home), (
        f"CONTAINMENT FAILURE: the hook's REGISTRY_PATH {str(registry_path)!r} "
        f"resolves to {str(resolved)!r}, which is outside the scratch tree "
        f"{home}. The hook would read the operator's real registry."
    )
    assert not _within(registry_path, pathlib.Path.home() / ".claude"), (
        f"CONTAINMENT FAILURE: the hook's REGISTRY_PATH {str(registry_path)!r} "
        f"resolves to {str(resolved)!r}, which points into the REAL relay "
        f"state {pathlib.Path.home() / '.claude'}."
    )


# ---------------------------------------------------------------------------
# The containment helpers' own controls -- they are the guard every other test
# in this file runs behind, so a hole in one is silent everywhere
# ---------------------------------------------------------------------------

class _ProbeMod:
    """Minimal stand-in exposing the three symbols `_assert_contained` reads.

    Used only to feed the helper a chosen path. Nothing is created on disk:
    `_within` resolves lexically and never touches the filesystem, which is
    what makes it safe to hand it a path INSIDE the operator's real relay
    state as a control.
    """

    def __init__(self, probe: pathlib.Path):
        self._SESSION_DIR = probe
        probe_ref = probe

        class _P:
            @staticmethod
            def home():
                return probe_ref

        self.Path = _P

    def _confirmed_dir(self):
        return self._SESSION_DIR


def test_containment_helper_rejects_a_traversal_probe(tmp_path):
    """NEGATIVE CONTROL FOR `_assert_contained`, and the reason it was rewritten.

    THE HOLE IT CLOSES, MEASURED. The helper used to compare UNNORMALIZED
    STRINGS. A probe spelled `<scratch>/../<something>` starts with the scratch
    path and does not start with the real relay-state path, so BOTH assertions
    passed -- for a path that resolves clean out of the scratch tree, and in the
    profile-rooted case straight into the operator's own `~/.claude`. The check
    reported containment for exactly the escape it exists to reject, and every
    other test in this file runs behind it.

    PROVEN IN BOTH DIRECTIONS, because a helper that rejected everything would
    satisfy the `raises` below while making the whole suite vacuous: an ordinary
    contained probe must PASS, a traversal probe must FAIL.

    THE OLD LOGIC IS ASSERTED INLINE rather than described. Without that, the
    `raises` calls would be evidence of a check that exists, not of a defect
    that was closed.
    """
    home = tmp_path / "relayhome"
    home.mkdir()

    # DIRECTION 1 -- an ordinary contained probe still passes. This is the
    # non-vacuity half: it is also the shape every real test in this file uses.
    _assert_contained(_ProbeMod(home / ".claude" / "sessions"), home)

    # DIRECTION 2 -- a probe spelled inside the scratch tree that resolves out
    # of it must fail.
    escaped = home / ".." / "escaped" / ".claude"
    assert str(escaped).lower().startswith(str(home).lower()), (
        "POSITIVE CONTROL FAILED: the traversal probe is not even spelled "
        "inside the scratch tree, so a string-prefix check would have caught "
        "it too and this proves nothing about resolution."
    )
    with pytest.raises(AssertionError, match="outside the scratch tree"):
        _assert_contained(_ProbeMod(escaped), home)

    # THE BRIEF'S OWN EXAMPLE, kept because it is the case with a consequence:
    # a traversal that resolves INTO the operator's real relay state. Rooted
    # under the profile rather than under `tmp_path` on purpose -- this box
    # redirects TEMP to another drive, and no number of `..` segments crosses a
    # drive. The directory is never created; the helper only resolves.
    fake_home = pathlib.Path.home() / "zz_scratch_relayhome_never_created"
    into_real = fake_home / ".." / ".claude" / "sessions"
    assert _within(into_real, pathlib.Path.home() / ".claude"), (
        "POSITIVE CONTROL FAILED: the probe does not actually resolve into "
        "real relay state, so rejecting it says nothing."
    )
    # THE OLD CHECK'S TWO CONDITIONS, VERBATIM. Both are True -- so the previous
    # helper returned cleanly on this probe.
    assert str(into_real).lower().startswith(str(fake_home).lower())
    assert not str(into_real).lower().startswith(
        str(pathlib.Path.home() / ".claude").lower())
    with pytest.raises(AssertionError, match="outside the scratch tree"):
        _assert_contained(_ProbeMod(into_real), fake_home)

    # DIRECTION 2, SECOND ASSERTION, ISOLATED. The traversal cases above are
    # caught by the FIRST assertion (escaping the scratch tree is what they do),
    # so the real-state half needs its own firing: a scratch root that IS real
    # relay state, with a probe honestly inside it. Nothing is created.
    real_state = pathlib.Path.home() / ".claude"
    with pytest.raises(AssertionError, match="REAL relay state"):
        _assert_contained(_ProbeMod(real_state / "zz_never_created"), real_state)


def test_registry_containment_helper_rejects_a_traversal_probe(tmp_path):
    """THE SAME CONTROL FOR THE HOOK-SIDE CHECK, and the reason it was extracted.

    `_load_hook` re-points the hook's frozen `REGISTRY_PATH` into the scratch
    tree and then asserts the result is contained. That assertion was an
    unnormalized `startswith` written inline -- the same defect as the plugin-side
    one, one helper over -- and it was UNREACHABLE: `_load_hook` builds the path
    as `home / ".claude" / "relay" / "registry.json"`, so any `..` in `home`
    traverses both operands equally and containment holds no matter what. A check
    no control can reach is not a verified check, whatever today's population of
    callers happens to be. Extracting it is what makes this test possible.

    PROVEN IN BOTH DIRECTIONS, as for `_assert_contained`: the shape `_load_hook`
    actually produces must PASS, and a traversal probe must FAIL. The old
    inline logic's condition is asserted here too, so the `raises` is evidence of
    a closed hole rather than of a check that was always adequate.
    """
    home = tmp_path / "relayhome"
    (home / ".claude" / "relay").mkdir(parents=True)

    # DIRECTION 1 -- the path `_load_hook` really builds still passes. This is
    # the non-vacuity half.
    _assert_registry_contained(home / ".claude" / "relay" / "registry.json", home)

    # DIRECTION 2 -- spelled inside the scratch tree, resolves out of it.
    escaped = home / ".." / ".." / ".claude" / "relay" / "registry.json"
    # THE OLD INLINE CONDITION, VERBATIM: it was True, so the previous check
    # returned cleanly on this path.
    assert str(escaped).lower().startswith(str(home).lower())
    with pytest.raises(AssertionError, match="outside the scratch tree"):
        _assert_registry_contained(escaped, home)

    # DIRECTION 2, SECOND ASSERTION, ISOLATED -- as with `_assert_contained`,
    # escaping the scratch tree is what a traversal does, so the real-state half
    # needs a scratch root that IS real relay state. Nothing is created; the
    # helper only resolves.
    real_state = pathlib.Path.home() / ".claude"
    with pytest.raises(AssertionError, match="REAL relay state"):
        _assert_registry_contained(real_state / "relay" / "zz_never_created.json",
                                   real_state)


class _StubTextContent:
    """Stand-in for `mcp.types.TextContent`.

    The real class is imported inside `_build_and_run`, which also starts the
    stdio loop, so it is not reachable from a test. Only `.text` is read here.
    """

    def __init__(self, type=None, text=None):   # noqa: A002 - mirrors the real kwarg
        self.type = type
        self.text = text


def _bind_call_tool(mod):
    """Return the plugin's REAL `call_tool` body, bound to the loaded module.

    `call_tool` is defined inside `_build_and_run`, whose import block pulls in
    anyio / httpx / httpx_sse / mcp / broker_client and whose tail runs the
    stdio loop -- so the function cannot be reached by calling its enclosing
    scope. It is extracted from the source by AST (the same technique
    `test_name_fold_plugin.py` uses to lift the hook's fold) and exec'd into the
    loaded module's own namespace, so every global it resolves -- `Path`,
    `_delivery_key`, `_gate_broker_ack`, `_read_confirmed_set` -- is the
    CONTAINED one. This runs the shipped bytes; it does not re-implement them.

    Fails loudly rather than skipping if the function moves or is renamed: a
    silent skip here would make T2 and T3 vacuous, and a vacuous absence claim
    is the failure mode this whole file exists to prevent.

    THE `posted` RECORDER IS TAKEN AT THE `asyncio.to_thread` SEAM, AND THE
    OBVIOUS PLACE IS THE WRONG ONE. This helper used to stub `_safe_post_ack`
    itself and hand back the list it appended to -- and that list was ALWAYS
    EMPTY, for every input, because `_spawn_bg` never awaits the coroutine
    `asyncio.to_thread(...)` produces: the stub is referenced and never called.
    Any assertion of the form "nothing was posted" answered from it would have
    been answered by an instrument that could not have found the counter-example.
    Recording the `to_thread` CALL instead observes the decision the plugin
    actually made. `_posted_control` below is the standing proof it fires.
    """
    src = PLUGIN.read_text(encoding="utf-8")
    tree = ast.parse(src)
    lines = src.splitlines(keepends=True)
    matches = [n for n in ast.walk(tree)
               if isinstance(n, ast.AsyncFunctionDef) and n.name == "call_tool"]
    assert len(matches) == 1, (
        f"expected exactly one `async def call_tool` in the plugin, found "
        f"{len(matches)}. The extractor is the instrument for T2/T3; do not "
        f"read a green run from an extractor that lost its target."
    )
    node = matches[0]
    # FunctionDef.lineno is the `def` line (not the decorator) since Python 3.8,
    # so this slice drops `@server.call_tool()` without string-matching it.
    chunk = textwrap.dedent("".join(lines[node.lineno - 1:node.end_lineno]))

    posted: list[tuple] = []
    real_asyncio = mod.asyncio

    class _AsyncioRecorder:
        """Delegates everything except `to_thread`, which it records."""

        def __getattr__(self, k):
            return getattr(real_asyncio, k)

        def to_thread(self, fn, *a, **kw):
            if getattr(fn, "__name__", "") == "_safe_post_ack":
                posted.append((a[0], sorted(a[1])))

            async def _noop():
                return None

            return _noop()

    mod.asyncio = _AsyncioRecorder()
    mod.TextContent = _StubTextContent

    def _safe_post_ack(agent, ids):
        # Never invoked (see the docstring); present because `call_tool` resolves
        # the NAME as a global before handing it to `to_thread`.
        return None

    mod._safe_post_ack = _safe_post_ack

    def _spawn_bg(coro):
        # The real one schedules on the running loop; the recorder above already
        # captured the decision, so close the coroutine to keep "never awaited"
        # noise out of the run.
        try:
            coro.close()
        except Exception:
            pass

    mod._spawn_bg = _spawn_bg
    mod.my_name = NAME
    exec(compile(chunk, str(PLUGIN), "exec"), mod.__dict__)   # noqa: S102
    return mod.__dict__["call_tool"], posted


def _ack_and_posts(mod, ids):
    """Drive `broker_ack` and return (parsed response, posted-set records)."""
    call_tool, posted = _bind_call_tool(mod)
    out = asyncio.run(call_tool("broker_ack", {"event_ids": list(ids)}))
    assert out and getattr(out[0], "text", None), (
        "broker_ack returned no text content -- the extractor bound something "
        "that is not the tool body."
    )
    return json.loads(out[0].text), posted


def _ack(mod, ids):
    """Drive `broker_ack` and return the parsed response object."""
    call_tool, _posted = _bind_call_tool(mod)
    out = asyncio.run(call_tool("broker_ack", {"event_ids": list(ids)}))
    assert out and getattr(out[0], "text", None), (
        "broker_ack returned no text content -- the extractor bound something "
        "that is not the tool body; T2/T3 results below would be meaningless."
    )
    result = json.loads(out[0].text)
    # POSITIVE CONTROL on the binding: whatever else changes, the tool has
    # always returned an `ok` field. If this is missing the harness is broken,
    # not the plugin.
    assert "ok" in result, (
        f"bound tool body returned {result!r}, which carries no `ok` field. "
        f"The harness is broken; do not read the assertions below."
    )
    return result




# ---------------------------------------------------------------------------
# Fixture builders -- synthetic home, no real relay state touched
# ---------------------------------------------------------------------------

def _fresh_home(tmp_path: pathlib.Path, monkeypatch) -> pathlib.Path:
    home = tmp_path / "relayhome"
    (home / ".claude" / "sessions").mkdir(parents=True)
    (home / ".claude" / "relay" / "confirmed").mkdir(parents=True)
    neutral = tmp_path / "neutral_cwd"
    neutral.mkdir()
    # HYGIENE, NOT A NAME SOURCE ANY MORE. This used to read "`_resolve_my_name`
    # falls back to os.getcwd() when the registry row carries no workspace",
    # which described the deleted third source: the resolver now reads the
    # daemon job-state and the relay registry and nothing else, so no
    # `.relay-name.json` in the runner's working directory can be picked up.
    # cwd is still pinned, because a test that depends on the ambient working
    # directory for ANY reason is a test that reads differently per machine --
    # and because the fixtures that DO plant a workspace name file
    # (`_corrupt_registry_named_workspace`) rely on cwd being theirs.
    monkeypatch.chdir(neutral)
    return home


def _write_session_file(home: pathlib.Path, sid: str) -> None:
    """The one identity record on the box not filed under the value that changes."""
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": sid}), encoding="utf-8")


def _write_registry(home: pathlib.Path, rows: dict) -> None:
    (home / ".claude" / "relay" / "registry.json").write_text(
        json.dumps(rows), encoding="utf-8")


def _write_confirmed(home: pathlib.Path, key: str, body: dict) -> None:
    (home / ".claude" / "relay" / "confirmed" / f"{key}.json").write_text(
        json.dumps(body), encoding="utf-8")


def _named_session(tmp_path, monkeypatch, *, demote: bool):
    """Session A, user-set name `broker`, workspace name file owned by A.

    When `demote` is set, apply the reclamation loser's state VERBATIM as the
    two live constructors write it -- `entry["name"] = None` paired with
    `entry.pop("naming_status", None)` (`relay-name/scripts/set_name.py`
    when a name is reclaimed from a live holder, and `hooks/relay/registry_gc.py`
    when duplicate holders are collected).

    THE WORKSPACE NAME FILE IS STILL PLANTED, and that is the point of the
    fixture rather than leftover scaffolding. NEITHER constructor can reach it,
    so it survives the demotion -- and while it was `_resolve_my_name`'s third
    source, that made the routing side keep answering while both delivery
    sources went quiet. It is not a source any more, so this fixture now
    produces a session that degrades. Keeping the file is what makes the
    degradation evidence: if the source ever returns, these tests go red.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    ws = home / "workspace"
    ws.mkdir()
    _write_session_file(home, SID_A)
    (ws / ".relay-name.json").write_text(
        json.dumps({"name": NAME, "set_by_sid": SID_A}), encoding="utf-8")
    row = {"name": NAME, "naming_status": "user_set", "workspace": str(ws)}
    if demote:
        row["name"] = None
        row.pop("naming_status", None)
    _write_registry(home, {SID_A: row})
    return _load(home), home


# ---------------------------------------------------------------------------
# T1 -- the root-cause assertion
# ---------------------------------------------------------------------------

def test_t1_the_demotion_deactivates_push_instead_of_splitting_the_identity(
        tmp_path, monkeypatch):
    """The session must not ACCEPT mail for a mailbox it cannot FILE into.

    THE PROPERTY IS UNCHANGED. WHAT SATISFIES IT IS. This test was written to
    fail against a routing resolver with THREE name-producing sources and a
    delivery resolver with TWO: the demotion nulls the registry row -- the only
    source they shared that still answered -- so routing reached its third
    source, the workspace `.relay-name.json` whose `set_by_sid` still matched,
    and returned `broker` while delivery fell back to the session id.

    THE THIRD SOURCE WAS DELETED (2026-08-15) RATHER THAN COMPENSATED FOR, so
    the assertion below is no longer `routing == delivery`. Under the demotion
    NO source answers: routing is None, push goes inactive, and the session
    accepts nothing rather than accepting what it cannot confirm. Mirroring the
    file onto the delivery side instead would have SPLIT the key across the
    process boundary (D139) -- the hook cannot see it.

    THIS IS THE RATIFIED COST, not an accident: a demoted session receives
    nothing further until a human re-claims a name for it, and nothing restores
    that automatically. The remedy is `/relay-name`, and the degraded-boot
    diagnostic prints the presence of the workspace file precisely so an
    operator can recognise this state.
    """
    mod, _home = _named_session(tmp_path, monkeypatch, demote=True)
    routing = mod._routing_name(SID_A)
    delivery = mod._delivery_key()
    # POSITIVE CONTROL: the demotion really did take the shared source away, so
    # this is the fault state and not a fixture that never had a name.
    assert delivery == SID_A, (
        f"fixture did not degrade the delivery key to the session id "
        f"(got {delivery!r}); this test would then be measuring nothing."
    )
    assert routing is None, (
        f"push is ACTIVE under {routing!r} for a session whose receipts are "
        f"filed under {delivery!r}. Every message it accepts is unconfirmable, "
        f"the sender's delivery row never closes, and a human is eventually "
        f"paged. The workspace name file is the only thing that can still "
        f"answer here -- it is not supposed to be a name source any more."
    )


def test_t1_control_without_the_demotion_both_sides_agree(tmp_path, monkeypatch):
    """CONTROL FOR T1 -- passes today, and must keep passing.

    Identical fixture, demotion NOT applied. Both resolvers return `broker`. So
    T1's failure is caused by the demotion, not by the harness: without this,
    a fixture that simply failed to construct a named session would produce the
    same red as the real defect.
    """
    mod, _home = _named_session(tmp_path, monkeypatch, demote=False)
    assert mod._routing_name(SID_A) == NAME
    assert mod._delivery_key() == NAME


# ---------------------------------------------------------------------------
# T2 -- the reporting assertion
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("awaiting,label", [
    (None, "allowlist_absent"),
    ([], "allowlist_present_but_empty"),
], ids=["allowlist_absent", "allowlist_present_but_empty"])
def test_t2_refusal_response_carries_the_mailbox_key_and_the_floor(
        tmp_path, monkeypatch, awaiting, label):
    """A refusal must name the mailbox it read and the floor it read there.

    IT FAILED WITH A MISSING KEY WHEN WRITTEN. The response was assembled from
    four fields only (`ok`, `acked`, `refused`, `reasons`); neither
    `delivery_key` nor `floor` appeared anywhere in it, and no wording change
    can supply a fact the response does not carry. `call_tool` now emits both on
    every answer, read out of `_gate_broker_ack`'s `diag` mapping, so this
    passes -- see the "NAME THE MAILBOX AND THE FLOOR ON EVERY ANSWER" comment
    at the tool's return.

    BOTH REFUSAL PATHS ARE COVERED, and when this was written they were
    byte-identical at the call site -- that indistinguishability was half the
    defect: an ABSENT allow-list (the fail-closed early return) and a PRESENT
    allow-list that simply does not list the id emitted the same result. They
    are distinguishable now (`ALLOWLIST_UNAVAILABLE` vs `NOT_AWAITED`, decided
    by the gate's own `allowlist` diagnostic), and the parametrisation stays
    because a fix that reported the mailbox on only one of them would leave the
    other exactly as opaque.

    WHY `delivery_key` STAYS IN THE RESPONSE EVEN AFTER THE SPLIT IS CLOSED:
    the plugin-side and hook-side key computations are held in step by a comment
    rather than by a test, and this field is the only thing that would expose a
    divergence between them.

    THE NO-IDENTITY REFUSAL CARRIES THE SAME TWO KEYS, as explicit nulls; that
    is `test_t2b_no_identity_refusal_carries_both_keys_explicitly_null`.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    body = {"floor": 0, "acked": []}
    if awaiting is not None:
        body["awaiting_ack"] = awaiting
    _write_confirmed(home, NAME, body)
    mod = _load(home)

    result = _ack(mod, [7777])
    # Precondition, not the assertion: this must genuinely be an all-refused
    # call, or T2 would be measuring the wrong response.
    assert result.get("acked") == [] and result.get("refused") == [7777], (
        f"fixture did not produce an all-refused call ({label}): {result!r}"
    )
    assert "delivery_key" in result and "floor" in result, (
        f"refusal response {result!r} names neither the mailbox it read nor "
        f"the floor it read there, so the reader cannot tell a transient miss "
        f"from a session filing into the wrong mailbox permanently."
    )


# ---------------------------------------------------------------------------
# T2b -- the same two keys on the path that cannot name a mailbox
# ---------------------------------------------------------------------------

def test_t2b_no_identity_refusal_carries_both_keys_explicitly_null(
        tmp_path, monkeypatch):
    """The NO_IDENTITY refusal must carry `delivery_key` and `floor` too, as
    explicit nulls.

    WHY THIS PATH IS NOT ALLOWED TO BE THE EXCEPTION. The tool description tells
    every agent the result carries the mailbox and the floor "so a refusal can be
    read without running anything else". A description that matches behaviour is
    exactly what separates a permanent failure from a transient one here, so a
    refusal path that quietly drops the pair re-creates the defect this change
    exists to remove -- one layer up, in the text rather than in the code. The
    alternative on the table was documenting an exception; a uniform shape and an
    honest value is the smaller thing to be right about.

    `null` IS THE HONEST VALUE, NOT A PLACEHOLDER. This branch is reached because
    `_delivery_key()` could not establish a mailbox at all, so there is no key to
    name and no floor was read. That is precisely what the caller needs told.

    THE PRESENCE AND THE VALUE ARE ASSERTED SEPARATELY, and that separation is
    the control. `result.get("delivery_key")` returns None for a MISSING key and
    for a null one alike, so a single `.get() is None` assertion would stay green
    if the pair were dropped again -- the exact regression this test exists to
    catch. `in` answers presence; `is None` answers the value.

    THE SECOND HALF IS THE OTHER DIRECTION. A fix that emitted null on EVERY path
    would satisfy the first half and destroy the field's entire purpose, so the
    same two keys are read back on an ordinary refusal and must be POPULATED
    there. T2 above pins their presence on both ordinary refusal arms; this pins
    that presence is not being bought with an empty value.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    # NO session file written -> _read_session_identity() returns None ->
    # _delivery_key() returns None -> the NO_IDENTITY branch.
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    mod = _load(home)

    result = _ack(mod, [7777])
    # POSITIVE CONTROL: this really is the no-identity branch and not some other
    # refusal that happens to be missing a key.
    assert result.get("ok") is False and \
        result.get("reasons", {}).get("7777") == "NO_IDENTITY", (
            f"fixture did not reach the NO_IDENTITY branch: {result!r}"
        )
    assert "delivery_key" in result, (
        f"the NO_IDENTITY refusal dropped `delivery_key`. The tool description "
        f"promises every result carries it, so a caller that reads the field to "
        f"tell a permanent identity fault from a race finds nothing on the one "
        f"path where the fault is certain. Response: {result!r}"
    )
    assert "floor" in result, (
        f"the NO_IDENTITY refusal dropped `floor`. Response: {result!r}"
    )
    assert result["delivery_key"] is None, (
        f"the NO_IDENTITY refusal names a mailbox ({result['delivery_key']!r}) "
        f"on the path that is reached precisely because no mailbox could be "
        f"established. Response: {result!r}"
    )
    assert result["floor"] is None, (
        f"the NO_IDENTITY refusal reports a floor ({result['floor']!r}) it never "
        f"read -- no confirmed-set was opened on this path. Response: {result!r}"
    )

    # CONTROL, FIRING THE OTHER WAY: an ordinary refusal must still POPULATE
    # both. Without this, a fix that returned null everywhere would be green
    # above while making the fields worthless.
    home2 = _fresh_home(tmp_path / "ordinary", monkeypatch)
    _write_session_file(home2, SID_A)
    _write_registry(home2, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    _write_confirmed(home2, NAME, {"floor": 3, "acked": []})   # NO awaiting_ack
    mod2 = _load(home2)

    ordinary = _ack(mod2, [7777])
    assert ordinary.get("refused") == [7777], (
        f"CONTROL FAILED: the second fixture is not an all-refused ordinary "
        f"call: {ordinary!r}"
    )
    assert ordinary.get("delivery_key") == NAME, (
        f"CONTROL FAILED: an ordinary refusal no longer names its mailbox "
        f"({ordinary!r}). The nulls asserted above would then be the whole "
        f"behaviour rather than the no-identity case."
    )
    assert ordinary.get("floor") == 3, (
        f"CONTROL FAILED: an ordinary refusal no longer reports the floor it "
        f"read ({ordinary!r})."
    )


# ---------------------------------------------------------------------------
# T2c -- the same two keys on the two arms that answer before a mailbox exists
# ---------------------------------------------------------------------------

# The shape EVERY acknowledgement-arm return declares. `error` is extra on the
# failure paths and is deliberately not in here: it is the one key whose absence
# carries information (there was no error). These six are the shape a consumer
# parses without branching.
_ACK_CORE_KEYS = {"ok", "acked", "refused", "reasons", "delivery_key", "floor"}


def _ack_raw(mod, arguments: dict):
    """Drive `broker_ack` with ARBITRARY arguments and return the parsed body.

    `_ack` builds `{"event_ids": [...]}` and so can never reach the
    rejected-input arm. This one hands the tool whatever it is given, which is
    the only way to exercise a branch whose trigger is a malformed argument.
    """
    call_tool, _posted = _bind_call_tool(mod)
    out = asyncio.run(call_tool("broker_ack", arguments))
    assert out and getattr(out[0], "text", None), (
        "broker_ack returned no text content -- the extractor bound something "
        "that is not the tool body."
    )
    return json.loads(out[0].text)


def _healthy_named_home(tmp_path, monkeypatch, **confirmed):
    """A session that COULD name a mailbox: session record, user-set row."""
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    if confirmed:
        _write_confirmed(home, NAME, dict(confirmed))
    return _load(home)


def test_t2c_the_early_ack_returns_carry_both_keys_explicitly_null(
        tmp_path, monkeypatch):
    """EVERY acknowledgement-arm return must carry `delivery_key` and `floor`,
    including the two that answer before any mailbox is opened.

    THE SHAPE WAS ADVERTISED AS UNIFORM AND WAS NOT. The tool description tells
    every agent the result carries the mailbox and its floor "so a refusal can be
    read without running anything else", and
    `test_t2b_no_identity_refusal_carries_both_keys_explicitly_null` pins that
    promise on the no-identity path. Two arms still dropped the pair: the
    rejected-input return (`event_ids` is not an array) and the nothing-to-do
    return (no usable id survived the sanitiser). A caller that reads
    `delivery_key` to tell a permanent identity fault from a race got nothing
    back from either, and could not tell which of those two things had happened.

    `null` IS THE HONEST VALUE ON BOTH, NOT A PLACEHOLDER. Neither arm reaches
    `_delivery_key()`: no mailbox was established and no confirmed-set was
    opened, so there is no key to name and no floor was read. THE FIXTURE IS A
    SESSION THAT COULD HAVE NAMED ONE -- session record present, user-set
    registry row -- precisely so the nulls are a statement about the PATH rather
    than about a degraded session.

    PRESENCE AND VALUE ARE ASSERTED SEPARATELY, and that separation is the
    control that makes this test able to fail. `result.get("floor")` returns None
    for a MISSING key and for a null one alike, so a single `.get() is None`
    assertion would stay green through the exact regression this exists to catch
    -- the pair being dropped again. `in` answers presence; `is None` answers the
    value.

    THE OTHER DIRECTION IS AT THE END. A change that emitted null everywhere
    would satisfy every assertion above while destroying what the fields are for,
    so the same two keys are read off the MAIN return on the same fixture and
    must be POPULATED there.

    NOT THE REPLY TOOL. `peer_reply`'s returns carry neither key and must not
    start: that tool makes no such promise, so adding them there would be a
    different false statement rather than the same true one.
    """
    branches = {}

    mod_bad = _healthy_named_home(tmp_path / "bad", monkeypatch)
    branches["rejected input"] = _ack_raw(mod_bad, {"event_ids": "2962"})
    # PRECONDITION: this really is the rejected-input arm.
    assert branches["rejected input"].get("ok") is False, branches["rejected input"]
    assert "must be an array" in branches["rejected input"].get("error", ""), (
        f"fixture did not reach the rejected-input arm: "
        f"{branches['rejected input']!r}"
    )

    mod_empty = _healthy_named_home(tmp_path / "empty", monkeypatch)
    # Every element is dropped by the sanitiser: a string, a bool (an int
    # subclass, excluded on purpose), a negative and a zero.
    branches["no usable ids"] = _ack(mod_empty, ["x", True, -3, 0])
    # PRECONDITION: this really is the nothing-to-do arm and not a refusal.
    assert branches["no usable ids"].get("ok") is True, branches["no usable ids"]
    assert branches["no usable ids"].get("acked") == [] and \
        branches["no usable ids"].get("refused") == [], branches["no usable ids"]

    for label, result in branches.items():
        assert "delivery_key" in result, (
            f"the {label} return dropped `delivery_key`. The tool description "
            f"promises every result carries it, so a caller reading the field "
            f"cannot tell this arm from a server that does not report it at "
            f"all. Response: {result!r}"
        )
        assert "floor" in result, (
            f"the {label} return dropped `floor`. Response: {result!r}"
        )
        assert result["delivery_key"] is None, (
            f"the {label} return names a mailbox ({result['delivery_key']!r}) "
            f"on a path that never called `_delivery_key()`. Response: {result!r}"
        )
        assert result["floor"] is None, (
            f"the {label} return reports a floor ({result['floor']!r}) it never "
            f"read -- no confirmed-set was opened on this path. Response: "
            f"{result!r}"
        )

    # CONTROL, FIRING THE OTHER WAY: the same fixture on the MAIN return must
    # POPULATE both. Without this, emitting null unconditionally would be green
    # above while making the two fields worthless.
    mod_ok = _healthy_named_home(tmp_path / "ok", monkeypatch,
                                 floor=3, acked=[], awaiting_ack=[2962])
    ordinary = _ack(mod_ok, [2962])
    assert ordinary.get("acked") == [2962], (
        f"CONTROL FAILED: the third fixture did not reach the main return: "
        f"{ordinary!r}"
    )
    assert ordinary.get("delivery_key") == NAME, (
        f"CONTROL FAILED: the main return no longer names its mailbox "
        f"({ordinary!r}). The nulls asserted above would then be the whole "
        f"behaviour rather than these two arms."
    )
    assert ordinary.get("floor") == 3, (
        f"CONTROL FAILED: the main return no longer reports the floor it read "
        f"({ordinary!r})."
    )


def test_t2c_control_the_reply_tool_returns_stay_out_of_this():
    """THE BOUNDARY CONTROL: the ack arm declares the full shape; `peer_reply`
    declares none of it.

    THIS IS THE EXHAUSTIVE HALF. `test_t2d_...` drives the four acknowledgement
    returns and reads their VALUES; this one reads the SOURCE, so it also proves
    there are exactly four of them and that a fifth could not be added without
    the shape. `_ACK_CORE_KEYS` on every one.

    The uniform-shape promise is made by `broker_ack`'s description and by
    nothing else. Adding `delivery_key` and `floor` to the reply tool's returns
    would state something that tool never established -- the same class of defect
    as omitting them from an acknowledgement return, pointed the other way. So
    the fix is pinned at its edge as well as at its target.

    Read out of the SOURCE. The reply arm's returns sit behind network and
    identity conditions this harness does not stand up, so the assertion is on
    the literal dict keys the arm can emit: every `json.dumps({...})` after the
    `if name not in ("peer_reply", "reply")` guard inside `call_tool`.
    """
    tree = ast.parse(PLUGIN.read_text(encoding="utf-8"))
    fn = [n for n in ast.walk(tree)
          if isinstance(n, ast.AsyncFunctionDef) and n.name == "call_tool"][0]

    # The reply arm begins at the `if name not in ("peer_reply", "reply")`
    # guard; everything at or after that statement's line is the reply half.
    guards = [n for n in fn.body
              if isinstance(n, ast.If) and "peer_reply" in ast.unparse(n.test)]
    assert len(guards) == 1, (
        f"POSITIVE CONTROL FAILED: expected exactly one `peer_reply` dispatch "
        f"guard in call_tool, found {len(guards)}. The assertion below would "
        f"scan the wrong half of the function."
    )
    split = guards[0].lineno

    ack_keys, reply_keys = [], []
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Call)
                and ast.unparse(node.func) == "json.dumps"
                and node.args and isinstance(node.args[0], ast.Dict)):
            continue
        keys = {k.value for k in node.args[0].keys
                if isinstance(k, ast.Constant)}
        (reply_keys if node.lineno >= split else ack_keys).append(keys)

    # POSITIVE CONTROL: both halves were actually found. An empty list makes the
    # assertions below vacuous in opposite directions.
    assert len(ack_keys) == 4, (
        f"expected 4 acknowledgement-arm `json.dumps` returns, found "
        f"{len(ack_keys)}: {ack_keys!r}"
    )
    assert reply_keys, "POSITIVE CONTROL FAILED: no reply-arm payloads found."

    for keys in ack_keys:
        assert _ACK_CORE_KEYS <= keys, (
            f"an acknowledgement-arm return omits "
            f"{sorted(_ACK_CORE_KEYS - keys)!r}: {sorted(keys)!r}"
        )
    for keys in reply_keys:
        assert not ({"delivery_key", "floor"} & keys), (
            f"a `peer_reply` return grew {sorted({'delivery_key', 'floor'} & keys)!r}. "
            f"That tool never opens a mailbox and promises no such field; "
            f"claiming one there is the same defect as omitting it on the ack "
            f"arm, pointed the other way. Payload keys: {sorted(keys)!r}"
        )


# ---------------------------------------------------------------------------
# T2d -- the full declared shape, on all four acknowledgement returns
# ---------------------------------------------------------------------------

def test_t2d_every_acknowledgement_return_declares_the_full_shape(
        tmp_path, monkeypatch):
    """All four acknowledgement returns must carry all six core keys, and the
    VALUES must be the ones each path actually established.

    THE HALF T2c LEFT OPEN. T2c pinned `delivery_key` and `floor` on the two
    early arms. The rejected-input arm was still the only response in the whole
    tool that carried no `acked`, no `refused` and no `reasons` either -- so a
    consumer parsing the acknowledgement result had to special-case the one
    response it gets by having sent the wrong thing. The tool description
    promises a per-id `reasons` map on the result; that promise was false on
    that arm, which is the same description-does-not-match-behaviour defect this
    change exists to remove, sitting inside the fix for it.

    THE THREE ADDED VALUES ARE HONEST, NOT PADDING. The call is rejected before
    any id exists: nothing was acked, nothing was refused, and no id has a
    reason. An empty list is what happened; an absent key is a refusal to say.

    PRESENCE AND VALUE ARE ASSERTED SEPARATELY throughout -- `k in result` then
    the value -- because `result.get(k)` cannot distinguish a missing key from a
    null or empty one, and a dropped key is the exact regression this exists to
    catch.

    THE VALUE TABLE IS ALSO THE OTHER-DIRECTION CONTROL. An implementation that
    emitted the shape with empty/null values everywhere would satisfy every
    presence assertion and destroy what the fields are for; the `main` row
    requires real ids, a real mailbox and a real floor, so it fails such an
    implementation immediately.
    """
    # arm -> (module, call, expected values for the six core keys)
    mod_bad = _healthy_named_home(tmp_path / "bad", monkeypatch)
    mod_empty = _healthy_named_home(tmp_path / "empty", monkeypatch)
    mod_main = _healthy_named_home(tmp_path / "main", monkeypatch,
                                   floor=3, acked=[], awaiting_ack=[2962])
    # NO session file -> no identity. Built directly rather than via
    # `_healthy_named_home`, which writes one.
    home_noid = _fresh_home(tmp_path / "noid", monkeypatch)
    _write_registry(home_noid, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    mod_noid = _load(home_noid)

    arms = {
        "rejected input": (
            _ack_raw(mod_bad, {"event_ids": "2962"}),
            {"ok": False, "acked": [], "refused": [], "reasons": {},
             "delivery_key": None, "floor": None},
        ),
        "no usable ids": (
            _ack(mod_empty, ["x", True, -3, 0]),
            {"ok": True, "acked": [], "refused": [], "reasons": {},
             "delivery_key": None, "floor": None},
        ),
        "no identity": (
            _ack(mod_noid, [7777]),
            {"ok": False, "acked": [], "refused": [7777],
             "reasons": {"7777": "NO_IDENTITY"},
             "delivery_key": None, "floor": None},
        ),
        "main": (
            _ack(mod_main, [2962]),
            {"ok": True, "acked": [2962], "refused": [], "reasons": {},
             "delivery_key": NAME, "floor": 3},
        ),
    }

    # POSITIVE CONTROL: the four fixtures reached four DIFFERENT arms. Without
    # this, a harness that produced the same response four times would satisfy
    # every assertion below.
    signatures = {label: (r.get("ok"), tuple(r.get("acked", [])),
                          tuple(r.get("refused", [])), r.get("delivery_key"))
                  for label, (r, _exp) in arms.items()}
    assert len(set(signatures.values())) == 4, (
        f"the four fixtures did not reach four distinct arms: {signatures!r}"
    )

    for label, (result, expected) in arms.items():
        missing = sorted(k for k in _ACK_CORE_KEYS if k not in result)
        assert not missing, (
            f"the {label} return omits {missing!r}. The tool description "
            f"declares one result shape for this arm, so a consumer that reads "
            f"a missing key cannot tell this path from a server that does not "
            f"report the field at all. Response: {result!r}"
        )
        for key, want in expected.items():
            got = result[key]
            assert got == want and type(got) is type(want), (
                f"the {label} return reports {key}={got!r}, expected {want!r}. "
                f"Response: {result!r}"
            )


# ---------------------------------------------------------------------------
# T3 -- the mislabel assertion
# ---------------------------------------------------------------------------

def test_t3_an_id_at_or_below_the_floor_is_not_reported_as_not_awaited(
        tmp_path, monkeypatch):
    """An id this session already confirmed must not be called unknown.

    Mailbox with the allow-list key ABSENT and a high-water mark of 10; confirm
    id 5.

    IT FAILED WHEN WRITTEN. The fail-closed early return in `_gate_broker_ack`
    -- the `awaiting is None` arm -- returned `([], [], clean_ids)` BEFORE the
    floor was read, so id 5 arrived at the reason loop in neither `already` nor
    `acked` and was labelled `NOT_AWAITED`: not-in-this-session's-allow-list,
    about an id the session's own floor says it has already confirmed. That is
    not vague, it is wrong. The floor read was MOVED ABOVE that return (see
    "READ THE FLOOR BEFORE THE FAIL-CLOSED RETURN" in `_gate_broker_ack`), which
    is what makes this pass.

    THIS ASSERTION DOES NOT ASK FOR THE FOLD TO MOVE (INV-1). Id 5 is at or
    below the floor, so there is nothing left to fold and nothing was denied;
    only the LABEL is wrong. A fix that starts folding ids the allow-list does
    not carry would satisfy this line and reopen the silent-loss hole -- so
    the assertion is on `reasons`, never on `acked`.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    _write_confirmed(home, NAME, {"floor": 10, "acked": []})   # NO awaiting_ack
    mod = _load(home)

    result = _ack(mod, [5])
    reasons = result.get("reasons", {})
    assert "5" in reasons, (
        f"no reason code was emitted for id 5 at all: {result!r}. The "
        f"assertion below would pass vacuously."
    )
    assert reasons["5"] != "NOT_AWAITED", (
        f"id 5 is at or below this session's own floor of 10 -- it was already "
        f"confirmed here -- yet it is reported as {reasons['5']!r}, i.e. not in "
        f"this session's allow-list. Full response: {result!r}"
    )


# ---------------------------------------------------------------------------
# T6 -- the regression guard on the fold (INV-2)
# ---------------------------------------------------------------------------

def test_t6_gate_returns_exactly_three_values(tmp_path, monkeypatch):
    """PASSES TODAY AND MUST KEEP PASSING. This is the assertion that fires if
    an implementer takes the obvious shortcut.

    Seven fixed-arity call sites of the form
    `foldable, already, refused = lrc_server._gate_broker_ack(MY_SID, [2962])`
    live in another peer's test file, which imports the DEPLOYED plugin by
    path. Widening the return to four values breaks all seven from a Release
    alone -- no commit in that tree, no gate to catch it. If the gate needs to
    surface the mailbox key or the floor, return it by another route: a mutable
    diagnostics dict passed in, or a module-level last-call record.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    _write_confirmed(home, NAME, {"floor": 0, "acked": [], "awaiting_ack": [2962]})
    mod = _load(home)

    out = mod._gate_broker_ack(NAME, [2962])
    assert isinstance(out, tuple) and len(out) == 3, (
        f"_gate_broker_ack returned {len(out) if hasattr(out, '__len__') else '?'} "
        f"value(s), not 3. Seven fixed-arity unpacks in another peer's tree "
        f"break on a Release alone."
    )
    foldable, already, refused = out          # the peer's exact unpack shape
    assert isinstance(refused, list)
    assert isinstance(foldable, list) and isinstance(already, list)
    # The fold itself, pinned so a widened arity cannot be smuggled in as a
    # behaviour change: an id IN the allow-list is foldable, and nothing else
    # moved. This does NOT re-test the gate (that is `test_relay_lifecycle.py`'s
    # job); it is the minimum that makes the arity assertion non-vacuous.
    assert foldable == [2962] and already == [] and refused == []


# ---------------------------------------------------------------------------
# INV-3 control -- "could not check" is not "checked, and different"
# ---------------------------------------------------------------------------

def test_control_unreadable_registry_is_not_a_disagreement(tmp_path, monkeypatch):
    """PASSES TODAY AND MUST KEEP PASSING.

    A momentarily unreadable registry must not be recorded as a fact about this
    session. The plugin already has the three-outcome vocabulary for it:
    `_NAME_UNKNOWN` (could not establish) is a distinct object from `None`
    (established nameless), and `_delivery_key` turns the first into a REFUSAL
    rather than into a session-id key.

    THE AGREEMENT GATE MUST BUILD ON THIS, NOT AROUND IT -- and that sentence is
    now history rather than instruction. The gate is gone (see the module
    docstring); what survives is the reason this vocabulary was needed in the
    first place, which has nothing to do with any gate: a transient read failure
    must not be RECORDED as a fact about this session. If it were, an unreadable
    registry would mint a permanent session-id-keyed confirmed-set beside the
    hook's name-keyed one, and the refusals that follows are indistinguishable
    from an ordinary miss. That is durable damage; push being off for one poll
    interval is not.

    THIS TEST NEVER REACHED THE GATE, AND SAYING SO IS WHY IT SURVIVES INTACT.
    It asserts on the two functions the vocabulary lives in and never calls
    `_routing_name` -- measured, back when the gate existed: deleting the
    `if dkey is None: return name` arm left both assertions below green. Its
    fixture pins cwd to an empty directory and writes no workspace name file, so
    nothing here depended on the deleted third source either.
    `test_inv3_unreadable_registry_degrades_quietly_rather_than_splitting`
    carries the behavioural half on a fixture that does plant that file.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    (home / ".claude" / "relay" / "registry.json").write_text(
        "{ this is not json", encoding="utf-8")
    mod = _load(home)

    assert mod._delivery_key_name(SID_A) is mod._NAME_UNKNOWN, (
        "an unreadable registry was reported as ESTABLISHED NAMELESS; the "
        "caller then keys by session id and a transient fault becomes a "
        "permanent orphan mailbox."
    )
    assert mod._delivery_key() is None, (
        "an unreadable registry produced a delivery key rather than a refusal."
    )


def _corrupt_registry_named_workspace(tmp_path, monkeypatch):
    """Fixture: registry UNREADABLE, workspace name file naming THIS session,
    cwd set to that workspace.

    Both arms of the deleted third source are supplied -- the file names this
    sid and sits in cwd -- so this is the shape that USED to resolve a routing
    name while `_delivery_key` refused. cwd rather than
    `registry[sid].workspace` because a corrupt registry cannot supply the
    workspace field, which is exactly the shape a real blip takes. Callers that
    want a READABLE registry overwrite it after calling.
    """
    home = tmp_path / "relayhome"
    (home / ".claude" / "sessions").mkdir(parents=True)
    (home / ".claude" / "relay" / "confirmed").mkdir(parents=True)
    ws = home / "workspace"
    ws.mkdir()
    (ws / ".relay-name.json").write_text(
        json.dumps({"name": NAME, "set_by_sid": SID_A}), encoding="utf-8")
    monkeypatch.chdir(ws)
    _write_session_file(home, SID_A)
    return home, ws


def test_inv3_unreadable_registry_degrades_quietly_rather_than_splitting(
        tmp_path, monkeypatch):
    """THE ONE MEASURED BEHAVIOUR CHANGE OF THE 2026-08-15 SUBTRACTION, pinned
    so it is a decision on the record rather than a surprise in the field.

    THIS TEST ASSERTED THE OPPOSITE UNTIL 2026-08-15, under the name
    `test_inv3_unreadable_registry_does_not_deactivate_push`, and it was right
    to: back then `_resolve_my_name` had a third source (`<workspace>/
    .relay-name.json`), so an unreadable registry still produced a routing name
    while `_delivery_key()` refused -- and an agreement gate that read that
    refusal as a DISAGREEMENT would have deactivated push on a momentary blip.
    INV-3 ("could not check" is not "checked, and different") is what stopped it.

    BOTH THE GATE AND THE SOURCE ARE GONE, so the shape INV-3 protected is no
    longer constructible: with the registry unreadable there is no source left to
    answer, routing is None, and push is inactive for the duration. That is
    strictly QUIET -- the session accepts nothing and confirms nothing -- rather
    than the loud failure INV-3 existed to prevent, in which a session accepted
    mail it could never acknowledge. Inactivity is safer than accepting a message
    under an identity for which this session cannot file a receipt.

    WHAT STILL HOLDS THE ORIGINAL LINE: `_delivery_key_name`'s three-outcome
    vocabulary. `test_control_unreadable_registry_is_not_a_disagreement` above
    pins it, and it is what stops a transient fault from being RECORDED as a
    fact -- an unreadable registry must never mint a session-id-keyed
    confirmed-set. That is the part with a durable consequence; push coming back
    on the next ~3s poll is not.
    """
    home, ws = _corrupt_registry_named_workspace(tmp_path, monkeypatch)
    (home / ".claude" / "relay" / "registry.json").write_text(
        "{ this is not json", encoding="utf-8")
    mod = _load(home)

    # POSITIVE CONTROL 1: the workspace name file is present and names THIS
    # session -- i.e. this is exactly the fixture the deleted third source used
    # to answer from, not a session that never had a name.
    assert (ws / ".relay-name.json").exists()
    assert json.loads((ws / ".relay-name.json").read_text(
        encoding="utf-8"))["set_by_sid"] == SID_A
    # POSITIVE CONTROL 2: the delivery key is genuinely UNESTABLISHED (the
    # `_NAME_UNKNOWN` arm), not merely different.
    assert mod._delivery_key() is None, (
        "fixture did not produce an unestablished delivery key; this test would "
        "then be measuring an ordinary nameless session."
    )
    assert mod._resolve_my_name(SID_A) is None, (
        "a name resolved from something other than the registry and the daemon "
        "job-state. The receive hook reads only those two, so whatever answered "
        "here is a routing identity with no matching delivery key."
    )
    assert mod._routing_name(SID_A) is None, (
        "push is ACTIVE while the delivery key is unestablished: the session "
        "would accept mail and answer NO_IDENTITY to every acknowledgement."
    )

    # NON-VACUITY CONTROL: the same fixture shape with a READABLE registry
    # carrying a user-set row resolves a name and activates push. Without it a
    # green above could just mean this fixture can never produce a name.
    home2, _ws2 = _corrupt_registry_named_workspace(tmp_path / "second", monkeypatch)
    _write_registry(home2, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    mod2 = _load(home2)
    assert mod2._routing_name(SID_A) == NAME, (
        "NON-VACUITY CONTROL FAILED: push does not activate even for a readable "
        "user-set row, so the assertions above are not evidence about the "
        "unreadable case."
    )
    assert mod2._delivery_key() == NAME


# ---------------------------------------------------------------------------
# The classification boundary the delivery key stands on (D139)
# ---------------------------------------------------------------------------

# Registry rows that are WELL-FORMED DICTS but whose FIELDS are odd. Cross-family
# review proposed that these are "could not establish" and should return
# `_NAME_UNKNOWN` like a corrupt ROW does. Measured, that would be wrong in a way
# no local reasoning shows: the HOOK -- another peer's file, on another ship
# channel -- classifies every one of them as ESTABLISHED NAMELESS and keys by
# session id. Changing only the plugin does not correct a classification, it
# SPLITS the delivery key across the process boundary (D139): `_delivery_key()`
# would return None and every `broker_ack` from a session with such a row would
# answer NO_IDENTITY while the hook went on filing under the sid.
_FIELD_ODD_ROWS = {
    "demotion (both live constructors)": {"name": None},
    "name present, naming_status absent": {"name": NAME},
    "naming_status inherited": {"name": NAME, "naming_status": "inherited"},
    "name null + user_set": {"name": None, "naming_status": "user_set"},
    "name empty + user_set": {"name": "", "naming_status": "user_set"},
    "name non-string + user_set": {"name": 5, "naming_status": "user_set"},
    "naming_status non-string": {"name": NAME, "naming_status": 7},
}

# Row shapes that ARE could-not-establish on both sides. Present as the control
# that fires the other way: if the assertion below passed for every input, it
# would not be discriminating between the two classes at all.
_ROW_TYPE_BROKEN = {
    "row is a string": NAME,
    "row is a list": [NAME],
    "row is null": None,
}


def _load_hook(home: pathlib.Path):
    """Import the receive hook against the same synthetic home.

    Uses the DEPLOYED copy (`~/.claude/hooks/relay/shared.py`), the copy that
    actually runs beside the plugin, and SKIPS when it is absent rather than
    passing vacuously -- the same discipline as the cross-file fold agreement
    test in `test_name_fold_plugin.py`. That file is another peer's (Channel B);
    it is imported read-only and never written.
    """
    shared_py = pathlib.Path.home() / ".claude" / "hooks" / "relay" / "shared.py"
    if not shared_py.exists():
        pytest.skip("hooks/relay/shared.py not deployed on this box")
    spec = importlib.util.spec_from_file_location("relay_shared_under_test",
                                                  str(shared_py))
    hook = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(hook)
    if not hasattr(hook, "_delivery_key"):
        pytest.skip("deployed shared.py predates _delivery_key (Channel B not landed)")
    # CONTAINMENT: the hook freezes its registry path at import. Re-point it, and
    # neutralise the job-state source, which resolves from the REAL home and is
    # not the source under test here. The check itself lives in
    # `_assert_registry_contained` -- same `_within` semantic as the plugin-side
    # `_assert_contained`, and reachable by a control, which it was not while it
    # sat inline here.
    hook.REGISTRY_PATH = home / ".claude" / "relay" / "registry.json"
    hook.resolve_daemon_job_name = lambda sid: None
    _assert_registry_contained(hook.REGISTRY_PATH, home)
    return hook


@pytest.mark.parametrize("label", sorted(_FIELD_ODD_ROWS))
def test_plugin_and_hook_agree_on_field_odd_registry_rows(tmp_path, monkeypatch,
                                                          label):
    """The two sides must compute the SAME delivery key for an odd-field row.

    THIS IS WHAT THE WHOLE IDENTITY FIX STANDS ON. The plugin's delivery key is
    only worth anything if it is the key the HOOK independently computes -- it
    names the confirmed-set file one writes and the other reads, and the broker
    mailbox `broker_ack` posts into. If the plugin ever reclassifies these rows
    while the hook keys by sid, the two part company across the process
    boundary, and on this box the change would look correct in every
    plugin-only test.

    NOT A TEST OF WHICH ANSWER IS RIGHT. It pins that there is ONE answer.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: dict(_FIELD_ODD_ROWS[label])})
    mod = _load(home)
    # Neutralise the plugin's job-state source too, so both sides are answering
    # from the registry alone and a difference cannot come from that leaf.
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)
    hook = _load_hook(home)

    plugin_key = mod._delivery_key()
    hook_key = hook._delivery_key(SID_A)
    # POSITIVE CONTROL: an odd-field row must resolve to a USABLE key on both
    # sides, not to a refusal. A pair of Nones would satisfy equality while
    # meaning the opposite of what this test claims.
    assert plugin_key == SID_A, (
        f"{label!r}: the plugin no longer keys an odd-field row by session id "
        f"(got {plugin_key!r}). If this was a deliberate reclassification, the "
        f"hook must change in the same commit -- see this test's own docstring."
    )
    assert plugin_key == hook_key, (
        f"{label!r}: PLUGIN/HOOK DELIVERY-KEY SPLIT -- plugin {plugin_key!r} vs "
        f"hook {hook_key!r}. A key that differs between the writer and the "
        f"reader is not a key (D139)."
    )


@pytest.mark.parametrize("label", sorted(_ROW_TYPE_BROKEN))
def test_control_row_type_defects_refuse_on_both_sides(tmp_path, monkeypatch,
                                                       label):
    """CONTROL FOR THE TEST ABOVE, firing the other way.

    A row whose TYPE is wrong is could-not-establish on both sides and both
    REFUSE. Without this, the parametrised test above could be green because the
    two sides agree on everything, which would tell us nothing about whether the
    classification boundary exists at all.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: _ROW_TYPE_BROKEN[label]})
    mod = _load(home)
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)
    hook = _load_hook(home)

    assert mod._delivery_key_name(SID_A) is mod._NAME_UNKNOWN, label
    assert mod._delivery_key() is None, label
    assert hook._delivery_key(SID_A) is None, label


# ---------------------------------------------------------------------------
# The invariant that replaced the gate: nothing can make the two disagree
# ---------------------------------------------------------------------------

_ROW_ABSENT = object()      # sentinel: write a registry with no row for SID_A


def _routing_and_delivery(tmp_path, monkeypatch, row, *, raw_registry=None):
    """Resolve BOTH identities for SID_A with a workspace name file present.

    The workspace file names THIS session in every case on purpose: it is what
    the deleted third source consumed, so it is the one ingredient that could
    reintroduce a disagreement. cwd is the workspace, and the registry row also
    points at it, so BOTH arms of the old source are supplied.
    """
    home = tmp_path / "h"
    (home / ".claude" / "sessions").mkdir(parents=True)
    (home / ".claude" / "relay" / "confirmed").mkdir(parents=True)
    ws = home / "workspace"
    ws.mkdir()
    (ws / ".relay-name.json").write_text(
        json.dumps({"name": NAME, "set_by_sid": SID_A}), encoding="utf-8")
    monkeypatch.chdir(ws)
    _write_session_file(home, SID_A)
    if raw_registry is not None:
        (home / ".claude" / "relay" / "registry.json").write_text(
            raw_registry, encoding="utf-8")
    elif row is _ROW_ABSENT:
        _write_registry(home, {})
    else:
        body = dict(row) if isinstance(row, dict) else row
        if isinstance(body, dict):
            body.setdefault("workspace", str(ws))
        _write_registry(home, {SID_A: body})
    mod = _load(home)
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)
    return mod._routing_name(SID_A), mod._delivery_key()


def test_the_two_identities_cannot_disagree_for_any_registry_shape(
        tmp_path, monkeypatch):
    """THE INVARIANT THAT REPLACED THE AGREEMENT GATE, asserted over the whole
    row-shape space rather than at the one fixture the gate was built for.

    A session may accept mail ONLY for the mailbox it files receipts into. Two
    resolvers answer those two questions, and since 2026-08-15 they read the
    SAME TWO RECORDS in the SAME ORDER -- the daemon job-state, then the relay
    registry's `user_set` row. So for every shape either the routing name equals
    the delivery key, or routing is None and push is inactive. There is no shape
    in which the session accepts mail it cannot confirm, and no check anywhere
    that says so: it is true by construction.

    WHY THE FIXTURE ALWAYS PLANTS `<workspace>/.relay-name.json`. That file is
    the deleted third source's only input. Every case below is therefore the
    adversarial one -- if the source ever comes back, the demoted and absent-row
    shapes resolve a routing name while the delivery key falls to the session
    id, and this test goes red. MEASURED in both directions against a mutant
    with the source re-added (2026-08-15): red with it, green without.

    THE TWO POPULATION CHECKS AT THE END ARE THE NON-VACUITY PROOF. An
    implementation that returned None from `_routing_name` for everything would
    satisfy the invariant and be useless, and one that never degraded would mean
    the fixture space is not exercising the interesting half.
    """
    shapes = {
        "valid user_set row": {"name": NAME, "naming_status": "user_set"},
        **{k: dict(v) for k, v in _FIELD_ODD_ROWS.items()},
        **{f"row type broken: {k}": v for k, v in _ROW_TYPE_BROKEN.items()},
        "row absent": _ROW_ABSENT,
    }
    results = {}
    for label, row in shapes.items():
        results[label] = _routing_and_delivery(
            tmp_path / label.replace(" ", "_").replace(":", ""), monkeypatch, row)
    results["registry unreadable"] = _routing_and_delivery(
        tmp_path / "unreadable", monkeypatch, None, raw_registry="{ not json")

    for label, (routing, delivery) in results.items():
        assert routing is None or routing == delivery, (
            f"{label!r}: SPLIT IDENTITY -- push would subscribe under "
            f"{routing!r} while receipts are filed under {delivery!r}. Every "
            f"message to {routing!r} is unconfirmable, the sender's delivery "
            f"row never closes, and a human is eventually paged."
        )

    named = {k for k, (r, _d) in results.items() if r is not None}
    degraded = {k for k, (r, _d) in results.items() if r is None}
    assert named, (
        "NON-VACUITY FAILED: no shape resolved a routing name at all, so the "
        "invariant above is satisfied by a resolver that answers nothing."
    )
    assert degraded, (
        "NON-VACUITY FAILED: every shape resolved a name, so the invariant is "
        "not distinguishing the degraded half."
    )
    assert "valid user_set row" in named
    assert "demotion (both live constructors)" in degraded, (
        "the demoted row -- the fault this whole change was opened for -- still "
        "resolves a routing name."
    )



# ---------------------------------------------------------------------------
# T7 -- INV-1's sibling: the POSTED SET must not move either (D1)
# ---------------------------------------------------------------------------

def _absent_allowlist_home(tmp_path, monkeypatch, *, floor, acked=()):
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    _write_confirmed(home, NAME, {"floor": floor, "acked": list(acked)})  # NO awaiting_ack
    return home


def test_posted_control_a_present_allowlist_does_post(tmp_path, monkeypatch):
    """POSITIVE CONTROL for T7, and it is the whole reason T7 is worth reading.

    The absence assertion in T7 is answered by the `posted` recorder in
    `_bind_call_tool`. Before this file recorded at the `asyncio.to_thread` seam
    that list was empty for EVERY input, so "nothing was posted" would have been
    true of a working plugin, a broken one, and a plugin with no post at all.
    This test fixes the instrument in place: on the allow-list-PRESENT path a
    gate-passed id IS posted, so a green T7 means the plugin decided not to post,
    not that the recorder cannot see one.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    _write_confirmed(home, NAME, {"floor": 0, "acked": [], "awaiting_ack": [2962]})
    mod = _load(home)

    result, posted = _ack_and_posts(mod, [2962])
    assert result.get("acked") == [2962], result
    assert posted == [(NAME, [2962])], (
        f"the posted-set recorder saw {posted!r} on a path that MUST post. "
        f"Every absence assertion built on this recorder is vacuous until this "
        f"passes."
    )


def test_t7_absent_allowlist_reconciles_the_label_and_posts_nothing(
        tmp_path, monkeypatch):
    """Reading the floor before the fail-closed return may change the LABEL. It
    may not change what is POSTED.

    THE DEFECT THIS PINS. `_gate_broker_ack`'s `awaiting is None` arm returned
    `([], [], clean_ids)` at HEAD, so `already` was EMPTY there and
    `post_ids = foldable | already` was EMPTY too -- that path posted NOTHING to
    the broker. Correcting T3's mislabel put at-or-below-floor ids into `already`
    on the same path, and with an unconditional union those ids began flowing
    into a broker ack POST that had never happened before. MEASURED on the
    shipped bytes, HEAD vs the unconditional union: 320 of 1,008 generated cases
    posted a set HEAD did not.

    WHY THAT IS NOT MERELY A DIFFERENCE. On this path the floor can be SEEDED,
    for a session keyed by its own id, from the legacy sid-scoped plain cursor
    (`_legacy_floor_for` returns a real value exactly when the key is a UUID). A
    floor of that provenance proves sid-addressed receipt; posting on its
    strength closes delivery rows that may never have been delivered, which is
    silent loss.

    BOTH HALVES ARE ASSERTED TOGETHER ON PURPOSE. Passing this by reverting the
    label correction would fail T3, and passing T3 by widening the post would
    fail this -- the pair is what makes "change what the caller is TOLD, never
    what is WRITTEN or POSTED" enforceable rather than aspirational.
    """
    home = _absent_allowlist_home(tmp_path, monkeypatch, floor=10)
    mod = _load(home)

    result, posted = _ack_and_posts(mod, [5, 20])
    # The label correction (T3's property) must still hold on this path...
    assert result.get("reasons", {}).get("5") == "IN_CONFIRMED_SET", (
        f"id 5 is at or below this session's floor of 10; the fail-closed path "
        f"must still say so. Full response: {result!r}"
    )
    assert result.get("reasons", {}).get("20") == "ALLOWLIST_UNAVAILABLE", result
    # ...and the broker must have been told nothing at all.
    assert posted == [], (
        f"the allow-list-ABSENT path POSTED {posted!r} to the broker. HEAD posted "
        f"nothing here. An at-or-below-floor id on this path can carry a floor "
        f"seeded from the legacy sid-scoped cursor, so posting it closes delivery "
        f"rows on evidence that no name-addressed message ever arrived."
    )


def test_t7b_absent_allowlist_posts_nothing_when_the_floor_is_legacy_seeded(
        tmp_path, monkeypatch):
    """The nameless-session shape the widening was actually unsound for.

    No registry row at all, so the delivery key IS the session id and
    `_legacy_floor_for` returns the legacy plain cursor's value rather than 0.
    The confirmed-set does not exist, so the ENTIRE floor comes from that cursor
    -- provenance that says nothing about what this mailbox received.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {})                      # nameless -> key is the sid
    (home / ".claude" / "relay" / "cursors").mkdir(parents=True, exist_ok=True)
    (home / ".claude" / "relay" / "cursors" / f"{SID_A}.json").write_text(
        json.dumps({"last_seen_event_id": 500}), encoding="utf-8")
    mod = _load(home)

    result, posted = _ack_and_posts(mod, [42])
    # POSITIVE CONTROL: the legacy seed really did install a floor above the id,
    # or this test would be measuring an ordinary refusal. The key is
    # `last_seen_event_id`; a plausible-looking `last_event_id` reads as 0 and
    # makes the whole fixture inert -- which is exactly what it did on the first
    # attempt, and what this assertion caught.
    assert result.get("floor", 0) >= 42, (
        f"fixture did not install a legacy-seeded floor above the id: {result!r}"
    )
    assert result.get("reasons", {}).get("42") == "IN_CONFIRMED_SET", result
    assert posted == [], (
        f"posted {posted!r} on a floor whose entire provenance is the legacy "
        f"sid-scoped plain cursor."
    )


# ---------------------------------------------------------------------------
# T8 -- the gate's two operands must be about the SAME session (D2)
# ---------------------------------------------------------------------------

def _rotated_session(tmp_path, monkeypatch, *, record_sid, row_for=SID_A,
                     demote=False):
    """Registry names `row_for`; the CC session record carries `record_sid`."""
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, record_sid)
    row = {"name": NAME, "naming_status": "user_set"}
    if demote:
        row = {"name": None}
    _write_registry(home, {row_for: row})
    return _load(home), home


def test_t8_routing_answers_about_the_session_id_it_was_given(
        tmp_path, monkeypatch):
    """`_routing_name(sid)` must answer about `sid`, not about whatever the CC
    session record currently names.

    WHY THIS TEST OUTLIVED THE THING IT WAS WRITTEN FOR. It was written against
    the agreement gate, which resolved a name from the `session_id` ARGUMENT and
    a delivery key from the CURRENT id -- two operands about two different
    sessions whenever Claude Code rotated the id inside the living process
    (`rename_watch_loop` and the degraded loop re-read it every tick for exactly
    that reason). Pre-fix, `_routing_name(SID_A)` returned SID_B: push repointed
    onto the id of the session the record now named, during the very event this
    subsystem exists to survive. MEASURED over 198 generated fixtures: identical
    on all 66 where the record's id equals the id asked about, different on 17 of
    the 132 where it does not.

    THE GATE IS GONE (see the module docstring) and with it the second read, so
    this now passes BY CONSTRUCTION rather than by a carve-out. It is kept
    because the property is still the one that matters and the fixture still
    discriminates: any future code that makes `_routing_name` consult the
    current session record reintroduces exactly this failure, and this is the
    fixture that catches it.
    """
    mod, _home = _rotated_session(tmp_path, monkeypatch, record_sid=SID_B)
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)

    # POSITIVE CONTROL 1: a routing name genuinely resolved for SID_A, so the
    # gate is reached rather than short-circuited.
    assert mod._resolve_my_name(SID_A) == NAME
    # POSITIVE CONTROL 2: the delivery key computed from the CURRENT record is
    # genuinely a different string -- a resolver that read it WOULD go wrong here.
    assert mod._delivery_key() == SID_B, (
        "fixture did not produce a cross-session operand pair; this test would "
        "then be measuring agreement."
    )
    assert mod._routing_name(SID_A) == NAME, (
        "`_routing_name` answered about the session the CC record currently "
        "names rather than the one it was asked about. An ordinary session-id "
        "rotation now repoints push onto another session's identity."
    )


def test_t8_control_a_same_session_demotion_also_deactivates_push(
        tmp_path, monkeypatch):
    """NON-VACUITY CONTROL FOR T8. Must produce the OTHER outcome, or T8's green
    says nothing about the rotation.

    THE OUTCOME IT REQUIRES CHANGED WITH THE 2026-08-15 SUBTRACTION, and the
    name changed with it -- it used to be `..._the_gate_still_fires` and required
    `_routing_name(SID_A) == SID_A`, the gate adopting the delivery key. There is
    no gate now: with the workspace name file no longer a source, a demoted row
    leaves NO source answering and routing is None.

    THE CONTROL STILL DISCRIMINATES, which is the only reason it survives.
    T8 asserts routing == NAME for a rotated session; this fixture differs only
    in that the record names the session being asked about and the row is
    demoted, and it must NOT return NAME. If both returned NAME, T8 would be
    green against a resolver that ignores its input entirely.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    ws = home / "workspace"
    ws.mkdir()
    _write_session_file(home, SID_A)
    (ws / ".relay-name.json").write_text(
        json.dumps({"name": NAME, "set_by_sid": SID_A}), encoding="utf-8")
    _write_registry(home, {SID_A: {"name": None, "workspace": str(ws)}})
    mod = _load(home)
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)

    assert mod._delivery_key() == SID_A
    assert mod._routing_name(SID_A) is None, (
        "NON-VACUITY CONTROL FAILED: a demoted row still resolves a routing "
        "name, so T8's green does not distinguish the rotation from anything."
    )


def test_t8b_delivery_key_still_takes_no_argument_and_has_no_second_door(
        tmp_path, monkeypatch):
    """The ack path's entry point must keep its parameterless signature, and
    must be the ONLY entry point.

    THE SECOND ASSERTION IS NEW AND IS THE STRONGER HALF. A `_delivery_key_for
    (sid)` helper existed between 2026-08-14 and 2026-08-15 so the agreement gate
    could ask about a NAMED session -- a hair's breadth from reinstating the
    parameter whose removal ended the 2026-08-05 outage, kept apart only by the
    ack path being unable to supply an id. With the gate gone the helper had no
    caller and was deleted, so the door is not merely unused: it does not exist.
    Pinned here rather than trusted to a docstring.
    """
    import inspect

    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    mod = _load(home)

    assert list(inspect.signature(mod._delivery_key).parameters) == [], (
        "`_delivery_key` grew a parameter. A caller can now hand it a session id "
        "it did not establish is current -- the exact shape of the outage."
    )
    assert not hasattr(mod, "_delivery_key_for"), (
        "a session-id-taking delivery-key entry point is back. Whatever needs "
        "it, the ack path must not be able to reach it; the last one existed "
        "for an agreement gate that no longer has anything to check."
    )
    # POSITIVE CONTROL: the function under test actually resolves, so the two
    # assertions above are about a working resolver rather than a dead one.
    assert mod._delivery_key() == NAME


# ---------------------------------------------------------------------------
# T9 -- an ABSENT registry row: classification, measured rather than assumed (D3)
# ---------------------------------------------------------------------------

def test_t9_absent_row_is_established_nameless_on_both_sides(tmp_path, monkeypatch):
    """Row ABSENT must key by session id on BOTH sides, exactly as it does today.

    THE PROPOSAL THIS REFUTES. Cross-family review argued that
    `session_id not in registry` is a TRANSIENT boot race (the MCP booting before
    set_name.py writes the row) and should therefore be `_NAME_UNKNOWN`. It is
    the same argument already refuted for FIELD-odd rows, reached through a
    different arm -- and it fails for the same reason:
    `shared.py::_resolve_name_from_registry` returns None for an absent row and
    its `_delivery_key` keys by sid, so reclassifying only the plugin SPLITS the
    delivery key across the process boundary (D139): `_delivery_key()` would
    return None and every `broker_ack` would answer NO_IDENTITY while the hook
    went on filing under the sid.

    THE BOOT RACE ALSO CANNOT PRODUCE A DIVERGENCE ANY MORE (see the test
    below), so the reclassification would buy nothing even if it were free.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    _write_session_file(home, SID_A)
    _write_registry(home, {})                 # readable registry, NO row for SID_A
    mod = _load(home)
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)
    hook = _load_hook(home)

    assert mod._delivery_key_name(SID_A) is None, (
        "an ABSENT row is now reported as could-not-establish. The hook still "
        "calls it established-nameless, so the two sides no longer compute the "
        "same key -- and a key that differs between writer and reader is not a "
        "key (D139)."
    )
    assert mod._delivery_key() == SID_A
    assert hook._delivery_key(SID_A) == SID_A, (
        "the hook changed its classification of an absent row; the plugin must "
        "move in the same commit or the key splits."
    )


def test_t9b_routing_resolution_never_reads_the_delivery_side_at_all(
        tmp_path, monkeypatch):
    """THIS TEST PASSES BECAUSE THE CODE PATH IT DESCRIBED WAS DELETED, and it
    now asserts the reason rather than the old consequence.

    IT USED TO BE ABOUT READ ORDER. `_routing_name` performed TWO reads -- the
    routing name, then the delivery key -- and compared them. That made the
    ORDER load-bearing: set_name.py writes the registry row FIRST and
    `<workspace>/.relay-name.json` SECOND inside one lock, so reading the file
    before the registry was what stopped an ordinary `/relay-name` from
    looking like a disagreement and deactivating push. The test simulated the
    tightest interleaving that ordering permits.

    THERE IS ONLY ONE READ NOW. The workspace file is not a name source and
    there is no comparison, so `_routing_name` never touches the delivery side
    and no interleaving of the two can produce anything. That is asserted
    DIRECTLY, with a tripwire rather than by reasoning about ordering: both
    delivery-side entry points are replaced with functions that raise, and
    `_routing_name` must still answer. A re-added gate makes this red at once,
    which is more than the old ordering assertion could say.

    THE FIXTURE STILL CARRIES THE INTERLEAVING, because the tripwire alone
    would be green on a resolver that returns None for everything.
    """
    home, ws = _corrupt_registry_named_workspace(tmp_path, monkeypatch)
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    mod = _load(home)
    monkeypatch.setattr(mod, "_read_daemon_job_name", lambda sid: None)

    # POSITIVE CONTROL: the fixture resolves a name at all.
    assert mod._routing_name(SID_A) == NAME

    def _tripwire(*a, **kw):
        raise AssertionError(
            "`_routing_name` consulted the delivery-key side. The two "
            "identities are supposed to be equal BY CONSTRUCTION -- same two "
            "records, same order -- so any read here is a re-added agreement "
            "check, and a check is what put a raw session id on the wire as a "
            "sender name."
        )

    monkeypatch.setattr(mod, "_delivery_key", _tripwire)
    monkeypatch.setattr(mod, "_delivery_key_name", _tripwire)
    assert mod._routing_name(SID_A) == NAME

    # AND THE INTERLEAVING ITSELF IS NOW INERT: a row that lands mid-resolution
    # simply resolves on the next call. Nothing is compared, so nothing about
    # WHEN it landed can deactivate push.
    home2, _ws2 = _corrupt_registry_named_workspace(tmp_path / "second", monkeypatch)
    _write_registry(home2, {})                 # readable, row NOT yet written
    mod2 = _load(home2)
    monkeypatch.setattr(mod2, "_read_daemon_job_name", lambda sid: None)
    assert mod2._routing_name(SID_A) is None, (
        "a name resolved with no registry row and no job-state; the workspace "
        "file is answering again."
    )
    _write_registry(home2, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    assert mod2._routing_name(SID_A) == NAME, (
        "the row landed and the very next call did not pick it up -- the "
        "degraded supervisor's ~3s poll would never activate push."
    )


# ---------------------------------------------------------------------------
# T10 -- remedies that actually work, in every place one is printed (D4)
# ---------------------------------------------------------------------------

def test_t10a_no_identity_payload_does_not_promise_a_remedy_that_cannot_work(
        tmp_path, monkeypatch):
    """`/relay-name` cannot repair the CC session record, so the payload
    must not say it does.

    THE FALSE CLAIM. The payload said re-claiming a name "rewrites both records".
    set_name.py writes the relay registry and the workspace name file; it never
    writes `~/.claude/sessions/<parent pid>.json`, which is Claude Code's own
    file and the other half of what `_delivery_key()` needs -- it resolves the
    CURRENT session id from that record before it can name a mailbox. (This
    sentence named `_delivery_key_for`, the session-id-taking helper that
    existed for the agreement gate; the gate went and the helper went with it,
    and `test_t8b_delivery_key_still_takes_no_argument_and_has_no_second_door`
    is what keeps it gone.) For a missing or unparseable session record the
    prescribed remedy therefore does nothing -- the same class of defect as the
    reconnect text this change removed, one record over.

    The fixture reaches the branch through that exact cause: no session record at
    all.
    """
    home = _fresh_home(tmp_path, monkeypatch)
    # NO session file written -> _read_session_identity() returns None.
    _write_registry(home, {SID_A: {"name": NAME, "naming_status": "user_set"}})
    mod = _load(home)

    result, posted = _ack_and_posts(mod, [7777])
    # POSITIVE CONTROL: this really is the no-identity branch.
    assert result.get("ok") is False, f"not the no-identity branch: {result!r}"
    assert result.get("reasons", {}).get("7777") == "NO_IDENTITY", result
    assert posted == [], "the no-identity branch posted to the broker"
    err = result.get("error", "")
    assert "zzz_not_a_real_token" not in err, "NEGATIVE CONTROL FAILED"
    assert "rewrites both records" not in err, (
        f"the payload still claims one remedy rewrites both records. Error: {err!r}"
    )
    assert "Claude Code" in err, (
        f"the payload does not tell the reader who writes the record "
        f"/relay-name cannot touch, so it names no remedy for that half. "
        f"Error: {err!r}"
    )


def test_t10c_degraded_reply_error_does_not_prescribe_reconnect():
    """The degraded `peer_reply` refusal must not ask for a reconnect.

    THE CONTRADICTION IT CARRIED. The same file, ~500 lines below, logs "NO
    /mcp reconnect needed" and runs a supervisor that re-resolves the name every
    ~3s and activates push by itself. The refusal told the model to ask the
    operator for a step the process performs unprompted.

    Read out of the SOURCE rather than by driving the tool, because the refusal
    sits behind `my_name is None` in a closure whose binding this file rewrites.
    THE STRING IS LIFTED AS AN AST CONSTANT, not searched for in the raw text:
    the refusal is written as adjacent literals across several source lines, so a
    substring search for its wording finds NOTHING and the positive control below
    fires -- measured, on the first attempt at this test.
    """
    tree = ast.parse(PLUGIN.read_text(encoding="utf-8"))
    node = [n for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef) and n.name == "call_tool"][0]
    literals = [n.value for n in ast.walk(node)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    hits = [s for s in literals if "cannot send replies" in s]
    assert len(hits) == 1, (
        f"POSITIVE CONTROL FAILED: expected exactly one degraded-reply refusal "
        f"literal in call_tool, found {len(hits)}. The absence assertion below "
        f"would be vacuous."
    )
    refusal = hits[0]
    assert "zzz_not_a_real_token" not in refusal, "NEGATIVE CONTROL FAILED"
    assert "/relay-name" in refusal, (
        f"POSITIVE CONTROL FAILED: the lifted literal is not the refusal: "
        f"{refusal!r}"
    )
    assert "/mcp reconnect" not in refusal, (
        f"the degraded reply refusal still prescribes a reconnect, which the "
        f"same file's supervisor makes unnecessary. Refusal: {refusal!r}"
    )


def test_t10d_reaper_alert_names_a_remedy_for_an_agent_that_has_exited():
    """Removing a wrong remedy must not leave the biggest arm with none.

    The reaper's own docstring lists "dead / exited" FIRST among the causes of a
    stuck row, and the alert's old action field ended "(else restart)". The
    replacement dropped it, so an operator paged about an agent that had exited
    was told to check it is live and then re-claim a name inside it -- which is
    impossible in a process that is gone.
    """
    from broker import server

    alert = server._build_reaper_alert(
        to_agent="trading_infra", event_id=42, from_agent="broker",
        preview="x", age_s=99, host="H1")
    assert "trading_infra" in alert and "99s" in alert, (
        f"POSITIVE CONTROL FAILED: {alert!r} is not the reaper alert."
    )
    assert "restart" in alert, (
        f"the alert names no action for an agent that is not running. Alert: "
        f"{alert!r}"
    )
    # The security invariant the action field sits inside is unchanged: the
    # untrusted preview must still contribute zero field separators, so the
    # template's own count must not have moved.
    assert alert.count(" " + chr(0x00B7) + " ") == 6, (
        f"the alert's field-separator count changed; test_reaper.py's forgery "
        f"assertion is calibrated on it. Alert: {alert!r}"
    )
