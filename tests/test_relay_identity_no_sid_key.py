# -*- coding: utf-8 -*-
"""ACCEPTANCE TEST for the relay identity root cause (D181).

THE PROPERTY UNDER TEST, stated as one sentence so it can fail:

    A delivery-key resolver must never answer with a SESSION-ID-SHAPED VALUE.
    When it cannot establish an identity it must say so -- "I do not know" has
    to be distinguishable from an answer.

WHY THIS IS THE TEST AND NOT SOMETHING LARGER. The two-day outage of 2026-08-05
was not caused by a missing re-read, a stale cache, or a bad lookup. It was
caused by ONE line answering a question it could not answer:

    return name if _key_accepts(name) else session_id

The returned string is well-formed. It is a valid filename. Nothing downstream
can tell it apart from a real key. So the confirmed-set was opened at a path the
receive hook never writes, every ack was refused, and three separate alarms
stayed silent because every layer was looking at a normal-looking value (D100 --
an unaudited transformation whose output stays well-formed and therefore does
not announce itself).

Five fixes in a row attacked the LOOKUP -- more sources, name-keying, aligning
both sides, re-reading the id more often. None touched the line that turns a
miss into a filename. C-37 was misread throughout as "get the session id out of
the system", which is impossible: Claude Code hands the plugin a session id and
nothing else. C-37 forbids the session id ever BECOMING the key; it has never
forbidden looking a name up from one. This test pins the distinction.

BOTH SIDES ARE TESTED IN ONE FILE, DELIBERATELY. The plugin (Channel A, this
repo) and the receive hook (Channel B, agent-config) each carry their own
`_delivery_key`, and D139 established that the two must compute an IDENTICAL key
or it is not a key. They ship on different channels, in either order, so a test
that pins only one half cannot see the failure that a split deploy produces.
`test_cross_side_agreement` is the half that catches that.

SCOPE, stated because verification machinery is itself scope (C-45): this file
asserts the ONE property whose violation costs silent message loss. It does not
re-test name folding, the ack gate, the confirmed-set writer or the supervisor
-- those have their own tests. It is deliberately small.

THE MUTATION CONTROL IS LOAD-BEARING (D175). A predecessor guard test in this
repo substring-matched its subject and consequently ACCEPTED four different
regressions, including the exactly-inverted guard. So `test_predicate_rejects_
the_original_defect` feeds the pre-fix behaviour to the same predicate the real
assertions use and requires it to be rejected. Without that control, a predicate
that can never fail would report green forever and read as coverage.
"""
import importlib.util
import json
import os
import pathlib
import re

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
PLUGIN = REPO / "harness" / "claude" / "local-relay-channel" / "server.py"

# The root fixtures install the source relay library into an isolated home.
# An explicit override can select another disposable source-derived layout.
HOOK_DIR = pathlib.Path(
    os.environ.get("RELAY_HOOK_DIR", str(pathlib.Path.home() / ".agents" / "hooks" / "relay"))
)
HOOK = HOOK_DIR / "shared.py"

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Two ids that differ, so a test can tell "returned the argument" apart from
# "returned some other id". Lowercase 8-4-4-4-12, the shape CC actually writes.
SID_A = "a0000000-0000-4000-8000-000000000001"
SID_B = "b0000000-0000-4000-8000-000000000002"


def _looks_like_a_session_id(value) -> bool:
    """The predicate the whole file turns on.

    A key is disqualified if it is session-id-shaped, not only if it equals a
    caller's stale argument: returning another session's ID is also aliasing.
    """
    return isinstance(value, str) and bool(UUID_RE.match(value))


def _load(path: pathlib.Path, name: str, home: pathlib.Path):
    """Import a module with its home redirected into a scratch tree.

    CONTAINMENT, not tidiness. On Windows `Path.home()` follows USERPROFILE and
    ignores HOME, so setting an env var does not move it and an uncontained test
    reads -- and could write -- the operator's real relay state.

    TWO REDIRECTS ARE NEEDED BECAUSE THE TWO MODULES RESOLVE PATHS AT DIFFERENT
    TIMES, and this cost a false finding when it was first written with only the
    first. The plugin builds its paths at CALL time (`Path.home() / ".claude" /
    ...` inside each function), so replacing the module's `Path` symbol contains
    it. The hook FREEZES its paths at IMPORT time (`HOME = Path.home()` and the
    seven constants derived from it), so by the time `Path` is replaced those
    constants already point at the real home -- and `shared.py` says so itself:
    "tests can monkeypatch shared.RELAY_DIR and have the sub-paths follow".

    THE FAILURE MODE THAT MOTIVATES THE ASSERTION BELOW: with only the `Path`
    redirect, the hook read the REAL registry, found no row for a fabricated
    session id, and returned that id -- which made the root-cause test pass for
    entirely the wrong reason while a POSITIVE CONTROL failed. An assertion that
    `Path.home()` moved was true and meaningless. So the containment check below
    interrogates the constant the code actually uses, not the function it was
    derived from (D100 rule 5 -- verify the instrument before believing the
    result, in whichever direction it flatters you).
    """
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    real_path_cls = mod.Path

    class HomePath(real_path_cls):
        @classmethod
        def home(cls):
            return cls(str(home))

    mod.Path = HomePath

    # Re-point every import-time constant that was frozen against the real home.
    # Absent attributes are skipped: the two modules do not carry the same set,
    # and this helper serves both.
    if getattr(mod, "HOME", None) is not None:
        mod.HOME = real_path_cls(str(home))
        mod.CLAUDE_DIR = mod.HOME / ".claude"
        mod.RELAY_DIR = mod.CLAUDE_DIR / "relay"
        mod.CURSORS_DIR = mod.RELAY_DIR / "cursors"
        mod.REGISTRY_PATH = mod.RELAY_DIR / "registry.json"
        mod.STATE_DIR = mod.RELAY_DIR / "state"
        mod.SESSIONS_DIR = mod.CLAUDE_DIR / "sessions"
        if hasattr(mod, "ERROR_CURSORS_DIR"):
            mod.ERROR_CURSORS_DIR = mod.RELAY_DIR / "error_cursors"

    # The plugin freezes ONE path at import (`_SESSION_DIR`) and builds the rest
    # at call time. Re-point it here, before containment is checked, so an
    # unresolved session id cannot walk the REAL process table and adopt the
    # operator's own live session file.
    if getattr(mod, "_SESSION_DIR", None) is not None:
        mod._SESSION_DIR = real_path_cls(str(home)) / ".claude" / "sessions"
        mod._session_file = None
        mod._session_file_resolved = False

    _assert_contained(mod, home)
    return mod


def _assert_contained(mod, home: pathlib.Path) -> None:
    """Prove the module cannot reach the operator's real relay state.

    Checks the SYMBOLS THE CODE READS, not `Path.home()`. Every candidate that
    the module actually defines must resolve inside the scratch tree; a module
    that defines none of them is contained by the `Path` redirect alone and is
    verified through the call-time probe at the end.
    """
    real_home = str(pathlib.Path.home()).lower()
    checked = 0
    for attr in ("HOME", "CLAUDE_DIR", "RELAY_DIR", "CURSORS_DIR",
                 "REGISTRY_PATH", "STATE_DIR", "SESSIONS_DIR", "_SESSION_DIR"):
        val = getattr(mod, attr, None)
        if val is None:
            continue
        checked += 1
        s = str(val)
        assert s.lower().startswith(str(home).lower()), (
            f"CONTAINMENT FAILURE: {mod.__name__}.{attr} = {s!r} is outside the "
            f"scratch tree {home}. This test would read the operator's real relay "
            f"state and any result it produced would be meaningless."
        )
        assert not s.lower().startswith(real_home), (
            f"CONTAINMENT FAILURE: {mod.__name__}.{attr} = {s!r} points into the "
            f"REAL home {real_home}."
        )
    # Call-time probe: whatever the module computes fresh must also be contained.
    if hasattr(mod, "_confirmed_dir"):
        s = str(mod._confirmed_dir())
        assert s.lower().startswith(str(home).lower()), (
            f"CONTAINMENT FAILURE: {mod.__name__}._confirmed_dir() = {s!r} is "
            f"outside the scratch tree {home}."
        )
        checked += 1
    assert checked, (
        f"{mod.__name__} exposed none of the paths this check knows about, so "
        f"containment was NOT verified. Do not read a green run as contained -- "
        f"add the symbol this module uses."
    )


def _write_registry(home: pathlib.Path, rows: dict) -> None:
    d = home / ".claude" / "relay"
    d.mkdir(parents=True, exist_ok=True)
    (d / "registry.json").write_text(json.dumps(rows), encoding="utf-8")


def _named(name: str) -> dict:
    return {"name": name, "naming_status": "user_set", "workspace": "/w"}


def _call_delivery_key(mod, boot_sid: str):
    """Call the plugin's `_delivery_key` across the signature change.

    THE SIGNATURE CHANGE IS THE FIX, WHICH IS WHY THIS SHIM EXISTS RATHER THAN
    TWO COPIES OF EACH TEST. Today the function takes the session id as a
    parameter, so a caller can hand it a value nobody established was current --
    which is precisely how a boot-bound id from two days earlier became a
    filename. After the fix it takes NO session id and resolves the current one
    itself, so the stale value is not merely rejected, it is UNPASSABLE (C-47:
    prefer making the failure class unrepresentable over detecting it).

    `inspect` decides which form is present, so the same assertions run against
    both and the pre-fix run is a genuine observation of the defect rather than
    a different test. Passing `boot_sid` into the old form models the boot
    binding exactly.
    """
    import inspect
    sig = inspect.signature(mod._delivery_key)
    if len(sig.parameters) == 0:
        return mod._delivery_key()
    return mod._delivery_key(boot_sid)


@pytest.fixture
def plugin(tmp_path):
    home = tmp_path / "plughome"
    (home / ".claude" / "sessions").mkdir(parents=True)
    (home / ".claude" / "relay").mkdir(parents=True)
    return _load(PLUGIN, "lrc_identity_under_test", home), home


@pytest.fixture
def hook(tmp_path):
    if not HOOK.is_file():
        pytest.skip(
            f"receive hook not found at {HOOK}. This is a SKIP, not a pass: the "
            f"cross-side half of the acceptance criterion was not evaluated. Set "
            f"RELAY_HOOK_DIR to a checkout of agents/hooks/relay to run it."
        )
    home = tmp_path / "hookhome"
    (home / ".claude" / "relay").mkdir(parents=True)
    mod = _load(HOOK, "relay_shared_under_test", home)
    return mod, home


# --------------------------------------------------------------------------
# P1 -- the root cause. FAILS on the pre-fix bytes, by construction.
#
# THE PROPERTY IS NOT "NEVER RETURN A SESSION ID", AND GETTING THAT WRONG WOULD
# HAVE FORCED A WRONG DESIGN. A genuinely nameless session SHOULD be keyed by
# its session id: nothing addresses it by name, so it can be neither
# renamed-into nor replayed, and both implementations say so in their own
# docstrings. The first draft of this file asserted the blunt form and would
# have deleted working behaviour.
#
# What the outage actually needed is the distinction between FOUR states that
# today collapse into one return value:
#     named                        -> the name
#     nameless, session id CURRENT -> the session id       (legitimate)
#     session id STALE             -> refuse               (the outage)
#     registry unreadable          -> refuse               ("could not check")
# --------------------------------------------------------------------------

def test_plugin_key_is_not_a_session_id_the_resolver_did_not_establish(plugin):
    """A living plugin must resolve a rotated session's current identity.

    A cached old session id can produce a different confirmation mailbox from
    the receive hook's current named mailbox. A correct lookup of that stale
    value cannot establish the current delivery identity.
    """
    mod, home = plugin
    # The session file -- the ONE identity record on the box that is not filed
    # under the value that changes -- says the session is now SID_B.
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_B}), encoding="utf-8"
    )
    # SID_B is genuinely nameless; SID_A is the rotated-away id the plugin still
    # holds from boot. A row for SID_A proves the point even more sharply: a
    # resolver keying on the stale id would resurrect a name this session no
    # longer has.
    _write_registry(home, {SID_A: _named("victim")})

    key = _call_delivery_key(mod, SID_A)

    assert key != SID_A and key != "victim", (
        f"the delivery key resolved to {key!r} from a session id the resolver "
        f"never established was current. A key computed from a stale identity is "
        f"the root cause (D181): the only route to the key ran through the exact "
        f"value that changes underneath a living process."
    )


def test_plugin_keys_a_genuinely_nameless_session_by_its_CURRENT_id(plugin):
    """The behaviour that must SURVIVE the fix.

    This is the positive control on the fix's blast radius. If the fix makes the
    test above pass by refusing everything, this one fails -- which is exactly
    the trade the first draft of this file would have forced.
    """
    mod, home = plugin
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_B}), encoding="utf-8"
    )
    _write_registry(home, {})

    key = _call_delivery_key(mod, SID_B)

    assert key == SID_B, (
        f"a nameless session with a CURRENT session id must still key by that "
        f"id; got {key!r}. Refusing here would be a regression, not a fix."
    )


def test_plugin_refuses_when_the_registry_cannot_be_read(plugin):
    """"I could not check" must not be reported as "I have no name".

    An unreadable or corrupt registry is a transient fault. Collapsing it into
    the same answer as established namelessness is how a transient fault becomes
    a permanent orphan file that silently converts a fail-closed refusal into a
    stale allow-list.
    """
    mod, home = plugin
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_B}), encoding="utf-8"
    )
    (home / ".claude" / "relay" / "registry.json").write_text("{ not json", encoding="utf-8")

    key = _call_delivery_key(mod, SID_B)

    assert key is None, (
        f"the resolver answered {key!r} when it could not read the registry. "
        f"It must refuse instead: a fault is not a fact about this session."
    )


def test_hook_refuses_when_the_registry_cannot_be_read(hook):
    """Same property, the other channel.

    The receive hook was called "correct by construction" by every design
    reviewed for this fix. It is not. Its `_delivery_key` has an
    `except Exception: return session_id` arm that cannot tell "no name" from
    "registry unreadable" -- the orphan generator runs on BOTH sides (C-55).

    The hook keeps its `session_id` PARAMETER, and that asymmetry with the
    plugin is deliberate rather than an oversight: the hook is a fresh
    subprocess handed the true id on stdin every fire, so its id is current by
    construction and it is asking the right question. Only the plugin -- a
    process that outlives the value -- can be handed a stale one.
    """
    mod, home = hook
    (home / ".claude" / "relay" / "registry.json").write_text("{ not json", encoding="utf-8")

    key = mod._delivery_key(SID_A)

    assert key is None, (
        f"the hook answered {key!r} when it could not read the registry. "
        f'"I could not check" must not be reported as "no name".'
    )


def test_hook_keys_a_genuinely_nameless_session_by_its_id(hook):
    """The hook's blast-radius control, mirroring the plugin's."""
    mod, home = hook
    _write_registry(home, {})
    assert mod._delivery_key(SID_A) == SID_A


@pytest.mark.parametrize(
    "bad_row,label",
    [
        ("a string", "row holding a string"),
        ([1, 2], "row holding a list"),
        (7, "row holding a number"),
        (None, "row holding an explicit null"),
        (True, "row holding a bool"),
    ],
)
def test_plugin_refuses_a_present_but_corrupt_registry_row(plugin, bad_row, label):
    """CROSS-FAMILY REVIEW FINDING, reproduced before it was fixed.

    The first draft of the fix wrote `entry = reg.get(session_id) or {}`. A row
    present but holding a non-object fell through the `isinstance(dict)` test to
    the same `return None` as a genuinely absent row -- so the resolver reported
    ESTABLISHED NAMELESS about a session whose registry state it could not read,
    and the caller turned that into a session-id key.

    Corrupt-row cases must be tested separately from absent and valid rows;
    otherwise an unreadable identity can silently become an orphan SID key.

    `None` is in the list deliberately: `.get()` cannot distinguish an absent key
    from one whose value is null, and those two require opposite answers.
    """
    mod, home = plugin
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_A}), encoding="utf-8"
    )
    (home / ".claude" / "relay" / "registry.json").write_text(
        json.dumps({SID_A: bad_row}), encoding="utf-8"
    )

    key = _call_delivery_key(mod, SID_A)

    assert key is None, (
        f"a {label} is a registry this session cannot read, not proof that it "
        f"has no name; the resolver answered {key!r}."
    )


def test_plugin_absent_row_is_still_nameless_not_a_refusal(plugin):
    """The control that keeps the fix above from over-refusing.

    An ABSENT row is the ordinary state of an unregistered session and must
    still key by the session id. If the corrupt-row fix were written with
    `.get()` truthiness it would catch this case too and refuse every nameless
    session -- turning a fix into an outage.
    """
    mod, home = plugin
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_A}), encoding="utf-8"
    )
    _write_registry(home, {SID_B: _named("someone-else")})

    assert _call_delivery_key(mod, SID_A) == SID_A


# --------------------------------------------------------------------------
# P2 -- positive controls. These pass BEFORE and AFTER. Their job is to prove
# the fix did not make P1 green by breaking resolution.
# --------------------------------------------------------------------------

def test_plugin_still_resolves_a_real_name(plugin):
    mod, home = plugin
    (home / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_A}), encoding="utf-8"
    )
    _write_registry(home, {SID_A: _named("broker")})
    assert _call_delivery_key(mod, SID_A) == "broker"


def test_hook_still_resolves_a_real_name(hook):
    mod, home = hook
    _write_registry(home, {SID_A: _named("broker")})
    assert mod._delivery_key(SID_A) == "broker"


# --------------------------------------------------------------------------
# P3 -- cross-side agreement (D139). The split-deploy invariant.
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "rows,label",
    [
        ({}, "no registry row -- the rotated-away case"),
        ({SID_A: _named("broker")}, "a plain user-set name"),
        ({SID_A: {"name": "broker", "workspace": "/w"}}, "a name with no user_set status"),
        ({SID_A: _named("CON")}, "a reserved Windows basename"),
        ({SID_A: _named("a" * 65)}, "a name too long to be a filename"),
        ({SID_A: _named(".")}, "a name that is a path component"),
        ({SID_B: _named("broker")}, "the row belongs to a DIFFERENT session"),
    ],
)
def test_cross_side_agreement(plugin, hook, rows, label):
    """Both halves must compute the SAME key from the same registry state.

    This is the invariant a split deploy attacks: the two files ship on
    different channels in either order, and a window where they disagree is the
    original bug wearing a new coat. Note the last case in particular -- a row
    that names a DIFFERENT session must not leak that name to this one.
    """
    pmod, phome = plugin
    hmod, hhome = hook
    # Give the plugin a CURRENT session id, so this test measures the key
    # computation and not the staleness detection that P1 already covers.
    (phome / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_A}), encoding="utf-8"
    )
    _write_registry(phome, rows)
    _write_registry(hhome, rows)

    pkey = _call_delivery_key(pmod, SID_A)
    hkey = hmod._delivery_key(SID_A)

    assert pkey == hkey, (
        f"plugin computed {pkey!r} and the hook computed {hkey!r} for {label}. "
        f"The key must be identical on both sides or it is not a key (D139)."
    )


# --------------------------------------------------------------------------
# P3b -- cross-side agreement on a BROKEN registry. THE CASE P3 STRUCTURALLY
# CANNOT SEE, and the one where a split deploy actually diverges.
#
# A serialized-dict fixture can never produce a broken JSON registry. Exercise
# raw bytes separately so parse failures cannot collapse into namelessness and
# make the plugin and hook choose different keys.
# --------------------------------------------------------------------------

def _write_registry_raw(home: pathlib.Path, text: str) -> None:
    """Write registry.json VERBATIM, bypassing json.dumps."""
    d = home / ".claude" / "relay"
    d.mkdir(parents=True, exist_ok=True)
    (d / "registry.json").write_text(text, encoding="utf-8")


@pytest.mark.parametrize(
    "raw,label,must_refuse",
    [
        ("{ not json at all",
         "the file is not JSON -- a truncated or half-written write", True),
        ("null",
         "valid JSON, but null where an object belongs", True),
        ("[]",
         "valid JSON, but a list where an object belongs", True),
        ('{"%s": null}' % SID_A,
         "the row EXISTS and its value is null -- membership and truthiness disagree", True),
        ('{"%s": "broker"}' % SID_A,
         "the row is a bare string, not an object", True),
        ('{"%s": {"naming_status": "user_set", "name": null}}' % SID_A,
         "internally inconsistent: user_set carrying no name (KNOWN RESIDUE)", False),
    ],
)
def test_cross_side_agreement_on_a_broken_registry(plugin, hook, raw, label, must_refuse):
    """A fault is not a fact, and both halves must call it the same fault.

    Two assertions, and they are different properties:

      1. AGREEMENT (D139). Whatever the two sides decide, they must decide it
         identically, or the key is not a key. This is the invariant a split
         deploy attacks.
      2. REFUSAL (D182). For a registry state that could not be READ, the answer
         must not be session-id-shaped. "I could not check" and "there is no
         name" require opposite answers, and `.get()` cannot tell them apart --
         which is exactly the HIGH cross-family review found inside the first
         cut of this fix, where `reg.get(sid) or {}` collapsed a present-but-
         corrupt row back into "established nameless".

    THE LAST ROW IS DELIBERATELY `must_refuse=False` AND IS NOT AN OVERSIGHT. An
    internally inconsistent row (`naming_status: user_set` with `name: null`)
    still resolves to a session-id key on BOTH sides, so they agree and nothing
    is lost. It is recorded here as the KNOWN RESIDUE so that a future one-sided
    change to either half fails this test instead of shipping silently.
    """
    pmod, phome = plugin
    hmod, hhome = hook
    # A CURRENT session id, so this measures key computation rather than the
    # staleness detection P1 already covers.
    (phome / ".claude" / "sessions" / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID_A}), encoding="utf-8"
    )
    _write_registry_raw(phome, raw)
    _write_registry_raw(hhome, raw)

    pkey = _call_delivery_key(pmod, SID_A)
    hkey = hmod._delivery_key(SID_A)

    assert pkey == hkey, (
        f"plugin computed {pkey!r} and the hook computed {hkey!r} for {label}. "
        f"The key must be identical on both sides or it is not a key (D139)."
    )

    if must_refuse:
        assert not _looks_like_a_session_id(pkey), (
            f"the registry could not be read ({label}) and the resolver answered "
            f"{pkey!r} -- a session-id-shaped value. That is the root cause: a "
            f"fault reported as a fact, well-formed and indistinguishable from an "
            f"answer (D182)."
        )


# --------------------------------------------------------------------------
# P4 -- the mutation control. Proves the predicate can fail (D175).
# --------------------------------------------------------------------------

def test_predicate_rejects_the_original_defect():
    """Feed the pre-fix behaviour to the same predicate the tests above use.

    Without this, a predicate that had been weakened -- or that never
    discriminated in the first place -- would report green forever and read as
    coverage. That has already happened once in this repo: a guard test
    substring-matched its subject and accepted the exactly-inverted guard.
    """
    def defective_delivery_key(session_id, name=None):
        """Verbatim behaviour of the line this fix deletes."""
        return name if name else session_id

    assert _looks_like_a_session_id(defective_delivery_key(SID_A)), (
        "the predicate failed to flag the original defect, so a green run above "
        "would mean nothing"
    )
    # ...and it must NOT flag a legitimate name, or it would flag everything.
    assert not _looks_like_a_session_id(defective_delivery_key(SID_A, "broker"))
    # ...nor a genuine no-identity answer, which is what the fix returns.
    assert not _looks_like_a_session_id(None)


def test_the_two_sample_ids_are_distinguishable():
    """Guards the fixtures themselves: an aliasing bug is invisible if the two
    ids are equal, and a shape bug is invisible if either is malformed."""
    assert SID_A != SID_B
    assert _looks_like_a_session_id(SID_A)
    assert _looks_like_a_session_id(SID_B)
