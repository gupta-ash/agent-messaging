"""C-48 relay-name fold -- the MCP plugin half.

WHY THIS FILE EXISTS, AND WHAT ITS PREDECESSOR GOT WRONG.

Two earlier attempts at this fix failed. The second was killed in review because
the plugin has TWO INDEPENDENT IDENTITY-PRODUCING RESOLVERS that share only the
job-state leaf:

    _routing_name  -> _resolve_my_name    -> subscription id, SSE filter,
                                             from_agent, resume cursor
    _delivery_key  -> _delivery_key_name  -> confirmed/<X>.json, /api/agent/<X>/ack

Folding one and not the other makes this server subscribe as 'alpha' while
filing its cursor as 'Alpha'. **On NTFS those two filenames collide into one, so
that defect is INVISIBLE on every Windows box in this fleet and breaks only on
a case-sensitive filesystem.** No test that merely runs green here could have
caught it -- which is why the central test below asserts the two resolvers
AGREE, and then proves it can fail by breaking the leaf on purpose.

The predecessor's test was a FALSE GREEN for a related reason: its only
collision seed was a stored 'dc' claimed as 'DC', the single orientation in
which folding just the request happens to work. Where an orientation is
non-discriminating here, it is asserted to be non-discriminating rather than
counted as evidence.

SCOPE. This covers the Channel-A (broker Release) half only. The Channel-B half
-- shared.py, set_name.py, send.py -- ships through the config repo and is
verified by the cross-file agreement test at the end, which SKIPS when that repo
is not deployed rather than passing vacuously.
"""
import importlib.util
import json
import os
import pathlib

import pytest

# The REPO copy -- this is what ships in the Release. Deliberately not
# `Path.home()/.claude/plugins/...`, which is the DEPLOYED copy: a source test
# must fail when the source is wrong, even on a box whose deployed copy is fine.
PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
          / "harness" / "claude" / "local-relay-channel" / "server.py")

SID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _load():
    spec = importlib.util.spec_from_file_location("lrc_fold_under_test", str(PLUGIN))
    mod = importlib.util.module_from_spec(spec)
    # import-safe: the mcp imports and server construction are gated under __main__
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def mod(tmp_path, monkeypatch):
    """Plugin module with Path.home() redirected into tmp_path.

    CONTAINMENT: the plugin resolves Path.home() at CALL time, so replacing the
    module's `Path` is enough and no environment variable is involved. That
    matters on Windows, where setting HOME alone does NOT move Path.home()
    (expanduser reads USERPROFILE) -- a test written the POSIX-habitual way
    would read and write the LIVE relay registry and still pass green.
    """
    m = _load()

    class HomePath(type(pathlib.Path())):
        @classmethod
        def home(cls):
            return cls(str(tmp_path))

    monkeypatch.setattr(m, "Path", HomePath)
    # Assert the RESOLVED path, not the thing we set.
    assert str(m.Path.home()) == str(tmp_path)
    assert str(tmp_path) != str(pathlib.Path.home())
    (tmp_path / ".claude" / "relay").mkdir(parents=True)
    # `_SESSION_DIR` is the ONE path this plugin freezes at import, so the
    # `Path` redirect above does not move it. `_delivery_key()` now resolves the
    # CURRENT session id through it, so without this the test would read the
    # operator's REAL ~/.claude/sessions and adopt whatever live session owns
    # this pytest process -- green, and meaningless.
    sessions = tmp_path / ".claude" / "sessions"
    sessions.mkdir(parents=True)
    monkeypatch.setattr(m, "_SESSION_DIR", sessions)
    assert str(m._SESSION_DIR).startswith(str(tmp_path))
    # Plant the record the parameterless resolver will read. Keyed by the PARENT
    # pid, which is what `_resolve_session_file` looks up.
    (sessions / f"{os.getppid()}.json").write_text(
        json.dumps({"pid": os.getppid(), "sessionId": SID}), encoding="utf-8")
    return m


def _registry(tmp_path, **rows):
    (tmp_path / ".claude" / "relay" / "registry.json").write_text(
        json.dumps(rows), encoding="utf-8")


def _broker_probe(mod, monkeypatch, result):
    calls = []

    def get(endpoint):
        calls.append(endpoint)
        return result

    monkeypatch.setattr(
        mod,
        "broker_client",
        type("Broker", (), {"_get": staticmethod(get)}),
        raising=False,
    )
    return calls


def _jobstate(tmp_path, name, sid=SID):
    d = tmp_path / ".claude" / "jobs" / sid[:8]
    d.mkdir(parents=True, exist_ok=True)
    (d / "state.json").write_text(json.dumps({
        "name": name, "nameSource": "user", "template": "bg",
        "sessionId": sid, "state": "running",
    }), encoding="utf-8")


# --------------------------------------------------------------------------
# 1. The fold itself
# --------------------------------------------------------------------------

def test_fold_lowercases_ascii_and_is_identity_on_lowercase(mod):
    assert mod._normalize_name("Alpha") == "alpha"
    assert mod._normalize_name("ExampleClient") == "exampleclient"
    assert mod._normalize_name("alpha") == "alpha"
    assert mod._normalize_name("a.b_c-1") == "a.b_c-1"


def test_fold_never_raises_on_non_str(mod):
    """`_resolve_my_name`'s only call site sits in ZERO enclosing try blocks, so
    a raise here kills the MCP server at startup for every session on the box."""
    for bad in (None, 123, b"Alpha", [], {}):
        assert mod._normalize_name(bad) is bad


def test_fold_does_not_alias_and_lower_casefold_would(mod):
    """The fold is A-Z only, ON PURPOSE.

    This is the arm that justifies not using the obvious builtin. `.casefold()`
    maps the Kelvin sign into 'k' and `.lower()` maps the Turkish dotted capital
    into 'i'+U+0307 -- either would let two genuinely DISTINCT names land in one
    mailbox, inventing the collision this whole change removes. The second half
    of each assertion is the control: it shows the builtin really does alias, so
    the first half is a deliberate choice rather than an accident.
    """
    kelvin, turkish = "K", "İ"
    assert mod._normalize_name(kelvin) == kelvin
    assert kelvin.casefold() == "k"          # control: casefold WOULD alias
    assert mod._normalize_name(turkish) == turkish
    assert turkish.lower() != turkish        # control: lower WOULD alias
    # Distinct inputs stay distinct.
    assert mod._normalize_name(kelvin) != mod._normalize_name("K")


# --------------------------------------------------------------------------
# 2. THE CENTRAL ARM -- the two resolvers must agree
# --------------------------------------------------------------------------

def _identities(m, sid=SID):
    # `_delivery_key` takes NO session id any more -- it resolves the current one
    # itself, which is what stops a caller handing it a stale value (D181). The
    # `mod` fixture plants a session record carrying SID, so both resolvers are
    # answering about the same session and the agreement assertion still means
    # what it meant before.
    return m._routing_name(sid), m._delivery_key()


def test_both_resolvers_agree_on_a_capitalised_jobstate_name(mod, tmp_path):
    """The defect that got the previous design killed, asserted directly.

    Note what is NOT asserted: that two filenames differ. On NTFS
    confirmed/Alpha.json and confirmed/alpha.json are the SAME FILE, so a
    filesystem-level check passes on this box no matter what the code does. The
    strings are compared instead, which is filesystem-independent.
    """
    _registry(tmp_path)
    _jobstate(tmp_path, "Alpha")
    routing, dkey = _identities(mod)
    assert routing == "alpha"
    assert dkey == "alpha"
    assert routing == dkey


def test_the_agreement_arm_can_fail(mod, tmp_path, monkeypatch):
    """NEGATIVE CONTROL. Break the leaf the way the killed design did and prove
    this test reports DISAGREE. Without this the arm above could be green for
    the wrong reason and nobody would know.

    THE CONTROL READS THROUGH `_routing_name` AGAIN, AND THAT IS A CONSEQUENCE
    OF A DELETION. Between 2026-08-14 and 2026-08-15 this control had to reach
    one frame lower, because `_routing_name` carried an AGREEMENT GATE that
    compared the routing name against the delivery key and adopted the key on a
    mismatch -- so a reproduced split was swallowed before it could be observed
    here. That gate is gone (its cause, `_resolve_my_name`'s third name source,
    was removed instead), so `_routing_name` is once more `_resolve_my_name`
    plus the filename validator and the split is visible at this frame.

    WHAT THAT MEANS FOR THIS CONTROL: it is again a control on the FOLD alone.
    It says the two resolvers can still be made to disagree by breaking the leaf
    -- which is what makes the arm above evidence that they agree because the
    fold works, not because nothing could ever differ.
    """
    _registry(tmp_path)
    _jobstate(tmp_path, "Alpha")
    real = mod._read_daemon_job_name
    monkeypatch.setattr(mod, "_read_daemon_job_name",
                        lambda sid: (lambda r: r if r is None else "Alpha")(real(sid)))
    # v1's shape: resolution-level fold on ONE resolver only.
    monkeypatch.setattr(mod, "_resolve_my_name",
                        lambda sid: mod._normalize_name(real(sid)))
    routing, dkey = _identities(mod)
    assert routing == "alpha"
    assert dkey == "Alpha"
    assert routing != dkey, "the negative control did not reproduce the split"


# --------------------------------------------------------------------------
# 3. Every name SOURCE folds -- and there are exactly two of them
# --------------------------------------------------------------------------

def test_registry_source_folds(mod, tmp_path):
    _registry(tmp_path, **{SID: {"name": "Alpha", "naming_status": "user_set"}})
    assert mod._resolve_my_name(SID) == "alpha"
    assert mod._delivery_key_name(SID) == "alpha"


def test_broker_ingress_key_matches_plugin_sse_filter_key(mod, tmp_path):
    from broker import addressing

    _registry(tmp_path, **{SID: {"name": "Alpha", "naming_status": "user_set"}})

    route = addressing.parse_recipient("ALPHA")
    assert route.disposition == "local"
    assert route.key == mod._routing_name(SID) == "alpha"


def test_workspace_name_file_is_no_longer_a_name_source(mod, tmp_path, monkeypatch):
    """THIS TEST PASSES BECAUSE THE CODE PATH IT DESCRIBES WAS DELETED. It
    replaces `test_relay_name_file_source_folds`, which asserted that the same
    file folded on its way in.

    `_resolve_my_name` read `<workspace>/.relay-name.json` as a THIRD source
    until 2026-08-15. The receive hook cannot see that file, so whenever it was
    the only source that answered, this plugin resolved a routing name for which
    `_delivery_key_name` returned nothing -- the session then accepted mail it
    could never file a receipt for. The source was removed rather than mirrored
    onto the delivery side, because mirroring it would have SPLIT the key across
    the process boundary instead of closing the gap (D139).

    So the assertion is now the ABSENCE of the resolution, and the control
    matters more than the assertion: the same fixture is re-run with a registry
    row, and the name must resolve then. Without that, this test would be green
    against a resolver that had stopped working entirely.
    """
    _registry(tmp_path)
    ws = tmp_path / "proj"
    ws.mkdir()
    (ws / ".relay-name.json").write_text(
        json.dumps({"name": "Alpha", "set_by_sid": SID}), encoding="utf-8")
    monkeypatch.chdir(ws)
    # The file names THIS session, sits in cwd, and is exactly what source 3
    # accepted. Nothing reads it now.
    assert mod._resolve_my_name(SID) is None, (
        "the workspace name file resolved a routing name. The receive hook does "
        "not read that file, so the session would accept mail addressed to a "
        "name it cannot file a receipt under -- the split this deletion removed."
    )
    # And the identity the hook computes agrees about it: both sides see a
    # nameless session, which is the whole point of removing the source.
    assert mod._delivery_key_name(SID) is None

    # POSITIVE CONTROL: the resolver is not simply broken. Same fixture, plus the
    # registry row that IS a source -- the name resolves, and folded.
    _registry(tmp_path, **{SID: {"name": "Alpha", "naming_status": "user_set",
                                 "workspace": str(ws)}})
    assert mod._resolve_my_name(SID) == "alpha", (
        "CONTROL FAILED: no source resolves at all, so the absence assertion "
        "above says nothing about the workspace file."
    )


def test_jobstate_source_folds_at_the_leaf(mod, tmp_path):
    _registry(tmp_path)
    _jobstate(tmp_path, "Alpha")
    assert mod._read_daemon_job_name(SID) == "alpha"


# --------------------------------------------------------------------------
# 4. The reply path
# --------------------------------------------------------------------------

def test_recipient_resolves_a_legacy_capitalised_row(
        mod, tmp_path, monkeypatch):
    """A REGRESSION THIS CHANGE COULD HAVE INTRODUCED, and nearly did.

    peer_reply now folds `to` before the guard runs. If the guard compared that
    folded value against a RAW stored name, every registry row written before
    the fold shipped would stop resolving -- turning a working send into a hard
    refusal. Both operands are folded, so it still resolves.
    """
    other = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    _registry(tmp_path, **{other: {"name": "Alpha", "naming_status": "user_set"}})
    calls = _broker_probe(mod, monkeypatch, None)
    assert mod._resolve_relay_recipient("alpha") == ("alpha", True)
    assert calls == []


def test_broker_owned_codex_recipient_resolves_after_local_registry_miss(
        mod, tmp_path, monkeypatch):
    _registry(tmp_path, **{
        SID: {"name": "claude.sender", "naming_status": "user_set"}
    })
    calls = _broker_probe(mod, monkeypatch, {
        "relay_name": "r4b.cx.recv",
        "registered": True,
        "transport": "codex-app-server",
        "status": "idle",
    })
    assert mod._resolve_relay_recipient("r4b.cx.recv") == (
        "r4b.cx.recv", True
    )
    assert calls == ["/api/relay-name/r4b.cx.recv"]


def test_broker_confirmed_unknown_recipient_is_refused(
        mod, tmp_path, monkeypatch):
    """Control for the test above: the guard has not been made to accept
    everything. A broker-confirmed absent name returns a verified refusal.
    """
    other = "cccccccc-dddd-eeee-ffff-000000000000"
    _registry(tmp_path, **{other: {"name": "someoneelse",
                                   "naming_status": "user_set"}})
    calls = _broker_probe(mod, monkeypatch, {
        "relay_name": "nobody-here",
        "registered": False,
        "status": None,
        "transport": None,
    })
    assert mod._resolve_relay_recipient("nobody-here") == (None, True)
    assert calls == ["/api/relay-name/nobody-here"]


def test_peer_reply_posts_nothing_for_broker_confirmed_unknown(tmp_path):
    harness_path = pathlib.Path(__file__).with_name("test_boot_subscription.py")
    spec = importlib.util.spec_from_file_location("r4b_plugin_harness", harness_path)
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    harness.SCRATCH = tmp_path

    class AbsentRelayStub(harness.Stub):
        def _serve_plain_get(self, handler):
            path = handler.path.split("?")[0]
            if path == "/api/relay-name/r4b.unknown":
                with self.lock:
                    self.other_gets.append({"path": handler.path})
                self._json(handler, {
                    "relay_name": "r4b.unknown",
                    "registered": False,
                    "status": None,
                    "transport": None,
                })
                return
            super()._serve_plain_get(handler)

    tree = harness.Tree("broker-confirmed-unknown")
    tree.write_registry({
        harness.SID_TRUE: harness.reg_row(harness.NAME, str(tree.ws))
    })
    tree.write_session(harness.SID_TRUE, harness.NAME)
    stub = AbsentRelayStub()
    child = None
    try:
        child = harness.Child(tree, stub.start(), harness.SID_TRUE)
        child.start()
        child.handshake()
        reply = child.request("tools/call", {
            "name": "peer_reply",
            "arguments": {
                "to_session_id": "r4b.unknown",
                "content": "must not post",
            },
        }, timeout=20.0)

        assert reply is not None
        payload = json.loads(reply["result"]["content"][0]["text"])
        with stub.lock:
            observed_gets = list(stub.other_gets)
            observed_posts = list(stub.posts)
        assert payload["ok"] is False, (
            payload, observed_gets, observed_posts, child.stderr_text()
        )
        assert payload["known_names"] == [harness.NAME]
        assert observed_posts == []
        assert [row["path"] for row in observed_gets] == [
            "/api/relay-name/r4b.unknown"
        ]
    finally:
        if child is not None:
            child.stop()
        stub.shutdown()


def test_broker_unavailable_recipient_fails_open(
        mod, tmp_path, monkeypatch):
    """Authority unavailable remains a visible, unverified fail-open result."""
    _registry(tmp_path)
    calls = _broker_probe(mod, monkeypatch, None)
    assert mod._resolve_relay_recipient("nobody-here") == (
        "nobody-here", False
    )
    assert calls == ["/api/relay-name/nobody-here"]


def test_known_names_are_folded_and_deduplicated(mod, tmp_path):
    a, b = SID, "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    _registry(tmp_path, **{a: {"name": "Alpha", "naming_status": "user_set"},
                           b: {"name": "alpha", "naming_status": "user_set"}})
    # Two rows, one mailbox -- the honest count post-fold.
    assert mod._registry_names() == ["alpha"]


# --------------------------------------------------------------------------
# 5. Cross-file agreement with the Channel-B copy
# --------------------------------------------------------------------------

def test_plugin_and_hook_folds_are_behaviourally_identical(mod):
    """The two folds are deliberate COPIES in different repos on different ship
    channels, so nothing but a test can hold them together.

    SKIPS rather than passes when the config repo is not deployed -- a vacuous
    pass here would be worse than no test at all, because this is the only thing
    that would catch the two definitions drifting apart.
    """
    shared_py = pathlib.Path.home() / ".claude" / "hooks" / "relay" / "shared.py"
    if not shared_py.exists():
        pytest.skip("hooks/relay/shared.py not deployed on this box")
    import ast
    src = shared_py.read_text(encoding="utf-8")
    tree = ast.parse(src)
    lines = src.splitlines(keepends=True)
    wanted, chunks = {"_NAME_FOLD", "normalize_name"}, []
    for node in tree.body:
        nm = getattr(node, "name", None)
        if nm is None and isinstance(node, ast.Assign) and len(node.targets) == 1:
            nm = getattr(node.targets[0], "id", None)
        if nm in wanted:
            chunks.append("".join(lines[node.lineno - 1:node.end_lineno]))
    if len(chunks) != 2:
        pytest.skip("deployed shared.py predates the fold (Channel B not landed)")
    ns = {}
    exec(compile("\n".join(chunks), "<shared>", "exec"), ns)
    corpus = ["Alpha", "alpha", "ExampleClient", "DC", "a.b_c-1", "",
              "K", "İ", "MiXeD-99", "ALLCAPS", None, 42]
    for v in corpus:
        assert ns["normalize_name"](v) == mod._normalize_name(v), (
            "hook and plugin folds disagree on %r -- the two copies have "
            "drifted, which is exactly the split this change removes" % (v,))
