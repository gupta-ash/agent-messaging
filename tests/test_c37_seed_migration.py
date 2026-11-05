"""T47 - guard over relay `shared._seed_name_key_from_sid`.

WHAT THIS GUARDS. The one-time, per-name migration carrying a delivery floor
from the old sid-keyed confirmed-set file to the new NAME-keyed one -- but ONLY
when the old file's `epoch_name` PROVES the floor accrued under that same name.
An unprovable floor must be DROPPED: a floor accrued under `alpha` applied to
`beta` permanently suppresses beta-addressed ids below it. Silent loss is worse
than re-delivery. Every DROP arm is a negative control
on that asymmetry.

The function under test is located by symbol, never by line number, in the
repository's shared relay module. Tests do not mutate that module.

`shared.py` freezes home-derived paths during import. Each arm uses a fresh
subprocess with both HOME and USERPROFILE set before loading it: on Windows,
Path.home() follows USERPROFILE rather than HOME. Redirecting only HOME could
otherwise write to the operator's real delivery cursors.

CONTAINMENT GATE -- the `arena` fixture, re-checked inside every child before
the call: HOME under the arm's scratch root, HOME != the real home
case-insensitively, `shared._confirmed_dir()` under the scratch root. Failure
aborts the run nonzero before a single call. `test_gate_positive_control` forces
it False over a fixture that provably lands, confirms nothing downstream fires,
then restores.

Fixtures assert non-degeneracy at seed time: destinations must not pre-exist,
missing epochs differ from malformed epochs, uppercase names exercise folding,
non-bool epoch_broken values exercise fail-closed parsing, and drop floors span
magnitudes. Source preservation and exact destination key sets are asserted.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REAL_HOME = str(Path.home().resolve())      # resolved BEFORE any env is doctored

# Every POSITIVE-INT source floor seeded by a DROP arm. Each drop arm asserts
# its own seeded value is a member, so the table cannot silently drift from the
# fixtures it describes. The module-level assert is the CROSS-ARM degeneracy the
# per-arm "floor > 0" checks cannot see: a drop-arm alphabet confined to one
# decade (this file's was [7505, 7509] plus B6's invalid values) makes ANY
# magnitude-gated laundering -- "carry it anyway when the floor is small, a
# short re-delivery is cheap" -- invisible to every drop arm at once. Include
# small as well as large floors so a magnitude-gated adoption is visible.
DROP_ARM_FLOORS = (3, 7505, 7506, 7507, 7508, 7509)
assert len({len(str(f)) for f in DROP_ARM_FLOORS}) > 1, (
    "drop-arm floors all share one order of magnitude: a magnitude-gated "
    "launder below the band would be invisible to every arm")
assert len(set(DROP_ARM_FLOORS)) == len(DROP_ARM_FLOORS), (
    "req 6: distinct floor per arm, so a cross-arm leak is visible")


def _find_relay_dir():
    candidate = Path(__file__).resolve().parents[1] / "harness/claude/hooks/relay"
    if not (candidate / "shared.py").is_file():
        raise RuntimeError("Required source relay library is absent")
    return candidate


RELAY_DIR = _find_relay_dir()

# Child driver: imports shared with the scratch HOME already in place, re-runs
# the containment gate, and only then calls. Reports raw file bytes so the
# parent asserts on content rather than on a re-derived summary.
_DRIVER = r'''
import io, json, os, sys
from pathlib import Path
job = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
sys.path.insert(0, job["relay_dir"]); import shared

def under(child, parent):
    c = os.path.normcase(os.path.abspath(str(child)))
    p = os.path.normcase(os.path.abspath(str(parent)))
    return c == p or c.startswith(p + os.sep)

home, cdir = Path(shared.HOME).resolve(), shared._confirmed_dir().resolve()
out = {"gate": "fail", "called": False, "home": str(home), "confirmed_dir": str(cdir)}
out["reasons"] = reasons = []
if not under(home, job["gate_root"]): reasons.append("home-outside-scratch")
if os.path.normcase(str(home)) == os.path.normcase(job["real_home"]):
    reasons.append("home-is-real-home")
if not under(cdir, job["gate_root"]): reasons.append("confirmed-dir-outside-scratch")
if reasons:
    print(json.dumps(out)); sys.exit(3)             # ABORT before any call
out["gate"] = "ok"
if job.get("probe_only"):
    print(json.dumps(out)); sys.exit(0)
buf, real = io.StringIO(), sys.stderr
sys.stderr = buf
try:
    shared._seed_name_key_from_sid(job["key"], job["sid"])
finally:
    sys.stderr = real
out["called"], out["stderr"] = True, buf.getvalue()

def rd(p):
    p = Path(p)
    return p.read_bytes().decode("utf-8", "replace") if p.exists() else None

out["dest"] = rd(shared._confirmed_path(job["key"]))
out["src"] = rd(shared._confirmed_path(job["sid"]))
print(json.dumps(out))
'''


class Arena:
    """One arm's scratch root. Fresh per test via pytest `tmp_path`."""

    def __init__(self, root):
        self.root = root
        self.home = root / "home"
        self.confirmed = self.home / ".claude" / "relay" / "confirmed"
        self.confirmed.mkdir(parents=True, exist_ok=True)
        (root / "driver.py").write_text(_DRIVER, encoding="utf-8")

    def seed(self, name, payload):
        """Write a confirmed-set file verbatim. Writes ONLY -- every
        anti-degeneracy assertion lives inline in the arm that seeded it."""
        p = self.confirmed / f"{name}.json"
        p.write_text(payload if isinstance(payload, str) else json.dumps(payload),
                     encoding="utf-8")
        return p

    def path(self, name):
        return self.confirmed / f"{name}.json"

    def run(self, key, sid, *, probe=False, gate_root=None):
        job = {"relay_dir": str(RELAY_DIR), "real_home": REAL_HOME, "key": key,
               "sid": sid, "probe_only": probe,
               "gate_root": str(gate_root if gate_root is not None else self.home)}
        jf = self.root / "job.json"
        jf.write_text(json.dumps(job), encoding="utf-8")
        env = dict(os.environ)
        env["HOME"] = str(self.home)          # POSIX habit: necessary, NOT sufficient
        env["USERPROFILE"] = str(self.home)   # the one ntpath.expanduser actually reads
        r = subprocess.run([sys.executable, str(self.root / "driver.py"), str(jf)],
                           env=env, capture_output=True, text=True)
        assert r.stdout.strip(), f"driver emitted no JSON; stderr={r.stderr[:800]}"
        return r.returncode, json.loads(r.stdout)


@pytest.fixture
def arena(tmp_path):
    """Fresh scratch root plus the containment gate, run in a child BEFORE any
    arm can call the function. Gate failure aborts the whole run nonzero."""
    a = Arena(tmp_path)
    rc, out = a.run("gate-probe", "gate-probe", probe=True)
    if rc != 0 or out["gate"] != "ok":
        pytest.exit(f"T47 containment gate FAILED: {out.get('reasons')}", returncode=2)
    assert Path(out["confirmed_dir"]) == a.confirmed.resolve(), (
        "parent path formula diverged from shared._confirmed_path")
    assert not list(a.confirmed.iterdir()), "scratch root is not fresh"   # req 7
    return a


def test_gate_positive_control(arena):
    """Force the gate False over a fixture that PROVABLY lands, confirm nothing
    downstream fires, then restore and confirm the same fixture DOES land."""
    src = arena.seed("sid-gate", {"floor": 7900, "epoch_name": "gatename"})
    assert json.loads(src.read_text())["floor"] > 0
    before = src.read_bytes()
    assert not arena.path("gatename").exists()

    rc, out = arena.run("gatename", "sid-gate",
                        gate_root=arena.root / "not-the-scratch-root")
    assert rc == 3, f"gate did not abort: rc={rc}"
    assert out["gate"] == "fail" and out["called"] is False
    # 2 of the 3 gate checks fire here. The third ("home-is-real-home") is NOT
    # positive-controlled: forcing it would mean pointing HOME at the real home,
    # which is precisely what this gate exists to prevent.
    assert set(out["reasons"]) == {"home-outside-scratch",
                                   "confirmed-dir-outside-scratch"}
    assert not arena.path("gatename").exists(), "seed ran despite a failed gate"

    rc, out = arena.run("gatename", "sid-gate")                    # restore
    assert rc == 0 and out["gate"] == "ok" and out["called"] is True
    assert json.loads(out["dest"])["floor"] == 7900, "control fixture could not land"
    assert src.exists() and src.read_bytes() == before, (
        "the SOURCE was consumed or rewritten; the migration reads it and must "
        "leave it byte-identical")


def test_b1_nameless_is_noop(arena):
    """B1: key == session_id -> no-op; the nameless file is never rewritten.

    B1 and B2 address the SAME path when key == sid, so a B1-ONLY removal is not
    observable here -- stated rather than overclaimed. Byte-equality is the
    strongest available observable and DOES catch B1+B2 both removed: the seed
    would rewrite the file, dropping epoch_name and stamping updated_at.
    """
    sid = "sid-b1"
    src = arena.seed(sid, {"floor": 7101, "acked": [], "epoch_name": sid})
    before = src.read_bytes()
    assert json.loads(before)["floor"] > 0

    rc, out = arena.run(sid, sid)
    assert rc == 0 and out["called"] is True
    assert out["stderr"] == ""
    assert src.read_bytes() == before, "nameless arm rewrote its own file"


def test_b2_already_migrated_is_byte_unchanged(arena):
    """B2: destination exists -> no-op, destination BYTE-unchanged."""
    dest = arena.seed("beta", {"floor": 7201, "acked": [], "updated_at": 1})
    src = arena.seed("sid-b2", {"floor": 7288, "epoch_name": "beta"})
    before = dest.read_bytes()
    src_before = src.read_bytes()
    assert "beta" != "sid-b2"                                          # req 2
    d_floor = json.loads(before)["floor"]
    s_floor = json.loads(src.read_text())["floor"]
    assert 0 < d_floor < s_floor, "monotonic: a re-run must be VISIBLE here"  # req 4
    # The source is fully provable (epoch_name == key, no broken flag, floor>0),
    # so deleting B2's early return WOULD land 7288 over 7201.
    rc, out = arena.run("beta", "sid-b2")
    assert rc == 0 and out["called"] is True
    assert dest.read_bytes() == before                        # req 5, by CONTENT
    assert json.loads(out["dest"])["floor"] == 7201
    # The early return must not "clean up" the legacy file either: `_delivery_key`
    # falls back to the session_id whenever name resolution fails, do_receive then
    # reads confirmed/<sid>.json, and a deleted one starts at floor 0 -- the
    # broker's only suppression is `event_id > since`, so it re-serves everything.
    assert src.exists() and src.read_bytes() == src_before, (
        "the already-migrated path CONSUMED or rewrote the source")


def test_b3_nothing_to_carry(arena):
    """B3: no sid-keyed file -> no-op. A decoy under a DIFFERENT sid proves the
    function reads the exact sid handed to it, not any provable file it finds."""
    decoy = arena.seed("sid-other", {"floor": 7303, "epoch_name": "gamma"})
    assert json.loads(decoy.read_text())["floor"] > 0
    decoy_before = decoy.read_bytes()
    assert "gamma" != "sid-b3"                                         # req 2
    assert not arena.path("gamma").exists()                            # req 3
    assert not arena.path("sid-b3").exists(), "B3 requires the source ABSENT"

    rc, out = arena.run("gamma", "sid-b3")
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "carried a floor with no source file"
    assert out["stderr"] == ""
    assert json.loads(decoy.read_text())["floor"] == 7303, "decoy was consumed"
    assert decoy.exists() and decoy.read_bytes() == decoy_before, (
        "decoy under another sid was consumed or rewritten")


def test_b4_not_a_dict(arena):
    """B4: valid JSON that is not an object -> no-op, and SILENT.

    Seeded as a one-element ARRAY wrapping an otherwise fully provable record,
    so the arm is not merely "unparseable". stderr must be EMPTY -- that is what
    separates B4 from B8: delete `isinstance(data, dict)` and `data.get` raises
    AttributeError, which the except arm CATCHES and reports.
    """
    src = arena.seed("sid-b4", [{"floor": 7404, "epoch_name": "delta"}])
    assert json.loads(src.read_text())[0]["floor"] > 0
    before = src.read_bytes()
    assert "delta" != "sid-b4"                                         # req 2
    assert not arena.path("delta").exists()                            # req 3

    rc, out = arena.run("delta", "sid-b4")
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "adopted a floor from a non-object payload"
    assert out["stderr"] == "", "B4 must return silently, not via the except arm"
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on the not-a-dict path")


def test_b5a_epoch_name_mismatch_drops(arena):
    """B5a: epoch_name != key -> DROP. THE CRITICAL-1 case -- a floor accrued
    under `epsilon-alpha` must never land on `epsilon-a`.

    The pair shares a NINE-character prefix on purpose. The previous pair
    (`epsilon-other` vs `epsilon`) shared exactly 7, so a mutant that compared
    only `[:8]` of each side was separated by ONE character of fixture luck and
    stayed green. The stamp is still an EXTENSION of the key, which is the
    direction the `startswith` calibration arm depends on.
    """
    key, stamp = "epsilon-a", "epsilon-alpha"
    src = arena.seed("sid-b5a", {"floor": 7505, "epoch_name": stamp})
    before = src.read_bytes()
    data = json.loads(before)
    assert data["floor"] > 0, "a zero source floor makes drop and adopt identical"
    assert data["floor"] in DROP_ARM_FLOORS
    assert data["epoch_name"] != key
    assert len(os.path.commonprefix([data["epoch_name"], key])) >= 8, (
        "the mismatch pair must share >= 8 leading chars, or a truncated "
        "prefix compare is separated by luck rather than by the assertion")
    assert data["epoch_name"].startswith(key), (
        "keep the stamp an EXTENSION of the key: the startswith calibration "
        "arm in the mutation harness depends on this direction")
    assert key != "sid-b5a"                                            # req 2
    assert not arena.path(key).exists()                                # req 3

    rc, out = arena.run(key, "sid-b5a")
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "LAUNDERED a floor across names (CRITICAL-1)"
    assert not arena.path(key).exists()
    assert json.loads(out["src"])["floor"] == 7505, "source was mutated"
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on a DROP path")


def test_b5b_sticky_broken_flag_drops(arena):
    """B5b: epoch_broken truthy -> DROP. Only the flag separates this fixture
    from B7's: the name matches and the floor is valid and non-zero."""
    src = arena.seed("sid-b5b",
                     {"floor": 7506, "epoch_name": "zeta", "epoch_broken": True})
    before = src.read_bytes()
    data = json.loads(before)
    assert data["floor"] > 0, "a zero source floor makes drop and adopt identical"
    assert data["floor"] in DROP_ARM_FLOORS
    assert data["epoch_name"] == "zeta", "fixture must be provable but for the flag"
    assert data["epoch_broken"] is True
    assert "zeta" != "sid-b5b"                                         # req 2
    assert not arena.path("zeta").exists()                             # req 3

    rc, out = arena.run("zeta", "sid-b5b")
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "adopted a floor conflating two name epochs"
    assert json.loads(out["src"])["floor"] == 7506, "source was mutated"
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on a DROP path")


def test_b5c_epochless_source_drops(arena):
    """B5c: a dict source with a valid positive floor and NO `epoch_name` AT
    ALL -> DROP. Absence of a stamp is absence of PROOF, never proof.

    No arm seeded this shape before, so `data.get("epoch_name")` and
    `data.get("epoch_name", key)` were indistinguishable -- the one-token slip
    that adopts an unstamped floor onto whatever name the session now holds.

    This arm also carries a small positive floor. Other drop cases use larger
    values, so this catches magnitude-dependent adoption of an unstamped floor.
    """
    key, sid = "kappa", "sid-b5c"
    src = arena.seed(sid, {"floor": 3, "acked": []})
    before = src.read_bytes()
    data = json.loads(before)
    assert "epoch_name" not in data, (
        "THE point of this arm: with a stamp present, no arm can distinguish "
        "'stamp missing' from 'stamp matches'")
    assert data["floor"] > 0, "a zero source floor makes drop and adopt identical"
    assert data["floor"] in DROP_ARM_FLOORS
    assert len(str(data["floor"])) < len(str(7505)), (
        "this arm exists to break the drop-arm floor alphabet's single decade")
    assert key != sid                                                  # req 2
    assert not arena.path(key).exists()                                # req 3

    rc, out = arena.run(key, sid)
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "ADOPTED an unstamped floor (CRITICAL-1)"
    assert not arena.path(key).exists()
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on a DROP path")


def test_b5d_case_only_epoch_mismatch_drops(arena):
    """B5d: epoch_name differs from the key ONLY by case -> DROP.

    shared.py's own comment above `_KEY_RE` records that NTFS collapses `Alpha`
    and `alpha` into ONE cursor file while the broker's SQL `to_agent = ?` is
    case-SENSITIVE, and that "case-folding the key here would MERGE two real
    mailboxes, which is the loss, not the fix". Every other fixture name in this
    file is lowercase, so a case-folded comparison was a literal no-op across
    the whole guard -- the defect the code explicitly warns about had no arm.
    """
    key, stamp, sid = "lambda", "Lambda", "sid-b5d"
    src = arena.seed(sid, {"floor": 7507, "epoch_name": stamp})
    before = src.read_bytes()
    data = json.loads(before)
    assert data["floor"] > 0, "a zero source floor makes drop and adopt identical"
    assert data["floor"] in DROP_ARM_FLOORS
    assert data["epoch_name"] != key, "this must still be a genuine mismatch"
    assert data["epoch_name"].lower() == key.lower(), (
        "THE point of this arm: the pair must differ ONLY by case, or it cannot "
        "separate an exact compare from a case-folded one")
    assert key != sid                                                  # req 2
    assert not arena.path(key).exists()                                # req 3

    rc, out = arena.run(key, sid)
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "MERGED two case-distinct mailboxes (CRITICAL-1)"
    assert not arena.path(key).exists()
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on a DROP path")


def test_b5e_truthy_non_bool_broken_flag_drops(arena):
    """B5e: `epoch_broken` TRUTHY but not the bool `True` -> DROP.

    B5b pins the one value (JSON `true`) for which truthiness and identity
    agree, so narrowing the check to `is True` was invisible. The sibling reader
    `_read_confirmed_set` in the same file normalises with
    `eb_raw if isinstance(eb_raw, bool) else True` -- it explicitly anticipates
    a present-but-non-bool flag on disk and treats it as BROKEN, so a narrowed
    check makes the two readers of the same bytes disagree.
    """
    key, sid, flag = "mu", "sid-b5e", "yes"
    src = arena.seed(sid, {"floor": 7508, "epoch_name": key, "epoch_broken": flag})
    before = src.read_bytes()
    data = json.loads(before)
    assert data["floor"] > 0, "a zero source floor makes drop and adopt identical"
    assert data["floor"] in DROP_ARM_FLOORS
    assert data["epoch_name"] == key, "fixture must be provable but for the flag"
    assert not isinstance(data["epoch_broken"], bool), (
        "THE point of this arm: a bool flag cannot separate truthiness from "
        "identity-with-True")
    assert bool(data["epoch_broken"]) is True, "the flag must still be TRUTHY"
    assert key != sid                                                  # req 2
    assert not arena.path(key).exists()                                # req 3

    rc, out = arena.run(key, sid)
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "adopted a floor conflating two name epochs"
    assert not arena.path(key).exists()
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on a DROP path")


def test_b5f_sid_stamped_epoch_drops(arena):
    """B5f: epoch_name == the SESSION ID while key != session_id -> DROP.

    A sid stamp means the floor accrued under the NAMELESS epoch: nothing was
    addressed to it by name. Adopting it onto a name suppresses name-addressed
    ids that were never delivered. B1 seeds this exact shape but passes
    key == sid, so the function returns on its first line and the epoch check is
    never evaluated -- the cell was structurally unreachable until this arm.
    """
    key, sid = "nu", "sid-b5f"
    src = arena.seed(sid, {"floor": 7509, "epoch_name": sid})
    before = src.read_bytes()
    data = json.loads(before)
    assert data["floor"] > 0, "a zero source floor makes drop and adopt identical"
    assert data["floor"] in DROP_ARM_FLOORS
    assert data["epoch_name"] == sid, "the stamp must BE the session id"
    assert key != sid, "key == sid short-circuits at B1 and makes this vacuous"
    assert not arena.path(key).exists()                                # req 3

    rc, out = arena.run(key, sid)
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, "laundered the nameless epoch onto a NAME"
    assert not arena.path(key).exists()
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on a DROP path")


@pytest.mark.parametrize(
    "case,floor,adopt_image",
    [("str", "7607", 7607), ("bool", True, 1), ("zero", 0, 0), ("neg", -7609, 0)],
)
def test_b6_invalid_floor_drops(arena, case, floor, adopt_image):
    """B6: floor not an int, or a bool, or <= 0 -> DROP.

    `True` gets its own case: `isinstance(True, int)` is True in Python, so the
    guard excludes bool explicitly. Requirement 1 wants a non-zero distinctive
    source floor so drop and adopt differ; for `zero`/`neg` that is impossible
    BY CONSTRUCTION -- the monotonic writer's adoption image is floor 0. The
    discriminator asserted there is FILE EXISTENCE: the destination is absent at
    seed, so adopting CREATES a file and dropping does not. `adopt_image` names,
    per case, what a removed guard would have written.
    """
    src = arena.seed("sid-b6", {"floor": floor, "epoch_name": "eta"})
    seeded = json.loads(src.read_text())
    before = src.read_bytes()
    assert seeded["floor"] == floor, "fixture did not round-trip the bad floor"
    assert seeded["epoch_name"] == "eta", "fixture must be provable but for the floor"
    assert "eta" != "sid-b6"                                           # req 2
    assert not arena.path("eta").exists(), (                           # req 3 + req 1
        "destination must be ABSENT so an adoption is visible as a file creation")
    if case in ("str", "bool"):
        assert adopt_image > 0, "adoption image must differ from the dropped state"

    rc, out = arena.run("eta", "sid-b6")
    assert rc == 0 and out["called"] is True
    assert out["dest"] is None, f"adopted an invalid floor ({case}) -> {adopt_image}"
    assert not arena.path("eta").exists()
    # A refusal must not also DELETE the legacy file. `_delivery_key` falls back
    # to the session_id when name resolution fails; do_receive then reads a file
    # that is gone, starts at floor 0, and the broker re-serves the whole history.
    assert src.exists() and src.read_bytes() == before, (
        "the invalid-floor DROP consumed or rewrote the source")


def test_b7_provable_floor_lands(arena):
    """B7: every guard passes -> the floor LANDS at the name key.

    Also proves `acked` is NOT carried: the migration carries the floor only, so
    a distinctive acked list is seeded and the destination must come back empty.
    """
    src = arena.seed("sid-b7",
                     {"floor": 7710, "acked": [7801, 7802], "epoch_name": "theta"})
    data = json.loads(src.read_text())
    before = src.read_bytes()
    assert data["floor"] > 0                                           # req 1
    assert data["epoch_name"] == "theta"
    assert "epoch_broken" not in data
    assert "theta" != "sid-b7"                                         # req 2
    assert not arena.path("theta").exists()                            # req 3
    # req 4: _advance_floor_locked is floor = max(prev, target). The destination
    # is absent, so prev is 0 and 0 < 7710 makes the write observable.
    assert 0 < data["floor"], "monotonic writer: target must exceed prev floor"

    rc, out = arena.run("theta", "sid-b7")
    assert rc == 0 and out["called"] is True
    assert out["dest"] is not None, "a provable floor was DROPPED"
    dest = json.loads(out["dest"])
    assert dest["floor"] == 7710, f"landed at the wrong floor: {dest['floor']}"
    assert dest["acked"] == [], "carried `acked`; the migration carries floor only"
    # The EXACT key set, not just the two named fields. `_advance_floor_locked`
    # emits `awaiting_ack` only when it is handed a list, and per
    # `_read_confirmed_set` an ABSENT awaiting_ack leaves broker_ack PERMISSIVE
    # while a list -- even an empty one -- is the AUTHORITATIVE allow-list that
    # makes _gate_broker_ack refuse every id for this session. A forged or empty
    # allow-list is an EXTRA KEY, so only a key-set assertion can observe it.
    assert set(dest) == {"floor", "acked", "updated_at"}, (
        f"destination key set drifted: {sorted(dest)}")
    assert "migrated delivery cursor" in out["stderr"]
    assert "epoch-proven" in out["stderr"]
    assert json.loads(out["src"])["floor"] == 7710, "source was mutated"
    # Byte equality, not the floor alone: a same-valued rewrite of the source is
    # absorbed by the monotonic max (max(7710,7710)) yet ERASES epoch_name, the
    # sole provenance evidence, which nothing writes any more.
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on the successful-seed path")


def test_b8_malformed_json_is_caught(arena):
    """B8: unparseable source -> no-op, no raise, and a stderr note.

    The note is the discriminator against B3/B4/B5, which are all silent, and
    against a bare `except: pass` that would swallow the diagnostic.
    """
    src = arena.seed("sid-b8", "{not json")
    assert src.read_text() == "{not json"
    before = src.read_bytes()
    with pytest.raises(json.JSONDecodeError):
        json.loads(src.read_text())
    assert "iota" != "sid-b8"                                          # req 2
    assert not arena.path("iota").exists()                             # req 3

    rc, out = arena.run("iota", "sid-b8")
    assert rc == 0, "the exception escaped the child"
    assert out["called"] is True
    assert out["dest"] is None, "created a destination from an unreadable source"
    assert "_seed_name_key_from_sid" in out["stderr"]
    assert "JSONDecodeError" in out["stderr"]
    assert "cursor starts fresh" in out["stderr"]
    assert src.exists() and src.read_bytes() == before, (
        "source consumed or rewritten on the exception path")
