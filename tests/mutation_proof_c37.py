"""Mutation proof for tests/test_c37_name_queue_continuity.py.

NOT collected by pytest (the filename does not start with `test_`). Run it by
hand; it is the acceptance evidence that the C-37(c) guard can actually FAIL:

    python tests/mutation_proof_c37.py

Each arm materialises a throwaway git worktree, applies one queue regression,
and records which guard tests fail. The source checkout is never mutated.

Exit 0 only if BASE is fully green, every arm collects the full test count, and
every arm's OBSERVED red set equals its DECLARED red set. A declared set is the
point: "some test went red" accepted an arm that crashed for an unrelated
reason (rev 1 shipped exactly that), so drift in WHICH tests fire is an error
here, not a detail.

The harness retains a reproducible mutation matrix alongside the behavior tests.
Known limitations of this matrix (not claims of covered behavior):
  R2  peek = min(limit+1, 100): at limit=100 with >100 messages next_cursor
      goes null and history truncates at exactly 100. Needs a 101-message
      fixture. Trigger to build: any recipient legitimately paging at limit=100.
  R3  `len(rows) > limit` -> `>=`: an exactly-full final page advertises a
      cursor, causing one spurious empty hop. Protocol wart, not history loss.
  R5  scope by delivery status, e.g. `AND event_id IN (SELECT event_id FROM
      delivery WHERE status='outstanding')`. Every freshly posted directed row
      is outstanding and no test here acks one, so the fixture cannot see it --
      yet in production it would hide all ACKED history. Needs a fixture that
      acks part of the queue. Trigger: any change touching the delivery table
      from the read path.
  R6  a GRACE WINDOW on the era cutoff, e.g. `registered_at - 60000`. The whole
      fixture spans ~22 s, so any grace wider than that swallows it. Needs eras
      separated by more than the grace under test. Trigger: any lookback
      constant appearing in the inbox path.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TEST = "tests/test_c37_name_queue_continuity.py"

CONTINUITY = "test_queue_is_continuous_after_every_holder_is_deleted"
PAGINATION = "test_full_history_survives_pagination"
ORDERING = "test_inbox_orders_by_event_id_not_timestamp"
SCHEMA_G = "test_message_log_has_no_recipient_session_column"
QUERY_G = "test_inbox_query_keys_on_to_agent_alone"
NEGCTRL = "test_negative_controls_fire"
TESTS = [CONTINUITY, PAGINATION, ORDERING, SCHEMA_G, QUERY_G, NEGCTRL]
N_TESTS = len(TESTS)

# The endpoint under test. SQL anchors are replaced ONLY inside this function:
# `"ORDER BY event_id ASC LIMIT ?"` occurs TWICE in server.py -- here and in
# get_agent_messages_aggregate -- so a file-wide replace(...,1) hit the right
# one only by file order. `_fn_span` is a TEXTUAL span (decorator to the next
# top-level `@app.` decorator), not an AST end-of-function: it is exact for
# the current layout and fails loudly on rename/move, but an undecorated
# function following the target would be swallowed. Tightened claim, not a
# guarantee -- the exactly-once check below is what actually protects the arms.
TARGET_FN = "get_agent_messages"

ORIG_WHERE = '"WHERE to_agent = ? AND event_id > ? AND kind = ? "'
ORIG_ORDER = '"ORDER BY event_id ASC LIMIT ?"'
ORIG_SELECT = '"FROM message_log "'
ORIG_SCHEMA = "  ts INTEGER NOT NULL\n);"
M_SCHEMA = "  ts INTEGER NOT NULL,\n  to_session_id TEXT\n);"

M1_WHERE = ('"WHERE to_agent = (SELECT id FROM agent_status '
            'WHERE display_name = ? LIMIT 1) AND event_id > ? AND kind = ? "')
M3_WHERE = ('"WHERE to_agent = ? AND event_id > ? AND kind = ? '
            'AND to_session_id = (SELECT id FROM agent_status '
            'WHERE display_name = to_agent LIMIT 1) "')
M4_WHERE = ('"WHERE to_agent = ? AND event_id > ? AND kind = ? '
            'AND to_agent IN (SELECT display_name FROM agent_status) "')
M5_ORDER = '"ORDER BY ts ASC LIMIT ?"'
M6_ORDER = '"ORDER BY ts DESC LIMIT ?"'
# M8: a THIRD ordering degeneracy. `client_event_id` is nullable and arbitrary,
# so ordering by it is genuinely incompatible with an event_id cursor. It was
# invisible while the fixture minted cei in event_id order ("c37-0".."c37-5"),
# which sorts identically -- the same class of degenerate fixture as M5 and M6.
M8_ORDER = '"ORDER BY client_event_id ASC LIMIT ?"'
M7_WHERE = ('"WHERE to_agent = ? AND event_id > ? AND kind = ? '
            'AND ts >= COALESCE((SELECT registered_at FROM agent_status '
            'WHERE display_name = to_agent LIMIT 1), 0) "')
# R1: page GLOBALLY first, filter by recipient second. Same arity, same
# placeholder ORDER is NOT preserved -- so it also needs the bind tuple
# reordered, which is why it rewrites the whole statement region.
R1_SELECT = ('"FROM (SELECT event_id, client_event_id, from_agent, to_agent, '
             'channel, thread_id, kind, content, ts FROM message_log "')
R1_WHERE = '"WHERE event_id > ? AND kind = ? ORDER BY event_id ASC LIMIT ?) "'
R1_ORDER = '"WHERE to_agent = ? ORDER BY event_id ASC"'
R1_BIND = "(since, kind, peek, agent_id),"
ORIG_BIND = "(agent_id, since, kind, peek),"
# R4: a non-default kind becomes permanently uninspectable (DENYLIST form).
R4_ANCHOR = "        async with connect_db() as conn:"
R4_NEW = ('        if kind != "agent_message":\n'
          '            return {"messages": [], "next_cursor": None}\n'
          '        async with connect_db() as conn:')
# X1: the ALLOWLIST form, and the more realistic of the two -- server.py
# already contains `AND kind IN ('agent_message', 'agent_reply', 'handoff')`
# and `DELIVERY_KINDS = {...}`, so a maintainer hardening this endpoint copies
# that idiom in. X1 is the one-element omission. It hides `agent_reply`, which
# is the LARGEST directed kind in the live queue (1,652 rows, all addressed --
# 10x agent_message), and it was invisible to all six tests while the positive
# kind control covered only `handoff` (2 directed rows live).
X1_NEW = ('        if kind not in ("agent_message", "handoff"):\n'
          '            return {"messages": [], "next_cursor": None}\n'
          '        async with connect_db() as conn:')

ARMS = {
    "BASE": dict(edits=[], schema=False, expect=set()),
    "M1": dict(edits=[(ORIG_WHERE, M1_WHERE)], schema=False,
               expect={CONTINUITY, PAGINATION, ORDERING, QUERY_G, NEGCTRL}),
    "M2": dict(edits=[], schema=True, expect={SCHEMA_G}),
    "M3": dict(edits=[(ORIG_WHERE, M3_WHERE)], schema=True,
               expect={CONTINUITY, PAGINATION, ORDERING, SCHEMA_G, QUERY_G, NEGCTRL}),
    "M4": dict(edits=[(ORIG_WHERE, M4_WHERE)], schema=False,
               expect={CONTINUITY, NEGCTRL}),
    "M5": dict(edits=[(ORIG_ORDER, M5_ORDER)], schema=False,
               expect={CONTINUITY, PAGINATION, ORDERING}),
    "M6": dict(edits=[(ORIG_ORDER, M6_ORDER)], schema=False,
               expect={CONTINUITY, PAGINATION, ORDERING}),
    "M7": dict(edits=[(ORIG_WHERE, M7_WHERE)], schema=False,
               expect={PAGINATION, ORDERING}),
    "R1": dict(edits=[(ORIG_SELECT, R1_SELECT), (ORIG_WHERE, R1_WHERE),
                      (ORIG_ORDER, R1_ORDER), (ORIG_BIND, R1_BIND)],
               schema=False, expect={PAGINATION}),
    "R4": dict(edits=[(R4_ANCHOR, R4_NEW)], schema=False, expect={NEGCTRL}),
    "M8": dict(edits=[(ORIG_ORDER, M8_ORDER)], schema=False,
               expect={CONTINUITY, PAGINATION, ORDERING}),
    "X1": dict(edits=[(R4_ANCHOR, X1_NEW)], schema=False, expect={NEGCTRL}),
}


def run(cmd, cwd=None, check=True):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and p.returncode != 0:
        print(p.stdout[-1500:], p.stderr[-1500:])
        raise SystemExit(f"command failed: {cmd}")
    return p


def _fn_span(src: str, fn: str) -> tuple[int, int]:
    """Character span of `async def <fn>` up to the next top-level decorator."""
    m = re.search(rf"^@app\.[a-z]+\([^\n]*\)\nasync def {re.escape(fn)}\(", src, re.M)
    if m is None:
        m = re.search(rf"^async def {re.escape(fn)}\(", src, re.M)
    if m is None:
        raise SystemExit(f"function {fn} not found in server.py -- renamed?")
    nxt = re.search(r"^@app\.[a-z]+\(", src[m.end():], re.M)
    return m.start(), (m.end() + nxt.start() if nxt else len(src))


def apply_mutation(wt: Path, arm: str) -> None:
    """Apply every edit INSIDE the target function only, and require each
    anchor to occur EXACTLY ONCE there. An anchor that is missing, or that
    matches twice, makes the arm's verdict meaningless -- so refuse rather
    than report."""
    spec = ARMS[arm]
    if spec["edits"]:
        sp = wt / "src/broker/server.py"
        src = sp.read_text(encoding="utf-8")
        for anchor, new in spec["edits"]:
            lo, hi = _fn_span(src, TARGET_FN)
            body = src[lo:hi]
            n = body.count(anchor)
            if n != 1:
                raise SystemExit(
                    f"{arm}: anchor occurs {n} times in {TARGET_FN} (need exactly 1). "
                    f"Refusing to report a verdict.\n  anchor: {anchor}")
            src = src[:lo] + body.replace(anchor, new, 1) + src[hi:]
        sp.write_text(src, encoding="utf-8", newline="")
    if spec["schema"]:
        sc = wt / "src/broker/schema.sql"
        src = sc.read_text(encoding="utf-8")
        if src.count(ORIG_SCHEMA) != 1:
            raise SystemExit(f"{arm}: schema anchor not unique. Refusing to report.")
        sc.write_text(src.replace(ORIG_SCHEMA, M_SCHEMA, 1), encoding="utf-8", newline="")


def fresh_worktree(root: Path) -> Path:
    """Materialize committed source without changing any Git worktree registry."""
    import io
    import zipfile
    wt = Path(tempfile.mkdtemp(prefix="arm-", dir=root))
    archive = subprocess.run(["git", "archive", "--format=zip", "HEAD"],
                             cwd=REPO, check=True, capture_output=True).stdout
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        z.extractall(wt)
    return wt


def pytest_arm(wt: Path):
    p = run([sys.executable, "-m", "pytest", TEST, "-v", "--no-header",
             "-p", "no:cacheprovider"], cwd=wt, check=False)
    out = p.stdout + p.stderr
    return (set(re.findall(r"::(\w+) PASSED", out)),
            set(re.findall(r"::(\w+) (?:FAILED|ERROR)", out)), p.returncode, out)


def main() -> int:
    print("=" * 78)
    print("C-37(c) GUARD -- MUTATION PROOF")
    print("=" * 78)
    observed, problems, undeclared = {}, [], []
    root = Path(tempfile.mkdtemp(prefix="c37-mut-"))
    try:
        for arm, spec in ARMS.items():
            wt = fresh_worktree(root)
            try:
                apply_mutation(wt, arm)
                passed, failed, rc, out = pytest_arm(wt)
            finally:
                shutil.rmtree(wt)
            observed[arm] = failed
            total = len(passed) + len(failed)
            print(f"\n--- {arm}: {len(passed)} passed / {len(failed)} failed "
                  f"(collected {total}, rc={rc})")
            for n in sorted(failed):
                print(f"      RED: {n}")

            if passed | failed != set(TESTS):
                problems.append(f"{arm}: collected {sorted(passed | failed)}, "
                                f"expected exactly {sorted(TESTS)}")
                print(f"      *** COLLECTED SET != EXPECTED SET -- VOID ***")
                print("      " + "\n      ".join(out.strip().splitlines()[-8:]))
                continue
            # rc is a second, independent signal: pytest exits 0 iff the whole
            # session succeeded, so a teardown/plugin failure that leaves six
            # clean node outcomes is still caught here.
            want_rc = 0 if arm == "BASE" else 1
            if rc != want_rc:
                problems.append(f"{arm}: pytest rc={rc}, expected {want_rc}")
                print(f"      *** rc={rc}, EXPECTED {want_rc} ***")
            if spec["expect"] is None:
                undeclared.append((arm, failed))
                print("      (red set UNDECLARED -- declare it in ARMS, see footer)")
            elif failed != spec["expect"]:
                problems.append(f"{arm}: red set drifted")
                print(f"      *** RED SET DRIFT ***\n        declared: "
                      f"{sorted(spec['expect'])}\n        observed: {sorted(failed)}")
            if arm != "BASE" and not failed:
                problems.append(f"{arm}: HOLE -- regression invisible to the guard")
                print("      *** HOLE: this regression is INVISIBLE ***")
    finally:
        shutil.rmtree(root, ignore_errors=True)
        # No source worktree registration or pruning is needed.

    void = any("collected" in p for p in problems)
    if not void:
        print("\n" + "=" * 78)
        print("SOLE-DETECTOR ANALYSIS (delete-one: which clause is load-bearing?)")
        print("=" * 78)
        mutants = [a for a in ARMS if a != "BASE"]
        for t in TESTS:
            fires = [a for a in mutants if t in observed.get(a, set())]
            sole = [a for a in fires if observed[a] == {t}]
            tag = (f"SOLE DETECTOR for {sole}" if sole else
                   "never fires alone" if fires else "NEVER FIRES ON ANY ARM")
            print(f"  {t:56} fires:{str(fires):30} {tag}")
    else:
        print("\n(sole-detector analysis SUPPRESSED: a run was void, so the "
              "attribution below it would be misleading)")

    print("=" * 78)
    if undeclared:
        print("  DECLARE THESE RED SETS IN `ARMS` (measured this run):")
        for arm, failed in undeclared:
            print(f"    {arm}: expect={{{', '.join(sorted(failed)) or ''}}}")
    for p in problems:
        print("  PROBLEM:", p)
    if problems or undeclared:
        return 1
    print("RESULT: every arm behaves as declared; the attribution is MEASURED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
