# -*- coding: utf-8 -*-
"""Unit tests for registry_gc.gc_stale_duplicate_names + shared GC throttle.

Hermetic: every test injects registry / jobs_dir / sessions_dir / now_ms so no
test touches the live ~/.claude state. Mirrors the sys.path setup of the sibling
relay tests (the module under test lives in ~/.claude/hooks/relay/).

Cardinal invariant under test: GC NEVER demotes a live row (own_sid, a live
foreground process-file, or a non-terminal bg job-state). Bias: under-clean.
"""

import json
import sys
from pathlib import Path

import pytest

RELAY_HOOKS_DIR = Path.home() / ".claude" / "hooks" / "relay"
sys.path.insert(0, str(RELAY_HOOKS_DIR))

import registry_gc  # noqa: E402
import shared  # noqa: E402
from registry_gc import gc_stale_duplicate_names, gc_dead_rows  # noqa: E402

NOW = 1_781_000_000_000  # fixed epoch-ms; tests never call time.time()
DAY = 24 * 3600 * 1000
AGED = NOW - 10 * DAY   # comfortably older than the default/1-day window
AGE_MS = DAY            # explicit window for gc_dead_rows tests


# --------------------------------------------------------------------------- #
# fixtures / builders
# --------------------------------------------------------------------------- #

def _row(name, ws="E:\\ws\\a", *, user_set=True, last_seen=NOW):
    r = {
        "name": name,
        "workspace": ws,
        "kind": "unknown",
        "entrypoint": "unknown",
        "registered_at": last_seen,
        "last_seen_ts": last_seen,
    }
    if user_set:
        r["naming_status"] = "user_set"
    return r


def _sessions_dir(tmp_path, live_sids, key="sessionId"):
    """Create a sessions dir with one <pid>.json per live sid."""
    d = tmp_path / "sessions"
    d.mkdir(exist_ok=True)
    for i, sid in enumerate(live_sids):
        (d / f"{1000 + i}.json").write_text(json.dumps({key: sid}), encoding="utf-8")
    return d


def _jobs_dir(tmp_path, states):
    """states: {sid: state_str}. Create jobs/<sid>/state.json each."""
    d = tmp_path / "jobs"
    d.mkdir(exist_ok=True)
    for sid, st in states.items():
        jd = d / sid
        jd.mkdir(parents=True, exist_ok=True)
        (jd / "state.json").write_text(
            json.dumps({"sessionId": sid, "state": st}), encoding="utf-8"
        )
    return d


def _empty(tmp_path, name):
    d = tmp_path / name
    d.mkdir(exist_ok=True)
    return d


# --------------------------------------------------------------------------- #
# core scope / liveness
# --------------------------------------------------------------------------- #

def test_one_live_fg_two_dead(tmp_path):
    reg = {"live": _row("broker"), "deadA": _row("broker"), "deadB": _row("broker")}
    sess = _sessions_dir(tmp_path, ["live"])
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "other", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert set(demoted) == {"deadA", "deadB"}
    assert reg["live"]["name"] == "broker" and reg["live"]["naming_status"] == "user_set"
    assert reg["deadA"]["name"] is None and "naming_status" not in reg["deadA"]
    assert reg["deadB"]["name"] is None and "naming_status" not in reg["deadB"]


def test_live_bg_running_keeps(tmp_path):
    reg = {"bg": _row("trd"), "dead": _row("trd")}
    sess = _empty(tmp_path, "sessions")
    jobs = _jobs_dir(tmp_path, {"bg": "running"})
    demoted = gc_stale_duplicate_names(reg, "other", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["bg"]["name"] == "trd"
    assert reg["dead"]["name"] is None


def test_bg_terminal_states_are_dead(tmp_path):
    for term in ("stopped", "done", "failed", "cancelled", "canceled", "error", "killed", "STOPPED"):
        reg = {"live": _row("n"), "term": _row("n")}
        sess = _sessions_dir(tmp_path, ["live"])
        jobs = _jobs_dir(tmp_path, {"term": term})
        demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
        assert demoted == ["term"], f"state {term!r} should be dead"


def test_all_dead_single_workspace_keeps_most_recent_anchor(tmp_path):
    reg = {
        "old": _row("orphan", last_seen=NOW - 5000),
        "mid": _row("orphan", last_seen=NOW - 1000),
        "new": _row("orphan", last_seen=NOW - 10),
    }
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert set(demoted) == {"old", "mid"}            # "new" kept as anchor
    assert reg["new"]["name"] == "orphan" and reg["new"]["naming_status"] == "user_set"


def test_all_dead_two_workspaces_anchor_per_workspace(tmp_path):
    reg = {
        "a1": _row("orphan", ws="E:\\ws\\a", last_seen=NOW - 9000),
        "a2": _row("orphan", ws="E:\\ws\\a", last_seen=NOW - 10),
        "b1": _row("orphan", ws="E:\\ws\\b", last_seen=NOW - 9000),
        "b2": _row("orphan", ws="E:\\ws\\b", last_seen=NOW - 10),
    }
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert set(demoted) == {"a1", "b1"}              # newest per workspace kept
    assert reg["a2"]["name"] == "orphan" and reg["b2"]["name"] == "orphan"


def test_two_live_holders_demote_none(tmp_path):
    reg = {"liveA": _row("dup"), "liveB": _row("dup")}
    sess = _sessions_dir(tmp_path, ["liveA", "liveB"])
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []                              # genuine conflict left for the user (C-24)
    assert reg["liveA"]["name"] == "dup" and reg["liveB"]["name"] == "dup"


# --------------------------------------------------------------------------- #
# own_sid + protection
# --------------------------------------------------------------------------- #

def test_own_sid_never_demoted_even_when_no_live_signal(tmp_path):
    # own_sid has no sessions file and no job-state, but is the registering session.
    reg = {"me": _row("broker"), "dead": _row("broker")}
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "me", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["me"]["name"] == "broker" and reg["me"]["naming_status"] == "user_set"


def test_own_sid_absent_from_registry_no_crash(tmp_path):
    reg = {"deadA": _row("broker"), "deadB": _row("broker")}
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    # own_sid not present; both dead, single workspace -> keep one anchor.
    demoted = gc_stale_duplicate_names(reg, "not-in-reg", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert len(demoted) == 1 and demoted[0] in ("deadA", "deadB")


def test_single_holder_untouched(tmp_path):
    reg = {"solo": _row("broker", last_seen=NOW - 99_000_000)}  # very stale, dead, but single
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []
    assert reg["solo"]["name"] == "broker"


def test_non_user_set_row_not_a_holder(tmp_path):
    # name on 2 sids but only ONE is user_set -> not a duplicate; nothing demoted,
    # the non-user_set row is left entirely alone.
    reg = {"us": _row("broker"), "legacy": _row("broker", user_set=False)}
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []
    assert reg["us"]["name"] == "broker"
    assert reg["legacy"]["name"] == "broker" and "naming_status" not in reg["legacy"]


def test_case_sensitive_names_are_distinct(tmp_path):
    reg = {"a": _row("Broker"), "b": _row("broker")}  # different names -> no dup
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []


# --------------------------------------------------------------------------- #
# robustness / degrade
# --------------------------------------------------------------------------- #

def test_malformed_rows_no_crash(tmp_path):
    reg = {
        "nondict": "garbage",
        "noname": {"naming_status": "user_set"},
        "nullname": _row(None),
        "nostatus": {"name": "broker", "workspace": "E:\\ws\\a", "last_seen_ts": NOW},
        "us1": _row("broker"),
        "us2": _row("broker"),
    }
    sess = _sessions_dir(tmp_path, ["us1"])
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    # only the two user_set "broker" rows are holders; us1 live -> demote us2.
    assert demoted == ["us2"]
    assert reg["nondict"] == "garbage"
    assert reg["nostatus"]["name"] == "broker"  # not a user_set holder -> untouched


def test_absent_dirs_own_protected(tmp_path):
    reg = {"me": _row("broker"), "dead": _row("broker")}
    jobs = tmp_path / "no-jobs"      # does not exist
    sess = tmp_path / "no-sessions"  # does not exist
    demoted = gc_stale_duplicate_names(reg, "me", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["me"]["name"] == "broker"


def test_non_dict_registry_returns_empty():
    assert gc_stale_duplicate_names(None, "x", now_ms=NOW) == []
    assert gc_stale_duplicate_names([], "x", now_ms=NOW) == []


def test_legacy_session_id_key_counts_as_live(tmp_path):
    # A sessions file using the legacy snake_case key must still mark the sid live.
    reg = {"liveLegacy": _row("broker"), "dead": _row("broker")}
    sess = _sessions_dir(tmp_path, ["liveLegacy"], key="session_id")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["liveLegacy"]["name"] == "broker"


# --------------------------------------------------------------------------- #
# throttle (shared._registry_gc_due / _stamp_registry_gc)
# --------------------------------------------------------------------------- #

def test_throttle_due_when_no_marker(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "RELAY_DIR", tmp_path)
    assert shared._registry_gc_due(NOW) is True


def test_throttle_not_due_within_interval(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "RELAY_DIR", tmp_path)
    shared._stamp_registry_gc(NOW)
    # 1h later, default interval 6h -> not due
    assert shared._registry_gc_due(NOW + 3_600_000) is False


def test_throttle_due_after_interval(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "RELAY_DIR", tmp_path)
    shared._stamp_registry_gc(NOW)
    assert shared._registry_gc_due(NOW + 7 * 3_600_000) is True


def test_throttle_fail_open_on_future_ts(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "RELAY_DIR", tmp_path)
    (tmp_path / "registry-gc.json").write_text(json.dumps({"last_run_ts": NOW + 10_000_000}), encoding="utf-8")
    assert shared._registry_gc_due(NOW) is True


def test_throttle_fail_open_on_malformed_marker(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "RELAY_DIR", tmp_path)
    (tmp_path / "registry-gc.json").write_text("not json{", encoding="utf-8")
    assert shared._registry_gc_due(NOW) is True
    (tmp_path / "registry-gc.json").write_text(json.dumps({"last_run_ts": "nope"}), encoding="utf-8")
    assert shared._registry_gc_due(NOW) is True


def test_throttle_env_override(tmp_path, monkeypatch):
    monkeypatch.setattr(shared, "RELAY_DIR", tmp_path)
    monkeypatch.setenv("REGISTRY_GC_INTERVAL_S", "60")  # 60s interval
    shared._stamp_registry_gc(NOW)
    assert shared._registry_gc_due(NOW + 30_000) is False   # 30s later -> not due
    assert shared._registry_gc_due(NOW + 90_000) is True    # 90s later -> due


# --------------------------------------------------------------------------- #
# discriminator-found regressions (iter-1: live-row demotion on probe errors)
# --------------------------------------------------------------------------- #

def test_unreadable_foreground_file_skips_whole_gc(tmp_path):
    # A corrupt/unreadable session file may HIDE a live session's sid -> the
    # foreground scan is untrustworthy -> GC must fail closed (demote nothing).
    reg = {"me": _row("broker"), "dead": _row("broker")}
    sess = tmp_path / "sessions"
    sess.mkdir()
    (sess / "1000.json").write_text(json.dumps({"sessionId": "me"}), encoding="utf-8")
    (sess / "1001.json").write_text("{ broken json", encoding="utf-8")  # unreadable
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []                              # fail closed — no demotion
    assert reg["dead"]["name"] == "broker"            # live row NOT demoted


def test_non_dict_foreground_file_skips_whole_gc(tmp_path):
    reg = {"a": _row("broker"), "b": _row("broker")}
    sess = tmp_path / "sessions"
    sess.mkdir()
    (sess / "1000.json").write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []


def test_unreadable_bg_state_fails_toward_live(tmp_path):
    # A holder whose bg state.json is PRESENT but unreadable must be treated LIVE
    # (uncertain), never demoted.
    reg = {"uncertain": _row("trd"), "dead": _row("trd")}
    sess = _empty(tmp_path, "sessions")
    jobs = tmp_path / "jobs"
    (jobs / "uncertain").mkdir(parents=True)
    (jobs / "uncertain" / "state.json").write_text("{ broken", encoding="utf-8")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["uncertain"]["name"] == "trd"          # uncertain -> live -> kept


def test_malformed_bg_state_nondict_fails_toward_live(tmp_path):
    reg = {"uncertain": _row("trd"), "dead": _row("trd")}
    sess = _empty(tmp_path, "sessions")
    jobs = tmp_path / "jobs"
    (jobs / "uncertain").mkdir(parents=True)
    (jobs / "uncertain" / "state.json").write_text(json.dumps(["x"]), encoding="utf-8")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["uncertain"]["name"] == "trd"


def test_bg_state_no_usable_state_field_fails_toward_live(tmp_path):
    reg = {"uncertain": _row("trd"), "dead": _row("trd")}
    sess = _empty(tmp_path, "sessions")
    jobs = tmp_path / "jobs"
    (jobs / "uncertain").mkdir(parents=True)
    (jobs / "uncertain" / "state.json").write_text(json.dumps({"sessionId": "uncertain"}), encoding="utf-8")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dead"]
    assert reg["uncertain"]["name"] == "trd"


def test_mixed_type_last_seen_ts_no_crash(tmp_path):
    # Anchor selection must not crash when last_seen_ts is a mix of int and str
    # (malformed row). The string sorts as 0; the newest int wins the anchor.
    reg = {
        "strts": _row("orphan", last_seen=NOW - 10),
        "intnew": _row("orphan", last_seen=NOW - 5),
        "intold": _row("orphan", last_seen=NOW - 9000),
    }
    reg["strts"]["last_seen_ts"] = "not-a-number"     # malformed -> sorts as 0
    sess = _empty(tmp_path, "sessions")
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    # all dead, single workspace -> keep the most-recent numeric (intnew), demote rest.
    assert set(demoted) == {"strts", "intold"}
    assert reg["intnew"]["name"] == "orphan"


def test_foreground_listing_oserror_skips_gc(tmp_path, monkeypatch):
    # iter-2 CRITICAL: a directory-LISTING failure must fail closed. (Path.glob
    # swallows the scandir OSError on 3.12; os.listdir raises it.) Simulate an
    # unlistable sessions dir -> GC must demote nothing.
    reg = {"me": _row("broker"), "dead": _row("broker")}
    sess = tmp_path / "sessions"
    sess.mkdir()
    (sess / "1000.json").write_text(json.dumps({"sessionId": "me"}), encoding="utf-8")
    jobs = _empty(tmp_path, "jobs")

    real_listdir = registry_gc.os.listdir

    def boom(path, *a, **k):
        if str(path) == str(sess):
            raise PermissionError("listing denied")
        return real_listdir(path, *a, **k)

    monkeypatch.setattr(registry_gc.os, "listdir", boom)
    demoted = gc_stale_duplicate_names(reg, "x", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []                              # fail closed on listing error
    assert reg["dead"]["name"] == "broker"


def test_foreground_absent_dir_is_clean_gc_proceeds(tmp_path):
    # FileNotFoundError on listing -> legitimately absent -> CLEAN -> GC proceeds.
    reg = {"me": _row("broker"), "dead": _row("broker")}
    jobs = _empty(tmp_path, "jobs")
    demoted = gc_stale_duplicate_names(reg, "me", now_ms=NOW, jobs_dir=jobs,
                                       sessions_dir=tmp_path / "does-not-exist")
    assert demoted == ["dead"]
    assert reg["me"]["name"] == "broker"


def test_dup_live_bg_behind_terminal_fullid_not_demoted(tmp_path):
    """The shared _live_bg_jobstate aggregation also protects D54: a live bg
    duplicate holder whose jobs/<full-sid> is terminal but jobs/<sid[:8]> is
    running must NOT be demoted (cardinal rule: never demote a live row)."""
    sid = "aabbccdd-1111-2222-3333-444444444444"
    reg = {"me": _row("broker"), sid: _row("broker")}
    jobs = tmp_path / "jobs"
    (jobs / sid).mkdir(parents=True)
    (jobs / sid / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "stopped"}), encoding="utf-8")
    (jobs / sid[:8]).mkdir(parents=True)
    (jobs / sid[:8] / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "running"}), encoding="utf-8")
    sess = _sessions_dir(tmp_path, ["me"])
    demoted = gc_stale_duplicate_names(reg, "me", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == []
    assert reg[sid]["name"] == "broker"


# --------------------------------------------------------------------------- #
# gc_dead_rows (#3 — row-count/age GC)                                         #
# --------------------------------------------------------------------------- #

def _nameless(ws="E:\\ws\\a", *, last_seen=AGED):
    """A dead nameless row (no name, no naming_status) — the bulk of the file."""
    return {"name": None, "workspace": ws, "kind": "unknown",
            "entrypoint": "unknown", "registered_at": last_seen, "last_seen_ts": last_seen}


def _gc(reg, own, tmp_path, *, live_fg=(), jobs=None, sess=None, age=AGE_MS):
    jobs_dir = _jobs_dir(tmp_path, jobs or {})
    sess_dir = _sessions_dir(tmp_path, live_fg) if sess is None else sess
    return gc_dead_rows(reg, own, now_ms=NOW, age_ms=age, jobs_dir=jobs_dir, sessions_dir=sess_dir)


def test_dead_aged_nameless_row_deleted(tmp_path):
    reg = {"me": _row("broker"), "ghost": _nameless()}
    deleted = _gc(reg, "me", tmp_path, live_fg=["me"])
    assert deleted == ["ghost"]
    assert "ghost" not in reg
    assert "me" in reg  # own row kept


def test_user_set_row_never_deleted_even_if_dead_and_aged(tmp_path):
    # A dead+aged user_set anchor must survive (C-19 inheritance source).
    reg = {"me": _row("broker"), "anchor": _row("trading", last_seen=AGED)}
    deleted = _gc(reg, "me", tmp_path, live_fg=["me"])
    assert deleted == []
    assert reg["anchor"]["name"] == "trading"


def test_recent_nameless_row_kept(tmp_path):
    reg = {"me": _row("broker"), "fresh": _nameless(last_seen=NOW)}
    deleted = _gc(reg, "me", tmp_path, live_fg=["me"])
    assert deleted == []
    assert "fresh" in reg


def test_live_foreground_nameless_row_kept(tmp_path):
    reg = {"me": _row("broker"), "livefg": _nameless()}
    deleted = _gc(reg, "me", tmp_path, live_fg=["me", "livefg"])
    assert deleted == []
    assert "livefg" in reg


def test_live_bg_nameless_row_kept(tmp_path):
    reg = {"me": _row("broker"), "livebg": _nameless()}
    deleted = _gc(reg, "me", tmp_path, live_fg=["me"], jobs={"livebg": "running"})
    assert deleted == []
    assert "livebg" in reg


def test_dead_gc_terminal_fullid_masks_running_prefix_kept(tmp_path):
    """HIGH regression: a live bg row whose jobs/<full-sid> is a stale OWNING
    terminal state but jobs/<sid[:8]> is running must NOT be deleted. Exercises the
    aggregating _live_bg_jobstate on the DESTRUCTIVE path (the masking bug drove a
    live-row DELETE before the fix)."""
    sid = "aabbccdd-1111-2222-3333-444444444444"
    reg = {"me": _row("broker"), sid: _nameless()}
    jobs = tmp_path / "jobs"
    (jobs / sid).mkdir(parents=True)
    (jobs / sid / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "stopped"}), encoding="utf-8")   # stale terminal
    (jobs / sid[:8]).mkdir(parents=True)
    (jobs / sid[:8] / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "running"}), encoding="utf-8")    # actually live
    sess = _sessions_dir(tmp_path, ["me"])
    deleted = gc_dead_rows(reg, "me", now_ms=NOW, age_ms=AGE_MS, jobs_dir=jobs, sessions_dir=sess)
    assert deleted == []
    assert sid in reg


def test_no_timestamp_row_kept(tmp_path):
    reg = {"me": _row("broker"), "notime": {"name": None, "workspace": "E:\\ws\\a"}}
    deleted = _gc(reg, "me", tmp_path, live_fg=["me"])
    assert deleted == []
    assert "notime" in reg


def test_foreground_unclean_fails_closed_deletes_nothing(tmp_path):
    # An unreadable session file -> scan not clean -> skip the whole run.
    reg = {"me": _row("broker"), "ghost": _nameless()}
    sess = tmp_path / "sessions"
    sess.mkdir()
    (sess / "1.json").write_text("{ truncated", encoding="utf-8")  # unreadable
    deleted = _gc(reg, "me", tmp_path, sess=sess)
    assert deleted == []
    assert "ghost" in reg


def test_bulk_delete_many_dead_keeps_live_and_anchor(tmp_path):
    reg = {"me": _row("broker")}
    for i in range(50):
        reg[f"d{i}"] = _nameless()
    reg["anchor"] = _row("trading", last_seen=AGED)   # user_set -> kept
    reg["livebg"] = _nameless()                        # live bg -> kept
    deleted = _gc(reg, "me", tmp_path, live_fg=["me"], jobs={"livebg": "running"})
    assert len(deleted) == 50
    assert set(reg) == {"me", "anchor", "livebg"}


def test_composition_d54_demote_then_dead_gc_deletes(tmp_path):
    # A dead duplicate user_set row: D54 demotes it to nameless (same run), then
    # gc_dead_rows deletes it because it is now non-user_set + dead + aged.
    reg = {"me": _row("broker"), "dupdead": _row("broker", last_seen=AGED)}
    jobs = _jobs_dir(tmp_path, {})
    sess = _sessions_dir(tmp_path, ["me"])
    demoted = gc_stale_duplicate_names(reg, "me", now_ms=NOW, jobs_dir=jobs, sessions_dir=sess)
    assert demoted == ["dupdead"]           # D54 demoted (name -> None)
    deleted = gc_dead_rows(reg, "me", now_ms=NOW, age_ms=AGE_MS, jobs_dir=jobs, sessions_dir=sess)
    assert deleted == ["dupdead"]           # now nameless+dead+aged -> deleted
    assert set(reg) == {"me"}


def test_dead_gc_short_sid_live_bg_kept(tmp_path):
    """Short sid (len <= 8): only the full-sid candidate is probed; a running
    job-state keeps the row (no crash, no i==1 prefix branch)."""
    sid = "shortie"  # len 7
    reg = {"me": _row("broker"), sid: _nameless()}
    jobs = tmp_path / "jobs"
    (jobs / sid).mkdir(parents=True)
    (jobs / sid / "state.json").write_text(
        json.dumps({"sessionId": sid, "state": "running"}), encoding="utf-8")
    sess = _sessions_dir(tmp_path, ["me"])
    deleted = gc_dead_rows(reg, "me", now_ms=NOW, age_ms=AGE_MS, jobs_dir=jobs, sessions_dir=sess)
    assert deleted == []
    assert sid in reg


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
