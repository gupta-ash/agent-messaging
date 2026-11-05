# -*- coding: utf-8 -*-
"""T4 boot-subscription harness for the local-relay-channel MCP plugin.

WHAT THIS MEASURES
------------------
Whether the plugin's SSE push subscription (`GET {broker}/api/events`) ever
happens, under five boot conditions. It does NOT fix anything and it does not
touch any deployed file: every run builds a throwaway HOME tree, redirects
`USERPROFILE`/`HOME` into it, and points `BROKER_BASE_URL` at a stub broker
bound to an EPHEMERAL port (never 8787).

The plugin under test is launched as a plain subprocess and driven over MCP
stdio (newline-delimited JSON-RPC) far enough to complete the handshake.

INSTRUMENT
----------
`Stub` is a stdlib `http.server` that:
  * records every `GET /api/events` with a monotonic connection counter and a
    wall-clock timestamp, plus the `Last-Event-ID` header the plugin sent;
  * records every POST (path, headers, parsed body) -- this is how outbound
    `from_agent` is measured;
  * can emit a chosen SSE event to a chosen recipient at a chosen moment.

The stub DELIBERATELY IGNORES `Last-Event-ID`. A real broker filters
`event_id > cursor`, and the plugin primes its first-start cursor to
SQLITE_I64_MAX (see `get_initial_cursor`), so a faithful filter would deliver
nothing and every delivery assertion would be vacuous. Events are only queued
AFTER a connection exists, so ignoring the header cannot manufacture a
delivery that a live broker would not also make.

RUNS
----
  1            positive control -- session file present at launch. MUST PASS.
  2            the defect -- session file written 2 s AFTER launch.
  3            over-fix guard -- as run 2, no `user_set` row. MUST NOT subscribe.
  3b          [added] same guard from a run-1 boot -- the variant whose zero is
               produced by the `user_set` gate rather than by the boot defect.
  4            duplicate-subscription guard (60 s, registry churn) after run-2 boot.
  4b          [added] same guard after a run-1 boot -- the variant that can
               actually observe a duplicate on the current build.
  5            D143 trap -- as run 2, session-file `name` = a conversation title.
  5-control    D143 negative control -- as run 5, registry row for T removed.
  5b          [added] D143 trap after a run-1 boot -- testable on the current build.
  5b-control  [added] its negative control.

Runs 3b / 4b / 5b / 5b-control are additions. Runs 3, 4, 5 and 5-control are
specified against a run-2 boot, and a run-2 boot does not subscribe on the
current build, so their zeros are all produced by the boot defect and their
assertions are VACUOUS today. The `b` variants ask the same questions from a
boot that does subscribe today.

USAGE
-----
    python tests/test_boot_subscription.py all
    python tests/test_boot_subscription.py 1 2
    T4_SCRATCH=S:/agent-messaging-tests/t4-harness python tests/test_boot_subscription.py all

Scratch tree defaults to `$T4_SCRATCH`, else `<tempdir>/t4-harness`. It is left
in place for inspection; each run gets `<scratch>/run<N>/{home,ws,plugin.log}`.

Not a pytest module: it defines no `test_*` callables, so collection finds zero
tests and importing it has no side effects.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import shutil
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

# --- Locations ---------------------------------------------------------------
# Plugin under test: the COMMITTED copy, resolved relative to this file so the
# harness moves with the repo.
REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN = REPO_ROOT / "harness" / "claude" / "local-relay-channel" / "server.py"
# The plugin imports its shared HTTP and connection helpers from the installed
# relay library. Each fake home receives source-derived copies at that path.
RELAY_LIBRARY = REPO_ROOT / "harness/claude/hooks/relay"
SCRATCH = Path(os.environ.get("T4_SCRATCH") or (Path(tempfile.gettempdir()) / "t4-harness"))

# --- Fixtures ----------------------------------------------------------------
SID_TRUE = "11111111-2222-4333-8444-555555555555"   # T -- the sid CC actually wrote
SID_WRONG = "99999999-8888-4777-8666-555555555555"  # W -- the env-var sid (no registry row)
NAME = "t_probe"                                    # the routing name registered for T
PEER = "peer_probe"                                 # a second registry row, for peer_reply
TITLE = "Fix relay delivery key bug"                # D143: a CONVERSATION TITLE, not a name

MCP_PROTOCOL_VERSION = "2025-06-18"
HANDSHAKE_TIMEOUT_S = 30.0


# =============================================================================
# Stub broker
# =============================================================================
class Stub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.connects: list[dict] = []
        self.posts: list[dict] = []
        self.other_gets: list[dict] = []
        self.queue: list[dict] = []
        self.stop = threading.Event()
        self._next_id = 5000
        self.httpd: socketserver.BaseServer | None = None
        self.port = 0

    # -- lifecycle ------------------------------------------------------------
    def start(self) -> str:
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):  # silence stderr spam
                pass

            def do_GET(self):
                if self.path.split("?")[0] == "/api/events":
                    stub._serve_sse(self)
                else:
                    stub._serve_plain_get(self)

            def do_POST(self):
                stub._serve_post(self)

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self.httpd = Server(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.1},
                         daemon=True).start()
        return f"http://127.0.0.1:{self.port}"

    def shutdown(self) -> None:
        self.stop.set()
        if self.httpd is not None:
            try:
                self.httpd.shutdown()
                self.httpd.server_close()
            except Exception:
                pass

    # -- emit -----------------------------------------------------------------
    def emit(self, to_agent: str, content: str, *, kind: str = "agent_message",
             from_agent: str = PEER, channel: str = "#cc-session-relay") -> int:
        """Queue one SSE event. Every live connection picks it up."""
        with self.lock:
            self._next_id += 1
            eid = self._next_id
        payload = {
            "ts": int(time.time() * 1000),
            "kind": kind,
            "agent": from_agent,
            "data": {"to_agent": to_agent, "content": content,
                     "channel": channel, "thread_id": None},
        }
        with self.lock:
            self.queue.append({"id": eid, "event": "message", "data": payload})
        return eid

    # -- handlers -------------------------------------------------------------
    @staticmethod
    def _chunk(h, data: bytes) -> None:
        h.wfile.write(b"%x\r\n" % len(data) + data + b"\r\n")
        h.wfile.flush()

    def _serve_sse(self, h) -> None:
        with self.lock:
            n = len(self.connects) + 1
            self.connects.append({
                "n": n,
                "t": time.time(),
                "last_event_id": h.headers.get("Last-Event-ID"),
                "peer": h.client_address,
            })
        h.send_response(200)
        h.send_header("Content-Type", "text/event-stream")
        h.send_header("Cache-Control", "no-cache")
        h.send_header("Transfer-Encoding", "chunked")
        h.end_headers()
        sent = 0
        last_hb = 0.0
        try:
            while not self.stop.is_set():
                with self.lock:
                    pending = self.queue[sent:]
                    sent = len(self.queue)
                for ev in pending:
                    frame = (f"id: {ev['id']}\n"
                             f"event: {ev['event']}\n"
                             f"data: {json.dumps(ev['data'])}\n\n")
                    self._chunk(h, frame.encode("utf-8"))
                now = time.time()
                if now - last_hb > 5.0:
                    self._chunk(h, b"event: heartbeat\ndata: {}\n\n")
                    last_hb = now
                time.sleep(0.05)
        except (OSError, ValueError):
            pass  # client went away

    def _serve_post(self, h) -> None:
        length = int(h.headers.get("Content-Length") or 0)
        raw = h.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            body = {"_raw": raw.decode("utf-8", "replace")}
        with self.lock:
            self._next_id += 1
            eid = self._next_id
            self.posts.append({"t": time.time(), "path": h.path, "body": body,
                               "headers": dict(h.headers.items())})
        self._json(h, {"ok": True, "event_id": eid})

    def _serve_plain_get(self, h) -> None:
        with self.lock:
            self.other_gets.append({"t": time.time(), "path": h.path})
        self._json(h, {"ok": True})

    @staticmethod
    def _json(h, obj: dict) -> None:
        blob = json.dumps(obj).encode("utf-8")
        h.send_response(200)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(blob)))
        h.end_headers()
        h.wfile.write(blob)

    # -- queries --------------------------------------------------------------
    def connect_count(self) -> int:
        with self.lock:
            return len(self.connects)

    def first_connect_t(self) -> float | None:
        with self.lock:
            return self.connects[0]["t"] if self.connects else None

    def post_bodies(self) -> list[dict]:
        with self.lock:
            return [p["body"] for p in self.posts]


# =============================================================================
# Fake HOME tree
# =============================================================================
def reg_row(name: str, workspace: str, *, naming_status: str = "user_set") -> dict:
    now = int(time.time() * 1000)
    return {"name": name, "naming_status": naming_status, "workspace": workspace,
            "registered_at": now, "last_seen_ts": now,
            "kind": "interactive", "entrypoint": "cli"}


def sess_payload(sid: str, name: str, cwd: str) -> dict:
    """Same key set as a live ~/.claude/sessions/<pid>.json (read off this box)."""
    now = int(time.time() * 1000)
    return {"pid": os.getpid(), "sessionId": sid, "cwd": cwd, "startedAt": now,
            "procStart": "0", "version": "2.1.220", "peerProtocol": 1,
            "kind": "interactive", "entrypoint": "cli", "name": name,
            "updatedAt": now, "status": "idle", "statusUpdatedAt": now}


def atomic_write_json(path: Path, obj: dict) -> None:
    """tempfile + os.replace -- the write shape CC itself uses. A plain
    open/write would let the plugin read a truncated file and would not be a
    faithful reproduction of the race."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class Tree:
    """A throwaway HOME + workspace for one run."""

    def __init__(self, tag: str) -> None:
        self.root = SCRATCH / f"run{tag}"
        self.home = self.root / "home"
        self.ws = self.root / "ws"
        self.log = self.root / "plugin.log"
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)
        for d in (self.home / ".claude" / "relay",
                  self.home / ".claude" / "sessions",
                  self.home / ".claude" / "hooks" / "relay",
                  self.ws):
            d.mkdir(parents=True, exist_ok=True)
        for filename in ("broker_client.py", "broker_connection.py"):
            shutil.copy2(RELAY_LIBRARY / filename,
                         self.home / ".claude/hooks/relay" / filename)

    @property
    def session_file(self) -> Path:
        # The plugin's hop 0 is `sessions/<os.getppid()>.json`; the plugin's
        # parent is THIS process.
        return self.home / ".claude" / "sessions" / f"{os.getpid()}.json"

    def write_registry(self, rows: dict) -> None:
        atomic_write_json(self.home / ".claude" / "relay" / "registry.json", rows)

    def write_session(self, sid: str, name: str) -> float:
        atomic_write_json(self.session_file, sess_payload(sid, name, str(self.ws)))
        return time.time()

    def sessions_empty(self) -> bool:
        return not list((self.home / ".claude" / "sessions").glob("*.json"))


# =============================================================================
# MCP stdio child
# =============================================================================
class Child:
    def __init__(self, tree: Tree, broker_url: str, env_session_id: str) -> None:
        self.tree = tree
        self.broker_url = broker_url
        self.env_session_id = env_session_id
        self.proc: subprocess.Popen | None = None
        self.msgs: list[dict] = []
        self.lock = threading.Lock()
        self._logf = None
        self.t_spawn = 0.0
        self.t_handshake = 0.0
        self._next_id = 1

    def start(self) -> None:
        env = {k: v for k, v in os.environ.items()
               if not (k.startswith("CLAUDE_") or k.startswith("BROKER_")
                       or k.startswith("RELAY_"))}
        env["USERPROFILE"] = str(self.tree.home)
        env["HOME"] = str(self.tree.home)
        env["HOMEDRIVE"] = os.path.splitdrive(str(self.tree.home))[0]
        env["HOMEPATH"] = os.path.splitdrive(str(self.tree.home))[1]
        env["BROKER_BASE_URL"] = self.broker_url
        env["CLAUDE_CODE_SESSION_ID"] = self.env_session_id
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        self._logf = open(self.tree.log, "wb")
        self.t_spawn = time.time()
        self.proc = subprocess.Popen(
            [sys.executable, str(PLUGIN)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self._logf,
            cwd=str(self.tree.ws), env=env,
        )
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line.decode("utf-8"))
            except Exception:
                continue
            with self.lock:
                self.msgs.append(obj)

    def _send(self, obj: dict) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        self.proc.stdin.write((json.dumps(obj) + "\n").encode("utf-8"))
        self.proc.stdin.flush()

    def request(self, method: str, params: dict, timeout: float = 15.0) -> dict | None:
        with self.lock:
            self._next_id += 1
            rid = self._next_id
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        return wait_for(lambda: self.find_response(rid), timeout)

    def handshake(self) -> dict:
        resp = self.request("initialize", {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "t4-boot-subscription-harness", "version": "1.0"},
        }, timeout=HANDSHAKE_TIMEOUT_S)
        if resp is None:
            raise RuntimeError("MCP initialize did not respond; see " + str(self.tree.log))
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
        self.t_handshake = time.time()
        return resp

    # -- inspection -----------------------------------------------------------
    def find_response(self, rid: int) -> dict | None:
        with self.lock:
            for m in self.msgs:
                if m.get("id") == rid and ("result" in m or "error" in m):
                    return m
        return None

    def channel_notifications(self) -> list[dict]:
        with self.lock:
            return [m for m in self.msgs
                    if m.get("method") == "notifications/claude/channel"]

    def stderr_text(self) -> str:
        try:
            return self.tree.log.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def stop(self) -> None:
        if self.proc is None:
            return
        try:
            if self.proc.stdin:
                self.proc.stdin.close()   # EOF -> the plugin's clean shutdown path
        except OSError:
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self._logf is not None:
            try:
                self._logf.close()
            except OSError:
                pass


def wait_for(pred, timeout: float, interval: float = 0.05):
    """Poll `pred` until truthy or `timeout`. Returns the value or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(interval)
    return pred() or None


def delivered_to(child: Child, name: str) -> list[dict]:
    out = []
    for n in child.channel_notifications():
        meta = (n.get("params") or {}).get("meta") or {}
        if meta.get("to") == name:
            out.append(n)
    return out


# =============================================================================
# Result plumbing
# =============================================================================
class Result:
    def __init__(self, run: str, expected: str) -> None:
        self.run = run
        self.expected = expected
        self.actual = ""
        self.passed = False
        self.notes: list[str] = []

    def note(self, s: str) -> None:
        self.notes.append(s)
        print(f"    . {s}", flush=True)

    def finish(self, passed: bool, actual: str) -> "Result":
        self.passed = passed
        self.actual = actual
        print(f"    => {'PASS' if passed else 'FAIL'}: {actual}", flush=True)
        return self


# =============================================================================
# Runs
# =============================================================================
def _boot(tag: str, *, registry: dict, session_at_launch: tuple[str, str] | None,
          env_sid: str) -> tuple[Tree, Stub, Child]:
    """Common setup. `session_at_launch` = (sid, name) or None for an empty
    sessions dir."""
    tree = Tree(tag)
    tree.write_registry(registry)
    if session_at_launch is not None:
        tree.write_session(*session_at_launch)
    stub = Stub()
    url = stub.start()
    child = Child(tree, url, env_sid)
    child.start()
    return tree, stub, child


def run1() -> Result:
    """POSITIVE CONTROL. Session file present at launch. Must PASS today."""
    r = Result("1", "1 subscription + delivery")
    tree, stub, child = _boot(
        "1",
        registry={SID_TRUE: reg_row(NAME, str(SCRATCH / "run1" / "ws"))},
        session_at_launch=(SID_TRUE, NAME),
        # env holds the WRONG sid in runs 1 AND 2, so the ONLY variable between
        # them is whether the session file exists at launch.
        env_sid=SID_WRONG,
    )
    try:
        child.handshake()
        r.note(f"handshake at +{child.t_handshake - child.t_spawn:.2f}s from spawn")
        wait_for(lambda: stub.connect_count() >= 1, 5.0)
        n = stub.connect_count()
        t0 = stub.first_connect_t()
        if n != 1 or t0 is None:
            return r.finish(False, f"{n} GET /api/events (expected 1)")
        dt = t0 - child.t_handshake
        r.note(f"GET /api/events #1 at handshake{dt:+.2f}s, "
               f"Last-Event-ID={stub.connects[0]['last_event_id']}")
        if dt > 1.0:
            return r.finish(False, f"1 GET but {dt:.2f}s after handshake (>1s)")
        eid = stub.emit(NAME, "hello-run1")
        got = wait_for(lambda: delivered_to(child, NAME), 10.0)
        if not got:
            return r.finish(False, f"1 GET, but event {eid} never delivered")
        meta = (got[0].get("params") or {}).get("meta") or {}
        r.note(f"delivered event_id={meta.get('event_id')} to={meta.get('to')} "
               f"from={meta.get('from')}")
        return r.finish(stub.connect_count() == 1,
                        f"1 GET within {dt:+.2f}s of handshake; event delivered")
    finally:
        child.stop()
        stub.shutdown()


def _deferred_boot_run(tag: str, registry: dict, deferred_name: str,
                       result: Result, *, settle_s: float = 2.0,
                       window_s: float = 5.0):
    """Runs 2/3/5/5-control share this: empty sessions dir at launch, wrong sid
    in env, then the session file appears. Returns (tree, stub, child, t_write,
    subscribed_within_window)."""
    tree, stub, child = _boot(tag, registry=registry, session_at_launch=None,
                              env_sid=SID_WRONG)
    child.handshake()
    result.note(f"handshake at +{child.t_handshake - child.t_spawn:.2f}s; "
                f"sessions dir empty at launch = {tree.sessions_empty()}")
    time.sleep(settle_s)
    pre = stub.connect_count()
    result.note(f"after {settle_s:.0f}s settle: GET /api/events count = {pre}")
    t_write = tree.write_session(SID_TRUE, deferred_name)
    result.note(f"session file written (tempfile+os.replace) -> {tree.session_file.name} "
                f"sessionId={SID_TRUE} name={deferred_name!r}")
    wait_for(lambda: stub.connect_count() > pre, window_s)
    post = stub.connect_count()
    within = post > pre and (stub.connects[pre]["t"] - t_write) <= window_s
    result.note(f"{window_s:.0f}s after the write: GET count = {post} "
                f"(subscribed_in_window={within})")
    return tree, stub, child, t_write, within, pre


def run2() -> Result:
    """THE DEFECT. Pre-T4-fix this could not subscribe at all (the sid was bound
    once at boot to the wrong env value and every name door is keyed by it).
    Post-fix the supervisor re-resolves the sid, so this must subscribe."""
    r = Result("2", "subscribe <=5s of the write, deliver, exactly 1 GET")
    tree = stub = child = None
    try:
        tree, stub, child, t_write, within, pre = _deferred_boot_run(
            "2", {SID_TRUE: reg_row(NAME, str(SCRATCH / "run2" / "ws"))}, NAME, r)
        if pre != 0:
            return r.finish(False, f"harness defect: {pre} GET before the write")
        if not within:
            return r.finish(False, "no GET /api/events within 5s of the session-file "
                                   "write; no subscription, ever")
        stub.emit(NAME, "hello-run2")
        got = wait_for(lambda: delivered_to(child, NAME), 10.0)
        time.sleep(1.0)          # let a would-be second connect show up
        total = stub.connect_count()
        dt = stub.connects[pre]["t"] - t_write
        r.note(f"subscribed at write{dt:+.2f}s; total GET /api/events = {total}")
        return r.finish(bool(got) and total == 1,
                        f"subscribed {dt:+.2f}s after the write, {total} GET total, "
                        f"{'event delivered' if got else 'event NOT delivered'}")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run2_ack() -> Result:
    """[added] Post-swap ACK PATH. `my_sid` is now mutable and `call_tool` reads
    it via `_delivery_key(my_sid)`, so an ack issued after an id swap must file
    under the TRUE id's delivery key.

    Blind by construction: the confirmed-set is pre-seeded with an `awaiting_ack`
    allow-list under BOTH candidate keys -- the registry name `t_probe` (what a
    correct swap computes) and the raw wrong sid W (what a stale `my_sid`
    computes) -- so the fold succeeds either way and the harness does not
    presuppose the answer. The observable is the ack URL, `/api/agent/{key}/ack`,
    which carries the delivery key in the path."""
    r = Result("2-ack", "ack files under 't_probe', not the stale sid")
    stub = child = None
    try:
        ws = str(SCRATCH / "run2-ack" / "ws")
        tree, stub, child, t_write, within, pre = _deferred_boot_run(
            "2-ack", {SID_TRUE: reg_row(NAME, ws), PEER: reg_row(PEER, ws)}, NAME, r)
        if not within:
            return r.finish(False, "no subscription after the write -- ack check is "
                                   "VACUOUS (the boot defect is back)")
        eid = stub.emit(NAME, "ack-probe")
        got = wait_for(lambda: delivered_to(child, NAME), 10.0)
        if not got:
            return r.finish(False, "subscribed but event never delivered; no id to ack")
        meta = (got[0].get("params") or {}).get("meta") or {}
        ack_id = int(meta.get("event_id") or eid)

        conf = tree.home / ".claude" / "relay" / "confirmed"
        for key in (NAME, SID_WRONG, SID_TRUE):
            atomic_write_json(conf / f"{key}.json",
                              {"floor": 0, "acked": [], "awaiting_ack": [ack_id]})
        r.note(f"seeded awaiting_ack=[{ack_id}] under ALL of "
               f"{NAME}.json / {SID_WRONG[:8]}.json / {SID_TRUE[:8]}.json (blind test)")

        resp = child.request("tools/call", {
            "name": "broker_ack", "arguments": {"event_ids": [ack_id]}}, timeout=20.0)
        r.note(f"broker_ack result: {json.dumps(resp)[:180] if resp else '<none>'}")
        wait_for(lambda: [p for p in stub.posts if "/ack" in p["path"]], 10.0)

        ack_paths = [p["path"] for p in stub.posts if "/ack" in p["path"]]
        r.note(f"ack POST paths seen at the stub: {ack_paths!r}")
        acked_files = {}
        for key in (NAME, SID_WRONG, SID_TRUE):
            try:
                acked_files[key] = json.loads(
                    (conf / f"{key}.json").read_text(encoding="utf-8")).get("acked")
            except OSError:
                acked_files[key] = "<missing>"
        r.note(f"confirmed-set 'acked' after the ack: {acked_files!r}")

        right = [p for p in ack_paths if f"/api/agent/{NAME}/ack" == p]
        wrong = [p for p in ack_paths if SID_WRONG in p]
        ok = (len(right) == 1 and not wrong
              and acked_files.get(NAME) == [ack_id]
              and acked_files.get(SID_WRONG) == [])
        return r.finish(ok, f"ack POSTed to {ack_paths!r}; "
                            f"confirmed-set updated under {NAME!r}="
                            f"{acked_files.get(NAME)!r}, stale-sid file="
                            f"{acked_files.get(SID_WRONG)!r}")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run3() -> Result:
    """OVER-FIX GUARD. No `user_set` row for T -> must never subscribe.

    The registry carries a row for T with naming_status='derived' rather than no
    row at all: that is still "no user_set row for T", and it additionally
    catches a fix that resolves the id and then drops the user_set gate."""
    r = Result("3", "no subscription, ever")
    tree = stub = child = None
    try:
        tree, stub, child, t_write, within, pre = _deferred_boot_run(
            "3",
            {SID_TRUE: reg_row(NAME, str(SCRATCH / "run3" / "ws"),
                               naming_status="derived")},
            NAME, r, window_s=8.0)
        time.sleep(4.0)   # give a hypothetical over-eager fix room to fire late
        total = stub.connect_count()
        return r.finish(total == 0, f"{total} GET /api/events (expected 0)")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run3b() -> Result:
    """[added] The over-fix guard in a form that DISCRIMINATES on the current
    build. Run 3 boots the run-2 way, so its zero is produced by the boot defect
    and would be zero whatever the `user_set` gate did. Here the session file is
    present at launch -- the plugin resolves the TRUE sid, reaches the registry,
    finds a row for T that is `derived` rather than `user_set`, and must still
    refuse to subscribe. This is the assertion run 3 is meant to carry."""
    r = Result("3b", "no subscription (row present but not user_set)")
    stub = child = None
    try:
        ws = str(SCRATCH / "run3b" / "ws")
        tree, stub, child = _boot(
            "3b", registry={SID_TRUE: reg_row(NAME, ws, naming_status="derived")},
            session_at_launch=(SID_TRUE, NAME), env_sid=SID_WRONG)
        child.handshake()
        r.note("session file present at launch; registry row for T is "
               "naming_status='derived'")
        time.sleep(8.0)
        n = stub.connect_count()
        resolved_true_sid = SID_TRUE[:8] in child.stderr_text()
        r.note(f"plugin stderr names the TRUE sid {SID_TRUE[:8]}: {resolved_true_sid} "
               f"(proves the id resolved and the registry was reached)")
        return r.finish(n == 0 and resolved_true_sid,
                        f"{n} GET /api/events (expected 0); true sid resolved="
                        f"{resolved_true_sid}")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def _churn_registry(tree: Tree, rows_fn, stop: threading.Event,
                    every_s: float = 8.0) -> None:
    i = 0
    while not stop.wait(every_s):
        i += 1
        tree.write_registry(rows_fn(i))


def run4() -> Result:
    """DUPLICATE SUBSCRIPTION after a run-2 boot. Exactly one GET for the run."""
    r = Result("4", "exactly 1 GET (2 = silent-loss regression)")
    tree = stub = child = None
    stop = threading.Event()
    try:
        ws = str(SCRATCH / "run4" / "ws")
        tree, stub, child, t_write, within, pre = _deferred_boot_run(
            "4", {SID_TRUE: reg_row(NAME, ws)}, NAME, r)
        t = threading.Thread(
            target=_churn_registry, daemon=True,
            args=(tree, lambda i: {SID_TRUE: reg_row(NAME, ws),
                                   PEER: reg_row(PEER, ws),
                                   f"0000000{i}-0000-4000-8000-000000000000":
                                       reg_row(f"churn{i}", ws)}, stop))
        t.start()
        r.note("running 60s with the registry rewritten every 8s")
        time.sleep(60.0)
        stop.set()
        n = stub.connect_count()
        return r.finish(n == 1, f"{n} GET /api/events over the 60s run")
    finally:
        stop.set()
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run4b() -> Result:
    """[added] Same duplicate-subscription guard, but from a run-1 boot, so it
    can actually observe a duplicate on the current build."""
    r = Result("4b", "exactly 1 GET (2 = silent-loss regression)")
    stop = threading.Event()
    stub = child = None
    try:
        ws = str(SCRATCH / "run4b" / "ws")
        tree, stub, child = _boot("4b", registry={SID_TRUE: reg_row(NAME, ws)},
                                  session_at_launch=(SID_TRUE, NAME),
                                  env_sid=SID_WRONG)
        child.handshake()
        wait_for(lambda: stub.connect_count() >= 1, 5.0)
        r.note(f"boot subscription count = {stub.connect_count()}")
        t = threading.Thread(
            target=_churn_registry, daemon=True,
            args=(tree, lambda i: {SID_TRUE: reg_row(NAME, ws),
                                   PEER: reg_row(PEER, ws),
                                   f"0000000{i}-0000-4000-8000-000000000000":
                                       reg_row(f"churn{i}", ws)}, stop))
        t.start()
        r.note("running 60s with the registry rewritten every 8s")
        time.sleep(60.0)
        stop.set()
        n = stub.connect_count()
        return r.finish(n == 1, f"{n} GET /api/events over the 60s run")
    finally:
        stop.set()
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def _d143_assert(r: Result, stub: Stub, child: Child, *, expect_subscription: bool,
                 tag: str) -> Result:
    """Shared D143 assertions once the boot phase is done."""
    n = stub.connect_count()
    if not expect_subscription:
        return r.finish(n == 0, f"{n} GET /api/events (expected 0)")
    if n != 1:
        return r.finish(False, f"{n} GET /api/events (expected 1)")

    stub.emit(NAME, f"addressed-to-registry-name-{tag}")
    stub.emit(TITLE, f"addressed-to-conversation-title-{tag}")
    wait_for(lambda: delivered_to(child, NAME), 10.0)
    time.sleep(1.5)   # give a wrong-name delivery time to show up too
    good = delivered_to(child, NAME)
    bad = delivered_to(child, TITLE)
    r.note(f"delivered to {NAME!r}: {len(good)}; delivered to the title string: {len(bad)}")

    reply = child.request("tools/call", {
        "name": "peer_reply",
        "arguments": {"to_session_id": PEER, "content": f"outbound-{tag}"},
    }, timeout=20.0)
    reply_txt = json.dumps(reply) if reply else "<no response>"
    r.note(f"peer_reply result: {reply_txt[:200]}")
    froms = [b.get("from_agent") for b in stub.post_bodies() if "from_agent" in b]
    r.note(f"outbound from_agent values seen at the stub: {froms!r}")

    blob = json.dumps(stub.post_bodies(), ensure_ascii=False) + json.dumps(
        child.channel_notifications(), ensure_ascii=False)
    title_in_wire = TITLE in blob
    title_in_stderr = TITLE in child.stderr_text()
    r.note(f"title string present on the wire={title_in_wire}, "
           f"in plugin stderr={title_in_stderr}")

    ok = (len(good) == 1 and len(bad) == 0 and froms == [NAME]
          and not title_in_wire and not title_in_stderr)
    return r.finish(ok, f"1 GET; delivered={len(good)} to {NAME!r}, {len(bad)} to the "
                        f"title; from_agent={froms!r}; title on wire={title_in_wire}")


def run5() -> Result:
    """D143 TRAP on a run-2 boot."""
    r = Result("5", "subscribe as t_probe; title appears nowhere")
    stub = child = None
    try:
        ws = str(SCRATCH / "run5" / "ws")
        tree, stub, child, t_write, within, pre = _deferred_boot_run(
            "5", {SID_TRUE: reg_row(NAME, ws), PEER: reg_row(PEER, ws)}, TITLE, r)
        if not within:
            return r.finish(False, "no subscription after the write -- the run-2 defect "
                                   "blocks this assertion (VACUOUS today)")
        return _d143_assert(r, stub, child, expect_subscription=True, tag="run5")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run5_control() -> Result:
    """D143 NEGATIVE CONTROL on a run-2 boot: registry row for T removed, so the
    conversation title is the only plausible name source."""
    r = Result("5-control", "no subscription (title is the only name source)")
    stub = child = None
    try:
        ws = str(SCRATCH / "run5-control" / "ws")
        tree, stub, child, t_write, within, pre = _deferred_boot_run(
            "5-control", {PEER: reg_row(PEER, ws)}, TITLE, r, window_s=8.0)
        time.sleep(4.0)
        return _d143_assert(r, stub, child, expect_subscription=False, tag="run5c")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run5b() -> Result:
    """[added] D143 TRAP on a run-1 boot -- the variant that is testable today."""
    r = Result("5b", "subscribe as t_probe; title appears nowhere")
    stub = child = None
    try:
        ws = str(SCRATCH / "run5b" / "ws")
        tree, stub, child = _boot(
            "5b", registry={SID_TRUE: reg_row(NAME, ws), PEER: reg_row(PEER, ws)},
            session_at_launch=(SID_TRUE, TITLE), env_sid=SID_WRONG)
        child.handshake()
        r.note(f"session file present at launch with name={TITLE!r}, "
               f"registry names {SID_TRUE[:8]} as {NAME!r}")
        wait_for(lambda: stub.connect_count() >= 1, 5.0)
        return _d143_assert(r, stub, child, expect_subscription=True, tag="run5b")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


def run5b_control() -> Result:
    """[added] Its negative control: session file present with the title as
    `name`, but NO registry row for T. Title is the only plausible name source."""
    r = Result("5b-control", "no subscription (title is the only name source)")
    stub = child = None
    try:
        ws = str(SCRATCH / "run5b-control" / "ws")
        tree, stub, child = _boot(
            "5b-control", registry={PEER: reg_row(PEER, ws)},
            session_at_launch=(SID_TRUE, TITLE), env_sid=SID_WRONG)
        child.handshake()
        r.note(f"session file present at launch with name={TITLE!r}, "
               f"NO registry row for {SID_TRUE[:8]}")
        time.sleep(8.0)
        return _d143_assert(r, stub, child, expect_subscription=False, tag="run5bc")
    finally:
        if child:
            child.stop()
        if stub:
            stub.shutdown()


RUNS = {
    "1": run1, "2": run2, "2-ack": run2_ack, "3": run3, "3b": run3b,
    "4": run4, "4b": run4b,
    "5": run5, "5-control": run5_control,
    "5b": run5b, "5b-control": run5b_control,
}
ORDER = ["1", "2", "2-ack", "3", "3b", "4", "4b",
         "5", "5-control", "5b", "5b-control"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="*", default=["all"],
                    help="run ids: " + " ".join(ORDER) + " | all")
    ns = ap.parse_args(argv)
    if not PLUGIN.is_file():
        print(f"plugin not found: {PLUGIN}", file=sys.stderr)
        return 2
    SCRATCH.mkdir(parents=True, exist_ok=True)
    wanted = ORDER if ("all" in ns.runs or not ns.runs) else ns.runs
    bad = [w for w in wanted if w not in RUNS]
    if bad:
        print(f"unknown run(s): {bad}; valid: {ORDER}", file=sys.stderr)
        return 2

    print(f"plugin  : {PLUGIN}")
    print(f"python  : {sys.executable}")
    print(f"scratch : {SCRATCH}")
    print(f"harness pid (= plugin ppid, so sessions/<pid>.json): {os.getpid()}\n")

    results: list[Result] = []
    for w in wanted:
        print(f"--- run {w} ---", flush=True)
        try:
            results.append(RUNS[w]())
        except Exception as e:
            r = Result(w, "(run raised)")
            results.append(r.finish(False, f"{type(e).__name__}: {e}"))
        print("", flush=True)

    w0 = max(len(r.run) for r in results)
    w1 = max(len(r.expected) for r in results)
    w2 = max(len(r.actual) for r in results)
    line = f"| {'run'.ljust(w0)} | {'expected'.ljust(w1)} | {'actual'.ljust(w2)} | verdict |"
    print("\n" + line)
    print("|" + "-" * (w0 + 2) + "|" + "-" * (w1 + 2) + "|" + "-" * (w2 + 2) + "|---------|")
    for r in results:
        print(f"| {r.run.ljust(w0)} | {r.expected.ljust(w1)} | {r.actual.ljust(w2)} "
              f"| {'PASS' if r.passed else 'FAIL'}    |")
    return int(not all(r.passed for r in results))


if __name__ == "__main__":
    raise SystemExit(main())
