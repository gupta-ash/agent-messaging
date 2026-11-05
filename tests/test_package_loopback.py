"""Actual packaged and installed entrypoints on owned disposable loopback ports."""
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def run_loopback(command, runtime, fixture):
    runtime.mkdir(parents=True, exist_ok=True)
    home = fixture / "home"
    home.mkdir(parents=True)
    for name in ("appdata", "localappdata", "tmp"):
        (fixture / name).mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    prefixes = ("BROKER_", "DISCORD_", "RELAY_", "PYTHON", "ANTHROPIC_", "OPENAI_", "GITHUB_", "GH_", "COPILOT_", "CLAUDE_", "CODEX_", "OMP_", "VAULT_")
    env = {key: value for key, value in os.environ.items() if not key.startswith(prefixes)}
    database = fixture / "coordination.db"
    env.update(HOME=str(home), USERPROFILE=str(home), APPDATA=str(fixture / "appdata"), LOCALAPPDATA=str(fixture / "localappdata"), BROKER_HOST="127.0.0.1", BROKER_PORT=str(port), BROKER_DB_PATH=str(database), BROKER_AUDIT_DIR=str(fixture / "audit"), BROKER_AGENT_MESSAGE_ARCHIVE_DIR=str(fixture / "messages"), DISCORD_STATE_DIR=str(fixture / "absent-discord"), BROKER_REAPER_ENABLED="0", PYTHONDONTWRITEBYTECODE="1")
    for key in ("TEMP", "TMP", "TMPDIR", "CLAUDE_CODE_TMPDIR"):
        env[key] = str(fixture / "tmp")
    with (fixture / "stdout.log").open("wb") as stdout, (fixture / "stderr.log").open("wb") as stderr:
        process = subprocess.Popen(command, cwd=runtime, env=env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=2, trust_env=False) as client:
                deadline = time.monotonic() + 30
                while True:
                    assert process.poll() is None, "owned fixture broker exited before readiness"
                    try:
                        health = client.get("/api/health").json()
                        if health["checks"]["sqlite"]:
                            break
                    except (httpx.HTTPError, ValueError, KeyError):
                        pass
                    assert time.monotonic() < deadline, "owned fixture readiness deadline exceeded"
                    time.sleep(.1)
                assert Path(health["checks"]["db_path"]).resolve() == database.resolve()
                response = client.post("/api/event", json={"from_agent": "smoke-sender", "to_agent": "smoke-recipient", "channel": "#smoke", "kind": "agent_message", "content": "public-install-roundtrip", "ts": int(time.time() * 1000)})
                response.raise_for_status()
                inbox = client.get("/api/agent/smoke-recipient/messages")
                inbox.raise_for_status()
                assert "public-install-roundtrip" in inbox.text
                dashboard = client.get("/dashboard")
                dashboard.raise_for_status()
                assert "<html" in dashboard.text.lower()
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    assert database.is_file()
    assert (runtime / "logs/broker.log").is_file()
    assert (runtime / "logs/broker.faulthandler").is_file()


def test_exported_server_write_read_dashboard(tmp_path):
    entries = json.loads((ROOT / "deploy/release-files.json").read_text())["entries"]
    for row in entries:
        destination = tmp_path / "bundle" / row["destination"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / row["source"], destination)
    runtime = tmp_path / "bundle/broker"
    run_loopback([sys.executable, "-B", str(runtime / "server.py")], runtime, tmp_path / "smoke")
    assert not (runtime / "broker/logs").exists()


@pytest.mark.parametrize("entrypoint", ["module", "console"])
def test_installed_entrypoint_from_unrelated_runtime_cwd(tmp_path, entrypoint):
    root = os.environ.get("AGENT_MESSAGING_TEST_INSTALLED_ROOT")
    if not root:
        pytest.skip("requires real archive installed into an owned disposable fixture")
    scripts = Path(root) / ".venv/Scripts"
    command = [str(scripts / "python.exe"), "-I", "-B", "-m", "broker"] if entrypoint == "module" else [str(scripts / "agent-messaging.exe")]
    run_loopback(command, tmp_path / "runtime", tmp_path / "smoke")
