"""Exercise the declared broker/resource and relay-helper closure without the source tree."""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def projected_release(tmp_path):
    declaration = json.loads((ROOT / "deploy/release-files.json").read_text())
    entries = declaration["entries"]
    destinations = [row["destination"].casefold() for row in entries]
    assert len(destinations) == len(set(destinations))
    for row in entries:
        destination = tmp_path / row["destination"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / row["source"], destination)
    return tmp_path, entries


def isolated_environment(home, temporary):
    temporary.mkdir()
    env = os.environ.copy()
    for name in ("HOME", "USERPROFILE"):
        env[name] = str(home)
    for name in ("TEMP", "TMP", "TMPDIR"):
        env[name] = str(temporary)
    for name in tuple(env):
        if name.startswith(("PYTHON", "BROKER_")) or name == "CLAUDE_CODE_SESSION_ID":
            env.pop(name)
    env["BROKER_BASE_URL"] = "http://127.0.0.1:8787"
    return env


def test_declared_package_export_imports_without_source(projected_release):
    projection, entries = projected_release
    package_files = {p.relative_to(ROOT).as_posix() for p in (ROOT / "src/broker").rglob("*")
                     if p.is_file() and p.suffix in {".py", ".sql", ".html"}}
    assert package_files <= {row["source"] for row in entries}
    plugin_prefix = "home/.claude/plugins/local-relay-channel/"
    assert len([row for row in entries if row["destination"].startswith(plugin_prefix)]) == 6
    code = (
        "import sys; from pathlib import Path; "
        f"sys.path.insert(0, {str(projection / 'broker')!r}); "
        "import broker.addressing; import broker; p=Path(broker.__file__).parent; "
        "assert (p/'schema.sql').is_file(); assert (p/'dashboard/dashboard.html').is_file(); "
        f"assert p.is_relative_to(Path({str(projection)!r}))"
    )
    env = isolated_environment(projection / "home", projection / "temporary")
    subprocess.run([sys.executable, "-I", "-B", "-c", code], cwd=projection,
                   env=env, check=True, capture_output=True, text=True)
    assert not (projection / "broker/addressing.py").exists()
    assert not (projection / "broker/discord_poller.py").exists()


@pytest.mark.parametrize("skill,helper", [
    ("relay-check", "check.py"), ("relay-name", "set_name.py"), ("relay-send", "send.py"),
])
def test_projected_relay_helpers_load_only_shipped_library(projected_release, skill, helper):
    projection, _ = projected_release
    home = projection / "home"
    skill_root = home / ".agents/skills" / skill
    assert (skill_root / "SKILL.md").is_file()
    library = home / ".claude/hooks/relay"
    assert {p.relative_to(library).as_posix() for p in library.rglob("*") if p.is_file()} == {
        "shared.py", "broker_client.py", "broker_connection.py", "register_session.py", "registry_gc.py", "bin/cc_channel.bat",
    }
    env = isolated_environment(home, projection / "temporary")
    result = subprocess.run([sys.executable, "-I", "-B", str(skill_root / "scripts" / helper), "--help"],
                            cwd=projection, env=env, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "usage:" in result.stdout.lower()
    assert not result.stderr
    assert not (home / ".claude/relay").exists()
