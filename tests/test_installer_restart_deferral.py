"""Owned file/receipt behavior, plus opt-in actual built-package installation.

The local fixture account interception is explicit and is not target-account proof.
No test starts a production service or interprets a healthy old listener as activation.
"""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("messaging_install_runtime", ROOT / "deploy/install_runtime.py")
installer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = installer
SPEC.loader.exec_module(installer)


def package(tmp_path):
    """Materialize the actual explicit source projection, without inventing code."""
    bundle = tmp_path / "payload"
    declaration = json.loads((ROOT / "deploy/release-files.json").read_text())
    for row in declaration["entries"]:
        target = bundle / row["destination"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / row["source"], target)
    (bundle / ".gitignore").write_text("*.db*\n.venv/\nbroker.env\n")
    (bundle / "VERSION.txt").write_text("2026.01.01.000001\n")
    (bundle / "manifest.json").write_text(json.dumps({"packagedCommit": "a" * 40, "bundleVersion": "2026.01.01.000001"}))
    rows = {path.relative_to(bundle).as_posix(): {"path": path.relative_to(bundle).as_posix(), "sha256": installer.sha(path), "mode": "100644"} for path in bundle.rglob("*") if path.is_file()}
    p = {"UserRoot": str(tmp_path / "home"), "InstallRoot": str(tmp_path / "home/.agents/services/broker"), "PythonExe": sys.executable, "Harness": ["none"], "Port": 8787}
    return bundle, rows, p


def receipt(p, writes):
    return {"schema_version": 1, "component": installer.COMPONENT, "repository": installer.REPOSITORY, "version": "2026.01.01.000001", "source_commit": "a" * 40, "archive_sha256": "b" * 64, "parameters": p, "files": [{"path": str(w.path), "sha256": installer.digest(w.data), "mode": w.mode} for w in writes if w.data is not None], "configuration_paths": [], "held_paths": [], "last_code_change_utc": None}


def test_owned_updates_preserve_database_and_unknown_neighbors(tmp_path):
    bundle, rows, p = package(tmp_path)
    writes = installer.output_plan(bundle, rows, p, None, {"schema_version": 1, "files": {}})
    backups = tmp_path / "recovery"
    backups.mkdir()
    effects = []
    installer.apply_writes(writes, backups, effects)
    database = Path(p["InstallRoot"]) / "coordination.db"
    database.write_bytes(b"synthetic-private-database")
    neighbor = database.with_name("user-note.txt")
    neighbor.write_bytes(b"retain unknown neighbor")
    prior = receipt(p, writes)
    installer.verify_files(prior)
    target = bundle / "broker/server.py"
    target.write_bytes(target.read_bytes() + b"\n# synthetic next release\n")
    rows["broker/server.py"]["sha256"] = installer.sha(target)
    plan = installer.output_plan(bundle, rows, p, prior, {"schema_version": 1, "files": {}})
    installer.apply_writes(plan, backups, effects)
    assert (Path(p["InstallRoot"]) / "server.py").read_bytes() == target.read_bytes()
    assert database.read_bytes() == b"synthetic-private-database"
    assert neighbor.read_bytes() == b"retain unknown neighbor"
    assert any(path.name.endswith("-server.py") for path in backups.iterdir())


def test_edited_owned_file_blocks_entire_plan_before_any_replacement(tmp_path):
    bundle, rows, p = package(tmp_path)
    writes = installer.output_plan(bundle, rows, p, None, {"schema_version": 1, "files": {}})
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    installer.apply_writes(writes, recovery, [])
    prior = receipt(p, writes)
    edited = Path(p["InstallRoot"]) / "server.py"
    edited.write_bytes(b"user edited immutable code")
    before = {path: path.read_bytes() for path in Path(p["UserRoot"]).rglob("*") if path.is_file()}
    with pytest.raises(installer.Refusal, match="drift"):
        installer.output_plan(bundle, rows, p, prior, {"schema_version": 1, "files": {}})
    assert {path: path.read_bytes() for path in before} == before
    with pytest.raises(installer.Refusal, match="drift"):
        installer.verify_files(prior)


def test_foreign_code_cannot_be_adopted_even_with_consent(tmp_path):
    bundle, rows, p = package(tmp_path)
    target = Path(p["InstallRoot"]) / "server.py"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"foreign application")
    p["_adopt"] = True
    with pytest.raises(installer.Refusal, match="Foreign"):
        installer.output_plan(bundle, rows, p, None, {"schema_version": 1, "files": {}})
    assert target.read_bytes() == b"foreign application"


def test_retirement_requires_explicit_prior_hash_and_preserves_edited_file(tmp_path):
    bundle, rows, p = package(tmp_path)
    target = Path(p["InstallRoot"]) / "obsolete.py"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"prior owned code")
    prior = receipt(p, [installer.Write(target, target.read_bytes(), None)])
    with pytest.raises(installer.Refusal, match="explicit hash-bound retirement"):
        installer.output_plan(bundle, rows, p, prior, {"schema_version": 1, "files": {}})
    retirement = {"schema_version": 1, "files": [{"path": "broker/obsolete.py", "sha256": installer.sha(target)}]}
    (bundle / "deploy/retirements.json").write_bytes(installer.encode(retirement))
    plan = installer.output_plan(bundle, rows, p, prior, {"schema_version": 1, "files": {}})
    assert [w.path for w in plan if w.data is None] == [target]
    target.write_bytes(b"locally modified")
    with pytest.raises(installer.Refusal, match="Changed obsolete"):
        installer.output_plan(bundle, rows, p, prior, {"schema_version": 1, "files": {}})
    assert target.read_bytes() == b"locally modified"


def test_selection_expansion_keeps_prior_code_and_installs_only_selected_surface(tmp_path):
    bundle, rows, p = package(tmp_path)
    initial = installer.output_plan(bundle, rows, p, None, {"schema_version": 1, "files": {}})
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    installer.apply_writes(initial, recovery, [])
    prior = receipt(p, initial)
    selected = installer.resolve_parameters({"Version": "2026.01.01.000002", "Harness": ["codex"]}, prior)
    plan = installer.output_plan(bundle, rows, selected, prior, {"schema_version": 1, "files": {}})
    installer.apply_writes(plan, recovery, [])
    root = Path(p["InstallRoot"])
    assert (root / "codex_adapter.py").read_bytes() == (bundle / "broker/codex_adapter.py").read_bytes()
    assert (root / "scripts/start-codex-relay-session.ps1").is_file()
    assert not (root / "omp_relay_mcp.py").exists()
    assert not (Path(p["UserRoot"]) / ".claude/plugins/local-relay-channel").exists()
    assert (Path(p["UserRoot"]) / ".agents/skills/relay-name/scripts/set_name.py").is_file()
    assert not list(Path(p["UserRoot"]).rglob("mcp.json"))


def test_atomic_failure_retains_prior_bytes_and_unknown_neighbor(tmp_path, monkeypatch):
    target = tmp_path / "owned.py"
    target.write_bytes(b"old")
    neighbor = tmp_path / "neighbor.db"
    neighbor.write_bytes(b"state")
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    actual_atomic = installer.atomic
    def interrupted(path, *args, **kwargs):
        if path == target:
            raise OSError("synthetic interrupted file replacement")
        return actual_atomic(path, *args, **kwargs)
    monkeypatch.setattr(installer, "atomic", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        installer.apply_writes([installer.Write(target, b"new", installer.sha(target))], recovery, [])
    assert target.read_bytes() == b"old" and neighbor.read_bytes() == b"state"
    entry, = json.loads((recovery / "operation.json").read_text())["backup_map"]
    assert entry == {"original": str(target), "backup": str(recovery / (installer.digest(str(target).encode()) + "-owned.py")), "kind": "code", "original_sha256": installer.digest(b"old"), "candidate_sha256": installer.digest(b"new")}
    assert Path(entry["backup"]).read_bytes() == b"old"


@pytest.mark.parametrize("name", ["../escape.py", "/absolute.py", "x/../escape", "a\\b", "x/CON.py", "x/name."])
def test_unsafe_members_refuse(name):
    with pytest.raises(installer.Refusal):
        installer.member(name)


def test_actual_built_package_check_install_and_retained_verify(tmp_path, monkeypatch):
    """Real venv/wheels/bytes; synthetic account and no-listener observation only."""
    bundle_input = os.environ.get("AGENT_MESSAGING_TEST_BUNDLE")
    receipt_input = os.environ.get("AGENT_MESSAGING_TEST_RECEIPT")
    python = os.environ.get("AGENT_MESSAGING_TEST_BASE_PYTHON")
    if not all((bundle_input, receipt_input, python)):
        pytest.skip("requires real built bundle, release receipt and selected base Python")
    bundle = tmp_path / "download/payload"
    shutil.copytree(bundle_input, bundle)
    release = json.loads(Path(receipt_input).read_text())
    release_path = tmp_path / "download/release.json"
    release_path.write_bytes(installer.encode(release))
    user, work = tmp_path / "home", tmp_path / "work"
    installer.private_directory(user)
    options = {"Version": release["version"], "Mode": "Check", "WorkRoot": str(work), "UserRoot": str(user), "InstallRoot": str(user / ".agents/services/broker"), "PythonExe": python, "Harness": ["none"], "BundleDir": str(bundle), "ReleaseReceipt": str(release_path)}
    # This file-only fixture must not inspect or adopt the production listener.
    # Actual installed entrypoints use separately owned ephemeral loopback ports.
    monkeypatch.setattr(installer, "listener", lambda port, root: None)
    result = installer.run(options, actual_profile=user)
    assert result["status"] == "checked"
    assert not (user / ".agents").exists()
    options["Mode"] = "Install"
    installed = installer.run(options, actual_profile=user)
    assert installed["status"] == "installed"
    assert installed["verification"] == "verified"
    root = Path(options["InstallRoot"])
    assert (root / ".venv/Scripts/agent-messaging.exe").is_file()
    assert not (user / ".claude").exists()
    env = root / "broker.env"
    assert not env.read_bytes().startswith(b"\xef\xbb\xbf")
    for name in ("coordination.db", "coordination.db-wal", "unmanaged.txt"):
        (root / name).write_bytes(b"preserve " + name.encode())
    neighbor = root / ".venv/user-owned-neighbor.txt"
    neighbor.write_bytes(b"not a dependency file")
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in user.rglob("*") if path.is_file()}
    assert installer.run(options, actual_profile=user)["status"] == "verified-no-op"
    assert {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in before} == before
    installed_receipt = json.loads((user / ".agents/installations/agent-messaging/installed.json").read_text())
    assert str(neighbor) not in {row["path"] for row in installed_receipt["files"]}
    shutil.rmtree(bundle.parent)
    def forbidden(*args, **kwargs):
        raise AssertionError("Verify attempted download/provisioning")
    monkeypatch.setattr(installer, "wheelhouse", forbidden)
    monkeypatch.setattr(installer, "interpreter", forbidden)
    verify = {key: value for key, value in options.items() if key not in {"BundleDir", "ReleaseReceipt", "Harness"}}
    verify["Mode"] = "Verify"
    assert installer.run(verify, actual_profile=user)["status"] == "verified"
    target = root / "server.py"
    target.write_bytes(target.read_bytes() + b"\n# tamper\n")
    with pytest.raises(installer.Refusal, match="drift"):
        installer.run(verify, actual_profile=user)
    for name in ("coordination.db", "coordination.db-wal", "unmanaged.txt"):
        assert (root / name).read_bytes() == b"preserve " + name.encode()


def test_correct_base_python_claude_registration_is_preserved_and_foreign_one_refuses(tmp_path, monkeypatch):
    _, _, p = package(tmp_path)
    p["Harness"] = ["claude"]
    monkeypatch.delenv("BROKER_BASE_URL", raising=False)
    target = Path(p["UserRoot"]) / ".claude.json"
    target.parent.mkdir(parents=True)
    entry = {"type": "stdio", "command": p["PythonExe"], "args": [str(Path(p["UserRoot"]) / ".claude/plugins/local-relay-channel/server.py")]}
    data = {"mcpServers": {"local-relay-channel": entry, "foreign": {"command": "foreign-command"}}, "unrelated": "retain"}
    target.write_bytes(installer.encode(data))
    before = target.read_bytes()
    assert installer.claude_registration(p) == (target, True)
    assert installer.claude_contribution(p)["command"] == p["PythonExe"]
    assert target.read_bytes() == before
    data["mcpServers"]["local-relay-channel"]["args"] = ["foreign-server.py"]
    target.write_bytes(installer.encode(data))
    with pytest.raises(installer.Refusal, match="Foreign/conflicting"):
        installer.claude_registration(p)
    assert json.loads(target.read_text())["mcpServers"]["foreign"] == {"command": "foreign-command"}


def test_missing_interpreter_refuses_before_creating_installation(tmp_path):
    user = tmp_path / "home"
    user.mkdir()
    work = tmp_path / "work"
    with pytest.raises(installer.Refusal, match="existing base Python"):
        installer.run({"Version": "2026.01.01.000001", "WorkRoot": str(work), "UserRoot": str(user), "InstallRoot": str(user / ".agents/services/broker"), "PythonExe": str(tmp_path / "absent-python.exe"), "Mode": "Check"}, actual_profile=user)
    assert not list(user.iterdir())
    assert not work.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell module isolation")
def test_native_acl_child_ignores_incompatible_inherited_modules_without_cache_writes(tmp_path, monkeypatch):
    poison = tmp_path / "incompatible-modules/Microsoft.PowerShell.Security"
    poison.mkdir(parents=True)
    (poison / "Microsoft.PowerShell.Security.psm1").write_text("throw 'An incompatible inherited security module was loaded'\n")
    modules = str(poison.parent)
    cache = str(tmp_path / "caller-cache-must-not-exist")
    monkeypatch.setenv("PSModulePath", modules)
    monkeypatch.setenv("PSModuleAnalysisCachePath", cache)
    result = installer.powershell("$a=Get-Acl -LiteralPath $args[0];[pscustomobject]@{owner=$a.GetOwner([Security.Principal.SecurityIdentifier]).Value;caller=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value}|ConvertTo-Json -Compress", str(tmp_path))
    assert result["owner"] == result["caller"]
    assert os.environ["PSModulePath"] == modules
    assert os.environ["PSModuleAnalysisCachePath"] == cache
    assert not Path(cache).exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell frontdoor restoration")
def test_direct_frontdoor_refusal_restores_callers_module_environment(tmp_path):
    import subprocess
    poison = tmp_path / "incompatible-modules"
    poison.mkdir()
    cache = tmp_path / "caller-cache-must-not-exist"
    script = tmp_path / "invoke-frontdoor.ps1"
    quote = lambda value: "'" + str(value).replace("'", "''") + "'"
    script.write_text("$ErrorActionPreference='Stop'\n$env:PSModulePath=" + quote(poison) + "\n$env:PSModuleAnalysisCachePath=" + quote(cache) + "\ntry { & " + quote(ROOT / "install.ps1") + " -Version latest -WorkRoot " + quote(tmp_path / "work") + " -Mode Verify; throw 'Expected exact-tag refusal' } catch { if ($_.Exception.Message -notlike '*exact release tag*') { throw } }\nif ($env:PSModulePath -cne " + quote(poison) + " -or $env:PSModuleAnalysisCachePath -cne " + quote(cache) + ") { throw 'Caller module environment changed' }\n[Console]::WriteLine('restored')\n")
    child_env = installer.powershell_environment(installer.native_powershell())
    result = subprocess.run([str(installer.native_powershell()), "-NoProfile", "-NonInteractive", "-File", str(script)], env=child_env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "restored"
    assert not cache.exists()
    assert not (tmp_path / "work").exists()


@pytest.mark.parametrize("declaration", ["BROKER_PORT=18491", "  BROKER_PORT \t= \t18491  ", "BROKER_PORT = ' 18491 '", 'BROKER_PORT = "18491"', "BROKER_PORT=12345\nBROKER_PORT = 18491"])
def test_adopted_port_grammar_preserves_existing_whitespace_selection(tmp_path, declaration):
    root = tmp_path / "home/.agents/services/broker"
    root.mkdir(parents=True)
    envfile = root / "broker.env"
    envfile.write_text("BROKER_HOST = '127.0.0.1'\n" + declaration + "\n")
    before = envfile.read_bytes()
    p = installer.resolve_parameters({"Version": "2026.01.01.000001", "UserRoot": str(tmp_path / "home"), "InstallRoot": str(root), "PythonExe": sys.executable, "Harness": ["none"]}, None)
    assert p["Port"] == 18491
    assert envfile.read_bytes() == before
    with pytest.raises(installer.Refusal, match="conflicts"):
        installer.resolve_parameters({"Version": "2026.01.01.000001", "UserRoot": str(tmp_path / "home"), "InstallRoot": str(root), "PythonExe": sys.executable, "Port": 8787, "Harness": ["none"]}, None)


@pytest.mark.parametrize("declaration", ["BROKER_PORT=", "BROKER_PORT=018491", "BROKER_PORT=+18491", "BROKER_PORT=65536", "BROKER_PORT=18491 # inline", "BROKER_PORT='18491", 'BROKER_PORT=18491"', "BROKER_PORT : 18491", "broker_port=18491", "export BROKER_PORT=18491", "BROKER_PORT=bad\nBROKER_PORT=18491"])
def test_invalid_existing_port_refuses_before_work_or_process_effects(tmp_path, declaration, monkeypatch):
    user = tmp_path / "home"
    root = user / ".agents/services/broker"
    root.mkdir(parents=True)
    envfile = root / "broker.env"
    envfile.write_text(declaration + "\n")
    before = envfile.read_bytes()
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid port reached process/prerequisite effects")
    monkeypatch.setattr(installer, "owned", lambda *args: None)
    monkeypatch.setattr(installer, "interpreter", forbidden)
    monkeypatch.setattr(installer, "listener", forbidden)
    with pytest.raises(installer.Refusal):
        installer.run({"Version": "2026.01.01.000001", "Mode": "Check", "WorkRoot": str(tmp_path / "work"), "UserRoot": str(user), "InstallRoot": str(root), "PythonExe": sys.executable, "Port": 8787}, actual_profile=user)
    assert envfile.read_bytes() == before
    assert not (tmp_path / "work").exists()


def test_private_configuration_backup_mapping_contains_no_rendered_secret_hash(tmp_path):
    target = tmp_path / "settings.json"
    secret = b'{"fixture_secret":"synthetic-value-never-in-report"}'
    target.write_bytes(secret)
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    installer.backup(target, recovery, kind="configuration")
    entry, = json.loads((recovery / "operation.json").read_text())["backup_map"]
    assert set(entry) == {"original", "backup", "kind"}
    assert entry["kind"] == "configuration" and entry["original"] == str(target)
    assert Path(entry["backup"]).read_bytes() == secret
    assert secret not in (recovery / "operation.json").read_bytes()
    assert installer.digest(secret) not in (recovery / "operation.json").read_text()


def test_metadata_only_reconciliation_preserves_code_time_and_no_activation_claim(tmp_path):
    code = {"path": str(tmp_path / "server.py"), "sha256": installer.digest(b"same executable"), "mode": "100644"}
    environment = {"path": str(tmp_path / ".venv/Scripts/python.exe"), "sha256": installer.digest(b"same interpreter"), "mode": "100644"}
    metadata = {"path": str(tmp_path / "release.json"), "sha256": installer.digest(b"old metadata"), "mode": "100644"}
    prior = {"files": [code, environment, metadata], "last_code_change_utc": "2026-01-01T00:00:00Z"}
    updated = [code, environment, {**metadata, "sha256": installer.digest(b"new metadata")}]
    status, timestamp, pending = installer.installation_outcome(prior, updated, reconciled=True, now="2026-02-01T00:00:00Z")
    assert status == "reconciled" and timestamp == prior["last_code_change_utc"] and pending is False
    assert installer.installation_outcome(prior, prior["files"], reconciled=False, now="2026-02-01T00:00:00Z") == ("verified-no-op", prior["last_code_change_utc"], False)
    changed = [{**code, "sha256": installer.digest(b"new executable")}, environment, metadata]
    assert installer.installation_outcome(prior, changed, reconciled=True, now="2026-02-01T00:00:00Z") == ("installed", "2026-02-01T00:00:00Z", True)


@pytest.mark.skipif(os.name != "nt", reason="Windows process argument identity")
@pytest.mark.parametrize("listening", [True, False])
def test_legacy_relative_process_refusal_names_pid_selected_root_and_port_without_stop(tmp_path, monkeypatch, listening):
    root = tmp_path / "broker"
    observation = {"owners": [43210] if listening else [], "processes": [{"pid": 43210, "command": '"C:/Python/python.exe" -B server.py', "executable": "C:/Python/python.exe", "started": "2026-01-01T00:00:00Z"}]}
    monkeypatch.setattr(installer, "powershell", lambda *args: observation)
    def forbidden(*args, **kwargs):
        raise AssertionError("Unknown-root process reached a stop/start command")
    monkeypatch.setattr(installer, "command", forbidden)
    with pytest.raises(installer.Refusal) as error:
        installer.listener(18491, root)
    assert all(value in str(error.value) for value in ("43210", "18491", str(root), "unproven root", "without Force"))
    assert not root.exists()


@pytest.mark.parametrize("new_instance", [False, True])
def test_activation_requires_a_new_process_identity_even_when_health_is_good(tmp_path, monkeypatch, new_instance):
    import io
    import time
    import types
    import urllib.request

    root = tmp_path / "broker"
    root.mkdir()
    (root / "broker.env").write_text("BROKER_DB_PATH=coordination.db\n", encoding="utf-8")
    before = {"pid": 43210, "started": "2026-01-01T00:00:00Z"}
    candidate = {"pid": 43211, "started": "2026-01-01T00:00:01Z"} if new_instance else before
    health = {"status": "ok", "checks": {"sqlite": True, "db_path": str(root / "coordination.db")}}
    health_queries = []

    def health_response(url, **kwargs):
        health_queries.append(url)
        return io.StringIO(json.dumps(health))

    def no_second_stop(*args, **kwargs):
        raise AssertionError("Activation must not re-stop an already stopped prior PID")

    with monkeypatch.context() as scoped:
        ticks = iter(range(0, 100, 10))
        scoped.setattr(time, "monotonic", lambda: next(ticks))
        scoped.setattr(time, "sleep", lambda seconds: None)
        scoped.setattr(installer, "listener", lambda port, selected_root: candidate)
        scoped.setattr(installer, "native_powershell", lambda: Path("C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"))
        scoped.setattr(installer, "command", no_second_stop)
        scoped.setattr(installer.subprocess, "Popen", lambda *args, **kwargs: types.SimpleNamespace(wait=lambda timeout: 0))
        scoped.setattr(urllib.request, "urlopen", health_response)
        if new_instance:
            result = installer.activate({"InstallRoot": str(root), "Port": 18491}, before, [])
            assert result["status"] == "activated" and result["pid"] == candidate["pid"] and result["started"] == candidate["started"]
            assert health_queries == ["http://127.0.0.1:18491/api/health"]
        else:
            with pytest.raises(installer.Refusal, match="old healthy listener"):
                installer.activate({"InstallRoot": str(root), "Port": 18491}, before, [])
            assert health_queries == []


def test_fresh_no_harness_is_service_only_and_environment_python_is_selected(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHON_EXE", sys.executable)
    monkeypatch.delenv("OMP_COMMAND", raising=False)
    monkeypatch.setattr(installer.shutil, "which", lambda name: None)
    p = installer.resolve_parameters({"Version": "2026.01.01.000001", "UserRoot": str(tmp_path / "home")}, None)
    assert p["Harness"] == ["none"]
    assert p["PythonExe"] == str(Path(sys.executable))
    assert p["InstallRoot"] == str(tmp_path / "home/.agents/services/broker")
    assert not (tmp_path / "home").exists()


def test_invalid_environment_python_never_falls_back_to_path(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHON_EXE", str(tmp_path / "missing-python.exe"))
    monkeypatch.setattr(installer.shutil, "which", lambda name: sys.executable)
    with pytest.raises(installer.Refusal, match="Missing/unsafe python"):
        installer.discover_python(tmp_path / "broker")
    assert not (tmp_path / "broker").exists()


def test_retained_interpreter_and_harness_win_over_changed_environment(tmp_path, monkeypatch):
    _, _, p = package(tmp_path)
    p["Harness"] = ["codex"]
    p["Port"] = 18491
    monkeypatch.setenv("PYTHON_EXE", str(tmp_path / "different-python.exe"))
    monkeypatch.setenv("OMP_COMMAND", str(tmp_path / "different-omp.exe"))
    prior = receipt(p, [])
    selected = installer.resolve_parameters({"Version": "2026.01.01.000002"}, prior)
    assert selected == p
    with pytest.raises(installer.Refusal, match="conflicting PythonExe"):
        installer.resolve_parameters({"Version": "2026.01.01.000002", "PythonExe": str(tmp_path / "different-python.exe")}, prior)


@pytest.mark.skipif(os.name != "nt", reason="Windows legacy venv home/python.exe contract")
def test_legacy_venv_base_is_preserved_before_environment_discovery(tmp_path, monkeypatch):
    root = tmp_path / "broker"
    configuration = root / ".venv/pyvenv.cfg"
    configuration.parent.mkdir(parents=True)
    base = Path(sys.base_prefix) / "python.exe"
    configuration.write_text(f"home = {base.parent}\nversion = 3.12.1\ninclude-system-site-packages = false\nexecutable = {base}\n")
    monkeypatch.setenv("PYTHON_EXE", str(tmp_path / "different-python.exe"))
    assert installer.discover_python(root) == str(base)
    assert installer.interpreter(str(base))["base"] is True
    configuration.write_text(configuration.read_text() + "home = duplicate\n")
    with pytest.raises(installer.Refusal, match="Duplicate legacy"):
        installer.discover_python(root)


def test_automatic_adoption_requires_real_known_broker_anchors(tmp_path):
    bundle, rows, p = package(tmp_path)
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    writes = installer.output_plan(bundle, rows, p, None, {"schema_version": 1, "files": {}})
    installer.apply_writes(writes, recovery, [])
    root = Path(p["InstallRoot"])
    (root / "broker.env").write_text("BROKER_HOST=127.0.0.1\nBROKER_PORT=8787\n")
    (root / "coordination.db-wal").write_bytes(b"private retained state")
    assert installer.recognizable_installation(bundle, rows, p, {"schema_version": 1, "files": {}})
    (root / "server.py").write_bytes(b"edited application, not adoption authority")
    with pytest.raises(installer.Refusal, match="Foreign/edited legacy"):
        installer.recognizable_installation(bundle, rows, p, {"schema_version": 1, "files": {}})
    assert (root / "coordination.db-wal").read_bytes() == b"private retained state"


def test_removed_adoption_override_cannot_authorize_unbound_metadata(tmp_path):
    bundle, rows, p = package(tmp_path)
    with pytest.raises(installer.Refusal, match="Unknown installer option"):
        installer.resolve_parameters({"Version": "2026.01.01.000001", "AdoptExisting": True}, None)
    metadata_file = Path(p["UserRoot"]) / ".agents/installations/agent-messaging/verifier/deploy/install_runtime.py"
    metadata_file.parent.mkdir(parents=True)
    metadata_file.write_bytes((bundle / "deploy/install_runtime.py").read_bytes())
    before = metadata_file.read_bytes()
    p["_adopt"] = installer.recognizable_installation(bundle, rows, p, {"schema_version": 1, "files": {}})
    assert p["_adopt"] is False
    with pytest.raises(installer.Refusal, match="recognizable same-user installation"):
        installer.output_plan(bundle, rows, p, None, {"schema_version": 1, "files": {}})
    assert metadata_file.read_bytes() == before
    assert not Path(p["InstallRoot"]).exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows legacy venv home/python.exe contract")
@pytest.mark.parametrize("metadata", ["venv", "alias", "other-base", "relative"])
def test_legacy_executable_metadata_cannot_override_declared_base(tmp_path, monkeypatch, metadata):
    root = tmp_path / "broker"
    configuration = root / ".venv/pyvenv.cfg"
    configuration.parent.mkdir(parents=True)
    base = Path(sys.base_prefix) / "python.exe"
    recorded = {"venv": root / ".venv/Scripts/python.exe", "alias": tmp_path / "WindowsApps/python.exe", "other-base": tmp_path / "other-python/python.exe", "relative": Path("python.exe")}[metadata]
    configuration.write_text(f"home = {base.parent}\nversion = 3.12.1\ninclude-system-site-packages = false\nexecutable = {recorded}\n")
    def forbidden(*args, **kwargs):
        raise AssertionError("Inconsistent executable metadata reached process execution")
    monkeypatch.setattr(installer, "command", forbidden)
    with pytest.raises(installer.Refusal, match="metadata conflicts|absolute local path"):
        installer.discover_python(root)


@pytest.mark.parametrize("fault", ["work-overlap", "source-install", "wrong-user"])
def test_known_unsafe_coordinates_refuse_before_python_or_harness_discovery(tmp_path, monkeypatch, fault):
    user = tmp_path / "home"
    user.mkdir()
    root = user / ".agents/services/broker"
    work = tmp_path / "work"
    actual = user
    if fault == "work-overlap":
        work = root / "installer-work"
    elif fault == "source-install":
        root.mkdir(parents=True)
        (root / ".git").mkdir()
    else:
        actual = tmp_path / "another-account"
    def forbidden(*args, **kwargs):
        raise AssertionError("Unsafe coordinates reached external prerequisite discovery")
    monkeypatch.setattr(installer, "owned", lambda *args: None)
    monkeypatch.setattr(installer, "interpreter", forbidden)
    monkeypatch.setattr(installer, "discover_omp", forbidden)
    with pytest.raises(installer.Refusal, match="overlaps|source checkout|actual invoking profile"):
        installer.run({"Version": "2026.01.01.000001", "WorkRoot": str(work), "UserRoot": str(user), "InstallRoot": str(root), "PythonExe": sys.executable, "Harness": ["omp"], "Mode": "Check"}, actual_profile=actual)
    assert not work.exists()
