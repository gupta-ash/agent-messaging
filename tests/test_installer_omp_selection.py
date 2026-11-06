"""OMP contribution ownership and optional real public-config API projection."""
import json
import os
from pathlib import Path
import sys

import pytest
from tests.test_installer_restart_deferral import installer


def parameters(tmp_path):
    user = tmp_path / "home"
    return {"UserRoot": str(user), "InstallRoot": str(user / ".agents/services/broker"), "PythonExe": sys.executable, "Harness": ["omp"], "Port": 48627, "OmpCommand": os.environ.get("AGENT_MESSAGING_TEST_OMP_COMMAND", ""), "OmpAgentRoot": str(user / ".omp/agent"), "OmpPythonExe": os.environ.get("AGENT_MESSAGING_TEST_OMP_PYTHON", sys.executable), "RelayStateRoot": str(user / ".omp-relay/state"), "RelayRuntimeRoot": str(user / ".omp-relay/runtime")}


def test_read_only_verify_checks_owned_contributions_and_preserves_neighbors(tmp_path):
    p = parameters(tmp_path)
    root, agent = Path(p["InstallRoot"]), Path(p["OmpAgentRoot"])
    for key in ("RelayStateRoot", "RelayRuntimeRoot"):
        Path(p[key]).mkdir(parents=True)
    root.mkdir(parents=True)
    agent.mkdir(parents=True)
    (root / "broker.env").write_text("BROKER_HOST=127.0.0.1\nBROKER_PORT=48627\n")
    extension = str(root / "omp_relay_extension.ts")
    config = {"extensions": ["foreign-extension.ts", extension], "model": "synthetic-model"}
    # JSON is a valid YAML document. The actual dependency parses it read-only.
    (agent / "config.yml").write_bytes(installer.encode(config))
    mcp = {"mcpServers": {"foreign": {"command": "foreign-command"}, "omp-relay": installer.omp_contribution(p)}, "disabledServers": ["foreign-disabled"], "enabledServers": ["foreign", "omp-relay"]}
    (agent / "mcp.json").write_bytes(installer.encode(mcp))
    before = {path: path.read_bytes() for path in (agent / "config.yml", agent / "mcp.json", root / "broker.env")}
    installer.verify_configuration(p, list(installer.configuration_snapshot(p)))
    assert {path: path.read_bytes() for path in before} == before
    mcp["mcpServers"]["omp-relay"]["args"][-1] = "http://127.0.0.1:8787"
    (agent / "mcp.json").write_bytes(installer.encode(mcp))
    with pytest.raises(installer.Refusal):
        installer.verify_configuration(p, list(installer.configuration_snapshot(p)))
    assert (agent / "mcp.json").read_bytes() == installer.encode(mcp)
    assert json.loads((agent / "mcp.json").read_text())["mcpServers"]["foreign"] == {"command": "foreign-command"}


def test_omp_state_root_overlap_refuses_before_creating_identity(tmp_path):
    p = parameters(tmp_path)
    p["RelayRuntimeRoot"] = str(Path(p["RelayStateRoot"]) / "nested")
    with pytest.raises(installer.Refusal, match="overlap"):
        installer.validate_roots(p, tmp_path / "work", actual_profile=Path(p["UserRoot"]))
    assert not Path(p["RelayStateRoot"]).exists()


def test_real_omp_public_config_projection_is_isolated_and_preserves_other_entries(tmp_path):
    p = parameters(tmp_path)
    if not p["OmpCommand"] or "AGENT_MESSAGING_TEST_OMP_PYTHON" not in os.environ:
        pytest.skip("requires explicitly selected real OMP CLI and dependency-bearing base Python")
    agent = Path(p["OmpAgentRoot"])
    installer.private_directory(agent)
    for key in ("RelayStateRoot", "RelayRuntimeRoot"):
        installer.private_directory(Path(p[key]))
    installer.private_directory(tmp_path / "work")
    (agent / "config.yml").write_text('extensions: ["foreign-extension.ts"]\n')
    original_mcp = {"mcpServers": {"foreign": {"command": "foreign-command"}}, "disabledServers": ["foreign-disabled"], "enabledServers": ["foreign"]}
    (agent / "mcp.json").write_bytes(installer.encode(original_mcp))
    before = {path: path.read_bytes() for path in agent.rglob("*") if path.is_file()}
    writes = installer.prepare_omp(p, tmp_path / "work")
    assert {path: path.read_bytes() for path in before} == before
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    installer.apply_writes(writes, recovery, [])
    projected = installer.yaml_document(agent / "config.yml", p["OmpPythonExe"])
    assert projected["extensions"] == ["foreign-extension.ts", str(Path(p["InstallRoot"]) / "omp_relay_extension.ts")]
    actual = json.loads((agent / "mcp.json").read_text())
    assert actual["mcpServers"]["foreign"] == original_mcp["mcpServers"]["foreign"]
    assert actual["disabledServers"] == original_mcp["disabledServers"]
    assert actual["enabledServers"] == [*original_mcp["enabledServers"], "omp-relay"]
    assert actual["mcpServers"]["omp-relay"] == installer.omp_contribution(p)


def test_bound_environment_registration_preserves_private_roots_and_entry_shape(tmp_path):
    p = parameters(tmp_path)
    agent = Path(p["OmpAgentRoot"])
    agent.mkdir(parents=True)
    for key in ("RelayStateRoot", "RelayRuntimeRoot"):
        Path(p[key]).mkdir(parents=True)
    entry = {"command": p["OmpPythonExe"], "args": [str(Path(p["InstallRoot"]) / "omp_relay_mcp.py")], "env": {"OMP_RELAY_STATE_ROOT": p["RelayStateRoot"], "OMP_RELAY_RUNTIME_ROOT": p["RelayRuntimeRoot"], "BROKER_BASE_URL": f"http://127.0.0.1:{p['Port']}"}}
    data = {"mcpServers": {"omp-relay": entry, "foreign": {"command": "foreign-command"}}, "disabledServers": ["foreign"]}
    path = agent / "mcp.json"
    path.write_bytes(installer.encode(data))
    before = path.read_bytes()
    discovered = installer.omp_entry(p, entry)
    assert all(Path(discovered[key]).samefile(p[key]) for key in ("OmpPythonExe", "RelayStateRoot", "RelayRuntimeRoot"))
    assert installer.omp_contribution(p) == entry
    assert path.read_bytes() == before
    conflicting = dict(p, RelayStateRoot=str(tmp_path / "other-state"))
    with pytest.raises(installer.Refusal, match="Conflicting retained/selected RelayStateRoot"):
        installer.omp_entry(conflicting, entry)
    entry["args"] = [str(tmp_path / "foreign/omp_relay_mcp.py")]
    with pytest.raises(installer.Refusal, match="Foreign omp-relay entrypoint/root"):
        installer.omp_entry(p, entry)
    assert path.read_bytes() == before


@pytest.mark.parametrize("profile", ["", "installer-fixture"])
def test_real_config_path_discovers_actual_default_or_named_agent_without_layout_guess(tmp_path, monkeypatch, profile):
    p = parameters(tmp_path)
    if not p["OmpCommand"] or "AGENT_MESSAGING_TEST_OMP_PYTHON" not in os.environ:
        pytest.skip("requires real OMP public config API and dependency-bearing base Python")
    user = Path(p["UserRoot"])
    installer.private_directory(user)
    p.pop("OmpAgentRoot")
    p.pop("OmpProfile", None)
    for key in ("PI_CODING_AGENT_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "PI_CONFIG_DIR"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("OMP_PROFILE", profile)
    monkeypatch.setenv("PI_PROFILE", "must-not-win")
    prefix = ["--profile", profile] if profile else []
    env = installer.omp_environment(p)
    # Only a supported config command creates this owned synthetic profile. No session/model launch.
    installer.command([p["OmpCommand"], *prefix, "config", "set", "extensions", "[]", "--json"], env=env, cwd=user)
    observed = installer.command([p["OmpCommand"], *prefix, "config", "path"], env=env, cwd=user).stdout.strip()
    actual = installer.absolute(observed)
    assert installer.within(actual, user) and actual.is_dir()
    installer.discover_omp(p)
    assert p["OmpAgentRoot"] == str(actual)
    assert p["OmpProfile"] == profile
    assert not (actual / "mcp.json").exists()
    # A retained successful installation already owns both existing identity roots.
    for key in ("RelayStateRoot", "RelayRuntimeRoot"):
        installer.private_directory(Path(p[key]))
    monkeypatch.setenv("OMP_PROFILE", "different-profile-must-not-win")
    installer.discover_omp(p, retained=True)
    assert p["OmpAgentRoot"] == str(actual) and p["OmpProfile"] == profile


@pytest.mark.parametrize("source", ["legacy-code", "legacy-extension", "native-extension", "extension-config", "runtime-config", "namespace", "environment", "one-root", "retained"])
def test_prior_identity_evidence_never_initializes_missing_relay_roots(tmp_path, monkeypatch, source):
    p = parameters(tmp_path)
    for key in ("OMP_RELAY_STATE_ROOT", "OMP_RELAY_RUNTIME_ROOT"):
        monkeypatch.delenv(key, raising=False)
    root, agent, user = Path(p["InstallRoot"]), Path(p["OmpAgentRoot"]), Path(p["UserRoot"])
    if source in {"legacy-code", "legacy-extension"}:
        path = root / ("omp_relay_mcp.py" if source == "legacy-code" else "omp_relay_extension.ts")
        path.parent.mkdir(parents=True)
        path.write_text("synthetic prior relay evidence\n")
    elif source == "native-extension":
        path = agent / "extensions/omp_relay_extension.ts"
        path.parent.mkdir(parents=True)
        path.write_text("synthetic prior native relay extension\n")
    elif source == "extension-config":
        agent.mkdir(parents=True)
        (agent / "config.yml").write_bytes(installer.encode({"extensions": [str(root / "omp_relay_extension.ts")]}))
    elif source == "runtime-config":
        path = user / ".agents/skills-runtime/runtime.json"
        path.parent.mkdir(parents=True)
        path.write_bytes(installer.encode({"tokens": {"__OMP_PYTHON__": sys.executable}, "mcpServers": {"omp-relay": {"command": sys.executable}}}))
    elif source == "namespace":
        (user / ".omp-relay").mkdir(parents=True)
    elif source == "environment":
        monkeypatch.setenv("OMP_RELAY_STATE_ROOT", p["RelayStateRoot"])
    elif source == "one-root":
        Path(p["RelayStateRoot"]).mkdir(parents=True)
    before = {path: path.read_bytes() for path in user.rglob("*") if path.is_file()} if user.exists() else {}
    with pytest.raises(installer.Refusal, match="Prior OMP relay identity evidence.*refusing replacement identity"):
        installer.discover_omp_roots(p, registered=False, retained=source == "retained")
    assert not Path(p["RelayRuntimeRoot"]).exists()
    assert {path: path.read_bytes() for path in before} == before


def test_only_genuinely_fresh_relay_may_choose_absent_default_roots(tmp_path, monkeypatch):
    p = parameters(tmp_path)
    for key in ("OMP_RELAY_STATE_ROOT", "OMP_RELAY_RUNTIME_ROOT"):
        monkeypatch.delenv(key, raising=False)
    p.pop("RelayStateRoot")
    p.pop("RelayRuntimeRoot")
    runtime = Path(p["UserRoot"]) / ".agents/skills-runtime/runtime.json"
    runtime.parent.mkdir(parents=True)
    # A standalone discriminator's Python input alone is not prior relay evidence.
    runtime.write_bytes(installer.encode({"tokens": {"__OMP_PYTHON__": sys.executable}}))
    assert installer.discover_omp_roots(p, registered=False, retained=False) is False
    assert p["RelayStateRoot"] == str(Path(p["UserRoot"]) / ".omp-relay/state")
    assert p["RelayRuntimeRoot"] == str(Path(p["UserRoot"]) / ".omp-relay/runtime")
    assert not Path(p["RelayStateRoot"]).exists() and not Path(p["RelayRuntimeRoot"]).exists()


def test_existing_paired_roots_are_preserved_and_retained_roots_win_environment(tmp_path, monkeypatch):
    p = parameters(tmp_path)
    for key in ("RelayStateRoot", "RelayRuntimeRoot"):
        Path(p[key]).mkdir(parents=True)
    identity = Path(p["RelayStateRoot"]) / "identity.json"
    identity.write_bytes(b"synthetic existing identity")
    monkeypatch.setenv("OMP_RELAY_STATE_ROOT", str(tmp_path / "must-not-win-state"))
    monkeypatch.setenv("OMP_RELAY_RUNTIME_ROOT", str(tmp_path / "must-not-win-runtime"))
    before = dict(p)
    assert installer.discover_omp_roots(p, registered=True, retained=True) is True
    assert p == before and identity.read_bytes() == b"synthetic existing identity"
    assert not (tmp_path / "must-not-win-state").exists()
