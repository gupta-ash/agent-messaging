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
    with pytest.raises(installer.Refusal, match="MCP contribution drifted"):
        installer.verify_configuration(p, list(installer.configuration_snapshot(p)))
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
