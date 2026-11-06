"""Local agent-messaging package installer and retained, read-only verifier.

This module owns only declaration-projected code, its private receipt and the
specified MCP contributions. It never owns a database, token, log or registrar.
"""
from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from typing import Any

COMPONENT = "agent-messaging"
REPOSITORY = "gupta-ash/agent-messaging"
RECEIPT_KEYS = {"schema_version", "component", "repository", "version", "source_commit", "archive_sha256", "parameters", "files", "configuration_paths", "held_paths", "last_code_change_utc"}
RELEASE_KEYS = {"schema_version", "component", "repository", "version", "source_commit", "archive_sha256", "files"}
OPTION_NAMES = {"Version", "WorkRoot", "Mode", "UserRoot", "InstallRoot", "PythonExe", "Harness", "Port", "OmpCommand", "OmpAgentRoot", "OmpProfile", "OmpPythonExe", "RelayStateRoot", "RelayRuntimeRoot", "Activate", "BundleDir", "ReleaseReceipt"}
IDENTITIES = {"UserRoot", "InstallRoot", "PythonExe", "Port", "OmpCommand", "OmpAgentRoot", "OmpProfile", "OmpPythonExe", "RelayStateRoot", "RelayRuntimeRoot"}
STATIC = {"LICENSE", "install.ps1", "manifest.json", "VERSION.txt", ".gitignore", "deploy/release-files.json", "deploy/adoption.json", "deploy/retirements.json", "deploy/install-broker-stack.ps1", "deploy/install_runtime.py", "deploy/RUNBOOK.md"}
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
EXECUTABLE = {".py", ".ps1", ".bat", ".cmd", ".sh", ".ts", ".exe", ".dll", ""}

class Refusal(ValueError):
    """A named prerequisite, conflict or integrity refusal before mutation."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Refusal(message)


def closed(value: Any, keys: set[str], label: str) -> dict:
    require(isinstance(value, dict) and set(value) == keys, f"Invalid closed {label} fields")
    return value


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load(path: Path) -> Any:
    no_links(path)
    return json.loads(path.read_text(encoding="utf-8-sig"), object_pairs_hook=unique_object)


def unique_object(items: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in items:
        require(key not in result, "Duplicate JSON object key")
        result[key] = value
    return result


def encode(value: Any) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def absolute(value: str | Path) -> Path:
    raw = str(value)
    require(bool(raw) and Path(raw).is_absolute() and not raw.startswith(("\\\\", "//")), "Require an absolute local path")
    parts = Path(raw).parts
    require(not any(p in {".", ".."} or p.endswith((".", " ")) or re.match(r"(?i)^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)", p) for p in parts), "Unsafe path member")
    require(not any(c in raw for c in "*?\x00") and re.search(r"~[0-9]", raw) is None and (os.name != "nt" or ":" not in raw[2:]), "Unsafe path alias, wildcard or stream")
    path = Path(os.path.abspath(raw))
    require(path.parent != path, "A drive root is not an owned directory")
    return path


def within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def no_links(path: Path, *, leaf_file: bool = False) -> None:
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        require(not stat.S_ISLNK(info.st_mode) and not getattr(info, "st_file_attributes", 0) & 0x400, f"Link/reparse ancestry refused: {item}")
        if item == path and stat.S_ISREG(info.st_mode):
            require(info.st_nlink == 1, f"Hardlinked file refused: {item}")
        if item == path and leaf_file:
            require(stat.S_ISREG(info.st_mode), f"Expected a regular file: {item}")


def native_powershell() -> Path:
    return Path(os.environ.get("SystemRoot", "C:/Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"


def powershell_environment(executable: Path, inherited: dict | None = None) -> dict:
    # A PowerShell 7 caller may export incompatible modules before PS 5.1's own.
    # Scope discovery/cache changes to this child; Verify must not write a cache.
    child = dict(os.environ if inherited is None else inherited)
    for key in tuple(child):
        if key.casefold() in {"psmodulepath", "psmoduleanalysiscachepath"}:
            child.pop(key)
    child["PSModulePath"] = str(executable.parent / "Modules")
    child["PSModuleAnalysisCachePath"] = os.devnull
    return child


def command(argv: list[str], *, env: dict | None = None, cwd: Path | None = None, capture: bool = True) -> subprocess.CompletedProcess:
    executable = str(argv[0])
    if os.name == "nt" and Path(executable) == native_powershell():
        env = powershell_environment(Path(executable), env)
    if os.name == "nt" and Path(executable).suffix.lower() in {".cmd", ".bat"}:
        argv = [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/s", "/c", subprocess.list2cmdline(argv)]
    result = subprocess.run(argv, env=env, cwd=cwd, stdin=subprocess.DEVNULL, capture_output=capture, text=True, timeout=300, check=False)
    require(result.returncode == 0, f"Prerequisite/operation failed: {Path(executable).name} (exit {result.returncode}); no success receipt written")
    return result


def powershell(script: str, *args: str) -> Any:
    exe = native_powershell()
    import base64
    literals = ",".join("'" + value.replace("'", "''") + "'" for value in args)
    text = "$args=@(" + literals + ");" + script
    encoded = base64.b64encode(text.encode("utf-16le")).decode("ascii")
    result = command([str(exe), "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded])
    return json.loads(result.stdout.strip() or "null")


def account_profile() -> Path:
    require(os.name == "nt", "The scripted installer supports Windows PowerShell 5.1+ only")
    # SHGetKnownFolderPath is bound to the actual access token, not spoofable HOME.
    import ctypes.wintypes as wt
    class GUID(ctypes.Structure):
        _fields_ = [("Data1", wt.DWORD), ("Data2", wt.WORD), ("Data3", wt.WORD), ("Data4", ctypes.c_ubyte * 8)]
    guid = GUID.from_buffer_copy(uuid.UUID("5e6c858f-0e22-4760-9afe-ea3317b67173").bytes_le)
    value = ctypes.c_wchar_p()
    require(ctypes.windll.shell32.SHGetKnownFolderPath(ctypes.byref(guid), 0, None, ctypes.byref(value)) == 0, "Cannot bind actual invoking profile")
    try:
        return absolute(value.value)
    finally:
        ctypes.windll.ole32.CoTaskMemFree(value)


def owned(path: Path) -> None:
    if not path.exists():
        return
    no_links(path)
    if os.name == "nt":
        result = powershell("$p=$args[0];$s=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value;$a=Get-Acl -LiteralPath $p;$ok=$a.GetOwner([Security.Principal.SecurityIdentifier]).Value -eq $s;foreach($r in $a.Access){if($r.AccessControlType -eq 'Allow' -and ($r.FileSystemRights -band [Security.AccessControl.FileSystemRights]::Write) -and $r.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -notin @($s,'S-1-5-18','S-1-5-32-544')){$ok=$false}};ConvertTo-Json -Compress $ok", str(path))
        require(result is True, f"Path must be same-user owned without foreign write access: {path}")
    else:
        require(path.stat().st_uid == os.getuid() and path.stat().st_mode & 0o022 == 0, f"Private owned path required: {path}")


def private_directory(path: Path) -> None:
    no_links(path)
    if path.exists():
        require(path.is_dir(), f"Private directory collides with a file: {path}")
        owned(path)
        return
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "nt":
        powershell("$p=$args[0];$s=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value;$a=New-Object Security.AccessControl.DirectorySecurity;$a.SetOwner((New-Object Security.Principal.SecurityIdentifier($s)));$a.SetAccessRuleProtection($true,$false);foreach($id in @($s,'S-1-5-18','S-1-5-32-544')){$a.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier($id)),'FullControl','ContainerInherit,ObjectInherit','None','Allow')))};Set-Acl -LiteralPath $p -AclObject $a;ConvertTo-Json $true", str(path))
    else:
        path.chmod(0o700)


def atomic(path: Path, data: bytes, *, private: bool = False) -> None:
    no_links(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    name = path.parent / ("." + path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with name.open("xb") as stream:
            if os.name != "nt":
                os.chmod(name, 0o600 if private else 0o644)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if private and os.name == "nt":
            powershell("$p=$args[0];$s=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value;$a=New-Object Security.AccessControl.FileSecurity;$a.SetOwner((New-Object Security.Principal.SecurityIdentifier($s)));$a.SetAccessRuleProtection($true,$false);foreach($id in @($s,'S-1-5-18','S-1-5-32-544')){$a.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier($id)),'FullControl','Allow')))};Set-Acl -LiteralPath $p -AclObject $a;ConvertTo-Json $true", str(name))
        no_links(path)
        if os.name == "nt" and path.exists():
            replace = ctypes.windll.kernel32.ReplaceFileW
            replace.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_void_p]
            replace.restype = ctypes.c_int
            require(bool(replace(str(path), str(name), None, 0, None, None)), f"Atomic metadata-preserving replacement failed: {path}")
        else:
            os.replace(name, path)
    finally:
        name.unlink(missing_ok=True)


def member(value: str) -> str:
    require(isinstance(value, str) and bool(re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", value)), "Invalid normalized package path")
    require(all(p not in {".", ".."} and not p.endswith((".", " ")) and not re.match(r"(?i)^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)", p) for p in PurePosixPath(value).parts), "Unsafe normalized package path")
    return value


def validate_release(bundle: Path, receipt: dict, version: str) -> dict[str, dict]:
    closed(receipt, RELEASE_KEYS, "release receipt")
    require(receipt["schema_version"] == 1 and receipt["component"] == COMPONENT and receipt["repository"] == REPOSITORY and receipt["version"] == version and HEX40.fullmatch(receipt["source_commit"]) is not None and HEX64.fullmatch(receipt["archive_sha256"]) is not None, "Release identity mismatch")
    rows = {}
    seen = set()
    for row in receipt["files"]:
        closed(row, {"path", "sha256", "mode"}, "package file")
        name = member(row["path"])
        require(name.casefold() not in seen and HEX64.fullmatch(row["sha256"]) is not None and row["mode"] in {"100644", "100755"}, "Duplicate/invalid package row")
        seen.add(name.casefold())
        rows[name] = row
    declaration = load(bundle / "deploy/release-files.json")
    closed(declaration, {"schema_version", "entries"}, "release declaration")
    require(declaration["schema_version"] == 1, "Unsupported release declaration")
    expected = {"manifest.json", "VERSION.txt", ".gitignore"}
    for entry in declaration["entries"]:
        closed(entry, {"source", "destination"}, "export entry")
        member(entry["source"])
        name = member(entry["destination"])
        require(name not in expected, "Duplicate declared destination")
        expected.add(name)
    require(set(rows) == expected and STATIC <= expected, "Archive membership differs from committed declaration")
    actual = set()
    for path in bundle.rglob("*"):
        no_links(path)
        if path.is_file():
            name = path.relative_to(bundle).as_posix()
            require(name in rows and sha(path) == rows[name]["sha256"], f"Unexpected/changed package file: {name}")
            actual.add(name)
    require(actual == expected, "Package payload is incomplete")
    manifest = load(bundle / "manifest.json")
    require(manifest.get("packagedCommit") == receipt["source_commit"] and manifest.get("bundleVersion") == version, "Package manifest source/tag mismatch")
    tree = "".join(name.lower() + ":" + rows[name]["sha256"] + "\r\n" for name in sorted(rows, key=str.lower) if name not in {"manifest.json", "VERSION.txt"})
    require(manifest.get("treeSha256") == digest(tree.encode("utf-8")) and manifest.get("releaseDeclarationSha256") == rows["deploy/release-files.json"]["sha256"], "Package tree/declaration binding mismatch")
    return rows


def canonical_home(user: Path, relative: str) -> Path:
    path = user / relative
    # Preserve only the two recognized same-user parent junctions, never replace them.
    for native, canonical in ((".claude/skills", ".agents/skills"), (".claude/hooks", ".agents/hooks")):
        parent = user / native
        if path == parent or within(path, parent):
            try:
                info = parent.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                target = user / canonical
                require(parent.resolve() == target.absolute(), f"Foreign Claude parent junction: {parent}")
                no_links(target)
                owned(target)
                path = target / path.relative_to(parent)
    no_links(path)
    return path


def selected_destination(name: str, p: dict) -> Path | None:
    user, root = Path(p["UserRoot"]), Path(p["InstallRoot"])
    harness = set(p["Harness"])
    if name == "LICENSE":
        return root / "LICENSE"
    if name == "deploy/RUNBOOK.md":
        return root / "RUNBOOK.md"
    if name.startswith("broker/"):
        rel = name.removeprefix("broker/")
        if rel == "requirements-mcp.txt":
            return root / rel if harness & {"claude", "codex"} else None
        if rel == "requirements-omp-mcp.txt":
            return root / rel if "omp" in harness else None
        if rel in {"codex_adapter.py", "codex_relay_mcp.py"} or rel in {"scripts/codex-relay.ps1", "scripts/start-codex-relay-session.ps1"}:
            return root / rel if "codex" in harness else None
        if rel in {"omp_relay_extension.ts", "omp_relay_mcp.py"}:
            return root / rel if "omp" in harness else None
        return root / rel
    if name.startswith("home/.agents/skills/"):
        return canonical_home(user, name.removeprefix("home/")) if harness - {"none"} else None
    if name.startswith(("home/.claude/plugins/", "home/.claude/hooks/")):
        return canonical_home(user, name.removeprefix("home/")) if "claude" in harness else None
    return None


def resolve_parameters(options: dict, prior: dict | None) -> dict:
    require(set(options) <= OPTION_NAMES, "Unknown installer option")
    require(options.get("Mode", "Install") in {"Check", "Install", "Verify"}, "Unknown operation")
    version = options.get("Version")
    require(isinstance(version, str) and bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", version)) and version not in {"latest", "HEAD", "main", "master"}, "Frontdoor must resolve an exact release tag")
    p = dict(prior["parameters"]) if prior else {}
    for key in IDENTITIES:
        if key in options:
            value = options[key]
            if key not in {"Port", "OmpProfile"}:
                value = str(absolute(value))
            if prior and key in p:
                same = value == p[key] if key in {"Port", "OmpProfile"} else Path(value) == Path(p[key])
                require(same, f"Root/identity relocation or conflicting {key} refused")
                continue
            p[key] = value
    p.setdefault("InstallRoot", str(Path(p["UserRoot"]) / ".agents/services/broker"))
    if "PythonExe" not in p:
        p["PythonExe"] = discover_python(Path(p["InstallRoot"]))
    envfile = Path(p["InstallRoot"]) / "broker.env"
    existing_env = read_env(envfile) if envfile.exists() else {}
    if "BROKER_PORT" in existing_env:
        port = int(existing_env["BROKER_PORT"])
        if "Port" in p:
            require(p["Port"] == port, "Selected port conflicts with existing private broker.env")
        p.setdefault("Port", port)
    p.setdefault("Port", 8787)
    require(type(p["Port"]) is int and 1 <= p["Port"] <= 65535, "Invalid Port")
    require(existing_env.get("BROKER_HOST", "127.0.0.1") == "127.0.0.1", "Existing broker must bind loopback before adoption")
    raw = options.get("Harness")
    if raw is None and not prior:
        raw = [name for name in ("claude", "codex") if shutil.which(name)]
        if options.get("OmpCommand") or os.environ.get("OMP_COMMAND") or shutil.which("omp"):
            raw.append("omp")
        raw = raw or ["none"]
    if isinstance(raw, str):
        raw = [raw]
    require(raw is None or bool(raw) and set(raw) <= {"none", "claude", "codex", "omp"} and not ("none" in raw and len(raw) != 1), "Invalid Harness selection")
    previous = set(p.get("Harness", ["none"])) - {"none"}
    requested = set(raw or []) - {"none"}
    if options.get("Mode") == "Verify" and raw is not None:
        require(requested == previous, "Verify Harness conflicts with retained selection")
    p["Harness"] = sorted(previous | requested) or ["none"]
    return p


def discover_command(value: str | None, environment: str, name: str) -> str:
    selected = value or os.environ.get(environment) or shutil.which(name)
    require(bool(selected), f"Missing {name} prerequisite: {environment} or PATH must select an existing executable")
    path = absolute(selected)
    no_links(path, leaf_file=True)
    require(path.is_file(), f"Missing/unsafe {name} executable: {path}")
    return str(path)


def discover_python(root: Path) -> str:
    configuration = root / ".venv/pyvenv.cfg"
    if configuration.exists():
        fields = {}
        no_links(configuration, leaf_file=True)
        for line in configuration.read_text(encoding="utf-8-sig").splitlines():
            if "=" in line:
                key, value = (item.strip() for item in line.split("=", 1))
                require(key not in fields, "Duplicate legacy venv configuration field")
                fields[key] = value
        require(fields.get("version", "").startswith("3.12.") and fields.get("include-system-site-packages") == "false" and fields.get("home"), "Existing broker venv must declare its base Python 3.12 without global site packages")
        home = absolute(fields["home"])
        no_links(home)
        require(home.is_dir(), "Legacy venv home must name an existing base Python directory")
        base = home / "python.exe"
        no_links(base, leaf_file=True)
        require(base.is_file(), "Legacy venv home is missing its declared base python.exe; no alternate interpreter is guessed")
        if "executable" in fields:
            recorded = absolute(fields["executable"])
            no_links(recorded, leaf_file=True)
            require(recorded == base, "Legacy venv executable metadata conflicts with home/python.exe; venv/alias takeover refused")
        return str(base)
    return discover_command(None, "PYTHON_EXE", "python")


def omp_environment(p: dict) -> dict:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith(("ANTHROPIC_", "OPENAI_", "COPILOT_", "GITHUB_", "GH_", "BROKER_", "DISCORD_")) or key in {"PI_CONFIG_FILES", "NODE_OPTIONS", "BUN_OPTIONS", "CLAUDE_CONFIG_DIR"}:
            env.pop(key)
    # Bind public directory discovery to the actual token profile, not inherited HOME.
    env["HOME"] = env["USERPROFILE"] = p["UserRoot"]
    if "OmpAgentRoot" in p:
        env["PI_CODING_AGENT_DIR"] = p["OmpAgentRoot"]
    if "OmpProfile" in p:
        env["OMP_PROFILE"] = env["PI_PROFILE"] = p["OmpProfile"]
    return env


def omp_entry(p: dict, entry: Any) -> dict:
    require(isinstance(entry, dict) and set(entry) <= {"command", "args", "type", "env", "envPolicy", "envLiteralKeys"} and entry.get("type", "stdio") == "stdio", "Foreign/conflicting omp-relay registration")
    python = str(absolute(entry.get("command", "")))
    args = entry.get("args")
    require(isinstance(args, list) and args and all(isinstance(item, str) for item in args) and Path(args[0]) == Path(p["InstallRoot"]) / "omp_relay_mcp.py", "Foreign omp-relay entrypoint/root; no takeover allowed")
    fields = {}
    require(len(args) % 2 == 1, "Invalid omp-relay argument pairs")
    for index in range(1, len(args), 2):
        key, value = args[index:index + 2]
        require(key in {"--state-root", "--runtime-root", "--broker-url"} and key not in fields, "Unknown/duplicate omp-relay coordinate")
        fields[key] = value
    env = entry.get("env", {})
    require(isinstance(env, dict) and set(env) <= {"OMP_RELAY_STATE_ROOT", "OMP_RELAY_RUNTIME_ROOT", "BROKER_BASE_URL"} and all(isinstance(value, str) for value in env.values()), "Only bound nonsecret relay environment inputs may be adopted")
    require(entry.get("envPolicy", "literal") == "literal" and ("envLiteralKeys" not in entry or isinstance(entry["envLiteralKeys"], list) and set(entry["envLiteralKeys"]) == set(env)), "Dynamic omp-relay environment inputs cannot be adopted")
    resolved = {"OmpPythonExe": python}
    for key, flag, variable in (("RelayStateRoot", "--state-root", "OMP_RELAY_STATE_ROOT"), ("RelayRuntimeRoot", "--runtime-root", "OMP_RELAY_RUNTIME_ROOT")):
        value = fields.get(flag, env.get(variable))
        require(bool(value), f"Existing omp-relay registration is missing {key}")
        path = absolute(value)
        require(variable not in env or Path(env[variable]) == path, f"Conflicting omp-relay {key}")
        no_links(path)
        require(path.is_dir() and within(path, Path(p["UserRoot"])), f"Existing {key} must be an existing private same-user directory; no replacement identity")
        resolved[key] = str(path)
    url = f"http://127.0.0.1:{p['Port']}"
    require(fields.get("--broker-url", env.get("BROKER_BASE_URL", "http://127.0.0.1:8787")).rstrip("/") == url and ("BROKER_BASE_URL" not in env or env["BROKER_BASE_URL"].rstrip("/") == url), "Existing OMP broker URL conflicts with selected port")
    for key, value in resolved.items():
        require(key not in p or Path(p[key]) == Path(value), f"Conflicting retained/selected {key}; no identity relocation")
    return resolved


def discover_omp(p: dict, *, retained: bool = False) -> bool:
    p["OmpCommand"] = discover_command(p.get("OmpCommand"), "OMP_COMMAND", "omp")
    if "OmpProfile" not in p:
        profile = "" if retained else os.environ.get("OMP_PROFILE", os.environ.get("PI_PROFILE", "")).strip()
        p["OmpProfile"] = "" if profile == "default" else profile
    profile = p["OmpProfile"]
    require(isinstance(profile, str) and (not profile or re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", profile) and not profile.endswith(".") and not re.match(r"(?i)^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\.|$)", profile)), "Invalid OmpProfile")
    if "OmpAgentRoot" not in p:
        prefix = ["--profile", profile] if profile else []
        result = command([p["OmpCommand"], *prefix, "config", "path"], env=omp_environment(p), cwd=Path(p["UserRoot"]))
        agent = absolute(result.stdout.strip())
        require(within(agent, Path(p["UserRoot"])), "OMP config path returned a foreign account root")
        no_links(agent)
        require(agent.is_dir(), "OMP config path must select an existing installed agent directory")
        owned(agent)
        p["OmpAgentRoot"] = str(agent)
    agent = omp_agent(p)
    require(within(agent, Path(p["UserRoot"])), "OmpAgentRoot must belong to the invoking profile")
    no_links(agent)
    require(agent.is_dir(), "OMP selection requires an existing actual agent directory")
    owned(agent)
    path = omp_agent(p) / "mcp.json"
    data = load(path) if path.exists() else {}
    require(isinstance(data, dict) and isinstance(data.get("mcpServers", {}), dict), "Invalid existing OMP MCP configuration")
    entry = data.get("mcpServers", {}).get("omp-relay")
    if entry is not None:
        p.update(omp_entry(p, entry))
    retained_entry = integration_inputs(p).get("omp")
    if retained_entry is not None:
        require(entry is None or entry == retained_entry, "Retained/live omp-relay registration conflict; no identity reconciliation")
        p.update(omp_entry(p, retained_entry))
    if "OmpPythonExe" not in p:
        candidates = []
        for namespace in ("skills-runtime", "agent-skills"):
            path = Path(p["UserRoot"]) / ".agents" / namespace / "runtime.json"
            if path.exists():
                data = load(path)
                require(isinstance(data, dict) and isinstance(data.get("tokens", {}), dict), "Invalid local OMP runtime configuration")
                candidate = data.get("tokens", {}).get("__OMP_PYTHON__")
                if candidate:
                    candidates.append(str(absolute(candidate)))
        require(len({os.path.normcase(item) for item in candidates}) <= 1, "Conflicting local OMP base-Python inputs")
        p["OmpPythonExe"] = discover_command(candidates[0] if candidates else os.environ.get("OMP_PYTHON_EXE") or p["PythonExe"], "OMP_PYTHON_EXE", "python")
    return discover_omp_roots(p, registered=entry is not None or retained_entry is not None, retained=retained)


def discover_omp_roots(p: dict, *, registered: bool, retained: bool) -> bool:
    user, root, agent = Path(p["UserRoot"]), Path(p["InstallRoot"]), omp_agent(p)
    evidence = []
    if registered or retained and any(key in p for key in ("RelayStateRoot", "RelayRuntimeRoot")):
        evidence.append("retained/current relay identity")
    for path in (*(root / name for name in ("omp_relay_mcp.py", "omp_relay_extension.ts", "requirements-omp-mcp.txt")), agent / "extensions/omp_relay_extension.ts"):
        no_links(path)
        if path.exists():
            evidence.append(str(path))
    namespace = user / ".omp-relay"
    no_links(namespace)
    if namespace.exists():
        require(namespace.is_dir(), "Existing relay identity namespace must be a directory")
        evidence.append(str(namespace))
    configuration = yaml_document(agent / "config.yml", p["OmpPythonExe"])
    extensions = configuration.get("extensions", [])
    require(isinstance(extensions, list) and all(isinstance(item, str) for item in extensions), "Invalid OMP extensions setting")
    servers = configuration.get("mcpServers", {})
    require(isinstance(servers, dict), "Invalid OMP MCP configuration setting")
    if any(Path(item).name == "omp_relay_extension.ts" for item in extensions) or "omp-relay" in servers:
        evidence.append(str(agent / "config.yml"))
    # Runtime declarations are Python inputs, not authority to guess identities.
    # Recognize finite relay markers; unknown/missing coordinates remain a refusal.
    def relay_marker(value: Any) -> bool:
        if isinstance(value, dict):
            return any(key in {"omp-relay", "OMP_RELAY_STATE_ROOT", "OMP_RELAY_RUNTIME_ROOT", "RelayStateRoot", "RelayRuntimeRoot"} or relay_marker(item) for key, item in value.items())
        if isinstance(value, list):
            return any(relay_marker(item) for item in value)
        return isinstance(value, str) and (value == "omp-relay" or Path(value).name in {"omp_relay_extension.ts", "omp_relay_mcp.py"})
    for distribution in ("skills-runtime", "agent-skills"):
        path = user / ".agents" / distribution / "runtime.json"
        if path.exists() and relay_marker(load(path)):
            evidence.append(str(path))
    for key, variable, name in (("RelayStateRoot", "OMP_RELAY_STATE_ROOT", "state"), ("RelayRuntimeRoot", "OMP_RELAY_RUNTIME_ROOT", "runtime")):
        if variable in os.environ and not (retained and key in p):
            supplied = absolute(os.environ[variable])
            require(key not in p or Path(p[key]) == supplied, f"Environment {variable} conflicts with selected/retained identity")
            p.setdefault(key, str(supplied))
            evidence.append(variable)
        p.setdefault(key, str(namespace / name))
        path = absolute(p[key])
        no_links(path)
        if path.exists():
            require(path.is_dir(), f"Existing {key} must be a directory")
            evidence.append(str(path))
    if evidence:
        require(all(Path(p[key]).is_dir() for key in ("RelayStateRoot", "RelayRuntimeRoot")), "Prior OMP relay identity evidence requires both observed existing state/runtime roots; refusing replacement identity: " + ", ".join(evidence))
    return bool(evidence)


def read_env(path: Path) -> dict[str, str]:
    no_links(path, leaf_file=True)
    result = {}
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Z_][A-Z0-9_]*)[ \t]*=[ \t]*(.*)", line)
        port_like = "BROKER_PORT" in line.upper()
        require(match is not None or not port_like, "Malformed BROKER_PORT declaration in private broker.env")
        if match is None:
            continue
        value = match[2].strip()
        if value[:1] in {"\"", "'"}:
            require(len(value) >= 2 and value[-1] == value[0], "Mismatched quotes in private broker.env")
            value = value[1:-1].strip()
        else:
            require(not value.endswith(("\"", "'")), "Mismatched quotes in private broker.env")
        if match[1] == "BROKER_PORT":
            require(re.fullmatch(r"[1-9][0-9]{0,4}", value) is not None and int(value) <= 65535, "Invalid BROKER_PORT in private broker.env")
        result[match[1]] = value
    return result


def validate_roots(p: dict, work: Path, *, actual_profile: Path | None = None) -> None:
    user, root = Path(p["UserRoot"]), Path(p["InstallRoot"])
    require(user == (actual_profile or account_profile()), "UserRoot differs from actual invoking profile/account")
    require(not any(part.casefold() in {"onedrive", "dropbox", "google drive"} for part in root.parts), "Sync-folder service root refused")
    for item in (user, root, work):
        no_links(item)
    require(not any((parent / ".git").exists() for parent in (root, *root.parents)), "Cannot install into a source checkout")
    source_roots = [parent for parent in Path(__file__).parents if (parent / ".git").exists()]
    config_roots = [Path(p["OmpAgentRoot"])] if "OmpAgentRoot" in p else []
    for protected in (root, *source_roots, *config_roots, *(user / name for name in (".agents", ".claude", ".codex", ".omp"))):
        require(not within(work, protected) and not within(protected, work), "WorkRoot overlaps source/install/state roots")
    require(not any(part.casefold() in {"ash_src", ".agents", ".claude", ".codex", ".omp"} for part in work.parts), "WorkRoot is inside source or installation tree")
    for key in ("RelayStateRoot", "RelayRuntimeRoot"):
        if key in p:
            state = Path(p[key])
            no_links(state)
            require(within(state, user) and not within(state, root) and not within(root, state) and not within(state, work) and not within(work, state), f"{key} must be a distinct private same-user state directory")
    if "RelayStateRoot" in p and "RelayRuntimeRoot" in p:
        a, b = Path(p["RelayStateRoot"]), Path(p["RelayRuntimeRoot"])
        require(not within(a, b) and not within(b, a), "Relay state/runtime roots overlap")
    for key in ("InstallRoot", "RelayStateRoot", "RelayRuntimeRoot", "OmpAgentRoot"):
        if key in p:
            owned(Path(p[key]))
    owned(work)


def owned_destination(path: Path) -> None:
    no_links(path)
    cursor = path
    while not cursor.exists():
        require(cursor.parent != cursor, "No existing owned destination ancestor")
        cursor = cursor.parent
    owned(cursor)


def validate_installed(receipt: dict, user: Path) -> None:
    closed(receipt, RECEIPT_KEYS, "installed receipt")
    require(receipt["schema_version"] == 1 and receipt["component"] == COMPONENT and receipt["repository"] == REPOSITORY and HEX40.fullmatch(receipt["source_commit"]) is not None and HEX64.fullmatch(receipt["archive_sha256"]) is not None, "Invalid retained identity")
    require(isinstance(receipt["parameters"], dict) and receipt["parameters"].get("UserRoot") == str(user) and set(receipt["parameters"]) <= IDENTITIES | {"Harness"}, "Invalid retained parameters")
    require(isinstance(receipt["configuration_paths"], list) and isinstance(receipt["held_paths"], list), "Invalid retained populations")
    require(not receipt["held_paths"], "Messaging cannot claim held paths as installed")
    root = absolute(receipt["parameters"]["InstallRoot"])
    meta = user / ".agents/installations/agent-messaging"
    seen = set()
    for row in receipt["files"]:
        closed(row, {"path", "sha256", "mode"}, "installed file")
        path = absolute(row["path"])
        require(str(path).casefold() not in seen and row["mode"] in {"100644", "100755"} and HEX64.fullmatch(row["sha256"]) is not None, "Invalid/duplicate installed row")
        seen.add(str(path).casefold())
        allowed = within(path, root) or within(path, meta)
        if not allowed:
            for selected in (user / ".agents/skills" / name for name in ("relay-check", "relay-name", "relay-send")):
                allowed = allowed or within(path, selected)
            allowed = allowed or within(path, canonical_home(user, ".claude/hooks/relay")) or within(path, user / ".claude/plugins/local-relay-channel")
            allowed = allowed or any(within(path, user / ".claude/skills" / name) for name in ("relay-check", "relay-name", "relay-send"))
        require(allowed, "Receipt file outside declared product roots")
        require(not within(path, root) or not any(part.lower() in {"logs", "audit", "sessions"} for part in path.relative_to(root).parts) and path.name != "broker.env" and not re.search(r"(?i)\.(?:db|sqlite|sqlite3)(?:-|$)", path.name), "Runtime state cannot be claimed as immutable code")
    require(receipt["last_code_change_utc"] is None or isinstance(receipt["last_code_change_utc"], str) and receipt["last_code_change_utc"].endswith("Z"), "Invalid code-change timestamp")
    if receipt["last_code_change_utc"] is not None:
        dt.datetime.fromisoformat(receipt["last_code_change_utc"].replace("Z", "+00:00"))


def verify_files(receipt: dict) -> None:
    for row in receipt["files"]:
        path = Path(row["path"])
        no_links(path, leaf_file=True)
        require(path.is_file() and sha(path) == row["sha256"], f"Owned-file drift/conflict: {path}")


def verify_retained(receipt: dict) -> None:
    p = receipt["parameters"]
    user = Path(p["UserRoot"])
    closure = user / ".agents/installations/agent-messaging/verifier"
    release = load(closure / "release.json")
    closed(release, RELEASE_KEYS, "retained release")
    require(all(release[key] == receipt[key] for key in ("schema_version", "component", "repository", "version", "source_commit", "archive_sha256")), "Retained release binding mismatch")
    declaration = load(closure / "deploy/release-files.json")
    closed(declaration, {"schema_version", "entries"}, "retained declaration")
    members = {member(row["path"]): row for row in release["files"]}
    declared = {member(row["destination"]) for row in declaration["entries"]} | {"manifest.json", "VERSION.txt", ".gitignore"}
    require(len(members) == len(release["files"]) and set(members) == declared and STATIC <= declared, "Retained package membership mismatch")
    expected = {}
    for name, row in members.items():
        closed(row, {"path", "sha256", "mode"}, "retained member")
        require(HEX64.fullmatch(row["sha256"]) is not None and row["mode"] in {"100644", "100755"}, "Invalid retained file binding")
        target = selected_destination(name, p)
        if target is not None:
            expected[str(target)] = (row["sha256"], row["mode"])
        if name in STATIC:
            expected[str(closure / name)] = (row["sha256"], row["mode"])
        if "claude" in p["Harness"] and name.startswith("home/.agents/skills/") and name.endswith("/SKILL.md"):
            native = canonical_home(user, ".claude/skills/" + name.removeprefix("home/.agents/skills/"))
            expected[str(native)] = (row["sha256"], row["mode"])
    expected[str(closure / "release.json")] = (digest(encode(release)), "100644")
    expected[str(closure.parent / "connection.json")] = (digest(encode({"schema_version": 1, "broker_url": f"http://127.0.0.1:{p['Port']}"})), "100644")
    integrations_path = closure.parent / "integrations.json"
    if "claude" in p["Harness"] or any(row["path"] == str(integrations_path) for row in receipt["files"]):
        inputs = integration_inputs(p)
        require(set(inputs) - {"schema_version"} <= set(p["Harness"]) and ("claude" not in p["Harness"] or "claude" in inputs), "Retained registration input population conflicts with selection")
        projected = {"schema_version": 1}
        if "claude" in inputs:
            projected["claude"] = claude_contribution(p)
        if "omp" in inputs:
            projected["omp"] = omp_contribution(p)
        expected[str(integrations_path)] = (digest(encode(projected)), "100644")
    actual = {row["path"]: (row["sha256"], row["mode"]) for row in receipt["files"]}
    require(all(actual.get(path) == binding for path, binding in expected.items()), "Installed immutable population differs from retained source projection")
    require(all(path in expected or within(Path(path), Path(p["InstallRoot"]) / ".venv") for path in actual), "Installed receipt claims undeclared files")
    require(all(str(Path(p["InstallRoot"]) / ".venv/Scripts" / name) in actual for name in ("python.exe", "agent-messaging.exe")), "Retained runtime/console environment is incomplete")
    manifest = load(closure / "manifest.json")
    require(manifest.get("packagedCommit") == release["source_commit"] and manifest.get("bundleVersion") == release["version"], "Retained manifest source/tag mismatch")


def interpreter(path: str, *, omp: bool = False) -> dict:
    exe = absolute(path)
    no_links(exe, leaf_file=True)
    require(exe.is_file(), "PythonExe must select an existing base Python 3.12 interpreter")
    code = "import json,sys,venv,ensurepip;print(json.dumps({'version':list(sys.version_info[:2]),'base':sys.prefix==sys.base_prefix,'executable':sys.executable,'prefix':sys.prefix}))"
    data = json.loads(command([str(exe), "-I", "-B", "-c", code]).stdout)
    require(data["version"] == [3, 12] and data["base"], "An existing base Python 3.12 interpreter is required (not a venv)")
    require(absolute(data["executable"]) == exe and absolute(data["prefix"]) == exe.parent, "Selected base Python executable/prefix differs from its declared directory; aliases are refused")
    if omp:
        command([str(exe), "-I", "-B", "-c", "import mcp,httpx,cryptography,yaml"])
    return data


def yaml_document(path: Path, python: str) -> dict:
    if not path.exists():
        return {}
    no_links(path, leaf_file=True)
    code = "import json,sys,yaml;v=yaml.safe_load(open(sys.argv[1],encoding='utf-8-sig'));assert v is None or isinstance(v,dict);print(json.dumps(v or {}))"
    return json.loads(command([python, "-I", "-B", "-c", code, str(path)]).stdout)


def claude_contribution(p: dict) -> dict:
    expected = {"command": str(Path(p["InstallRoot"]) / ".venv/Scripts/python.exe"), "args": [str(Path(p["UserRoot"]) / ".claude/plugins/local-relay-channel/server.py")]}
    value = integration_inputs(p)
    if "claude" in value:
        selected = closed(value["claude"], {"command", "args"}, "Claude registration inputs")
        require(selected["command"] in {expected["command"], p["PythonExe"]} and selected["args"] == expected["args"], "Local Claude interpreter/root input conflict")
        return selected
    target = Path(p["UserRoot"]) / ".claude.json"
    if target.exists():
        current = load(target).get("mcpServers", {}).get("local-relay-channel")
        if isinstance(current, dict) and current.get("command") == p["PythonExe"] and current.get("args") == expected["args"]:
            expected["command"] = p["PythonExe"]
    return expected


def claude_registration(p: dict) -> tuple[Path, bool]:
    expected_url = f"http://127.0.0.1:{p['Port']}"
    supplied = os.environ.get("BROKER_BASE_URL")
    require(not supplied or supplied.rstrip("/") == expected_url, "Inherited BROKER_BASE_URL conflicts with selected private broker coordinates")
    settings_path = Path(p["UserRoot"]) / ".claude/settings.json"
    if settings_path.exists():
        settings = load(settings_path)
        configured = settings.get("env", {}).get("BROKER_BASE_URL")
        require(configured is None or isinstance(configured, str) and configured.rstrip("/") == expected_url, "Provisioned Claude broker URL conflicts with selected port; no implicit change")
    target = Path(p["UserRoot"]) / ".claude.json"
    data = load(target) if target.exists() else {}
    require(isinstance(data, dict) and isinstance(data.get("mcpServers", {}), dict), "Invalid existing Claude MCP configuration")
    entry = data.get("mcpServers", {}).get("local-relay-channel")
    if entry is None:
        return target, False
    expected = claude_contribution(p)
    env = entry.get("env", {}) if isinstance(entry, dict) else None
    require(isinstance(entry, dict) and set(entry) <= {"command", "args", "type", "env"} and entry.get("command") == expected["command"] and entry.get("args") == expected["args"] and entry.get("type", "stdio") == "stdio" and isinstance(env, dict) and all(isinstance(value, str) for value in env.values()), "Foreign/conflicting local-relay-channel registration; no takeover allowed")
    require("BROKER_BASE_URL" not in env or env["BROKER_BASE_URL"].rstrip("/") == expected_url, "Registered Claude broker URL conflicts with selected port")
    return target, True


def omp_contribution(p: dict) -> dict:
    retained = integration_inputs(p).get("omp")
    if retained is not None:
        omp_entry(p, retained)
        return retained
    target = omp_agent(p) / "mcp.json"
    if target.exists():
        existing = load(target).get("mcpServers", {}).get("omp-relay")
        if existing is not None:
            omp_entry(p, existing)
            return existing
    return {"command": p["OmpPythonExe"], "args": [str(Path(p["InstallRoot"]) / "omp_relay_mcp.py"), "--state-root", p["RelayStateRoot"], "--runtime-root", p["RelayRuntimeRoot"], "--broker-url", f"http://127.0.0.1:{p['Port']}"], "type": "stdio"}


def integration_inputs(p: dict) -> dict:
    path = Path(p["UserRoot"]) / ".agents/installations/agent-messaging/integrations.json"
    value = load(path) if path.exists() else {"schema_version": 1}
    require(isinstance(value, dict) and set(value) <= {"schema_version", "claude", "omp"} and value.get("schema_version") == 1, "Invalid local registration inputs")
    return value


def integration_contribution(p: dict) -> dict:
    value = {"schema_version": 1}
    if "claude" in p["Harness"]:
        value["claude"] = claude_contribution(p)
    if "omp" in p["Harness"]:
        value["omp"] = omp_contribution(p)
    return value


def omp_agent(p: dict) -> Path:
    root = Path(p["OmpAgentRoot"])
    if p.get("OmpProfile"):
        # OmpAgentRoot is the selected actual agent directory, not an inferred profile.
        require(bool(re.fullmatch(r"[A-Za-z0-9_.-]+", p["OmpProfile"])), "Invalid OmpProfile")
    return root


def configuration_snapshot(p: dict) -> dict[str, str | None]:
    paths = [Path(p["InstallRoot"]) / "broker.env"]
    if "claude" in p["Harness"]:
        paths.append(Path(p["UserRoot"]) / ".claude.json")
    if "omp" in p["Harness"]:
        paths.extend(omp_agent(p) / name for name in ("config.yml", "mcp.json"))
    result = {}
    for path in paths:
        no_links(path)
        result[str(path)] = sha(path) if path.exists() else None
    return result


def verify_configuration(p: dict, expected_paths: list[str]) -> None:
    root = Path(p["InstallRoot"])
    env = read_env(root / "broker.env")
    require(env.get("BROKER_HOST") == "127.0.0.1" and env.get("BROKER_PORT") == str(p["Port"]), "Private broker.env owned bind fields drifted")
    require(set(expected_paths) == set(configuration_snapshot(p)), "Retained configuration population differs from selected projection")
    if "claude" in p["Harness"]:
        require(claude_registration(p)[1], "Owned Claude registration is absent")
    if "omp" in p["Harness"]:
        config = yaml_document(omp_agent(p) / "config.yml", p["OmpPythonExe"])
        extensions = config.get("extensions", [])
        require(isinstance(extensions, list) and extensions.count(str(root / "omp_relay_extension.ts")) == 1, "OMP extension contribution drifted")
        data = load(omp_agent(p) / "mcp.json")
        require(data.get("mcpServers", {}).get("omp-relay") == omp_contribution(p), "OMP MCP contribution drifted")
        require("omp-relay" not in data.get("disabledServers", []), "Selected OMP relay is explicitly disabled")
        require("enabledServers" not in data or "omp-relay" in data["enabledServers"], "Selected OMP relay is excluded by enabledServers")


@dataclass(frozen=True)
class Write:
    path: Path
    data: bytes | None
    old_hash: str | None
    mode: str = "100644"


def output_plan(bundle: Path, rows: dict[str, dict], p: dict, prior: dict | None, adoption: dict) -> list[Write]:
    writes = []
    old = {row["path"]: row for row in prior["files"]} if prior else {}
    desired: dict[Path, tuple[bytes, str, str]] = {}
    user = Path(p["UserRoot"])
    for name, row in rows.items():
        target = selected_destination(name, p)
        if target is not None:
            no_links(target)
            desired[target] = ((bundle / name).read_bytes(), row["mode"], name)
    if "claude" in p["Harness"]:
        # Native fronts are required only without the canonical shared-skills junction.
        parent = user / ".claude/skills"
        linked = parent.exists() and parent.resolve() == (user / ".agents/skills").absolute()
        if not linked:
            for path, (data, mode, name) in list(desired.items()):
                if name.startswith("home/.agents/skills/") and name.endswith("/SKILL.md"):
                    relative = Path(name.removeprefix("home/.agents/skills/"))
                    native = canonical_home(user, str(Path(".claude/skills") / relative))
                    desired[native] = (data, mode, name)
    meta = user / ".agents/installations/agent-messaging"
    desired[meta / "connection.json"] = (encode({"schema_version": 1, "broker_url": f"http://127.0.0.1:{p['Port']}"}), "100644", "generated/connection.json")
    if "claude" in p["Harness"]:
        require(prior is not None or not claude_registration(p)[1] or p.get("_adopt", False), "Existing Claude registration requires a recognizable same-user installation")
    if set(p["Harness"]) & {"claude", "omp"}:
        desired[meta / "integrations.json"] = (encode(integration_contribution(p)), "100644", "generated/integrations.json")
    for name in sorted(STATIC):
        desired[meta / "verifier" / name] = ((bundle / name).read_bytes(), rows[name]["mode"], name)
    for target, (data, mode, source) in desired.items():
        previous = sha(target) if target.exists() else None
        if str(target) in old:
            require(previous == old[str(target)]["sha256"], f"Owned-file drift: {target}")
        elif previous is not None:
            allowed = adoption.get("files", {}).get(source, [])
            require(previous == digest(data) or previous in allowed, f"Foreign same-name file refused: {target}")
            require(p.get("_adopt", False), f"Existing files require a recognizable same-user installation: {target}")
        writes.append(Write(target, data, previous, mode))
    retirement = load(bundle / "deploy/retirements.json")
    closed(retirement, {"schema_version", "files"}, "retirement declaration")
    require(retirement["schema_version"] == 1, "Invalid retirement declaration")
    retired = {}
    for entry in retirement["files"]:
        closed(entry, {"path", "sha256"}, "retirement row")
        require(HEX64.fullmatch(entry["sha256"]) is not None, "Invalid retirement hash")
        destination = selected_destination(member(entry["path"]), p)
        require(destination is not None, "Retirement outside selected owned population")
        retired[str(destination)] = entry["sha256"]
    for filename, row in old.items():
        path = Path(filename)
        if path in desired or within(path, Path(p["InstallRoot"]) / ".venv") or path == meta / "verifier/release.json":
            continue
        require(retired.get(filename) == row["sha256"], f"No explicit hash-bound retirement for removed owned file: {path}")
        require(path.exists() and sha(path) == row["sha256"], f"Changed obsolete owned file cannot be retired: {path}")
        writes.append(Write(path, None, row["sha256"], row["mode"]))
    return writes


def prepare_omp(p: dict, work: Path) -> list[Write]:
    for key in ("OmpCommand", "OmpAgentRoot", "OmpPythonExe", "RelayStateRoot", "RelayRuntimeRoot"):
        require(key in p and bool(p[key]), f"Missing observed OMP prerequisite: {key}")
    interpreter(p["OmpPythonExe"], omp=True)
    agent = omp_agent(p)
    require(within(agent, Path(p["UserRoot"])), "OmpAgentRoot must belong to invoking profile")
    no_links(agent)
    executable = Path(p["OmpCommand"])
    no_links(executable, leaf_file=True)
    require(executable.is_file(), "OmpCommand must be an existing public CLI executable")
    work_agent = work / "omp-agent"
    private_directory(work_agent)
    config = agent / "config.yml"
    if config.exists():
        no_links(config, leaf_file=True)
        shutil.copyfile(config, work_agent / "config.yml")
    env = os.environ.copy()
    for key in ("OMP_PROFILE", "PI_PROFILE", "PI_CONFIG_FILES", "NODE_OPTIONS", "BUN_OPTIONS", "CLAUDE_CONFIG_DIR"):
        env.pop(key, None)
    for key in tuple(env):
        if key.startswith(("ANTHROPIC_", "OPENAI_", "COPILOT_", "GITHUB_", "GH_", "BROKER_", "DISCORD_")):
            env.pop(key)
    for key, name in (("HOME", "home"), ("USERPROFILE", "home"), ("APPDATA", "appdata"), ("LOCALAPPDATA", "localappdata")):
        directory = work / name
        private_directory(directory)
        env[key] = str(directory)
    env["PI_CODING_AGENT_DIR"] = str(work_agent)
    # OmpAgentRoot is already the actual selected native/profile agent directory.
    # Never let --profile redirect this isolated copy into the invoking user's home.
    prefix = []
    result = command([str(executable), *prefix, "config", "get", "extensions", "--json"], env=env, cwd=work)
    data = json.loads(result.stdout)
    require(isinstance(data, dict) and data.get("key") == "extensions", "OMP public typed config API is unavailable")
    extensions = data.get("value") or []
    require(isinstance(extensions, list) and all(isinstance(item, str) for item in extensions), "Invalid OMP extensions setting")
    extension = str(Path(p["InstallRoot"]) / "omp_relay_extension.ts")
    foreign = [item for item in extensions if Path(item).name == "omp_relay_extension.ts" and item != extension]
    require(not foreign, "Foreign relay extension path conflicts with selected installation")
    if extension not in extensions:
        extensions.append(extension)
    result = command([str(executable), *prefix, "config", "set", "extensions", json.dumps(extensions), "--json"], env=env, cwd=work)
    result = json.loads(result.stdout)
    require(result.get("key") == "extensions" and result.get("value") == extensions, "OMP typed config set/readback mismatch")
    # Some CLI profiles place their config beneath the isolated agent root. Require exactly one output.
    candidates = list(work_agent.rglob("config.yml"))
    require(len(candidates) == 1, "OMP CLI selected an unexpected configuration output")
    projected = yaml_document(candidates[0], p["OmpPythonExe"])
    require(projected.get("extensions") == extensions, "OMP projected YAML disagrees with public API")
    current = load(agent / "mcp.json") if (agent / "mcp.json").exists() else {}
    require(isinstance(current, dict) and isinstance(current.get("mcpServers", {}), dict), "Invalid existing OMP MCP configuration")
    expected = omp_contribution(p)
    existing = current.get("mcpServers", {}).get("omp-relay")
    require(existing is None or existing == expected, "Foreign/conflicting omp-relay registration; explicit reconciliation required")
    require("omp-relay" not in current.get("disabledServers", []), "OMP relay is explicitly disabled; installer does not release that hold")
    current.setdefault("mcpServers", {})["omp-relay"] = expected
    for key in ("enabledServers", "disabledServers"):
        require(key not in current or isinstance(current[key], list) and all(isinstance(item, str) for item in current[key]), f"Invalid OMP {key}")
    if "enabledServers" in current and "omp-relay" not in current["enabledServers"]:
        current["enabledServers"].append("omp-relay")
    # Preserve order/enable/disable lists and every unrelated field verbatim semantically.
    return [Write(config, candidates[0].read_bytes(), sha(config) if config.exists() else None), Write(agent / "mcp.json", encode(current), sha(agent / "mcp.json") if (agent / "mcp.json").exists() else None)]


def windows_arguments(value: str) -> list[str]:
    import ctypes.wintypes as wt
    count = ctypes.c_int()
    parse = ctypes.windll.shell32.CommandLineToArgvW
    parse.argtypes = [wt.LPCWSTR, ctypes.POINTER(ctypes.c_int)]
    parse.restype = ctypes.POINTER(wt.LPWSTR)
    pointer = parse(value, ctypes.byref(count))
    require(bool(pointer), "Cannot parse actual process arguments")
    try:
        return [pointer[index] for index in range(count.value)]
    finally:
        ctypes.windll.kernel32.LocalFree(pointer)


def listener(port: int, root: Path) -> dict | None:
    if os.name != "nt":
        return None
    snapshot = powershell("$ErrorActionPreference='Stop';$port=[int]$args[0];$bound=@([Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()|Where-Object Port -eq $port);$ids=@();if($bound.Count){$connections=@(Get-NetTCPConnection -State Listen -ErrorAction Stop|Where-Object LocalPort -eq $port);if(-not $connections.Count){throw 'Listener identity unavailable'};$ids=@($connections|Select-Object -ExpandProperty OwningProcess -Unique)};$rows=@(Get-CimInstance Win32_Process -Filter \"Name = 'python.exe' OR Name = 'python'\" -ErrorAction Stop|ForEach-Object{[pscustomobject]@{pid=$_.ProcessId;command=$_.CommandLine;executable=$_.ExecutablePath;started=$_.CreationDate.ToUniversalTime().ToString('o')}});[pscustomobject]@{owners=$ids;processes=$rows}|ConvertTo-Json -Depth 5 -Compress", str(port))
    owners = snapshot["owners"]
    require(len(owners) <= 1, "Selected port has ambiguous owners")
    selected = []
    for row in snapshot["processes"]:
        command_line = row.get("command")
        require(bool(command_line), "An unreadable Python process may be using the selected root")
        argv = windows_arguments(command_line)
        script_args = [Path(argument) for argument in argv[1:] if Path(argument).name.lower() == "server.py"]
        matched = any(argument.is_absolute() and argument == root / "server.py" for argument in script_args)
        if owners and row["pid"] == owners[0]:
            require(matched, f"Broker PID {row['pid']} at selected port {port} has an unproven root (selected InstallRoot {root}); Activate cannot authorize an unknown root. Independently identify and explicitly stop that owned legacy process without Force before first adoption")
            return row
        if matched:
            selected.append(row)
        elif not owners and any(not argument.is_absolute() for argument in script_args):
            raise Refusal(f"Legacy relative-entrypoint broker PID {row['pid']} has an unproven root (selected InstallRoot {root}, port {port}); Activate cannot authorize an unknown root. Independently identify and explicitly stop that owned process without Force before first adoption")
    require(not owners, "Selected port has a foreign or unreadable listener; no process will be stopped")
    require(len(selected) <= 1, "Multiple broker processes use the selected root")
    return selected[0] if selected else None


def wheelhouse(bundle: Path, p: dict, work: Path) -> Path:
    wheels = work / "wheels"
    private_directory(wheels)
    arguments = [p["PythonExe"], "-I", "-B", "-m", "pip", "download", "--disable-pip-version-check", "--only-binary=:all:", "--dest", str(wheels), "-r", str(bundle / "broker/requirements.txt")]
    if "claude" in p["Harness"] or "codex" in p["Harness"]:
        arguments.extend(["-r", str(bundle / "broker/requirements-mcp.txt")])
    command(arguments, cwd=work)
    project = work / "project"
    private_directory(project)
    shutil.copyfile(bundle / "broker/pyproject.toml", project / "pyproject.toml")
    shutil.copyfile(bundle / "broker/requirements.txt", project / "requirements.txt")
    for row in load(bundle / "deploy/release-files.json")["entries"]:
        name = row["destination"]
        if name.startswith("broker/broker/"):
            source = bundle / name
            no_links(source, leaf_file=True)
            target = project / "src/broker" / name.removeprefix("broker/broker/")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    command([p["PythonExe"], "-I", "-B", "-m", "pip", "wheel", "--disable-pip-version-check", "--no-deps", "--wheel-dir", str(wheels), str(project)], cwd=work)
    require(len(list(wheels.glob("agent_messaging-*.whl"))) == 1, "Source-bound agent-messaging console wheel was not produced")
    return wheels


def record_backup(destination: Path, entry: dict) -> None:
    operation = destination / "operation.json"
    document = load(operation) if operation.exists() else {"schema_version": 1, "component": COMPONENT, "backup_map": []}
    mappings = document.setdefault("backup_map", [])
    require(not any(row["original"] == entry["original"] for row in mappings), "Duplicate recovery backup mapping")
    mappings.append(entry)
    atomic(operation, encode(document), private=True)


def backup(path: Path, destination: Path, *, kind: str = "code", candidate_sha256: str | None = None) -> None:
    require(kind in {"code", "configuration", "environment", "state"}, "Unknown backup kind")
    if path.exists():
        no_links(path, leaf_file=True)
        target = destination / (digest(str(path).encode()) + "-" + path.name)
        require(not target.exists(), "Recovery backup must not overwrite prior preservation")
        shutil.copy2(path, target)
        entry = {"original": str(path), "backup": str(target), "kind": kind}
        if kind == "code":
            entry.update(original_sha256=sha(target), candidate_sha256=candidate_sha256)
        record_backup(destination, entry)


def check_write(write: Write) -> None:
    no_links(write.path)
    require((sha(write.path) if write.path.exists() else None) == write.old_hash, f"Concurrent modification before write: {write.path}")


def apply_writes(writes: list[Write], backups: Path, effects: list[str], *, configuration_paths: set[Path] | None = None) -> None:
    for write in writes:
        check_write(write)
        if write.data is not None and write.old_hash == digest(write.data):
            continue
        kind = "configuration" if write.path in (configuration_paths or set()) else "code"
        backup(write.path, backups, kind=kind, candidate_sha256=digest(write.data) if write.data is not None and kind == "code" else None)
        if write.data is None:
            write.path.unlink()
        else:
            atomic(write.path, write.data, private=write.path.name == "broker.env")
            if os.name != "nt":
                write.path.chmod(0o755 if write.mode == "100755" else 0o644)
        effects.append(str(write.path))


def dependency_records(venv: Path, names: set[str] | None = None) -> set[Path]:
    import csv
    import base64
    import email.parser
    managed = set()
    library = venv / "Lib/site-packages"
    for metadata in library.glob("*.dist-info/METADATA"):
        no_links(metadata, leaf_file=True)
        name = email.parser.Parser().parsestr(metadata.read_text(encoding="utf-8"))["Name"]
        normalized = re.sub(r"[-_.]+", "-", name or "").lower()
        if names is not None and normalized not in names:
            continue
        record = metadata.parent / "RECORD"
        require(record.is_file(), "Installed dependency lacks its ownership RECORD")
        no_links(record, leaf_file=True)
        for relative, binding, _ in csv.reader(record.read_text(encoding="utf-8").splitlines()):
            target = Path(os.path.abspath(library / relative))
            require(within(target, venv), "Dependency RECORD escapes the component environment")
            no_links(target)
            if target.is_file():
                if binding:
                    algorithm, value = binding.split("=", 1)
                    require(algorithm == "sha256" and base64.urlsafe_b64encode(bytes.fromhex(sha(target))).decode().rstrip("=") == value, f"Installed dependency drift: {target}")
                managed.add(target)
    return managed


def dependency_preflight(wheels: Path, p: dict, prior: dict | None, adopt: bool) -> tuple[set[Path], set[str], set[Path]]:
    import zipfile
    import email.parser
    import configparser
    venv = Path(p["InstallRoot"]) / ".venv"
    targets, names = set(), set()
    for wheel in sorted(wheels.glob("*.whl")):
        with zipfile.ZipFile(wheel) as archive:
            for entry in archive.infolist():
                if entry.is_dir():
                    continue
                parts = PurePosixPath(entry.filename).parts
                require(parts and not PurePosixPath(entry.filename).is_absolute() and ".." not in parts, "Unsafe dependency wheel member")
                require((entry.external_attr >> 16) & 0o170000 not in {0o120000}, "Dependency wheel links are refused")
                if parts[0].endswith(".data"):
                    require(len(parts) > 2 and parts[1] in {"purelib", "platlib", "scripts", "data"}, "Unsupported dependency wheel install scheme")
                    base = venv / ("Lib/site-packages" if parts[1] in {"purelib", "platlib"} else "Scripts" if parts[1] == "scripts" else "")
                    target = base.joinpath(*parts[2:])
                else:
                    target = (venv / "Lib/site-packages").joinpath(*parts)
                no_links(target)
                targets.add(target)
                if entry.filename.endswith(".dist-info/METADATA"):
                    name = email.parser.Parser().parsestr(archive.read(entry).decode("utf-8"))["Name"]
                    names.add(re.sub(r"[-_.]+", "-", name or "").lower())
                if entry.filename.endswith(".dist-info/entry_points.txt"):
                    config = configparser.ConfigParser(interpolation=None)
                    config.read_string(archive.read(entry).decode("utf-8"))
                    for section in ("console_scripts", "gui_scripts"):
                        for name in config[section] if section in config else []:
                            require(re.fullmatch(r"[A-Za-z0-9._-]+", name) is not None, "Unsafe dependency entrypoint")
                            targets.add(venv / "Scripts" / (name + ".exe"))
    previous = {Path(row["path"]) for row in prior["files"] if within(Path(row["path"]), venv)} if prior else set()
    recognized = dependency_records(venv, names | {"pip", "setuptools"}) if adopt and not prior and venv.exists() else set()
    for target in targets:
        require(not target.exists() or target in previous or target in recognized, f"Foreign dependency target would be overwritten: {target}")
    before = set()
    if venv.exists():
        require((venv / "Scripts/python.exe").is_file() and (venv / "pyvenv.cfg").is_file(), "Existing component environment is incomplete; no unknown bootstrap files will be overwritten")
        no_links(venv / "pyvenv.cfg", leaf_file=True)
        configuration = dict(line.split("=", 1) for line in (venv / "pyvenv.cfg").read_text(encoding="utf-8-sig").splitlines() if "=" in line)
        configuration = {key.strip(): value.strip() for key, value in configuration.items()}
        require(configuration.get("version", "").startswith("3.12.") and configuration.get("include-system-site-packages") == "false" and Path(configuration.get("home", "")) == Path(p["PythonExe"]).parent, "Existing venv must use the selected base Python 3.12 without global site packages")
        if adopt and not prior:
            recognized.update(path for path in (venv / "pyvenv.cfg", venv / "Scripts/python.exe", venv / "Scripts/pythonw.exe") if path.is_file())
        no_links(venv)
        for path in venv.rglob("*"):
            no_links(path)
            if path.is_file():
                before.add(path)
    return before, names, previous | recognized


def build_venv(bundle: Path, p: dict, wheels: Path) -> None:
    root = Path(p["InstallRoot"])
    python = root / ".venv/Scripts/python.exe"
    if not python.exists():
        command([p["PythonExe"], "-I", "-B", "-m", "venv", str(root / ".venv")], cwd=wheels.parent)
    arguments = [str(python), "-I", "-B", "-m", "pip", "install", "--disable-pip-version-check", "--no-index", "--no-deps", "--no-compile", "--force-reinstall", *[str(wheel) for wheel in sorted(wheels.glob("*.whl"))]]
    command(arguments, cwd=wheels.parent)
    imports = "import fastapi,uvicorn,sse_starlette,aiosqlite,watchdog,httpx,websockets"
    if "claude" in p["Harness"] or "codex" in p["Harness"]:
        imports += ",mcp"
    command([str(python), "-I", "-B", "-c", imports], cwd=wheels.parent)


def activate(p: dict, before: dict | None, effects: list[str]) -> dict:
    """Start installed code and prove a new identity; run() owns the prior stop."""
    root = Path(p["InstallRoot"])
    exe = str(native_powershell())
    # No stdout PIPE: detached descendants inherit handles. Wait on launcher handle only.
    effects.append(f"start requested for {root} on port {p['Port']}")
    proc = subprocess.Popen([exe, "-NoProfile", "-File", str(root / "scripts/start-broker.ps1"), "-Autostart", "-Port", str(p["Port"])], stdin=subprocess.DEVNULL, cwd=root, env=powershell_environment(Path(exe)))
    require(proc.wait(timeout=60) == 0, "Activation launcher failed; installed bytes remain, no rollback attempted")
    import time
    import urllib.request
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        candidate = listener(p["Port"], root)
        if candidate and (not before or candidate["pid"] != before["pid"] or candidate["started"] != before["started"]):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{p['Port']}/api/health", timeout=2) as response:
                    health = json.load(response)
                private_env = read_env(root / "broker.env")
                database = Path(private_env.get("BROKER_DB_PATH", "coordination.db"))
                if not database.is_absolute():
                    database = root / database
                checks = health.get("checks", {})
                if health.get("status") == "ok" and checks.get("sqlite") is True and Path(checks.get("db_path", "")).absolute() == database.absolute():
                    return {"status": "activated", "pid": candidate["pid"], "started": candidate["started"], "root": str(root)}
            except (OSError, ValueError):
                pass
        time.sleep(0.5)
    raise Refusal("Activation failed/pending; an old healthy listener is not proof of new code. Installed files remain; no rollback attempted")


def installation_outcome(prior: dict | None, installed_files: list[dict], *, reconciled: bool, now: str) -> tuple[str, str | None, bool]:
    def code(rows: list[dict]) -> dict[str, str]:
        return {row["path"]: row["sha256"] for row in rows if Path(row["path"]).suffix.lower() in EXECUTABLE - {""} or row["mode"] == "100755"}
    code_changed = code(prior["files"] if prior else []) != code(installed_files)
    timestamp = now if code_changed else prior["last_code_change_utc"] if prior else None
    return ("installed" if code_changed else "reconciled" if reconciled else "verified-no-op", timestamp, code_changed)


def recognizable_installation(bundle: Path, rows: dict[str, dict], p: dict, adoption: dict) -> bool:
    root = Path(p["InstallRoot"])
    if not root.exists() or not any(root.iterdir()):
        return False
    require((root / "broker.env").is_file(), "Existing broker requires private broker.env; state coordinates cannot be guessed")
    for name in ("broker/server.py", "broker/broker/schema.sql"):
        target = selected_destination(name, p)
        require(name in rows and target is not None and target.is_file(), "Not a recognizable bound broker installation")
        no_links(target, leaf_file=True)
        allowed = adoption.get("files", {}).get(name, [])
        current = sha(target)
        require(current == rows[name]["sha256"] or current in allowed, f"Foreign/edited legacy broker anchor: {target}")
    return True


def run(options: dict, *, actual_profile: Path | None = None) -> dict:
    mode = options.get("Mode", "Install")
    user = absolute(options["UserRoot"])
    require(user == (actual_profile or account_profile()), "UserRoot differs from actual invoking profile/account")
    meta = user / ".agents/installations/agent-messaging"
    for ancestor in (meta, *meta.parents):
        if within(ancestor, user):
            owned(ancestor)
    receipt_path = meta / "installed.json"
    prior = load(receipt_path) if receipt_path.exists() else None
    if prior:
        validate_installed(prior, user)
    p = resolve_parameters(options, prior)
    work = absolute(options["WorkRoot"])
    validate_roots(p, work, actual_profile=actual_profile)
    existing_omp_identity = False
    if mode != "Verify":
        interpreter(p["PythonExe"])
        if "omp" in p["Harness"]:
            existing_omp_identity = discover_omp(p, retained=bool(prior))
            validate_roots(p, work, actual_profile=actual_profile)
    if prior:
        verify_files(prior)
        verify_retained(prior)
        verify_configuration(prior["parameters"], prior["configuration_paths"])
    if mode == "Verify":
        require(not options.get("Activate"), "Verify is read-only")
        require(prior is not None, "Missing retained installation; Verify does not repair")
        require(prior["version"] == options["Version"], "Expected version differs from installed version")
        verify_configuration(p, prior["configuration_paths"])
        retained = load(meta / "verifier/release.json")
        require(retained["source_commit"] == prior["source_commit"] and retained["archive_sha256"] == prior["archive_sha256"], "Retained package binding differs from installed receipt")
        return {"status": "verified", "version": prior["version"], "activation": "not-observed", "files": len(prior["files"])}
    bundle = absolute(options["BundleDir"])
    release = load(absolute(options["ReleaseReceipt"]))
    rows = validate_release(bundle, release, options["Version"])
    if prior and prior["version"] == options["Version"]:
        require(prior["source_commit"] == release["source_commit"] and prior["archive_sha256"] == release["archive_sha256"], "Same release tag changed source/archive identity")
    if prior and prior["version"] != options["Version"]:
        # Existing service tags are timestamp coordinates; new releases must move forward.
        require(all(re.fullmatch(r"\d{4}\.\d{2}\.\d{2}\.\d{6}", tag) for tag in (prior["version"], options["Version"])), "Unordered non-timestamp version transition refused")
        require(options["Version"] > prior["version"], "Downgrade or unordered version transition refused")
    require(digest(Path(__file__).read_bytes()) == rows["deploy/install_runtime.py"]["sha256"], "Executing backend differs from selected committed package")
    adoption = load(bundle / "deploy/adoption.json")
    closed(adoption, {"schema_version", "files"}, "adoption declaration")
    require(adoption["schema_version"] == 1, "Invalid adoption declaration")
    adopt = False
    if prior is None:
        adopt = recognizable_installation(bundle, rows, p, adoption)
        if "omp" in p["Harness"] and (Path(p["InstallRoot"]) / "omp_relay_mcp.py").exists():
            require(all(Path(p[key]).is_dir() for key in ("RelayStateRoot", "RelayRuntimeRoot")), "Legacy OMP adoption needs observed existing state/runtime roots; no replacement identity")
    p["_adopt"] = adopt
    writes = output_plan(bundle, rows, p, prior, adoption)
    p.pop("_adopt")
    # Configuration planning/CLI calls operate exclusively in WorkRoot, never the live agent directory.
    private_directory(work)
    operation = work / ("operation-" + uuid.uuid4().hex)
    private_directory(operation)
    for key in ("TEMP", "TMP", "TMPDIR", "CLAUDE_CODE_TMPDIR"):
        os.environ[key] = str(operation)
    os.environ.update(PYTHONDONTWRITEBYTECODE="1", PIP_CACHE_DIR=str(work / "cache/pip"), UV_CACHE_DIR=str(work / "cache/uv"), BUN_INSTALL_CACHE_DIR=str(work / "cache/bun"), npm_config_cache=str(work / "cache/npm"))
    snapshot = configuration_snapshot(p)
    configs = []
    envfile = Path(p["InstallRoot"]) / "broker.env"
    if not envfile.exists():
        envdata = f"BROKER_HOST=127.0.0.1\nBROKER_PORT={p['Port']}\nBROKER_DB_PATH=coordination.db\n"
        configs.append(Write(envfile, envdata.encode("utf-8"), None))
    claude_exe = None
    register_claude = False
    if "claude" in p["Harness"]:
        claude_exe = shutil.which("claude")
        require(claude_exe is not None, "Claude CLI is a prerequisite; installer does not provision harnesses")
        _, present = claude_registration(p)
        register_claude = not present
        if present and claude_contribution(p)["command"] == p["PythonExe"]:
            command([p["PythonExe"], "-I", "-B", "-c", "import mcp,httpx"], cwd=operation)
    if "omp" in p["Harness"]:
        configs.extend(prepare_omp(p, operation))
    if "codex" in p["Harness"]:
        require(shutil.which("codex") is not None, "Codex CLI is a prerequisite; no global MCP registration is installed")
    changed = [w for w in writes if w.data is None or w.old_hash != digest(w.data)]
    code_changed = any(w.path.suffix.lower() in EXECUTABLE - {""} or w.mode == "100755" for w in changed)
    dependency_change = prior is None or any(w.path.name.startswith("requirements") or w.path.name == "pyproject.toml" or within(w.path, Path(p["InstallRoot"]) / "broker") for w in changed) or prior["parameters"]["Harness"] != p["Harness"]
    before = listener(p["Port"], Path(p["InstallRoot"]))
    if before and (code_changed or dependency_change):
        require(options.get("Activate", False), f"Broker PID {before['pid']} is running; code change requires separately confirmed Activate")
    if options.get("Activate"):
        require(mode == "Install", "Activate requires Install mode")
        displayed = f"{COMPONENT} {options['Version']} PID {before['pid'] if before else 'none'} root {p['InstallRoot']}"
        print("Activation requested: " + displayed, flush=True)
        # Exact operation-specific confirmation, never a reusable or hidden consent switch.
        require(sys.stdin.isatty(), "Activate requires an interactive operation-specific confirmation")
        require(input("Type the displayed line exactly to confirm: ") == displayed, "Activation not confirmed")
    wheels = wheelhouse(bundle, p, operation) if dependency_change else None
    before_environment, dependency_names, previous_environment = dependency_preflight(wheels, p, prior, adopt) if wheels else (set(), set(), set())
    release_target = meta / "verifier/release.json"
    release_write = Write(release_target, encode(release), sha(release_target) if release_target.exists() else None)
    all_writes = writes + configs + [release_write]
    for write in all_writes:
        owned_destination(write.path)
        check_write(write)
    venv_root = Path(p["InstallRoot"]) / ".venv"
    if venv_root.exists():
        for path in (venv_root, *venv_root.rglob("*")):
            no_links(path)
    meaningful = [w for w in all_writes if w.data is None or w.old_hash != digest(w.data)]
    if prior and not meaningful and not dependency_change and p == prior["parameters"] and not options.get("Activate"):
        return {"status": "verified-no-op", "version": options["Version"], "work_root": str(work), "operation": str(operation), "backups": None, "activation": "not-observed", "files": len(prior["files"])}
    if mode == "Check":
        return {"status": "checked", "version": options["Version"], "write_paths": [str(w.path) for w in all_writes if w.data is None or w.old_hash != digest(w.data)], "work_root": str(work), "operation": str(operation), "backups": None, "activation": "not-requested", "harness": p["Harness"]}
    require(mode == "Install", "Unknown operation")
    # Complete preflight succeeded. Every live mutation below is individually atomic where applicable.
    effects: list[str] = []
    backups = work / ("backups-" + uuid.uuid4().hex)
    private_directory(backups)
    atomic(backups / "operation.json", encode({"schema_version": 1, "component": COMPONENT, "version": options["Version"], "roots": {"user": str(user), "install": p["InstallRoot"]}, "paths": [str(w.path) for w in all_writes], "backup_map": [], "state_backed_up": False}), private=True)
    stamped = False
    try:
        if prior:
            verify_files(prior)
        require(configuration_snapshot(p) == snapshot, "Configuration changed after preflight")
        require(listener(p["Port"], Path(p["InstallRoot"])) == before, "Listener changed after preflight")
        if existing_omp_identity:
            for key in ("RelayStateRoot", "RelayRuntimeRoot"):
                no_links(Path(p[key]))
                require(Path(p[key]).is_dir(), f"Existing {key} disappeared after preflight; refusing replacement identity")
        if before and options.get("Activate"):
            exe = str(native_powershell())
            effects.append(f"stop requested for PID {before['pid']}")
            command([exe, "-NoProfile", "-File", str(Path(p["InstallRoot"]) / "scripts/stop-broker.ps1"), "-Port", str(p["Port"])], capture=False)
            require(listener(p["Port"], Path(p["InstallRoot"])) is None, "Broker is still running/restarted after stop; no code was copied")
        private_directory(meta)
        private_directory(Path(p["InstallRoot"]))
        for key in ("RelayStateRoot", "RelayRuntimeRoot"):
            if "omp" in p["Harness"] and not Path(p[key]).exists():
                private_directory(Path(p[key]))
                effects.append(p[key])
        apply_writes(all_writes, backups, effects, configuration_paths={write.path for write in configs} | {release_target})
        if dependency_change:
            # Preserve the old dependency environment in private recovery, never a live database.
            oldvenv = Path(p["InstallRoot"]) / ".venv"
            if oldvenv.exists():
                record_backup(backups, {"original": str(oldvenv), "backup": str(backups / "venv"), "kind": "environment"})
                shutil.copytree(oldvenv, backups / "venv", symlinks=True)
            effects.append(str(oldvenv) + " (dependency environment may change)")
            build_venv(bundle, p, wheels)
        if register_claude:
            target = Path(p["UserRoot"]) / ".claude.json"
            backup(target, backups, kind="configuration")
            contribution = claude_contribution(p)
            effects.append(str(target) + " (local-relay-channel registration may change)")
            command([claude_exe, "mcp", "add", "--scope", "user", "local-relay-channel", "--", contribution["command"], *contribution["args"]], cwd=operation)
            require(claude_registration(p)[1], "Claude registration readback failed")
        verify_configuration(p, list(snapshot))
        installed_files = []
        modes = {str(w.path): w.mode for w in writes if w.data is not None}
        modes[str(release_target)] = "100644"
        venv = Path(p["InstallRoot"]) / ".venv"
        dependency_owned = dependency_records(venv, dependency_names) if dependency_change else set()
        prior_environment = {Path(row["path"]) for row in prior["files"] if within(Path(row["path"]), venv)} if prior else set()
        for path in venv.rglob("*"):
            no_links(path)
            if path.is_file() and (path in prior_environment or path in dependency_owned or path in previous_environment or dependency_change and path not in before_environment):
                modes[str(path)] = "100644"
        for filename, source_mode in sorted(modes.items()):
            path = Path(filename)
            no_links(path, leaf_file=True)
            installed_files.append({"path": str(path), "sha256": sha(path), "mode": source_mode})
        status, timestamp, actual_code_changed = installation_outcome(prior, installed_files, reconciled=bool(meaningful or dependency_change or register_claude or p != (prior["parameters"] if prior else {})), now=dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"))
        installed = {"schema_version": 1, "component": COMPONENT, "repository": REPOSITORY, "version": options["Version"], "source_commit": release["source_commit"], "archive_sha256": release["archive_sha256"], "parameters": p, "files": installed_files, "configuration_paths": sorted(snapshot), "held_paths": [], "last_code_change_utc": timestamp}
        verify_files(installed)
        validate_installed(installed, user)
        verify_retained(installed)
        verify_configuration(installed["parameters"], installed["configuration_paths"])
        backup(receipt_path, backups, kind="configuration")
        atomic(receipt_path, encode(installed), private=True)
        stamped = True
        activation = {"status": "pending" if actual_code_changed else "not-observed"}
        if options.get("Activate"):
            # The prior process was already stopped before changing its executable code.
            activation = activate(p, before, effects)
        return {"status": status, "version": options["Version"], "verification": "verified", "harness": p["Harness"], "work_root": str(work), "operation": str(operation), "backups": str(backups), "activation": activation, "effects": effects}
    except BaseException as exc:
        diagnostic = {"status": "activation-failure" if stamped else "partial-failure", "error": str(exc), "possible_effects": effects, "work_root": str(work), "operation": str(operation), "backups": str(backups), "installed_bytes_recorded": stamped, "operation_success": False}
        diagnostic["backup_map"] = load(backups / "operation.json").get("backup_map", [])
        atomic(backups / "failure.json", encode(diagnostic), private=True)
        print(json.dumps(diagnostic), file=sys.stderr, flush=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--parameters-json")
    group.add_argument("--parameters-env", action="store_true")
    args = parser.parse_args()
    try:
        raw = os.environ["AGENT_MESSAGING_INSTALL_PARAMETERS"] if args.parameters_env else args.parameters_json
        result = run(json.loads(raw, object_pairs_hook=unique_object))
    except (Refusal, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"agent-messaging: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
