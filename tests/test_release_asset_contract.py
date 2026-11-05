"""Local-only builder, exact receipt/ZIP closure and release-publication boundary checks.

Fixtures commit only declared inputs in disposable repositories. Publication tests
replace gh with an in-process fixture function; no remote, service or real home is used.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
PACKAGER = ROOT / "deploy/package-broker-stack.ps1"
POWERSHELL = Path(os.environ.get("SystemRoot", "C:/Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"


def native_ps_environment(env):
    child = {key: value for key, value in env.items() if key.casefold() not in {"psmodulepath", "psmoduleanalysiscachepath"}}
    child["PSModulePath"] = str(POWERSHELL.parent / "Modules")
    child["PSModuleAnalysisCachePath"] = os.devnull
    return child


def run_ps(script, env):
    return subprocess.run(
        [str(POWERSHELL), "-NoProfile", "-NonInteractive", "-Command", script],
        env=native_ps_environment(env), stdin=subprocess.DEVNULL, capture_output=True, text=True,
    )


def ps_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


@contextmanager
def package_source_tree(tmp_path, mutate_source=None):
    if os.name != "nt":
        pytest.skip("Requires the actual Windows PowerShell 5.1 fixture host")
    assert POWERSHELL.is_file(), "Native Windows PowerShell 5.1 is required"
    source = tmp_path / "source checkout"
    declaration = json.loads((ROOT / "deploy/release-files.json").read_text())
    inputs = {row["source"] for row in declaration["entries"]} | {"deploy/package-broker-stack.ps1"}
    for relative in inputs:
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    if mutate_source is not None:
        mutate_source(source)
    declaration = json.loads((source / "deploy/release-files.json").read_text())
    env = os.environ.copy()
    for name in ("HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP"):
        target = tmp_path / name
        target.mkdir(exist_ok=True)
        env[name] = str(target)
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_AUTHOR_NAME="Package fixture", GIT_COMMITTER_NAME="Package fixture",
               GIT_AUTHOR_EMAIL="fixture@example.invalid", GIT_COMMITTER_EMAIL="fixture@example.invalid")
    def git(*args):
        return subprocess.run(["git", "-C", str(source), *args], check=True, env=env,
                              stdin=subprocess.DEVNULL, capture_output=True, text=True).stdout.strip()
    git("init")
    git("config", "core.autocrlf", "false")
    git("config", "core.hooksPath", str(tmp_path / "no-hooks"))
    git("add", "--all")
    git("update-index", "--chmod=+x", "deploy/scripts/start-broker.sh")
    git("commit", "-m", "Disposable package fixture")
    commit = git("rev-parse", "HEAD")
    before = {p: hashlib.sha256((source / p).read_bytes()).hexdigest() for p in inputs if (source / p).exists()}
    yield source, commit, env, declaration
    assert before == {p: hashlib.sha256((source / p).read_bytes()).hexdigest() for p in before}
    assert git("status", "--porcelain") == ""


@pytest.fixture
def package_source(tmp_path):
    with package_source_tree(tmp_path) as fixture:
        yield fixture


def package(fixture, out, revision=None, extra=()):
    source, commit, env, _ = fixture
    return subprocess.run([
        str(POWERSHELL), "-NoProfile", "-NonInteractive", "-File",
        str(source / "deploy/package-broker-stack.ps1"),
        "-SourceBrokerRoot", str(source), "-ReleaseTarget", revision or commit,
        "-NoRelease", "-Zip", "-OutDir", str(out), *extra,
    ], env=native_ps_environment(env), stdin=subprocess.DEVNULL, capture_output=True, text=True)


def helpers(fixture):
    source, _, _, _ = fixture
    # Load only actual function ASTs: never dot-source the executable packager.
    return (
        "$ErrorActionPreference='Stop'; $tokens=$null; $errors=$null; "
        "$ast=[Management.Automation.Language.Parser]::ParseFile("
        + ps_quote(source / "deploy/package-broker-stack.ps1")
        + ",[ref]$tokens,[ref]$errors); if($errors.Count){throw $errors}; "
        "$ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$false) | "
        "ForEach-Object { . ([scriptblock]::Create($_.Extent.Text)) }; "
    )


def test_raw_blob_proof_handles_binary_and_empty_materialized_files_without_git_context(package_source, tmp_path):
    root = tmp_path / "materialized no repository"
    while len(str(root)) < 280:
        root /= "long-materialized-component"
    root.mkdir(parents=True)
    values = {"binary.dat": b"\x00\xff\r\n$Format:%H$\x80", "empty.dat": b""}
    files, sources = [], []
    for relative, content in values.items():
        (root / relative).write_bytes(content)
        oid = hashlib.sha1(b"blob " + str(len(content)).encode("ascii") + b"\0" + content).hexdigest()
        files.append(ps_quote(relative) + "=[pscustomobject]@{Source=" + ps_quote(relative) + ";Mode='100644'}")
        sources.append(ps_quote(relative) + "=[pscustomobject]@{Mode='100644';Oid=" + ps_quote(oid) + "}")
    proof = helpers(package_source) + "$files=@{" + ";".join(files) + "};$sources=@{" + ";".join(sources) + "};Assert-RawCommittedFiles -Root " + ps_quote(root) + " -Files $files -Sources $sources;[Console]::WriteLine('raw-bound')"
    result = run_ps(proof, package_source[2])
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "raw-bound"
    (root / "binary.dat").write_bytes(values["binary.dat"] + b"transformed")
    refused = run_ps(proof, package_source[2])
    assert refused.returncode != 0
    assert "differs from raw committed blob" in refused.stderr
    assert not (root / ".git").exists()


def test_local_package_exact_archive_and_private_output(package_source, tmp_path):
    out = tmp_path / "nonsecret candidates" / "bundle"
    source, commit, env, declaration = package_source
    private_settings = Path(env["USERPROFILE"]) / ".claude/settings.json"
    private_settings.parent.mkdir()
    private_bytes = b'{"env":{"PRIVATE_FIXTURE":"never-package-this-value"}}'
    private_settings.write_bytes(private_bytes)
    result = package(package_source, out)
    assert result.returncode == 0, result.stdout + result.stderr
    expected = {r["destination"] for r in declaration["entries"]} | {
        "manifest.json", "VERSION.txt", ".gitignore",
    }
    with zipfile.ZipFile(str(out) + ".zip") as archive:
        assert len(archive.namelist()) == len(expected)
        assert set(archive.namelist()) == expected
        for name in expected:
            assert archive.read(name) == (out / name).read_bytes()
        for row in declaration["entries"]:
            assert archive.read(row["destination"]) == (source / row["source"]).read_bytes()
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["packagedCommit"] == commit
        assert manifest["releaseDeclarationSha256"] == hashlib.sha256(archive.read("deploy/release-files.json")).hexdigest()
        tree_members = {name.lower(): archive.read(name) for name in expected if name not in {"manifest.json", "VERSION.txt"}}
        tree_wire = "".join(f"{name}:{hashlib.sha256(tree_members[name]).hexdigest()}\r\n" for name in sorted(tree_members))
        assert manifest["treeSha256"] == hashlib.sha256(tree_wire.encode()).hexdigest()
        assert archive.read("VERSION.txt").decode().splitlines() == [manifest["bundleVersion"], manifest["treeSha256"]]
        assert archive.read("install.ps1") == (source / "install.ps1").read_bytes()
        assert archive.read("broker/pyproject.toml") == (source / "pyproject.toml").read_bytes()
        assert_receipt(Path(str(out) + ".zip"), commit, manifest["bundleVersion"], expected)
        assert all(b"never-package-this-value" not in archive.read(name) for name in expected)
        assert "home/.claude/plugins/local-relay-channel/.claude-plugin/plugin.json" in expected
        assert len([name for name in expected if name.startswith("home/.claude/plugins/local-relay-channel/")]) == 6
        assert len([name for name in expected if name.startswith("home/.agents/skills/")]) == 6
        assert len([name for name in expected if name.startswith("home/.claude/hooks/relay/")]) == 6
        assert not any(name.endswith(("broker.env.example", "settings-fragment.json", "exports.json")) for name in expected)
    assert {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()} == expected
    assert private_settings.read_bytes() == private_bytes
    sentinel = out / "foreign.private"
    sentinel.write_bytes(b"do not replace")
    again = package(package_source, out)
    assert again.returncode != 0
    assert sentinel.read_bytes() == b"do not replace"


def test_direct_packager_ignores_incompatible_inherited_utility_modules(package_source, tmp_path):
    source, commit, env, declaration = package_source
    poison = tmp_path / "incompatible-modules/Microsoft.PowerShell.Utility"
    poison.mkdir(parents=True)
    (poison / "Microsoft.PowerShell.Utility.psm1").write_text("throw 'Incompatible inherited Utility module loaded'\n")
    child_env = dict(env)
    child_env["PSModulePath"] = str(poison.parent)
    child_env["PSModuleAnalysisCachePath"] = os.devnull
    before = dict(child_env)
    out = tmp_path / "isolated module package"
    # Do not use the test helper's sanitized child: exercise the script's boundary.
    result = subprocess.run([str(POWERSHELL), "-NoProfile", "-NonInteractive", "-File", str(source / "deploy/package-broker-stack.ps1"), "-SourceBrokerRoot", str(source), "-ReleaseTarget", commit, "-NoRelease", "-Zip", "-OutDir", str(out)], env=child_env, stdin=subprocess.DEVNULL, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert child_env == before
    with zipfile.ZipFile(str(out) + ".zip") as archive:
        expected = {row["destination"] for row in declaration["entries"]} | {"manifest.json", "VERSION.txt", ".gitignore"}
        assert set(archive.namelist()) == expected
        manifest = json.loads(archive.read("manifest.json"))
        assert_receipt(Path(str(out) + ".zip"), commit, manifest["bundleVersion"], expected)


@pytest.mark.parametrize("kind", ["source", "source_child", "ancestor", "service", "foreign", "git", "zip", "receipt", "wrong_revision"])
def test_output_refusal_preserves_existing_bytes(package_source, tmp_path, kind):
    source, _, env, _ = package_source
    out = tmp_path / "outputs" / "bundle"
    sentinel = tmp_path / "untouched.private"
    sentinel.write_bytes(b"private sentinel")
    if kind == "source":
        out = source
    elif kind == "source_child":
        out = source / "fresh output"
    elif kind == "ancestor":
        out = source.parent
    elif kind == "service":
        out = Path(env["USERPROFILE"]) / ".agents/services/broker/candidate"
    elif kind in {"foreign", "git"}:
        out.mkdir(parents=True)
        (out / "sentinel").write_bytes(b"foreign")
        if kind == "git":
            (out / ".git").mkdir()
            (out / ".git/config").write_bytes(b"foreign git")
    elif kind in {"zip", "receipt"}:
        out.parent.mkdir()
        Path(str(out) + (".zip" if kind == "zip" else ".zip.json")).write_bytes(b"foreign archive metadata")
    result = package(package_source, out, "0" * 40 if kind == "wrong_revision" else None)
    assert result.returncode != 0, result.stdout
    assert sentinel.read_bytes() == b"private sentinel"
    if kind in {"foreign", "git"}:
        assert (out / "sentinel").read_bytes() == b"foreign"
    elif kind in {"zip", "receipt"}:
        assert Path(str(out) + (".zip" if kind == "zip" else ".zip.json")).read_bytes() == b"foreign archive metadata"
        assert not out.exists()
    elif kind in {"source_child", "service", "wrong_revision"}:
        assert not out.exists()
    assert not list(Path(env["TEMP"]).iterdir())


def test_reparse_output_parent_refused(package_source, tmp_path):
    target = tmp_path / "foreign target"
    target.mkdir()
    (target / "sentinel").write_bytes(b"unchanged")
    link = tmp_path / "junction"
    result = run_ps("$ErrorActionPreference='Stop'; New-Item -ItemType Junction -Path "
                    + ps_quote(link) + " -Target " + ps_quote(target) + " | Out-Null", package_source[2])
    assert result.returncode == 0, "Fixture host must support junction coverage: " + result.stderr
    try:
        result = package(package_source, link / "bundle")
        assert result.returncode != 0
        assert not (target / "bundle").exists()
        assert (target / "sentinel").read_bytes() == b"unchanged"
    finally:
        os.rmdir(link)


def test_archive_hidden_children_and_tampering(package_source, tmp_path):
    payload = tmp_path / "payload"
    hidden = payload / "home/.claude/plugins/local-relay-channel/.claude-plugin/plugin.json"
    hidden.parent.mkdir(parents=True)
    hidden.write_bytes(b'{"name":"fixture"}')
    (payload / ".gitignore").write_bytes(b"private.env\n")
    archive = tmp_path / "validated.zip"
    script = helpers(package_source) + (
        "$hidden=Get-Item -Force -LiteralPath " + ps_quote(hidden) + "; "
        "$hidden.Attributes=$hidden.Attributes -bor [IO.FileAttributes]::Hidden; "
        "$hidden.Directory.Attributes=$hidden.Directory.Attributes -bor [IO.FileAttributes]::Hidden; "
        "$files=Get-PackageFileMap -Root " + ps_quote(payload) + "; "
        "New-PackageArchive -Archive " + ps_quote(archive) + " -Expected $files"
    )
    result = run_ps(script, package_source[2])
    assert result.returncode == 0, result.stdout + result.stderr
    with zipfile.ZipFile(archive) as z:
        original = {entry.filename: (entry, z.read(entry)) for entry in z.infolist()}
    assert set(original) == {".gitignore", hidden.relative_to(payload).as_posix()}
    for fault in ("missing", "extra", "changed", "duplicate", "case", "wrapper", "mode"):
        bad = tmp_path / (fault + ".zip")
        with zipfile.ZipFile(bad, "w") as z:
            for name, (entry, content) in original.items():
                if fault == "missing" and name == ".gitignore":
                    continue
                changed_name = name.upper() if fault == "case" else "wrapper/" + name if fault == "wrapper" else name
                replacement = zipfile.ZipInfo(changed_name)
                replacement.external_attr = (0o120777 << 16) if fault == "mode" else entry.external_attr
                z.writestr(replacement, b"changed" if fault == "changed" else content)
            if fault == "extra":
                z.writestr("../escape", b"bad")
            if fault == "duplicate":
                z.writestr(".gitignore", b"private.env\n")
        result = run_ps(helpers(package_source) + "$files=Get-PackageFileMap -Root "
                        + ps_quote(payload) + "; Assert-PackageArchive -Archive "
                        + ps_quote(bad) + " -Expected $files", package_source[2])
        assert result.returncode != 0, fault


def assert_receipt(archive_path, commit, version, expected, repository="gupta-ash/agent-messaging"):
    receipt = json.loads(Path(str(archive_path) + ".json").read_text(encoding="utf-8"))
    assert set(receipt) == {"schema_version", "component", "repository", "version", "source_commit", "archive_sha256", "files"}
    assert receipt["schema_version"] == 1
    assert receipt["component"] == "agent-messaging"
    assert receipt["repository"] == repository
    assert receipt["version"] == version
    assert receipt["source_commit"] == commit
    assert receipt["archive_sha256"] == hashlib.sha256(archive_path.read_bytes()).hexdigest()
    assert [row["path"] for row in receipt["files"]] == sorted(expected)
    with zipfile.ZipFile(archive_path) as archive:
        assert set(archive.namelist()) == expected
        for row in receipt["files"]:
            assert set(row) == {"path", "sha256", "mode"}
            assert row["sha256"] == hashlib.sha256(archive.read(row["path"])).hexdigest()
            assert row["mode"] in {"100644", "100755"}
            assert archive.getinfo(row["path"]).external_attr >> 16 == int(row["mode"], 8)
        assert next(row for row in receipt["files"] if row["path"] == "broker/scripts/start-broker.sh")["mode"] == "100755"
    return receipt


def assert_raw_static_sources(fixture, archive_path, frontdoor=None):
    source, commit, env, declaration = fixture
    result = subprocess.run(
        ["git", "-C", str(source), "ls-tree", "-r", commit], check=True, env=env,
        stdin=subprocess.DEVNULL, capture_output=True, text=True,
    )
    sources = {}
    for line in result.stdout.splitlines():
        metadata, relative = line.split("\t", 1)
        mode, kind, oid = metadata.split()
        assert kind == "blob"
        sources[relative] = (oid, int(mode, 8))

    def blob_oid(content):
        return hashlib.sha1(b"blob " + str(len(content)).encode("ascii") + b"\0" + content).hexdigest()

    with zipfile.ZipFile(archive_path) as archive:
        for row in declaration["entries"]:
            expected_oid, expected_mode = sources[row["source"]]
            assert blob_oid(archive.read(row["destination"])) == expected_oid, row["source"]
            assert archive.getinfo(row["destination"]).external_attr >> 16 == expected_mode, row["source"]
    if frontdoor is not None:
        assert blob_oid(frontdoor.read_bytes()) == sources["install.ps1"][0]


def test_literal_commit_ignores_dirty_payload_and_frontdoor(package_source, tmp_path):
    source, commit, _, declaration = package_source
    relatives = ["install.ps1", "pyproject.toml", "skills/relay-send/SKILL.md", "harness/claude/hooks/relay/shared.py"]
    original = {name: (source / name).read_bytes() for name in relatives}
    try:
        for name in relatives:
            (source / name).write_bytes(b"uncommitted private fixture bytes")
        out = tmp_path / "committed bundle"
        result = package(package_source, out, extra=("-BundleVersion", "2026.11.04.123456"))
        assert result.returncode == 0, result.stdout + result.stderr
        with zipfile.ZipFile(str(out) + ".zip") as archive:
            for row in declaration["entries"]:
                if row["source"] in original:
                    assert archive.read(row["destination"]) == original[row["source"]]
            expected = set(archive.namelist())
        assert_receipt(Path(str(out) + ".zip"), commit, "2026.11.04.123456", expected)
    finally:
        for name, content in original.items():
            (source / name).write_bytes(content)


def test_modified_executing_packager_refused_before_output(package_source, tmp_path):
    source, _, env, _ = package_source
    policy = source / "deploy/package-broker-stack.ps1"
    original = policy.read_bytes()
    try:
        policy.write_bytes(original + b"\n# uncommitted policy fixture\n")
        out = tmp_path / "refused policy"
        result = package(package_source, out)
        assert result.returncode != 0
        assert not out.exists()
        assert not list(Path(env["TEMP"]).iterdir())
    finally:
        policy.write_bytes(original)


@pytest.mark.parametrize("fault", ["duplicate", "traversal", "dot", "empty", "unowned", "state", "plugin_extra", "missing_skill", "missing_retirements", "missing_metadata", "metadata_source", "frontdoor_source"])
def test_committed_declaration_rejects_nonclosure(tmp_path, fault):
    def mutate(source):
        path = source / "deploy/release-files.json"
        declaration = json.loads(path.read_text())
        entries = declaration["entries"]
        if fault == "missing_skill":
            entries[:] = [row for row in entries if row["destination"] != "home/.agents/skills/relay-check/scripts/check.py"]
        elif fault == "missing_retirements":
            entries[:] = [row for row in entries if row["destination"] != "deploy/retirements.json"]
        elif fault == "missing_metadata":
            entries[:] = [row for row in entries if row["destination"] != "broker/pyproject.toml"]
        elif fault == "metadata_source":
            next(row for row in entries if row["destination"] == "broker/pyproject.toml")["source"] = "requirements.txt"
        elif fault == "frontdoor_source":
            next(row for row in entries if row["destination"] == "install.ps1")["source"] = "deploy/install-broker-stack.ps1"
        else:
            destinations = {
                "duplicate": "INSTALL.PS1", "traversal": "broker/../escape", "dot": "broker/./extra.py",
                "empty": "broker//extra.py", "unowned": "policy/settings.json", "state": "broker/coordination.db",
                "plugin_extra": "home/.claude/plugins/local-relay-channel/extra.py",
            }
            entries.append({"source": "src/broker/server.py", "destination": destinations[fault]})
        path.write_text(json.dumps(declaration), encoding="utf-8")
    with package_source_tree(tmp_path, mutate) as fixture:
        out = tmp_path / "invalid declaration"
        result = package(fixture, out)
        assert result.returncode != 0, result.stdout
        assert not out.exists()
        assert not Path(str(out) + ".zip").exists()


def publish_with_fixture_gh(fixture, out, capture, *, version="2026.11.04.123456", tree="", assets=(),
                            held=False, collision=False, wrong_commit=False, notice="", promote=False):
    source, commit, base_env, _ = fixture
    capture.mkdir()
    env = base_env.copy()
    env.update(FIXTURE_CAPTURE=str(capture), FIXTURE_COMMIT=commit, FIXTURE_TREE=tree,
               FIXTURE_ASSETS=json.dumps(list(assets)), FIXTURE_HELD=str(int(held)),
               FIXTURE_COLLISION=str(int(collision)), FIXTURE_WRONG_COMMIT=str(int(wrong_commit)))
    script = r"""
function global:gh {
    $a = @($args)
    $global:LASTEXITCODE = 0
    $line = ConvertTo-Json -InputObject $a -Compress
    [IO.File]::AppendAllText((Join-Path $env:FIXTURE_CAPTURE 'calls.jsonl'), $line + "`n")
    if ($a[0] -eq 'auth') { return }
    if ($a[0] -eq 'api' -and $a -contains '--paginate') {
        if ($env:FIXTURE_HELD -eq '1') {
            ConvertTo-Json -InputObject @(@{prerelease=$true; body=$env:FIXTURE_TREE; tag_name='held-coordinate'}) -Compress
        } else { '[]' }
        return
    }
    if ($a[0] -eq 'api' -and $a[1].Contains('/git/matching-refs/')) {
        if ($env:FIXTURE_COLLISION -eq '1') {
            $tag = $a[1].Substring($a[1].LastIndexOf('/') + 1)
            ConvertTo-Json -InputObject @(@{ref="refs/tags/$tag"}) -Compress
        } else { '[]' }
        return
    }
    if ($a[0] -eq 'api' -and $a[1].Contains('/git/ref/')) {
        @{object=@{type='tag'; sha=('e' * 40)}} | ConvertTo-Json -Compress
        return
    }
    if ($a[0] -eq 'api' -and $a[1].EndsWith('/git/tags/' + ('e' * 40))) {
        @{object=@{type='tag'; sha=('f' * 40)}} | ConvertTo-Json -Compress
        return
    }
    if ($a[0] -eq 'api' -and $a[1].EndsWith('/git/tags/' + ('f' * 40))) {
        $sha = $env:FIXTURE_COMMIT
        if ($env:FIXTURE_WRONG_COMMIT -eq '1') { $sha = '0' * 40 }
        @{object=@{type='commit'; sha=$sha}} | ConvertTo-Json -Compress
        return
    }
    if ($a[0] -eq 'release' -and $a[1] -eq 'view') {
        if ($a -contains 'body') { $env:FIXTURE_TREE; return }
        if ($a -contains 'assets') { $env:FIXTURE_ASSETS | ConvertFrom-Json | ForEach-Object { $_ }; return }
        if ($a -contains 'isPrerelease') { 'true'; return }
    }
    if ($a[0] -eq 'release' -and $a[1] -eq 'create') {
        foreach ($asset in $a[3..5]) {
            [IO.File]::Copy($asset, (Join-Path $env:FIXTURE_CAPTURE (Split-Path -Leaf $asset)), $false)
        }
        'https://example.invalid/release'
        return
    }
    if ($a[0] -eq 'release' -and $a[1] -eq 'edit') { return }
    throw "Unexpected fixture gh invocation: $($a -join ' ')"
}
# Create the capture log only after the actual in-process gh seam is installed.
# Early builder refusals then retain meaningful evidence of zero gh invocations.
if ((Get-Command gh).CommandType -ne 'Function') { throw 'Fixture gh seam is not installed' }
[IO.File]::WriteAllText((Join-Path $env:FIXTURE_CAPTURE 'calls.jsonl'), '')
"""
    script += "& " + ps_quote(source / "deploy/package-broker-stack.ps1")
    script += " -SourceBrokerRoot " + ps_quote(source) + " -ReleaseTarget " + ps_quote(commit)
    script += " -Zip -OutDir " + ps_quote(out)
    if version is not None:
        script += " -BundleVersion " + ps_quote(version)
    if notice:
        script += " -ReleaseNotice " + ps_quote(notice)
    if promote:
        script += " -PromoteHeldTree"
    result = run_ps(script, env)
    calls = [json.loads(line) for line in (capture / "calls.jsonl").read_text(encoding="utf-8-sig").splitlines()]
    return result, calls


@pytest.mark.parametrize("attribute", ["export-subst", "export-ignore"])
@pytest.mark.parametrize("relative", [
    "src/broker/server.py", "install.ps1", "pyproject.toml",
    "deploy/release-files.json", "deploy/scripts/start-broker.sh",
])
def test_archive_attributes_cannot_change_or_omit_raw_sources(tmp_path, relative, attribute):
    def mutate(source):
        (source / ".gitattributes").write_text(f"{relative} -text {attribute}\n", encoding="utf-8")
        if attribute == "export-subst":
            target = source / relative
            if relative == "deploy/release-files.json":
                # The expanded archive would describe a valid extra package member;
                # the declaration must be rejected as transformed before interpreting it.
                declaration = json.loads(target.read_text())
                declaration["entries"].append({"source": "LICENSE", "destination": "broker/commit-$Format:%H$.txt"})
                target.write_text(json.dumps(declaration), encoding="utf-8")
            else:
                target.write_bytes(target.read_bytes() + b"\n# raw committed archive fixture $Format:%H$\n")

    with package_source_tree(tmp_path, mutate) as fixture:
        out = tmp_path / "refused archive attributes"
        capture = tmp_path / "refused publication capture"
        result, calls = publish_with_fixture_gh(fixture, out, capture)
        diagnostic = " ".join((result.stdout + result.stderr).split())
        assert result.returncode != 0, diagnostic
        if attribute == "export-subst":
            assert "differs from raw committed blob" in diagnostic, diagnostic
        elif relative == "src/broker/server.py":
            assert "does not contain server.py" in diagnostic, diagnostic
        else:
            assert "Missing raw committed release source" in diagnostic, diagnostic
        assert Path(relative).name in diagnostic, diagnostic
        assert calls == []
        assert {path.name for path in capture.iterdir()} == {"calls.jsonl"}
        assert not out.exists()
        assert not Path(str(out) + ".zip").exists()
        assert not Path(str(out) + ".zip.json").exists()


@pytest.mark.parametrize("relative", ["install.ps1", "pyproject.toml", "deploy/scripts/start-broker.sh"])
@pytest.mark.parametrize("substitute", [False, True])
def test_unchanged_archive_attributes_publish_raw_sources(tmp_path, relative, substitute):
    def mutate(source):
        attribute = "export-subst" if substitute else "-export-subst"
        (source / ".gitattributes").write_text(f"{relative} -text -export-ignore {attribute}\n", encoding="utf-8")
        if not substitute:
            target = source / relative
            target.write_bytes(target.read_bytes() + b"\n# unexpanded committed fixture $Format:%H$\n")

    with package_source_tree(tmp_path, mutate) as fixture:
        capture = tmp_path / "unchanged attribute upload capture"
        result, calls = publish_with_fixture_gh(fixture, tmp_path / "unchanged attributes", capture)
        assert result.returncode == 0, result.stdout + result.stderr
        assert len([call for call in calls if call[:2] == ["release", "create"]]) == 1
        asset = capture / "agent-broker-deploy-2026.11.04.123456.zip"
        expected = {row["destination"] for row in fixture[3]["entries"]} | {"manifest.json", "VERSION.txt", ".gitignore"}
        assert {path.name for path in capture.iterdir()} == {asset.name, asset.name + ".json", "install.ps1", "calls.jsonl"}
        assert_receipt(asset, fixture[1], "2026.11.04.123456", expected)
        assert_raw_static_sources(fixture, asset, capture / "install.ps1")


def test_release_uploads_exact_zip_receipt_and_committed_frontdoor(package_source, tmp_path):
    out = tmp_path / "arbitrary local staging name"
    capture = tmp_path / "release upload capture"
    result, calls = publish_with_fixture_gh(package_source, out, capture)
    assert result.returncode == 0, result.stdout + result.stderr
    asset = capture / "agent-broker-deploy-2026.11.04.123456.zip"
    assert {p.name for p in capture.iterdir()} == {asset.name, asset.name + ".json", "install.ps1", "calls.jsonl"}
    assert asset.read_bytes() == Path(str(out) + ".zip").read_bytes()
    assert Path(str(asset) + ".json").read_bytes() == Path(str(out) + ".zip.json").read_bytes()
    expected = {row["destination"] for row in package_source[3]["entries"]} | {"manifest.json", "VERSION.txt", ".gitignore"}
    assert_receipt(asset, package_source[1], "2026.11.04.123456", expected)
    assert_raw_static_sources(package_source, asset, capture / "install.ps1")
    with zipfile.ZipFile(asset) as archive:
        assert (capture / "install.ps1").read_bytes() == archive.read("install.ps1") == (package_source[0] / "install.ps1").read_bytes()
    creates = [call for call in calls if call[:2] == ["release", "create"]]
    assert len(creates) == 1
    assert creates[0][creates[0].index("--repo") + 1] == "gupta-ash/agent-messaging"
    assert creates[0][creates[0].index("--target") + 1] == package_source[1]
    assert [call[1] for call in calls if call[0] == "api" and "/git/tags/" in call[1]] == [
        "repos/gupta-ash/agent-messaging/git/tags/" + "e" * 40,
        "repos/gupta-ash/agent-messaging/git/tags/" + "f" * 40,
    ]


@pytest.mark.parametrize("complete", [False, True])
def test_unchanged_skip_requires_complete_release_assets(package_source, tmp_path, complete):
    baseline = tmp_path / "baseline"
    result = package(package_source, baseline)
    assert result.returncode == 0, result.stdout + result.stderr
    tree = json.loads((baseline / "manifest.json").read_text())["treeSha256"]
    asset = "agent-broker-deploy-2026.11.04.000000.zip"
    assets = [asset, asset + ".json", "install.ps1"] if complete else [asset]
    result, calls = publish_with_fixture_gh(package_source, tmp_path / "candidate", tmp_path / "capture",
                                          version=None, tree=tree, assets=assets)
    assert result.returncode == 0, result.stdout + result.stderr
    creates = [call for call in calls if call[:2] == ["release", "create"]]
    assert len(creates) == (0 if complete else 1)


@pytest.mark.parametrize("notice,promote,allowed", [("", False, False), ("Retained hold", False, True), ("", True, True)])
def test_held_content_requires_notice_or_explicit_promotion(package_source, tmp_path, notice, promote, allowed):
    baseline = tmp_path / "baseline"
    result = package(package_source, baseline)
    assert result.returncode == 0, result.stdout + result.stderr
    tree = json.loads((baseline / "manifest.json").read_text())["treeSha256"]
    result, calls = publish_with_fixture_gh(package_source, tmp_path / "held candidate", tmp_path / "capture",
                                          tree=tree, held=True, notice=notice, promote=promote)
    assert (result.returncode == 0) is allowed, result.stdout + result.stderr
    creates = [call for call in calls if call[:2] == ["release", "create"]]
    assert len(creates) == int(allowed)
    if notice:
        assert "--prerelease" in creates[0] and "--latest=false" in creates[0]


def test_existing_tag_refuses_create_without_clobber(package_source, tmp_path):
    result, calls = publish_with_fixture_gh(package_source, tmp_path / "candidate", tmp_path / "capture", collision=True)
    assert result.returncode != 0
    assert not any(call[:2] in (["release", "create"], ["release", "upload"]) for call in calls)


def test_source_commit_readback_mismatch_demotes_without_recreate(package_source, tmp_path):
    result, calls = publish_with_fixture_gh(package_source, tmp_path / "candidate", tmp_path / "capture", wrong_commit=True)
    assert result.returncode != 0
    assert len([call for call in calls if call[:2] == ["release", "create"]]) == 1
    demotions = [call for call in calls if call[:2] == ["release", "edit"]]
    assert len(demotions) == 1
    assert "--prerelease" in demotions[0] and "--latest=false" in demotions[0]


def test_frontdoor_tree_digest_matches_real_packager_underscore_and_letter_paths(package_source, tmp_path):
    out = tmp_path / "actual digest bundle"
    source, _, env, _ = package_source
    result = package(package_source, out)
    assert result.returncode == 0, result.stdout + result.stderr
    receipt_path = Path(str(out) + ".zip.json")
    with zipfile.ZipFile(str(out) + ".zip") as archive:
        expected_digest = json.loads(archive.read("manifest.json"))["treeSha256"]
        # These actual declared siblings distinguish lowercase ordinal order
        # from OrdinalIgnoreCase; the test does not synthesize the wire hash.
        assert "broker/broker/__init__.py" in archive.namelist()
        assert "broker/broker/addressing.py" in archive.namelist()
    script = (
        "$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest; "
        "$ast=[Management.Automation.Language.Parser]::ParseFile(" + ps_quote(source / "install.ps1") + ",[ref]$null,[ref]$null); "
        "$function=$ast.Find({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'TreeDigest'},$true); "
        "if ($null -eq $function) { throw 'Missing actual frontdoor digest implementation' }; "
        ". ([scriptblock]::Create($function.Extent.Text)); "
        "$receipt=Get-Content -LiteralPath " + ps_quote(receipt_path) + " -Raw | ConvertFrom-Json; "
        "$rows=New-Object 'Collections.Generic.Dictionary[string,object]' ([StringComparer]::Ordinal); "
        "foreach($row in $receipt.files){ $rows.Add($row.path,$row) }; "
        "$actual=TreeDigest $rows; if ($actual -cne " + ps_quote(expected_digest) + ") { throw ('Packager/frontdoor tree wire mismatch: '+$actual) }; "
        "Write-Output $actual"
    )
    verified = run_ps(script, env)
    assert verified.returncode == 0, verified.stdout + verified.stderr
    assert verified.stdout.strip() == expected_digest
