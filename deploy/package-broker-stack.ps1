<#
.SYNOPSIS
    Build the curated agent-messaging deployment bundle from a literal committed tree.

.DESCRIPTION
    deploy/release-files.json explicitly declares all static package members: broker
    service/operators, the six-file Claude plugin, relay skills and helpers, relay
    library, license and installers. No live home, environment, settings or policy
    files are packaging inputs. Runtime state never enters the bundle.

    The executing packager must equal its committed generation policy. Every static
    export and staged member must retain its raw committed blob and Git mode; archive
    attributes that transform or omit declared sources are refused. Output must be
    fresh and disjoint from source worktrees and installed harness trees. Every staged
    and ZIP member is checked against the declaration and generated envelope.
    manifest.json and VERSION.txt retain the payload tree-hash/version contract.

    A ZIP is accompanied by a source-bound JSON receipt with the repository, release
    version, literal source commit, archive checksum and exact normalized file list.
    Releases publish agent-broker-deploy-<version>.zip, that name plus .json, and the
    committed root install.ps1. Publication requires authenticated gh; installation
    from public release assets does not require repository access or authentication.

.PARAMETER SourceBrokerRoot
    Repository root to package. Defaults to the parent of this script's deploy directory.
    ReleaseTarget must equal its full HEAD OID; dirty adjacent files cannot alter payload bytes.
.PARAMETER OutDir
    Fresh absolute local bundle directory. Existing outputs, source overlaps and reparse
    ancestors are refused. Default: $env:LOCALAPPDATA\agent-broker-deploy.
.PARAMETER GitPush
    Optional remote URL/name for publishing the fresh bundle as a new Git repository.
.PARAMETER Zip
    Retain <OutDir>.zip and <OutDir>.zip.json beside the bundle directory.
.PARAMETER BundleVersion
    Exact yyyy.MM.dd.HHmmss release coordinate. Defaults to packaging time.
.PARAMETER ReleaseNotice
    Optional one-line notice. Such releases are prereleases excluded from latest selection.
.PARAMETER NoRelease
    Build locally without publishing to GitHub Releases.
.PARAMETER ForceRelease
    Publish unchanged content under a fresh coordinate instead of skipping it.
.PARAMETER PromoteHeldTree
    Explicitly permit a stable release of content already held in a prerelease.
.PARAMETER ReleaseRepo
    Public release OWNER/REPO. Default: gupta-ash/agent-messaging.
.PARAMETER ReleaseTarget
    Exact full lowercase commit SHA; if supplied, it must equal packaged HEAD.

.EXAMPLE
    powershell -NoProfile -File .\deploy\package-broker-stack.ps1 -NoRelease -Zip -OutDir S:\releases\candidate
#>

[CmdletBinding()]
param(
    # Resolve in the body because PSScriptRoot is unavailable during parameter binding.
    [string]$SourceBrokerRoot = '',
    [string]$OutDir = "$env:LOCALAPPDATA\agent-broker-deploy",
    [string]$GitPush,
    [string]$BundleVersion = '',
    [string]$ReleaseNotice = '',
    [switch]$Zip,
    # A normal run publishes a release; local-only builds must explicitly use NoRelease.
    [switch]$NoRelease,
    # Force a Release publish even when the tree is unchanged (skip-if-unchanged is bypassed). Use to
    # re-cut the ASSET for identical content -- e.g. an asset-format fix or a corrupted asset -- under
    # a fresh tag. Tag collisions fail closed and never clobber an existing Release.
    [switch]$ForceRelease,
    [switch]$PromoteHeldTree,
    [string]$ReleaseRepo = 'gupta-ash/agent-messaging',
    [string]$ReleaseTarget = ''
)
# Native PS 5.1 must not autoload a caller's incompatible PowerShell 7 modules.
$savedModulePath=[Environment]::GetEnvironmentVariable('PSModulePath','Process')
$savedModuleCache=[Environment]::GetEnvironmentVariable('PSModuleAnalysisCachePath','Process')
try {
$env:PSModulePath=[IO.Path]::Combine($PSHOME,'Modules')
$env:PSModuleAnalysisCachePath='NUL'

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Every static input comes from the committed archive, never a live runtime home.
$PythonTargetVersion = '3.12.x'

function Write-Step { param([string]$Msg) Write-Host "[package] $Msg" -ForegroundColor Cyan }
function Write-Info { param([string]$Msg) Write-Host "          $Msg" }

function New-PackageReceipt {
    param([string]$Archive, [string]$Receipt, [hashtable]$Files,
          [string]$Repository, [string]$Version, [string]$SourceCommit)
    Assert-PackageArchive -Archive $Archive -Expected $Files
    [string[]]$paths = @($Files.Keys)
    [Array]::Sort($paths, [StringComparer]::Ordinal)
    $members = @($paths | ForEach-Object {
        [ordered]@{ path = $_; sha256 = $Files[$_].Sha256.ToLowerInvariant(); mode = $Files[$_].Mode }
    })
    $receiptObject = [ordered]@{
        schema_version = 1
        component = 'agent-messaging'
        repository = $Repository
        version = $Version
        source_commit = $SourceCommit
        archive_sha256 = (Get-FileHash -LiteralPath $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
        files = $members
    }
    $json = ($receiptObject | ConvertTo-Json -Depth 6) + "`r`n"
    $stream = [IO.File]::Open($Receipt, [IO.FileMode]::CreateNew)
    try {
        $bytes = (New-Object Text.UTF8Encoding $false).GetBytes($json)
        $stream.Write($bytes, 0, $bytes.Length)
    } finally { $stream.Dispose() }
}

function Resolve-GitHubTagCommit {
    param([string]$Repository, [string]$Tag)
    $result = Invoke-Native -Exe 'gh' -Arguments @('api', "repos/$Repository/git/ref/tags/$Tag")
    $seen = @{}
    for ($depth = 0; $depth -lt 16; $depth++) {
        if ($result.ExitCode -ne 0) { return '' }
        try { $value = (@($result.Output) -join "`n") | ConvertFrom-Json }
        catch { return '' }
        if ($null -eq $value -or $value.PSObject.Properties.Name -notcontains 'object') { return '' }
        $object = $value.object
        if ($null -eq $object -or $object.PSObject.Properties.Name -notcontains 'sha' -or
            $object.PSObject.Properties.Name -notcontains 'type' -or $object.sha -cnotmatch '^[0-9a-f]{40}$') { return '' }
        if ($object.type -ceq 'commit') { return [string]$object.sha }
        if ($object.type -cne 'tag' -or $seen.ContainsKey($object.sha)) { return '' }
        $seen[$object.sha] = $true
        $result = Invoke-Native -Exe 'gh' -Arguments @('api', "repos/$Repository/git/tags/$($object.sha)")
    }
    return ''
}

function Invoke-Native {
    # Windows PowerShell 5.1 promotes redirected native stderr under ErrorActionPreference=Stop.
    # Capture stdout and status without allowing a failed probe to abort its caller.
    param([Parameter(Mandatory)][string]$Exe, [string[]]$Arguments = @())
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & $Exe @Arguments 2>$null
        return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $out }
    } finally { $ErrorActionPreference = $saved }
}

function Assert-RawCommittedFiles {
    param([string]$Root, [hashtable]$Files, [hashtable]$Sources)
    [string[]]$paths = @($Files.Keys)
    if (-not $paths.Count) { throw 'Raw committed source proof requires files.' }
    # Compute Git's exact raw blob object ID locally. This avoids native batch
    # argument/stdout framing while retaining the commit's ls-tree OID authority.
    $buffer = New-Object byte[] 65536
    foreach ($relative in $paths) {
        $binding = $Files[$relative]
        if (-not $Sources.ContainsKey($binding.Source)) {
            throw "Release source is not a regular committed file: $($binding.Source)"
        }
        $path = Join-Path $Root $relative
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Missing raw committed release source: $($binding.Source)"
        }
        $item = Get-Item -LiteralPath $path -Force
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw "Reparse release source: $($binding.Source)"
        }
        if ($binding.Mode -cne $Sources[$binding.Source].Mode) {
            throw "Release source mode differs from raw committed tree: $($binding.Source)"
        }
        $algorithm = [Security.Cryptography.SHA1]::Create()
        $nativePath = $path
        if ([IO.Path]::DirectorySeparatorChar -eq '\' -and -not $nativePath.StartsWith('\\?\')) {
            $nativePath = '\\?\' + $nativePath
        }
        $stream = [IO.File]::Open($nativePath, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        try {
            $prefix = [Text.Encoding]::ASCII.GetBytes(('blob ' + $stream.Length + [char]0))
            [void]$algorithm.TransformBlock($prefix, 0, $prefix.Length, $prefix, 0)
            while (($count = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                [void]$algorithm.TransformBlock($buffer, 0, $count, $buffer, 0)
            }
            [void]$algorithm.TransformFinalBlock((New-Object byte[] 0), 0, 0)
            $oid = ([BitConverter]::ToString($algorithm.Hash)).Replace('-', '').ToLowerInvariant()
        } finally { $stream.Dispose(); $algorithm.Dispose() }
        if ($oid -cne $Sources[$binding.Source].Oid) {
            throw "Release source differs from raw committed blob (archive attributes may transform bytes): $($binding.Source)"
        }
    }
}

function Assert-PackagePath {
    param([string]$Path, [string[]]$ProtectedRoots = @(), [switch]$MustBeAbsent)
    if ($Path -notmatch '^[A-Za-z]:[\\/]' -or $Path -match '[*?]' -or $Path.Substring(2).Contains(':')) {
        throw 'Package paths must be absolute local filesystem paths without wildcards or streams.'
    }
    if ($Path -match '[. ]([\\/]|$)' -or $Path -match '~[0-9]') {
        throw 'Package paths must not use trailing-dot/space or short-name aliases.'
    }
    $full = [IO.Path]::GetFullPath($Path).TrimEnd('\', '/')
    if ($full.Length -le 2) { throw 'A drive root is not a package destination.' }
    if ($full -match '(?i)(^|[\\/])(\.agents|\.claude|\.codex|\.omp)([\\/]|$)') {
        throw "Package output cannot use a source or installed harness tree: $full"
    }
    foreach ($root in $ProtectedRoots) {
        $root = [IO.Path]::GetFullPath($root).TrimEnd('\', '/')
        if ($full.Equals($root, [StringComparison]::OrdinalIgnoreCase) -or
            $full.StartsWith($root + '\', [StringComparison]::OrdinalIgnoreCase) -or
            $root.StartsWith($full + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw "Package output overlaps a source worktree: $full"
        }
    }
    $cursor = $full
    while ($cursor) {
        $item = $null
        try { $item = Get-Item -LiteralPath $cursor -Force -ErrorAction Stop }
        catch [System.Management.Automation.ItemNotFoundException] { }
        if ($null -ne $item) {
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Package path contains a reparse point: $cursor"
            }
            if ($cursor -ne $full -and -not $item.PSIsContainer) { throw "Package parent is not a directory: $cursor" }
            if ($cursor -eq $full -and $MustBeAbsent) { throw "Package output already exists; choose a fresh destination: $full" }
        }
        $cursor = Split-Path -Parent $cursor
    }
}

function Get-PackageFileMap {
    param([string]$Root, [hashtable]$Modes = @{})
    $rootItem = Get-Item -LiteralPath $Root -Force
    if (-not $rootItem.PSIsContainer -or ($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'Package root must be a regular directory.'
    }
    $prefix = $rootItem.FullName.TrimEnd('\', '/') + '\'
    $files = @{}
    $pending = New-Object 'System.Collections.Generic.Stack[string]'
    $pending.Push($rootItem.FullName)
    while ($pending.Count) {
        foreach ($item in Get-ChildItem -LiteralPath $pending.Pop() -Force) {
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Reparse package member: $($item.FullName)" }
            if ($item.PSIsContainer) { $pending.Push($item.FullName); continue }
            $relative = $item.FullName.Substring($prefix.Length).Replace('\', '/')
            if ($files.ContainsKey($relative)) { throw "Case-colliding package member: $relative" }
            $files[$relative] = [pscustomobject]@{
                Path = $item.FullName
                Sha256 = (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash
                Mode = if ($Modes.ContainsKey($relative)) { $Modes[$relative] } else { '100644' }
            }
        }
    }
    if (-not $files.Count) { throw 'Empty package payload.' }
    return $files
}

function Assert-PackageArchive {
    param([string]$Archive, [hashtable]$Expected)
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zipFile = [IO.Compression.ZipFile]::OpenRead($Archive)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $seen = @{}
        foreach ($entry in $zipFile.Entries) {
            $name = $entry.FullName
            if (($name -split '/') -contains '' -or ($name -split '/') -contains '.' -or
                ($name -split '/') -contains '..' -or $name.Contains('\') -or $name.Contains(':')) {
                throw "Non-normalized archive member: $name"
            }
            if (-not $Expected.ContainsKey($name) -or $seen.ContainsKey($name) -or
                $name -cne $Expected[$name].Path.Substring($Expected[$name].Path.Length - $name.Length).Replace('\', '/')) {
                throw "Unexpected, duplicate or mis-cased archive member: $name"
            }
            $stream = $entry.Open()
            try { $digest = [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-', '') }
            finally { $stream.Dispose() }
            if ($digest -ne $Expected[$name].Sha256) { throw "Archive byte mismatch: $name" }
            $mode = ($entry.ExternalAttributes -shr 16) -band 0xffff
            if ($mode -ne [Convert]::ToInt32($Expected[$name].Mode, 8)) { throw "Archive mode mismatch: $name" }
            $seen[$name] = $true
        }
        if ($seen.Count -ne $Expected.Count) { throw 'Archive membership is incomplete.' }
    } finally { $sha.Dispose(); $zipFile.Dispose() }
}

function New-PackageArchive {
    param([string]$Archive, [hashtable]$Expected)
    Add-Type -AssemblyName System.IO.Compression, System.IO.Compression.FileSystem
    $stream = [IO.File]::Open($Archive, [IO.FileMode]::CreateNew)
    try {
        $zipFile = New-Object IO.Compression.ZipArchive($stream, [IO.Compression.ZipArchiveMode]::Create, $true)
        try {
            [string[]]$names = @($Expected.Keys)
            [Array]::Sort($names, [StringComparer]::Ordinal)
            foreach ($name in $names) {
                $zipEntry = [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zipFile, $Expected[$name].Path, $name)
                $zipEntry.ExternalAttributes = [Convert]::ToInt32($Expected[$name].Mode, 8) -shl 16
            }
        } finally { $zipFile.Dispose() }
    } finally { $stream.Dispose() }
    Assert-PackageArchive -Archive $Archive -Expected $Expected
}

# --------------------------------------------------------------------------------------
# 0. Resolve + validate sources
# --------------------------------------------------------------------------------------
$DeployDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# The default follows this repository across relocations.
if ([string]::IsNullOrWhiteSpace($SourceBrokerRoot)) {
    $SourceBrokerRoot = Split-Path -Parent $DeployDir
}
$SourceBrokerRoot = (Resolve-Path -LiteralPath $SourceBrokerRoot).Path

Write-Step "Source broker root : $SourceBrokerRoot"
Write-Step "Output bundle dir  : $OutDir"


# --------------------------------------------------------------------------------------
# 0.5 Export the literal committed tree; never fall back to worktree bytes.
# --------------------------------------------------------------------------------------
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "git is not on PATH, and the bundle is built from the COMMITTED tree ('git archive HEAD') so that a Release is reproducible from a commit. There is no worktree fallback. Install git on the build host and re-run."
}

$gitTop = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'rev-parse', '--show-toplevel')
if ($gitTop.ExitCode -ne 0) {
    throw "-SourceBrokerRoot is not inside a git repository: $SourceBrokerRoot ('git rev-parse --show-toplevel' exited $($gitTop.ExitCode)). The bundle is built from 'git archive HEAD'; refusing to package an unversioned tree."
}
$repoTop = ((@($gitTop.Output) -join '').Trim() -replace '/', '\').TrimEnd('\')
if ($repoTop -ne $SourceBrokerRoot.TrimEnd('\')) {
    throw "-SourceBrokerRoot ($SourceBrokerRoot) is not the ROOT of its git repository (root is $repoTop). 'git archive HEAD' always exports from the repository root, so packaging a subdirectory would export the wrong layout. Pass the repository root as -SourceBrokerRoot."
}

$gitHead = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'rev-parse', 'HEAD')
if ($gitHead.ExitCode -ne 0) {
    throw "Could not resolve HEAD in $SourceBrokerRoot ('git rev-parse HEAD' exited $($gitHead.ExitCode)); an unborn branch has nothing to package. Commit first."
}
$PackagedCommit = (@($gitHead.Output) -join '').Trim()
if ($PackagedCommit -notmatch '^[0-9a-f]{40}$') {
    throw "'git rev-parse HEAD' did not return a 40-char sha for $SourceBrokerRoot (got '$PackagedCommit'). Refusing to package a tree whose commit cannot be recorded."
}
if ($ReleaseTarget) {
    if ($ReleaseTarget -ne $ReleaseTarget.Trim() -or $ReleaseTarget -notmatch '^[0-9a-f]{40}$') {
        throw "ReleaseTarget must be the exact full lowercase 40-char packaged commit SHA."
    }
    if ($ReleaseTarget -ne $PackagedCommit) {
        throw "ReleaseTarget $ReleaseTarget does not equal packaged commit $PackagedCommit. Refusing a tag/source split."
    }
} else {
    $ReleaseTarget = $PackagedCommit
}
if ($ReleaseRepo -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') {
    throw 'ReleaseRepo must be an OWNER/REPO coordinate.'
}

# The running generation policy itself must match the literal selected revision.
$policyBlob = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'rev-parse', "${PackagedCommit}:deploy/package-broker-stack.ps1")
$runningBlob = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'hash-object', '--no-filters', '--', $PSCommandPath)
if ($policyBlob.ExitCode -ne 0 -or $runningBlob.ExitCode -ne 0 -or
    ((@($policyBlob.Output) -join '').Trim() -ne (@($runningBlob.Output) -join '').Trim())) {
    throw 'Executing packager differs from the accepted committed generation policy.'
}
$sourceModes = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, '-c', 'core.quotePath=false', 'ls-tree', '-r', $PackagedCommit)
if ($sourceModes.ExitCode -ne 0 -or @($sourceModes.Output | Where-Object { $_ -match '^(120000|160000) ' }).Count -gt 0) {
    throw 'Release source must contain regular Git files, not symlinks or submodules.'
}
$sourceFileMap = @{}
foreach ($line in @($sourceModes.Output)) {
    if ([string]$line -match '^(100644|100755) blob ([0-9a-f]{40})\t(.+)$') {
        $sourceFileMap[$Matches[3]] = [pscustomobject]@{ Mode = $Matches[1]; Oid = $Matches[2] }
    }
}

# Fail before creating even temporary output. Every registered worktree includes the
# canonical checkout; an existing bundle (including .git) is never adoption evidence.
$worktrees = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, '-c', 'core.quotePath=false', 'worktree', 'list', '--porcelain')
if ($worktrees.ExitCode -ne 0) { throw 'Cannot identify protected source worktrees.' }
$ProtectedPackageRoots = @($SourceBrokerRoot, (Split-Path -Parent $DeployDir))
foreach ($line in @($worktrees.Output)) {
    if ($line.StartsWith('worktree ')) { $ProtectedPackageRoots += $line.Substring(9) }
}
Assert-PackagePath -Path $OutDir -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
$OutDir = [IO.Path]::GetFullPath($OutDir).TrimEnd('\', '/')
if ($Zip) {
    Assert-PackagePath -Path "$OutDir.zip" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
    Assert-PackagePath -Path "$OutDir.zip.json" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
}

$packagedPathRe = '^(?:[^/]+|(?:src|harness|skills|deploy)/.+)$'
# Dirty-worktree warnings compare content blobs, not git status' stat-cache hints.
$headBlob = @{}
$dirtyCheckOk = $true
$gitLsTree = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'ls-tree', '-r', '--full-tree', 'HEAD')
if ($gitLsTree.ExitCode -ne 0) {
    $dirtyCheckOk = $false
} else {
    foreach ($ln in @($gitLsTree.Output)) {
        if (([string]$ln) -match '^\s*\d+\s+blob\s+([0-9a-f]{40})\t(.+)$') {
            # Capture BEFORE the next -match: an -match anywhere REPLACES $Matches wholesale, and
            # $packagedPathRe captures nothing, so reading $Matches[2] after it yields $null.
            $blobSha = $Matches[1]
            $blobPath = $Matches[2]
            if ($blobPath -match $packagedPathRe) { $headBlob[$blobPath] = $blobSha }
        }
    }
}
$trackedPaths = @($headBlob.Keys)
$presentPaths = @($trackedPaths | Where-Object {
        Test-Path -LiteralPath (Join-Path $SourceBrokerRoot ($_ -replace '/', '\')) -PathType Leaf
    })
$deletedPaths = @($trackedPaths | Where-Object { $presentPaths -notcontains $_ })
$changedPaths = @()
if ($dirtyCheckOk -and $presentPaths.Count -gt 0) {
    # Hash all present paths in one native call, preserving their argument order.
    $gitHashes = Invoke-Native -Exe 'git' -Arguments (@('-C', $SourceBrokerRoot, 'hash-object', '--no-filters', '--') + $presentPaths)
    $hashOut = @($gitHashes.Output)
    if ($gitHashes.ExitCode -eq 0 -and $hashOut.Count -eq $presentPaths.Count) {
        for ($i = 0; $i -lt $presentPaths.Count; $i++) {
            if ((([string]$hashOut[$i]).Trim()) -ne $headBlob[$presentPaths[$i]]) { $changedPaths += $presentPaths[$i] }
        }
    } else {
        $dirtyCheckOk = $false
    }
}
$untrackedPaths = @()
$gitOthers = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'ls-files', '--others', '--exclude-standard')
if ($gitOthers.ExitCode -ne 0) {
    $dirtyCheckOk = $false
} else {
    $untrackedPaths = @(@($gitOthers.Output) |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { $_ -and ($_ -match $packagedPathRe) })
}

if (-not $dirtyCheckOk) {
    Write-Warning "Could not compare the worktree against HEAD (a git probe failed). Packaging HEAD $PackagedCommit regardless -- any uncommitted edits are NOT in this bundle."
} elseif ($changedPaths.Count -gt 0 -or $deletedPaths.Count -gt 0 -or $untrackedPaths.Count -gt 0) {
    $dirty = @("Worktree differs from HEAD in paths this bundle carries. The bundle is built from COMMITTED bytes (HEAD $PackagedCommit), so NONE of the differences below are in it:")
    foreach ($p in @($changedPaths | Sort-Object))   { $dirty += "  MODIFIED  $p   -> packaged at its HEAD content" }
    foreach ($p in @($deletedPaths | Sort-Object))   { $dirty += "  DELETED   $p   -> still packaged, from HEAD" }
    foreach ($p in @($untrackedPaths | Sort-Object)) { $dirty += "  UNTRACKED $p   -> NOT packaged" }
    $dirty += "Commit them and re-run if they were meant to ship."
    Write-Warning ($dirty -join "`n")
} else {
    Write-Info "Worktree matches HEAD across the packaged paths (HEAD blob vs git hash-object --no-filters; git status' stat-cache flags are not evidence and were not used)."
}

# Export HEAD via a zip on disk. NOT `git archive --format=tar | tar -x`: a PowerShell pipeline
# turns a native command's stdout into decoded STRINGS, which corrupts a binary stream. Letting
# git write the file itself keeps the bytes intact.
$Export = Join-Path ([System.IO.Path]::GetTempPath()) ("agent-broker-deploy-export-" + [Guid]::NewGuid().ToString('N'))
$ExportZip = "$Export.zip"
Assert-PackagePath -Path $Export -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
Assert-PackagePath -Path $ExportZip -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
New-Item -ItemType Directory -Path $Export -Force | Out-Null
$gitArchive = Invoke-Native -Exe 'git' -Arguments @('-C', $SourceBrokerRoot, 'archive', '--format=zip', '-o', $ExportZip, $PackagedCommit)
if ($gitArchive.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $ExportZip)) {
    Remove-Item -LiteralPath $Export -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $ExportZip -Force -ErrorAction SilentlyContinue
    throw "'git archive HEAD' failed (exit $($gitArchive.ExitCode)) for $SourceBrokerRoot. The bundle is built from the committed tree and has no worktree fallback; refusing to publish."
}
try {
    # Git ZIPs encode ordinary 100644 files with zero external attributes; executable
    # files carry their full POSIX mode. Preserve that archive mode evidence on Windows.
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $exportModes = @{}
    $sourceArchive = [IO.Compression.ZipFile]::OpenRead($ExportZip)
    try {
        foreach ($member in $sourceArchive.Entries) {
            if ($member.FullName.EndsWith('/')) { continue }
            if ($exportModes.ContainsKey($member.FullName)) { throw "Duplicate exported source: $($member.FullName)" }
            $mode = ($member.ExternalAttributes -shr 16) -band 0xffff
            $exportModes[$member.FullName] = if ($mode -eq 0) { '100644' } else { [Convert]::ToString($mode, 8) }
        }
    } finally { $sourceArchive.Dispose() }
    Expand-Archive -LiteralPath $ExportZip -DestinationPath $Export -Force
} catch {
    Remove-Item -LiteralPath $Export -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $ExportZip -Force -ErrorAction SilentlyContinue
    throw "Extracting the 'git archive HEAD' export failed ($ExportZip -> $Export): $($_.Exception.Message). Refusing to publish."
}
Remove-Item -LiteralPath $ExportZip -Force -ErrorAction SilentlyContinue
if (-not (Test-Path -LiteralPath (Join-Path $Export 'src/broker/server.py'))) {
    Remove-Item -LiteralPath $Export -Recurse -Force -ErrorAction SilentlyContinue
    throw "The 'git archive HEAD' export at $Export does not contain server.py, so the extract did not produce the broker tree. Refusing to publish."
}

# From here on, EVERY repo-sourced copy reads the committed export instead of the worktree.
$SourceBrokerRoot = (Resolve-Path -LiteralPath $Export).Path
$InstallerSrc = Join-Path $SourceBrokerRoot 'install.ps1'
# The declaration itself must be raw-source-proved before it can choose package inputs.
$declarationProof = @{
    'deploy/release-files.json' = [pscustomobject]@{
        Source = 'deploy/release-files.json'; Mode = $exportModes['deploy/release-files.json']
    }
}
Assert-RawCommittedFiles -Root $SourceBrokerRoot -Files $declarationProof -Sources $sourceFileMap
Write-Step "Packaging committed tree : HEAD $PackagedCommit"
Write-Info "export dir         : $SourceBrokerRoot"

# --------------------------------------------------------------------------------------
# 1. Build in disposable storage; only a complete package can enter a fresh OutDir.
# --------------------------------------------------------------------------------------
$Stage = Join-Path ([System.IO.Path]::GetTempPath()) ("agent-broker-deploy-stage-" + [Guid]::NewGuid().ToString('N'))
Assert-PackagePath -Path $Stage -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
Assert-PackagePath -Path "$Stage.zip" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
Assert-PackagePath -Path "$Stage.zip.json" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
New-Item -ItemType Directory -Path $Stage -Force | Out-Null
$BundleBroker = Join-Path $Stage 'broker'
$BundleHome = Join-Path $Stage 'home'
New-Item -ItemType Directory -Path $BundleBroker -Force | Out-Null
New-Item -ItemType Directory -Path $BundleHome -Force | Out-Null

# The declaration is the only static membership source; never recursively copy a source tree.
$releaseDeclaration = Get-Content -LiteralPath (Join-Path $SourceBrokerRoot 'deploy/release-files.json') -Raw | ConvertFrom-Json
if (@($releaseDeclaration.PSObject.Properties).Count -ne 2 -or
    $releaseDeclaration.schema_version -ne 1 -or @($releaseDeclaration.entries).Count -eq 0) {
    throw 'Invalid or empty release declaration.'
}
$destinations = @{}
$packageModes = @{}
$exportFiles = @{}
$staticFiles = @{}
foreach ($entry in $releaseDeclaration.entries) {
    if (@($entry.PSObject.Properties).Count -ne 2 -or $entry.source -isnot [string] -or $entry.destination -isnot [string]) {
        throw 'Release entries must contain exactly source and destination strings.'
    }
    foreach ($rel in @([string]$entry.source, [string]$entry.destination)) {
        if ([string]::IsNullOrWhiteSpace($rel) -or [IO.Path]::IsPathRooted($rel) -or
            $rel -match '[\x00-\x1f:*?"<>|\\]' -or ($rel -split '/') -contains '' -or
            ($rel -split '/') -contains '.' -or ($rel -split '/') -contains '..' -or $rel -match '[. ](/|$)') {
            throw "Unsafe or non-normalized release path: $rel"
        }
    }
    $key = ([string]$entry.destination).ToLowerInvariant()
    if (-not ($key.StartsWith('broker/') -or $key.StartsWith('home/.claude/plugins/local-relay-channel/') -or
        $key -match '^home/\.agents/skills/relay-(check|name|send)/(skill\.md|scripts/(check|set_name|send)\.py)$' -or
        $key -match '^home/\.claude/hooks/relay/(shared\.py|broker_client\.py|broker_connection\.py|register_session\.py|registry_gc\.py|bin/cc_channel\.bat)$' -or
        $key -in @('license', 'install.ps1', 'deploy/release-files.json', 'deploy/install-broker-stack.ps1',
                   'deploy/install_runtime.py', 'deploy/adoption.json', 'deploy/retirements.json', 'deploy/runbook.md'))) {
        throw "Release declaration claims an unowned destination: $key"
    }
    if ($key -match '(^|/)(tests|history|__pycache__|\.venv|logs|audit|policy|vps)(/|$)' -or $key -match '(\.db[^/]*|\.pyc|/broker\.env(?:\.example)?)$') {
        throw "Source-only or private state cannot enter a release: $key"
    }
    if ($destinations.ContainsKey($key)) { throw "Duplicate release destination: $key" }
    $destinations[$key] = $true
    if (-not $sourceFileMap.ContainsKey($entry.source)) { throw "Release source is not a regular committed file: $($entry.source)" }
    $packageModes[[string]$entry.destination] = $sourceFileMap[$entry.source].Mode
    $exportFiles[[string]$entry.source] = [pscustomobject]@{
        Source = [string]$entry.source; Mode = $exportModes[$entry.source]
    }
    $staticFiles[[string]$entry.destination] = [pscustomobject]@{
        Source = [string]$entry.source; Mode = $packageModes[[string]$entry.destination]
    }
}
Assert-RawCommittedFiles -Root $SourceBrokerRoot -Files $exportFiles -Sources $sourceFileMap
foreach ($entry in $releaseDeclaration.entries) {
    $src = Join-Path $SourceBrokerRoot $entry.source
    $dst = Join-Path $Stage $entry.destination
    New-Item -ItemType Directory -Path (Split-Path -Parent $dst) -Force | Out-Null
    Copy-Item -LiteralPath $src -Destination $dst -Force
}
$homeRequired = @('.claude-plugin/plugin.json', '.mcp.json', 'server.py', 'cc_channel', 'cc_channel.bat', 'README.md')
$pluginDst = Join-Path $BundleHome '.claude/plugins/local-relay-channel'
$homeActual = @(Get-ChildItem -LiteralPath $pluginDst -Recurse -File -Force)
if ($homeActual.Count -ne 6) { throw 'Release must carry exactly six Claude plugin files.' }
foreach ($relative in $homeRequired) {
    if (-not (Test-Path -LiteralPath (Join-Path $pluginDst $relative) -PathType Leaf)) { throw "Missing plugin file: $relative" }
}

$requiredStatic = @('LICENSE', 'install.ps1', 'deploy/release-files.json', 'deploy/install-broker-stack.ps1',
    'deploy/install_runtime.py', 'deploy/adoption.json', 'deploy/retirements.json', 'deploy/RUNBOOK.md',
    'broker/pyproject.toml',
    'home/.agents/skills/relay-check/SKILL.md', 'home/.agents/skills/relay-check/scripts/check.py',
    'home/.agents/skills/relay-name/SKILL.md', 'home/.agents/skills/relay-name/scripts/set_name.py',
    'home/.agents/skills/relay-send/SKILL.md', 'home/.agents/skills/relay-send/scripts/send.py',
    'home/.claude/hooks/relay/shared.py', 'home/.claude/hooks/relay/broker_client.py',
    'home/.claude/hooks/relay/broker_connection.py',
    'home/.claude/hooks/relay/register_session.py', 'home/.claude/hooks/relay/registry_gc.py',
    'home/.claude/hooks/relay/bin/cc_channel.bat')
foreach ($required in $requiredStatic) {
    if (-not $destinations.ContainsKey($required)) { throw "Missing required release destination: $required" }
}
foreach ($rootSource in @('LICENSE', 'install.ps1', 'deploy/release-files.json', 'deploy/install-broker-stack.ps1',
                         'deploy/install_runtime.py', 'deploy/adoption.json', 'deploy/retirements.json', 'deploy/RUNBOOK.md')) {
    $binding = @($releaseDeclaration.entries | Where-Object { $_.destination -ceq $rootSource })
    if ($binding.Count -ne 1 -or $binding[0].source -cne $rootSource) { throw "Release frontdoor/source binding differs: $rootSource" }
}
$projectBinding = @($releaseDeclaration.entries | Where-Object { $_.destination -ceq 'broker/pyproject.toml' })
if ($projectBinding.Count -ne 1 -or $projectBinding[0].source -cne 'pyproject.toml') {
    throw 'Runtime project metadata must come from the committed root pyproject.toml.'
}

# Generated envelope, excluded from the static source declaration.

$gitignore = @'
# Runtime state must never enter a deployment package.
.venv/
__pycache__/
*.pyc
*.db
*.db-shm
*.db-wal
coordination.db*
broker.env
.pytest_cache/
'@
Set-Content -Path (Join-Path $Stage '.gitignore') -Value $gitignore -Encoding ascii -Force

# --------------------------------------------------------------------------------------
# 6. manifest.json  -  version stamp + tree SHA-256 (over the code payload)
# --------------------------------------------------------------------------------------
Write-Step "Computing tree SHA-256 + writing manifest.json..."

# Stable CRLF hash wire format over lowercased ordinal paths and per-file SHA-256.
# Only the root manifest/version envelope is excluded from the payload tree hash.
function Get-TreeSha256 {
    param([string]$Root)
    $Root = (Get-Item -LiteralPath $Root).FullName.TrimEnd('\', '/')
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $filesByPath = @{}
        foreach ($file in Get-ChildItem -LiteralPath $Root -Recurse -File -Force) {
            $relative = ($file.FullName.Substring($Root.Length).TrimStart('\', '/') -replace '\\', '/').ToLowerInvariant()
            if ($relative -in @('manifest.json', 'version.txt') -or $relative.StartsWith('.git/')) { continue }
            if ($filesByPath.ContainsKey($relative)) { throw "Case-colliding payload path: $relative" }
            $filesByPath[$relative] = $file.FullName
        }
        [string[]]$paths = @($filesByPath.Keys)
        [Array]::Sort($paths, [StringComparer]::Ordinal)
        $builder = [System.Text.StringBuilder]::new()
        foreach ($relative in $paths) {
            $stream = [IO.File]::OpenRead($filesByPath[$relative])
            try { $digest = ([BitConverter]::ToString($sha.ComputeHash($stream)) -replace '-', '').ToLowerInvariant() }
            finally { $stream.Dispose() }
            # Fixed CRLF preserves the Windows hash wire format without host culture/newline dependence.
            [void]$builder.Append("$relative`:$digest`r`n")
        }
        $bytes = [Text.Encoding]::UTF8.GetBytes($builder.ToString())
        return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace '-', '').ToLowerInvariant()
    }
    finally { $sha.Dispose() }
}

$treeSha = Get-TreeSha256 -Root $Stage
$ExplicitReleaseCoordinate = [bool]$BundleVersion -or [bool]$ReleaseNotice
if ($BundleVersion) {
    if ($BundleVersion -ne $BundleVersion.Trim() -or $BundleVersion -notmatch '^\d{4}\.\d{2}\.\d{2}\.\d{6}$') {
        throw "BundleVersion must use exact yyyy.MM.dd.HHmmss form with no surrounding whitespace."
    }
} else {
    $BundleVersion = (Get-Date -Format 'yyyy.MM.dd.HHmmss')
}
if ($ReleaseNotice -match '[\r\n]' -or ($ReleaseNotice -and $ReleaseNotice -ne $ReleaseNotice.Trim())) {
    throw "ReleaseNotice must be one line with no surrounding whitespace."
}
if ($PromoteHeldTree -and $ReleaseNotice) {
    throw "PromoteHeldTree cannot be combined with ReleaseNotice: choose a held prerelease or an explicit stable promotion."
}

$manifest = [ordered]@{
    bundleVersion       = $bundleVersion
    createdUtc          = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    pythonTargetVersion = $PythonTargetVersion
    treeSha256          = $treeSha
    # This envelope is hash-neutral; every static member is bound to the literal source commit.
    packagedCommit      = $PackagedCommit
    releaseDeclarationSha256 = (Get-FileHash -LiteralPath (Join-Path $SourceBrokerRoot 'deploy/release-files.json') -Algorithm SHA256).Hash.ToLowerInvariant()
}
$manifestJson = $manifest | ConvertTo-Json -Depth 6
# UTF-8 without BOM; manifest and VERSION are independently bound by the archive receipt.
[System.IO.File]::WriteAllText((Join-Path $Stage 'manifest.json'), ($manifestJson + "`r`n"), (New-Object System.Text.UTF8Encoding $false))

# VERSION.txt line 1 = release coordinate; line 2 = reproducible payload tree hash.
Set-Content -Path (Join-Path $Stage 'VERSION.txt') -Value @($bundleVersion, $treeSha) -Encoding ascii -Force

Write-Info "bundleVersion : $bundleVersion"
Write-Info "treeSha256    : $treeSha"

# --------------------------------------------------------------------------------------
# 7. Publish to a fresh, disjoint OutDir; never replace or preserve an occupied tree.
# --------------------------------------------------------------------------------------
Write-Step "Publishing bundle to OutDir..."

$packageFiles = Get-PackageFileMap -Root $Stage -Modes $packageModes
$expectedNames = @($releaseDeclaration.entries | ForEach-Object { [string]$_.destination }) +
    @('.gitignore', 'manifest.json', 'VERSION.txt')
if ($packageFiles.Count -ne $expectedNames.Count) { throw 'Staged package membership differs from the declaration and envelope.' }
foreach ($name in $expectedNames) {
    if (-not $packageFiles.ContainsKey($name)) { throw "Missing staged package member: $name" }
}
# Every static destination retains the raw source blob and mode, not merely a hash of
# archive-transformed bytes. New-PackageArchive independently enforces those ZIP modes.
Assert-RawCommittedFiles -Root $Stage -Files $staticFiles -Sources $sourceFileMap
$PreparedZip = "$Stage.zip"
if ($Zip -or -not $NoRelease) {
    New-PackageArchive -Archive $PreparedZip -Expected $packageFiles
    New-PackageReceipt -Archive $PreparedZip -Receipt "$PreparedZip.json" -Files $packageFiles -Repository $ReleaseRepo -Version $BundleVersion -SourceCommit $PackagedCommit
}
Assert-PackagePath -Path $OutDir -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
if ($Zip) {
    Assert-PackagePath -Path "$OutDir.zip" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
    Assert-PackagePath -Path "$OutDir.zip.json" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
}

$OutParent = Split-Path -Parent $OutDir
if ($OutParent -and -not (Test-Path -LiteralPath $OutParent)) {
    New-Item -ItemType Directory -Path $OutParent -Force | Out-Null
}

# Copy explicit files into a fresh sibling, verify them, then use a non-overwriting
# directory rename. No foreign directory is renamed, recursively deleted or adopted.
# Release asset names are independent of the operator's local staging directory name.
$ReleaseAssetStem = 'agent-broker-deploy'

$leaf = Split-Path -Leaf $OutDir
$publishTmp = Join-Path $OutParent ("$leaf.publish-tmp-" + [Guid]::NewGuid().ToString('N'))
$publishCreated = $false
try {
    Assert-PackagePath -Path $publishTmp -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
    New-Item -ItemType Directory -Path $publishTmp | Out-Null
    $publishCreated = $true
    foreach ($name in $packageFiles.Keys) {
        $destination = Join-Path $publishTmp $name
        [void][IO.Directory]::CreateDirectory((Split-Path -Parent $destination))
        [IO.File]::Copy($packageFiles[$name].Path, $destination, $false)
    }
    $copiedFiles = Get-PackageFileMap -Root $publishTmp
    if ($copiedFiles.Count -ne $packageFiles.Count) { throw 'Output copy membership mismatch.' }
    foreach ($name in $packageFiles.Keys) {
        if (-not $copiedFiles.ContainsKey($name) -or $copiedFiles[$name].Sha256 -ne $packageFiles[$name].Sha256) {
            throw "Output copy byte mismatch: $name"
        }
    }
    Assert-PackagePath -Path $OutDir -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
    [IO.Directory]::Move($publishTmp, $OutDir)
} catch {
    # Only the uniquely named directory created by this invocation is eligible for cleanup.
    if ($publishCreated -and (Test-Path -LiteralPath $publishTmp)) {
        Assert-PackagePath -Path $publishTmp -ProtectedRoots $ProtectedPackageRoots
        [void](Get-PackageFileMap -Root $publishTmp)
        Remove-Item -LiteralPath $publishTmp -Recurse -Force
    }
    throw "Publish to fresh OutDir failed; no existing output was replaced. Error: $($_.Exception.Message)"
}

# --------------------------------------------------------------------------------------
# 8. Optional zip
# --------------------------------------------------------------------------------------
if ($Zip) {
    Write-Step "Retaining validated zip..."
    $zipPath = "$OutDir.zip"
    Assert-PackagePath -Path $zipPath -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
    Assert-PackagePath -Path "$zipPath.json" -ProtectedRoots $ProtectedPackageRoots -MustBeAbsent
    [IO.File]::Copy($PreparedZip, $zipPath, $false)
    Assert-PackageArchive -Archive $zipPath -Expected $packageFiles
    [IO.File]::Copy("$PreparedZip.json", "$zipPath.json", $false)
    Write-Info "Zip: $zipPath"
    Write-Info "Receipt: $zipPath.json"
}

# --------------------------------------------------------------------------------------
# 8.5 GitHub Release publish; skip only complete unchanged releases.
# --------------------------------------------------------------------------------------
# The archive is wrapper-free. Publishing needs gh authentication, not installation.
$releaseResult = 'not attempted'
if ($NoRelease) {
    Write-Info "GitHub Release: skipped (-NoRelease)."
    $releaseResult = 'skipped (-NoRelease)'
} else {
    Write-Step "Publishing GitHub Release to $ReleaseRepo ..."
    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw 'GitHub Release publish requires authenticated gh on PATH. Use -NoRelease for a local-only bundle.'
    } else {
        # Check only the active account and never log authentication output.
        $ghAuth = Invoke-Native -Exe 'gh' -Arguments @('auth','status','--active')
        if ($ghAuth.ExitCode -ne 0) {
            throw "GitHub Release publish requires the active gh account to be authenticated (exit $($ghAuth.ExitCode)). Use -NoRelease for a local-only bundle."
        } else {
            # A hold attaches to CONTENT, not only one tag. GitHub's latest-release query excludes
            # prereleases, so checking only the latest stable would let a later default run publish
            # identical held bytes as stable. Inventory prereleases and refuse that promotion unless
            # the operator supplies the explicit promotion switch.
            $ghReleaseInventory = Invoke-Native -Exe 'gh' -Arguments @('api','--paginate','--slurp', "repos/$ReleaseRepo/releases?per_page=100")
            if ($ghReleaseInventory.ExitCode -ne 0) {
                throw "Cannot inspect existing prereleases for held content; refusing to publish toward an unknown hold state."
            }
            try {
                $releasePages = ((@($ghReleaseInventory.Output) -join "`n") | ConvertFrom-Json)
                $releaseInventory = @($releasePages | ForEach-Object { $_ })
            } catch {
                throw "GitHub release inventory was not valid JSON; refusing to publish toward an unknown hold state."
            }
            $heldTreeTags = @($releaseInventory | Where-Object {
                $_.prerelease -eq $true -and $_.body -and $_.body.ToLowerInvariant().Contains($treeSha.ToLowerInvariant())
            } | ForEach-Object { [string]$_.tag_name })
            # Body-only or ZIP-only matches cannot hide a partial three-asset release.
            $unchanged = $false
            $ghBody   = Invoke-Native -Exe 'gh' -Arguments @('release','view','--repo',$ReleaseRepo,'--json','body','--jq','.body')
            $rawBody  = $ghBody.Output
            $bodyExit = $ghBody.ExitCode
            $latestBody = (@($rawBody) -join "`n")
            # Keep the jq QUOTE-FREE: Windows PowerShell 5.1 mangles double quotes inside a native-command
            # argument, so a jq like join("|") reaches gh broken (parse error). Emit names line-per-line
            # (no quotes) and join in PowerShell instead.
            $ghAssets   = Invoke-Native -Exe 'gh' -Arguments @('release','view','--repo',$ReleaseRepo,'--json','assets','--jq','.assets[].name')
            $rawAssets  = $ghAssets.Output
            $assetsExit = $ghAssets.ExitCode
            $latestAssetNames = @($rawAssets | ForEach-Object { ([string]$_).Trim() })
            $hasSha = ($bodyExit -eq 0 -and $latestBody -and $latestBody.ToLowerInvariant().Contains($treeSha))
            $completeArchives = @($latestAssetNames | Where-Object {
                $_ -cmatch '^agent-broker-deploy-\d{4}\.\d{2}\.\d{2}\.\d{6}\.zip$' -and
                $latestAssetNames -ccontains "${_}.json"
            })
            $hasBundleAsset = ($assetsExit -eq 0 -and $completeArchives.Count -gt 0 -and $latestAssetNames -ccontains 'install.ps1')
            if ($hasSha -and $hasBundleAsset) {
                $unchanged = $true
            }
            if ($unchanged -and -not $ForceRelease -and -not $ExplicitReleaseCoordinate) {
                Write-Info "GitHub Release: latest already carries treeSha256 $treeSha; unchanged, skipping (pass -ForceRelease or an explicit -BundleVersion/-ReleaseNotice to cut a fresh release)."
                $releaseResult = 'skipped (unchanged)'
            } else {
                if ($unchanged) { Write-Info "treeSha256 unchanged but an explicit release coordinate/control or -ForceRelease requires a fresh wrapper-free asset under tag $BundleVersion." }
                # A default unchanged run skips above. Enforce a retained hold only when this run
                # would actually create a stable Release; explicit held recuts remain prereleases.
                if ($heldTreeTags.Count -gt 0 -and -not $ReleaseNotice -and -not $PromoteHeldTree) {
                    throw "treeSha256 $treeSha is held in prerelease tag(s) $($heldTreeTags -join ', '). Refusing a stable recut without -PromoteHeldTree."
                }
                if ($heldTreeTags.Count -gt 0 -and $PromoteHeldTree) {
                    Write-Info "Explicit -PromoteHeldTree authorizes a stable cut of treeSha256 $treeSha held by $($heldTreeTags -join ', ')."
                }
                # A bare pre-existing git tag makes GitHub ignore --target even when no Release exists.
                # Query matching refs (an absent exact ref is an empty JSON array with exit 0) so API,
                # auth, and network failures cannot be mistaken for a collision-free coordinate.
                $ghTagRefs = Invoke-Native -Exe 'gh' -Arguments @('api', "repos/$ReleaseRepo/git/matching-refs/tags/$BundleVersion")
                if ($ghTagRefs.ExitCode -ne 0) {
                    throw "Cannot verify whether tag $BundleVersion already exists; refusing to publish."
                }
                try {
                    $tagRefs = ((@($ghTagRefs.Output) -join "`n") | ConvertFrom-Json)
                } catch {
                    throw "GitHub tag inventory was not valid JSON; refusing to publish."
                }
                $exactTagRefs = @($tagRefs | Where-Object {
                    $null -ne $_ -and $_.PSObject.Properties.Name -contains 'ref' -and $_.ref -eq "refs/tags/$BundleVersion"
                })
                if ($exactTagRefs.Count -gt 0) {
                    throw "Tag $BundleVersion already exists. No Release or asset was created or clobbered."
                }
                # Reuse the already validated bytes, with the contracted wrapper-free asset name.
                $assetName = "$ReleaseAssetStem-$bundleVersion.zip"
                $assetPath = Join-Path $Export $assetName
                [IO.File]::Copy($PreparedZip, $assetPath, $false)
                Assert-PackageArchive -Archive $assetPath -Expected $packageFiles
                $receiptAssetPath = "$assetPath.json"
                [IO.File]::Copy("$PreparedZip.json", $receiptAssetPath, $false)
                $frontdoorProof = @{
                    'install.ps1' = [pscustomobject]@{ Source = 'install.ps1'; Mode = $exportModes['install.ps1'] }
                }
                Assert-RawCommittedFiles -Root $Export -Files $frontdoorProof -Sources $sourceFileMap
                if ((Get-FileHash -LiteralPath $InstallerSrc -Algorithm SHA256).Hash -ne $packageFiles['install.ps1'].Sha256) {
                    throw 'Release frontdoor asset differs from the committed package member.'
                }
                # Release notes carry treeSha256 (drives the NEXT run's skip-if-unchanged compare).
                # Passed via --notes-file: a multi-line --notes STRING is fragile under Windows
                # PowerShell 5.1 native-arg quoting (same hazard as the jq above).
                $notesLines = @(
                    "agent-messaging $BundleVersion",
                    ""
                )
                if ($ReleaseNotice) { $notesLines += @($ReleaseNotice, "") }
                $notesLines += @(
                    "bundleVersion = $BundleVersion (VERSION.txt L1)",
                    "treeSha256 = $treeSha (VERSION.txt L2)",
                    "asset = $assetName (no wrapper folder)",
                    "receipt = $assetName.json (source-bound SHA-256 and exact member list)",
                    'installer = install.ps1 (same committed bytes as the archive member)',
                    "source = $ReleaseRepo@$PackagedCommit",
                    "Auto-published by package-broker-stack.ps1 (skip-if-unchanged)."
                )
                $notes = $notesLines -join "`n"
                $notesFile = Join-Path $Export "agent-broker-relnotes-$bundleVersion.txt"
                [System.IO.File]::WriteAllText($notesFile, $notes, (New-Object System.Text.UTF8Encoding($false)))
                # A tag collision is not idempotency: clobbering only the asset leaves stale notes,
                # target, and hold semantics. Fail closed; an operator must inspect the existing
                # Release and choose a fresh coordinate or explicitly remove a known-bad cut.
                $createArgs = @('release','create',$BundleVersion,$assetPath,$receiptAssetPath,$InstallerSrc,'--repo',$ReleaseRepo,'--target',$ReleaseTarget,'--title',$BundleVersion,'--notes-file',$notesFile)
                if ($ReleaseNotice) { $createArgs += @('--prerelease','--latest=false') }
                $ghCreate   = Invoke-Native -Exe 'gh' -Arguments $createArgs
                $createOut  = $ghCreate.Output
                $createExit = $ghCreate.ExitCode
                foreach ($ln in @($createOut)) { Write-Info ([string]$ln) }
                if ($createExit -ne 0) {
                    Remove-Item -LiteralPath $assetPath -Force -ErrorAction SilentlyContinue
                    Remove-Item -LiteralPath $receiptAssetPath -Force -ErrorAction SilentlyContinue
                    Remove-Item -LiteralPath $notesFile -Force -ErrorAction SilentlyContinue
                    throw "GitHub Release publish FAILED for tag $BundleVersion. The tag or Release may already exist; no existing asset or notes were clobbered. Inspect the collision and use a fresh authorized coordinate, or explicitly remove a known-bad cut before retrying."
                }
                # GitHub can return the completed Release before its new tag is visible through the
                # git-ref API. Retry only that read; never retry or duplicate the create operation.
                $createdSha = ''
                for ($verifyAttempt = 0; $verifyAttempt -lt 10 -and -not $createdSha; $verifyAttempt++) {
                    if ($verifyAttempt -gt 0) { Start-Sleep -Milliseconds 500 }
                    $createdSha = Resolve-GitHubTagCommit -Repository $ReleaseRepo -Tag $BundleVersion
                }
                if ($createdSha -ne $PackagedCommit) {
                    # Make any unverifiable normal cut non-latest before failing. A noticed cut is
                    # already held, so the same command is idempotent there.
                    $demote = Invoke-Native -Exe 'gh' -Arguments @('release','edit',$BundleVersion,'--repo',$ReleaseRepo,'--prerelease','--latest=false')
                    $demotionVerified = $false
                    if ($demote.ExitCode -eq 0) {
                        $heldReadback = Invoke-Native -Exe 'gh' -Arguments @('release','view',$BundleVersion,'--repo',$ReleaseRepo,'--json','isPrerelease','--jq','.isPrerelease')
                        $demotionVerified = ($heldReadback.ExitCode -eq 0 -and ((@($heldReadback.Output) -join '').Trim().ToLowerInvariant() -eq 'true'))
                    }
                    Remove-Item -LiteralPath $assetPath -Force -ErrorAction SilentlyContinue
                    Remove-Item -LiteralPath $receiptAssetPath -Force -ErrorAction SilentlyContinue
                    Remove-Item -LiteralPath $notesFile -Force -ErrorAction SilentlyContinue
                    if ($demotionVerified) {
                        throw "Published tag $BundleVersion did not verify as exact packaged commit $PackagedCommit; Release was forced to a non-latest prerelease and requires operator repair."
                    }
                    throw "Published tag $BundleVersion did not verify as exact packaged commit $PackagedCommit, and safe demotion could not be verified. RELEASE MAY STILL BE LATEST; operator repair is required immediately."
                }
                Remove-Item -LiteralPath $assetPath -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $receiptAssetPath -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $notesFile -Force -ErrorAction SilentlyContinue
                if ($ReleaseNotice) {
                    Write-Info "GitHub held prerelease published: $ReleaseRepo tag $BundleVersion ($assetName); excluded from latest-release selection."
                    $releaseResult = "$ReleaseRepo $BundleVersion (prerelease hold)"
                } else {
                    Write-Info "GitHub Release published: $ReleaseRepo tag $BundleVersion ($assetName)."
                    $releaseResult = "$ReleaseRepo $BundleVersion"
                }
            }
        }
    }
}

# --------------------------------------------------------------------------------------
# 9. Optional git publish
# --------------------------------------------------------------------------------------
if ($GitPush) {
    Write-Step "Publishing to git remote '$GitPush'..."
    Push-Location $OutDir
    try {
        if (-not (Test-Path -LiteralPath (Join-Path $OutDir '.git'))) {
            & git init | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "git init failed (exit $LASTEXITCODE)." }
            # Use git's configured default branch (init.defaultBranch). We do NOT force a branch
            # name: forcing 'main' on a host whose bare-repo default HEAD is 'master' leaves the
            # remote HEAD pointing at a nonexistent ref (clone-checkout warning). The actual branch
            # name is irrelevant to the bundle; matching the host default keeps clones clean.
        }
        & git add -A | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "git add failed (exit $LASTEXITCODE)." }
        # Commit only if there is something staged (avoid 'nothing to commit' failure).
        $pending = & git status --porcelain
        if ($pending) {
            & git commit -m "broker stack bundle $bundleVersion" | Out-Null
            # MUST verify the commit succeeded: a failed commit (missing user.name/email, hook
            # failure, index lock) under native git does NOT throw, and we would otherwise push
            # the PREVIOUS HEAD -> the git channel silently carries a stale bundle.
            if ($LASTEXITCODE -ne 0) { throw "git commit failed (exit $LASTEXITCODE). Bundle was NOT committed; refusing to push a stale commit. Check git user.name/user.email and repo state." }
            Write-Info "Committed bundle $bundleVersion."
        } else {
            Write-Info "No changes to commit (bundle identical to last commit)."
        }
        # Resolve remote: treat $GitPush as a remote NAME if it already exists, else as a URL or
        # local path to wire up as 'origin'. A bare remote NAME is a simple token (no slash, no
        # backslash, no colon); anything else (https://, ssh://, file://, user@host:path, OR a
        # local/UNC filesystem path like E:/repos/x.git or \\server\share) is a clone target.
        $remotes = @(& git remote)
        $remoteName = $GitPush
        if ($remotes -notcontains $GitPush) {
            $looksLikeName = ($GitPush -notmatch '[/\\:]')
            if (-not $looksLikeName) {
                # URL or filesystem path  -  wire it up as 'origin'.
                if ($remotes -contains 'origin') {
                    & git remote set-url origin $GitPush | Out-Null
                } else {
                    & git remote add origin $GitPush | Out-Null
                }
                if ($LASTEXITCODE -ne 0) { throw "git remote add/set-url origin '$GitPush' failed (exit $LASTEXITCODE)." }
                $remoteName = 'origin'
            } else {
                throw "git remote '$GitPush' is not a configured remote name and is not a URL/path. Configure it (git remote add) or pass a clone URL/path."
            }
        }
        # Branch detection: --quiet --short avoids printing 'HEAD' on a detached/unborn ref.
        $branch = (& git symbolic-ref --quiet --short HEAD)
        if ($LASTEXITCODE -ne 0 -or -not $branch) { throw "Could not resolve a branch to push (no commits yet?). Bundle not pushed." }
        $branch = $branch.Trim()
        & git push -u $remoteName $branch
        if ($LASTEXITCODE -ne 0) { throw "git push to '$remoteName/$branch' failed (exit $LASTEXITCODE)." }
        Write-Info "Pushed to $remoteName/$branch."
    } finally {
        Pop-Location
    }
}

# --------------------------------------------------------------------------------------
# 10. Cleanup stage + summary
# --------------------------------------------------------------------------------------
Remove-Item -LiteralPath $Stage -Recurse -Force -ErrorAction SilentlyContinue
if (Test-Path -LiteralPath $PreparedZip) { Remove-Item -LiteralPath $PreparedZip -Force }
if (Test-Path -LiteralPath "$PreparedZip.json") { Remove-Item -LiteralPath "$PreparedZip.json" -Force }
# The committed-tree export from section 0.5 is temp too; drop it with the stage.
Remove-Item -LiteralPath $Export -Recurse -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Step "Bundle published."
Write-Info "OutDir        : $OutDir"
Write-Info "commit        : $PackagedCommit (bundle built from this commit's tree, not the worktree)"
Write-Info "bundleVersion : $bundleVersion"
if ($Zip)     { Write-Info "zip           : $OutDir.zip" }
if ($Zip)     { Write-Info "receipt       : $OutDir.zip.json" }
if ($GitPush) { Write-Info "git remote    : $GitPush" }
Write-Info "release       : $releaseResult"
Write-Host ""
Write-Info "Next: run install.ps1 -Version $BundleVersion -Mode Check with an explicit nonsecret -WorkRoot."
Write-Info 'See deploy/RUNBOOK.md for installation and opt-in harness activation.'
} finally {
    [Environment]::SetEnvironmentVariable('PSModulePath',$savedModulePath,'Process')
    [Environment]::SetEnvironmentVariable('PSModuleAnalysisCachePath',$savedModuleCache,'Process')
}
