#requires -Version 5.1
<#
.SYNOPSIS
Install or verify a public agent-messaging release for the invoking user.
.DESCRIPTION
Check downloads only into WorkRoot. Verify is read-only and never downloads.
Install never activates, registers autostart, or enables hooks by default.
#>
[CmdletBinding()]
param(
    [string]$Version,
    [string]$WorkRoot,
    [ValidateSet('Check','Install','Verify')][string]$Mode = 'Install',
    [string]$InstallRoot, [string]$PythonExe,
    [ValidateSet('none','claude','codex','omp')][string[]]$Harness,
    [ValidateRange(1,65535)][int]$Port,
    [string]$OmpCommand, [string]$OmpAgentRoot, [string]$OmpProfile,
    [string]$OmpPythonExe, [string]$RelayStateRoot, [string]$RelayRuntimeRoot,
    [switch]$Activate
)
# Match built-in modules to the executing shell even when launched from PS 7.
# Restore the caller's process environment on every return/refusal/exit path.
$savedModulePath=[Environment]::GetEnvironmentVariable('PSModulePath','Process')
$savedModuleCache=[Environment]::GetEnvironmentVariable('PSModuleAnalysisCachePath','Process')
try {
$env:PSModulePath=[IO.Path]::Combine($PSHOME,'Modules')
$env:PSModuleAnalysisCachePath='NUL'
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Repository = 'gupta-ash/agent-messaging'
$Component = 'agent-messaging'
function Hash([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Full([string]$Path) {
    if ($Path -notmatch '^[A-Za-z]:[\\/]' -or $Path -match '[*?]' -or $Path.Substring(2).Contains(':') -or $Path -match '(?:^|[\\/])(?:\.|\.\.)(?:[\\/]|$)' -or $Path -match '[. ](?:[\\/]|$)' -or $Path -match '~[0-9]') { throw 'Require an absolute local path without traversal, aliases, streams or wildcards.' }
    $result = [IO.Path]::GetFullPath($Path).TrimEnd('\','/')
    if ($result.Length -le 3) { throw 'A drive root is not an installation/work directory.' }
    foreach ($part in ($result.Substring(3) -split '[\\/]')) { if ($part -match '^(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)') { throw 'Reserved path member.' } }
    return $result
}
function Contains([string]$Root,[string]$Child) { $Root.Equals($Child,[StringComparison]::OrdinalIgnoreCase) -or $Child.StartsWith($Root+'\',[StringComparison]::OrdinalIgnoreCase) }
function Plain([string]$Path) {
    $cursor=$Path
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) { if ((Get-Item -Force -LiteralPath $cursor).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Reparse ancestry is not allowed: $cursor" } }
        $parent=Split-Path -Parent $cursor
        if ($parent -eq $cursor) { break }; $cursor=$parent
    }
}
function Owned([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $acl=Get-Acl -LiteralPath $Path
    if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -ne $sid) { throw "Directory must be owned by the invoking user: $Path" }
    foreach ($rule in $acl.Access) {
        if ($rule.AccessControlType -eq 'Allow' -and ($rule.FileSystemRights -band [Security.AccessControl.FileSystemRights]::Write) -and $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -notin @($sid,'S-1-5-18','S-1-5-32-544')) { throw "Directory grants another principal write access: $Path" }
    }
}
function PrivateDirectory([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { [IO.Directory]::CreateDirectory($Path) | Out-Null }
    $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $acl=New-Object Security.AccessControl.DirectorySecurity
    $acl.SetOwner((New-Object Security.Principal.SecurityIdentifier($sid)))
    $acl.SetAccessRuleProtection($true,$false)
    foreach ($id in @($sid,'S-1-5-18','S-1-5-32-544')) {
        $rule=New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier($id)),'FullControl','ContainerInherit,ObjectInherit','None','Allow')
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}
function Closed($Object,[string[]]$Keys) { if ((@($Object.PSObject.Properties.Name | Sort-Object) -join '|') -cne (@($Keys | Sort-Object) -join '|')) { throw 'Unexpected metadata fields.' } }
function Download([string]$Url,[string]$Destination) {
    if ($Url -notmatch '^https://(?:api\.github\.com|github\.com|objects\.githubusercontent\.com|release-assets\.githubusercontent\.com)/') { throw 'Only public GitHub HTTPS assets are accepted.' }
    Invoke-WebRequest -UseBasicParsing -Uri $Url -Headers @{'User-Agent'='agent-messaging-installer';'Accept'='application/vnd.github+json'} -OutFile $Destination
}
function TreeDigest($Expected) {
    $names=New-Object 'Collections.Generic.List[string]'
    $digests=@{}
    foreach ($key in $Expected.Keys) {
        if ($key -cnotin @('manifest.json','VERSION.txt')) {
            $lower=$key.ToLowerInvariant(); $names.Add($lower); $digests[$lower]=$Expected[$key].sha256
        }
    }
    $names.Sort([StringComparer]::Ordinal)
    $text=New-Object Text.StringBuilder
    foreach ($key in $names) { [void]$text.Append($key+':'+$digests[$key]+"`r`n") }
    $algorithm=[Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($text.ToString())))).Replace('-','').ToLowerInvariant() }
    finally { $algorithm.Dispose() }
}
if ($Version -and ($Version -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' -or $Version -in @('latest','HEAD','main','master'))) { throw 'Version must be an exact release tag when supplied.' }
$actualProfile=[Environment]::GetFolderPath('UserProfile')
$UserRoot=Full $actualProfile
if (-not $WorkRoot) {
    $local=Full ([Environment]::GetFolderPath('LocalApplicationData'))
    $WorkRoot=Join-Path $local ('agent-messaging/installer-work/'+[guid]::NewGuid().ToString('N'))
}
$WorkRoot=Full $WorkRoot
Plain $UserRoot; Plain $WorkRoot
$metadata=Join-Path $UserRoot '.agents/installations/agent-messaging'
$installed=Join-Path $metadata 'installed.json'
Plain $metadata; Plain $installed
$cursor=$metadata
while ($cursor -and (Contains $UserRoot $cursor)) { Owned $cursor; if ($cursor -ieq $UserRoot) { break }; $cursor=Split-Path -Parent $cursor }
Owned $installed
$prior=$null
if (Test-Path -LiteralPath $installed) {
    $prior=Get-Content -Raw -LiteralPath $installed | ConvertFrom-Json
    if ($prior.component -cne $Component -or $prior.repository -cne $Repository -or $prior.parameters.UserRoot -ine $UserRoot) { throw 'Retained installation identity differs from the invoking user/product.' }
}
if (-not $InstallRoot) { if ($prior) { $InstallRoot=[string]$prior.parameters.InstallRoot } else { $InstallRoot=Join-Path $UserRoot '.agents/services/broker' } }
$InstallRoot=Full $InstallRoot
Plain $InstallRoot
$sourceRoots=@()
$cursor=$PSScriptRoot
while ($cursor) { if (Test-Path -LiteralPath (Join-Path $cursor '.git')) { $sourceRoots+= $cursor; break }; $next=Split-Path -Parent $cursor; if ($next -eq $cursor) { break }; $cursor=$next }
foreach ($protected in (@($UserRoot+'\.agents',$UserRoot+'\.claude',$UserRoot+'\.codex',$UserRoot+'\.omp',$InstallRoot)+$sourceRoots)) {
    $protected=Full $protected
    if ((Contains $protected $WorkRoot) -or (Contains $WorkRoot $protected)) { throw 'WorkRoot overlaps source, installed code, configuration or state.' }
}
if ($WorkRoot -match '(?i)[\\/](?:ash_src|\.agents|\.claude|\.codex|\.omp)(?:[\\/]|$)') { throw 'WorkRoot cannot be a source or installed tree.' }
if ($InstallRoot -match '(?i)[\\/](?:OneDrive|Dropbox|Google Drive)(?:[\\/]|$)' -or (Test-Path -LiteralPath (Join-Path $InstallRoot '.git'))) { throw 'InstallRoot cannot be a source checkout or sync folder.' }
$arguments=[ordered]@{Version=$Version;WorkRoot=$WorkRoot;Mode=$Mode;UserRoot=$UserRoot;InstallRoot=$InstallRoot}
foreach ($key in @('PythonExe','Harness','Port','OmpCommand','OmpAgentRoot','OmpProfile','OmpPythonExe','RelayStateRoot','RelayRuntimeRoot')) { if ($PSBoundParameters.ContainsKey($key)) { $arguments[$key]=$PSBoundParameters[$key] } }
$arguments.Activate=[bool]$Activate
if ($Mode -eq 'Verify') {
    if ($Activate) { throw 'Verify accepts no activation consent.' }
    if (-not $prior) { throw 'No retained installation exists; Verify does not repair it.' }
    if (-not $Version) { $Version=[string]$prior.version; $arguments.Version=$Version }
    $backend=Join-Path $metadata 'verifier/deploy/install-broker-stack.ps1'
    foreach ($file in @($backend,(Join-Path $metadata 'verifier/deploy/install_runtime.py'))) {
        Plain $file
        $row=@($prior.files | Where-Object { $_.path -ieq $file })
        if ($row.Count -ne 1 -or (Hash $file) -cne $row[0].sha256) { throw 'Retained verifier is missing or changed.' }
    }
    & $backend @arguments
    exit $LASTEXITCODE
}
$cursor=$WorkRoot
while (-not (Test-Path -LiteralPath $cursor)) {
    $parent=Split-Path -Parent $cursor
    if (-not $parent -or $parent -eq $cursor) { throw 'WorkRoot requires an existing owned local ancestor.' }
    $cursor=$parent
}
Owned $cursor
Owned $WorkRoot
PrivateDirectory $WorkRoot
foreach ($name in @('tmp','cache')) { $path=Join-Path $WorkRoot $name; Plain $path; if (Test-Path $path) { Owned $path }; PrivateDirectory $path }
foreach ($name in @('TEMP','TMP','TMPDIR','CLAUDE_CODE_TMPDIR')) { [Environment]::SetEnvironmentVariable($name,(Join-Path $WorkRoot 'tmp'),'Process') }
$env:PYTHONDONTWRITEBYTECODE='1'; $env:PIP_CACHE_DIR=Join-Path $WorkRoot 'cache/pip'; $env:UV_CACHE_DIR=Join-Path $WorkRoot 'cache/uv'; $env:BUN_INSTALL_CACHE_DIR=Join-Path $WorkRoot 'cache/bun'; $env:npm_config_cache=Join-Path $WorkRoot 'cache/npm'
$operation=Join-Path $WorkRoot ('release-'+[guid]::NewGuid().ToString('N'))
PrivateDirectory $operation
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
$api="https://api.github.com/repos/$Repository"
if (-not $Version) {
    $latest=Invoke-RestMethod -Uri "$api/releases/latest" -Headers @{'User-Agent'='agent-messaging-installer';'Accept'='application/vnd.github+json'}
    if ($latest.draft -or $latest.prerelease) { throw 'Latest release is not a published stable release.' }
    $Version=[string]$latest.tag_name
}
if ($Version -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' -or $Version -in @('latest','HEAD','main','master')) { throw 'Version must resolve to an exact release tag.' }
$arguments.Version=$Version
$headers=@{'User-Agent'='agent-messaging-installer';'Accept'='application/vnd.github+json'}
$tag=Invoke-RestMethod -Uri "$api/git/ref/tags/$([Uri]::EscapeDataString($Version))" -Headers $headers
$obj=$tag.object
$depth=0
while ($obj.type -eq 'tag') { if (++$depth -gt 8) { throw 'Annotated tag chain is too deep.' }; $obj=(Invoke-RestMethod -Uri "$api/git/tags/$($obj.sha)" -Headers $headers).object }
if ($obj.type -ne 'commit' -or $obj.sha -cnotmatch '^[0-9a-f]{40}$') { throw 'Tag does not resolve to a full commit.' }
$oid=[string]$obj.sha
$release=Invoke-RestMethod -Uri "$api/releases/tags/$([Uri]::EscapeDataString($Version))" -Headers $headers
if ($release.tag_name -cne $Version -or $release.draft -or $release.prerelease) { throw 'Release must have the selected published stable identity.' }
if ($prior -and $prior.source_commit -cne $oid) {
    if ($prior.source_commit -cnotmatch '^[0-9a-f]{40}$') { throw 'Invalid retained source commit.' }
    $comparison=Invoke-RestMethod -Uri "$api/compare/$($prior.source_commit)...$oid" -Headers $headers
    if ($comparison.status -cne 'ahead') { throw 'Downgrade or non-descendant source transition refused.' }
}
$zipAssets=@($release.assets | Where-Object { $_.name -cmatch '^agent-broker-deploy-[A-Za-z0-9._-]+\.zip$' })
if ($zipAssets.Count -ne 1) { throw 'Release must have exactly one deployment ZIP.' }
$zipName=[string]$zipAssets[0].name
$receiptAsset=@($release.assets | Where-Object { $_.name -ceq ($zipName+'.json') })
$frontAsset=@($release.assets | Where-Object { $_.name -ceq 'install.ps1' })
if ($receiptAsset.Count -ne 1 -or $frontAsset.Count -ne 1) { throw 'Missing release receipt or committed frontdoor.' }
$archive=Join-Path $operation $zipName; $receiptPath=$archive+'.json'; $front=Join-Path $operation 'install.ps1'
Download $receiptAsset[0].browser_download_url $receiptPath
Download $zipAssets[0].browser_download_url $archive
Download $frontAsset[0].browser_download_url $front
$receipt=Get-Content -Raw -LiteralPath $receiptPath | ConvertFrom-Json
Closed $receipt @('schema_version','component','repository','version','source_commit','archive_sha256','files')
if ($receipt.schema_version -ne 1 -or $receipt.component -cne $Component -or $receipt.repository -cne $Repository -or $receipt.version -cne $Version -or $receipt.source_commit -cne $oid -or $receipt.archive_sha256 -cnotmatch '^[0-9a-f]{64}$' -or (Hash $archive) -cne $receipt.archive_sha256) { throw 'Release digest/source binding mismatch.' }
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip=[IO.Compression.ZipFile]::OpenRead($archive)
$destination=Join-Path $operation 'payload'
$tree=Invoke-RestMethod -Uri ($api+'/git/trees/'+$oid+'?recursive=1') -Headers $headers
if ($tree.truncated) { $zip.Dispose(); throw 'GitHub returned an incomplete source tree.' }
$sourceTree=New-Object 'Collections.Generic.Dictionary[string,object]' ([StringComparer]::Ordinal)
foreach ($item in $tree.tree) { if ($item.type -eq 'blob') { $sourceTree.Add($item.path,$item) } }
function JsonMember([string]$Name) {
    $item=$zip.GetEntry($Name)
    if (-not $item) { throw "Missing package metadata: $Name" }
    $reader=New-Object IO.StreamReader($item.Open())
    try { return ($reader.ReadToEnd() | ConvertFrom-Json) } finally { $reader.Dispose() }
}
function GitBlob($Entry) {
    $algorithm=[Security.Cryptography.SHA1]::Create()
    $stream=$Entry.Open()
    try {
        $prefix=[Text.Encoding]::ASCII.GetBytes(('blob '+$Entry.Length+[char]0))
        [void]$algorithm.TransformBlock($prefix,0,$prefix.Length,$prefix,0)
        $buffer=New-Object byte[] 65536
        while (($count=$stream.Read($buffer,0,$buffer.Length)) -gt 0) { [void]$algorithm.TransformBlock($buffer,0,$count,$buffer,0) }
        [void]$algorithm.TransformFinalBlock((New-Object byte[] 0),0,0)
        return ([BitConverter]::ToString($algorithm.Hash)).Replace('-','').ToLowerInvariant()
    } finally { $stream.Dispose(); $algorithm.Dispose() }
}
try {
    $expected=New-Object 'Collections.Generic.Dictionary[string,object]' ([StringComparer]::Ordinal)
    $case=New-Object 'Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    foreach ($row in $receipt.files) {
        Closed $row @('path','sha256','mode')
        if ($row.path -cnotmatch '^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$' -or $row.path -match '(?:^|/)\.\.?(/|$)' -or $row.path -match '(?i)(?:^|/)(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|/|$)' -or $row.path -match '[. ](?:/|$)' -or $row.sha256 -cnotmatch '^[0-9a-f]{64}$' -or $row.mode -cnotin @('100644','100755') -or -not $case.Add($row.path)) { throw 'Invalid/duplicate package member.' }
        $expected.Add($row.path,$row)
    }
    if ($zip.Entries.Count -ne $expected.Count) { throw 'Incomplete or extra archive members.' }
    $declaration=JsonMember 'deploy/release-files.json'
    Closed $declaration @('schema_version','entries')
    if ($declaration.schema_version -ne 1) { throw 'Unsupported release declaration.' }
    $bindings=New-Object 'Collections.Generic.Dictionary[string,string]' ([StringComparer]::Ordinal)
    foreach ($mapping in $declaration.entries) {
        Closed $mapping @('source','destination')
        if (-not $sourceTree.ContainsKey($mapping.source) -or -not $expected.ContainsKey($mapping.destination)) { throw 'Declared source/destination is absent.' }
        $bindings.Add($mapping.destination,$mapping.source)
    }
    if ($bindings.Count+3 -ne $expected.Count -or -not $expected.ContainsKey('manifest.json') -or -not $expected.ContainsKey('VERSION.txt') -or -not $expected.ContainsKey('.gitignore')) { throw 'Archive membership differs from committed declaration.' }
    $seen=New-Object 'Collections.Generic.HashSet[string]' ([StringComparer]::Ordinal)
    foreach ($entry in $zip.Entries) {
        $name=$entry.FullName
        if (-not $expected.ContainsKey($name) -or -not $seen.Add($name)) { throw 'Unexpected or duplicate archive member.' }
        $unix=($entry.ExternalAttributes -shr 16) -band 65535
        $kind=$unix -band 61440
        if ($kind -notin @(0,32768) -or ($entry.ExternalAttributes -band 1024)) { throw 'Links, reparse files and directories are forbidden archive members.' }
        if ($unix -ne [Convert]::ToInt32($expected[$name].mode,8)) { throw 'Archive executable mode differs from source-bound receipt.' }
        $stream=$entry.Open(); $sha=[Security.Cryptography.SHA256]::Create()
        try { $digest=([BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-','').ToLowerInvariant() } finally { $stream.Dispose(); $sha.Dispose() }
        if ($digest -cne $expected[$name].sha256) { throw "Archive member digest mismatch: $name" }
        if ($bindings.ContainsKey($name)) {
            $source=$sourceTree[$bindings[$name]]
            if ($source.mode -cne $expected[$name].mode -or $source.sha -cne (GitBlob $entry)) { throw "Archive bytes/mode differ from selected committed source: $name" }
        }
    }
    foreach ($required in @('install.ps1','deploy/install-broker-stack.ps1','deploy/install_runtime.py','deploy/release-files.json','deploy/adoption.json','deploy/retirements.json','broker/pyproject.toml','manifest.json')) { if (-not $expected.ContainsKey($required)) { throw "Incomplete installer closure: $required" } }
    $manifest=JsonMember 'manifest.json'
    if ($manifest.packagedCommit -cne $oid -or $manifest.bundleVersion -cne $Version -or $manifest.releaseDeclarationSha256 -cne $expected['deploy/release-files.json'].sha256) { throw 'Manifest source/tag/declaration binding mismatch.' }
    if ($manifest.treeSha256 -cne (TreeDigest $expected)) { throw 'Manifest tree digest differs from exact admitted members.' }
    if ((Hash $front) -cne $expected['install.ps1'].sha256) { throw 'Release frontdoor asset differs from selected committed archive bytes.' }
    $handoff=[Environment]::GetEnvironmentVariable('AGENT_MESSAGING_FRONTDOOR_HANDOFF','Process')
    if ($handoff) {
        $bound=$handoff | ConvertFrom-Json
        Closed $bound @('version','source_commit','archive_sha256','frontdoor_sha256')
        if ($bound.version -cne $Version -or $bound.source_commit -cne $oid -or $bound.archive_sha256 -cne $receipt.archive_sha256 -or $bound.frontdoor_sha256 -cne $expected['install.ps1'].sha256) { throw 'Selected release changed during frontdoor handoff.' }
        if ((Hash $PSCommandPath) -cne $expected['install.ps1'].sha256) { throw 'Selected frontdoor handoff cannot recurse or bypass source binding.' }
    }
    [IO.Directory]::CreateDirectory($destination) | Out-Null
    foreach ($entry in $zip.Entries) { $target=Join-Path $destination ($entry.FullName.Replace('/','\')); [IO.Directory]::CreateDirectory((Split-Path -Parent $target)) | Out-Null; [IO.Compression.ZipFileExtensions]::ExtractToFile($entry,$target,$false) }
} finally { $zip.Dispose() }
if ((Hash $PSCommandPath) -cne $expected['install.ps1'].sha256) {
    if ($handoff) { throw 'A frontdoor handoff has already occurred.' }
    $selected=Join-Path $destination 'install.ps1'
    if ((Hash $selected) -cne $expected['install.ps1'].sha256) { throw 'Extracted frontdoor changed before handoff.' }
    $bound=[ordered]@{version=$Version;source_commit=$oid;archive_sha256=$receipt.archive_sha256;frontdoor_sha256=$expected['install.ps1'].sha256}
    $savedHandoff=[Environment]::GetEnvironmentVariable('AGENT_MESSAGING_FRONTDOOR_HANDOFF','Process')
    try {
        $env:AGENT_MESSAGING_FRONTDOOR_HANDOFF=ConvertTo-Json -InputObject $bound -Compress
        $handoffArguments=@{}
        foreach ($key in $arguments.Keys) { if ($key -ne 'UserRoot') { $handoffArguments[$key]=$arguments[$key] } }
        & $selected @handoffArguments
        $result=$LASTEXITCODE
    } finally { [Environment]::SetEnvironmentVariable('AGENT_MESSAGING_FRONTDOOR_HANDOFF',$savedHandoff,'Process') }
    exit $result
}
$arguments.BundleDir=$destination; $arguments.ReleaseReceipt=$receiptPath
& (Join-Path $destination 'deploy/install-broker-stack.ps1') @arguments
exit $LASTEXITCODE
} finally {
    [Environment]::SetEnvironmentVariable('PSModulePath',$savedModulePath,'Process')
    [Environment]::SetEnvironmentVariable('PSModuleAnalysisCachePath',$savedModuleCache,'Process')
}
