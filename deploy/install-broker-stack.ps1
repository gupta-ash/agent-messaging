#requires -Version 5.1
<# Local, nonactivating backend for the committed agent-messaging frontdoor.
All payloads are explicit declaration members. No home mirror, hooks, autostart,
state relocation, runtime provisioning or automatic rollback is performed. #>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$Version,
    [Parameter(Mandatory=$true)][string]$WorkRoot,
    [ValidateSet('Check','Install','Verify')][string]$Mode='Install',
    [Parameter(Mandatory=$true)][string]$UserRoot,
    [Parameter(Mandatory=$true)][string]$InstallRoot,
    [string]$PythonExe,
    [ValidateSet('none','claude','codex','omp')][string[]]$Harness,
    [ValidateRange(1,65535)][int]$Port,
    [string]$OmpCommand,[string]$OmpAgentRoot,[string]$OmpProfile,
    [string]$OmpPythonExe,[string]$RelayStateRoot,[string]$RelayRuntimeRoot,
    [switch]$Activate,
    [string]$BundleDir,[string]$ReleaseReceipt
)
$savedModulePath=[Environment]::GetEnvironmentVariable('PSModulePath','Process')
$savedModuleCache=[Environment]::GetEnvironmentVariable('PSModuleAnalysisCachePath','Process')
try {
$env:PSModulePath=[IO.Path]::Combine($PSHOME,'Modules')
$env:PSModuleAnalysisCachePath='NUL'
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
function Full([string]$Path) {
    if ($Path -notmatch '^[A-Za-z]:[\\/]' -or $Path -match '[*?]' -or $Path.Substring(2).Contains(':') -or $Path -match '(?:^|[\\/])(?:\.|\.\.)(?:[\\/]|$)' -or $Path -match '[. ](?:[\\/]|$)' -or $Path -match '~[0-9]') { throw 'Require an absolute local path without traversal, aliases, streams or wildcards.' }
    $result=[IO.Path]::GetFullPath($Path).TrimEnd('\','/')
    if ($result.Length -le 3) { throw 'A drive root is not an installation/work/runtime directory.' }
    foreach ($part in ($result.Substring(3) -split '[\\/]')) { if ($part -match '^(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)') { throw 'Reserved path member.' } }
    return $result
}
function Plain([string]$Path) {
    $cursor=$Path
    while ($cursor) {
        if ((Test-Path -LiteralPath $cursor) -and ((Get-Item -Force -LiteralPath $cursor).Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw "Unsafe path ancestry: $cursor" }
        $parent=Split-Path -Parent $cursor; if ($parent -eq $cursor) { break }; $cursor=$parent
    }
}
function Contains([string]$Root,[string]$Child) { $Root.Equals($Child,[StringComparison]::OrdinalIgnoreCase) -or $Child.StartsWith($Root+'\',[StringComparison]::OrdinalIgnoreCase) }
$UserRoot=Full $UserRoot; $InstallRoot=Full $InstallRoot; $WorkRoot=Full $WorkRoot
if ($UserRoot -ine (Full ([Environment]::GetFolderPath('UserProfile')))) { throw 'UserRoot differs from the actual invoking Windows account.' }
foreach ($path in @($UserRoot,$InstallRoot,$WorkRoot)) { Plain $path }
$protected=@($InstallRoot,$UserRoot+'\.agents',$UserRoot+'\.claude',$UserRoot+'\.codex',$UserRoot+'\.omp')
$cursor=$PSScriptRoot
while ($cursor) {
    if (Test-Path -LiteralPath (Join-Path $cursor '.git')) { $protected+=$cursor; break }
    $parent=Split-Path -Parent $cursor; if ($parent -eq $cursor) { break }; $cursor=$parent
}
foreach ($root in $protected) { if ((Contains $root $WorkRoot) -or (Contains $WorkRoot $root)) { throw 'WorkRoot overlaps source/install/configuration/state roots.' } }
if ($WorkRoot -match '(?i)[\\/](?:ash_src|\.agents|\.claude|\.codex|\.omp)(?:[\\/]|$)') { throw 'WorkRoot cannot be a source or installed tree.' }
if ($InstallRoot -match '(?i)[\\/](?:OneDrive|Dropbox|Google Drive)(?:[\\/]|$)' -or (Test-Path -LiteralPath (Join-Path $InstallRoot '.git'))) { throw 'InstallRoot cannot be a source checkout or sync folder.' }
$cursor=$InstallRoot
while ($cursor) {
    if (Test-Path -LiteralPath (Join-Path $cursor '.git')) { throw 'InstallRoot cannot be inside a source checkout.' }
    $parent=Split-Path -Parent $cursor; if ($parent -eq $cursor) { break }; $cursor=$parent
}
$parameters=@{}
foreach ($key in $PSBoundParameters.Keys) { $parameters[$key]=$PSBoundParameters[$key] }
$parameters.Mode=$Mode
$parameters.Activate=[bool]$Activate
$parameters.UserRoot=$UserRoot; $parameters.InstallRoot=$InstallRoot; $parameters.WorkRoot=$WorkRoot
if (-not $PythonExe) {
    $record=Join-Path $UserRoot '.agents/installations/agent-messaging/installed.json'
    if (Test-Path -LiteralPath $record) { $PythonExe=(Get-Content -Raw -LiteralPath $record | ConvertFrom-Json).parameters.PythonExe }
    else {
        $configuration=Join-Path $InstallRoot '.venv/pyvenv.cfg'
        if (Test-Path -LiteralPath $configuration) {
            Plain $configuration
            $fields=@{}
            foreach ($line in [IO.File]::ReadAllLines($configuration)) {
                if ($line -match '^([^=]+)=(.*)$') {
                    $key=$matches[1].Trim(); if ($fields.ContainsKey($key)) { throw 'Duplicate legacy venv configuration field.' }; $fields[$key]=$matches[2].Trim()
                }
            }
            if (-not $fields.ContainsKey('home') -or $fields['version'] -notmatch '^3\.12\.' -or $fields['include-system-site-packages'] -cne 'false') { throw 'Existing broker venv must declare its base Python 3.12 without global site packages.' }
            $baseHome=Full ([string]$fields['home'])
            Plain $baseHome
            if (-not (Test-Path -LiteralPath $baseHome -PathType Container)) { throw 'Legacy venv home must name an existing base Python directory.' }
            $PythonExe=Join-Path $baseHome 'python.exe'
            if ($fields.ContainsKey('executable') -and (Full ([string]$fields['executable'])) -ine $PythonExe) { throw 'Legacy venv executable metadata conflicts with home/python.exe; venv/alias takeover refused.' }
        } elseif ($env:PYTHON_EXE) { $PythonExe=$env:PYTHON_EXE }
        else {
            foreach ($name in @('python','python.exe')) {
                $candidate=Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
                if ($candidate) { $PythonExe=$candidate.Source; break }
            }
        }
    }
}
if (-not $PythonExe -or $PythonExe -notmatch '^[A-Za-z]:[\\/]' -or -not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) { throw 'Missing/invalid base Python: select PythonExe or PYTHON_EXE, or place Python 3.12 on PATH.' }
$PythonExe=Full $PythonExe
Plain $PythonExe
$probe=& $PythonExe -I -B -c "import json,sys,venv,ensurepip;print(json.dumps({'version':list(sys.version_info[:2]),'base':sys.prefix==sys.base_prefix,'executable':sys.executable,'prefix':sys.prefix}))"
if ($LASTEXITCODE -ne 0) { throw 'Selected Python prerequisite probe failed; no alternate interpreter is guessed.' }
$identity=$probe | ConvertFrom-Json
if (($identity.version -join '.') -cne '3.12' -or -not $identity.base -or (Full ([string]$identity.executable)) -ine $PythonExe -or (Full ([string]$identity.prefix)) -ine (Split-Path -Parent $PythonExe)) { throw 'Selected Python must be the existing base Python 3.12 at its declared executable/prefix, not a venv or executable alias.' }
$parameters.PythonExe=[IO.Path]::GetFullPath($PythonExe)
$env:PYTHONDONTWRITEBYTECODE='1'
$saved=$env:AGENT_MESSAGING_INSTALL_PARAMETERS
try {
    $env:AGENT_MESSAGING_INSTALL_PARAMETERS=ConvertTo-Json -InputObject $parameters -Depth 20 -Compress
    & $PythonExe -I -B (Join-Path $PSScriptRoot 'install_runtime.py') --parameters-env
    $result=$LASTEXITCODE
} finally { $env:AGENT_MESSAGING_INSTALL_PARAMETERS=$saved }
exit $result
} finally {
    [Environment]::SetEnvironmentVariable('PSModulePath',$savedModulePath,'Process')
    [Environment]::SetEnvironmentVariable('PSModuleAnalysisCachePath',$savedModuleCache,'Process')
}
