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
    [switch]$AdoptExisting,[switch]$Activate,
    [string]$BundleDir,[string]$ReleaseReceipt
)
$savedModulePath=[Environment]::GetEnvironmentVariable('PSModulePath','Process')
$savedModuleCache=[Environment]::GetEnvironmentVariable('PSModuleAnalysisCachePath','Process')
try {
$env:PSModulePath=[IO.Path]::Combine($PSHOME,'Modules')
$env:PSModuleAnalysisCachePath='NUL'
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$parameters=@{}
foreach ($key in $PSBoundParameters.Keys) { $parameters[$key]=$PSBoundParameters[$key] }
$parameters.Mode=$Mode
$parameters.AdoptExisting=[bool]$AdoptExisting
$parameters.Activate=[bool]$Activate
if (-not $PythonExe) {
    $record=Join-Path $UserRoot '.agents/installations/agent-messaging/installed.json'
    if (Test-Path -LiteralPath $record) { $PythonExe=(Get-Content -Raw -LiteralPath $record | ConvertFrom-Json).parameters.PythonExe }
    else { $candidate=Get-Command python -CommandType Application -ErrorAction SilentlyContinue; if ($candidate) { $PythonExe=$candidate.Source } }
}
if (-not $PythonExe -or -not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) { throw 'PythonExe must select an existing base Python 3.12 interpreter.' }
$parameters.PythonExe=$PythonExe
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
