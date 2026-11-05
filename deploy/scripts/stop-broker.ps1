# Stop only the selected local broker; never kill arbitrary port owners.
# Default port is read from this installation's broker.env (8787 when absent).
[CmdletBinding()]
param([ValidateRange(1,65535)][int]$Port, [switch]$Force)
$ErrorActionPreference='Stop'
$BrokerRoot=Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'broker-env.ps1')
$selectedPort=Get-BrokerEnvPort -Path (Join-Path $BrokerRoot 'broker.env')
if ($PSBoundParameters.ContainsKey('Port') -and $Port -ne $selectedPort) { throw 'Selected Port conflicts with broker.env; refusing to stop a different port.' }
$Port=$selectedPort
function IsBroker($Process,[switch]$RequireRoot) {
    if (-not $Process -or $Process.Name -notin @('python.exe','python') -or -not $Process.CommandLine -or $Process.CommandLine -notmatch '[\s"\\/]server\.py(\s|"|$)') { return $false }
    if ($RequireRoot) { return $Process.CommandLine.IndexOf($BrokerRoot,[StringComparison]::OrdinalIgnoreCase) -ge 0 -or $Process.CommandLine.IndexOf($BrokerRoot.Replace('\','/'),[StringComparison]::OrdinalIgnoreCase) -ge 0 }
    return $true
}
try {
    $connections=@(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
    $ids=@($connections | Select-Object -ExpandProperty OwningProcess -Unique)
    if ($ids.Count -gt 1) { throw 'Multiple owners for selected port; refusing.' }
    $candidates=@()
    if ($ids.Count -eq 1) {
        $process=Get-CimInstance Win32_Process -Filter "ProcessId = $($ids[0])" -ErrorAction Stop
        if (-not (IsBroker $process)) { Write-Error "Port $Port has a foreign owner; refusing to stop it."; exit 2 }
        $candidates=@($process)
    } else {
        $candidates=@(Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { IsBroker $_ -RequireRoot })
    }
    foreach ($process in $candidates) {
        Write-Host "Stopping broker PID $($process.ProcessId), port $Port, root $BrokerRoot"
        if ($Force) { Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop }
        else { Stop-Process -Id $process.ProcessId -ErrorAction Stop }
    }
    $deadline=(Get-Date).AddSeconds(5)
    do {
        $remaining=@(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
        if ($remaining.Count -eq 0) { exit 0 }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    Write-Error "Port $Port remains occupied; restart must not proceed."
    exit 3
} catch { Write-Error $_; exit 1 }
