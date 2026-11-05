# Restart only this local installation after separate operator authorization.
# Do not pipe/drain detached output: wait on the launcher process handle instead.
[CmdletBinding()]
param([ValidateRange(1,65535)][int]$Port,[switch]$Force,[switch]$Foreground)
$ErrorActionPreference='Stop'
$BrokerRoot=Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'broker-env.ps1')
$selectedPort=Get-BrokerEnvPort -Path (Join-Path $BrokerRoot 'broker.env')
if ($PSBoundParameters.ContainsKey('Port') -and $Port -ne $selectedPort) { throw 'Selected Port conflicts with broker.env; refusing to restart a different port.' }
$Port=$selectedPort
$stop=Join-Path $PSScriptRoot 'stop-broker.ps1'
$start=Join-Path $PSScriptRoot 'start-broker.ps1'
$stopArguments=@{Port=$Port}
if ($Force) { $stopArguments.Force=$true }
& $stop @stopArguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($Foreground) { & $start -Port $Port -Foreground; exit $LASTEXITCODE }
& $start -Port $Port -Autostart
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$deadline=(Get-Date).AddSeconds(40)
do {
    try {
        $response=Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/api/health" -TimeoutSec 2
        if ($response.StatusCode -eq 200) { Write-Host "Broker healthy on loopback port $Port"; exit 0 }
    } catch { }
    Start-Sleep -Milliseconds 500
} while ((Get-Date) -lt $deadline)
Write-Error "Activation pending/failed on port $Port; inspect installed logs. No rollback was attempted."
exit 4
