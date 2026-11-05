#Requires -Version 7.0
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ('broker-env-' + [guid]::NewGuid().ToString('N'))
$brokerRoot = Join-Path $fixtureRoot 'selected broker'
$scripts = Join-Path $brokerRoot 'scripts'
$environmentPath = Join-Path $brokerRoot 'broker.env'
$effects = [Collections.Generic.List[object]]::new()
# The unmodified consumers run in child script scopes; OS doubles share only
# this fixture-owned state, not the consumer's `$script:` namespace.
$global:__BrokerEnvFixture = @{ Effects = $effects; ListenerPresent = $false; ForeignOwner = $false; LastSelectedPort = 0; BrokerRoot = $brokerRoot; FixtureRoot = $fixtureRoot; EnvironmentPath = $environmentPath; Scripts = $scripts }
$originalLocation = Get-Location
$originalEnvironment = @{}
foreach ($key in @('BROKER_PORT', 'BROKER_HOST', 'BROKER_DB_PATH', 'BROKER_BEARER_TOKEN', 'BROKER_REAPER_ENABLED', 'PYTHONDONTWRITEBYTECODE')) {
    $originalEnvironment[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
}

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERTION FAILED: $Message" }
}

# Copy the real consumers unchanged. Only OS process/network effects are stubbed;
# file parsing, sibling script calls, validation, env import, and writes stay real.
function Start-Process {
    [CmdletBinding()]
    param($FilePath, $ArgumentList, $WorkingDirectory, $RedirectStandardOutput, $RedirectStandardError, $WindowStyle, [switch]$PassThru)
    $global:__BrokerEnvFixture.Effects.Add([pscustomobject]@{
        Kind = 'start'; Port = [int]$env:BROKER_PORT; File = $FilePath
        Db = $env:BROKER_DB_PATH; Token = $env:BROKER_BEARER_TOKEN
    })
    return [pscustomobject]@{ Id = 43210; HasExited = $false; ExitCode = 0 }
}
function Get-NetTCPConnection {
    [CmdletBinding()]
    param([int]$LocalPort, [string]$State)
    $global:__BrokerEnvFixture.LastSelectedPort = $LocalPort
    $global:__BrokerEnvFixture.Effects.Add([pscustomobject]@{ Kind = 'listener-query'; Port = $LocalPort })
    if ($global:__BrokerEnvFixture.ListenerPresent) { [pscustomobject]@{ OwningProcess = 43210 } }
}
function Get-CimInstance {
    [CmdletBinding()]
    param([string]$ClassName, [string]$Filter)
    $global:__BrokerEnvFixture.Effects.Add([pscustomobject]@{ Kind = 'process-query'; Port = $global:__BrokerEnvFixture.LastSelectedPort })
    if ($global:__BrokerEnvFixture.ForeignOwner) {
        return [pscustomobject]@{ Name = 'foreign.exe'; CommandLine = 'foreign.exe'; ProcessId = 43210 }
    }
    return [pscustomobject]@{
        Name = 'python.exe'; ProcessId = 43210
        CommandLine = 'python "' + (Join-Path $global:__BrokerEnvFixture.BrokerRoot 'server.py') + '"'
    }
}
function Stop-Process {
    [CmdletBinding()]
    param([int]$Id, [switch]$Force)
    $global:__BrokerEnvFixture.Effects.Add([pscustomobject]@{ Kind = 'stop'; Port = $global:__BrokerEnvFixture.LastSelectedPort; Id = $Id; Force = [bool]$Force })
    $global:__BrokerEnvFixture.ListenerPresent = $false
}
function Invoke-WebRequest {
    [CmdletBinding()]
    param([uri]$Uri, [switch]$UseBasicParsing, [int]$TimeoutSec)
    $global:__BrokerEnvFixture.Effects.Add([pscustomobject]@{ Kind = 'health-query'; Port = $Uri.Port; Host = $Uri.Host })
    return [pscustomobject]@{ StatusCode = 200; Content = '{"checks":{"reaper_running":false}}' }
}

function Reset-Consumer {
    $global:__BrokerEnvFixture.Effects.Clear()
    $global:__BrokerEnvFixture.ListenerPresent = $true
    $global:__BrokerEnvFixture.ForeignOwner = $false
    $global:__BrokerEnvFixture.LastSelectedPort = 0
    $logs = Join-Path $global:__BrokerEnvFixture.BrokerRoot 'logs'
    if (Test-Path -LiteralPath $logs) { Remove-Item -LiteralPath $logs -Recurse -Force }
    # An inherited selection must never hide an invalid declaration or override
    # the fresh default of a file without BROKER_PORT.
    $env:BROKER_PORT = '61234'
    $env:BROKER_HOST = 'inherited-host'
    $env:BROKER_DB_PATH = 'inherited.db'
    $env:BROKER_BEARER_TOKEN = 'inherited-not-imported'
    $env:BROKER_REAPER_ENABLED = 'false'
    Set-Location $global:__BrokerEnvFixture.FixtureRoot
}
function Write-Environment([string]$Declaration) {
    [IO.File]::WriteAllText($global:__BrokerEnvFixture.EnvironmentPath, (@(
        '# BROKER_PORT=invalid commented declarations are ignored'
        'BROKER_HOST=127.0.0.1'
        'BROKER_DB_PATH=" private/state.db "'
        'BROKER_BEARER_TOKEN=" private-fixture-token "'
        'BROKER_REAPER_ENABLED=false'
        $Declaration
    ) -join "`n"), [Text.UTF8Encoding]::new($false))
}
function Get-FixtureSnapshot {
    ((Get-ChildItem -LiteralPath $global:__BrokerEnvFixture.BrokerRoot -Recurse -File | Sort-Object FullName | ForEach-Object {
        $_.FullName + ':' + (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
    }) -join "`n")
}
function Invoke-Consumer([string]$Name, [hashtable]$Parameters = @{}) {
    $failure = $null
    $global:LASTEXITCODE = 0
    try { & (Join-Path $global:__BrokerEnvFixture.Scripts $Name) @Parameters }
    catch { $failure = $_ }
    [pscustomobject]@{ Failure = $failure; ExitCode = $global:LASTEXITCODE }
}

try {
    New-Item -ItemType Directory -Path $scripts, (Join-Path $brokerRoot '.venv/Scripts') -Force | Out-Null
    foreach ($name in @('broker-env.ps1', 'start-broker.ps1', 'stop-broker.ps1', 'restart-broker.ps1', 'enable-reaper.ps1')) {
        Copy-Item -LiteralPath (Join-Path $repoRoot "deploy/scripts/$name") -Destination $scripts
    }
    # The real start precondition remains active, but no interpreter is launched.
    [IO.File]::WriteAllText((Join-Path $brokerRoot '.venv/Scripts/python.exe'), 'process-effect stub target')

    $valid = @(
        @{ Text = 'BROKER_PORT=18491'; Port = 18491 },
        @{ Text = '  BROKER_PORT = 18491  '; Port = 18491 },
        @{ Text = "`tBROKER_PORT`t=`t18491`t"; Port = 18491 },
        @{ Text = 'BROKER_PORT = " 18491 "'; Port = 18491 },
        @{ Text = "BROKER_PORT = ' 18491 '"; Port = 18491 },
        @{ Text = 'BROKER_PORT=1'; Port = 1 },
        @{ Text = 'BROKER_PORT=65535'; Port = 65535 },
        @{ Text = "BROKER_PORT=18491`nBROKER_PORT = '18502'"; Port = 18502 },
        @{ Text = 'OTHER_KEY=missing port'; Port = 8787 },
        @{ Text = 'OTHER_KEY=BROKER_PORT is just a value'; Port = 8787 }
    )
    foreach ($case in $valid) {
        foreach ($name in @('start-broker.ps1', 'stop-broker.ps1', 'restart-broker.ps1', 'enable-reaper.ps1')) {
            Reset-Consumer
            Write-Environment $case.Text
            $before = [IO.File]::ReadAllText($environmentPath)
            $parameters = @{}
            if ($name -eq 'start-broker.ps1') { $parameters.Autostart = $true }
            $result = Invoke-Consumer $name $parameters
            Assert-True ($null -eq $result.Failure) "$name must adopt the shared valid grammar: $($case.Text); consumer failure: $($result.Failure)"
            if ($name -eq 'enable-reaper.ps1') {
                # Its real notification prerequisite intentionally refuses after
                # selecting/probing the port, before stop/write/native ledger work.
                Assert-True ($result.ExitCode -eq 1) 'reaper must preserve the notification configuration guard'
                Assert-True (@($effects | Where-Object Kind -ne 'health-query').Count -eq 0) 'reaper prerequisite refusal must not stop or start a process'
            } else {
                Assert-True ($result.ExitCode -eq 0) "$name should complete with stubbed process effects"
            }
            Assert-True ($effects.Count -gt 0) "$name must exercise the actual selected-port consumer"
            Assert-True (@($effects | Where-Object Port -ne $case.Port).Count -eq 0) "$name must use only selected port $($case.Port)"
            Assert-True (@($effects | Where-Object { $_.Kind -eq 'health-query' -and $_.Host -ne '127.0.0.1' }).Count -eq 0) 'health probes must remain loopback-only'
            Assert-True ([IO.File]::ReadAllText($environmentPath) -ceq $before) "$name must preserve the private env file"
            foreach ($start in @($effects | Where-Object Kind -eq 'start')) {
                Assert-True ($start.Db -eq 'private/state.db' -and $start.Token -eq 'private-fixture-token') 'manual/restart launches must retain private coordinates under the shared value grammar'
            }
            if ($name -in @('stop-broker.ps1', 'enable-reaper.ps1')) {
                Assert-True ($env:BROKER_BEARER_TOKEN -eq 'inherited-not-imported' -and $env:BROKER_PORT -eq '61234') "$name must not import broker.env into the process environment"
            }
        }
    }

    $invalid = @(
        'BROKER_PORT=', 'BROKER_PORT=" "', 'BROKER_PORT=0', 'BROKER_PORT=-1', 'BROKER_PORT=+1',
        'BROKER_PORT=65536', 'BROKER_PORT=999999999999999999999999', 'BROKER_PORT=018491',
        'BROKER_PORT=01', 'BROKER_PORT=18491.0', 'BROKER_PORT=text', 'BROKER_PORT=18491 # comment',
        'BROKER_PORT="18491', "BROKER_PORT='18491", 'BROKER_PORT="18491''',
        'BROKER_PORT="18491"junk', 'BROKER_PORT=18491"', 'BROKER_PORT==18491',
        'broker_port=18491', 'BROKER_PORT :18491', 'export BROKER_PORT=18491',
        "BROKER_PORT=invalid`nBROKER_PORT=18491", "BROKER_PORT=18491`nBROKER_PORT=invalid"
    )
    foreach ($declaration in $invalid) {
        foreach ($name in @('start-broker.ps1', 'stop-broker.ps1', 'restart-broker.ps1', 'enable-reaper.ps1')) {
            Reset-Consumer
            Write-Environment $declaration
            $before = Get-FixtureSnapshot
            $parameters = @{}
            if ($name -eq 'start-broker.ps1') { $parameters.Autostart = $true }
            $result = Invoke-Consumer $name $parameters
            Assert-True ($null -ne $result.Failure -or $result.ExitCode -ne 0) "$name must refuse invalid declaration: $declaration"
            Assert-True ($effects.Count -eq 0) "$name must reject invalid ports before any process or network query/action"
            Assert-True ((Get-FixtureSnapshot) -ceq $before) "$name must reject invalid ports before any file write"
            Assert-True ($env:BROKER_PORT -eq '61234' -and $env:BROKER_BEARER_TOKEN -eq 'inherited-not-imported') "$name must validate the entire file before importing values"
        }
    }

    foreach ($name in @('start-broker.ps1', 'stop-broker.ps1', 'restart-broker.ps1')) {
        Reset-Consumer
        Write-Environment 'BROKER_PORT = " 18491 "'
        $before = Get-FixtureSnapshot
        $parameters = @{ Port = 18502; Force = $true }
        if ($name -eq 'start-broker.ps1') { $parameters = @{ Port = 18502; Autostart = $true } }
        $result = Invoke-Consumer $name $parameters
        Assert-True ($null -ne $result.Failure) "$name must refuse an explicit conflicting port even with Force"
        Assert-True ($effects.Count -eq 0 -and (Get-FixtureSnapshot) -ceq $before) "$name must never act on the caller's wrong selected port"
        $parameters.Port = 18491
        $result = Invoke-Consumer $name $parameters
        Assert-True ($null -eq $result.Failure -and $result.ExitCode -eq 0) "$name must allow an explicit matching port"
        Assert-True (@($effects | Where-Object Port -ne 18491).Count -eq 0) "$name must retain the parsed private port"
        if ($name -ne 'start-broker.ps1') {
            $stops = @($effects | Where-Object Kind -eq 'stop')
            Assert-True ($stops.Count -eq 1 -and $stops[0].Force) 'Force must still be forwarded only when explicitly selected'
        }
    }

    Reset-Consumer
    Write-Environment 'BROKER_PORT=18491'
    $global:__BrokerEnvFixture.ForeignOwner = $true
    $result = Invoke-Consumer 'stop-broker.ps1' @{ Force = $true }
    Assert-True ($null -ne $result.Failure -or $result.ExitCode -ne 0) 'a foreign owner must remain protected even with Force'
    Assert-True (@($effects | Where-Object Kind -eq 'stop').Count -eq 0) 'port parsing must never weaken the process identity guard'
    Write-Output 'PASS: actual Windows broker consumers share strict port grammar and refuse invalid/conflicting declarations before effects.'
}
finally {
    Set-Location $originalLocation
    foreach ($key in $originalEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($key, $originalEnvironment[$key], 'Process')
    }
    Remove-Variable -Name __BrokerEnvFixture -Scope Global -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $fixtureRoot) { Remove-Item -LiteralPath $fixtureRoot -Recurse -Force }
}
