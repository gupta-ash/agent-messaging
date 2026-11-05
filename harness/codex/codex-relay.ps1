#Requires -Version 7.0

<#
.SYNOPSIS
Starts one Codex relay session through the accepted public-build launcher.

.DESCRIPTION
BrokerDirectory is the selected broker root containing the Python adapters and
broker.env. An explicit BrokerUrl wins over CODEX_RELAY_BROKER_URL. When both
are omitted, the full launcher uses that root's BROKER_PORT on loopback; a
missing file or port uses the fresh-install default 8787.

.EXAMPLE
codex-relay research -New -BrokerDirectory <broker-root> -StateDirectory <private-state>

.EXAMPLE
codex-relay research -BrokerDirectory <broker-root> -StateDirectory <private-state>

.EXAMPLE
codex-relay -BrokerDirectory <broker-root> -StateDirectory <private-state>

.EXAMPLE
codex-relay -List -BrokerDirectory <broker-root> -StateDirectory <private-state>

.EXAMPLE
codex-relay -New -BrokerDirectory <broker-root> -StateDirectory <private-state>

.EXAMPLE
codex-relay -SessionKey unnamed -New -BrokerDirectory <broker-root> -StateDirectory <private-state>
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidatePattern('^[A-Za-z0-9._-]{1,64}$')]
    [string]$Name,

    [ValidatePattern('^[A-Za-z0-9._-]{1,64}$')]
    [string]$SessionKey,

    [switch]$List,

    [Alias('New')]
    [switch]$NewThread,

    [string]$WorkingDirectory = (Get-Location).Path,

    [Parameter(Mandatory)]
    [string]$StateDirectory,

    [string]$BrokerUrl = $env:CODEX_RELAY_BROKER_URL,

    [Parameter(Mandatory)]
    [string]$BrokerDirectory,

    [ValidateRange(0, 65535)]
    [int]$Port = 0,

    [ValidateRange(0, 65535)]
    [int]$AppServerPort = 0,

    [string]$Model,

    [ValidateSet('read-only', 'workspace-write', 'danger-full-access')]
    [string]$Sandbox = 'read-only',

    [ValidateSet('untrusted', 'on-failure', 'on-request', 'never')]
    [string]$ApprovalPolicy = 'on-request',

    [string[]]$CodexArguments = @('--no-alt-screen')
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function ConvertTo-NormalizedDirectoryPath {
    param([string]$Path)

    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if ($fullPath.Equals($root, [StringComparison]::OrdinalIgnoreCase)) { return $fullPath }
    $trimCharacters = [char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    $fullPath.TrimEnd($trimCharacters)
}

function New-RelaySessionKey {
    param([string]$StateDirectory)

    do { $key = [guid]::NewGuid().ToString() }
    while (Test-Path -LiteralPath (Join-Path $StateDirectory "$key.json") -PathType Leaf)
    $key
}

function Get-RelayIdentities {
    param(
        [string]$StateDirectory,
        [string]$ResolvedWorkingDirectory
    )

    if (-not (Test-Path -LiteralPath $StateDirectory -PathType Container)) { return @() }

    $identities = foreach ($file in Get-ChildItem -LiteralPath $StateDirectory -Filter '*.json' -File) {
        if ($file.Name.EndsWith('.ready.json', [StringComparison]::OrdinalIgnoreCase)) { continue }
        try { $state = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json -ErrorAction Stop }
        catch { throw "Stored relay state is invalid: $($file.FullName): $($_.Exception.Message)" }

        $workingDirectoryProperty = $state.PSObject.Properties['working_directory']
        if ($null -eq $workingDirectoryProperty -or -not ($workingDirectoryProperty.Value -is [string])) { continue }
        try { $stateWorkingDirectory = ConvertTo-NormalizedDirectoryPath $workingDirectoryProperty.Value }
        catch { continue }
        if (-not $stateWorkingDirectory.Equals($ResolvedWorkingDirectory, [StringComparison]::OrdinalIgnoreCase)) { continue }

        $brokerNameProperty = $state.PSObject.Properties['last_broker_confirmed_name']
        $brokerName = if ($null -ne $brokerNameProperty -and $brokerNameProperty.Value -is [string] -and
            -not [string]::IsNullOrWhiteSpace($brokerNameProperty.Value)) {
            $brokerNameProperty.Value
        }
        else { $null }
        $threadProperty = $state.PSObject.Properties['thread_id']
        $threadId = if ($null -ne $threadProperty -and $threadProperty.Value -is [string] -and
            -not [string]::IsNullOrWhiteSpace($threadProperty.Value)) {
            $threadProperty.Value
        }
        else { $null }

        [pscustomobject]@{
            SessionKey = $file.BaseName
            BrokerName = $brokerName
            ThreadId   = $threadId
        }
    }

    @($identities | Sort-Object @{
            Expression = { if ($_.BrokerName) { $_.BrokerName } else { $_.SessionKey } }
        }, SessionKey)
}

function Get-RelayInventoryRows {
    param([object[]]$Identities)

    $number = 0
    @($Identities | Where-Object { $_.ThreadId } | ForEach-Object {
            $number++
            [pscustomobject]@{
                Number     = $number
                RelayName  = if ($_.BrokerName) { $_.BrokerName } else { '(unnamed)' }
                SessionKey = $_.SessionKey
                ThreadId   = $_.ThreadId
            }
        })
}

$launcher = Join-Path $env:USERPROFILE '.agents/services/broker/scripts/start-codex-relay-session.ps1'
if (-not (Test-Path -LiteralPath $launcher -PathType Leaf)) {
    throw "Codex relay launcher is missing: $launcher"
}

$resolvedWorkingDirectory = ConvertTo-NormalizedDirectoryPath (Resolve-Path -LiteralPath $WorkingDirectory -ErrorAction Stop).Path
if (-not [IO.Path]::IsPathFullyQualified($StateDirectory)) { throw 'StateDirectory must be absolute.' }
$identities = @(Get-RelayIdentities $stateDirectory $resolvedWorkingDirectory)
$inventoryRows = @(Get-RelayInventoryRows $identities)

if ($List) {
    $inventoryRows
    return
}

$relayNameForNewIdentity = $null
if ($SessionKey) {
    $statePath = Join-Path $stateDirectory "$SessionKey.json"
    if ($Name -and -not (Test-Path -LiteralPath $statePath -PathType Leaf)) {
        if (-not $NewThread) {
            throw "No relay identity named '$Name' was found for $resolvedWorkingDirectory. Use -New to create it."
        }
        $relayNameForNewIdentity = $Name
    }
}
elseif ($Name) {
    $brokerMatches = @($identities | Where-Object {
            $_.BrokerName -and $_.BrokerName.Equals($Name, [StringComparison]::OrdinalIgnoreCase)
        })
    if ($brokerMatches.Count -gt 1) {
        throw "Multiple relay identities have broker name '$Name' for $resolvedWorkingDirectory. Use -SessionKey to choose one."
    }
    if ($brokerMatches.Count -eq 1) {
        $SessionKey = $brokerMatches[0].SessionKey
    }
    else {
        $keyMatches = @($identities | Where-Object {
                $_.SessionKey.Equals($Name, [StringComparison]::OrdinalIgnoreCase)
            })
        if ($keyMatches.Count -eq 1) {
            $SessionKey = $keyMatches[0].SessionKey
        }
        elseif ($NewThread) {
            $candidateStatePath = Join-Path $stateDirectory "$Name.json"
            $SessionKey = if (Test-Path -LiteralPath $candidateStatePath -PathType Leaf) {
                New-RelaySessionKey $stateDirectory
            }
            else { $Name }
            $relayNameForNewIdentity = $Name
        }
        else {
            throw "No relay identity named '$Name' was found for $resolvedWorkingDirectory. Use -New to create it."
        }
    }
}
elseif ($NewThread) {
    $SessionKey = New-RelaySessionKey $stateDirectory
}
else {
    if ($inventoryRows.Count -eq 0) {
        throw "No resumable relay identities were found for $resolvedWorkingDirectory. Use codex-relay -New or codex-relay <name> -New."
    }
    $inventoryRows | Format-Table -AutoSize | Out-Host
    $selectionText = Read-Host "Select a relay session [1-$($inventoryRows.Count)]"
    $selection = 0
    if (-not [int]::TryParse($selectionText, [ref]$selection) -or $selection -lt 1 -or $selection -gt $inventoryRows.Count) {
        throw "Relay session selection must be a number from 1 to $($inventoryRows.Count)."
    }
    $SessionKey = $inventoryRows[$selection - 1].SessionKey
}

if (-not [IO.Path]::IsPathFullyQualified($BrokerDirectory)) { throw 'BrokerDirectory must be absolute.' }
if (-not (Test-Path -LiteralPath $BrokerDirectory -PathType Container)) {
    throw "Codex relay broker directory is missing: $BrokerDirectory"
}

$launcherParameters = @{
    SessionKey       = $SessionKey
    BrokerDirectory  = $BrokerDirectory
    StateDirectory   = $StateDirectory
    WorkingDirectory = $resolvedWorkingDirectory
    Port             = $Port
    AppServerPort    = $AppServerPort
    TimeoutSeconds   = 120
    CodexPrefix      = @(
        '-c', 'approvals_reviewer="user"',
        '-s', $Sandbox,
        '-a', $ApprovalPolicy
    )
    CodexArguments   = $CodexArguments
}
# Leave an omitted URL unbound so the full launcher reads this broker root's port.
if ($PSBoundParameters.ContainsKey('BrokerUrl') -or $BrokerUrl) {
    $launcherParameters.BrokerUrl = $BrokerUrl
}
if ($Model) { $launcherParameters.CodexPrefix += @('-m', $Model) }
if ($relayNameForNewIdentity) { $launcherParameters.RelayName = $relayNameForNewIdentity }
if ($NewThread) { $launcherParameters.NewThread = $true }

& $launcher @launcherParameters
