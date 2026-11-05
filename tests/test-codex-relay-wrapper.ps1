#Requires -Version 7.0
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$source = Join-Path (Split-Path -Parent $PSScriptRoot) 'harness/codex/codex-relay.ps1'
$root = Join-Path ([IO.Path]::GetTempPath()) ('broker-wrapper-' + [guid]::NewGuid().ToString('N'))
$oldProfile = $env:USERPROFILE
$oldBrokerUrl = $env:CODEX_RELAY_BROKER_URL
$oldBrokerDirectory = $env:CODEX_RELAY_BROKER_DIRECTORY
try {
    $env:USERPROFILE = Join-Path $root 'home'
    Remove-Item Env:CODEX_RELAY_BROKER_URL -ErrorAction SilentlyContinue
    $env:CODEX_RELAY_BROKER_DIRECTORY = 'ignored-private-default'
    $parameters = (Get-Command $source).Parameters
    foreach ($required in @('BrokerDirectory', 'StateDirectory')) {
        $mandatory = @($parameters[$required].Attributes | Where-Object {
            $_ -is [Management.Automation.ParameterAttribute] -and $_.Mandatory
        })
        if ($mandatory.Count -eq 0) { throw "$required must be an explicit mandatory parameter." }
    }
    $legacy = Join-Path $root 'old-private-state'
    $current = Join-Path $env:USERPROFILE '.codex/relay'
    $work = Join-Path $root 'project'
    $broker = Join-Path $env:USERPROFILE '.agents/services/broker'
    New-Item -ItemType Directory -Path $legacy, $current, $work, (Join-Path $broker 'scripts') -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path (Split-Path -Parent $PSScriptRoot) 'deploy/scripts/broker-env.ps1') -Destination (Join-Path $broker 'scripts/broker-env.ps1')
    # Run the actual launcher's nonsecret resolver before installing any launch
    # stub, so the wrapper cannot hide an endpoint bug behind a fake default.
    $launcherSource = Join-Path (Split-Path -Parent $PSScriptRoot) 'harness/codex/start-codex-relay-session.ps1'
    $launcherAst = [Management.Automation.Language.Parser]::ParseFile($launcherSource, [ref]$null, [ref]$null)
    $resolver = $launcherAst.Find({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Resolve-RelayBrokerUrl'
    }, $true)
    if ($null -eq $resolver) { throw 'Launcher must expose its selected broker port resolver.' }
    . ([scriptblock]::Create($resolver.Extent.Text))
    if ((Resolve-RelayBrokerUrl $broker) -ne 'http://127.0.0.1:8787') { throw 'Absent broker.env must use the fresh loopback default.' }
    $selectedEnvironment = Join-Path $broker 'broker.env'
    @('BROKER_HOST=0.0.0.0', 'BROKER_PORT=18491', 'BROKER_BEARER_TOKEN=synthetic-not-imported') |
        Set-Content -LiteralPath $selectedEnvironment
    if ((Resolve-RelayBrokerUrl $broker) -ne 'http://127.0.0.1:18491') { throw 'Selected broker root must resolve its nondefault loopback port.' }
    $capture = Join-Path $root 'capture.json'
    $env:BROKER_WRAPPER_CAPTURE = $capture
    @'
param($SessionKey, $BrokerDirectory, $StateDirectory, $WorkingDirectory, $BrokerUrl,
      $Port, $AppServerPort, $TimeoutSeconds, $CodexPrefix, $CodexArguments, $RelayName,
      [switch]$NewThread)
@{ SessionKey=$SessionKey; StateDirectory=$StateDirectory; NewThread=[bool]$NewThread;
   BrokerDirectory=$BrokerDirectory; BrokerUrl=$BrokerUrl; BrokerUrlBound=$PSBoundParameters.ContainsKey('BrokerUrl'); CodexPrefix=@($CodexPrefix) } |
    ConvertTo-Json | Set-Content -LiteralPath $env:BROKER_WRAPPER_CAPTURE
'@ | Set-Content -LiteralPath (Join-Path $broker 'scripts/start-codex-relay-session.ps1')
    @{ working_directory=$work; last_broker_confirmed_name='existing'; thread_id='old-thread' } |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $legacy 'existing-key.json')
    $arguments = @{ StateDirectory=$legacy; WorkingDirectory=$work;
                    BrokerDirectory=$broker; BrokerUrl='http://127.0.0.1:1' }
    & $source existing -New @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if ($receipt.SessionKey -ne 'existing-key' -or $receipt.StateDirectory -ne $legacy -or -not $receipt.NewThread) {
        throw 'NewThread must retain the existing identity and literal prior state root.'
    }
    $arguments.StateDirectory = $current
    & $source fresh -New @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if ($receipt.SessionKey -ne 'fresh' -or $receipt.StateDirectory -ne $current) {
        throw 'A genuinely new identity must use the declared per-user state root.'
    }
    $arguments.StateDirectory = $legacy
    & $source -List @arguments | Out-Null
    & $source -SessionKey existing-key @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if ($receipt.SessionKey -ne 'existing-key' -or $receipt.StateDirectory -ne $legacy) { throw 'Explicit-key resume changed the selected state root.' }
    $arguments.Remove('BrokerUrl')
    & $source -SessionKey existing-key @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if ($receipt.BrokerUrlBound -or $receipt.BrokerUrl) { throw 'Wrapper must leave an omitted URL unbound for selected broker.env resolution.' }
    if ($receipt.BrokerDirectory -ne $broker) { throw 'Environment must not replace the explicit broker directory.' }
    if (@($receipt.CodexPrefix) -contains '-m' -or ($receipt.CodexPrefix -join ' ') -match 'model_provider=') {
        throw 'Wrapper must retain the caller model/provider configuration by default.'
    }
    & $source -SessionKey existing-key -Model example-model @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    $modelIndex = [Array]::IndexOf(@($receipt.CodexPrefix), '-m')
    if ($modelIndex -lt 0 -or $receipt.CodexPrefix[$modelIndex + 1] -ne 'example-model') {
        throw 'An explicit model must be forwarded.'
    }
    $env:CODEX_RELAY_BROKER_URL = 'http://127.0.0.1:29491'
    & $source -SessionKey existing-key @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if (-not $receipt.BrokerUrlBound -or $receipt.BrokerUrl -ne $env:CODEX_RELAY_BROKER_URL) {
        throw 'An explicit environment URL must take precedence over broker.env.'
    }
    & $source -SessionKey existing-key -BrokerUrl 'http://127.0.0.1:39491' @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if (-not $receipt.BrokerUrlBound -or $receipt.BrokerUrl -ne 'http://127.0.0.1:39491') {
        throw 'A caller URL must take precedence over the environment and broker.env.'
    }
    Remove-Item Env:CODEX_RELAY_BROKER_URL
    Set-Content -LiteralPath $selectedEnvironment -Value 'BROKER_PORT=invalid'
    & $source -SessionKey existing-key -BrokerUrl 'http://127.0.0.1:39491' @arguments
    $receipt = Get-Content -LiteralPath $capture -Raw | ConvertFrom-Json
    if ($receipt.BrokerUrl -ne 'http://127.0.0.1:39491') { throw 'Explicit URLs must not depend on broker.env parsing.' }
    $arguments.BrokerDirectory = 'relative-broker'
    $refusedRelative = $false
    try { & $source -SessionKey existing-key @arguments }
    catch { $refusedRelative = $_.Exception.Message -like '*BrokerDirectory must be absolute*' }
    if (-not $refusedRelative) { throw 'Wrapper must refuse relative broker directories.' }
    Write-Output 'PASS: wrapper uses one explicit root for enumeration, name/key selection and launch, including NewThread.'
}
finally {
    $env:USERPROFILE = $oldProfile
    $env:CODEX_RELAY_BROKER_URL = $oldBrokerUrl
    $env:CODEX_RELAY_BROKER_DIRECTORY = $oldBrokerDirectory
    Remove-Item Env:BROKER_WRAPPER_CAPTURE -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $root) { Remove-Item -LiteralPath $root -Recurse -Force }
}
