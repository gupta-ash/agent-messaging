[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$launcher = Join-Path $repoRoot 'harness/codex/start-codex-relay-session.ps1'
if (-not (Test-Path -LiteralPath $launcher -PathType Leaf)) {
    throw "registration launcher is missing: $launcher"
}
$launcherText = Get-Content -LiteralPath $launcher -Raw
if ($launcherText -notmatch '(?m)^#Requires -Version 7\.0$') {
    throw 'registration launcher must fail fast under Windows PowerShell 5.1'
}
if ($launcherText -notmatch "'--listen-url'") {
    throw 'relay adapter must expose a proxy listener for the normal TUI connection'
}
if ($launcherText -match 'Start-RelayProcess \$processJob \$python\.BackgroundFile \$adapterArguments \$repoRoot Normal') {
    throw 'relay adapter must not require a second visible console'
}
if ($launcherText -notmatch 'enabled_tools=\["register","name","check","send","ack"\]') {
    throw 'session MCP must expose the receipt ack tool'
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw "ASSERTION FAILED: $Message" }
}

function Get-FakeEvents {
    param([string]$Directory)
    @(Get-ChildItem -LiteralPath $Directory -Filter '*.json' -File |
        ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json } |
        Sort-Object timestamp, pid)
}

function Reset-FakeRun {
    Get-ChildItem -LiteralPath $script:eventDirectory -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem -LiteralPath $script:brokerRequestDirectory -File -ErrorAction SilentlyContinue | Remove-Item -Force
}

function Get-BrokerRequests {
    @(Get-ChildItem -LiteralPath $script:brokerRequestDirectory -Filter '*.json' -File |
        ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json } |
        Sort-Object timestamp_ns)
}

function Get-FreeLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        ([Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally { $listener.Stop() }
}

function Wait-FakeBroker {
    param([string]$Url, [Diagnostics.Process]$Process)
    $deadline = [DateTime]::UtcNow.AddSeconds(5)
    do {
        $Process.Refresh()
        if ($Process.HasExited) { throw "fake broker exited with code $($Process.ExitCode)" }
        try {
            $response = Invoke-WebRequest -Uri "$Url/health" -UseBasicParsing -TimeoutSec 1
            if ($response.StatusCode -eq 200) { return }
        }
        catch { }
        Start-Sleep -Milliseconds 50
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "fake broker did not become ready: $Url"
}

function Assert-ProcessStopped {
    param([int]$ProcessId, [string]$Message)
    $deadline = [DateTime]::UtcNow.AddSeconds(3)
    do {
        if ($null -eq (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) { return }
        Start-Sleep -Milliseconds 50
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "ASSERTION FAILED: $Message (PID $ProcessId remains alive)"
}

function Assert-FakeTreesStopped {
    param(
        [object[]]$Events,
        [string]$Context,
        [string[]]$ExpectedKinds = @('adapter', 'app-server')
    )
    $treeEvents = @($Events | Where-Object kind -in @('app-server', 'adapter'))
    $actualKinds = @($treeEvents.kind | Sort-Object)
    $expected = @($ExpectedKinds | Sort-Object)
    Assert-True (($actualKinds -join ',') -eq ($expected -join ',')) "$Context process topology must be $($expected -join ','); actual: $($actualKinds -join ',')"
    foreach ($event in $treeEvents) {
        Assert-ProcessStopped -ProcessId $event.pid -Message "$Context $($event.kind) process must stop"
        Assert-ProcessStopped -ProcessId $event.descendant_pid -Message "$Context $($event.kind) descendant must stop"
    }
}

function Invoke-RelayLaunch {
    param(
        [string]$SessionKey,
        [int]$Port,
        [switch]$NewThread,
        [switch]$ExplicitCodex,
        [switch]$UseSelectedBroker,
        [string[]]$CodexArguments = @('-c', 'approvals_reviewer="user"', '--model', 'gpt-test', '--profile', 'relay-test', '-s', 'workspace-write', '-a', 'on-request'),
        [double]$TimeoutSeconds = 8
    )
    $arguments = @{
        RelayName       = 'r3b.test'
        SessionKey      = $SessionKey
        BrokerDirectory = $script:brokerDirectory
        StateDirectory = $script:stateDirectory
        WorkingDirectory = $script:targetDirectory
        Port            = $Port
        BrokerUrl       = $script:brokerUrl
        PythonCommand   = $script:pwshPath
        PythonPrefix    = @('-NoProfile', '-File', $script:fakeDriver, 'python')
        TimeoutSeconds  = $TimeoutSeconds
        CodexArguments  = $CodexArguments
    }
    if ($UseSelectedBroker) { $arguments.Remove('BrokerUrl') }
    if ($NewThread) { $arguments.NewThread = $true }
    if ($ExplicitCodex) {
        $arguments.CodexCommand = $script:pwshPath
        $arguments.CodexPrefix = @('-NoProfile', '-File', $script:fakeDriver, 'codex')
    }
    $env:FAKE_OPERATOR_THREAD = Join-Path $script:testRoot "$SessionKey.operator-thread"
    $env:FAKE_REGISTRATION = Join-Path $script:testRoot "$SessionKey.registration"
    $env:FAKE_NEW_THREAD_ID = "thread-$SessionKey"
    $env:FAKE_READINESS = Join-Path $script:stateDirectory "$SessionKey.ready.json"
    Remove-Item -LiteralPath $env:FAKE_OPERATOR_THREAD, $env:FAKE_REGISTRATION -Force -ErrorAction SilentlyContinue
    & $script:launcher @arguments
}

$testRoot = Join-Path ([IO.Path]::GetTempPath()) ("codex-r3b-{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
$brokerDirectory = Join-Path $testRoot 'selected-broker'
$targetDirectory = Join-Path $testRoot 'target'
$fakeBin = Join-Path $testRoot 'bin'
$eventDirectory = Join-Path $testRoot 'events'
$brokerRequestDirectory = Join-Path $testRoot 'broker-requests'
$fakeDriver = Join-Path $testRoot 'fake-driver.ps1'
$adapterHelper = Join-Path $testRoot 'adapter-register.py'
$fakeBroker = Join-Path $testRoot 'fake-broker.py'
$pwshPath = (Get-Process -Id $PID).Path
$pythonPath = (Get-Command python -ErrorAction Stop | Select-Object -First 1).Source
$originalPath = $env:PATH
$originalSessionId = $env:CLAUDE_CODE_SESSION_ID
$fakeBrokerProcess = $null

try {
    New-Item -ItemType Directory -Path $targetDirectory, $fakeBin, $eventDirectory, $brokerRequestDirectory, $brokerDirectory | Out-Null
    foreach ($filename in @('codex_adapter.py', 'codex_relay_mcp.py')) {
        Copy-Item -LiteralPath (Join-Path $repoRoot "harness/codex/$filename") -Destination $brokerDirectory
    }
    $selectedScripts = Join-Path $brokerDirectory 'scripts'
    New-Item -ItemType Directory -Path $selectedScripts | Out-Null
    Copy-Item -LiteralPath (Join-Path $repoRoot 'deploy/scripts/broker-env.ps1') -Destination $selectedScripts

    # Exercise the actual resolver before constructing any fake session-launch
    # driver. No test below can pass by having a stub synthesize the expected URL.
    $launcherAst = [Management.Automation.Language.Parser]::ParseFile($launcher, [ref]$null, [ref]$null)
    $resolver = $launcherAst.Find({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Resolve-RelayBrokerUrl'
    }, $true)
    Assert-True ($null -ne $resolver) 'selected broker port resolver must exist'
    . ([scriptblock]::Create($resolver.Extent.Text))
    $selectedEnvironment = Join-Path $brokerDirectory 'broker.env'
    Assert-True ((Resolve-RelayBrokerUrl $brokerDirectory) -eq 'http://127.0.0.1:8787') 'missing broker.env uses the fresh loopback default'
    $originalBrokerPort = $env:BROKER_PORT
    $originalBrokerToken = $env:BROKER_BEARER_TOKEN
    foreach ($selection in @('18491', '1', '65535', '"18491"', "'18491'", '" 18491 "', "' 18491 '")) {
        @('BROKER_HOST=0.0.0.0', 'BROKER_BEARER_TOKEN=synthetic-not-imported', " `tBROKER_PORT `t= `t$selection `t") |
            Set-Content -LiteralPath $selectedEnvironment
        $expectedPort = $selection.Trim([char[]]@('"', "'")).Trim()
        Assert-True ((Resolve-RelayBrokerUrl $brokerDirectory) -eq "http://127.0.0.1:$expectedPort") 'selected port determines the loopback URL under the shared whitespace/quote grammar'
        Assert-True ($env:BROKER_PORT -eq $originalBrokerPort -and $env:BROKER_BEARER_TOKEN -eq $originalBrokerToken) 'URL resolution must not import broker.env credentials or process environment'
    }
    Set-Content -LiteralPath $selectedEnvironment -Value 'BROKER_HOST=127.0.0.1'
    Assert-True ((Resolve-RelayBrokerUrl $brokerDirectory) -eq 'http://127.0.0.1:8787') 'missing BROKER_PORT uses the fresh loopback default'
    $invalidDeclarations = @(
        'BROKER_PORT=', 'BROKER_PORT=0', 'BROKER_PORT=-1', 'BROKER_PORT=+1',
        'BROKER_PORT=65536', 'BROKER_PORT=18491.0', 'BROKER_PORT=text',
        'BROKER_PORT=18491 # comment', 'BROKER_PORT=$(throw "must not evaluate")',
        'BROKER_PORT=018491', 'BROKER_PORT="18491', "BROKER_PORT='18491", 
        'BROKER_PORT="18491''', 'BROKER_PORT="18491"junk', 'BROKER_PORT=18491"',
        'broker_port=18491', 'BROKER_PORT :18491', 'export BROKER_PORT=18491',
        "BROKER_PORT=invalid`nBROKER_PORT=18491", "BROKER_PORT=18491`nBROKER_PORT=invalid"
    )
    $refusedStateDirectory = Join-Path $testRoot 'invalid-port-state'
    foreach ($declaration in $invalidDeclarations) {
        Set-Content -LiteralPath $selectedEnvironment -Value $declaration
        $failure = $null
        try {
            & $launcher -SessionKey 'invalid-port' -BrokerDirectory $brokerDirectory `
                -StateDirectory $refusedStateDirectory -WorkingDirectory $targetDirectory
        } catch { $failure = $_ }
        Assert-True ($null -ne $failure -and $failure.Exception.Message -match 'invalid BROKER_PORT') 'invalid selected declaration must fail in the actual launcher before session launch'
        Assert-True (@(Get-ChildItem -LiteralPath $eventDirectory -File).Count -eq 0) 'port validation must not launch any process'
        Assert-True (-not (Test-Path -LiteralPath $refusedStateDirectory)) 'port validation must not create or probe private session state'
        Assert-True ($env:BROKER_PORT -eq $originalBrokerPort -and $env:BROKER_BEARER_TOKEN -eq $originalBrokerToken) 'invalid declarations must not import any process environment'
    }

    @'
import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4


class Broker(ThreadingHTTPServer):
    def __init__(self, address, handler, event_directory):
        super().__init__(address, handler)
        self.event_directory = Path(event_directory)
        self.agents = {}
        self.lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def record(self, payload):
        event = {
            "timestamp_ns": time.time_ns(),
            "method": self.command,
            "path": urlsplit(self.path).path,
            "json": payload,
        }
        path = self.server.event_directory / f"{event['timestamp_ns']}-{uuid4().hex}.json"
        path.write_text(json.dumps(event), encoding="utf-8")

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length)) if length else None

    def send_json(self, status, payload):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def agent_id(self, suffix):
        path = urlsplit(self.path).path
        prefix = "/api/agent/"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None
        return path[len(prefix):-len(suffix)]

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/health":
            self.send_json(200, {"status": "ok"})
            return
        agent_id = self.agent_id("/status")
        self.record(None)
        with self.server.lock:
            agent = dict(self.server.agents.get(agent_id, {})) if agent_id else None
        if not agent:
            self.send_json(404, {"detail": "not found"})
        else:
            self.send_json(200, agent)

    def do_POST(self):
        payload = self.body()
        self.record(payload)
        agent_id = self.agent_id("/status")
        if agent_id is None:
            self.send_json(404, {"detail": "not found"})
            return
        with self.server.lock:
            relay_name = self.server.agents.get(agent_id, {}).get("relay_name")
            self.server.agents[agent_id] = dict(payload, relay_name=relay_name)
        self.send_json(200, {"ok": True})

    def do_PUT(self):
        payload = self.body()
        self.record(payload)
        agent_id = self.agent_id("/relay-name")
        if agent_id is None:
            self.send_json(404, {"detail": "not found"})
            return
        with self.server.lock:
            agent = self.server.agents.setdefault(agent_id, {})
            agent["relay_name"] = payload["relay_name"]
        self.send_json(200, {"ok": True})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--event-directory", required=True)
    args = parser.parse_args()
    Broker(("127.0.0.1", args.port), Handler, args.event_directory).serve_forever()


if __name__ == "__main__":
    main()
'@ | Set-Content -LiteralPath $fakeBroker

    @'
import argparse
import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path


def load_adapter(path):
    spec = importlib.util.spec_from_file_location("r3a_codex_adapter", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegistrationProtocol:
    def __init__(self, module, thread_id):
        self.thread_id = thread_id
        self.thread_status = "idle"


async def run(args):
    module = load_adapter(args.entrypoint)
    store = module.StateStore(args.state)
    state = await store.load()
    if state is None:
        state = module.AdapterState.new(
            working_directory=args.working_directory,
            agent_id=args.agent_id,
        )
        await store.save(state)
    elif state.agent_id != args.agent_id:
        raise RuntimeError("launcher agent identity differs from persisted state")
    readiness_agent_id = os.environ.get("FAKE_READINESS_AGENT_ID")
    if readiness_agent_id:
        Path(args.readiness_file).write_text(
            json.dumps(
                {
                    "version": 1,
                    "agent_id": readiness_agent_id,
                    "status": "ready",
                }
            ),
            encoding="utf-8",
        )
    else:
        await module.publish_readiness_file(args.readiness_file, state)

    operator_thread = Path(os.environ["FAKE_OPERATOR_THREAD"])
    for _ in range(100):
        if operator_thread.is_file():
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("operator thread did not appear")
    state.thread_id = operator_thread.read_text(encoding="utf-8").strip()
    await store.save(state)

    protocol = RegistrationProtocol(module, state.thread_id)
    broker = module.BrokerHttpClient(args.broker_url)
    adapter = module.CodexAdapter(
        protocol,
        broker,
        store,
        state,
        relay_name=args.relay_name,
    )
    try:
        await adapter.register()
    finally:
        await broker.close()
    Path(os.environ["FAKE_REGISTRATION"]).write_text(state.thread_id, encoding="utf-8")
    if os.environ.get("FAKE_ADAPTER_EXIT_AFTER_READY"):
        await asyncio.sleep(0.1)
        return int(os.environ["FAKE_ADAPTER_EXIT_AFTER_READY"])
    while True:
        await asyncio.sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("entrypoint")
    parser.add_argument("--listen-url", required=True)
    parser.add_argument("--app-server-url", required=True)
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--readiness-file", required=True)
    parser.add_argument("--relay-name")
    parser.add_argument("--working-directory", required=True)
    parser.add_argument("--new-thread", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
'@ | Set-Content -LiteralPath $adapterHelper

    @'
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Remaining)
$ErrorActionPreference = 'Stop'
$role = $Remaining[0]
$arguments = @($Remaining | Select-Object -Skip 1)

if ($role -eq 'sentinel') {
    while ($true) { Start-Sleep -Milliseconds 100 }
}

function Write-FakeEvent {
    param([string]$Kind, [hashtable]$Extra = @{})
    $record = [ordered]@{
        timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        pid = $PID
        kind = $Kind
        role = $role
        arguments = $arguments
        cwd = (Get-Location).Path
        claude_session_id = $env:CLAUDE_CODE_SESSION_ID
        readiness_exists = (Test-Path -LiteralPath $env:FAKE_READINESS -PathType Leaf)
    }
    foreach ($key in $Extra.Keys) { $record[$key] = $Extra[$key] }
    $recordPath = Join-Path $env:FAKE_EVENT_DIRECTORY ("{0}-{1}-{2}.json" -f $record.timestamp, $PID, [guid]::NewGuid().ToString('N'))
    $record | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $recordPath
}

function Start-FakeDescendant {
    Start-Process -FilePath $env:FAKE_PWSH -ArgumentList "-NoProfile -File `"$env:FAKE_DRIVER`" sentinel $PID" -WindowStyle Hidden -PassThru
}

if ($role -eq 'codex' -and $arguments -contains 'app-server') {
    $descendant = Start-FakeDescendant
    Write-FakeEvent -Kind 'app-server' -Extra @{ descendant_pid = $descendant.Id }
    if ($env:FAKE_APP_EXIT) { exit [int]$env:FAKE_APP_EXIT }
    $commandConfig = $arguments | Where-Object { $_ -like 'mcp_servers.relay.command=*' } | Select-Object -First 1
    $argumentConfig = $arguments | Where-Object { $_ -like 'mcp_servers.relay.args=*' } | Select-Object -First 1
    if (-not $commandConfig -or -not $argumentConfig) { exit 25 }
    $mcpCommand = $commandConfig.Substring($commandConfig.IndexOf('=') + 1) | ConvertFrom-Json
    $mcpArguments = @($argumentConfig.Substring($argumentConfig.IndexOf('=') + 1) | ConvertFrom-Json)
    & $mcpCommand @mcpArguments
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $listenIndex = [Array]::IndexOf($arguments, '--listen')
    $listenUri = [uri]$arguments[$listenIndex + 1]
    if ($env:FAKE_APP_DELAY_MS) { Start-Sleep -Milliseconds ([int]$env:FAKE_APP_DELAY_MS) }
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $listenUri.Port)
    try {
        $listener.Start()
        while ($true) { Start-Sleep -Milliseconds 100 }
    }
    finally { $listener.Stop() }
}

if ($role -eq 'codex') {
    Write-FakeEvent -Kind 'tui'
    $threadId = if ($arguments -contains 'resume') { $arguments[-1] } else { $env:FAKE_NEW_THREAD_ID }
    Set-Content -LiteralPath $env:FAKE_OPERATOR_THREAD -Value $threadId

    $registrationDeadline = [DateTime]::UtcNow.AddSeconds(3)
    while (-not (Test-Path -LiteralPath $env:FAKE_REGISTRATION -PathType Leaf)) {
        if ([DateTime]::UtcNow -ge $registrationDeadline) { exit 27 }
        Start-Sleep -Milliseconds 50
    }
    if ($env:FAKE_TUI_DELAY_MS) {
        Start-Sleep -Milliseconds ([int]$env:FAKE_TUI_DELAY_MS)
        if ($env:FAKE_TUI_DELAY_COMPLETED) {
            Set-Content -LiteralPath $env:FAKE_TUI_DELAY_COMPLETED -Value 'completed'
        }
    }
    if ($env:FAKE_TUI_EXIT) { exit [int]$env:FAKE_TUI_EXIT }
    exit 0
}

if ($role -eq 'python') {
    $entrypoint = $arguments[0]
    if ([IO.Path]::GetFileName($entrypoint) -eq 'codex_relay_mcp.py') {
        $agentIndex = [Array]::IndexOf($arguments, '--agent-id')
        Write-FakeEvent -Kind 'mcp' -Extra @{
            entrypoint = $entrypoint
            agent_id = $arguments[$agentIndex + 1]
            tool_names = @('register', 'name', 'check', 'send', 'ack')
        }
        if ($env:FAKE_MCP_EXIT) { exit [int]$env:FAKE_MCP_EXIT }
        exit 0
    }

    $descendant = Start-FakeDescendant
    Write-FakeEvent -Kind 'adapter' -Extra @{ descendant_pid = $descendant.Id }
    if ($env:FAKE_ADAPTER_EXIT) { exit [int]$env:FAKE_ADAPTER_EXIT }
    $serverIndex = [Array]::IndexOf($arguments, '--app-server-url')
    $serverUri = [uri]$arguments[$serverIndex + 1]
    $client = [Net.Sockets.TcpClient]::new()
    try {
        $connect = $client.ConnectAsync($serverUri.Host, $serverUri.Port)
        if (-not $connect.Wait(100) -or -not $client.Connected) { exit 19 }
    }
    catch { exit 19 }
    finally { $client.Dispose() }
    if ($env:FAKE_SKIP_READINESS) {
        while ($true) { Start-Sleep -Milliseconds 100 }
    }
    & $env:FAKE_PYTHON -B $env:FAKE_ADAPTER_HELPER @arguments 2> $env:FAKE_ADAPTER_LOG
    exit $LASTEXITCODE
}
throw "unexpected fake role: $role"
'@ | Set-Content -LiteralPath $fakeDriver

    @'
& $env:FAKE_PWSH -NoProfile -File $env:FAKE_DRIVER codex @args
exit $LASTEXITCODE
'@ | Set-Content -LiteralPath (Join-Path $fakeBin 'codex.ps1')

    $env:PATH = "$fakeBin;$originalPath"
    $env:FAKE_PWSH = $pwshPath
    $env:FAKE_PYTHON = $pythonPath
    $env:FAKE_DRIVER = $fakeDriver
    $env:FAKE_ADAPTER_HELPER = $adapterHelper
    $env:FAKE_EVENT_DIRECTORY = $eventDirectory
    $env:FAKE_TARGET_DIRECTORY = $targetDirectory
    $env:FAKE_ADAPTER_LOG = Join-Path $testRoot 'adapter.log'
    $env:CLAUDE_CODE_SESSION_ID = 'inherited-claude-session'

    $fakeBrokerPort = Get-FreeLoopbackPort
    $brokerUrl = "http://127.0.0.1:$fakeBrokerPort"
    Set-Content -LiteralPath $selectedEnvironment -Value " `tBROKER_PORT `t= `t' $fakeBrokerPort ' `t"
    $fakeBrokerProcess = Start-Process -FilePath $pythonPath -ArgumentList @(
        '-B', $fakeBroker,
        '--port', $fakeBrokerPort,
        '--event-directory', $brokerRequestDirectory
    ) -WindowStyle Hidden -PassThru
    Wait-FakeBroker -Url $brokerUrl -Process $fakeBrokerProcess

    $stateDirectory = Join-Path $testRoot 'private-state'
    New-Item -ItemType Directory -Path $stateDirectory | Out-Null
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $acl = [Security.AccessControl.DirectorySecurity]::new()
    $acl.SetOwner($sid)
    $acl.SetAccessRuleProtection($true, $false)
    $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow'))
    Set-Acl -LiteralPath $stateDirectory -AclObject $acl
    $sessionKey = "r3b-test-$PID"
    $statePath = Join-Path $stateDirectory "$sessionKey.json"
    $readinessPath = Join-Path $stateDirectory "$sessionKey.ready.json"
    $env:FAKE_READINESS = $readinessPath
    Remove-Item -LiteralPath $statePath, $readinessPath -Force -ErrorAction SilentlyContinue

    $env:FAKE_APP_DELAY_MS = '2000'
    try { Invoke-RelayLaunch -SessionKey $sessionKey -Port 45101 -UseSelectedBroker }
    catch {
        $adapterLog = if (Test-Path -LiteralPath $env:FAKE_ADAPTER_LOG) { Get-Content -LiteralPath $env:FAKE_ADAPTER_LOG -Raw } else { '<missing>' }
        throw "$($_.Exception.Message); adapter log: $adapterLog"
    }
    Remove-Item Env:FAKE_APP_DELAY_MS
    $firstEvents = Get-FakeEvents $eventDirectory
    $firstAdapter = @($firstEvents | Where-Object kind -eq 'adapter')[-1]
    $firstAppServer = @($firstEvents | Where-Object kind -eq 'app-server')[-1]
    $firstTui = @($firstEvents | Where-Object kind -eq 'tui')[-1]
    $firstMcp = @($firstEvents | Where-Object kind -eq 'mcp')[-1]
    $firstBrokerRequests = Get-BrokerRequests
    $firstAppServerCommandLine = $firstAppServer.arguments -join ' '
    $firstTuiCommandLine = $firstTui.arguments -join ' '
    # The fake app-server child serializes its own received argv into this event. These checks cover
    # Windows native command-line quoting, not merely the launcher's pre-spawn argument array.
    $firstMcpConfig = @($firstAppServer.arguments | Where-Object { $_ -like 'mcp_servers.relay.*' })
    $firstMcpConfigIsPaired = $true
    for ($argumentIndex = 0; $argumentIndex -lt $firstAppServer.arguments.Count; $argumentIndex++) {
        if ($firstAppServer.arguments[$argumentIndex] -like 'mcp_servers.relay.*' -and
            ($argumentIndex -eq 0 -or $firstAppServer.arguments[$argumentIndex - 1] -ne '-c')) {
            $firstMcpConfigIsPaired = $false
        }
    }
    $persisted = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    Assert-True ($persisted.version -eq 3) 'adapter must persist the permission-aware v3 state schema'
    Assert-True ($firstTui.readiness_exists -eq $true) 'adapter readiness must precede the TUI'
    Assert-True ($firstAppServer.arguments -notcontains 'ws://127.0.0.1:45101') 'app-server must stay behind the adapter proxy endpoint'
    Assert-True ($firstTui.arguments -contains 'ws://127.0.0.1:45101') 'TUI must attach to the requested adapter proxy endpoint'
    Assert-True ($firstAdapter.arguments -contains '--listen-url') 'adapter must expose the normal TUI proxy endpoint'
    Assert-True ($firstAdapter.arguments -contains 'ws://127.0.0.1:45101') 'adapter proxy endpoint must match the TUI remote endpoint'
    Assert-True ($firstAdapter.arguments -contains '--readiness-file') 'adapter must receive the readiness path'
    Assert-True ($firstAdapter.arguments -contains '--relay-name') 'adapter must receive the relay name'
    Assert-True ($firstAdapter.arguments -contains 'r3b.test') 'adapter must receive the relay-name value'
    Assert-True ($firstAdapter.arguments -contains $statePath) 'adapter must own the project-local state path'
    Assert-True ($firstAdapter.arguments -contains (Join-Path $brokerDirectory 'codex_adapter.py')) 'launcher must use the broker adapter entrypoint'
    Assert-True ($firstMcpConfig.Count -eq 11) 'app-server must receive the complete eleven-entry relay configuration'
    Assert-True $firstMcpConfigIsPaired 'app-server child must receive every relay entry immediately after its own -c flag'
    Assert-True (@($firstMcpConfig | Where-Object { $_ -like 'mcp_servers.relay.command=*' }).Count -eq 1) 'app-server must receive exactly one relay command argument'
    Assert-True (@($firstMcpConfig | Where-Object { $_ -like 'mcp_servers.relay.args=*' }).Count -eq 1) 'app-server must receive exactly one relay args argument'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.enabled=true') 'app-server must enable relay for the session'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.required=true') 'app-server must require relay for the session'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.enabled_tools=["register","name","check","send","ack"]') 'app-server must expose the session-bound receipt ack tool'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.default_tools_approval_mode="prompt"') 'app-server must default relay tools to prompt'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.tools.check.approval_mode="approve"') 'app-server must automatically approve relay check'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.tools.register.approval_mode="prompt"') 'app-server must prompt for relay register'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.tools.name.approval_mode="prompt"') 'app-server must prompt for relay name'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.tools.send.approval_mode="prompt"') 'app-server must prompt for relay send'
    Assert-True ($firstMcpConfig -contains 'mcp_servers.relay.tools.ack.approval_mode="approve"') 'app-server must permit immediate receipt ack without a human prompt'
    Assert-True ($firstAppServerCommandLine.Contains('codex_relay_mcp.py')) 'app-server must configure the R3A MCP entrypoint before a remote TUI attaches'
    Assert-True ($firstAppServerCommandLine.Contains('--agent-id')) 'app-server MCP config must include the adapter agent id'
    Assert-True ([string]$firstMcp.agent_id -eq [string]$persisted.agent_id) 'app-server MCP identity must equal the persisted adapter identity'
    Assert-True ($firstAppServerCommandLine.Contains($brokerUrl)) 'app-server MCP config must include the broker URL'
    $adapterBrokerIndex = [Array]::IndexOf(@($firstAdapter.arguments), '--broker-url')
    Assert-True ($adapterBrokerIndex -ge 0 -and $firstAdapter.arguments[$adapterBrokerIndex + 1] -eq $brokerUrl) 'omitted BrokerUrl must resolve the selected broker.env port for the adapter'
    Assert-True (-not $firstAppServerCommandLine.Contains("$sessionKey.json")) 'MCP config must not expose adapter state'
    Assert-True (-not $firstTuiCommandLine.Contains('codex_relay_mcp.py')) 'remote TUI must rely on the app-server MCP configuration'
    Assert-True ($firstTui.arguments -notcontains 'resume') 'first launch must let the TUI create its thread'
    Assert-True ($firstTui.cwd -eq $targetDirectory) 'TUI must run in the target working directory'
    Assert-True (($firstTui.arguments -join ' ') -match '--model gpt-test --profile relay-test -s workspace-write -a on-request') 'launcher must preserve model, profile, sandbox, and approval choices'
    Assert-True ($firstTui.claude_session_id -eq 'inherited-claude-session') 'Claude session identity must be inherited unchanged'
    Assert-True ($firstMcp.entrypoint -eq (Join-Path $brokerDirectory 'codex_relay_mcp.py')) 'app-server must invoke the configured R3A MCP entrypoint'
    $parsedMcpAgentId = [guid]::Empty
    Assert-True ([guid]::TryParse([string]$firstMcp.agent_id, [ref]$parsedMcpAgentId)) 'MCP invocation must receive the readiness agent id'
    Assert-True (@($firstMcp.tool_names) -join ',' -eq 'register,name,check,send,ack') 'relay front door must expose register, name, check, send, and ack'
    $statusPath = "/api/agent/$($persisted.agent_id)/status"
    $namePath = "/api/agent/$($persisted.agent_id)/relay-name"
    $statusRequest = @($firstBrokerRequests | Where-Object { $_.method -eq 'POST' -and $_.path -eq $statusPath })[-1]
    $nameRequest = @($firstBrokerRequests | Where-Object { $_.method -eq 'PUT' -and $_.path -eq $namePath })[-1]
    Assert-True ($null -ne $statusRequest) 'actual R3A adapter registration must contact the fake broker status endpoint'
    Assert-True ($statusRequest.json.thread_id -eq "thread-$sessionKey") 'actual R3A adapter must register the operator-created thread'
    Assert-True ($statusRequest.json.kind -eq 'codex-session') 'actual R3A adapter must identify a Codex session'
    Assert-True ($statusRequest.json.status -eq 'idle') 'actual R3A adapter must register an idle operator session'
    Assert-True ($null -ne $nameRequest) 'actual R3A adapter registration must contact the fake broker name endpoint'
    Assert-True ($nameRequest.json.relay_name -eq 'r3b.test') 'actual R3A adapter must claim the requested relay name'
    Assert-FakeTreesStopped -Events $firstEvents -Context 'first launch'

    Assert-True ($persisted.thread_id -eq "thread-$sessionKey") 'adapter must persist the operator-created thread before registration'
    $persistedAgentId = ([guid]$persisted.agent_id).ToString()
    $persisted.version = 3
    $persisted.agent_id = '{' + $persistedAgentId.ToUpperInvariant() + '}'
    $persisted.thread_id = 'thread-existing'
    $persisted.sandbox = 'workspace-write'
    $persisted.approval_policy = 'on-request'
    $persisted.approvals_reviewer = 'user'
    $persisted | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $statePath
    Reset-FakeRun
    $attachedPermissionArguments = @(
        '--model', 'gpt-test',
        '--profile', 'relay-test',
        '--sandbox=workspace-write',
        '-sworkspace-write',
        '--ask-for-approval=on-request',
        '-aon-request',
        '--config=approvals_reviewer="user"',
        '-capprovals_reviewer="user"'
    )
    # Subsequent explicit URLs remain authoritative even if broker.env is invalid.
    Set-Content -LiteralPath $selectedEnvironment -Value 'BROKER_PORT=invalid'
    Invoke-RelayLaunch -SessionKey $sessionKey -Port 45102 -CodexArguments $attachedPermissionArguments
    $resumeEvents = Get-FakeEvents $eventDirectory
    $resumeBrokerRequests = Get-BrokerRequests
    $resumeTui = @($resumeEvents | Where-Object kind -eq 'tui')[-1]
    $resumeMcp = @($resumeEvents | Where-Object kind -eq 'mcp')[-1]
    $resumedState = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    Assert-True ($resumeTui.arguments -contains 'resume') 'resume must remain the Codex subcommand'
    Assert-True ($resumeTui.arguments[-1] -eq 'thread-existing') 'stored thread must follow all resume options'
    Assert-True ($resumeTui.arguments -contains '--model') 'resume must preserve the model choice'
    Assert-True ($resumeTui.arguments -contains '--profile') 'resume must preserve the profile choice'
    Assert-True ($resumeTui.arguments -notcontains '-s') 'remote resume must not repeat the sandbox override'
    Assert-True ($resumeTui.arguments -notcontains 'workspace-write') 'remote resume must remove the sandbox value with its flag'
    Assert-True (-not (@($resumeTui.arguments | Where-Object { $_ -match '^(?:--sandbox=|-s.+)' }).Count)) 'remote resume must remove attached sandbox overrides'
    Assert-True ($resumeTui.arguments -notcontains '-a') 'remote resume must not repeat the approval override'
    Assert-True ($resumeTui.arguments -notcontains 'on-request') 'remote resume must remove the approval value with its flag'
    Assert-True (-not (@($resumeTui.arguments | Where-Object { $_ -match '^(?:--ask-for-approval=|-a.+)' }).Count)) 'remote resume must remove attached approval overrides'
    Assert-True ($resumeTui.arguments -notcontains 'approvals_reviewer="user"') 'remote resume must not repeat the approval-reviewer override'
    Assert-True (-not (@($resumeTui.arguments | Where-Object { $_ -match '^(?:--config=|-c)approvals_reviewer=' }).Count)) 'remote resume must remove attached approval-reviewer overrides'
    Assert-True ([string]$resumeMcp.agent_id -eq $persistedAgentId) 'resume MCP identity must equal the canonical persisted adapter identity'
    Assert-True ([string]$resumedState.agent_id -eq $persistedAgentId) 'resume must canonicalize a valid persisted adapter identity'
    $resumeStatus = @($resumeBrokerRequests | Where-Object { $_.method -eq 'POST' -and $_.path -eq $statusPath })[-1]
    Assert-True ($resumeStatus.json.thread_id -eq 'thread-existing') 'actual R3A adapter must register the stored thread on resume'
    Assert-FakeTreesStopped -Events $resumeEvents -Context 'resume launch'

    Reset-FakeRun
    Invoke-RelayLaunch -SessionKey $sessionKey -Port 45103 -NewThread -ExplicitCodex -CodexArguments $attachedPermissionArguments
    $newEvents = Get-FakeEvents $eventDirectory
    $newBrokerRequests = Get-BrokerRequests
    $newAdapter = @($newEvents | Where-Object kind -eq 'adapter')[-1]
    $newTui = @($newEvents | Where-Object kind -eq 'tui')[-1]
    $newMcp = @($newEvents | Where-Object kind -eq 'mcp')[-1]
    $newState = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    Assert-True ($newAdapter.arguments -contains '--new-thread') 'new-thread action must reach the adapter'
    Assert-True ($newTui.arguments -notcontains 'resume') 'new-thread action must create a normal TUI thread'
    Assert-True ($newTui.arguments -notcontains 'thread-existing') 'new-thread action must not resume the stored thread'
    foreach ($permissionArgument in @(
        '--sandbox=workspace-write',
        '-sworkspace-write',
        '--ask-for-approval=on-request',
        '-aon-request',
        '--config=approvals_reviewer="user"',
        '-capprovals_reviewer="user"'
    )) {
        Assert-True ($newTui.arguments -contains $permissionArgument) "new-thread action must preserve $permissionArgument"
    }
    Assert-True ([string]$newMcp.agent_id -eq $persistedAgentId) 'new-thread MCP identity must keep the persisted adapter identity'
    Assert-True ([string]$newState.agent_id -eq $persistedAgentId) 'new-thread launch must keep the persisted adapter identity'
    $newStatus = @($newBrokerRequests | Where-Object { $_.method -eq 'POST' -and $_.path -eq $statusPath })[-1]
    Assert-True ($newStatus.json.thread_id -eq "thread-$sessionKey") 'actual R3A adapter must register the new operator-created thread'
    Assert-FakeTreesStopped -Events $newEvents -Context 'new-thread launch'

    Reset-FakeRun
    $env:FAKE_TUI_EXIT = '23'
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-fail-$PID" -Port 45104 } catch { $failure = $_ }
    Assert-True ($null -ne $failure) 'nonzero TUI exit must fail the launcher'
    Assert-True ($failure.Exception.Message -match '23') 'launcher failure must preserve the TUI exit code'
    Remove-Item Env:FAKE_TUI_EXIT
    Assert-FakeTreesStopped -Events (Get-FakeEvents $eventDirectory) -Context 'TUI failure'

    Reset-FakeRun
    $env:FAKE_ADAPTER_EXIT = '17'
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-adapter-fail-$PID" -Port 45105 } catch { $failure = $_ }
    Assert-True ($failure.Exception.Message -match 'adapter exited with code 17') "adapter failure must be deterministic; actual: $($failure.Exception.Message)"
    Remove-Item Env:FAKE_ADAPTER_EXIT
    Assert-FakeTreesStopped -Events (Get-FakeEvents $eventDirectory) -Context 'adapter startup failure'

    Reset-FakeRun
    $env:FAKE_APP_EXIT = '31'
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-app-fail-$PID" -Port 45106 } catch { $failure = $_ }
    Assert-True ($failure.Exception.Message -match 'app-server exited with code 31') "app-server failure must be deterministic; actual: $($failure.Exception.Message)"
    Remove-Item Env:FAKE_APP_EXIT
    Assert-FakeTreesStopped -Events (Get-FakeEvents $eventDirectory) -Context 'app-server startup failure' -ExpectedKinds @('app-server')

    Reset-FakeRun
    $env:FAKE_MCP_EXIT = '44'
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-mcp-fail-$PID" -Port 45112 } catch { $failure = $_ }
    Assert-True ($null -ne $failure) 'required MCP startup failure must stop relay startup'
    Assert-True ($failure.Exception.Message -match 'app-server exited with code 44') "required MCP failure must be visible with its exit code; actual: $($failure.Exception.Message)"
    Remove-Item Env:FAKE_MCP_EXIT
    Assert-True (@(Get-FakeEvents $eventDirectory | Where-Object kind -eq 'tui').Count -eq 0) 'required MCP failure must stop before the TUI starts'
    Assert-FakeTreesStopped -Events (Get-FakeEvents $eventDirectory) -Context 'required MCP failure' -ExpectedKinds @('app-server')

    Reset-FakeRun
    $env:FAKE_SKIP_READINESS = '1'
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-ready-fail-$PID" -Port 45108 -TimeoutSeconds 3 } catch { $failure = $_ }
    Assert-True ($failure.Exception.Message -match 'adapter readiness timed out') "readiness failure must be deterministic; actual: $($failure.Exception.Message)"
    Remove-Item Env:FAKE_SKIP_READINESS
    Assert-FakeTreesStopped -Events (Get-FakeEvents $eventDirectory) -Context 'readiness failure'

    Reset-FakeRun
    $env:FAKE_ADAPTER_EXIT_AFTER_READY = '42'
    $env:FAKE_TUI_DELAY_MS = '3000'
    $env:FAKE_TUI_DELAY_COMPLETED = Join-Path $testRoot 'tui-delay-completed'
    Remove-Item -LiteralPath $env:FAKE_TUI_DELAY_COMPLETED -Force -ErrorAction SilentlyContinue
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-runtime-fail-$PID" -Port 45109 } catch { $failure = $_ }
    Assert-True ($null -ne $failure) 'post-readiness adapter exit must fail the launcher'
    Assert-True ($failure.Exception.Message -match 'adapter exited with code 42 while the TUI was active') "post-readiness adapter exit must retain its code and lifecycle phase; actual: $($failure.Exception.Message)"
    Assert-True (-not (Test-Path -LiteralPath $env:FAKE_TUI_DELAY_COMPLETED -PathType Leaf)) 'post-readiness adapter exit must interrupt the TUI before its foreground delay completes'
    Remove-Item Env:FAKE_ADAPTER_EXIT_AFTER_READY, Env:FAKE_TUI_DELAY_MS, Env:FAKE_TUI_DELAY_COMPLETED
    Assert-FakeTreesStopped -Events (Get-FakeEvents $eventDirectory) -Context 'post-readiness adapter failure'

    Reset-FakeRun
    $env:FAKE_READINESS_AGENT_ID = [guid]::NewGuid().ToString()
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey "r3b-readiness-id-fail-$PID" -Port 45110 } catch { $failure = $_ }
    Assert-True ($null -ne $failure) 'mismatched readiness identity must fail the launcher'
    Assert-True ($failure.Exception.Message -match 'readiness agent identity differs') "readiness identity failure must be deterministic; actual: $($failure.Exception.Message)"
    Remove-Item Env:FAKE_READINESS_AGENT_ID
    $readinessMismatchEvents = Get-FakeEvents $eventDirectory
    Assert-True (@($readinessMismatchEvents | Where-Object kind -eq 'tui').Count -eq 0) 'readiness identity mismatch must fail before TUI launch'
    Assert-FakeTreesStopped -Events $readinessMismatchEvents -Context 'readiness identity mismatch'

    Reset-FakeRun
    $badKey = "r3b-bad-state-$PID"
    Set-Content -LiteralPath (Join-Path $stateDirectory "$badKey.json") -Value '{not-json'
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey $badKey -Port 45107 } catch { $failure = $_ }
    Assert-True ($failure.Exception.Message -match 'stored adapter state is invalid') 'invalid stored state must fail before launch'
    Assert-True (@(Get-FakeEvents $eventDirectory).Count -eq 0) 'invalid state must fail before any process starts'

    Reset-FakeRun
    $badIdKey = "r3b-bad-agent-id-$PID"
    @{
        version = 1
        agent_id = 'not-a-guid'
        thread_id = $null
        working_directory = $targetDirectory
        accepted_unacked = @()
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $stateDirectory "$badIdKey.json")
    $failure = $null
    try { Invoke-RelayLaunch -SessionKey $badIdKey -Port 45111 } catch { $failure = $_ }
    Assert-True ($failure.Exception.Message -match 'invalid version or agent_id') 'invalid stored agent identity must fail with the state validation error'
    Assert-True (@(Get-FakeEvents $eventDirectory).Count -eq 0) 'invalid stored agent identity must fail before any process starts'

    Assert-True (@(Get-ChildItem -LiteralPath $targetDirectory -Force).Count -eq 0) 'launcher must not write the target project'
    Write-Output 'PASS: actual R3A CodexAdapter.register used fake broker HTTP with stable agent, thread, and relay name'
    Write-Output 'PASS: background exit interrupted the still-open foreground TUI'
    Write-Output 'PASS: cleanup job stopped descendants that intentionally outlived their parent'
    Write-Output 'PASS: project-local relay launcher registration and orchestration'
}
finally {
    $env:PATH = $originalPath
    if ($null -eq $originalSessionId) { Remove-Item Env:CLAUDE_CODE_SESSION_ID -ErrorAction SilentlyContinue }
    else { $env:CLAUDE_CODE_SESSION_ID = $originalSessionId }
    if ($null -ne $fakeBrokerProcess) {
        $fakeBrokerProcess.Refresh()
        if (-not $fakeBrokerProcess.HasExited) { $fakeBrokerProcess.Kill() }
        $fakeBrokerProcess.WaitForExit(3000) | Out-Null
    }
    if (Test-Path -LiteralPath $eventDirectory -PathType Container) {
        $fakePids = foreach ($event in (Get-FakeEvents $eventDirectory)) {
            $event.pid
            if ($event.PSObject.Properties['descendant_pid']) { $event.descendant_pid }
        }
        $fakePids | Where-Object { $_ } | Sort-Object -Unique | ForEach-Object {
            Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
        }
    }
    Remove-Item Env:FAKE_PWSH, Env:FAKE_PYTHON, Env:FAKE_DRIVER, Env:FAKE_ADAPTER_HELPER, Env:FAKE_ADAPTER_LOG, Env:FAKE_EVENT_DIRECTORY, Env:FAKE_TARGET_DIRECTORY, Env:FAKE_READINESS, Env:FAKE_OPERATOR_THREAD, Env:FAKE_REGISTRATION, Env:FAKE_NEW_THREAD_ID, Env:FAKE_TUI_EXIT, Env:FAKE_TUI_DELAY_MS, Env:FAKE_TUI_DELAY_COMPLETED, Env:FAKE_ADAPTER_EXIT, Env:FAKE_ADAPTER_EXIT_AFTER_READY, Env:FAKE_APP_EXIT, Env:FAKE_APP_DELAY_MS, Env:FAKE_MCP_EXIT, Env:FAKE_SKIP_READINESS, Env:FAKE_READINESS_AGENT_ID -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $testRoot -Recurse -Force -ErrorAction SilentlyContinue
    # State and readiness were contained under the removed disposable testRoot.
}
