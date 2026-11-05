#Requires -Version 7.0

[CmdletBinding()]
param(
    [ValidatePattern('^[A-Za-z0-9._-]{1,64}$')][string]$RelayName,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9._-]{1,64}$')][string]$SessionKey,
    [Parameter(Mandatory)][string]$BrokerDirectory,
    [Parameter(Mandatory)][string]$StateDirectory,
    [Parameter(Mandatory)][string]$WorkingDirectory,
    [switch]$NewThread,
    [ValidateRange(0, 65535)][int]$Port = 0,
    [ValidateRange(0, 65535)][int]$AppServerPort = 0,
    [string]$BrokerUrl,
    [string]$CodexCommand = 'codex',
    [string[]]$CodexPrefix = @(),
    [string]$PythonCommand = 'python',
    [string[]]$PythonPrefix = @(),
    [ValidateRange(1, 3600)][double]$TimeoutSeconds = 120,
    [Parameter(ValueFromRemainingArguments)][string[]]$CodexArguments = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-RelayBrokerUrl {
    param([string]$BrokerDirectory)

    # Installed launchers share their sibling utility. Direct source launches use
    # the exact checkout utility; selected installed roots also support callers
    # that load this resolver independently. Never search ancestor/private roots.
    $helperPaths = @()
    if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) {
        $helperPaths += Join-Path $PSScriptRoot 'broker-env.ps1'
        $helperPaths += Join-Path $PSScriptRoot '../../deploy/scripts/broker-env.ps1'
    }
    $helperPaths += Join-Path $BrokerDirectory 'scripts/broker-env.ps1'
    $helper = $helperPaths | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
    if (-not $helper) { throw 'The shared broker.env parser is missing; reinstall the selected exact release.' }
    . $helper
    # Validate only the nonsecret bind selection; never import credentials or
    # change process environment/state while resolving the selected loopback URL.
    $selectedPort = Get-BrokerEnvPort -Path (Join-Path $BrokerDirectory 'broker.env')
    "http://127.0.0.1:$selectedPort"
}

# Resolve before touching private session state or starting any child process.
# An explicit caller URL remains authoritative and does not read broker.env.
if (-not $PSBoundParameters.ContainsKey('BrokerUrl')) {
    $BrokerUrl = Resolve-RelayBrokerUrl $BrokerDirectory
}

Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

public sealed class RelayProcessJob : IDisposable
{
    private const uint CREATE_SUSPENDED = 0x00000004;
    private const uint CREATE_NEW_CONSOLE = 0x00000010;
    private const uint CREATE_NO_WINDOW = 0x08000000;
    private const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;
    private IntPtr handle;

    [StructLayout(LayoutKind.Sequential)]
    private struct SECURITY_ATTRIBUTES
    {
        public int nLength;
        public IntPtr lpSecurityDescriptor;
        public int bInheritHandle;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct STARTUPINFO
    {
        public int cb;
        public string lpReserved;
        public string lpDesktop;
        public string lpTitle;
        public uint dwX;
        public uint dwY;
        public uint dwXSize;
        public uint dwYSize;
        public uint dwXCountChars;
        public uint dwYCountChars;
        public uint dwFillAttribute;
        public uint dwFlags;
        public short wShowWindow;
        public short cbReserved2;
        public IntPtr lpReserved2;
        public IntPtr hStdInput;
        public IntPtr hStdOutput;
        public IntPtr hStdError;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct PROCESS_INFORMATION
    {
        public IntPtr hProcess;
        public IntPtr hThread;
        public uint dwProcessId;
        public uint dwThreadId;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public uint LimitFlags;
        public UIntPtr MinimumWorkingSetSize;
        public UIntPtr MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public UIntPtr Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct IO_COUNTERS
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    {
        public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
        public IO_COUNTERS IoInfo;
        public UIntPtr ProcessMemoryLimit;
        public UIntPtr JobMemoryLimit;
        public UIntPtr PeakProcessMemoryUsed;
        public UIntPtr PeakJobMemoryUsed;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateJobObject(IntPtr attributes, string name);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetInformationJobObject(
        IntPtr job,
        int informationClass,
        IntPtr information,
        uint informationLength);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool CreateProcess(
        string applicationName,
        StringBuilder commandLine,
        IntPtr processAttributes,
        IntPtr threadAttributes,
        bool inheritHandles,
        uint creationFlags,
        IntPtr environment,
        string currentDirectory,
        ref STARTUPINFO startupInfo,
        out PROCESS_INFORMATION processInformation);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint ResumeThread(IntPtr thread);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool TerminateProcess(IntPtr process, uint exitCode);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr value);

    public RelayProcessJob()
    {
        handle = CreateJobObject(IntPtr.Zero, null);
        if (handle == IntPtr.Zero)
            throw new Win32Exception(Marshal.GetLastWin32Error(), "could not create relay process job");

        JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        int size = Marshal.SizeOf(limits);
        IntPtr buffer = Marshal.AllocHGlobal(size);
        try
        {
            Marshal.StructureToPtr(limits, buffer, false);
            if (!SetInformationJobObject(handle, 9, buffer, (uint)size))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "could not configure relay process job");
        }
        catch
        {
            CloseHandle(handle);
            handle = IntPtr.Zero;
            throw;
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    public Process StartProcess(
        string filePath,
        string commandLine,
        string workingDirectory,
        string windowMode)
    {
        if (handle == IntPtr.Zero)
            throw new ObjectDisposedException(nameof(RelayProcessJob));

        uint flags = CREATE_SUSPENDED;
        if (windowMode == "Hidden")
            flags |= CREATE_NO_WINDOW;
        else if (windowMode == "Normal")
            flags |= CREATE_NEW_CONSOLE;
        else if (windowMode != "CurrentConsole")
            throw new ArgumentOutOfRangeException(nameof(windowMode));

        STARTUPINFO startup = new STARTUPINFO();
        startup.cb = Marshal.SizeOf(startup);
        PROCESS_INFORMATION information;
        if (!CreateProcess(
                filePath,
                new StringBuilder(commandLine),
                IntPtr.Zero,
                IntPtr.Zero,
                true,
                flags,
                IntPtr.Zero,
                workingDirectory,
                ref startup,
                out information))
            throw new Win32Exception(Marshal.GetLastWin32Error(), $"could not start relay process: {filePath}");

        try
        {
            if (!AssignProcessToJobObject(handle, information.hProcess))
            {
                int error = Marshal.GetLastWin32Error();
                TerminateProcess(information.hProcess, 1);
                throw new Win32Exception(error, $"could not assign relay process to cleanup job: {filePath}");
            }
            Process process = Process.GetProcessById((int)information.dwProcessId);
            IntPtr retainedProcessHandle = process.Handle;
            if (ResumeThread(information.hThread) == UInt32.MaxValue)
            {
                int error = Marshal.GetLastWin32Error();
                TerminateProcess(information.hProcess, 1);
                process.Dispose();
                throw new Win32Exception(error, $"could not resume relay process: {filePath}");
            }
            return process;
        }
        finally
        {
            CloseHandle(information.hThread);
            CloseHandle(information.hProcess);
        }
    }

    public void Dispose()
    {
        if (handle == IntPtr.Zero)
            return;
        IntPtr current = handle;
        handle = IntPtr.Zero;
        if (!CloseHandle(current))
            throw new Win32Exception(Marshal.GetLastWin32Error(), "could not close relay process job");
    }
}
'@

function Resolve-RelayPath {
    param([string]$Path, [string]$Description, [switch]$Directory)
    $item = Get-Item -LiteralPath $Path -ErrorAction Stop
    if ($Directory -and -not $item.PSIsContainer) {
        throw "$Description is not a directory: $Path"
    }
    $item.FullName
}

function Resolve-RelayCommand {
    param([string]$Command, [string[]]$Prefix)
    $commandInfo = Get-Command $Command -ErrorAction Stop | Select-Object -First 1
    if ($commandInfo.CommandType -eq [Management.Automation.CommandTypes]::ExternalScript) {
        $hostExecutable = (Get-Process -Id $PID).Path
        return [pscustomobject]@{
            ForegroundFile = $hostExecutable
            ForegroundPrefix = @('-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $commandInfo.Source) + @($Prefix)
            BackgroundFile = $hostExecutable
            BackgroundPrefix = @('-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $commandInfo.Source) + @($Prefix)
        }
    }
    if ($commandInfo.CommandType -notin @(
            [Management.Automation.CommandTypes]::Application,
            [Management.Automation.CommandTypes]::ExternalScript
        )) {
        throw "relay command must resolve to an application or external script: $Command"
    }
    [pscustomobject]@{
        ForegroundFile = $commandInfo.Source
        ForegroundPrefix = @($Prefix)
        BackgroundFile = $commandInfo.Source
        BackgroundPrefix = @($Prefix)
    }
}

function ConvertTo-NativeArgument {
    param([AllowEmptyString()][string]$Value)
    if ($Value -notmatch '[\s"]' -and $Value.Length -gt 0) { return $Value }
    $builder = [Text.StringBuilder]::new('"')
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') {
            $backslashes++
            continue
        }
        if ($character -eq '"') {
            [void]$builder.Append(('\' * (($backslashes * 2) + 1)) + '"')
            $backslashes = 0
            continue
        }
        if ($backslashes) { [void]$builder.Append('\' * $backslashes); $backslashes = 0 }
        [void]$builder.Append($character)
    }
    if ($backslashes) { [void]$builder.Append('\' * ($backslashes * 2)) }
    [void]$builder.Append('"')
    $builder.ToString()
}

function Start-RelayProcess {
    param(
        [object]$Job,
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$ProcessWorkingDirectory,
        [ValidateSet('Hidden', 'Normal', 'CurrentConsole')][string]$WindowMode
    )
    $argumentLine = (@($Arguments) | ForEach-Object { ConvertTo-NativeArgument ([string]$_) }) -join ' '
    $commandLine = (ConvertTo-NativeArgument $FilePath) + $(if ($argumentLine) { " $argumentLine" } else { '' })
    $Job.StartProcess($FilePath, $commandLine, $ProcessWorkingDirectory, $WindowMode)
}

function Stop-RelayProcess {
    param([Diagnostics.Process]$Process, [string]$Description)
    if ($null -eq $Process) { return }
    $Process.Refresh()
    if ($Process.HasExited) { return }
    $closed = $Process.CloseMainWindow()
    if ($closed -and $Process.WaitForExit(1500)) { return }
    $Process.Refresh()
    if ($Process.HasExited) { return }
    $killer = Start-Process -FilePath (Join-Path $env:SystemRoot 'System32/taskkill.exe') -ArgumentList "/PID $($Process.Id) /T /F" -WindowStyle Hidden -Wait -PassThru
    if ($killer.ExitCode -ne 0) {
        $Process.Refresh()
        if (-not $Process.HasExited) {
            throw "$Description tree cleanup failed: taskkill exited $($killer.ExitCode) for PID $($Process.Id)"
        }
    }
    if (-not $Process.WaitForExit(5000)) {
        throw "$Description tree cleanup timed out for PID $($Process.Id)"
    }
}

function ConvertTo-TomlString {
    param([string]$Value)
    '"' + $Value.Replace('\', '\\').Replace('"', '\"').Replace("`r", '\r').Replace("`n", '\n') + '"'
}

function Remove-RemoteResumePermissionOverrides {
    param([string[]]$Arguments)
    $filtered = [Collections.Generic.List[string]]::new()
    for ($index = 0; $index -lt $Arguments.Count; $index++) {
        if ($Arguments[$index] -match '^(?:--config=|-c)approvals_reviewer=' -or
            $Arguments[$index] -match '^(?:--sandbox=|-s.+)' -or
            $Arguments[$index] -match '^(?:--ask-for-approval=|-a.+)') {
            continue
        }
        if ($Arguments[$index] -in @('-c', '--config') -and $index + 1 -lt $Arguments.Count -and
            $Arguments[$index + 1] -match '^approvals_reviewer=') {
            $index++
            continue
        }
        if ($Arguments[$index] -in @('-s', '--sandbox', '-a', '--ask-for-approval')) {
            if ($index + 1 -ge $Arguments.Count) {
                throw "Codex permission option is missing its value: $($Arguments[$index])"
            }
            $index++
            continue
        }
        $filtered.Add($Arguments[$index])
    }
    @($filtered)
}

function Get-FreeLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        ([Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Wait-LoopbackListener {
    param(
        [Diagnostics.Process]$Process,
        [int]$ListenerPort,
        [DateTime]$Deadline
    )
    while ([DateTime]::UtcNow -lt $Deadline) {
        $Process.Refresh()
        if ($Process.HasExited) {
            throw "app-server exited with code $($Process.ExitCode) before listening"
        }
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $connect = $client.ConnectAsync([Net.IPAddress]::Loopback, $ListenerPort)
            if ($connect.Wait(100) -and $client.Connected) { return }
        }
        catch { }
        finally { $client.Dispose() }
        Start-Sleep -Milliseconds 50
    }
    throw "app-server listener timed out before ws://127.0.0.1:$ListenerPort became available"
}

function Read-AdapterState {
    param([string]$Path, [string]$ExpectedWorkingDirectory)
    try { $state = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "stored adapter state is invalid: $Path`: $($_.Exception.Message)" }
    $parsedAgentId = [guid]::Empty
    if ($state.version -notin @(1, 2, 3) -or -not ($state.agent_id -is [string]) -or
        -not [guid]::TryParse($state.agent_id, [ref]$parsedAgentId)) {
        throw "stored adapter state has an invalid version or agent_id: $Path"
    }
    $state.agent_id = $parsedAgentId.ToString()
    if (-not ($state.working_directory -is [string]) -or
        -not [IO.Path]::GetFullPath($state.working_directory).Equals($ExpectedWorkingDirectory, [StringComparison]::OrdinalIgnoreCase)) {
        throw "stored adapter state working_directory differs from this launch: $Path"
    }
    if ($null -ne $state.thread_id -and (-not ($state.thread_id -is [string]) -or [string]::IsNullOrWhiteSpace($state.thread_id))) {
        throw "stored adapter state has an invalid thread_id: $Path"
    }
    $state
}

# Durable state is explicit and independent of the installed launcher location.
if (-not [IO.Path]::IsPathFullyQualified($StateDirectory)) { throw 'StateDirectory must be absolute.' }
$stateAncestor = [IO.Path]::GetFullPath($StateDirectory)
while ($stateAncestor) {
    $ancestorItem = Get-Item -LiteralPath $stateAncestor -ErrorAction Stop
    if ($ancestorItem.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'StateDirectory must not traverse a reparse point.'
    }
    $parent = Split-Path -Parent $stateAncestor
    if (-not $parent -or $parent -eq $stateAncestor) { break }
    $stateAncestor = $parent
}
$stateDirectory = Resolve-RelayPath $StateDirectory 'private state directory' -Directory
$stateItem = Get-Item -LiteralPath $stateDirectory
if ($stateItem.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'StateDirectory must not be a reparse point.' }
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$acl = Get-Acl -LiteralPath $stateDirectory
if ($acl.Owner -notin @($identity.Name, $identity.User.Value)) { throw 'StateDirectory must be owned by the invoking account.' }
$publicSids = @('S-1-1-0', 'S-1-5-11', 'S-1-5-32-545')
foreach ($rule in $acl.Access) {
    $sid = $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
    if ($rule.AccessControlType -eq 'Allow' -and $sid -in $publicSids) { throw 'StateDirectory must not grant public access.' }
}
$probe = Join-Path $stateDirectory ('.write-probe-' + [guid]::NewGuid().ToString('N'))
try { [IO.File]::WriteAllText($probe, '') } finally { if (Test-Path -LiteralPath $probe) { Remove-Item -LiteralPath $probe -Force } }
$resolvedBrokerDirectory = Resolve-RelayPath $BrokerDirectory 'broker directory' -Directory
$resolvedWorkingDirectory = Resolve-RelayPath $WorkingDirectory 'working directory' -Directory
$adapterEntrypoint = Resolve-RelayPath (Join-Path $resolvedBrokerDirectory 'codex_adapter.py') 'adapter entrypoint'
$mcpEntrypoint = Resolve-RelayPath (Join-Path $resolvedBrokerDirectory 'codex_relay_mcp.py') 'MCP entrypoint'
$codex = Resolve-RelayCommand $CodexCommand $CodexPrefix
$python = Resolve-RelayCommand $PythonCommand $PythonPrefix
if ($Port -eq 0) { $Port = Get-FreeLoopbackPort }
if ($AppServerPort -eq 0) {
    do { $AppServerPort = Get-FreeLoopbackPort } while ($AppServerPort -eq $Port)
}
if ($AppServerPort -eq $Port) { throw 'proxy and app-server ports must differ' }
$proxyEndpoint = "ws://127.0.0.1:$Port"
$appServerEndpoint = "ws://127.0.0.1:$AppServerPort"

# Reuse the validated explicit directory; never derive state beside the executable.
$statePath = Join-Path $stateDirectory "$SessionKey.json"
$readinessPath = Join-Path $stateDirectory "$SessionKey.ready.json"
$storedThread = $null
$sessionAgentId = [guid]::NewGuid().ToString()
if (Test-Path -LiteralPath $statePath -PathType Leaf) {
    $storedState = Read-AdapterState $statePath $resolvedWorkingDirectory
    $sessionAgentId = ([guid]$storedState.agent_id).ToString()
    if (-not $NewThread) { $storedThread = $storedState.thread_id }
}
Remove-Item -LiteralPath $readinessPath -Force -ErrorAction SilentlyContinue

$timeoutText = $TimeoutSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)
$mcpArguments = @($python.ForegroundPrefix) + @(
    $mcpEntrypoint,
    '--agent-id', $sessionAgentId,
    '--broker-url', $BrokerUrl
)
$mcpCommandConfig = 'mcp_servers.relay.command=' + (ConvertTo-TomlString $python.ForegroundFile)
$mcpArgumentsConfig = 'mcp_servers.relay.args=[' + (($mcpArguments | ForEach-Object { ConvertTo-TomlString ([string]$_) }) -join ',') + ']'
$mcpEnabledConfig = 'mcp_servers.relay.enabled=true'
$mcpRequiredConfig = 'mcp_servers.relay.required=true'
$mcpEnabledToolsConfig = 'mcp_servers.relay.enabled_tools=["register","name","check","send","ack"]'
$mcpApprovalModeConfig = 'mcp_servers.relay.default_tools_approval_mode="prompt"'
$mcpCheckApprovalConfig = 'mcp_servers.relay.tools.check.approval_mode="approve"'
$mcpRegisterApprovalConfig = 'mcp_servers.relay.tools.register.approval_mode="prompt"'
$mcpNameApprovalConfig = 'mcp_servers.relay.tools.name.approval_mode="prompt"'
$mcpSendApprovalConfig = 'mcp_servers.relay.tools.send.approval_mode="prompt"'
$mcpAckApprovalConfig = 'mcp_servers.relay.tools.ack.approval_mode="approve"'
$appServerArguments = @($codex.BackgroundPrefix) + @(
    'app-server',
    '--listen', $appServerEndpoint,
    '-c', $mcpCommandConfig,
    '-c', $mcpArgumentsConfig,
    '-c', $mcpEnabledConfig,
    '-c', $mcpRequiredConfig,
    '-c', $mcpEnabledToolsConfig,
    '-c', $mcpApprovalModeConfig,
    '-c', $mcpCheckApprovalConfig,
    '-c', $mcpRegisterApprovalConfig,
    '-c', $mcpNameApprovalConfig,
    '-c', $mcpSendApprovalConfig,
    '-c', $mcpAckApprovalConfig
)
$adapterArguments = @($python.BackgroundPrefix) + @(
    $adapterEntrypoint,
    '--listen-url', $proxyEndpoint,
    '--app-server-url', $appServerEndpoint,
    '--broker-url', $BrokerUrl,
    '--state', $statePath,
    '--agent-id', $sessionAgentId,
    '--readiness-file', $readinessPath,
    '--working-directory', $resolvedWorkingDirectory
)
if ($RelayName) { $adapterArguments += @('--relay-name', $RelayName) }
if ($NewThread) { $adapterArguments += '--new-thread' }

$appServerProcess = $null
$adapterProcess = $null
$tuiProcess = $null
$processJob = [RelayProcessJob]::new()
$primaryFailure = $null
$cleanupFailures = [Collections.Generic.List[string]]::new()
try {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $appServerProcess = Start-RelayProcess $processJob $codex.BackgroundFile $appServerArguments $repoRoot Hidden
    Wait-LoopbackListener $appServerProcess $AppServerPort $deadline
    $adapterProcess = Start-RelayProcess $processJob $python.BackgroundFile $adapterArguments $repoRoot Hidden

    $readiness = $null
    while ($null -eq $readiness) {
        $appServerProcess.Refresh()
        if ($appServerProcess.HasExited) {
            throw "app-server exited with code $($appServerProcess.ExitCode) before adapter readiness"
        }
        $adapterProcess.Refresh()
        if ($adapterProcess.HasExited) {
            throw "adapter exited with code $($adapterProcess.ExitCode) before readiness"
        }
        if (Test-Path -LiteralPath $readinessPath -PathType Leaf) {
            try { $candidate = Get-Content -LiteralPath $readinessPath -Raw | ConvertFrom-Json -ErrorAction Stop }
            catch { $candidate = $null }
            $candidateAgentId = if ($null -ne $candidate) { $candidate.agent_id } else { $null }
            $parsedAgentId = [guid]::Empty
            if ($null -ne $candidate -and $candidate.version -eq 1 -and $candidate.status -eq 'ready' -and
                $candidateAgentId -is [string] -and [guid]::TryParse($candidateAgentId, [ref]$parsedAgentId)) {
                $readiness = $candidate
            }
        }
        if ($null -eq $readiness -and [DateTime]::UtcNow -ge $deadline) {
            throw "adapter readiness timed out after $timeoutText seconds: $readinessPath"
        }
        if ($null -eq $readiness) { Start-Sleep -Milliseconds 100 }
    }
    $adapterProcess.Refresh()
    if ($adapterProcess.HasExited) {
        throw "adapter exited with code $($adapterProcess.ExitCode) after publishing readiness"
    }
    if (-not ([guid]$readiness.agent_id).ToString().Equals($sessionAgentId, [StringComparison]::OrdinalIgnoreCase)) {
        throw "adapter readiness agent identity differs from this launch"
    }

    $remoteOptions = @(
        '--remote', $proxyEndpoint,
        '-C', $resolvedWorkingDirectory
    )
    $tuiPrefix = @($codex.ForegroundPrefix) + @($CodexArguments)
    if ($storedThread) {
        $tuiPrefix = @(Remove-RemoteResumePermissionOverrides $tuiPrefix)
        $tuiArguments = $tuiPrefix + @('resume') + $remoteOptions + @($storedThread)
    }
    else {
        $tuiArguments = $tuiPrefix + $remoteOptions
    }
    $tuiProcess = Start-RelayProcess $processJob $codex.ForegroundFile $tuiArguments $resolvedWorkingDirectory CurrentConsole
    $runtimeFailures = [Collections.Generic.List[string]]::new()
    while ($true) {
        foreach ($processRecord in @(
                [pscustomobject]@{ Process = $adapterProcess; Description = 'adapter' },
                [pscustomobject]@{ Process = $appServerProcess; Description = 'app-server' }
            )) {
            $processRecord.Process.Refresh()
            if ($processRecord.Process.HasExited) {
                $runtimeFailures.Add("$($processRecord.Description) exited with code $($processRecord.Process.ExitCode) while the TUI was active")
            }
        }
        if ($runtimeFailures.Count) { break }
        $tuiProcess.Refresh()
        if ($tuiProcess.HasExited) { break }
        Start-Sleep -Milliseconds 100
    }
    if ($runtimeFailures.Count) { throw ($runtimeFailures -join '; ') }
    if ($tuiProcess.ExitCode -ne 0) { throw "Codex TUI exited with code $($tuiProcess.ExitCode)" }
}
catch {
    $primaryFailure = $_
}
finally {
    foreach ($processRecord in @(
            [pscustomobject]@{ Process = $tuiProcess; Description = 'Codex TUI' },
            [pscustomobject]@{ Process = $adapterProcess; Description = 'adapter' },
            [pscustomobject]@{ Process = $appServerProcess; Description = 'app-server' }
        )) {
        try { Stop-RelayProcess $processRecord.Process $processRecord.Description }
        catch { $cleanupFailures.Add($_.Exception.Message) }
    }
    try { $processJob.Dispose() }
    catch { $cleanupFailures.Add($_.Exception.Message) }
    Remove-Item -LiteralPath $readinessPath -Force -ErrorAction SilentlyContinue
}

if ($null -ne $primaryFailure) {
    if ($cleanupFailures.Count) {
        throw "$($primaryFailure.Exception.Message); cleanup failures: $($cleanupFailures -join '; ')"
    }
    throw $primaryFailure
}
if ($cleanupFailures.Count) { throw "relay cleanup failures: $($cleanupFailures -join '; ')" }
