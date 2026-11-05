# start-broker.ps1
# Manual broker launch - sources broker.env automatically; no manual env-var setting needed.
#
# NOTE (pipe-hang): with -Autostart this launches python via Start-Process with output redirection, which
#   sets bInheritHandles=TRUE -- so the DETACHED python inherits the caller's OTHER open handles, including
#   the stdout PIPE handle a draining caller created (python's own stdout goes to a log file). Do NOT drain
#   this script's output through a PowerShell pipe (`| ...` OR `*> file`) -- python holds the inherited pipe
#   handle open, the pipe never EOFs -> the caller HANGS (~20s+, measured). Call it directly, or from a
#   script use Start-Process -WindowStyle Hidden -PassThru + $p.WaitForExit().
#
# Usage:
#   cd "$env:USERPROFILE\.agents\services\broker"
#   .\scripts\start-broker.ps1                    # interactive (operator at PS terminal)
#   .\scripts\start-broker.ps1 -Autostart         # detached background launch (Task Scheduler)
# Or invoke from anywhere -- substitute YOUR install root; the autostart task's own Action is the
# authoritative record of where this box installed the broker:
#   powershell -ExecutionPolicy Bypass -File <BrokerRoot>\scripts\start-broker.ps1

param(
    [ValidateRange(1,65535)][int]$Port,
    # When set, detach python via Start-Process and exit immediately so
    # PowerShell doesn't stay alive blocked on python (which would leave a
    # visible/lingering terminal under Task Scheduler launches). Output goes
    # to broker-autostart.log (overwritten on each launch, not appended).
    [switch]$Autostart,

    # Foreground/interactive: run python synchronously, tee'd to the console
    # (Ctrl+C to stop). This was the OLD default; a bare launch now DETACHES.
    [switch]$Foreground
)

$ErrorActionPreference = 'Stop'

# Resolve broker root (parent of scripts/) regardless of cwd
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BrokerRoot = Split-Path -Parent $ScriptDir

# Validate the complete file before importing any values or touching process/log state.
. (Join-Path $ScriptDir 'broker-env.ps1')
$EnvFile = Join-Path $BrokerRoot 'broker.env'
$envMap = Read-BrokerEnvFile -Path $EnvFile
$selectedPort = if ($envMap.ContainsKey('BROKER_PORT')) { [int]$envMap['BROKER_PORT'] } else { 8787 }
if ($PSBoundParameters.ContainsKey('Port') -and $selectedPort -ne $Port) { throw 'Selected Port conflicts with broker.env; root/state relocation is not an upgrade.' }
foreach ($key in $envMap.Keys) {
    [System.Environment]::SetEnvironmentVariable($key, $envMap[$key], 'Process')
}
$env:BROKER_PORT = [string]$selectedPort
Set-Location $BrokerRoot
$env:BROKER_HOST = '127.0.0.1'
$env:PYTHONDONTWRITEBYTECODE = '1'

# Resolve venv python
$VenvPython = Join-Path $BrokerRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $VenvPython)) {
    Write-Error "venv python not found at $VenvPython. Run: python -m venv .venv && .venv\Scripts\pip install -r requirements.txt"
    exit 1
}

# Persistent log file for manual launches. The autostart wrapper
# (install-windows-autostart.ps1) writes to broker-autostart.log via parent
# powershell.exe redirection (*>>), so we use a separate file here to avoid
# double-writes when launched via autostart.
$LogDir = Join-Path $BrokerRoot 'logs'
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}
$LogFile = Join-Path $LogDir 'broker-manual.log'

# Banner is operator-facing; skip in autostart mode where it would race
# with the script's own broker-autostart.log management (the script truncates
# the log just before spawning python, so any earlier writes via TS-level
# *>> redirect would either be lost or interleaved unpredictably).
if ($Foreground) {
    Write-Host ""
    Write-Host "Launching broker:"
    Write-Host "  cwd:    $BrokerRoot"
    Write-Host "  python: $VenvPython"
    Write-Host "  host:   $env:BROKER_HOST"
    Write-Host "  port:   $env:BROKER_PORT"
    Write-Host "  db:     $env:BROKER_DB_PATH"
    Write-Host "  log:    $LogFile  (tee'd to console)"
    Write-Host ""
    Write-Host "Ctrl+C to stop."
    Write-Host ""
}

# When parent already redirects (autostart wrapper case), don't tee — would
# duplicate every line in the parent's log file. Otherwise tee to manual log
# so operator sees output AND gets a persistent file.
#
# Downgrade ErrorActionPreference around the long-running python invocation.
# uvicorn writes its INFO startup logs to stderr; with $ErrorActionPreference='Stop',
# PowerShell wraps each stderr line from the native command as a NativeCommandError
# and aborts the pipeline (causing server.py to die before bind to port).
# Localized 'Continue' here prevents that without weakening the early-validation
# fail-fast above (broker.env / venv-python checks at lines 18-21 + 38-41).
$ErrorActionPreference = 'Continue'

# Detach by DEFAULT. Only -Foreground runs python in the foreground (tee'd,
# blocking, Ctrl+C to stop). -Autostart or a redirected stdout (legacy Task
# Scheduler *>>) also detach and use the broker-autostart.* logs; a bare
# manual launch detaches and uses broker-manual.* logs + prints the PID and a
# /api/health summary (mirrors copilot-proxy start_proxy.ps1).
$isAutostartMode = $Autostart -or [Console]::IsOutputRedirected
if (-not $Foreground) {
    # AUTOSTART MODE — triggered by either:
    #   (a) -Autostart switch passed by Task Scheduler command line (preferred)
    #   (b) [Console]::IsOutputRedirected = true (legacy: TS command line ends
    #       with `*>> "...log"` which redirects powershell's stdout)
    # Goals:
    #   1. Detach python so this PowerShell process exits immediately —
    #      otherwise the powershell.exe spawned by Task Scheduler stays alive
    #      blocking on python for the broker's entire lifetime, leaving a
    #      stale process visible during/after logon.
    #   2. Capture all uvicorn output (stdout + stderr) to log files in $LogDir.
    #   3. Overwrite the logs on each startup so they don't accumulate forever.
    #   4. No visible console window at all — direct Start-Process, NO cmd shim.
    #
    # Spawn the installed interpreter directly, with separate stdout/stderr logs.
    #
    # Two-file split (.log = stdout, .err.log = stderr) is required because
    # Start-Process cannot redirect both streams to the same file (.NET file-lock
    # collision). Operationally minor: cat both for the full picture.
    #
    # NOTE: when this branch is taken, REMOVE any `*>> "...log"` from the
    # Task Scheduler command line — the script handles its own logging now,
    # and a TS-level append redirect would re-introduce accumulation.

    $logBase = if ($isAutostartMode) { 'broker-autostart' } else { 'broker-manual' }
    $AutostartLog = Join-Path $LogDir "$logBase.log"
    $AutostartErr = Join-Path $LogDir "$logBase.err.log"
    $AutostartBanner = Join-Path $LogDir "$logBase.banner.log"

    # Truncate both python-output logs on each startup (overwrite, not append).
    # NOTE: Start-Process -RedirectStandardOutput/-RedirectStandardError will
    # ALSO truncate these in CREATE mode the moment python spawns, but this
    # explicit clear ensures any prior content is gone even if python fails
    # to spawn for some reason.
    Set-Content -Path $AutostartLog -Value '' -Encoding utf8 -Force
    Set-Content -Path $AutostartErr -Value '' -Encoding utf8 -Force

    # Banner goes to a SIDECAR file (not broker-autostart.log) because
    # Start-Process redirect would truncate the .log file at python-spawn time
    # and erase the banner. The sidecar persists across the broker lifetime
    # so operators can `Get-Content broker-autostart.banner.log` to see when
    # / where / what was launched without parsing uvicorn output.
    $launchTs = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Set-Content -Path $AutostartBanner -Encoding utf8 -Force -Value @"
[$launchTs] start-broker.ps1 (autostart): launched detached
  cwd:      $BrokerRoot
  python:   $VenvPython
  host:     $env:BROKER_HOST
  port:     $env:BROKER_PORT
  db:       $env:BROKER_DB_PATH
  stdout:   $AutostartLog (overwrite-on-startup)
  stderr:   $AutostartErr (overwrite-on-startup)
"@

    # Spawn python directly — no cmd intermediary, no Tee-Object pipe.
    # Start-Process with both -RedirectStandardOutput AND -RedirectStandardError
    # implicitly sets CreateNoWindow on the .NET process API, so python runs
    # with NO console window allocated. -WindowStyle Hidden is belt-and-suspenders.
    # Start-Process is non-blocking (no -Wait), so this PowerShell script exits
    # immediately after spawning python. python.exe persists alone.
    $proc = Start-Process -FilePath $VenvPython `
                  -ArgumentList @('-B', ('"' + (Join-Path $BrokerRoot 'server.py') + '"')) `
                  -WorkingDirectory $BrokerRoot `
                  -RedirectStandardOutput $AutostartLog `
                  -RedirectStandardError $AutostartErr `
                  -WindowStyle Hidden `
                  -PassThru
    # Script exits here. ONLY python.exe persists (no cmd, no powershell).

    # Manual detach (bare launch): print PID + a short /api/health probe + a
    # summary so the operator sees the broker came up; then the prompt is free
    # and the broker survives closing the window. -Autostart / redirected
    # (Task Scheduler) stays silent (restart-broker.ps1 runs its own probe).
    if (-not $isAutostartMode) {
        Write-Host "broker detached: PID $($proc.Id)"
        $probeUrl = "http://127.0.0.1:$($env:BROKER_PORT)/api/health"
        $deadline = (Get-Date).AddSeconds(10)
        $healthy = $false
        while ((Get-Date) -lt $deadline) {
            try {
                $r = Invoke-WebRequest -Uri $probeUrl -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
                if ($r.StatusCode -eq 200) { $healthy = $true; break }
            } catch { }
            Start-Sleep -Milliseconds 500
        }
        # Confirm the port listener is owned by the process WE spawned before
        # reporting success. A health 200 alone can come from a DIFFERENT broker
        # already holding the port (common when re-launching while one is up),
        # and our python may still be importing (not yet exited) at loop-end --
        # so neither $healthy nor $proc.HasExited alone proves our spawn won the
        # port. Get-NetTCPConnection is available on PS 5.1+ (Windows).
        $ownerPids = @()
        try {
            $ownerPids = @(Get-NetTCPConnection -LocalPort ([int]$env:BROKER_PORT) -State Listen -ErrorAction Stop |
                           Select-Object -ExpandProperty OwningProcess -Unique)
        } catch { $ownerPids = @() }
        $weOwnPort = ($ownerPids -contains $proc.Id)
        $otherOwnsPort = (($ownerPids.Count -gt 0) -and -not $weOwnPort)

        Write-Host ""
        if ($proc.HasExited) {
            # The python we spawned is already GONE -- it did not stay up. Most
            # common cause: a broker already held the port (bind fails -> exit).
            Write-Host "=== broker NOT started by this launch ===" -ForegroundColor Red
            Write-Host "  spawned PID $($proc.Id) exited immediately (exit code $($proc.ExitCode))."
            if ($healthy -or $otherOwnsPort) {
                Write-Host "  note:    a broker is already running/answering on port $env:BROKER_PORT."
                Write-Host "  action:  restart with .\scripts\restart-broker.ps1, or .\scripts\stop-broker.ps1 first."
            } else {
                Write-Host "  action:  check $AutostartErr for the startup error (port in use? bad broker.env?)."
            }
        } elseif ($otherOwnsPort) {
            # Our python is still alive but a DIFFERENT process owns the port --
            # ours did not acquire it (almost always: a broker was already up).
            # Do NOT print green off the other broker's health 200.
            Write-Host "=== broker NOT started by this launch ===" -ForegroundColor Red
            Write-Host "  port $env:BROKER_PORT is owned by a DIFFERENT process (PID $($ownerPids -join ',')), not our spawn (PID $($proc.Id))."
            Write-Host "  a broker is likely already running; our python will exit when its bind fails."
            Write-Host "  action:  restart with .\scripts\restart-broker.ps1, or .\scripts\stop-broker.ps1 first."
        } elseif ($healthy -and $weOwnPort) {
            Write-Host "=== broker started ===" -ForegroundColor Green
            Write-Host "  PID:     $($proc.Id)"
            Write-Host "  health:  OK ($probeUrl)"
        } else {
            Write-Host "=== broker launched (health not yet confirmed) ===" -ForegroundColor Yellow
            Write-Host "  PID:     $($proc.Id)"
            Write-Host "  health:  $probeUrl not confirmed for PID $($proc.Id) within 10s (still starting? check logs)"
        }
        Write-Host "  logs:    $AutostartLog (+ .err.log)"
        Write-Host "  stop:    .\scripts\stop-broker.ps1   (restart: .\scripts\restart-broker.ps1)"
        Write-Host ""
    }
} else {
    & $VenvPython -B (Join-Path $BrokerRoot 'server.py') 2>&1 | Tee-Object -FilePath $LogFile -Append
}
