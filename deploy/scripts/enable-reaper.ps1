# enable-reaper.ps1
# Idempotent, self-contained per-machine ENABLE for the #18 relay escalation reaper.
#
# The optional reaper escalates outstanding messages to an explicitly configured
# Discord destination. It is dormant by default and is never enabled by install.
# This operator changes broker.env and restarts the local service; authorize it
# separately for this invocation after reviewing its DryRun output.
#
# Safe to run anywhere, any number of times:
#   - already enabled + healthy   -> no-op (exit 0); reports the current backlog, which the
#       already-live reaper is handling
#   - already enabled but broker DOWN -> resume the start (no re-flip)
#   - not enabled                 -> stop-first enable
#
# This script does NOT touch the delivery ledger -- it never stamps/suppresses any row. An
# earlier design "reconciled" the pre-enable backlog (stamped alerted_ts=now on outstanding
# rows) to avoid a first-enable alert burst, but stamping a genuinely-stuck row would HIDE it
# from the reaper (missed escalation = the fail-dangerous direction), and NO database signal
# can prove a row is safe to stamp (the DB cannot know whether an older reaper ran before).
#  (CAVEAT: server.py's run-once user_version 1->2 migration IS a provably-safe exception -- it stamps
#   alerted_ts on the outstanding backlog EXACTLY ONCE at the schema cutover BEFORE the reaper ever ran on
#   this DB, so those rows are provably pre-reaper, and user_version guards it from ever re-stamping on a
#   later enable/restart. THIS script has no such run-once marker -- it can be re-run any time against a DB
#   a prior reaper may have touched -- which is why ITS reconcile was removed, not because stamping is
#   never safe.)
# So the reconcile is removed. Certified operating procedure instead: run -DryRun first,
# confirm "outstanding backlog: 0", THEN enable. On a non-empty backlog the freshly-enabled
# reaper simply (over-)alerts on the stuck rows -- the accepted benign direction.
#
# Stop first to avoid changing runtime policy while the broker is writing state:
#   stop-broker.ps1 (abort on 1/2/3, BEFORE any env change)
#     -> flip broker.env BROKER_REAPER_ENABLED false->true (atomic; only if not already true)
#     -> start-broker.ps1 -Autostart  -> poll /api/health (<=15s)
#     -> positive-verify: re-query /api/health -> checks.reaper_running must be true
#        require the selected loopback listener's actual reaper state.
#
# Portable: everything resolves from this script's location (parent of scripts/ =
# broker root). No hardcoded machine paths -> satellite- and fresh-machine-safe.
#
# Usage:
#   .\scripts\enable-reaper.ps1
#   .\scripts\enable-reaper.ps1 -DryRun     # report planned actions; touch nothing
#   .\scripts\enable-reaper.ps1 -Force      # forward -Force to stop-broker (SIGKILL first try)
#   powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\.agents\services\broker\scripts\enable-reaper.ps1"
#
# Exit codes:
#   0 = reaper enabled + verified live, OR idempotent no-op (already enabled)
#   1 = precondition failure (missing broker.env / venv python / sibling script / bad DB path /
#       broker.env missing the reaper's Discord config so the broker would refuse to start -- D82)
#   2 = selected port owned by a non-broker process (passthrough)
#   3 = selected port remains occupied after stop (passthrough)
#   4 = broker started but /api/health did not become reachable within 15s
#   6 = verify failed: post-restart /api/health checks.reaper_running was not true
#   (5 and 7 retired with the delivery-ledger reconcile they guarded: this script no longer
#    writes to the DB, and the backlog read is informational + non-fatal.)

param(
    [switch]$DryRun,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------- path resolution
$ScriptDir     = Split-Path -Parent $MyInvocation.MyCommand.Path
$BrokerRoot    = Split-Path -Parent $ScriptDir
$EnvFile       = Join-Path $BrokerRoot 'broker.env'
$VenvPython    = Join-Path $BrokerRoot '.venv\Scripts\python.exe'
$StopScript    = Join-Path $ScriptDir 'stop-broker.ps1'
$StartScript   = Join-Path $ScriptDir 'start-broker.ps1'
$LogDir        = Join-Path $BrokerRoot 'logs'

function Fail-Precondition([string]$msg) {
    Write-Host "enable-reaper: PRECONDITION FAILURE -- $msg" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path -LiteralPath $EnvFile))     { Fail-Precondition "broker.env not found at $EnvFile" }
if (-not (Test-Path -LiteralPath $VenvPython))  { Fail-Precondition "venv python not found at $VenvPython" }
if (-not (Test-Path -LiteralPath $StopScript))  { Fail-Precondition "stop-broker.ps1 not found at $StopScript" }
if (-not (Test-Path -LiteralPath $StartScript)) { Fail-Precondition "start-broker.ps1 not found at $StartScript" }

# ---------------------------------------------------------------- broker.env parse
. (Join-Path $ScriptDir 'broker-env.ps1')

function ConvertTo-Bool([string]$v) {
    if ($null -eq $v) { return $false }
    return ($v.Trim().ToLower() -in @('true', '1', 'yes', 'on'))
}

$envMap     = Read-BrokerEnvFile -Path $EnvFile
$curEnabled = ConvertTo-Bool ([string]$envMap['BROKER_REAPER_ENABLED'])
$brokerPort = if ($envMap.ContainsKey('BROKER_PORT')) { [int]$envMap['BROKER_PORT'] } else { 8787 }
$HealthUrl  = "http://127.0.0.1:$brokerPort/api/health"   # always probe loopback (bind host may be 0.0.0.0)

# Resolve the delivery DB (BROKER_DB_PATH, default ./coordination.db, relative to broker root).
$dbRel = if ($envMap.ContainsKey('BROKER_DB_PATH') -and $envMap['BROKER_DB_PATH']) { $envMap['BROKER_DB_PATH'] } else { './coordination.db' }
if ([System.IO.Path]::IsPathRooted($dbRel)) {
    $DbPath = $dbRel
} else {
    $DbPath = Join-Path $BrokerRoot ($dbRel -replace '^\.[\\/]', '')
}
$DbPath = [System.IO.Path]::GetFullPath($DbPath)

# ---------------------------------------------------------------- helpers
function Test-BrokerHealthy {
    try {
        $r = Invoke-WebRequest -Uri $HealthUrl -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        return ($r.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Wait-BrokerHealthy([int]$TimeoutSec = 15) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-BrokerHealthy) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# Embedded READ-ONLY delivery-backlog counter. Runs under the broker's OWN venv python
# (sqlite3 guaranteed present -- no sqlite3-CLI-on-PATH assumption). Reads the script from
# stdin (`-`); argv[1]=db path. Prints COUNT=<n> = outstanding + un-alerted delivery rows
# (exactly what the reaper would consider). It NEVER writes: this script does not
# reconcile/stamp anything, so the count is purely informational (the -DryRun verify-before-
# enable check + the enable-time honesty line). A missing db / delivery table reads as 0. On
# a pre-#18 table (no alerted_ts column) every outstanding row is un-alerted, so count those.
$LedgerPy = @'
import sqlite3, sys, os
db = sys.argv[1]

# Never CREATE the db: sqlite3.connect() would materialize an empty file. A missing db has
# no backlog.
if not os.path.exists(db):
    print("COUNT=0")
    sys.exit(0)
try:
    con = sqlite3.connect(db, timeout=10)
except Exception as e:
    sys.stderr.write("DBOPEN_ERROR: %s\n" % e)
    sys.exit(2)
try:
    # delivery table absent (pre-#19 db) => no reaper ledger yet => zero backlog.
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='delivery'"
    ).fetchone()
    if row is None:
        print("COUNT=0")
        sys.exit(0)
    cols = [r[1] for r in con.execute("PRAGMA table_info(delivery)").fetchall()]
    if 'alerted_ts' in cols:
        n = con.execute(
            "SELECT COUNT(*) FROM delivery "
            "WHERE status='outstanding' AND alerted_ts IS NULL"
        ).fetchone()[0]
    else:
        # pre-#18 delivery table (no alerted_ts column): every outstanding row is un-alerted.
        n = con.execute(
            "SELECT COUNT(*) FROM delivery WHERE status='outstanding'"
        ).fetchone()[0]
    print("COUNT=%d" % n)
except sqlite3.OperationalError as e:
    sys.stderr.write("SQL_ERROR: %s\n" % e)
    sys.exit(2)
finally:
    con.close()
'@

function Get-BacklogCount {
    # Read-only. Returns @{ Ok=<bool>; Count=<int> } -- outstanding + un-alerted delivery
    # rows. Informational only (this script never reconciles); a read failure is non-fatal.
    $out = ($LedgerPy | & $VenvPython - $DbPath)
    $code = $LASTEXITCODE
    $result = @{ Ok = ($code -eq 0); Count = -1 }
    foreach ($l in $out) {
        if ($l -match '^COUNT=(-?\d+)$') { $result.Count = [int]$matches[1] }
    }
    if ($code -ne 0) {
        Write-Host "enable-reaper: backlog read failed (python exit $code) -- informational, continuing." -ForegroundColor Yellow
    }
    return $result
}

# Single /api/health GET, decoded for the D80/Option-A reaper-state field. Only "is the
# reaper alive right now" (Running) matters now -- this script no longer reconciles, so the
# earlier disabled-vs-crashed distinction (which existed only to gate the reconcile) is gone.
# Returns:
#   @{ Reachable=$true;  Running=$true  }  -- reaper alive
#   @{ Reachable=$true;  Running=$false }  -- reaper not running
#   @{ Reachable=$true;  Running=$null  }  -- reaper_running field ABSENT (pre-Option-A binary)
#   @{ Reachable=$false; Running=$null  }  -- broker not answering
function Get-HealthReaper {
    try {
        $r = Invoke-WebRequest -Uri $HealthUrl -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($r.StatusCode -ne 200) { return @{ Reachable = $false; Running = $null } }
        $j = $r.Content | ConvertFrom-Json
        $checks = $j.checks
        $names = @()
        if ($null -ne $checks) { $names = @($checks.PSObject.Properties.Name) }
        if ($names -contains 'reaper_running') {
            return @{ Reachable = $true; Running = [bool]$checks.reaper_running }
        }
        return @{ Reachable = $true; Running = $null }   # reachable, field absent
    } catch {
        return @{ Reachable = $false; Running = $null }
    }
}

# Determine whether the reaper is LIVE in the RUNNING broker from the broker's OWN
# authoritative /api/health report (D80 / Option A) -- only the process that BOUND :8787 can
# answer, so the signal is provenance-correct. The enable path only needs Live-vs-not (it no
# longer reconciles, so it treats every not-Live state identically), and the post-restart
# verify passes ONLY on 'Live' (fail-closed). Returns:
#   'Live'    -- reachable AND reaper_running == true
#   'NotLive' -- reachable AND reaper_running == false (reaper not running -> enable it)
#   'Unknown' -- reachable but the field is ABSENT (pre-Option-A binary), OR unreachable
function Get-ReaperRunState {
    # Retry the health GET a few times so a single transient blip (busy broker, GC pause,
    # a 2s timeout) is not misread as unreachable.
    for ($i = 0; $i -lt 3; $i++) {
        $h = Get-HealthReaper
        if ($h.Reachable) {
            if ($null -eq $h.Running) { return 'Unknown' }   # reachable but pre-Option-A field-absent
            if ($h.Running) { return 'Live' }
            return 'NotLive'
        }
        Start-Sleep -Milliseconds 500
    }
    return 'Unknown'   # unreachable after retries
}

# Reaper enablement requires nonblank DISCORD_BOT_TOKEN and
# BROKER_ALERT_DISCORD_CHANNEL_ID in the private broker.env before any stop/write,
# using the same validated parser as start-broker.ps1. Never import these secrets.
# Returns @{ Ok=<bool>; Missing=@(<names>) }.
function Test-ReaperDiscordConfig {
    $loaded = Read-BrokerEnvFile -Path $EnvFile
    $missing = @()
    foreach ($k in @('DISCORD_BOT_TOKEN', 'BROKER_ALERT_DISCORD_CHANNEL_ID')) {
        if (-not $loaded.ContainsKey($k) -or [string]::IsNullOrWhiteSpace([string]$loaded[$k])) {
            $missing += $k
        }
    }
    return @{ Ok = ($missing.Count -eq 0); Missing = $missing }
}

# ---------------------------------------------------------------- broker.env flip (atomic, no-BOM)
function Enable-ReaperEnvFlag {
    $lines = Get-Content -LiteralPath $EnvFile
    $found = $false
    $new = foreach ($l in $lines) {
        if ($l -match '^\s*BROKER_REAPER_ENABLED\s*=') { $found = $true; 'BROKER_REAPER_ENABLED=true' }
        else { $l }
    }
    if (-not $found) { $new = @($new) + 'BROKER_REAPER_ENABLED=true' }
    $tmp = "$EnvFile.tmp"
    # Preserve this operator's existing no-BOM write format.
    Set-Content -LiteralPath $tmp -Value $new -Encoding ascii
    Move-Item -LiteralPath $tmp -Destination $EnvFile -Force
}

# ================================================================ main
# Decisions key off the RUNNING process's actual reaper state, NOT broker.env's declared
# value -- this closes the env-vs-process drift class (a stale env=false while a live
# broker actually has the reaper on, or vice-versa). $initiallyEnabled (the env-file value)
# is used only to know whether a flip is needed, and as the sole proxy when the broker is
# Down (no live process to consult).
$initiallyEnabled = $curEnabled
$runState = Get-ReaperRunState          # Live | NotLive | Unknown
Write-Host "enable-reaper: broker root = $BrokerRoot"
Write-Host "enable-reaper: delivery db  = $DbPath"
Write-Host "enable-reaper: broker.env BROKER_REAPER_ENABLED = $initiallyEnabled"
Write-Host "enable-reaper: running reaper state = $runState (health $HealthUrl)"

# ---- DryRun: report intended actions, touch nothing ----
if ($DryRun) {
    $probe = Get-BacklogCount
    Write-Host ""
    Write-Host "enable-reaper: --DryRun (no changes made)" -ForegroundColor Cyan
    Write-Host "  broker.env enabled : $initiallyEnabled"
    Write-Host "  running reaper     : $runState"
    if ($probe.Ok) {
        Write-Host "  outstanding un-alerted backlog : $($probe.Count)"
        if ($probe.Count -ne 0) {
            Write-Host "  NOTE: certified procedure is to enable on a 0-backlog machine. With $($probe.Count) outstanding row(s), the freshly-enabled reaper will alert on any still-stuck past the grace window (benign over-alert). Verify these are expected before enabling." -ForegroundColor Yellow
        }
    } else {
        Write-Host "  outstanding un-alerted backlog : (read failed; informational only)" -ForegroundColor Yellow
    }
    $cfg = Test-ReaperDiscordConfig
    if ($cfg.Ok) {
        Write-Host "  reaper Discord config : present (DISCORD_BOT_TOKEN + BROKER_ALERT_DISCORD_CHANNEL_ID in broker.env)"
    } else {
        Write-Host "  reaper Discord config : MISSING $($cfg.Missing -join ', ') -- enabling would be REFUSED; the broker refuses to start without it (D82)." -ForegroundColor Red
    }
    switch ($runState) {
        'Live' {
            $flipNote = if (-not $initiallyEnabled) { " (and persist broker.env -> true; no restart)" } else { "" }
            Write-Host "  WOULD: nothing -- reaper already live$flipNote (the live reaper is already handling the backlog)."
        }
        default {
            # NotLive or Unknown -> enable it.
            Write-Host "  WOULD: stop -> ensure broker.env true -> start -> verify."
        }
    }
    exit 0
}

# ---- runState=Live: the reaper is already running in the current process ----
# Nothing to enable (the live reaper is already handling the backlog). Decide off the actual
# running state, regardless of broker.env. No reconcile -- this script never stamps rows.
if ($runState -eq 'Live') {
    if (-not $initiallyEnabled) {
        # Drift: process has the reaper live but broker.env=false -> a future restart would
        # silently LOSE the reaper. Persist the intent (no restart needed; the flag is only
        # read at startup). Non-destructive.
        Write-Host "enable-reaper: reaper live but broker.env=false (drift) -> persisting broker.env -> true (no restart)." -ForegroundColor Yellow
        Enable-ReaperEnvFlag
    }
    $probe = Get-BacklogCount
    if ($probe.Ok -and $probe.Count -gt 0) {
        Write-Host "enable-reaper: reaper already live; $($probe.Count) outstanding un-alerted row(s) are being handled by it (it alerts on any still-stuck past the grace window)." -ForegroundColor Yellow
    }
    Write-Host "enable-reaper: reaper verified live -> no-op." -ForegroundColor Green
    exit 0
}

# ================================================================ enable / restart path
# Reached for runState in {NotLive, Unknown}: the reaper is not (provably) live, so enable it.
# This script does NOT reconcile the delivery ledger (see the header note) -- the freshly-
# enabled reaper re-evaluates the backlog and (over-)alerts anything genuinely stuck, which is
# the benign direction. Certified procedure: verify a 0 backlog via -DryRun before enabling.

# STEP 0 -- refuse if the reaper's Discord config is absent (D82). The refuse-to-start binary
# won't boot with the reaper enabled but DISCORD_BOT_TOKEN / BROKER_ALERT_DISCORD_CHANNEL_ID
# missing/blank, so flipping + restarting would take a currently-healthy broker DOWN. Check
# BEFORE stopping anything; leave the running broker UNTOUCHED on a miss.
$cfg = Test-ReaperDiscordConfig
if (-not $cfg.Ok) {
    Write-Host "enable-reaper: REFUSING to enable -- broker.env is missing required reaper Discord config: $($cfg.Missing -join ', ')." -ForegroundColor Red
    Write-Host "  The broker REFUSES TO START with the reaper enabled but these unset (D82), so enabling now would take it DOWN." -ForegroundColor Red
    Write-Host '  Fix the private broker.env notification prerequisites explicitly, then re-run.' -ForegroundColor Red
    Write-Host "  The running broker was left UNTOUCHED (not stopped)." -ForegroundColor Yellow
    exit 1
}

# STEP 1 -- stop first (so the env flip + restart don't race a live broker's WAL writer).
$stopArgs = @('-Port', [string]$brokerPort)
if ($Force) { $stopArgs += '-Force' }
Write-Host "enable-reaper: stopping broker (stop-first)..."
& $StopScript @stopArgs
$stopExit = $LASTEXITCODE
switch ($stopExit) {
    0 { Write-Host "enable-reaper: broker stopped (or was not running)." }
    2 {
        Write-Host "enable-reaper: aborted -- port $brokerPort has a non-broker owner." -ForegroundColor Red
        exit 2
    }
    3 {
        Write-Host "enable-reaper: aborted -- port $brokerPort remains occupied." -ForegroundColor Red
        exit 3
    }
    default {
        Write-Host "enable-reaper: aborted -- stop-broker failed (exit $stopExit)." -ForegroundColor Red
        exit 1
    }
}

# STEP 2 -- ensure broker.env BROKER_REAPER_ENABLED=true (flip only if currently false).
if (-not $initiallyEnabled) {
    Write-Host "enable-reaper: flipping broker.env BROKER_REAPER_ENABLED -> true ..."
    Enable-ReaperEnvFlag
} else {
    Write-Host "enable-reaper: broker.env already BROKER_REAPER_ENABLED=true (no flip)."
}

# STEP 3 -- start detached + wait for health.
Write-Host "enable-reaper: starting broker (-Autostart)..."
& $StartScript -Autostart -Port ([int]$brokerPort)
Write-Host "enable-reaper: probing $HealthUrl (<=15s)..."
if (-not (Wait-BrokerHealthy 15)) {
    Write-Host "enable-reaper: WARNING -- broker started but /api/health did not respond within 15s." -ForegroundColor Yellow
    # Surface the actual cause from the autostart stderr log. The most likely reason a just-
    # flipped broker fails to come up is the D82 refuse-to-start FATAL: the reaper is enabled
    # but DISCORD_BOT_TOKEN / BROKER_ALERT_DISCORD_CHANNEL_ID is missing/blank in the PROCESS
    # env (e.g. broker.env has it but the launch didn't load it, or a deploy skew). Print the
    # tail + call out any FATAL line by name. (The STEP-0 pre-check catches a broker.env miss
    # before we ever stop; this covers a process-env/startup miss the file-level check can't see.)
    $errLog = Join-Path $LogDir 'broker-autostart.err.log'
    $fatalLines = $null
    if (Test-Path -LiteralPath $errLog) {
        $tail = @(Get-Content -LiteralPath $errLog -Tail 15 -ErrorAction SilentlyContinue)
        $fatalLines = @($tail | Where-Object { $_ -match 'FATAL' })
        Write-Host "  --- broker-autostart.err.log (tail) ---" -ForegroundColor Yellow
        foreach ($l in $tail) { Write-Host "  $l" }
    } else {
        Write-Host "  (no broker-autostart.err.log at $errLog yet)" -ForegroundColor Yellow
    }
    if ($fatalLines -and $fatalLines.Count -gt 0) {
        Write-Host "enable-reaper: LIKELY CAUSE -- the broker refused to start (D82 fail-loud):" -ForegroundColor Red
        foreach ($f in $fatalLines) { Write-Host "  $f" -ForegroundColor Red }
        Write-Host "  Fix broker.env's reaper Discord config (DISCORD_BOT_TOKEN / BROKER_ALERT_DISCORD_CHANNEL_ID) and re-run." -ForegroundColor Red
    } else {
        Write-Host "  Check $LogDir\broker-autostart.log + broker-autostart.err.log" -ForegroundColor Yellow
    }
    exit 4
}
Write-Host "enable-reaper: broker healthy on $HealthUrl." -ForegroundColor Green

# STEP 4 -- positive-verify the reaper actually spawned in THIS launch (FAIL-CLOSED).
# Get-ReaperRunState reads the just-restarted broker's OWN /api/health.reaper_running (D80),
# which only the :8787-binder can answer, so a stale/imposter process can't produce a false
# pass; anything but 'Live' fails the gate. (A pre-Option-A binary would read 'Unknown' here
# -> exit 6, correctly fail-closed; the shipped binary reports the field so verify passes.)
$postState = Get-ReaperRunState
if ($postState -ne 'Live') {
    Write-Host "enable-reaper: VERIFY FAILED -- post-restart reaper state = $postState (expected Live)." -ForegroundColor Red
    if ($postState -eq 'NotLive') {
        Write-Host "  /api/health reports reaper_running=false -> BROKER_REAPER_ENABLED did not take." -ForegroundColor Red
    } else {
        Write-Host "  Could not read reaper_running from /api/health (field absent or broker unreachable). Check $LogDir + broker.env." -ForegroundColor Red
    }
    exit 6
}

# ---- success ----
$post = Get-BacklogCount
Write-Host ""
Write-Host "enable-reaper: DONE -- reaper ENABLED + verified live." -ForegroundColor Green
Write-Host "  BROKER_REAPER_ENABLED = true"
if ($post.Ok) {
    Write-Host "  outstanding backlog   = $($post.Count) row(s) (NOT stamped; the reaper alerts on any still-stuck past the grace window)"
}
Write-Host "  health                = 200 OK on $HealthUrl"
Write-Host "  reaper                = live (/api/health reaper_running=true; interval 60s, grace 3600s -> #relay-alerts)"
exit 0
