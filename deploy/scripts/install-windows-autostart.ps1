# Explicit opt-in Task Scheduler registration. The normal installer never calls this.
# This does not start/stop the broker and never replaces a foreign/variant registrar.
[CmdletBinding()]
param([switch]$Uninstall,[ValidateSet('AtLogon','AtStartup')][string]$Trigger='AtLogon',[string]$TaskPath='\AgentMessaging\')
$ErrorActionPreference='Stop'
$TaskName='AgentOrchestrationBroker'
$BrokerRoot=Split-Path -Parent $PSScriptRoot
$StartScript=Join-Path $PSScriptRoot 'start-broker.ps1'
if (-not (Test-Path -LiteralPath $StartScript -PathType Leaf)) { throw 'Installed start-broker.ps1 is required.' }
$existing=Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -ErrorAction SilentlyContinue
$psExe=Join-Path $env:SystemRoot 'System32/WindowsPowerShell/v1.0/powershell.exe'
$arguments="-NoProfile -WindowStyle Hidden -File `"$StartScript`" -Autostart"
if ($existing) {
    $actions=@($existing.Actions)
    if ($actions.Count -ne 1 -or $actions[0].Execute -ine $psExe -or $actions[0].Arguments -cne $arguments) { throw 'Existing registrar is foreign or a preserved variant. Reconcile it explicitly; no replacement performed.' }
    if ($Uninstall) { Unregister-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Confirm:$false; Write-Host 'Owned registrar removed; a running broker was not stopped.'; exit 0 }
    Write-Host 'Correct owned registrar already exists; unchanged.'
    exit 0
}
if ($Uninstall) { Write-Host 'No owned registrar found; no change.'; exit 0 }
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$action=New-ScheduledTaskAction -Execute $psExe -Argument $arguments -WorkingDirectory $BrokerRoot
if ($Trigger -eq 'AtStartup') {
    $taskTrigger=New-ScheduledTaskTrigger -AtStartup
    $principal=New-ScheduledTaskPrincipal -UserId $sid -LogonType S4U -RunLevel Limited
    Write-Host 'AtStartup requires the existing Log on as a batch job right; this command does not grant it.'
} else {
    $taskTrigger=New-ScheduledTaskTrigger -AtLogOn -User $sid
    $principal=New-ScheduledTaskPrincipal -UserId $sid -LogonType Interactive -RunLevel Limited
}
$settings=New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 0)
Register-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Action $action -Trigger $taskTrigger -Principal $principal -Settings $settings -Description 'User-owned agent-messaging broker; explicit opt-in autostart.' | Out-Null
Write-Host "Registered $TaskPath$TaskName. No process started. Activate explicitly with scripts/restart-broker.ps1 using the installed port."
