param(
    [string]$TaskName = "KOL-Media-Archive-Worker",
    [System.Management.Automation.PSCredential]$Credential,
    [switch]$CurrentUserS4U
)

$ErrorActionPreference = "Stop"
if ($CurrentUserS4U -and $Credential) {
    throw "CurrentUserS4U 与 Credential 只能选择一种。"
}
if (-not $CurrentUserS4U -and -not $Credential) {
    throw "请传入 Credential，或明确使用 -CurrentUserS4U。"
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$startScript = Join-Path $scriptRoot "start-worker.ps1"
$pwsh = (Get-Command pwsh.exe -ErrorAction Stop).Source
$arguments = "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$startScript`""

$action = New-ScheduledTaskAction -Execute $pwsh -Argument $arguments -WorkingDirectory $scriptRoot
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 5 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

if ($CurrentUserS4U) {
    $userName = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $principal = New-ScheduledTaskPrincipal `
        -UserId $userName `
        -LogonType S4U `
        -RunLevel Highest
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null
    Write-Output "已安装 $TaskName：$userName 无密码后台启动、开机运行、静默无窗口。"
    exit 0
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -User $Credential.UserName `
    -Password $Credential.GetNetworkCredential().Password `
    -RunLevel Highest `
    -Force | Out-Null

Write-Output "已安装 $TaskName：开机启动、无需用户登录、静默运行。"
