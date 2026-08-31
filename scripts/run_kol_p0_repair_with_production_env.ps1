param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('reconcile', 'profile', 'email')]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$Output,
    [string]$Cohorts = 'C:/tmp/kol-p0-repair-cohorts-20260831.json',
    [int]$Limit = 20,
    [switch]$Commit,
    [string]$Confirm = ''
)

$ErrorActionPreference = 'Stop'
if (-not $env:ZEABUR_API_KEY) { throw 'ZEABUR_API_KEY is required.' }
$serviceId = '69eae010c5278d4159c1f664'
$environmentId = '69856f0c86311f632dc2c2c9'
$query = @'
query($serviceID: ObjectID!, $environmentID: ObjectID!) {
  service(_id: $serviceID) {
    variables(environmentID: $environmentID) { key value }
  }
}
'@
$body = @{
    query = $query
    variables = @{ serviceID = $serviceId; environmentID = $environmentId }
} | ConvertTo-Json -Depth 8 -Compress
$response = Invoke-RestMethod `
    -Method Post `
    -Uri 'https://api.zeabur.com/graphql' `
    -Headers @{ Authorization = "Bearer $env:ZEABUR_API_KEY" } `
    -ContentType 'application/json' `
    -Body $body
if ($response.errors) { throw 'Unable to read production service configuration.' }

$base = @(
    'FEISHU_BITABLE_APP_ID', 'FEISHU_BITABLE_APP_SECRET', 'FEISHU_APP_TOKEN',
    'T_KOL'
)
$byMode = @{
    email = @('SNOV_CLIENT_ID', 'SNOV_CLIENT_SECRET')
    profile = @()
    reconcile = @(
        'T_LAUNCH_PARTICIPANT'
    )
}
$allowed = @($base + $byMode[$Mode] | Select-Object -Unique)
foreach ($variable in @($response.data.service.variables)) {
    if ($allowed -contains [string]$variable.key) {
        [Environment]::SetEnvironmentVariable(
            [string]$variable.key, [string]$variable.value, 'Process'
        )
    }
}
$missing = @($allowed | Where-Object {
    -not [Environment]::GetEnvironmentVariable($_, 'Process')
})
if ($missing.Count -gt 0) {
    throw "Missing production configuration names: $($missing -join ', ')"
}
[Environment]::SetEnvironmentVariable('PYTHONIOENCODING', 'utf-8', 'Process')

$script = switch ($Mode) {
    'reconcile' { 'scripts/reconcile_launch_review_routes.py' }
    'profile' { 'scripts/refresh_kol_profile_quality.py' }
    'email' { 'scripts/repair_kol_email_quality.py' }
}
$arguments = @($script, '--output', $Output)
if ($Mode -in @('profile', 'email')) {
    $arguments += @('--cohorts', $Cohorts, '--limit', [string]$Limit)
}
if ($Commit) { $arguments += @('--commit', '--confirm', $Confirm) }
& 'C:/tmp/py311-embed/python.exe' @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
