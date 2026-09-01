param(
    [switch]$CommitLinks,
    [switch]$CommitEmails,
    [int]$Limit = 30,
    [string]$IdsFile = 'C:/tmp/kol-public-source-trial-ids-20260831.json',
    [string]$EvidenceFile = 'C:/tmp/kol-public-source-trial-evidence-20260831.json',
    [string]$ExcludeIdsFile = '',
    [switch]$EmptyEmailPublicSource
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

$allowed = @(
    'FEISHU_BITABLE_APP_ID', 'FEISHU_BITABLE_APP_SECRET',
    'FEISHU_APP_TOKEN', 'T_KOL'
)
foreach ($variable in @($response.data.service.variables)) {
    if ($allowed -contains [string]$variable.key) {
        [Environment]::SetEnvironmentVariable(
            [string]$variable.key, [string]$variable.value, 'Process'
        )
    }
}
$missing = @($allowed | Where-Object { -not [Environment]::GetEnvironmentVariable($_, 'Process') })
if ($missing.Count -gt 0) { throw "Missing configuration names: $($missing -join ', ')" }
[Environment]::SetEnvironmentVariable('PYTHONIOENCODING', 'utf-8', 'Process')

$arguments = @(
    'scripts/run_kol_public_source_trial.py',
    '--limit', [string]$Limit,
    '--ids-file', $IdsFile,
    '--evidence-file', $EvidenceFile
)
if ($CommitLinks) { $arguments += '--commit-links' }
if ($CommitEmails) { $arguments += '--commit-emails' }
if ($ExcludeIdsFile) { $arguments += @('--exclude-ids-file', $ExcludeIdsFile) }
if ($EmptyEmailPublicSource) { $arguments += '--empty-email-public-source' }

& 'C:/tmp/py311-embed/python.exe' @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
