param(
    [switch]$Commit
)

$ErrorActionPreference = 'Stop'

$serviceId = '69eae010c5278d4159c1f664'
$environmentId = '69856f0c86311f632dc2c2c9'
$graphqlUrl = 'https://api.zeabur.com/graphql'
$repoRoot = Split-Path -Parent $PSScriptRoot

function Require-Env([string]$Name) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Missing local environment variable: $Name"
    }
    return $value
}

function Invoke-Zeabur([string]$Query, [hashtable]$Variables) {
    $headers = @{ Authorization = "Bearer $(Require-Env 'ZEABUR_API_KEY')" }
    $body = @{ query = $Query; variables = $Variables } | ConvertTo-Json -Depth 20 -Compress
    $response = Invoke-RestMethod -Uri $graphqlUrl -Method Post -Headers $headers `
        -ContentType 'application/json' -Body $body
    if ($response.errors) {
        $messages = @($response.errors | ForEach-Object { $_.message }) -join '; '
        throw "Zeabur GraphQL error: $messages"
    }
    return $response.data
}

function Is-UsableValue([string]$Value) {
    if ([string]::IsNullOrWhiteSpace($Value)) { return $false }
    return $Value -notmatch '(?i)xxx|your[_-]|placeholder|replace[_-]|generate[_-]|<[^>]+>'
}

function Read-ExampleValues {
    $values = @{}
    foreach ($line in Get-Content (Join-Path $repoRoot '.env.example')) {
        if ($line -notmatch '^([A-Z][A-Z0-9_]*)=(.*)$') { continue }
        $key = $Matches[1]
        $value = $Matches[2]
        if (Is-UsableValue $value) { $values[$key] = $value }
    }
    return $values
}

function Read-SnovEvidence {
    $path = 'C:\Users\Administrator\.claude\projects\C--Users-Administrator\d557b09d-ce37-4de2-8a30-43935cf01e1b.jsonl'
    if (-not (Test-Path $path)) { throw "Historical SNOV evidence missing: $path" }
    $raw = [IO.File]::ReadAllText($path)
    $result = @{}
    foreach ($key in @('SNOV_CLIENT_ID', 'SNOV_CLIENT_SECRET')) {
        $pattern = '(?<![A-Z0-9_])' + [regex]::Escape($key) + '=([^\s''"&;]+)'
        $candidates = @(
            [regex]::Matches($raw, $pattern) |
                ForEach-Object { $_.Groups[1].Value } |
                Where-Object { Is-UsableValue $_ } |
                Sort-Object -Unique
        )
        if ($key -eq 'SNOV_CLIENT_ID') {
            $candidates = @($candidates | Where-Object { $_.Length -ge 24 })
        }
        if ($candidates.Count -ne 1) {
            throw "Expected one usable historical value for $key; found $($candidates.Count)"
        }
        $result[$key] = $candidates[0]
    }
    return $result
}

function Read-InternalTokenFromN8n {
    $base = (Require-Env 'N8N_BASE_URL').TrimEnd('/')
    if ($base -notmatch '/api/v1$') { $base += '/api/v1' }
    $headers = @{ 'X-N8N-API-KEY' = (Require-Env 'N8N_API_KEY') }
    $workflow = Invoke-RestMethod -Uri "$base/workflows/UIShaANEi8M0rx1v" -Headers $headers -Method Get
    $tokens = New-Object System.Collections.Generic.HashSet[string]
    foreach ($node in $workflow.nodes) {
        foreach ($header in @($node.parameters.headerParameters.parameters)) {
            if ($header.name -eq 'Authorization' -and "$($header.value)" -like 'Bearer *') {
                [void]$tokens.Add("$($header.value)".Substring(7))
            }
        }
    }
    if ($tokens.Count -ne 1) { throw "Expected one INTERNAL_TOKEN in n8n; found $($tokens.Count)" }
    return @($tokens)[0]
}

$desired = Read-ExampleValues

$localMappings = @{
    FEISHU_BITABLE_APP_ID = 'FEISHU_BITABLE_APP_ID'
    FEISHU_BITABLE_APP_SECRET = 'FEISHU_BITABLE_APP_SECRET'
    FEISHU_NOTIFY_APP_ID = 'FEISHU_MCP_APP_ID'
    FEISHU_NOTIFY_APP_SECRET = 'FEISHU_MCP_APP_SECRET'
    FEISHU_APP3_ID = 'FEISHU_EVENT_APP_ID'
    FEISHU_APP3_SECRET = 'FEISHU_EVENT_APP_SECRET'
    FEISHU_B2B_ASSISTANT_APP_ID = 'FEISHU_B2B_ASSISTANT_APP_ID'
    FEISHU_B2B_ASSISTANT_APP_SECRET = 'FEISHU_B2B_ASSISTANT_APP_SECRET'
    FEISHU_CUSTOMER_SERVICE_APP_ID = 'FEISHU_CUSTOMER_SERVICE_APP_ID'
    FEISHU_CUSTOMER_SERVICE_APP_SECRET = 'FEISHU_CUSTOMER_SERVICE_APP_SECRET'
    ZOHO_FUNLAB_CLIENT_ID = 'ZOHO_FUNLAB_CLIENT_ID'
    ZOHO_FUNLAB_CLIENT_SECRET = 'ZOHO_FUNLAB_CLIENT_SECRET'
    ZOHO_FUNLAB_REFRESH_TOKEN = 'ZOHO_FUNLAB_REFRESH_TOKEN'
    ZOHO_FUNLAB_ACCOUNT_ID = 'ZOHO_FUNLAB_ACCOUNT_ID'
    ZOHO_FUNLAB_ALIAS = 'ZOHO_FUNLAB_ALIAS'
    ZOHO_POWKONG_CLIENT_ID = 'ZOHO_POWKONG_CLIENT_ID'
    ZOHO_POWKONG_CLIENT_SECRET = 'ZOHO_POWKONG_CLIENT_SECRET'
    ZOHO_POWKONG_REFRESH_TOKEN = 'ZOHO_POWKONG_REFRESH_TOKEN'
    ZOHO_POWKONG_ACCOUNT_ID = 'ZOHO_POWKONG_ACCOUNT_ID'
    ZOHO_POWKONG_ALIAS = 'ZOHO_POWKONG_ALIAS'
    ZOHO_WHITELABEL_CLIENT_ID = 'ZOHO_WHITELABEL_CLIENT_ID'
    ZOHO_WHITELABEL_CLIENT_SECRET = 'ZOHO_WHITELABEL_CLIENT_SECRET'
    ZOHO_WHITELABEL_REFRESH_TOKEN = 'ZOHO_WHITELABEL_REFRESH_TOKEN'
    ZOHO_WHITELABEL_ACCOUNT_ID = 'ZOHO_WHITELABEL_ACCOUNT_ID'
    ZOHO_WHITELABEL_ALIAS = 'ZOHO_WHITELABEL_ALIAS'
    DEEPSEEK_API_KEY = 'DEEPSEEK_API_KEY'
    N8N_BASE_URL = 'N8N_BASE_URL'
    N8N_API_KEY = 'N8N_API_KEY'
}
foreach ($item in $localMappings.GetEnumerator()) {
    $value = [Environment]::GetEnvironmentVariable($item.Value)
    if (Is-UsableValue $value) { $desired[$item.Key] = $value }
}

foreach ($item in (Read-SnovEvidence).GetEnumerator()) { $desired[$item.Key] = $item.Value }
$desired['INTERNAL_TOKEN'] = Read-InternalTokenFromN8n

$fixed = @{
    FEISHU_APP_TOKEN = 'KINabIENjak8fRsB6AHcIDALntc'
    T_LAUNCH_CAMPAIGN = 'tbl8w0O7pI5PsRnq'
    T_LAUNCH_NODE = 'tblUljeSSAvFdFT6'
    T_COMPETITOR_POST = 'tblCDbvLtnLzdxEp'
    T_COMPETITOR_EVENT = 'tblpZaWYEWy54Sll'
    T_LAUNCH_PARTICIPANT = 'tblt0zD4hDb7sFqn'
    LAUNCH_EVIDENCE_ENABLED = '1'
    LAUNCH_PARTICIPATION_WRITE_ENABLED = '1'
    LAUNCH_ACTIVITY_QUEUE_ENABLED = '1'
    SKU_LIB_APP_TOKEN = 'MvtZb6OE9aJFaisO913cWSErnFe'
    SKU_LIB_TABLE_ID = 'tblwJ3BRkIuHDuSK'
    T_UPLOAD_REPORT = 'tblHrlzTeSIhOjCY'
    T_ROI_ATTR_MAP = 'tblzxyUxNF7gWqJe'
    T_ROI_ATTR_GAP = 'tbliU8GDl6SU9b4y'
    KOL_LAUNCH_DAILY_GROUP_ENABLED = '1'
    EMAIL_TEST_ALLOWLIST = 'frankiepan501@gmail.com'
    EMAIL_DRY_RUN_TO = 'frankiepan501@gmail.com'
    SEND_RATE_PER_RUN = '12'
    SEND_PER_BRAND_PER_RUN = '6'
    SEND_DAILY_CAP = '120'
    KOL_ENRICH_TEMPLATE_MODE = '1'
}
foreach ($item in $fixed.GetEnumerator()) { $desired[$item.Key] = $item.Value }

$query = 'query($serviceID:ObjectID!,$environmentID:ObjectID!){service(_id:$serviceID){variables(environmentID:$environmentID){key value}}}'
$data = Invoke-Zeabur $query @{ serviceID = $serviceId; environmentID = $environmentId }
$current = @{}
foreach ($item in $data.service.variables) { $current[$item.key] = "$($item.value)" }

$creates = @()
$updates = @()
foreach ($key in @($desired.Keys | Sort-Object)) {
    if (-not (Is-UsableValue "$($desired[$key])")) { throw "Refusing unusable value for $key" }
    if (-not $current.ContainsKey($key)) { $creates += $key }
    elseif ($current[$key] -ne $desired[$key]) { $updates += $key }
}

if ($Commit) {
    $createMutation = 'mutation($serviceID:ObjectID!,$environmentID:ObjectID!,$key:String!,$value:String!){createEnvironmentVariable(serviceID:$serviceID,environmentID:$environmentID,key:$key,value:$value){key}}'
    $updateMutation = 'mutation($serviceID:ObjectID!,$environmentID:ObjectID!,$oldKey:String!,$newKey:String!,$value:String!){updateSingleEnvironmentVariable(serviceID:$serviceID,environmentID:$environmentID,oldKey:$oldKey,newKey:$newKey,value:$value){key}}'
    foreach ($key in $creates) {
        [void](Invoke-Zeabur $createMutation @{ serviceID=$serviceId; environmentID=$environmentId; key=$key; value=$desired[$key] })
    }
    foreach ($key in $updates) {
        [void](Invoke-Zeabur $updateMutation @{ serviceID=$serviceId; environmentID=$environmentId; oldKey=$key; newKey=$key; value=$desired[$key] })
    }

    $readback = Invoke-Zeabur $query @{ serviceID = $serviceId; environmentID = $environmentId }
    $actual = @{}
    foreach ($item in $readback.service.variables) { $actual[$item.key] = "$($item.value)" }
    $missing = @($desired.Keys | Where-Object { -not $actual.ContainsKey($_) } | Sort-Object)
    $mismatch = @($desired.Keys | Where-Object { $actual.ContainsKey($_) -and $actual[$_] -ne $desired[$_] } | Sort-Object)
    if ($missing.Count -or $mismatch.Count) {
        throw "Recovery readback failed: missing=$($missing -join ',') mismatch=$($mismatch -join ',')"
    }
}

[pscustomobject]@{
    mode = if ($Commit) { 'commit' } else { 'dry-run' }
    desired_count = $desired.Count
    current_count_before = $current.Count
    create_count = $creates.Count
    update_count = $updates.Count
    create_keys = $creates
    update_keys = $updates
    secrets_printed = 0
} | ConvertTo-Json -Depth 5
