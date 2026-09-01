param(
    [string]$N8nBase = $env:N8N_BASE_URL,
    [string]$ApiKey = $env:N8N_API_KEY,
    [string]$WorkflowId = 'YjTXaoWAcy89xZpT',
    [string]$ServiceBase = 'https://kol-auto.zeabur.app'
)

$ErrorActionPreference = 'Stop'

function Resolve-ConfigValue {
    param([string]$Name, [string]$Value)
    if (-not [string]::IsNullOrWhiteSpace($Value)) { return $Value }
    $userValue = [Environment]::GetEnvironmentVariable($Name, 'User')
    if (-not [string]::IsNullOrWhiteSpace($userValue)) { return $userValue }
    return [Environment]::GetEnvironmentVariable($Name, 'Machine')
}

$N8nBase = Resolve-ConfigValue -Name 'N8N_BASE_URL' -Value $N8nBase
$ApiKey = Resolve-ConfigValue -Name 'N8N_API_KEY' -Value $ApiKey
if ([string]::IsNullOrWhiteSpace($N8nBase) -or [string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'N8N_BASE_URL and N8N_API_KEY are required.'
}

$apiBase = $N8nBase.TrimEnd('/')
if (-not $apiBase.EndsWith('/api/v1')) { $apiBase = "$apiBase/api/v1" }
$headers = @{ 'X-N8N-API-KEY' = $ApiKey }

function Invoke-N8n {
    param(
        [ValidateSet('GET', 'PUT')][string]$Method,
        [string]$Path,
        [object]$Body = $null
    )
    $params = @{
        Method = $Method
        Uri = "$apiBase$Path"
        Headers = $headers
        ContentType = 'application/json'
    }
    if ($null -ne $Body) {
        $params.Body = $Body | ConvertTo-Json -Depth 60 -Compress
    }
    return Invoke-RestMethod @params
}

function New-MainEdge {
    param([string]$Target)
    return @{ node = $Target; type = 'main'; index = 0 }
}

$workflow = Invoke-N8n -Method GET -Path "/workflows/$WorkflowId"
if (-not $workflow.active) {
    throw 'Refusing to patch an inactive Event Hub workflow.'
}

$beforeCount = @($workflow.nodes).Count
$nodeByName = @{}
foreach ($node in @($workflow.nodes)) {
    if ($nodeByName.ContainsKey($node.name)) { throw "Duplicate node name: $($node.name)" }
    $nodeByName[$node.name] = $node
}

$sourceName = 'Is FBIG Daily Confirm?'
$nextName = 'Is Draft Action?'
$ifName = 'Is KOL Contact Acquisition?'
$callbackName = 'KOL Contact Acquisition Callback'
foreach ($name in @($sourceName, $nextName, $ifName, $callbackName)) {
    if (-not $nodeByName.ContainsKey($name)) { throw "Missing required node: $name" }
}

$sourceConnection = $workflow.connections.$sourceName.main
$routeConnection = $workflow.connections.$ifName.main
if (@($sourceConnection).Count -ne 2 -or $sourceConnection[1][0].node -ne $ifName) {
    throw 'Existing contact-acquisition entry branch drifted; refusing to patch.'
}
if (@($routeConnection).Count -ne 2 -or
    $routeConnection[0][0].node -ne $callbackName -or
    $routeConnection[1][0].node -ne $nextName) {
    throw 'Existing contact-acquisition result branches drifted; refusing to patch.'
}

$ifNode = $nodeByName[$ifName]
$callbackNode = $nodeByName[$callbackName]

$actions = @(
    'kol_no_email_email_captured',
    'kol_no_email_platform_ongoing',
    'kol_no_email_not_fit',
    'kol_no_email_no_response'
)
$conditions = @()
for ($i = 0; $i -lt $actions.Count; $i++) {
    $conditions += @{
        id = "kol-no-email-c$($i + 1)"
        leftValue = '={{ $json.card_action && $json.card_action.action ? $json.card_action.action : "" }}'
        rightValue = $actions[$i]
        operator = @{ type = 'string'; operation = 'equals' }
    }
}
$ifNode.parameters = @{
    conditions = @{
        options = @{ caseSensitive = $true; leftValue = ''; typeValidation = 'strict' }
        conditions = $conditions
        combinator = 'or'
    }
    options = @{}
}

$authHeaders = $callbackNode.parameters.headerParameters
if (-not $authHeaders -or @($authHeaders.parameters).Count -lt 1) {
    throw 'Existing contact callback has no Authorization header; refusing to patch.'
}
$callbackNode.parameters = @{
    method = 'POST'
    url = "$($ServiceBase.TrimEnd('/'))/kol/no-email-outreach/callback"
    sendHeaders = $true
    headerParameters = $authHeaders
    sendBody = $true
    specifyBody = 'json'
    jsonBody = '={{ JSON.stringify({ event: { action: { value: $json.card_action || {}, form_value: $json.card_form_value || {} }, operator: { open_id: $json.sender_open_id || "", name: $json.sender_name || "" }, operator_open_id: $json.sender_open_id || "", open_id: $json.sender_open_id || "", message_id: $json.message_id || "", open_message_id: $json.message_id || "", chat_id: $json.chat_id || "", open_chat_id: $json.chat_id || "", chat_type: $json.chat_type || "p2p" } }) }}'
    options = @{ timeout = 300000 }
}

$backupDir = Join-Path $env:TEMP 'kol-event-hub-backups'
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
$backupPath = Join-Path $backupDir ("event-hub-$WorkflowId-" + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.json')
$workflow | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath $backupPath -Encoding UTF8

$body = @{
    name = $workflow.name
    nodes = $workflow.nodes
    connections = $workflow.connections
    settings = $workflow.settings
}
$null = Invoke-N8n -Method PUT -Path "/workflows/$WorkflowId" -Body $body
$after = Invoke-N8n -Method GET -Path "/workflows/$WorkflowId"

if (-not $after.active) { throw 'Readback failed: Event Hub is no longer active.' }
$afterIf = @($after.nodes | Where-Object { $_.name -eq $ifName })[0]
$afterCallback = @($after.nodes | Where-Object { $_.name -eq $callbackName })[0]
$afterActions = @($afterIf.parameters.conditions.conditions | ForEach-Object { $_.rightValue })
if (($afterActions -join '|') -ne ($actions -join '|')) {
    throw 'Readback failed: no-email action routes drifted.'
}
if ($afterCallback.parameters.url -ne "$($ServiceBase.TrimEnd('/'))/kol/no-email-outreach/callback") {
    throw 'Readback failed: no-email callback URL drifted.'
}

[pscustomobject]@{
    workflow_id = $after.id
    active = $after.active
    before_node_count = $beforeCount
    after_node_count = @($after.nodes).Count
    reused_existing_branch = $true
    route_actions = $afterActions
    callback_path = '/kol/no-email-outreach/callback'
    backup_created = (Test-Path -LiteralPath $backupPath)
} | ConvertTo-Json -Compress
