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
if ($workflow.name -ne '飞书事件中心 - Event Hub') {
    throw "Workflow ID $WorkflowId belongs to '$($workflow.name)'."
}
if (-not $workflow.active) {
    throw 'Refusing to patch an inactive Event Hub workflow.'
}

$beforeCount = @($workflow.nodes).Count
$nodeByName = @{}
foreach ($node in @($workflow.nodes)) {
    if ($nodeByName.ContainsKey($node.name)) { throw "Duplicate node name: $($node.name)" }
    $nodeByName[$node.name] = $node
}

$required = @('Is KOL ROI Action', 'KOL ROI Callback', 'Is ML Profit Action')
foreach ($name in $required) {
    if (-not $nodeByName.ContainsKey($name)) { throw "Missing expected node: $name" }
}

$sourceConnection = $workflow.connections.'Is KOL ROI Action'.main
if (@($sourceConnection).Count -ne 2 -or
    @($sourceConnection[1]).Count -ne 1 -or
    $sourceConnection[1][0].node -notin @('Is ML Profit Action', 'Is Launch Reply Attribution?')) {
    throw 'Unexpected false branch after Is KOL ROI Action; refusing a blind full-workflow PUT.'
}

$ifName = 'Is Launch Reply Attribution?'
$callbackName = 'Launch Reply Attribution Callback'
$ifNode = $nodeByName[$ifName]
$callbackNode = $nodeByName[$callbackName]

if (-not $ifNode) {
    $ifNode = @{
        id = [guid]::NewGuid().ToString()
        name = $ifName
        type = 'n8n-nodes-base.if'
        typeVersion = 2
        position = @(1230, 720)
        parameters = @{
            conditions = @{
                options = @{ caseSensitive = $true; leftValue = ''; typeValidation = 'strict' }
                conditions = @(@{
                    id = 'launch-reply-attribution-c1'
                    leftValue = '={{ $json.card_action && $json.card_action.action ? $json.card_action.action : "" }}'
                    rightValue = 'launch_reply_attribution_confirm'
                    operator = @{ type = 'string'; operation = 'equals' }
                })
                combinator = 'and'
            }
            options = @{}
        }
    }
    $workflow.nodes += $ifNode
}

if (-not $callbackNode) {
    $authHeaders = $nodeByName['KOL ROI Callback'].parameters.headerParameters
    if (-not $authHeaders -or @($authHeaders.parameters).Count -lt 1) {
        throw 'Could not reuse the existing kol-auto Authorization headers.'
    }
    $callbackNode = @{
        id = [guid]::NewGuid().ToString()
        name = $callbackName
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(1450, 720)
        parameters = @{
            method = 'POST'
            url = "$($ServiceBase.TrimEnd('/'))/launch/reply-attribution/callback"
            sendHeaders = $true
            headerParameters = $authHeaders
            sendBody = $true
            specifyBody = 'json'
            jsonBody = '={{ JSON.stringify({ event: { action: { value: $json.card_action || {}, form_value: $json.card_form_value || {} }, operator_open_id: $json.sender_open_id || "", open_id: $json.sender_open_id || "", message_id: $json.message_id || "", open_message_id: $json.message_id || "", chat_id: $json.chat_id || "", open_chat_id: $json.chat_id || "", chat_type: $json.chat_type || "p2p" } }) }}'
            options = @{ timeout = 300000 }
        }
    }
    $workflow.nodes += $callbackNode
}

$workflow.connections.'Is KOL ROI Action'.main = @(
    @((New-MainEdge -Target 'KOL ROI Callback')),
    @((New-MainEdge -Target $ifName))
)
$workflow.connections | Add-Member -Force -NotePropertyName $ifName -NotePropertyValue @{
    main = @(
        @((New-MainEdge -Target $callbackName)),
        @((New-MainEdge -Target 'Is ML Profit Action'))
    )
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

$afterNames = @($after.nodes | ForEach-Object { $_.name })
if ($ifName -notin $afterNames -or $callbackName -notin $afterNames) {
    throw 'Readback failed: new attribution nodes are missing.'
}
if (-not $after.active) { throw 'Readback failed: Event Hub is no longer active.' }
$afterIf = $after.connections.$ifName.main
if ($afterIf[0][0].node -ne $callbackName -or $afterIf[1][0].node -ne 'Is ML Profit Action') {
    throw 'Readback failed: attribution branch connections drifted.'
}

[pscustomobject]@{
    workflow_id = $after.id
    active = $after.active
    before_node_count = $beforeCount
    after_node_count = @($after.nodes).Count
    route_action = 'launch_reply_attribution_confirm'
    callback_path = '/launch/reply-attribution/callback'
    backup_created = (Test-Path -LiteralPath $backupPath)
} | ConvertTo-Json -Compress
