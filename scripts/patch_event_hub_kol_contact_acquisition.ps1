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
    param([ValidateSet('GET', 'PUT')][string]$Method, [string]$Path, [object]$Body = $null)
    $params = @{ Method=$Method; Uri="$apiBase$Path"; Headers=$headers; ContentType='application/json' }
    if ($null -ne $Body) { $params.Body = $Body | ConvertTo-Json -Depth 80 -Compress }
    Invoke-RestMethod @params
}

function New-MainEdge { param([string]$Target); @{ node=$Target; type='main'; index=0 } }

$workflow = Invoke-N8n -Method GET -Path "/workflows/$WorkflowId"
if ($workflow.name -ne '飞书事件中心 - Event Hub' -or -not $workflow.active) {
    throw 'Refusing to patch an unexpected or inactive workflow.'
}
$originalJson = $workflow | ConvertTo-Json -Depth 80
$nodeByName = @{}
foreach ($node in @($workflow.nodes)) { $nodeByName[$node.name] = $node }
$sourceName = 'Is FBIG Daily Confirm?'
$nextName = 'Is Draft Action?'
$ifName = 'Is KOL Contact Acquisition?'
$callbackName = 'KOL Contact Acquisition Callback'
foreach ($required in @($sourceName, $nextName, 'KOL ROI Callback')) {
    if (-not $nodeByName.ContainsKey($required)) { throw "Missing expected node: $required" }
}
$sourceMain = $workflow.connections.$sourceName.main
if (@($sourceMain).Count -ne 2 -or @($sourceMain[1]).Count -ne 1 -or $sourceMain[1][0].node -notin @($nextName, $ifName)) {
    throw 'Unexpected Event Hub branch; refusing a blind full-workflow PUT.'
}

if (-not $nodeByName.ContainsKey($ifName)) {
    $workflow.nodes += @{
        id=[guid]::NewGuid().ToString(); name=$ifName; type='n8n-nodes-base.if'; typeVersion=2; position=@(1780, 330)
        parameters=@{
            conditions=@{
                options=@{caseSensitive=$true;leftValue='';typeValidation='strict'}
                conditions=@(@{id='kol-contact-c1';leftValue='={{ $json.card_action && $json.card_action.action ? $json.card_action.action : "" }}';rightValue='kol_contact_';operator=@{type='string';operation='startsWith'}})
                combinator='and'
            }
            options=@{}
        }
    }
}
if (-not $nodeByName.ContainsKey($callbackName)) {
    $authHeaders = $nodeByName['KOL ROI Callback'].parameters.headerParameters
    if (-not $authHeaders -or @($authHeaders.parameters).Count -lt 1) { throw 'Cannot reuse kol-auto auth headers.' }
    $workflow.nodes += @{
        id=[guid]::NewGuid().ToString(); name=$callbackName; type='n8n-nodes-base.httpRequest'; typeVersion=4.2; position=@(1990, 330)
        parameters=@{
            method='POST'; url="$($ServiceBase.TrimEnd('/'))/kol/contact-acquisition/callback"
            sendHeaders=$true; headerParameters=$authHeaders; sendBody=$true; specifyBody='json'
            jsonBody='={{ JSON.stringify({ event: { action: { value: $json.card_action || {}, form_value: $json.card_form_value || {} }, operator_open_id: $json.sender_open_id || "", open_id: $json.sender_open_id || "", message_id: $json.message_id || "", open_message_id: $json.message_id || "", chat_id: $json.chat_id || "", open_chat_id: $json.chat_id || "", chat_type: $json.chat_type || "p2p" } }) }}'
            options=@{timeout=300000}
        }
    }
}

$workflow.connections.$sourceName.main = @(
    @((New-MainEdge -Target 'FBIG Daily Confirm Payload')),
    @((New-MainEdge -Target $ifName))
)
$workflow.connections | Add-Member -Force -NotePropertyName $ifName -NotePropertyValue @{
    main=@(
        @((New-MainEdge -Target $callbackName)),
        @((New-MainEdge -Target $nextName))
    )
}

$backupDir = Join-Path $env:TEMP 'kol-event-hub-backups'
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
$backupPath = Join-Path $backupDir ("event-hub-$WorkflowId-" + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-before-kol-contact.json')
Set-Content -LiteralPath $backupPath -Value $originalJson -Encoding UTF8
$body = @{name=$workflow.name;nodes=$workflow.nodes;connections=$workflow.connections;settings=$workflow.settings}
$null = Invoke-N8n -Method PUT -Path "/workflows/$WorkflowId" -Body $body
$after = Invoke-N8n -Method GET -Path "/workflows/$WorkflowId"
if (-not $after.active) { throw 'Readback failed: Event Hub became inactive.' }
$afterNames = @($after.nodes | ForEach-Object {$_.name})
if ($ifName -notin $afterNames -or $callbackName -notin $afterNames) { throw 'Readback failed: new nodes missing.' }
if ($after.connections.$sourceName.main[1][0].node -ne $ifName -or
    $after.connections.$ifName.main[0][0].node -ne $callbackName -or
    $after.connections.$ifName.main[1][0].node -ne $nextName) {
    throw 'Readback failed: branch connections drifted.'
}
[pscustomobject]@{
    workflow_id=$after.id;active=$after.active;node_count=@($after.nodes).Count
    route_prefix='kol_contact_';callback_path='/kol/contact-acquisition/callback'
    backup_path=$backupPath
} | ConvertTo-Json -Compress
