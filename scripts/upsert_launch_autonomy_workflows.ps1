$ErrorActionPreference = 'Stop'

if (-not $env:N8N_API_KEY) {
    throw 'N8N_API_KEY is required.'
}

$n8nBase = 'https://frankiepan501.zeabur.app/api/v1'
$apiHeaders = @{ 'X-N8N-API-KEY' = $env:N8N_API_KEY }
$sourceWorkflowId = 'VWTMNkXf0zcs4Kvz'
$serviceBase = 'https://kol-auto.zeabur.app'
$legacyStartName = 'KOL Launch - Hourly Autonomous Refill + Completion Poll'
$startName = 'KOL Launch - Hourly Autonomous Refill Start'
$auditName = 'KOL Launch - Hourly Completion Audit'
$daveCron = if ($env:LAUNCH_AUTONOMY_DAVE_CRON) { $env:LAUNCH_AUTONOMY_DAVE_CRON } else { '0 * * * *' }
$piranhaCron = if ($env:LAUNCH_AUTONOMY_PIRANHA_CRON) { $env:LAUNCH_AUTONOMY_PIRANHA_CRON } else { '5 * * * *' }
$auditCron = if ($env:LAUNCH_AUTONOMY_AUDIT_CRON) { $env:LAUNCH_AUTONOMY_AUDIT_CRON } else { '50 * * * *' }

# Reuse the existing launch controller's Authorization header in memory. Do not
# persist the service token in this repository or print it to stdout.
$source = Invoke-RestMethod -Headers $apiHeaders -Uri "$n8nBase/workflows/$sourceWorkflowId"
$sourceHttp = $source.nodes | Where-Object {
    $_.type -eq 'n8n-nodes-base.httpRequest' -and $_.parameters.headerParameters
} | Select-Object -First 1
if (-not $sourceHttp) {
    throw "Could not find a reusable Authorization header in workflow $sourceWorkflowId."
}
$authHeaders = $sourceHttp.parameters.headerParameters

$all = Invoke-RestMethod -Headers $apiHeaders -Uri "$n8nBase/workflows?limit=250"

function Get-ExistingWorkflow([string]$Name) {
    return $all.data | Where-Object { $_.name -eq $Name } | Select-Object -First 1
}

function Get-NodeIdMap($Workflow) {
    $map = @{}
    if ($Workflow) {
        $current = Invoke-RestMethod -Headers $apiHeaders -Uri "$n8nBase/workflows/$($Workflow.id)"
        foreach ($node in $current.nodes) { $map[$node.name] = $node.id }
        return @{ map = $map; current = $current }
    }
    return @{ map = $map; current = $null }
}

function Node-Id($Map, [string]$Name) {
    if ($Map.ContainsKey($Name)) { return $Map[$Name] }
    return [guid]::NewGuid().ToString()
}

function Main-To([string]$NodeName) {
    return @{ main = ,@(@{ node = $NodeName; type = 'main'; index = 0 }) }
}

function Upsert-Workflow([string]$Name, $Nodes, $Connections, $Settings, $Existing = $null) {
    $existing = $Existing
    if (-not $existing) { $existing = Get-ExistingWorkflow $Name }
    $current = $null
    if ($existing) {
        $current = Invoke-RestMethod -Headers $apiHeaders -Uri "$n8nBase/workflows/$($existing.id)"
        if ($current.active) {
            Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($existing.id)/deactivate" | Out-Null
        }
    }
    $payload = @{ name = $Name; nodes = $Nodes; connections = $Connections; settings = $Settings }
    $json = $payload | ConvertTo-Json -Depth 30 -Compress
    if ($existing) {
        $workflow = Invoke-RestMethod -Method Put -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($existing.id)" -Body $json
    } else {
        $workflow = Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows" -Body $json
    }
    $workflow = Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($workflow.id)/activate"
    return $workflow
}

$settings = @{
    availableInMCP = $false
    executionOrder = 'v1'
    timezone = 'Asia/Shanghai'
    callerPolicy = 'workflowsFromSameOwner'
}

$startExisting = Get-ExistingWorkflow $startName
if (-not $startExisting) { $startExisting = Get-ExistingWorkflow $legacyStartName }
$startState = Get-NodeIdMap $startExisting
$sm = $startState.map
$startNodes = @(
    @{
        id = Node-Id $sm 'Dave Hourly BJ'
        name = 'Dave Hourly BJ'
        type = 'n8n-nodes-base.scheduleTrigger'
        typeVersion = 1.1
        position = @(0, 80)
        parameters = @{ rule = @{ interval = @(@{ field = 'cronExpression'; expression = $daveCron }) } }
    },
    @{
        id = Node-Id $sm 'Dave Start'
        name = 'Dave Start'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(260, 80)
        parameters = @{
            method = 'POST'
            url = "$serviceBase/launch/runtime/autonomous-refill"
            sendHeaders = $true
            headerParameters = $authHeaders
            sendBody = $true
            contentType = 'raw'
            rawContentType = 'application/json'
            body = '{"campaign_id":"launch-20260915-funlab-dave-ys11-5"}'
            options = @{ timeout = 30000 }
        }
    },
    @{
        id = Node-Id $sm 'Piranha Hourly BJ'
        name = 'Piranha Hourly BJ'
        type = 'n8n-nodes-base.scheduleTrigger'
        typeVersion = 1.1
        position = @(0, 360)
        parameters = @{ rule = @{ interval = @(@{ field = 'cronExpression'; expression = $piranhaCron }) } }
    },
    @{
        id = Node-Id $sm 'Piranha Start'
        name = 'Piranha Start'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(260, 360)
        parameters = @{
            method = 'POST'
            url = "$serviceBase/launch/runtime/autonomous-refill"
            sendHeaders = $true
            headerParameters = $authHeaders
            sendBody = $true
            contentType = 'raw'
            rawContentType = 'application/json'
            body = '{"campaign_id":"launch-20260915-powkong-piranha-v2"}'
            options = @{ timeout = 30000 }
        }
    }
)
$startConnections = @{
    'Dave Hourly BJ' = Main-To 'Dave Start'
    'Piranha Hourly BJ' = Main-To 'Piranha Start'
}
$startWorkflow = Upsert-Workflow $startName $startNodes $startConnections $settings $startExisting

$auditExisting = Get-ExistingWorkflow $auditName
$auditState = Get-NodeIdMap $auditExisting
$am = $auditState.map
$validateDave = @'
const data = $input.first().json;
const age = Math.floor(Date.now() / 1000) - Number(data.started_ts || 0);
if (data.status !== 'success' || age > 70 * 60) {
  throw new Error('Dave latest autonomous job is not a fresh success: ' + JSON.stringify(data).slice(0, 1000));
}
return [{json: data}];
'@
$validatePiranha = @'
const data = $input.first().json;
const age = Math.floor(Date.now() / 1000) - Number(data.started_ts || 0);
if (data.status !== 'success' || age > 70 * 60) {
  throw new Error('Piranha latest autonomous job is not a fresh success: ' + JSON.stringify(data).slice(0, 1000));
}
return [{json: data}];
'@
$auditNodes = @(
    @{
        id = Node-Id $am 'Audit Hourly BJ'
        name = 'Audit Hourly BJ'
        type = 'n8n-nodes-base.scheduleTrigger'
        typeVersion = 1.1
        position = @(0, 220)
        parameters = @{ rule = @{ interval = @(@{ field = 'cronExpression'; expression = $auditCron }) } }
    },
    @{
        id = Node-Id $am 'Dave Latest Status'
        name = 'Dave Latest Status'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(260, 80)
        parameters = @{
            method = 'GET'
            url = "$serviceBase/launch/runtime/jobs/latest?campaign_id=launch-20260915-funlab-dave-ys11-5"
            sendHeaders = $true
            headerParameters = $authHeaders
            options = @{ timeout = 30000 }
        }
    },
    @{
        id = Node-Id $am 'Dave Validate Fresh Success'
        name = 'Dave Validate Fresh Success'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(520, 80)
        parameters = @{ jsCode = $validateDave }
    },
    @{
        id = Node-Id $am 'Piranha Latest Status'
        name = 'Piranha Latest Status'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(260, 360)
        parameters = @{
            method = 'GET'
            url = "$serviceBase/launch/runtime/jobs/latest?campaign_id=launch-20260915-powkong-piranha-v2"
            sendHeaders = $true
            headerParameters = $authHeaders
            options = @{ timeout = 30000 }
        }
    },
    @{
        id = Node-Id $am 'Piranha Validate Fresh Success'
        name = 'Piranha Validate Fresh Success'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(520, 360)
        parameters = @{ jsCode = $validatePiranha }
    }
)
$auditConnections = @{
    'Audit Hourly BJ' = @{ main = ,@(
        @{ node = 'Dave Latest Status'; type = 'main'; index = 0 },
        @{ node = 'Piranha Latest Status'; type = 'main'; index = 0 }
    ) }
    'Dave Latest Status' = Main-To 'Dave Validate Fresh Success'
    'Piranha Latest Status' = Main-To 'Piranha Validate Fresh Success'
}
$auditWorkflow = Upsert-Workflow $auditName $auditNodes $auditConnections $settings

@(
    [pscustomobject]@{
        id = $startWorkflow.id; name = $startWorkflow.name; active = $startWorkflow.active
        node_count = @($startWorkflow.nodes).Count
        schedule = "Dave=$daveCron; Piranha=$piranhaCron; Asia/Shanghai"
    },
    [pscustomobject]@{
        id = $auditWorkflow.id; name = $auditWorkflow.name; active = $auditWorkflow.active
        node_count = @($auditWorkflow.nodes).Count
        schedule = "Audit=$auditCron; Asia/Shanghai"
    }
) | ConvertTo-Json -Depth 5
