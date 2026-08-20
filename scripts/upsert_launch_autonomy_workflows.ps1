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
$legacyAuditName = 'KOL Launch - Hourly Completion Audit'
$auditName = 'KOL Launch - 15min Business Result Audit'
$daveCron = if ($env:LAUNCH_AUTONOMY_DAVE_CRON) { $env:LAUNCH_AUTONOMY_DAVE_CRON } else { '0 * * * *' }
$piranhaCron = if ($env:LAUNCH_AUTONOMY_PIRANHA_CRON) { $env:LAUNCH_AUTONOMY_PIRANHA_CRON } else { '5,20,35,50 * * * *' }
$auditCron = if ($env:LAUNCH_AUTONOMY_AUDIT_CRON) { $env:LAUNCH_AUTONOMY_AUDIT_CRON } else { '12,27,42,57 * * * *' }

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
        id = Node-Id $sm 'Piranha Every 15m BJ'
        name = 'Piranha Every 15m BJ'
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
    'Piranha Every 15m BJ' = Main-To 'Piranha Start'
}
$startWorkflow = Upsert-Workflow $startName $startNodes $startConnections $settings $startExisting

$auditExisting = Get-ExistingWorkflow $auditName
$legacyAuditExisting = Get-ExistingWorkflow $legacyAuditName
if (-not $auditExisting) { $auditExisting = $legacyAuditExisting }
$auditState = Get-NodeIdMap $auditExisting
$am = $auditState.map
$validateDave = @'
const data = $input.first().json;
const age = Math.floor(Date.now() / 1000) - Number(data.updated_ts || data.started_ts || 0);
if (data.status === 'running' && age <= 70 * 60) {
  return [{json: {...data, validation: 'dave_running_within_expected_window'}}];
}
if (data.status !== 'success' || age > 70 * 60) {
  throw new Error('Dave latest autonomous job is not a fresh success: ' + JSON.stringify(data).slice(0, 1000));
}
return [{json: data}];
'@
$validatePiranha = @'
const data = $input.first().json;
const age = Math.floor(Date.now() / 1000) - Number(data.updated_ts || data.started_ts || 0);
if (data.status === 'running' && age <= 45 * 60) {
  return [{json: {...data, validation: 'running_within_expected_window'}}];
}
const result = data.result || {};
if (data.status !== 'success' || age > 35 * 60) {
  throw new Error('Piranha autonomous job is stale or not successful: ' + JSON.stringify(data).slice(0, 1000));
}
const allowedOutcomes = new Set([
  'stopped', 'held', 'inventory_sufficient', 'quota_exhausted',
  'ready_inventory_created', 'supply_in_progress', 'supply_blocked', 'no_action_needed',
]);
const hasQuota = result.quota && Number.isFinite(result.quota.remaining);
const hasInventory = Number.isFinite(result.inventory_after);
const hasProgress = typeof result.made_supply_progress === 'boolean'
  && result.supply_progress_breakdown
  && typeof result.supply_progress_breakdown === 'object'
  && !Array.isArray(result.supply_progress_breakdown);
if (!allowedOutcomes.has(result.business_outcome) || !hasQuota || !hasInventory || !hasProgress) {
  throw new Error('Piranha missing or invalid business result fields: ' + JSON.stringify(data).slice(0, 1000));
}
if (result.business_outcome === 'supply_blocked') {
  throw new Error('Piranha has unused quota but refill made no supply progress: ' + JSON.stringify(data).slice(0, 1000));
}
return [{json: {
  status: data.status, updated_ts: data.updated_ts, validation: 'business_result_ok',
  result: {
    business_outcome: result.business_outcome,
    quota_remaining: result.quota.remaining,
    inventory_after: result.inventory_after,
    made_supply_progress: result.made_supply_progress,
    supply_progress_breakdown: result.supply_progress_breakdown,
  },
}}];
'@
$auditNodes = @(
    @{
        id = Node-Id $am 'Audit Every 15m BJ'
        name = 'Audit Every 15m BJ'
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
    'Audit Every 15m BJ' = @{ main = ,@(
        @{ node = 'Dave Latest Status'; type = 'main'; index = 0 },
        @{ node = 'Piranha Latest Status'; type = 'main'; index = 0 }
    ) }
    'Dave Latest Status' = Main-To 'Dave Validate Fresh Success'
    'Piranha Latest Status' = Main-To 'Piranha Validate Fresh Success'
}
$auditWorkflow = Upsert-Workflow $auditName $auditNodes $auditConnections $settings $auditExisting
if ($legacyAuditExisting -and $legacyAuditExisting.id -ne $auditWorkflow.id) {
    $legacyCurrent = Invoke-RestMethod -Headers $apiHeaders -Uri "$n8nBase/workflows/$($legacyAuditExisting.id)"
    if ($legacyCurrent.active) {
        Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($legacyAuditExisting.id)/deactivate" | Out-Null
    }
}

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
