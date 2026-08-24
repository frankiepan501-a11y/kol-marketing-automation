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

function Merge-MissingProperties($Desired, $Existing) {
    if (-not ($Desired -is [System.Collections.IDictionary]) -or -not $Existing) {
        return $Desired
    }
    $properties = if ($Existing -is [System.Collections.IDictionary]) {
        $Existing.GetEnumerator() | ForEach-Object {
            [pscustomobject]@{ Name = $_.Key; Value = $_.Value }
        }
    } else {
        $Existing.PSObject.Properties
    }
    foreach ($property in $properties) {
        $name = [string]$property.Name
        if (-not $Desired.Contains($name)) {
            $Desired[$name] = $property.Value
        } elseif ($Desired[$name] -is [System.Collections.IDictionary]) {
            Merge-MissingProperties $Desired[$name] $property.Value | Out-Null
        }
    }
    return $Desired
}

function Upsert-Workflow([string]$Name, $Nodes, $Connections, $Settings, $Existing = $null) {
    $existing = $Existing
    if (-not $existing) { $existing = Get-ExistingWorkflow $Name }
    $current = $null
    if ($existing) {
        $current = Invoke-RestMethod -Headers $apiHeaders -Uri "$n8nBase/workflows/$($existing.id)"
    }
    $managedNames = @($Nodes | ForEach-Object { $_.name })
    $payloadNodes = @($Nodes)
    $payloadConnections = @{}
    $payloadSettings = @{}
    if ($current) {
        # Existing workflows may contain production-only nodes or settings added after this
        # script was written. Preserve them and replace only this script's managed nodes.
        $mergedManagedNodes = @($Nodes | ForEach-Object {
            $desiredNode = $_
            $currentNode = $current.nodes | Where-Object { $_.name -eq $desiredNode.name } |
                Select-Object -First 1
            Merge-MissingProperties $desiredNode $currentNode
        })
        $payloadNodes = @(
            @($current.nodes | Where-Object { $_.name -notin $managedNames }) + @($mergedManagedNodes)
        )
        foreach ($property in $current.connections.PSObject.Properties) {
            $payloadConnections[$property.Name] = $property.Value
        }
        if ($current.settings) {
            foreach ($property in $current.settings.PSObject.Properties) {
                $payloadSettings[$property.Name] = $property.Value
            }
        }
    }
    foreach ($key in $Connections.Keys) {
        $replacement = $Connections[$key]
        if ($current -and $current.connections.$key) {
            # 保留生产上后来接到“非受管节点”的分支；本脚本只替换受管节点之间的线。
            $extras = @()
            foreach ($branch in @($current.connections.$key.main)) {
                foreach ($target in @($branch)) {
                    if ($target.node -and $target.node -notin $managedNames) {
                        $extras += $target
                    }
                }
            }
            if ($extras.Count -gt 0) {
                $firstBranch = @($replacement.main[0]) + $extras
                $replacement = @{ main = ,@($firstBranch) }
            }
        }
        $payloadConnections[$key] = $replacement
    }
    foreach ($key in $Settings.Keys) { $payloadSettings[$key] = $Settings[$key] }
    $payload = @{
        name = $Name
        nodes = $payloadNodes
        connections = $payloadConnections
        settings = $payloadSettings
    }
    $json = $payload | ConvertTo-Json -Depth 30 -Compress
    if ($existing) {
        $wasActive = [bool]$current.active
        try {
            if ($wasActive) {
                Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($existing.id)/deactivate" | Out-Null
            }
            $workflow = Invoke-RestMethod -Method Put -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($existing.id)" -Body $json
            if ($wasActive) {
                $workflow = Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($workflow.id)/activate"
            }
        } catch {
            if ($wasActive) {
                try {
                    Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($existing.id)/activate" | Out-Null
                } catch {
                    Write-Warning "Workflow $($existing.id) update failed and automatic reactivation also failed."
                }
            }
            throw
        }
    } else {
        $workflow = Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows" -Body $json
        $workflow = Invoke-RestMethod -Method Post -Headers $apiHeaders -ContentType 'application/json' -Uri "$n8nBase/workflows/$($workflow.id)/activate"
    }
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
            body = '{"campaign_id":"launch-20260915-funlab-dave-ys11-5","dry_run":false,"ai_mode":"legacy_deepseek","confirm":"RUN_LEGACY_DEEPSEEK_REFILL"}'
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
            body = '{"campaign_id":"launch-20260915-powkong-piranha-v2","dry_run":false,"ai_mode":"legacy_deepseek","confirm":"RUN_LEGACY_DEEPSEEK_REFILL"}'
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
const data = $input.first().json || {};
function parseServiceTimestamp(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value > 1e12 ? Math.floor(value / 1000) : Math.floor(value);
  }
  const raw = String(value || '').trim();
  if (!raw) return 0;
  if (/^\d+(\.\d+)?$/.test(raw)) {
    const numeric = Number(raw);
    return numeric > 1e12 ? Math.floor(numeric / 1000) : Math.floor(numeric);
  }
  const normalized = raw.replace(' ', 'T').replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
}
const result = data.result || {};
const updated = parseServiceTimestamp(data.updated_ts || data.started_ts || data.started_at);
const age = updated ? Math.max(0, Math.floor(Date.now() / 1000) - updated) : 999999999;
let ok = true;
let validation = 'business_result_ok';
let error = '';
let nextStep = 'continue_normal_operation';
if (data.status === 'running' && age <= 70 * 60) {
  validation = 'dave_running_within_expected_window';
  nextStep = 'wait_for_background_job';
} else {
  const allowedOutcomes = new Set([
    'stopped', 'held', 'inventory_sufficient', 'quota_exhausted',
    'ready_inventory_created', 'supply_in_progress', 'supply_cooling_down',
    'supply_blocked', 'no_action_needed',
  ]);
  const hasQuota = result.quota && Number.isFinite(result.quota.remaining);
  const hasInventory = Number.isFinite(result.inventory_after);
  const hasProgress = typeof result.made_supply_progress === 'boolean'
    && result.supply_progress_breakdown
    && typeof result.supply_progress_breakdown === 'object'
    && !Array.isArray(result.supply_progress_breakdown);
  if (data.status !== 'success' || age > 70 * 60) {
    ok = false;
    validation = 'unhealthy';
    error = 'Dave autonomous job is stale, degraded, or failed';
    nextStep = result.business_outcome === 'supply_blocked'
      ? 'refresh_pending_reviews_or_continue_activity_competitor_evidence'
      : 'inspect_latest_job_and_rerun_autonomous_refill';
  } else if (!allowedOutcomes.has(result.business_outcome) || !hasQuota || !hasInventory || !hasProgress) {
    ok = false;
    validation = 'invalid_business_result';
    error = 'Dave missing or invalid business result fields';
    nextStep = 'inspect_service_result_contract';
  } else if (result.business_outcome === 'supply_blocked') {
    ok = false;
    validation = 'supply_blocked';
    error = 'Dave has unused quota but refill made no supply progress';
    nextStep = 'refresh_pending_reviews_or_continue_activity_competitor_evidence';
  }
}
const parts = result.supply_progress_breakdown || {};
return [{json: {
  campaign: 'dave', activity: 'Dave', ok, validation, error,
  updated_at: data.updated_ts || data.started_ts || data.started_at || 'missing',
  age_seconds: age, status: data.status || 'missing',
  inventory: Number(result.inventory_after || 0),
  quota_remaining: Number((result.quota || {}).remaining || 0),
  supply: result.business_outcome || 'unknown',
  supply_parts: {
    drafts: Number(parts.drafts_queued || 0) + Number(parts.drafts_queued_after_refresh || 0),
    approved: Number(parts.auto_approved_created || 0) + Number(parts.auto_approved_after_refresh || 0),
    discovery: Number(parts.discovery_tasks_created || 0),
    review: Number(parts.review_candidates_created || 0) + Number(parts.evidence_candidates_imported || 0),
  },
  next_step: nextStep,
  data: {status: data.status, updated_ts: data.updated_ts, result},
}}];
'@
$validatePiranha = @'
const data = $input.first().json || {};
function parseServiceTimestamp(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value > 1e12 ? Math.floor(value / 1000) : Math.floor(value);
  }
  const raw = String(value || '').trim();
  if (!raw) return 0;
  if (/^\d+(\.\d+)?$/.test(raw)) {
    const numeric = Number(raw);
    return numeric > 1e12 ? Math.floor(numeric / 1000) : Math.floor(numeric);
  }
  const normalized = raw.replace(' ', 'T').replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
}
const updated = parseServiceTimestamp(data.updated_ts || data.started_ts || data.started_at);
const age = updated ? Math.max(0, Math.floor(Date.now() / 1000) - updated) : 999999999;
let ok = true;
let validation = 'business_result_ok';
let error = '';
let nextStep = 'continue_normal_operation';
if (data.status === 'running' && age <= 45 * 60) {
  validation = 'running_within_expected_window';
  return [{json: {
    campaign: 'piranha', activity: 'Piranha', ok, validation, error,
    updated_at: data.updated_ts || data.started_ts || data.started_at || 'missing',
    age_seconds: age, status: data.status, inventory: 0, quota_remaining: 0,
    supply: 'background_running', supply_parts: {},
    next_step: 'wait_for_background_job', data,
  }}];
}
const result = data.result || {};
const allowedOutcomes = new Set([
  'stopped', 'held', 'inventory_sufficient', 'quota_exhausted',
  'ready_inventory_created', 'supply_in_progress', 'supply_cooling_down',
  'supply_blocked', 'no_action_needed',
]);
const hasQuota = result.quota && Number.isFinite(result.quota.remaining);
const hasInventory = Number.isFinite(result.inventory_after);
const hasProgress = typeof result.made_supply_progress === 'boolean'
  && result.supply_progress_breakdown
  && typeof result.supply_progress_breakdown === 'object'
  && !Array.isArray(result.supply_progress_breakdown);
if (data.status !== 'success' || age > 35 * 60) {
  ok = false;
  validation = 'unhealthy';
  error = 'Piranha autonomous job is stale or not successful';
  nextStep = result.business_outcome === 'supply_blocked'
    ? 'run_seven_layer_candidate_supply_and_refresh_pending_reviews'
    : 'inspect_latest_job_and_rerun_autonomous_refill';
} else if (!allowedOutcomes.has(result.business_outcome) || !hasQuota || !hasInventory || !hasProgress) {
  ok = false;
  validation = 'invalid_business_result';
  error = 'Piranha missing or invalid business result fields';
  nextStep = 'inspect_service_result_contract';
} else if (result.business_outcome === 'supply_blocked') {
  ok = false;
  validation = 'supply_blocked';
  error = 'Piranha has unused quota but refill made no supply progress';
  nextStep = 'run_seven_layer_candidate_supply_and_refresh_pending_reviews';
}
const parts = result.supply_progress_breakdown || {};
return [{json: {
  campaign: 'piranha', activity: 'Piranha', ok, validation, error,
  updated_at: data.updated_ts || data.started_ts || data.started_at || 'missing',
  age_seconds: age, status: data.status || 'missing',
  inventory: Number(result.inventory_after || 0),
  quota_remaining: Number((result.quota || {}).remaining || 0),
  supply: result.business_outcome || 'unknown',
  supply_parts: {
    drafts: Number(parts.drafts_queued || 0) + Number(parts.drafts_queued_after_refresh || 0),
    approved: Number(parts.auto_approved_created || 0) + Number(parts.auto_approved_after_refresh || 0),
    discovery: Number(parts.discovery_tasks_created || 0),
    review: Number(parts.review_candidates_created || 0) + Number(parts.evidence_candidates_imported || 0),
  },
  next_step: nextStep,
  data: {status: data.status, updated_ts: data.updated_ts, result},
}}];
'@
$validateBoth = @'
const reports = $input.all().map(item => item.json || {});
const byCampaign = Object.fromEntries(reports.map(report => [report.campaign, report]));
const missing = ['dave', 'piranha'].filter(name => !byCampaign[name]);
const failed = reports.filter(report => report.ok !== true);
const summary = {
  validation: 'both_campaigns_checked',
  checked_campaigns: reports.map(report => report.campaign).filter(Boolean).sort(),
  missing_campaigns: missing,
  dave: byCampaign.dave || null,
  piranha: byCampaign.piranha || null,
};
if (missing.length || failed.length) {
  const missingReports = missing.map(name => ({
    activity: name === 'dave' ? 'Dave' : 'Piranha', status: 'missing', inventory: 0,
    updated_at: 'missing', quota_remaining: 0, supply: 'missing', supply_parts: {},
    next_step: 'rerun_autonomous_refill', error: 'campaign_result_missing',
  }));
  const blockedReports = [...failed, ...missingReports];
  const readable = blockedReports.map(report => {
    const parts = report.supply_parts || {};
    const compactParts = `d${Number(parts.drafts || 0)}/a${Number(parts.approved || 0)}`
      + `/x${Number(parts.discovery || 0)}/r${Number(parts.review || 0)}`;
    return `${report.activity} blocked: latest=${report.updated_at}, status=${report.status}, `
      + `inventory=${report.inventory}, quota=${report.quota_remaining}, supply=${report.supply}, `
      + `parts=${compactParts}, next=${report.next_step}`;
  }).join(' | ');
  throw new Error(readable);
}
return [{json: summary}];
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
        onError = 'continueRegularOutput'
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
        onError = 'continueRegularOutput'
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
    },
    @{
        id = Node-Id $am 'Campaign Audit Merge'
        name = 'Campaign Audit Merge'
        type = 'n8n-nodes-base.merge'
        typeVersion = 3
        position = @(780, 220)
        parameters = @{ mode = 'append'; options = @{} }
    },
    @{
        id = Node-Id $am 'Validate Both Campaigns'
        name = 'Validate Both Campaigns'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(1040, 220)
        parameters = @{ jsCode = $validateBoth }
    }
)
$auditConnections = @{
    'Audit Every 15m BJ' = @{ main = ,@(
        @{ node = 'Dave Latest Status'; type = 'main'; index = 0 },
        @{ node = 'Piranha Latest Status'; type = 'main'; index = 0 }
    ) }
    'Dave Latest Status' = Main-To 'Dave Validate Fresh Success'
    'Piranha Latest Status' = Main-To 'Piranha Validate Fresh Success'
    'Dave Validate Fresh Success' = @{ main = ,@(
        @{ node = 'Campaign Audit Merge'; type = 'main'; index = 0 }
    ) }
    'Piranha Validate Fresh Success' = @{ main = ,@(
        @{ node = 'Campaign Audit Merge'; type = 'main'; index = 1 }
    ) }
    'Campaign Audit Merge' = Main-To 'Validate Both Campaigns'
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
