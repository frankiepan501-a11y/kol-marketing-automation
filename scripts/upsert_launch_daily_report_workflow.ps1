param(
    [string]$N8nBase = $env:N8N_BASE_URL,
    [string]$ApiKey = $env:N8N_API_KEY,
    [string]$ServiceBase = 'https://kol-auto.zeabur.app',
    [string]$AuthSourceWorkflowId = 'VWTMNkXf0zcs4Kvz',
    [string]$WorkflowId = '3GDllutHPUNPEDHs',
    [string]$WorkflowName = 'KOL Launch - Daily Report (17:15 BJ)',
    [string]$Cron = '15 17 * * *'
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'N8N_API_KEY is required.'
}
if ([string]::IsNullOrWhiteSpace($N8nBase)) {
    throw 'N8N_BASE_URL is required.'
}

$apiBase = $N8nBase.TrimEnd('/')
if (-not $apiBase.EndsWith('/api/v1')) {
    $apiBase = "$apiBase/api/v1"
}
$headers = @{ 'X-N8N-API-KEY' = $ApiKey }

function Invoke-N8n {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('GET', 'POST', 'PUT', 'DELETE')][string]$Method,
        [Parameter(Mandatory = $true)][string]$Path,
        [object]$Body = $null
    )
    $params = @{
        Method = $Method
        Uri = "$apiBase$Path"
        Headers = $headers
        ContentType = 'application/json'
    }
    if ($null -ne $Body) {
        $params.Body = $Body | ConvertTo-Json -Depth 40 -Compress
    }
    return Invoke-RestMethod @params
}

function Node-Id {
    param([hashtable]$Map, [string]$Name)
    if ($Map.ContainsKey($Name)) { return $Map[$Name] }
    return [guid]::NewGuid().ToString()
}

function Main-To {
    param([string]$NodeName)
    return @{ main = ,@(@{ node = $NodeName; type = 'main'; index = 0 }) }
}

function Copy-Hashtable {
    param([Parameter(Mandatory = $true)][object]$Value)
    return $Value | ConvertTo-Json -Depth 40 | ConvertFrom-Json -AsHashtable
}

function Get-AllWorkflows {
    $result = @()
    $cursor = $null
    do {
        $path = '/workflows?limit=100'
        if ($cursor) {
            $path += '&cursor=' + [uri]::EscapeDataString($cursor)
        }
        $page = Invoke-N8n -Method GET -Path $path
        $result += @($page.data)
        $cursor = $page.nextCursor
    } while ($cursor)
    return $result
}

function Try-GetWorkflow {
    param([string]$Id)
    if ([string]::IsNullOrWhiteSpace($Id)) { return $null }
    try {
        return Invoke-N8n -Method GET -Path "/workflows/$Id"
    } catch {
        $statusCode = $null
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        if ($statusCode -eq 404) { return $null }
        throw
    }
}

function Get-InboundSources {
    param(
        [Parameter(Mandatory = $true)][object]$Connections,
        [Parameter(Mandatory = $true)][string]$TargetName
    )
    $connectionMap = if ($Connections -is [hashtable]) { $Connections } else { Copy-Hashtable -Value $Connections }
    $sources = @()
    foreach ($sourceName in $connectionMap.Keys) {
        foreach ($outputGroup in @($connectionMap[$sourceName].main)) {
            foreach ($edge in @($outputGroup)) {
                if ([string]$edge.node -eq $TargetName) { $sources += [string]$sourceName }
            }
        }
    }
    return @($sources | Sort-Object)
}

function Get-OutboundTargets {
    param(
        [Parameter(Mandatory = $true)][object]$Connections,
        [Parameter(Mandatory = $true)][string]$SourceName
    )
    $connectionMap = if ($Connections -is [hashtable]) { $Connections } else { Copy-Hashtable -Value $Connections }
    if (-not $connectionMap.ContainsKey($SourceName)) { return @() }
    $targets = @()
    foreach ($outputGroup in @($connectionMap[$SourceName].main)) {
        foreach ($edge in @($outputGroup)) {
            $targets += [string]$edge.node
        }
    }
    return @($targets | Sort-Object)
}

function Assert-NoTriggerDrift {
    param(
        [Parameter(Mandatory = $true)][object[]]$Nodes,
        [Parameter(Mandatory = $true)][object]$Connections
    )
    $triggerNodes = @($Nodes | Where-Object { [string]$_.type -match '(?i)(trigger|webhook)' })
    if ($triggerNodes.Count -ne 1 -or $triggerNodes[0].name -ne 'Daily 17:15 BJ' -or $triggerNodes[0].type -ne 'n8n-nodes-base.scheduleTrigger') {
        throw 'Refusing to continue: the daily report workflow must contain exactly one trigger, Daily 17:15 BJ.'
    }
    $expectedInbound = @{
        'Daily 17:15 BJ' = @()
        'Start Group Daily Report' = @('Daily 17:15 BJ')
        'Wait 12 Minutes' = @('Start Group Daily Report')
        'Read Group Report Job' = @('Wait 12 Minutes')
        'Verify Group Report Result' = @('Read Group Report Job')
    }
    $expectedOutbound = @{
        'Daily 17:15 BJ' = @('Start Group Daily Report')
        'Start Group Daily Report' = @('Wait 12 Minutes')
        'Wait 12 Minutes' = @('Read Group Report Job')
        'Read Group Report Job' = @('Verify Group Report Result')
        'Verify Group Report Result' = @()
    }
    foreach ($targetName in $expectedInbound.Keys) {
        $actual = @(Get-InboundSources -Connections $Connections -TargetName $targetName)
        $expected = @($expectedInbound[$targetName] | Sort-Object)
        if (($actual -join '|') -ne ($expected -join '|')) {
            throw "Refusing to continue: unexpected inbound connection(s) for '$targetName': $($actual -join ', ')."
        }
    }
    foreach ($sourceName in $expectedOutbound.Keys) {
        $actual = @(Get-OutboundTargets -Connections $Connections -SourceName $sourceName)
        $expected = @($expectedOutbound[$sourceName] | Sort-Object)
        if (($actual -join '|') -ne ($expected -join '|')) {
            throw "Refusing to continue: unexpected outbound connection(s) for '$sourceName': $($actual -join ', ')."
        }
    }
}

$serviceUri = [uri]$ServiceBase
$source = Invoke-N8n -Method GET -Path "/workflows/$AuthSourceWorkflowId"
$authValues = @()
foreach ($node in @($source.nodes)) {
    if ($node.type -ne 'n8n-nodes-base.httpRequest') { continue }
    $nodeUrl = [string]$node.parameters.url
    $nodeUri = $null
    if (-not [uri]::TryCreate($nodeUrl, [System.UriKind]::Absolute, [ref]$nodeUri)) { continue }
    if ($nodeUri.Scheme -ne $serviceUri.Scheme -or $nodeUri.Host -ne $serviceUri.Host) { continue }
    foreach ($header in @($node.parameters.headerParameters.parameters)) {
        if ([string]$header.name -ieq 'Authorization' -and -not [string]::IsNullOrWhiteSpace([string]$header.value)) {
            $authValues += [string]$header.value
        }
    }
}
$uniqueAuthValues = @($authValues | Sort-Object -Unique)
if ($uniqueAuthValues.Count -ne 1) {
    throw "Expected exactly one unique kol-auto Authorization value in workflow $AuthSourceWorkflowId; found $($uniqueAuthValues.Count)."
}
$serviceAuthHeaders = @{ parameters = @(@{ name = 'Authorization'; value = $uniqueAuthValues[0] }) }

$allWorkflows = @(Get-AllWorkflows)
$nameMatches = @($allWorkflows | Where-Object { $_.name -eq $WorkflowName })
if ($nameMatches.Count -gt 1) {
    throw "Refusing to continue: found $($nameMatches.Count) workflows named '$WorkflowName'."
}
$existing = $null
if (-not [string]::IsNullOrWhiteSpace($WorkflowId)) {
    $existing = Try-GetWorkflow -Id $WorkflowId
    if (-not $existing) {
        throw "Configured workflow ID $WorkflowId was not found. Pass -WorkflowId '' only for an explicit first-time creation."
    }
    if ($existing.name -ne $WorkflowName) {
        throw "Workflow ID $WorkflowId belongs to '$($existing.name)', not '$WorkflowName'."
    }
    if ($nameMatches.Count -ne 1 -or $nameMatches[0].id -ne $existing.id) {
        throw "Refusing to continue: workflow ID $WorkflowId and the unique name match do not point to the same workflow."
    }
} elseif ($nameMatches.Count -eq 1) {
    $existing = Invoke-N8n -Method GET -Path "/workflows/$($nameMatches[0].id)"
}

$nodeIds = @{}
if ($existing) {
    $current = $existing
    foreach ($node in @($current.nodes)) {
        if ($nodeIds.ContainsKey($node.name)) {
            throw "Refusing to continue: workflow contains duplicate node name '$($node.name)'."
        }
        $nodeIds[$node.name] = $node.id
    }
    Assert-NoTriggerDrift -Nodes @($current.nodes) -Connections $current.connections
}

$validateGroupReport = @'
const data = $input.first().json;
if (data.status !== 'success') {
  throw new Error('Daily group report background job did not finish successfully: ' + JSON.stringify(data).slice(0, 1200));
}
const result = data.result || {};
const validation = result.validation || {};
const messageIds = Array.isArray(result.message_ids) ? result.message_ids : [];
if (result.ok !== true || validation.ok !== true) {
  throw new Error('Daily group report card validation failed: ' + JSON.stringify(data).slice(0, 1200));
}
if (result.notified !== true || result.frankie_only !== false || messageIds.length !== 1) {
  throw new Error('Daily group report was not delivered exactly once to the operations group: ' + JSON.stringify(data).slice(0, 1200));
}
if (Number(result.business_writes || 0) !== 0) {
  throw new Error('Daily group report unexpectedly changed business data: ' + JSON.stringify(data).slice(0, 1200));
}
return [{json: {
  status: 'daily_group_report_verified',
  job_id: data.job_id,
  message_id: messageIds[0],
  day: result.day,
  campaigns: result.campaigns,
  deduplicated: Boolean(result.deduplicated),
  business_writes: Number(result.business_writes || 0),
  operational_receipt_writes: Number(result.operational_receipt_writes || 0),
}}];
'@

$managedNodes = @(
    @{
        id = Node-Id $nodeIds 'Daily 17:15 BJ'
        name = 'Daily 17:15 BJ'
        type = 'n8n-nodes-base.scheduleTrigger'
        typeVersion = 1.1
        position = @(0, 200)
        parameters = @{ rule = @{ interval = @(@{ field = 'cronExpression'; expression = $Cron }) } }
    },
    @{
        id = Node-Id $nodeIds 'Start Group Daily Report'
        name = 'Start Group Daily Report'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(260, 200)
        parameters = @{
            method = 'POST'
            url = "$ServiceBase/launch/daily-report/run"
            sendQuery = $true
            queryParameters = @{ parameters = @(
                @{ name = 'notify'; value = 'true' },
                @{ name = 'frankie_only'; value = 'false' },
                @{ name = 'async_mode'; value = 'true' }
            ) }
            sendHeaders = $true
            headerParameters = $serviceAuthHeaders
            options = @{ timeout = 30000 }
        }
    },
    @{
        id = Node-Id $nodeIds 'Wait 12 Minutes'
        name = 'Wait 12 Minutes'
        type = 'n8n-nodes-base.wait'
        typeVersion = 1.1
        position = @(520, 200)
        parameters = @{ resume = 'timeInterval'; amount = 12; unit = 'minutes' }
    },
    @{
        id = Node-Id $nodeIds 'Read Group Report Job'
        name = 'Read Group Report Job'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(780, 200)
        parameters = @{
            method = 'GET'
            url = "={{ '$ServiceBase/launch/daily-report/jobs/' + `$json.job_id }}"
            sendHeaders = $true
            headerParameters = $serviceAuthHeaders
            options = @{ timeout = 30000 }
        }
    },
    @{
        id = Node-Id $nodeIds 'Verify Group Report Result'
        name = 'Verify Group Report Result'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(1040, 200)
        parameters = @{ jsCode = $validateGroupReport }
    }
)

$managedConnections = @{
    'Daily 17:15 BJ' = Main-To 'Start Group Daily Report'
    'Start Group Daily Report' = Main-To 'Wait 12 Minutes'
    'Wait 12 Minutes' = Main-To 'Read Group Report Job'
    'Read Group Report Job' = Main-To 'Verify Group Report Result'
}
$managedNodeNames = @($managedNodes | ForEach-Object { $_.name })
if ($existing) {
    $nodes = @($current.nodes | Where-Object { $_.name -notin $managedNodeNames }) + $managedNodes
    $connections = Copy-Hashtable -Value $current.connections
    foreach ($name in $managedConnections.Keys) {
        $connections[$name] = $managedConnections[$name]
    }
    $settings = Copy-Hashtable -Value $current.settings
} else {
    $nodes = $managedNodes
    $connections = $managedConnections
    $settings = @{}
}
$settings.availableInMCP = $false
$settings.executionOrder = 'v1'
$settings.timezone = 'Asia/Shanghai'
$settings.callerPolicy = 'workflowsFromSameOwner'
$settings.saveDataErrorExecution = 'all'
$settings.saveDataSuccessExecution = 'all'

$payload = @{ name = $WorkflowName; nodes = $nodes; connections = $connections; settings = $settings }
Assert-NoTriggerDrift -Nodes @($nodes) -Connections $connections

function Assert-DailyReportReadback {
    param([Parameter(Mandatory = $true)][object]$Readback)

    $schedule = $Readback.nodes | Where-Object { $_.name -eq 'Daily 17:15 BJ' } | Select-Object -First 1
    $start = $Readback.nodes | Where-Object { $_.name -eq 'Start Group Daily Report' } | Select-Object -First 1
    $wait = $Readback.nodes | Where-Object { $_.name -eq 'Wait 12 Minutes' } | Select-Object -First 1
    $verify = $Readback.nodes | Where-Object { $_.name -eq 'Verify Group Report Result' } | Select-Object -First 1

    if (-not $Readback.active) { throw 'Daily report workflow was not activated.' }
    if ($Readback.settings.timezone -ne 'Asia/Shanghai') { throw 'Daily report workflow timezone drifted.' }
    if ($schedule.parameters.rule.interval[0].expression -ne $Cron) { throw 'Daily report cron readback mismatch.' }
    $queryValues = @{}
    foreach ($parameter in @($start.parameters.queryParameters.parameters)) {
        $queryValues[[string]$parameter.name] = [string]$parameter.value
    }
    if ($queryValues.Count -ne 3 -or $queryValues.notify -ne 'true' -or $queryValues.frankie_only -ne 'false' -or $queryValues.async_mode -ne 'true') {
        throw 'Daily report send parameters readback mismatch.'
    }
    if ($wait.parameters.amount -ne 12 -or $wait.parameters.unit -ne 'minutes') { throw 'Daily report status wait readback mismatch.' }
    if (-not $verify.parameters.jsCode.Contains('daily_group_report_verified')) { throw 'Daily report result validator readback mismatch.' }
    Assert-NoTriggerDrift -Nodes @($Readback.nodes) -Connections $Readback.connections
}

if ($existing) {
    $wasActive = [bool]$current.active
    $originalPayload = Copy-Hashtable -Value ([pscustomobject]@{
        name = $current.name
        nodes = $current.nodes
        connections = $current.connections
        settings = $current.settings
    })
    try {
        if ($wasActive) {
            Invoke-N8n -Method POST -Path "/workflows/$($existing.id)/deactivate" -Body @{} | Out-Null
        }
        $workflow = Invoke-N8n -Method PUT -Path "/workflows/$($existing.id)" -Body $payload
        $workflow = Invoke-N8n -Method POST -Path "/workflows/$($workflow.id)/activate" -Body @{}
        $readback = Invoke-N8n -Method GET -Path "/workflows/$($workflow.id)"
        Assert-DailyReportReadback -Readback $readback
    } catch {
        $updateError = $_
        try {
            $afterFailure = Try-GetWorkflow -Id $existing.id
            if ($afterFailure -and $afterFailure.active) {
                Invoke-N8n -Method POST -Path "/workflows/$($existing.id)/deactivate" -Body @{} | Out-Null
            }
            Invoke-N8n -Method PUT -Path "/workflows/$($existing.id)" -Body $originalPayload | Out-Null
            if ($wasActive) {
                Invoke-N8n -Method POST -Path "/workflows/$($existing.id)/activate" -Body @{} | Out-Null
            }
        } catch {
            throw "Workflow update failed and rollback also failed for $($existing.id). Original error: $($updateError.Exception.Message). Rollback error: $($_.Exception.Message)"
        }
        throw $updateError
    }
} else {
    $workflow = $null
    try {
        $workflow = Invoke-N8n -Method POST -Path '/workflows' -Body $payload
        $workflow = Invoke-N8n -Method POST -Path "/workflows/$($workflow.id)/activate" -Body @{}
        $readback = Invoke-N8n -Method GET -Path "/workflows/$($workflow.id)"
        Assert-DailyReportReadback -Readback $readback
    } catch {
        if ($workflow -and $workflow.id) {
            try { Invoke-N8n -Method POST -Path "/workflows/$($workflow.id)/deactivate" -Body @{} | Out-Null } catch {}
            try { Invoke-N8n -Method DELETE -Path "/workflows/$($workflow.id)" | Out-Null } catch {}
        }
        throw
    }
}

$schedule = $readback.nodes | Where-Object { $_.name -eq 'Daily 17:15 BJ' } | Select-Object -First 1
$start = $readback.nodes | Where-Object { $_.name -eq 'Start Group Daily Report' } | Select-Object -First 1
$wait = $readback.nodes | Where-Object { $_.name -eq 'Wait 12 Minutes' } | Select-Object -First 1

[pscustomobject]@{
    id = $readback.id
    name = $readback.name
    active = $readback.active
    node_count = @($readback.nodes).Count
    timezone = $readback.settings.timezone
    cron = $schedule.parameters.rule.interval[0].expression
    target_endpoint = $start.parameters.url
    notify = ($start.parameters.queryParameters.parameters | Where-Object { $_.name -eq 'notify' }).value
    frankie_only = ($start.parameters.queryParameters.parameters | Where-Object { $_.name -eq 'frankie_only' }).value
    async_mode = ($start.parameters.queryParameters.parameters | Where-Object { $_.name -eq 'async_mode' }).value
    status_wait = "$($wait.parameters.amount) $($wait.parameters.unit)"
    result_validation = 'enabled'
} | ConvertTo-Json -Depth 6
