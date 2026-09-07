param(
    [switch]$Commit
)

$ErrorActionPreference = 'Stop'
$baseUrl = $env:N8N_BASE_URL
$apiKey = $env:N8N_API_KEY
if (-not $baseUrl -or -not $apiKey) {
    throw 'N8N_BASE_URL and N8N_API_KEY are required'
}
$baseUrl = $baseUrl.TrimEnd('/')
if (-not $baseUrl.EndsWith('/api/v1')) {
    $baseUrl = "$baseUrl/api/v1"
}
$headers = @{ 'X-N8N-API-KEY' = $apiKey; 'Content-Type' = 'application/json' }
$replyWorkflowId = 'Kai5Xn79SBg6gUKe'
$cleanupWorkflowId = 'ugM1hX94RrzDWmhj'

function Invoke-N8n {
    param([string]$Method, [string]$Path, $Body = $null)
    $params = @{ Uri = "$baseUrl$Path"; Headers = $headers; Method = $Method }
    if ($null -ne $Body) {
        $params.Body = $Body | ConvertTo-Json -Depth 100 -Compress
    }
    Invoke-RestMethod @params
}

function Get-Workflow {
    param([string]$WorkflowId)
    Invoke-N8n -Method Get -Path "/workflows/$WorkflowId"
}

function Copy-JsonObject {
    param($Value)
    $Value | ConvertTo-Json -Depth 100 | ConvertFrom-Json
}

function New-Connection {
    param([string]$Target)
    @"
{"main":[[{"node":"$Target","type":"main","index":0}]]}
"@ | ConvertFrom-Json
}

function Set-Connection {
    param($Connections, [string]$Source, [string]$Target)
    $value = New-Connection -Target $Target
    if ($Connections.PSObject.Properties.Name -contains $Source) {
        $Connections.$Source = $value
    } else {
        $Connections | Add-Member -NotePropertyName $Source -NotePropertyValue $value
    }
}

function New-KolAuthHeaders {
    [pscustomobject]@{
        parameters = @(
            [pscustomobject]@{
                name = 'Authorization'
                value = '={{"Bearer " + $env.KOL_AUTOMATION_API_TOKEN}}'
            }
        )
    }
}

function Set-HttpTimeout {
    param($Parameters, [int]$Milliseconds)
    if (-not $Parameters.options) {
        $Parameters | Add-Member -NotePropertyName options -NotePropertyValue ([pscustomobject]@{})
    }
    if ($Parameters.options.PSObject.Properties.Name -contains 'timeout') {
        $Parameters.options.timeout = $Milliseconds
    } else {
        $Parameters.options | Add-Member -NotePropertyName timeout -NotePropertyValue $Milliseconds
    }
}

function Update-WorkflowSafely {
    param($Workflow, [string]$Label)
    $wasActive = [bool]$Workflow.active
    $body = @{
        name = $Workflow.name
        nodes = $Workflow.nodes
        connections = $Workflow.connections
        settings = $Workflow.settings
    }
    if (-not $Commit) {
        Write-Output "DRY-RUN $Label id=$($Workflow.id) nodes=$($Workflow.nodes.Count) active=$wasActive"
        return
    }

    if ($wasActive) {
        Invoke-N8n -Method Post -Path "/workflows/$($Workflow.id)/deactivate" | Out-Null
    }
    try {
        Invoke-N8n -Method Put -Path "/workflows/$($Workflow.id)" -Body $body | Out-Null
    } finally {
        if ($wasActive) {
            Invoke-N8n -Method Post -Path "/workflows/$($Workflow.id)/activate" | Out-Null
        }
    }
    $verified = Get-Workflow -WorkflowId $Workflow.id
    Write-Output "UPDATED $Label id=$($Workflow.id) nodes=$($verified.nodes.Count) active=$($verified.active)"
}

function Patch-ReplyMonitorWorkflow {
    $workflow = Get-Workflow -WorkflowId $replyWorkflowId
    $call = $workflow.nodes | Where-Object name -eq 'Call Reply Monitor' | Select-Object -First 1
    if (-not $call) {
        throw 'Reply Monitor workflow shape changed: Call Reply Monitor missing'
    }
    $call.parameters.url = 'https://kol-auto.zeabur.app/reply-monitor/run?async_mode=true'
    $call.parameters.headerParameters = New-KolAuthHeaders
    Set-HttpTimeout -Parameters $call.parameters -Milliseconds 30000

    $wait = [pscustomobject]@{
        id = 'wait-reply-monitor-p0'
        name = 'Wait Reply Monitor'
        type = 'n8n-nodes-base.wait'
        typeVersion = 1.1
        position = @(680, 300)
        parameters = @{ resume = 'timeInterval'; amount = 1; unit = 'minutes' }
    }
    $replyStatusParams = Copy-JsonObject $call.parameters
    $replyStatusParams.method = 'GET'
    $replyStatusParams.url = '={{ ''https://kol-auto.zeabur.app/reply-monitor/jobs/'' + $(''Call Reply Monitor'').first().json.job_id }}'
    $replyStatusParams.headerParameters = New-KolAuthHeaders
    Set-HttpTimeout -Parameters $replyStatusParams -Milliseconds 30000
    $status = [pscustomobject]@{
        id = 'get-reply-monitor-status-p0'
        name = 'Get Reply Monitor Status'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(900, 300)
        parameters = $replyStatusParams
    }
    $deadline = [pscustomobject]@{
        id = 'check-reply-monitor-deadline-p0'
        name = 'Check Reply Monitor Deadline'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(1080, 300)
        parameters = @{ jsCode = @'
const job = $input.first().json || {};
const startedMs = Date.parse(String(job.started_at || ''));
if (!Number.isFinite(startedMs)) {
  throw new Error('[REPLY_MONITOR_TIMEOUT] job started_at is missing or invalid');
}
const elapsedMs = Date.now() - startedMs;
if (job.status === 'running' && elapsedMs > 12 * 60 * 1000) {
  throw new Error(`[REPLY_MONITOR_TIMEOUT] job still running after ${Math.round(elapsedMs / 60000)} minutes`);
}
return $input.all();
'@ }
    }
    $finished = [pscustomobject]@{
        id = 'reply-monitor-finished-p0'
        name = 'Reply Monitor Finished?'
        type = 'n8n-nodes-base.if'
        typeVersion = 2.2
        position = @(1280, 300)
        parameters = @{
            conditions = @{
                options = @{ version = 2; leftValue = ''; caseSensitive = $true; typeValidation = 'strict' }
                combinator = 'and'
                conditions = @(@{
                    id = 'reply-monitor-status-terminal'
                    leftValue = '={{ $json.status }}'
                    rightValue = 'running'
                    operator = @{ type = 'string'; operation = 'notEquals' }
                })
            }
            options = @{}
        }
    }
    $assert = [pscustomobject]@{
        id = 'require-reply-monitor-success-p0'
        name = 'Require Reply Monitor Success'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(1500, 220)
        parameters = @{ jsCode = @'
const job = $input.first().json || {};
if (job.status !== 'success' || !job.result || job.result.ok === false) {
  throw new Error(`[REPLY_MONITOR_JOB] background job ended with status=${job.status}; error=${job.error || 'unknown'}`);
}
return $input.all();
'@ }
    }

    $managed = @('Wait Reply Monitor', 'Get Reply Monitor Status', 'Check Reply Monitor Deadline', 'Reply Monitor Finished?', 'Require Reply Monitor Success')
    $workflow.nodes = @($workflow.nodes | Where-Object { $_.name -notin $managed }) + @($wait, $status, $deadline, $finished, $assert)
    Set-Connection $workflow.connections 'Call Reply Monitor' 'Wait Reply Monitor'
    Set-Connection $workflow.connections 'Wait Reply Monitor' 'Get Reply Monitor Status'
    Set-Connection $workflow.connections 'Get Reply Monitor Status' 'Check Reply Monitor Deadline'
    Set-Connection $workflow.connections 'Check Reply Monitor Deadline' 'Reply Monitor Finished?'
    $replyBranches = @'
{"main":[[{"node":"Require Reply Monitor Success","type":"main","index":0}],[{"node":"Wait Reply Monitor","type":"main","index":0}]]}
'@ | ConvertFrom-Json
    $workflow.connections | Add-Member -Force -NotePropertyName 'Reply Monitor Finished?' -NotePropertyValue $replyBranches

    Update-WorkflowSafely -Workflow $workflow -Label 'Reply Monitor async polling'
}

function Patch-DraftCleanupWorkflow {
    $workflow = Get-Workflow -WorkflowId $cleanupWorkflowId
    $call = $workflow.nodes | Where-Object name -eq 'Call Draft Cleanup' | Select-Object -First 1
    $status = $workflow.nodes | Where-Object name -eq 'Get Cleanup Status' | Select-Object -First 1
    if (-not $call -or -not $status) {
        throw 'Draft Cleanup workflow shape changed: required nodes missing'
    }
    $call.parameters.headerParameters = New-KolAuthHeaders
    $status.parameters.headerParameters = New-KolAuthHeaders
    Set-HttpTimeout -Parameters $call.parameters -Milliseconds 30000
    Set-HttpTimeout -Parameters $status.parameters -Milliseconds 30000

    Update-WorkflowSafely -Workflow $workflow -Label 'Draft Cleanup shared environment auth'
}

Patch-ReplyMonitorWorkflow
Patch-DraftCleanupWorkflow
