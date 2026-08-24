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
$headers = @{ 'X-N8N-API-KEY' = $apiKey; 'Content-Type' = 'application/json' }
$seoWorkflowId = 'ee779GzBI8Bj4Bx3'
$cleanupWorkflowId = 'ugM1hX94RrzDWmhj'
$guardDir = Join-Path $PSScriptRoot 'n8n'

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

function Patch-SeoWorkflow {
    $workflow = Get-Workflow -WorkflowId $seoWorkflowId
    $deep = $workflow.nodes | Where-Object name -eq 'DeepSeek API'
    $extract = $workflow.nodes |
        Where-Object { $_.name -in @('Extract Response', 'Validate DeepSeek Response') } |
        Select-Object -First 1
    if (-not $deep -or -not $extract) {
        throw 'SEO workflow shape changed: required nodes are missing'
    }

    $balanceParams = Copy-JsonObject $deep.parameters
    $balanceParams.method = 'GET'
    $balanceParams.url = 'https://api.deepseek.com/user/balance'
    foreach ($property in @('sendBody', 'specifyBody', 'jsonBody', 'bodyParameters', 'contentType')) {
        $balanceParams.PSObject.Properties.Remove($property)
    }
    $balance = [pscustomobject]@{
        id = 'deepseek-balance-preflight-p1'
        name = 'DeepSeek Balance Preflight'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(560, 120)
        parameters = $balanceParams
    }
    $balanceGuard = [pscustomobject]@{
        id = 'require-deepseek-balance-p1'
        name = 'Require DeepSeek Balance'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(760, 120)
        parameters = @{ jsCode = Get-Content (Join-Path $guardDir 'seo_balance_guard.js') -Raw }
    }

    $managed = @('DeepSeek Balance Preflight', 'Require DeepSeek Balance')
    $workflow.nodes = @($workflow.nodes | Where-Object { $_.name -notin $managed }) + @($balance, $balanceGuard)

    $extract.name = 'Validate DeepSeek Response'
    $extract.type = 'n8n-nodes-base.code'
    $extract.typeVersion = 2
    $extract.parameters = @{ jsCode = Get-Content (Join-Path $guardDir 'seo_response_guard.js') -Raw }

    $qaTemplate = Get-Content (Join-Path $guardDir 'seo_deterministic_qa.js') -Raw
    $qaPk = $workflow.nodes | Where-Object name -eq 'QA Score PK'
    $qaFl = $workflow.nodes | Where-Object name -eq 'QA Score FL'
    $qaPk.parameters = @{ jsCode = $qaTemplate.Replace('__ARTICLE_KEY__', 'article1').Replace('__SITE__', 'powkong') }
    $qaFl.parameters = @{ jsCode = $qaTemplate.Replace('__ARTICLE_KEY__', 'article2').Replace('__SITE__', 'funlab') }

    Set-Connection $workflow.connections 'Build 2-Article Prompt' 'DeepSeek Balance Preflight'
    Set-Connection $workflow.connections 'DeepSeek Balance Preflight' 'Require DeepSeek Balance'
    Set-Connection $workflow.connections 'Require DeepSeek Balance' 'DeepSeek API'
    Set-Connection $workflow.connections 'DeepSeek API' 'Validate DeepSeek Response'
    $workflow.connections.PSObject.Properties.Remove('Extract Response')
    Set-Connection $workflow.connections 'Validate DeepSeek Response' 'Parse Both Articles'

    Update-WorkflowSafely -Workflow $workflow -Label 'SEO DeepSeek cost gates'
}

function Patch-CleanupWorkflow {
    $workflow = Get-Workflow -WorkflowId $cleanupWorkflowId
    $call = $workflow.nodes | Where-Object name -eq 'Call Draft Cleanup'
    if (-not $call) {
        throw 'Cleanup workflow shape changed: Call Draft Cleanup missing'
    }
    $call.parameters.url = 'https://kol-auto.zeabur.app/draft-cleanup/run?days=30&dry_run=false'
    if (-not $call.parameters.options) {
        $call.parameters | Add-Member -NotePropertyName options -NotePropertyValue ([pscustomobject]@{})
    }
    if ($call.parameters.options.PSObject.Properties.Name -contains 'timeout') {
        $call.parameters.options.timeout = 30000
    } else {
        $call.parameters.options | Add-Member -NotePropertyName timeout -NotePropertyValue 30000
    }

    $wait = [pscustomobject]@{
        id = 'wait-draft-cleanup-p1'
        name = 'Wait Cleanup'
        type = 'n8n-nodes-base.wait'
        typeVersion = 1.1
        position = @(680, 300)
        parameters = @{ resume = 'timeInterval'; amount = 1; unit = 'minutes' }
    }
    $statusParams = Copy-JsonObject $call.parameters
    $statusParams.method = 'GET'
    $statusParams.url = '={{ ''https://kol-auto.zeabur.app/draft-cleanup/jobs/'' + $(''Call Draft Cleanup'').first().json.job_id }}'
    $status = [pscustomobject]@{
        id = 'get-draft-cleanup-status-p1'
        name = 'Get Cleanup Status'
        type = 'n8n-nodes-base.httpRequest'
        typeVersion = 4.2
        position = @(900, 300)
        parameters = $statusParams
    }
    $deadline = [pscustomobject]@{
        id = 'check-draft-cleanup-deadline-p1'
        name = 'Check Cleanup Deadline'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(1080, 300)
        parameters = @{ jsCode = @'
const job = $input.first().json || {};
const startedMs = Date.parse(String(job.started_at || ''));
if (!Number.isFinite(startedMs)) {
  throw new Error('[DRAFT_CLEANUP_TIMEOUT] job started_at is missing or invalid');
}
const elapsedMs = Date.now() - startedMs;
if (job.status === 'running' && elapsedMs > 2 * 60 * 60 * 1000) {
  throw new Error(`[DRAFT_CLEANUP_TIMEOUT] job still running after ${Math.round(elapsedMs / 60000)} minutes`);
}
return $input.all();
'@ }
    }
    $finished = [pscustomobject]@{
        id = 'draft-cleanup-finished-p1'
        name = 'Cleanup Finished?'
        type = 'n8n-nodes-base.if'
        typeVersion = 2.2
        position = @(1280, 300)
        parameters = @{
            conditions = @{
                options = @{ version = 2; leftValue = ''; caseSensitive = $true; typeValidation = 'strict' }
                combinator = 'and'
                conditions = @(@{
                    id = 'cleanup-status-terminal'
                    leftValue = '={{ $json.status }}'
                    rightValue = 'running'
                    operator = @{ type = 'string'; operation = 'notEquals' }
                })
            }
            options = @{}
        }
    }
    $assert = [pscustomobject]@{
        id = 'require-draft-cleanup-success-p1'
        name = 'Require Cleanup Success'
        type = 'n8n-nodes-base.code'
        typeVersion = 2
        position = @(1500, 220)
        parameters = @{ jsCode = @'
const job = $input.first().json || {};
if (job.status !== 'success') {
  throw new Error(`[DRAFT_CLEANUP_JOB] background job ended with status=${job.status}; error=${job.error || 'unknown'}`);
}
return $input.all();
'@ }
    }

    $managed = @('Wait Cleanup', 'Get Cleanup Status', 'Check Cleanup Deadline', 'Cleanup Finished?', 'Require Cleanup Success')
    $workflow.nodes = @($workflow.nodes | Where-Object { $_.name -notin $managed }) + @($wait, $status, $deadline, $finished, $assert)
    Set-Connection $workflow.connections 'Call Draft Cleanup' 'Wait Cleanup'
    Set-Connection $workflow.connections 'Wait Cleanup' 'Get Cleanup Status'
    Set-Connection $workflow.connections 'Get Cleanup Status' 'Check Cleanup Deadline'
    Set-Connection $workflow.connections 'Check Cleanup Deadline' 'Cleanup Finished?'
    $cleanupBranches = @'
{"main":[[{"node":"Require Cleanup Success","type":"main","index":0}],[{"node":"Wait Cleanup","type":"main","index":0}]]}
'@ | ConvertFrom-Json
    $workflow.connections | Add-Member -Force -NotePropertyName 'Cleanup Finished?' -NotePropertyValue $cleanupBranches

    Update-WorkflowSafely -Workflow $workflow -Label 'Draft cleanup async polling'
}

Patch-SeoWorkflow
Patch-CleanupWorkflow
