param(
    [switch]$Commit
)

$ErrorActionPreference = 'Stop'
$serviceId = '69eae010c5278d4159c1f664'
$environmentId = '69856f0c86311f632dc2c2c9'
$serviceBase = 'https://kol-auto.zeabur.app/'
$n8nBase = ($env:N8N_BASE_URL ?? '').TrimEnd('/')
if (-not $n8nBase) { throw 'N8N_BASE_URL is required' }
if ($n8nBase -notmatch '/api/v1$') { $n8nBase += '/api/v1' }
if (-not $env:N8N_API_KEY) { throw 'N8N_API_KEY is required' }
if (-not $env:ZEABUR_API_KEY) { throw 'ZEABUR_API_KEY is required' }

$n8nHeaders = @{ 'X-N8N-API-KEY' = $env:N8N_API_KEY }
$zeaburHeaders = @{ Authorization = "Bearer $env:ZEABUR_API_KEY" }

function Invoke-N8n([string]$Method, [string]$Path, $Body = $null) {
    $params = @{
        Method = $Method
        Uri = "$n8nBase$Path"
        Headers = $n8nHeaders
        ContentType = 'application/json'
    }
    if ($null -ne $Body) {
        $params.Body = $Body | ConvertTo-Json -Depth 40 -Compress
    }
    return Invoke-RestMethod @params
}

function Invoke-Zeabur([string]$Query, [hashtable]$Variables) {
    $body = @{ query = $Query; variables = $Variables } | ConvertTo-Json -Depth 10 -Compress
    $response = Invoke-RestMethod -Uri 'https://api.zeabur.com/graphql' -Method Post `
        -Headers $zeaburHeaders -ContentType 'application/json' -Body $body
    if ($response.errors) {
        throw (($response.errors | ForEach-Object { $_.message }) -join '; ')
    }
    return $response.data
}

function Copy-Json($Value) {
    return ($Value | ConvertTo-Json -Depth 40 -Compress | ConvertFrom-Json)
}

function Workflow-Payload($Workflow) {
    return [ordered]@{
        name = $Workflow.name
        nodes = $Workflow.nodes
        connections = $Workflow.connections
        settings = $Workflow.settings
    }
}

function Target-Headers($Workflow) {
    $headers = @()
    foreach ($node in @($Workflow.nodes)) {
        if ("$($node.parameters.url)" -notlike "$serviceBase*") { continue }
        foreach ($header in @($node.parameters.headerParameters.parameters)) {
            if ($header.name -eq 'Authorization' -and "$($header.value)" -like 'Bearer *') {
                $headers += $header
            }
        }
    }
    return $headers
}

function Restore-Workflow($Original) {
    $current = Invoke-N8n GET "/workflows/$($Original.id)"
    if ($current.active) {
        [void](Invoke-N8n POST "/workflows/$($Original.id)/deactivate" @{})
    }
    [void](Invoke-N8n PUT "/workflows/$($Original.id)" (Workflow-Payload $Original))
    if ($Original.active) {
        [void](Invoke-N8n POST "/workflows/$($Original.id)/activate" @{})
    }
}

$all = Invoke-N8n GET '/workflows?limit=250'
$targets = @($all.data | Where-Object { (Target-Headers $_).Count -gt 0 })
$targetNodeCount = @($targets | ForEach-Object { (Target-Headers $_).Count } | Measure-Object -Sum).Sum

if (-not $Commit) {
    [pscustomobject]@{
        mode = 'dry-run'
        workflow_count = $targets.Count
        node_count = $targetNodeCount
        active_workflows = @($targets | Where-Object active).Count
        inactive_workflows = @($targets | Where-Object { -not $_.active }).Count
        secrets_printed = 0
    } | ConvertTo-Json -Compress
    exit 0
}

$bytes = New-Object byte[] 32
[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
$newToken = 'kol_auto_' + [Convert]::ToHexString($bytes).ToLowerInvariant()

$envQuery = 'query($serviceID:ObjectID!,$environmentID:ObjectID!){service(_id:$serviceID){variables(environmentID:$environmentID){key value}}}'
$vars = @{ serviceID = $serviceId; environmentID = $environmentId }
$beforeEnv = (Invoke-Zeabur $envQuery $vars).service.variables
$oldToken = ($beforeEnv | Where-Object key -eq 'INTERNAL_TOKEN' | Select-Object -First 1).value
if (-not $oldToken) { throw 'INTERNAL_TOKEN missing before rotation' }

$updated = New-Object System.Collections.Generic.List[object]
try {
    foreach ($summary in $targets) {
        $original = Invoke-N8n GET "/workflows/$($summary.id)"
        $desired = Copy-Json $original
        foreach ($header in @(Target-Headers $desired)) {
            $header.value = "Bearer $newToken"
        }
        if ($original.active) {
            [void](Invoke-N8n POST "/workflows/$($original.id)/deactivate" @{})
        }
        try {
            [void](Invoke-N8n PUT "/workflows/$($original.id)" (Workflow-Payload $desired))
            if ($original.active) {
                [void](Invoke-N8n POST "/workflows/$($original.id)/activate" @{})
            }
            $readback = Invoke-N8n GET "/workflows/$($original.id)"
            $readbackHeaders = @(Target-Headers $readback)
            if (-not $readbackHeaders.Count -or @($readbackHeaders | Where-Object value -ne "Bearer $newToken").Count) {
                throw "Authorization readback mismatch for workflow $($original.id)"
            }
            if ([bool]$readback.active -ne [bool]$original.active) {
                throw "Active-state drift for workflow $($original.id)"
            }
            $updated.Add($original)
        } catch {
            Restore-Workflow $original
            throw
        }
    }

    $updateMutation = 'mutation($serviceID:ObjectID!,$environmentID:ObjectID!,$oldKey:String!,$newKey:String!,$value:String!){updateSingleEnvironmentVariable(serviceID:$serviceID,environmentID:$environmentID,oldKey:$oldKey,newKey:$newKey,value:$value){key}}'
    [void](Invoke-Zeabur $updateMutation @{
        serviceID=$serviceId; environmentID=$environmentId;
        oldKey='INTERNAL_TOKEN'; newKey='INTERNAL_TOKEN'; value=$newToken
    })
    $afterEnv = (Invoke-Zeabur $envQuery $vars).service.variables
    $actualToken = ($afterEnv | Where-Object key -eq 'INTERNAL_TOKEN' | Select-Object -First 1).value
    if ($actualToken -ne $newToken) { throw 'INTERNAL_TOKEN readback mismatch' }

    $redeployMutation = 'mutation($serviceID:ObjectID!,$environmentID:ObjectID!){redeployService(serviceID:$serviceID,environmentID:$environmentID)}'
    [void](Invoke-Zeabur $redeployMutation $vars)
} catch {
    try {
        if ($oldToken) {
            $updateMutation = 'mutation($serviceID:ObjectID!,$environmentID:ObjectID!,$oldKey:String!,$newKey:String!,$value:String!){updateSingleEnvironmentVariable(serviceID:$serviceID,environmentID:$environmentID,oldKey:$oldKey,newKey:$newKey,value:$value){key}}'
            [void](Invoke-Zeabur $updateMutation @{
                serviceID=$serviceId; environmentID=$environmentId;
                oldKey='INTERNAL_TOKEN'; newKey='INTERNAL_TOKEN'; value=$oldToken
            })
        }
        foreach ($original in @($updated | Select-Object -Reverse)) {
            Restore-Workflow $original
        }
    } catch {
        throw 'Token rotation failed and rollback also failed; inspect production immediately.'
    }
    throw
}

[pscustomobject]@{
    mode = 'commit'
    workflow_count = $targets.Count
    node_count = $targetNodeCount
    active_workflows_preserved = @($targets | Where-Object active).Count
    inactive_workflows_preserved = @($targets | Where-Object { -not $_.active }).Count
    env_key = 'INTERNAL_TOKEN'
    redeploy_requested = $true
    secrets_printed = 0
} | ConvertTo-Json -Compress
