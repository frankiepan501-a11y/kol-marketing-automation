param(
    [switch]$Commit,
    [switch]$SelfTest
)

$ErrorActionPreference = 'Stop'
$serviceId = '69eae010c5278d4159c1f664'
$environmentId = '69856f0c86311f632dc2c2c9'
$serviceBase = 'https://kol-auto.zeabur.app/'
$authorizationRegex = [regex]::new(
    '(?is)[''"]?Authorization[''"]?\s*:\s*[''"]Bearer (?<token>[A-Za-z0-9._-]{20,})[''"]'
)
$kolUrlPropertyRegex = [regex]::new(
    '(?is)[''"]?url[''"]?\s*:\s*[''"]https://kol-auto\.zeabur\.app/'
)

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

function ConvertTo-CanonicalJsonElement([System.Text.Json.JsonElement]$Element) {
    switch ($Element.ValueKind) {
        ([System.Text.Json.JsonValueKind]::Object) {
            $parts = foreach ($property in @($Element.EnumerateObject()) | Sort-Object Name) {
                $name = '"' + [System.Text.Json.JsonEncodedText]::Encode([string]$property.Name).ToString() + '"'
                $value = ConvertTo-CanonicalJsonElement $property.Value
                "$name`:$value"
            }
            return '{' + ($parts -join ',') + '}'
        }
        ([System.Text.Json.JsonValueKind]::Array) {
            $parts = foreach ($item in @($Element.EnumerateArray())) {
                ConvertTo-CanonicalJsonElement $item
            }
            return '[' + ($parts -join ',') + ']'
        }
        default { return $Element.GetRawText() }
    }
}

function Value-Hash($Value) {
    $json = $Value | ConvertTo-Json -Depth 40 -Compress
    $document = [System.Text.Json.JsonDocument]::Parse($json)
    try {
        $canonicalJson = ConvertTo-CanonicalJsonElement $document.RootElement
    } finally {
        $document.Dispose()
    }
    $bytes = [Text.Encoding]::UTF8.GetBytes($canonicalJson)
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Token-Fingerprint([string]$Token) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Token)
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).Substring(0, 12).ToLowerInvariant()
}

function Find-JavascriptObjectSpans([string]$Code) {
    $stack = [System.Collections.Generic.Stack[int]]::new()
    $spans = [System.Collections.Generic.List[object]]::new()
    $quote = [char]0
    $escaped = $false
    $lineComment = $false
    $blockComment = $false

    for ($index = 0; $index -lt $Code.Length; $index++) {
        $character = $Code[$index]
        $next = if ($index + 1 -lt $Code.Length) { $Code[$index + 1] } else { [char]0 }

        if ($lineComment) {
            if ($character -eq "`n" -or $character -eq "`r") { $lineComment = $false }
            continue
        }
        if ($blockComment) {
            if ($character -eq '*' -and $next -eq '/') {
                $blockComment = $false
                $index++
            }
            continue
        }
        if ($quote -ne [char]0) {
            if ($escaped) { $escaped = $false; continue }
            if ([int]$character -eq 92) { $escaped = $true; continue }
            if ($character -eq $quote) { $quote = [char]0 }
            continue
        }
        if ($character -eq '/' -and $next -eq '/') {
            $lineComment = $true
            $index++
            continue
        }
        if ($character -eq '/' -and $next -eq '*') {
            $blockComment = $true
            $index++
            continue
        }
        if ($character -eq "'" -or $character -eq '"' -or $character -eq '`') {
            $quote = $character
            continue
        }
        if ($character -eq '{') {
            $stack.Push($index)
            continue
        }
        if ($character -eq '}' -and $stack.Count) {
            $start = $stack.Pop()
            $spans.Add([pscustomobject]@{ start = $start; length = $index - $start + 1 })
        }
    }
    return $spans
}

function Test-JavascriptRequestObjectStart([string]$Code, [int]$Start) {
    $prefixStart = [Math]::Max(0, $Start - 100)
    $prefix = $Code.Substring($prefixStart, $Start - $prefixStart)
    return $prefix -match '(?is)(?:\bHR|this\.helpers\.httpRequest)\s*\(\s*$'
}

function Remove-JavascriptComments([string]$Code) {
    $builder = [Text.StringBuilder]::new($Code.Length)
    $quote = [char]0
    $escaped = $false
    $lineComment = $false
    $blockComment = $false

    for ($index = 0; $index -lt $Code.Length; $index++) {
        $character = $Code[$index]
        $next = if ($index + 1 -lt $Code.Length) { $Code[$index + 1] } else { [char]0 }

        if ($lineComment) {
            if ($character -eq "`n" -or $character -eq "`r") {
                $lineComment = $false
                [void]$builder.Append($character)
            } else {
                [void]$builder.Append(' ')
            }
            continue
        }
        if ($blockComment) {
            if ($character -eq '*' -and $next -eq '/') {
                [void]$builder.Append(' ')
                [void]$builder.Append(' ')
                $blockComment = $false
                $index++
            } else {
                [void]$builder.Append($(if ($character -eq "`n" -or $character -eq "`r") { $character } else { ' ' }))
            }
            continue
        }
        if ($quote -ne [char]0) {
            [void]$builder.Append($character)
            if ($escaped) { $escaped = $false; continue }
            if ([int]$character -eq 92) { $escaped = $true; continue }
            if ($character -eq $quote) { $quote = [char]0 }
            continue
        }
        if ($character -eq '/' -and $next -eq '/') {
            [void]$builder.Append(' ')
            [void]$builder.Append(' ')
            $lineComment = $true
            $index++
            continue
        }
        if ($character -eq '/' -and $next -eq '*') {
            [void]$builder.Append(' ')
            [void]$builder.Append(' ')
            $blockComment = $true
            $index++
            continue
        }
        [void]$builder.Append($character)
        if ($character -eq "'" -or $character -eq '"' -or $character -eq '`') {
            $quote = $character
        }
    }
    return $builder.ToString()
}

function Find-KolCodeReferences([string]$Code) {
    $candidates = @()
    foreach ($span in @(Find-JavascriptObjectSpans $Code)) {
        if (-not (Test-JavascriptRequestObjectStart $Code $span.start)) { continue }
        $segment = $Code.Substring($span.start, $span.length)
        $segmentWithoutComments = Remove-JavascriptComments $segment
        if (-not $kolUrlPropertyRegex.IsMatch($segmentWithoutComments)) { continue }
        $matches = @($authorizationRegex.Matches($segmentWithoutComments))
        if ($matches.Count) {
            $candidates += [pscustomobject]@{
                start = $span.start
                length = $span.length
                matches = $matches
            }
        }
    }

    $minimalCandidates = @($candidates | Where-Object {
        $candidate = $_
        -not @($candidates | Where-Object {
            $_.start -ge $candidate.start -and
            ($_.start + $_.length) -le ($candidate.start + $candidate.length) -and
            ($_.start -ne $candidate.start -or $_.length -ne $candidate.length)
        }).Count
    })

    $references = @()
    foreach ($candidate in $minimalCandidates) {
        foreach ($match in @($candidate.matches)) {
            $references += [pscustomobject]@{
                token = $match.Groups['token'].Value
                token_start = $candidate.start + $match.Groups['token'].Index
                token_length = $match.Groups['token'].Length
            }
        }
    }
    return $references
}

function Target-CodeReferences($Workflow, [string]$ExpectedToken = '') {
    $references = @()
    foreach ($node in @($Workflow.nodes)) {
        $code = "$($node.parameters.jsCode)"
        foreach ($reference in @(Find-KolCodeReferences $code)) {
            if (-not $ExpectedToken -or $reference.token -eq $ExpectedToken) {
                $references += [pscustomobject]@{
                    node = $node
                    token = $reference.token
                    token_start = $reference.token_start
                    token_length = $reference.token_length
                }
            }
        }
    }
    return $references
}

function Update-CodeReferences($Workflow, [string]$OldToken, [string]$NewToken) {
    foreach ($node in @($Workflow.nodes)) {
        $code = "$($node.parameters.jsCode)"
        if (-not $code) { continue }
        $references = @(Find-KolCodeReferences $code | Where-Object token -eq $OldToken | Sort-Object token_start -Descending)
        foreach ($reference in $references) {
            $code = $code.Substring(0, $reference.token_start) + $NewToken + $code.Substring($reference.token_start + $reference.token_length)
        }
        $node.parameters.jsCode = $code
    }
}

function Target-ReferenceCount($Workflow) {
    return @(Target-Headers $Workflow).Count + @(Target-CodeReferences $Workflow).Count
}

function Assert-WorkflowReadback($Original, $Readback, [string]$ExpectedToken, [int]$ExpectedHttpCount, [int]$ExpectedCodeCount) {
    $readbackHeaders = @(Target-Headers $Readback)
    if ($readbackHeaders.Count -ne $ExpectedHttpCount) {
        throw "HTTP Authorization reference count mismatch for workflow $($Original.id)"
    }
    if (@($readbackHeaders | Where-Object value -ne "Bearer $ExpectedToken").Count) {
        throw "HTTP Authorization readback mismatch for workflow $($Original.id)"
    }

    $readbackCodeReferences = @(Target-CodeReferences $Readback)
    if ($readbackCodeReferences.Count -ne $ExpectedCodeCount) {
        throw "Code-node reference count mismatch for workflow $($Original.id)"
    }
    if (@($readbackCodeReferences | Where-Object token -ne $ExpectedToken).Count) {
        throw "Code-node Authorization readback mismatch for workflow $($Original.id)"
    }
    if (@($Readback.nodes).Count -ne @($Original.nodes).Count) {
        throw "Node-count drift for workflow $($Original.id)"
    }
    if ((Value-Hash $Readback.connections) -ne (Value-Hash $Original.connections)) {
        throw "Connection drift for workflow $($Original.id)"
    }
    if ([bool]$Readback.active -ne [bool]$Original.active) {
        throw "Active-state drift for workflow $($Original.id)"
    }
    return $true
}

if ($SelfTest) {
    $oldFixtureToken = 'old_fixture_token_12345678901234567890'
    $newFixtureToken = 'new_fixture_token_12345678901234567890'
    $unrelatedToken = 'unrelated_token_12345678901234567890'
    $fixture = [pscustomobject]@{
        id = 'fixture-http-and-code'
        active = $true
        connections = [pscustomobject]@{ Main = @() }
        nodes = @(
            [pscustomobject]@{
                name = 'HTTP Request fixture'
                parameters = [pscustomobject]@{
                    url = "${serviceBase}health"
                    headerParameters = [pscustomobject]@{
                        parameters = @(
                            [pscustomobject]@{
                                name = 'Authorization'
                                value = "Bearer $oldFixtureToken"
                            }
                        )
                    }
                }
            },
            [pscustomobject]@{
                name = 'Code fixture'
                parameters = [pscustomobject]@{
                    jsCode = "await HR({ headers: { Authorization: 'Bearer $oldFixtureToken' }, url: '${serviceBase}draft/regen' }); const unrelated = { url: 'https://other.example/api', headers: { Authorization: 'Bearer $unrelatedToken' } };"
                }
            },
            [pscustomobject]@{
                name = 'Cross-object fixture'
                parameters = [pscustomobject]@{
                    jsCode = "const kol = { url: '${serviceBase}health' }; const other = { url: 'https://other.example/api', headers: { Authorization: 'Bearer $unrelatedToken' } };"
                }
            }
        )
    }
    $codeOnlyFixture = [pscustomobject]@{
        id = 'fixture-code-only'
        active = $false
        connections = [pscustomobject]@{}
        nodes = @(
            [pscustomobject]@{
                name = 'Code-only fixture'
                parameters = [pscustomobject]@{
                    jsCode = "await this.helpers.httpRequest({ url: '${serviceBase}cs/callback', headers: { Authorization: 'Bearer $oldFixtureToken' } });"
                }
            }
        )
    }
    $blockCrossObjectFixture = [pscustomobject]@{
        id = 'fixture-block-cross-object'
        active = $false
        connections = [pscustomobject]@{}
        nodes = @(
            [pscustomobject]@{
                name = 'Block cross-object fixture'
                parameters = [pscustomobject]@{
                    jsCode = "if (true) { const kol = { url: '${serviceBase}health' }; const other = { url: 'https://other.example/api', headers: { Authorization: 'Bearer $oldFixtureToken' } }; }"
                }
            }
        )
    }
    $nestedSiblingFixture = [pscustomobject]@{
        id = 'fixture-nested-sibling'
        active = $false
        connections = [pscustomobject]@{}
        nodes = @(
            [pscustomobject]@{
                name = 'Nested sibling fixture'
                parameters = [pscustomobject]@{
                    jsCode = "const parent = { kol: { url: '${serviceBase}health' }, other: { url: 'https://other.example/api', headers: { Authorization: 'Bearer $oldFixtureToken' } } };"
                }
            }
        )
    }
    $functionSiblingFixture = [pscustomobject]@{
        id = 'fixture-function-sibling'
        active = $false
        connections = [pscustomobject]@{}
        nodes = @(
            [pscustomobject]@{
                name = 'Function sibling fixture'
                parameters = [pscustomobject]@{
                    jsCode = "async function run() { const kol = { url: '${serviceBase}health' }; const other = { url: 'https://other.example/api', headers: { Authorization: 'Bearer $oldFixtureToken' } }; }"
                }
            }
        )
    }
    $commentFixture = [pscustomobject]@{
        id = 'fixture-comment'
        active = $false
        connections = [pscustomobject]@{}
        nodes = @(
            [pscustomobject]@{
                name = 'Comment fixture'
                parameters = [pscustomobject]@{
                    jsCode = "await HR({ url: 'https://other.example/api', headers: { Authorization: 'Bearer $oldFixtureToken' } /* url: '${serviceBase}health' */ });"
                }
            }
        )
    }
    $fixtureOriginal = Copy-Json $fixture
    $codeOnlyOriginal = Copy-Json $codeOnlyFixture
    $httpReferences = @(Target-Headers $fixture)
    $codeReferences = @(Target-CodeReferences $fixture $oldFixtureToken)
    foreach ($header in $httpReferences) {
        $header.value = "Bearer $newFixtureToken"
    }
    Update-CodeReferences $fixture $oldFixtureToken $newFixtureToken
    Update-CodeReferences $codeOnlyFixture $oldFixtureToken $newFixtureToken
    $blockCrossObjectReferenceCount = @(Target-CodeReferences $blockCrossObjectFixture).Count
    $nestedSiblingReferenceCount = @(Target-CodeReferences $nestedSiblingFixture).Count
    $functionSiblingReferenceCount = @(Target-CodeReferences $functionSiblingFixture).Count
    $commentReferenceCount = @(Target-CodeReferences $commentFixture).Count
    Update-CodeReferences $blockCrossObjectFixture $oldFixtureToken $newFixtureToken
    Update-CodeReferences $nestedSiblingFixture $oldFixtureToken $newFixtureToken
    Update-CodeReferences $functionSiblingFixture $oldFixtureToken $newFixtureToken
    Update-CodeReferences $commentFixture $oldFixtureToken $newFixtureToken
    $fixtureReadbackVerified = Assert-WorkflowReadback $fixtureOriginal $fixture $newFixtureToken 1 1
    $codeOnlyReadbackVerified = Assert-WorkflowReadback $codeOnlyOriginal $codeOnlyFixture $newFixtureToken 0 1
    $orderedConnectionsA = [pscustomobject][ordered]@{ first = 1; second = 2 }
    $orderedConnectionsB = [pscustomobject][ordered]@{ second = 2; first = 1 }
    [pscustomobject]@{
        mode = 'self-test'
        http_reference_count = $httpReferences.Count
        code_reference_count = $codeReferences.Count
        total_reference_count = $httpReferences.Count + $codeReferences.Count
        code_token_updated = "$($fixture.nodes[1].parameters.jsCode)".Contains("Bearer $newFixtureToken")
        unrelated_code_token_preserved = "$($fixture.nodes[1].parameters.jsCode)".Contains("Bearer $unrelatedToken")
        cross_object_token_preserved = "$($fixture.nodes[2].parameters.jsCode)".Contains("Bearer $unrelatedToken")
        block_cross_object_reference_count = $blockCrossObjectReferenceCount
        block_cross_object_token_preserved = "$($blockCrossObjectFixture.nodes[0].parameters.jsCode)".Contains("Bearer $oldFixtureToken")
        nested_sibling_reference_count = $nestedSiblingReferenceCount
        nested_sibling_token_preserved = "$($nestedSiblingFixture.nodes[0].parameters.jsCode)".Contains("Bearer $oldFixtureToken")
        function_sibling_reference_count = $functionSiblingReferenceCount
        function_sibling_token_preserved = "$($functionSiblingFixture.nodes[0].parameters.jsCode)".Contains("Bearer $oldFixtureToken")
        comment_reference_count = $commentReferenceCount
        comment_token_preserved = "$($commentFixture.nodes[0].parameters.jsCode)".Contains("Bearer $oldFixtureToken")
        header_before_url_supported = "$($fixture.nodes[1].parameters.jsCode)".Contains("Bearer $newFixtureToken")
        code_only_workflow_supported = "$($codeOnlyFixture.nodes[0].parameters.jsCode)".Contains("Bearer $newFixtureToken")
        fixture_readback_verified = $fixtureReadbackVerified
        code_only_readback_verified = $codeOnlyReadbackVerified
        structure_preserved = ((Value-Hash $fixture.connections) -eq (Value-Hash $fixtureOriginal.connections))
        canonical_hash_order_independent = ((Value-Hash $orderedConnectionsA) -eq (Value-Hash $orderedConnectionsB))
        secrets_printed = 0
    } | ConvertTo-Json -Compress
    exit 0
}

$n8nBase = ($env:N8N_BASE_URL ?? '').TrimEnd('/')
if (-not $n8nBase) { throw 'N8N_BASE_URL is required' }
if ($n8nBase -notmatch '/api/v1$') { $n8nBase += '/api/v1' }
if (-not $env:N8N_API_KEY) { throw 'N8N_API_KEY is required' }
if (-not $env:ZEABUR_API_KEY) { throw 'ZEABUR_API_KEY is required' }

$n8nHeaders = @{ 'X-N8N-API-KEY' = $env:N8N_API_KEY }
$zeaburHeaders = @{ Authorization = "Bearer $env:ZEABUR_API_KEY" }

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

$envQuery = 'query($serviceID:ObjectID!,$environmentID:ObjectID!){service(_id:$serviceID){variables(environmentID:$environmentID){key value}}}'
$vars = @{ serviceID = $serviceId; environmentID = $environmentId }
$beforeEnv = (Invoke-Zeabur $envQuery $vars).service.variables
$oldToken = ($beforeEnv | Where-Object key -eq 'INTERNAL_TOKEN' | Select-Object -First 1).value
if (-not $oldToken) { throw 'INTERNAL_TOKEN missing before rotation' }

$workflowList = Invoke-N8n GET '/workflows?limit=250'
$targets = @($workflowList.data | Where-Object { (Target-ReferenceCount $_) -gt 0 })
$allTargetHeaders = @($targets | ForEach-Object { Target-Headers $_ })
$allTargetCodeReferences = @($targets | ForEach-Object { Target-CodeReferences $_ })
$targetReferenceCount = [int]($allTargetHeaders.Count + $allTargetCodeReferences.Count)
$matchingReferenceCount = @($allTargetHeaders | Where-Object value -eq "Bearer $oldToken").Count + @($allTargetCodeReferences | Where-Object token -eq $oldToken).Count
$mismatchedReferenceCount = $targetReferenceCount - $matchingReferenceCount

if (-not $Commit) {
    [pscustomobject]@{
        mode = 'dry-run'
        workflow_count = $targets.Count
        reference_count = $targetReferenceCount
        http_reference_count = $allTargetHeaders.Count
        code_reference_count = $allTargetCodeReferences.Count
        active_workflows = @($targets | Where-Object active).Count
        inactive_workflows = @($targets | Where-Object { -not $_.active }).Count
        matching_reference_count = $matchingReferenceCount
        mismatched_reference_count = $mismatchedReferenceCount
        current_token_fingerprint = Token-Fingerprint $oldToken
        secrets_printed = 0
    } | ConvertTo-Json -Compress
    exit 0
}

$bytes = New-Object byte[] 32
[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
$newToken = 'kol_auto_' + [Convert]::ToHexString($bytes).ToLowerInvariant()

$updated = New-Object System.Collections.Generic.List[object]
try {
    foreach ($summary in $targets) {
        $original = Invoke-N8n GET "/workflows/$($summary.id)"
        $desired = Copy-Json $original
        $targetHeaders = @(Target-Headers $desired)
        if (@($targetHeaders | Where-Object value -ne "Bearer $oldToken").Count) {
            throw "HTTP Authorization token drift before rotation for workflow $($original.id)"
        }
        foreach ($header in $targetHeaders) {
            $header.value = "Bearer $newToken"
        }
        $allCodeReferences = @(Target-CodeReferences $desired)
        $targetCodeReferences = @(Target-CodeReferences $desired $oldToken)
        if ($allCodeReferences.Count -ne $targetCodeReferences.Count) {
            throw "Code-node Authorization token drift before rotation for workflow $($original.id)"
        }
        Update-CodeReferences $desired $oldToken $newToken
        if ($original.active) {
            [void](Invoke-N8n POST "/workflows/$($original.id)/deactivate" @{})
        }
        try {
            [void](Invoke-N8n PUT "/workflows/$($original.id)" (Workflow-Payload $desired))
            if ($original.active) {
                [void](Invoke-N8n POST "/workflows/$($original.id)/activate" @{})
            }
            $readback = Invoke-N8n GET "/workflows/$($original.id)"
            [void](Assert-WorkflowReadback $original $readback $newToken $targetHeaders.Count $targetCodeReferences.Count)
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
    reference_count = $targetReferenceCount
    active_workflows_preserved = @($targets | Where-Object active).Count
    inactive_workflows_preserved = @($targets | Where-Object { -not $_.active }).Count
    env_key = 'INTERNAL_TOKEN'
    redeploy_requested = $true
    secrets_printed = 0
} | ConvertTo-Json -Compress
