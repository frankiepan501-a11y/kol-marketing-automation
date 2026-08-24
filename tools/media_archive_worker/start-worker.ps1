param(
    [switch]$Once,
    [switch]$Probe,
    [string]$RecordId = ""
)

$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptRoot "../..")
$envFile = Join-Path $scriptRoot ".env.worker"

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "缺少 $envFile，请从 .env.worker.example 复制并填写。"
}

foreach ($line in Get-Content -LiteralPath $envFile -Encoding UTF8) {
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) {
        continue
    }
    $parts = $trimmed.Split("=", 2)
    [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1], "Process")
}

$pythonExe = if ($env:MEDIA_ARCHIVE_PYTHON) { $env:MEDIA_ARCHIVE_PYTHON } else { "python.exe" }
$arguments = @("-m", "tools.media_archive_worker.runtime")
if ($Once) { $arguments += "--once" }
if ($Probe) { $arguments += "--probe" }
if ($RecordId) { $arguments += @("--record-id", $RecordId) }

Push-Location $repoRoot
try {
    & $pythonExe @arguments
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
