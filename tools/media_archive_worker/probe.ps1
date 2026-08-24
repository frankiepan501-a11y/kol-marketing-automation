$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$startScript = Join-Path $scriptRoot "start-worker.ps1"

& $startScript -Probe
exit $LASTEXITCODE
