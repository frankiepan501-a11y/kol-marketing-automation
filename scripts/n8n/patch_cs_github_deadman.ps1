param(
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

$headers = @{
    'X-N8N-API-KEY' = $env:N8N_API_KEY
    Accept = 'application/json'
    'Content-Type' = 'application/json'
}

$workflowId = 'YLVywhEu3lLwXmKo'
$workflowUrl = "$($env:N8N_BASE_URL.TrimEnd('/'))/workflows/$workflowId"
$workflow = Invoke-RestMethod -Headers $headers -Uri $workflowUrl
$scheduleNode = $workflow.nodes | Where-Object { $_.name -eq 'Every 4 hours' }
$codeNode = $workflow.nodes | Where-Object { $_.name -eq 'Check GitHub watchdog heartbeat' }

if (-not $scheduleNode -or -not $codeNode) {
    throw 'Required watchdog nodes were not found.'
}

$expectedSchedule = '43 17 */4 * * *'
if ($scheduleNode.parameters.rule.interval[0].expression -ne $expectedSchedule) {
    throw "Unexpected production schedule; refusing to patch: $($scheduleNode.parameters.rule.interval[0].expression)"
}

$currentCode = [string]$codeNode.parameters.jsCode
$codeNode.parameters.jsCode = @'
const state = $getWorkflowStaticData('global');
const now = Date.now();
const cooldownMs = 24 * 60 * 60 * 1000;
const maxSuccessAgeMs = 8 * 60 * 60 * 1000;
const apiWindowMs = 24 * 60 * 60 * 1000;
const requiredApiFailures = 2;
const retentionMs = 7 * 24 * 60 * 60 * 1000;
let runs = [];
let issueKey = '';
let reason = '';
let latest = null;

state.alertedAtByIssue = state.alertedAtByIssue || {};

try {
  const apiWindowStartMs = now - apiWindowMs;
  const apiWindowStart = new Date(apiWindowStartMs).toISOString();
  const createdFilter = encodeURIComponent(`>=${apiWindowStart}`);
  const response = await this.helpers.httpRequest({
    method: 'GET',
    url: `https://api.github.com/repos/frankiepan501-a11y/kol-marketing-automation/actions/workflows/zeabur-watchdog.yml/runs?event=schedule&created=${createdFilter}&per_page=20`,
    headers: {
      Accept: 'application/vnd.github+json',
      'User-Agent': 'n8n-cs-watchdog-deadman',
      'X-GitHub-Api-Version': '2022-11-28',
      'Cache-Control': 'no-cache',
      Pragma: 'no-cache',
    },
    json: true,
    timeout: 15000,
  });
  const returnedRuns = Array.isArray(response.workflow_runs) ? response.workflow_runs : [];
  runs = returnedRuns
    .map((run) => ({run, createdAtMs: Date.parse(run.created_at || '')}))
    .filter((entry) => Number.isFinite(entry.createdAtMs))
    .sort((left, right) => {
      if (left.createdAtMs !== right.createdAtMs) return right.createdAtMs - left.createdAtMs;
      return Number(right.run.run_number || right.run.id || 0) - Number(left.run.run_number || left.run.id || 0);
    })
    .map((entry) => entry.run);
  if (returnedRuns.length && !runs.length) {
    throw new Error('GitHub API returned workflow runs with no valid created_at timestamps');
  }
  const candidateLatest = runs[0] || null;
  if (candidateLatest) {
    const candidateLatestAt = Date.parse(candidateLatest.created_at || '');
    if (candidateLatestAt < apiWindowStartMs) {
      runs = [];
      throw new Error('GitHub API returned a stale or invalid workflow-runs response outside the requested 24-hour window');
    }
  }
  state.apiFailureCount = 0;
  latest = candidateLatest;

  const completed = runs.filter((run) => run.status === 'completed');
  const latestSuccess = completed.find((run) => run.conclusion === 'success') || null;
  const latestTwo = completed.slice(0, 2);
  const twoConsecutiveFailures = latestTwo.length === 2 && latestTwo.every((run) => run.conclusion !== 'success');

  if (twoConsecutiveFailures) {
    state.noSuccessSince = state.noSuccessSince || now;
    issueKey = 'consecutive_schedule_failures';
    reason = `最近两次 GitHub 客服巡检均失败：${latestTwo.map((run) => run.conclusion || 'unknown').join(' / ')}。`;
  } else if (latestSuccess) {
    state.noSuccessSince = null;
    const successAt = Date.parse(latestSuccess.created_at || '');
    const ageMs = Number.isFinite(successAt) ? now - successAt : null;
    if (ageMs === null) {
      issueKey = 'invalid_success_time';
      reason = '最近一次成功巡检的时间无法解析。';
    } else if (ageMs > maxSuccessAgeMs) {
      const ageMinutes = Math.floor(ageMs / 60000);
      issueKey = 'successful_schedule_stale';
      reason = `最近一次成功巡检距今 ${ageMinutes} 分钟，已超过 8 小时宽限期。`;
    }
  } else {
    state.noSuccessSince = Number(state.noSuccessSince || now);
    const noSuccessAgeMs = now - state.noSuccessSince;
    if (noSuccessAgeMs >= maxSuccessAgeMs) {
      issueKey = runs.length ? 'no_successful_schedule' : 'no_schedule_runs';
      reason = runs.length
        ? 'GitHub 客服巡检已连续 8 小时没有成功记录。'
        : 'GitHub 已连续 8 小时没有返回客服巡检定时运行记录。';
    }
  }
} catch (error) {
  state.apiFailureCount = Number(state.apiFailureCount || 0) + 1;
  if (state.apiFailureCount < requiredApiFailures) return [];
  issueKey = 'github_api_unavailable';
  reason = `连续 ${state.apiFailureCount} 次读取 GitHub Actions 失败：${String(error.message || error).slice(0, 160)}`;
}

state.lastCheckedAt = new Date(now).toISOString();
state.lastRunId = latest && latest.id ? String(latest.id) : null;
state.currentIssueKey = issueKey || null;

if (!issueKey) return [];

const lastAlertAt = Number(state.alertedAtByIssue[issueKey] || 0);
if (now - lastAlertAt < cooldownMs) return [];
state.alertedAtByIssue[issueKey] = now;

for (const [key, value] of Object.entries(state.alertedAtByIssue)) {
  if (now - Number(value || 0) > retentionMs) delete state.alertedAtByIssue[key];
}

const runLink = latest && latest.html_url ? `\n运行记录：${latest.html_url}` : '';
return [{ json: {
  biz: 'AUDIT',
  level: 'P1',
  title: 'GitHub 客服巡检持续异常',
  suffix: '客服助手',
  body: `${reason}${runLink}\n处理：检查 GitHub Actions 是否被暂停、密钥是否失效或运行是否持续失败。`,
  mode: 'auto',
  dry_run: false,
} }];
'@

$body = [ordered]@{
    name = $workflow.name
    nodes = $workflow.nodes
    connections = $workflow.connections
    settings = $workflow.settings
} | ConvertTo-Json -Depth 100 -Compress

$currentCodeBytes = [Text.Encoding]::UTF8.GetBytes($currentCode)
$plannedCodeBytes = [Text.Encoding]::UTF8.GetBytes([string]$codeNode.parameters.jsCode)
$currentCodeSha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($currentCodeBytes)).ToLowerInvariant()
$plannedCodeSha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($plannedCodeBytes)).ToLowerInvariant()

if ($DryRun) {
    [pscustomobject]@{
        dryRun = $true
        workflowId = $workflowId
        workflowName = $workflow.name
        active = $workflow.active
        versionId = $workflow.versionId
        activeVersionId = $workflow.activeVersionId
        schedule = $expectedSchedule
        changedNode = $codeNode.name
        nodeCount = @($workflow.nodes).Count
        connectionGroupCount = @($workflow.connections.PSObject.Properties).Count
        currentCodeSha256 = $currentCodeSha256
        plannedCodeSha256 = $plannedCodeSha256
        willWrite = $false
        willActivate = $false
    } | ConvertTo-Json -Compress
    exit 0
}

$updated = Invoke-RestMethod -Method Put -Headers $headers -Uri $workflowUrl -Body $body
$active = Invoke-RestMethod -Method Post -Headers $headers -Uri "$workflowUrl/activate"

[pscustomobject]@{
    workflowId = $workflowId
    versionId = $active.versionId
    activeVersionId = $active.activeVersionId
    active = $active.active
    schedule = (($active.nodes | Where-Object { $_.name -eq 'Every 4 hours' }).parameters.rule.interval[0].expression)
} | ConvertTo-Json -Compress
