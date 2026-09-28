import base64
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PATCH_SCRIPT = ROOT / "scripts" / "n8n" / "patch_cs_github_deadman.ps1"


def _extract_code_node_source() -> str:
    text = PATCH_SCRIPT.read_text(encoding="utf-8")
    start_marker = "$codeNode.parameters.jsCode = @'\n"
    end_marker = "\n'@\n\n$body ="
    start = text.index(start_marker) + len(start_marker)
    end = text.index(end_marker, start)
    return text[start:end]


def test_patch_script_has_read_only_dry_run_gate():
    text = PATCH_SCRIPT.read_text(encoding="utf-8")
    dry_run_branch = text.index("if ($DryRun)")
    first_put = text.index("Invoke-RestMethod -Method Put")
    assert dry_run_branch < first_put
    assert "willWrite = $false" in text[dry_run_branch:first_put]
    assert "exit 0" in text[dry_run_branch:first_put]


def _run_code_node(runs: list[dict], repeats: int = 1) -> list[dict]:
    code = _extract_code_node_source()
    runner = r"""
const code = Buffer.from(process.env.CODE_B64, 'base64').toString('utf8');
const runs = JSON.parse(process.env.RUNS_JSON);
const repeats = Number(process.env.REPEATS || '1');
const state = {};
const requests = [];
const context = {helpers: {httpRequest: async (request) => {
  requests.push(request);
  return {workflow_runs: runs};
}}};
const execute = new Function('$getWorkflowStaticData', 'context',
  `return (async function(){${code}\n}).call(context);`);
(async () => {
  const results = [];
  for (let i = 0; i < repeats; i += 1) {
    results.push(await execute(() => state, context));
  }
  process.stdout.write(JSON.stringify({results, state, requests}));
})().catch((error) => {
  process.stderr.write(String(error && error.stack || error));
  process.exit(1);
});
"""
    env = os.environ.copy()
    env.update({
        "CODE_B64": base64.b64encode(code.encode("utf-8")).decode("ascii"),
        "RUNS_JSON": json.dumps(runs),
        "REPEATS": str(repeats),
    })
    completed = subprocess.run(
        ["node", "-e", runner],
        cwd=ROOT,
        env=env,
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=True,
    )
    return json.loads(completed.stdout)


def _run(run_id: int, age: timedelta, conclusion: str = "success") -> dict:
    created_at = datetime.now(timezone.utc) - age
    return {
        "id": run_id,
        "status": "completed",
        "conclusion": conclusion,
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "html_url": f"https://github.com/example/actions/runs/{run_id}",
    }


def test_unordered_response_uses_newest_success_and_stays_silent():
    payload = [_run(1177, timedelta(days=19)), _run(1312, timedelta(minutes=5))]

    result = _run_code_node(payload)

    assert result["results"] == [[]]
    assert result["state"]["lastRunId"] == "1312"
    assert "created=%3E%3D" in result["requests"][0]["url"]
    assert result["requests"][0]["headers"]["Cache-Control"] == "no-cache"


def test_two_latest_failures_alert_with_newest_run_link():
    payload = [
        _run(1400, timedelta(minutes=20), "failure"),
        _run(1401, timedelta(minutes=5), "failure"),
        _run(1399, timedelta(minutes=30), "success"),
    ]

    result = _run_code_node(payload)

    body = result["results"][0][0]["json"]["body"]
    assert "最近两次 GitHub 客服巡检均失败" in body
    assert body.count("https://github.com/example/actions/runs/1401") == 1


def test_stale_api_payload_is_reported_as_api_failure_without_old_run_link():
    payload = [_run(1177, timedelta(days=19))]

    result = _run_code_node(payload, repeats=2)

    assert result["results"][0] == []
    body = result["results"][1][0]["json"]["body"]
    assert result["state"]["currentIssueKey"] == "github_api_unavailable"
    assert "https://github.com/example/actions/runs/1177" not in body
