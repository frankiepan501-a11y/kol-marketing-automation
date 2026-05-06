"""n8n 工作流执行状态 collector.

数据源: n8n REST API (env N8N_API_KEY, base N8N_BASE_URL)
默认 base: https://frankiepan501.zeabur.app/api/v1

监控 6 个 SEO 系列工作流:
  ee779GzBI8Bj4Bx3 - SEO 新闻稿 (周一至周六每天 1 次, 应 6/周)
  bxqthAOVFjGviUEr - SEO 商业意图 (周二/周四 2/周)
  PEzTmqGwOqcHOPfc - SEO 周报 (周一 1/周)
  xLEIAVos3YmynRsq - 竞品 Gap 扫描 (周一 1/周)
  9gMvXqs3mjS1zBZJ - PSI 月度审计 (每月 1 日)
  z8OmSc1gWqc9cnsH - GSC 排名追踪 (每月 1 日)

⚠️ Zeabur 默认 prune executions 2-3 天 - 拿全周数据需在 n8n 设
   EXECUTIONS_DATA_PRUNE_MAX_AGE > 7d. 否则 fallback 到 last 20 executions 看趋势.

输出 collected.n8n.data:
{
  "workflows": {
    "ee779GzBI8Bj4Bx3": {
      "name": "SEO 新闻稿",
      "expected_per_week": 6,
      "in_window": {  # 本周窗口内 (受 prune 限制)
        "success": int, "error": int, "crashed": int, "total": int,
      },
      "last_run": "2026-05-04T01:30:00Z" | None,
      "health": "healthy" | "degraded" | "stale",
    },
    ... 6 个
  },
  "summary": {
    "total_runs": int, "total_errors": int, "error_rate": float,
    "stale_workflows": [(id, name), ...],  # 24h+ 无 run
  },
}
"""
import asyncio
import datetime
import logging
import os

import httpx

log = logging.getLogger("weekly_report.n8n_executions")

DEFAULT_BASE = "https://frankiepan501.zeabur.app/api/v1"
TIMEOUT = 30.0

WORKFLOWS = [
    ("ee779GzBI8Bj4Bx3", "SEO 新闻稿", 6),
    ("bxqthAOVFjGviUEr", "SEO 商业意图", 2),
    ("PEzTmqGwOqcHOPfc", "SEO 周报", 1),
    ("xLEIAVos3YmynRsq", "竞品 Gap 扫描", 1),
    ("9gMvXqs3mjS1zBZJ", "PSI 月度审计", 0),  # 月级, 周报参考
    ("z8OmSc1gWqc9cnsH", "GSC 排名追踪", 0),
]


def _get_creds():
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("N8N_API_KEY env 未设")
    # .strip() + 补 https:// schema 防 user paste 时去掉了协议
    base = os.environ.get("N8N_BASE_URL", DEFAULT_BASE).strip().rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base
    return api_key, base


async def _fetch_executions(workflow_id: str, limit: int = 50) -> list:
    api_key, base = _get_creds()
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        r = await cli.get(f"{base}/executions",
                           headers={"X-N8N-API-KEY": api_key},
                           params={"workflowId": workflow_id, "limit": limit})
        r.raise_for_status()
        return r.json().get("data") or []


def _analyze_executions(execs: list, workflow_id: str, name: str,
                         expected: int, start_iso: str, end_iso: str) -> dict:
    """统计本周窗口内的执行情况."""
    in_window_success = 0
    in_window_error = 0
    in_window_crashed = 0
    last_run = None
    for ex in execs:
        started = ex.get("startedAt") or ""
        if started > (last_run or ""):
            last_run = started
        if start_iso <= started <= end_iso:
            status = (ex.get("status") or "").lower()
            if status in ("success",):
                in_window_success += 1
            elif status in ("error", "failed"):
                in_window_error += 1
            elif status in ("crashed",):
                in_window_crashed += 1

    in_window_total = in_window_success + in_window_error + in_window_crashed

    # 健康度判断
    health = "healthy"
    now = datetime.datetime.utcnow()
    if not last_run:
        health = "stale"
    else:
        try:
            last_dt = datetime.datetime.fromisoformat(last_run.replace("Z", "+00:00")).replace(tzinfo=None)
            hours_since = (now - last_dt).total_seconds() / 3600
            if hours_since > 24 and expected > 0:
                health = "stale"
            elif in_window_error > 0 or in_window_crashed > 0:
                health = "degraded"
        except (ValueError, AttributeError):
            health = "unknown"

    return {
        "workflow_id": workflow_id,
        "name": name,
        "expected_per_week": expected,
        "in_window": {
            "success": in_window_success,
            "error": in_window_error,
            "crashed": in_window_crashed,
            "total": in_window_total,
        },
        "last_run": last_run,
        "health": health,
    }


async def collect(start_date, end_date) -> dict:
    log.info("n8n_executions.collect %s ~ %s", start_date, end_date)
    start_iso = start_date.isoformat() + "T00:00:00.000Z"
    end_iso = (end_date + datetime.timedelta(days=1)).isoformat() + "T00:00:00.000Z"

    try:
        # 6 个 workflow 并发拉
        results = await asyncio.gather(
            *[_fetch_executions(wf_id) for wf_id, _, _ in WORKFLOWS],
            return_exceptions=True,
        )

        workflows = {}
        total_runs = 0
        total_errors = 0
        stale = []

        for (wf_id, name, expected), execs in zip(WORKFLOWS, results):
            if isinstance(execs, Exception):
                workflows[wf_id] = {"name": name, "error": f"{type(execs).__name__}: {execs}"}
                continue
            stat = _analyze_executions(execs, wf_id, name, expected, start_iso, end_iso)
            workflows[wf_id] = stat
            total_runs += stat["in_window"]["total"]
            total_errors += stat["in_window"]["error"] + stat["in_window"]["crashed"]
            if stat["health"] == "stale":
                stale.append({"id": wf_id, "name": name})

        return {
            "status": "ok",
            "data": {
                "workflows": workflows,
                "summary": {
                    "total_runs": total_runs,
                    "total_errors": total_errors,
                    "error_rate": round(total_errors / max(total_runs, 1), 4),
                    "stale_workflows": stale,
                },
                "window": f"{start_date}~{end_date}",
                "note": "Zeabur n8n 默认 prune 2-3 天, 周报数据可能不完整",
            },
        }
    except Exception as e:
        log.exception("n8n_executions collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))
