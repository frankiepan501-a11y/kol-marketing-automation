"""Audit B2B LinkedIn async n8n jobs.

n8n executions can be marked success as soon as an async endpoint accepts the
job. This module follows the job_id returned by n8n and checks the real backend
result so zero-output or failed background jobs do not stay invisible.
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Any

import httpx

from . import b2b_linkedin_daily_card, b2b_linkedin_discovery, config, feishu

N8N_DEFAULT_BASE = "https://frankiepan501.zeabur.app/api/v1"
SERVICE_DEFAULT_BASE = "https://kol-auto.zeabur.app"
TIMEOUT = 30.0
BJ = dt.timezone(dt.timedelta(hours=8))

WORKFLOWS = {
    "discovery": {
        "workflow_id": "uPyR64HJPdFe1F9p",
        "name": "B2B LinkedIn External Discovery - Daily 08:20 BJ",
        "node_names": ["Call B2B External Discovery"],
        "job_path": "/b2b-linkedin-discovery/jobs/{job_id}",
        "created_key": "created_candidates",
        "planned_key": "planned_candidates",
        "zero_ok_status": {"skip_target_met"},
    },
    "auto_pool": {
        "workflow_id": "ukpF2oBrGgvOIecU",
        "name": "B2B LinkedIn Auto Pool - Daily 09:00 BJ",
        "node_names": ["Call B2B LinkedIn Auto Pool"],
        "job_path": "/b2b-linkedin-auto-pool/jobs/{job_id}",
        "created_key": "created_records",
        "planned_key": "planned_records",
        "zero_ok_status": set(),
    },
}


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_iso(value: str) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _bj_day(value: str) -> str:
    parsed = _parse_iso(value)
    if not parsed:
        return dt.datetime.now(BJ).strftime("%Y-%m-%d")
    return parsed.astimezone(BJ).strftime("%Y-%m-%d")


def _n8n_creds() -> tuple[str, str]:
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("N8N_API_KEY env 未设，无法巡检 n8n execution")
    base = os.environ.get("N8N_BASE_URL", N8N_DEFAULT_BASE).strip().rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base
    return api_key, base


def _service_base() -> str:
    base = os.environ.get("KOL_AUTO_PUBLIC_BASE_URL", SERVICE_DEFAULT_BASE).strip().rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base
    return base


async def _fetch_executions(workflow_id: str, limit: int = 5) -> list[dict]:
    api_key, base = _n8n_creds()
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        resp = await cli.get(
            f"{base}/executions",
            headers={"X-N8N-API-KEY": api_key},
            params={"workflowId": workflow_id, "limit": limit},
        )
        resp.raise_for_status()
        return resp.json().get("data") or []


async def _fetch_execution_detail(execution_id: str) -> dict:
    api_key, base = _n8n_creds()
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        resp = await cli.get(
            f"{base}/executions/{execution_id}",
            headers={"X-N8N-API-KEY": api_key},
            params={"includeData": "true"},
        )
        resp.raise_for_status()
        return resp.json()


async def _fetch_job(job_path: str, job_id: str) -> dict:
    url = _service_base() + job_path.format(job_id=job_id)
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        resp = await cli.get(url, headers={"Authorization": f"Bearer {config.INTERNAL_TOKEN}"})
        if resp.status_code == 404:
            return {"ok": False, "status": "missing", "error": "job not found"}
        resp.raise_for_status()
        return resp.json()


def _extract_job_id(execution_detail: dict, node_names: list[str]) -> str:
    run_data = (
        (execution_detail.get("data") or {})
        .get("resultData", {})
        .get("runData", {})
    )
    for node_name in node_names:
        for run in run_data.get(node_name) or []:
            for branch in (((run.get("data") or {}).get("main")) or []):
                for item in branch or []:
                    payload = item.get("json") or {}
                    job_id = str(payload.get("job_id") or "").strip()
                    if job_id:
                        return job_id
    return ""


def _search_provider_readiness() -> dict:
    provider = (os.environ.get("GOOGLE_SEARCH_PROVIDER") or "custom_search").strip().lower()
    missing = []
    if provider == "serpapi":
        if not (os.environ.get("SERPAPI_API_KEY") or os.environ.get("GOOGLE_SEARCH_API_KEY")):
            missing.append("SERPAPI_API_KEY")
    elif provider == "manual":
        if not os.environ.get("B2B_DISCOVERY_MANUAL_RESULTS_JSON"):
            missing.append("B2B_DISCOVERY_MANUAL_RESULTS_JSON")
    else:
        if not os.environ.get("GOOGLE_SEARCH_API_KEY"):
            missing.append("GOOGLE_SEARCH_API_KEY")
        if not (os.environ.get("GOOGLE_SEARCH_ENGINE_ID") or os.environ.get("GOOGLE_SEARCH_CX")):
            missing.append("GOOGLE_SEARCH_ENGINE_ID")
    return {"provider": provider, "ok": not missing, "missing": missing}


def _status_from_created(defn: dict, result: dict) -> tuple[str, str]:
    waterline = str(result.get("waterline_status") or "")
    created = int(result.get(defn["created_key"]) or 0)
    planned = int(result.get(defn["planned_key"]) or 0)
    provider_errors = result.get("provider_errors") or []

    if waterline in defn.get("zero_ok_status", set()):
        return "ok", f"高水位跳过：{waterline}"
    if created > 0:
        return "ok", f"created={created}, planned={planned}"
    if provider_errors:
        return "error", f"created=0, provider_errors={len(provider_errors)}"
    return "error", f"created=0, planned={planned}, waterline={waterline or '-'}"


def _search_provider_issue_counts(search_provider: dict, items: list[dict]) -> bool:
    if search_provider.get("ok"):
        return False
    discovery = next((item for item in items if item.get("key") == "discovery"), {})
    if not discovery:
        return False
    job_result = discovery.get("job_result") or {}
    if discovery.get("health") == "ok" and job_result.get("waterline_status") in WORKFLOWS["discovery"].get("zero_ok_status", set()):
        return False
    return True


async def _fallback_missing_job(key: str, latest_execution: dict) -> dict | None:
    """Recover audit signal when in-process async job cache was lost.

    The async job endpoint stores compact results in service memory. A restart
    after the n8n ACK can erase that cache even though the business write
    already happened. Fall back to durable Bitable state before alerting.
    """
    if key == "auto_pool":
        day = _bj_day(latest_execution.get("startedAt") or "")
        summary = await b2b_linkedin_daily_card.run_pool_summary(
            commit=False,
            notify=False,
            day=day,
        )
        result = {
            "created_records": summary.get("new_records"),
            "planned_records": summary.get("new_records"),
            "waterline_status": "pool_summary_fallback",
            "provider_errors": [],
            "skip_reasons": {},
            "country_counts": summary.get("country_counts") or {},
            "current_queue_total": summary.get("current_queue_total"),
        }
        if int(summary.get("new_records") or 0) > 0:
            return {"health": "ok", "summary": f"job cache missing; pool summary created={summary.get('new_records')}", "result": result}
        return {"health": "error", "issues": [f"job cache missing; pool summary new_records={summary.get('new_records') or 0}"], "result": result}

    if key == "discovery":
        result = await b2b_linkedin_discovery.run(
            commit=False,
            provider="all",
            limit=1,
            pending_target=0,
            min_score=0,
        )
        health, message = _status_from_created(WORKFLOWS["discovery"], result)
        if health == "ok":
            return {"health": "ok", "summary": f"job cache missing; live discovery check {message}", "result": result}
        return {"health": "error", "issues": [f"job cache missing; live discovery check {message}"], "result": result}

    return None


async def _audit_one(key: str, defn: dict, *, since: dt.datetime) -> dict:
    item: dict[str, Any] = {
        "key": key,
        "workflow_id": defn["workflow_id"],
        "name": defn["name"],
        "health": "unknown",
        "issues": [],
    }
    try:
        executions = await _fetch_executions(defn["workflow_id"], limit=5)
    except Exception as exc:
        item.update(health="error", issues=[f"n8n execution 拉取失败: {type(exc).__name__}: {exc}"])
        return item

    item["execution_count"] = len(executions)
    if not executions:
        item.update(health="error", issues=["没有找到 n8n execution"])
        return item

    latest = executions[0]
    item["latest_execution"] = {
        "id": latest.get("id"),
        "status": latest.get("status"),
        "startedAt": latest.get("startedAt"),
        "stoppedAt": latest.get("stoppedAt"),
    }
    started = _parse_iso(latest.get("startedAt") or "")
    if started and started < since:
        item.update(health="error", issues=[f"最近 execution 已过期: {latest.get('startedAt')}"])
        return item
    if (latest.get("status") or "").lower() != "success":
        item.update(health="error", issues=[f"n8n latest status={latest.get('status')}"])
        return item

    try:
        detail = await _fetch_execution_detail(str(latest["id"]))
    except Exception as exc:
        item.update(health="error", issues=[f"execution detail 拉取失败: {type(exc).__name__}: {exc}"])
        return item

    job_id = _extract_job_id(detail, defn["node_names"])
    item["job_id"] = job_id
    if not job_id:
        item.update(health="error", issues=["n8n success 但 HTTP 节点没有返回 job_id"])
        return item

    try:
        job = await _fetch_job(defn["job_path"], job_id)
    except Exception as exc:
        item.update(health="error", issues=[f"后台 job 查询失败: {type(exc).__name__}: {exc}"])
        return item

    item["job_status"] = job.get("status")
    result = job.get("result") or {}
    item["job_result"] = {
        "created": result.get(defn["created_key"]),
        "planned": result.get(defn["planned_key"]),
        "waterline_status": result.get("waterline_status"),
        "provider_errors_count": len(result.get("provider_errors") or []),
        "skip_reasons": result.get("skip_reasons") or {},
    }

    if job.get("status") == "missing":
        fallback = await _fallback_missing_job(key, latest)
        if fallback:
            fallback_result = fallback.get("result") or {}
            item["fallback"] = "durable_bitable_state"
            item["job_result"] = {
                "created": fallback_result.get(defn["created_key"]),
                "planned": fallback_result.get(defn["planned_key"]),
                "waterline_status": fallback_result.get("waterline_status"),
                "provider_errors_count": len(fallback_result.get("provider_errors") or []),
                "skip_reasons": fallback_result.get("skip_reasons") or {},
            }
            if key == "auto_pool":
                item["job_result"]["country_counts"] = fallback_result.get("country_counts") or {}
                item["job_result"]["current_queue_total"] = fallback_result.get("current_queue_total")
            item["health"] = fallback.get("health")
            if fallback.get("health") == "ok":
                item["summary"] = fallback.get("summary")
            else:
                item["issues"].extend(fallback.get("issues") or [])
            return item

    if job.get("status") == "running":
        item.update(health="warn", issues=["后台 job 仍在 running，稍后需复查"])
        return item
    if job.get("status") != "success":
        item.update(health="error", issues=[f"后台 job status={job.get('status')}: {job.get('error') or ''}".strip()])
        return item

    health, message = _status_from_created(defn, result)
    item["health"] = health
    if health != "ok":
        item["issues"].append(message)
    else:
        item["summary"] = message
    return item


def _build_alert_card(audit: dict) -> dict:
    issue_lines = []
    for item in audit.get("items") or []:
        if item.get("health") == "ok":
            continue
        latest = item.get("latest_execution") or {}
        issues = "; ".join(item.get("issues") or [])
        issue_lines.append(
            f"- **{item.get('key')}**: {issues}\n"
            f"  n8n={latest.get('id') or '-'} / job={item.get('job_id') or '-'} / status={item.get('job_status') or '-'}"
        )
    readiness = audit.get("search_provider") or {}
    if readiness and audit.get("search_provider_issue_counted"):
        issue_lines.append(
            f"- **search_provider**: {readiness.get('provider')} 缺 {', '.join(readiness.get('missing') or [])}"
        )
    content = "\n".join(issue_lines)[:3500] or "未发现异常。"
    return {
        "header": {
            "template": "red" if audit.get("health") == "error" else "yellow",
            "title": {"tag": "plain_text", "content": "B2B LinkedIn 异步任务巡检异常"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "检查项：n8n success 后的后台 job 状态与真实产出。"}},
        ],
    }


async def _notify_frankie(audit: dict) -> list[str]:
    card = _build_alert_card(audit)
    message_ids = []
    for name, oid in config.NOTIFY_USERS:
        if name.startswith("潘"):
            msg_id = await feishu.send_card_message("open_id", oid, card, biz="AUDIT", level="P1")
            message_ids.append(msg_id)
    return message_ids


async def run(*, notify: bool = False, workflow: str = "all", lookback_hours: int = 30) -> dict:
    workflow = (workflow or "all").strip().lower()
    if workflow not in {"all", *WORKFLOWS.keys()}:
        raise ValueError("workflow must be all, discovery, or auto_pool")
    since = _utc_now() - dt.timedelta(hours=max(1, int(lookback_hours or 30)))
    selected = WORKFLOWS.items() if workflow == "all" else [(workflow, WORKFLOWS[workflow])]

    items = [await _audit_one(key, defn, since=since) for key, defn in selected]
    search_provider = _search_provider_readiness()

    issue_count = sum(1 for item in items if item.get("health") not in {"ok"})
    search_provider_issue_counted = _search_provider_issue_counts(search_provider, items)
    if search_provider_issue_counted:
        issue_count += 1
    health = "ok" if issue_count == 0 else "error"
    audit = {
        "ok": health == "ok",
        "health": health,
        "issue_count": issue_count,
        "lookback_hours": lookback_hours,
        "search_provider": search_provider,
        "search_provider_issue_counted": search_provider_issue_counted,
        "items": items,
    }
    if notify and issue_count:
        try:
            audit["message_ids"] = await _notify_frankie(audit)
        except Exception as exc:
            audit["notify_error"] = f"{type(exc).__name__}: {exc}"
    return audit
