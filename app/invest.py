"""Investment assistant daily X intelligence workflow.

This module is intentionally isolated from KOL/CS business logic. It only uses
environment variables and does not write business data unless a caller enables
notification.
"""
import json
import os
import re
import time
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException

from . import config


BJ = timezone(timedelta(hours=8))
UTC = timezone.utc
X_API = "https://api.x.com/2"
TARGET_USERNAME = os.environ.get("INVEST_X_USERNAME", "aleabitoreddit").strip().lstrip("@")
TARGET_LABEL = os.environ.get("INVEST_X_LABEL", "Serenity / @aleabitoreddit").strip()
TARGET_PROFILE_URL = os.environ.get("INVEST_X_PROFILE_URL", "https://x.com/aleabitoreddit").strip()
DEFAULT_NOTIFY_UNION = "on_6e85dd60606f76f2d5af892785ac1dfe"
A_SHARE_CODE_RE = re.compile(r"^\d{6}$")

router = APIRouter(prefix="/invest", tags=["invest"])
_x_user_cache: dict[str, Any] = {"username": "", "id": "", "ts": 0.0}
_feishu_token_cache: dict[str, Any] = {"v": "", "exp": 0.0}
_jobs: dict[str, dict[str, Any]] = {}
_JOB_TTL = 24 * 3600


class InvestConfigError(RuntimeError):
    pass


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


def _int_env(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _safe_text(value: Any, limit: int = 500) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= limit else text[: limit - 1] + "..."


def _normalize_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    """Keep AI output display-safe before it reaches Feishu."""
    candidates = analysis.get("a_share_candidates")
    if not isinstance(candidates, list):
        analysis["a_share_candidates"] = []
        return analysis

    cleaned = []
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        c = dict(raw)
        code = str(c.get("code") or "").strip()
        if code and not A_SHARE_CODE_RE.fullmatch(code):
            c["code"] = ""
            risks = c.get("risks")
            if not isinstance(risks, list):
                risks = [str(risks)] if risks else []
            risks.append("原模型输出了非标准A股代码，已清空，需人工核对代码")
            c["risks"] = risks
        action = str(c.get("action") or "观察").strip()
        if action not in ("观察", "加入候选", "暂不建议追"):
            c["action"] = "观察"
        try:
            c["confidence"] = max(0, min(100, int(c.get("confidence") or 0)))
        except (TypeError, ValueError):
            c["confidence"] = 0
        cleaned.append(c)
    analysis["a_share_candidates"] = cleaned
    return analysis


def _now_string() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S%z")


def _cleanup_jobs() -> None:
    now = time.time()
    for job_id in list(_jobs):
        if now - float(_jobs[job_id].get("started_ts") or 0) > _JOB_TTL:
            _jobs.pop(job_id, None)


def _running_job() -> tuple[str, dict[str, Any] | None]:
    _cleanup_jobs()
    for job_id, job in _jobs.items():
        if job.get("status") == "running":
            return job_id, job
    return "", None


def _compact_result(result: dict[str, Any]) -> dict[str, Any]:
    keep = [
        "ok", "dry_run", "notify", "sent", "message_id", "target", "start_time",
        "post_count", "newest_post_id", "candidate_count", "analysis",
    ]
    return {k: result.get(k) for k in keep if k in result}


async def _run_daily_job(job_id: str, *, notify: bool, dry_run: bool,
                         limit: int | None, lookback_hours: int | None) -> None:
    try:
        result = await run_daily(
            notify=notify,
            dry_run=dry_run,
            limit=limit,
            lookback_hours=lookback_hours,
        )
        _jobs[job_id].update(
            status="success",
            finished_at=_now_string(),
            result=_compact_result(result),
        )
    except InvestConfigError as e:
        _jobs[job_id].update(
            status="error",
            finished_at=_now_string(),
            error_type="missing_config",
            error=str(e),
        )
    except Exception as e:
        _jobs[job_id].update(
            status="error",
            finished_at=_now_string(),
            error_type="runtime",
            error=str(e),
        )


def _x_headers() -> dict[str, str]:
    token = os.environ.get("X_BEARER_TOKEN") or os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        raise InvestConfigError("missing X_BEARER_TOKEN or TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {token}"}


async def _x_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=45.0) as cli:
        r = await cli.get(f"{X_API}{path}", params=params or {}, headers=_x_headers())
    if r.status_code >= 400:
        raise RuntimeError(f"X API {path} -> {r.status_code}: {r.text[:300]}")
    return r.json()


async def _x_user_id(username: str = TARGET_USERNAME) -> str:
    env_user_id = os.environ.get("INVEST_X_USER_ID", "").strip()
    if env_user_id:
        return env_user_id
    now = time.time()
    if _x_user_cache["username"] == username and _x_user_cache["id"] and now - _x_user_cache["ts"] < 86400:
        return _x_user_cache["id"]
    data = await _x_get(f"/users/by/username/{username}", {
        "user.fields": "id,name,username,verified,description,public_metrics",
    })
    user_id = ((data.get("data") or {}).get("id") or "").strip()
    if not user_id:
        raise RuntimeError(f"cannot resolve X user id for @{username}: {data}")
    _x_user_cache.update({"username": username, "id": user_id, "ts": now})
    return user_id


async def fetch_posts(limit: int = 10, lookback_hours: int = 30) -> dict[str, Any]:
    """Fetch recent original posts from the target X account."""
    user_id = await _x_user_id()
    start_time = (datetime.now(UTC) - timedelta(hours=lookback_hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    params = {
        "max_results": max(5, min(100, limit)),
        "exclude": "retweets,replies",
        "start_time": start_time,
        "tweet.fields": "created_at,public_metrics,entities,context_annotations,attachments,note_tweet",
        "expansions": "attachments.media_keys",
        "media.fields": "type,url,preview_image_url,alt_text,width,height",
    }
    data = await _x_get(f"/users/{user_id}/tweets", params)
    media_map = {m.get("media_key"): m for m in (data.get("includes") or {}).get("media", [])}
    posts = []
    for tw in data.get("data") or []:
        text = ((tw.get("note_tweet") or {}).get("text") or tw.get("text") or "").strip()
        media = []
        for key in ((tw.get("attachments") or {}).get("media_keys") or []):
            m = media_map.get(key)
            if m:
                media.append({
                    "type": m.get("type"),
                    "url": m.get("url") or m.get("preview_image_url"),
                    "alt_text": m.get("alt_text"),
                })
        posts.append({
            "id": tw.get("id"),
            "created_at": tw.get("created_at"),
            "url": f"{TARGET_PROFILE_URL}/status/{tw.get('id')}",
            "text": text,
            "metrics": tw.get("public_metrics") or {},
            "media": media,
        })
    posts.sort(key=lambda p: p.get("created_at") or "", reverse=True)
    return {"username": TARGET_USERNAME, "user_id": user_id, "start_time": start_time, "posts": posts}


async def _call_deepseek(system_prompt: str, user_prompt: str, max_tokens: int = 2500) -> str:
    if not config.DEEPSEEK_API_KEY:
        raise InvestConfigError("missing DEEPSEEK_API_KEY")
    payload = {
        "model": os.environ.get("INVEST_AI_MODEL", "deepseek-chat"),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    async with httpx.AsyncClient(timeout=120.0) as cli:
        r = await cli.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {config.DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json=payload,
        )
    if r.status_code >= 400:
        raise RuntimeError(f"DeepSeek -> {r.status_code}: {r.text[:300]}")
    data = r.json()
    return ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")


def _extract_json(text: str) -> dict[str, Any]:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        obj = json.loads(cleaned)
        return obj if isinstance(obj, dict) else {"raw": obj}
    except Exception:
        pass
    match = re.search(r"\{.*\}", cleaned, flags=re.S)
    if match:
        try:
            obj = json.loads(match.group(0))
            return obj if isinstance(obj, dict) else {"raw": obj}
        except Exception:
            pass
    return {
        "summary": cleaned[:1000],
        "themes": [],
        "us_tickers": [],
        "a_share_candidates": [],
        "risks": ["AI output was not valid JSON; manual review required."],
    }


async def analyze_posts(posts: list[dict[str, Any]]) -> dict[str, Any]:
    if not posts:
        return {
            "summary": "过去窗口内未抓到新原创帖子。",
            "themes": [],
            "us_tickers": [],
            "a_share_candidates": [],
            "risks": [],
        }
    system_prompt = """你是A股研究助理，只做研究观察，不给确定性买卖指令。
任务：阅读海外投资分析师关于AI、半导体、云、算力、能源、供应链等帖子，
提取可映射到中国A股的产业链线索，并给出观察型建议。

硬规则：
1. 输出必须是JSON对象，不能有markdown包裹。
2. A股代码必须是6位数字；不确定具体A股代码时，code留空，不要编造；严禁输出300XXX、688XXX、002XXX等占位代码。
3. action只能是：观察、加入候选、暂不建议追。
4. 每个候选必须有reason、risks、confidence(0-100)。
5. 明确区分：原帖说了什么、你推导了什么。
6. 全文必须带“非投资建议，仅供研究观察”的风控表述。"""
    post_lines = []
    for p in posts[:20]:
        metrics = p.get("metrics") or {}
        post_lines.append(
            "\n".join([
                f"ID: {p.get('id')}",
                f"Time: {p.get('created_at')}",
                f"URL: {p.get('url')}",
                f"Metrics: {json.dumps(metrics, ensure_ascii=False)}",
                f"Text: {p.get('text')}",
            ])
        )
    schema = {
        "summary": "中文总摘要",
        "themes": ["主题1", "主题2"],
        "us_tickers": [{"ticker": "NVDA", "reason": "原帖或推导理由"}],
        "a_share_candidates": [
            {
                "code": "",
                "name": "公司名",
                "theme": "对应主题",
                "action": "观察",
                "confidence": 60,
                "reason": "为什么与原帖产业链相关",
                "risks": ["风险1", "风险2"],
                "source_post_ids": ["post id"],
            }
        ],
        "follow_up": ["明天/盘后需要跟踪什么"],
        "disclaimer": "非投资建议，仅供研究观察。",
    }
    user_prompt = (
        f"目标账号：{TARGET_LABEL}\n"
        f"抓取帖子数：{len(posts)}\n\n"
        "请按以下JSON schema输出，不要输出schema以外解释：\n"
        f"{json.dumps(schema, ensure_ascii=False)}\n\n"
        "帖子内容：\n"
        + "\n\n---\n\n".join(post_lines)
    )
    raw = await _call_deepseek(system_prompt, user_prompt)
    analysis = _extract_json(raw)
    analysis = _normalize_analysis(analysis)
    analysis["_raw_model_chars"] = len(raw)
    return analysis


def _format_card(posts: list[dict[str, Any]], analysis: dict[str, Any], *, lookback_hours: int) -> dict[str, Any]:
    now_bj = datetime.now(BJ).strftime("%Y-%m-%d %H:%M")
    candidates = analysis.get("a_share_candidates") or []
    themes = analysis.get("themes") or []
    us_tickers = analysis.get("us_tickers") or []
    newest = posts[0]["created_at"] if posts else "N/A"
    title = f"🟡 [INVEST·P2] Alea每日A股映射 · {now_bj}"

    parts = [
        f"**目标账号**: [{TARGET_LABEL}]({TARGET_PROFILE_URL})",
        f"**窗口**: 过去 {lookback_hours} 小时 · **帖子数**: {len(posts)} · **最新帖**: {newest}",
        "",
        f"**原帖摘要**: {_safe_text(analysis.get('summary'), 900)}",
    ]
    if themes:
        parts.append("**主题**: " + " / ".join(_safe_text(x, 40) for x in themes[:8]))
    if us_tickers:
        tickers = []
        for item in us_tickers[:10]:
            if isinstance(item, dict):
                tickers.append(item.get("ticker") or item.get("symbol") or "")
            else:
                tickers.append(str(item))
        parts.append("**涉及美股/海外标的**: " + ", ".join([x for x in tickers if x]))

    elements = [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(parts)}}]

    if candidates:
        lines = []
        for idx, c in enumerate(candidates[:8], 1):
            code = c.get("code") or "代码待核对"
            name = c.get("name") or "公司待核对"
            action = c.get("action") or "观察"
            conf = c.get("confidence", "")
            reason = _safe_text(c.get("reason"), 260)
            risks = c.get("risks") or []
            risk_text = "；".join(_safe_text(x, 70) for x in risks[:3])
            lines.append(
                f"{idx}. **{code} {name}** · {action} · 置信度 {conf}\n"
                f"   - 逻辑: {reason}\n"
                f"   - 风险: {risk_text or '待补充'}"
            )
        elements.extend([
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**A股候选观察**\n" + "\n".join(lines)}},
        ])
    else:
        elements.extend([
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**A股候选观察**\n本轮没有足够明确的A股映射，建议不强行追。"}},
        ])

    post_lines = []
    for p in posts[:5]:
        post_lines.append(f"- [{p.get('id')}]({p.get('url')}) · {_safe_text(p.get('text'), 180)}")
    if post_lines:
        elements.extend([
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**原帖索引**\n" + "\n".join(post_lines)}},
        ])

    follow_up = analysis.get("follow_up") or []
    if follow_up:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "**后续跟踪**\n" + "\n".join(f"- {_safe_text(x, 120)}" for x in follow_up[:6])},
        })
    elements.append({"tag": "note", "elements": [{"tag": "plain_text", "content": "非投资建议，仅供研究观察；代码与公司映射需盘前/盘后人工复核。"}]})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "yellow", "title": {"tag": "plain_text", "content": title}},
        "elements": elements,
    }


async def _invest_feishu_token() -> str:
    app_id = os.environ.get("FEISHU_INVEST_ASSISTANT_APP_ID", "").strip()
    secret = os.environ.get("FEISHU_INVEST_ASSISTANT_APP_SECRET", "").strip()
    if not app_id or not secret:
        raise InvestConfigError("missing FEISHU_INVEST_ASSISTANT_APP_ID/SECRET")
    now = time.time()
    if _feishu_token_cache["v"] and _feishu_token_cache["exp"] > now:
        return _feishu_token_cache["v"]
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": app_id, "app_secret": secret},
        )
    if r.status_code >= 400:
        raise RuntimeError(f"Feishu token -> {r.status_code}: {r.text[:300]}")
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Feishu token error: {data}")
    _feishu_token_cache["v"] = data.get("tenant_access_token", "")
    _feishu_token_cache["exp"] = now + max(60, int(data.get("expire") or 3600) - 300)
    return _feishu_token_cache["v"]


async def _send_invest_card(card: dict[str, Any]) -> str:
    token = await _invest_feishu_token()
    receive_type = os.environ.get("INVEST_NOTIFY_RECEIVE_ID_TYPE", "union_id").strip() or "union_id"
    receive_id = (
        os.environ.get("INVEST_NOTIFY_RECEIVE_ID")
        or os.environ.get("INVEST_NOTIFY_UNION_ID")
        or DEFAULT_NOTIFY_UNION
    ).strip()
    if not receive_id:
        raise InvestConfigError("missing INVEST_NOTIFY_RECEIVE_ID or INVEST_NOTIFY_UNION_ID")
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_type}"
    payload = {"receive_id": receive_id, "msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)}
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    if r.status_code >= 400:
        raise RuntimeError(f"Feishu send -> {r.status_code}: {r.text[:300]}")
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Feishu send error: {data}")
    return ((data.get("data") or {}).get("message_id") or "").strip()


async def run_daily(*, notify: bool = False, dry_run: bool = True,
                    limit: int | None = None, lookback_hours: int | None = None) -> dict[str, Any]:
    limit = limit or _int_env("INVEST_X_MAX_POSTS", 12, 5, 100)
    lookback_hours = lookback_hours or _int_env("INVEST_X_LOOKBACK_HOURS", 30, 1, 168)
    fetched = await fetch_posts(limit=limit, lookback_hours=lookback_hours)
    posts = fetched["posts"]
    analysis = await analyze_posts(posts)
    card = _format_card(posts, analysis, lookback_hours=lookback_hours)
    message_id = ""
    if notify and not dry_run:
        message_id = await _send_invest_card(card)
    return {
        "ok": True,
        "dry_run": dry_run,
        "notify": notify,
        "sent": bool(message_id),
        "message_id": message_id,
        "target": {"username": TARGET_USERNAME, "user_id": fetched.get("user_id")},
        "start_time": fetched.get("start_time"),
        "post_count": len(posts),
        "newest_post_id": posts[0]["id"] if posts else "",
        "candidate_count": len(analysis.get("a_share_candidates") or []),
        "analysis": analysis,
        "card_preview": card if dry_run or _bool_env("INVEST_RETURN_CARD_PREVIEW", False) else None,
    }


@router.post("/daily/run")
async def invest_daily_run(
    authorization: str = Header(default=""),
    notify: bool = False,
    dry_run: bool = True,
    async_mode: bool = False,
    limit: int | None = None,
    lookback_hours: int | None = None,
):
    """Fetch Alea/Serenity X posts, map themes to A-share candidates, optionally notify Feishu."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if authorization[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")
    if async_mode:
        running_id, running = _running_job()
        if running:
            return {
                "ok": True,
                "accepted": True,
                "already_running": True,
                "job_id": running_id,
                "status": running.get("status"),
                "started_at": running.get("started_at"),
            }
        job_id = "invest-" + uuid.uuid4().hex[:12]
        _jobs[job_id] = {
            "status": "running",
            "started_ts": time.time(),
            "started_at": _now_string(),
            "params": {
                "notify": notify,
                "dry_run": dry_run,
                "limit": limit,
                "lookback_hours": lookback_hours,
            },
        }
        asyncio.create_task(_run_daily_job(
            job_id,
            notify=notify,
            dry_run=dry_run,
            limit=limit,
            lookback_hours=lookback_hours,
        ))
        return {"ok": True, "accepted": True, "already_running": False, "job_id": job_id}
    try:
        return await run_daily(notify=notify, dry_run=dry_run, limit=limit, lookback_hours=lookback_hours)
    except InvestConfigError as e:
        return {"ok": False, "error_type": "missing_config", "error": str(e)}
    except Exception as e:
        return {"ok": False, "error_type": "runtime", "error": str(e)}


@router.get("/jobs/{job_id}")
async def invest_job_status(job_id: str, authorization: str = Header(default="")):
    """Return in-process status for async investment assistant jobs."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if authorization[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")
    _cleanup_jobs()
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return {"ok": True, "job_id": job_id, **job}


@router.get("/config-check")
async def invest_config_check(authorization: str = Header(default="")):
    """Show which required investment-assistant configs are present without exposing values."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if authorization[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")
    keys = [
        "X_BEARER_TOKEN",
        "TWITTER_BEARER_TOKEN",
        "DEEPSEEK_API_KEY",
        "FEISHU_INVEST_ASSISTANT_APP_ID",
        "FEISHU_INVEST_ASSISTANT_APP_SECRET",
        "INVEST_NOTIFY_UNION_ID",
    ]
    return {
        "ok": True,
        "target_username": TARGET_USERNAME,
        "configured": {k: bool(os.environ.get(k)) for k in keys},
        "note": "Secret values are intentionally not returned.",
    }
