"""NYXI X full-history collector.

This module is isolated from the outreach pipeline. It reads public X data and
writes only the existing competitor-post/config tables. The KOL master table is
read for a before/after count only and is never mutated.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import httpx
from fastapi import APIRouter, Header, HTTPException, Query

from . import config, feishu


UTC = timezone.utc
X_API = "https://api.x.com/2"
PLATFORM_APP_ID = "7"
BRAND = "NYXI"
OFFICIAL_HANDLES = {"nyxigaming"}
HISTORY_VERSION = "nyxi-x-full-v1"
POST_TABLE_ID = os.environ.get("X_HISTORY_POST_TABLE_ID", "tblCDbvLtnLzdxEp")
CONFIG_TABLE_ID = os.environ.get("X_HISTORY_CONFIG_TABLE_ID", "tblgWfvdPgbkq541")
CONFIG_RECORD_ID = os.environ.get("X_HISTORY_CONFIG_RECORD_ID", "recvrM7WSAjzCF")
PROBE_QUERY = "from:NyxiGaming -is:retweet"
PROBE_START = "2024-01-01T00:00:00Z"
PROBE_END = "2024-01-02T00:00:00Z"
DEFAULT_START = datetime(2006, 3, 21, tzinfo=UTC)
DELIMITER = "；"

PRODUCT_TERMS = (
    "Hyperion 3", "Hyperion 2", "Hyperion Pro", "Hyperion", "Wizard 2 TMR",
    "Wizard", "Warrior Lite", "Warrior", "Master P1", "Chaos Pro", "NJ12",
    "Flexi", "Athena", "Striker", "Imperial",
)
EXCLUSION_TERMS = ("nyxi leon", "nyxi vex", "lady nyxi", "ergonomic office chair")

router = APIRouter(prefix="/x-history", tags=["x-history"])
_jobs: dict[str, dict[str, Any]] = {}
_JOB_TTL = 48 * 3600


@dataclass(frozen=True)
class QuerySpec:
    slug: str
    label: str
    query: str


@dataclass(frozen=True)
class SearchWindow:
    index: int
    spec: QuerySpec
    start: datetime
    end: datetime

    @property
    def start_iso(self) -> str:
        return _iso(self.start)

    @property
    def end_iso(self) -> str:
        return _iso(self.end)

    @property
    def label(self) -> str:
        if (
            self.start.month == 1
            and self.start.day == 1
            and self.end == datetime(self.start.year + 1, 1, 1, tzinfo=UTC)
        ):
            return str(self.start.year)
        return f"{self.start:%Y-%m-%d}~{(self.end - timedelta(seconds=1)):%Y-%m-%d}"


QUERY_SPECS = (
    QuerySpec("brand", "NYXI品牌词", '("NYXI" OR "#NYXI" OR "#NYXIGAME") -is:retweet'),
    QuerySpec("domain", "NYXI官网链接", "url:nyxigame.com -is:retweet"),
    QuerySpec(
        "hyperion_wizard_warrior",
        "Hyperion/Wizard/Warrior型号词",
        '("Hyperion 3" OR "Hyperion 2" OR "Hyperion Pro" OR "Wizard 2 TMR" OR "Warrior Lite") '
        '(controller OR joycon OR "joy-con" OR gamepad OR "Nintendo Switch") -is:retweet',
    ),
    QuerySpec(
        "other_models",
        "NYXI其他型号词",
        '("Master P1" OR "Chaos Pro" OR "NJ12" OR "NYXI Flexi" OR "NYXI Athena" OR "NYXI Striker" OR "NYXI Imperial") '
        '(controller OR joycon OR "joy-con" OR gamepad OR "Nintendo Switch" OR NYXI) -is:retweet',
    ),
    QuerySpec("official", "NYXI官方账号", "from:NyxiGaming -is:retweet"),
)


@dataclass
class XApiError(RuntimeError):
    status_code: int
    category: str
    message: str = ""

    def __str__(self) -> str:
        return f"X API error {self.status_code}: {self.category}"


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def history_end(now: datetime | None = None) -> datetime:
    """Stay behind X server time so the current partial window is accepted."""
    current = (now or datetime.now(UTC)).astimezone(UTC)
    return current - timedelta(seconds=30)


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except (TypeError, ValueError):
        return None


def _millis(value: Any) -> int | None:
    parsed = _parse_datetime(value)
    return int(parsed.timestamp() * 1000) if parsed else None


def _check_auth(authorization: str) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if authorization[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")


def _x_headers() -> dict[str, str]:
    token = os.environ.get("X_BEARER_TOKEN") or os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        raise XApiError(0, "missing_x_bearer_token")
    return {"Authorization": f"Bearer {token}"}


def _error_category(status_code: int) -> str:
    return {
        401: "credential_invalid",
        402: "credits_required",
        403: "full_archive_not_authorized",
        429: "rate_limited",
    }.get(status_code, "x_api_error")


async def _x_search_all(params: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            f"{X_API}/tweets/search/all",
            params=params,
            headers=_x_headers(),
        )
    if response.status_code >= 400:
        raise XApiError(response.status_code, _error_category(response.status_code))
    return response.json()


def build_year_windows(
    start: datetime,
    end: datetime,
    specs: Iterable[QuerySpec] = QUERY_SPECS,
) -> list[SearchWindow]:
    """Build deterministic, contiguous, end-exclusive yearly windows."""
    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    if end <= start:
        raise ValueError("end must be after start")
    windows: list[SearchWindow] = []
    for spec in specs:
        cursor = start
        while cursor < end:
            year_end = datetime(cursor.year + 1, 1, 1, tzinfo=UTC)
            window_end = min(year_end, end)
            windows.append(SearchWindow(len(windows), spec, cursor, window_end))
            cursor = window_end
    return windows


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _unique(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _split_evidence(value: Any) -> list[str]:
    if isinstance(value, list):
        return _unique(value)
    return _unique(re.split(r"[；;]\s*", str(value or "")))


def _field_text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("text") or value.get("link") or value.get("name") or "")
    if isinstance(value, list):
        return "".join(_field_text(item) for item in value)
    return str(value or "")


def _field_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return _unique(_field_text(item) for item in value)
    text = _field_text(value)
    return [text] if text else []


def _classify(text: str, username: str) -> tuple[str, int, str, str]:
    haystack = text.casefold()
    if username.casefold() in OFFICIAL_HANDLES:
        return "相关", 100, "NYXI官方账号；后续KOL合作统计应排除官方帖", ""
    for term in EXCLUSION_TERMS:
        if term in haystack:
            return "无关", 0, f"命中排除词：{term}", ""
    products = [term for term in PRODUCT_TERMS if term.casefold() in haystack]
    product_text = ", ".join(products)
    if "nyxi" in haystack and products:
        return "疑似", 85, f"命中NYXI品牌词及型号词：{product_text}", product_text
    if "nyxi" in haystack or "nyxigame.com" in haystack:
        return "疑似", 70, "命中NYXI品牌词或官网链接；待AI复核", product_text
    if products:
        return "疑似", 60, f"命中型号词：{product_text}；待AI复核", product_text
    return "待分析", 20, "查询命中但文本证据较弱；待AI复核", ""


def normalize_tweet(
    tweet: dict[str, Any],
    user: dict[str, Any],
    media_by_key: dict[str, dict[str, Any]],
    window: SearchWindow,
    batch_id: str,
    captured_at: str,
) -> dict[str, Any]:
    tweet_id = str(tweet.get("id") or "").strip()
    if not tweet_id:
        raise ValueError("tweet.id is required")
    username = str(user.get("username") or "").strip()
    text = str(tweet.get("text") or "")
    metrics = tweet.get("public_metrics") if isinstance(tweet.get("public_metrics"), dict) else {}
    user_metrics = user.get("public_metrics") if isinstance(user.get("public_metrics"), dict) else {}
    attachments = tweet.get("attachments") if isinstance(tweet.get("attachments"), dict) else {}
    media = [media_by_key[key] for key in attachments.get("media_keys") or [] if key in media_by_key]
    attachment_types = _unique(
        "image" if item.get("type") == "photo" else "video"
        for item in media
        if item.get("type") in ("photo", "video", "animated_gif")
    )
    thumbnail = ""
    for item in media:
        thumbnail = str(item.get("preview_image_url") or item.get("url") or "")
        if thumbnail:
            break
    relevance, score, reason, products = _classify(text, username)
    post_url = (
        f"https://x.com/{username}/status/{tweet_id}"
        if username
        else f"https://x.com/i/web/status/{tweet_id}"
    )
    source_for_hash = {"tweet": tweet, "user": user, "media": media}
    fields: dict[str, Any] = {
        "唯一键": f"{PLATFORM_APP_ID}:{tweet_id}",
        "竞品品牌": BRAND,
        "命中关键词": "nyxi",
        "平台": "X",
        "帖子ID": tweet_id,
        "帖子标题": "",
        "帖子内容": text,
        "发布时间": _millis(tweet.get("created_at")),
        "帖子URL": post_url,
        "缩略图URL": thumbnail,
        "附件类型": attachment_types,
        "KOL平台ID": str(user.get("id") or tweet.get("author_id") or ""),
        "KOL账号名": str(user.get("name") or ""),
        "KOL账号Handle": username,
        "KOL主页URL": f"https://x.com/{username}" if username else "",
        "粉丝数快照": int(user_metrics.get("followers_count") or 0),
        "关注数快照": int(user_metrics.get("following_count") or 0),
        "帖子数快照": int(user_metrics.get("tweet_count") or 0),
        "曝光量": int(metrics.get("impression_count") or 0),
        "点赞数": int(metrics.get("like_count") or 0),
        "评论数": int(metrics.get("reply_count") or 0),
        "分享数": int(metrics.get("retweet_count") or 0),
        "收藏数": int(metrics.get("bookmark_count") or 0),
        "引用数": int(metrics.get("quote_count") or 0),
        "覆盖量": int(metrics.get("impression_count") or 0),
        "抓取时间": _millis(captured_at),
        "采集批次ID": batch_id,
        "原始数据哈希": _stable_hash(source_for_hash),
        "采集来源": ["X API"],
        "X命中查询词": window.spec.label,
        "X查询时间窗": window.label,
        "相关性": relevance,
        "AI相关性分": score,
        "AI判断依据": reason,
        "提及产品系列": products,
        "内容类型": "待分析",
        "合作信号": "待分析",
        "营销阶段": "待分析",
        "AI分析状态": "待分析",
        "人工复核状态": "待复核",
        "关联监控任务": [{"id": CONFIG_RECORD_ID}],
    }
    return {key: value for key, value in fields.items() if value not in (None, "", [])}


async def collect_window(
    window: SearchWindow,
    batch_id: str,
    captured_at: str,
    max_pages: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    next_token = ""
    calls = 0
    while True:
        params: dict[str, Any] = {
            "query": window.spec.query,
            "start_time": window.start_iso,
            "end_time": window.end_iso,
            "max_results": 100,
            "expansions": "author_id,attachments.media_keys",
            "tweet.fields": "id,text,author_id,created_at,public_metrics,lang,entities,attachments",
            "user.fields": "id,name,username,created_at,description,location,public_metrics,url,verified,verified_type",
            "media.fields": "media_key,type,url,preview_image_url,width,height,public_metrics",
        }
        if next_token:
            params["next_token"] = next_token
        payload = await _x_search_all(params)
        calls += 1
        includes = payload.get("includes") if isinstance(payload.get("includes"), dict) else {}
        users = {str(item.get("id")): item for item in includes.get("users") or [] if isinstance(item, dict)}
        media = {str(item.get("media_key")): item for item in includes.get("media") or [] if isinstance(item, dict)}
        for tweet in payload.get("data") or []:
            if not isinstance(tweet, dict):
                continue
            user = users.get(str(tweet.get("author_id") or ""), {})
            rows.append(normalize_tweet(tweet, user, media, window, batch_id, captured_at))
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        next_token = str(meta.get("next_token") or "")
        if not next_token or (max_pages and calls >= max_pages):
            break
    return merge_candidate_rows(rows), calls


def merge_candidate_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("唯一键") or "")
        if not key:
            continue
        if key not in merged:
            merged[key] = dict(row)
            continue
        previous = merged[key]
        queries = _unique(_split_evidence(previous.get("X命中查询词")) + _split_evidence(row.get("X命中查询词")))
        windows = _unique(_split_evidence(previous.get("X查询时间窗")) + _split_evidence(row.get("X查询时间窗")))
        sources = _unique(_field_list(previous.get("采集来源")) + _field_list(row.get("采集来源")))
        types = _unique(_field_list(previous.get("附件类型")) + _field_list(row.get("附件类型")))
        previous.update(row)
        previous["X命中查询词"] = DELIMITER.join(queries)
        previous["X查询时间窗"] = DELIMITER.join(windows)
        previous["采集来源"] = sources
        if types:
            previous["附件类型"] = types
    return list(merged.values())


EXISTING_FIELDS = [
    "唯一键", "采集来源", "X命中查询词", "X查询时间窗", "原始数据哈希",
    "帖子内容", "帖子URL", "缩略图URL", "附件类型", "KOL平台ID", "KOL账号名",
    "KOL账号Handle", "KOL主页URL", "粉丝数快照", "关注数快照", "帖子数快照",
    "曝光量", "点赞数", "评论数", "分享数", "收藏数", "引用数", "覆盖量",
]

MUTABLE_UPDATE_FIELDS = {
    "帖子内容", "帖子URL", "缩略图URL", "附件类型", "KOL平台ID", "KOL账号名",
    "KOL账号Handle", "KOL主页URL", "粉丝数快照", "关注数快照", "帖子数快照",
    "曝光量", "点赞数", "评论数", "分享数", "收藏数", "引用数", "覆盖量",
    "抓取时间", "采集批次ID", "原始数据哈希", "采集来源", "X命中查询词",
    "X查询时间窗", "关联监控任务",
}


async def _load_existing_index() -> tuple[dict[str, dict[str, Any]], int]:
    records = await feishu.fetch_all_records(POST_TABLE_ID, field_names=EXISTING_FIELDS, page_size=500)
    index: dict[str, dict[str, Any]] = {}
    duplicate_keys = 0
    for record in records:
        fields = record.get("fields") or {}
        key = _field_text(fields.get("唯一键")).strip()
        if not key.startswith(f"{PLATFORM_APP_ID}:"):
            continue
        if key in index:
            duplicate_keys += 1
            continue
        index[key] = {"record_id": record.get("record_id") or record.get("id") or "", "fields": fields}
    return index, duplicate_keys


def _merge_for_update(row: dict[str, Any], old_fields: dict[str, Any]) -> dict[str, Any]:
    candidate = {key: value for key, value in row.items() if key in MUTABLE_UPDATE_FIELDS}
    candidate["采集来源"] = _unique(_field_list(old_fields.get("采集来源")) + _field_list(row.get("采集来源")))
    candidate["X命中查询词"] = DELIMITER.join(
        _unique(_split_evidence(old_fields.get("X命中查询词")) + _split_evidence(row.get("X命中查询词")))
    )
    candidate["X查询时间窗"] = DELIMITER.join(
        _unique(_split_evidence(old_fields.get("X查询时间窗")) + _split_evidence(row.get("X查询时间窗")))
    )
    return {key: value for key, value in candidate.items() if value not in (None, "", [])}


def _comparable(value: Any) -> Any:
    if isinstance(value, list):
        return sorted(_field_list(value))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


async def _feishu_json_once(method: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
    """One-shot write: never blindly retry an uncertain POST/PUT response."""
    token = await feishu.token("bitable")
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.request(
            method,
            f"https://open.feishu.cn/open-apis{path}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
            json=body,
        )
    if response.status_code >= 400:
        raise RuntimeError(f"Feishu write {method} {path} -> HTTP {response.status_code}")
    payload = response.json()
    if int(payload.get("code") or 0) != 0:
        raise RuntimeError(f"Feishu write {method} {path} -> code {payload.get('code')}")
    return payload


async def _batch_create_once(rows: list[dict[str, Any]]) -> list[str]:
    record_ids: list[str] = []
    for start in range(0, len(rows), 100):
        chunk = rows[start:start + 100]
        payload = await _feishu_json_once(
            "POST",
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{POST_TABLE_ID}/records/batch_create",
            {"records": [{"fields": row} for row in chunk]},
        )
        records = (payload.get("data") or {}).get("records") or []
        ids = [str(record.get("record_id") or record.get("id") or "") for record in records]
        if len(ids) != len(chunk) or not all(ids):
            raise RuntimeError(f"Feishu batch_create returned {len(ids)} ids for {len(chunk)} rows")
        record_ids.extend(ids)
    return record_ids


async def _batch_update_once(records: list[dict[str, Any]]) -> None:
    for start in range(0, len(records), 100):
        chunk = records[start:start + 100]
        await _feishu_json_once(
            "POST",
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{POST_TABLE_ID}/records/batch_update",
            {"records": chunk},
        )


async def upsert_rows(
    rows: list[dict[str, Any]],
    existing: dict[str, dict[str, Any]],
    *,
    commit: bool,
) -> dict[str, Any]:
    creates: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    update_keys: list[str] = []
    unchanged = 0
    for row in merge_candidate_rows(rows):
        key = str(row.get("唯一键") or "")
        current = existing.get(key)
        if not current:
            creates.append(row)
            continue
        old_fields = current.get("fields") or {}
        merged = _merge_for_update(row, old_fields)
        patch = {
            name: value
            for name, value in merged.items()
            if _comparable(value) != _comparable(old_fields.get(name))
        }
        if patch:
            updates.append({"record_id": current.get("record_id"), "fields": patch})
            update_keys.append(key)
        else:
            unchanged += 1

    result = {
        "would_create": len(creates),
        "would_update": len(updates),
        "created": 0,
        "updated": 0,
        "unchanged": unchanged,
        "created_keys": [],
        "updated_keys": [],
    }
    if not commit:
        return result

    if creates:
        ids = await _batch_create_once(creates)
        for row, record_id in zip(creates, ids):
            existing[row["唯一键"]] = {"record_id": record_id, "fields": dict(row)}
        result["created"] = len(ids)
        result["created_keys"] = [row["唯一键"] for row in creates]
    if updates:
        await _batch_update_once(updates)
        for item in updates:
            for value in existing.values():
                if value.get("record_id") == item["record_id"]:
                    value.setdefault("fields", {}).update(item["fields"])
                    break
        result["updated"] = len(updates)
        result["updated_keys"] = update_keys
    return result


async def _table_total(table_id: str) -> int:
    response = await feishu.api(
        "GET",
        f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records?page_size=1",
        which="bitable",
    )
    return int((response.get("data") or {}).get("total") or 0)


async def _load_checkpoint() -> int:
    try:
        record = await feishu.get_record(CONFIG_TABLE_ID, CONFIG_RECORD_ID)
        raw = _field_text((record.get("fields") or {}).get("X历史进度"))
        data = json.loads(raw) if raw else {}
        if data.get("version") == HISTORY_VERSION:
            return max(0, int(data.get("next_index") or 0))
    except Exception:
        return 0
    return 0


async def _update_config(fields: dict[str, Any]) -> None:
    await _feishu_json_once(
        "PUT",
        f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{CONFIG_TABLE_ID}/records/{CONFIG_RECORD_ID}",
        {"fields": fields},
    )


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


async def run_history(
    *,
    commit: bool,
    resume: bool,
    start_index: int,
    max_windows: int,
    max_pages_per_window: int,
    job_id: str = "",
) -> dict[str, Any]:
    captured_at = _iso(datetime.now(UTC))
    batch_id = "xhist-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    end = history_end()
    windows = build_year_windows(DEFAULT_START, end)
    checkpoint = await _load_checkpoint() if commit and resume else 0
    effective_start = max(0, start_index, checkpoint)
    selected = windows[effective_start:]
    if max_windows:
        selected = selected[:max_windows]
    existing, existing_duplicate_keys = await _load_existing_index()
    kol_before = await _table_total(config.T_KOL) if config.T_KOL else 0
    all_rows: list[dict[str, Any]] = []
    total_calls = 0
    created_keys: set[str] = set()
    updated_keys: set[str] = set()
    occurrence_count = 0

    for position, window in enumerate(selected, start=1):
        if job_id and (_jobs.get(job_id) or {}).get("cancel_requested"):
            return {
                "ok": False,
                "status": "cancelled",
                "commit": commit,
                "batch_id": batch_id,
                "windows_completed": position - 1,
                "next_index": window.index,
            }
        rows, calls = await collect_window(
            window,
            batch_id,
            captured_at,
            max_pages=max_pages_per_window,
        )
        occurrence_count += len(rows)
        total_calls += calls
        all_rows.extend(rows)
        if commit:
            written = await upsert_rows(rows, existing, commit=True)
            created_keys.update(written["created_keys"])
            updated_keys.update(written["updated_keys"])
            checkpoint_data = {
                "version": HISTORY_VERSION,
                "next_index": window.index + 1,
                "total_windows": len(windows),
                "last_window": f"{window.spec.slug}:{window.label}",
                "updated_at": captured_at,
            }
            await _update_config({
                "X历史进度": json.dumps(checkpoint_data, ensure_ascii=False, separators=(",", ":")),
                "运行状态": "正常",
                "错误摘要": "",
            })
        if job_id and job_id in _jobs:
            _jobs[job_id]["progress"] = {
                "window": position,
                "selected_windows": len(selected),
                "absolute_index": window.index,
                "query_group": window.spec.slug,
                "time_window": window.label,
                "query_calls": total_calls,
                "candidate_occurrences": occurrence_count,
            }

    unique_rows = merge_candidate_rows(all_rows)
    dry_plan = await upsert_rows(unique_rows, existing, commit=False) if not commit else None
    if commit:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        distinct_authors = {
            str(row.get("KOL平台ID"))
            for row in unique_rows
            if row.get("KOL平台ID") and str(row.get("KOL账号Handle") or "").casefold() not in OFFICIAL_HANDLES
        }
        await _update_config({
            "最近成功采集时间": now_ms,
            "最近采集水位": now_ms,
            "最近新增帖子数": len(created_keys),
            "最近新增KOL候选数": len(distinct_authors),
            "运行状态": "正常",
            "错误摘要": "",
        })
    kol_after = await _table_total(config.T_KOL) if config.T_KOL else 0
    published = [int(row["发布时间"]) for row in unique_rows if row.get("发布时间")]
    return {
        "ok": True,
        "status": "success",
        "commit": commit,
        "batch_id": batch_id,
        "history_version": HISTORY_VERSION,
        "full_archive_start": _iso(DEFAULT_START),
        "full_archive_end": _iso(end),
        "start_index": effective_start,
        "total_windows": len(windows),
        "windows_completed": len(selected),
        "next_index": selected[-1].index + 1 if selected else effective_start,
        "query_calls": total_calls,
        "candidate_occurrences": occurrence_count,
        "unique_posts": len(unique_rows),
        "earliest_post": _iso(datetime.fromtimestamp(min(published) / 1000, UTC)) if published else "",
        "latest_post": _iso(datetime.fromtimestamp(max(published) / 1000, UTC)) if published else "",
        "would_create": dry_plan["would_create"] if dry_plan else len(created_keys),
        "would_update": dry_plan["would_update"] if dry_plan else len(updated_keys),
        "created": len(created_keys),
        "updated": len(updated_keys),
        "existing_x_posts_before": len(existing) - len(created_keys),
        "existing_duplicate_keys": existing_duplicate_keys,
        "kol_master_before": kol_before,
        "kol_master_after": kol_after,
        "kol_master_unchanged": kol_before == kol_after,
        "official_posts": sum(
            1 for row in unique_rows if str(row.get("KOL账号Handle") or "").casefold() in OFFICIAL_HANDLES
        ),
        "distinct_nonofficial_authors": len({
            str(row.get("KOL平台ID"))
            for row in unique_rows
            if row.get("KOL平台ID") and str(row.get("KOL账号Handle") or "").casefold() not in OFFICIAL_HANDLES
        }),
        "writes_performed": (len(created_keys) + len(updated_keys)) if commit else 0,
    }


async def _run_job(job_id: str, params: dict[str, Any]) -> None:
    try:
        result = await run_history(job_id=job_id, **params)
        _jobs[job_id].update(status=result.get("status", "success"), finished_at=_iso(datetime.now(UTC)), result=result)
    except Exception as exc:
        error = str(exc)
        _jobs[job_id].update(status="error", finished_at=_iso(datetime.now(UTC)), error=error)
        if params.get("commit"):
            try:
                await _update_config({"运行状态": "失败", "错误摘要": error[:500]})
            except Exception:
                pass


@router.get("/probe")
async def probe_full_archive(authorization: str = Header(default="")) -> dict[str, Any]:
    """Probe X full-archive access without writing data or exposing results."""
    _check_auth(authorization)
    try:
        payload = await _x_search_all({
            "query": PROBE_QUERY,
            "start_time": PROBE_START,
            "end_time": PROBE_END,
            "max_results": 10,
            "tweet.fields": "id",
        })
    except XApiError as exc:
        return {
            "ok": False,
            "full_archive_supported": False,
            "reason": exc.category,
            "http_status": exc.status_code,
            "writes_performed": 0,
        }
    meta = payload.get("meta") or {}
    return {
        "ok": True,
        "full_archive_supported": True,
        "result_count": int(meta.get("result_count") or 0),
        "writes_performed": 0,
    }


@router.post("/run")
async def start_history_job(
    authorization: str = Header(default=""),
    commit: bool = Query(False),
    resume: bool = Query(True),
    start_index: int = Query(0, ge=0),
    max_windows: int = Query(0, ge=0, le=500),
    max_pages_per_window: int = Query(0, ge=0, le=1000),
    async_mode: bool = Query(True),
) -> dict[str, Any]:
    _check_auth(authorization)
    if commit and max_pages_per_window:
        raise HTTPException(400, "commit cannot use max_pages_per_window; capped pages would create an incomplete checkpoint")
    params = {
        "commit": commit,
        "resume": resume,
        "start_index": start_index,
        "max_windows": max_windows,
        "max_pages_per_window": max_pages_per_window,
    }
    if not async_mode:
        return await run_history(**params)
    running_id, running = _running_job()
    if running:
        return {"ok": True, "accepted": True, "already_running": True, "job_id": running_id}
    job_id = "xhist-" + uuid.uuid4().hex[:12]
    _jobs[job_id] = {
        "status": "running",
        "started_ts": time.time(),
        "started_at": _iso(datetime.now(UTC)),
        "params": params,
        "cancel_requested": False,
    }
    asyncio.create_task(_run_job(job_id, params))
    return {"ok": True, "accepted": True, "already_running": False, "job_id": job_id}


@router.get("/jobs/{job_id}")
async def get_history_job(job_id: str, authorization: str = Header(default="")) -> dict[str, Any]:
    _check_auth(authorization)
    _cleanup_jobs()
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return {"ok": True, "job_id": job_id, **job}


@router.post("/jobs/{job_id}/stop")
async def stop_history_job(job_id: str, authorization: str = Header(default="")) -> dict[str, Any]:
    _check_auth(authorization)
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    job["cancel_requested"] = True
    return {"ok": True, "job_id": job_id, "cancel_requested": True}


@router.get("/status")
async def history_status(authorization: str = Header(default="")) -> dict[str, Any]:
    _check_auth(authorization)
    _cleanup_jobs()
    running_id, running = _running_job()
    return {
        "ok": True,
        "history_version": HISTORY_VERSION,
        "query_groups": [spec.slug for spec in QUERY_SPECS],
        "running_job_id": running_id,
        "running": bool(running),
        "progress": (running or {}).get("progress") or {},
        "post_table": POST_TABLE_ID,
        "config_record": CONFIG_RECORD_ID,
        "kol_master_write_enabled": False,
    }
