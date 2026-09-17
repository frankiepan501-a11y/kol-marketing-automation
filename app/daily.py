"""NYXI-first daily run and one-message Feishu receipt gate."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .clients import ApiError, FeishuClient
from .collector import IncrementalCollector, NYXI_OFFICIAL_CHANNEL_IDS, is_youtube_identity
from .constants import BASE_TOKEN, TABLES
from .core import BEIJING, parse_datetime, scalar

NYXI_CONFIG_ID = "recvrM7WDZ0ZV9"
REPORT_CHAT_ID = "oc_4ddd938ddb73201ed7354337eb2226ac"
REPORT_APP_ID = "cli_aa143b0a11b89be4"
RECEIPT_FIELD = "YouTube日报回执JSON"


def business_key(now: datetime) -> str:
    return f"{now.astimezone(BEIJING).date().isoformat()}|NYXI日报"


def _ledger(config: dict[str, Any]) -> dict[str, Any]:
    raw = scalar(config.get(RECEIPT_FIELD))
    try:
        data = json.loads(raw) if isinstance(raw, str) and raw else {}
    except ValueError:
        raise ApiError("feishu", "invalid_report_receipt", "daily report receipt is invalid JSON") from None
    if not isinstance(data, dict):
        raise ApiError("feishu", "invalid_report_receipt", "daily report receipt is not an object")
    return data


class DailyReporter:
    def __init__(self, feishu: FeishuClient):
        if feishu.app_id != REPORT_APP_ID:
            raise ApiError("feishu", "wrong_app", "daily report requires the KOL media assistant")
        self.feishu = feishu

    def _read(self) -> dict[str, Any]:
        return self.feishu.get_record(BASE_TOKEN, TABLES["keyword_config"], NYXI_CONFIG_ID)

    def _write(self, ledger: dict[str, Any]) -> None:
        self.feishu.batch_update(
            BASE_TOKEN, TABLES["keyword_config"],
            [(NYXI_CONFIG_ID, {RECEIPT_FIELD: json.dumps(ledger, ensure_ascii=False, separators=(",", ":"))})],
        )

    def begin(self, now: datetime) -> dict[str, Any]:
        key = business_key(now)
        config = self._read()
        ledger = _ledger(config)
        entry = ledger.get(key)
        if isinstance(entry, dict):
            return entry
        entry = {
            "state": "collecting",
            "first_attempt_at": now.astimezone(timezone.utc).isoformat(),
            "first_waterline": str(scalar(config.get("最近采集水位")) or ""),
        }
        ledger[key] = entry
        self._write(ledger)
        if _ledger(self._read()).get(key) != entry:
            raise ApiError("feishu", "receipt_readback", "first attempt receipt did not persist")
        return entry

    def today_posts(self, now: datetime, entry: dict[str, Any]) -> list[dict[str, Any]]:
        first = parse_datetime(entry["first_attempt_at"]).astimezone(timezone.utc)
        rows = self.feishu.list_records(BASE_TOKEN, TABLES["competitor_posts"])
        result = []
        for row in rows:
            if not is_youtube_identity(row, brand="NYXI"):
                continue
            created = row.get("创建时间") or row.get("_created_time")
            if created in (None, ""):
                raise ApiError("feishu", "created_time_missing", "daily post count cannot be verified")
            if parse_datetime(created).astimezone(timezone.utc) >= first:
                result.append(row)
        return result

    def known_channels_before(self, entry: dict[str, Any]) -> set[str]:
        first = parse_datetime(entry["first_attempt_at"]).astimezone(timezone.utc)
        rows = self.feishu.list_records(BASE_TOKEN, TABLES["competitor_posts"])
        known: set[str] = set()
        for row in rows:
            if not is_youtube_identity(row, brand="NYXI"):
                continue
            created = row.get("创建时间") or row.get("_created_time")
            if created in (None, ""):
                raise ApiError("feishu", "created_time_missing", "new channel count cannot be verified")
            if parse_datetime(created).astimezone(timezone.utc) < first:
                channel_id = str(scalar(row.get("KOL平台ID")) or "")
                if channel_id:
                    known.add(channel_id)
        return known

    def send_once(self, now: datetime, text: str, *, job_id: str, key: str | None = None) -> dict[str, Any]:
        key = key or business_key(now)
        ledger = _ledger(self._read())
        existing = ledger.get(key) or {}
        if existing.get("state") == "sent":
            return {"status": "already_sent", "message_id": existing.get("message_id"), "key": key}
        if existing.get("state") == "sending":
            raise ApiError("feishu", "send_outcome_unknown", "message may have been sent; manual reconciliation required")
        ledger[key] = {**existing, "state": "sending", "job_id": job_id}
        self._write(ledger)
        if _ledger(self._read()).get(key, {}).get("state") != "sending":
            raise ApiError("feishu", "receipt_readback", "sending receipt did not persist")
        message_id = self.feishu.send_text_to_chat(REPORT_CHAT_ID, text)
        ledger[key] = {
            **ledger[key], "state": "sent", "message_id": message_id,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write(ledger)
        return {"status": "sent", "message_id": message_id, "key": key}


def format_report(
    now: datetime, *, nyxi: dict[str, Any], backfill: dict[str, Any],
    posts: list[dict[str, Any]], weekly: dict[str, Any] | None = None,
    known_channel_ids: set[str] | None = None,
) -> str:
    date = now.astimezone(BEIJING).date().isoformat()
    lines = [f"🟡 [KOL·P2] NYXI YouTube 竞品监控日报 · {date}"]
    if nyxi.get("status") == "completed":
        nonofficial = {
            str(scalar(row.get("KOL平台ID")) or "")
            for row in posts
            if str(scalar(row.get("KOL平台ID")) or "") not in NYXI_OFFICIAL_CHANNEL_IDS
            and str(scalar(row.get("相关性")) or "") != "无关"
        } - {""} - (known_channel_ids or set())
        lines.extend([
            f"NYXI：采集成功；本日新增帖子 {len(posts)} 条，非官方新账号候选 {len(nonofficial)} 个。",
            f"近30天公开数据更新 {nyxi.get('updated_existing', 0)} 条；本次搜索请求 {nyxi.get('search_calls', 0)} 次。",
            "说明：关键词命中仅为合作候选，不代表已确认签约。",
        ])
        for row in posts[:10]:
            title = str(scalar(row.get("帖子标题")) or "")[:80]
            url = row.get("帖子URL")
            if isinstance(url, dict):
                url = url.get("link")
            lines.append(f"• {title} — {url or ''}")
        if len(posts) > 10:
            lines.append(f"其余 {len(posts)-10} 条见竞品帖子库。")
    else:
        lines.append(f"NYXI：采集失败；原因 {nyxi.get('error_type', '未知')}；成功水位未推进。")
    if weekly and weekly.get("status") == "failed":
        lines.append(f"旧帖周刷新：失败（{weekly.get('error_type', '未知')}），不影响已完成的日增量。")
    elif weekly:
        lines.append(
            f"旧帖周刷新：批次 {weekly.get('batch_index', 0)+1}/{weekly.get('batch_count', 0)}，"
            f"选中 {weekly.get('selected', 0)} 条，完成 {weekly.get('available', 0)} 条。"
        )
    lines.append(f"8BitDo：{backfill.get('message', '配额不可验证，历史补采暂停')}。")
    lines.append(f"任务编号：{nyxi.get('job_id', 'unknown')}")
    return "\n".join(lines)


def quota_decision() -> dict[str, Any]:
    """Fail closed until same-project Search Queries/day read access is verified."""
    return {"status": "quota_unknown", "message": "配额不可验证，历史补采暂停"}
