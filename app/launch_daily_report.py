"""新品集中宣发活动日报（只读）。

本模块只读取活动、参与记录、邮件草稿和 Zoho 发件箱计数，生成飞书
Card JSON 2.0。它不会补池、建草稿、发邮件或修改飞书业务表。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any

from . import auto_send, config, feishu
from .feishu import ext


BJ = timezone(timedelta(hours=8))
ACTIVE_ACTIVITY_MODE = "正式运行"
ACTIVE_ACTIVITY_STATUS = "正式执行中"
ACTIVE_PARTICIPANT_STATES = {"已入围", "锁定准备中"}
APPROVED_REVIEW_STATES = {"通过"}
READY_DRAFT_STATES = {"自动通过", "通过"}
SENT_STATES = {"已发", "已发送"}
CURRENT_GROUP_CHAT_ID = "oc_8b71a652a25ec0dd1c8af2c53e86ed93"
FRANKIE_OPEN_ID = "ou_629ce01f4bc31de078e10fcb038dbf78"
VALID_STATUS_COLORS = {"red", "green", "orange", "yellow"}

# 同一服务进程内的发送回执。防止网关重试或运营重复点击导致同日同对象重复发卡；
# 服务重启后由上层后台 job 的 request_key 继续兜住普通重试。
_SEND_RECEIPTS: dict[str, str] = {}
_SEND_LOCK = asyncio.Lock()
REPORT_RECEIPT_PREFIX = "[LAUNCH_DAILY_REPORT]"

ACTIVITY_FIELDS = [
    "活动ID", "活动名称", "品牌", "运行模式", "状态", "目标上稿数", "窗口结束",
    "数据口径备注",
]
PARTICIPANT_FIELDS = [
    "活动ID", "参与状态", "审核结论", "系统审核分流", "审核时间",
    "关联KOL", "产品家族ID", "关联邮件草稿", "承诺上稿时间", "实际上稿时间",
]
DRAFT_FIELDS = [
    "邮件草稿来源", "邮件草稿状态", "发送状态", "建议发送时间",
    "生成时间", "发送时间", "是否回复", "回复日期", "回复意图", "寄样阶段",
    "回复原文", "回复目标MsgID", "审核路径", "卡片已标记已审", "关联KOL", "关联产品",
    "集中宣发活动ID", "活动归属状态",
]
POSITIVE_REPLY_INTENTS = {"感兴趣", "要报价"}
SHIPPED_STAGES = {"已发货", "在途", "已签收", "已产出"}


class DailyReportError(RuntimeError):
    """日报数据、卡片结构或发送安全闸不满足。"""


def _text(value: Any) -> str:
    return str(ext(value) or "").strip()


def _number(value: Any) -> int:
    if isinstance(value, dict):
        value = value.get("value") or value.get("text") or 0
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _ts(value: Any) -> int:
    return max(0, _number(value))


def _ids(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, dict):
        return [
            str(item) for item in (
                value.get("link_record_ids") or value.get("record_ids") or []
            ) if item
        ]
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            if isinstance(item, str) and item:
                out.append(item)
            elif isinstance(item, dict):
                out.extend(_ids(item))
        return out
    return []


def _bounded_ids(values: set[str] | list[str], *, limit: int = 3) -> str:
    ordered = sorted({str(value) for value in values if value})
    shown = "、".join(ordered[:limit])
    remaining = len(ordered) - limit
    return f"{shown}（另{remaining}项）" if remaining > 0 else shown


def _in_day(value: Any, day_start_ms: int, day_end_ms: int) -> bool:
    stamp = _ts(value)
    return day_start_ms <= stamp < day_end_ms


def _is_active_activity(record: dict) -> bool:
    fields = record.get("fields") or {}
    return (
        _text(fields.get("运行模式")) == ACTIVE_ACTIVITY_MODE
        and _text(fields.get("状态")) == ACTIVE_ACTIVITY_STATUS
    )


def _is_active_participant(record: dict) -> bool:
    fields = record.get("fields") or {}
    return (
        _text(fields.get("参与状态")) in ACTIVE_PARTICIPANT_STATES
        and _text(fields.get("审核结论")) in APPROVED_REVIEW_STATES
    )


def _draft_identity_parts(record: dict) -> tuple[set[str], set[str]]:
    fields = record.get("fields") or {}
    return set(_ids(fields.get("关联KOL"))), set(_ids(fields.get("关联产品")))


def _draft_identity_keys(record: dict) -> set[tuple[str, str]]:
    """仅返回唯一KOL＋唯一产品；多值或缺值时不做笛卡尔积猜测。"""
    contact_ids, product_ids = _draft_identity_parts(record)
    if len(contact_ids) != 1 or len(product_ids) != 1:
        return set()
    return {(next(iter(contact_ids)), next(iter(product_ids)))}


def _source_reply_message_id(record: dict) -> str:
    body = _text((record.get("fields") or {}).get("回复原文"))
    match = re.match(r"^\[MID:([^\]]+)\]", body)
    return match.group(1).strip() if match else ""


def _reply_target_message_id(record: dict) -> str:
    return _text((record.get("fields") or {}).get("回复目标MsgID"))


def _campaign_reply_evidence(
    participants: list[dict],
    drafts_by_id: dict[str, dict],
    *,
    excluded_draft_ids: set[str] | None = None,
) -> dict[str, Any]:
    """用活动直接关联cold上的入站MID建立reply的唯一归属证据。"""
    excluded = excluded_draft_ids or set()
    raw_sources: dict[str, list[tuple[str, tuple[str, str]]]] = {}
    candidate_keys: set[tuple[str, str]] = set()
    invalid_identity_source_ids: set[str] = set()
    shared_source_message_ids: dict[str, str] = {}
    duplicate_source_message_ids: set[str] = set()
    duplicate_source_draft_ids: set[str] = set()
    invalid_keys: set[tuple[str, str]] = set()
    invalid_message_ids: set[str] = set()
    participant_keys_by_draft: dict[str, list[tuple[str, str] | None]] = {}
    for participant in participants:
        if not _is_active_participant(participant):
            continue
        fields = participant.get("fields") or {}
        contact_ids = set(_ids(fields.get("关联KOL")))
        product_id = _text(fields.get("产品家族ID"))
        participant_key = (
            (next(iter(contact_ids)), product_id)
            if len(contact_ids) == 1 and product_id else None
        )
        if participant_key:
            candidate_keys.add(participant_key)
        for draft_id in _ids(fields.get("关联邮件草稿")):
            if draft_id in excluded:
                continue
            participant_keys_by_draft.setdefault(draft_id, []).append(participant_key)

    for draft_id, participant_keys in participant_keys_by_draft.items():
        draft = drafts_by_id.get(draft_id) or {}
        draft_fields = draft.get("fields") or {}
        if _text(draft_fields.get("邮件草稿来源")).lower() != "cold":
            continue
        draft_keys = _draft_identity_keys(draft)
        draft_key = next(iter(draft_keys)) if len(draft_keys) == 1 else None
        message_id = _source_reply_message_id(draft)
        replied = bool(draft_fields.get("是否回复")) or bool(message_id)
        if not replied:
            continue
        shared_source = len(participant_keys) != 1
        participant_key = participant_keys[0] if len(participant_keys) == 1 else None
        if (
            shared_source
            or not participant_key
            or not draft_key
            or draft_key != participant_key
            or not message_id
        ):
            invalid_identity_source_ids.add(draft_id)
            invalid_keys.update(key for key in participant_keys if key)
            if draft_key:
                invalid_keys.add(draft_key)
            if message_id:
                invalid_message_ids.add(message_id)
            if shared_source:
                shared_source_message_ids[draft_id] = message_id
            continue
        raw_sources.setdefault(message_id, []).append((draft_id, draft_key))

    sources: dict[str, tuple[str, str]] = {}
    source_ids: dict[str, str] = {}
    for message_id, rows in raw_sources.items():
        if len(rows) != 1:
            duplicate_source_message_ids.add(message_id)
            duplicate_source_draft_ids.update(draft_id for draft_id, _ in rows)
            invalid_message_ids.add(message_id)
            invalid_keys.update(key for _, key in rows)
            continue
        sources[message_id] = rows[0][1]
        source_ids[message_id] = rows[0][0]
    return {
        "sources": sources,
        "source_ids": source_ids,
        "candidate_keys": candidate_keys,
        "invalid_identity_source_ids": invalid_identity_source_ids,
        "shared_source_message_ids": shared_source_message_ids,
        "duplicate_source_message_ids": duplicate_source_message_ids,
        "duplicate_source_draft_ids": duplicate_source_draft_ids,
        "invalid_keys": invalid_keys,
        "invalid_message_ids": invalid_message_ids,
    }


def _is_live_human_reply_draft(record: dict) -> bool:
    """当前仍需运营处理的reply草稿；历史回复意图不属于实时队列。"""
    fields = record.get("fields") or {}
    return (
        _text(fields.get("邮件草稿来源")).lower() == "reply"
        and _text(fields.get("邮件草稿状态")) == "待审"
        and _text(fields.get("审核路径")) == "待人审"
        and not bool(fields.get("卡片已标记已审"))
    )


def _reply_matches_campaign(
    record: dict,
    evidence: dict[str, Any],
    campaign_id: str = "",
) -> bool:
    fields = record.get("fields") or {}
    explicit_campaign = _text(fields.get("集中宣发活动ID"))
    explicit_status = _text(fields.get("活动归属状态"))
    if explicit_status in {"已确认", "自动确认"}:
        return bool(campaign_id and explicit_campaign == campaign_id)
    if explicit_status == "无需归属":
        return False
    keys = _draft_identity_keys(record)
    if len(keys) != 1:
        return False
    key = next(iter(keys))
    message_id = _reply_target_message_id(record)
    return bool(message_id and evidence["sources"].get(message_id) == key)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, min(100.0, round(numerator * 100.0 / denominator, 1)))


def status_for(snapshot: dict) -> dict:
    """按红 > 绿 > 橙 > 黄固定优先级返回状态与系统动作。"""
    errors = list(snapshot.get("data_errors") or [])
    sent = _number(snapshot.get("quota_sent_24h"))
    cap = _number(snapshot.get("quota_cap"))
    target = _number(snapshot.get("target_posts"))
    on_time = _number(snapshot.get("on_time_posts"))
    ready = _number(snapshot.get("ready_due"))
    remaining = max(0, _number(snapshot.get("quota_remaining")))

    if errors or cap < 1 or target < 1 or sent > cap:
        if sent > cap:
            errors.append(f"邮箱滚动24小时发送 {sent} 封，超过硬上限 {cap} 封")
        return {
            "label": "数据异常",
            "color": "red",
            "next_action": "暂停自动判断并修复数据或额度计数，避免错误放量。",
            "reasons": list(dict.fromkeys(errors)),
        }

    green_threshold = min(12, remaining)
    if on_time >= target:
        return {
            "label": "目标已达成",
            "color": "green",
            "next_action": "继续跟进已承诺对象，不再为达标数量盲目扩池。",
            "reasons": [],
        }
    if ready >= green_threshold:
        return {
            "label": "进度正常",
            "color": "green",
            "next_action": "继续从已审核池自动发送；可发送池低于阈值时自动补池。",
            "reasons": [],
        }
    if remaining >= 12 and ready == 0:
        return {
            "label": "进度落后",
            "color": "orange",
            "next_action": "立即自动补池并生成可发送草稿；不降低国家、语言和内容匹配标准。",
            "reasons": [],
        }
    return {
        "label": "需要关注",
        "color": "yellow",
        "next_action": "继续发送并补充可发送库存；待审核对象只由运营审核边界项。",
        "reasons": [],
    }


def summarize_campaign(
    activity: dict,
    participants: list[dict],
    drafts_by_id: dict[str, dict],
    *,
    quota: dict,
    now_ms: int,
    day_start_ms: int,
    day_end_ms: int,
    quota_error: str = "",
    excluded_draft_ids: set[str] | None = None,
    source_errors: list[str] | None = None,
    source_diagnostics: list[dict[str, Any]] | None = None,
) -> dict:
    """按已确认口径汇总一个活动，不产生任何写入。"""
    activity_fields = activity.get("fields") or {}
    campaign_id = _text(activity_fields.get("活动ID"))
    name = _text(activity_fields.get("活动名称"))
    brand = _text(activity_fields.get("品牌")).upper()
    target_posts = _number(activity_fields.get("目标上稿数"))
    window_end = _ts(activity_fields.get("窗口结束"))

    data_errors: list[str] = []
    if not campaign_id:
        data_errors.append("活动ID缺失")
    if not brand:
        data_errors.append("品牌缺失")
    if target_posts < 1:
        data_errors.append("目标上稿数缺失或小于1")
    if window_end < 1:
        data_errors.append("窗口结束时间缺失")
    if quota_error:
        data_errors.append(f"{brand or '未知品牌'} Zoho计数失败：{quota_error}")
    data_errors.extend(source_errors or [])

    active = [record for record in participants if _is_active_participant(record)]
    today_eligible = 0
    eligible_time_missing = 0
    draft_ids: set[str] = set()
    commitments = 0
    actual_posts = 0
    on_time_posts = 0
    awaiting_post_date = 0
    shipped = 0
    excluded = excluded_draft_ids or set()

    for participant in active:
        fields = participant.get("fields") or {}
        audit_time = _ts(fields.get("审核时间"))
        if not audit_time and _text(fields.get("系统审核分流")) == "系统建议通过":
            audit_time = _ts(participant.get("created_time"))
        if audit_time:
            today_eligible += int(day_start_ms <= audit_time < day_end_ms)
        else:
            eligible_time_missing += 1
        participant_draft_ids = _ids(fields.get("关联邮件草稿"))
        draft_ids.update(participant_draft_ids)
        committed = _ts(fields.get("承诺上稿时间")) > 0
        commitments += int(committed)
        actual = _ts(fields.get("实际上稿时间"))
        if actual:
            actual_posts += 1
            on_time_posts += int(window_end > 0 and actual <= window_end)

        linked_drafts = [
            drafts_by_id.get(draft_id) or {}
            for draft_id in participant_draft_ids
            if draft_id not in excluded
        ]
        shipped += int(any(
            _text((draft.get("fields") or {}).get("寄样阶段")) in SHIPPED_STAGES
            for draft in linked_drafts
        ))
        if committed or actual:
            continue
        reply_intents = [
            _text(draft_fields.get("回复意图"))
            for draft in linked_drafts
            for draft_fields in [draft.get("fields") or {}]
            if _text(draft_fields.get("邮件草稿来源")).lower() == "cold"
            and bool(draft_fields.get("是否回复"))
        ]
        if any(intent in POSITIVE_REPLY_INTENTS for intent in reply_intents):
            awaiting_post_date += 1

    campaign_evidence = _campaign_reply_evidence(
        active, drafts_by_id, excluded_draft_ids=excluded,
    )
    reply_pending = sum(
        1
        for draft_id, draft in drafts_by_id.items()
        if draft_id not in excluded
        and _is_live_human_reply_draft(draft)
        and _reply_matches_campaign(draft, campaign_evidence, campaign_id)
    )

    draft_ids.difference_update(excluded_draft_ids or set())

    sent_total = 0
    sent_today = 0
    replies_total = 0
    replies_today = 0
    ready_due = 0
    for draft_id in sorted(draft_ids):
        draft = drafts_by_id.get(draft_id) or {}
        fields = draft.get("fields") or {}
        if _text(fields.get("邮件草稿来源")).lower() != "cold":
            continue
        send_state = _text(fields.get("发送状态"))
        if send_state in SENT_STATES:
            sent_total += 1
            sent_today += int(_in_day(fields.get("发送时间"), day_start_ms, day_end_ms))
            replied = bool(fields.get("是否回复"))
            replies_total += int(replied)
            if replied:
                replies_today += int(_in_day(fields.get("回复日期"), day_start_ms, day_end_ms))
            continue
        if (
            _text(fields.get("邮件草稿状态")) in READY_DRAFT_STATES
            and send_state in {"", "未发"}
        ):
            scheduled = _ts(fields.get("建议发送时间"))
            ready_due += int(not scheduled or scheduled <= now_ms)

    quota_sent = max(0, _number(quota.get("sent_24h")))
    quota_cap = max(0, _number(quota.get("cap")))
    snapshot = {
        "campaign_id": campaign_id,
        "name": name or campaign_id or "未命名活动",
        "brand": brand,
        "today_eligible": today_eligible,
        "eligible_total": len(active),
        "eligible_time_missing": eligible_time_missing,
        "ready_due": ready_due,
        "sent_today": sent_today,
        "sent_total": sent_total,
        "replies_today": replies_today,
        "replies_total": replies_total,
        "reply_pending": reply_pending,
        "awaiting_post_date": awaiting_post_date,
        "commitments": commitments,
        "shipped": shipped,
        "on_time_posts": on_time_posts,
        "actual_posts": actual_posts,
        "target_posts": target_posts,
        "window_end": window_end,
        "quota_sent_24h": quota_sent,
        "quota_cap": quota_cap,
        "quota_remaining": max(0, quota_cap - quota_sent),
        "post_progress_pct": _pct(on_time_posts, target_posts),
        "quota_progress_pct": _pct(quota_sent, quota_cap),
        "data_errors": data_errors,
        "data_error_details": source_diagnostics or [],
        "next_action_owner": "独立站运营专员",
    }
    snapshot["status"] = status_for(snapshot)
    if reply_pending:
        snapshot["funnel_next_action"] = (
            f"先处理 {reply_pending} 条未归入下一阶段的真实回复，完成回信草稿审核和业务分流。"
        )
    elif awaiting_post_date:
        snapshot["funnel_next_action"] = (
            f"推进 {awaiting_post_date} 位已明确感兴趣或询价对象确认具体上稿日；未写日期不计承诺。"
        )
    elif commitments > actual_posts:
        snapshot["funnel_next_action"] = (
            f"跟进 {commitments - actual_posts} 位已承诺对象按期寄样或上稿，异常单独升级。"
        )
    elif shipped > actual_posts:
        snapshot["funnel_next_action"] = (
            f"跟进 {shipped - actual_posts} 位已寄样对象确认收货和上稿安排。"
        )
    else:
        snapshot["funnel_next_action"] = "当前没有待推进的真实回复、承诺或寄样阶段对象。"
    return snapshot


def _compact_progress(*, label: str, current: int, target: int, percent: float, color: str) -> str:
    """Return a compact progress row that cannot expand into a chart canvas."""
    bounded = max(0.0, min(100.0, float(percent or 0)))
    filled = min(10, max(0, int(round(bounded / 10))))
    bar = f'<text_tag color="{color}">{"█" * filled or "·"}</text_tag>{"░" * (10 - filled)}'
    return f"**{label}**　{current} / {target}　{bounded:g}%\n{bar}"


def _worst_color(snapshots: list[dict]) -> str:
    rank = {"red": 4, "orange": 3, "yellow": 2, "green": 1}
    return max(
        (str((row.get("status") or {}).get("color") or "yellow") for row in snapshots),
        key=lambda color: rank.get(color, 2),
        default="green",
    )


def build_card(snapshots: list[dict], *, day: date) -> dict:
    title = f"KOL集中宣发任务日报 · {day.isoformat()}"
    total_today_eligible = sum(_number(row.get("today_eligible")) for row in snapshots)
    total_today_sent = sum(_number(row.get("sent_today")) for row in snapshots)
    total_on_time = sum(_number(row.get("on_time_posts")) for row in snapshots)
    total_target = sum(_number(row.get("target_posts")) for row in snapshots)
    overall_color = _worst_color(snapshots)
    campaign_tags = []
    for index, row in enumerate(snapshots[:2]):
        status = row.get("status") or {}
        campaign_tags.append({
            "tag": "text_tag",
            "element_id": f"campaign_tag_{index}",
            "text": {
                "tag": "plain_text",
                "content": f"{row.get('brand') or '活动'}·{status.get('label') or '需要关注'}",
            },
            "color": status.get("color") if status.get("color") in VALID_STATUS_COLORS else "yellow",
        })

    elements: list[dict] = []
    if not snapshots:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "今天没有处于“正式运行 + 正式执行中”的集中宣发任务。"},
        })
    else:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": (
                f"**执行中任务**　{len(snapshots)}\n"
                f"**今日合适入池**　{total_today_eligible} 人　　"
                f"**今日活动开发信**　{total_today_sent} 封\n"
                f"**按时上稿 / 总目标**　{total_on_time} / {total_target}"
            )},
        })

    for index, row in enumerate(snapshots):
        status = row.get("status") or {}
        color = status.get("color") or "yellow"
        missing_note = (
            f"；入池时间缺失 {row['eligible_time_missing']} 人"
            if _number(row.get("eligible_time_missing")) else ""
        )
        errors = status.get("reasons") or []
        error_note = f"\n**异常依据**　{'；'.join(errors)}" if errors else ""
        elements.extend([
            {"tag": "hr"},
            {
                "tag": "markdown",
                "element_id": f"campaign_{index}",
                "content": (
                    f"### {row.get('brand') or '-'} · {row.get('name') or row.get('campaign_id')}\n"
                    f"<text_tag color=\"{color}\">{status.get('label') or '需要关注'}</text_tag>\n\n"
                    f"**今日合适入池**　{row.get('today_eligible', 0)} 人　　"
                    f"**当前合适入池**　{row.get('eligible_total', 0)} 人{missing_note}\n"
                    f"**当前可发送池**　{row.get('ready_due', 0)} 人　　"
                    f"**今日 / 累计活动开发信**　{row.get('sent_today', 0)} / {row.get('sent_total', 0)} 封\n"
                    f"**今日 / 累计回复**　{row.get('replies_today', 0)} / {row.get('replies_total', 0)}　　"
                    f"**回复待处理**　{row.get('reply_pending', 0)} 条\n"
                    f"**待确认上稿日**　{row.get('awaiting_post_date', 0)} 人　　"
                    f"**明确承诺上稿**　{row.get('commitments', 0)} 人\n"
                    f"**已寄样**　{row.get('shipped', 0)} 人　　"
                    f"**按时上稿 / 目标**　{row.get('on_time_posts', 0)} / {row.get('target_posts', 0)}　　"
                    f"**全部已上稿**　{row.get('actual_posts', 0)} 人\n"
                    f"**{row.get('brand') or '-'} 邮箱滚动24小时额度**　"
                    f"{row.get('quota_sent_24h', 0)} / {row.get('quota_cap', 0)}"
                    f"{error_note}"
                ),
            },
            {
                "tag": "markdown",
                "element_id": f"progress_{index}",
                "content": (
                    _compact_progress(
                        label="上稿进度",
                        current=_number(row.get("on_time_posts")),
                        target=_number(row.get("target_posts")),
                        percent=float(row.get("post_progress_pct") or 0),
                        color=color,
                    )
                    + "\n\n"
                    + _compact_progress(
                        label="邮箱额度",
                        current=_number(row.get("quota_sent_24h")),
                        target=_number(row.get("quota_cap")),
                        percent=float(row.get("quota_progress_pct") or 0),
                        color="orange" if float(row.get("quota_progress_pct") or 0) >= 85 else "blue",
                    )
                ),
            },
            {
                "tag": "markdown",
                "element_id": f"next_action_{index}",
                "content": (
                    f"**业务负责人**　{row.get('next_action_owner') or '-'}\n"
                    f"**业务下一步**　{row.get('funnel_next_action') or '-'}\n"
                    f"**系统动作**　{status.get('next_action') or '-'}"
                ),
            },
        ])

    elements.extend([
        {"tag": "hr"},
        {
            "tag": "markdown",
            "element_id": "metric_scope_note",
            "content": (
                "活动开发信只计算“活动参与记录”关联且真实发送成功的开发信；"
                "邮箱额度按 Zoho 滚动24小时全部外发邮件统计，两者口径不同。"
                "回复、承诺、寄样和上稿按各自真实字段统计，阶段之间允许重叠。"
            ),
        },
    ])
    return {
        "schema": "2.0",
        "header": {
            "template": overall_color,
            "title": {"tag": "plain_text", "content": title},
            "subtitle": {"tag": "plain_text", "content": "每日 17:15 · 只读进度汇总"},
            "text_tag_list": [
                {
                    "tag": "text_tag",
                    "element_id": "priority_tag",
                    "text": {"tag": "plain_text", "content": "KOL·P2"},
                    "color": "yellow",
                },
                *campaign_tags,
            ],
        },
        "body": {"elements": elements},
    }


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def validate_card(card: dict, snapshots: list[dict], *, day: date) -> dict:
    errors: list[str] = []
    expected_title = f"KOL集中宣发任务日报 · {day.isoformat()}"
    if card.get("schema") != "2.0":
        errors.append("schema必须为2.0")
    if (card.get("config") or {}).get("width_mode") == "fill":
        errors.append("日报卡片不得强制全宽显示")
    if ((card.get("header") or {}).get("title") or {}).get("content") != expected_title:
        errors.append("主标题不符合固定格式")
    elements = ((card.get("body") or {}).get("elements") or [])
    if not isinstance(elements, list):
        errors.append("body.elements缺失")
        elements = []
    nodes = list(_walk(card))
    forbidden = sorted({node.get("tag") for node in nodes if node.get("tag") in {"button", "form"}})
    if forbidden:
        errors.append(f"只读卡片不得包含：{','.join(forbidden)}")
    rendered = json.dumps(card, ensure_ascii=False, separators=(",", ":"))
    size_bytes = len(rendered.encode("utf-8"))
    if size_bytes >= 30_000:
        errors.append(f"卡片JSON超过30KB：{size_bytes}")

    charts = [node for node in nodes if node.get("tag") == "chart"]
    if charts:
        errors.append("日报卡片不得使用会随卡片宽度放大的chart画布")
    progress_rows = [
        node for node in nodes
        if node.get("tag") == "markdown" and str(node.get("element_id") or "").startswith("progress_")
    ]
    if snapshots and len(progress_rows) != len(snapshots):
        errors.append("每个活动必须有一块紧凑进度信息")
    if not snapshots and progress_rows:
        errors.append("空态卡片不应伪造进度信息")

    required_metrics = {
        "today_eligible", "eligible_total", "ready_due", "sent_today", "sent_total",
        "replies_today", "replies_total", "reply_pending", "awaiting_post_date",
        "commitments", "shipped", "on_time_posts", "actual_posts", "target_posts",
        "quota_sent_24h", "quota_cap", "status", "next_action_owner", "funnel_next_action",
    }
    for row in snapshots:
        campaign_id = row.get("campaign_id") or row.get("name") or "unknown"
        missing = sorted(required_metrics - set(row))
        if missing:
            errors.append(f"{campaign_id}缺少指标：{','.join(missing)}")
        color = str((row.get("status") or {}).get("color") or "")
        if color not in VALID_STATUS_COLORS:
            errors.append(f"{campaign_id}状态颜色不合法")
        if str(row.get("name") or campaign_id) not in rendered:
            errors.append(f"{campaign_id}未显示在卡片")

    tag_colors = [
        node.get("color") for node in ((card.get("header") or {}).get("text_tag_list") or [])
        if node.get("tag") == "text_tag"
    ]
    if not tag_colors or any(color not in VALID_STATUS_COLORS | {"blue"} for color in tag_colors):
        errors.append("标题彩色状态标签缺失或颜色不合法")
    return {"ok": not errors, "errors": errors, "size_bytes": size_bytes}


async def _list_records_with_created(table_id: str, field_names: list[str]) -> list[dict]:
    if not table_id:
        raise DailyReportError("活动参与表T_LAUNCH_PARTICIPANT未配置")
    items: list[dict] = []
    page_token = ""
    fields_json = json.dumps(field_names, ensure_ascii=False, separators=(",", ":"))
    while True:
        params = {
            "page_size": 500,
            "automatic_fields": "true",
            "field_names": fields_json,
        }
        if page_token:
            params["page_token"] = page_token
        path = (
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records?"
            + urllib.parse.urlencode(params)
        )
        response = await feishu.api("GET", path, which="bitable")
        data = response.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token") or ""
        if not page_token:
            break
    return items


async def _load_report_source() -> dict:
    activities = await feishu.fetch_all_records(
        config.T_LAUNCH_CAMPAIGN, field_names=ACTIVITY_FIELDS, page_size=100,
    )
    activities = [record for record in activities if _is_active_activity(record)]
    participants = await _list_records_with_created(
        config.T_LAUNCH_PARTICIPANT, PARTICIPANT_FIELDS,
    )
    draft_rows = await feishu.fetch_all_records(
        config.T_DRAFT, field_names=DRAFT_FIELDS, page_size=500,
    )
    brands = sorted({
        _text((record.get("fields") or {}).get("品牌")).upper()
        for record in activities
        if _text((record.get("fields") or {}).get("品牌"))
    })
    counts, quota_errors = await auto_send.zoho_sent_counts_24h(brands)
    cap = max(1, _number(auto_send.SEND_DAILY_CAP))
    return {
        "activities": activities,
        "participants": participants,
        "drafts": {record.get("record_id"): record for record in draft_rows if record.get("record_id")},
        "quotas": {brand: {"sent_24h": counts.get(brand, 0), "cap": cap} for brand in brands},
        "quota_errors": quota_errors,
    }


async def active_campaign_ids() -> tuple[str, ...]:
    """只读当前活动主表的小集合，供后台请求去重键使用。"""
    activities = await feishu.fetch_all_records(
        config.T_LAUNCH_CAMPAIGN, field_names=ACTIVITY_FIELDS, page_size=100,
    )
    return tuple(sorted({
        _text((record.get("fields") or {}).get("活动ID"))
        for record in activities
        if _is_active_activity(record)
        and _text((record.get("fields") or {}).get("活动ID"))
    }))


def _send_identity(*, report_day: date, recipient_type: str, recipient_id: str,
                   snapshots: list[dict]) -> tuple[str, str]:
    campaigns = ",".join(sorted(str(row.get("campaign_id") or "") for row in snapshots))
    raw = f"{report_day.isoformat()}|{recipient_type}:{recipient_id}|{campaigns}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return digest, f"kolrpt-{digest}"


def _find_persisted_receipt(activities: list[dict], receipt_key: str) -> dict | None:
    for activity in activities:
        note = _text((activity.get("fields") or {}).get("数据口径备注"))
        for line in reversed(note.splitlines()):
            if not line.startswith(REPORT_RECEIPT_PREFIX):
                continue
            try:
                payload = json.loads(line[len(REPORT_RECEIPT_PREFIX):])
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if payload.get("key") == receipt_key:
                return payload
    return None


async def _persist_report_receipt(anchor_activity: dict, payload: dict) -> None:
    """把发卡回执写入一个活动的运行备注；不修改任何业务统计字段。"""
    record_id = str(anchor_activity.get("record_id") or "")
    if not record_id:
        raise DailyReportError("活动记录缺少record_id，无法持久化防重复回执")
    current = await feishu.get_record(config.T_LAUNCH_CAMPAIGN, record_id)
    fields = current.get("fields") or {}
    key = str(payload.get("key") or "")
    keep: list[str] = []
    for line in _text(fields.get("数据口径备注")).splitlines():
        if line.startswith(REPORT_RECEIPT_PREFIX):
            try:
                existing = json.loads(line[len(REPORT_RECEIPT_PREFIX):])
            except (TypeError, ValueError, json.JSONDecodeError):
                existing = {}
            # 防重只需要当前日报日期。旧日或损坏的技术回执可安全清理；
            # 非技术业务备注必须逐字保留。
            if existing.get("key") == key or existing.get("day") != payload.get("day"):
                continue
        keep.append(line)
    receipt_line = REPORT_RECEIPT_PREFIX + json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"),
    )
    note = "\n".join([*keep, receipt_line]).strip()
    if len(note) > 3000:
        raise DailyReportError(
            "活动数据口径备注空间不足，未写技术回执；原有业务备注保持不变"
        )
    await feishu.update_record(
        config.T_LAUNCH_CAMPAIGN, record_id, {"数据口径备注": note},
    )
    readback = await feishu.get_record(config.T_LAUNCH_CAMPAIGN, record_id)
    persisted = _find_persisted_receipt([readback], key)
    if not persisted or persisted.get("status") != payload.get("status"):
        raise DailyReportError("发卡技术回执写后回读不一致")
    if payload.get("status") == "sent" and persisted.get("message_id") != payload.get("message_id"):
        raise DailyReportError("发卡已完成，但持久化发送回执message_id回读不一致")


def _frankie_open_id() -> str:
    for name, open_id in config.NOTIFY_USERS:
        if "Frankie" in name or name.startswith("潘志聪"):
            return open_id
    return FRANKIE_OPEN_ID


def _coerce_day(value: date | str | None) -> date:
    if isinstance(value, datetime):
        return value.astimezone(BJ).date()
    if isinstance(value, date):
        return value
    if value:
        return date.fromisoformat(str(value))
    return datetime.now(BJ).date()


async def run(*, day: date | str | None = None, notify: bool = False, frankie_only: bool = True) -> dict:
    """生成日报；只有 notify=True 时才发送，默认只允许 Frankie 样卡。"""
    report_day = _coerce_day(day)
    source = await _load_report_source()
    activities = [record for record in source.get("activities", []) if _is_active_activity(record)]
    participants = source.get("participants") or []
    drafts = source.get("drafts") or {}
    quotas = source.get("quotas") or {}
    quota_errors = source.get("quota_errors") or {}
    now_ms = int(time.time() * 1000)
    day_start = datetime.combine(report_day, datetime_time.min, tzinfo=BJ)
    day_end = day_start + timedelta(days=1)
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int(day_end.timestamp() * 1000)

    activities_by_campaign_id: dict[str, list[dict]] = {}
    for activity in activities:
        campaign_id = _text((activity.get("fields") or {}).get("活动ID"))
        if campaign_id:
            activities_by_campaign_id.setdefault(campaign_id, []).append(activity)
    active_campaign_ids = set(activities_by_campaign_id)
    draft_campaigns: dict[str, set[str]] = {}
    for participant in participants:
        if not _is_active_participant(participant):
            continue
        fields = participant.get("fields") or {}
        campaign_id = _text(fields.get("活动ID"))
        if campaign_id not in active_campaign_ids:
            continue
        for draft_id in set(_ids(fields.get("关联邮件草稿"))):
            draft_campaigns.setdefault(draft_id, set()).add(campaign_id)
    campaign_evidence = {
        campaign_id: _campaign_reply_evidence(
            [
                row for row in participants
                if _text((row.get("fields") or {}).get("活动ID")) == campaign_id
            ],
            drafts,
        )
        for campaign_id in active_campaign_ids
    }
    source_mid_campaigns: dict[str, set[str]] = {}
    for campaign_id, evidence in campaign_evidence.items():
        for message_id in evidence["sources"]:
            source_mid_campaigns.setdefault(message_id, set()).add(campaign_id)
    ambiguous_source_mids = {
        message_id: campaign_ids
        for message_id, campaign_ids in source_mid_campaigns.items()
        if len(campaign_ids) > 1
    }
    for message_id, campaign_ids in ambiguous_source_mids.items():
        for campaign_id in campaign_ids:
            evidence = campaign_evidence[campaign_id]
            evidence["source_ids"].pop(message_id, None)
            evidence["sources"].pop(message_id, None)
            evidence["invalid_message_ids"].add(message_id)

    live_reply_ids_by_mid: dict[str, set[str]] = {}
    for draft_id, draft in drafts.items():
        if not _is_live_human_reply_draft(draft):
            continue
        message_id = _reply_target_message_id(draft)
        if message_id:
            live_reply_ids_by_mid.setdefault(message_id, set()).add(draft_id)
    duplicate_live_reply_ids = {
        draft_id
        for draft_ids in live_reply_ids_by_mid.values()
        if len(draft_ids) > 1
        for draft_id in draft_ids
    }
    malformed_reply_campaigns: dict[str, set[str]] = {
        campaign_id: set() for campaign_id in active_campaign_ids
    }
    duplicate_live_reply_mids_by_campaign: dict[str, set[str]] = {
        campaign_id: set() for campaign_id in active_campaign_ids
    }
    for draft_id, draft in drafts.items():
        if not _is_live_human_reply_draft(draft):
            continue
        draft_fields = draft.get("fields") or {}
        explicit_campaign = _text(draft_fields.get("集中宣发活动ID"))
        explicit_status = _text(draft_fields.get("活动归属状态"))
        if explicit_status == "无需归属":
            continue
        if explicit_status in {"已确认", "自动确认"} and explicit_campaign in active_campaign_ids:
            draft_campaigns.setdefault(draft_id, set()).add(explicit_campaign)
            continue
        reply_keys = _draft_identity_keys(draft)
        reply_message_id = _reply_target_message_id(draft)
        source_campaign_ids = {
            campaign_id
            for campaign_id, evidence in campaign_evidence.items()
            if reply_message_id and reply_message_id in evidence["sources"]
        }
        invalid_mid_campaign_ids = {
            campaign_id
            for campaign_id, evidence in campaign_evidence.items()
            if reply_message_id and reply_message_id in evidence["invalid_message_ids"]
        }
        direct_mid_campaign_ids = source_campaign_ids or invalid_mid_campaign_ids
        if draft_id in duplicate_live_reply_ids:
            target_campaign_ids = set(direct_mid_campaign_ids)
            if not target_campaign_ids and len(reply_keys) == 1:
                reply_key = next(iter(reply_keys))
                target_campaign_ids = {
                    campaign_id
                    for campaign_id, evidence in campaign_evidence.items()
                    if reply_key in evidence["candidate_keys"]
                }
            for campaign_id in target_campaign_ids:
                duplicate_live_reply_mids_by_campaign[campaign_id].add(reply_message_id)
            continue
        if len(reply_keys) != 1:
            contacts, products = _draft_identity_parts(draft)
            attributed = False
            if direct_mid_campaign_ids:
                for campaign_id in direct_mid_campaign_ids:
                    malformed_reply_campaigns[campaign_id].add(draft_id)
                continue
            for campaign_id, evidence in campaign_evidence.items():
                campaign_contacts = {key[0] for key in evidence["candidate_keys"]}
                campaign_products = {key[1] for key in evidence["candidate_keys"]}
                if (
                    (contacts and contacts & campaign_contacts)
                    or (products and products & campaign_products)
                ):
                    malformed_reply_campaigns[campaign_id].add(draft_id)
                    attributed = True
            if not attributed and not contacts and not products:
                for campaign_id in active_campaign_ids:
                    malformed_reply_campaigns[campaign_id].add(draft_id)
            continue
        reply_key = next(iter(reply_keys))
        if source_campaign_ids:
            for campaign_id in source_campaign_ids:
                evidence = campaign_evidence[campaign_id]
                if evidence["sources"].get(reply_message_id) == reply_key:
                    draft_campaigns.setdefault(draft_id, set()).add(campaign_id)
                else:
                    malformed_reply_campaigns[campaign_id].add(draft_id)
            continue
        if invalid_mid_campaign_ids:
            for campaign_id in invalid_mid_campaign_ids:
                malformed_reply_campaigns[campaign_id].add(draft_id)
            continue
        for campaign_id, evidence in campaign_evidence.items():
            if reply_key in evidence["candidate_keys"]:
                malformed_reply_campaigns[campaign_id].add(draft_id)
    ambiguous_drafts = {
        draft_id: campaign_ids
        for draft_id, campaign_ids in draft_campaigns.items()
        if len(campaign_ids) > 1
    }

    snapshots: list[dict] = []
    for activity in activities:
        fields = activity.get("fields") or {}
        campaign_id = _text(fields.get("活动ID"))
        brand = _text(fields.get("品牌")).upper()
        duplicate_activity_records = activities_by_campaign_id.get(campaign_id) or []
        duplicate_activity_record_ids = sorted({
            str(row.get("record_id") or "<缺失record_id>")
            for row in duplicate_activity_records
        })
        duplicate_campaign_id = len(duplicate_activity_records) > 1
        campaign_participants = [] if duplicate_campaign_id else [
            row for row in participants
            if _text((row.get("fields") or {}).get("活动ID")) == campaign_id
        ]
        campaign_ambiguous = {
            draft_id for draft_id, campaign_ids in ambiguous_drafts.items()
            if campaign_id in campaign_ids
        }
        campaign_malformed = malformed_reply_campaigns.get(campaign_id) or set()
        campaign_duplicate_reply_mids = (
            duplicate_live_reply_mids_by_campaign.get(campaign_id) or set()
        )
        campaign_duplicate_reply_ids = {
            draft_id
            for message_id in campaign_duplicate_reply_mids
            for draft_id in live_reply_ids_by_mid.get(message_id, set())
        }
        campaign_invalid_identity_sources = (
            campaign_evidence.get(campaign_id, {}).get("invalid_identity_source_ids") or set()
        )
        campaign_shared_source_messages = (
            campaign_evidence.get(campaign_id, {}).get("shared_source_message_ids") or {}
        )
        campaign_shared_source_ids = set(campaign_shared_source_messages)
        campaign_invalid_identity_sources = (
            campaign_invalid_identity_sources - campaign_shared_source_ids
        )
        campaign_duplicate_source_mids = (
            campaign_evidence.get(campaign_id, {}).get("duplicate_source_message_ids") or set()
        )
        campaign_duplicate_source_ids = (
            campaign_evidence.get(campaign_id, {}).get("duplicate_source_draft_ids") or set()
        )
        source_errors = []
        source_diagnostics: list[dict[str, Any]] = []
        if duplicate_campaign_id:
            source_errors.append(
                f"活动ID重复，无法判断参与者和草稿属于哪条活动记录，"
                "该活动全部业务统计已暂停"
                f"（活动记录:{_bounded_ids(duplicate_activity_record_ids)}）"
            )
            source_diagnostics.append({
                "kind": "duplicate_active_campaign_id",
                "message_ids": [],
                "draft_ids": [],
                "activity_record_ids": duplicate_activity_record_ids,
            })
        campaign_ambiguous_source_mids = {
            message_id
            for message_id, campaign_ids in ambiguous_source_mids.items()
            if campaign_id in campaign_ids
        }
        if campaign_ambiguous_source_mids:
            source_errors.append(
                f"{len(campaign_ambiguous_source_mids)}个原邮件ID跨活动重复关联，"
                "相关reply已从统计中排除"
                f"（MID:{_bounded_ids(campaign_ambiguous_source_mids)}）"
            )
            source_diagnostics.append({
                "kind": "cross_campaign_source_mid",
                "message_ids": sorted(campaign_ambiguous_source_mids),
                "draft_ids": [],
            })
        if campaign_ambiguous:
            source_errors.append(
                f"{len(campaign_ambiguous)}个草稿跨活动重复关联，已从统计中排除"
                f"（草稿:{_bounded_ids(campaign_ambiguous)}）"
            )
            source_diagnostics.append({
                "kind": "cross_campaign_draft",
                "message_ids": [],
                "draft_ids": sorted(campaign_ambiguous),
            })
        if campaign_malformed:
            source_errors.append(
                f"{len(campaign_malformed)}个待审reply草稿缺少唯一KOL/产品或原邮件ID，"
                "或与活动cold归属不一致，已从统计中排除"
                f"（草稿:{_bounded_ids(campaign_malformed)}）"
            )
            source_diagnostics.append({
                "kind": "malformed_reply",
                "message_ids": sorted({
                    _reply_target_message_id(drafts.get(draft_id) or {})
                    for draft_id in campaign_malformed
                    if _reply_target_message_id(drafts.get(draft_id) or {})
                }),
                "draft_ids": sorted(campaign_malformed),
            })
        if campaign_duplicate_reply_mids:
            source_errors.append(
                f"{len(campaign_duplicate_reply_mids)}个原邮件ID对应"
                f"{len(campaign_duplicate_reply_ids)}条待审reply，全部从统计中排除"
                f"（MID:{_bounded_ids(campaign_duplicate_reply_mids)}；"
                f"草稿:{_bounded_ids(campaign_duplicate_reply_ids)}）"
            )
            source_diagnostics.append({
                "kind": "duplicate_live_reply",
                "message_ids": sorted(campaign_duplicate_reply_mids),
                "draft_ids": sorted(campaign_duplicate_reply_ids),
            })
        if campaign_shared_source_ids:
            shared_message_ids = {
                message_id for message_id in campaign_shared_source_messages.values()
                if message_id
            }
            source_errors.append(
                f"{len(campaign_shared_source_ids)}个已回复cold草稿关联多个参与记录，"
                "整条来源及相关reply已从统计中排除"
                f"（草稿:{_bounded_ids(campaign_shared_source_ids)}；"
                f"MID:{_bounded_ids(shared_message_ids)}）"
            )
            source_diagnostics.append({
                "kind": "shared_cold_source",
                "message_ids": sorted(shared_message_ids),
                "draft_ids": sorted(campaign_shared_source_ids),
            })
        if campaign_invalid_identity_sources:
            invalid_identity_mids = {
                _source_reply_message_id(drafts.get(draft_id) or {})
                for draft_id in campaign_invalid_identity_sources
                if _source_reply_message_id(drafts.get(draft_id) or {})
            }
            source_errors.append(
                f"{len(campaign_invalid_identity_sources)}个已回复cold草稿归属字段缺失、不唯一，"
                "或与活动参与记录不一致；该cold来源已排除，"
                "如有其他唯一有效直接证据，reply仍按有效证据统计"
                f"（草稿:{_bounded_ids(campaign_invalid_identity_sources)}）"
            )
            source_diagnostics.append({
                "kind": "invalid_cold_identity",
                "message_ids": sorted(invalid_identity_mids),
                "draft_ids": sorted(campaign_invalid_identity_sources),
            })
        if campaign_duplicate_source_mids:
            source_errors.append(
                f"{len(campaign_duplicate_source_mids)}个原邮件ID对应多条有效cold来源，"
                "相关reply已从统计中排除"
                f"（MID:{_bounded_ids(campaign_duplicate_source_mids)}；"
                f"草稿:{_bounded_ids(campaign_duplicate_source_ids)}）"
            )
            source_diagnostics.append({
                "kind": "duplicate_cold_source_mid",
                "message_ids": sorted(campaign_duplicate_source_mids),
                "draft_ids": sorted(campaign_duplicate_source_ids),
            })
        snapshots.append(summarize_campaign(
            activity,
            campaign_participants,
            drafts,
            quota=quotas.get(brand) or {"sent_24h": 0, "cap": auto_send.SEND_DAILY_CAP},
            quota_error=str(quota_errors.get(brand) or ""),
            now_ms=now_ms,
            day_start_ms=day_start_ms,
            day_end_ms=day_end_ms,
            excluded_draft_ids=(
                campaign_ambiguous
                | campaign_malformed
                | campaign_duplicate_reply_ids
                | campaign_shared_source_ids
            ),
            source_errors=source_errors,
            source_diagnostics=source_diagnostics,
        ))

    card = build_card(snapshots, day=report_day)
    validation = validate_card(card, snapshots, day=report_day)
    message_ids: list[str] = []
    send_key = ""
    deduplicated = False
    receipt_writes = 0
    empty_state_send_skipped = bool(notify and not snapshots)
    if notify and snapshots:
        if not validation["ok"]:
            raise DailyReportError("卡片结构自检未通过：" + "；".join(validation["errors"]))
        if frankie_only:
            recipient_type = "open_id"
            recipient_id = _frankie_open_id()
        else:
            enabled = os.environ.get("KOL_LAUNCH_DAILY_GROUP_ENABLED", "0") == "1"
            if not enabled:
                raise DailyReportError("群日报开关未开启，已拒绝发送")
            if config.NOTIFY_CHAT_ID != CURRENT_GROUP_CHAT_ID:
                raise DailyReportError("群目标不是当前KOL运营群，已拒绝发送")
            recipient_type = "chat_id"
            recipient_id = CURRENT_GROUP_CHAT_ID

        send_key, message_uuid = _send_identity(
            report_day=report_day,
            recipient_type=recipient_type,
            recipient_id=recipient_id,
            snapshots=snapshots,
        )
        async with _SEND_LOCK:
            persisted = _find_persisted_receipt(activities, send_key)
            if persisted and persisted.get("status") not in {"sent", "rejected"}:
                raise DailyReportError(
                    "检测到同一日报上次停在发送中；为避免重复发卡，已暂停自动重发，"
                    "请先按uuid核查飞书消息后再处理"
                )
            message_id = str((persisted or {}).get("message_id") or _SEND_RECEIPTS.get(send_key, ""))
            if message_id:
                deduplicated = True
            else:
                anchor = min(
                    activities,
                    key=lambda item: _text((item.get("fields") or {}).get("活动ID")),
                    default=None,
                )
                if not anchor:
                    raise DailyReportError("没有活动记录可持久化发卡回执")
                await _persist_report_receipt(anchor, {
                    "key": send_key,
                    "uuid": message_uuid,
                    "day": report_day.isoformat(),
                    "status": "sending",
                    "updated_ts": int(time.time()),
                })
                receipt_writes += 1
                try:
                    message_id = await feishu.send_card_message(
                        recipient_type, recipient_id, card,
                        biz="KOL", level="P2", format_title=False,
                        message_uuid=message_uuid,
                    )
                except Exception as exc:
                    error_text = str(exc)
                    status_prefix, separator, _ = error_text.partition(":")
                    is_precreate_rejection = (
                        bool(separator)
                        and status_prefix.startswith("POST /im/v1/messages")
                        and status_prefix.rstrip().endswith("→ 400")
                        and "Failed to create card content" in error_text
                    )
                    if is_precreate_rejection:
                        await _persist_report_receipt(anchor, {
                            "key": send_key,
                            "uuid": message_uuid,
                            "day": report_day.isoformat(),
                            "status": "rejected",
                            "error": "Feishu rejected card before message creation",
                            "updated_ts": int(time.time()),
                        })
                        receipt_writes += 1
                    raise
                if not message_id:
                    target_name = "Frankie样卡" if frankie_only else "运营群日报"
                    raise DailyReportError(f"{target_name}发送后未返回message_id，已按失败处理")
                await _persist_report_receipt(anchor, {
                    "key": send_key,
                    "uuid": message_uuid,
                    "day": report_day.isoformat(),
                    "status": "sent",
                    "message_id": message_id,
                    "updated_ts": int(time.time()),
                })
                receipt_writes += 1
                _SEND_RECEIPTS[send_key] = message_id
        message_ids.append(message_id)

    return {
        "ok": validation["ok"],
        "day": report_day.isoformat(),
        "campaigns": len(snapshots),
        "snapshots": snapshots,
        "card": card,
        "validation": validation,
        "notified": bool(message_ids),
        "frankie_only": bool(frankie_only),
        "message_ids": message_ids,
        "send_key": send_key,
        "deduplicated": deduplicated,
        "business_writes": 0,
        "operational_receipt_writes": receipt_writes,
        "empty_state_send_skipped": empty_state_send_skipped,
    }
