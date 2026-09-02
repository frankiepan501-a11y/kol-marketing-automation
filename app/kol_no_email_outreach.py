# -*- coding: utf-8 -*-
"""真正无公开邮箱 KOL 的人工联系闭环。

本模块只把“系统已经完成自动邮箱检查，仍没有可用邮箱”的例外变成一张可执行卡片：
运营复制模板到公开平台私信；KOL 提供邮箱后回填主表并回到邮箱质量检查。

安全边界：
- 不调用社媒私信 API，不代替运营发送私信；
- 不发送邮件；取得的邮箱先标记为“未验”，继续走既有邮箱质量检查；
- YouTube 在登录态主页检查完成前不允许转给运营；
- 生产扩散默认关闭，只允许给 Frankie 发样卡；
- 互动卡由聪哥分身3号发送，回调后由同一 App PATCH 原卡。
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from . import config, feishu, kol_email_repair, launch_daily_report
from .feishu import ext, ext_url


ACTION_EMAIL_CAPTURED = "kol_no_email_email_captured"
ACTION_PLATFORM_ONGOING = "kol_no_email_platform_ongoing"
ACTION_NOT_FIT = "kol_no_email_not_fit"
ACTION_NO_RESPONSE = "kol_no_email_no_response"
ALLOWED_ACTIONS = {
    ACTION_EMAIL_CAPTURED,
    ACTION_PLATFORM_ONGOING,
    ACTION_NOT_FIT,
    ACTION_NO_RESPONSE,
}

DM_TEMPLATE_EN = (
    "Hi [Name], we'd love to discuss a collaboration. "
    "Could you share your best business email?"
)

BJ = timezone(timedelta(hours=8))
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
T_CRAWLER_TASK = "tblQnLHnBa1RjJUE"
YOUTUBE_AUTH_TASK_PREFIX = "[系统邮箱检查:"
YOUTUBE_AUTH_RESULT_PREFIX = "AUTH_EMAIL_RESULT "
YOUTUBE_AUTH_PENDING_STATES = {"1-待触发", "2-执行中", "2-运行中"}
YOUTUBE_AUTH_RESULT_TTL_MS = 24 * 60 * 60 * 1000

ACTIVITY_FIELDS = [
    "活动ID", "活动名称", "品牌", "产品英文名", "ERP SKU", "运行模式", "状态",
    "窗口开始", "发布窗口中点", "窗口结束", "目标上稿数",
]
PARTICIPANT_FIELDS = [
    "活动ID", "参与状态", "审核结论", "关联KOL", "产品家族ID", "对象类型",
    "进入方式", "活动分池", "计划上稿时间", "承诺上稿时间", "实际上稿时间",
]
KOL_FIELDS = [
    "账号名", "主平台", "主链接", "国家", "国家原文", "语言", "粉丝数",
    "近期视频标题", "聚合页URL", "其他链接", "邮箱", "邮箱验真状态",
    "资料可用状态", "触达路由状态", "合作状态", "决策反哺日志", "迁移备注",
]


def _text(value: Any) -> str:
    return str(ext(value) or "").strip()


def _ids(value: Any) -> list[str]:
    return launch_daily_report._ids(value)


def _ts(value: Any) -> int:
    return launch_daily_report._ts(value)


def _fmt_ms(value: Any) -> str:
    stamp = _ts(value)
    if not stamp:
        return "未设置"
    return datetime.fromtimestamp(stamp / 1000, BJ).strftime("%Y-%m-%d %H:%M")


def _record_url(record_id: str) -> str:
    return (
        f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
        f"?table={config.T_KOL}&record={record_id}"
    )


def _page_links(fields: dict) -> list[str]:
    values = [
        ext_url(fields.get("主链接")),
        ext_url(fields.get("聚合页URL")),
        _text(fields.get("其他链接")),
    ]
    out: list[str] = []
    for value in values:
        for part in re.split(r"[\s,;，；]+", value or ""):
            part = part.strip()
            if part.startswith(("http://", "https://")) and part not in out:
                out.append(part)
    return out


def _deadline(activity_fields: dict, participant_fields: dict) -> int:
    return (
        _ts(participant_fields.get("承诺上稿时间"))
        or _ts(participant_fields.get("计划上稿时间"))
        or _ts(activity_fields.get("发布窗口中点"))
        or _ts(activity_fields.get("窗口结束"))
    )


def build_case(*, kol: dict, activity: dict, participant: dict | None = None) -> dict:
    kf = kol.get("fields") or {}
    af = activity.get("fields") or {}
    pf = (participant or {}).get("fields") or {}
    links = _page_links(kf)
    raw_email = _text(kf.get("邮箱"))
    clean_email, email_note = feishu.clean_email(raw_email)
    if clean_email:
        raise ValueError("该 KOL 已有可用邮箱，不应进入无邮箱私信卡")

    platform = _text(kf.get("主平台")) or "公开社媒平台"
    campaign_id = _text(af.get("活动ID"))
    product = (
        _text(af.get("产品英文名"))
        or _text(pf.get("产品家族ID"))
        or _text(af.get("ERP SKU"))
        or "当前活动产品"
    )
    task_nature = " / ".join(filter(None, [
        _text(pf.get("进入方式")), _text(pf.get("对象类型")),
    ])) or "活动候选 KOL"
    return {
        "kol_record_id": kol.get("record_id") or "",
        "kol_name": _text(kf.get("账号名")) or "未命名 KOL",
        "platform": platform,
        "main_url": ext_url(kf.get("主链接")),
        "country": _text(kf.get("国家")) or _text(kf.get("国家原文")) or "未记录",
        "language": _text(kf.get("语言")) or "未记录",
        "followers": _text(kf.get("粉丝数")) or "未记录",
        "recent_content": _text(kf.get("近期视频标题")) or "未记录",
        "public_pages": links,
        "email_gap": email_note or (f"邮箱字段不可用：{raw_email[:80]}" if raw_email else "公开页面未找到可用邮箱"),
        "fit_reason": (
            f"参与状态={_text(pf.get('参与状态')) or '未建参与记录'}；"
            f"审核结论={_text(pf.get('审核结论')) or '未记录'}"
        ),
        "campaign_id": campaign_id,
        "campaign_name": _text(af.get("活动名称")) or campaign_id,
        "brand": _text(af.get("品牌")) or "品牌未记录",
        "product": product,
        "task_nature": task_nature,
        "deadline": _deadline(af, pf),
        "timed_upload_required": bool(_deadline(af, pf)),
        "target_uploads": _text(af.get("目标上稿数")) or "未设置",
    }


def _youtube_auth_task_name(kol_record_id: str) -> str:
    return f"{YOUTUBE_AUTH_TASK_PREFIX}{kol_record_id}] YouTube登录态公开邮箱"


def _youtube_auth_result(log_text: Any) -> dict:
    text = _text(log_text)
    marker_index = text.rfind(YOUTUBE_AUTH_RESULT_PREFIX)
    if marker_index < 0:
        return {}
    payload = text[marker_index + len(YOUTUBE_AUTH_RESULT_PREFIX):].splitlines()[0].strip()
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _normalized_channel_url(value: Any) -> str:
    raw = _text(value)
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw if "://" in raw else f"https://{raw}")
    except ValueError:
        return raw.rstrip("/").casefold()
    host = (parsed.hostname or "").casefold()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+", "/", parsed.path or "").rstrip("/").casefold()
    return f"{host}{path}"


def _youtube_auth_task_input(fields: dict) -> dict:
    raw = _text(fields.get("关键词列表"))
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


async def _ensure_youtube_authenticated_check(case: dict, *, dry_run: bool) -> dict:
    """Queue/read one deterministic local authenticated YouTube profile check."""
    kol_record_id = _text(case.get("kol_record_id"))
    task_name = _youtube_auth_task_name(kol_record_id)
    rows = await feishu.search_records(
        T_CRAWLER_TASK,
        [{"field_name": "任务名", "operator": "contains", "value": [
            f"{YOUTUBE_AUTH_TASK_PREFIX}{kol_record_id}]",
        ]}],
        field_names=["任务名", "任务状态", "执行日志", "创建日期", "关键词列表"],
    )
    current_channel = _normalized_channel_url(case.get("main_url"))
    fresh_after = int(time.time() * 1000) - YOUTUBE_AUTH_RESULT_TTL_MS
    exact_rows = [
        row for row in rows
        if (
            _text((row.get("fields") or {}).get("任务名")) == task_name
            and _normalized_channel_url(
                _youtube_auth_task_input(row.get("fields") or {}).get("channel_url")
            ) == current_channel
            and _ts((row.get("fields") or {}).get("创建日期")) >= fresh_after
        )
    ]
    exact_rows.sort(
        key=lambda row: _ts((row.get("fields") or {}).get("创建日期")),
        reverse=True,
    )
    for row in exact_rows:
        fields = row.get("fields") or {}
        status = _text(fields.get("任务状态"))
        result = _youtube_auth_result(fields.get("执行日志"))
        result_status = _text(result.get("status"))
        base = {
            "task_record_id": row.get("record_id") or "",
            "task_status": status,
            "auth_result": result,
        }
        if status in YOUTUBE_AUTH_PENDING_STATES:
            return {**base, "ok": True, "status": "youtube_auth_check_pending"}
        if status == "4-失败":
            return {**base, "ok": False, "status": "youtube_auth_check_failed"}
        if status == "3-已完成" and result_status in {
            "written", "already_has_email",
        }:
            return {**base, "ok": True, "status": "youtube_auth_email_written"}
        if status == "3-已完成" and result_status == "no_visible_email":
            return {**base, "ok": True, "status": "youtube_auth_no_visible_email"}
        if status == "3-已完成":
            return {**base, "ok": False, "status": "youtube_auth_result_unknown"}

    if dry_run:
        return {"ok": True, "status": "youtube_auth_check_would_queue"}
    payload = json.dumps({
        "kol_record_id": kol_record_id,
        "channel_url": _text(case.get("main_url")),
    }, ensure_ascii=False, separators=(",", ":"))
    task_record_id = await feishu.create_record(T_CRAWLER_TASK, {
        "任务名": task_name,
        # Reuse the existing select option; daemon routes this task by its exact name marker.
        "爬虫类型": "KOL-YouTube",
        "关键词列表": payload,
        "每批数量上限": 1,
        "任务状态": "1-待触发",
        "触发": True,
        "创建日期": int(time.time() * 1000),
    })
    return {
        "ok": True,
        "status": "youtube_auth_check_queued",
        "task_record_id": task_record_id,
    }


def build_outreach_card(case: dict) -> dict:
    action_base = {
        "kol_record_id": case.get("kol_record_id"),
        "campaign_id": case.get("campaign_id"),
        "platform": case.get("platform"),
    }
    deadline_label = _fmt_ms(case.get("deadline")) if case.get("timed_upload_required") else "无规定时间"
    main_url = case.get("main_url") or _record_url(case.get("kol_record_id") or "")
    checked_sources = (
        "系统已自动检查公开主页、官网、Linktree/Beacons，"
        "并用 YouTube 登录态检查了频道“更多信息”，仍未找到可用邮箱。"
        if case.get("youtube_authenticated_checked")
        else
        "系统已自动检查 KOL 主页、简介、官网及 Linktree/Beacons 等公开页，仍未找到可用邮箱。"
    )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "🟠 [KOL·P1] 需要人工联系 1 位 KOL"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "**你只需要做 1 件事**\n"
                f"系统已查完公开渠道。请在 {case.get('platform')} 私信 **{case.get('kol_name')}**，"
                "请对方提供合作邮箱，然后填在下方。\n"
                f"> 可直接复制：{DM_TEMPLATE_EN}"
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "**为什么需要人工处理**\n"
                f"{checked_sources}这是一条例外，不需要运营重新筛选 KOL。"
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"**合作任务**：{case.get('brand')} · {case.get('product')}\n"
                f"**任务性质**：{case.get('task_nature')}\n"
                f"**上稿截止**：{deadline_label}\n"
                f"[打开 KOL 主页]({main_url})"
            )}},
            {"tag": "form", "name": f"kol_no_email_{case.get('kol_record_id')}", "elements": [
                {
                    "tag": "input", "name": "business_email", "width": "fill",
                    "label_position": "top", "required": True,
                    "label": {"tag": "plain_text", "content": "KOL 提供的合作邮箱"},
                    "placeholder": {"tag": "plain_text", "content": "例如 creator@example.com"},
                    "max_length": 254,
                },
                {
                    "tag": "button", "action_type": "form_submit", "name": "submit_email",
                    "type": "primary", "text": {"tag": "plain_text", "content": "回填邮箱并进入质量检查"},
                    "value": {**action_base, "action": ACTION_EMAIL_CAPTURED},
                },
            ]},
            {"tag": "action", "actions": [
                {
                    "tag": "button", "type": "default",
                    "text": {"tag": "plain_text", "content": "只愿在平台沟通"},
                    "value": {**action_base, "action": ACTION_PLATFORM_ONGOING},
                },
                {
                    "tag": "button", "type": "danger",
                    "text": {"tag": "plain_text", "content": "拒绝/不适合"},
                    "value": {**action_base, "action": ACTION_NOT_FIT},
                },
                {
                    "tag": "button", "type": "default",
                    "text": {"tag": "plain_text", "content": "仍未回复"},
                    "value": {**action_base, "action": ACTION_NO_RESPONSE},
                },
            ]},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"[打开 KOL 主表记录]({_record_url(case.get('kol_record_id') or '')})"
            )}},
        ],
    }


def card_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from card_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from card_nodes(child)


def validate_outreach_card(card: dict) -> list[str]:
    errors: list[str] = []
    nodes = list(card_nodes(card))
    forms = [node for node in nodes if node.get("tag") == "form"]
    if len(forms) != 1:
        errors.append("卡片必须且只能有一个 form")
    submits = [
        node for node in nodes
        if node.get("tag") == "button" and node.get("action_type") == "form_submit"
    ]
    if len(submits) != 1:
        errors.append("卡片必须且只能有一个 form_submit")
    inputs = [node for node in nodes if node.get("tag") == "input" and node.get("name") == "business_email"]
    if len(inputs) != 1:
        errors.append("卡片缺少唯一邮箱输入框")
    if forms and any(
        isinstance(item, dict) and item.get("tag") == "action"
        for item in forms[0].get("elements", [])
    ):
        errors.append("禁止 form -> action 嵌套")
    rendered = json.dumps(card, ensure_ascii=False)
    for required in (
        "需要人工联系 1 位 KOL", "系统已自动检查", "上稿截止",
        *sorted(ALLOWED_ACTIONS),
    ):
        if required not in rendered:
            errors.append(f"卡片缺少必要内容：{required}")
    return errors


async def load_case(*, kol_record_id: str, campaign_id: str = "") -> dict:
    if not kol_record_id:
        raise ValueError("必须指定 kol_record_id，禁止批量猜测")
    kol = await feishu.get_record(config.T_KOL, kol_record_id)
    activities = await feishu.fetch_all_records(
        config.T_LAUNCH_CAMPAIGN, field_names=ACTIVITY_FIELDS, page_size=500,
    )
    participants = await feishu.fetch_all_records(
        config.T_LAUNCH_PARTICIPANT, field_names=PARTICIPANT_FIELDS, page_size=500,
    )
    active_activities = {
        _text((row.get("fields") or {}).get("活动ID")): row
        for row in activities
        if launch_daily_report._is_active_activity(row)
        and _text((row.get("fields") or {}).get("活动ID"))
    }
    matches = [
        row for row in participants
        if launch_daily_report._is_active_participant(row)
        and kol_record_id in _ids((row.get("fields") or {}).get("关联KOL"))
        and _text((row.get("fields") or {}).get("活动ID")) in active_activities
    ]
    if campaign_id:
        activity = active_activities.get(campaign_id)
        if not activity:
            raise ValueError("指定活动不是当前正式执行中的活动")
        participant = next(
            (row for row in matches if _text((row.get("fields") or {}).get("活动ID")) == campaign_id),
            None,
        )
    else:
        campaign_ids = sorted({
            _text((row.get("fields") or {}).get("活动ID")) for row in matches
        })
        if not campaign_ids:
            raise ValueError("该 KOL 没有唯一的活动参与记录，请明确传入 campaign_id")
        if len(campaign_ids) > 1:
            raise ValueError("该 KOL 同时属于多个活动，请明确传入 campaign_id，系统不会猜")
        campaign_id = campaign_ids[0]
        activity = active_activities[campaign_id]
        participant = next(row for row in matches if _text((row.get("fields") or {}).get("活动ID")) == campaign_id)
    preflight_kol = kol
    raw_email = _text((kol.get("fields") or {}).get("邮箱"))
    clean_email, _ = feishu.clean_email(raw_email)
    if raw_email and not clean_email:
        clean_fields = dict(kol.get("fields") or {})
        clean_fields["邮箱"] = ""
        preflight_kol = {**kol, "fields": clean_fields}
    contact_preflight = await kol_email_repair.inspect_record(preflight_kol)
    case = build_case(kol=kol, activity=activity, participant=participant)
    case["contact_preflight"] = contact_preflight
    return case


def _frankie_targets() -> list[tuple[str, str]]:
    return [
        (name, open_id) for name, open_id in config.NOTIFY_USERS
        if name.startswith("潘") or "Frankie" in name
    ]


async def _send_outreach_card(card: dict, *, frankie_only: bool) -> list[dict]:
    sent: list[dict] = []
    targets = _frankie_targets() if frankie_only else await feishu.resolve_notify_targets("reviewer")
    for name, open_id in targets:
        union_id = await feishu.open_id_to_union_id(open_id)
        if not union_id:
            sent.append({"name": name, "ok": False, "error": "open_id_to_union_id failed"})
            continue
        try:
            message_id = await feishu.send_card_via_app3("union_id", union_id, card)
            sent.append({"name": name, "ok": bool(message_id), "message_id": message_id})
        except Exception as exc:
            sent.append({"name": name, "ok": False, "error": str(exc)[:200]})
    return sent


async def send_card(
    *,
    kol_record_id: str,
    campaign_id: str = "",
    dry_run: bool = True,
    frankie_only: bool = True,
) -> dict:
    if not frankie_only and not config.KOL_NO_EMAIL_OUTREACH_ENABLED:
        raise ValueError("生产扩散未开放；当前只允许 Frankie 样卡")
    case = await load_case(kol_record_id=kol_record_id, campaign_id=campaign_id)
    preflight = case.get("contact_preflight") or {}
    preflight_status = _text(preflight.get("status"))
    if preflight_status == "public_contact_found":
        if dry_run:
            return {
                "ok": True, "dry_run": True, "frankie_only": frankie_only,
                "card_required": False, "status": "public_email_found",
                "kol_record_id": case["kol_record_id"],
                "campaign_id": case["campaign_id"],
                "source": preflight.get("source"),
                "email_fingerprint": preflight.get("email_fingerprint"),
            }
        repair = await kol_email_repair.run_email_repair(
            [case["kol_record_id"]], dry_run=False, limit=1,
        )
        repaired = bool(repair.get("safe_to_continue")) and int(repair.get("writes") or 0) == 1
        return {
            "ok": repaired, "dry_run": False, "frankie_only": frankie_only,
            "card_required": False,
            "status": "public_email_repaired" if repaired else "public_email_repair_failed",
            "kol_record_id": case["kol_record_id"],
            "campaign_id": case["campaign_id"],
            "repair": repair,
        }
    if (
        case.get("platform", "").casefold() == "youtube"
        and preflight_status == "no_public_email"
    ):
        auth_check = await _ensure_youtube_authenticated_check(case, dry_run=dry_run)
        if auth_check.get("status") != "youtube_auth_no_visible_email":
            return {
                "ok": bool(auth_check.get("ok")),
                "dry_run": dry_run,
                "frankie_only": frankie_only,
                "card_required": False,
                "kol_record_id": case["kol_record_id"],
                "campaign_id": case["campaign_id"],
                **auth_check,
            }
        case["youtube_authenticated_checked"] = True
    card = build_outreach_card(case)
    errors = validate_outreach_card(card)
    if errors:
        raise ValueError("无邮箱联系卡结构不合格：" + "；".join(errors))
    if dry_run:
        return {
            "ok": True, "dry_run": True, "frankie_only": frankie_only,
            "card_required": True, "case": case, "card": card,
        }
    sent = await _send_outreach_card(card, frankie_only=frankie_only)
    return {
        "ok": all(item.get("ok") for item in sent) and bool(sent),
        "dry_run": False,
        "frankie_only": frankie_only,
        "card_required": True,
        "kol_record_id": case["kol_record_id"],
        "campaign_id": case["campaign_id"],
        "sent": sent,
    }


def _message_id(event: dict) -> str:
    for value in (
        event.get("message_id"), event.get("open_message_id"),
        (event.get("context") or {}).get("open_message_id"),
        (event.get("context") or {}).get("message_id"),
    ):
        if value:
            return _text(value)
    return ""


def _operator_label(event: dict) -> str:
    operator = event.get("operator") or event.get("operator_user") or event.get("user") or {}
    return (
        _text(operator.get("name")) or _text(operator.get("open_id"))
        or _text(event.get("sender_name")) or _text(event.get("operator_open_id"))
        or _text(event.get("sender_open_id")) or "unknown"
    )


def _extract_callback(event: dict) -> tuple[str, dict, dict]:
    action = event.get("action") or {}
    value = action.get("value") or event.get("card_action") or event.get("value") or {}
    form = action.get("form_value") or event.get("card_form_value") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {"action": value}
    if isinstance(form, str):
        try:
            form = json.loads(form)
        except json.JSONDecodeError:
            form = {}
    return _text(value.get("action")), value, form


def _marker(campaign_id: str, action: str) -> str:
    suffix = {
        ACTION_EMAIL_CAPTURED: "email_captured",
        ACTION_PLATFORM_ONGOING: "platform_ongoing",
        ACTION_NOT_FIT: "not_fit",
        ACTION_NO_RESPONSE: "no_response",
    }[action]
    return f"[NO_EMAIL_DM:{campaign_id}:{suffix}]"


def _existing_action(current_log: str, campaign_id: str) -> str:
    match = re.search(
        rf"\[NO_EMAIL_DM:{re.escape(campaign_id)}:"
        r"(email_captured|platform_ongoing|not_fit|no_response)\]",
        current_log,
    )
    if not match:
        return ""
    return {
        "email_captured": ACTION_EMAIL_CAPTURED,
        "platform_ongoing": ACTION_PLATFORM_ONGOING,
        "not_fit": ACTION_NOT_FIT,
        "no_response": ACTION_NO_RESPONSE,
    }[match.group(1)]


def _desired_fields(action: str, *, email: str = "") -> dict:
    if action == ACTION_EMAIL_CAPTURED:
        return {
            "邮箱": email,
            "邮箱验真状态": "未验",
            "资料可用状态": "人工核实有效",
            "触达路由状态": "可新开发",
            "合作状态": "建联中",
        }
    if action == ACTION_PLATFORM_ONGOING:
        return {"触达路由状态": "沿用原线程", "合作状态": "建联中"}
    if action == ACTION_NOT_FIT:
        return {"触达路由状态": "禁止新开发", "合作状态": "不合适"}
    return {"资料可用状态": "缺资料", "触达路由状态": "禁止新开发"}


def _fields_match(fields: dict, desired: dict) -> bool:
    return all(_text(fields.get(key)).lower() == _text(value).lower() for key, value in desired.items())


def build_processed_card(*, action: str, operator: str, campaign_id: str, email: str = "") -> dict:
    labels = {
        ACTION_EMAIL_CAPTURED: ("邮箱已回填，等待质量检查", f"商务邮箱：`{email}`\n不会自动发送邮件。"),
        ACTION_PLATFORM_ONGOING: ("已转人工平台沟通", "保留原平台线程，不开启新的自动邮件。"),
        ACTION_NOT_FIT: ("已停止开发", "该 KOL 已标记为不合适，后续禁止新开发。"),
        ACTION_NO_RESPONSE: ("已标记暂不可达", "已提醒仍无回复；保留记录，不把它误判为内容不合适。"),
    }
    title, detail = labels[action]
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": "green", "title": {"tag": "plain_text", "content": f"🟢 [KOL·P1] {title}"}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": (
            f"**活动ID**：`{campaign_id}`\n**操作人**：{operator}\n{detail}"
        )}}],
    }


async def handle_callback(event: dict) -> dict:
    action, value, form = _extract_callback(event)
    if action not in ALLOWED_ACTIONS:
        return {"ok": False, "ignored": True, "action": action}
    kol_record_id = _text(value.get("kol_record_id"))
    campaign_id = _text(value.get("campaign_id"))
    platform = _text(value.get("platform")) or "公开社媒平台"
    if not kol_record_id or not campaign_id:
        return {"ok": False, "error": "卡片缺少 KOL 或活动标识，请刷新卡片"}
    email = _text(form.get("business_email")).lower()
    if action == ACTION_EMAIL_CAPTURED and not EMAIL_RE.fullmatch(email):
        return {"ok": False, "error": "邮箱格式不正确，请填写完整商务邮箱后再提交"}

    kol = await feishu.get_record(config.T_KOL, kol_record_id)
    fields = kol.get("fields") or {}
    marker = _marker(campaign_id, action)
    desired = _desired_fields(action, email=email)
    current_log = _text(fields.get("决策反哺日志"))
    msg_id = _message_id(event)
    operator = _operator_label(event)

    existing_action = _existing_action(current_log, campaign_id)
    if existing_action and existing_action != action:
        existing_email = _text(fields.get("邮箱")) if existing_action == ACTION_EMAIL_CAPTURED else ""
        if msg_id:
            await feishu.update_card_message_with_app(
                msg_id,
                build_processed_card(
                    action=existing_action, operator=operator,
                    campaign_id=campaign_id, email=existing_email,
                ),
                which="app3",
            )
        return {
            "ok": True, "idempotent": True, "conflict_ignored": True,
            "kol_record_id": kol_record_id, "campaign_id": campaign_id,
            "action": existing_action, "requested_action": action,
            "patched": bool(msg_id),
        }

    if marker in current_log and _fields_match(fields, desired):
        if msg_id:
            await feishu.update_card_message_with_app(
                msg_id,
                build_processed_card(action=action, operator=operator, campaign_id=campaign_id, email=email),
                which="app3",
            )
        return {
            "ok": True, "idempotent": True, "kol_record_id": kol_record_id,
            "campaign_id": campaign_id, "action": action, "patched": bool(msg_id),
        }

    update = dict(desired)
    if marker not in current_log:
        timestamp = datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S %z")
        provenance = (
            f"{marker} source={'KOL本人提供' if action == ACTION_EMAIL_CAPTURED else '运营确认'}; "
            f"channel={platform}; operator={operator}; time={timestamp}"
        )
        update["决策反哺日志"] = "\n".join(filter(None, [current_log, provenance]))
    await feishu.update_record(config.T_KOL, kol_record_id, update)
    if msg_id:
        await feishu.update_card_message_with_app(
            msg_id,
            build_processed_card(action=action, operator=operator, campaign_id=campaign_id, email=email),
            which="app3",
        )
    return {
        "ok": True, "idempotent": False, "kol_record_id": kol_record_id,
        "campaign_id": campaign_id, "action": action, "patched": bool(msg_id),
    }
