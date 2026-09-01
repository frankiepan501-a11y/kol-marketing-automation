# -*- coding: utf-8 -*-
"""无邮箱 KOL 的人工私信取邮箱闭环。

本模块只把“系统已检查公开页面但没有可用邮箱”的例外变成一张可执行卡片：
运营复制模板到公开平台私信；KOL 提供邮箱后回填主表并回到邮箱质量检查。

安全边界：
- 不调用社媒私信 API，不代替运营发送私信；
- 不发送邮件；取得的邮箱先标记为“未验”，继续走既有邮箱质量检查；
- 生产扩散默认关闭，只允许给 Frankie 发 1 张样卡；
- 互动卡由聪哥分身3号发送，回调后由同一 App PATCH 原卡。
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from . import config, feishu, launch_daily_report
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
    "Hi [Name], we’re reaching out from [Brand] about a possible collaboration "
    "for [Product]. What’s the best business email for us to send the full brief and timeline?"
)
DM_TEMPLATE_ZH = (
    "你好，[Name]，我们是 [Brand]，想就 [Product] 的潜在合作与你联系。"
    "请问哪个商务邮箱最适合接收完整合作说明和时间安排？"
)

BJ = timezone(timedelta(hours=8))
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)

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
        _text(pf.get("产品家族ID"))
        or _text(af.get("产品英文名"))
        or _text(af.get("ERP SKU"))
        or "当前活动产品"
    )
    task_nature = " / ".join(filter(None, [
        _text(pf.get("进入方式")), _text(pf.get("活动分池")), _text(pf.get("对象类型")),
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


def build_outreach_card(case: dict) -> dict:
    action_base = {
        "kol_record_id": case.get("kol_record_id"),
        "campaign_id": case.get("campaign_id"),
        "platform": case.get("platform"),
    }
    page_lines = case.get("public_pages") or []
    pages = "\n".join(f"> {item}" for item in page_lines) or "> 未记录可打开的公开页"
    deadline_label = _fmt_ms(case.get("deadline")) if case.get("timed_upload_required") else "无规定时间"
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "🟠 [KOL·P1] 无邮箱私信获取联系邮箱 · Frankie样卡"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"**KOL**：{case.get('kol_name')}\n"
                f"**平台**：{case.get('platform')}　**国家/语言**：{case.get('country')} / {case.get('language')}\n"
                f"**粉丝数**：{case.get('followers')}\n"
                f"**近期内容**：{case.get('recent_content')}\n"
                f"**适配依据**：{case.get('fit_reason')}\n"
                f"**缺邮箱原因**：{case.get('email_gap')}"
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "**公开页面检查**\n" + pages
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"**活动**：{case.get('campaign_name')} (`{case.get('campaign_id')}`)\n"
                f"**品牌/产品**：{case.get('brand')} / {case.get('product')}\n"
                f"**任务性质**：{case.get('task_nature')}\n"
                f"**规定时间上稿**：{'是' if case.get('timed_upload_required') else '否'}；截止 {deadline_label}\n"
                f"**活动目标上稿数**：{case.get('target_uploads')}"
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "**运营要做什么**\n"
                "1. 打开 KOL 公开主页，在平台里人工发送下面英文模板。\n"
                "2. KOL 给出邮箱后，填入卡片并提交；系统只回填主表和进入邮箱质量检查。\n"
                "3. 若对方要求一直在平台沟通，选“继续平台沟通”；系统不会开启自动邮件。\n\n"
                "**英文私信模板（复制发送）**\n> " + DM_TEMPLATE_EN + "\n\n"
                "**中文翻译（仅供运营理解）**\n> " + DM_TEMPLATE_ZH + "\n\n"
                "⚠️ 系统不会代发私信，也不会因本卡直接发送邮件。"
            )}},
            {"tag": "form", "name": f"kol_no_email_{case.get('kol_record_id')}", "elements": [
                {
                    "tag": "input", "name": "business_email", "width": "fill",
                    "label_position": "top", "required": True,
                    "label": {"tag": "plain_text", "content": "KOL 本人回复的商务邮箱"},
                    "placeholder": {"tag": "plain_text", "content": "例如 creator@example.com"},
                    "max_length": 254,
                },
                {
                    "tag": "button", "action_type": "form_submit", "name": "submit_email",
                    "type": "primary", "text": {"tag": "plain_text", "content": "回填邮箱并进入质量检查"},
                    "value": {**action_base, "action": ACTION_EMAIL_CAPTURED},
                },
            ]},
            {"tag": "action", "actions": [{
                "tag": "button", "type": "default",
                "text": {"tag": "plain_text", "content": "对方要求继续平台沟通"},
                "value": {**action_base, "action": ACTION_PLATFORM_ONGOING},
            }]},
            {"tag": "action", "actions": [{
                "tag": "button", "type": "danger",
                "text": {"tag": "plain_text", "content": "拒绝/不相关，停止开发"},
                "value": {**action_base, "action": ACTION_NOT_FIT},
            }]},
            {"tag": "action", "actions": [{
                "tag": "button", "type": "default",
                "text": {"tag": "plain_text", "content": "已提醒仍无回复"},
                "value": {**action_base, "action": ACTION_NO_RESPONSE},
            }]},
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
        "无邮箱私信获取联系邮箱", "公开页面检查", "规定时间上稿", "系统不会代发私信",
        DM_TEMPLATE_EN, DM_TEMPLATE_ZH, *sorted(ALLOWED_ACTIONS),
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
    return build_case(kol=kol, activity=activity, participant=participant)


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
    card = build_outreach_card(case)
    errors = validate_outreach_card(card)
    if errors:
        raise ValueError("无邮箱联系卡结构不合格：" + "；".join(errors))
    if dry_run:
        return {"ok": True, "dry_run": True, "frankie_only": frankie_only, "case": case, "card": card}
    sent = await _send_outreach_card(card, frankie_only=frankie_only)
    return {
        "ok": all(item.get("ok") for item in sent) and bool(sent),
        "dry_run": False,
        "frankie_only": frankie_only,
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
