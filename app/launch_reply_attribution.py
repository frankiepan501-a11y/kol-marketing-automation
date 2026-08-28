# -*- coding: utf-8 -*-
"""集中宣发入站回复的活动归属确认。

当原始邮件 message id 无法把回复唯一归入某个正式活动时，本模块把
“系统无法判断”转换为一张可操作的运营卡，而不是从活动进度里静默排除。

安全边界：
- 只写回复草稿上的活动归属字段，不发送 KOL 邮件；
- 拒绝/退订、合作结束后的纯道谢不要求活动归属；
- 互动卡固定由聪哥分身3号发送并由同一 App PATCH 原卡；
- 卡片只有一个 form_submit，避免飞书客户端按钮丢失或竖排。
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from . import config, feishu, launch_daily_report
from .feishu import ext


ACTION_CONFIRM = "launch_reply_attribution_confirm"
NO_CAMPAIGN = "__no_current_campaign__"
STATUS_PENDING = "待运营确认"
STATUS_CONFIRMED = "已确认"
STATUS_AUTO = "自动确认"
STATUS_NONE = "无需归属"

BJ = timezone(timedelta(hours=8))
TERMINAL_INTENTS = {"委婉拒绝", "退订"}
COMPLETED_COOP_STATES = {
    "已合作", "已合作-免费", "已合作-免费(多次)", "已合作-付费", "多次合作",
}
THANKS_RE = re.compile(
    r"^(?:hi\s+[^,]+[,!]?\s*)?(?:thank(?:s|\s+you)(?:\s+(?:again|so\s+much))?[.!\s]*)+$",
    re.I,
)
NONTERMINAL_THANKS_HINTS = (
    "sample", "arrived", "received", "post", "publish", "upload", "video", "review",
    "tracking", "address", "when", "deadline", "date", "link", "question", "?",
)

ACTIVITY_FIELDS = [
    "活动ID", "活动名称", "品牌", "运行模式", "状态", "窗口开始",
    "发布窗口中点", "窗口结束", "目标上稿数",
]
PARTICIPANT_FIELDS = [
    "活动ID", "参与状态", "审核结论", "关联KOL", "产品家族ID", "对象类型",
    "进入方式", "活动分池", "计划上稿时间", "承诺上稿时间", "实际上稿时间",
    "关联邮件草稿",
]
DRAFT_FIELDS = list(dict.fromkeys([
    *launch_daily_report.DRAFT_FIELDS,
    "邮件主题", "邮件正文", "审批意见", "关联任务",
    "集中宣发活动ID", "活动归属状态", "活动归属确认人",
    "活动归属确认时间", "活动归属卡片消息ID",
]))
CONTACT_FIELDS = ["账号名", "邮箱", "合作状态", "主平台", "国家", "国家原文"]
PRODUCT_FIELDS = ["产品名", "产品英文名", "品牌"]


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


def _record_url(table_id: str, record_id: str) -> str:
    return (
        f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
        f"?table={table_id}&record={record_id}"
    )


def is_terminal_without_attribution(reply: dict, *, cooperation_status: str = "") -> bool:
    """判断回复是否无需继续分配活动。

    “谢谢，样品已到/何时上稿”等仍会改变业务阶段，不能按纯道谢关闭。
    """
    fields = reply.get("fields") or {}
    if _text(fields.get("回复意图")) in TERMINAL_INTENTS:
        return True
    if _text(cooperation_status) not in COMPLETED_COOP_STATES:
        return False
    body = _text(fields.get("回复原文"))
    body = re.sub(r"^\[MID:[^\]]+\]\s*", "", body).strip()
    lowered = body.lower()
    if any(hint in lowered for hint in NONTERMINAL_THANKS_HINTS):
        return False
    return bool(THANKS_RE.fullmatch(body))


def _campaign_rows(activities: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in activities:
        if not launch_daily_report._is_active_activity(row):
            continue
        campaign_id = _text((row.get("fields") or {}).get("活动ID"))
        if campaign_id and campaign_id not in out:
            out[campaign_id] = row
    return out


def _candidate(
    activity: dict,
    participant: dict | None,
    *,
    reason: str,
) -> dict:
    af = activity.get("fields") or {}
    pf = (participant or {}).get("fields") or {}
    planned = _ts(pf.get("计划上稿时间"))
    committed = _ts(pf.get("承诺上稿时间"))
    midpoint = _ts(af.get("发布窗口中点"))
    window_end = _ts(af.get("窗口结束"))
    deadline = committed or planned or midpoint or window_end
    return {
        "campaign_id": _text(af.get("活动ID")),
        "campaign_name": _text(af.get("活动名称")),
        "brand": _text(af.get("品牌")),
        "participant_record_id": (participant or {}).get("record_id", ""),
        "object_type": _text(pf.get("对象类型")) or "KOL",
        "entry_mode": _text(pf.get("进入方式")) or "未标明",
        "task_nature": _text(pf.get("活动分池")) or "未标明",
        "planned_upload_at": planned,
        "committed_upload_at": committed,
        "window_start": _ts(af.get("窗口开始")),
        "window_midpoint": midpoint,
        "window_end": window_end,
        "deadline": deadline,
        "timed_upload_required": bool(deadline),
        "match_reason": reason,
    }


def _case_candidates(
    reply: dict,
    *,
    activities_by_id: dict[str, dict],
    participants: list[dict],
) -> list[dict]:
    fields = reply.get("fields") or {}
    contact_ids = set(_ids(fields.get("关联KOL")))
    product_ids = set(_ids(fields.get("关联产品")))
    ranked: dict[str, tuple[int, dict]] = {}

    for participant in participants:
        if not launch_daily_report._is_active_participant(participant):
            continue
        pf = participant.get("fields") or {}
        campaign_id = _text(pf.get("活动ID"))
        activity = activities_by_id.get(campaign_id)
        if not activity:
            continue
        participant_contacts = set(_ids(pf.get("关联KOL")))
        participant_product = _text(pf.get("产品家族ID"))
        contact_match = bool(contact_ids and participant_contacts and contact_ids == participant_contacts)
        product_match = bool(product_ids and participant_product in product_ids)
        if contact_match and product_match:
            rank, reason = 3, "同一KOL＋同一产品"
        elif contact_match:
            rank, reason = 2, "同一KOL，产品需运营确认"
        elif product_match:
            rank, reason = 1, "同一产品，KOL需运营确认"
        else:
            continue
        current = ranked.get(campaign_id)
        row = _candidate(activity, participant, reason=reason)
        if not current or rank > current[0]:
            ranked[campaign_id] = (rank, row)

    if not ranked:
        # 没有身份线索时仍给当前活动选项，避免“系统判断不了”变成死路。
        for campaign_id, activity in activities_by_id.items():
            ranked[campaign_id] = (0, _candidate(activity, None, reason="当前正式活动"))

    return [item[1] for item in sorted(
        ranked.values(), key=lambda item: (-item[0], item[1]["campaign_name"], item[1]["campaign_id"]),
    )]


def collect_unmatched_reply_cases(
    *,
    activities: list[dict],
    participants: list[dict],
    drafts: list[dict],
    contacts: dict[str, dict] | None = None,
    products: dict[str, dict] | None = None,
) -> list[dict]:
    """把无法唯一归属的实时回复转换为可发卡案例。"""
    activities_by_id = _campaign_rows(activities)
    if not activities_by_id:
        return []
    contacts = contacts or {}
    products = products or {}
    drafts_by_id = {
        row.get("record_id"): row for row in drafts if row.get("record_id")
    }
    evidence_by_campaign = {
        campaign_id: launch_daily_report._campaign_reply_evidence(
            [
                row for row in participants
                if _text((row.get("fields") or {}).get("活动ID")) == campaign_id
            ],
            drafts_by_id,
        )
        for campaign_id in activities_by_id
    }
    out: list[dict] = []
    for reply in drafts:
        if not launch_daily_report._is_live_human_reply_draft(reply):
            continue
        fields = reply.get("fields") or {}
        explicit_campaign = _text(fields.get("集中宣发活动ID"))
        explicit_status = _text(fields.get("活动归属状态"))
        if explicit_campaign and explicit_status in {STATUS_CONFIRMED, STATUS_AUTO}:
            continue
        if explicit_status == STATUS_NONE:
            continue
        automatic_matches = [
            campaign_id for campaign_id, evidence in evidence_by_campaign.items()
            if launch_daily_report._reply_matches_campaign(reply, evidence)
        ]
        if len(automatic_matches) == 1:
            continue

        contact_ids = _ids(fields.get("关联KOL"))
        product_ids = _ids(fields.get("关联产品"))
        contact = contacts.get(contact_ids[0], {}) if len(contact_ids) == 1 else {}
        product = products.get(product_ids[0], {}) if len(product_ids) == 1 else {}
        cf = contact.get("fields") or {}
        pf = product.get("fields") or {}
        if is_terminal_without_attribution(
            reply, cooperation_status=_text(cf.get("合作状态")),
        ):
            continue
        candidates = _case_candidates(
            reply, activities_by_id=activities_by_id, participants=participants,
        )
        if not candidates:
            continue
        out.append({
            "reply_record_id": reply.get("record_id", ""),
            "reply_subject": _text(fields.get("邮件主题")),
            # `回复原文` is the inbound KOL email. `邮件正文` is our outbound
            # reply suggestion. Never fall back from the former to the latter:
            # legacy records may not have persisted the inbound body, and that
            # fallback would falsely present our draft as the KOL's own words.
            "reply_body": _text(fields.get("回复原文")),
            "suggested_reply_body": _text(fields.get("邮件正文")),
            "reply_intent": _text(fields.get("回复意图")) or "不明意图",
            "reply_target_message_id": _text(fields.get("回复目标MsgID")),
            "kol_record_id": contact_ids[0] if len(contact_ids) == 1 else "",
            "kol_name": _text(cf.get("账号名")) or (contact_ids[0] if len(contact_ids) == 1 else "未能唯一识别"),
            "cooperation_status": _text(cf.get("合作状态")) or "未标明",
            "product_record_id": product_ids[0] if len(product_ids) == 1 else "",
            "product_name": _text(pf.get("产品名")) or _text(pf.get("产品英文名")) or (
                product_ids[0] if len(product_ids) == 1 else "未能唯一识别"
            ),
            "attribution_status": STATUS_PENDING,
            "attribution_reason": (
                "原邮件ID没有唯一命中当前活动"
                if _text(fields.get("回复目标MsgID")) else "回复缺少原邮件ID"
            ),
            "card_message_id": _text(fields.get("活动归属卡片消息ID")),
            "candidates": candidates,
        })
    return out


def _option_text(candidate: dict) -> str:
    deadline = _fmt_ms(candidate.get("deadline")) if candidate.get("timed_upload_required") else "无固定上稿时间"
    text = (
        f"{candidate.get('campaign_name') or candidate.get('campaign_id')}｜"
        f"{candidate.get('task_nature')}｜{deadline}"
    )
    return text[:95]


def build_attribution_card(case: dict) -> dict:
    options = [
        {
            "text": {"tag": "plain_text", "content": _option_text(candidate)},
            "value": candidate["campaign_id"],
        }
        for candidate in case.get("candidates") or []
        if candidate.get("campaign_id")
    ]
    options.append({
        "text": {"tag": "plain_text", "content": "不属于当前活动／无需继续归属"},
        "value": NO_CAMPAIGN,
    })
    candidate_lines = []
    for candidate in case.get("candidates") or []:
        deadline_label = (
            f"规定时间上稿：是；当前截止 {_fmt_ms(candidate.get('deadline'))}"
            if candidate.get("timed_upload_required")
            else "规定时间上稿：否"
        )
        candidate_lines.append(
            f"- **{candidate.get('campaign_name') or candidate.get('campaign_id')}** "
            f"(`{candidate.get('campaign_id')}`)\n"
            f"  任务性质：{candidate.get('entry_mode')} / {candidate.get('task_nature')} / "
            f"{candidate.get('object_type')}；{deadline_label}；匹配依据：{candidate.get('match_reason')}"
        )
    reply_body = re.sub(r"^\[MID:[^\]]+\]\s*", "", case.get("reply_body") or "")[:500]
    suggested_reply_body = (case.get("suggested_reply_body") or "")[:500]
    inbound_panel = (
        f"**KOL 来信原文**\n> {reply_body}"
        if reply_body
        else (
            "**KOL 来信原文**\n"
            "> ⚠️ 历史记录未保存 KOL 来信原文。"
            "请查看原邮件线程；不要把下方系统草稿当作 KOL 原话。"
        )
    )
    suggested_panel = (
        f"**系统建议回复草稿（供审核，不是 KOL 来信）**\n> {suggested_reply_body}"
        if suggested_reply_body
        else "**系统建议回复草稿（供审核，不是 KOL 来信）**\n> 当前记录没有建议草稿。"
    )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "yellow",
            "title": {"tag": "plain_text", "content": "🟠 [KOL·P1] 活动归属待确认 · 回复继续处理"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"**KOL**：{case.get('kol_name') or '-'}\n"
                f"**产品**：{case.get('product_name') or '-'}\n"
                f"**回复意图**：{case.get('reply_intent') or '-'}\n"
                f"**系统卡点**：{case.get('attribution_reason') or '-'}\n\n"
                "系统没有猜活动，也没有停止回复流程。请只确认该回复属于哪个正在执行的活动；"
                "确认后，原回复审核卡继续处理。"
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": inbound_panel}},
            {"tag": "div", "text": {"tag": "lark_md", "content": suggested_panel}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**当前可选活动与任务要求**\n" + "\n".join(candidate_lines)}},
            {"tag": "form", "name": f"launch_reply_attr_{case.get('reply_record_id')}", "elements": [
                {
                    "tag": "select_static",
                    "name": "campaign_id",
                    "placeholder": {"tag": "plain_text", "content": "选择该回复所属活动"},
                    "options": options,
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": "submit",
                    "type": "primary",
                    "text": {"tag": "plain_text", "content": "确认归属并继续回复流程"},
                    "value": {
                        "action": ACTION_CONFIRM,
                        "reply_record_id": case.get("reply_record_id"),
                        "allowed_campaign_ids": [c.get("campaign_id") for c in case.get("candidates") or []],
                    },
                },
            ]},
            {"tag": "action", "actions": [{
                "tag": "button",
                "text": {"tag": "plain_text", "content": "打开回复草稿"},
                "url": _record_url(config.T_DRAFT, case.get("reply_record_id") or ""),
                "type": "default",
            }]},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": (
                "拒绝、退订和合作结束后的纯道谢由系统直接收口；样品已到、询问发布日期、提供链接等仍继续处理。"
            )}]},
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


def validate_attribution_card(card: dict) -> list[str]:
    errors: list[str] = []
    nodes = list(card_nodes(card))
    forms = [node for node in nodes if node.get("tag") == "form"]
    if len(forms) != 1:
        errors.append("卡片必须且只能有一个form")
    submits = [
        node for node in nodes
        if node.get("tag") == "button" and node.get("action_type") == "form_submit"
    ]
    if len(submits) != 1:
        errors.append("卡片必须且只能有一个form_submit")
    selects = [node for node in nodes if node.get("tag") == "select_static" and node.get("name") == "campaign_id"]
    if len(selects) != 1:
        errors.append("卡片缺少唯一活动选择器")
    rendered = json.dumps(card, ensure_ascii=False)
    for required in (
        "任务性质", "规定时间上稿", "活动归属待确认",
        "KOL 来信原文", "系统建议回复草稿", ACTION_CONFIRM,
    ):
        if required not in rendered:
            errors.append(f"卡片缺少必要内容：{required}")
    return errors


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


def build_processed_card(*, campaign_id: str, operator: str, no_campaign: bool = False) -> dict:
    if no_campaign:
        title = "🟢 [KOL·P1] 回复活动归属已收口"
        body = f"**结论**：不属于当前集中宣发活动\n**操作人**：{operator}\n该回复不再进入活动进度，但原回复审核记录保留。"
    else:
        title = "🟢 [KOL·P1] 回复活动归属已确认"
        body = (
            f"**活动ID**：`{campaign_id}`\n**操作人**：{operator}\n"
            "活动归属已写入；请继续处理原回复审核卡，不需要重新生成回复。"
        )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": "green", "title": {"tag": "plain_text", "content": title}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": body}}],
    }


async def handle_callback(event: dict) -> dict:
    action, value, form = _extract_callback(event)
    if action != ACTION_CONFIRM:
        return {"ok": False, "ignored": True, "action": action}
    reply_record_id = _text(value.get("reply_record_id"))
    campaign_id = _text(form.get("campaign_id"))
    allowed = {_text(item) for item in (value.get("allowed_campaign_ids") or []) if _text(item)}
    if not reply_record_id:
        return {"ok": False, "error": "missing reply_record_id"}
    if not campaign_id:
        return {"ok": False, "error": "请选择活动后再提交"}
    if campaign_id != NO_CAMPAIGN and campaign_id not in allowed:
        return {"ok": False, "error": "所选活动不在本卡允许范围，请刷新卡片"}

    draft = await feishu.get_record(config.T_DRAFT, reply_record_id)
    fields = draft.get("fields") or {}
    current_campaign = _text(fields.get("集中宣发活动ID"))
    current_status = _text(fields.get("活动归属状态"))
    msg_id = _message_id(event) or _text(fields.get("活动归属卡片消息ID"))
    operator = _operator_label(event)
    if current_status in {STATUS_CONFIRMED, STATUS_AUTO, STATUS_NONE}:
        if msg_id:
            await feishu.update_card_message_with_app(
                msg_id,
                build_processed_card(
                    campaign_id=current_campaign,
                    operator=_text(fields.get("活动归属确认人")) or operator,
                    no_campaign=current_status == STATUS_NONE,
                ),
                which="app3",
            )
        return {
            "ok": True, "idempotent": True, "reply_record_id": reply_record_id,
            "campaign_id": current_campaign, "patched": bool(msg_id),
        }

    no_campaign = campaign_id == NO_CAMPAIGN
    update = {
        "集中宣发活动ID": "" if no_campaign else campaign_id,
        "活动归属状态": STATUS_NONE if no_campaign else STATUS_CONFIRMED,
        "活动归属确认人": operator,
        "活动归属确认时间": int(time.time() * 1000),
    }
    if msg_id:
        update["活动归属卡片消息ID"] = msg_id
    await feishu.update_record(config.T_DRAFT, reply_record_id, update)
    if msg_id:
        await feishu.update_card_message_with_app(
            msg_id,
            build_processed_card(
                campaign_id="" if no_campaign else campaign_id,
                operator=operator,
                no_campaign=no_campaign,
            ),
            which="app3",
        )
    return {
        "ok": True, "idempotent": False, "reply_record_id": reply_record_id,
        "campaign_id": "" if no_campaign else campaign_id,
        "no_campaign": no_campaign, "patched": bool(msg_id),
    }


def _candidate_source_draft_ids(
    reply: dict,
    *,
    activities: list[dict],
    participants: list[dict],
) -> list[str]:
    """只取可能与目标回复有关的活动原草稿，避免扫描整张草稿表。"""
    active_campaigns = set(_campaign_rows(activities))
    fields = reply.get("fields") or {}
    contact_ids = set(_ids(fields.get("关联KOL")))
    product_ids = set(_ids(fields.get("关联产品")))
    draft_ids: set[str] = set()
    for participant in participants:
        if not launch_daily_report._is_active_participant(participant):
            continue
        pf = participant.get("fields") or {}
        if _text(pf.get("活动ID")) not in active_campaigns:
            continue
        participant_contacts = set(_ids(pf.get("关联KOL")))
        participant_product = _text(pf.get("产品家族ID"))
        contact_match = bool(contact_ids and participant_contacts and contact_ids == participant_contacts)
        product_match = bool(product_ids and participant_product in product_ids)
        if contact_match or product_match:
            draft_ids.update(_ids(pf.get("关联邮件草稿")))
    return sorted(draft_ids)


async def _load_source(*, reply_record_id: str = "") -> dict:
    activities, participants = await asyncio.gather(
        feishu.fetch_all_records(
            config.T_LAUNCH_CAMPAIGN, field_names=ACTIVITY_FIELDS, page_size=100,
        ),
        feishu.fetch_all_records(
            config.T_LAUNCH_PARTICIPANT, field_names=PARTICIPANT_FIELDS, page_size=500,
        ),
    )
    if reply_record_id:
        reply = await feishu.get_record(config.T_DRAFT, reply_record_id)
        load_warnings: list[str] = []

        async def optional_record(table_id: str, record_id: str, kind: str):
            try:
                return await feishu.get_record(table_id, record_id)
            except Exception as exc:
                # 旧活动可能仍关联已归档/删除记录；这类缺口不能让回复归属流程再次卡死。
                load_warnings.append(f"{kind}:{record_id}:{type(exc).__name__}")
                return None

        source_draft_ids = _candidate_source_draft_ids(
            reply, activities=activities, participants=participants,
        )
        source_drafts = await asyncio.gather(*(
            optional_record(config.T_DRAFT, record_id, "draft")
            for record_id in source_draft_ids
            if record_id != reply_record_id
        ))
        fields = reply.get("fields") or {}
        contact_ids = sorted(set(_ids(fields.get("关联KOL"))))
        product_ids = sorted(set(_ids(fields.get("关联产品"))))
        contact_rows, product_rows = await asyncio.gather(
            asyncio.gather(*(
                optional_record(config.T_KOL, record_id, "contact") for record_id in contact_ids
            )),
            asyncio.gather(*(
                optional_record(config.T_PRODUCT, record_id, "product") for record_id in product_ids
            )),
        )
        source_drafts = [row for row in source_drafts if row]
        contact_rows = [row for row in contact_rows if row]
        product_rows = [row for row in product_rows if row]
        return {
            "activities": activities,
            "participants": participants,
            "drafts": [reply, *source_drafts],
            "contacts": {row.get("record_id"): row for row in contact_rows if row.get("record_id")},
            "products": {row.get("record_id"): row for row in product_rows if row.get("record_id")},
            "load_warnings": load_warnings,
        }

    drafts = await feishu.fetch_all_records(
        config.T_DRAFT, field_names=DRAFT_FIELDS, page_size=500,
    )
    contact_rows = await feishu.fetch_all_records(
        config.T_KOL, field_names=CONTACT_FIELDS, page_size=500,
    )
    product_rows = await feishu.fetch_all_records(
        config.T_PRODUCT, field_names=PRODUCT_FIELDS, page_size=500,
    )
    return {
        "activities": activities,
        "participants": participants,
        "drafts": drafts,
        "contacts": {row.get("record_id"): row for row in contact_rows if row.get("record_id")},
        "products": {row.get("record_id"): row for row in product_rows if row.get("record_id")},
    }


def _frankie_targets() -> list[tuple[str, str]]:
    return [
        (name, open_id) for name, open_id in config.NOTIFY_USERS
        if name.startswith("潘") or "Frankie" in name
    ]


async def _send_card(card: dict, *, frankie_only: bool) -> list[dict]:
    targets = _frankie_targets() if frankie_only else await feishu.resolve_notify_targets("reviewer")
    sent: list[dict] = []
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


async def scan_and_send(
    *,
    dry_run: bool = True,
    frankie_only: bool = True,
    campaign_id: str = "",
    reply_record_id: str = "",
    limit: int = 10,
    refresh_existing_card: bool = False,
) -> dict:
    if refresh_existing_card and not reply_record_id:
        raise ValueError("刷新现有归属卡必须指定 reply_record_id，禁止批量刷新或补发新卡")
    source = await _load_source(reply_record_id=reply_record_id)
    # `_load_source` also returns read-only diagnostic metadata such as
    # `load_warnings`.  Pass only business inputs to the collector so a stale
    # optional link can be reported without turning a valid scan into HTTP 500.
    cases = collect_unmatched_reply_cases(
        activities=source["activities"],
        participants=source["participants"],
        drafts=source["drafts"],
        contacts=source.get("contacts") or {},
        products=source.get("products") or {},
    )
    if campaign_id:
        cases = [
            case for case in cases
            if any(c.get("campaign_id") == campaign_id for c in case.get("candidates") or [])
        ]
    items = []
    for case in cases[:max(0, int(limit or 0))]:
        card = build_attribution_card(case)
        errors = validate_attribution_card(card)
        if errors:
            raise ValueError("活动归属卡结构不合格：" + "；".join(errors))
        if case.get("card_message_id"):
            item = {
                "reply_record_id": case["reply_record_id"],
                "message_id": case["card_message_id"],
            }
            if not refresh_existing_card:
                item["skipped"] = "card_already_sent"
            elif dry_run:
                item["dry_run_card"] = card
                item["would_patch_existing_card"] = True
            else:
                item["patched_existing_card"] = await feishu.update_card_message_with_app(
                    case["card_message_id"], card, which="app3",
                )
            items.append(item)
            continue
        if refresh_existing_card:
            items.append({
                "reply_record_id": case["reply_record_id"],
                "skipped": "no_existing_card_to_refresh",
            })
            continue
        item = {
            "reply_record_id": case["reply_record_id"],
            "kol_name": case["kol_name"],
            "product_name": case["product_name"],
            "candidate_campaign_ids": [c["campaign_id"] for c in case["candidates"]],
        }
        if dry_run:
            item["dry_run_card"] = card
        else:
            sent = await _send_card(card, frankie_only=frankie_only)
            message_id = next((row.get("message_id") for row in sent if row.get("message_id")), "")
            if message_id:
                await feishu.update_record(config.T_DRAFT, case["reply_record_id"], {
                    "活动归属状态": STATUS_PENDING,
                    "活动归属卡片消息ID": message_id,
                })
            item["sent"] = sent
            item["message_id"] = message_id
        items.append(item)
    return {
        "ok": True, "dry_run": dry_run, "frankie_only": frankie_only,
        "campaign_id": campaign_id, "reply_record_id": reply_record_id,
        "refresh_existing_card": refresh_existing_card,
        "matched_cases": len(cases),
        "processed": len(items), "load_warnings": source.get("load_warnings") or [],
        "items": items,
    }
