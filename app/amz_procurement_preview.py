# -*- coding: utf-8 -*-
"""Amazon Europe procurement-stage preview cards.

This card is a read-only handoff preview after selection confirmation. It is
sent to Frankie first so the procurement-stage scope can be checked before a
formal procurement card is sent to the purchasing team.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

from . import amz_assistant, amz_procurement_quote as proc, amz_selection_confirmation as selection


SOURCE = "amz_procurement_preview"

DEFAULT_BATCH_ID = os.environ.get("AMZ_PROCUREMENT_PREVIEW_DEFAULT_BATCH_ID", "AMZ-EU-PROC-PREVIEW-20260729-P0")
DEFAULT_RECORD_IDS = [
    x.strip()
    for x in os.environ.get(
        "AMZ_PROCUREMENT_PREVIEW_DEFAULT_RECORD_IDS",
        "recvq1QtafnVjX,recvq1QtUEEcXv,recvq1QtFKPwoI,recvq1Quaar3h2",
    ).split(",")
    if x.strip()
]
FRANKIE_ONLY = (os.environ.get("AMZ_PROCUREMENT_PREVIEW_FRANKIE_ONLY", "1") or "1") != "0"
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", amz_assistant.FRANKIE_UNION_ID)
GRAY_UNION_IDS = [x.strip() for x in os.environ.get("AMZ_PROCUREMENT_PREVIEW_GRAY_UNION_IDS", "").split(",") if x.strip()]
GRAY_CHAT_IDS = [x.strip() for x in os.environ.get("AMZ_PROCUREMENT_PREVIEW_GRAY_CHAT_IDS", "").split(",") if x.strip()]

DECISIONS = ("Go", "条件推进", "暂缓", "淘汰")
ACTIVE_DECISIONS = ("Go", "条件推进")
ROUTE_LABEL = {
    "Go": "直接入采购",
    "条件推进": "条件采购复核",
    "暂缓": "暂缓不发采购",
    "淘汰": "淘汰归档",
}
ROUTE_DESC = {
    "Go": "采购部复核 MOQ、交期、同款、套装件数和供应商报价，无异常后进入采购下单/采购计划。",
    "条件推进": "采购部先处理卡片列出的条件，比如压价、限站点、补月销或复核套装；条件未达成不下单。",
    "暂缓": "本批不发采购部执行，只留档；补售价、月销、FBA费、合规或供应链资料后再重算。",
    "淘汰": "本批归档，不进入采购阶段；除非重新跑选品，否则不再推进。",
}


def _text(value: Any) -> str:
    return proc._text(value)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = _text(value).replace(",", "").replace("RMB", "").replace("€", "").replace("£", "").replace("%", "")
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _fmt_int(value: Any) -> str:
    num = _num(value)
    if num is None:
        return "-"
    return str(int(round(num)))


def _fmt_qty(value: Any) -> str:
    num = _num(value)
    if num is None:
        return "待补"
    return f"{int(round(num))}件"


def _field(label: str, value: Any) -> dict:
    return proc._field(label, value)


def _url_button(text: str, url: str, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "url": url}


def _record_url(record_id: str) -> str:
    return proc._record_url(record_id)


def _path(record_id: str = "") -> str:
    return proc._path(record_id)


async def _feishu_api(method: str, path: str, body: dict | None = None) -> dict:
    return await proc._feishu_api(method, path, body)


async def _get_candidate(record_id: str) -> dict:
    data = await _feishu_api("GET", _path(record_id))
    record = ((data.get("data") or {}).get("record") or {})
    return _candidate_from_record(record)


async def _get_candidates_by_ids(record_ids: list[str]) -> list[dict]:
    out: list[dict] = []
    for rid in record_ids:
        if rid:
            out.append(await _get_candidate(rid))
    return out


async def _search_candidates(limit: int = 10) -> list[dict]:
    body = {
        "page_size": min(max(int(limit or 10), 1), 20),
        "filter": {
            "conjunction": "or",
            "conditions": [
                {"field_name": "当前状态", "operator": "contains", "value": ["待采购确认"]},
                {"field_name": "当前状态", "operator": "contains", "value": ["待采购复核"]},
            ],
        },
    }
    data = await _feishu_api("POST", _path() + "/search", body)
    rows = ((data.get("data") or {}).get("items") or [])
    return [_candidate_from_record(row) for row in rows]


async def _prepare_card_images(candidates: list[dict]) -> None:
    await proc._prepare_card_images(candidates)


def _decision_from_review(review_note: str) -> str:
    matches = re.findall(r"选品结果确认=(Go|条件推进|暂缓|淘汰)", _text(review_note))
    return matches[-1] if matches else ""


def _final_decision(candidate: dict) -> str:
    for key in ("selection_decision", "overall_decision"):
        value = _text(candidate.get(key))
        if value in DECISIONS:
            return value
    return _decision_from_review(candidate.get("review_note", "")) or "暂缓"


def _suggested_qty_from_note(review_note: str) -> int | None:
    matches = re.findall(r"建议采购总量=(\d+)件", _text(review_note))
    return int(matches[-1]) if matches else None


def _suggested_qty(candidate: dict) -> int | None:
    from_note = _suggested_qty_from_note(candidate.get("review_note", ""))
    if from_note is not None:
        return from_note
    total = selection._total_suggested_qty(candidate)
    return total if total is not None else None


def _condition_brief(candidate: dict) -> str:
    decision = candidate.get("final_decision")
    review = _text(candidate.get("review_note"))
    notes: list[str] = []
    price_target = re.search(r"压到约?([0-9.]+)元/套以内", review)
    if price_target:
        notes.append(f"目标采购价建议压到 {price_target.group(1)} RMB/套以内。")
    if "德国/西班牙" in review:
        notes.append("优先只看德国/西班牙方向，不先铺全站。")
    if "复核套装" in review or _text(candidate.get("set_count")):
        notes.append("采购需复核套装件数、套装内容和主图一致。")
    if decision == "Go":
        notes.append("重点确认供应商同款、MOQ、交期和报价口径。")
    elif decision == "条件推进":
        notes.append("条件未满足前不要下单；先完成压价或补资料。")
    elif decision == "暂缓":
        notes.append("本批不发采购部执行，先补资料后重算。")
    elif decision == "淘汰":
        notes.append("本批不进入采购。")
    return "\n".join(f"- {note}" for note in notes[:4]) or "-"


def _risk_brief(candidate: dict) -> str:
    note = _text(candidate.get("risk_note"))
    if not note:
        return "只保留兼容/适配表达；Listing、包装和说明书不得出现原厂/官方/正版等暗示。"
    lines = [line.strip() for line in note.splitlines() if line.strip().startswith("- [")]
    if lines:
        return "\n".join(lines[:4])
    return proc._short(note, 500)


def _candidate_from_record(record: dict) -> dict:
    base = selection._candidate_from_record(record)
    base["final_decision"] = _final_decision(base)
    base["procurement_route"] = ROUTE_LABEL[base["final_decision"]]
    base["suggested_procurement_qty"] = _suggested_qty(base)
    return base


def _is_procurement_actionable(candidate: dict) -> bool:
    return (candidate.get("final_decision") or "暂缓") in ACTIVE_DECISIONS


def _active_candidates(candidates: list[dict]) -> list[dict]:
    return [candidate for candidate in candidates if _is_procurement_actionable(candidate)]


def _route_summary(candidates: list[dict]) -> str:
    counts = {label: 0 for label in ROUTE_LABEL.values()}
    excluded = 0
    for candidate in candidates:
        if _is_procurement_actionable(candidate):
            counts[candidate.get("procurement_route") or ROUTE_LABEL["暂缓"]] += 1
        else:
            excluded += 1
    active_total = counts["直接入采购"] + counts["条件采购复核"]
    return (
        f"正式采购产品 {active_total} 个｜"
        f"直接入采购 {counts['直接入采购']} 个｜"
        f"条件采购复核 {counts['条件采购复核']} 个｜"
        f"暂缓/淘汰不发采购 {excluded} 个"
    )


def _route_template(candidates: list[dict]) -> str:
    active = _active_candidates(candidates)
    if any(c.get("final_decision") == "条件推进" for c in active):
        return "yellow"
    if not active:
        return "yellow"
    return "green"


def _workflow_text(candidates: list[dict]) -> str:
    excluded = len(candidates) - len(_active_candidates(candidates))
    lines = [
        "**本卡目的**: 给 Frankie 确认采购部将收到哪些可执行产品；正式发采购部时只包含 Go / 条件推进。",
        "**采购部收到后要做**: 直接入采购=复核 MOQ、交期、同款、套装和报价后下单；条件采购复核=先完成压价、限站点、补资料或套装复核，条件未满足不下单。",
        "**不会发给采购部**: 暂缓/淘汰只留档，不生成采购待办，不要求采购操作。",
        "**下一步**: Frankie 确认本口径后，再生成正式采购部卡/采购复核清单；采购完成复核后再回填采购阶段状态。",
    ]
    if excluded:
        lines.append(f"**本批已剔除**: {excluded} 个暂缓/淘汰产品只保留在候选表，后续补数或重新选品后再重算。")
    return "\n".join(lines)


def _product_elements(candidate: dict) -> list[dict]:
    rid = candidate.get("record_id", "")
    decision = candidate.get("final_decision") or "暂缓"
    route = candidate.get("procurement_route") or ROUTE_LABEL[decision]
    title = candidate.get("cn_name") or candidate.get("title") or candidate.get("asin") or rid
    amazon = candidate.get("amazon_url")
    image = candidate.get("image_url")
    supplier = candidate.get("supplier_link")
    elements: list[dict] = [
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**{route}｜{title}**\n{proc._short(candidate.get('title'), 180)}"}},
    ]
    if candidate.get("image_key"):
        elements.append(
            {
                "tag": "img",
                "img_key": candidate["image_key"],
                "alt": {"tag": "plain_text", "content": f"{title} 主图"},
                "mode": "fit_horizontal",
                "preview": True,
            }
        )
    elements.append(
        {
            "tag": "div",
            "fields": [
                _field("ASIN", candidate.get("asin")),
                _field("选品确认结论", decision),
                _field("采购阶段动作", route),
                _field("建议采购总量", _fmt_qty(candidate.get("suggested_procurement_qty"))),
                _field("采购成本（单套）", proc._format_rmb(candidate.get("quote_cost"))),
                _field("推荐履约", candidate.get("fulfillment")),
                _field("包装尺寸", candidate.get("package_size") or "待核"),
                _field("重量", f"{candidate.get('weight_g')}g" if candidate.get("weight_g") else "待核"),
                _field("套装件数（每套内含）", candidate.get("set_count") or "待核"),
                _field("FBA配送费 / 佣金", f"{proc._format_eur(candidate.get('fba_fee_eur'))} / {proc._format_eur(candidate.get('commission_eur'))}"),
            ],
        }
    )
    elements.extend(
        [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**采购部下一步**\n"
                        f"{ROUTE_DESC[decision]}\n\n"
                        "**采购条件/注意**\n"
                        f"{_condition_brief(candidate)}"
                    ),
                },
            },
            {"tag": "div", "text": {"tag": "lark_md", "content": proc._channel_compare_text(candidate)}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**合规/适配留档注意**\n" + _risk_brief(candidate)}},
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": "采购成本是单套 Amazon 售卖单位成本；套装件数是每套内含配件数，毛利计算不再按件数二次相乘。",
                    }
                ],
            },
        ]
    )
    actions = []
    if amazon:
        actions.append(_url_button("打开 Listing", amazon, "primary"))
    if image:
        actions.append(_url_button("查看主图原图", image))
    actions.append(_url_button("打开候选表记录", _record_url(rid)))
    if supplier:
        actions.append(_url_button("打开1688供应商", supplier))
    elements.append({"tag": "action", "actions": actions})
    return elements


def build_procurement_preview_card(candidates: list[dict], batch_id: str = "") -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    active = _active_candidates(candidates)
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    "**状态**: 采购阶段预览，待 Frankie 确认\n"
                    f"**范围**: {_route_summary(candidates)}\n"
                    "**说明**: 本卡只预览采购阶段将如何派发，不写采购阶段触发表，不发采购部，不改变候选表状态。"
                ),
            },
        },
        {"tag": "div", "text": {"tag": "lark_md", "content": _workflow_text(candidates)}},
        {
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "请在聊天里回复是否按本口径进入采购阶段；预览卡不放业务决策按钮，避免误触发采购流程。",
                }
            ],
        },
    ]
    if not active:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "**本批没有需要采购部处理的产品**\n全部产品都已被暂缓或淘汰，只留候选表归档。",
                },
            }
        )
    for candidate in active:
        elements.extend(_product_elements(candidate))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": _route_template(candidates),
            "title": {"tag": "plain_text", "content": f"🟡 [AMZ·P0] 采购阶段预览 · 待Frankie确认 {len(active)}个待采购动作"},
        },
        "elements": elements,
    }


def validate_procurement_preview_card(card: dict, candidates: list[dict]) -> list[str]:
    errors: list[str] = []
    nodes = list(proc._card_nodes(card))
    rendered = json.dumps(card, ensure_ascii=False)
    buttons = [n for n in nodes if n.get("tag") == "button"]

    def url_button_exists(label: str, expected_url: str) -> bool:
        for button in buttons:
            if proc._card_text(button.get("text")) != label:
                continue
            url = _text(button.get("url"))
            if url == expected_url and url.startswith(("http://", "https://")):
                return True
        return False

    active = _active_candidates(candidates)
    for candidate in active:
        rid = candidate.get("record_id") or ""
        label = candidate.get("asin") or rid or "unknown"
        if candidate.get("amazon_url") and not url_button_exists("打开 Listing", candidate["amazon_url"]):
            errors.append(f"{label}: missing or invalid Amazon Listing button")
        if candidate.get("image_url") and not url_button_exists("查看主图原图", candidate["image_url"]):
            errors.append(f"{label}: missing or invalid image button")
        if not url_button_exists("打开候选表记录", _record_url(rid)):
            errors.append(f"{label}: missing or invalid candidate-record button")
        if candidate.get("supplier_link") and not url_button_exists("打开1688供应商", candidate["supplier_link"]):
            errors.append(f"{label}: missing or invalid supplier button")
    for candidate in candidates:
        if _is_procurement_actionable(candidate):
            continue
        route = candidate.get("procurement_route") or ROUTE_LABEL.get(candidate.get("final_decision") or "暂缓", ROUTE_LABEL["暂缓"])
        if f"{route}｜" in rendered:
            label = candidate.get("asin") or candidate.get("record_id") or "unknown"
            errors.append(f"{label}: excluded decision rendered as procurement product")
    base_required = (
        "采购阶段预览",
        "待 Frankie 确认",
        "不写采购阶段触发表",
        "不发采购部",
        "本卡目的",
        "正式发采购部时只包含",
        "采购部收到后要做",
        "不会发给采购部",
        "暂缓/淘汰只留档",
        "下一步",
        "直接入采购",
        "条件采购复核",
        "请在聊天里回复",
    )
    product_required = (
        "采购部下一步",
        "采购成本（单套）",
        "套装件数（每套内含）",
        "三渠道对比",
    )
    for required in base_required + (product_required if active else ()):
        if required not in rendered:
            errors.append(f"card missing {required}")
    if '"tag": "form"' in rendered or "form_submit" in rendered:
        errors.append("procurement preview card must not contain forms")
    action_buttons = [button for button in buttons if button.get("value") or button.get("action_type")]
    if action_buttons:
        errors.append("procurement preview card must use URL buttons only")
    return errors


async def send_procurement_preview_card(
    *,
    mode: str = "dry_run",
    limit: int = 10,
    batch_id: str = "",
    record_ids: list[str] | None = None,
    frankie_only: bool = True,
    gray_union_ids: list[str] | None = None,
    gray_chat_ids: list[str] | None = None,
) -> dict:
    if mode not in ("dry_run", "commit"):
        raise ValueError("mode must be dry_run or commit")
    batch = batch_id or DEFAULT_BATCH_ID
    ids = record_ids if record_ids is not None else DEFAULT_RECORD_IDS
    candidates = await _get_candidates_by_ids(ids) if ids else await _search_candidates(limit=limit)
    active = _active_candidates(candidates)
    if mode == "commit":
        await _prepare_card_images(active)
    card = build_procurement_preview_card(candidates, batch)
    validation_errors = validate_procurement_preview_card(card, candidates)
    if validation_errors:
        raise RuntimeError("Procurement preview card self-test failed: " + "; ".join(validation_errors))
    effective_frankie_only = bool(frankie_only or FRANKIE_ONLY)
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "batch_id": batch,
        "count": len(active),
        "source_count": len(candidates),
        "record_ids": [c.get("record_id") for c in active],
        "source_record_ids": [c.get("record_id") for c in candidates],
        "routes": {label: sum(1 for c in candidates if c.get("procurement_route") == label) for label in ROUTE_LABEL.values()},
        "card_selftest": "passed",
        **proc._card_media_stats(active),
    }
    if mode == "dry_run":
        result["card"] = card
        result["would_write"] = []
        result["would_send_to_procurement"] = False
        return result
    message_ids: list[str] = []
    recipients: list[dict[str, str]] = []
    if effective_frankie_only:
        recipients.append({"type": "union_id", "id": FRANKIE_UNION_ID})
        msg_id = await amz_assistant.send_card_to_union(FRANKIE_UNION_ID, card)
        if msg_id:
            message_ids.append(msg_id)
    else:
        unions = [x for x in (gray_union_ids if gray_union_ids is not None else GRAY_UNION_IDS) if x]
        chats = [x for x in (gray_chat_ids if gray_chat_ids is not None else GRAY_CHAT_IDS) if x]
        if not unions and not chats:
            raise RuntimeError("Procurement preview recipients are not configured. Set AMZ_PROCUREMENT_PREVIEW_GRAY_UNION_IDS or AMZ_PROCUREMENT_PREVIEW_GRAY_CHAT_IDS.")
        for chat_id in chats:
            recipients.append({"type": "chat_id", "id": chat_id})
            msg_id = await amz_assistant.send_card_to_chat(chat_id, card)
            if msg_id:
                message_ids.append(msg_id)
        for union_id in unions:
            recipients.append({"type": "union_id", "id": union_id})
            msg_id = await amz_assistant.send_card_to_union(union_id, card)
            if msg_id:
                message_ids.append(msg_id)
    result["sent"] = bool(message_ids)
    result["message_id"] = message_ids[0] if message_ids else ""
    result["message_ids"] = message_ids
    result["recipients"] = recipients
    return result
