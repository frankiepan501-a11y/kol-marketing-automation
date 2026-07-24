# -*- coding: utf-8 -*-
"""Amazon Europe 50-unit validation start node.

P0 scope:
- pick candidates that already passed finance/procurement/compliance gates;
- mark their 50-unit validation status as started;
- send a Frankie-only launch card with image, links, costs, and validation focus.
"""
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from . import amz_assistant, amz_procurement_quote as proc


BJ = timezone(timedelta(hours=8))

DEFAULT_BATCH_ID = os.environ.get("AMZ_VALIDATION50_DEFAULT_BATCH_ID", "AMZ-DE-VAL50-20260724-P0")
DEFAULT_RECORD_IDS = [
    x.strip()
    for x in os.environ.get(
        "AMZ_VALIDATION50_DEFAULT_RECORD_IDS",
        "recvq1QtafnVjX,recvq1QtUEEcXv",
    ).split(",")
    if x.strip()
]
VALIDATION_QTY = int(os.environ.get("AMZ_VALIDATION50_QTY", "50") or "50")
FRANKIE_ONLY = (os.environ.get("AMZ_VALIDATION50_CARD_FRANKIE_ONLY", "1") or "1") != "0"
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", amz_assistant.FRANKIE_UNION_ID)
GRAY_UNION_IDS = [x.strip() for x in os.environ.get("AMZ_VALIDATION50_GRAY_UNION_IDS", "").split(",") if x.strip()]
GRAY_CHAT_IDS = [x.strip() for x in os.environ.get("AMZ_VALIDATION50_GRAY_CHAT_IDS", "").split(",") if x.strip()]

FIELD_NAMES = [
    "ASIN",
    "站点",
    "候选标题",
    "产品中文名",
    "Amazon链接",
    "样本ASIN主图URL",
    "包装尺寸",
    "商品重量g",
    "套装件数",
    "套装内容",
    "采购成本RMB",
    "1688供应商链接",
    "采购链接",
    "采购回填状态",
    "三方案推荐履约",
    "FBA€",
    "佣金€",
    "A-物流成本RMB",
    "A-货运比",
    "A-毛利RMB",
    "A-毛利率%",
    "B-物流成本RMB",
    "B-货运比",
    "B-毛利RMB",
    "B-毛利率%",
    "C-物流成本RMB",
    "C-货运比",
    "C-毛利RMB",
    "C-毛利率%",
    "财务闸结论",
    "合规闸结论",
    "IP/外观风险",
    "侵权风险说明",
    "当前状态",
    "综合结论",
    "下一步动作",
    "50件验证状态",
    "人审备注",
]


def _now_label() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M")


def _text(value: Any) -> str:
    return proc._text(value)


def _num(value: Any) -> float | None:
    text = _text(value).replace(",", "").replace("RMB", "").replace("€", "").replace("%", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fmt_rmb(value: Any) -> str:
    return proc._format_rmb(value)


def _fmt_eur(value: Any) -> str:
    return proc._format_eur(value)


def _fmt_rate(value: Any) -> str:
    return proc._format_rate(value)


def _candidate_from_record(record: dict) -> dict:
    base = proc._candidate_from_record(record)
    fields = record.get("fields") or {}
    base.update(
        {
            "site": " / ".join(proc._list_values(fields.get("站点"))) or "-",
            "current_status": " / ".join(proc._list_values(fields.get("当前状态"))) or "-",
            "overall_decision": " / ".join(proc._list_values(fields.get("综合结论"))) or "-",
            "next_action": " / ".join(proc._list_values(fields.get("下一步动作"))) or "-",
            "finance_gate": " / ".join(proc._list_values(fields.get("财务闸结论"))) or "-",
            "compliance_gate": " / ".join(proc._list_values(fields.get("合规闸结论"))) or "-",
            "ip_risk": " / ".join(proc._list_values(fields.get("IP/外观风险"))) or "-",
            "risk_note": _text(fields.get("侵权风险说明")),
            "validation_status": " / ".join(proc._list_values(fields.get("50件验证状态"))) or "未开始",
            "review_note": _text(fields.get("人审备注")),
        }
    )
    return base


def _path(record_id: str = "") -> str:
    return proc._path(record_id)


async def _feishu_api(method: str, path: str, body: dict | None = None) -> dict:
    return await proc._feishu_api(method, path, body)


async def _get_candidate(record_id: str) -> dict:
    data = await _feishu_api("GET", _path(record_id))
    record = ((data.get("data") or {}).get("record") or {})
    return _candidate_from_record(record)


async def _update_candidate(record_id: str, fields: dict) -> None:
    await proc._update_candidate(record_id, fields)


async def _search_candidates(limit: int = 10) -> list[dict]:
    body = {
        "page_size": min(max(int(limit or 10), 1), 20),
        "field_names": FIELD_NAMES,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {"field_name": "合规闸结论", "operator": "contains", "value": ["Go"]},
                {"field_name": "当前状态", "operator": "contains", "value": ["待50件验证"]},
                {"field_name": "综合结论", "operator": "contains", "value": ["50件验证"]},
            ],
        },
    }
    data = await _feishu_api("POST", _path() + "/search", body)
    rows = ((data.get("data") or {}).get("items") or [])
    return [_candidate_from_record(row) for row in rows]


async def _get_candidates_by_ids(record_ids: list[str]) -> list[dict]:
    out = []
    for rid in record_ids:
        if rid:
            out.append(await _get_candidate(rid))
    return out


async def _prepare_card_images(candidates: list[dict]) -> None:
    await proc._prepare_card_images(candidates)


def _record_url(record_id: str) -> str:
    return proc._record_url(record_id)


def _url_button(text: str, url: str, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "url": url}


def _field(label: str, value: Any) -> dict:
    return proc._field(label, value)


def _recommended_channel(candidate: dict) -> dict:
    channels = [c for c in (candidate.get("channels") or []) if proc._channel_has_data(c)]
    fulfillment = candidate.get("fulfillment")
    for channel in channels:
        if proc._recommended_suffix(channel, fulfillment):
            return channel
    return channels[0] if channels else {}


def _validation_budget_line(candidate: dict, qty: int) -> str:
    cost = _num(candidate.get("quote_cost"))
    channel = _recommended_channel(candidate)
    logistics = _num(channel.get("logistics_rmb"))
    if cost is None:
        return "采购成本缺失，不能启动50件验证"
    purchase_total = round(cost * qty, 2)
    if logistics is None:
        return f"{qty}件采购约 {purchase_total} RMB；物流成本缺失，需补后再锁定渠道"
    logistics_total = round(logistics * qty, 2)
    total = round(purchase_total + logistics_total, 2)
    return f"{qty}件采购约 {purchase_total} RMB + 推荐渠道物流约 {logistics_total} RMB = {total} RMB（不含平台费/VAT）"


def _channel_line(candidate: dict) -> str:
    lines = []
    for channel in (candidate.get("channels") or []):
        if not proc._channel_has_data(channel):
            continue
        suffix = proc._recommended_suffix(channel, candidate.get("fulfillment"))
        lines.append(
            f"{channel.get('code')} {channel.get('label')}{suffix}: "
            f"物流 {_fmt_rmb(channel.get('logistics_rmb'))}｜"
            f"毛利 {_fmt_rmb(channel.get('margin_rmb'))}/{_fmt_rate(channel.get('margin_rate'))}｜"
            f"货运比 {_text(channel.get('freight_ratio')) or '-'}"
        )
    return "\n".join(lines) if lines else "三渠道毛利暂缺"


def _risk_brief(candidate: dict) -> str:
    note = _text(candidate.get("risk_note"))
    if not note:
        return "暂无自动风险说明；验证前仍需按 Listing/包装/标签资料清单核对。"
    lines = [line.strip() for line in note.splitlines() if line.strip().startswith("- [")]
    if not lines:
        return proc._short(note, 420)
    return "\n".join(lines[:4])


def _eligible(candidate: dict) -> tuple[bool, str]:
    if _text(candidate.get("compliance_gate")) != "Go":
        return False, "合规闸未Go"
    if _text(candidate.get("current_status")) != "待50件验证":
        return False, "当前状态不是待50件验证"
    if _text(candidate.get("overall_decision")) != "50件验证":
        return False, "综合结论不是50件验证"
    if _text(candidate.get("quote_status")) != "已回填" or _num(candidate.get("quote_cost")) is None:
        return False, "采购成本未回填"
    if not candidate.get("supplier_link"):
        return False, "1688供应商链接缺失"
    if _text(candidate.get("validation_status")) in ("进行中", "已通过"):
        return False, f"50件验证状态已是{candidate.get('validation_status')}"
    return True, "ok"


def _product_elements(candidate: dict, qty: int) -> list[dict]:
    rid = candidate.get("record_id", "")
    title = candidate.get("cn_name") or candidate.get("title") or candidate.get("asin") or rid
    amazon = candidate.get("amazon_url")
    image = candidate.get("image_url")
    supplier = candidate.get("supplier_link")
    elements: list[dict] = [
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**{title}**\n{proc._short(candidate.get('title'), 180)}"}},
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
    elements.extend(
        [
            {
                "tag": "div",
                "fields": [
                    _field("ASIN", candidate.get("asin")),
                    _field("站点", candidate.get("site")),
                    _field("建议履约", candidate.get("fulfillment")),
                    _field("50件验证状态", candidate.get("validation_status") or "未开始"),
                    _field("采购成本", _fmt_rmb(candidate.get("quote_cost"))),
                    _field("包装尺寸", candidate.get("package_size") or "待核"),
                    _field("重量", f"{candidate.get('weight_g')}g" if candidate.get("weight_g") else "待核"),
                    _field("件数", candidate.get("set_count") or "待核"),
                    _field("FBA配送费/佣金", f"{_fmt_eur(candidate.get('fba_fee_eur'))} / {_fmt_eur(candidate.get('commission_eur'))}"),
                ],
            },
            {"tag": "div", "text": {"tag": "lark_md", "content": "**50件粗算**\n" + _validation_budget_line(candidate, qty)}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**三渠道经济性**\n" + _channel_line(candidate)}},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**50件验证要看**\n"
                        "- 采购：50件同款、同套装件数、同包装口径，供应商报价/MOQ/交期要能落地。\n"
                        "- 上架：Listing 只写 compatible/replacement/适配，不写 official/original/OEM/原厂/正版。\n"
                        "- 物流：先按推荐履约走，记录实际计费重、物流单价和到仓/妥投时效。\n"
                        "- 结果：看 7/14/30 天订单、转化、退款退货、差评和型号适配投诉。"
                    ),
                },
            },
            {"tag": "div", "text": {"tag": "lark_md", "content": "**系统注意点**\n" + _risk_brief(candidate)}},
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


def build_validation50_card(candidates: list[dict], batch_id: str = "", qty: int = VALIDATION_QTY) -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    f"**数量口径**: 每个 ASIN 先按 {qty} 件做验证\n"
                    "**状态**: 已进入 50 件验证节点\n"
                    "**目的**: 验证真实采购、物流、上架、销量、退货和适配投诉，不再重复做人工合规审批。"
                ),
            },
        },
        {"tag": "note", "elements": [{"tag": "plain_text", "content": "系统已把普通风险点留档；只有验证结果异常才进入后续复核。"}]},
    ]
    for candidate in candidates:
        elements.extend(_product_elements(candidate, qty))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": f"🟢 [AMZ·P0] 德国站50件验证启动 · {len(candidates)}个产品"},
        },
        "elements": elements,
    }


def validate_validation50_card(card: dict, candidates: list[dict]) -> list[str]:
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

    for candidate in candidates:
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
    if '"tag": "form"' in rendered or "form_submit" in rendered:
        errors.append("validation start card must not contain approval forms")
    for required in ("50件验证要看", "三渠道经济性", "系统注意点", "50件粗算", "不再重复做人工合规审批"):
        if required not in rendered:
            errors.append(f"card missing {required}")
    return errors


def _start_note(candidate: dict, batch_id: str, qty: int) -> str:
    line = (
        f"{_now_label()} system: 进入50件验证；批次={batch_id}; 数量={qty}; "
        f"推荐履约={candidate.get('fulfillment') or '-'}; "
        "验证重点=采购同款/套装件数/包装标签/实际物流成本/7-14-30天销量和退货适配问题。"
    )
    old = _text(candidate.get("review_note"))
    if not old:
        return line
    return old if line in old else f"{old}\n{line}"


def _build_start_fields(candidate: dict, batch_id: str, qty: int) -> dict:
    return {
        "50件验证状态": "进行中",
        "当前状态": "待50件验证",
        "综合结论": "50件验证",
        "下一步动作": "发起50件验证",
        "人审备注": _start_note(candidate, batch_id, qty),
    }


async def start_validation50(
    *,
    mode: str = "dry_run",
    limit: int = 10,
    batch_id: str = "",
    record_ids: list[str] | None = None,
    frankie_only: bool = True,
    gray_union_ids: list[str] | None = None,
    gray_chat_ids: list[str] | None = None,
    qty: int = VALIDATION_QTY,
) -> dict:
    if mode not in ("dry_run", "commit"):
        raise ValueError("mode must be dry_run or commit")
    batch = batch_id or DEFAULT_BATCH_ID
    ids = record_ids if record_ids is not None else DEFAULT_RECORD_IDS
    candidates = await _get_candidates_by_ids(ids) if ids else await _search_candidates(limit=limit)
    eligible: list[dict] = []
    skipped: list[dict] = []
    for candidate in candidates:
        ok, reason = _eligible(candidate)
        if ok:
            eligible.append(candidate)
        else:
            skipped.append({"record_id": candidate.get("record_id"), "asin": candidate.get("asin"), "reason": reason})
    if mode == "commit":
        await _prepare_card_images(eligible)
    card = build_validation50_card(eligible, batch, qty)
    validation_errors = validate_validation50_card(card, eligible)
    if validation_errors:
        raise RuntimeError("50-unit validation card self-test failed: " + "; ".join(validation_errors))
    effective_frankie_only = bool(frankie_only or FRANKIE_ONLY)
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "batch_id": batch,
        "qty": qty,
        "count": len(candidates),
        "eligible_count": len(eligible),
        "skipped_count": len(skipped),
        "record_ids": [c.get("record_id") for c in candidates],
        "eligible_record_ids": [c.get("record_id") for c in eligible],
        "skipped_records": skipped,
        "card_selftest": "passed",
        **proc._card_media_stats(eligible),
    }
    if mode == "dry_run":
        result["card"] = card
        result["would_update"] = [
            {"record_id": c.get("record_id"), "fields": _build_start_fields(c, batch, qty)}
            for c in eligible
        ]
        return result
    for candidate in eligible:
        fields = _build_start_fields(candidate, batch, qty)
        await _update_candidate(candidate["record_id"], fields)
        candidate.update(
            {
                "validation_status": fields["50件验证状态"],
                "current_status": fields["当前状态"],
                "overall_decision": fields["综合结论"],
                "next_action": fields["下一步动作"],
                "review_note": fields["人审备注"],
            }
        )
    result["updated_record_ids"] = [c.get("record_id") for c in eligible]
    if not eligible:
        result["sent"] = False
        result["message_id"] = ""
        result["message_ids"] = []
        result["recipients"] = []
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
            raise RuntimeError("50-unit validation recipients are not configured. Set AMZ_VALIDATION50_GRAY_UNION_IDS or AMZ_VALIDATION50_GRAY_CHAT_IDS.")
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
