# -*- coding: utf-8 -*-
"""Amazon Europe procurement-stage cards.

Frankie cards remain read-only previews. Procurement cards collect one
per-product supplier review so the next purchase-confirmation step has enough
data without treating this stage as ERP product creation or purchase-order
execution.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from typing import Any

from . import amz_assistant, amz_procurement_quote as proc, amz_selection_confirmation as selection


SOURCE = "amz_procurement_preview"
ACTION_REVIEW_SUBMIT = "amz_proc_review_submit"

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
AUDIENCES = ("frankie", "procurement")
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
SAME_MATCH_OPTIONS = ("同款可采购", "近似款需确认", "不同款/不建议采购")
STOCK_OPTIONS = ("有现货", "无现货", "需询问")
SUPPLIER_RESULT_OPTIONS = ("供应商可用", "供应商备选", "供应商不建议")
PURCHASE_SUGGESTION_OPTIONS = ("可采购", "压价后采购", "换供应商", "补资料后复核", "暂缓采购")

_bg_tasks: set[asyncio.Task] = set()
_recent_callbacks: dict[str, float] = {}


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


def _button_option(text: str) -> dict:
    return {"text": {"tag": "plain_text", "content": text}, "value": text}


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


async def _update_candidate(record_id: str, fields: dict) -> None:
    await proc._update_candidate(record_id, fields)


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


def _latest_procurement_review(review_note: str) -> dict[str, str]:
    latest: dict[str, str] = {}
    for line in _text(review_note).splitlines():
        if "采购复核回填=" not in line:
            continue
        parsed: dict[str, str] = {}
        for part in line.split(";"):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            key = re.sub(r"^.*采购复核回填", "采购复核回填", key).strip()
            parsed[key] = value.strip().rstrip(".")
        if parsed:
            latest = parsed
    return latest


def _candidate_from_record(record: dict) -> dict:
    base = selection._candidate_from_record(record)
    fields = record.get("fields") or {}
    review = _latest_procurement_review(base.get("review_note", ""))
    base["final_decision"] = _final_decision(base)
    base["procurement_route"] = ROUTE_LABEL[base["final_decision"]]
    base["suggested_procurement_qty"] = _suggested_qty(base)
    base["procurement_review"] = review
    base["procurement_review_status"] = review.get("采购复核回填", "")
    base["procurement_review_note"] = _text(fields.get("采购备注"))
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


def _workflow_text(candidates: list[dict], audience: str = "frankie") -> str:
    excluded = len(candidates) - len(_active_candidates(candidates))
    if audience == "procurement":
        lines = [
            "**本卡目的**: 采购复核回填卡。只收集供应商、同款、价格、MOQ、交期、库存和箱规资料，给后续最终采购确认使用。",
            "**采购部要做**: 每个产品逐项核对供应商页面和 Amazon 主图/套装，填完整本产品复核表，再点 `提交本产品复核`。每个产品单独提交，互不影响。",
            "**现在不做**: 本卡不是 ERP 新品录入、不是采购下单、不是采购计划确认，也不是 Listing 上架；填完不会自动下单。",
            "**下一步**: 全部产品复核完成后，系统再汇总成采购确认清单给 Frankie/运营确认。确认后才进入下游 ERP 新品/采购计划/下单节点。",
            "**ERP资料口径**: 这张卡先收集 ERP 会用到的供应链资料；ERP 新品录入放在最终采购确认之后，不能在采购复核卡里直接触发。",
        ]
    else:
        lines = [
            "**本卡目的**: 给 Frankie 确认采购部将收到哪些可执行产品；正式发采购部时只包含 Go / 条件推进。",
            "**采购部收到后要做**: 直接入采购=复核 MOQ、交期、同款、套装和报价后下单；条件采购复核=先完成压价、限站点、补资料或套装复核，条件未满足不下单。",
            "**不会发给采购部**: 暂缓/淘汰只留档，不生成采购待办，不要求采购操作。",
            "**下一步**: Frankie 确认本口径后，再生成正式采购部卡/采购复核清单；采购完成复核后再回填采购阶段状态。",
        ]
    if excluded:
        lines.append(f"**本批已剔除**: {excluded} 个暂缓/淘汰产品只保留在候选表，后续补数或重新选品后再重算。")
    return "\n".join(lines)


def _review_payload(candidate: dict, card_record_ids: list[str], batch_id: str) -> dict:
    return {
        "source": SOURCE,
        "action": ACTION_REVIEW_SUBMIT,
        "record_id": candidate.get("record_id") or "",
        "asin": candidate.get("asin") or "",
        "batch_id": batch_id or DEFAULT_BATCH_ID,
        "card_record_ids": card_record_ids,
    }


def _review_value(candidate: dict, key: str) -> str:
    review = candidate.get("procurement_review") or {}
    return _text(review.get(key))


def _review_result_text(candidate: dict) -> str:
    lines = [
        f"- 同款确认: {_review_value(candidate, '同款确认') or '-'}",
        f"- MOQ: {_review_value(candidate, 'MOQ') or '-'}",
        f"- 阶梯价: {_review_value(candidate, '阶梯价') or '-'}",
        f"- 交期: {_review_value(candidate, '交期') or '-'}",
        f"- 箱规/尺寸重量: {_review_value(candidate, '箱规尺寸重量') or '-'}",
        f"- 现货状态: {_review_value(candidate, '现货') or '-'}",
        f"- 供应商1688链接: {_review_value(candidate, '供应商1688链接') or candidate.get('supplier_link') or '-'}",
        f"- 供应商结论: {_review_value(candidate, '供应商结论') or '-'}",
        f"- 采购建议: {_review_value(candidate, '采购建议') or '-'}",
        f"- 后续待补: {_review_value(candidate, '后续待补') or '无'}",
    ]
    note = _review_value(candidate, "备注") or _text(candidate.get("procurement_review_note"))
    if note:
        lines.append(f"- 备注: {note}")
    return "\n".join(lines)


def _review_form(candidate: dict, card_record_ids: list[str], batch_id: str) -> dict:
    sid = proc._safe_id(candidate.get("record_id") or "")
    return {
        "tag": "form",
        "name": f"proc_review_form_{sid}",
        "elements": [
            {
                "tag": "select_static",
                "name": f"proc_review_same_{sid}",
                "placeholder": {"tag": "plain_text", "content": "同款确认"},
                "options": [_button_option(x) for x in SAME_MATCH_OPTIONS],
            },
            {
                "tag": "input",
                "name": f"proc_review_moq_{sid}",
                "label_position": "left",
                "label": {"tag": "plain_text", "content": "MOQ"},
                "placeholder": {"tag": "plain_text", "content": "例如 50套；若无MOQ填 无"},
            },
            {
                "tag": "input",
                "name": f"proc_review_tiers_{sid}",
                "label_position": "left",
                "label": {"tag": "plain_text", "content": "阶梯价（选填）"},
                "placeholder": {"tag": "plain_text", "content": "可后补；例如 50套=12.5；100套=11.8"},
            },
            {
                "tag": "input",
                "name": f"proc_review_leadtime_{sid}",
                "label_position": "left",
                "label": {"tag": "plain_text", "content": "交期"},
                "placeholder": {"tag": "plain_text", "content": "现货1-2天 / 订做7天 / 需确认"},
            },
            {
                "tag": "input",
                "name": f"proc_review_carton_{sid}",
                "label_position": "left",
                "label": {"tag": "plain_text", "content": "箱规/尺寸重量（选填）"},
                "placeholder": {"tag": "plain_text", "content": "可后补；单套包装、外箱尺寸、毛重、每箱数量"},
            },
            {
                "tag": "select_static",
                "name": f"proc_review_stock_{sid}",
                "placeholder": {"tag": "plain_text", "content": "是否有现货"},
                "options": [_button_option(x) for x in STOCK_OPTIONS],
            },
            {
                "tag": "input",
                "name": f"proc_review_supplier_link_{sid}",
                "label_position": "left",
                "label": {"tag": "plain_text", "content": "供应商1688链接"},
                "placeholder": {"tag": "plain_text", "content": "如已换供应商，请粘贴新1688链接；不填则保留原链接"},
            },
            {
                "tag": "select_static",
                "name": f"proc_review_supplier_{sid}",
                "placeholder": {"tag": "plain_text", "content": "供应商结论"},
                "options": [_button_option(x) for x in SUPPLIER_RESULT_OPTIONS],
            },
            {
                "tag": "select_static",
                "name": f"proc_review_suggestion_{sid}",
                "placeholder": {"tag": "plain_text", "content": "采购建议"},
                "options": [_button_option(x) for x in PURCHASE_SUGGESTION_OPTIONS],
            },
            {
                "tag": "input",
                "name": f"proc_review_note_{sid}",
                "label_position": "left",
                "label": {"tag": "plain_text", "content": "备注"},
                "placeholder": {"tag": "plain_text", "content": "型号差异、颜色、Logo、报价口径、需补图片/样品"},
            },
            {
                "tag": "button",
                "action_type": "form_submit",
                "name": f"proc_review_submit_{sid}",
                "type": "primary",
                "text": {"tag": "plain_text", "content": "提交本产品复核"},
                "value": _review_payload(candidate, card_record_ids, batch_id),
            },
        ],
    }


def _product_elements(candidate: dict, audience: str = "frankie", batch_id: str = "", card_record_ids: list[str] | None = None) -> list[dict]:
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
    if audience == "procurement":
        if candidate.get("procurement_review_status"):
            elements.append(
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**采购复核已提交**\n"
                            f"{_review_result_text(candidate)}\n\n"
                            "**后续流向**: 等本批产品全部复核后，系统汇总给 Frankie/运营做最终采购确认；本卡不会自动录 ERP 或下单。"
                        ),
                    },
                }
            )
        else:
            elements.append(
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**请采购回填本产品复核结果**\n"
                            "- 同款确认必须看 Amazon 主图、套装件数、适配型号和供应商页。\n"
                            "- 必填：同款、MOQ、交期、是否有现货、供应商结论、采购建议。\n"
                            "- 供应商1688链接：如采购已换成更合适的供应商，请粘贴新链接；不填则保留候选表原链接。\n"
                            "- 选填：阶梯价、箱规/尺寸重量；可先提交，系统会带到下一步待补资料。\n"
                            "- 如果不是同款或供应商条件不达标，请选 `暂缓采购` 或 `换供应商`，不要默认推进。"
                        ),
                    },
                }
            )
            elements.append(_review_form(candidate, card_record_ids or [], batch_id))
    return elements


def build_procurement_preview_card(candidates: list[dict], batch_id: str = "", audience: str = "frankie") -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    if audience not in AUDIENCES:
        raise ValueError("audience must be frankie or procurement")
    active = _active_candidates(candidates)
    card_record_ids = [c.get("record_id") for c in active if c.get("record_id")]
    if audience == "procurement":
        done = sum(1 for item in active if item.get("procurement_review_status"))
        status_text = "**状态**: 采购复核回填中\n"
        explain_text = "**说明**: 本卡只发需要采购部复核的 Go / 条件推进产品；暂缓/淘汰不出现在产品区块。"
        note_text = "采购只需要核对并提交本产品复核资料；本卡不会自动录 ERP 新品、不会生成采购单、不会触发上架。"
        suffix = "已全部复核" if active and done == len(active) else f"待复核 {len(active) - done}/{len(active)}"
        header_title = f"🟡 [AMZ·P0] 欧洲站采购复核回填 · {suffix}"
    else:
        status_text = "**状态**: 采购阶段预览，待 Frankie 确认\n"
        explain_text = "**说明**: 本卡只预览采购阶段将如何派发，不写采购阶段触发表，不发采购部，不改变候选表状态。"
        note_text = "请在聊天里回复是否按本口径进入采购阶段；预览卡不放业务决策按钮，避免误触发采购流程。"
        header_title = f"🟡 [AMZ·P0] 采购阶段预览 · 待Frankie确认 {len(active)}个待采购动作"
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    f"{status_text}"
                    f"**范围**: {_route_summary(candidates)}\n"
                    f"{explain_text}"
                ),
            },
        },
        {"tag": "div", "text": {"tag": "lark_md", "content": _workflow_text(candidates, audience)}},
        {
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": note_text,
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
        elements.extend(_product_elements(candidate, audience=audience, batch_id=batch, card_record_ids=card_record_ids))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": _route_template(candidates),
            "title": {"tag": "plain_text", "content": header_title},
        },
        "elements": elements,
    }


def validate_procurement_preview_card(card: dict, candidates: list[dict], audience: str = "frankie") -> list[str]:
    errors: list[str] = []
    if audience not in AUDIENCES:
        errors.append("audience must be frankie or procurement")
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
    if audience == "procurement":
        base_required = [
            "本卡目的",
            "采购部要做",
            "现在不做",
            "下一步",
            "ERP资料口径",
            "直接入采购",
            "条件采购复核",
            "本卡只发需要采购部复核",
            "暂缓/淘汰不出现在产品区块",
        ]
        for forbidden in ("待 Frankie 确认", "给 Frankie 确认", "请在聊天里回复是否按本口径"):
            if forbidden in rendered:
                errors.append(f"procurement card must not contain {forbidden}")
    else:
        base_required = [
            "不写采购阶段触发表",
            "本卡目的",
            "采购部收到后要做",
            "下一步",
            "直接入采购",
            "条件采购复核",
        ]
        base_required.extend(("采购阶段预览", "待 Frankie 确认", "不发采购部", "正式发采购部时只包含", "不会发给采购部", "暂缓/淘汰只留档", "请在聊天里回复"))
    product_required = (
        "采购部下一步",
        "采购成本（单套）",
        "套装件数（每套内含）",
        "三渠道对比",
    )
    for required in tuple(base_required) + (product_required if active else ()):
        if required not in rendered:
            errors.append(f"card missing {required}")
    forms = {n.get("name"): n for n in nodes if n.get("tag") == "form" and n.get("name")}
    if audience == "frankie":
        if '"tag": "form"' in rendered or "form_submit" in rendered:
            errors.append("Frankie procurement preview card must not contain forms")
        action_buttons = [button for button in buttons if button.get("value") or button.get("action_type")]
        if action_buttons:
            errors.append("Frankie procurement preview card must use URL buttons only")
    else:
        has_pending = any(not c.get("procurement_review_status") for c in active)
        required_procurement_text = [
            "采购复核回填卡",
            "不是 ERP 新品录入",
            "不会自动下单",
            "ERP 新品录入放在最终采购确认之后",
        ]
        if has_pending:
            required_procurement_text.extend(
                [
                    "提交本产品复核",
                    "同款确认",
                    "MOQ",
                    "阶梯价",
                    "交期",
                    "箱规/尺寸重量",
                    "供应商结论",
                    "采购建议",
                    "是否有现货",
                    "供应商1688链接",
                ]
            )
        else:
            required_procurement_text.append("采购复核已提交")
        for required in required_procurement_text:
            if required not in rendered:
                errors.append(f"procurement review card missing {required}")
        for candidate in active:
            if candidate.get("procurement_review_status"):
                continue
            rid = candidate.get("record_id") or ""
            label = candidate.get("asin") or rid or "unknown"
            sid = proc._safe_id(rid)
            form_name = f"proc_review_form_{sid}"
            form = forms.get(form_name)
            if not form:
                errors.append(f"{label}: missing form {form_name}")
                continue
            form_elements = form.get("elements") or []
            names = {x.get("name"): x.get("tag") for x in form_elements if isinstance(x, dict) and x.get("name")}
            expected = {
                f"proc_review_same_{sid}": "select_static",
                f"proc_review_moq_{sid}": "input",
                f"proc_review_tiers_{sid}": "input",
                f"proc_review_leadtime_{sid}": "input",
                f"proc_review_carton_{sid}": "input",
                f"proc_review_stock_{sid}": "select_static",
                f"proc_review_supplier_link_{sid}": "input",
                f"proc_review_supplier_{sid}": "select_static",
                f"proc_review_suggestion_{sid}": "select_static",
                f"proc_review_note_{sid}": "input",
            }
            for name, tag in expected.items():
                if names.get(name) != tag:
                    errors.append(f"{label}: missing {tag} {name}")
            submit = None
            for item in form_elements:
                if isinstance(item, dict) and item.get("tag") == "button" and item.get("action_type") == "form_submit":
                    submit = item
                    break
            if not submit:
                errors.append(f"{label}: missing form_submit button")
                continue
            value = submit.get("value") or {}
            if _text(value.get("action")) != ACTION_REVIEW_SUBMIT:
                errors.append(f"{label}: submit payload action is invalid")
            if _text(value.get("record_id")) != rid:
                errors.append(f"{label}: submit payload record_id is invalid")
            payload_ids = [_text(x) for x in (value.get("card_record_ids") or []) if _text(x)]
            expected_ids = [c.get("record_id") for c in active if c.get("record_id")]
            if payload_ids != expected_ids:
                errors.append(f"{label}: submit payload card_record_ids is invalid")
    return errors


def _extract_action(event: dict) -> tuple[str, dict, dict]:
    return proc._extract_action(event)


def _form_value(form: dict, record_id: str, suffix: str) -> str:
    return proc._form_value(form, record_id, f"review_{suffix}")


def _message_id(event: dict) -> str:
    return proc._message_id(event)


def _operator_label(event: dict) -> str:
    return proc._operator_label(event).replace("采购回填", "采购复核")


def _toast(content: str, typ: str = "success") -> dict:
    return proc._toast(content, typ)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _callback_key(record_id: str, form: dict) -> str:
    text = json.dumps(form or {}, ensure_ascii=False, sort_keys=True)
    return f"{record_id}:{hash(text)}"


def _recent_seen(key: str, ttl_sec: int = 300) -> bool:
    now = time.time()
    for old, ts in list(_recent_callbacks.items()):
        if now - ts > ttl_sec:
            _recent_callbacks.pop(old, None)
    return key in _recent_callbacks and now - _recent_callbacks[key] <= ttl_sec


def _review_from_form(form: dict, record_id: str) -> dict[str, str]:
    return {
        "same": _form_value(form, record_id, "same"),
        "moq": _form_value(form, record_id, "moq"),
        "tiers": _form_value(form, record_id, "tiers"),
        "leadtime": _form_value(form, record_id, "leadtime"),
        "carton": _form_value(form, record_id, "carton"),
        "stock": _form_value(form, record_id, "stock"),
        "stock_qty": _form_value(form, record_id, "stock_qty"),
        "supplier_link": _form_value(form, record_id, "supplier_link"),
        "supplier": _form_value(form, record_id, "supplier"),
        "suggestion": _form_value(form, record_id, "suggestion"),
        "note": _form_value(form, record_id, "note"),
    }


def _validate_review(review: dict[str, str]) -> str:
    labels = {
        "same": "同款确认",
        "moq": "MOQ",
        "leadtime": "交期",
        "stock": "是否有现货",
        "supplier": "供应商结论",
        "suggestion": "采购建议",
    }
    missing = [label for key, label in labels.items() if not _text(review.get(key))]
    if missing:
        return "请补齐：" + "、".join(missing)
    if review.get("same") not in SAME_MATCH_OPTIONS:
        return "同款确认选项无效，请重新选择"
    if review.get("stock") not in STOCK_OPTIONS:
        return "现货库存选项无效，请重新选择"
    if review.get("supplier") not in SUPPLIER_RESULT_OPTIONS:
        return "供应商结论选项无效，请重新选择"
    if review.get("suggestion") not in PURCHASE_SUGGESTION_OPTIONS:
        return "采购建议选项无效，请重新选择"
    supplier_link = _text(review.get("supplier_link"))
    if supplier_link and not supplier_link.startswith(("http://", "https://")):
        return "请填写可打开的1688供应商链接"
    return ""


def _optional_gap_text(review: dict[str, str]) -> str:
    gaps = []
    if not _text(review.get("tiers")):
        gaps.append("阶梯价")
    if not _text(review.get("carton")):
        gaps.append("箱规/尺寸重量")
    return "、".join(gaps) if gaps else "无"


def _review_line(candidate: dict, review: dict[str, str], actor: str, batch_id: str) -> str:
    note = re.sub(r"\s+", " ", _text(review.get("note")))[:300]
    optional_gaps = _optional_gap_text(review)
    supplier_link = re.sub(r"\s+", "", _text(review.get("supplier_link")))[:1000]
    return (
        f"{proc._now_label()} {actor}: 采购复核回填=已提交; "
        f"同款确认={review.get('same') or '-'}; "
        f"MOQ={review.get('moq') or '-'}; "
        f"阶梯价={review.get('tiers') or '-'}; "
        f"交期={review.get('leadtime') or '-'}; "
        f"箱规尺寸重量={review.get('carton') or '-'}; "
        f"现货={review.get('stock') or '-'}; "
        f"供应商1688链接={supplier_link or '-'}; "
        f"供应商结论={review.get('supplier') or '-'}; "
        f"采购建议={review.get('suggestion') or '-'}; "
        f"后续待补={optional_gaps}; "
        f"备注={note or '-'}; "
        f"批次={batch_id or DEFAULT_BATCH_ID}."
    )


def _append_review_note(candidate: dict, review: dict[str, str], actor: str, batch_id: str) -> str:
    line = _review_line(candidate, review, actor, batch_id)
    old = _text(candidate.get("review_note"))
    if not old:
        return line
    return old if line in old else f"{old}\n{line}"


def _review_next_action(review: dict[str, str]) -> str:
    suggestion = review.get("suggestion")
    optional_gaps = _optional_gap_text(review)
    if suggestion == "可采购":
        if optional_gaps != "无":
            return f"采购复核已提交：可进入最终采购确认；待补资料={optional_gaps}，箱规/尺寸重量在ERP新品和物流复算前必须补齐"
        return "采购复核已提交：待Frankie/运营最终确认采购量，再进入ERP新品/采购计划节点"
    if suggestion == "压价后采购":
        return "采购复核待处理：采购继续压价，达到目标价后再提交最终采购确认"
    if suggestion == "换供应商":
        return "采购复核待处理：当前供应商不作为最终供应商，需重新找供应商后复核"
    if suggestion == "补资料后复核":
        return "采购复核待处理：补同款、箱规、库存、交期或报价资料后再复核"
    return "采购暂缓：供应商或同款条件不满足，本批不进入下单"


def _review_summary(review: dict[str, str]) -> str:
    optional_gaps = _optional_gap_text(review)
    return (
        f"采购复核已提交：同款确认={review.get('same') or '-'}｜"
        f"MOQ={review.get('moq') or '-'}｜阶梯价={review.get('tiers') or '-'}｜"
        f"交期={review.get('leadtime') or '-'}｜箱规/尺寸重量={review.get('carton') or '-'}｜"
        f"现货={review.get('stock') or '-'}｜供应商1688链接={review.get('supplier_link') or '-'}｜"
        f"供应商结论={review.get('supplier') or '-'}｜采购建议={review.get('suggestion') or '-'}｜"
        f"后续待补={optional_gaps}｜"
        f"备注={review.get('note') or '-'}"
    )[:5000]


async def _process_callback_background(event: dict, callback_key: str) -> None:
    try:
        result = await _process_callback(event)
        if ((result.get("toast") or {}).get("type") or "") == "error":
            _recent_callbacks.pop(callback_key, None)
    except Exception as exc:
        _recent_callbacks.pop(callback_key, None)
        print(f"[amz_procurement_preview.callback_bg] {callback_key} fail: {exc}")


async def _process_callback(event: dict) -> dict:
    action, value, form = _extract_action(event)
    if action != ACTION_REVIEW_SUBMIT:
        return _toast("未知采购复核动作", "error")
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    review = _review_from_form(form, record_id)
    error = _validate_review(review)
    if error:
        return _toast(error, "error")

    candidate = await _get_candidate(record_id)
    msg_id = _message_id(event)
    actor = _operator_label(event)
    batch_id = _text(value.get("batch_id")) or DEFAULT_BATCH_ID
    fields = {
        "下一步动作": _review_next_action(review),
        "采购备注": _review_summary(review),
        "人审备注": _append_review_note(candidate, review, actor, batch_id),
    }
    supplier_link = _text(review.get("supplier_link"))
    if supplier_link:
        fields["1688供应商链接"] = proc._url_cell(supplier_link)
        fields["采购链接"] = proc._url_cell(supplier_link)
    await _update_candidate(record_id, fields)
    candidate.update(
        {
            "next_action": fields["下一步动作"],
            "procurement_review_note": fields["采购备注"],
            "review_note": fields["人审备注"],
            "procurement_review": _latest_procurement_review(fields["人审备注"]),
            "procurement_review_status": "已提交",
            "supplier_link": supplier_link or candidate.get("supplier_link"),
        }
    )
    record_ids = [x for x in (value.get("card_record_ids") or []) if _text(x)]
    if msg_id:
        if record_ids:
            candidates = await _get_candidates_by_ids(record_ids)
            for idx, item in enumerate(candidates):
                if item.get("record_id") == record_id:
                    candidates[idx] = candidate
                    break
            await _prepare_card_images(candidates)
            await amz_assistant.update_card(msg_id, build_procurement_preview_card(candidates, batch_id, audience="procurement"))
        else:
            await amz_assistant.update_card(msg_id, build_procurement_preview_card([candidate], batch_id, audience="procurement"))
    return _toast("本产品采购复核已提交")


async def handle_callback(event: dict) -> dict:
    action, value, form = _extract_action(event)
    if action != ACTION_REVIEW_SUBMIT:
        return {"ok": False, "ignored": True, "action": action}
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    review = _review_from_form(form, record_id)
    error = _validate_review(review)
    if error:
        return _toast(error, "error")
    callback_key = _callback_key(record_id, form)
    if _recent_seen(callback_key):
        try:
            current = await _get_candidate(record_id)
            latest = current.get("procurement_review") or {}
            if (
                _text(latest.get("采购复核回填")) == "已提交"
                and _text(latest.get("采购建议")) == review.get("suggestion")
                and _text(latest.get("同款确认")) == review.get("same")
            ):
                return _toast("该产品采购复核已提交，无需重复点击")
        except Exception as exc:
            print(f"[amz_procurement_preview.callback_duplicate_check] {record_id} fail: {exc}")
        _recent_callbacks.pop(callback_key, None)
        _recent_callbacks[callback_key] = time.time()
        _spawn(_process_callback_background(event, callback_key))
        return _toast("已重新收到本产品采购复核，正在补写候选表并更新原卡")
    _recent_callbacks[callback_key] = time.time()
    _spawn(_process_callback_background(event, callback_key))
    return _toast("已收到本产品采购复核，正在写回候选表并更新原卡")


async def send_procurement_preview_card(
    *,
    mode: str = "dry_run",
    limit: int = 10,
    batch_id: str = "",
    record_ids: list[str] | None = None,
    frankie_only: bool = True,
    gray_union_ids: list[str] | None = None,
    gray_chat_ids: list[str] | None = None,
    audience: str = "frankie",
    procurement_approved: bool = False,
) -> dict:
    if mode not in ("dry_run", "commit"):
        raise ValueError("mode must be dry_run or commit")
    if audience not in AUDIENCES:
        raise ValueError("audience must be frankie or procurement")
    batch = batch_id or DEFAULT_BATCH_ID
    ids = record_ids if record_ids is not None else DEFAULT_RECORD_IDS
    candidates = await _get_candidates_by_ids(ids) if ids else await _search_candidates(limit=limit)
    active = _active_candidates(candidates)
    if mode == "commit":
        await _prepare_card_images(active)
    card = build_procurement_preview_card(candidates, batch, audience=audience)
    validation_errors = validate_procurement_preview_card(card, candidates, audience=audience)
    if validation_errors:
        raise RuntimeError("Procurement preview card self-test failed: " + "; ".join(validation_errors))
    if audience == "procurement" and not frankie_only and not procurement_approved:
        raise ValueError("sending procurement audience to non-Frankie recipients requires procurement_approved=true")
    effective_frankie_only = bool(frankie_only or (FRANKIE_ONLY and not procurement_approved))
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "audience": audience,
        "procurement_approved": bool(procurement_approved),
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
        result["would_send_to_procurement"] = bool(audience == "procurement" and not effective_frankie_only)
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
