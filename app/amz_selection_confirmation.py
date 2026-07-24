# -*- coding: utf-8 -*-
"""Amazon Europe selection-result confirmation cards.

This replaces the old fixed "50-unit validation" handoff at the end of the
selection phase. The card asks for a business decision: enter procurement,
enter procurement with conditions, hold, or reject.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from . import amz_assistant, amz_procurement_quote as proc


BJ = timezone(timedelta(hours=8))

SOURCE = "amz_selection_confirmation"
ACTION_GO = "amz_selection_confirm_go"
ACTION_CONDITIONAL = "amz_selection_confirm_conditional_go"
ACTION_HOLD = "amz_selection_confirm_hold"
ACTION_REJECT = "amz_selection_confirm_reject"
ACTION_TO_DECISION = {
    ACTION_GO: "Go",
    ACTION_CONDITIONAL: "条件推进",
    ACTION_HOLD: "暂缓",
    ACTION_REJECT: "淘汰",
}
DECISION_ACTIONS = tuple(ACTION_TO_DECISION.keys())

DEFAULT_BATCH_ID = os.environ.get("AMZ_SELECTION_CONFIRM_DEFAULT_BATCH_ID", "AMZ-EU-SELCONF-20260724-P0")
DEFAULT_RECORD_IDS = [
    x.strip()
    for x in os.environ.get(
        "AMZ_SELECTION_CONFIRM_DEFAULT_RECORD_IDS",
        "recvq1QtafnVjX,recvq1QtUEEcXv,recvq1QtFKPwoI,recvq1Quaar3h2",
    ).split(",")
    if x.strip()
]
FRANKIE_ONLY = (os.environ.get("AMZ_SELECTION_CONFIRM_FRANKIE_ONLY", "1") or "1") != "0"
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", amz_assistant.FRANKIE_UNION_ID)
GRAY_UNION_IDS = [x.strip() for x in os.environ.get("AMZ_SELECTION_CONFIRM_GRAY_UNION_IDS", "").split(",") if x.strip()]
GRAY_CHAT_IDS = [x.strip() for x in os.environ.get("AMZ_SELECTION_CONFIRM_GRAY_CHAT_IDS", "").split(",") if x.strip()]

EUR_RMB = float(os.environ.get("AMZ_SELECTION_EUR_RMB", "7.85"))
GBP_RMB = float(os.environ.get("AMZ_SELECTION_GBP_RMB", "9.30"))
DEFAULT_PACK_MULTIPLE = int(os.environ.get("AMZ_SELECTION_PACK_MULTIPLE", "5"))

SITES = [
    {"code": "DE", "label": "德国", "currency": "EUR", "symbol": "€", "fx": EUR_RMB},
    {"code": "UK", "label": "英国", "currency": "GBP", "symbol": "£", "fx": GBP_RMB},
    {"code": "FR", "label": "法国", "currency": "EUR", "symbol": "€", "fx": EUR_RMB},
    {"code": "IT", "label": "意大利", "currency": "EUR", "symbol": "€", "fx": EUR_RMB},
    {"code": "ES", "label": "西班牙", "currency": "EUR", "symbol": "€", "fx": EUR_RMB},
]

ENTRY_FACTORS = {
    "Go": 0.12,
    "条件推进": 0.08,
    "暂缓": 0.0,
    "淘汰": 0.0,
}
COVERAGE_DAYS = {
    "A": 30,
    "FBA经济线": 30,
    "FBA头程-经济线": 30,
    "经济线": 30,
    "B": 21,
    "FBA快速线": 21,
    "FBA头程-快速线": 21,
    "快速线": 21,
    "C": 14,
    "FBM-4PX": 14,
    "FBM": 14,
    "4PX": 14,
    "自发货": 14,
}
QTY_LIMITS = {
    "Go": {"min": 10, "max_site": 80, "max_total": 150},
    "条件推进": {"min": 5, "max_site": 30, "max_total": 60},
    "暂缓": {"min": 0, "max_site": 0, "max_total": 0},
    "淘汰": {"min": 0, "max_site": 0, "max_total": 0},
}

_bg_tasks: set[asyncio.Task] = set()
_recent_callbacks: dict[str, float] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_label() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M")


def _text(value: Any) -> str:
    return proc._text(value)


def _safe_id(value: str) -> str:
    return proc._safe_id(value)


def _field(label: str, value: Any) -> dict:
    return proc._field(label, value)


def _url_button(text: str, url: str, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "url": url}


def _button(text: str, action: str, candidate: dict, card_record_ids: list[str], typ: str = "default") -> dict:
    return {
        "tag": "button",
        "type": typ,
        "text": {"tag": "plain_text", "content": text},
        "value": _payload(candidate, card_record_ids, action),
    }


def _record_url(record_id: str) -> str:
    return proc._record_url(record_id)


def _path(record_id: str = "") -> str:
    return proc._path(record_id)


async def _feishu_api(method: str, path: str, body: dict | None = None) -> dict:
    return await proc._feishu_api(method, path, body)


async def _update_candidate(record_id: str, fields: dict) -> None:
    await proc._update_candidate(record_id, fields)


async def _get_candidate(record_id: str) -> dict:
    data = await _feishu_api("GET", _path(record_id))
    record = ((data.get("data") or {}).get("record") or {})
    return _candidate_from_record(record)


async def _get_candidates_by_ids(record_ids: list[str]) -> list[dict]:
    out = []
    for rid in record_ids:
        if rid:
            out.append(await _get_candidate(rid))
    return out


async def _search_candidates(batch_id: str = "", limit: int = 10) -> list[dict]:
    conditions = [
        {
            "field_name": "综合结论",
            "operator": "contains",
            "value": ["50件验证"],
        }
    ]
    body = {
        "page_size": min(max(int(limit or 10), 1), 20),
        "filter": {"conjunction": "and", "conditions": conditions},
    }
    data = await _feishu_api("POST", _path() + "/search", body)
    rows = ((data.get("data") or {}).get("items") or [])
    return [_candidate_from_record(row) for row in rows]


async def _prepare_card_images(candidates: list[dict]) -> None:
    await proc._prepare_card_images(candidates)


def _url(value: Any) -> str:
    return proc._url(value)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, bool):
            return None
        return float(value)
    text = _text(value)
    if not text:
        return None
    text = text.replace(",", "").replace("￥", "").replace("¥", "").replace("€", "").replace("£", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _fmt_num(value: Any, digits: int = 2) -> str:
    num = _num(value)
    if num is None:
        return "-"
    if abs(num - round(num)) < 0.005:
        return str(int(round(num)))
    return f"{num:.{digits}f}".rstrip("0").rstrip(".")


def _fmt_money(value: Any, symbol: str = "€") -> str:
    num = _num(value)
    if num is None:
        return "-"
    return f"{symbol}{num:.2f}".rstrip("0").rstrip(".")


def _fmt_rmb(value: Any) -> str:
    return proc._format_rmb(value)


def _fmt_rate(value: Any) -> str:
    return proc._format_rate(value)


def _field_value(fields: dict, *names: str) -> Any:
    for name in names:
        if name in fields:
            value = fields.get(name)
            if _text(value):
                return value
    return None


def _site_value(fields: dict, site: str, names: list[str]) -> Any:
    prefixes = [
        f"{site}",
        f"{site}站",
        f"{site}-",
        f"{site}_",
        f"{site} ",
    ]
    suffixes = [
        f"-{site}",
        f"_{site}",
        f" {site}",
        f"({site})",
    ]
    candidates: list[str] = []
    for name in names:
        candidates.append(f"{site}{name}")
        candidates.append(f"{site}站{name}")
        candidates.append(f"{site}-{name}")
        candidates.append(f"{site}_{name}")
        candidates.append(f"{name}-{site}")
        candidates.append(f"{name}_{site}")
        candidates.append(f"{name}{site}")
        candidates.append(f"{name}({site})")
    for key, value in fields.items():
        key_text = _text(key)
        for name in names:
            if key_text in candidates:
                return value
            if any(key_text.startswith(prefix) and name in key_text for prefix in prefixes):
                return value
            if any(key_text.endswith(suffix) and name in key_text for suffix in suffixes):
                return value
    if site == "DE":
        return _field_value(fields, *names)
    return None


def _candidate_from_record(record: dict) -> dict:
    base = proc._candidate_from_record(record)
    fields = record.get("fields") or {}
    base.update(
        {
            "raw_fields": fields,
            "site": " / ".join(proc._list_values(fields.get("站点"))) or _text(fields.get("站点")) or "DE",
            "current_status": " / ".join(proc._list_values(fields.get("当前状态"))) or "-",
            "overall_decision": " / ".join(proc._list_values(fields.get("综合结论"))) or "-",
            "finance_gate": " / ".join(proc._list_values(fields.get("财务闸结论"))) or "-",
            "compliance_gate": " / ".join(proc._list_values(fields.get("合规闸结论"))) or "待核",
            "risk_note": _text(fields.get("侵权风险说明")),
            "data_gaps": proc._list_values(fields.get("数据缺口")),
            "next_action": " / ".join(proc._list_values(fields.get("下一步动作"))) or "-",
            "review_note": _text(fields.get("人审备注")),
            "selection_decision": " / ".join(proc._list_values(fields.get("选品确认状态"))) or "",
            "selection_batch_id": _text(fields.get("选品确认批次ID")),
            "selection_message_id": _text(fields.get("选品确认卡片消息ID")),
        }
    )
    base["site_suggestions"] = _build_site_suggestions(base)
    base["system_selection_decision"], base["system_selection_reason"] = _system_decision(base)
    return base


def _recommended_channel(candidate: dict) -> dict:
    channels = [c for c in (candidate.get("channels") or []) if proc._channel_has_data(c)]
    if not channels:
        channels = [proc._legacy_channel(candidate)]
    fulfillment = _text(candidate.get("fulfillment"))
    for channel in channels:
        items = [channel.get("code"), channel.get("label"), *(channel.get("aliases") or [])]
        if fulfillment and any(_text(item) and _text(item) in fulfillment for item in items):
            return channel
    with_rates = [(c, _num(c.get("margin_rate"))) for c in channels]
    with_rates = [(c, r) for c, r in with_rates if r is not None]
    if with_rates:
        return max(with_rates, key=lambda item: item[1])[0]
    return channels[0]


def _coverage_days(candidate: dict) -> int:
    channel = _recommended_channel(candidate)
    keys = [channel.get("code"), channel.get("label"), *(channel.get("aliases") or []), candidate.get("fulfillment")]
    for key in keys:
        text = _text(key)
        if text in COVERAGE_DAYS:
            return COVERAGE_DAYS[text]
        for known, days in COVERAGE_DAYS.items():
            if known and known in text:
                return days
    return 21


def _round_up(value: float, multiple: int) -> int:
    if value <= 0:
        return 0
    multiple = max(int(multiple or 1), 1)
    return int(math.ceil(value / multiple) * multiple)


def reference_monthly_sales(competitor_avg: Any, new_avg: Any) -> float | None:
    comp = _num(competitor_avg)
    new = _num(new_avg)
    if comp is None and new is None:
        return None
    if comp is None:
        return new
    if new is None:
        return comp
    return comp * 0.6 + new * 0.4


def suggest_purchase_qty(
    *,
    competitor_avg_monthly_sales: Any,
    category_new_avg_monthly_sales: Any,
    decision: str,
    coverage_days: int,
    moq: Any = None,
    pack_multiple: int = DEFAULT_PACK_MULTIPLE,
) -> tuple[int | None, str]:
    decision = ACTION_TO_DECISION.get(decision, decision)
    if decision not in QTY_LIMITS:
        decision = "条件推进"
    if decision in ("暂缓", "淘汰"):
        return 0, decision
    ref_sales = reference_monthly_sales(competitor_avg_monthly_sales, category_new_avg_monthly_sales)
    if ref_sales is None:
        return None, "需补竞品月销量和类目新品月销量"
    limits = QTY_LIMITS[decision]
    factor = ENTRY_FACTORS[decision]
    raw_qty = ref_sales / 30 * int(coverage_days or 21) * factor
    lower = max(int(_num(moq) or 0), int(limits["min"]))
    capped = min(max(raw_qty, lower), int(limits["max_site"]))
    return _round_up(capped, pack_multiple), f"参考月销{_fmt_num(ref_sales)} × 入场系数{factor:.0%} × {coverage_days}天"


def _suggested_price(sample_price: Any, median_price: Any, avg_price: Any, decision: str) -> float | None:
    sample = _num(sample_price)
    median = _num(median_price)
    avg = _num(avg_price)
    if median is not None and sample is not None:
        anchor = median * 0.7 + sample * 0.3
    elif median is not None:
        anchor = median
    elif sample is not None:
        anchor = sample
    elif avg is not None:
        anchor = avg
    else:
        return None
    coef = 0.95 if decision == "Go" else 0.92 if decision == "条件推进" else 0.0
    if coef <= 0:
        return None
    return round(anchor * coef, 2)


def _site_margin_text(fields: dict, site: str) -> str:
    cn_rate = _site_value(fields, site, ["中企号毛利率%", "中企号A-毛利率%", "中企号-毛利率%", "A-毛利率%"])
    local_rate = _site_value(fields, site, ["本土号毛利率%", "本本号毛利率%", "本土号A-毛利率%", "本本号A-毛利率%"])
    parts = []
    if _text(cn_rate):
        parts.append(f"中企 {_fmt_rate(cn_rate)}")
    if _text(local_rate):
        parts.append(f"本土 {_fmt_rate(local_rate)}")
    return " / ".join(parts) if parts else "毛利待结构化"


def _build_site_suggestions(candidate: dict) -> list[dict]:
    fields = candidate.get("raw_fields") or {}
    product_decision, _ = _system_decision_from_fields(candidate, fields)
    coverage = _coverage_days(candidate)
    out = []
    for site in SITES:
        code = site["code"]
        sample_price = _site_value(
            fields,
            code,
            ["样本竞品售价", "样本ASIN售价", "样本售价", "售价€", "售价£", "售价", "价格€", "价格£", "价格", "竞品售价"],
        )
        avg_price = _site_value(fields, code, ["竞品均价", "竞品平均售价", "竞品平均价", "平均售价", "平均价格"])
        median_price = _site_value(fields, code, ["竞品中位价", "竞品价格中位数", "竞品售价中位数", "价格中位数"])
        price_range = _site_value(fields, code, ["竞品价格区间", "价格区间", "售价区间"])
        comp_sales = _site_value(fields, code, ["竞品平均月销量", "竞品月销量均值", "竞品月销均值", "平均月销量", "月销量", "月销量估算"])
        new_sales = _site_value(fields, code, ["类目新品平均月销量", "新品平均月销量", "新品月销均值", "新品月销量"])
        site_decision = product_decision
        reasons = []
        if _num(sample_price) is None and _num(avg_price) is None and _num(median_price) is None:
            site_decision = "暂缓" if product_decision != "淘汰" else "淘汰"
            reasons.append("需补本站售价")
        qty, qty_note = suggest_purchase_qty(
            competitor_avg_monthly_sales=comp_sales,
            category_new_avg_monthly_sales=new_sales,
            decision=site_decision,
            coverage_days=coverage,
            moq=_site_value(fields, code, ["MOQ", "最小起订量"]),
        )
        if qty is None:
            reasons.append(qty_note)
        suggested_price = _site_value(fields, code, ["建议售价", "建议售价€", "建议售价£", "建议价格"])
        if _num(suggested_price) is None:
            suggested_price = _suggested_price(sample_price, median_price, avg_price, site_decision)
        out.append(
            {
                "site": code,
                "label": site["label"],
                "currency": site["currency"],
                "symbol": site["symbol"],
                "fx": site["fx"],
                "sample_price": sample_price,
                "avg_price": avg_price,
                "median_price": median_price,
                "price_range": price_range,
                "suggested_price": suggested_price,
                "competitor_avg_monthly_sales": comp_sales,
                "category_new_avg_monthly_sales": new_sales,
                "reference_monthly_sales": reference_monthly_sales(comp_sales, new_sales),
                "coverage_days": coverage,
                "suggested_qty": qty,
                "qty_note": qty_note,
                "decision": site_decision,
                "margin_text": _site_margin_text(fields, code),
                "reason": "；".join(reasons) if reasons else "数据可用",
            }
        )
    _apply_total_qty_cap(out, product_decision)
    return out


def _apply_total_qty_cap(rows: list[dict], decision: str) -> None:
    cap = int(QTY_LIMITS.get(decision, QTY_LIMITS["条件推进"])["max_total"])
    if cap <= 0:
        return
    numeric_rows = [row for row in rows if isinstance(row.get("suggested_qty"), int) and row["suggested_qty"] > 0]
    total = sum(row["suggested_qty"] for row in numeric_rows)
    if total <= cap:
        return
    ratio = cap / total
    for row in numeric_rows:
        row["suggested_qty"] = max(DEFAULT_PACK_MULTIPLE, _round_up(row["suggested_qty"] * ratio, DEFAULT_PACK_MULTIPLE))
    while sum(row["suggested_qty"] for row in numeric_rows) > cap and numeric_rows:
        largest = max(numeric_rows, key=lambda item: item["suggested_qty"])
        if largest["suggested_qty"] <= DEFAULT_PACK_MULTIPLE:
            break
        largest["suggested_qty"] -= DEFAULT_PACK_MULTIPLE


def _best_margin_rate(candidate: dict) -> float | None:
    rates = []
    for channel in candidate.get("channels") or []:
        value = _num(channel.get("margin_rate"))
        if value is not None:
            rates.append(value)
    return max(rates) if rates else None


def _system_decision_from_fields(candidate: dict, fields: dict) -> tuple[str, str]:
    current = _text(candidate.get("current_status"))
    overall = _text(candidate.get("overall_decision"))
    finance = _text(candidate.get("finance_gate"))
    compliance = _text(candidate.get("compliance_gate"))
    margin = _best_margin_rate(candidate)
    if "淘汰" in current or "淘汰" in overall:
        return "淘汰", "候选表当前已标淘汰"
    if "暂缓" in current or "暂缓" in overall:
        return "暂缓", "候选表当前已标暂缓"
    if "通过" in finance and compliance == "Go":
        return "Go", "财务和自动合规均通过，进入采购阶段前只需确认首批采购量"
    if margin is not None and margin >= 30:
        return "条件推进", "毛利达到观察线，但仍需补齐售价/月销/合规或压价条件"
    if "暂缓" in finance:
        return "条件推进", "财务闸为暂缓，需压采购价或限制站点后再推进"
    return "暂缓", "关键数据不足或利润未过线，先补数据重算"


def _system_decision(candidate: dict) -> tuple[str, str]:
    return _system_decision_from_fields(candidate, candidate.get("raw_fields") or {})


def _site_line(row: dict) -> str:
    symbol = row["symbol"]
    qty = row.get("suggested_qty")
    qty_text = f"{qty}件" if isinstance(qty, int) else "需补月销"
    ref = _fmt_num(row.get("reference_monthly_sales"))
    sample = _fmt_money(row.get("sample_price"), symbol)
    suggested = _fmt_money(row.get("suggested_price"), symbol)
    return (
        f"- {row['site']}: 竞品价 {sample}｜建议价 {suggested}｜"
        f"竞品月销 {_fmt_num(row.get('competitor_avg_monthly_sales'))}｜"
        f"新品月销 {_fmt_num(row.get('category_new_avg_monthly_sales'))}｜"
        f"参考月销 {ref}｜建议采购 {qty_text}｜{row.get('margin_text')}｜{row.get('reason')}"
    )


def _total_suggested_qty(candidate: dict) -> int | None:
    rows = candidate.get("site_suggestions") or []
    quantities = [row.get("suggested_qty") for row in rows if isinstance(row.get("suggested_qty"), int)]
    if not quantities:
        return None
    return int(sum(quantities))


def _site_price_summary(candidate: dict) -> str:
    rows = candidate.get("site_suggestions") or []
    return "\n".join(_site_line(row) for row in rows)


def _unit_price_rmb(candidate: dict) -> float | None:
    for row in candidate.get("site_suggestions") or []:
        if row.get("site") != "DE":
            continue
        price = _num(row.get("suggested_price") or row.get("sample_price") or row.get("median_price") or row.get("avg_price"))
        if price is not None:
            return price * float(row.get("fx") or EUR_RMB)
    for row in candidate.get("site_suggestions") or []:
        price = _num(row.get("suggested_price") or row.get("sample_price") or row.get("median_price") or row.get("avg_price"))
        if price is not None:
            return price * float(row.get("fx") or EUR_RMB)
    return None


def _cashflow_line(candidate: dict) -> str:
    qty = _total_suggested_qty(candidate)
    price_rmb = _unit_price_rmb(candidate)
    channel = _recommended_channel(candidate)
    procurement = _num(candidate.get("quote_cost"))
    logistics = _num(channel.get("logistics_rmb"))
    margin = _num(channel.get("margin_rmb"))
    if qty is None:
        return "建议采购量待补月销量后计算；当前不能给总投入。"
    if procurement is None or logistics is None:
        return f"建议采购总量 {qty} 件；采购成本或物流成本缺失，暂不能算总投入。"
    unit_invest = procurement + logistics
    total_invest = unit_invest * qty
    if margin is None or price_rmb is None or price_rmb <= 0:
        return f"建议采购总量 {qty} 件；预计采购+物流投入约 {total_invest:.2f} RMB；回款比待补售价/毛利字段。"
    net_receipt = procurement + logistics + margin
    payback_ratio = net_receipt / price_rmb
    gross_receipt = net_receipt * qty
    return (
        f"建议采购总量 {qty} 件；单件采购+物流投入约 {unit_invest:.2f} RMB；"
        f"首批投入约 {total_invest:.2f} RMB；预计净回款约 {gross_receipt:.2f} RMB；"
        f"回款比约 {payback_ratio:.1%}。"
    )


def _decision_help_text() -> str:
    return (
        "**四个按钮怎么用**\n"
        "- **Go**：同意进入采购阶段；采购只做 MOQ、交期、同款和供应商复核，不再退回选品。\n"
        "- **条件推进**：只允许按卡片条件推进，比如限站点、压采购价、补月销量、复核套装件数；条件没达成不下单。\n"
        "- **暂缓**：当前不采购，先补售价、月销、FBA费、合规或供应链资料后再重算。\n"
        "- **淘汰**：从本批次移出，不进入采购阶段；除非重新跑选品，否则不再推进。"
    )


def _payload(candidate: dict, card_record_ids: list[str], action: str) -> dict:
    return {
        "source": SOURCE,
        "action": action,
        "record_id": candidate.get("record_id"),
        "asin": candidate.get("asin"),
        "batch_id": candidate.get("selection_batch_id") or DEFAULT_BATCH_ID,
        "card_record_ids": card_record_ids,
        "system_decision": candidate.get("system_selection_decision"),
        "suggested_total_qty": _total_suggested_qty(candidate),
    }


def _completed(candidate: dict) -> bool:
    decision = _text(candidate.get("selection_decision"))
    return decision in ("已Go", "Go", "条件推进", "暂缓", "淘汰")


def _product_elements(candidate: dict, card_record_ids: list[str]) -> list[dict]:
    rid = candidate.get("record_id", "")
    title = candidate.get("cn_name") or candidate.get("title") or candidate.get("asin") or rid
    amazon = candidate.get("amazon_url")
    image = candidate.get("image_url")
    supplier = candidate.get("supplier_link")
    system_decision = candidate.get("system_selection_decision") or "暂缓"
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
    elements.append(
        {
            "tag": "div",
            "fields": [
                _field("ASIN", candidate.get("asin")),
                _field("系统建议", system_decision),
                _field("采购成本", _fmt_rmb(candidate.get("quote_cost"))),
                _field("建议采购总量", f"{_total_suggested_qty(candidate)}件" if _total_suggested_qty(candidate) is not None else "需补月销"),
                _field("推荐履约", candidate.get("fulfillment")),
                _field("包装尺寸", candidate.get("package_size") or "待核"),
                _field("重量", f"{candidate.get('weight_g')}g" if candidate.get("weight_g") else "待核"),
                _field("件数", candidate.get("set_count") or "待核"),
                _field("FBA配送费 / 佣金", f"{proc._format_eur(candidate.get('fba_fee_eur'))} / {proc._format_eur(candidate.get('commission_eur'))}"),
            ],
        }
    )
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**竞品售价、建议售价与各站采购量**\n" + _site_price_summary(candidate)}})
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": proc._channel_compare_text(candidate)}})
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**回款/投入分析**\n" + _cashflow_line(candidate)}})
    elements.append(
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**系统判断原因**\n{candidate.get('system_selection_reason') or '-'}\n\n"
                    f"**合规/适配注意点**\n{candidate.get('risk_note') or '已自动快速扫描；Listing 和包装仍需只写 compatible/replacement/适配关系。'}\n\n"
                    f"**套装内容/采购注意**\n{candidate.get('set_content') or '待按主图和供应商页核对'}"
                ),
            },
        }
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
    if _completed(candidate):
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**选品确认已处理**\n当前确认动作：{candidate.get('selection_decision')}"},
            }
        )
        return elements
    elements.append(
        {
            "tag": "action",
            "actions": [
                _button("Go", ACTION_GO, candidate, card_record_ids, "primary"),
                _button("条件推进", ACTION_CONDITIONAL, candidate, card_record_ids, "default"),
                _button("暂缓", ACTION_HOLD, candidate, card_record_ids, "default"),
                _button("淘汰", ACTION_REJECT, candidate, card_record_ids, "danger"),
            ],
        }
    )
    return elements


def build_selection_confirmation_card(candidates: list[dict], batch_id: str = "") -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    total = len(candidates)
    done = sum(1 for item in candidates if _completed(item))
    pending = total - done
    template = "green" if total and pending == 0 else "yellow"
    title_status = "已全部确认" if total and pending == 0 else f"待确认 {pending}/{total}"
    record_ids = [c.get("record_id", "") for c in candidates if c.get("record_id")]
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    f"**状态**: {title_status}\n"
                    "**定位**: 这是选品阶段收尾卡，确认是否进入采购阶段；不是上架验证卡，也不是固定50件试销卡。\n\n"
                    + _decision_help_text()
                ),
            },
        },
        {"tag": "note", "elements": [{"tag": "plain_text", "content": "首批采购量按竞品月销量×60% + 类目新品月销量×40% 估算；缺月销时只展示缺口，不硬算数量。"}]},
    ]
    for candidate in candidates:
        elements.extend(_product_elements(candidate, record_ids))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": f"🟡 [AMZ·P0] 欧洲站选品结果确认 · {title_status}"},
        },
        "elements": elements,
    }


def validate_selection_confirmation_card(card: dict, candidates: list[dict]) -> list[str]:
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
        if not _completed(candidate):
            actions = {
                _text((button.get("value") or {}).get("action"))
                for button in buttons
                if _text((button.get("value") or {}).get("record_id")) == rid
            }
            for action in DECISION_ACTIONS:
                if action not in actions:
                    errors.append(f"{label}: missing decision action {action}")
    for required in ("四个按钮怎么用", "竞品售价", "建议售价", "建议采购", "回款/投入分析", "三渠道对比", "Go", "条件推进", "暂缓", "淘汰"):
        if required not in rendered:
            errors.append(f"card missing {required}")
    if '"tag": "form"' in rendered or "form_submit" in rendered:
        errors.append("selection confirmation card should use direct decision buttons, not forms")
    return errors


def _message_id(event: dict) -> str:
    return proc._message_id(event)


def _operator_label(event: dict) -> str:
    return proc._operator_label(event)


def _toast(content: str, typ: str = "success") -> dict:
    return proc._toast(content, typ)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _extract_action(event: dict) -> tuple[str, dict]:
    action, value, _ = proc._extract_action(event)
    return action, value


def _callback_key(record_id: str, action: str) -> str:
    return f"{record_id}:{action}"


def _recent_seen(key: str, ttl_sec: int = 300) -> bool:
    now = time.time()
    for old, ts in list(_recent_callbacks.items()):
        if now - ts > ttl_sec:
            _recent_callbacks.pop(old, None)
    return key in _recent_callbacks and now - _recent_callbacks[key] <= ttl_sec


def _decision_next_action(decision: str) -> str:
    return {
        "Go": "进入采购阶段：采购复核MOQ/交期/同款后下单",
        "条件推进": "条件进入采购阶段：限站点/压价/补月销/复核套装后再下单",
        "暂缓": "暂缓采购：补售价/月销/FBA费/合规或供应链资料后重算",
        "淘汰": "淘汰归档：不进入本批采购",
    }.get(decision, "待处理")


def _append_review_note(candidate: dict, decision: str, actor: str, value: dict) -> str:
    qty = value.get("suggested_total_qty")
    qty_text = f"{qty}件" if isinstance(qty, int) else "待补月销后计算"
    line = (
        f"{_now_label()} {actor}: 选品结果确认={decision}; "
        f"系统建议={value.get('system_decision') or '-'}; "
        f"建议采购总量={qty_text}; 批次={value.get('batch_id') or DEFAULT_BATCH_ID}."
    )
    old = _text(candidate.get("review_note"))
    if not old:
        return line
    return old if line in old else f"{old}\n{line}"


def _build_update_fields(candidate: dict, action: str, actor: str, value: dict) -> dict:
    decision = ACTION_TO_DECISION[action]
    fields = {
        "当前状态": "待采购确认" if decision == "Go" else "待采购复核" if decision == "条件推进" else decision,
        "综合结论": decision,
        "下一步动作": _decision_next_action(decision),
        "人审备注": _append_review_note(candidate, decision, actor, value),
    }
    return fields


async def _process_callback_background(event: dict, callback_key: str) -> None:
    try:
        result = await _process_callback(event)
        if ((result.get("toast") or {}).get("type") or "") == "error":
            _recent_callbacks.pop(callback_key, None)
    except Exception as exc:
        _recent_callbacks.pop(callback_key, None)
        print(f"[amz_selection_confirmation.callback_bg] {callback_key} fail: {exc}")


async def _process_callback(event: dict) -> dict:
    action, value = _extract_action(event)
    if action not in DECISION_ACTIONS:
        return _toast("未知选品确认动作", "error")
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    candidate = await _get_candidate(record_id)
    msg_id = _message_id(event) or candidate.get("selection_message_id")
    actor = _operator_label(event)
    fields = _build_update_fields(candidate, action, actor, value)
    await _update_candidate(record_id, fields)
    candidate.update(
        {
            "current_status": fields["当前状态"],
            "overall_decision": fields["综合结论"],
            "next_action": fields["下一步动作"],
            "review_note": fields["人审备注"],
            "selection_decision": fields["综合结论"],
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
            await amz_assistant.update_card(msg_id, build_selection_confirmation_card(candidates, _text(value.get("batch_id"))))
        else:
            await amz_assistant.update_card(msg_id, build_selection_confirmation_card([candidate], _text(value.get("batch_id"))))
    return _toast(f"本产品已确认：{ACTION_TO_DECISION[action]}")


async def handle_callback(event: dict) -> dict:
    action, value = _extract_action(event)
    if action not in DECISION_ACTIONS:
        return {"ok": False, "ignored": True, "action": action}
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    callback_key = _callback_key(record_id, action)
    if _recent_seen(callback_key):
        try:
            current = await _get_candidate(record_id)
            if _text(current.get("overall_decision")) == ACTION_TO_DECISION[action]:
                return _toast("该产品已按相同动作确认，无需重复点击")
        except Exception as exc:
            print(f"[amz_selection_confirmation.callback_duplicate_check] {record_id} fail: {exc}")
        _recent_callbacks.pop(callback_key, None)
        _recent_callbacks[callback_key] = time.time()
        _spawn(_process_callback_background(event, callback_key))
        return _toast("已重新收到选品确认动作，正在补写候选表并更新原卡")
    _recent_callbacks[callback_key] = time.time()
    _spawn(_process_callback_background(event, callback_key))
    return _toast("已收到选品确认动作，正在写回候选表并更新原卡")


async def send_selection_confirmation_card(
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
    candidates = await _get_candidates_by_ids(ids) if ids else await _search_candidates(batch, limit=limit)
    if mode == "commit":
        await _prepare_card_images(candidates)
    card = build_selection_confirmation_card(candidates, batch)
    validation_errors = validate_selection_confirmation_card(card, candidates)
    if validation_errors:
        raise RuntimeError("Selection confirmation card self-test failed: " + "; ".join(validation_errors))
    effective_frankie_only = bool(frankie_only or FRANKIE_ONLY)
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "batch_id": batch,
        "count": len(candidates),
        "record_ids": [c.get("record_id") for c in candidates],
        "card_selftest": "passed",
        "suggested_total_qty": sum(
            _total_suggested_qty(c) or 0
            for c in candidates
        ),
        "data_gap_count": sum(
            1
            for c in candidates
            for row in (c.get("site_suggestions") or [])
            if row.get("suggested_qty") is None
        ),
        **proc._card_media_stats(candidates),
    }
    if mode == "dry_run":
        result["card"] = card
        result["would_update_on_click"] = [
            {
                "record_id": c.get("record_id"),
                "buttons": {
                    ACTION_TO_DECISION[action]: _build_update_fields(c, action, "dry-run", _payload(c, result["record_ids"], action))
                    for action in DECISION_ACTIONS
                },
            }
            for c in candidates
        ]
        return result
    if not candidates:
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
            raise RuntimeError("Selection confirmation recipients are not configured. Set AMZ_SELECTION_CONFIRM_GRAY_UNION_IDS or AMZ_SELECTION_CONFIRM_GRAY_CHAT_IDS.")
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
