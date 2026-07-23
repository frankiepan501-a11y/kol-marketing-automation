# -*- coding: utf-8 -*-
"""Amazon Europe compliance and fitment review cards.

P0 scope:
- send a Frankie-only card for selected candidate records;
- each product has its own compliance result/risk/note controls;
- callback writes only that product row and patches the original card.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from . import amz_assistant, amz_procurement_quote as proc


BJ = timezone(timedelta(hours=8))

ACTION_SUBMIT = "amz_fit_check_submit"

DEFAULT_BATCH_ID = os.environ.get("AMZ_COMPLIANCE_DEFAULT_BATCH_ID", "AMZ-DE-FITCHECK-20260723-P0")
DEFAULT_RECORD_IDS = [
    x.strip()
    for x in os.environ.get(
        "AMZ_COMPLIANCE_DEFAULT_RECORD_IDS",
        "recvq1QtafnVjX,recvq1QtUEEcXv",
    ).split(",")
    if x.strip()
]
FRANKIE_ONLY = (os.environ.get("AMZ_COMPLIANCE_CARD_FRANKIE_ONLY", "1") or "1") != "0"
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", amz_assistant.FRANKIE_UNION_ID)
GRAY_UNION_IDS = [x.strip() for x in os.environ.get("AMZ_COMPLIANCE_GRAY_UNION_IDS", "").split(",") if x.strip()]
GRAY_CHAT_IDS = [x.strip() for x in os.environ.get("AMZ_COMPLIANCE_GRAY_CHAT_IDS", "").split(",") if x.strip()]

FIELD_NAMES = [
    "ASIN",
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
    "数据缺口",
    "下一步动作",
    "人审备注",
]

FIT_RESULTS = ("Go", "需整改", "No-Go")
IP_RISKS = ("低", "中", "高", "不可做")
DONE_GATES = ("Go", "暂缓", "No-Go")

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


def _button_option(value: str) -> dict:
    return {"text": {"tag": "plain_text", "content": value}, "value": value}


def _candidate_from_record(record: dict) -> dict:
    base = proc._candidate_from_record(record)
    fields = record.get("fields") or {}
    base.update(
        {
            "current_status": " / ".join(proc._list_values(fields.get("当前状态"))) or "-",
            "overall_decision": " / ".join(proc._list_values(fields.get("综合结论"))) or "-",
            "finance_gate": " / ".join(proc._list_values(fields.get("财务闸结论"))) or "-",
            "compliance_gate": " / ".join(proc._list_values(fields.get("合规闸结论"))) or "待核",
            "ip_risk": " / ".join(proc._list_values(fields.get("IP/外观风险"))) or "待核",
            "risk_note": _text(fields.get("侵权风险说明")),
            "data_gaps": proc._list_values(fields.get("数据缺口")),
            "next_action": " / ".join(proc._list_values(fields.get("下一步动作"))) or "-",
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


async def _search_candidates(batch_id: str = "", limit: int = 2) -> list[dict]:
    conditions = [
        {"field_name": "当前状态", "operator": "contains", "value": ["待合规核查"]},
    ]
    body = {
        "page_size": min(max(int(limit or 2), 1), 20),
        "field_names": FIELD_NAMES,
        "filter": {"conjunction": "and", "conditions": conditions},
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


def _completed(candidate: dict) -> bool:
    gate = _text(candidate.get("compliance_gate"))
    return gate in DONE_GATES


def _payload(candidate: dict, card_record_ids: list[str]) -> dict:
    return {
        "source": "amz_compliance_fit",
        "action": ACTION_SUBMIT,
        "record_id": candidate.get("record_id"),
        "asin": candidate.get("asin"),
        "batch_id": candidate.get("fit_batch_id") or DEFAULT_BATCH_ID,
        "card_record_ids": card_record_ids,
    }


def _line_item(label: str, value: Any) -> str:
    return f"**{label}**: {_text(value) or '-'}"


def _margin_line(candidate: dict) -> str:
    channels = [c for c in (candidate.get("channels") or []) if proc._channel_has_data(c)]
    if not channels:
        return "A/B/C 毛利暂缺"
    parts = []
    for channel in channels:
        suffix = proc._recommended_suffix(channel, candidate.get("fulfillment"))
        parts.append(
            f"{channel.get('code')}{suffix}: "
            f"{proc._format_rmb(channel.get('margin_rmb'))}/{proc._format_rate(channel.get('margin_rate'))}"
        )
    return " ｜ ".join(parts)


def _risk_hint(candidate: dict) -> str:
    title = " ".join(
        x
        for x in [
            _text(candidate.get("title")),
            _text(candidate.get("cn_name")),
            _text(candidate.get("set_content")),
        ]
        if x
    )
    brand_hits = []
    for name in ("Dreame", "Xiaomi", "Roborock", "Dyson"):
        if re.search(name, title, re.I):
            brand_hits.append(name)
    brand_text = "、".join(brand_hits) if brand_hits else "兼容品牌词待核"
    return (
        f"型号适配：按 Listing、主图、1688实物和套装件数核对；品牌/型号词：{brand_text}，只能写兼容，不能暗示原厂；"
        "欧洲上架还需核 GPSR 负责人、警示/标签、包装和说明书语言。"
    )


def _product_elements(candidate: dict, card_record_ids: list[str]) -> list[dict]:
    rid = candidate.get("record_id", "")
    sid = _safe_id(rid)
    completed = _completed(candidate)
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
    elements.append(
        {
            "tag": "div",
            "fields": [
                _field("ASIN", candidate.get("asin")),
                _field("当前状态", candidate.get("current_status")),
                _field("建议履约", candidate.get("fulfillment")),
                _field("采购成本", proc._format_rmb(candidate.get("quote_cost"))),
                _field("包装尺寸", candidate.get("package_size") or "待核"),
                _field("重量", f"{candidate.get('weight_g')}g" if candidate.get("weight_g") else "待核"),
                _field("件数", candidate.get("set_count") or "待核"),
                _field("FBA配送费 / 佣金", f"{proc._format_eur(candidate.get('fba_fee_eur'))} / {proc._format_eur(candidate.get('commission_eur'))}"),
            ],
        }
    )
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**三渠道毛利**\n" + _margin_line(candidate)}})
    elements.append(
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    "**核查重点**\n"
                    f"- {_risk_hint(candidate)}\n"
                    f"- 套装内容/采购注意：{candidate.get('set_content') or '待按主图和供应商页核对'}"
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
    if completed:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**合规/适配已核查**\n"
                        f"{_line_item('合规结论', candidate.get('compliance_gate'))}\n"
                        f"{_line_item('IP/外观风险', candidate.get('ip_risk'))}\n"
                        f"{_line_item('说明', candidate.get('risk_note'))}\n"
                        f"{_line_item('下一步', candidate.get('next_action'))}"
                    ),
                },
            }
        )
        return elements
    elements.append(
        {
            "tag": "form",
            "name": f"fit_check_form_{sid}",
            "elements": [
                {
                    "tag": "select_static",
                    "name": f"fit_result_{sid}",
                    "placeholder": {"tag": "plain_text", "content": "选择结论：Go / 需整改 / No-Go"},
                    "options": [_button_option(x) for x in FIT_RESULTS],
                },
                {
                    "tag": "select_static",
                    "name": f"fit_iprisk_{sid}",
                    "placeholder": {"tag": "plain_text", "content": "选择IP/外观风险"},
                    "options": [_button_option(x) for x in IP_RISKS],
                },
                {
                    "tag": "input",
                    "name": f"fit_note_{sid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "核查备注"},
                    "placeholder": {"tag": "plain_text", "content": "型号适配证据、需整改点、GPSR/标签/包装缺口"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"fit_submit_{sid}",
                    "type": "primary",
                    "text": {"tag": "plain_text", "content": "确认核查本产品"},
                    "value": _payload(candidate, card_record_ids),
                },
            ],
        }
    )
    return elements


def build_fit_card(candidates: list[dict], batch_id: str = "") -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    total = len(candidates)
    done = sum(1 for item in candidates if _completed(item))
    pending = total - done
    template = "green" if total and pending == 0 else "yellow"
    title_status = "已全部核查" if total and pending == 0 else f"待核查 {pending}/{total}"
    record_ids = [c.get("record_id", "") for c in candidates if c.get("record_id")]
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    f"**状态**: {title_status}\n"
                    "**要求**: 逐个产品核查型号适配、兼容品牌词、IP/外观风险、GPSR/包装标签；提交只更新当前产品。"
                ),
            },
        },
        {"tag": "note", "elements": [{"tag": "plain_text", "content": "P0 默认只发 Frankie 样卡确认。通过合规/适配后才进入 50 件验证，不代表已正式上架。"}]},
    ]
    for candidate in candidates:
        elements.extend(_product_elements(candidate, record_ids))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": f"🟡 [AMZ·P0] 德国站合规/适配核查 · {title_status}"},
        },
        "elements": elements,
    }


def _card_text(value: Any) -> str:
    return proc._card_text(value)


def _card_nodes(value: Any):
    yield from proc._card_nodes(value)


def validate_fit_card(card: dict, candidates: list[dict]) -> list[str]:
    errors: list[str] = []
    nodes = list(_card_nodes(card))
    rendered = json.dumps(card, ensure_ascii=False)
    buttons = [n for n in nodes if n.get("tag") == "button"]
    forms = {n.get("name"): n for n in nodes if n.get("tag") == "form" and n.get("name")}

    def url_button_exists(label: str, expected_url: str) -> bool:
        for button in buttons:
            if _card_text(button.get("text")) != label:
                continue
            url = _text(button.get("url"))
            if url == expected_url and url.startswith(("http://", "https://")):
                return True
        return False

    for candidate in candidates:
        rid = candidate.get("record_id") or ""
        sid = _safe_id(rid)
        label = candidate.get("asin") or rid or "unknown"
        if candidate.get("amazon_url") and not url_button_exists("打开 Listing", candidate["amazon_url"]):
            errors.append(f"{label}: missing or invalid Amazon Listing button")
        if candidate.get("image_url") and not url_button_exists("查看主图原图", candidate["image_url"]):
            errors.append(f"{label}: missing or invalid image button")
        if not url_button_exists("打开候选表记录", _record_url(rid)):
            errors.append(f"{label}: missing or invalid candidate-record button")
        if candidate.get("supplier_link") and not url_button_exists("打开1688供应商", candidate["supplier_link"]):
            errors.append(f"{label}: missing or invalid supplier button")
        if _completed(candidate):
            continue
        form_name = f"fit_check_form_{sid}"
        form = forms.get(form_name)
        if not form:
            errors.append(f"{label}: missing form {form_name}")
            continue
        form_elements = form.get("elements") or []
        names = {x.get("name"): x.get("tag") for x in form_elements if isinstance(x, dict) and x.get("name")}
        expected = {
            f"fit_result_{sid}": "select_static",
            f"fit_iprisk_{sid}": "select_static",
            f"fit_note_{sid}": "input",
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
        if _text(value.get("action")) != ACTION_SUBMIT:
            errors.append(f"{label}: submit payload action is invalid")
        if _text(value.get("record_id")) != rid:
            errors.append(f"{label}: submit payload record_id is invalid")
        record_ids = [_text(x) for x in (value.get("card_record_ids") or []) if _text(x)]
        expected_ids = [c.get("record_id") for c in candidates if c.get("record_id")]
        if record_ids != expected_ids:
            errors.append(f"{label}: submit payload card_record_ids is invalid")
    for required in ("核查重点", "三渠道毛利", "GPSR", "提交只更新当前产品"):
        if required not in rendered:
            errors.append(f"card missing {required}")
    return errors


def _extract_action(event: dict) -> tuple[str, dict, dict]:
    return proc._extract_action(event)


def _merge_form_values(out: dict[str, str], raw: Any) -> None:
    raw = proc._jsonish(raw)
    if not raw:
        return
    if isinstance(raw, list):
        for item in raw:
            item = proc._jsonish(item)
            if isinstance(item, dict):
                name = _text(item.get("name") or item.get("key") or item.get("id") or item.get("field"))
                has_value = any(k in item for k in ("value", "input_value", "selected_value", "text", "content", "link", "url"))
                if name and has_value:
                    out[name] = proc._form_scalar(item)
                _merge_form_values(out, item)
        return
    if not isinstance(raw, dict):
        return
    wrapper_keys = {"form_value", "form_values", "card_form_value", "input_values", "inputs", "form", "fields", "elements"}
    field_prefixes = ("fit_", "amz_fit_")
    for key, value in raw.items():
        key_text = _text(key)
        value = proc._jsonish(value)
        if key_text in wrapper_keys:
            _merge_form_values(out, value)
            continue
        if key_text.startswith(field_prefixes) or key_text in ("result", "iprisk", "note"):
            out[key_text] = proc._form_scalar(value)
        if isinstance(value, (dict, list, str)):
            _merge_form_values(out, value)


def _extract_form_values(event: dict, action: dict | None = None) -> dict[str, str]:
    action = action or event.get("action") or {}
    out: dict[str, str] = {}
    for raw in (
        action.get("form_value"),
        action.get("form_values"),
        action.get("input_values"),
        action.get("inputs"),
        event.get("card_form_value"),
        event.get("form_value"),
        event.get("form_values"),
        event.get("input_values"),
        event.get("inputs"),
    ):
        _merge_form_values(out, raw)
    return out


def _form_value(form: dict, record_id: str, suffix: str) -> str:
    sid = _safe_id(record_id)
    keys = [f"fit_{suffix}_{sid}", f"amz_fit_{suffix}_{sid}", suffix]
    for key in keys:
        if key in form:
            return _text(form.get(key))
    for key, value in form.items():
        if key.startswith(f"fit_{suffix}_") or key.startswith(f"amz_fit_{suffix}_"):
            return _text(value)
    return ""


def _normalize_result(raw: str) -> str:
    text = _text(raw)
    aliases = {
        "通过": "Go",
        "go": "Go",
        "GO": "Go",
        "整改": "需整改",
        "暂缓": "需整改",
        "不通过": "No-Go",
        "淘汰": "No-Go",
        "no-go": "No-Go",
        "NO-GO": "No-Go",
    }
    return aliases.get(text, text)


def _normalize_risk(raw: str) -> str:
    text = _text(raw)
    aliases = {"低风险": "低", "中风险": "中", "高风险": "高", "禁做": "不可做", "不能做": "不可做"}
    return aliases.get(text, text)


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


def _callback_key(record_id: str, form: dict) -> str:
    text = json.dumps(form or {}, ensure_ascii=False, sort_keys=True)
    return f"{record_id}:{hash(text)}"


def _recent_seen(key: str, ttl_sec: int = 300) -> bool:
    now = time.time()
    for old, ts in list(_recent_callbacks.items()):
        if now - ts > ttl_sec:
            _recent_callbacks.pop(old, None)
    return key in _recent_callbacks and now - _recent_callbacks[key] <= ttl_sec


def _build_update_fields(result: str, risk: str, note: str, actor: str) -> dict:
    reviewed = f"{_now_label()} {actor}: 结论={result}; IP/外观风险={risk}; 备注={note or '-'}"
    if result == "Go" and risk in ("低", "中"):
        return {
            "合规闸结论": "Go",
            "IP/外观风险": risk,
            "侵权风险说明": note or "型号适配、兼容品牌词、包装标签待按样品复核；卡片人审先通过。",
            "当前状态": "待50件验证",
            "综合结论": "50件验证",
            "下一步动作": "发起50件验证",
            "数据缺口": [],
            "人审备注": reviewed,
        }
    if result == "No-Go" or risk == "不可做":
        return {
            "合规闸结论": "No-Go",
            "IP/外观风险": risk,
            "侵权风险说明": note or "合规/型号适配不通过。",
            "当前状态": "淘汰",
            "综合结论": "淘汰",
            "下一步动作": "淘汰归档",
            "数据缺口": ["认证"],
            "人审备注": reviewed,
        }
    return {
        "合规闸结论": "暂缓",
        "IP/外观风险": risk,
        "侵权风险说明": note or "需补型号适配、包装标签、GPSR或供应商实物证据后再判断。",
        "当前状态": "待合规核查",
        "综合结论": "暂缓",
        "下一步动作": "查合规/型号适配",
        "数据缺口": ["认证"],
        "人审备注": reviewed,
    }


async def _process_callback_background(event: dict, callback_key: str) -> None:
    try:
        result = await _process_callback(event)
        if ((result.get("toast") or {}).get("type") or "") == "error":
            _recent_callbacks.pop(callback_key, None)
    except Exception as exc:
        _recent_callbacks.pop(callback_key, None)
        print(f"[amz_compliance_fit.callback_bg] {callback_key} fail: {exc}")


async def _process_callback(event: dict) -> dict:
    action, value, _ = _extract_action(event)
    form = _extract_form_values(event, event.get("action") or {})
    if action != ACTION_SUBMIT:
        return _toast("未知合规核查动作", "error")
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    result = _normalize_result(_form_value(form, record_id, "result"))
    risk = _normalize_risk(_form_value(form, record_id, "iprisk"))
    note = _form_value(form, record_id, "note")
    if result not in FIT_RESULTS:
        return _toast("请选择合规/适配结论", "error")
    if risk not in IP_RISKS:
        return _toast("请选择IP/外观风险", "error")
    if (result != "Go" or risk in ("高", "不可做")) and not note:
        return _toast("需整改、No-Go或高风险时必须填写核查备注", "error")

    candidate = await _get_candidate(record_id)
    msg_id = _message_id(event) or candidate.get("fit_message_id")
    actor = _operator_label(event)
    fields = _build_update_fields(result, risk, note, actor)
    await _update_candidate(record_id, fields)
    candidate.update(
        {
            "compliance_gate": fields.get("合规闸结论"),
            "ip_risk": fields.get("IP/外观风险"),
            "risk_note": fields.get("侵权风险说明"),
            "current_status": fields.get("当前状态"),
            "overall_decision": fields.get("综合结论"),
            "next_action": fields.get("下一步动作"),
            "review_note": fields.get("人审备注"),
            "data_gaps": fields.get("数据缺口") or [],
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
            await amz_assistant.update_card(msg_id, build_fit_card(candidates, _text(value.get("batch_id"))))
        else:
            await amz_assistant.update_card(msg_id, build_fit_card([candidate], _text(value.get("batch_id"))))
    return _toast("本产品合规/适配核查已写回")


async def handle_callback(event: dict) -> dict:
    action, value, _ = _extract_action(event)
    form = _extract_form_values(event, event.get("action") or {})
    if action != ACTION_SUBMIT:
        return {"ok": False, "ignored": True, "action": action}
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    result = _normalize_result(_form_value(form, record_id, "result"))
    risk = _normalize_risk(_form_value(form, record_id, "iprisk"))
    note = _form_value(form, record_id, "note")
    if result not in FIT_RESULTS:
        return _toast("请选择合规/适配结论", "error")
    if risk not in IP_RISKS:
        return _toast("请选择IP/外观风险", "error")
    if (result != "Go" or risk in ("高", "不可做")) and not note:
        return _toast("需整改、No-Go或高风险时必须填写核查备注", "error")
    callback_key = _callback_key(record_id, form)
    if _recent_seen(callback_key):
        try:
            current = await _get_candidate(record_id)
            if _completed(current):
                return _toast("该产品已核查，无需重复点击")
        except Exception as exc:
            print(f"[amz_compliance_fit.callback_duplicate_check] {record_id} fail: {exc}")
        _recent_callbacks.pop(callback_key, None)
        _recent_callbacks[callback_key] = time.time()
        _spawn(_process_callback_background(event, callback_key))
        return _toast("已重新收到本产品核查结果，正在补写候选表并更新原卡")
    _recent_callbacks[callback_key] = time.time()
    _spawn(_process_callback_background(event, callback_key))
    return _toast("已收到本产品核查结果，正在写回候选表并更新原卡")


async def send_fit_card(
    *,
    mode: str = "dry_run",
    limit: int = 2,
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
    card = build_fit_card(candidates, batch)
    validation_errors = validate_fit_card(card, candidates)
    if validation_errors:
        raise RuntimeError("Compliance fit card self-test failed: " + "; ".join(validation_errors))
    effective_frankie_only = bool(frankie_only or FRANKIE_ONLY)
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "batch_id": batch,
        "count": len(candidates),
        "record_ids": [c.get("record_id") for c in candidates],
        "card_selftest": "passed",
        **proc._card_media_stats(candidates),
    }
    if mode == "dry_run":
        result["card"] = card
        return result
    if not candidates:
        result["sent"] = False
        result["message_id"] = ""
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
            raise RuntimeError("Compliance gray recipients are not configured. Set AMZ_COMPLIANCE_GRAY_UNION_IDS or AMZ_COMPLIANCE_GRAY_CHAT_IDS.")
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
