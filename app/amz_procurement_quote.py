# -*- coding: utf-8 -*-
"""Amazon Europe procurement quote cards.

P0 scope:
- send a Frankie-only card for selected candidate records;
- each product has its own cost/link inputs and submit button;
- callback writes only that product row and patches the original card.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from . import amz_assistant, feishu


BJ = timezone(timedelta(hours=8))

ACTION_SUBMIT = "amz_proc_quote_submit"

CANDIDATE_APP_TOKEN = os.environ.get("AMZ_PROCUREMENT_CANDIDATE_APP_TOKEN", "UvNcbvWufaPMSvseOogcBhbFn1y")
CANDIDATE_TABLE_ID = os.environ.get("AMZ_PROCUREMENT_CANDIDATE_TABLE_ID", "tblrIPsxm3E8ZCXn")
FEISHU_API_WHICH = os.environ.get("AMZ_PROCUREMENT_FEISHU_API_WHICH", "notify")
FRANKIE_ONLY = (os.environ.get("AMZ_PROCUREMENT_CARD_FRANKIE_ONLY", "1") or "1") != "0"
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", amz_assistant.FRANKIE_UNION_ID)
DEFAULT_BATCH_ID = os.environ.get("AMZ_PROCUREMENT_DEFAULT_BATCH_ID", "AMZ-DE-PROCQ-20260723-P0")
GRAY_UNION_IDS = [x.strip() for x in os.environ.get("AMZ_PROCUREMENT_GRAY_UNION_IDS", "").split(",") if x.strip()]
GRAY_CHAT_IDS = [x.strip() for x in os.environ.get("AMZ_PROCUREMENT_GRAY_CHAT_IDS", "").split(",") if x.strip()]

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
    "采购回填状态",
    "采购回填人",
    "采购回填时间",
    "采购备注",
    "采购卡片批次ID",
    "采购卡片消息ID",
    "三方案推荐履约",
    "C-采购前可用毛利RMB",
    "C-采购前毛利率%",
    "C-物流成本RMB",
    "C-货运比",
    "财务闸结论",
    "下一步动作",
]

_bg_tasks: set[asyncio.Task] = set()
_recent_callbacks: dict[str, float] = {}
_image_key_cache: dict[str, str] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_label() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value).strip()
    if isinstance(value, dict):
        return _text(value.get("text") or value.get("link") or value.get("url") or value.get("name") or "")
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(_text(item.get("text") or item.get("link") or item.get("url") or item.get("name") or ""))
            else:
                parts.append(_text(item))
        return "".join(parts).strip()
    return str(value).strip()


def _url(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("link") or value.get("url") or value.get("text"))
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return _text(value[0].get("link") or value[0].get("url") or value[0].get("text"))
    return _text(value)


def _list_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_text(v) for v in value if _text(v)]
    text = _text(value)
    if not text:
        return []
    if text.startswith("["):
        try:
            return _list_values(json.loads(text))
        except Exception:
            pass
    return [x.strip() for x in re.split(r"[,，/、\n]+", text) if x.strip()]


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value or "")[:48] or "row"


def _short(value: Any, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", _text(value))
    return text if len(text) <= limit else text[: limit - 1] + "..."


def _field(label: str, value: Any) -> dict:
    return {"is_short": True, "text": {"tag": "lark_md", "content": f"**{label}**\n{_text(value) or '-'}"}}


def _record_url(record_id: str) -> str:
    return f"https://u1wpma3xuhr.feishu.cn/base/{CANDIDATE_APP_TOKEN}?table={CANDIDATE_TABLE_ID}&record={record_id}"


def _candidate_from_record(record: dict) -> dict:
    fields = record.get("fields") or {}
    record_id = record.get("record_id") or ""
    return {
        "record_id": record_id,
        "asin": _text(fields.get("ASIN")),
        "title": _text(fields.get("候选标题")),
        "cn_name": _text(fields.get("产品中文名")) or _text(fields.get("采购关键词")) or _text(fields.get("候选标题")),
        "amazon_url": _url(fields.get("Amazon链接")),
        "image_url": _url(fields.get("样本ASIN主图URL")),
        "package_size": _text(fields.get("包装尺寸")),
        "weight_g": _text(fields.get("商品重量g")),
        "set_count": _text(fields.get("套装件数")),
        "set_content": _text(fields.get("套装内容")),
        "quote_cost": fields.get("采购成本RMB"),
        "supplier_link": _url(fields.get("1688供应商链接")) or _url(fields.get("采购链接")),
        "quote_status": _text(fields.get("采购回填状态")) or "待回填",
        "quote_user": _text(fields.get("采购回填人")),
        "quote_time": _text(fields.get("采购回填时间")),
        "quote_note": _text(fields.get("采购备注")),
        "batch_id": _text(fields.get("采购卡片批次ID")),
        "message_id": _text(fields.get("采购卡片消息ID")),
        "fulfillment": " / ".join(_list_values(fields.get("三方案推荐履约"))) or "-",
        "pre_margin_rmb": _text(fields.get("C-采购前可用毛利RMB")),
        "pre_margin_rate": _text(fields.get("C-采购前毛利率%")),
        "logistics_rmb": _text(fields.get("C-物流成本RMB")),
        "freight_ratio": _text(fields.get("C-货运比")),
        "finance_gate": " / ".join(_list_values(fields.get("财务闸结论"))) or "-",
        "next_action": " / ".join(_list_values(fields.get("下一步动作"))) or "-",
    }


def _payload(candidate: dict, card_record_ids: list[str]) -> dict:
    return {
        "source": "amz_procurement_quote",
        "action": ACTION_SUBMIT,
        "record_id": candidate.get("record_id"),
        "asin": candidate.get("asin"),
        "batch_id": candidate.get("batch_id") or DEFAULT_BATCH_ID,
        "card_record_ids": card_record_ids,
    }


def _url_button(text: str, url: str, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "url": url}


def _md_link(label: str, url: str) -> str:
    safe = _text(url)
    return f"[{label}]({safe})" if safe.startswith(("http://", "https://")) else label


def _guess_ext(content_type: str, url: str) -> str:
    ctype = (content_type or "").lower()
    if "png" in ctype:
        return ".png"
    if "webp" in ctype:
        return ".webp"
    if "gif" in ctype:
        return ".gif"
    if "jpeg" in ctype or "jpg" in ctype:
        return ".jpg"
    m = re.search(r"\.(jpg|jpeg|png|webp|gif)(?:[?#]|$)", url, re.I)
    return f".{m.group(1).lower()}" if m else ".jpg"


async def _download_image(image_url: str) -> tuple[bytes, str]:
    url = _text(image_url)
    if not url.startswith(("http://", "https://")):
        return b"", ""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    }
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True, headers=headers) as client:
        resp = await client.get(url)
    if resp.status_code >= 400:
        raise RuntimeError(f"image fetch HTTP {resp.status_code}")
    content_type = (resp.headers.get("content-type") or "image/jpeg").split(";")[0].strip()
    data = resp.content or b""
    if len(data) < 100:
        raise RuntimeError("image fetch returned too little data")
    if len(data) > 10 * 1024 * 1024:
        raise RuntimeError("image is larger than 10MB")
    return data, content_type


async def _image_key_for_url(image_url: str, asin: str = "") -> str:
    url = _text(image_url)
    if not url:
        return ""
    if url in _image_key_cache:
        return _image_key_cache[url]
    try:
        data, content_type = await _download_image(url)
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
        filename = f"amz_{_safe_id(asin) or digest}{_guess_ext(content_type, url)}"
        image_key = await amz_assistant.upload_image_for_card(data, filename, content_type)
        if image_key:
            _image_key_cache[url] = image_key
        return image_key
    except Exception as exc:
        print(f"[amz_procurement_quote.image] upload skipped asin={asin} err={exc}")
        return ""


async def _prepare_card_images(candidates: list[dict]) -> None:
    for candidate in candidates:
        if candidate.get("image_key") or not candidate.get("image_url"):
            continue
        candidate["image_key"] = await _image_key_for_url(candidate.get("image_url"), candidate.get("asin"))


def _card_media_stats(candidates: list[dict]) -> dict[str, int]:
    return {
        "image_url_count": sum(1 for c in candidates if c.get("image_url")),
        "image_embedded_count": sum(1 for c in candidates if c.get("image_key")),
        "listing_url_count": sum(1 for c in candidates if c.get("amazon_url")),
    }


def _product_elements(candidate: dict, card_record_ids: list[str]) -> list[dict]:
    rid = candidate.get("record_id", "")
    sid = _safe_id(rid)
    completed = _text(candidate.get("quote_status")) == "已回填" and candidate.get("quote_cost")
    title = candidate.get("cn_name") or candidate.get("title") or candidate.get("asin") or rid
    amazon = candidate.get("amazon_url")
    image = candidate.get("image_url")
    fields = [
        _field("ASIN", candidate.get("asin")),
        _field("建议履约", candidate.get("fulfillment")),
        _field("包装尺寸", candidate.get("package_size") or "待核"),
        _field("重量", f"{candidate.get('weight_g')}g" if candidate.get("weight_g") else "待核"),
        _field("件数", candidate.get("set_count") or "待核"),
        _field("采购前空间", f"{candidate.get('pre_margin_rmb') or '-'} RMB / {candidate.get('pre_margin_rate') or '-'}%"),
        _field("物流成本", f"{candidate.get('logistics_rmb') or '-'} RMB"),
        _field("货运比", candidate.get("freight_ratio") or "-"),
    ]
    elements: list[dict] = [
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**{title}**\n{_short(candidate.get('title'), 180)}"}},
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
    reference_links = []
    if amazon:
        reference_links.append(_md_link("打开 Amazon Listing", amazon))
    if image:
        reference_links.append(_md_link("查看主图原图", image))
    if reference_links:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**采购参考**\n" + " ｜ ".join(reference_links)}})
    elements.extend(
        [
            {"tag": "div", "fields": fields},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**套装内容/采购注意**\n{candidate.get('set_content') or '待采购按主图和供应商页核对'}"}},
        ]
    )
    actions = []
    if amazon:
        actions.append(_url_button("打开 Listing", amazon, "primary"))
    if image:
        actions.append(_url_button("查看主图原图", image))
    actions.append(_url_button("打开候选表记录", _record_url(rid)))
    elements.append({"tag": "action", "actions": actions})
    if completed:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**采购已回填**\n"
                        f"采购成本: {candidate.get('quote_cost')} RMB\n"
                        f"供应商链接: {candidate.get('supplier_link') or '-'}\n"
                        f"回填人: {candidate.get('quote_user') or '-'}  ·  回填时间: {candidate.get('quote_time') or '-'}"
                    ),
                },
            }
        )
        return elements
    elements.append(
        {
            "tag": "form",
            "name": f"proc_quote_form_{sid}",
            "elements": [
                {
                    "tag": "input",
                    "name": f"proc_cost_{sid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "采购成本RMB"},
                    "placeholder": {"tag": "plain_text", "content": "只填数字，例如 18.5"},
                },
                {
                    "tag": "input",
                    "name": f"proc_link_{sid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "1688供应商链接"},
                    "placeholder": {"tag": "plain_text", "content": "https://detail.1688.com/..."},
                },
                {
                    "tag": "input",
                    "name": f"proc_note_{sid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "备注"},
                    "placeholder": {"tag": "plain_text", "content": "MOQ、颜色、套装差异、报价口径"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"proc_submit_{sid}",
                    "type": "primary",
                    "text": {"tag": "plain_text", "content": "确认回填本产品"},
                    "value": _payload(candidate, card_record_ids),
                },
            ],
        }
    )
    return elements


def build_quote_card(candidates: list[dict], batch_id: str = "") -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    total = len(candidates)
    done = sum(1 for item in candidates if _text(item.get("quote_status")) == "已回填" and item.get("quote_cost"))
    pending = total - done
    template = "green" if total and pending == 0 else "yellow"
    title_status = "已全部回填" if total and pending == 0 else f"待采购回填 {pending}/{total}"
    record_ids = [c.get("record_id", "") for c in candidates if c.get("record_id")]
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    f"**状态**: {title_status}\n"
                    "**要求**: 每个产品单独填写采购成本和1688供应商链接；提交只更新当前产品，不影响同卡其他产品。"
                ),
            },
        },
        {"tag": "note", "elements": [{"tag": "plain_text", "content": "P0 灰测默认只发 Frankie。采购成本不由系统猜，必须由采购确认。"}]},
    ]
    for candidate in candidates:
        elements.extend(_product_elements(candidate, record_ids))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": f"🟡 [AMZ·P0] 德国站采购成本回填 · {title_status}"},
        },
        "elements": elements,
    }


def build_processed_card(candidate: dict, result: str, template: str = "green") -> dict:
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": template, "title": {"tag": "plain_text", "content": "✅ [AMZ·P0] 采购成本已回填"}},
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**产品**: {candidate.get('cn_name') or candidate.get('title') or candidate.get('asin')}\n"
                        f"**ASIN**: {candidate.get('asin') or '-'}\n"
                        f"**结果**: {result}\n\n"
                        "_此产品已处理；同卡其他产品仍可继续单独回填。_"
                    ),
                },
            }
        ],
    }


def _path(record_id: str = "") -> str:
    base = f"/bitable/v1/apps/{CANDIDATE_APP_TOKEN}/tables/{CANDIDATE_TABLE_ID}/records"
    return f"{base}/{record_id}" if record_id else base


async def _get_candidate(record_id: str) -> dict:
    data = await feishu.api("GET", _path(record_id), which=FEISHU_API_WHICH)
    record = ((data.get("data") or {}).get("record") or {})
    return _candidate_from_record(record)


async def _update_candidate(record_id: str, fields: dict) -> None:
    await feishu.api("PUT", _path(record_id), {"fields": fields}, which=FEISHU_API_WHICH)


async def _search_candidates(batch_id: str = "", limit: int = 4) -> list[dict]:
    conditions = [
        {"field_name": "采购回填状态", "operator": "is", "value": ["待回填"]},
        {"field_name": "下一步动作", "operator": "contains", "value": ["查1688采购"]},
    ]
    if batch_id:
        conditions.append({"field_name": "采购卡片批次ID", "operator": "is", "value": [batch_id]})
    body = {
        "page_size": min(max(int(limit or 4), 1), 20),
        "field_names": FIELD_NAMES,
        "filter": {"conjunction": "and", "conditions": conditions},
    }
    data = await feishu.api("POST", _path() + "/search", body, which=FEISHU_API_WHICH)
    rows = ((data.get("data") or {}).get("items") or [])
    return [_candidate_from_record(row) for row in rows]


async def _get_candidates_by_ids(record_ids: list[str]) -> list[dict]:
    out = []
    for rid in record_ids:
        if rid:
            out.append(await _get_candidate(rid))
    return out


def _extract_action(event: dict) -> tuple[str, dict, dict]:
    action = event.get("action") or {}
    value = action.get("value") or event.get("value") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {"action": value}
    form = action.get("form_value") or event.get("card_form_value") or {}
    if isinstance(form, str):
        try:
            form = json.loads(form)
        except Exception:
            form = {}
    return _text(value.get("action") or value.get("act")), value, form


def _form_value(form: dict, record_id: str, suffix: str) -> str:
    sid = _safe_id(record_id)
    keys = [f"proc_{suffix}_{sid}", f"amz_proc_{suffix}_{sid}", suffix]
    for key in keys:
        if key in form:
            return _text(form.get(key))
    for key, value in form.items():
        if key.startswith(f"proc_{suffix}_") or key.startswith(f"amz_proc_{suffix}_"):
            return _text(value)
    return ""


def _parse_cost(raw: str) -> float | None:
    text = _text(raw).replace("￥", "").replace("¥", "").replace("元", "").strip()
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    value = float(match.group(0))
    return value if value > 0 else None


def _message_id(event: dict) -> str:
    for value in (
        event.get("message_id"),
        event.get("open_message_id"),
        event.get("card_open_message_id"),
        (event.get("message") or {}).get("message_id"),
        (event.get("context") or {}).get("open_message_id"),
        (event.get("context") or {}).get("message_id"),
    ):
        if value:
            return _text(value)
    return ""


def _operator_label(event: dict) -> str:
    op = event.get("operator") or event.get("operator_user") or event.get("user") or {}
    return (
        _text(op.get("name"))
        or _text(op.get("union_id"))
        or _text(op.get("open_id"))
        or _text(event.get("operator_open_id"))
        or "采购回填"
    )[:120]


def _toast(content: str, typ: str = "success") -> dict:
    return {"toast": {"type": typ, "content": content}}


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


async def _process_callback_background(event: dict, callback_key: str) -> None:
    try:
        result = await _process_callback(event)
        if ((result.get("toast") or {}).get("type") or "") == "error":
            _recent_callbacks.pop(callback_key, None)
    except Exception as exc:
        _recent_callbacks.pop(callback_key, None)
        print(f"[amz_procurement_quote.callback_bg] {callback_key} fail: {exc}")


async def _process_callback(event: dict) -> dict:
    action, value, form = _extract_action(event)
    if action != ACTION_SUBMIT:
        return _toast("未知采购回填动作", "error")
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    cost = _parse_cost(_form_value(form, record_id, "cost"))
    supplier_link = _form_value(form, record_id, "link")
    note = _form_value(form, record_id, "note")
    if cost is None:
        return _toast("采购成本必须填写为大于0的数字", "error")
    if not supplier_link.startswith(("http://", "https://")):
        return _toast("请填写可打开的1688供应商链接", "error")

    candidate = await _get_candidate(record_id)
    msg_id = _message_id(event) or candidate.get("message_id")
    actor = _operator_label(event)
    await _update_candidate(
        record_id,
        {
            "采购成本RMB": round(cost, 2),
            "1688供应商链接": supplier_link,
            "采购链接": supplier_link,
            "采购回填状态": "已回填",
            "采购回填人": actor,
            "采购回填时间": _now_ms(),
            "采购备注": note or f"采购卡片回填于 {_now_label()}",
        },
    )
    candidate.update(
        {
            "quote_cost": round(cost, 2),
            "supplier_link": supplier_link,
            "quote_status": "已回填",
            "quote_user": actor,
            "quote_time": _now_label(),
            "quote_note": note,
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
            await amz_assistant.update_card(msg_id, build_quote_card(candidates, _text(value.get("batch_id"))))
        else:
            await amz_assistant.update_card(
                msg_id,
                build_processed_card(candidate, f"采购成本 {round(cost, 2)} RMB，供应商链接已登记。"),
            )
    return _toast("本产品采购成本已回填")


async def handle_callback(event: dict) -> dict:
    action, value, form = _extract_action(event)
    if action != ACTION_SUBMIT:
        return {"ok": False, "ignored": True, "action": action}
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    if _parse_cost(_form_value(form, record_id, "cost")) is None:
        return _toast("采购成本必须填写为大于0的数字", "error")
    if not _form_value(form, record_id, "link").startswith(("http://", "https://")):
        return _toast("请填写可打开的1688供应商链接", "error")
    callback_key = _callback_key(record_id, form)
    if _recent_seen(callback_key):
        return _toast("该产品回填已收到，正在处理或已处理，无需重复点击")
    _recent_callbacks[callback_key] = time.time()
    _spawn(_process_callback_background(event, callback_key))
    return _toast("已收到本产品采购成本，正在写回候选表并更新原卡")


async def send_quote_card(
    *,
    mode: str = "dry_run",
    limit: int = 4,
    batch_id: str = "",
    record_ids: list[str] | None = None,
    frankie_only: bool = True,
    gray_union_ids: list[str] | None = None,
    gray_chat_ids: list[str] | None = None,
) -> dict:
    if mode not in ("dry_run", "commit"):
        raise ValueError("mode must be dry_run or commit")
    if not CANDIDATE_APP_TOKEN or not CANDIDATE_TABLE_ID:
        raise RuntimeError("AMZ_PROCUREMENT_CANDIDATE_APP_TOKEN/TABLE_ID not configured")
    batch = batch_id or DEFAULT_BATCH_ID
    candidates = await _get_candidates_by_ids(record_ids or []) if record_ids else await _search_candidates(batch, limit=limit)
    if mode == "commit":
        await _prepare_card_images(candidates)
    card = build_quote_card(candidates, batch)
    effective_frankie_only = bool(frankie_only or FRANKIE_ONLY)
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "batch_id": batch,
        "count": len(candidates),
        "record_ids": [c.get("record_id") for c in candidates],
        **_card_media_stats(candidates),
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
            raise RuntimeError("Procurement gray recipients are not configured. Set AMZ_PROCUREMENT_GRAY_UNION_IDS or AMZ_PROCUREMENT_GRAY_CHAT_IDS.")
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
    if message_ids:
        msg_text = ",".join(message_ids)
        for candidate in candidates:
            await _update_candidate(candidate["record_id"], {"采购卡片消息ID": msg_text})
    return result
