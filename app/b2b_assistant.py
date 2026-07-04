"""B2B Assistant event handlers.

The Feishu app "外贸助手" receives messages/card callbacks in n8n. n8n only
normalizes the event envelope; this module owns the deterministic business
write-back logic for B2B customers and LinkedIn lead receipts.
"""
import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse

from . import feishu

BJ = timezone(timedelta(hours=8))

B2B_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_CUSTOMER_TABLE = os.environ.get("B2B_CUSTOMER_TABLE", "tbl2OoqVb7Uf1pWd")
B2B_FOLLOWUP_TABLE = os.environ.get("B2B_FOLLOWUP_TABLE", "tblbS5sGAb72OjL4")
B2B_LINKEDIN_TABLE = os.environ.get("B2B_LINKEDIN_TABLE", "tblN8XszEatuTJgP")
B2B_LINKEDIN_VIEW = os.environ.get("B2B_LINKEDIN_VIEW", "vew9f7zQ7s")

CUSTOMER_STATUS_OPTIONS = {
    "未联系", "已发开发邮件", "初步建联", "样品评估", "视频会议",
    "条款谈判", "首单执行", "活跃合作", "不合适",
}
COMPANY_TYPE_OPTIONS = {
    "贸易商", "分销商", "品牌商", "批发商", "混合型", "游戏IP",
    "电商卖家", "电商平台", "行业协会", "零售商", "待判断",
}
CHANNEL_OPTIONS = {"线下连锁", "独立店", "本地电商", "海外众筹", "商超", "EBAY", "虾皮", "Amazon", "分销"}
CRM_SOURCE_OPTIONS = {"领英", "Google", "Snovio", "apollo", "开发平台", "独立站", "对方主动联系", "展会", "其他"}
CONTACT_METHOD_OPTIONS = {"邮箱", "WhatsApp", "微信", "电话", "网站填写开发信", "Zalo"}
FOLLOWUP_METHOD_OPTIONS = {"邮件", "WhatsApp", "电话", "LinkedIn", "微信", "面谈", "视频会议", "zalo"}


def _now_ms() -> int:
    return int(datetime.now(BJ).timestamp() * 1000)


def _today() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d")


def _now_text() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M")


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "link", "name", "value"):
            if value.get(key) is not None:
                return _text(value.get(key))
        return " ".join(_text(v) for v in value.values()).strip()
    if isinstance(value, list):
        return " ".join(_text(v) for v in value).strip()
    return str(value).strip()


def _normalize_key(value: str) -> str:
    return re.sub(r"[\W_]+", "", (value or "").lower(), flags=re.UNICODE)


def _record_url(table_id: str, record_id: str, view_id: str = "") -> str:
    url = f"https://u1wpma3xuhr.feishu.cn/base/{B2B_APP_TOKEN}?table={table_id}&record={record_id}"
    if view_id:
        url = f"https://u1wpma3xuhr.feishu.cn/base/{B2B_APP_TOKEN}?table={table_id}&view={view_id}&record={record_id}"
    return url


def _normalize_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if "@" in value and not value.lower().startswith("http"):
        return ""
    if not re.match(r"^https?://", value, flags=re.I):
        value = "https://" + value
    try:
        parsed = urlparse(value)
        if "." not in parsed.netloc:
            return ""
        return value
    except Exception:
        return ""


def _link(value: str, text: str = "") -> dict | None:
    url = _normalize_url(value)
    if not url:
        return None
    return {"link": url, "text": text or url}


def _split_options(value: str, allowed: set[str]) -> list[str]:
    raw = _text(value)
    if not raw:
        return []
    parts = re.split(r"[,;；、/|]+", raw)
    out = []
    for part in parts:
        part = part.strip()
        if part in allowed and part not in out:
            out.append(part)
    return out


def _pick_option(value: str, allowed: set[str]) -> str:
    value = _text(value)
    return value if value in allowed else ""


def _grade(value: str) -> str:
    value = _text(value)
    mapping = {
        "A": "🔴A-热线索",
        "a": "🔴A-热线索",
        "🔴A-热线索": "🔴A-热线索",
        "B": "🟡B-温线索",
        "b": "🟡B-温线索",
        "🟡B-温线索": "🟡B-温线索",
        "C": "🔵C-冷线索",
        "c": "🔵C-冷线索",
        "🔵C-冷线索": "🔵C-冷线索",
    }
    return mapping.get(value, "")


def _source(value: str, exhibition: str = "") -> str:
    raw = _text(value)
    if raw.lower() in {"linkedin", "linkedIn".lower()} or raw == "领英":
        return "领英"
    if raw.lower() in {"snov", "snovio", "snov.io"}:
        return "Snovio"
    if raw in CRM_SOURCE_OPTIONS:
        return raw
    return "展会" if exhibition else "其他"


def _contact_method(fields: dict) -> str:
    if _text(fields.get("email")):
        return "邮箱"
    if _text(fields.get("whatsapp")):
        return "WhatsApp"
    if _text(fields.get("wechat")):
        return "微信"
    if _text(fields.get("phone")):
        return "电话"
    if _text(fields.get("website")):
        return "网站填写开发信"
    return ""


def _extract_fields_from_text(text: str) -> dict:
    text = text or ""
    out = {}
    patterns = [
        ("展会", "exhibition"), ("意向产品", "products"), ("感兴趣产品", "products"),
        ("客户等级", "grade"), ("沟通记录", "notes"), ("备注", "notes"),
        ("采集人", "collector"), ("公司名称", "company"), ("公司", "company"),
        ("客户", "customer"), ("联系人", "contact"), ("姓名", "contact"),
        ("职位", "title"), ("职务", "title"), ("邮箱", "email"), ("Email", "email"),
        ("电话", "phone"), ("手机", "phone"), ("WhatsApp", "whatsapp"),
        ("微信", "wechat"), ("国家/地区", "country"), ("国家", "country"),
        ("官网", "website"), ("网站", "website"), ("LinkedIn", "linkedin"),
        ("领英", "linkedin"), ("预估采购量", "quantity"), ("公司类型", "company_type"),
        ("代理竞品", "competitors"), ("主营类目", "categories"), ("主营", "categories"),
        ("主力渠道", "channels"), ("渠道", "channels"), ("客户来源", "source"),
        ("来源", "source"), ("跟进方式", "method"), ("方式", "method"),
        ("跟进内容", "content"), ("内容", "content"), ("客户反馈", "feedback"),
        ("反馈", "feedback"), ("下一步行动", "next_action"), ("下一步", "next_action"),
        ("跟进人", "owner"), ("合作状态", "status"), ("record_id", "record_id"),
        ("记录ID", "record_id"), ("线索ID", "lead_record_id"), ("动作", "receipt_action"),
        ("回执", "receipt_note"), ("原因", "receipt_note"),
    ]
    for label, field in patterns:
        escaped = re.escape(label)
        match = re.search(r"(?:^|\n)\s*" + escaped + r"[：:]\s*(.+)", text, flags=re.I)
        if match:
            out[field] = (match.group(1) or "").strip()
    return out


def _merge_fields(payload: dict) -> dict:
    parsed = _extract_fields_from_text(_text(payload.get("text")))
    fields = payload.get("fields") or {}
    merged = {**parsed, **{k: v for k, v in fields.items() if v not in (None, "")}}
    return merged


def _after_command(text: str, command: str) -> str:
    text = _text(text)
    if not text:
        return ""
    idx = text.lower().find(command.lower())
    if idx < 0:
        return ""
    rest = text[idx + len(command):].strip()
    return rest.splitlines()[0].strip() if rest else ""


async def _list_records(table_id: str, *, field_names: list[str] | None = None) -> list[dict]:
    items = []
    page_token = ""
    encoded_fields = ""
    if field_names:
        encoded_fields = "&field_names=" + quote(json.dumps(field_names, ensure_ascii=False), safe="")
    while True:
        path = f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records?page_size=500{encoded_fields}"
        if page_token:
            path += "&page_token=" + quote(page_token, safe="")
        resp = await feishu.api("GET", path, which="bitable")
        data = resp.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token") or ""
        if not page_token:
            break
    return items


async def _get_record(table_id: str, record_id: str) -> dict:
    resp = await feishu.api(
        "GET",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records/{record_id}",
        which="bitable",
    )
    return (resp.get("data") or {}).get("record") or {}


async def _create_record(table_id: str, fields: dict) -> str:
    resp = await feishu.api(
        "POST",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records",
        {"fields": fields},
        which="bitable",
    )
    return (((resp.get("data") or {}).get("record") or {}).get("record_id") or "")


async def _update_record(table_id: str, record_id: str, fields: dict) -> None:
    await feishu.api(
        "PUT",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records/{record_id}",
        {"fields": fields},
        which="bitable",
    )


async def _operator_name(open_id: str) -> str:
    if not open_id:
        return ""
    try:
        resp = await feishu.api("GET", f"/contact/v3/users/{open_id}?user_id_type=open_id", which="b2b_assistant")
        return _text(((resp.get("data") or {}).get("user") or {}).get("name"))
    except Exception as exc:
        print(f"[b2b_assistant] resolve operator failed: {type(exc).__name__}: {str(exc)[:120]}")
        return ""


async def _send_reply(payload: dict, text: str) -> dict:
    chat_id = _text(payload.get("chat_id"))
    sender_open_id = _text(payload.get("sender_open_id"))
    if chat_id:
        receive_type, receive_id = "chat_id", chat_id
    elif sender_open_id:
        receive_type, receive_id = "open_id", sender_open_id
    else:
        return {"sent": False, "error": "missing chat_id/sender_open_id"}
    body = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    try:
        resp = await feishu.api("POST", f"/im/v1/messages?receive_id_type={receive_type}", body, which="b2b_assistant")
        return {"sent": (resp.get("code", 0) == 0), "message_id": ((resp.get("data") or {}).get("message_id") or "")}
    except Exception as exc:
        return {"sent": False, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}


def _customer_summary(rec: dict) -> str:
    fields = rec.get("fields") or {}
    company = _text(fields.get("公司名称")) or "(未命名)"
    country = _text(fields.get("国家/地区")) or "-"
    status = _text(fields.get("合作状态")) or "-"
    contact = _text(fields.get("核心联系人")) or "-"
    email = _text(fields.get("邮箱")) or "-"
    grade = _text(fields.get("客户等级")) or "-"
    return f"{company}｜{country}｜{status}｜{grade}｜{contact}｜{email}"


async def _find_customers(keyword: str) -> list[dict]:
    keyword = _text(keyword)
    if not keyword:
        return []
    key = _normalize_key(keyword)
    rows = await _list_records(
        B2B_CUSTOMER_TABLE,
        field_names=["公司名称", "核心联系人", "国家/地区", "合作状态", "客户等级", "邮箱", "电话", "LinkedIn", "公司官网", "主营类目"],
    )
    out = []
    for rec in rows:
        fields = rec.get("fields") or {}
        haystack = " ".join(
            _text(fields.get(name))
            for name in ["公司名称", "核心联系人", "国家/地区", "合作状态", "客户等级", "邮箱", "电话", "LinkedIn", "公司官网", "主营类目"]
        )
        if keyword.lower() in haystack.lower() or (key and key in _normalize_key(haystack)):
            out.append(rec)
    return out


async def _find_linkedin_leads(keyword: str) -> list[dict]:
    keyword = _text(keyword)
    if not keyword:
        return []
    key = _normalize_key(keyword)
    rows = await _list_records(
        B2B_LINKEDIN_TABLE,
        field_names=["线索名称", "公司名称", "联系人姓名", "职位", "开发状态", "LinkedIn公司页", "LinkedIn联系人页", "邮箱"],
    )
    out = []
    for rec in rows:
        fields = rec.get("fields") or {}
        haystack = " ".join(
            _text(fields.get(name))
            for name in ["线索名称", "公司名称", "联系人姓名", "职位", "LinkedIn公司页", "LinkedIn联系人页", "邮箱"]
        )
        if keyword.lower() in haystack.lower() or (key and key in _normalize_key(haystack)):
            out.append(rec)
    return out


async def _handle_customer_query(payload: dict) -> dict:
    fields = _merge_fields(payload)
    keyword = _text(fields.get("customer") or fields.get("company") or fields.get("query"))
    if not keyword:
        keyword = _after_command(_text(payload.get("text")), "#客户查询")
    if not keyword:
        reply = "客户查询需要关键词，例如：#客户查询 ABC Trading / #客户查询 德国"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    matches = await _find_customers(keyword)
    if not matches:
        reply = f"未找到匹配客户：{keyword}"
        return {"ok": True, "matches": 0, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    lines = [f"客户查询结果：{keyword}，共 {len(matches)} 条，显示前 8 条"]
    for idx, rec in enumerate(matches[:8], 1):
        lines.append(f"{idx}. {_customer_summary(rec)}\n{_record_url(B2B_CUSTOMER_TABLE, rec.get('record_id') or '')}")
    reply = "\n".join(lines)
    return {"ok": True, "matches": len(matches), "reply": reply, "reply_result": await _send_reply(payload, reply)}


async def _handle_customer_stats(payload: dict) -> dict:
    rows = await _list_records(B2B_CUSTOMER_TABLE, field_names=["公司名称", "国家/地区", "合作状态", "客户等级", "客户来源"])
    status = Counter()
    country = Counter()
    grade = Counter()
    source = Counter()
    for rec in rows:
        fields = rec.get("fields") or {}
        status[_text(fields.get("合作状态")) or "空"] += 1
        country[_text(fields.get("国家/地区")) or "空"] += 1
        grade[_text(fields.get("客户等级")) or "空"] += 1
        source[_text(fields.get("客户来源")) or "空"] += 1
    def fmt(counter: Counter, n: int = 8) -> str:
        return " / ".join(f"{k}:{v}" for k, v in counter.most_common(n)) or "-"
    reply = (
        f"外贸客户统计：共 {len(rows)} 条\n"
        f"合作状态：{fmt(status)}\n"
        f"客户等级：{fmt(grade)}\n"
        f"客户来源：{fmt(source)}\n"
        f"国家/地区Top：{fmt(country, 10)}"
    )
    return {"ok": True, "total": len(rows), "reply": reply, "reply_result": await _send_reply(payload, reply)}


async def _handle_customer_intake(payload: dict) -> dict:
    fields = _merge_fields(payload)
    text = _text(payload.get("text"))
    company = _text(fields.get("company") or fields.get("customer")) or _after_command(text, "#客户入库")
    if not company:
        reply = "客户入库缺少公司名称，请补：公司：xxx"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    exhibition = _text(fields.get("exhibition"))
    source = _source(fields.get("source"), exhibition)
    if not exhibition and source == "展会":
        reply = "客户入库缺少展会名称，请补：展会：xxx。非展会线索请写：客户来源：领英/Snovio/Google/其他"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    duplicates = await _find_customers(company)
    exact_key = _normalize_key(company)
    exact_dups = [rec for rec in duplicates if _normalize_key(_text((rec.get("fields") or {}).get("公司名称"))) == exact_key]
    if exact_dups:
        lines = [f"发现重复客户，未新建：{company}"]
        for rec in exact_dups[:5]:
            lines.append(f"- {_customer_summary(rec)}\n{_record_url(B2B_CUSTOMER_TABLE, rec.get('record_id') or '')}")
        reply = "\n".join(lines)
        return {"ok": False, "duplicate": True, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    actor = _text(fields.get("collector")) or await _operator_name(_text(payload.get("sender_open_id"))) or "外贸助手"
    record = {
        "公司名称": company,
        "登记日期": _now_ms(),
        "合作状态": _pick_option(fields.get("status"), CUSTOMER_STATUS_OPTIONS) or "初步建联",
        "客户来源": source,
        "开发人": actor,
        "跟进人": _text(fields.get("owner")) or actor,
    }
    simple_map = {
        "contact": "核心联系人",
        "title": "职位",
        "email": "邮箱",
        "whatsapp": "WhatsApp",
        "phone": "电话",
        "wechat": "微信",
        "country": "国家/地区",
        "competitors": "代理竞品",
        "categories": "主营类目",
        "quantity": "预估采购量",
        "products": "感兴趣产品",
        "exhibition": "展会名称",
        "notes": "跟进日志",
    }
    for src, dest in simple_map.items():
        value = _text(fields.get(src))
        if value:
            record[dest] = value
    if record.get("跟进日志"):
        record["跟进日志"] = f"{_now_text()} {actor}：{record['跟进日志']}"
    company_type = _pick_option(fields.get("company_type"), COMPANY_TYPE_OPTIONS)
    if company_type:
        record["公司类型"] = company_type
    channels = _split_options(fields.get("channels"), CHANNEL_OPTIONS)
    if channels:
        record["主力渠道"] = channels
    grade = _grade(fields.get("grade"))
    if grade:
        record["客户等级"] = grade
    method = _contact_method(fields)
    if method:
        record["联系方式"] = method
    website = _link(fields.get("website"))
    if website:
        record["公司官网"] = website
    linkedin = _link(fields.get("linkedin"), "LinkedIn")
    if linkedin:
        record["LinkedIn"] = linkedin

    record = {k: v for k, v in record.items() if v not in ("", [], None)}
    record_id = await _create_record(B2B_CUSTOMER_TABLE, record)
    reply = (
        "客户入库成功\n"
        f"公司：{record.get('公司名称')}\n"
        f"客户来源：{record.get('客户来源')}\n"
        f"合作状态：{record.get('合作状态')}\n"
        f"开发人：{actor}\n"
        f"记录：{_record_url(B2B_CUSTOMER_TABLE, record_id)}"
    )
    return {"ok": True, "record_id": record_id, "reply": reply, "reply_result": await _send_reply(payload, reply)}


async def _handle_customer_followup(payload: dict) -> dict:
    fields = _merge_fields(payload)
    text = _text(payload.get("text"))
    keyword = _text(fields.get("customer") or fields.get("company")) or _after_command(text, "#客户跟进")
    if not keyword:
        reply = "客户跟进缺少客户关键词，请补：客户：xxx"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    matches = await _find_customers(keyword)
    if not matches:
        reply = f"未找到客户：{keyword}。请先 #客户入库。"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}
    if len(matches) > 1:
        lines = [f"找到 {len(matches)} 个可能客户，请用更精确公司名或 record_id："]
        for rec in matches[:8]:
            lines.append(f"- {_customer_summary(rec)}\n{_record_url(B2B_CUSTOMER_TABLE, rec.get('record_id') or '')}")
        reply = "\n".join(lines)
        return {"ok": False, "ambiguous": True, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    customer = matches[0]
    customer_id = customer.get("record_id") or ""
    customer_fields = customer.get("fields") or {}
    company = _text(customer_fields.get("公司名称")) or keyword
    actor = _text(fields.get("owner")) or await _operator_name(_text(payload.get("sender_open_id"))) or "外贸助手"
    method = _pick_option(fields.get("method"), FOLLOWUP_METHOD_OPTIONS) or "LinkedIn"
    content = _text(fields.get("content") or fields.get("notes") or _after_command(text, "#客户跟进"))
    feedback = _text(fields.get("feedback"))
    next_action = _text(fields.get("next_action"))

    follow_fields = {
        "客户": f"{company} - {_today()} - {method}",
        "关联客户": [customer_id],
        "跟进日期": _now_ms(),
        "跟进方式": method,
        "跟进内容": content or "外贸助手登记跟进",
        "跟进人": actor,
    }
    if feedback:
        follow_fields["客户反馈"] = feedback
    if next_action:
        follow_fields["下一步行动"] = next_action
    follow_id = await _create_record(B2B_FOLLOWUP_TABLE, follow_fields)

    old_log = _text(customer_fields.get("跟进日志"))
    new_line = f"{_now_text()} {actor} [{method}] {content or '登记跟进'}"
    if feedback:
        new_line += f"；反馈：{feedback}"
    if next_action:
        new_line += f"；下一步：{next_action}"
    update = {"跟进日志": (old_log + "\n" + new_line).strip() if old_log else new_line}
    status = _pick_option(fields.get("status"), CUSTOMER_STATUS_OPTIONS)
    if status:
        update["合作状态"] = status
    await _update_record(B2B_CUSTOMER_TABLE, customer_id, update)

    reply = (
        "客户跟进已登记\n"
        f"客户：{company}\n"
        f"方式：{method}\n"
        f"跟进记录：{_record_url(B2B_FOLLOWUP_TABLE, follow_id)}\n"
        f"客户记录：{_record_url(B2B_CUSTOMER_TABLE, customer_id)}"
    )
    return {"ok": True, "record_id": follow_id, "customer_record_id": customer_id, "reply": reply, "reply_result": await _send_reply(payload, reply)}


def _linkedin_action(raw: str) -> str:
    raw = _text(raw).lower()
    if not raw:
        return ""
    if "加人" in raw or "connect" in raw or "connected" in raw or raw in {"已加人", "li_added"}:
        return "linkedin_connected"
    if "私信" in raw or "message" in raw or raw in {"已发私信", "li_sent"}:
        return "linkedin_message_sent"
    if "email" in raw or "邮件" in raw or "转email" in raw or "转邮件" in raw:
        return "linkedin_to_email"
    if "回复" in raw or "replied" in raw:
        return "linkedin_replied"
    if "不合适" in raw or "not_fit" in raw or "not fit" in raw:
        return "linkedin_not_fit"
    if raw.startswith("b2b_linkedin_"):
        return raw.replace("b2b_", "", 1)
    if raw.startswith("li_"):
        return {
            "li_added": "linkedin_connected",
            "li_sent": "linkedin_message_sent",
        }.get(raw, raw)
    return raw


def _first_form_value(form_value: dict, keys: list[str]) -> str:
    for key in keys:
        value = form_value.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            value = "、".join(_text(x) for x in value if _text(x))
        else:
            value = _text(value)
        if value:
            return value
    return ""


async def _handle_linkedin_receipt(payload: dict) -> dict:
    fields = _merge_fields(payload)
    card_action = payload.get("card_action") or {}
    form_value = payload.get("card_form_value") or {}
    record_id = _text(card_action.get("record_id") or fields.get("lead_record_id") or fields.get("record_id"))
    action = _linkedin_action(card_action.get("action") or fields.get("receipt_action"))
    company_hint = _text(card_action.get("company") or fields.get("company") or fields.get("customer"))
    note = _first_form_value(form_value, ["linkedin_note", f"linkedin_note_{record_id}", "note"]) or _text(fields.get("receipt_note") or fields.get("notes"))

    if not record_id and company_hint:
        matches = await _find_linkedin_leads(company_hint)
        if len(matches) == 1:
            record_id = matches[0].get("record_id") or ""
        elif len(matches) > 1:
            lines = [f"LinkedIn回执找到 {len(matches)} 条可能线索，请补线索ID："]
            for rec in matches[:8]:
                f = rec.get("fields") or {}
                lines.append(f"- {_text(f.get('公司名称'))} / {_text(f.get('联系人姓名'))} / {_text(f.get('开发状态'))}\n{_record_url(B2B_LINKEDIN_TABLE, rec.get('record_id') or '', B2B_LINKEDIN_VIEW)}")
            reply = "\n".join(lines)
            return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}
    if not record_id:
        reply = "LinkedIn回执失败：缺少线索ID或公司名。"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}
    if not action:
        reply = "LinkedIn回执失败：缺少动作。可用：已加人 / 已发私信 / 已转Email / 已回复 / 不合适"
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}

    actor = await _operator_name(_text(payload.get("sender_open_id"))) or _text(payload.get("sender_open_id")) or "外贸助手"
    rec = await _get_record(B2B_LINKEDIN_TABLE, record_id)
    old_fields = rec.get("fields") or {}
    company = _text(old_fields.get("公司名称") or company_hint) or record_id
    old_note = _text(old_fields.get("备注"))
    action_map = {
        "linkedin_connected": {
            "开发状态": "已加人",
            "触达状态": "已发连接",
            "触达渠道": ["LinkedIn"],
            "下一步行动": "等待对方接受；接受后发送推荐私信并回执“已发私信”。",
        },
        "linkedin_message_sent": {
            "开发状态": "已发私信",
            "触达状态": "已发连接",
            "触达渠道": ["LinkedIn"],
            "触达验证结果": "送达",
            "下一步行动": "3-5天未回复则转Email/官网表单；有回复则回执“已回复”。",
        },
        "linkedin_to_email": {
            "开发状态": "已转Email",
            "触达状态": "已发邮件",
            "触达渠道": ["LinkedIn", "Email"],
            "下一步行动": "进入B2B邮件跟进提醒体系，等待邮件回复。",
        },
        "linkedin_replied": {
            "开发状态": "已回复",
            "触达状态": "有回复",
            "触达渠道": ["LinkedIn"],
            "触达验证结果": "送达",
            "下一步行动": "把沟通摘要补进CRM/跟进记录，判断是否转样品、报价或会议。",
        },
        "linkedin_not_fit": {
            "开发状态": "不合适",
            "触达状态": "不合适",
            "触达渠道": ["LinkedIn"],
            "触达验证结果": "不相关",
            "下一步行动": "暂停该线索，后续不进入日常开发清单。",
        },
    }
    update = dict(action_map.get(action) or {})
    if not update:
        reply = "未知 LinkedIn 回执动作：" + action
        return {"ok": False, "reply": reply, "reply_result": await _send_reply(payload, reply)}
    update["最近LinkedIn动作时间"] = _now_ms()
    update["最近触达时间"] = _now_ms()
    if not _text(old_fields.get("跟进人")):
        update["跟进人"] = actor
    line = f"{_now_text()} {actor}：{update['开发状态']}"
    if note:
        line += f"；{note}"
    update["备注"] = (old_note + "\n" + line).strip() if old_note else line
    await _update_record(B2B_LINKEDIN_TABLE, record_id, update)
    reply = (
        "LinkedIn回执已写入\n"
        f"公司：{company}\n"
        f"状态：{update['开发状态']}\n"
        f"下一步：{update.get('下一步行动')}\n"
        f"记录：{_record_url(B2B_LINKEDIN_TABLE, record_id, B2B_LINKEDIN_VIEW)}"
    )
    return {"ok": True, "record_id": record_id, "action": action, "fields": update, "reply": reply, "reply_result": await _send_reply(payload, reply)}


async def handle_event(payload: dict) -> dict:
    route = _text(payload.get("route"))
    command = _text(payload.get("command"))
    if route == "b2b_customer_query" or command == "customer_query":
        return await _handle_customer_query(payload)
    if route == "b2b_customer_stats" or command == "customer_stats":
        return await _handle_customer_stats(payload)
    if route == "b2b_customer_intake" or command == "customer_intake":
        return await _handle_customer_intake(payload)
    if route == "b2b_customer_followup" or command == "customer_followup":
        return await _handle_customer_followup(payload)
    if route == "linkedin_lead_receipt" or command == "linkedin_lead_receipt":
        return await _handle_linkedin_receipt(payload)
    return {"ok": True, "skipped": True, "route": route, "command": command}
