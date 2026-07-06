"""B2B CRM write-back helpers.

This module is the shared boundary between LinkedIn/email automation and the
foreign-trade CRM tables. It keeps outreach state auditable in the customer
master table and follow-up table so mailbox reminders can match future replies
by customer email/domain.
"""
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from . import feishu

BJ = timezone(timedelta(hours=8))

B2B_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_CUSTOMER_TABLE = os.environ.get("B2B_CUSTOMER_TABLE", "tbl2OoqVb7Uf1pWd")
B2B_FOLLOWUP_TABLE = os.environ.get("B2B_FOLLOWUP_TABLE", "tblbS5sGAb72OjL4")

CUSTOMER_STATUS_EARLY = {"", "未联系", "已发开发邮件"}
CUSTOMER_TYPE_OPTIONS = {
    "贸易商", "分销商", "品牌商", "批发商", "混合型", "游戏IP", "电商卖家", "电商平台", "行业协会", "零售商",
}
FOLLOWUP_METHOD_OPTIONS = {"邮件", "WhatsApp", "电话", "LinkedIn", "微信", "面谈", "视频会议", "zalo"}
FREE_DOMAINS = {
    "gmail.com", "googlemail.com", "hotmail.com", "outlook.com", "live.com",
    "yahoo.com", "icloud.com", "qq.com", "163.com", "126.com", "foxmail.com",
    "proton.me", "protonmail.com",
}
INTERNAL_DOMAINS = {"powkong.com", "funlabswitch.com", "funlab.net", "fireflyfunlab.com", "linyuvo.com"}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


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


def _first_link_id(value) -> str:
    if isinstance(value, dict):
        ids = value.get("record_ids") or value.get("link_record_ids") or []
        return ids[0] if ids else ""
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.startswith("rec"):
                return item
            if isinstance(item, dict):
                ids = item.get("record_ids") or item.get("link_record_ids") or []
                if ids:
                    return ids[0]
                if item.get("id"):
                    return item["id"]
    return ""


def _normalize_key(value: str) -> str:
    return re.sub(r"[\W_]+", "", (value or "").lower(), flags=re.UNICODE)


def _emails(value) -> list[str]:
    return sorted({x.lower() for x in EMAIL_RE.findall(_text(value))})


def _normalize_domain(domain: str) -> str:
    domain = (domain or "").lower().strip()
    domain = re.sub(r"^https?://", "", domain)
    domain = domain.split("/", 1)[0].split(":", 1)[0].lstrip("@.")
    return domain[4:] if domain.startswith("www.") else domain


def _domain_of_email(addr: str) -> str:
    return addr.rsplit("@", 1)[1].lower() if addr and "@" in addr else ""


def _url_from_field(value) -> str:
    raw = _text(value)
    if not raw:
        return ""
    if "@" in raw and not raw.lower().startswith("http"):
        return ""
    if not re.match(r"^https?://", raw, flags=re.I):
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        if "." not in parsed.netloc:
            return ""
        return raw
    except Exception:
        return ""


def _domain_of_url(value) -> str:
    url = _url_from_field(value)
    if not url:
        return ""
    try:
        return _normalize_domain(urlparse(url).netloc)
    except Exception:
        return ""


def _url_cell(value: str, text: str = ""):
    url = _url_from_field(value)
    if not url:
        return None
    return {"link": url, "text": text or url}


def _ms_from_iso(value: str) -> int:
    if not value:
        return _now_ms()
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.astimezone(BJ).timestamp() * 1000)
    except Exception:
        return _now_ms()


def _bj_text_from_iso(value: str) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BJ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return _text(value)


async def _list_records(table_id: str, *, field_names: list[str] | None = None) -> list[dict]:
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records?page_size=500"
        if page_token:
            path += f"&page_token={page_token}"
        if field_names:
            resp = await feishu.api("POST", path.replace("/records?", "/records/search?"), {"field_names": field_names}, which="bitable")
        else:
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
    clean = {k: v for k, v in fields.items() if v not in ("", [], None)}
    resp = await feishu.api(
        "POST",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records",
        {"fields": clean},
        which="bitable",
    )
    return (((resp.get("data") or {}).get("record") or {}).get("record_id") or "")


async def _update_record(table_id: str, record_id: str, fields: dict) -> None:
    clean = {k: v for k, v in fields.items() if v is not None}
    if not clean:
        return
    await feishu.api(
        "PUT",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records/{record_id}",
        {"fields": clean},
        which="bitable",
    )


def _safe_status_after_outreach(old_status: str) -> str:
    old_status = _text(old_status)
    return "已发开发邮件" if old_status in {"", "未联系"} else old_status


def _safe_status_after_reply(old_status: str) -> str:
    old_status = _text(old_status)
    return "初步建联" if old_status in CUSTOMER_STATUS_EARLY else old_status


def _grade_from_lead(fields: dict) -> str:
    raw = _text(fields.get("AI建议等级") or fields.get("客户等级"))
    if raw.startswith("A") or "优先" in raw or "热线索" in raw:
        return "🔴A-热线索"
    if raw.startswith("B") or "可开发" in raw or "温线索" in raw:
        return "🟡B-温线索"
    if raw.startswith("C") or "冷线索" in raw:
        return "🔵C-冷线索"
    return ""


def _company_type_from_lead(fields: dict) -> str:
    value = _text(fields.get("公司类型"))
    return value if value in CUSTOMER_TYPE_OPTIONS else ""


def _append_line(old_log: str, line: str, dedupe_key: str = "") -> str:
    old_log = _text(old_log)
    if dedupe_key and dedupe_key in old_log:
        return old_log
    return (old_log + "\n" + line).strip() if old_log else line


async def _find_customer_match(*, existing_id: str = "", email: str = "", company: str = "", website: str = "") -> tuple[dict | None, str]:
    if existing_id:
        try:
            rec = await _get_record(B2B_CUSTOMER_TABLE, existing_id)
            if rec:
                return rec, "existing_id"
        except Exception:
            pass

    email = (_emails(email) or [""])[0]
    email_domain = _domain_of_email(email)
    website_domain = _domain_of_url(website)
    candidate_domains = {d for d in [email_domain, website_domain] if d and d not in FREE_DOMAINS and d not in INTERNAL_DOMAINS}
    company_key = _normalize_key(company)

    rows = await _list_records(
        B2B_CUSTOMER_TABLE,
        field_names=["公司名称", "邮箱", "公司官网", "合作状态", "跟进日志", "跟进人", "客户来源"],
    )
    company_hit = None
    domain_hit = None
    for rec in rows:
        fields = rec.get("fields") or {}
        row_emails = _emails(fields.get("邮箱"))
        if email and email in row_emails:
            return rec, "email"
        row_domains = {_domain_of_email(e) for e in row_emails}
        row_site_domain = _domain_of_url(fields.get("公司官网"))
        if row_site_domain:
            row_domains.add(row_site_domain)
        row_domains = {d for d in row_domains if d and d not in FREE_DOMAINS and d not in INTERNAL_DOMAINS}
        if candidate_domains and row_domains & candidate_domains and not domain_hit:
            domain_hit = rec
        if company_key and _normalize_key(_text(fields.get("公司名称"))) == company_key and not company_hit:
            company_hit = rec
    if domain_hit:
        return domain_hit, "domain"
    if company_hit:
        return company_hit, "company"
    return None, "created"


async def _create_followup(*, customer_id: str, company: str, method: str, content: str, owner: str, feedback: str = "", next_action: str = "", at_ms: int | None = None) -> str:
    method = method if method in FOLLOWUP_METHOD_OPTIONS else "邮件"
    fields = {
        "客户": f"{company or customer_id} - {_today()} - {method}",
        "关联客户": [customer_id],
        "跟进日期": at_ms or _now_ms(),
        "跟进方式": method,
        "跟进内容": content[:5000],
        "跟进人": owner or "外贸助手",
    }
    if feedback:
        fields["客户反馈"] = feedback[:5000]
    if next_action:
        fields["下一步行动"] = next_action[:5000]
    return await _create_record(B2B_FOLLOWUP_TABLE, fields)


def _field_if_empty(update: dict, old_fields: dict, field_name: str, value):
    if value in ("", [], None):
        return
    if not _text(old_fields.get(field_name)):
        update[field_name] = value


async def sync_outreach_sent(queue_row: dict, lead_fields: dict, *, message_id: str, batch_id: str, sent_at_ms: int | None = None) -> dict:
    """Ensure a real first-touch email send is visible in CRM master/follow-up."""
    lead_fields = lead_fields or {}
    sent_at_ms = sent_at_ms or _now_ms()
    company = _text(queue_row.get("company") or lead_fields.get("公司名称") or lead_fields.get("线索名称"))
    email = _text(queue_row.get("email") or lead_fields.get("邮箱"))
    owner = _text(queue_row.get("owner") or lead_fields.get("跟进人")) or "待确认"
    contact = _text(queue_row.get("contact") or lead_fields.get("联系人姓名"))
    title = _text(queue_row.get("title") or lead_fields.get("职位"))
    website = _url_from_field(lead_fields.get("公司官网"))
    linkedin = _url_from_field(lead_fields.get("LinkedIn公司页")) or _url_from_field(lead_fields.get("LinkedIn联系人页"))
    existing_id = _text(queue_row.get("crm_record_id")) or _first_link_id(queue_row.get("crm_link"))

    match, match_type = await _find_customer_match(existing_id=existing_id, email=email, company=company, website=website)
    old_fields = (match or {}).get("fields") or {}
    old_log = _text(old_fields.get("跟进日志"))
    line = (
        f"{_now_text()} 系统 [邮件] 已发送LinkedIn转Email开发信；"
        f"主题：{_text(queue_row.get('subject')) or '-'}；收件人：{email or '-'}；"
        f"发件邮箱：{_text(queue_row.get('sender')) or '-'}；Message-ID：{message_id or '-'}；批次：{batch_id or '-'}"
    )
    already_logged = bool(message_id and message_id in old_log)

    if match:
        customer_id = match.get("record_id") or existing_id
        update = {}
        _field_if_empty(update, old_fields, "邮箱", email)
        _field_if_empty(update, old_fields, "核心联系人", contact)
        _field_if_empty(update, old_fields, "职位", title)
        _field_if_empty(update, old_fields, "跟进人", owner)
        _field_if_empty(update, old_fields, "开发人", owner)
        _field_if_empty(update, old_fields, "客户来源", "领英")
        _field_if_empty(update, old_fields, "联系方式", "邮箱")
        _field_if_empty(update, old_fields, "国家/地区", _text(lead_fields.get("国家/地区")))
        _field_if_empty(update, old_fields, "主营类目", _text(lead_fields.get("主营类目")))
        _field_if_empty(update, old_fields, "代理竞品", _text(lead_fields.get("代理竞品")))
        _field_if_empty(update, old_fields, "公司类型", _company_type_from_lead(lead_fields))
        _field_if_empty(update, old_fields, "客户等级", _grade_from_lead(lead_fields))
        if website and not _text(old_fields.get("公司官网")):
            update["公司官网"] = _url_cell(website)
        if linkedin and not _text(old_fields.get("LinkedIn")):
            update["LinkedIn"] = _url_cell(linkedin, "LinkedIn")
        status = _safe_status_after_outreach(old_fields.get("合作状态"))
        if status and status != _text(old_fields.get("合作状态")):
            update["合作状态"] = status
        if not already_logged:
            update["跟进日志"] = _append_line(old_log, line, message_id)
        await _update_record(B2B_CUSTOMER_TABLE, customer_id, update)
        created = False
    else:
        fields = {
            "公司名称": company or email or "LinkedIn Email Lead",
            "登记日期": sent_at_ms,
            "开发日期": sent_at_ms,
            "合作状态": "已发开发邮件",
            "客户来源": "领英",
            "开发人": owner,
            "跟进人": owner,
            "联系方式": "邮箱" if email else "",
            "邮箱": email,
            "核心联系人": contact,
            "职位": title,
            "国家/地区": _text(lead_fields.get("国家/地区")),
            "公司类型": _company_type_from_lead(lead_fields),
            "主营类目": _text(lead_fields.get("主营类目")),
            "代理竞品": _text(lead_fields.get("代理竞品")),
            "客户等级": _grade_from_lead(lead_fields),
            "跟进日志": line,
        }
        website_cell = _url_cell(website)
        if website_cell:
            fields["公司官网"] = website_cell
        linkedin_cell = _url_cell(linkedin, "LinkedIn")
        if linkedin_cell:
            fields["LinkedIn"] = linkedin_cell
        customer_id = await _create_record(B2B_CUSTOMER_TABLE, fields)
        created = True

    followup_id = ""
    if customer_id and not already_logged:
        content = (
            f"已发送LinkedIn转Email开发信；主题：{_text(queue_row.get('subject')) or '-'}；"
            f"收件人：{email or '-'}；发件邮箱：{_text(queue_row.get('sender')) or '-'}；"
            f"Message-ID：{message_id or '-'}；队列记录：{_text(queue_row.get('record_id')) or '-'}"
        )
        followup_id = await _create_followup(
            customer_id=customer_id,
            company=company,
            method="邮件",
            content=content,
            owner=owner,
            next_action="等待客户邮件回复；B2B邮件提醒每日扫描该客户邮箱/域名。",
            at_ms=sent_at_ms,
        )

    return {
        "ok": True,
        "customer_record_id": customer_id,
        "customer_created": created,
        "matched_by": match_type,
        "followup_record_id": followup_id,
        "already_logged": already_logged,
    }


async def sync_inbound_reply(row: dict, existing_reminder: dict | None = None) -> dict:
    """Log a new customer inbound email from the reminder scanner into CRM."""
    customer_id = _text(row.get("record_id"))
    if not customer_id:
        return {"ok": True, "skipped": "no_crm_customer"}
    last_in_at = _text(row.get("last_in_at"))
    if not last_in_at:
        return {"ok": True, "skipped": "no_inbound_time"}

    existing_fields = (existing_reminder or {}).get("fields") or {}
    new_msg_id = _text(row.get("last_in_message_id"))
    old_msg_id = _text(existing_fields.get("最后来信Message-ID"))
    if new_msg_id and old_msg_id == new_msg_id:
        return {"ok": True, "skipped": "same_message_id", "customer_record_id": customer_id}
    new_time_text = _bj_text_from_iso(last_in_at)
    if not new_msg_id and new_time_text and _text(existing_fields.get("最后来信时间")).startswith(new_time_text):
        return {"ok": True, "skipped": "same_inbound_time", "customer_record_id": customer_id}

    customer = await _get_record(B2B_CUSTOMER_TABLE, customer_id)
    customer_fields = customer.get("fields") or {}
    company = _text(customer_fields.get("公司名称") or row.get("company")) or customer_id
    owner = _text(customer_fields.get("跟进人") or row.get("crm_owner") or row.get("mailbox_owner")) or "待确认"
    subject = _text(row.get("last_in_subject"))
    from_email = _text(row.get("last_in_from"))
    line = (
        f"{new_time_text or _now_text()} 系统 [邮件] 客户来信；"
        f"主题：{subject or '-'}；来源：{from_email or '-'}；"
        f"Message-ID：{new_msg_id or '-'}；提醒状态：{_text(row.get('status')) or '-'}"
    )
    old_log = _text(customer_fields.get("跟进日志"))
    already_logged = bool(new_msg_id and new_msg_id in old_log)
    update = {}
    status = _safe_status_after_reply(customer_fields.get("合作状态"))
    if status and status != _text(customer_fields.get("合作状态")):
        update["合作状态"] = status
    if not already_logged:
        update["跟进日志"] = _append_line(old_log, line, new_msg_id)
    await _update_record(B2B_CUSTOMER_TABLE, customer_id, update)

    followup_id = ""
    if not already_logged:
        followup_id = await _create_followup(
            customer_id=customer_id,
            company=company,
            method="邮件",
            content=(
                f"客户邮件来信；主题：{subject or '-'}；来源：{from_email or '-'}；"
                f"收件邮箱：{_text(row.get('mailbox_account')) or '-'}；Message-ID：{new_msg_id or '-'}"
            ),
            owner=owner,
            feedback=subject,
            next_action="查看邮箱并回复客户；处理后在B2B邮件提醒卡回执。",
            at_ms=_ms_from_iso(last_in_at),
        )
    return {
        "ok": True,
        "customer_record_id": customer_id,
        "followup_record_id": followup_id,
        "already_logged": already_logged,
    }


def _method_from_channels(channels: list[str], receipt_type: str) -> str:
    for item in channels or []:
        if item in FOLLOWUP_METHOD_OPTIONS:
            return item
    return "邮件" if receipt_type in {"已邮件回复", "无需回复", "转交他人处理"} else "LinkedIn"


async def sync_mail_receipt_to_customer(reminder_record: dict, *, receipt_type: str, actor: str, note: str = "", channels: list[str] | None = None) -> dict:
    """Mirror B2B reminder card receipts into CRM follow-up history."""
    fields = (reminder_record or {}).get("fields") or {}
    customer_id = _first_link_id(fields.get("关联CRM客户"))
    if not customer_id:
        return {"ok": True, "skipped": "no_crm_customer"}
    customer = await _get_record(B2B_CUSTOMER_TABLE, customer_id)
    customer_fields = customer.get("fields") or {}
    company = _text(customer_fields.get("公司名称") or fields.get("客户/域名")) or customer_id
    subject = _text(fields.get("最后来信主题"))
    external = _text(fields.get("外部邮箱"))
    method = _method_from_channels(channels or [], receipt_type)
    content = {
        "已邮件回复": "业务员已通过邮件回复客户",
        "其他渠道已跟进": f"业务员已通过{method}跟进客户",
        "无需回复": "业务员确认该邮件无需回复",
        "转交他人处理": "业务员确认该邮件已转交他人处理",
    }.get(receipt_type, f"业务员回执：{receipt_type or '-'}")
    detail = f"{content}；原来信主题：{subject or '-'}；外部邮箱：{external or '-'}"
    if note:
        detail += f"；说明：{note}"
    line = f"{_now_text()} {actor or '外贸助手'} [{method}] {detail}"
    dedupe_key = f"{receipt_type}|{_text(fields.get('最后来信Message-ID'))}|{actor}|{note}"
    old_log = _text(customer_fields.get("跟进日志"))
    update = {"跟进日志": _append_line(old_log, line, dedupe_key)}
    if receipt_type in {"已邮件回复", "其他渠道已跟进"}:
        status = _safe_status_after_reply(customer_fields.get("合作状态"))
        if status and status != _text(customer_fields.get("合作状态")):
            update["合作状态"] = status
    await _update_record(B2B_CUSTOMER_TABLE, customer_id, update)
    followup_id = await _create_followup(
        customer_id=customer_id,
        company=company,
        method=method,
        content=detail,
        owner=actor or _text(customer_fields.get("跟进人")) or "外贸助手",
        feedback=subject,
        next_action="CRM已同步邮件提醒回执。",
    )
    return {"ok": True, "customer_record_id": customer_id, "followup_record_id": followup_id}
