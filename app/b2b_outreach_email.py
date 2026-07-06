"""B2B LinkedIn-to-email outreach queue and dry-run sender.

This module keeps first-touch B2B development emails separate from the
mailbox follow-up reminder table. The queue owns "should send / has sent"
state; the existing B2B mailbox scanner owns replies and SLA reminders after
the customer writes back.
"""
import asyncio
import html
import json
import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from urllib.parse import quote

from . import b2b_crm_sync, b2b_mail_reminder, feishu

BJ = timezone(timedelta(hours=8))

B2B_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_LINKEDIN_TABLE = os.environ.get("B2B_LINKEDIN_TABLE", "tblN8XszEatuTJgP")
B2B_LINKEDIN_VIEW = os.environ.get("B2B_LINKEDIN_VIEW", "vew9f7zQ7s")
B2B_EMAIL_QUEUE_TABLE = os.environ.get("B2B_EMAIL_QUEUE_TABLE", "tblIjgVtoqn3TXDn")
B2B_EMAIL_QUEUE_VIEW = os.environ.get("B2B_EMAIL_QUEUE_VIEW", "vewI068ywU")

B2B_SMTP_HOST = os.environ.get("B2B_SMTP_HOST", "smtp.zoho.com")
B2B_SMTP_PORT = int(os.environ.get("B2B_SMTP_PORT", "465"))
B2B_EMAIL_DRY_RUN_TO = os.environ.get("B2B_EMAIL_DRY_RUN_TO", "398459272@qq.com")
B2B_EMAIL_DAILY_CAP_PER_ACCOUNT = int(os.environ.get("B2B_EMAIL_DAILY_CAP_PER_ACCOUNT", "30"))
B2B_EMAIL_SEND_COMMIT = os.environ.get("B2B_EMAIL_SEND_COMMIT", "").strip() == "1"

QUEUE_FIELD_NAMES = [
    "队列标题",
    "关联LinkedIn线索",
    "关联CRM客户",
    "线索记录ID",
    "CRM记录ID",
    "公司名称",
    "联系人姓名",
    "职位",
    "邮箱",
    "跟进人",
    "发送邮箱",
    "发送身份",
    "邮件主题",
    "邮件正文",
    "发送状态",
    "创建来源",
    "触发动作时间",
    "计划发送时间",
    "发送时间",
    "最近处理时间",
    "Zoho Message ID",
    "Dry-run收件人",
    "发送批次",
    "错误信息",
]

ACTIVE_QUEUE_STATES = {"待发送", "Dry-run已发送", "已发送", "缺邮箱"}
SENDABLE_STATES = {"待发送"}
COMMIT_SENDABLE_STATES = {"Dry-run已发送"}

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PLACEHOLDER_BLACKLIST = [
    "[TRACKING#",
    "[CARRIER",
    "[TBD",
    "[ETA",
    "[ADDRESS",
    "[PRICE",
    "[QUANTITY",
    "[xxx",
    "[XXX",
    "待填",
]


def _now_ms() -> int:
    return int(datetime.now(BJ).timestamp() * 1000)


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
    if isinstance(value, list) and value and isinstance(value[0], dict):
        ids = value[0].get("record_ids") or value[0].get("link_record_ids") or []
        return ids[0] if ids else ""
    return ""


def _record_url(table_id: str, record_id: str, view_id: str = "") -> str:
    if not record_id:
        return ""
    url = f"https://u1wpma3xuhr.feishu.cn/base/{B2B_APP_TOKEN}?table={table_id}&record={record_id}"
    if view_id:
        url = f"https://u1wpma3xuhr.feishu.cn/base/{B2B_APP_TOKEN}?table={table_id}&view={view_id}&record={record_id}"
    return url


def queue_record_url(record_id: str) -> str:
    return _record_url(B2B_EMAIL_QUEUE_TABLE, record_id, B2B_EMAIL_QUEUE_VIEW)


def linkedin_record_url(record_id: str) -> str:
    return _record_url(B2B_LINKEDIN_TABLE, record_id, B2B_LINKEDIN_VIEW)


def _clean_email(raw: str) -> tuple[str, str]:
    raw = _text(raw)
    if not raw:
        return "", "邮箱字段为空"
    matches = _EMAIL_RE.findall(raw)
    if not matches:
        return "", f"未找到有效邮箱: {raw[:80]}"
    first = matches[0].lower()
    if len(matches) > 1:
        return first, f"原字段含 {len(matches)} 个邮箱，系统先取第一个: {first}"
    return first, ""


def _owner_account(owner: str) -> tuple[str, str]:
    targets = b2b_mail_reminder._target_accounts()
    owner = _text(owner)
    for account, account_owner in targets.items():
        if account_owner == owner:
            return account, account_owner
    return "", owner or "待确认"


def _first_name(name: str) -> str:
    name = _text(name)
    if not name:
        return "there"
    return re.split(r"\s+", name)[0].strip(",") or name


def _default_subject(company: str) -> str:
    company = _text(company)
    return f"Switch / gaming accessories distribution{f' for {company}' if company else ''}"


def _default_body(fields: dict) -> str:
    contact = _first_name(_text(fields.get("联系人姓名")))
    company = _text(fields.get("公司名称")) or "your company"
    category = _text(fields.get("主营类目")) or "gaming accessories"
    reason = _text(fields.get("AI开发理由"))
    reason_line = f"\n\nI noticed {reason[0].lower() + reason[1:]}" if reason else (
        f"\n\nI noticed {company} works around {category}, so this may be relevant."
    )
    return (
        f"Hi {contact},"
        f"{reason_line}\n\n"
        "We supply Nintendo Switch / gaming accessories for distributors and retailers. "
        "Would you be the right person for sourcing these products? If useful, I can send a short line sheet and wholesale info.\n\n"
        "Best,\n"
        "POWKONG Partnerships"
    )


def _parse_subject_body(copy: str, fields: dict) -> tuple[str, str]:
    copy = _text(copy)
    subject = ""
    body = copy
    lines = [line.rstrip() for line in copy.splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    if lines:
        m = re.match(r"^(subject|邮件主题|主题)\s*[:：]\s*(.+)$", lines[0].strip(), flags=re.I)
        if m:
            subject = m.group(2).strip()
            body = "\n".join(lines[1:]).strip()
    if not subject:
        subject = _default_subject(_text(fields.get("公司名称")))
    if not _text(body):
        body = _default_body(fields)
    return subject[:250], body


def _plain_text_from_html(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value or "").replace("&nbsp;", " ").strip()


def _to_html(body: str) -> str:
    if re.search(r"<(p|div|br|h[1-6]|li|strong|em|a)[\s>/]", body or "", re.I):
        return body
    paragraphs = [p.strip() for p in (body or "").split("\n\n") if p.strip()]
    return "".join(f"<p>{html.escape(p).replace(chr(10), '<br>')}</p>" for p in paragraphs)


def _validate_email_content(subject: str, body: str) -> None:
    plain = _plain_text_from_html(_to_html(body))
    if len(plain) < 50:
        raise ValueError(f"邮件正文过短: {len(plain)} chars")
    if len(_text(subject)) < 5:
        raise ValueError("邮件主题过短")
    scan = f"{subject}\n{plain}"
    for keyword in _PLACEHOLDER_BLACKLIST:
        if keyword in scan:
            raise ValueError(f"命中未替换占位符: {keyword}")


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


async def _create_record(table_id: str, fields: dict) -> str:
    resp = await feishu.api(
        "POST",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records",
        {"fields": {k: v for k, v in fields.items() if v not in ("", [], None)}},
        which="bitable",
    )
    return (((resp.get("data") or {}).get("record") or {}).get("record_id") or "")


async def _update_record(table_id: str, record_id: str, fields: dict) -> None:
    await feishu.api(
        "PUT",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records/{record_id}",
        {"fields": {k: v for k, v in fields.items() if v is not None}},
        which="bitable",
    )


async def _get_record(table_id: str, record_id: str) -> dict:
    resp = await feishu.api(
        "GET",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records/{record_id}",
        which="bitable",
    )
    return (resp.get("data") or {}).get("record") or {}


async def _existing_queue_for_lead(lead_record_id: str) -> dict | None:
    if not lead_record_id:
        return None
    for rec in await _list_records(B2B_EMAIL_QUEUE_TABLE, field_names=["线索记录ID", "发送状态"]):
        fields = rec.get("fields") or {}
        if _text(fields.get("线索记录ID")) != lead_record_id:
            continue
        if _text(fields.get("发送状态")) in ACTIVE_QUEUE_STATES:
            return rec
    return None


async def enqueue_from_linkedin(lead_record_id: str, lead_fields: dict, *, actor: str = "", note: str = "") -> dict:
    """Create or refresh a first-touch email queue item from a LinkedIn lead."""
    company = _text(lead_fields.get("公司名称")) or _text(lead_fields.get("线索名称")) or lead_record_id
    contact = _text(lead_fields.get("联系人姓名"))
    email, email_note = _clean_email(_text(lead_fields.get("邮箱")))
    email_status = _text(lead_fields.get("邮箱验真状态"))
    owner = _text(lead_fields.get("跟进人")) or _text(actor) or "待确认"
    sender_account, sender_owner = _owner_account(owner)
    crm_id = _text(lead_fields.get("CRM记录ID")) or _first_link_id(lead_fields.get("关联CRM客户"))
    subject, body = _parse_subject_body(_text(lead_fields.get("推荐开发信")), lead_fields)

    status = "待发送"
    error = email_note if email_note and email else ""
    if not email:
        status = "缺邮箱"
        error = email_note
    elif email_status in {"invalid", "no_email", "not_found"}:
        status = "发送失败"
        error = f"邮箱验真状态为 {email_status}，系统不自动发送"
    elif not sender_account:
        status = "发送失败"
        error = f"找不到跟进人 {owner} 对应的 B2B 发件邮箱"
    else:
        try:
            _validate_email_content(subject, body)
        except Exception as exc:
            status = "发送失败"
            error = str(exc)

    fields = {
        "队列标题": f"{company} - LinkedIn转Email - {datetime.now(BJ).strftime('%Y-%m-%d')}",
        "关联LinkedIn线索": [lead_record_id] if lead_record_id else [],
        "关联CRM客户": [crm_id] if crm_id else [],
        "线索记录ID": lead_record_id,
        "CRM记录ID": crm_id,
        "公司名称": company,
        "联系人姓名": contact,
        "职位": _text(lead_fields.get("职位")),
        "邮箱": email,
        "跟进人": sender_owner if sender_owner in b2b_mail_reminder.OWNER_OPTIONS else "待确认",
        "发送邮箱": sender_account,
        "发送身份": "POWKONG",
        "邮件主题": subject,
        "邮件正文": body,
        "发送状态": status,
        "创建来源": "LinkedIn转Email",
        "触发动作时间": _now_ms(),
        "计划发送时间": _now_ms(),
        "最近处理时间": _now_ms(),
        "错误信息": error,
    }
    if note:
        fields["错误信息"] = (error + "\n" + f"回执备注: {note}").strip()

    existing = await _existing_queue_for_lead(lead_record_id)
    if existing:
        queue_id = existing.get("record_id") or ""
        await _update_record(B2B_EMAIL_QUEUE_TABLE, queue_id, fields)
        created = False
    else:
        queue_id = await _create_record(B2B_EMAIL_QUEUE_TABLE, fields)
        created = True

    return {
        "ok": status == "待发送",
        "created": created,
        "queue_record_id": queue_id,
        "queue_url": queue_record_url(queue_id),
        "status": status,
        "subject": subject,
        "email": email,
        "sender_account": sender_account,
        "error": error,
    }


async def _account_by_email(account_email: str) -> dict:
    accounts = await b2b_mail_reminder._load_accounts()
    target = (account_email or "").strip().lower()
    for account in accounts:
        if account.get("account") == target:
            return account
    return {"account": target, "error": "account_not_found"}


def _send_smtp_sync(account: str, password: str, to_addr: str, subject: str, body: str, *, real_to: str = "") -> str:
    msg_id = make_msgid(domain=(account.split("@")[-1] if "@" in account else "powkong.com"))
    html_body = _to_html(body)
    text_body = _plain_text_from_html(html_body)
    msg = EmailMessage()
    msg["From"] = account
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = msg_id
    if real_to:
        msg["X-B2B-Dry-Run-Original-To"] = real_to
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP_SSL(B2B_SMTP_HOST, B2B_SMTP_PORT, timeout=45) as smtp:
        smtp.login(account, password)
        smtp.send_message(msg)
    return msg_id


async def _send_smtp(account: str, password: str, to_addr: str, subject: str, body: str, *, real_to: str = "") -> str:
    return await asyncio.to_thread(_send_smtp_sync, account, password, to_addr, subject, body, real_to=real_to)


def _dry_run_body(body: str, real_to: str, dry_run_to: str) -> str:
    banner = (
        "<div style=\"background:#fff3cd;padding:8px;border:1px solid #ffc107;margin-bottom:12px\">"
        "<strong>DRY-RUN MODE</strong> - "
        f"this email was intended for <code>{html.escape(real_to)}</code>, "
        f"but was sent to <code>{html.escape(dry_run_to)}</code> for review. The real customer did not receive it."
        "</div>"
    )
    return banner + _to_html(body)


def _batch_id(commit: bool) -> str:
    mode = "commit" if commit else "dryrun"
    return "b2b-email-" + mode + "-" + datetime.now(BJ).strftime("%Y%m%d-%H%M%S")


def _field_ms(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, list) and value:
        return _field_ms(value[0])
    if isinstance(value, dict):
        return _field_ms(value.get("timestamp") or value.get("value"))
    return 0


async def _sent_count_24h(sender_account: str) -> int:
    cutoff = _now_ms() - 24 * 3600 * 1000
    count = 0
    for rec in await _list_records(B2B_EMAIL_QUEUE_TABLE, field_names=["发送邮箱", "发送状态", "发送时间"]):
        fields = rec.get("fields") or {}
        if _text(fields.get("发送邮箱")).lower() != sender_account.lower():
            continue
        if _text(fields.get("发送状态")) != "已发送":
            continue
        if _field_ms(fields.get("发送时间")) >= cutoff:
            count += 1
    return count


def _queue_row(rec: dict) -> dict:
    fields = rec.get("fields") or {}
    return {
        "record_id": rec.get("record_id") or "",
        "lead_record_id": _text(fields.get("线索记录ID")),
        "crm_record_id": _text(fields.get("CRM记录ID")),
        "crm_link": fields.get("关联CRM客户"),
        "company": _text(fields.get("公司名称")),
        "contact": _text(fields.get("联系人姓名")),
        "title": _text(fields.get("职位")),
        "email": _text(fields.get("邮箱")),
        "owner": _text(fields.get("跟进人")),
        "sender": _text(fields.get("发送邮箱")),
        "subject": _text(fields.get("邮件主题")),
        "body": _text(fields.get("邮件正文")),
        "status": _text(fields.get("发送状态")),
        "message_id": _text(fields.get("Zoho Message ID")),
        "batch_id": _text(fields.get("发送批次")),
        "sent_at_ms": _field_ms(fields.get("发送时间")),
    }


async def _sync_sent_row_to_crm(row: dict) -> dict:
    if row["status"] != "已发送":
        return {"ok": True, "skipped": "not_sent", "queue_record_id": row["record_id"], "status": row["status"]}
    if not row.get("message_id"):
        return {"ok": False, "queue_record_id": row["record_id"], "error": "已发送队列缺少 Zoho Message ID，无法幂等回填 CRM"}
    lead_fields = {}
    if row.get("lead_record_id"):
        try:
            lead_rec = await _get_record(B2B_LINKEDIN_TABLE, row["lead_record_id"])
            lead_fields = lead_rec.get("fields") or {}
        except Exception as exc:
            print(f"[b2b_outreach_email] sync_crm load lead failed {row['lead_record_id']}: {type(exc).__name__}: {str(exc)[:160]}")
    crm_sync = await b2b_crm_sync.sync_outreach_sent(
        row,
        lead_fields,
        message_id=row["message_id"],
        batch_id=row.get("batch_id") or "b2b-email-existing-sent",
        sent_at_ms=row.get("sent_at_ms") or _now_ms(),
    )
    crm_id = crm_sync.get("customer_record_id") or ""
    if crm_id:
        await _update_record(
            B2B_EMAIL_QUEUE_TABLE,
            row["record_id"],
            {"CRM记录ID": crm_id, "关联CRM客户": [crm_id], "最近处理时间": _now_ms()},
        )
        if row.get("lead_record_id"):
            await _update_record(
                B2B_LINKEDIN_TABLE,
                row["lead_record_id"],
                {
                    "CRM记录ID": crm_id,
                    "关联CRM客户": [crm_id],
                    "开发状态": "已转Email",
                    "触达状态": "已发邮件",
                    "触达渠道": ["LinkedIn", "Email"],
                    "触达验证结果": "送达",
                    "下一步行动": "等待客户邮件回复；进入B2B邮件提醒扫描。",
                },
            )
    return {"ok": True, "queue_record_id": row["record_id"], "crm_sync": crm_sync}


async def sync_sent_to_crm(*, record_id: str = "", limit: int = 20) -> dict:
    """Backfill CRM linkage for already-sent B2B outreach queue rows.

    This endpoint does not send email. It only mirrors sent queue state into
    the CRM master/follow-up tables and writes the CRM relation back.
    """
    limit = max(1, min(int(limit or 20), 100))
    selected = []
    for rec in await _list_records(B2B_EMAIL_QUEUE_TABLE, field_names=QUEUE_FIELD_NAMES):
        row = _queue_row(rec)
        if record_id and row["record_id"] != record_id:
            continue
        if row["status"] != "已发送":
            continue
        selected.append(row)
        if len(selected) >= limit:
            break
    results = []
    for row in selected:
        try:
            results.append(await _sync_sent_row_to_crm(row))
        except Exception as exc:
            results.append({"ok": False, "queue_record_id": row["record_id"], "error": f"{type(exc).__name__}: {str(exc)[:500]}"})
    return {
        "ok": all(r.get("ok") for r in results),
        "selected": len(selected),
        "synced": sum(1 for r in results if r.get("ok") and not r.get("skipped")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "results": results,
    }


async def run(*, commit: bool = False, dry_run_to: str = "", limit: int = 1, record_id: str = "", owner: str = "") -> dict:
    """Send queued B2B outreach emails.

    Default is dry-run only. Real sends require commit=true,
    B2B_EMAIL_SEND_COMMIT=1, and a prior Dry-run已发送 state.
    """
    limit = max(1, min(int(limit or 1), 10))
    dry_run_to = (dry_run_to or B2B_EMAIL_DRY_RUN_TO).strip()
    if commit and not B2B_EMAIL_SEND_COMMIT:
        return {"ok": False, "error": "Real send blocked: set B2B_EMAIL_SEND_COMMIT=1 first."}
    if not commit and not dry_run_to:
        return {"ok": False, "error": "Dry-run requires dry_run_to or B2B_EMAIL_DRY_RUN_TO."}

    batch_id = _batch_id(commit)
    rows = []
    for rec in await _list_records(B2B_EMAIL_QUEUE_TABLE, field_names=QUEUE_FIELD_NAMES):
        row = _queue_row(rec)
        if record_id and row["record_id"] != record_id:
            continue
        if owner and row["owner"] != owner:
            continue
        allowed = COMMIT_SENDABLE_STATES if commit else SENDABLE_STATES
        if row["status"] not in allowed:
            continue
        rows.append(row)
    rows = rows[:limit]
    if commit and not rows:
        return {
            "ok": False,
            "error": "No sendable records. Real send only accepts records already marked Dry-run已发送.",
            "commit": commit,
            "dry_run_to": "",
            "batch_id": batch_id,
            "selected": 0,
            "sent": 0,
            "failed": 0,
            "results": [],
        }

    results = []
    for row in rows:
        qid = row["record_id"]
        try:
            email, email_note = _clean_email(row["email"])
            if not email:
                raise ValueError(email_note)
            _validate_email_content(row["subject"], row["body"])
            account = await _account_by_email(row["sender"])
            if account.get("error"):
                raise ValueError(f"发件邮箱不可用: {account.get('error')}")
            if not account.get("password"):
                raise ValueError("发件邮箱缺少密码")
            if commit:
                sent_24h = await _sent_count_24h(row["sender"])
                if sent_24h >= B2B_EMAIL_DAILY_CAP_PER_ACCOUNT:
                    raise ValueError(f"{row['sender']} 24h已发送 {sent_24h} 封，达到上限 {B2B_EMAIL_DAILY_CAP_PER_ACCOUNT}")
                to_addr = email
                subject = row["subject"]
                body = row["body"]
                real_to = ""
            else:
                to_addr = dry_run_to
                subject = f"[B2B-DRY-RUN->{email}] {row['subject']}"
                body = _dry_run_body(row["body"], email, dry_run_to)
                real_to = email
            msg_id = await _send_smtp(account["account"], account["password"], to_addr, subject, body, real_to=real_to)
            sent_at_ms = _now_ms()
            lead_fields = {}
            if row["lead_record_id"]:
                try:
                    lead_rec = await _get_record(B2B_LINKEDIN_TABLE, row["lead_record_id"])
                    lead_fields = lead_rec.get("fields") or {}
                except Exception as exc:
                    lead_fields = {}
                    print(f"[b2b_outreach_email] load lead for CRM sync failed {row['lead_record_id']}: {type(exc).__name__}: {str(exc)[:160]}")
            crm_sync = {}
            crm_warning = ""
            if commit:
                try:
                    crm_sync = await b2b_crm_sync.sync_outreach_sent(
                        row,
                        lead_fields,
                        message_id=msg_id,
                        batch_id=batch_id,
                        sent_at_ms=sent_at_ms,
                    )
                except Exception as exc:
                    crm_warning = f"CRM同步失败但邮件已发出: {type(exc).__name__}: {str(exc)[:500]}"
                    print(f"[b2b_outreach_email] {crm_warning}")
            update = {
                "发送状态": "已发送" if commit else "Dry-run已发送",
                "发送时间": sent_at_ms,
                "最近处理时间": _now_ms(),
                "Zoho Message ID": msg_id,
                "Dry-run收件人": dry_run_to if not commit else "",
                "发送批次": batch_id,
                "错误信息": crm_warning,
            }
            crm_id = (crm_sync or {}).get("customer_record_id") or ""
            if crm_id:
                update["CRM记录ID"] = crm_id
                update["关联CRM客户"] = [crm_id]
            await _update_record(B2B_EMAIL_QUEUE_TABLE, qid, update)
            if row["lead_record_id"]:
                lead_update = {
                    "开发状态": "已转Email" if commit else "已转Email待发送",
                    "触达状态": "已发邮件" if commit else "待发邮件",
                    "触达渠道": ["LinkedIn", "Email"],
                    "触达验证结果": "送达" if commit else "待验证",
                    "下一步行动": "等待客户邮件回复；进入B2B邮件提醒扫描。" if commit else "开发信 dry-run 已发送给 Frankie，待确认后再真发。",
                    "最近触达时间": _now_ms() if commit else None,
                }
                if crm_id:
                    lead_update["CRM记录ID"] = crm_id
                    lead_update["关联CRM客户"] = [crm_id]
                await _update_record(B2B_LINKEDIN_TABLE, row["lead_record_id"], lead_update)
            results.append({
                "ok": True,
                "queue_record_id": qid,
                "company": row["company"],
                "to": to_addr,
                "real_to": email,
                "message_id": msg_id,
                "crm_sync": crm_sync,
                "crm_warning": crm_warning,
            })
        except Exception as exc:
            error = str(exc)[:1000]
            await _update_record(
                B2B_EMAIL_QUEUE_TABLE,
                qid,
                {"发送状态": "发送失败", "最近处理时间": _now_ms(), "发送批次": batch_id, "错误信息": error},
            )
            results.append({"ok": False, "queue_record_id": qid, "company": row["company"], "error": error})

    return {
        "ok": all(r.get("ok") for r in results) if results else True,
        "commit": commit,
        "dry_run_to": "" if commit else dry_run_to,
        "batch_id": batch_id,
        "selected": len(rows),
        "sent": sum(1 for r in results if r.get("ok")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "results": results,
    }
