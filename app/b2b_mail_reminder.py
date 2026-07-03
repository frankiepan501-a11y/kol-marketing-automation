"""B2B foreign-trade mailbox follow-up reminder.

Daily cloud job:
1. Read the three B2B Zoho mailbox credentials from the internal account table.
2. Scan inbox/sent/drafts/junk/trash for the last N days.
3. Compare latest customer inbound mail with sent/draft follow-up.
4. Sync the B2B reminder Bitable.
5. Optionally send one Feishu App3 interactive receipt card to the B2B group.

All customer feedback/suppression state lives in the reminder table, not in repo
JSON files. This keeps business data in Bitable and lets card callbacks suppress
future reminders without code changes.
"""
import asyncio
import email
import imaplib
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from email import policy
from email.header import decode_header, make_header
from email.utils import getaddresses, parsedate_to_datetime
from urllib.parse import urlparse

from . import feishu

BJ = timezone(timedelta(hours=8))

B2B_CUSTOMER_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_CUSTOMER_TABLE = os.environ.get("B2B_CUSTOMER_TABLE", "tbl2OoqVb7Uf1pWd")
B2B_REMINDER_TABLE = os.environ.get("B2B_REMINDER_TABLE", "tblULtGR2SJ4MoNf")
B2B_REMINDER_VIEW = os.environ.get("B2B_REMINDER_VIEW", "vew4j62x5G")
B2B_MAIL_ACCOUNT_BASE = os.environ.get("B2B_MAIL_ACCOUNT_BASE", "NBM2bRFugaxLnjs8UUmc6iV0n8c")
B2B_MAIL_ACCOUNT_TABLE = os.environ.get("B2B_MAIL_ACCOUNT_TABLE", "tblJKzaKAH2O3Rop")
B2B_GROUP_CHAT_ID = os.environ.get("B2B_GROUP_CHAT_ID", "oc_2e878553984592d7396401fdd6a37d61")
B2B_WU_NOTIFY_CHAT_ID = os.environ.get("B2B_WU_NOTIFY_CHAT_ID", "oc_19d06fe15e949a50d860c8b8c73cbd10")

DEFAULT_TARGET_ACCOUNTS = {
    "silvia.wu@powkong.com": "吴晓丹",
    "carlos.xian@powkong.com": "冼浩华",
    "goya.li@powkong.com": "李桐欣",
}
OWNER_OPTIONS = {"冼浩华", "李桐欣", "吴晓丹"}
CHANNEL_OPTIONS = {"微信", "WhatsApp", "电话", "面谈", "LinkedIn", "其他"}
TEST_THREAD_PREFIX = "__test__"

INTERNAL_DOMAINS = {
    "powkong.com",
    "funlabswitch.com",
    "funlab.net",
    "fireflyfunlab.com",
    "linyuvo.com",
}
NOISE_DOMAINS = {
    "mail.zoho.com",
    "zoho.com",
    "zohomail.com",
    "zohoaccounts.com",
    "google.com",
    "accounts.google.com",
}
FREE_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "qq.com",
    "163.com",
    "126.com",
    "foxmail.com",
    "proton.me",
    "protonmail.com",
}
NOISE_LOCAL_PATTERNS = (
    "noreply",
    "no-reply",
    "mailer-daemon",
    "postmaster",
    "notification",
    "notify",
    "bounce",
    "support@zoho",
)
FOLDER_PRIORITY = {
    "sent": 1,
    "draft": 2,
    "inbox": 3,
    "junk": 4,
    "trash": 5,
    "other": 6,
}


def _target_accounts() -> dict:
    raw = os.environ.get("B2B_TARGET_ACCOUNTS", "").strip()
    if not raw:
        return dict(DEFAULT_TARGET_ACCOUNTS)
    out = {}
    for part in raw.split(","):
        if not part.strip():
            continue
        account, _, owner = part.partition(":")
        account = account.strip().lower()
        owner = owner.strip()
        if account:
            out[account] = owner or DEFAULT_TARGET_ACCOUNTS.get(account, "待确认")
    return out or dict(DEFAULT_TARGET_ACCOUNTS)


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "link", "name"):
            if value.get(key) is not None:
                return _text(value.get(key))
        return " ".join(_text(v) for v in value.values()).strip()
    if isinstance(value, list):
        return " ".join(_text(v) for v in value).strip()
    return str(value).strip()


def _extract_emails(text: str) -> list[str]:
    if not text:
        return []
    found = re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text)
    return sorted({e.lower().strip(".,;:()[]<>") for e in found})


def _domain_of(addr: str) -> str:
    if not addr or "@" not in addr:
        return ""
    return addr.rsplit("@", 1)[1].lower().strip()


def _normalize_domain(domain: str) -> str:
    domain = (domain or "").lower().strip()
    domain = re.sub(r"^https?://", "", domain)
    domain = domain.split("/", 1)[0].split(":", 1)[0].lstrip("@.")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _website_domain(value) -> str:
    text = _text(value)
    if not text:
        return ""
    if not re.match(r"^https?://", text):
        text = "https://" + text
    try:
        return _normalize_domain(urlparse(text).netloc)
    except Exception:
        return ""


def _is_internal(addr: str) -> bool:
    return _domain_of(addr) in INTERNAL_DOMAINS


def _is_noise(addr: str) -> bool:
    local = addr.split("@", 1)[0].lower() if "@" in addr else addr.lower()
    dom = _domain_of(addr)
    if dom in NOISE_DOMAINS:
        return True
    return any(pat in local for pat in NOISE_LOCAL_PATTERNS)


def _decode_mime(value: str) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value))).replace("\r", " ").replace("\n", " ").strip()
    except Exception:
        return str(value).replace("\r", " ").replace("\n", " ").strip()


def _parse_dt(value: str):
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _to_bj_string(value: str) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _now_bj_string() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S")


async def _list_records(app_token: str, table_id: str, *, field_names: list[str] | None = None) -> list[dict]:
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{app_token}/tables/{table_id}/records?page_size=500"
        if page_token:
            path += f"&page_token={page_token}"
        body = {"field_names": field_names} if field_names else None
        if body:
            data = await feishu.api("POST", path.replace("/records?", "/records/search?"), body, which="bitable")
        else:
            data = await feishu.api("GET", path, which="bitable")
        payload = data.get("data") or {}
        items.extend(payload.get("items") or [])
        if not payload.get("has_more"):
            break
        page_token = payload.get("page_token") or ""
        if not page_token:
            break
    return items


async def _load_accounts() -> list[dict]:
    targets = _target_accounts()
    rows = await _list_records(
        B2B_MAIL_ACCOUNT_BASE,
        B2B_MAIL_ACCOUNT_TABLE,
        field_names=["账号", "密码", "邮箱负责人", "状态", "使用目的"],
    )
    accounts = []
    for rec in rows:
        fields = rec.get("fields") or {}
        account_text = _text(fields.get("账号")).lower()
        found = _extract_emails(account_text)
        account = found[0] if found else account_text
        if account not in targets:
            continue
        password = _text(fields.get("密码"))
        if not password:
            accounts.append({"account": account, "owner": targets.get(account, ""), "error": "missing_password"})
            continue
        accounts.append(
            {
                "account": account,
                "password": password,
                "owner": _text(fields.get("邮箱负责人")) or targets.get(account, "待确认"),
                "status": _text(fields.get("状态")),
                "purpose": _text(fields.get("使用目的")),
            }
        )
    return accounts


async def _load_customers() -> list[dict]:
    rows = await _list_records(
        B2B_CUSTOMER_APP_TOKEN,
        B2B_CUSTOMER_TABLE,
        field_names=["公司名称", "邮箱", "公司官网", "跟进人", "合作状态", "国家/地区", "核心联系人"],
    )
    customers = []
    for rec in rows:
        fields = rec.get("fields") or {}
        emails = _extract_emails(_text(fields.get("邮箱")))
        email_domains = sorted({_domain_of(e) for e in emails if _domain_of(e)})
        web_domain = _website_domain(fields.get("公司官网"))
        domains = sorted({d for d in email_domains + ([web_domain] if web_domain else []) if d})
        customers.append(
            {
                "record_id": rec.get("record_id", ""),
                "company": _text(fields.get("公司名称")),
                "owner": _text(fields.get("跟进人")),
                "status": _text(fields.get("合作状态")),
                "country": _text(fields.get("国家/地区")),
                "contact": _text(fields.get("核心联系人")),
                "emails": emails,
                "domains": domains,
            }
        )
    return customers


def _parse_folder_line(raw) -> dict:
    line = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    flags = ""
    m_flags = re.search(r"\((.*?)\)", line)
    if m_flags:
        flags = m_flags.group(1).lower()
    m_name = re.search(r' "(?:[^"]*)" ("(?:[^"\\]|\\.)*"|[^ ]+)$', line)
    name = m_name.group(1).strip() if m_name else line.rsplit(" ", 1)[-1].strip()
    if name.startswith('"') and name.endswith('"'):
        name = name[1:-1].replace('\\"', '"')
    low_name = name.lower()
    if low_name == "inbox":
        category = "inbox"
    elif "\\sent" in flags or low_name in {"sent", "sent items"}:
        category = "sent"
    elif "\\drafts" in flags or low_name in {"draft", "drafts"}:
        category = "draft"
    elif "\\junk" in flags or "\\spam" in flags or "junk" in low_name or "spam" in low_name:
        category = "junk"
    elif "\\trash" in flags or low_name in {"trash", "deleted items"}:
        category = "trash"
    else:
        category = "other"
    return {"raw": line, "name": name, "flags": flags, "category": category}


def _list_folders(imap) -> list[dict]:
    status, data = imap.list()
    folders = [{"name": "INBOX", "flags": "", "category": "inbox", "raw": "INBOX"}]
    if status == "OK":
        for raw in data:
            if raw:
                item = _parse_folder_line(raw)
                if item["name"].upper() != "INBOX":
                    folders.append(item)
    unique = []
    seen = set()
    for folder in folders:
        if folder["name"] in seen:
            continue
        seen.add(folder["name"])
        unique.append(folder)
    unique.sort(key=lambda f: (FOLDER_PRIORITY.get(f["category"], 9), f["name"].lower()))
    return unique


def _select_folder(imap, folder_name: str) -> bool:
    for candidate in (f'"{folder_name}"', folder_name):
        status, _ = imap.select(candidate, readonly=True)
        if status == "OK":
            return True
    return False


def _parse_addrs(msg, headers: list[str]) -> list[str]:
    pairs = []
    for header in headers:
        pairs.extend(getaddresses(msg.get_all(header, [])))
    out = []
    for _, addr in pairs:
        addr = (addr or "").lower().strip()
        if "@" in addr:
            out.append(addr)
    return sorted(set(out))


def _classify_direction(category: str, from_addrs: list[str]) -> str:
    if category in {"sent", "draft"}:
        return "out" if category == "sent" else "draft"
    if category in {"inbox", "junk", "trash"}:
        return "in"
    return "out" if any(_is_internal(a) for a in from_addrs) else "in"


def _fetch_events_for_account(account_item: dict, since_imap: str) -> tuple[list[dict], list[dict]]:
    if account_item.get("error"):
        raise RuntimeError(account_item["error"])
    account = account_item["account"]
    owner = account_item["owner"]
    events = []
    folder_stats = []
    with imaplib.IMAP4_SSL("imap.zoho.com", 993, timeout=30) as imap:
        imap.login(account, account_item["password"])
        folders = _list_folders(imap)
        for folder in folders:
            selected = _select_folder(imap, folder["name"])
            stat = {
                "account": account,
                "owner": owner,
                "folder": folder["name"],
                "category": folder["category"],
                "selected": selected,
                "raw_count": 0,
                "event_count": 0,
            }
            if not selected:
                folder_stats.append(stat)
                continue
            status, data = imap.search(None, "SINCE", since_imap)
            if status != "OK":
                stat["search_error"] = status
                folder_stats.append(stat)
                continue
            ids = data[0].split()
            stat["raw_count"] = len(ids)
            header_expr = b"(BODY.PEEK[HEADER.FIELDS (FROM TO CC BCC DATE SUBJECT MESSAGE-ID IN-REPLY-TO REFERENCES)])"
            for num in ids:
                status, msg_data = imap.fetch(num, header_expr)
                if status != "OK":
                    continue
                header_bytes = b""
                for part in msg_data:
                    if isinstance(part, tuple):
                        header_bytes += part[1]
                if not header_bytes:
                    continue
                msg = email.message_from_bytes(header_bytes, policy=policy.default)
                from_addrs = _parse_addrs(msg, ["From"])
                to_addrs = _parse_addrs(msg, ["To", "Cc", "Bcc"])
                direction = _classify_direction(folder["category"], from_addrs)
                external = from_addrs if direction == "in" else to_addrs
                external = sorted(a for a in external if not _is_internal(a) and not _is_noise(a))
                if not external:
                    continue
                dt = _parse_dt(msg.get("Date"))
                if not dt:
                    continue
                event = {
                    "account": account,
                    "owner": owner,
                    "folder": folder["name"],
                    "folder_category": folder["category"],
                    "direction": direction,
                    "at": dt.isoformat(),
                    "ts": dt.timestamp(),
                    "subject": _decode_mime(msg.get("Subject"))[:180],
                    "message_id": (msg.get("Message-ID") or "").strip(),
                    "in_reply_to": (msg.get("In-Reply-To") or "").strip(),
                    "references": (msg.get("References") or "").strip(),
                    "external_emails": external,
                    "external_domains": sorted({_domain_of(a) for a in external if _domain_of(a)}),
                }
                events.append(event)
                stat["event_count"] += 1
            folder_stats.append(stat)
        try:
            imap.logout()
        except Exception:
            pass
    return events, folder_stats


def _build_customer_indexes(customers: list[dict]):
    exact = defaultdict(list)
    domain = defaultdict(list)
    for customer in customers:
        for addr in customer["emails"]:
            exact[addr].append(customer)
        for dom in customer["domains"]:
            if dom and dom not in FREE_DOMAINS and dom not in INTERNAL_DOMAINS and dom not in NOISE_DOMAINS:
                domain[dom].append(customer)
    return exact, domain


def _match_event_customers(event: dict, exact_index, domain_index):
    hits = []
    for addr in event["external_emails"]:
        for customer in exact_index.get(addr, []):
            hits.append(("exact", customer, addr, _domain_of(addr)))
    if hits:
        return hits
    for dom in event["external_domains"]:
        if dom in FREE_DOMAINS:
            continue
        for customer in domain_index.get(dom, []):
            hits.append(("domain", customer, "", dom))
    return hits


def _group_key_for_event(event: dict, exact_index, domain_index):
    hits = _match_event_customers(event, exact_index, domain_index)
    if hits:
        match_type, customer, addr, dom = hits[0]
        return (
            customer["record_id"] or f"customer:{customer['company']}",
            {
                "matched": True,
                "match_type": match_type,
                "record_id": customer["record_id"],
                "company": customer["company"],
                "crm_owner": customer["owner"],
                "crm_status": customer["status"],
                "country": customer["country"],
                "customer_emails": ";".join(customer["emails"]),
                "identity": dom or _domain_of(addr),
            },
        )
    nonfree_domains = [d for d in event["external_domains"] if d not in FREE_DOMAINS]
    if nonfree_domains:
        dom = nonfree_domains[0]
        return (
            f"unmatched-domain:{event['account']}:{dom}",
            {
                "matched": False,
                "match_type": "unmatched_domain",
                "record_id": "",
                "company": f"(未入库域名) {dom}",
                "crm_owner": "",
                "crm_status": "",
                "country": "",
                "customer_emails": "",
                "identity": dom,
            },
        )
    addr = event["external_emails"][0]
    return (
        f"unmatched-email:{event['account']}:{addr}",
        {
            "matched": False,
            "match_type": "unmatched_email",
            "record_id": "",
            "company": f"(未入库邮箱) {addr}",
            "crm_owner": "",
            "crm_status": "",
            "country": "",
            "customer_emails": "",
            "identity": addr,
        },
    )


def _thread_key(row: dict) -> str:
    identity = (
        row.get("record_id")
        or row.get("identity")
        or row.get("last_in_from")
        or row.get("company")
        or ""
    ).strip().lower()
    return f"{row.get('mailbox_account','').strip().lower()}|{identity}"


def _reminder_status(row: dict) -> str:
    status = (row.get("status") or "").strip()
    risk = (row.get("risk") or "").strip()
    if row.get("other_channel_followed_up") == "yes":
        return "其他渠道已跟进"
    if row.get("audit_suppressed") == "yes":
        sup = (row.get("suppression_status") or "").strip()
        if sup == "已邮件转交":
            return "转交他人处理"
        return "无需回复"
    if status == "已跟进":
        return "已邮件回复"
    if status == "有草稿未发送":
        return "待首次提醒"
    if risk in {"P0", "P1"} and _float(row.get("hours_open")) >= 24:
        return "24h待升级"
    return "待首次提醒"


def _receipt_type(row: dict) -> str:
    if row.get("other_channel_followed_up") == "yes":
        return "其他渠道已跟进"
    if row.get("audit_suppressed") == "yes":
        sup = (row.get("suppression_status") or "").strip()
        if sup == "已邮件转交":
            return "转交他人处理"
        return "无需回复"
    if (row.get("status") or "").strip() == "已跟进":
        return "已邮件回复"
    return ""


def _channel_values(row: dict) -> list[str]:
    raw = (row.get("other_channel_channel") or "").strip()
    if not raw:
        return []
    return [raw] if raw in CHANNEL_OPTIONS else ["其他"]


def _match_label(match_type: str) -> str:
    return {
        "exact": "客户邮箱命中",
        "domain": "客户域名命中",
        "unmatched_domain": "库外候选",
        "unmatched_email": "库外候选",
    }.get((match_type or "").strip(), "库外候选")


def _float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _trigger_reason(row: dict) -> str:
    parts = []
    status = (row.get("status") or "").strip()
    if status:
        parts.append(status)
    if row.get("other_channel_note"):
        parts.append("其他渠道：" + row["other_channel_note"].strip())
    if row.get("suppression_reason"):
        parts.append("抑制：" + row["suppression_reason"].strip())
    return "；".join(parts)[:1000]


def _existing_receipt_overrides(existing_by_key: dict) -> dict:
    overrides = {}
    for key, rec in existing_by_key.items():
        fields = rec.get("fields") or {}
        receipt_type = _text(fields.get("回执类型"))
        is_suppressed = bool(fields.get("审计抑制"))
        immune = bool(fields.get("是否已回执免提醒"))
        if not (receipt_type or is_suppressed or immune):
            continue
        status = _text(fields.get("提醒状态"))
        overrides[key] = {
            "receipt_type": receipt_type,
            "status": status,
            "channel": _text(fields.get("其他渠道")),
            "note": _text(fields.get("回执原因")) or _text(fields.get("抑制原因")),
            "confirmed_by": _text(fields.get("回执人")),
            "confirmed_at": _text(fields.get("回执时间")),
            "audit_suppressed": is_suppressed or receipt_type in {"无需回复", "转交他人处理"},
            "other_channel": receipt_type == "其他渠道已跟进",
        }
    return overrides


def _apply_existing_receipt(row: dict, overrides: dict):
    key = _thread_key(row)
    receipt = overrides.get(key)
    if not receipt:
        return
    if receipt["other_channel"]:
        row["other_channel_followed_up"] = "yes"
        row["other_channel_channel"] = receipt["channel"]
        row["other_channel_note"] = receipt["note"]
        row["other_channel_confirmed_by"] = receipt["confirmed_by"]
        row["other_channel_confirmed_at"] = receipt["confirmed_at"]
        if row["risk"]:
            row["status"] = "其他渠道已跟进"
            row["risk"] = ""
    elif receipt["audit_suppressed"]:
        row["audit_suppressed"] = "yes"
        row["suppression_status"] = "已邮件转交" if receipt["receipt_type"] == "转交他人处理" else "无需提醒"
        row["suppression_reason"] = receipt["note"]
        row["suppression_confirmed_by"] = receipt["confirmed_by"]
        row["suppression_confirmed_at"] = receipt["confirmed_at"]
        if row["risk"]:
            row["status"] = row["suppression_status"]
            row["risk"] = ""


def _audit_groups(events: list[dict], customers: list[dict], existing_by_key: dict, now_dt: datetime):
    exact_index, domain_index = _build_customer_indexes(customers)
    groups = {}
    for ev in events:
        key, meta = _group_key_for_event(ev, exact_index, domain_index)
        group = groups.setdefault(key, {**meta, "events": []})
        group["events"].append(ev)

    overrides = _existing_receipt_overrides(existing_by_key)
    rows = []
    unreplied = []
    for group in groups.values():
        evs = sorted(group["events"], key=lambda e: e["ts"])
        inbound = [e for e in evs if e["direction"] == "in"]
        sent = [e for e in evs if e["direction"] == "out"]
        drafts = [e for e in evs if e["direction"] == "draft"]
        if not inbound:
            continue
        last_in = inbound[-1]
        sent_after = [e for e in sent if e["ts"] > last_in["ts"]]
        draft_after = [e for e in drafts if e["ts"] > last_in["ts"]]
        last_out = sent[-1] if sent else None
        last_draft = drafts[-1] if drafts else None
        hours_open = max(0, (now_dt.timestamp() - last_in["ts"]) / 3600)
        if sent_after:
            status = "已跟进"
            risk = ""
            followup = sent_after[-1]
        elif draft_after:
            status = "有草稿未发送"
            risk = "P1"
            followup = draft_after[-1]
        else:
            followup = None
            if last_in["folder_category"] in {"junk", "trash"}:
                status = "垃圾/删除箱来信未见后续"
                risk = "P0" if hours_open >= 24 else "P1"
            elif hours_open >= 72:
                status = "超过72h未见邮件跟进"
                risk = "P0"
            elif hours_open >= 24:
                status = "超过24h未见邮件跟进"
                risk = "P1"
            else:
                status = "24h内待处理"
                risk = "P2"
        row = {
            "risk": risk,
            "status": status,
            "matched": "yes" if group["matched"] else "",
            "match_type": group["match_type"],
            "record_id": group["record_id"],
            "company": group["company"],
            "crm_owner": group["crm_owner"],
            "crm_status": group["crm_status"],
            "mailbox_owner": last_in["owner"],
            "mailbox_account": last_in["account"],
            "identity": group["identity"],
            "last_in_at": last_in["at"],
            "last_in_folder": last_in["folder"],
            "last_in_folder_category": last_in["folder_category"],
            "last_in_from": ";".join(last_in["external_emails"][:6]),
            "last_in_subject": last_in["subject"],
            "last_in_message_id": last_in.get("message_id", ""),
            "last_sent_at": last_out["at"] if last_out else "",
            "last_sent_subject": last_out["subject"] if last_out else "",
            "last_sent_message_id": last_out.get("message_id", "") if last_out else "",
            "last_draft_at": last_draft["at"] if last_draft else "",
            "last_draft_subject": last_draft["subject"] if last_draft else "",
            "last_draft_message_id": last_draft.get("message_id", "") if last_draft else "",
            "followup_after_last_in_at": followup["at"] if followup else "",
            "followup_folder": followup["folder"] if followup else "",
            "inbound_count": len(inbound),
            "sent_count": len(sent),
            "draft_count": len(drafts),
            "total_events": len(evs),
            "hours_open": round(hours_open, 1),
            "other_channel_followed_up": "",
            "other_channel_channel": "",
            "other_channel_note": "",
            "other_channel_confirmed_by": "",
            "other_channel_confirmed_at": "",
            "audit_suppressed": "",
            "suppression_status": "",
            "suppression_reason": "",
            "suppression_confirmed_by": "",
            "suppression_confirmed_at": "",
        }
        _apply_existing_receipt(row, overrides)
        rows.append(row)
        if row["risk"]:
            unreplied.append(row)
    rows.sort(key=lambda r: (r["risk"] or "P9", r["hours_open"]))
    unreplied.sort(key=lambda r: (r["risk"], -float(r["hours_open"])))
    return rows, unreplied


def _risk_label(row: dict) -> str:
    risk = (row.get("risk") or "").strip()
    return risk if risk in {"P0", "P1", "P2"} else "OK"


def _first_semicolon(value: str) -> str:
    return (value or "").split(";", 1)[0].strip()


def _row_to_fields(row: dict, batch_id: str, scan_time: str) -> dict:
    owner = (row.get("mailbox_owner") or "").strip()
    if owner not in OWNER_OPTIONS:
        owner = "待确认"
    hours = _float(row.get("hours_open"))
    fields = {
        "线程Key": _thread_key(row),
        "邮箱账号": row.get("mailbox_account", ""),
        "邮箱负责人": owner,
        "外部邮箱": _first_semicolon(row.get("last_in_from", "")),
        "外部域名": row.get("identity", ""),
        "客户/域名": row.get("company", ""),
        "匹配方式": _match_label(row.get("match_type", "")),
        "最后来信时间": _to_bj_string(row.get("last_in_at", "")),
        "最后来信主题": row.get("last_in_subject", "")[:1000],
        "最后来信文件夹": row.get("last_in_folder", ""),
        "最后发件时间": _to_bj_string(row.get("last_sent_at", "")),
        "最后草稿时间": _to_bj_string(row.get("last_draft_at", "")),
        "提醒状态": _reminder_status(row),
        "风险等级": _risk_label(row),
        "触发原因": _trigger_reason(row),
        "回执类型": _receipt_type(row),
        "其他渠道": _channel_values(row),
        "回执原因": (row.get("other_channel_note") or row.get("suppression_reason") or "")[:1000],
        "回执人": row.get("other_channel_confirmed_by") or row.get("suppression_confirmed_by") or "",
        "回执时间": _to_bj_string(row.get("other_channel_confirmed_at") or row.get("suppression_confirmed_at") or ""),
        "审计抑制": row.get("audit_suppressed") == "yes",
        "抑制原因": row.get("suppression_reason", "")[:1000],
        "是否已回执免提醒": bool(
            row.get("other_channel_followed_up") == "yes"
            or row.get("audit_suppressed") == "yes"
            or row.get("status") == "已跟进"
        ),
        "是否超过24h": hours >= 24,
        "超时小时数": round(hours, 1),
        "最后扫描时间": scan_time,
        "扫描批次": batch_id,
        "最后来信Message-ID": row.get("last_in_message_id", "")[:1000],
        "最后发件Message-ID": row.get("last_sent_message_id", "")[:1000],
        "原始事件数": int(_float(row.get("total_events"))),
    }
    record_id = (row.get("record_id") or "").strip()
    if record_id:
        fields["关联CRM客户"] = [record_id]
    return {k: v for k, v in fields.items() if v not in ("", [], None)}


async def _existing_reminders_by_key() -> dict:
    rows = await _list_records(B2B_CUSTOMER_APP_TOKEN, B2B_REMINDER_TABLE)
    out = {}
    for rec in rows:
        key = _text((rec.get("fields") or {}).get("线程Key"))
        if key:
            out[key] = rec
    return out


async def _upsert_reminder(fields: dict, existing_rec: dict | None = None):
    rid = (existing_rec or {}).get("record_id")
    body = {"fields": fields}
    path_base = f"/bitable/v1/apps/{B2B_CUSTOMER_APP_TOKEN}/tables/{B2B_REMINDER_TABLE}/records"
    try:
        if rid:
            return await feishu.api("PUT", f"{path_base}/{rid}", body, which="bitable")
        return await feishu.api("POST", path_base, body, which="bitable")
    except Exception as exc:
        # Relation-field payloads can vary by API/field type. The relation is
        # useful but non-critical; retry without it to avoid dropping the whole
        # daily sync on one schema nuance.
        if "关联CRM客户" in fields:
            retry = dict(fields)
            retry.pop("关联CRM客户", None)
            body = {"fields": retry}
            if rid:
                return await feishu.api("PUT", f"{path_base}/{rid}", body, which="bitable")
            return await feishu.api("POST", path_base, body, which="bitable")
        raise exc


async def _sync_rows(rows: list[dict], existing: dict, *, commit: bool) -> dict:
    batch_id = "b2b-mail-reminder-cloud-" + datetime.now(BJ).strftime("%Y%m%d-%H%M%S")
    scan_time = _now_bj_string()
    payloads = [_row_to_fields(row, batch_id, scan_time) for row in rows]
    results = []
    for fields in payloads:
        existing_rec = existing.get(fields["线程Key"])
        action = "update" if existing_rec else "create"
        rid = (existing_rec or {}).get("record_id", "")
        if commit:
            response = await _upsert_reminder(fields, existing_rec)
            record = ((response.get("data") or {}).get("record") or {})
            rid = record.get("record_id") or record.get("recordId") or rid
        results.append({"action": action, "record_id": rid, "thread_key": fields["线程Key"], "status": fields.get("提醒状态", "")})
    return {
        "batch_id": batch_id,
        "scan_time": scan_time,
        "rows": len(rows),
        "creates": sum(1 for r in results if r["action"] == "create"),
        "updates": sum(1 for r in results if r["action"] == "update"),
        "status_counts": dict(Counter(r["status"] for r in results)),
        "results": results[:50],
    }


def _record_url(record_id: str) -> str:
    return (
        f"https://u1wpma3xuhr.feishu.cn/base/{B2B_CUSTOMER_APP_TOKEN}"
        f"?table={B2B_REMINDER_TABLE}&view={B2B_REMINDER_VIEW}&record={record_id}"
    )


def _row_from_record(rec: dict) -> dict:
    fields = rec.get("fields") or {}
    return {
        "record_id": rec.get("record_id") or "",
        "thread_key": _text(fields.get("线程Key")),
        "mailbox": _text(fields.get("邮箱账号")),
        "owner": _text(fields.get("邮箱负责人")),
        "external_email": _text(fields.get("外部邮箱")),
        "customer": _text(fields.get("客户/域名")),
        "last_in_at": _text(fields.get("最后来信时间")),
        "subject": _text(fields.get("最后来信主题")),
        "status": _text(fields.get("提醒状态")),
        "risk": _text(fields.get("风险等级")),
        "trigger_reason": _text(fields.get("触发原因")),
        "hours_open": _text(fields.get("超时小时数")),
        "first_reminded_at": _text(fields.get("首次提醒时间")),
        "escalated_at": _text(fields.get("升级提醒时间")),
    }


def _already_reminded_after_last_in(reminded_at: str, last_in_at: str) -> bool:
    if not reminded_at:
        return False
    if not last_in_at:
        return True
    return reminded_at >= last_in_at


async def _eligible_rows(limit: int) -> list[dict]:
    rows = []
    for rec in await _list_records(B2B_CUSTOMER_APP_TOKEN, B2B_REMINDER_TABLE):
        row = _row_from_record(rec)
        if row["thread_key"].startswith(TEST_THREAD_PREFIX):
            continue
        first_due = row["status"] == "待首次提醒" and not _already_reminded_after_last_in(row["first_reminded_at"], row["last_in_at"])
        escalation_due = row["status"] == "24h待升级" and not _already_reminded_after_last_in(row["escalated_at"], row["last_in_at"])
        if first_due or escalation_due:
            rows.append(row)
    rows.sort(key=lambda x: (x["risk"] != "P0", x["risk"] != "P1", x["last_in_at"]))
    return rows[:limit] if limit else rows


def _field_value(row: dict, key: str) -> dict:
    return {
        "action": key,
        "app_token": B2B_CUSTOMER_APP_TOKEN,
        "table_id": B2B_REMINDER_TABLE,
        "record_id": row["record_id"],
        "thread_key": row["thread_key"],
        "mailbox": row["mailbox"],
        "owner": row["owner"],
        "customer": row["customer"],
    }


def _build_row_elements(row: dict) -> list[dict]:
    status_line = f"风险 **{row['risk'] or '-'}** / 状态 **{row['status'] or '-'}**"
    if row["hours_open"]:
        status_line += f" / 超时 {row['hours_open']}h"
    details = "\n".join(
        [
            f"**客户/域名**：{row['customer'] or '-'}",
            f"**邮箱负责人**：{row['owner'] or '-'}",
            f"**邮箱账号**：{row['mailbox'] or '-'}",
            f"**外部邮箱**：{row['external_email'] or '-'}",
            f"**最后来信**：{row['last_in_at'] or '-'}",
            f"**主题**：{row['subject'] or '-'}",
            status_line,
            f"[打开提醒表记录]({_record_url(row['record_id'])})",
        ]
    )
    rid = row["record_id"]
    return [
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": details}},
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "已邮件回复"},
                    "type": "primary",
                    "value": _field_value(row, "b2b_mail_replied"),
                }
            ],
        },
        {
            "tag": "form",
            "name": f"b2b_other_f_{rid}",
            "elements": [
                {
                    "tag": "multi_select_static",
                    "name": f"b2b_channel_{rid}",
                    "placeholder": {"tag": "plain_text", "content": "其他渠道"},
                    "options": [{"text": {"tag": "plain_text", "content": x}, "value": x} for x in ["微信", "WhatsApp", "电话", "面谈", "LinkedIn", "其他"]],
                },
                {
                    "tag": "input",
                    "name": f"b2b_note_{rid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "原因:"},
                    "placeholder": {"tag": "plain_text", "content": "微信/WhatsApp/电话已跟进的说明"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"b2b_submit_other_{rid}",
                    "text": {"tag": "plain_text", "content": "其他渠道已跟进"},
                    "type": "default",
                    "value": _field_value(row, "b2b_mail_other_channel"),
                },
            ],
        },
        {
            "tag": "form",
            "name": f"b2b_no_reply_f_{rid}",
            "elements": [
                {
                    "tag": "input",
                    "name": f"b2b_note_{rid}_no",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "无需回复原因:"},
                    "placeholder": {"tag": "plain_text", "content": "物流/自动回复/系统邮件/广告等"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"b2b_submit_no_{rid}",
                    "text": {"tag": "plain_text", "content": "无需回复"},
                    "type": "default",
                    "value": _field_value(row, "b2b_mail_no_reply"),
                },
            ],
        },
        {
            "tag": "form",
            "name": f"b2b_handoff_f_{rid}",
            "elements": [
                {
                    "tag": "input",
                    "name": f"b2b_note_{rid}_handoff",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "转交说明:"},
                    "placeholder": {"tag": "plain_text", "content": "已转交谁/哪个邮箱继续处理"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"b2b_submit_handoff_{rid}",
                    "text": {"tag": "plain_text", "content": "转交他人处理"},
                    "type": "default",
                    "value": _field_value(row, "b2b_mail_handoff"),
                },
            ],
        },
    ]


def _build_card(rows: list[dict], *, escalation_copy: bool = False) -> dict:
    level = "P0" if any(r["status"] == "24h待升级" for r in rows) else "P1"
    emoji = {"P0": "🔴", "P1": "🟠"}[level]
    title_text = "B2B邮件24h升级确认" if escalation_copy else "B2B邮件跟进回执"
    title = f"{emoji} [CUS·{level}] {title_text} · {len(rows)}条"
    intro = (
        "以下客户邮件已超过 24h 未见邮件回复，请与对应负责人确认；若已在微信/WhatsApp等渠道跟进，请在卡片填写回执。"
        if escalation_copy
        else "请在今天处理客户邮件；若已在微信/WhatsApp等渠道跟进，请在卡片填写回执，系统后续不再重复提醒。"
    )
    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": intro,
            },
        }
    ]
    for row in rows:
        elements.extend(_build_row_elements(row))
    elements.append(
        {
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "回执后即视为已解释，不再重复提醒；24h 未回执会升级给吴晓丹确认。",
                }
            ],
        }
    )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"title": {"tag": "plain_text", "content": title}, "template": "red" if level == "P0" else "orange"},
        "elements": elements,
    }


async def _mark_card_sent(rows: list[dict]) -> list[dict]:
    sent_at = _now_bj_string()
    updates = []
    for row in rows:
        if row["status"] == "待首次提醒":
            fields = {"首次提醒时间": sent_at}
        elif row["status"] == "24h待升级":
            fields = {"升级提醒时间": sent_at}
        else:
            continue
        await _upsert_reminder(fields, {"record_id": row["record_id"]})
        updates.append({"record_id": row["record_id"], **fields})
    return updates


async def _collect_mail_events(accounts: list[dict], since_imap: str):
    all_events = []
    all_folder_stats = []
    for account in accounts:
        try:
            events, stats = await asyncio.to_thread(_fetch_events_for_account, account, since_imap)
            all_events.extend(events)
            all_folder_stats.extend(stats)
        except Exception as exc:
            all_folder_stats.append(
                {
                    "account": account.get("account", ""),
                    "owner": account.get("owner", ""),
                    "folder": "(account)",
                    "category": "account_error",
                    "selected": False,
                    "raw_count": 0,
                    "event_count": 0,
                    "error": f"{type(exc).__name__}: {str(exc)[:200]}",
                }
            )
    deduped = {}
    for ev in all_events:
        key = (
            ev["account"],
            ev["message_id"] or ev["at"] + "|" + ev["subject"] + "|" + ";".join(ev["external_emails"]),
        )
        old = deduped.get(key)
        if not old or FOLDER_PRIORITY.get(ev["folder_category"], 9) < FOLDER_PRIORITY.get(old["folder_category"], 9):
            deduped[key] = ev
    return list(deduped.values()), all_folder_stats


async def run(*, commit: bool = False, notify: bool = False, limit: int = 10, days: int = 30) -> dict:
    if notify and not commit:
        raise ValueError("notify=true requires commit=true")
    now_dt = datetime.now(BJ)
    since_dt = now_dt - timedelta(days=max(1, days))
    since_imap = since_dt.strftime("%d-%b-%Y")

    existing = await _existing_reminders_by_key()
    accounts = await _load_accounts()
    customers = await _load_customers()
    events, folder_stats = await _collect_mail_events(accounts, since_imap)
    rows, unreplied = _audit_groups(events, customers, existing, now_dt)
    sync = await _sync_rows(rows, existing, commit=commit)

    message_id = ""
    wu_message_id = ""
    marked_sent = []
    notify_errors = []
    eligible = []
    if commit:
        eligible = await _eligible_rows(limit)
        if notify and eligible:
            card = _build_card(eligible)
            message_id = await feishu.send_card_via_app3("chat_id", B2B_GROUP_CHAT_ID, card)
            escalation_rows = [row for row in eligible if row["status"] == "24h待升级"]
            if escalation_rows and B2B_WU_NOTIFY_CHAT_ID:
                try:
                    wu_card = _build_card(escalation_rows, escalation_copy=True)
                    wu_message_id = await feishu.send_card_via_app3("chat_id", B2B_WU_NOTIFY_CHAT_ID, wu_card)
                except Exception as exc:
                    notify_errors.append(f"吴晓丹升级抄送失败: {type(exc).__name__}: {str(exc)[:200]}")
            marked_sent = await _mark_card_sent(eligible)

    summary = {
        "commit": commit,
        "notify": notify,
        "since": since_imap,
        "accounts": [{"account": a.get("account", ""), "owner": a.get("owner", ""), "has_password": bool(a.get("password"))} for a in accounts],
        "account_errors": [s for s in folder_stats if s.get("category") == "account_error"],
        "folder_event_counts": dict(Counter(s["category"] for s in folder_stats for _ in range(int(s.get("event_count", 0))))),
        "events": len(events),
        "customer_groups_with_inbound": len(rows),
        "unreplied_or_pending": len(unreplied),
        "risk_counts": dict(Counter(r["risk"] or "OK" for r in rows)),
        "status_counts": dict(Counter(r["status"] for r in rows)),
        "sync": sync,
        "eligible_count": len(eligible),
        "eligible_preview": [
            {
                "record_id": r["record_id"],
                "status": r["status"],
                "risk": r["risk"],
                "owner": r["owner"],
                "customer": r["customer"],
                "url": _record_url(r["record_id"]),
            }
            for r in eligible[:10]
        ],
        "message_id": message_id,
        "wu_message_id": wu_message_id,
        "notify_errors": notify_errors,
        "marked_sent": marked_sent,
    }
    return summary
