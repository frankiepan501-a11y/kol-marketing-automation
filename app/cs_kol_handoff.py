"""Move creator-partnership mail from customer service into the KOL workflow.

The handoff creates/reuses the KOL master record, writes the inbound message to
the KOL follow-up table and sends an internal KOL intake card.  It deliberately
does not create an email draft: profile, product and rights must be reviewed in
the KOL record before the normal reply-drafting workflow may be used.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any

from . import config, feishu


CONFIRMED_REPAIR_RECORD_IDS = {
    "reczz28KPmJdKv3i",
    "reczz28KPmDPYy9s",
    "recvoU74BsNzVR",
}
HANDOFF_MARKER = "KOL_HANDOFF_MESSAGE_IDS:"
PENDING_MARKER = "KOL_HANDOFF_STATE:pending"
COMPLETED_MARKER = "KOL_HANDOFF_STATE:completed"
WORKFLOW_MARKER = "KOL_WORKFLOW_FOLLOWUP_ID:"
INTAKE_ATTEMPT_MARKER = "KOL_INTAKE_CARD_ATTEMPT:"
INTAKE_RECEIPT_MARKER = "KOL_INTAKE_CARD_IDS:"
INBOUND_MARKER_PREFIX = "[CS_INBOUND_KOL]"
_workflow_locks: dict[str, asyncio.Lock] = {}


def _text(value: Any) -> str:
    return str(feishu.ext(value) or "").strip()


def _ticket_url(record_id: str) -> str:
    from . import cs_ingest
    return (f"https://u1wpma3xuhr.feishu.cn/base/{cs_ingest.CS_APP_TOKEN}"
            f"?table={cs_ingest.T_TICKET}&record={record_id}")


def _contact_url(contact_type: str, record_id: str) -> str:
    table = config.T_KOL if contact_type == "KOL" else config.T_EDITOR
    return (f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
            f"?table={table}&record={record_id}")


async def lookup_contact(email: str):
    wanted = str(email or "").strip().lower()
    if not wanted:
        return None, None
    for table_id, contact_type in ((config.T_KOL, "KOL"), (config.T_EDITOR, "媒体人")):
        rows = await feishu.search_records(
            table_id,
            [{"field_name": "邮箱", "operator": "contains", "value": [wanted]}],
        )
        for row in rows:
            fields = row.get("fields") or {}
            row_email, _ = feishu.clean_email(_text(fields.get("邮箱")))
            if row_email.lower() == wanted:
                return row, contact_type
    return None, None


async def _exact_kol_email_matches(email: str) -> list[dict]:
    wanted = str(email or "").strip().lower()
    rows = await feishu.search_records(
        config.T_KOL,
        [{"field_name": "邮箱", "operator": "contains", "value": [wanted]}],
    )
    return [
        row for row in rows
        if feishu.clean_email(_text((row.get("fields") or {}).get("邮箱")))[0].lower() == wanted
    ]


def _inbound_marker(record_id: str) -> str:
    return f"{INBOUND_MARKER_PREFIX} ticket={record_id}"


def controlled_kol_fields(record_id: str, fields: dict) -> dict:
    """Fields for a new inbound creator, closed to ordinary cold outreach."""
    email = _resolved_customer_email(fields).lower()
    if not email:
        raise ValueError("creator email is required for controlled KOL intake")
    account_name = email.split("@", 1)[0] or email
    marker = _inbound_marker(record_id)
    return {
        "账号名": account_name[:200],
        "邮箱": email,
        "邮箱验真状态": "未验",
        "合作状态": "未建联",
        # New inbound creators are controlled imports.  The original message is
        # preserved in follow-up history, but no outreach route is opened until
        # an operator verifies the creator profile and relationship.
        "触达路由状态": "待核对",
        "资料可用状态": "缺资料",
        "迁移备注": (
            f"{marker}; source=customer_service_mailbox; "
            "no_auto_email=true; needs_profile_review=true"
        )[:1000],
    }


def _message_subject(fields: dict) -> str:
    explicit = _text(fields.get("主题"))
    if explicit:
        return explicit[:180]
    original = _text(fields.get("原文"))
    return (original.splitlines()[0].strip() if original else "Creator partnership request")[:180]


def _message_body(fields: dict) -> str:
    original = _text(fields.get("原文"))
    if not original:
        return _text(fields.get("客诉摘要"))
    lines = original.splitlines()
    if lines and lines[0].strip() == _message_subject(fields):
        return "\n".join(lines[1:]).strip() or original
    return original


def inbound_followup_fields(record_id: str, fields: dict, contact: dict) -> dict:
    marker = _inbound_marker(record_id)
    subject = _message_subject(fields)
    body = _message_body(fields)
    return {
        "跟进摘要": f"[客服邮箱转KOL] {subject}"[:200],
        "跟进日期": int(fields.get("入站时间") or time.time() * 1000),
        "跟进方式": "邮件",
        "跟进内容": (
            f"{marker}\n来信来自客服邮箱，系统已转入 KOL 流程并强制人工审核。\n"
            f"主题: {subject}\n\n原文:\n{body[:1200]}"
        )[:4000],
        "客户反馈": (_text(fields.get("客诉摘要")) or body)[:500],
        "下一步行动": "核对达人主页、历史关系、产品和合作条件；资料齐全后再走标准回复草稿。",
        "关联KOL": [contact["record_id"]],
    }


async def _find_existing_followup(record_id: str):
    marker = _inbound_marker(record_id)
    rows = await feishu.search_records(
        config.T_KOL_FU,
        [{"field_name": "跟进内容", "operator": "contains", "value": [marker]}],
    )
    return next(
        (row for row in rows if marker in _text((row.get("fields") or {}).get("跟进内容"))),
        None,
    )


async def _verify_controlled_master(contact: dict, expected_email: str) -> dict:
    record_id = str(contact.get("record_id") or "")
    master = await feishu.get_record(config.T_KOL, record_id)
    current = master.get("fields") or {}
    required = {
        "合作状态": "未建联",
        "触达路由状态": "待核对",
        "资料可用状态": "缺资料",
    }
    if feishu.clean_email(_text(current.get("邮箱")))[0] != expected_email:
        raise RuntimeError("controlled KOL email readback failed")
    missing = {key: value for key, value in required.items() if _text(current.get(key)) != value}
    for key, value in missing.items():
        await feishu.update_record(config.T_KOL, record_id, {key: value})
    if missing:
        master = await feishu.get_record(config.T_KOL, record_id)
        current = master.get("fields") or {}
    failed = [key for key, value in required.items() if _text(current.get(key)) != value]
    if failed:
        raise RuntimeError("controlled KOL safety state readback failed: " + ",".join(failed))
    return master


async def ensure_kol_workflow(record_id: str, fields: dict, *, contact=None,
                              contact_type: str = "") -> dict:
    """Idempotently establish a non-sending KOL intake workflow."""
    email = _resolved_customer_email(fields).lower()
    if not email:
        raise ValueError("creator email is required")
    # Email is the business identity.  Locking by ticket would still allow two
    # simultaneous tickets for one creator to create duplicate master rows.
    lock = _workflow_locks.setdefault(email, asyncio.Lock())
    async with lock:
        if contact is None:
            contact, contact_type = await lookup_contact(email)
        if contact and contact_type not in {"", "KOL"}:
            raise ValueError("customer-service creator intake matched a media record; manual review required")

        master_created = False
        if not contact:
            # Recheck while holding the in-process claim.  The stable email and
            # ticket marker make normal retries reuse the same controlled row.
            contact, contact_type = await lookup_contact(email)
        if not contact:
            master_fields = controlled_kol_fields(record_id, fields)
            master_id = await feishu.create_record(config.T_KOL, master_fields)
            contact = {"record_id": master_id, "fields": master_fields}
            contact = await _verify_controlled_master(contact, email)
            master_created = True
            matches = await _exact_kol_email_matches(email)
            match_ids = {str(row.get("record_id") or "") for row in matches}
            if match_ids != {master_id}:
                raise RuntimeError(
                    "duplicate KOL email detected after controlled create; stop before follow-up/card"
                )

        followup = await _find_existing_followup(record_id)
        if followup:
            followup_id = str(followup.get("record_id") or "")
        else:
            followup_id = await feishu.create_record(
                config.T_KOL_FU, inbound_followup_fields(record_id, fields, contact),
            )

        card_result = await send_intake_card_once(
            record_id, fields, contact=contact, contact_type="KOL",
            followup_id=followup_id,
        )
        return {
            "ok": True,
            "workflow_type": "kol_intake_review",
            "contact_type": "KOL",
            "contact_record_id": str(contact.get("record_id") or ""),
            "kol_master_created": master_created,
            "followup_record_id": followup_id,
            "draft_record_id": "",
            "message_ids": card_result.get("message_ids") or [],
            "sent_to": card_result.get("sent_to") or [],
            "emails_sent": 0,
        }


def build_intake_card(record_id: str, fields: dict, *, contact: dict,
                      followup_id: str) -> dict:
    """KOL intake card: actionable in the real KOL records, never sends mail."""
    brand = _text(fields.get("品牌")) or "未识别品牌"
    customer = _text(fields.get("客户标识")) or "未识别联系邮箱"
    summary = _text(fields.get("客诉摘要")) or "红人/内容创作者提出合作事项。"
    ticket = _text(fields.get("工单ID")) or record_id
    contact_fields = (contact or {}).get("fields") or {}
    contact_id = str((contact or {}).get("record_id") or "")
    name = _text(contact_fields.get("账号名")) or customer
    status = _text(contact_fields.get("合作状态")) or "未填写"
    route_state = _text(contact_fields.get("触达路由状态")) or "未填写"
    data_state = _text(contact_fields.get("资料可用状态")) or "未填写"
    platform = _text(contact_fields.get("主平台")) or "待补"
    country = _text(contact_fields.get("国家原文")) or _text(contact_fields.get("国家")) or "待补"
    followup_url = (f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
                    f"?table={config.T_KOL_FU}&record={followup_id}")
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": f"🟠 [KOL·P1] 入站合作申请待核对 · {brand}"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "**🧭 当前阶段:** 入站合作申请 / 资料核对"}},
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**KOL:** {name}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**品牌:** {brand}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**邮箱:** {customer}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**平台:** {platform}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**国家:** {country}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**合作状态:** {status}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**触达路由:** {route_state}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**资料状态:** {data_state}"}},
            ]},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**原客服工单:** `{ticket}`\n**来信摘要:** {summary[:900]}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "**当前轮到谁:** KOL 运营审核/补资料\n"
                "**现在是否需要我方回复/操作:** 是（先核对，当前没有可发送邮件草稿）\n"
                "**只需判断:** 主页真实性、近 3 个月内容品类/语言/地区、历史关系、产品与合作条件是否齐全。\n"
                "**回填位置:** KOL 主记录补主页/平台/国家/资料状态；跟进记录填写审核结论。\n"
                "**触发条件:** 资料和合作条件齐全后，再从 KOL 系统生成标准回复草稿。\n"
                "**截止时间:** 收到卡片后 1 个工作日内"
            )}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开 KOL 主记录"},
                 "type": "primary", "url": _contact_url("KOL", contact_id)},
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开跟进记录"},
                 "type": "default", "url": followup_url},
                {"tag": "button", "text": {"tag": "plain_text", "content": "查看原客服工单"},
                 "type": "default", "url": _ticket_url(record_id)},
            ]},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": (
                "已进入 KOL 主表和跟进表；未生成可发送草稿，也不会自动向联系人发邮件。"
            )}]},
        ],
    }


def _history_marker_values(fields: dict, marker: str) -> list[str]:
    history = _text(fields.get("沟通历史摘要"))
    for line in history.splitlines():
        if not line.startswith(marker):
            continue
        try:
            parsed = json.loads(line[len(marker):])
        except json.JSONDecodeError:
            return []
        return [str(value) for value in parsed if str(value)] if isinstance(parsed, list) else []
    return []


async def _write_source_history(record_id: str, history: str) -> None:
    from . import cs_ingest
    await feishu.api(
        "PUT",
        f"/bitable/v1/apps/{cs_ingest.CS_APP_TOKEN}/tables/{cs_ingest.T_TICKET}/records/{record_id}",
        {"fields": {"沟通历史摘要": history}},
        which="notify",
    )


async def send_intake_card_once(record_id: str, fields: dict, *, contact: dict,
                                contact_type: str, followup_id: str) -> dict:
    """Send one KOL intake card per ticket with a durable fail-closed claim."""
    existing = _history_marker_values(fields, INTAKE_RECEIPT_MARKER)
    if existing:
        return {"ok": True, "duplicate": True, "message_ids": existing, "sent_to": []}
    history = _text(fields.get("沟通历史摘要"))
    attempt_line = f"{INTAKE_ATTEMPT_MARKER}{record_id}"
    if attempt_line in history:
        raise RuntimeError("previous KOL intake card attempt needs reconciliation; automatic resend blocked")
    claimed_history = _prepend_history(history, attempt_line)
    await _write_source_history(record_id, claimed_history)

    card = build_intake_card(record_id, fields, contact=contact, followup_id=followup_id)
    targets = await feishu.resolve_partnership_targets("reviewer")
    if not targets:
        raise RuntimeError("KOL reviewer target is empty")
    message_ids = []
    sent_to = []
    recipients = [(name, "union_id", union_id) for name, union_id in targets]
    for name, receive_type, receive_id in recipients:
        digest = hashlib.sha256(f"{record_id}:{receive_type}:{receive_id}".encode()).hexdigest()[:20]
        message_id = await feishu.send_card_message(
            receive_type, receive_id, card,
            biz="KOL", level="P1", which="kol_assistant",
            message_uuid=f"cs-kol-intake-{digest}",
        )
        if not message_id:
            raise RuntimeError(f"KOL intake card send failed for {name}; automatic resend blocked")
        message_ids.append(message_id)
        sent_to.append(name)

    receipt = f"{INTAKE_RECEIPT_MARKER}{json.dumps(message_ids, ensure_ascii=False, separators=(',', ':'))}"
    completed_history = _prepend_history(claimed_history, receipt)
    await _write_source_history(record_id, completed_history)
    return {
        "ok": True,
        "record_id": record_id,
        "message_ids": message_ids,
        "sent_to": sent_to,
        "contact_type": contact_type,
        "contact_record_id": str((contact or {}).get("record_id") or ""),
        "customer_writes": 0,
        "emails_sent": 0,
    }


def _prepend_history(history: str, line: str) -> str:
    current = str(history or "").strip()
    return (line + ("\n" + current if current else ""))[:5000]


def mark_pending_fields(fields: dict) -> dict:
    """Persist a retryable state in the same write that creates the CS row."""
    result = dict(fields or {})
    history = _text(result.get("沟通历史摘要"))
    if WORKFLOW_MARKER not in history and PENDING_MARKER not in history:
        result["沟通历史摘要"] = _prepend_history(history, PENDING_MARKER)
    return result


async def record_workflow_marker(record_id: str, fields: dict, workflow: dict,
                                 *, run_id: str) -> dict:
    """Persist KOL master/follow-up receipt without losing card-attempt evidence."""
    from . import cs_ingest
    followup_id = str(workflow.get("followup_record_id") or "")
    if not followup_id:
        raise ValueError("KOL intake workflow is missing followup_record_id")
    marker = f"{WORKFLOW_MARKER}{followup_id}"
    note = (
        f"系统接入({run_id}): 已进入标准KOL流程；联系人={workflow.get('contact_record_id','')}；"
        f"跟进={followup_id}；可发送草稿=0；自动外发=0。\n{marker}"
    )
    path = (f"/bitable/v1/apps/{cs_ingest.CS_APP_TOKEN}/tables/"
            f"{cs_ingest.T_TICKET}/records/{record_id}")
    latest = await feishu.api("GET", path, which="notify")
    latest_fields = (((latest.get("data") or {}).get("record") or {}).get("fields") or {})
    previous = _text(latest_fields.get("沟通历史摘要")).replace(PENDING_MARKER, COMPLETED_MARKER)
    history = _prepend_history(previous, note)
    await feishu.api(
        "PUT", path,
        {"fields": {"沟通历史摘要": history}},
        which="notify",
    )
    return {"marker": marker, "history": history}


def _marker_message_ids(fields: dict) -> list[str]:
    history = _text(fields.get("沟通历史摘要"))
    for line in history.splitlines():
        if not line.startswith(HANDOFF_MARKER):
            continue
        try:
            parsed = json.loads(line[len(HANDOFF_MARKER):])
        except json.JSONDecodeError:
            return []
        return [str(mid) for mid in parsed if str(mid)] if isinstance(parsed, list) else []
    return []


def _workflow_followup_id(fields: dict) -> str:
    history = _text(fields.get("沟通历史摘要"))
    for line in history.splitlines():
        if line.startswith(WORKFLOW_MARKER):
            return line[len(WORKFLOW_MARKER):].strip()
    return ""


def _legacy_migrated_card(contact_id: str, followup_id: str) -> dict:
    contact_url = _contact_url("KOL", contact_id)
    followup_url = (f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
                    f"?table={config.T_KOL_FU}&record={followup_id}")
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": "✅ 已迁移至标准 KOL 工作流"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "该旁路审核卡已停用。系统已建立/复用 KOL 主记录并写入跟进记录；"
                "请以后只在 KOL 入站审核卡处理，资料核对完成后再生成回复草稿。"
            )}},
            {"tag": "action", "actions": [{
                "tag": "button", "text": {"tag": "plain_text", "content": "打开 KOL 主记录"},
                "type": "primary", "url": contact_url,
            }, {
                "tag": "button", "text": {"tag": "plain_text", "content": "打开跟进记录"},
                "type": "default", "url": followup_url,
            }]},
        ],
    }


async def close_legacy_review_cards(message_ids: list[str], contact_id: str,
                                    followup_id: str) -> dict:
    results = []
    card = _legacy_migrated_card(contact_id, followup_id)
    for message_id in message_ids:
        ok = await feishu.update_card_message_with_app(
            str(message_id), card, which="kol_assistant",
        )
        results.append({"message_id": str(message_id), "ok": bool(ok)})
    return {
        "ok": all(item["ok"] for item in results),
        "updated": sum(item["ok"] for item in results),
        "results": results,
    }


def _resolved_customer_email(fields: dict) -> str:
    from . import cs_ingest
    current = _text(fields.get("客户标识"))
    resolved = cs_ingest._message_customer_email({
        "frm": current,
        "subj": _text(fields.get("主题")),
        "body": _text(fields.get("原文")),
    })
    return resolved or current


async def retry_pending_handoffs(limit: int = 20) -> dict:
    """Compensate failed post-ingest standard KOL workflow creation."""
    from . import cs_ingest
    maximum = max(1, min(int(limit or 20), 100))
    path = (f"/bitable/v1/apps/{cs_ingest.CS_APP_TOKEN}/tables/"
            f"{cs_ingest.T_TICKET}/records/search?page_size={maximum}")
    body = {"filter": {"conjunction": "and", "conditions": [
        {"field_name": "状态", "operator": "is", "value": ["归档非客服"]},
        {"field_name": "沟通历史摘要", "operator": "contains", "value": [PENDING_MARKER]},
    ]}}
    response = await feishu.api("POST", path, body, which="notify")
    rows = ((response.get("data") or {}).get("items") or [])[:maximum]
    results = []
    for row in rows:
        record_id = str(row.get("record_id") or "")
        fields = row.get("fields") or {}
        if not record_id or _workflow_followup_id(fields):
            continue
        try:
            customer = _resolved_customer_email(fields)
            if customer and customer != _text(fields.get("客户标识")):
                await feishu.api(
                    "PUT",
                    f"/bitable/v1/apps/{cs_ingest.CS_APP_TOKEN}/tables/"
                    f"{cs_ingest.T_TICKET}/records/{record_id}",
                    {"fields": {"客户标识": customer}}, which="notify",
                )
                fields = {**fields, "客户标识": customer}
            contact, contact_type = await lookup_contact(customer)
            workflow = await ensure_kol_workflow(
                record_id, fields, contact=contact, contact_type=contact_type or "",
            )
            if not workflow.get("ok"):
                raise RuntimeError(workflow.get("error") or "standard KOL workflow failed")
            await record_workflow_marker(record_id, fields, workflow, run_id="retry")
            results.append({"record_id": record_id, "ok": True,
                            "message_ids": workflow.get("message_ids") or [],
                            "followup_record_id": workflow.get("followup_record_id")})
        except Exception as exc:
            results.append({"record_id": record_id, "ok": False,
                            "error": f"{type(exc).__name__}: {str(exc)[:180]}"})
    return {"ok": all(item.get("ok") for item in results),
            "attempted": len(results),
            "sent": sum(bool(item.get("ok")) for item in results),
            "results": results}


async def correct_confirmed_ticket(record_id: str, *, dry_run: bool = True,
                                   confirm: bool = False, run_id: str = "") -> dict:
    """Correct one of the three explicitly confirmed 2026-10-07 misroutes."""
    from . import cs_dispatch, cs_ingest
    if record_id not in CONFIRMED_REPAIR_RECORD_IDS:
        raise ValueError("record_id is outside the confirmed three-ticket repair scope")
    if not dry_run and (not confirm or not run_id.strip()):
        raise ValueError("commit requires confirm=True and run_id")

    path = (f"/bitable/v1/apps/{cs_ingest.CS_APP_TOKEN}/tables/"
            f"{cs_ingest.T_TICKET}/records/{record_id}")
    raw_record = await feishu.api("GET", path, which="notify")
    fields = (((raw_record.get("data") or {}).get("record") or {}).get("fields") or {})
    if not fields:
        raise ValueError("ticket not found")
    signal = {"subj": _text(fields.get("客诉摘要")), "body": _text(fields.get("原文"))}
    if not cs_ingest._is_kol_collaboration_message(signal):
        raise ValueError("ticket no longer satisfies the deterministic KOL gate")

    customer = _resolved_customer_email(fields)
    contact, contact_type = await lookup_contact(customer)
    corrected_summary = _text(fields.get("客诉摘要"))
    if not corrected_summary.startswith("[→KOL红人]"):
        corrected_summary = f"[→KOL红人] {corrected_summary}"
    update = {
        "状态": "归档非客服",
        "销售平台": "未知",
        "分配运营": "",
        "AI草稿": "",
        "AI置信度": "必须人工",
        "客诉摘要": corrected_summary[:500],
        "沟通历史摘要": mark_pending_fields(fields).get("沟通历史摘要", ""),
    }
    if customer:
        update["客户标识"] = customer
    preview = {
        "ok": True,
        "dry_run": dry_run,
        "record_id": record_id,
        "update": update,
        "contact_type": contact_type or "new_creator",
        "contact_record_id": str((contact or {}).get("record_id") or ""),
        "existing_handoff_message_ids": _marker_message_ids(fields),
        "customer_writes": 0,
        "kol_master_creates": 0 if contact else 1,
        "emails_sent": 0,
    }
    if dry_run:
        preview_contact = contact or {
            "record_id": "<controlled-kol-record>",
            "fields": controlled_kol_fields(record_id, {**fields, **update}),
        }
        preview["card"] = build_intake_card(
            record_id, {**fields, **update}, contact=preview_contact,
            followup_id="<kol-followup-record>",
        )
        preview["workflow"] = {
            "master": "reuse" if contact else "create-controlled",
            "followup": "create-or-reuse",
            "draft": "none-until-profile-and-terms-reviewed",
            "card": "kol-intake-human-review",
            "automatic_email": False,
        }
        return preview

    await feishu.api("PUT", path, {"fields": update}, which="notify")
    verify_raw = await feishu.api("GET", path, which="notify")
    verified_fields = (((verify_raw.get("data") or {}).get("record") or {}).get("fields") or {})
    if (_text(verified_fields.get("状态")) != "归档非客服"
            or _text(verified_fields.get("客户标识")) != customer
            or _text(verified_fields.get("分配运营"))):
        raise RuntimeError("ticket correction readback failed")

    old_cards = await cs_dispatch.resolve_ticket_to_kol(record_id)
    if not old_cards.get("ok"):
        raise RuntimeError("old customer-service card could not be closed")

    workflow = await ensure_kol_workflow(
        record_id, verified_fields, contact=contact, contact_type=contact_type or "",
    )
    if not workflow.get("ok"):
        raise RuntimeError(workflow.get("error") or "standard KOL workflow failed")
    if not _workflow_followup_id(verified_fields):
        await record_workflow_marker(
            record_id, verified_fields, workflow, run_id=run_id,
        )

    legacy_mids = _marker_message_ids(verified_fields)
    legacy_cards = await close_legacy_review_cards(
        legacy_mids, workflow["contact_record_id"], workflow["followup_record_id"],
    ) if legacy_mids else {"ok": True, "updated": 0, "results": []}
    if not legacy_cards.get("ok"):
        raise RuntimeError("legacy KOL quarantine card could not be closed")

    final_raw = await feishu.api("GET", path, which="notify")
    final_fields = (((final_raw.get("data") or {}).get("record") or {}).get("fields") or {})
    final_followup_id = _workflow_followup_id(final_fields)
    if final_followup_id != workflow["followup_record_id"]:
        raise RuntimeError("standard KOL workflow marker readback failed")
    return {**preview, "dry_run": False, "verified": True,
            "old_cards": old_cards, "workflow": workflow,
            "workflow_followup_id": final_followup_id,
            "legacy_cards": legacy_cards,
            "handoff_message_ids": workflow.get("message_ids") or []}
