"""Safe handoff from customer-service tickets to KOL operations.

This module never sends customer/creator email and never creates a KOL master
record. It only sends a read-only internal review card through the ACTIVE KOL
media assistant and records the handoff on the original CS ticket.
"""
from __future__ import annotations

import hashlib
import json
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


def build_review_card(record_id: str, fields: dict, *, contact=None,
                      contact_type: str = "") -> dict:
    brand = _text(fields.get("品牌")) or "未识别品牌"
    customer = _text(fields.get("客户标识")) or "未识别联系邮箱"
    summary = _text(fields.get("客诉摘要")) or "红人/内容创作者提出合作事项。"
    ticket = _text(fields.get("工单ID")) or record_id
    contact_fields = (contact or {}).get("fields") or {}
    contact_id = str((contact or {}).get("record_id") or "")

    if contact_id:
        name = _text(contact_fields.get("账号名")) or customer
        status = _text(contact_fields.get("合作状态")) or "未填写"
        match_md = (f"**系统核查:** 已匹配现有{contact_type} `{name}` · 当前状态 `{status}`\n"
                    f"[打开现有{contact_type}记录]({_contact_url(contact_type, contact_id)})")
        next_step = "请在现有 KOL 线程核对历史沟通，再决定是否需要人工回复。"
        writeback = "在上方现有 KOL 记录的跟进记录中填写结论。"
    else:
        match_md = "**系统核查:** KOL 与媒体人主表均未命中；系统没有自动新建主表记录。"
        next_step = "请核查账号/主页及合作条件，确认合适后再入库并由 KOL 团队人工回复。"
        writeback = "先在原客服工单的沟通历史摘要填写结论；仅“通过并入库”时再建 KOL 记录。"

    body = (
        f"**对象:** {customer}  ·  **品牌:** {brand}\n"
        f"**原客服工单:** `{ticket}`\n"
        f"**来信摘要:** {summary[:900]}\n\n"
        f"{match_md}\n\n"
        "**当前轮到谁:** 我方需审核/操作\n"
        "**现在是否需要我方回复/操作:** 是（先内部审核；本卡不会自动外发）\n"
        "**审核标准:** 核对主页真实性、近 3 个月内容品类/语言/地区、历史合作及来信信息是否完整。\n"
        "**允许结论:** 通过并入库 / 补资料 / 排除（须写理由）\n"
        f"**回填位置:** {writeback}\n"
        "**截止时间:** 收到卡片后 1 个工作日内\n"
        f"**下一步:** {next_step}\n\n"
        f"[打开原客服工单]({_ticket_url(record_id)})\n\n"
        "_已从客服流程移除；陈翔宇无需按客诉处理。此卡无发送按钮，不会向联系人发邮件。_"
    )
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": f"创作者合作来信待审核 · {brand}"},
        },
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": body}}],
    }


async def send_review_card(record_id: str, fields: dict, *, contact=None,
                           contact_type: str = "") -> dict:
    card = build_review_card(
        record_id, fields, contact=contact, contact_type=contact_type,
    )
    targets = await feishu.resolve_partnership_targets("reviewer")
    if not targets:
        return {"ok": False, "record_id": record_id, "error": "KOL reviewer target is empty"}
    message_ids = []
    sent_to = []
    for name, union_id in targets:
        digest = hashlib.sha1(str(union_id).encode("utf-8")).hexdigest()[:10]
        message_id = await feishu.send_card_message(
            "union_id", union_id, card,
            biz="KOL", level="P1", which="kol_assistant",
            message_uuid=f"cs-kol-{record_id}-{digest}",
        )
        if not message_id:
            return {"ok": False, "record_id": record_id, "message_ids": message_ids,
                    "sent_to": sent_to, "error": f"KOL card send failed for {name}"}
        message_ids.append(message_id)
        sent_to.append(name)
    return {
        "ok": True,
        "record_id": record_id,
        "message_ids": message_ids,
        "sent_to": sent_to,
        "contact_type": contact_type or "new_creator",
        "contact_record_id": str((contact or {}).get("record_id") or ""),
        "customer_writes": 0,
        "kol_master_creates": 0,
    }


async def read_review_cards(message_ids: list[str]) -> dict:
    results = []
    for message_id in message_ids:
        response = await feishu.api(
            "GET", f"/im/v1/messages/{message_id}", which="kol_assistant",
        )
        items = (response.get("data") or {}).get("items") or []
        content = (((items[0].get("body") or {}).get("content")) if items else "") or ""
        try:
            card = json.loads(content)
        except (TypeError, json.JSONDecodeError):
            card = {}
        rendered = json.dumps(card, ensure_ascii=False)
        ok = ("创作者合作来信待审核" in rendered
              and "button" not in rendered and "form_submit" not in rendered)
        results.append({"message_id": message_id, "ok": ok})
    return {"ok": bool(results) and all(item["ok"] for item in results),
            "verified": sum(bool(item["ok"]) for item in results),
            "results": results}


def _prepend_history(history: str, line: str) -> str:
    current = str(history or "").strip()
    return (line + ("\n" + current if current else ""))[:5000]


def mark_pending_fields(fields: dict) -> dict:
    """Persist a retryable state in the same write that creates the CS row."""
    result = dict(fields or {})
    history = _text(result.get("沟通历史摘要"))
    if HANDOFF_MARKER not in history and PENDING_MARKER not in history:
        result["沟通历史摘要"] = _prepend_history(history, PENDING_MARKER)
    return result


async def record_handoff_marker(record_id: str, fields: dict, handoff: dict,
                                *, run_id: str) -> dict:
    from . import cs_ingest
    mids = [str(mid) for mid in handoff.get("message_ids") or [] if str(mid)]
    marker = f"{HANDOFF_MARKER}{json.dumps(mids, ensure_ascii=False, separators=(',', ':'))}"
    note = (f"系统纠偏({run_id}): 此邮件属于KOL/创作者合作，已从客服流程转交KOL媒体助手。\n"
            f"{marker}")
    previous = _text(fields.get("沟通历史摘要")).replace(PENDING_MARKER, COMPLETED_MARKER)
    history = _prepend_history(previous, note)
    await feishu.api(
        "PUT",
        f"/bitable/v1/apps/{cs_ingest.CS_APP_TOKEN}/tables/{cs_ingest.T_TICKET}/records/{record_id}",
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
    """Compensate failed post-ingest KOL card sends on the next mailbox run."""
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
        if not record_id or _marker_message_ids(fields):
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
            handoff = await send_review_card(
                record_id, fields, contact=contact, contact_type=contact_type or "",
            )
            if not handoff.get("ok"):
                raise RuntimeError(handoff.get("error") or "KOL review card send failed")
            await record_handoff_marker(record_id, fields, handoff, run_id="retry")
            results.append({"record_id": record_id, "ok": True,
                            "message_ids": handoff.get("message_ids") or []})
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
        "kol_master_creates": 0,
    }
    if dry_run:
        preview["card"] = build_review_card(
            record_id, {**fields, **update}, contact=contact, contact_type=contact_type or "",
        )
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
    existing_mids = _marker_message_ids(verified_fields)
    if existing_mids:
        handoff = {"ok": True, "sent": False, "duplicate": True,
                   "message_ids": existing_mids}
    else:
        handoff = await send_review_card(
            record_id, verified_fields, contact=contact, contact_type=contact_type or "",
        )
        if not handoff.get("ok"):
            raise RuntimeError(handoff.get("error") or "KOL review card send failed")
        await record_handoff_marker(record_id, verified_fields, handoff, run_id=run_id)

    final_raw = await feishu.api("GET", path, which="notify")
    final_fields = (((final_raw.get("data") or {}).get("record") or {}).get("fields") or {})
    final_mids = _marker_message_ids(final_fields)
    if not final_mids:
        raise RuntimeError("KOL handoff marker readback failed")
    card_readback = await read_review_cards(final_mids)
    if not card_readback.get("ok"):
        raise RuntimeError("KOL review card readback failed")
    return {**preview, "dry_run": False, "verified": True,
            "old_cards": old_cards, "handoff": handoff,
            "handoff_message_ids": final_mids, "card_readback": card_readback}
