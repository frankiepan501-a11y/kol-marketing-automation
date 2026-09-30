"""NYXI session ownership; no production release or global notification switch.

The marker belongs to our persisted draft ID, never inferred from subject/body,
brand, sender or competitor tags. These guards only HOLD; they cannot send.
"""
from .feishu import ext

DRAFT_PREFIX = "nyxi-trial-20260930-"


def owns_draft(record: dict) -> bool:
    value = ext(record.get("fields", {}).get("邮件草稿ID"))
    return value.startswith(DRAFT_PREFIX) and bool(value[len(DRAFT_PREFIX):].strip())


def hold_result(record: dict) -> dict:
    return {"ok": False, "rid": record.get("record_id"), "skipped": True,
            "reason": "nyxi_session_owned",
            "error": "NYXI试跑由本任务处理；原自动流程不发送、不发卡"}


CONTACT_MARKER = "[NYXI_TRIAL:20260930]"


def owns_contact(record: dict) -> bool:
    # Explicit operational reservation, never the broad 合作竞品=NYXI label.
    return CONTACT_MARKER in ext(record.get("fields", {}).get("迁移备注"))


def legacy_rows(rows: list, *, contacts: bool = False) -> list:
    predicate = owns_contact if contacts else owns_draft
    return [r for r in rows if not predicate(r)]



def reservation_patch(contact: dict, related_drafts: list, related_participants: list) -> dict:
    """Prepare only; caller must supply a fresh COMPLETE contact draft history.

    Existing partnerships never transfer implicitly to the trial. Text is
    written and verified before enum updates by the eventual registration step.
    """
    f = contact.get("fields", {})
    if related_participants:
        raise ValueError("prior activity decisions require original-record review")
    if any(not owns_draft(d) for d in related_drafts):
        raise ValueError("legacy draft history requires original-thread handling")
    if ext(f.get("合作状态")) not in {"", "未建联"}:
        raise ValueError("existing relationship cannot be reserved")
    if ext(f.get("邮箱验真状态")) in {"无效", "风险"}:
        raise ValueError("email stop condition")
    if any(f.get(k) for k in ("上稿日期", "上次寄样日期", "上次寄样订单号", "寄样次数")):
        raise ValueError("existing fulfillment cannot be reserved")
    if ext(f.get("触达路由状态")) in {"沿用原线程", "禁止新开发"}:
        raise ValueError("existing routing restriction")
    note = ext(f.get("迁移备注"))
    if not owns_contact(contact):
        note = (note + "\n[CONTROLLED_IMPORT] " + CONTACT_MARKER).strip()
    if len(note) > 1000:
        raise ValueError("reservation must not truncate existing notes")
    return {"迁移备注": note, "触达路由状态": "待核对"}


RECEIPT_PREFIX = "[NYXI_RECEIPT] "


async def persist_accepted_receipt(draft_id: str, brand: str, message_id: str):
    """on_accepted hook: persist API acceptance before sent-content verification.

    This is NOT a send entry point. The sender must durably reserve an attempt
    before submitting to Zoho, and reconcile ambiguous requests without retries.
    Only writes existing text fields, so failed enum writes cannot lose the ID.
    """
    import json
    from . import config, feishu
    if not message_id:
        raise ValueError("message ID required")
    record = await feishu.get_record(config.T_DRAFT, draft_id)
    if not owns_draft(record):
        raise ValueError("receipt destination is not a NYXI draft")
    old = ext(record["fields"].get("发送错误"))
    payload = {"state": "api_accepted_verification_pending", "brand": brand,
               "message_id": message_id, "do_not_resend": True}
    line = RECEIPT_PREFIX + json.dumps(payload, ensure_ascii=True, sort_keys=True)
    if RECEIPT_PREFIX in old:
        if line not in old:
            raise ValueError("different receipt already exists; reconcile only")
        return payload
    value = (old + "\n" + line).strip()
    if len(value) > 10000:
        raise ValueError("receipt would truncate existing error history")
    await feishu.update_record(config.T_DRAFT, draft_id, {"发送错误": value})
    saved = await feishu.get_record(config.T_DRAFT, draft_id)
    if line not in ext(saved.get("fields", {}).get("发送错误")):
        raise RuntimeError("receipt read-back failed; do not resend")
    return payload
