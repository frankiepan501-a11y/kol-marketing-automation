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
