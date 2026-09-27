"""Narrow callback authorization for the temporary independent-site handoff."""
from . import config, feishu

HANDOFF_ACTIONS = frozenset({
    "draft_approve", "draft_reject", "draft_regen", "draft_tracking",
    "draft_uploadreg", "warm_recap_send",
})


async def authorize_kol(data: dict) -> dict:
    value = data.get("card_action") or {}
    action = value.get("action") or ""
    if not config.KOL_PARTNERSHIP_JOB_TITLE or action not in HANDOFF_ACTIONS:
        return {"allowed": True, "scoped": False}
    table = value.get("table_id") or config.T_DRAFT
    rid = value.get("record_id") or ""
    allowed_tables = {config.T_KOL, config.T_EDITOR} if action == "draft_uploadreg" else {config.T_DRAFT}
    if (not rid or table not in allowed_tables
            or value.get("app_token", config.FEISHU_APP_TOKEN) != config.FEISHU_APP_TOKEN):
        return {"allowed": False, "scoped": True, "reason": "invalid_record_scope"}
    try:
        record = await feishu.get_record(table, rid)
        fields = record.get("fields") or {}
        if not fields:
            raise ValueError("record_missing")
        scoped = action == "draft_uploadreg" or feishu.is_existing_partnership_draft(fields)
        if not scoped:
            return {"allowed": True, "scoped": False}
        identity = "kol_assistant" if value.get("_delivery_identity") == "kol_assistant" else "app3"
        uid = await feishu.open_id_to_union_id(data.get("sender_open_id") or "", which=identity)
        # Frankie retains escalation responsibility, not routine task ownership.
        permitted = {config.KOL_ASSISTANT_FRANKIE_UNION_ID}
        permitted.update(uid for _, uid in await feishu.resolve_partnership_targets())
        if action == "draft_tracking" and not config.KOL_PARTNERSHIP_FRANKIE_ONLY:
            permitted.update(uid for _, uid in await feishu.resolve_notify_targets("ship_cc"))
        permitted.discard("")
        return {"allowed": bool(uid and uid in permitted), "scoped": True,
                "reason": "" if uid and uid in permitted else "not_current_owner"}
    except Exception:
        return {"allowed": False, "scoped": True, "reason": "identity_or_record_lookup_failed"}
