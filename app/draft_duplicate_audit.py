# -*- coding: utf-8 -*-
"""Duplicate draft audit for KOL/media outreach.

This module never sends email. It reports duplicate outreach drafts and can
auto-deny only the ready unsent duplicates, leaving already-sent records as
history for audit/reply matching.
"""
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import config, feishu
from .feishu import ext, xrid


READY_DRAFT_STATUSES = {"通过", "自动通过"}
SENT_SEND_STATUSES = {"已发", "已发送"}
UNSENT_SEND_STATUSES = {"", "未发"}
COLD_SOURCES = {"cold", "followup"}

FIELD_NAMES = [
    "邮件草稿ID",
    "邮件草稿状态",
    "发送状态",
    "发送时间",
    "邮件草稿来源",
    "对象类型",
    "关联KOL",
    "关联媒体人",
    "关联产品",
    "发送邮箱",
    "收件邮箱",
    "邮件主题",
    "生成时间",
    "建议发送时间",
    "审批意见",
]


def _as_int(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
    return 0


def draft_id_of(fields: Dict[str, Any]) -> str:
    return ext(fields.get("邮件草稿ID")).strip()


def contact_rid_of(fields: Dict[str, Any]) -> str:
    return xrid(fields.get("关联KOL")) or xrid(fields.get("关联媒体人")) or ""


def product_rid_of(fields: Dict[str, Any]) -> str:
    return xrid(fields.get("关联产品")) or ""


def brand_of(fields: Dict[str, Any]) -> str:
    return config.brand_from_text(ext(fields.get("发送邮箱"))) or "FUNLAB"


def is_ready_unsent(fields: Dict[str, Any]) -> bool:
    return (
        ext(fields.get("邮件草稿状态")) in READY_DRAFT_STATUSES
        and ext(fields.get("发送状态")) in UNSENT_SEND_STATUSES
    )


def is_sent(fields: Dict[str, Any]) -> bool:
    return ext(fields.get("发送状态")) in SENT_SEND_STATUSES


def cold_key_of(fields: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    source = ext(fields.get("邮件草稿来源")) or "cold"
    if source not in COLD_SOURCES:
        return None
    contact = contact_rid_of(fields)
    product = product_rid_of(fields)
    if not contact or not product:
        return None
    return source, contact, product, brand_of(fields)


def ready_order(rec: Dict[str, Any]) -> Tuple[int, int, str]:
    fields = rec.get("fields") or {}
    sched = _as_int(fields.get("建议发送时间"))
    gen = _as_int(fields.get("生成时间"))
    return sched or gen or 0, gen or 0, rec.get("record_id", "")


def _record_brief(rec: Dict[str, Any]) -> Dict[str, Any]:
    fields = rec.get("fields") or {}
    return {
        "record_id": rec.get("record_id", ""),
        "draft_id": draft_id_of(fields),
        "draft_status": ext(fields.get("邮件草稿状态")) or "空",
        "send_status": ext(fields.get("发送状态")) or "空",
        "source": ext(fields.get("邮件草稿来源")) or "空",
        "contact_rid": contact_rid_of(fields),
        "product_rid": product_rid_of(fields),
        "brand": brand_of(fields),
        "email": ext(fields.get("收件邮箱")) or "",
        "subject": (ext(fields.get("邮件主题")) or "")[:120],
        "send_time": _as_int(fields.get("发送时间")),
        "ready": is_ready_unsent(fields),
        "sent": is_sent(fields),
    }


def build_duplicate_groups(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return duplicate groups by exact draft_id and cold business key."""
    by_draft_id = defaultdict(list)
    by_cold_key = defaultdict(list)

    for rec in records:
        fields = rec.get("fields") or {}
        did = draft_id_of(fields)
        if did:
            by_draft_id[did].append(rec)
        ckey = cold_key_of(fields)
        if ckey:
            by_cold_key[ckey].append(rec)

    groups = []
    for did, recs in by_draft_id.items():
        if len(recs) > 1:
            groups.append({
                "group_type": "draft_id",
                "group_key": did,
                "records": sorted(recs, key=ready_order),
            })
    for ckey, recs in by_cold_key.items():
        if len(recs) > 1:
            source, contact, product, brand = ckey
            groups.append({
                "group_type": "cold_key",
                "group_key": f"{source}|{contact}|{product}|{brand}",
                "records": sorted(recs, key=ready_order),
            })
    return groups


def plan_auto_denials(groups: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Plan safe auto-denials for ready unsent duplicate records.

    Rules:
    - Seed sent draft IDs / cold keys from already-sent records.
    - Walk ready records by send order.
    - Keep the first ready record for each draft ID and cold business key.
    - Deny later ready records that collide with a sent or already-kept key.

    This mirrors app.auto_send.scan_ready. It avoids over-denying chained
    duplicates such as A~B by cold key and B~C by draft ID; if B is denied,
    C can remain as the first kept record for its draft ID.
    """
    unique_records: Dict[str, Dict[str, Any]] = {}
    rid_to_groups = defaultdict(list)
    for group in groups:
        marker = {"group_type": group["group_type"], "group_key": group["group_key"]}
        for rec in group["records"]:
            rid = rec.get("record_id", "")
            if not rid:
                continue
            unique_records[rid] = rec
            rid_to_groups[rid].append(marker)

    sent_draft_ids = set()
    sent_cold_keys = set()
    for rec in unique_records.values():
        fields = rec.get("fields") or {}
        if not is_sent(fields):
            continue
        did = draft_id_of(fields)
        if did:
            sent_draft_ids.add(did)
        ckey = cold_key_of(fields)
        if ckey:
            sent_cold_keys.add(ckey)

    plan: Dict[str, Dict[str, Any]] = {}
    run_draft_ids = set()
    run_cold_keys = set()

    for rec in sorted(unique_records.values(), key=ready_order):
        rid = rec.get("record_id", "")
        fields = rec.get("fields") or {}
        if not rid or not is_ready_unsent(fields):
            continue

        did = draft_id_of(fields)
        ckey = cold_key_of(fields)
        reason = ""
        if did and did in sent_draft_ids:
            reason = f"同一邮件草稿ID {did} 已有已发记录"
        elif did and did in run_draft_ids:
            reason = f"本批同一邮件草稿ID {did} 已保留更早一条"
        elif ckey and ckey in sent_cold_keys:
            reason = "同一联系人×产品×品牌 cold/followup 已有已发记录"
        elif ckey and ckey in run_cold_keys:
            reason = "本批同一联系人×产品×品牌 cold/followup 已保留更早一条"

        if reason:
            plan[rid] = {
                "record": rec,
                "reason": reason[:500],
                "groups": rid_to_groups.get(rid, [])[:5],
            }
            continue

        if did:
            run_draft_ids.add(did)
        if ckey:
            run_cold_keys.add(ckey)

    return plan


def _group_summary(group: Dict[str, Any]) -> Dict[str, Any]:
    recs = group["records"]
    return {
        "group_type": group["group_type"],
        "group_key": group["group_key"],
        "count": len(recs),
        "ready_count": sum(1 for r in recs if is_ready_unsent(r.get("fields") or {})),
        "sent_count": sum(1 for r in recs if is_sent(r.get("fields") or {})),
        "records": [_record_brief(r) for r in recs[:8]],
    }


def _sample_plan(plan: Dict[str, Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    out = []
    for rid, item in list(plan.items())[:limit]:
        brief = _record_brief(item["record"])
        brief["reason"] = item["reason"]
        brief["groups"] = item["groups"][:3]
        out.append(brief)
    return out


async def _deny_ready_duplicate(record_id: str, rec: Dict[str, Any], reason: str) -> None:
    fields = rec.get("fields") or {}
    old_note = ext(fields.get("审批意见"))
    note = f"[自动去重] {reason}"
    if old_note and note not in old_note:
        note = (old_note + " | " + note)[:500]
    if note:
        await feishu.update_record(config.T_DRAFT, record_id, {"审批意见": note[:500]})
    # Single-select write isolated to avoid Feishu select-clearing behavior.
    await feishu.update_record(config.T_DRAFT, record_id, {"邮件草稿状态": "已否决"})


async def run(dry_run: bool = True, auto_fix: bool = False,
              notify: bool = False, notify_report_only: bool = False,
              sample_limit: int = 20) -> Dict[str, Any]:
    started = time.time()
    records = await feishu.search_records(config.T_DRAFT, [], field_names=FIELD_NAMES)
    groups = build_duplicate_groups(records)
    plan = plan_auto_denials(groups)

    fixed = []
    update_errors = []
    if auto_fix and not dry_run:
        for rid, item in plan.items():
            try:
                await _deny_ready_duplicate(rid, item["record"], item["reason"])
                fixed.append({
                    "record": item["record"],
                    "reason": item["reason"],
                    "groups": item["groups"],
                })
            except Exception as exc:
                update_errors.append({
                    "record_id": rid,
                    "draft_id": draft_id_of((item["record"].get("fields") or {})),
                    "reason": item["reason"],
                    "error": str(exc)[:200],
                })

    duplicate_record_ids = {
        r.get("record_id", "")
        for g in groups
        for r in g["records"]
        if r.get("record_id")
    }
    report_only_groups = [
        g for g in groups
        if not any(rid in plan for rid in [r.get("record_id", "") for r in g["records"]])
    ]

    result = {
        "dry_run": dry_run,
        "auto_fix": auto_fix,
        "notify_report_only": notify_report_only,
        "scanned": len(records),
        "duplicate_group_count": len(groups),
        "duplicate_record_count": len(duplicate_record_ids),
        "auto_fixable_count": len(plan),
        "fixed_count": len(fixed),
        "update_error_count": len(update_errors),
        "groups_sample": [_group_summary(g) for g in groups[:sample_limit]],
        "auto_fixable_sample": _sample_plan(plan, sample_limit),
        "report_only_group_count": len(report_only_groups),
        "report_only_sample": [_group_summary(g) for g in report_only_groups[:sample_limit]],
        "fixed_sample": [
            {
                **_record_brief(x["record"]),
                "reason": x["reason"],
                "groups": x["groups"][:3],
            }
            for x in fixed[:sample_limit]
        ],
        "update_errors": update_errors[:sample_limit],
        "elapsed_s": round(time.time() - started, 1),
    }

    should_notify = (
        notify
        and (
            bool(fixed)
            or bool(update_errors)
            or (notify_report_only and bool(groups))
        )
    )
    result["notified"] = await _notify(result) if should_notify else 0
    return result


def _line(item: Dict[str, Any]) -> str:
    did = item.get("draft_id") or item.get("record_id", "")
    email = item.get("email") or "?"
    reason = item.get("reason") or ""
    return f"- `{did}` · {item.get('source')} · {email} · {reason}"


async def _notify(result: Dict[str, Any]) -> int:
    level = "P1" if result.get("update_error_count") else "P2"
    template = "orange" if level == "P1" else "blue"
    mode = "DRY-RUN" if result.get("dry_run") else ("AUTO-FIX" if result.get("auto_fix") else "REPORT")
    body = (
        f"**模式**: {mode}\n"
        f"**扫描**: {result.get('scanned')} 条\n"
        f"**重复组**: {result.get('duplicate_group_count')} 组\n"
        f"**重复记录**: {result.get('duplicate_record_count')} 条\n"
        f"**可自动否决待发重复**: {result.get('auto_fixable_count')} 条\n"
        f"**已否决**: {result.get('fixed_count')} 条\n"
        f"**写入失败**: {result.get('update_error_count')} 条\n"
        f"**仅报告组**: {result.get('report_only_group_count')} 组"
    )
    detail_items = result.get("fixed_sample") or result.get("auto_fixable_sample") or []
    detail = "\n".join(_line(x) for x in detail_items[:12]) or "无"
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": "KOL 重复草稿审计"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": detail[:1800]}},
        ],
    }

    sent = 0
    for name, open_id in config.NOTIFY_USERS:
        if not name.startswith("潘"):
            continue
        try:
            await feishu.send_card_message("open_id", open_id, card, biz="AUDIT", level=level)
            sent += 1
        except Exception as exc:
            print(f"[draft_duplicate_audit] notify {name} fail: {exc}")
    return sent
