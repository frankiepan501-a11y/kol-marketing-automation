# -*- coding: utf-8 -*-
"""Draft status consistency audit.

Invariant:
    send status says "sent" -> draft status should be "已发送".

This module is intentionally separate from auto_send. It never sends email and
only writes the single select field "邮件草稿状态" for a narrow safe subset.
"""
import time
from typing import Any, Dict, List, Optional

from . import config, feishu
from .feishu import ext


SENT_STATUSES = {"已发", "已发送"}
FINAL_DRAFT_STATUS = "已发送"
AUTO_FIXABLE_DRAFT_STATUSES = {"", "通过", "自动通过"}

FIELD_NAMES = [
    "邮件草稿ID",
    "邮件草稿状态",
    "发送状态",
    "发送时间",
    "邮件草稿来源",
    "收件邮箱",
    "邮件主题",
    "发送错误",
    "审批意见",
]


def _as_ms(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
    return 0


def _status_label(value: str) -> str:
    return value or "空"


def classify_issue(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return an issue dict if the record violates the sent-status invariant."""
    fields = rec.get("fields") or {}
    draft_status = ext(fields.get("邮件草稿状态")) or ""
    send_status = ext(fields.get("发送状态")) or ""
    send_ms = _as_ms(fields.get("发送时间"))

    if send_status not in SENT_STATUSES:
        return None
    if draft_status == FINAL_DRAFT_STATUS:
        return None

    auto_fixable = draft_status in AUTO_FIXABLE_DRAFT_STATUSES and send_ms > 0
    if auto_fixable:
        reason = "safe_autofix"
    elif send_ms <= 0:
        reason = "missing_send_time"
    else:
        reason = "manual_review_status"

    return {
        "record_id": rec.get("record_id", ""),
        "draft_id": ext(fields.get("邮件草稿ID")) or "",
        "draft_status": _status_label(draft_status),
        "send_status": send_status,
        "send_time": send_ms,
        "source": ext(fields.get("邮件草稿来源")) or "空",
        "email": ext(fields.get("收件邮箱")) or "",
        "subject": (ext(fields.get("邮件主题")) or "")[:120],
        "reason": reason,
        "auto_fixable": auto_fixable,
    }


def _sample(items: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    return [
        {
            "record_id": x["record_id"],
            "draft_id": x["draft_id"],
            "draft_status": x["draft_status"],
            "send_status": x["send_status"],
            "send_time": x["send_time"],
            "source": x["source"],
            "email": x["email"],
            "reason": x["reason"],
        }
        for x in items[:limit]
    ]


async def run(dry_run: bool = True, auto_fix: bool = False,
              notify: bool = False, notify_report_only: bool = False,
              sample_limit: int = 20) -> Dict[str, Any]:
    started = time.time()
    records = await feishu.search_records(config.T_DRAFT, [], field_names=FIELD_NAMES)
    issues = [i for i in (classify_issue(r) for r in records) if i]
    auto_candidates = [i for i in issues if i["auto_fixable"]]
    report_only = [i for i in issues if not i["auto_fixable"]]

    fixed = []
    update_errors = []
    if auto_fix and not dry_run:
        for issue in auto_candidates:
            try:
                # Single-select field only. Keep this isolated to avoid the
                # Feishu multi-field PUT select-clearing pitfall.
                await feishu.update_record(
                    config.T_DRAFT,
                    issue["record_id"],
                    {"邮件草稿状态": FINAL_DRAFT_STATUS},
                )
                fixed.append(issue)
            except Exception as exc:
                update_errors.append({
                    "record_id": issue["record_id"],
                    "draft_id": issue["draft_id"],
                    "error": str(exc)[:200],
                })

    result = {
        "dry_run": dry_run,
        "auto_fix": auto_fix,
        "notify_report_only": notify_report_only,
        "scanned": len(records),
        "issue_count": len(issues),
        "auto_fixable_count": len(auto_candidates),
        "report_only_count": len(report_only),
        "fixed_count": len(fixed),
        "update_error_count": len(update_errors),
        "issues_sample": _sample(issues, sample_limit),
        "report_only_sample": _sample(report_only, sample_limit),
        "fixed_sample": _sample(fixed, sample_limit),
        "update_errors": update_errors[:sample_limit],
        "elapsed_s": round(time.time() - started, 1),
    }

    should_notify = (
        notify
        and (
            bool(fixed)
            or bool(update_errors)
            or (notify_report_only and bool(issues))
        )
    )
    if should_notify:
        result["notified"] = await _notify(result)
    else:
        result["notified"] = 0
    return result


def _line(issue: Dict[str, Any]) -> str:
    rid = issue.get("record_id", "")
    draft_id = issue.get("draft_id") or rid
    status = issue.get("draft_status")
    source = issue.get("source") or "空"
    email = issue.get("email") or "?"
    return f"- `{draft_id}` · {source} · 草稿状态={status} · {email}"


async def _notify(result: Dict[str, Any]) -> int:
    report_only = result.get("report_only_sample") or []
    fixed = result.get("fixed_sample") or []
    issues = result.get("issues_sample") or []
    level = "P1" if report_only or result.get("update_error_count") else "P2"
    template = "orange" if level == "P1" else "blue"

    mode = "DRY-RUN" if result.get("dry_run") else ("AUTO-FIX" if result.get("auto_fix") else "REPORT")
    body = (
        f"**模式**: {mode}\n"
        f"**扫描**: {result.get('scanned')} 条\n"
        f"**异常**: {result.get('issue_count')} 条\n"
        f"**可自动回填**: {result.get('auto_fixable_count')} 条\n"
        f"**仅报出**: {result.get('report_only_count')} 条\n"
        f"**已回填**: {result.get('fixed_count')} 条\n"
        f"**写入失败**: {result.get('update_error_count')} 条"
    )
    detail_items = report_only or fixed or issues
    detail = "\n".join(_line(x) for x in detail_items[:10]) or "无"
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": "KOL 草稿状态一致性审计"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": detail}},
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
            print(f"[draft_status_audit] notify {name} fail: {exc}")
    return sent
