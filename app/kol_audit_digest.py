# -*- coding: utf-8 -*-
"""Unified KOL outbound audit digest.

Runs the status-consistency audit and duplicate-draft audit, then sends one
operator-readable card only when there is actionable abnormal state.
"""
from typing import Any, Dict, List

from . import config, draft_duplicate_audit, draft_status_audit, feishu


def _mode_label(dry_run: bool, auto_fix: bool) -> str:
    if dry_run:
        return "DRY-RUN"
    return "AUTO-FIX" if auto_fix else "REPORT"


def _status_actionable(status: Dict[str, Any], notify_report_only: bool) -> int:
    count = int(status.get("fixed_count") or 0)
    count += int(status.get("update_error_count") or 0)
    if notify_report_only:
        count += int(status.get("report_only_count") or 0)
    return count


def _duplicate_actionable(dup: Dict[str, Any], dry_run: bool) -> int:
    count = int(dup.get("update_error_count") or 0)
    if dry_run:
        count += int(dup.get("auto_fixable_count") or 0)
    else:
        count += int(dup.get("fixed_count") or 0)
    return count


def _line_status(item: Dict[str, Any]) -> str:
    did = item.get("draft_id") or item.get("record_id") or "-"
    source = item.get("source") or "-"
    state = item.get("draft_status") or "-"
    reason = item.get("reason") or "-"
    return f"- `{did}` · {source} · 草稿状态={state} · {reason}"


def _line_duplicate(item: Dict[str, Any]) -> str:
    did = item.get("draft_id") or item.get("record_id") or "-"
    source = item.get("source") or "-"
    brand = item.get("brand") or "-"
    reason = item.get("reason") or ""
    return f"- `{did}` · {source} · {brand} · {reason}"


def _sample_lines(status: Dict[str, Any], dup: Dict[str, Any], limit: int) -> str:
    lines: List[str] = []
    status_items = (
        status.get("update_errors")
        or status.get("report_only_sample")
        or status.get("fixed_sample")
        or status.get("issues_sample")
        or []
    )
    if status_items:
        lines.append("**草稿状态样例**")
        lines.extend(_line_status(x) for x in status_items[:limit])

    dup_items = (
        dup.get("update_errors")
        or dup.get("fixed_sample")
        or dup.get("auto_fixable_sample")
        or []
    )
    if dup_items:
        if lines:
            lines.append("")
        lines.append("**重复草稿样例**")
        lines.extend(_line_duplicate(x) for x in dup_items[:limit])
    return "\n".join(lines)[:1800] if lines else "无需要运营处理的样例。"


async def run(dry_run: bool = False, auto_fix: bool = True,
              notify: bool = True, notify_clean: bool = False,
              notify_report_only: bool = False,
              sample_limit: int = 5) -> Dict[str, Any]:
    """Run both audits and send one card only when needed.

    This endpoint never sends email. With auto_fix=true it only writes the
    narrow safe fields inside the two underlying audit modules.
    """
    status = await draft_status_audit.run(
        dry_run=dry_run,
        auto_fix=auto_fix,
        notify=False,
        notify_report_only=False,
        sample_limit=sample_limit,
    )
    dup = await draft_duplicate_audit.run(
        dry_run=dry_run,
        auto_fix=auto_fix,
        notify=False,
        notify_report_only=False,
        sample_limit=sample_limit,
    )

    actionable = _status_actionable(status, notify_report_only)
    actionable += _duplicate_actionable(dup, dry_run)
    if notify_report_only:
        actionable += int(dup.get("report_only_actionable_count") or 0)

    result = {
        "dry_run": dry_run,
        "auto_fix": auto_fix,
        "notify_clean": notify_clean,
        "notify_report_only": notify_report_only,
        "actionable_count": actionable,
        "status_audit": status,
        "duplicate_audit": dup,
        "notified": 0,
    }

    if notify and (actionable > 0 or notify_clean):
        result["notified"] = await _notify(result, sample_limit=sample_limit)
    return result


async def _notify(result: Dict[str, Any], sample_limit: int = 5) -> int:
    status = result["status_audit"]
    dup = result["duplicate_audit"]
    dry_run = bool(result.get("dry_run"))
    auto_fix = bool(result.get("auto_fix"))
    notify_report_only = bool(result.get("notify_report_only"))

    write_errors = int(status.get("update_error_count") or 0) + int(dup.get("update_error_count") or 0)
    manual = int(status.get("report_only_count") or 0) if notify_report_only else 0
    resolved = int(status.get("fixed_count") or 0) + int(dup.get("fixed_count") or 0)
    pending = 0
    if dry_run:
        pending = int(status.get("auto_fixable_count") or 0) + int(dup.get("auto_fixable_count") or 0)

    if write_errors:
        level, template, conclusion = "P1", "red", "写入失败，需要排查"
    elif manual:
        level, template, conclusion = "P1", "orange", "存在仅报出项，需要人工判断"
    elif pending:
        level, template, conclusion = "P1", "orange", "dry-run 发现待处理异常"
    elif resolved:
        level, template, conclusion = "P2", "blue", "异常已自动处理"
    else:
        level, template, conclusion = "P3", "green", "无异常"

    mode = _mode_label(dry_run, auto_fix)
    summary = (
        f"**结论**: {conclusion}\n"
        f"**模式**: {mode}\n"
        f"**草稿状态一致性**: 扫描 {status.get('scanned')} 条 / 异常 {status.get('issue_count')} 条 / "
        f"已回填 {status.get('fixed_count')} 条 / 仅报出 {status.get('report_only_count')} 条 / "
        f"写入失败 {status.get('update_error_count')} 条\n"
        f"**重复草稿**: 扫描 {dup.get('scanned')} 条 / 危险待发重复 {dup.get('auto_fixable_count')} 条 / "
        f"已否决 {dup.get('fixed_count')} 条 / 写入失败 {dup.get('update_error_count')} 条\n"
        f"**历史重复组**: {dup.get('report_only_group_count')} 组仅留档，不代表本轮要处理\n"
        f"**静默规则**: 无异常时不推送卡片"
    )

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": "KOL 发信链审计摘要"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": summary[:1800]}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md",
                                    "content": _sample_lines(status, dup, sample_limit)}},
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
            print(f"[kol_audit_digest] notify {name} fail: {exc}")
    return sent
