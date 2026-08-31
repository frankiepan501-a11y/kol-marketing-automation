"""Repair eight audited, misrouted pending launch-review rows.

This is an exact migration for the 2026-08-31 audit.  It does not recompute the
whole 9,500-row pool, create participants, tasks, cards, drafts or emails.
Future runs are protected by ``launch_runtime._pending_review_terminal_route``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from app import config, feishu, launch_runtime
from app.feishu import ext


TARGETS = {
    "launch-20260915-funlab-dave-ys11-5": {
        "route": "existing_thread",
        "names": {
            "pinha___", "Eagles Guy", "The YouTube Tech Guy", "ATech Flow ®",
            "Jose M Channel", "KevDecimates", "Deegital Tech",
        },
    },
    "launch-20260915-powkong-piranha-v2": {
        "route": "system_excluded",
        "names": {"HandheldBOSS"},
    },
}


def _is_pending(fields: dict) -> bool:
    return (
        ext(fields.get("参与状态")) in {"锁定准备中", "已入围"}
        and ext(fields.get("审核结论")) in {"待审核", "待补资料"}
        and not launch_runtime._ids(fields.get("关联邮件草稿"))
    )


def _repair_fields(route: str, *, now_ms: int) -> dict:
    explanation = (
        "该对象已有活动中的原邮件线程；已退出新开发待审，继续走原线程。"
        if route == "existing_thread" else
        "该对象命中确定性排除规则；已退出运营待审，无需人工复核。"
    )
    return {
        "参与状态": "已取消", "审核结论": "排除", "审核时间": now_ms,
        "系统审核分流": "自动排除", "系统审核说明": explanation,
        "取消原因代码": "不再符合",
    }


async def _linked_name(fields: dict) -> tuple[str, str]:
    linked = launch_runtime._ids(fields.get("关联KOL"))
    if len(linked) != 1:
        return "", ""
    record = await feishu.get_record(config.T_KOL, linked[0])
    return linked[0], ext((record.get("fields") or {}).get("账号名"))


async def run(*, commit: bool) -> dict:
    output = {"dry_run": not commit, "campaigns": []}
    now_ms = int(time.time() * 1000)
    for campaign_id, target in TARGETS.items():
        participants = await launch_runtime._participants(campaign_id)
        pending_rows = [
            row for row in participants if _is_pending(row.get("fields") or {})
        ]
        semaphore = asyncio.Semaphore(5)

        async def identify(row: dict) -> tuple[dict, str, str]:
            async with semaphore:
                contact_id, name = await _linked_name(row.get("fields") or {})
                return row, contact_id, name

        matched = []
        found_names = set()
        identified = await asyncio.gather(*(identify(row) for row in pending_rows))
        for row, contact_id, name in identified:
            fields = row.get("fields") or {}
            if name not in target["names"]:
                continue
            found_names.add(name)
            matched.append({
                "participant_id": str(row.get("record_id") or ""),
                "contact_id": contact_id, "name": name,
                "pending": _is_pending(fields),
            })
        missing = sorted(target["names"] - found_names)
        if missing:
            raise RuntimeError(
                f"audited participant targets missing for {campaign_id}: {len(missing)}"
            )

        pending = [row for row in matched if row["pending"]]
        already_terminal = [row for row in matched if not row["pending"]]
        item = {
            "campaign_id": campaign_id, "route": target["route"],
            "target_count": len(target["names"]),
            "pending_to_update": len(pending),
            "already_terminal_or_changed": len(already_terminal),
            "targets": matched,
        }
        if commit:
            for row in pending:
                fields = _repair_fields(target["route"], now_ms=now_ms)
                await launch_runtime.launch_participation._update_and_confirm(
                    config.T_LAUNCH_PARTICIPANT, row["participant_id"], fields,
                )
            readback_mismatches = []
            for row in matched:
                latest = await feishu.get_record(
                    config.T_LAUNCH_PARTICIPANT, row["participant_id"],
                )
                fields = latest.get("fields") or {}
                if not (
                    ext(fields.get("参与状态")) == "已取消"
                    and ext(fields.get("审核结论")) == "排除"
                    and ext(fields.get("系统审核分流")) == "自动排除"
                    and ext(fields.get("取消原因代码")) == "不再符合"
                ):
                    readback_mismatches.append(row["participant_id"])
            item["updated"] = len(pending)
            item["readback_mismatches"] = readback_mismatches
            if readback_mismatches:
                raise RuntimeError(
                    f"participant readback mismatch: {campaign_id}: "
                    f"{len(readback_mismatches)}"
                )
        output["campaigns"].append(item)
    return output


def _stdout_summary(result: dict, output: Path) -> dict:
    return {
        "dry_run": result["dry_run"],
        "campaigns": [{
            "campaign_id": item["campaign_id"], "route": item["route"],
            "target_count": item["target_count"],
            "pending_to_update": item["pending_to_update"],
            "already_terminal_or_changed": item["already_terminal_or_changed"],
            **({"updated": item["updated"]} if "updated" in item else {}),
        } for item in result["campaigns"]],
        "output": str(output.resolve()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.commit and args.confirm != "RECONCILE_PENDING_REVIEW_ROUTES":
        raise SystemExit(
            "commit requires --confirm RECONCILE_PENDING_REVIEW_ROUTES"
        )
    result = asyncio.run(run(commit=args.commit))
    output = Path(args.output)
    output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(json.dumps(_stdout_summary(result, output), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
