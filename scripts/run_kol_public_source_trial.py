"""Run a bounded public-source + strict-email trial against the KOL master."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from app import config, feishu, kol_email_repair, kol_public_source_backfill


FIELD_NAMES = [
    "主平台", "主链接", "聚合页URL", "其他链接", "邮箱", "邮箱验真状态",
]


async def _field_types() -> dict[str, int]:
    raw = await feishu.api(
        "GET",
        f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_KOL}/fields?page_size=100",
    )
    return {
        str(item.get("field_name") or ""): int(item.get("type") or 0)
        for item in ((raw.get("data") or {}).get("items") or [])
        if str(item.get("field_name") or "") in kol_public_source_backfill.TARGET_FIELDS
    }


def _summary(result: dict) -> dict:
    summary = {
        "dry_run": bool(result.get("dry_run")),
        "requested": int(result.get("requested") or 0),
        "processed": int(result.get("processed") or 0),
        "writes": int(result.get("writes") or 0),
        "by_status": dict(result.get("by_status") or {}),
    }
    if "safe_to_continue" in result:
        summary["safe_to_continue"] = bool(result.get("safe_to_continue"))
        summary["abort_reason"] = str(result.get("abort_reason") or "")
    return summary


async def main(args) -> dict:
    field_types = await _field_types()
    if set(field_types) != set(kol_public_source_backfill.TARGET_FIELDS):
        raise RuntimeError("master public-source fields are missing or renamed")

    if args.ids_file and Path(args.ids_file).exists():
        record_ids = list(json.loads(Path(args.ids_file).read_text(encoding="utf-8")))
        total_records = None
    else:
        records = await feishu.fetch_all_records(
            config.T_KOL, field_names=FIELD_NAMES, page_size=500,
        )
        total_records = len(records)
        record_ids = kol_public_source_backfill.select_trial_record_ids(
            records, limit=args.limit,
        )
        if args.ids_file:
            Path(args.ids_file).write_text(
                json.dumps(record_ids, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    link_result = await kol_public_source_backfill.run_public_source_backfill(
        record_ids,
        field_types=field_types,
        dry_run=not args.commit_links,
        limit=args.limit,
    )
    if link_result.get("safe_to_continue"):
        email_result = await kol_email_repair.run_email_repair(
            record_ids,
            dry_run=not args.commit_emails,
            limit=min(args.limit, 50),
        )
    else:
        email_result = {
            "dry_run": not args.commit_emails,
            "requested": len(record_ids),
            "processed": 0,
            "writes": 0,
            "by_status": {"skipped_after_public_source_failure": len(record_ids)},
            "results": [],
        }
    evidence = {
        "total_records": total_records,
        "trial_size": len(record_ids),
        "links": _summary(link_result),
        "emails": _summary(email_result),
        "record_status": [
            {"record_id": item.get("record_id"), "status": item.get("status")}
            for item in link_result.get("results") or []
        ],
    }
    if args.evidence_file:
        Path(args.evidence_file).write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return {key: value for key, value in evidence.items() if key != "record_status"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--ids-file", default="")
    parser.add_argument("--evidence-file", default="")
    parser.add_argument("--commit-links", action="store_true")
    parser.add_argument("--commit-emails", action="store_true")
    print(json.dumps(asyncio.run(main(parser.parse_args())), ensure_ascii=False, indent=2))
