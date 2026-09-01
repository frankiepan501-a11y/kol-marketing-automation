"""Run a bounded public-source + public-contact trial against the KOL master."""
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
EMPTY_EMAIL_PUBLIC_SOURCE_PLATFORMS = ("Instagram", "TikTok", "YouTube", "X")


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


def _select_empty_email_public_source_record_ids(
    records: list[dict], *, limit: int, excluded_ids: set[str] | None = None,
) -> list[str]:
    """Select a new balanced batch with empty email and existing public evidence."""
    excluded = {str(item) for item in (excluded_ids or set())}
    buckets = {platform: [] for platform in EMPTY_EMAIL_PUBLIC_SOURCE_PLATFORMS}
    for record in records:
        record_id = str(record.get("record_id") or "").strip()
        fields = record.get("fields") or {}
        platform = str(feishu.ext(fields.get("主平台")) or "").strip()
        has_public_source = bool(
            str(feishu.ext_url(fields.get("聚合页URL")) or "").strip()
            or str(feishu.ext(fields.get("其他链接")) or "").strip()
        )
        if (
            not record_id or record_id in excluded or platform not in buckets
            or str(feishu.ext(fields.get("邮箱")) or "").strip()
            or not has_public_source
        ):
            continue
        buckets[platform].append(record_id)

    selected = []
    target = max(1, min(int(limit), 200))
    index = 0
    while len(selected) < target:
        added = False
        for platform in EMPTY_EMAIL_PUBLIC_SOURCE_PLATFORMS:
            values = buckets[platform]
            if index < len(values):
                selected.append(values[index])
                added = True
                if len(selected) >= target:
                    break
        if not added:
            break
        index += 1
    return selected


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
        exclude_file = str(getattr(args, "exclude_ids_file", "") or "")
        excluded_ids = set()
        if exclude_file and Path(exclude_file).exists():
            excluded_ids = set(json.loads(Path(exclude_file).read_text(encoding="utf-8")))
        if bool(getattr(args, "empty_email_public_source", False)):
            record_ids = _select_empty_email_public_source_record_ids(
                records, limit=args.limit, excluded_ids=excluded_ids,
            )
        else:
            filtered = [
                record for record in records
                if str(record.get("record_id") or "") not in excluded_ids
            ]
            record_ids = kol_public_source_backfill.select_trial_record_ids(
                filtered, limit=args.limit,
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
        include_handoff_fields=True,
    )
    source_overrides = dict(link_result.pop("handoff_fields", {}) or {})
    if link_result.get("safe_to_continue"):
        email_result = await kol_email_repair.run_email_repair(
            record_ids,
            dry_run=not args.commit_emails,
            limit=min(args.limit, 50),
            source_overrides=source_overrides,
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
        "email_record_status": [
            {"record_id": item.get("record_id"), "status": item.get("status")}
            for item in email_result.get("results") or []
        ],
    }
    if args.evidence_file:
        Path(args.evidence_file).write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return {
        key: value for key, value in evidence.items()
        if key not in {"record_status", "email_record_status"}
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--ids-file", default="")
    parser.add_argument("--evidence-file", default="")
    parser.add_argument("--exclude-ids-file", default="")
    parser.add_argument("--empty-email-public-source", action="store_true")
    parser.add_argument("--commit-links", action="store_true")
    parser.add_argument("--commit-emails", action="store_true")
    print(json.dumps(asyncio.run(main(parser.parse_args())), ensure_ascii=False, indent=2))
