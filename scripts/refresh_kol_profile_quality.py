"""Refresh an exact, audited YouTube profile cohort.

Instagram rows are reported but deliberately excluded because the cloud profile
refresher only supports public YouTube pages.  The command never creates tasks,
cards, drafts or outreach messages.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from app import config, feishu
from app.relabel import run_profile_records


def _normalized(value):
    """Normalize Bitable write/read shapes for exact readback checks."""
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        if all(isinstance(item, dict) and "text" in item for item in value):
            return "".join(str(item.get("text") or "") for item in value)
        normalized = [_normalized(item) for item in value]
        return tuple(sorted(normalized, key=lambda item: str(item)))
    if isinstance(value, dict):
        return str(
            value.get("link") or value.get("text") or value.get("name") or ""
        )
    return value


async def _run_with_readback(record_ids: list[str], *, dry_run: bool,
                             limit: int) -> dict:
    result = await run_profile_records(
        record_ids, dry_run=dry_run, limit=limit,
        classification_mode="deterministic",
    )
    mismatches = []
    if not dry_run:
        for item in result.get("results") or []:
            if not item.get("write_applied"):
                continue
            latest = await feishu.get_record(config.T_KOL, item["record_id"])
            latest_fields = latest.get("fields") or {}
            different = [
                field_name
                for field_name, expected in (item.get("planned_fields") or {}).items()
                if _normalized(latest_fields.get(field_name)) != _normalized(expected)
            ]
            if different:
                mismatches.append({
                    "record_id": item["record_id"],
                    "fields": sorted(different),
                })
    result["readback_checked"] = sum(
        bool(item.get("write_applied")) for item in result.get("results") or []
    ) if not dry_run else 0
    result["readback_mismatches"] = mismatches
    return result


def selected_ids(path: str) -> tuple[list[str], list[dict]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = list(((data.get("campaigns") or {}).get("食人花") or {}).get(
        "profile_refresh"
    ) or [])
    youtube = [
        str(row.get("record_id") or "")
        for row in rows
        if row.get("record_id") and str(row.get("platform") or "") == "YouTube"
    ]
    unsupported = [
        {"record_id": str(row.get("record_id") or ""),
         "platform": str(row.get("platform") or "")}
        for row in rows
        if row.get("record_id") and str(row.get("platform") or "") != "YouTube"
    ]
    return youtube, unsupported


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohorts", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.commit and args.confirm != "PROFILE_REFRESH_YOUTUBE_ONLY":
        raise SystemExit("commit requires --confirm PROFILE_REFRESH_YOUTUBE_ONLY")

    record_ids, unsupported = selected_ids(args.cohorts)
    limit = max(1, min(args.limit, 100))
    result = asyncio.run(_run_with_readback(
        record_ids, dry_run=not args.commit, limit=limit,
    ))
    result["unsupported_excluded"] = len(unsupported)
    result["unsupported_platforms"] = sorted({
        row["platform"] or "unknown" for row in unsupported
    })
    # Exact record IDs stay in the local output for audit, never in stdout.
    result["unsupported_records"] = unsupported
    output = Path(args.output)
    output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(json.dumps({
        "dry_run": result["dry_run"], "processed": result["processed"],
        "writes": result["writes"], "by_status": result["by_status"],
        "unsupported_excluded": result["unsupported_excluded"],
        "unsupported_platforms": result["unsupported_platforms"],
        "readback_checked": result["readback_checked"],
        "readback_mismatch_count": len(result["readback_mismatches"]),
        "output": str(output.resolve()),
    }, ensure_ascii=False))
    if result["readback_mismatches"]:
        raise SystemExit("profile refresh readback mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
