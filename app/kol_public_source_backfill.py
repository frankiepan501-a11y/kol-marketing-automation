"""Safe backfill of public KOL landing-page fields.

Only URLs explicitly published by the KOL profile are eligible.  Existing
master values are never overwritten and every production write is re-read.
"""
from __future__ import annotations

import asyncio
from urllib.parse import urlparse

from . import config, feishu, kol_email_sources


TARGET_FIELDS = ("聚合页URL", "其他链接")
SUPPORTED_PLATFORMS = ("YouTube", "Instagram", "TikTok")
BLOCKING_STATUSES = {"processing_error", "concurrent_change", "readback_mismatch"}


def _current_value(fields: dict, field_name: str) -> str:
    value = fields.get(field_name)
    if field_name == "聚合页URL":
        return str(feishu.ext_url(value) or "").strip()
    return str(feishu.ext(value) or "").strip()


def _format_value(url: str, field_type: int):
    if int(field_type or 0) == 15:
        host = (urlparse(url).hostname or url)[:80]
        return {"link": url, "text": host}
    return url


def plan_public_source_fields(
    fields: dict, candidates: list[dict], *, field_types: dict[str, int],
) -> dict:
    """Plan only empty master fields; never replace operator or prior data."""
    planned = {}
    mapping = {"aggregate": "聚合页URL", "website": "其他链接"}
    for candidate in candidates:
        field_name = mapping.get(str(candidate.get("kind") or ""))
        if (
            not field_name
            or field_name not in field_types
            or _current_value(fields, field_name)
            or field_name in planned
        ):
            continue
        url = str(candidate.get("url") or "").strip()
        if not url:
            continue
        planned[field_name] = _format_value(url, field_types[field_name])
    return planned


def select_trial_record_ids(records: list[dict], *, limit: int = 50) -> list[str]:
    """Choose a deterministic, cross-platform trial without exposing row data."""
    buckets = {platform: [] for platform in SUPPORTED_PLATFORMS}
    for record in records:
        fields = record.get("fields") or {}
        platform = str(feishu.ext(fields.get("主平台")) or "")
        if platform not in buckets:
            continue
        if not str(feishu.ext_url(fields.get("主链接")) or "").strip():
            continue
        if all(_current_value(fields, name) for name in TARGET_FIELDS):
            continue
        record_id = str(record.get("record_id") or "").strip()
        if record_id:
            buckets[platform].append(record_id)

    selected = []
    target = max(1, min(int(limit), 200))
    index = 0
    while len(selected) < target:
        added = False
        for platform in SUPPORTED_PLATFORMS:
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


async def inspect_record(record: dict, *, field_types: dict[str, int]) -> dict:
    fields = record.get("fields") or {}
    candidates = await kol_email_sources.discover_public_landing_page_candidates(fields)
    planned = plan_public_source_fields(fields, candidates, field_types=field_types)
    return {
        "record_id": str(record.get("record_id") or ""),
        "status": "candidate_found" if planned else "no_new_public_source",
        "planned_fields": planned,
        "original_values": {
            name: _current_value(fields, name) for name in TARGET_FIELDS
        },
        "candidate_kinds": sorted({
            str(item.get("kind") or "") for item in candidates if item.get("kind")
        }),
    }


async def run_public_source_backfill(
    record_ids: list[str], *, field_types: dict[str, int], dry_run: bool = True,
    limit: int = 50,
) -> dict:
    unique_ids = list(dict.fromkeys(
        str(record_id).strip() for record_id in record_ids if str(record_id).strip()
    ))[:max(1, min(int(limit), 200))]
    # The KOL master is large and Feishu can answer concurrent per-record reads
    # with 1254607 (data not ready). Keep this intentionally low so a bounded
    # repair run does not turn temporary read pressure into skipped records.
    semaphore = asyncio.Semaphore(2)

    async def one(record_id: str) -> dict:
        async with semaphore:
            try:
                record = await feishu.get_record(config.T_KOL, record_id)
                return await inspect_record(record, field_types=field_types)
            except Exception as exc:
                return {
                    "record_id": record_id,
                    "status": "processing_error",
                    "planned_fields": {},
                    "error": type(exc).__name__,
                }

    results = await asyncio.gather(*(one(record_id) for record_id in unique_ids))
    writes = 0
    abort_reason = ""
    if any(result.get("status") == "processing_error" for result in results):
        abort_reason = "inspection_error"

    for index, result in enumerate(results):
        planned = dict(result.get("planned_fields") or {})
        if not planned:
            continue
        if abort_reason:
            result["status"] = "not_written_after_failure"
            result["planned_fields"] = {}
            continue
        if dry_run:
            result["status"] = "would_write_public_source"
            continue

        latest = await feishu.get_record(config.T_KOL, result["record_id"])
        latest_fields = latest.get("fields") or {}
        changed = any(
            _current_value(latest_fields, name)
            != (result.get("original_values") or {}).get(name, "")
            for name in TARGET_FIELDS
        )
        if changed:
            result["status"] = "concurrent_change"
            result["planned_fields"] = {}
            abort_reason = "concurrent_change"
            for pending in results[index + 1:]:
                if pending.get("planned_fields"):
                    pending["status"] = "not_written_after_failure"
                    pending["planned_fields"] = {}
            break

        await feishu.update_record(config.T_KOL, result["record_id"], planned)
        readback = await feishu.get_record(config.T_KOL, result["record_id"])
        readback_fields = readback.get("fields") or {}
        mismatch = False
        for field_name, value in planned.items():
            expected = str(feishu.ext_url(value) if field_name == "聚合页URL" else feishu.ext(value))
            if _current_value(readback_fields, field_name) != expected:
                mismatch = True
                break
        if mismatch:
            result["status"] = "readback_mismatch"
            abort_reason = "readback_mismatch"
            for pending in results[index + 1:]:
                if pending.get("planned_fields"):
                    pending["status"] = "not_written_after_failure"
                    pending["planned_fields"] = {}
            break
        result["status"] = "written_public_source"
        writes += 1

    counts: dict[str, int] = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        result.pop("original_values", None)
        if result.get("planned_fields"):
            result["planned_fields"] = {
                name: "<public-url>" for name in result["planned_fields"]
            }
    return {
        "dry_run": dry_run,
        "requested": len(unique_ids),
        "processed": len(results),
        "writes": writes,
        "safe_to_continue": not abort_reason,
        "abort_reason": abort_reason,
        "by_status": counts,
        "results": results,
    }
