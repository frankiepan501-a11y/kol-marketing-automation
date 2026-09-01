"""Strict, targeted KOL public-contact email repair.

An email explicitly published on a KOL-owned public profile, aggregator,
website or contact page is accepted as that KOL's contact email without Snov.
It remains marked ``未验`` so downstream sending keeps its normal domain and
bounce protections.  The runner never creates drafts, cards or outreach
messages.
"""
from __future__ import annotations

import asyncio
import hashlib

from . import config, feishu, kol_email_sources


BLOCKING_STATUSES = {
    "processing_error", "owner_index_error", "concurrent_change",
    "readback_mismatch", "write_error",
}
BULK_READ_FIELDS = [
    "账号名", "邮箱", "邮箱验真状态", "主链接", "聚合页URL", "其他链接",
]


def _source_evidence_state(fields: dict) -> dict[str, str]:
    return {
        "账号名": str(feishu.ext(fields.get("账号名")) or "").strip(),
        "主链接": str(feishu.ext_url(fields.get("主链接")) or "").strip(),
        "聚合页URL": str(feishu.ext_url(fields.get("聚合页URL")) or "").strip(),
        "其他链接": str(feishu.ext(fields.get("其他链接")) or "").strip(),
    }


def _fingerprint(email: str) -> str:
    return hashlib.sha256(email.casefold().encode("utf-8")).hexdigest()[:12]


async def inspect_record(record: dict, *, candidates: list[dict] | None = None) -> dict:
    fields = record.get("fields") or {}
    record_id = str(record.get("record_id") or "")
    original_raw = str(feishu.ext(fields.get("邮箱")) or "").strip()
    if original_raw:
        return {
            "record_id": record_id, "status": "existing_email_skipped",
            "source": "master_email", "planned_fields": {},
            "original_raw": original_raw,
        }
    if candidates is None:
        candidates = await kol_email_sources.discover_public_email_candidates(fields)
    if not candidates:
        return {
            "record_id": record_id, "status": "no_public_email",
            "source": "bounded_public_sources", "planned_fields": {},
            "original_raw": original_raw,
        }
    candidate = ""
    source = "bounded_public_sources"
    source_url = ""
    for item in candidates:
        if not kol_email_sources.is_trusted_public_contact_candidate(item, fields):
            continue
        candidate, _ = feishu.clean_email(item.get("email") or "")
        if not candidate:
            continue
        source = item.get("source") or "public_contact"
        source_url = item.get("source_url") or ""
        break
    if not candidate:
        return {
            "record_id": record_id, "status": "no_public_email",
            "source": "bounded_public_sources", "planned_fields": {},
            "original_raw": original_raw,
            "candidate_count": len(candidates),
        }
    return {
        "record_id": record_id, "status": "public_contact_found", "source": source,
        "planned_fields": {"邮箱": candidate, "邮箱验真状态": "未验"},
        "original_raw": original_raw,
        "original_status": str(feishu.ext(fields.get("邮箱验真状态")) or ""),
        "original_source_evidence": _source_evidence_state(fields),
        "email_fingerprint": _fingerprint(candidate),
        "candidate_count": len(candidates),
        "source_url": source_url,
    }


async def run_email_repair(record_ids: list[str], *, dry_run: bool = True,
                           limit: int = 20,
                           source_overrides: dict[str, dict] | None = None) -> dict:
    unique_ids = list(dict.fromkeys(
        str(record_id).strip() for record_id in record_ids if str(record_id).strip()
    ))[:max(1, min(int(limit), 50))]

    try:
        all_rows = await feishu.fetch_all_records(
            config.T_KOL, field_names=BULK_READ_FIELDS, page_size=500,
        )
        rows_by_id = {
            str(row.get("record_id") or ""): row
            for row in all_rows if str(row.get("record_id") or "")
        }
    except Exception as exc:
        results = [{
            "record_id": record_id,
            "status": "processing_error",
            "planned_fields": {},
            "error": type(exc).__name__,
        } for record_id in unique_ids]
        return {
            "dry_run": dry_run,
            "requested": len(unique_ids),
            "processed": len(results),
            "writes": 0,
            "safe_to_continue": False,
            "abort_reason": "inspection_error",
            "by_status": {"processing_error": len(results)} if results else {},
            "results": results,
        }

    semaphore = asyncio.Semaphore(3)

    async def one(record_id: str) -> dict:
        async with semaphore:
            try:
                record = rows_by_id.get(record_id)
                if record is None:
                    return {
                        "record_id": record_id,
                        "status": "processing_error",
                        "planned_fields": {},
                        "error": "RecordNotFound",
                    }
                override = (source_overrides or {}).get(record_id) or {}
                if override:
                    merged_fields = dict(record.get("fields") or {})
                    for field_name in ("聚合页URL", "其他链接"):
                        if field_name in override:
                            merged_fields[field_name] = override[field_name]
                    record = {**record, "fields": merged_fields}
                return await inspect_record(record)
            except Exception as exc:
                return {
                    "record_id": record_id, "status": "processing_error",
                    "planned_fields": {}, "error": type(exc).__name__,
                }

    results = await asyncio.gather(*(one(record_id) for record_id in unique_ids))
    writes = 0
    abort_reason = ""
    if any(result.get("status") == "processing_error" for result in results):
        abort_reason = "inspection_error"

    # Dry-run reuses the stable inspection snapshot.  Commit mode refreshes the
    # owner snapshot after all external discovery/verification work and before
    # the first write, so a newly claimed email cannot slip through unnoticed.
    owner_rows = all_rows
    if (
        not abort_reason and not dry_run
        and any(result.get("planned_fields") for result in results)
    ):
        try:
            owner_rows = await feishu.fetch_all_records(
                config.T_KOL, field_names=["邮箱"], page_size=500,
            )
        except Exception as exc:
            abort_reason = "owner_index_error"
            for result in results:
                if result.get("planned_fields"):
                    result["status"] = "owner_index_error"
                    result["error"] = type(exc).__name__

    owners: dict[str, set[str]] = {}
    if not abort_reason:
        try:
            for row in owner_rows:
                email, _ = feishu.clean_email(
                    feishu.ext((row.get("fields") or {}).get("邮箱"))
                )
                if email:
                    owners.setdefault(email.casefold(), set()).add(
                        str(row.get("record_id") or "")
                    )
        except Exception as exc:
            abort_reason = "owner_index_error"
            for result in results:
                if result.get("planned_fields"):
                    result["status"] = "owner_index_error"
                    result["error"] = type(exc).__name__

    # Complete all ownership checks before the first write.  Any index failure
    # therefore aborts the whole bounded batch without partial writes.
    claimed: dict[str, str] = {}
    for result in results:
        if abort_reason:
            break
        planned = dict(result.get("planned_fields") or {})
        email = str(planned.get("邮箱") or "").casefold()
        if not email:
            continue
        other_owners = owners.get(email, set()) - {result["record_id"]}
        if email in claimed and claimed[email] != result["record_id"]:
            other_owners.add(claimed[email])
        if other_owners:
            result["status"] = "duplicate_email_owner"
            result["planned_fields"] = {}
            continue
        claimed[email] = result["record_id"]

    if abort_reason:
        for result in results:
            if result.get("planned_fields"):
                result["status"] = "not_written_after_failure"
                result["planned_fields"] = {}
    elif dry_run:
        for result in results:
            if result.get("planned_fields"):
                result["status"] = "would_write_public_contact"
    else:
        for index, result in enumerate(results):
            planned = dict(result.get("planned_fields") or {})
            email = str(planned.get("邮箱") or "").casefold()
            if not email:
                continue
            try:
                latest = await feishu.get_record(config.T_KOL, result["record_id"])
                latest_fields = latest.get("fields") or {}
                if (
                    str(feishu.ext(latest_fields.get("邮箱")) or "").strip()
                    != result.get("original_raw", "")
                    or str(feishu.ext(latest_fields.get("邮箱验真状态")) or "")
                    != result.get("original_status", "")
                    or _source_evidence_state(latest_fields)
                    != result.get("original_source_evidence", {})
                ):
                    result["status"] = "concurrent_change"
                    result["planned_fields"] = {}
                    abort_reason = "concurrent_change"
                else:
                    await feishu.update_record(config.T_KOL, result["record_id"], planned)
                    readback = await feishu.get_record(config.T_KOL, result["record_id"])
                    readback_fields = readback.get("fields") or {}
                    readback_email, _ = feishu.clean_email(
                        feishu.ext(readback_fields.get("邮箱"))
                    )
                    if (
                        readback_email.casefold() != email
                        or str(feishu.ext(readback_fields.get("邮箱验真状态")) or "")
                        != "未验"
                    ):
                        result["status"] = "readback_mismatch"
                        abort_reason = "readback_mismatch"
                    else:
                        result["status"] = "written_public_contact"
                        writes += 1
            except Exception as exc:
                result["status"] = "write_error"
                result["planned_fields"] = {}
                result["error"] = type(exc).__name__
                abort_reason = "write_error"
            if abort_reason:
                for pending in results[index + 1:]:
                    if pending.get("planned_fields"):
                        pending["status"] = "not_written_after_failure"
                        pending["planned_fields"] = {}
                break

    counts: dict[str, int] = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        result.pop("original_raw", None)
        result.pop("original_status", None)
        result.pop("original_source_evidence", None)
        if result.get("planned_fields", {}).get("邮箱"):
            result["planned_fields"]["邮箱"] = "<public-contact>"
    return {
        "dry_run": dry_run, "requested": len(unique_ids), "processed": len(results),
        "writes": writes, "safe_to_continue": not abort_reason,
        "abort_reason": abort_reason, "by_status": counts, "results": results,
    }
