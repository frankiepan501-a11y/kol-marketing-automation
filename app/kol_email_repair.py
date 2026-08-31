"""Strict, targeted KOL email repair.

Only Snov results explicitly marked ``valid`` may be written.  Public profile
emails are discovery evidence, not proof of deliverability, so they are first
passed through the same verifier.  The runner never creates drafts, cards or
outreach messages.
"""
from __future__ import annotations

import asyncio
import hashlib

from . import config, feishu, kol_email_sources, snov


BLOCKING_STATUSES = {
    "processing_error", "owner_index_error", "concurrent_change",
    "readback_mismatch", "write_error",
}
BULK_READ_FIELDS = [
    "账号名", "邮箱", "邮箱验真状态", "主链接", "聚合页URL", "其他链接",
]


def _domain(email: str) -> str:
    return email.rsplit("@", 1)[1].casefold() if "@" in email else ""


def _fingerprint(email: str) -> str:
    return hashlib.sha256(email.casefold().encode("utf-8")).hexdigest()[:12]


async def inspect_record(record: dict) -> dict:
    fields = record.get("fields") or {}
    record_id = str(record.get("record_id") or "")
    name = str(feishu.ext(fields.get("账号名")) or "").strip()
    original_raw = str(feishu.ext(fields.get("邮箱")) or "").strip()
    if original_raw:
        return {
            "record_id": record_id, "status": "existing_email_skipped",
            "source": "master_email", "planned_fields": {},
            "original_raw": original_raw,
        }
    candidates = await kol_email_sources.discover_public_email_candidates(fields)
    if not candidates:
        return {
            "record_id": record_id, "status": "no_public_email",
            "source": "bounded_public_sources", "planned_fields": {},
            "original_raw": original_raw,
        }
    verification_statuses = []
    verified = ""
    source = ""
    source_url = ""
    candidate = ""
    for item in candidates:
        candidate, _ = feishu.clean_email(item.get("email") or "")
        if not candidate:
            continue
        verification = await snov.find_email(name, _domain(candidate))
        status = verification.get("status") or "unavailable"
        verification_statuses.append(status)
        if status != "valid":
            continue
        verified, _ = feishu.clean_email(verification.get("email") or "")
        if verified and verified.casefold() == candidate.casefold():
            source = item.get("source") or "public_source"
            source_url = item.get("source_url") or ""
            break
        verified = ""
        verification_statuses.append("mismatch")
    if not verified:
        final_status = ("not_valid" if "not_valid" in verification_statuses else
                        "mismatch" if "mismatch" in verification_statuses else
                        "unknown" if "unknown" in verification_statuses else
                        "unavailable" if "unavailable" in verification_statuses else
                        "not_found")
        return {
            "record_id": record_id, "status": f"verification_{final_status}",
            "source": "bounded_public_sources", "planned_fields": {},
            "original_raw": original_raw,
            "candidate_count": len(candidates),
        }
    return {
        "record_id": record_id, "status": "verified_valid", "source": source,
        "planned_fields": {"邮箱": verified, "邮箱验真状态": "有效"},
        "original_raw": original_raw,
        "original_status": str(feishu.ext(fields.get("邮箱验真状态")) or ""),
        "email_fingerprint": _fingerprint(verified),
        "candidate_count": len(candidates),
        "source_url": source_url,
    }


async def run_email_repair(record_ids: list[str], *, dry_run: bool = True,
                           limit: int = 20) -> dict:
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

    # Build the duplicate-owner index from the same stable bulk read used for
    # inspection.  This avoids the flaky single-record endpoint and ensures
    # inspection plus ownership checks share one consistent table snapshot.
    owners: dict[str, set[str]] = {}
    if not abort_reason:
        try:
            for row in all_rows:
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
                result["status"] = "would_write_valid"
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
                        != "有效"
                    ):
                        result["status"] = "readback_mismatch"
                        abort_reason = "readback_mismatch"
                    else:
                        result["status"] = "written_valid"
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
        if result.get("planned_fields", {}).get("邮箱"):
            result["planned_fields"]["邮箱"] = "<verified>"
    return {
        "dry_run": dry_run, "requested": len(unique_ids), "processed": len(results),
        "writes": writes, "safe_to_continue": not abort_reason,
        "abort_reason": abort_reason, "by_status": counts, "results": results,
    }
