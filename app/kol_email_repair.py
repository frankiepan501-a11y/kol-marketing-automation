"""Strict, targeted KOL email repair.

Only Snov results explicitly marked ``valid`` may be written.  Public profile
emails are discovery evidence, not proof of deliverability, so they are first
passed through the same verifier.  The runner never creates drafts, cards or
outreach messages.
"""
from __future__ import annotations

import asyncio
import hashlib
from urllib.parse import urlparse

from . import config, feishu, relabel, snov


def _profile_url(fields: dict) -> str:
    return str(feishu.ext_url(fields.get("主链接")) or "").strip()


def _youtube_lookup_value(profile_url: str) -> str:
    parsed = urlparse(str(profile_url or "").strip())
    if "youtube.com" not in parsed.netloc.casefold():
        return ""
    path = parsed.path.strip("/")
    if path.startswith("@"):
        return path.split("/", 1)[0]
    if path.startswith("channel/"):
        return path.split("/", 1)[1].split("/", 1)[0]
    return ""


def _domain(email: str) -> str:
    return email.rsplit("@", 1)[1].casefold() if "@" in email else ""


def _fingerprint(email: str) -> str:
    return hashlib.sha256(email.casefold().encode("utf-8")).hexdigest()[:12]


async def inspect_record(record: dict) -> dict:
    fields = record.get("fields") or {}
    record_id = str(record.get("record_id") or "")
    name = str(feishu.ext(fields.get("账号名")) or "").strip()
    original_raw = str(feishu.ext(fields.get("邮箱")) or "").strip()
    original_email, _ = feishu.clean_email(original_raw)
    candidate = original_email
    source = "current_email" if candidate else ""
    profile_url = _profile_url(fields)

    if not candidate:
        youtube_value = _youtube_lookup_value(profile_url)
        if not youtube_value:
            return {
                "record_id": record_id, "status": "no_verifiable_source",
                "source": "unsupported_or_missing_profile", "planned_fields": {},
                "original_raw": original_raw,
            }
        public_profile = await relabel.fetch_youtube_public_profile(youtube_value)
        candidate, _ = feishu.clean_email(public_profile.get("email") or "")
        if not candidate:
            return {
                "record_id": record_id, "status": "no_public_email",
                "source": "youtube_about", "planned_fields": {},
                "original_raw": original_raw,
            }
        source = "youtube_about"

    verification = await snov.find_email(name, _domain(candidate))
    if verification.get("status") != "valid":
        return {
            "record_id": record_id,
            "status": f"verification_{verification.get('status') or 'unavailable'}",
            "source": source, "planned_fields": {},
            "original_raw": original_raw,
            "email_fingerprint": _fingerprint(candidate),
        }
    verified, _ = feishu.clean_email(verification.get("email") or "")
    if not verified:
        return {
            "record_id": record_id, "status": "verification_invalid_response",
            "source": source, "planned_fields": {}, "original_raw": original_raw,
        }
    return {
        "record_id": record_id, "status": "verified_valid", "source": source,
        "planned_fields": {"邮箱": verified, "邮箱验真状态": "有效"},
        "original_raw": original_raw,
        "original_status": str(feishu.ext(fields.get("邮箱验真状态")) or ""),
        "email_fingerprint": _fingerprint(verified),
    }


async def run_email_repair(record_ids: list[str], *, dry_run: bool = True,
                           limit: int = 20) -> dict:
    unique_ids = list(dict.fromkeys(
        str(record_id).strip() for record_id in record_ids if str(record_id).strip()
    ))[:max(1, min(int(limit), 50))]
    all_rows = await feishu.fetch_all_records(
        config.T_KOL, field_names=["邮箱"], page_size=500,
    )
    owners: dict[str, set[str]] = {}
    for row in all_rows:
        email, _ = feishu.clean_email(feishu.ext((row.get("fields") or {}).get("邮箱")))
        if email:
            owners.setdefault(email.casefold(), set()).add(str(row.get("record_id") or ""))

    semaphore = asyncio.Semaphore(3)

    async def one(record_id: str) -> dict:
        async with semaphore:
            try:
                return await inspect_record(await feishu.get_record(config.T_KOL, record_id))
            except Exception as exc:
                return {
                    "record_id": record_id, "status": "processing_error",
                    "planned_fields": {}, "error": type(exc).__name__,
                }

    results = await asyncio.gather(*(one(record_id) for record_id in unique_ids))
    writes = 0
    for result in results:
        planned = dict(result.get("planned_fields") or {})
        email = str(planned.get("邮箱") or "").casefold()
        if not email:
            continue
        other_owners = owners.get(email, set()) - {result["record_id"]}
        if other_owners:
            result["status"] = "duplicate_email_owner"
            result["planned_fields"] = {}
            continue
        owners.setdefault(email, set()).add(result["record_id"])
        if dry_run:
            result["status"] = "would_write_valid"
            continue

        latest = await feishu.get_record(config.T_KOL, result["record_id"])
        latest_fields = latest.get("fields") or {}
        if (
            str(feishu.ext(latest_fields.get("邮箱")) or "").strip() != result.get("original_raw", "")
            or str(feishu.ext(latest_fields.get("邮箱验真状态")) or "")
            != result.get("original_status", "")
        ):
            result["status"] = "concurrent_change"
            result["planned_fields"] = {}
            continue
        await feishu.update_record(config.T_KOL, result["record_id"], planned)
        readback = await feishu.get_record(config.T_KOL, result["record_id"])
        readback_fields = readback.get("fields") or {}
        readback_email, _ = feishu.clean_email(feishu.ext(readback_fields.get("邮箱")))
        if (
            readback_email.casefold() != email
            or str(feishu.ext(readback_fields.get("邮箱验真状态")) or "") != "有效"
        ):
            result["status"] = "readback_mismatch"
            continue
        result["status"] = "written_valid"
        writes += 1

    counts: dict[str, int] = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        result.pop("original_raw", None)
        result.pop("original_status", None)
        if result.get("planned_fields", {}).get("邮箱"):
            result["planned_fields"]["邮箱"] = "<verified>"
    return {
        "dry_run": dry_run, "requested": len(unique_ids), "processed": len(results),
        "writes": writes, "by_status": counts, "results": results,
    }
