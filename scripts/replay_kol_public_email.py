"""Read-only, single-record replay for public KOL email discovery.

The output intentionally excludes email values, full URLs, page bodies and
provider raw responses.  It never updates Base or sends any message.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from app import config, feishu, kol_email_repair, kol_email_sources


TRACE_FIELDS = (
    "stage", "source", "source_kind", "host", "url_fingerprint", "status",
    "contact_pages_found", "linked_pages_found", "email_candidates_found",
)
REPLAY_FIELDS = list(kol_email_repair.BULK_READ_FIELDS)


def _redacted_trace(items: list[dict]) -> list[dict]:
    return [
        {key: item.get(key) for key in TRACE_FIELDS if key in item}
        for item in items
        if isinstance(item, dict)
    ]


async def replay_record(record_id: str, *, max_pages: int = 4) -> dict:
    record_id = str(record_id or "").strip()
    if not record_id:
        raise ValueError("record_id is required")

    records = await feishu.fetch_all_records(
        config.T_KOL, field_names=REPLAY_FIELDS, page_size=500,
    )
    record = next((
        item for item in records
        if str(item.get("record_id") or "").strip() == record_id
    ), None)
    if record is None:
        raise LookupError("record_id was not found in the KOL master snapshot")
    fields = record.get("fields") or {}
    discovery = await kol_email_sources.discover_public_email_candidates_with_trace(
        fields, max_pages=max_pages,
    )
    candidates = list(discovery.get("candidates") or [])
    inspection = await kol_email_repair.inspect_record(
        record, candidates=candidates,
    )
    return {
        "record_id": record_id,
        "mode": "read_only",
        "write_attempted": False,
        "public_candidate_count": len(candidates),
        "verification_status": str(inspection.get("status") or ""),
        "verification_candidate_count": int(
            inspection.get("candidate_count") or len(candidates)
        ),
        "public_trace": _redacted_trace(list(discovery.get("trace") or [])),
    }


async def main(args: argparse.Namespace) -> dict:
    result = await replay_record(args.record_id, max_pages=args.max_pages)
    if args.evidence_file:
        Path(args.evidence_file).write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay one KOL public-email record without writing data.",
    )
    parser.add_argument("--record-id", required=True)
    parser.add_argument("--max-pages", type=int, choices=range(1, 5), default=4)
    parser.add_argument("--evidence-file", default="")
    return parser.parse_args(argv)


if __name__ == "__main__":
    print(json.dumps(
        asyncio.run(main(parse_args())),
        ensure_ascii=False,
        indent=2,
    ))
