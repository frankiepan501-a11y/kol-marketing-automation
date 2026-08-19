"""Export one Feishu Base table through lark-cli and write a verified gzip JSONL backup."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


BJ_TZ = timezone(timedelta(hours=8))


def run_cli(args: list[str]) -> dict:
    exe = shutil.which("lark-cli")
    if not exe:
        raise RuntimeError("lark-cli not found")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    completed = subprocess.run(
        [exe, *args], capture_output=True, text=True, encoding="utf-8",
        errors="replace", env=env, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    payload = json.loads(completed.stdout)
    if not payload.get("ok"):
        raise RuntimeError(json.dumps(payload.get("error") or payload, ensure_ascii=False))
    return payload


def date_label(value) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=BJ_TZ).date().isoformat()
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=BJ_TZ)
            return parsed.astimezone(BJ_TZ).date().isoformat()
        except ValueError:
            pass
    return "missing_or_invalid"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-token", required=True)
    parser.add_argument("--table-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--date-field", default="统计日期")
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / f"{args.table_id}-records.jsonl.gz"
    schema_path = output_dir / f"{args.table_id}-schema.json"
    manifest_path = output_dir / f"{args.table_id}-manifest.json"

    schema = run_cli([
        "base", "+field-list", "--base-token", args.base_token,
        "--table-id", args.table_id, "--format", "json",
    ])
    schema_path.write_text(
        json.dumps(schema["data"], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    offset = 0
    total = 0
    seen_ids: set[str] = set()
    dates: Counter[str] = Counter()
    with gzip.open(records_path, "wt", encoding="utf-8", newline="\n") as handle:
        while True:
            page = run_cli([
                "base", "+record-list", "--base-token", args.base_token,
                "--table-id", args.table_id, "--offset", str(offset),
                "--limit", "200", "--format", "json",
            ])["data"]
            fields = page.get("fields") or []
            rows = page.get("data") or []
            ids = page.get("record_id_list") or []
            if len(rows) != len(ids):
                raise RuntimeError(f"page mismatch at offset={offset}: rows={len(rows)} ids={len(ids)}")
            for record_id, values in zip(ids, rows):
                if record_id in seen_ids:
                    raise RuntimeError(f"duplicate record_id during export: {record_id}")
                seen_ids.add(record_id)
                record_fields = dict(zip(fields, values))
                dates[date_label(record_fields.get(args.date_field))] += 1
                handle.write(json.dumps(
                    {"record_id": record_id, "fields": record_fields},
                    ensure_ascii=False, separators=(",", ":"),
                ) + "\n")
                total += 1
            if not page.get("has_more"):
                break
            if not rows:
                raise RuntimeError(f"has_more=true but empty page at offset={offset}")
            offset += len(rows)

    if total != args.expected_count or len(seen_ids) != args.expected_count:
        raise RuntimeError(
            f"backup count mismatch: expected={args.expected_count} rows={total} unique={len(seen_ids)}"
        )
    digest = hashlib.sha256(records_path.read_bytes()).hexdigest()
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_token": args.base_token,
        "table_id": args.table_id,
        "record_count": total,
        "unique_record_ids": len(seen_ids),
        "schema_field_count": schema["data"].get("total"),
        "records_file": records_path.name,
        "records_sha256": digest,
        "records_size_bytes": records_path.stat().st_size,
        "date_counts": dict(sorted(dates.items())),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
