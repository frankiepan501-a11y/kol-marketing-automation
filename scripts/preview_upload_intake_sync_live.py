"""Read-only live preview for the unified KOL upload-intake synchronizer.

This adapter intentionally uses the current lark-cli user identity so a local
grey audit can inspect production Base data without copying app credentials.
It never calls a write command.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import logging
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
logging.disable(logging.WARNING)

from app.upload_intake_sync import plan_sync  # noqa: E402


BASE_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"
TABLES = {
    "sources": ("tblKwm9Ock8ObRgf", ["上稿平台链接", "社媒平台", "KOL", "邮箱", "产品", "品牌"]),
    "works": (
        "tblMSUCDUm7ceVxV",
        ["来源记录ID", "作品链接", "发布平台", "平台作品ID", "同源作品组", "运营备注"],
    ),
    "kols": ("tblMMhnj2hEbhF6y", ["账号名", "邮箱"]),
    "products": ("tblate6wgHYWmD6s", ["产品名", "素材归档名", "品牌", "SKU", "老库ERP SKU"]),
}


def _decode_output(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _run_lark(args: list[str]) -> dict:
    for attempt in range(1, 5):
        completed = subprocess.run(
            ["lark-cli", *args],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        text = _decode_output(completed.stdout)
        start = text.find("{")
        if start < 0:
            raise RuntimeError(f"lark-cli returned no JSON: {text[:300]}")
        payload = json.loads(text[start:])
        if payload.get("ok"):
            return payload.get("data") or {}
        error = payload.get("error") or {}
        if str(error.get("code") or "") == "1663" and attempt < 4:
            time.sleep(attempt * 2)
            continue
        raise RuntimeError(json.dumps(error, ensure_ascii=False))
    raise RuntimeError("lark-cli retry loop exhausted")


def _fetch_rows(table_id: str, fields: list[str]) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        args = [
            "base", "+record-list", "--base-token", BASE_TOKEN,
            "--table-id", table_id,
        ]
        for field in fields:
            args.extend(["--field-id", field])
        args.extend([
            "--offset", str(offset), "--limit", "200", "--format", "json", "--as", "user",
        ])
        data = _run_lark(args)
        record_ids = list(data.get("record_id_list") or [])
        values = list(data.get("data") or [])
        for record_id, row in zip(record_ids, values):
            rows.append({
                "record_id": str(record_id),
                "fields": {field: row[index] if index < len(row) else None for index, field in enumerate(fields)},
            })
        if len(record_ids) < 200:
            break
        offset += len(record_ids)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-record-id", default="")
    parser.add_argument("--sample-limit", type=int, default=20)
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    datasets = {name: _fetch_rows(*spec) for name, spec in TABLES.items()}
    sources = datasets["sources"]
    if args.source_record_id:
        sources = [row for row in sources if row["record_id"] == args.source_record_id]
    result = plan_sync(
        sources,
        datasets["works"],
        datasets["kols"],
        datasets["products"],
    )
    reasons = Counter(reason for item in result["items"] for reason in item["reasons"])
    candidates = [
        {
            "source_record_id": item["source_record_id"],
            "source_key": item["source_key"],
            "platform": item["platform"],
            "url": item["url"],
            "kol_id": (item["fields"].get("关联KOL") or [""])[0],
            "product_id": (item["fields"].get("关联产品") or [""])[0],
            "brand": item["fields"].get("品牌"),
            "work_name": item["fields"].get("作品名称"),
        }
        for item in result["items"] if item["action"] == "create"
    ][:max(0, args.sample_limit)]
    print(json.dumps({
        "live_counts": {name: len(rows) for name, rows in datasets.items()},
        "selected_sources": len(sources),
        "plan_counts": result["counts"],
        "review_reasons": dict(reasons),
        "create_samples": candidates,
        "selected_items": result["items"] if args.source_record_id else [],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
