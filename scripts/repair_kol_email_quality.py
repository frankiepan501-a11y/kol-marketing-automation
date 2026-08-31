"""Run a capped, valid-only email repair batch from an audited cohort file."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from app.kol_email_repair import run_email_repair


def selected_ids(path: str, limit: int) -> list[str]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    campaigns = data.get("campaigns") or {}
    ordered = list((campaigns.get("食人花") or {}).get("email") or [])
    ordered += list((campaigns.get("Dave") or {}).get("email") or [])
    return [str(item.get("record_id") or "") for item in ordered if item.get("record_id")][:limit]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohorts", required=True)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.commit and args.confirm != "EMAIL_REPAIR_VALID_ONLY":
        raise SystemExit("commit requires --confirm EMAIL_REPAIR_VALID_ONLY")
    limit = max(1, min(args.limit, 50))
    result = asyncio.run(run_email_repair(
        selected_ids(args.cohorts, limit), dry_run=not args.commit, limit=limit,
    ))
    Path(args.output).write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(json.dumps({
        "dry_run": result["dry_run"], "processed": result["processed"],
        "writes": result["writes"], "by_status": result["by_status"],
        "output": str(Path(args.output).resolve()),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
