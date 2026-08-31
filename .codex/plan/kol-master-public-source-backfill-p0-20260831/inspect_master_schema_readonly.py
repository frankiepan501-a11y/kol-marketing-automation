"""Read-only schema and coverage check for public KOL contact sources.

Prints aggregate counts only.  It never prints names, URLs, emails, record IDs,
credentials, or other KOL business records.
"""
from __future__ import annotations

import asyncio
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from app import config, feishu


TARGET_FIELDS = (
    "账号名", "主平台", "主链接", "官网", "网站", "聚合页URL", "其他链接",
    "邮箱", "邮箱验真状态", "资料可用状态", "资料核实时间",
)


def _present(value) -> bool:
    if value is None or value == "":
        return False
    if isinstance(value, (list, dict)):
        return bool(value)
    return bool(str(value).strip())


async def main() -> dict:
    raw = await feishu.api(
        "GET",
        f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_KOL}/fields?page_size=100",
        which="bitable",
    )
    schema = list((raw.get("data") or {}).get("items") or [])
    selected_schema = []
    available = []
    for item in schema:
        name = str(item.get("field_name") or "")
        if name not in TARGET_FIELDS:
            continue
        available.append(name)
        selected_schema.append({
            "field_name": name,
            "type": item.get("type"),
            "is_primary": bool(item.get("is_primary")),
        })
    records = await feishu.fetch_all_records(
        config.T_KOL,
        field_names=available,
        page_size=500,
    )
    coverage = Counter()
    email_status = Counter()
    platforms = Counter()
    for record in records:
        fields = record.get("fields") or {}
        for name in available:
            if _present(fields.get(name)):
                coverage[name] += 1
        platforms[str(feishu.ext(fields.get("主平台")) or "未标记")] += 1
        email_status[str(feishu.ext(fields.get("邮箱验真状态")) or "空")] += 1
    return {
        "table_configured": bool(config.T_KOL),
        "total_records": len(records),
        "schema": selected_schema,
        "missing_target_fields": [name for name in TARGET_FIELDS if name not in available],
        "coverage": dict(sorted(coverage.items())),
        "platforms": dict(sorted(platforms.items())),
        "email_status": dict(sorted(email_status.items())),
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main()), ensure_ascii=False, indent=2))

