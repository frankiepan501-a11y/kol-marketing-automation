"""为 2026-09-15 KOL 集中上稿试点写入活动专用锁。

默认只演练；只有显式传 ``--commit`` 才创建字段并更新产品记录。
本脚本不删除或覆盖历史产品，只把重复记录归并到一个活动主记录。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import httpx


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.product_dispatch_mode import ACTIVITY_MODE, build_activity_group


APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "KINabIENjak8fRsB6AHcIDALntc")
PRODUCT_TABLE = os.environ.get("T_PRODUCT", "tblate6wgHYWmD6s")

FIELD_DEFINITIONS = [
    {
        "field_name": "派单模式",
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {
            "options": [
                {"name": "常规派单"},
                {"name": ACTIVITY_MODE},
                {"name": "暂停"},
            ]
        },
    },
    {"field_name": "活动归并键", "type": 1},
    {"field_name": "活动主记录", "type": 7},
    {"field_name": "活动主记录ID", "type": 1},
]

PIRANHA_CANONICAL = "recvhAqrCyCPgl"
LOCK_RECORDS = {
    PIRANHA_CANONICAL: {
        "派单模式": ACTIVITY_MODE,
        "活动归并键": "launch-20260915-powkong-piranha-v2",
        "活动主记录": True,
        "活动主记录ID": PIRANHA_CANONICAL,
    },
    "recvqD87uSM1Fh": {
        "派单模式": ACTIVITY_MODE,
        "活动归并键": "launch-20260915-powkong-piranha-v2",
        "活动主记录": False,
        "活动主记录ID": PIRANHA_CANONICAL,
    },
    "recvkJOoCsNb1s": {
        "派单模式": ACTIVITY_MODE,
        "活动归并键": "launch-20260915-funlab-dave-ys11-5",
        "活动主记录": True,
        "活动主记录ID": "recvkJOoCsNb1s",
    },
}


class FeishuClient:
    def __init__(self) -> None:
        self.app_id = os.environ.get("FEISHU_BITABLE_APP_ID", "")
        self.app_secret = os.environ.get("FEISHU_BITABLE_APP_SECRET", "")
        if not self.app_id or not self.app_secret:
            raise RuntimeError("缺少 FEISHU_BITABLE_APP_ID / FEISHU_BITABLE_APP_SECRET")
        self.token = ""

    async def authenticate(self) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
            )
            response.raise_for_status()
            data = response.json()
            if data.get("code") not in (None, 0):
                raise RuntimeError(f"飞书认证失败: code={data.get('code')} msg={data.get('msg')}")
            self.token = data["tenant_access_token"]

    async def request(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"https://open.feishu.cn/open-apis{path}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        delays = [2, 5, 10]
        for attempt in range(len(delays) + 1):
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.request(method, url, headers=headers, json=body)
            data = response.json()
            if response.status_code < 400 and data.get("code") in (None, 0):
                return data
            code = data.get("code")
            if code == 1254607 and attempt < len(delays):
                await asyncio.sleep(delays[attempt])
                continue
            raise RuntimeError(
                f"{method} {path} 失败: HTTP={response.status_code} "
                f"code={code} msg={data.get('msg')}"
            )
        raise RuntimeError(f"{method} {path} 重试耗尽")


async def list_fields(client: FeishuClient) -> dict[str, dict]:
    response = await client.request(
        "GET",
        f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/fields?page_size=100",
    )
    return {
        item["field_name"]: item
        for item in (response.get("data") or {}).get("items") or []
    }


async def get_record(client: FeishuClient, record_id: str) -> dict:
    response = await client.request(
        "GET",
        f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/records/{record_id}",
    )
    return (response.get("data") or {}).get("record") or {}


async def run(commit: bool) -> dict:
    client = FeishuClient()
    await client.authenticate()
    existing_fields = await list_fields(client)
    missing_fields = [
        definition for definition in FIELD_DEFINITIONS
        if definition["field_name"] not in existing_fields
    ]

    before = {}
    for record_id in LOCK_RECORDS:
        record = await get_record(client, record_id)
        before[record_id] = {
            "产品名": (record.get("fields") or {}).get("产品名"),
            **{
                key: (record.get("fields") or {}).get(key)
                for key in ("派单模式", "活动归并键", "活动主记录", "活动主记录ID")
            },
        }

    result = {
        "mode": "commit" if commit else "dry-run",
        "missing_fields": [item["field_name"] for item in missing_fields],
        "planned_records": LOCK_RECORDS,
        "before": before,
    }
    if not commit:
        return result

    for definition in missing_fields:
        await client.request(
            "POST",
            f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/fields",
            definition,
        )

    for record_id, fields in LOCK_RECORDS.items():
        await client.request(
            "PUT",
            f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/records/{record_id}",
            {"fields": fields},
        )

    after = {record_id: await get_record(client, record_id) for record_id in LOCK_RECORDS}
    piranha_group = build_activity_group([
        after[PIRANHA_CANONICAL],
        after["recvqD87uSM1Fh"],
    ])
    dave_group = build_activity_group([after["recvkJOoCsNb1s"]])
    result.update({
        "after": {
            record_id: {
                "产品名": (record.get("fields") or {}).get("产品名"),
                **{
                    key: (record.get("fields") or {}).get(key)
                    for key in ("派单模式", "活动归并键", "活动主记录", "活动主记录ID")
                },
            }
            for record_id, record in after.items()
        },
        "validated_groups": [piranha_group, dave_group],
    })
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true", help="创建字段并写入活动锁")
    args = parser.parse_args()
    result = asyncio.run(run(commit=args.commit))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
