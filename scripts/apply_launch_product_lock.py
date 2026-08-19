"""为 2026-09-15 KOL 集中上稿试点写入活动专用锁。

默认只演练；只有显式传 ``--commit`` 才创建字段并更新产品记录。
本脚本不删除或覆盖历史产品，只把重复记录归并到一个活动主记录。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import httpx


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.product_dispatch_mode import (
    ACTIVITY_MODE,
    FIELD_CANONICAL_ID,
    FIELD_DISPATCH_MODE,
    FIELD_IS_CANONICAL,
    FIELD_MERGE_KEY,
    PAUSED_MODE,
    REGULAR_MODE,
    build_activity_group,
)


APP_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"
PRODUCT_TABLE = "tblate6wgHYWmD6s"
LOCK_FIELD_NAMES = (
    FIELD_DISPATCH_MODE,
    FIELD_MERGE_KEY,
    FIELD_IS_CANONICAL,
    FIELD_CANONICAL_ID,
)

FIELD_DEFINITIONS = [
    {
        "field_name": FIELD_DISPATCH_MODE,
        "type": 3,
        "ui_type": "SingleSelect",
        "property": {
            "options": [
                {"name": REGULAR_MODE},
                {"name": ACTIVITY_MODE},
                {"name": PAUSED_MODE},
            ]
        },
    },
    {"field_name": FIELD_MERGE_KEY, "type": 1},
    {"field_name": FIELD_IS_CANONICAL, "type": 7},
    {"field_name": FIELD_CANONICAL_ID, "type": 1},
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

EXPECTED_IDENTITIES = {
    PIRANHA_CANONICAL: {"品牌": "POWKONG", "SKU": "PK02-S2", "产品名包含": "食人花"},
    "recvqD87uSM1Fh": {"品牌": "POWKONG", "SKU": "PK02-S3", "产品名包含": "食人花"},
    "recvkJOoCsNb1s": {"品牌": "FUNLAB", "SKU": "FF05A-04", "产品名包含": "戴夫"},
}
REQUIRED_SENTINELS = {"产品名": 1, "品牌": 3, "SKU": 1}


class FeishuClient:
    def __init__(self) -> None:
        import os
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
    fields = {}
    page_token = ""
    while True:
        token_query = f"&page_token={page_token}" if page_token else ""
        response = await client.request(
            "GET",
            f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/fields?page_size=100{token_query}",
        )
        data = response.get("data") or {}
        for item in data.get("items") or []:
            fields[item["field_name"]] = item
        if not data.get("has_more"):
            return fields
        page_token = data.get("page_token") or ""
        if not page_token:
            raise RuntimeError("字段分页返回 has_more=true 但没有 page_token")


async def get_record(client: FeishuClient, record_id: str) -> dict:
    response = await client.request(
        "GET",
        f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/records/{record_id}",
    )
    return (response.get("data") or {}).get("record") or {}


def _plain(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(_plain(item) for item in value).strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or value.get("value") or "").strip()
    return str(value).strip()


def validate_field_schema(existing_fields: dict[str, dict], require_lock_fields: bool = False) -> None:
    """确认脚本连的是预期产品表，且同名字段类型/选项没有漂移。"""
    for field_name, expected_type in REQUIRED_SENTINELS.items():
        field = existing_fields.get(field_name)
        if not field or field.get("type") != expected_type:
            raise RuntimeError(
                f"目标表哨兵字段异常: {field_name} 预期 type={expected_type}, 实际={field and field.get('type')}"
            )

    for definition in FIELD_DEFINITIONS:
        name = definition["field_name"]
        field = existing_fields.get(name)
        if not field:
            if require_lock_fields:
                raise RuntimeError(f"活动锁字段创建后仍缺失: {name}")
            continue
        if field.get("type") != definition["type"]:
            raise RuntimeError(
                f"活动锁字段类型冲突: {name} 预期 type={definition['type']}, 实际={field.get('type')}"
            )
        if name == FIELD_DISPATCH_MODE:
            actual_options = {
                _plain(option.get("name"))
                for option in ((field.get("property") or {}).get("options") or [])
            }
            required_options = {REGULAR_MODE, ACTIVITY_MODE, PAUSED_MODE}
            if not required_options.issubset(actual_options):
                raise RuntimeError(
                    f"派单模式选项不完整: 缺少 {sorted(required_options - actual_options)}"
                )


def validate_record_identity(record_id: str, record: dict) -> None:
    expected = EXPECTED_IDENTITIES[record_id]
    fields = record.get("fields") or {}
    actual_brand = _plain(fields.get("品牌"))
    actual_sku = _plain(fields.get("SKU")) or _plain(fields.get("老库ERP SKU"))
    actual_name = _plain(fields.get("产品名"))
    if actual_brand != expected["品牌"] or actual_sku != expected["SKU"] or expected["产品名包含"] not in actual_name:
        raise RuntimeError(
            "产品身份校验失败，拒绝写入: "
            f"record_id={record_id} 实际=({actual_brand},{actual_sku},{actual_name}) "
            f"预期=({expected['品牌']},{expected['SKU']},名称含{expected['产品名包含']})"
        )


def original_lock_values(record: dict) -> dict:
    fields = record.get("fields") or {}
    return {
        FIELD_DISPATCH_MODE: fields.get(FIELD_DISPATCH_MODE) or None,
        FIELD_MERGE_KEY: fields.get(FIELD_MERGE_KEY) or None,
        FIELD_IS_CANONICAL: bool(fields.get(FIELD_IS_CANONICAL)),
        FIELD_CANONICAL_ID: fields.get(FIELD_CANONICAL_ID) or None,
    }


def validate_written_locks(records: dict[str, dict]) -> None:
    for record_id, expected in LOCK_RECORDS.items():
        actual = records.get(record_id) or {}
        validate_record_identity(record_id, actual)
        fields = actual.get("fields") or {}
        for field_name, expected_value in expected.items():
            if fields.get(field_name) != expected_value:
                raise RuntimeError(
                    f"活动锁写后校验失败: {record_id}.{field_name} "
                    f"预期={expected_value!r} 实际={fields.get(field_name)!r}"
                )


async def run(commit: bool) -> dict:
    client = FeishuClient()
    await client.authenticate()
    existing_fields = await list_fields(client)
    validate_field_schema(existing_fields)
    missing_fields = [
        definition for definition in FIELD_DEFINITIONS
        if definition["field_name"] not in existing_fields
    ]

    before = {}
    original_records = {}
    for record_id in LOCK_RECORDS:
        record = await get_record(client, record_id)
        if not record:
            raise RuntimeError(f"产品记录不存在: {record_id}")
        validate_record_identity(record_id, record)
        original_records[record_id] = record
        before[record_id] = {
            "产品名": (record.get("fields") or {}).get("产品名"),
            "品牌": (record.get("fields") or {}).get("品牌"),
            "SKU": (record.get("fields") or {}).get("SKU"),
            **{
                key: (record.get("fields") or {}).get(key)
                for key in LOCK_FIELD_NAMES
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

    validate_field_schema(await list_fields(client), require_lock_fields=True)

    touched = []
    after = {}
    piranha_group = None
    dave_group = None
    try:
        for record_id, fields in LOCK_RECORDS.items():
            # 先登记再写：即使服务端已写入但客户端收响应超时，也会尝试恢复原值。
            touched.append(record_id)
            await client.request(
                "PUT",
                f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/records/{record_id}",
                {"fields": fields},
            )
        after = {record_id: await get_record(client, record_id) for record_id in LOCK_RECORDS}
        validate_written_locks(after)
        piranha_group = build_activity_group([
            after[PIRANHA_CANONICAL],
            after["recvqD87uSM1Fh"],
        ])
        dave_group = build_activity_group([after["recvkJOoCsNb1s"]])
    except Exception as write_error:
        rollback_errors = []
        for record_id in reversed(touched):
            try:
                await client.request(
                    "PUT",
                    f"/bitable/v1/apps/{APP_TOKEN}/tables/{PRODUCT_TABLE}/records/{record_id}",
                    {"fields": original_lock_values(original_records[record_id])},
                )
            except Exception as rollback_error:
                rollback_errors.append(f"{record_id}: {rollback_error}")
        raise RuntimeError(
            f"活动锁写入失败，已回滚 {len(touched)} 条；回滚异常={rollback_errors or '无'}；原错误={write_error}"
        ) from write_error

    result.update({
        "after": {
            record_id: {
                "产品名": (record.get("fields") or {}).get("产品名"),
                **{
                    key: (record.get("fields") or {}).get(key)
                    for key in LOCK_FIELD_NAMES
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
