"""Idempotent Feishu Bitable v1 migration for KOL media archiving.

Dry-run is the default.  The script uses the existing lark-cli bot profile so
no App secret is copied into the repository or command output.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from typing import Any


APP_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"
WORK_TABLE = "tblMSUCDUm7ceVxV"
PRODUCT_TABLE = "tblate6wgHYWmD6s"


def select_field(name: str, options: list[tuple[str, int]]) -> dict:
    return {
        "field_name": name,
        "type": 3,
        "property": {"options": [{"name": option, "color": color} for option, color in options]},
    }


WORK_FIELDS = [
    {"field_name": "允许自动归档", "type": 7},
    select_field("自动处理状态", [
        ("待执行", 3), ("等待同源母版", 1), ("处理中", 4),
        ("已完成", 7), ("失败", 5), ("无需处理", 10),
    ]),
    {"field_name": "归档任务ID", "type": 1},
    {"field_name": "同源主素材", "type": 7},
    {"field_name": "处理终端", "type": 1},
    {"field_name": "处理开始时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "处理完成时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    select_field("失败环节", [
        ("格式检查", 3), ("高画质下载", 4), ("清晰度检查", 6),
        ("飞书上传", 2), ("表格回填", 5),
    ]),
    {"field_name": "失败原因", "type": 1},
    {"field_name": "重试次数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "飞书file_token", "type": 1},
    {"field_name": "平台最高分辨率", "type": 1},
    {"field_name": "视频分辨率", "type": 1},
    {"field_name": "视频帧率", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "视频码率", "type": 2, "property": {"formatter": "1,000"}},
    {"field_name": "视频编码", "type": 1},
    {"field_name": "音频编码", "type": 1},
    {"field_name": "视频时长(秒)", "type": 2, "property": {"formatter": "0.00"}},
    {"field_name": "文件大小(字节)", "type": 2, "property": {"formatter": "1,000"}},
    {"field_name": "文件SHA256", "type": 1},
    select_field("清晰度检查", [
        ("最高可用画质", 7), ("低于平台最高档", 3), ("检查失败", 5),
    ]),
    {"field_name": "下次数据抓取时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "数据抓取失败原因", "type": 1},
]

SNAPSHOT_FIELDS = [
    {"field_name": "关联作品", "type": 18,
     "property": {"table_id": WORK_TABLE, "multiple": False}},
    {"field_name": "平台作品ID", "type": 1},
    {"field_name": "采集时间", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "上稿后天数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "播放量", "type": 2, "property": {"formatter": "1,000"}},
    {"field_name": "点赞量", "type": 2, "property": {"formatter": "1,000"}},
    {"field_name": "评论数", "type": 2, "property": {"formatter": "1,000"}},
    {"field_name": "数据源", "type": 1},
    {"field_name": "运行ID", "type": 1},
]

WORKER_FIELDS = [
    {"field_name": "最后心跳", "type": 5, "property": {"date_formatter": "yyyy-MM-dd HH:mm"}},
    {"field_name": "版本", "type": 1},
    {"field_name": "主机", "type": 1},
    select_field("状态", [("在线", 7), ("忙碌", 4), ("离线", 5), ("维护", 3)]),
    {"field_name": "扫描数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "领取数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "成功数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "失败数", "type": 2, "property": {"formatter": "0"}},
    {"field_name": "最后错误", "type": 1},
    {"field_name": "最后记录ID", "type": 1},
]


class LarkApi:
    def __init__(self, executable: str = "lark-cli"):
        self.executable = executable

    def call(self, method: str, path: str, *, params: dict | None = None,
             data: dict | None = None) -> dict:
        command = [self.executable, "api", method, path, "--as", "bot", "--format", "json"]
        if params is not None:
            command.extend(["--params", json.dumps(params, ensure_ascii=False, separators=(",", ":"))])
        if data is not None:
            command.extend(["--data", json.dumps(data, ensure_ascii=False, separators=(",", ":"))])
        for attempt in range(4):
            try:
                completed = subprocess.run(
                    command, check=False, capture_output=True, text=True,
                    encoding="utf-8", timeout=45,
                )
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(f"lark-cli调用超过45秒: {method} {path}") from exc
            if completed.returncode:
                raise RuntimeError((completed.stderr or completed.stdout)[-2000:])
            result = json.loads(completed.stdout)
            if result.get("ok"):
                if method != "GET":
                    time.sleep(0.6)
                return result.get("data") or {}
            error = result.get("error") or {}
            if int(error.get("code") or 0) not in {1254290, 1254291, 1254607} or attempt == 3:
                raise RuntimeError(json.dumps(error or result, ensure_ascii=False))
            time.sleep(1.0 + attempt)
        raise RuntimeError("unreachable")

    def tables(self) -> list[dict]:
        return self.call("GET", f"/open-apis/bitable/v1/apps/{APP_TOKEN}/tables",
                         params={"page_size": 100}).get("items") or []

    def fields(self, table_id: str) -> list[dict]:
        return self.call(
            "GET", f"/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/fields",
            params={"page_size": 100},
        ).get("items") or []

    def ensure_table(self, name: str, default_view: str,
                     primary_field: str) -> tuple[str, bool]:
        existing = next((item for item in self.tables() if item.get("name") == name), None)
        if existing:
            return str(existing["table_id"]), False
        data = self.call("POST", f"/open-apis/bitable/v1/apps/{APP_TOKEN}/tables", data={
            "table": {
                "name": name,
                "default_view_name": default_view,
                "fields": [{"field_name": primary_field, "type": 1}],
            },
        })
        table_id = str(data.get("table_id") or "")
        if not table_id:
            raise RuntimeError(f"创建{name}后未返回table_id")
        return table_id, True

    def rename_primary(self, table_id: str, field_name: str) -> bool:
        fields = self.fields(table_id)
        primary = next((item for item in fields if item.get("is_primary")), fields[0] if fields else None)
        if not primary or primary.get("field_name") == field_name:
            return False
        self.call(
            "PUT",
            f"/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/fields/{primary['field_id']}",
            data={"field_name": field_name, "type": 1},
        )
        return True

    def ensure_fields(self, table_id: str, definitions: list[dict]) -> list[str]:
        existing = {str(item.get("field_name") or "") for item in self.fields(table_id)}
        created: list[str] = []
        for definition in definitions:
            name = str(definition["field_name"])
            if name in existing:
                continue
            self.call(
                "POST", f"/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/fields",
                data=definition,
            )
            existing.add(name)
            created.append(name)
        return created


def migration_plan() -> dict:
    return {
        "mode": "dry-run",
        "writes_records": False,
        "enables_existing_records": False,
        "work_table": WORK_TABLE,
        "work_fields_to_ensure": [item["field_name"] for item in WORK_FIELDS],
        "product_fields_to_ensure": ["素材归档名"],
        "tables_to_ensure": {
            "KOL作品数据快照": ["快照名称", *[item["field_name"] for item in SNAPSHOT_FIELDS]],
            "KOL素材归档终端": ["Worker ID", *[item["field_name"] for item in WORKER_FIELDS]],
        },
    }


def commit(api: LarkApi) -> dict:
    snapshot_table, snapshot_created = api.ensure_table(
        "KOL作品数据快照", "全部快照", "快照名称",
    )
    worker_table, worker_created = api.ensure_table(
        "KOL素材归档终端", "终端状态", "Worker ID",
    )
    renamed = {
        snapshot_table: api.rename_primary(snapshot_table, "快照名称"),
        worker_table: api.rename_primary(worker_table, "Worker ID"),
    }
    return {
        "mode": "commit",
        "writes_records": False,
        "enables_existing_records": False,
        "snapshot_table": snapshot_table,
        "worker_table": worker_table,
        "tables_created": {
            "KOL作品数据快照": snapshot_created,
            "KOL素材归档终端": worker_created,
        },
        "primary_fields_renamed": renamed,
        "created_fields": {
            WORK_TABLE: api.ensure_fields(WORK_TABLE, WORK_FIELDS),
            PRODUCT_TABLE: api.ensure_fields(PRODUCT_TABLE, [{"field_name": "素材归档名", "type": 1}]),
            snapshot_table: api.ensure_fields(snapshot_table, SNAPSHOT_FIELDS),
            worker_table: api.ensure_fields(worker_table, WORKER_FIELDS),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--lark-cli", default="lark-cli")
    args = parser.parse_args()
    result = commit(LarkApi(args.lark_cli)) if args.commit else migration_plan()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
