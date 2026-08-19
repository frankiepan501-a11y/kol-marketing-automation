"""集中上稿活动证据与参与记录表结构迁移。

默认只读取并输出差异；只有 ``--commit`` 才新增表或字段。本脚本不删除、
重命名或覆盖现有字段。若同名字段类型不同，会在第一笔写入前整批停止。
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

from app import config


APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "KINabIENjak8fRsB6AHcIDALntc")
PARTICIPANT_TABLE_NAME = "新品集中上稿活动参与记录"
T_KOL = os.environ.get("T_KOL", "tblMMhnj2hEbhF6y")
T_EDITOR = os.environ.get("T_EDITOR", "tblinUWFZHtmXZbC")
T_DRAFT = os.environ.get("T_DRAFT", "tblpWteXNX34vds4")
T_TASK_KOL = os.environ.get("T_TASK_KOL", "tblBtDYex6eiW2ui")
T_TASK_EDITOR = os.environ.get("T_TASK_EDITOR", "tblQ9Be4tisn8gcs")


def text(name):
    return {"field_name": name, "type": 1}


def number(name):
    return {"field_name": name, "type": 2}


def select(name, options):
    return {"field_name": name, "type": 3,
            "property": {"options": [{"name": value} for value in options]}}


def multiselect(name, options):
    return {"field_name": name, "type": 4,
            "property": {"options": [{"name": value} for value in options]}}


def datetime_field(name):
    return {"field_name": name, "type": 5}


def checkbox(name):
    return {"field_name": name, "type": 7}


def url(name):
    return {"field_name": name, "type": 15}


def person(name):
    return {"field_name": name, "type": 11, "property": {"multiple": False}}


def relation(name, table_id):
    # Base v3 创建关联字段时不接受 legacy `multiple` 参数。
    return {"field_name": name, "type": 18,
            "property": {"table_id": table_id}}


ACTIVITY_FIELDS = [
    text("产品主记录ID"),
    multiselect("活动目标国家", ["US", "UK", "DE", "FR", "ES", "IT", "NL", "SE", "PT", "CA", "AU", "BR"]),
    multiselect("活动目标语言", ["en", "de", "es", "fr", "it", "nl", "sv", "pt"]),
    select("竞品证据模式", ["发起新分析", "引用历史证据", "不使用竞品证据"]),
    select("竞品分析状态", ["不适用", "待分析", "分析中", "待人工确认", "已就绪", "失败", "配置无效"]),
    text("竞品品牌"),
    relation("关联竞品营销事件", config.T_COMPETITOR_EVENT),
    relation("关联竞品帖子", config.T_COMPETITOR_POST),
    datetime_field("证据快照时间"), text("证据排序版本"), number("证据配置版本"),
    checkbox("名单锁定授权"), text("KOL已锁定名单版本"),
    select("KOL名单阻塞代码", ["LOCK_BATCH_RETRYABLE", "LOCK_BATCH_MANUAL_REVIEW", "DUPLICATE_PARTICIPANT_MANUAL"]),
    text("KOL失败锁定批次ID"), text("KOL阻塞待处理记录"),
    text("媒体人已锁定名单版本"),
    select("媒体人名单阻塞代码", ["LOCK_BATCH_RETRYABLE", "LOCK_BATCH_MANUAL_REVIEW", "DUPLICATE_PARTICIPANT_MANUAL"]),
    text("媒体人失败锁定批次ID"), text("媒体人阻塞待处理记录"),
    text("证据等待/变更说明"),
]

NODE_FIELDS = [
    select("节点状态", ["待执行", "执行中", "待活动提交", "待人工确认", "已确认", "已阻塞"]),
    text("节点阻塞说明"), text("竞品品牌"), number("目标证据配置版本"),
    relation("待确认竞品帖子", config.T_COMPETITOR_POST),
    relation("待确认竞品事件", config.T_COMPETITOR_EVENT),
    number("调查提交版本"), datetime_field("调查提交时间"), text("调查提交说明"),
]

PARTICIPANT_FIELDS = [
    # Base v3 不接受关联字段的 legacy `multiple` 创建参数；生产字段按平台默认关系类型创建。
    # 业务上的单值与唯一性由 参与记录ID + 写后唯一键回查强制保证。
    text("参与记录ID"), text("活动ID"), relation("关联活动", config.T_LAUNCH_CAMPAIGN),
    select("对象类型", ["KOL", "媒体人"]),
    relation("关联KOL", T_KOL),
    relation("关联媒体人", T_EDITOR),
    text("产品家族ID"), select("进入方式", ["新开发", "同线程激活", "继续洽谈"]),
    number("基础评分快照"), select("竞品证据等级", ["A", "B", "C", "无加分", "待人工匹配"]),
    relation("关联竞品帖子", config.T_COMPETITOR_POST), number("最终优先级"),
    text("选择原因"), text("排序版本"), text("排序快照历史"),
    select("参与状态", ["锁定准备中", "已入围", "已取消"]),
    text("锁定批次ID"), text("名单版本"),
    select("取消原因代码", ["锁定失败", "运营取消", "不再符合"]),
    relation("关联KOL任务", T_TASK_KOL),
    relation("关联媒体人任务", T_TASK_EDITOR),
    relation("关联邮件草稿", T_DRAFT),
    datetime_field("计划上稿时间"), datetime_field("承诺上稿时间"),
    datetime_field("实际上稿时间"), url("上稿链接"),
    url("达人主页"), text("主平台快照"), text("国家快照"), text("语言快照"),
    number("粉丝数快照"), text("内容与活跃摘要"), datetime_field("内容数据更新时间"),
    text("历史关系与触达摘要"), text("竞品证据摘要"), url("主证据帖子"),
    select("系统审核分流", ["系统建议通过", "KOL运营审核", "Frankie例外审核", "自动排除"]),
    text("系统审核说明"),
    select("审核结论", ["待审核", "通过", "待补资料", "排除"]),
    text("审核原因"), person("审核人"), datetime_field("审核时间"),
]


class SchemaConflict(RuntimeError):
    pass


def diff_fields(existing: list[dict], desired: list[dict]) -> dict:
    by_name = {field.get("field_name"): field for field in existing}
    missing = []
    reused = []
    conflicts = []
    for definition in desired:
        current = by_name.get(definition["field_name"])
        if not current:
            missing.append(definition)
        elif current.get("type") != definition.get("type"):
            conflicts.append({
                "field_name": definition["field_name"],
                "expected_type": definition.get("type"),
                "actual_type": current.get("type"),
            })
        elif definition.get("type") == 18 and (
            (current.get("property") or {}).get("table_id")
            != (definition.get("property") or {}).get("table_id")
        ):
            conflicts.append({
                "field_name": definition["field_name"],
                "expected_type": definition.get("type"),
                "actual_type": current.get("type"),
                "reason": "关联目标表不一致",
            })
        elif definition.get("type") == 3 and not {
            option.get("name") for option in (definition.get("property") or {}).get("options") or []
        }.issubset({
            option.get("name") for option in (current.get("property") or {}).get("options") or []
        }):
            conflicts.append({
                "field_name": definition["field_name"],
                "expected_type": definition.get("type"),
                "actual_type": current.get("type"),
                "reason": "单选选项不完整",
            })
        else:
            reused.append(definition["field_name"])
    return {"missing": missing, "reused": reused, "conflicts": conflicts}


def validate_no_conflicts(diffs: dict[str, dict]) -> None:
    conflicts = [
        {"table": table, **item}
        for table, diff in diffs.items() for item in diff.get("conflicts") or []
    ]
    if conflicts:
        raise SchemaConflict("同名字段类型冲突: " + json.dumps(conflicts, ensure_ascii=False))


def validate_participant_primary(fields: list[dict]) -> None:
    if not fields:
        return
    primary = next((field for field in fields if field.get("is_primary")), None)
    if not primary or primary.get("field_name") != "参与记录ID" or primary.get("type") != 1:
        raise SchemaConflict("参与表主字段必须是文本字段「参与记录ID」")


class Client:
    def __init__(self):
        self.app_id = os.environ.get("FEISHU_BITABLE_APP_ID", "")
        self.app_secret = os.environ.get("FEISHU_BITABLE_APP_SECRET", "")
        if not self.app_id or not self.app_secret:
            raise RuntimeError("缺少 FEISHU_BITABLE_APP_ID / FEISHU_BITABLE_APP_SECRET")
        self.access_token = ""

    async def authenticate(self):
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
            )
        response.raise_for_status()
        data = response.json()
        if data.get("code") not in (None, 0):
            raise RuntimeError(f"飞书认证失败: {data.get('code')} {data.get('msg')}")
        self.access_token = data["tenant_access_token"]

    async def request(self, method, path, body=None):
        headers = {"Authorization": f"Bearer {self.access_token}",
                   "Content-Type": "application/json; charset=utf-8"}
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.request(
                method, f"https://open.feishu.cn/open-apis{path}", headers=headers, json=body,
            )
        data = response.json()
        if response.status_code >= 400 or data.get("code") not in (None, 0):
            raise RuntimeError(
                f"{method} {path} 失败: HTTP={response.status_code} "
                f"code={data.get('code')} msg={data.get('msg')}"
            )
        return data


async def list_all(client, path):
    items = []
    page_token = ""
    while True:
        joiner = "&" if "?" in path else "?"
        suffix = f"{joiner}page_size=100"
        if page_token:
            suffix += f"&page_token={page_token}"
        response = await client.request("GET", path + suffix)
        data = response.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            return items
        page_token = data.get("page_token") or ""
        if not page_token:
            raise RuntimeError("分页返回 has_more=true 但没有 page_token")


async def list_fields(client, table_id):
    return await list_all(client, f"/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/fields")


async def run(*, commit=False, verify_only=False):
    client = Client()
    await client.authenticate()
    tables = await list_all(client, f"/bitable/v1/apps/{APP_TOKEN}/tables")
    participant = next((x for x in tables if x.get("name") == PARTICIPANT_TABLE_NAME), None)
    participant_id = config.T_LAUNCH_PARTICIPANT or (participant or {}).get("table_id", "")
    if config.T_LAUNCH_PARTICIPANT and participant and participant["table_id"] != config.T_LAUNCH_PARTICIPANT:
        raise SchemaConflict("T_LAUNCH_PARTICIPANT 与同名表不一致")

    activity_existing = await list_fields(client, config.T_LAUNCH_CAMPAIGN)
    node_existing = await list_fields(client, config.T_LAUNCH_NODE)
    participant_existing = await list_fields(client, participant_id) if participant_id else []
    validate_participant_primary(participant_existing)
    diffs = {
        "activity": diff_fields(activity_existing, ACTIVITY_FIELDS),
        "node": diff_fields(node_existing, NODE_FIELDS),
        "participant": diff_fields(participant_existing, PARTICIPANT_FIELDS),
    }
    validate_no_conflicts(diffs)
    result = {
        "mode": "commit" if commit else ("verify-only" if verify_only else "dry-run"),
        "participant_table_id": participant_id,
        "participant_table_will_create": not bool(participant_id),
        "tables": {
            name: {"missing": [x["field_name"] for x in diff["missing"]],
                   "reused": diff["reused"], "conflicts": diff["conflicts"]}
            for name, diff in diffs.items()
        },
        "deletes": [],
    }
    if verify_only:
        result["verified"] = bool(participant_id) and not any(diff["missing"] for diff in diffs.values())
        return result
    if not commit:
        return result

    relation_targets = [
        config.T_LAUNCH_CAMPAIGN, config.T_COMPETITOR_POST, config.T_COMPETITOR_EVENT,
        T_KOL, T_EDITOR, T_TASK_KOL, T_TASK_EDITOR, T_DRAFT,
    ]
    if not all(relation_targets):
        raise RuntimeError("commit 前必须配置全部关联表 ID")

    for table_id, diff in (
        (config.T_LAUNCH_CAMPAIGN, diffs["activity"]),
        (config.T_LAUNCH_NODE, diffs["node"]),
    ):
        for definition in diff["missing"]:
            await client.request(
                "POST", f"/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/fields", definition,
            )

    if not participant_id:
        response = await client.request(
            "POST", f"/bitable/v1/apps/{APP_TOKEN}/tables",
            {"table": {"name": PARTICIPANT_TABLE_NAME, "fields": PARTICIPANT_FIELDS}},
        )
        participant_id = ((response.get("data") or {}).get("table") or {}).get("table_id", "")
        if not participant_id:
            raise RuntimeError("参与表创建后未返回 table_id")
    else:
        for definition in diffs["participant"]["missing"]:
            await client.request(
                "POST", f"/bitable/v1/apps/{APP_TOKEN}/tables/{participant_id}/fields", definition,
            )

    verified_fields = {
        "activity": await list_fields(client, config.T_LAUNCH_CAMPAIGN),
        "node": await list_fields(client, config.T_LAUNCH_NODE),
        "participant": await list_fields(client, participant_id),
    }
    validate_participant_primary(verified_fields["participant"])
    verified_diffs = {
        "activity": diff_fields(verified_fields["activity"], ACTIVITY_FIELDS),
        "node": diff_fields(verified_fields["node"], NODE_FIELDS),
        "participant": diff_fields(verified_fields["participant"], PARTICIPANT_FIELDS),
    }
    validate_no_conflicts(verified_diffs)
    if any(diff["missing"] for diff in verified_diffs.values()):
        raise RuntimeError("写后回读仍缺字段")
    result.update({
        "participant_table_id": participant_id,
        "verified": True,
        "verified_field_ids": {
            table: {field.get("field_name"): field.get("field_id") for field in fields}
            for table, fields in verified_fields.items()
        },
    })
    return result


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--commit", action="store_true")
    group.add_argument("--verify-only", action="store_true")
    group.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    output = asyncio.run(run(commit=args.commit, verify_only=args.verify_only))
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
