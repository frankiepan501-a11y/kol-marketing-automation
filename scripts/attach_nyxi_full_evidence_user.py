"""用 Frankie 的飞书用户身份补接 NYXI 全量非官方证据。

原因：生产机器人受 Base 行级权限影响，已知 ID 可读、整表列表不可见；用户身份是
竞品帖子表的完整读取口径。默认 dry-run，--commit 才原子更新一条活动记录。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import launch_competitor_evidence


BASE_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"
POST_TABLE = "tblCDbvLtnLzdxEp"
KOL_TABLE = "tblMMhnj2hEbhF6y"
ACTIVITY_TABLE = "tbl8w0O7pI5PsRnq"
NODE_TABLE = "tblUljeSSAvFdFT6"
ACTIVITY_RECORD = "recvsFoRmeGj4Y"
CAMPAIGN_ID = "launch-20260915-funlab-dave-ys11-5"
TARGET_RANKING_VERSION = "evidence-v4"
TARGET_CONFIG_VERSION = 2
SNAPSHOT_PREFIX = "FULL_EVIDENCE_SNAPSHOT:"
SNAPSHOT_CHUNK_SIZE = 60
OFFICIAL_CREATOR_IDS = {"UCIY4yC2qUCPcM7ws-xTARYg", "UCbvp-CTcH3Mhtj2UWsSy8sA"}
OFFICIAL_HANDLES = {"nyxigaming", "nyxi_official"}
POST_FIELDS = [
    "竞品品牌", "采集来源", "KOL平台ID", "KOL账号Handle", "KOL主页URL",
    "关联KOL", "平台", "内容类型", "曝光量", "覆盖量", "发布时间", "帖子URL", "帖子标题",
]
KOL_FIELDS = ["账号名", "主平台", "YouTube频道ID", "主链接"]


def lark(args: list[str], *, stdin: str | None = None) -> dict:
    command = ["lark-cli", *args, "--format", "json", "--as", "user"]
    proc = subprocess.run(
        command, input=stdin, text=True, encoding="utf-8",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "lark-cli failed")
    payload = json.loads(proc.stdout)
    if payload.get("ok") is False:
        raise RuntimeError(json.dumps(payload.get("error") or payload, ensure_ascii=False))
    return payload


def list_records(table_id: str, fields: list[str]) -> list[dict]:
    rows = []
    offset = 0
    while True:
        args = [
            "base", "+record-list", "--base-token", BASE_TOKEN,
            "--table-id", table_id, "--offset", str(offset), "--limit", "200",
        ]
        for field in fields:
            args.extend(["--field-id", field])
        data = lark(args).get("data") or {}
        names = data.get("fields") or []
        values = data.get("data") or []
        record_ids = data.get("record_id_list") or []
        for record_id, cells in zip(record_ids, values):
            rows.append({"record_id": record_id, "fields": dict(zip(names, cells))})
        if not data.get("has_more"):
            break
        if not values:
            raise RuntimeError("飞书分页返回 has_more=true 但没有记录，已停止防死循环")
        offset += len(values)
    return rows


def first(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return first(value[0]) if value else ""
    if isinstance(value, dict):
        return str(value.get("text") or value.get("link") or value.get("name") or "")
    return str(value)


def normalize_handle(value) -> str:
    text = first(value).strip().lower()
    text = re.sub(r"^https?://[^/]+/", "", text)
    return text.split("?", 1)[0].strip("/@ ")


def normalize_url(value) -> str:
    raw = first(value).strip()
    markdown = re.fullmatch(r"\[[^]]*\]\((https?://[^)]+)\)", raw)
    if markdown:
        raw = markdown.group(1)
    if not raw:
        return ""
    if not re.match(r"^https?://", raw, re.I):
        raw = "https://" + raw
    parts = urlsplit(raw)
    return urlunsplit(("https", parts.netloc.lower().removeprefix("www."), parts.path.rstrip("/").lower(), "", ""))


def platform(fields: dict) -> str:
    return first(fields.get("主平台") or fields.get("平台")).strip().lower()


def author_keys(fields: dict, *, contact: bool) -> set[str]:
    p = platform(fields)
    creator = first(fields.get("YouTube频道ID") if contact else fields.get("KOL平台ID")).strip()
    url = normalize_url(fields.get("主链接") if contact else fields.get("KOL主页URL"))
    handle = normalize_handle(fields.get("账号名") if contact else fields.get("KOL账号Handle"))
    keys = set()
    if p and creator:
        keys.add(f"{p}|creator:{creator}")
    if p and url:
        keys.add(f"{p}|url:{url}")
    if p and handle:
        keys.add(f"{p}|handle:{handle}")
    if not contact:
        keys.update(f"kol_record:{record_id}" for record_id in link_ids(fields.get("关联KOL")))
    return keys


def canonical_author_components(rows: list[dict]) -> tuple[dict[str, set[str]], dict[str, str]]:
    parent: dict[str, str] = {}

    def find(key: str) -> str:
        parent.setdefault(key, key)
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    aliases_by_post = {}
    for row in rows:
        aliases = author_keys(row["fields"], contact=False) or {f"post:{row['record_id']}"}
        aliases_by_post[row["record_id"]] = aliases
        ordered = sorted(aliases)
        for alias in ordered:
            find(alias)
        for alias in ordered[1:]:
            union(ordered[0], alias)
    components: dict[str, set[str]] = {}
    for aliases in aliases_by_post.values():
        root = find(sorted(aliases)[0])
        components.setdefault(root, set()).update(aliases)
    return components, {alias: find(alias) for alias in parent}


def is_official(fields: dict) -> bool:
    sources = fields.get("采集来源") or []
    if not isinstance(sources, list):
        sources = [sources]
    return (
        "YouTube官方频道" in {first(item).strip() for item in sources}
        or first(fields.get("KOL平台ID")).strip() in OFFICIAL_CREATOR_IDS
        or normalize_handle(fields.get("KOL账号Handle")) in OFFICIAL_HANDLES
    )


def get_activity() -> dict:
    path = f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{ACTIVITY_TABLE}/records/{ACTIVITY_RECORD}"
    return (lark(["api", "GET", path]).get("data") or {}).get("record") or {}


def link_ids(value) -> list[str]:
    if isinstance(value, dict):
        direct = value.get("id")
        return ([direct] if direct else []) + list(
            value.get("record_ids") or value.get("link_record_ids") or []
        )
    if isinstance(value, list):
        ids = []
        for item in value:
            if isinstance(item, str):
                ids.append(item)
            elif isinstance(item, dict):
                if item.get("id"):
                    ids.append(item["id"])
                ids.extend(item.get("record_ids") or item.get("link_record_ids") or [])
        return ids
    return []


def split_chunks(record_ids: list[str], chunk_size: int = SNAPSHOT_CHUNK_SIZE) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size 必须大于 0")
    return [record_ids[index:index + chunk_size] for index in range(0, len(record_ids), chunk_size)]


def snapshot_metadata(result: dict, partner_ids: list[str]) -> dict:
    chunks = split_chunks(partner_ids)
    return {
        "ranking_version": TARGET_RANKING_VERSION,
        "config_version": TARGET_CONFIG_VERSION,
        "total_posts": len(partner_ids),
        "official_excluded": result["official_posts_excluded"],
        "chunks": len(chunks),
        "chunk_size": SNAPSHOT_CHUNK_SIZE,
        "distinct_authors": result["distinct_partner_authors"],
        "matched_authors": result["matched_authors"],
        "matched_master_kols": result["matched_master_kols"],
        "unmatched_authors": result["unmatched_authors"],
        "post_ids_sha256": hashlib.sha256("\n".join(partner_ids).encode("utf-8")).hexdigest(),
        "source": "NYXI non-official default collaboration",
    }


def snapshot_note(metadata: dict) -> str:
    return SNAPSHOT_PREFIX + json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))


def activity_write_fields(fields: dict, metadata: dict, *, snapshot_ms: int) -> dict:
    return {
        "竞品证据模式": "引用历史证据",
        "竞品分析状态": "已就绪",
        "竞品品牌": "NYXI",
        # 活动主记录只保留原有代表样本；完整证据从活动节点分块快照重建。
        "关联竞品帖子": link_ids(fields.get("关联竞品帖子")),
        "关联竞品营销事件": link_ids(fields.get("关联竞品营销事件")),
        "证据配置版本": TARGET_CONFIG_VERSION,
        "证据排序版本": TARGET_RANKING_VERSION,
        "证据快照时间": snapshot_ms,
        "证据等待/变更说明": snapshot_note(metadata),
    }


def snapshot_node_fields(*, index: int, total: int, post_ids: list[str], metadata: dict) -> dict:
    return {
        "活动ID": CAMPAIGN_ID,
        "节点代码": f"{TARGET_RANKING_VERSION}-chunk-{index:03d}",
        "节点名称": f"NYXI 全证据快照 {index:02d}/{total:02d}",
        "待确认竞品帖子": post_ids,
        "节点状态": "已确认",
        "目标证据配置版本": TARGET_CONFIG_VERSION,
        "竞品品牌": "NYXI",
        "调查提交说明": snapshot_note(metadata),
        "允许外部动作": False,
    }


def snapshot_rows(rows: list[dict]) -> list[dict]:
    selected = []
    prefix = f"{TARGET_RANKING_VERSION}-chunk-"
    for row in rows:
        fields = row.get("fields") or {}
        if first(fields.get("活动ID")) == CAMPAIGN_ID and first(fields.get("节点代码")).startswith(prefix):
            selected.append(row)
    return selected


def verify_snapshot_rows(rows: list[dict], partner_ids: list[str], metadata: dict) -> list[str]:
    selected = snapshot_rows(rows)
    expected_chunks = split_chunks(partner_ids)
    expected_codes = [f"{TARGET_RANKING_VERSION}-chunk-{index:03d}" for index in range(1, len(expected_chunks) + 1)]
    by_code = {}
    for row in selected:
        code = first((row.get("fields") or {}).get("节点代码"))
        if code in by_code:
            raise RuntimeError(f"全证据快照节点代码重复: {code}")
        by_code[code] = row
    if sorted(by_code) != expected_codes:
        raise RuntimeError("全证据快照节点不完整或含未知分块")
    flattened = []
    for code, expected_ids in zip(expected_codes, expected_chunks):
        fields = by_code[code].get("fields") or {}
        actual_ids = link_ids(fields.get("待确认竞品帖子"))
        if (
            actual_ids != expected_ids
            or first(fields.get("节点状态")) != "已确认"
            or first(fields.get("竞品品牌")).upper() != "NYXI"
            or int(fields.get("目标证据配置版本") or 0) != TARGET_CONFIG_VERSION
            or fields.get("允许外部动作") not in (None, "", False, 0)
            or first(fields.get("调查提交说明")) != snapshot_note(metadata)
        ):
            raise RuntimeError(f"全证据快照节点回读不一致: {code}")
        flattened.extend(actual_ids)
    if flattened != partner_ids or len(set(flattened)) != len(flattened):
        raise RuntimeError("全证据快照汇总后数量、顺序或唯一性不一致")
    return [by_code[code].get("record_id", "") for code in expected_codes]


def verify_snapshot_subset(rows: list[dict], partner_ids: list[str], metadata: dict) -> set[str]:
    selected = snapshot_rows(rows)
    expected_chunks = split_chunks(partner_ids)
    expected_by_code = {
        f"{TARGET_RANKING_VERSION}-chunk-{index:03d}": chunk
        for index, chunk in enumerate(expected_chunks, start=1)
    }
    seen = set()
    for row in selected:
        fields = row.get("fields") or {}
        code = first(fields.get("节点代码"))
        if code in seen or code not in expected_by_code:
            raise RuntimeError(f"已有全证据快照节点未知或重复: {code}")
        expected_fields = snapshot_node_fields(
            index=int(code.rsplit("-", 1)[1]), total=len(expected_chunks),
            post_ids=expected_by_code[code], metadata=metadata,
        )
        if (
            link_ids(fields.get("待确认竞品帖子")) != expected_fields["待确认竞品帖子"]
            or first(fields.get("节点状态")) != expected_fields["节点状态"]
            or first(fields.get("竞品品牌")).upper() != "NYXI"
            or int(fields.get("目标证据配置版本") or 0) != TARGET_CONFIG_VERSION
            or fields.get("允许外部动作") not in (None, "", False, 0)
            or first(fields.get("调查提交说明")) != expected_fields["调查提交说明"]
        ):
            raise RuntimeError(f"已有全证据快照节点与本次证据不一致: {code}")
        seen.add(code)
    return seen


def create_snapshot_node(fields: dict) -> str:
    path = f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{NODE_TABLE}/records"
    payload = lark(["api", "POST", path, "--data", "-"], stdin=json.dumps({"fields": fields}, ensure_ascii=False))
    data = payload.get("data") or {}
    record = data.get("record") or data
    record_id = record.get("record_id") or data.get("record_id")
    if not record_id:
        raise RuntimeError("飞书创建证据快照节点后未返回 record_id")
    return record_id


def delete_snapshot_node(record_id: str) -> None:
    path = f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{NODE_TABLE}/records/{record_id}"
    lark(["api", "DELETE", path])


def cleanup_created_nodes(record_ids: list[str]) -> list[str]:
    failures = []
    for record_id in reversed(record_ids):
        try:
            delete_snapshot_node(record_id)
        except Exception:
            failures.append(record_id)
    return failures


def restore_fields(fields: dict) -> dict:
    return {
        "竞品证据模式": first(fields.get("竞品证据模式")),
        "竞品分析状态": first(fields.get("竞品分析状态")),
        "竞品品牌": first(fields.get("竞品品牌")),
        "关联竞品帖子": link_ids(fields.get("关联竞品帖子")),
        "关联竞品营销事件": link_ids(fields.get("关联竞品营销事件")),
        "证据配置版本": int(fields.get("证据配置版本") or 0),
        "证据排序版本": first(fields.get("证据排序版本")),
        "证据快照时间": fields.get("证据快照时间"),
        "证据等待/变更说明": first(fields.get("证据等待/变更说明")),
    }


def activity_fields_match(actual: dict, expected: dict) -> bool:
    scalar_names = (
        "竞品证据模式", "竞品分析状态", "竞品品牌", "证据配置版本",
        "证据排序版本", "证据快照时间", "证据等待/变更说明",
    )
    for name in scalar_names:
        actual_value = actual.get(name)
        expected_value = expected.get(name)
        if name == "证据配置版本":
            if int(actual_value or 0) != int(expected_value or 0):
                return False
        elif name == "证据快照时间":
            if int(actual_value or 0) != int(expected_value or 0):
                return False
        elif first(actual_value) != first(expected_value):
            return False
    return (
        set(link_ids(actual.get("关联竞品帖子"))) == set(expected.get("关联竞品帖子") or [])
        and set(link_ids(actual.get("关联竞品营销事件"))) == set(expected.get("关联竞品营销事件") or [])
    )


def run(*, commit: bool, sample_limit: int = 0) -> dict:
    posts = list_records(POST_TABLE, POST_FIELDS)
    nyxi = [row for row in posts if first(row["fields"].get("竞品品牌")).strip().upper() == "NYXI"]
    official = [row for row in nyxi if is_official(row["fields"])]
    partner = [row for row in nyxi if not is_official(row["fields"])]
    kols = list_records(KOL_TABLE, KOL_FIELDS)

    authors, _ = canonical_author_components(partner)
    kol_key_to_ids = {}
    for row in kols:
        keys = author_keys(row["fields"], contact=True)
        keys.add(f"kol_record:{row['record_id']}")
        for key in keys:
            kol_key_to_ids.setdefault(key, set()).add(row["record_id"])
    matched_author_keys = {
        author for author, keys in authors.items() if any(key in kol_key_to_ids for key in keys)
    }
    matched_kols = {
        kol_id for author, keys in authors.items() if author in matched_author_keys
        for key in keys for kol_id in kol_key_to_ids.get(key, set())
    }

    activity = get_activity()
    fields = activity.get("fields") or {}
    gates = ["发送邮件授权", "样品寄送授权", "付费承诺授权", "名单锁定授权", "储备金释放授权"]
    gates_closed = all(fields.get(name) in (None, "", False, 0) for name in gates)
    result = {
        "dry_run": not commit,
        "campaign_id": CAMPAIGN_ID,
        "activity_record_id": activity.get("record_id"),
        "nyxi_posts_total": len(nyxi),
        "official_posts_excluded": len(official),
        "partner_posts_to_link": len(partner),
        "distinct_partner_authors": len(authors),
        "matched_master_kols": len(matched_kols),
        "matched_authors": len(matched_author_keys),
        "unmatched_authors": len(authors) - len(matched_author_keys),
        "current_config_version": int(fields.get("证据配置版本") or 0),
        "current_ranking_version": first(fields.get("证据排序版本")),
        "all_outbound_gates_closed": gates_closed,
    }
    partner_ids = sorted(row["record_id"] for row in partner)
    metadata = snapshot_metadata(result, partner_ids)
    result.update({
        "snapshot_chunks": metadata["chunks"],
        "snapshot_chunk_size": metadata["chunk_size"],
        "activity_relation_posts_preserved": len(link_ids(fields.get("关联竞品帖子"))),
        "target_config_version": TARGET_CONFIG_VERSION,
        "target_ranking_version": TARGET_RANKING_VERSION,
    })
    if sample_limit:
        sample = launch_competitor_evidence.rank_unmatched_author_candidates(
            launch_competitor_evidence.build_evidence_index(partner),
            kols, limit=sample_limit,
        )
        result["unmatched_author_sample"] = sample
    if not commit:
        return result
    if activity.get("record_id") != ACTIVITY_RECORD or first(fields.get("活动ID")) != CAMPAIGN_ID:
        raise RuntimeError("活动记录护栏不匹配")
    if not gates_closed:
        raise RuntimeError("有真实外联授权已开启，停止补接")
    already_active = (
        result["current_config_version"] == TARGET_CONFIG_VERSION
        and result["current_ranking_version"] == TARGET_RANKING_VERSION
    )
    if not already_active and (
        result["current_config_version"] != 1 or result["current_ranking_version"] != "evidence-v3"
    ):
        raise RuntimeError("活动证据版本已变化，请重新 dry-run")
    if len(nyxi) != 3423 or len(official) != 435 or len(partner) != 2988:
        raise RuntimeError("NYXI 证据数量与已审定口径不一致，停止写入")

    node_fields = [
        "活动ID", "节点代码", "节点名称", "待确认竞品帖子", "节点状态",
        "目标证据配置版本", "竞品品牌", "调查提交说明", "允许外部动作",
    ]
    nodes_before = list_records(NODE_TABLE, node_fields)
    created_ids = []
    try:
        existing_codes = verify_snapshot_subset(nodes_before, partner_ids, metadata)
        chunks = split_chunks(partner_ids)
        for index, chunk in enumerate(chunks, start=1):
            code = f"{TARGET_RANKING_VERSION}-chunk-{index:03d}"
            if code not in existing_codes:
                created_ids.append(create_snapshot_node(snapshot_node_fields(
                    index=index, total=len(chunks), post_ids=chunk, metadata=metadata,
                )))
        verify_snapshot_rows(list_records(NODE_TABLE, node_fields), partner_ids, metadata)
    except Exception as snapshot_error:
        cleanup_failures = cleanup_created_nodes(created_ids)
        if cleanup_failures:
            raise RuntimeError(f"证据块创建失败，且这些本轮节点未能清理: {cleanup_failures}") from snapshot_error
        raise

    if already_active:
        expected = activity_write_fields(fields, metadata, snapshot_ms=int(fields.get("证据快照时间") or 0))
        if not activity_fields_match(fields, expected):
            raise RuntimeError("活动已标记 evidence-v4，但元数据或代表证据回读不一致")
        result.update({
            "dry_run": False,
            "snapshot_nodes_created": len(created_ids),
            "snapshot_nodes_verified": metadata["chunks"],
            "written_ranking_version": TARGET_RANKING_VERSION,
            "written_config_version": TARGET_CONFIG_VERSION,
            "written_linked_posts": len(link_ids(fields.get("关联竞品帖子"))),
            "written_linked_events": len(link_ids(fields.get("关联竞品营销事件"))),
        })
        return result

    body = {"fields": activity_write_fields(fields, metadata, snapshot_ms=int(time.time() * 1000))}
    rollback = {"fields": restore_fields(fields)}
    path = f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{ACTIVITY_TABLE}/records/{ACTIVITY_RECORD}"
    try:
        # PUT 也放进结果不确定处理：即使服务端已写入、客户端只收到超时，
        # except 分支仍会按旧快照恢复并清理本轮新建节点。
        lark(["api", "PUT", path, "--data", "-"], stdin=json.dumps(body, ensure_ascii=False))
        readback = get_activity().get("fields") or {}
        written_post_ids = link_ids(readback.get("关联竞品帖子"))
        written_event_ids = link_ids(readback.get("关联竞品营销事件"))
        result.update({
            "dry_run": False,
            "written_ranking_version": first(readback.get("证据排序版本")),
            "written_config_version": int(readback.get("证据配置版本") or 0),
            "written_linked_posts": len(written_post_ids),
            "written_linked_events": len(written_event_ids),
            "snapshot_nodes_created": len(created_ids),
            "snapshot_nodes_verified": metadata["chunks"],
        })
        if not activity_fields_match(readback, body["fields"]):
            raise RuntimeError("写入后回读校验失败")
    except Exception as write_error:
        lark(["api", "PUT", path, "--data", "-"], stdin=json.dumps(rollback, ensure_ascii=False))
        restored = get_activity().get("fields") or {}
        rollback_ok = activity_fields_match(restored, rollback["fields"])
        cleanup_failures = cleanup_created_nodes(created_ids)
        if not rollback_ok or cleanup_failures:
            raise RuntimeError(
                f"全证据启用失败；活动恢复={rollback_ok}，未清理节点={cleanup_failures}"
            ) from write_error
        raise
    return result


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--sample-limit", type=int, default=0)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    result = run(commit=args.commit, sample_limit=max(0, min(args.sample_limit, 100)))
    if args.compact and result.get("unmatched_author_sample"):
        sample = result["unmatched_author_sample"]
        sample["candidates"] = [{
            key: row.get(key) for key in (
                "creator_id", "handle", "name", "platform", "profile_url", "post_count",
                "evidence_level", "high_performance", "primary_evidence_url",
                "primary_evidence_title", "promotion_status", "eligible_for_master_write",
            )
        } for row in sample["candidates"]]
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
