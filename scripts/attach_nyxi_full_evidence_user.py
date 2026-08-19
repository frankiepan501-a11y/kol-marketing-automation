"""用 Frankie 的飞书用户身份补接 NYXI 全量非官方证据。

原因：生产机器人受 Base 行级权限影响，已知 ID 可读、整表列表不可见；用户身份是
竞品帖子表的完整读取口径。默认 dry-run，--commit 才原子更新一条活动记录。
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


BASE_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"
POST_TABLE = "tblCDbvLtnLzdxEp"
KOL_TABLE = "tblMMhnj2hEbhF6y"
ACTIVITY_TABLE = "tbl8w0O7pI5PsRnq"
ACTIVITY_RECORD = "recvsFoRmeGj4Y"
CAMPAIGN_ID = "launch-20260915-funlab-dave-ys11-5"
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
    return keys


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


def run(*, commit: bool) -> dict:
    posts = list_records(POST_TABLE, POST_FIELDS)
    nyxi = [row for row in posts if first(row["fields"].get("竞品品牌")).strip().upper() == "NYXI"]
    official = [row for row in nyxi if is_official(row["fields"])]
    partner = [row for row in nyxi if not is_official(row["fields"])]
    kols = list_records(KOL_TABLE, KOL_FIELDS)

    authors = {}
    for row in partner:
        keys = author_keys(row["fields"], contact=False)
        canonical = sorted(keys)[0] if keys else f"post:{row['record_id']}"
        authors.setdefault(canonical, set()).update(keys or {canonical})
    kol_key_to_ids = {}
    for row in kols:
        for key in author_keys(row["fields"], contact=True):
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
    if not commit:
        return result
    if activity.get("record_id") != ACTIVITY_RECORD or first(fields.get("活动ID")) != CAMPAIGN_ID:
        raise RuntimeError("活动记录护栏不匹配")
    if not gates_closed:
        raise RuntimeError("有真实外联授权已开启，停止补接")
    if result["current_config_version"] != 1 or result["current_ranking_version"] != "evidence-v3":
        raise RuntimeError("活动证据版本已变化，请重新 dry-run")
    if len(nyxi) != 3423 or len(official) != 435 or len(partner) != 2988:
        raise RuntimeError("NYXI 证据数量与已审定口径不一致，停止写入")

    now = datetime.now().astimezone().isoformat(timespec="seconds")
    body = {"fields": {
        "竞品证据模式": "引用历史证据",
        "竞品分析状态": "已就绪",
        "竞品品牌": "NYXI",
        "关联竞品帖子": [{"id": row["record_id"]} for row in partner],
        "关联竞品营销事件": [],
        "证据配置版本": 2,
        "证据排序版本": "evidence-v4",
        "证据快照时间": now,
        "证据等待/变更说明": "NYXI 3423 条帖子中排除 435 条官方渠道，接入 2988 条非官方合作证据；仅用于本次 Dave 活动排序。",
    }}
    path = f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{ACTIVITY_TABLE}/records/{ACTIVITY_RECORD}"
    lark(["api", "PUT", path, "--data", "-"], stdin=json.dumps(body, ensure_ascii=False))
    readback = get_activity().get("fields") or {}
    linked = readback.get("关联竞品帖子") or []
    result.update({
        "dry_run": False,
        "written_ranking_version": first(readback.get("证据排序版本")),
        "written_config_version": int(readback.get("证据配置版本") or 0),
        "written_linked_posts": len(linked),
    })
    if result["written_ranking_version"] != "evidence-v4" or result["written_linked_posts"] != 2988:
        raise RuntimeError("写入后回读校验失败")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(commit=args.commit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
