"""活动内竞品 KOL 证据的确定性身份匹配与排序。"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from urllib.parse import urlsplit, urlunsplit

from .feishu import ext, ext_url


MIN_P75_SAMPLE = 8
LONG_TERM_DAYS = 90
DAY_MS = 86_400_000
NYXI_OFFICIAL_CREATOR_IDS = {
    "UCIY4yC2qUCPcM7ws-xTARYg",
    "UCbvp-CTcH3Mhtj2UWsSy8sA",
}
NYXI_OFFICIAL_HANDLES = {"nyxigaming", "nyxi_official"}


def _ids(value) -> set[str]:
    if isinstance(value, dict):
        return set(value.get("link_record_ids") or value.get("record_ids") or [])
    if isinstance(value, list):
        out = set()
        for item in value:
            if isinstance(item, str):
                out.add(item)
            elif isinstance(item, dict):
                out.update(item.get("link_record_ids") or item.get("record_ids") or [])
        return out
    return set()


def _first(fields: dict, names: tuple[str, ...]) -> str:
    for name in names:
        value = ext(fields.get(name)).strip()
        if value:
            return value
    return ""


def _first_url(fields: dict, names: tuple[str, ...]) -> str:
    for name in names:
        value = ext_url(fields.get(name)).strip()
        if value:
            return value
    return ""


def normalize_handle(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"^https?://[^/]+/", "", text)
    text = text.split("?", 1)[0].strip("/@ ")
    return text


def normalize_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if not re.match(r"^https?://", raw, re.I):
        raw = "https://" + raw
    parts = urlsplit(raw)
    host = parts.netloc.lower().removeprefix("www.")
    path = re.sub(r"/+", "/", parts.path).rstrip("/").lower()
    return urlunsplit(("https", host, path, "", ""))


def _number(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _timestamp(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def is_nyxi_official_post(fields: dict) -> bool:
    if ext(fields.get("竞品品牌")).strip().upper() != "NYXI":
        return False
    sources = fields.get("采集来源") or []
    if not isinstance(sources, list):
        sources = [sources]
    if "YouTube官方频道" in {ext(value).strip() for value in sources}:
        return True
    creator_id = _first(fields, ("KOL平台ID", "平台creator_id", "creator_id", "频道ID"))
    handle = normalize_handle(_first(fields, (
        "KOL账号Handle", "KOL账号名", "作者Handle", "Handle", "作者账号",
    )))
    return creator_id in NYXI_OFFICIAL_CREATOR_IDS or handle in NYXI_OFFICIAL_HANDLES


def evidence_basis(fields: dict) -> str:
    is_nyxi = ext(fields.get("竞品品牌")).strip().upper() == "NYXI"
    if is_nyxi and is_nyxi_official_post(fields):
        return ""
    manually_confirmed = (
        ext(fields.get("人工复核状态")) == "已确认"
        and ext(fields.get("相关性")) == "相关"
        and ext(fields.get("合作信号")) == "明确合作"
    )
    if manually_confirmed:
        return "manual_confirmed"
    if (
        is_nyxi
    ):
        return "rule_inferred_non_official"
    return ""


def _valid_post(fields: dict) -> bool:
    return bool(evidence_basis(fields))


def _metric(fields: dict) -> tuple[str, float | None]:
    exposure = _number(fields.get("曝光量"))
    if exposure is not None:
        return "曝光量", exposure
    return "覆盖量", _number(fields.get("覆盖量"))


def _group_key(fields: dict) -> str:
    return f"{ext(fields.get('平台')).strip()}|{ext(fields.get('内容类型')).strip()}"


def _p75_thresholds(posts: list[dict]) -> tuple[dict[str, float], dict[str, int]]:
    groups: dict[str, list[float]] = defaultdict(list)
    for post in posts:
        fields = post.get("fields") or {}
        if not _valid_post(fields):
            continue
        _, value = _metric(fields)
        key = _group_key(fields)
        if value is not None and not key.startswith("|") and not key.endswith("|"):
            groups[key].append(value)
    thresholds = {}
    samples = {key: len(values) for key, values in groups.items()}
    for key, values in groups.items():
        if len(values) < MIN_P75_SAMPLE:
            continue
        ordered = sorted(values)
        thresholds[key] = ordered[math.ceil(0.75 * len(ordered)) - 1]
    return thresholds, samples


def _platform(fields: dict) -> str:
    return _first(fields, ("主平台", "平台")).lower()


def _creator_id(fields: dict, *, contact: bool) -> str:
    names = (
        ("YouTube频道ID", "平台creator_id", "creator_id", "作者ID", "频道ID", "KOL平台ID")
        if contact else
        ("KOL平台ID", "平台creator_id", "creator_id", "作者ID", "频道ID")
    )
    return _first(fields, names)


def _profile_url(fields: dict, *, contact: bool) -> str:
    names = (
        ("主链接", "主页URL", "账号主页", "主页链接", "频道链接")
        if contact else
        ("KOL主页URL", "作者主页", "主页URL", "账号主页", "频道链接")
    )
    return normalize_url(_first(fields, names))


def _handle(fields: dict, *, contact: bool) -> str:
    names = (
        ("账号Handle", "Handle", "账号名")
        if contact else
        ("KOL账号Handle", "KOL账号名", "作者Handle", "Handle", "作者账号")
    )
    return normalize_handle(_first(fields, names))


def _post_author_key(post: dict) -> str:
    fields = post.get("fields") or {}
    platform = _platform(fields)
    creator = _creator_id(fields, contact=False)
    if platform and creator:
        return f"{platform}|creator:{creator}"
    url = _profile_url(fields, contact=False)
    if platform and url:
        return f"{platform}|url:{url}"
    handle = _handle(fields, contact=False)
    if platform and handle:
        return f"{platform}|handle:{handle}"
    linked = sorted(_ids(fields.get("关联KOL")))
    if linked:
        return f"kol_record:{linked[0]}"
    return f"post:{post.get('record_id', '')}"


def build_evidence_index(posts: list[dict]) -> dict:
    """一次建立活动证据索引，避免每名候选重复扫描全部帖子。"""
    thresholds, samples = _p75_thresholds(posts)
    valid_posts = []
    official_excluded = 0
    invalid_excluded = 0
    by_linked = defaultdict(list)
    by_creator = defaultdict(list)
    by_url = defaultdict(list)
    by_handle = defaultdict(list)
    author_keys = set()
    author_by_post = {}

    for post in posts:
        fields = post.get("fields") or {}
        if is_nyxi_official_post(fields):
            official_excluded += 1
            continue
        if not _valid_post(fields):
            invalid_excluded += 1
            continue
        valid_posts.append(post)
        post_id = post.get("record_id", "")
        author_key = _post_author_key(post)
        author_keys.add(author_key)
        author_by_post[post_id] = author_key
        for linked_id in _ids(fields.get("关联KOL")):
            by_linked[linked_id].append(post)
        platform = _platform(fields)
        creator = _creator_id(fields, contact=False)
        url = _profile_url(fields, contact=False)
        handle = _handle(fields, contact=False)
        if platform and creator:
            by_creator[(platform, creator)].append(post)
        if platform and url:
            by_url[(platform, url)].append(post)
        if platform and handle:
            by_handle[(platform, handle)].append(post)

    return {
        "linked_posts_total": len(posts),
        "valid_posts": valid_posts,
        "official_excluded": official_excluded,
        "invalid_excluded": invalid_excluded,
        "thresholds": thresholds,
        "samples": samples,
        "by_linked": by_linked,
        "by_creator": by_creator,
        "by_url": by_url,
        "by_handle": by_handle,
        "author_keys": author_keys,
        "author_by_post": author_by_post,
    }


def _indexed_matches(contact: dict, index: dict) -> list[tuple[dict, str]]:
    fields = contact.get("fields") or {}
    platform = _platform(fields)
    buckets = [
        (index["by_linked"].get(contact.get("record_id", ""), []), "linked_kol"),
        (index["by_creator"].get((platform, _creator_id(fields, contact=True)), []), "platform_creator_id"),
        (index["by_url"].get((platform, _profile_url(fields, contact=True)), []), "profile_url"),
        (index["by_handle"].get((platform, _handle(fields, contact=True)), []), "platform_handle"),
    ]
    matched = []
    seen = set()
    for posts, path in buckets:
        for post in posts:
            post_id = post.get("record_id", "")
            if post_id in seen:
                continue
            seen.add(post_id)
            matched.append((post, path))
    return matched


def summarize_evidence_coverage(index: dict, contacts: list[dict]) -> dict:
    matched_contacts = 0
    matched_authors = set()
    for contact in contacts:
        matches = _indexed_matches(contact, index)
        if not matches:
            continue
        matched_contacts += 1
        matched_authors.update(
            index["author_by_post"].get(post.get("record_id", ""), "")
            for post, _ in matches
        )
    matched_authors.discard("")
    all_authors = set(index["author_keys"])
    return {
        "linked_posts_total": index["linked_posts_total"],
        "valid_partner_posts": len(index["valid_posts"]),
        "official_excluded": index["official_excluded"],
        "invalid_excluded": index["invalid_excluded"],
        "distinct_authors": len(all_authors),
        "matched_contacts": matched_contacts,
        "matched_authors": len(matched_authors),
        "unmatched_authors": len(all_authors - matched_authors),
    }


def match_post_identity(contact: dict, post: dict) -> str:
    contact_id = contact.get("record_id", "")
    contact_fields = contact.get("fields") or {}
    post_fields = post.get("fields") or {}
    if contact_id and contact_id in _ids(post_fields.get("关联KOL")):
        return "linked_kol"

    contact_platform = _first(contact_fields, ("主平台", "平台")).lower()
    post_platform = _first(post_fields, ("平台", "主平台")).lower()
    if not contact_platform or contact_platform != post_platform:
        return ""

    contact_creator = _first(contact_fields, (
        "YouTube频道ID", "平台creator_id", "creator_id", "作者ID", "频道ID", "KOL平台ID",
    ))
    post_creator = _first(post_fields, (
        "KOL平台ID", "平台creator_id", "creator_id", "作者ID", "频道ID",
    ))
    if contact_creator and contact_creator == post_creator:
        return "platform_creator_id"

    contact_url = normalize_url(_first(contact_fields, (
        "主链接", "主页URL", "账号主页", "主页链接", "频道链接",
    )))
    post_url = normalize_url(_first(post_fields, (
        "KOL主页URL", "作者主页", "主页URL", "账号主页", "频道链接",
    )))
    if contact_url and contact_url == post_url:
        return "profile_url"

    contact_handle = normalize_handle(_first(contact_fields, ("账号Handle", "Handle", "账号名")))
    post_handle = normalize_handle(_first(post_fields, (
        "KOL账号Handle", "KOL账号名", "作者Handle", "Handle", "作者账号",
    )))
    if contact_handle and contact_handle == post_handle:
        return "platform_handle"
    return ""


def identity_key(contact: dict, post: dict, path: str) -> str:
    contact_fields = contact.get("fields") or {}
    post_fields = post.get("fields") or {}
    platform = _first(contact_fields, ("主平台", "平台")).lower()
    if path == "linked_kol":
        return f"kol_record:{contact.get('record_id', '')}"
    if path == "platform_creator_id":
        value = _first(contact_fields, (
            "YouTube频道ID", "平台creator_id", "creator_id", "作者ID", "频道ID", "KOL平台ID",
        ))
        return f"{platform}|creator:{value}"
    if path == "profile_url":
        value = normalize_url(_first(contact_fields, (
            "主链接", "主页URL", "账号主页", "主页链接", "频道链接",
        )))
        return f"{platform}|url:{value}"
    if path == "platform_handle":
        value = normalize_handle(_first(contact_fields, ("账号Handle", "Handle", "账号名")))
        return f"{platform}|handle:{value}"
    return f"post:{post.get('record_id', '')}|unknown"


def rank_contact_evidence_from_index(contact: dict, index: dict, *, base_score: float) -> dict:
    """使用已建立的活动证据索引为一名候选排序。"""
    thresholds, samples = index["thresholds"], index["samples"]
    matched = []
    match_paths = []
    for post, path in _indexed_matches(contact, index):
        fields = post.get("fields") or {}
        metric_name, metric_value = _metric(fields)
        key = _group_key(fields)
        high = bool(
            metric_value is not None and key in thresholds
            and metric_value >= thresholds[key]
        )
        matched.append({
            "post_id": post.get("record_id", ""),
            "post_url": _first_url(fields, ("帖子URL", "内容链接", "视频链接")),
            "post_title": _first(fields, ("帖子标题", "内容标题", "视频标题")),
            "platform": ext(fields.get("平台")),
            "content_type": ext(fields.get("内容类型")),
            "published_at": _timestamp(fields.get("发布时间")),
            "metric_name": metric_name,
            "metric_value": metric_value,
            "p75_group": key,
            "p75_sample": samples.get(key, 0),
            "p75_threshold": thresholds.get(key),
            "is_high_performance": high,
            "identity_path": path,
            "identity_key": identity_key(contact, post, path),
            "evidence_basis": evidence_basis(fields),
        })
        if path not in match_paths:
            match_paths.append(path)

    timestamps = sorted(x["published_at"] for x in matched if x["published_at"] > 0)
    span_days = ((timestamps[-1] - timestamps[0]) // DAY_MS) if len(timestamps) >= 2 else 0
    long_term = len(matched) >= 2 and span_days >= LONG_TERM_DAYS
    high_performance = any(x["is_high_performance"] for x in matched)
    if long_term and high_performance:
        level, priority = "A", 3000 + base_score
    elif long_term or high_performance:
        level, priority = "B", 2000 + base_score
    elif matched:
        level, priority = "C", base_score + 5
    else:
        level, priority = "无加分", base_score
    return {
        "evidence_level": level,
        "final_priority": priority,
        "long_term": long_term,
        "long_term_span_days": span_days,
        "high_performance": high_performance,
        "identity_paths": match_paths,
        "stable_identity_keys": list(dict.fromkeys(
            x["identity_key"] for x in matched if x.get("identity_key")
        )),
        "matched_post_ids": [x["post_id"] for x in matched],
        "evidence_posts": matched,
        "p75_thresholds": thresholds,
        "p75_samples": samples,
    }


def rank_contact_evidence(contact: dict, posts: list[dict], *, base_score: float) -> dict:
    """兼容单条回放；批量预览应复用 build_evidence_index 的结果。"""
    return rank_contact_evidence_from_index(
        contact, build_evidence_index(posts), base_score=base_score,
    )
