"""活动内竞品 KOL 证据的确定性身份匹配与排序。"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from urllib.parse import urlsplit, urlunsplit

from .feishu import ext


MIN_P75_SAMPLE = 8
LONG_TERM_DAYS = 90
DAY_MS = 86_400_000


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


def _valid_post(fields: dict) -> bool:
    return (
        ext(fields.get("人工复核状态")) == "已确认"
        and ext(fields.get("相关性")) == "相关"
        and ext(fields.get("合作信号")) == "明确合作"
    )


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

    contact_creator = _first(contact_fields, ("平台creator_id", "creator_id", "作者ID", "频道ID"))
    post_creator = _first(post_fields, ("平台creator_id", "creator_id", "作者ID", "频道ID"))
    if contact_creator and contact_creator == post_creator:
        return "platform_creator_id"

    contact_url = normalize_url(_first(contact_fields, ("主页URL", "账号主页", "主页链接", "频道链接")))
    post_url = normalize_url(_first(post_fields, ("作者主页", "主页URL", "账号主页", "频道链接")))
    if contact_url and contact_url == post_url:
        return "profile_url"

    contact_handle = normalize_handle(_first(contact_fields, ("账号Handle", "Handle", "账号名")))
    post_handle = normalize_handle(_first(post_fields, ("作者Handle", "Handle", "作者账号")))
    if contact_handle and contact_handle == post_handle:
        return "platform_handle"
    return ""


def rank_contact_evidence(contact: dict, posts: list[dict], *, base_score: float) -> dict:
    """只使用调用方传入的活动直接关联帖子，不扫描品牌的其他证据。"""
    thresholds, samples = _p75_thresholds(posts)
    matched = []
    match_paths = []
    for post in posts:
        fields = post.get("fields") or {}
        if not _valid_post(fields):
            continue
        path = match_post_identity(contact, post)
        if not path:
            continue
        metric_name, metric_value = _metric(fields)
        key = _group_key(fields)
        high = bool(
            metric_value is not None and key in thresholds
            and metric_value >= thresholds[key]
        )
        matched.append({
            "post_id": post.get("record_id", ""),
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
        "matched_post_ids": [x["post_id"] for x in matched],
        "evidence_posts": matched,
        "p75_thresholds": thresholds,
        "p75_samples": samples,
    }
