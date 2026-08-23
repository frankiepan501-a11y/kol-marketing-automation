"""活动内竞品 KOL 证据的确定性身份匹配与排序。"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime
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
        direct_id = value.get("id") or value.get("record_id")
        return set(value.get("link_record_ids") or value.get("record_ids") or (
            [direct_id] if direct_id else []
        ))
    if isinstance(value, list):
        out = set()
        for item in value:
            if isinstance(item, str):
                out.add(item)
            elif isinstance(item, dict):
                direct_id = item.get("id") or item.get("record_id")
                out.update(item.get("link_record_ids") or item.get("record_ids") or (
                    [direct_id] if direct_id else []
                ))
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
        markdown = re.fullmatch(r"\[[^]]*\]\((https?://[^)]+)\)", value)
        if markdown:
            value = markdown.group(1)
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
    markdown = re.fullmatch(r"\[[^]]*\]\((https?://[^)]+)\)", raw)
    if markdown:
        raw = markdown.group(1)
    if not re.match(r"^https?://", raw, re.I):
        raw = "https://" + raw
    try:
        parts = urlsplit(raw)
    except ValueError:
        return ""
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
        parsed = int(value or 0)
        return parsed * 1000 if 0 < parsed < 100_000_000_000 else parsed
    except (TypeError, ValueError):
        text = ext(value).strip()
        if not text:
            return 0
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return int(parsed.timestamp() * 1000)
        except ValueError:
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
    platform = _first(fields, ("主平台", "平台")).lower()
    if platform:
        return platform
    url = _first_url(fields, (
        "主链接", "作者主页URL", "主页URL", "账号主页", "主页链接", "频道链接",
    )).lower()
    if "youtube.com" in url or "youtu.be" in url:
        return "youtube"
    if "tiktok.com" in url:
        return "tiktok"
    if "instagram.com" in url:
        return "instagram"
    if "twitter.com" in url or "x.com" in url:
        return "x"
    return ""


def _creator_id(fields: dict, *, contact: bool) -> str:
    names = (
        ("YouTube频道ID", "平台creator_id", "creator_id", "作者ID", "频道ID", "KOL平台ID")
        if contact else
        ("KOL平台ID", "平台creator_id", "creator_id", "作者ID", "频道ID")
    )
    return _first(fields, names)


def _profile_url(fields: dict, *, contact: bool) -> str:
    names = (
        ("主链接", "作者主页URL", "主页URL", "账号主页", "主页链接", "频道链接")
        if contact else
        ("KOL主页URL", "作者主页", "主页URL", "账号主页", "频道链接")
    )
    return normalize_url(_first_url(fields, names))


def _handle(fields: dict, *, contact: bool) -> str:
    names = (
        ("账号Handle", "Handle", "账号名", "媒体人姓名")
        if contact else
        ("KOL账号Handle", "KOL账号名", "作者Handle", "Handle", "作者账号")
    )
    return normalize_handle(_first(fields, names))


def _post_author_aliases(post: dict) -> set[str]:
    fields = post.get("fields") or {}
    platform = _platform(fields)
    aliases = {f"kol_record:{record_id}" for record_id in _ids(fields.get("关联KOL"))}
    creator = _creator_id(fields, contact=False)
    if platform and creator:
        aliases.add(f"{platform}|creator:{creator}")
    url = _profile_url(fields, contact=False)
    if platform and url:
        aliases.add(f"{platform}|url:{url}")
    handle = _handle(fields, contact=False)
    if platform and handle:
        aliases.add(f"{platform}|handle:{handle}")
    return aliases or {f"post:{post.get('record_id', '')}"}


def _canonical_authors(post_aliases: dict[str, set[str]]) -> tuple[set[str], dict[str, str]]:
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

    for aliases in post_aliases.values():
        ordered = sorted(aliases)
        for alias in ordered:
            find(alias)
        for alias in ordered[1:]:
            union(ordered[0], alias)
    canonical = {alias: find(alias) for alias in parent}
    author_by_post = {
        post_id: canonical[sorted(aliases)[0]] for post_id, aliases in post_aliases.items()
    }
    return set(author_by_post.values()), author_by_post


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
    post_aliases = {}

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
        post_aliases[post_id] = _post_author_aliases(post)
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

    author_keys, author_by_post = _canonical_authors(post_aliases)
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


def _matched_author_keys(index: dict, contacts: list[dict]) -> set[str]:
    matched_authors = set()
    for contact in contacts:
        for post, _ in _indexed_matches(contact, index):
            author_key = index["author_by_post"].get(post.get("record_id", ""), "")
            if author_key:
                matched_authors.add(author_key)
    return matched_authors


def _author_candidate(author_key: str, posts: list[dict], index: dict) -> dict:
    evidence_posts = []
    aliases = set()
    for post in posts:
        fields = post.get("fields") or {}
        aliases.update(_post_author_aliases(post))
        metric_name, metric_value = _metric(fields)
        group = _group_key(fields)
        threshold = index["thresholds"].get(group)
        evidence_posts.append({
            "post_id": post.get("record_id", ""),
            "post_url": _first_url(fields, ("帖子URL", "内容链接", "视频链接")),
            "post_title": _first(fields, ("帖子标题", "内容标题", "视频标题")),
            "published_at": _timestamp(fields.get("发布时间")),
            "metric_name": metric_name,
            "metric_value": metric_value,
            "p75_group": group,
            "p75_sample": index["samples"].get(group, 0),
            "p75_threshold": threshold,
            "is_high_performance": bool(
                metric_value is not None and threshold is not None
                and metric_value >= threshold
            ),
            "evidence_basis": evidence_basis(fields),
        })
    evidence_posts.sort(key=lambda row: (
        -float(row["metric_value"] or 0), -row["published_at"], row["post_id"],
    ))
    timestamps = sorted(
        row["published_at"] for row in evidence_posts if row["published_at"] > 0
    )
    span_days = ((timestamps[-1] - timestamps[0]) // DAY_MS) if len(timestamps) >= 2 else 0
    long_term = len(evidence_posts) >= 2 and span_days >= LONG_TERM_DAYS
    high_performance = any(row["is_high_performance"] for row in evidence_posts)
    if long_term and high_performance:
        evidence_level = "A"
    elif long_term or high_performance:
        evidence_level = "B"
    else:
        evidence_level = "C"

    source_fields = posts[0].get("fields") or {}
    for post in posts:
        fields = post.get("fields") or {}
        if _creator_id(fields, contact=False) or _profile_url(fields, contact=False):
            source_fields = fields
            break
    return {
        "author_key": author_key,
        "stable_identity_keys": sorted(aliases),
        "platform": ext(source_fields.get("平台")),
        "creator_id": _creator_id(source_fields, contact=False),
        "handle": _handle(source_fields, contact=False),
        "name": _first(source_fields, ("KOL账号名", "作者名称", "账号名")),
        # 身份匹配使用标准化 URL；展示给运营时保留原始大小写，避免改坏可点击链接。
        "profile_url": _first_url(source_fields, (
            "KOL主页URL", "作者主页", "主页URL", "账号主页", "频道链接",
        )),
        "post_count": len(evidence_posts),
        "first_published_at": timestamps[0] if timestamps else 0,
        "last_published_at": timestamps[-1] if timestamps else 0,
        "long_term_span_days": span_days,
        "long_term": long_term,
        "high_performance": high_performance,
        "evidence_level": evidence_level,
        # 人工查看只需代表证据；避免长期合作作者把后台响应放大到网关超时。
        "evidence_posts": evidence_posts[:5],
        "evidence_posts_truncated": len(evidence_posts) > 5,
        "primary_evidence_url": evidence_posts[0]["post_url"] if evidence_posts else "",
        "primary_evidence_title": evidence_posts[0]["post_title"] if evidence_posts else "",
        "promotion_status": "needs_profile_enrichment",
        "eligible_for_master_write": False,
        "write_block_reasons": [
            "country_not_verified", "language_not_verified",
            "semantic_fit_not_verified", "email_not_verified",
        ],
    }


def candidate_for_verified_author_key(index: dict, author_key: str) -> dict | None:
    """用服务端已验证帖子索引还原一个指定作者，拒绝客户端自带证据。"""
    key = str(author_key or "").strip().casefold()
    if not key:
        return None
    canonical_keys = {
        index["author_by_post"].get(post.get("record_id", ""), "")
        for post in index["valid_posts"]
        if key in {alias.casefold() for alias in _post_author_aliases(post)}
    }
    canonical_keys.discard("")
    if len(canonical_keys) != 1:
        return None
    canonical_key = next(iter(canonical_keys))
    posts = [
        post for post in index["valid_posts"]
        if index["author_by_post"].get(post.get("record_id", ""), "") == canonical_key
    ]
    if not posts:
        return None
    candidate = _author_candidate(key, posts, index)
    candidate["matched_post_ids"] = [
        post.get("record_id", "") for post in posts if post.get("record_id")
    ][:100]
    return candidate


def rank_unmatched_author_candidates(
    index: dict, contacts: list[dict], *, limit: int = 20, offset: int = 0,
) -> dict:
    """从竞品帖子反推出尚未进入主库的作者；结果只读且默认禁止写入。"""
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    matched_authors = _matched_author_keys(index, contacts)
    posts_by_author: dict[str, list[dict]] = defaultdict(list)
    for post in index["valid_posts"]:
        author_key = index["author_by_post"].get(post.get("record_id", ""), "")
        if author_key and author_key not in matched_authors:
            posts_by_author[author_key].append(post)
    candidates = [
        _author_candidate(author_key, posts, index)
        for author_key, posts in posts_by_author.items()
    ]
    level_order = {"A": 0, "B": 1, "C": 2}
    candidates.sort(key=lambda row: (
        level_order.get(row["evidence_level"], 9),
        -row["post_count"],
        -max((float(post["metric_value"] or 0) for post in row["evidence_posts"]), default=0),
        -row["last_published_at"], row["author_key"],
    ))
    return {
        "read_only": True,
        "writes": 0,
        "drafts_created": 0,
        "emails_sent": 0,
        "unmatched_authors": len(candidates),
        "offset": offset,
        "sample_size": min(limit, max(0, len(candidates) - offset)),
        "candidates": candidates[offset:offset + limit],
    }


def author_prewrite_gate(
    profile: dict, *, target_countries: set[str], target_languages: set[str],
    semantic_cues: set[str],
) -> dict:
    """作者进入 KOL 主表前的确定性硬闸；资料缺失一律不放行。"""
    reasons = []
    platform = str(profile.get("platform") or "").strip().lower()
    country = str(profile.get("country") or "").strip().upper()
    language = str(profile.get("language") or "").strip().lower()
    email = str(profile.get("email") or "").strip().lower()
    content = str(profile.get("content_text") or "").casefold()
    if platform not in {"youtube", "x", "tiktok", "instagram"}:
        reasons.append("unsupported_platform")
    if profile.get("is_official"):
        reasons.append("official_or_brand_channel")
    if not country or country not in {value.upper() for value in target_countries}:
        reasons.append("country_outside_target_or_unknown")
    if not language or language not in {value.lower() for value in target_languages}:
        reasons.append("language_outside_target_or_unknown")
    if not content or not any(cue.casefold() in content for cue in semantic_cues if cue):
        reasons.append("semantic_fit_not_verified")
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        reasons.append("missing_valid_email")
    return {
        "passed": not reasons,
        "reason_codes": reasons,
        "eligible_for_master_write": not reasons,
    }


def match_post_identity(contact: dict, post: dict) -> str:
    contact_id = contact.get("record_id", "")
    contact_fields = contact.get("fields") or {}
    post_fields = post.get("fields") or {}
    if contact_id and contact_id in _ids(post_fields.get("关联KOL")):
        return "linked_kol"

    contact_platform = _platform(contact_fields)
    post_platform = _platform(post_fields)
    if not contact_platform or contact_platform != post_platform:
        return ""

    contact_creator = _creator_id(contact_fields, contact=True)
    post_creator = _creator_id(post_fields, contact=False)
    if contact_creator and contact_creator == post_creator:
        return "platform_creator_id"

    contact_url = _profile_url(contact_fields, contact=True)
    post_url = _profile_url(post_fields, contact=False)
    if contact_url and contact_url == post_url:
        return "profile_url"

    contact_handle = _handle(contact_fields, contact=True)
    post_handle = _handle(post_fields, contact=False)
    if contact_handle and contact_handle == post_handle:
        return "platform_handle"
    return ""


def identity_key(contact: dict, post: dict, path: str) -> str:
    contact_fields = contact.get("fields") or {}
    post_fields = post.get("fields") or {}
    platform = _platform(contact_fields)
    if path == "linked_kol":
        return f"kol_record:{contact.get('record_id', '')}"
    if path == "platform_creator_id":
        value = _creator_id(contact_fields, contact=True)
        return f"{platform}|creator:{value}"
    if path == "profile_url":
        value = _profile_url(contact_fields, contact=True)
        return f"{platform}|url:{value}"
    if path == "platform_handle":
        value = _handle(contact_fields, contact=True)
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
