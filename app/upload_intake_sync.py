"""Deterministic sync from the operator upload entry into one-work-per-platform rows.

The source table stays simple for operators.  This module owns URL extraction,
deduplication and relation matching.  New rows are deliberately created with
``允许自动归档=False`` so row creation can be grey-tested without opening the
local download queue.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from hashlib import sha1
import re
from urllib.parse import parse_qs, urlparse

from . import config, feishu, media_archive


SOURCE_FIELDS = ["上稿平台链接", "社媒平台", "KOL", "邮箱", "产品", "品牌"]
WORK_SYNC_FIELDS = [
    "来源记录ID", "作品链接", "发布平台", "平台作品ID", "关联KOL", "关联产品",
    "品牌", "同源作品组", "作品名称", "允许自动归档", "归档状态",
    "归档文件链接", "归档文件名", "内容类型", "数据抓取状态", "作品状态",
    "迁移状态", "自动处理状态", "运营备注", "素材可复用",
]
KOL_SYNC_FIELDS = ["账号名", "邮箱"]
PRODUCT_SYNC_FIELDS = ["产品名", "素材归档名", "品牌", "SKU", "老库ERP SKU"]

PLATFORM_CODES = {
    "YouTube": "YT",
    "TikTok": "TK",
    "Instagram": "IG",
    "Twitch": "TWI",
    "Amazon Live": "AMZ",
    "X/Twitter": "X",
    "Facebook": "FB",
    "Threads": "TH",
}
ARCHIVE_SUPPORTED = {"YouTube", "TikTok", "Instagram"}
SELECT_FIELDS = {
    "发布平台", "品牌", "归档状态", "内容类型", "数据抓取状态", "作品状态",
    "迁移状态", "自动处理状态",
}
REPAIRABLE_INITIAL_FIELDS = (
    "来源记录ID", "作品链接", "发布平台", "平台作品ID", "关联KOL", "关联产品",
    "品牌", "同源作品组", "作品名称", "归档状态", "内容类型", "数据抓取状态",
    "作品状态", "迁移状态", "自动处理状态", "运营备注",
)

# These aliases were already confirmed during the 2026-08-24 historical
# migration.  The value is a stable SKU/product key, not a guessed record id.
PRODUCT_ALIASES = {
    "戴夫": "ff05a04",
    "ys115戴夫": "ff05a04",
    "蜂窝": "ff01a07",
    "砖块1代": "pk01a",
    "蓝色积木": "pk04",
    "透明手柄": "ks3518",
    "机械赛博": "ff0410",
    "2代大红包": "fb01s2",
    "波纹": "ff01a04",
}

URL_RE = re.compile(r"https?://[^\s<>\]\)）】\"']+", re.IGNORECASE)
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
TRAILING_URL_PUNCTUATION = ".,;:!?，。；：！？、"
SYNC_NOTE = "由统一上稿入口自动拆分；灰度期默认未放行素材归档。"
_SYNC_COMMIT_LOCK = asyncio.Lock()


@dataclass(frozen=True)
class ParsedPublicURL:
    original_url: str
    normalized_url: str
    platform: str
    platform_id: str
    content_type: str


def _text(value) -> str:
    return media_archive.field_text(value)


def _key(value) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", _text(value).casefold())


def _name_key(value) -> str:
    text = _text(value)
    markdown = re.search(r"\[([^\]]+)\]\(https?://[^)]+\)", text, re.IGNORECASE)
    return _key(markdown.group(1) if markdown else text)


def _emails(value) -> tuple[str, ...]:
    return tuple(dict.fromkeys(match.group(0).casefold() for match in EMAIL_RE.finditer(_text(value))))


def extract_urls(value) -> list[str]:
    """Return unique public URLs from markdown or plain text in source order."""
    urls: list[str] = []
    for match in URL_RE.finditer(_text(value)):
        url = match.group(0).rstrip(TRAILING_URL_PUNCTUATION)
        if url and url not in urls:
            urls.append(url)
    return urls


def _clean_path(path: str) -> str:
    return "/" + path.strip("/") if path.strip("/") else ""


def parse_public_url(url: str) -> ParsedPublicURL:
    raw = str(url or "").strip().rstrip(TRAILING_URL_PUNCTUATION)
    try:
        parsed = urlparse(raw)
    except Exception:
        return ParsedPublicURL(raw, raw, "", "", "其他")
    host = (parsed.hostname or "").casefold().removeprefix("www.")
    path = parsed.path.strip("/")
    platform = ""
    platform_id = ""
    content_type = "其他"
    normalized = raw

    if host in {"youtu.be", "youtube.com", "m.youtube.com", "music.youtube.com"}:
        platform = "YouTube"
        route = ""
        if host == "youtu.be":
            platform_id = path.split("/", 1)[0]
            content_type = "长视频"
        else:
            platform_id = (parse_qs(parsed.query).get("v") or [""])[0]
            route = path.split("/", 1)[0].casefold() if path else ""
            if not platform_id:
                match = re.match(r"(?:shorts|live|embed)/([^/?#]+)", path, re.IGNORECASE)
                platform_id = match.group(1) if match else ""
            content_type = (
                "短视频" if route == "shorts"
                else "直播" if route == "live"
                else "帖子" if route == "post"
                else "长视频"
            )
        if platform_id:
            if content_type == "短视频":
                normalized = f"https://www.youtube.com/shorts/{platform_id}"
            elif content_type == "直播":
                normalized = f"https://www.youtube.com/live/{platform_id}"
            else:
                normalized = f"https://www.youtube.com/watch?v={platform_id}"
        elif route == "post" and len(path.split("/", 1)) == 2:
            platform_id = path.split("/", 1)[1]
            normalized = f"https://www.youtube.com/post/{platform_id}"

    elif host.endswith("tiktok.com"):
        platform = "TikTok"
        match = re.search(r"/video/(\d+)", parsed.path)
        platform_id = match.group(1) if match else ""
        content_type = "短视频"
        # The numeric id is enough for deduplication, but TikTok's dependable
        # public/download URL normally retains the ``@handle`` path.  Keep the
        # source path and only remove query/fragment tracking parameters.
        normalized_host = host if host in {"vm.tiktok.com", "vt.tiktok.com"} else "www.tiktok.com"
        normalized = f"https://{normalized_host}{_clean_path(parsed.path)}"

    elif host in {"instagram.com", "m.instagram.com"}:
        platform = "Instagram"
        match = re.match(r"(reel|reels|p|tv)/([^/?#]+)", path, re.IGNORECASE)
        route = match.group(1).casefold() if match else ""
        platform_id = match.group(2) if match else ""
        content_type = "图文" if route == "p" else "短视频"
        if platform_id:
            normalized_route = "p" if route == "p" else "reel"
            normalized = f"https://www.instagram.com/{normalized_route}/{platform_id}"

    elif host.endswith("twitch.tv"):
        platform = "Twitch"
        match = re.search(r"(?:^|/)videos/(\d+)", path)
        platform_id = match.group(1) if match else ""
        content_type = "直播"
        normalized = f"https://www.twitch.tv/videos/{platform_id}" if platform_id else f"https://{host}{_clean_path(parsed.path)}"

    elif "amazon." in host and ("live" in path.casefold() or "live" in host):
        platform = "Amazon Live"
        content_type = "直播"
        normalized = f"https://{host}{_clean_path(parsed.path)}"

    elif host in {"x.com", "twitter.com", "mobile.twitter.com"}:
        platform = "X/Twitter"
        match = re.search(r"/status/(\d+)", parsed.path)
        platform_id = match.group(1) if match else ""
        content_type = "帖子"
        normalized = f"https://x.com/i/status/{platform_id}" if platform_id else f"https://{host}{_clean_path(parsed.path)}"

    elif host.endswith("facebook.com") or host == "fb.watch":
        platform = "Facebook"
        content_type = "短视频" if "reel" in path.casefold() or host == "fb.watch" else "帖子"
        normalized = f"https://{host}{_clean_path(parsed.path)}"

    elif host in {"threads.net", "threads.com"}:
        platform = "Threads"
        content_type = "帖子"
        normalized = f"https://{host}{_clean_path(parsed.path)}"

    return ParsedPublicURL(raw, normalized.rstrip("/"), platform, platform_id, content_type)


def parse_url_field(value) -> ParsedPublicURL:
    """Parse either a raw API URL value or lark-cli's markdown link rendering."""
    urls = extract_urls(value)
    return parse_public_url(urls[0] if urls else _text(value))


def is_public_work_url(parsed: ParsedPublicURL) -> bool:
    """Reject profile/home URLs while allowing known public work URL shapes."""
    try:
        raw = urlparse(parsed.original_url)
    except Exception:
        return False
    host = (raw.hostname or "").casefold().removeprefix("www.")
    path = raw.path.strip("/")
    if parsed.platform == "YouTube":
        return bool(parsed.platform_id)
    if parsed.platform == "TikTok":
        return bool(
            parsed.platform_id
            or host in {"vm.tiktok.com", "vt.tiktok.com"}
            or path.casefold().startswith("t/")
        )
    if parsed.platform == "Instagram":
        return bool(parsed.platform_id)
    if parsed.platform == "Twitch":
        return bool(parsed.platform_id)
    if parsed.platform == "Amazon Live":
        return bool(
            re.search(r"(?:^|/)live/(?:video|broadcast)/[^/]+", path, re.IGNORECASE)
            or parse_qs(raw.query).get("broadcast")
            or parse_qs(raw.query).get("video")
        )
    if parsed.platform == "X/Twitter":
        return bool(parsed.platform_id)
    if parsed.platform == "Facebook":
        lowered = path.casefold()
        return bool(
            (host == "fb.watch" and path)
            or "reel" in lowered
            or "videos" in lowered
            or "posts" in lowered
            or (lowered == "watch" and parse_qs(raw.query).get("v"))
        )
    if parsed.platform == "Threads":
        return bool(re.search(r"(?:^|/)post/[^/]+", path, re.IGNORECASE))
    return False


def source_base_record_id(value) -> str:
    return _text(value).split("#", 1)[0]


def _url_identity(parsed: ParsedPublicURL) -> tuple[str, str]:
    if parsed.platform and parsed.platform_id:
        return parsed.platform, parsed.platform_id.casefold()
    return "url", parsed.normalized_url.casefold()


def _all_url_identities(parsed: ParsedPublicURL, *, platform: str = "",
                        platform_id: str = "") -> set[tuple[str, str]]:
    """Keep URL and platform-ID keys simultaneously; neither replaces the other."""
    identities: set[tuple[str, str]] = set()
    if parsed.normalized_url:
        identities.add(("url", parsed.normalized_url.casefold()))
    effective_platform = _text(platform) or parsed.platform
    effective_platform_id = _text(platform_id) or parsed.platform_id
    if effective_platform and effective_platform_id:
        identities.add((effective_platform, effective_platform_id.casefold()))
    return identities


def _work_identities(record: dict) -> set[tuple[str, str]]:
    fields = record.get("fields") or {}
    return _all_url_identities(
        parse_url_field(fields.get("作品链接")),
        platform=_text(fields.get("发布平台")),
        platform_id=_text(fields.get("平台作品ID")),
    )


def _identity_match_closure(
    seed_identities: set[tuple[str, str]],
    seed_records: list[dict],
    identity_index: dict[tuple[str, str], list[dict]],
) -> tuple[set[tuple[str, str]], list[dict]]:
    """Follow every URL/platform-ID edge until no additional work row appears."""
    identities = set(seed_identities)
    records: dict[str, dict] = {}
    pending_records = list(seed_records)
    checked_identities: set[tuple[str, str]] = set()
    while pending_records or identities - checked_identities:
        while pending_records:
            record = pending_records.pop()
            record_id = str(record.get("record_id") or "")
            if not record_id or record_id in records:
                continue
            records[record_id] = record
            identities.update(_work_identities(record))
        pending_identities = identities - checked_identities
        checked_identities.update(pending_identities)
        for identity in pending_identities:
            for record in identity_index.get(identity, []):
                record_id = str(record.get("record_id") or "")
                if record_id and record_id not in records:
                    pending_records.append(record)
    return identities, list(records.values())


def _match_kol(source_fields: dict, kol_records: list[dict]) -> tuple[str, list[str]]:
    email_index: dict[str, set[str]] = {}
    name_index: dict[str, set[str]] = {}
    for record in kol_records:
        record_id = str(record.get("record_id") or "")
        fields = record.get("fields") or {}
        for email in _emails(fields.get("邮箱")):
            email_index.setdefault(email, set()).add(record_id)
        name = _name_key(fields.get("账号名"))
        if name:
            name_index.setdefault(name, set()).add(record_id)

    email_candidates: set[str] = set()
    for email in _emails(source_fields.get("邮箱")):
        email_candidates.update(email_index.get(email, set()))
    name_candidates = name_index.get(_name_key(source_fields.get("KOL")), set())

    if len(email_candidates) == 1:
        return next(iter(email_candidates)), []
    if len(email_candidates) > 1:
        intersection = email_candidates & name_candidates
        if len(intersection) == 1:
            return next(iter(intersection)), []
        return "", ["KOL匹配不唯一"]
    if len(name_candidates) == 1:
        return next(iter(name_candidates)), []
    if len(name_candidates) > 1:
        return "", ["KOL匹配不唯一"]
    return "", ["KOL未匹配"]


def _match_product(source_fields: dict, product_records: list[dict]) -> tuple[str, list[str]]:
    source_product = _key(source_fields.get("产品"))
    target_key = PRODUCT_ALIASES.get(source_product, source_product)
    source_brand = _text(source_fields.get("品牌"))
    if not source_brand:
        return "", ["品牌缺失"]
    candidates: set[str] = set()
    for record in product_records:
        fields = record.get("fields") or {}
        brand = _text(fields.get("品牌"))
        if brand != source_brand:
            continue
        keys = {
            _key(fields.get("产品名")),
            _key(fields.get("素材归档名")),
            _key(fields.get("SKU")),
            _key(fields.get("老库ERP SKU")),
        }
        keys.discard("")
        if target_key and target_key in keys:
            candidates.add(str(record.get("record_id") or ""))
    if len(candidates) == 1:
        return next(iter(candidates)), []
    return "", ["产品未唯一匹配"]


def _source_key(record_id: str, parsed: ParsedPublicURL,
                platform_counts: dict[str, int], total_urls: int) -> str:
    if total_urls == 1:
        return record_id
    code = PLATFORM_CODES.get(parsed.platform, "OTHER")
    if platform_counts.get(parsed.platform, 0) == 1:
        return f"{record_id}#{code}"
    suffix = parsed.platform_id or sha1(parsed.normalized_url.encode("utf-8")).hexdigest()[:8]
    return f"{record_id}#{code}-{suffix}"


def _work_name(kol_name: str, product_name: str, parsed: ParsedPublicURL) -> str:
    code = PLATFORM_CODES.get(parsed.platform, "OTHER")
    identity = parsed.platform_id or sha1(parsed.normalized_url.encode("utf-8")).hexdigest()[:8]
    return f"{code}-{_text(kol_name) or 'KOL'}-{_text(product_name) or '产品'}-{identity}"


def plan_sync(source_records: list[dict], existing_work_records: list[dict],
              kol_records: list[dict], product_records: list[dict],
              created_not_before_ms: int = 0) -> dict:
    """Create a complete, non-writing plan for every source record/link."""
    source_key_index: dict[str, list[dict]] = {}
    identity_index: dict[tuple[str, str], list[dict]] = {}
    source_group_index: dict[str, set[str]] = {}
    for work in existing_work_records:
        fields = work.get("fields") or {}
        source_key = _text(fields.get("来源记录ID"))
        if source_key:
            source_key_index.setdefault(source_key, []).append(work)
            source_group = _text(fields.get("同源作品组"))
            if source_group:
                source_group_index.setdefault(source_base_record_id(source_key), set()).add(source_group)
        for identity in _work_identities(work):
            identity_index.setdefault(identity, []).append(work)

    product_by_id = {str(record.get("record_id") or ""): record for record in product_records}
    kol_by_id = {str(record.get("record_id") or ""): record for record in kol_records}
    items: list[dict] = []
    planned_source_keys: set[str] = set()
    planned_identities: set[tuple[str, str]] = set()

    ordered_sources = sorted(
        source_records,
        key=lambda record: (int(record.get("created_time") or 0), str(record.get("record_id") or "")),
    )
    for source in ordered_sources:
        source_record_id = str(source.get("record_id") or "")
        fields = source.get("fields") or {}
        created_time = int(source.get("created_time") or 0)
        urls = extract_urls(fields.get("上稿平台链接"))
        if created_not_before_ms and created_time < created_not_before_ms:
            items.append({
                "source_record_id": source_record_id,
                "source_key": source_record_id,
                "action": "ignored",
                "reasons": ["早于灰度起点"],
                "url": "",
                "platform": "",
                "platform_id": "",
                "fields": {},
                "existing_record_id": "",
            })
            continue
        if not urls:
            items.append({
                "source_record_id": source_record_id,
                "source_key": source_record_id,
                "action": "ignored",
                "reasons": ["未解析到公开作品URL"],
                "url": "",
                "platform": "",
                "platform_id": "",
                "fields": {},
                "existing_record_id": "",
            })
            continue

        parsed_urls = [parse_public_url(url) for url in urls]
        platform_counts: dict[str, int] = {}
        for parsed in parsed_urls:
            platform_counts[parsed.platform] = platform_counts.get(parsed.platform, 0) + 1
        repeated_platforms = {platform for platform, count in platform_counts.items() if platform and count > 1}
        kol_id, kol_reasons = _match_kol(fields, kol_records)
        product_id, product_reasons = _match_product(fields, product_records)
        relation_reasons = kol_reasons + product_reasons
        source_brand = _text(fields.get("品牌"))
        kol_name = _text((kol_by_id.get(kol_id) or {}).get("fields", {}).get("账号名"))
        product_fields = (product_by_id.get(product_id) or {}).get("fields") or {}
        product_name = _text(product_fields.get("素材归档名") or fields.get("产品"))
        existing_groups = source_group_index.get(source_record_id, set())
        group_name = (
            next(iter(existing_groups)) if len(existing_groups) == 1
            else f"{kol_name or _text(fields.get('KOL'))}｜{product_name or _text(fields.get('产品'))}｜{source_record_id}"
        )

        for parsed in parsed_urls:
            source_key = _source_key(source_record_id, parsed, platform_counts, len(parsed_urls))
            reasons = list(relation_reasons)
            if not is_public_work_url(parsed):
                reasons.append("平台链接不受支持或格式无效")
            if parsed.platform in repeated_platforms:
                reasons.append("同一平台含多个作品，无法自动判断同源关系")
            if len(existing_groups) > 1:
                reasons.append("来源记录已有多个同源作品组")

            exact_matches = source_key_index.get(source_key, [])
            exact_existing = exact_matches[0] if len(exact_matches) == 1 else None
            identities, identity_matches = _identity_match_closure(
                _all_url_identities(parsed), exact_matches, identity_index,
            )
            identity_existing = identity_matches[0] if len(identity_matches) == 1 else None
            exact_record_id = str((exact_existing or {}).get("record_id") or "")
            identity_record_id = str((identity_existing or {}).get("record_id") or "")
            multiple_existing_conflict = len(identity_matches) > 1
            cross_existing_conflict = bool(
                exact_existing and identity_existing and exact_record_id != identity_record_id
            )
            existing = None if multiple_existing_conflict or cross_existing_conflict else exact_existing or identity_existing
            if len(exact_matches) > 1:
                reasons.append("来源记录ID已命中多条作品行")
            if len(identity_matches) > 1:
                reasons.append("平台作品身份已命中多条作品行")
            if cross_existing_conflict:
                reasons.append("来源记录ID与作品链接分别命中不同作品行")
            elif exact_existing and not multiple_existing_conflict:
                exact_fields = exact_existing.get("fields") or {}
                existing_parsed = parse_url_field(exact_fields.get("作品链接"))
                existing_identity = _url_identity(existing_parsed)
                existing_platform = _text(exact_fields.get("发布平台"))
                existing_platform_id = _text(exact_fields.get("平台作品ID"))
                if existing_platform and existing_platform != parsed.platform:
                    reasons.append("来源记录ID已有平台与当前链接不一致")
                    existing = None
                elif existing_platform_id and parsed.platform_id and existing_platform_id.casefold() != parsed.platform_id.casefold():
                    reasons.append("来源记录ID已有平台作品ID与当前链接不一致")
                    existing = None
                elif existing_identity[1] and existing_identity != _url_identity(parsed):
                    reasons.append("来源记录ID已对应另一条作品链接")
                    existing = None
                elif not existing_identity[1] and not _is_sync_owned_exact_row(exact_existing, source_key):
                    reasons.append("来源记录ID对应作品缺少链接且不属于同步器")
                    existing = None
            repair_blocked_reasons: list[str] = []
            if existing:
                repair_blocked_reasons = list(dict.fromkeys(reasons))
                action = "existing"
                reasons = []
            elif source_key in planned_source_keys or bool(identities & planned_identities):
                action = "manual_review"
                reasons.append("本批与另一来源记录重复作品")
            elif reasons:
                action = "manual_review"
            else:
                action = "create"
                planned_source_keys.add(source_key)
                planned_identities.update(identities)

            archive_required = parsed.platform in ARCHIVE_SUPPORTED and parsed.content_type not in {"图文", "帖子"}
            work_fields = {
                "作品名称": _work_name(kol_name or fields.get("KOL"), product_name or fields.get("产品"), parsed),
                "同源作品组": group_name,
                "作品链接": {
                    "link": parsed.normalized_url,
                    "text": parsed.normalized_url,
                },
                "发布平台": parsed.platform,
                "关联KOL": [kol_id] if kol_id else [],
                "关联产品": [product_id] if product_id else [],
                "品牌": source_brand,
                "归档状态": "待下载" if archive_required else "无需下载",
                "内容类型": parsed.content_type,
                "数据抓取状态": "待抓取" if parsed.platform == "YouTube" else "平台不支持",
                "素材可复用": False,
                "作品状态": "正常",
                "平台作品ID": parsed.platform_id,
                "运营备注": SYNC_NOTE,
                "来源记录ID": source_key,
                "迁移状态": "新流程",
                "允许自动归档": False,
            }
            if not archive_required:
                work_fields["自动处理状态"] = "无需处理"
            items.append({
                "source_record_id": source_record_id,
                "source_key": source_key,
                "action": action,
                "reasons": list(dict.fromkeys(reasons)),
                "url": parsed.normalized_url,
                "platform": parsed.platform,
                "platform_id": parsed.platform_id,
                "fields": work_fields,
                "existing_record_id": str((existing or {}).get("record_id") or ""),
                "repair_blocked_reasons": repair_blocked_reasons,
            })

    counts = {name: sum(1 for item in items if item["action"] == name)
              for name in ("create", "existing", "manual_review", "ignored")}
    return {"counts": counts, "items": items}


def plan_source_backfill(source_record_id: str, source_fields: dict,
                         work_records: list[dict],
                         overrides: dict[str, dict] | None = None) -> dict:
    """Return a source-table update only after all sibling works reach a final state."""
    overrides = overrides or {}
    siblings: list[dict] = []
    for record in work_records:
        fields = record.get("fields") or {}
        if source_base_record_id(fields.get("来源记录ID")) != source_record_id:
            continue
        merged = {**fields, **(overrides.get(str(record.get("record_id") or "")) or {})}
        siblings.append({"record_id": record.get("record_id"), "fields": merged})
    if not siblings:
        return {"ready": False, "source_record_id": source_record_id, "fields": {}, "reason": "source_has_no_works"}
    source_urls = extract_urls(source_fields.get("上稿平台链接"))
    source_identities: set[tuple[str, str]] = set()
    for url in source_urls:
        parsed = parse_public_url(url)
        if not is_public_work_url(parsed):
            return {
                "ready": False,
                "source_record_id": source_record_id,
                "fields": {},
                "reason": "source_has_unrepresented_or_invalid_urls",
            }
        source_identities.update(_all_url_identities(parsed))
    sibling_identities: set[tuple[str, str]] = set()
    for record in siblings:
        fields = record.get("fields") or {}
        sibling_identities.update(_all_url_identities(
            parse_url_field(fields.get("作品链接")),
            platform=_text(fields.get("发布平台")),
            platform_id=_text(fields.get("平台作品ID")),
        ))
    if not source_identities or not source_identities.issubset(sibling_identities):
        return {
            "ready": False,
            "source_record_id": source_record_id,
            "fields": {},
            "reason": "source_has_unrepresented_or_invalid_urls",
        }
    states = [_text((record.get("fields") or {}).get("归档状态")) for record in siblings]
    if any(state not in {"已归档", "无需下载"} for state in states):
        return {"ready": False, "source_record_id": source_record_id, "fields": {}, "reason": "source_has_pending_works"}

    files: dict[str, str] = {}
    for record in siblings:
        fields = record.get("fields") or {}
        if _text(fields.get("归档状态")) != "已归档":
            continue
        link = _text(fields.get("归档文件链接"))
        filename = _text(fields.get("归档文件名")) or "归档视频"
        if "/file/" not in link:
            return {
                "ready": False,
                "source_record_id": source_record_id,
                "fields": {},
                "reason": "source_archive_file_missing",
            }
        files.setdefault(link, filename)
    if len(files) > 1:
        return {"ready": False, "source_record_id": source_record_id, "fields": {}, "reason": "source_has_multiple_archive_files"}
    if len(files) == 1:
        link, filename = next(iter(files.items()))
        return {
            "ready": True,
            "source_record_id": source_record_id,
            "fields": {
                "飞书云盘链接": {"link": link, "text": filename},
                "素材情况": "已下载",
            },
            "reason": "",
        }
    if all(state == "无需下载" for state in states):
        content_types = {
            _text((record.get("fields") or {}).get("内容类型")) for record in siblings
        }
        source_status = "图文" if content_types and content_types <= {"图文", "帖子"} else "下载不了"
        return {
            "ready": True,
            "source_record_id": source_record_id,
            "fields": {"素材情况": source_status},
            "reason": "",
        }
    return {"ready": False, "source_record_id": source_record_id, "fields": {}, "reason": "source_archive_file_missing"}


def _critical_readback_ok(record: dict, planned_fields: dict) -> bool:
    fields = record.get("fields") or {}
    if not (
        _text(fields.get("来源记录ID")) == _text(planned_fields.get("来源记录ID"))
        and _url_identity(parse_url_field(fields.get("作品链接")))
        == _url_identity(parse_url_field(planned_fields.get("作品链接")))
        and media_archive.linked_record_ids(fields.get("关联KOL")) == tuple(planned_fields.get("关联KOL") or [])
        and media_archive.linked_record_ids(fields.get("关联产品")) == tuple(planned_fields.get("关联产品") or [])
        and not bool(fields.get("允许自动归档"))
        and not bool(fields.get("素材可复用"))
        and _text(fields.get("发布平台")) == _text(planned_fields.get("发布平台"))
    ):
        return False
    for field_name in (
        "作品名称", "同源作品组", "品牌", "归档状态", "内容类型",
        "数据抓取状态", "作品状态", "平台作品ID", "迁移状态",
    ):
        if _text(fields.get(field_name)) != _text(planned_fields.get(field_name)):
            return False
    planned_auto_state = _text(planned_fields.get("自动处理状态"))
    if planned_auto_state and _text(fields.get("自动处理状态")) != planned_auto_state:
        return False
    return True


def _is_sync_owned_exact_row(record: dict, source_key: str) -> bool:
    fields = record.get("fields") or {}
    return (
        _text(fields.get("来源记录ID")) == source_key
        and SYNC_NOTE in _text(fields.get("运营备注"))
    )


def _field_is_missing(field_name: str, value) -> bool:
    if field_name in {"关联KOL", "关联产品"}:
        return not media_archive.linked_record_ids(value)
    if field_name == "作品链接":
        return not parse_url_field(value).original_url.strip()
    return not _text(value)


def _field_matches_plan(field_name: str, actual, expected) -> bool:
    if field_name in {"关联KOL", "关联产品"}:
        return media_archive.linked_record_ids(actual) == tuple(expected or [])
    if field_name == "作品链接":
        return _url_identity(parse_url_field(actual)) == _url_identity(parse_url_field(expected))
    return _text(actual) == _text(expected)


def _missing_initial_fields(record: dict, planned_fields: dict) -> dict:
    """Return only blank create-time fields; never roll back lifecycle state."""
    current = record.get("fields") or {}
    return {
        field_name: planned_fields[field_name]
        for field_name in REPAIRABLE_INITIAL_FIELDS
        if planned_fields.get(field_name) not in (None, "")
        and _field_is_missing(field_name, current.get(field_name))
    }


async def _write_planned_fields(record_id: str, planned_fields: dict,
                                 include_non_select: bool) -> None:
    if include_non_select:
        data_fields = {
            key: value for key, value in planned_fields.items()
            if key not in SELECT_FIELDS and value not in (None, "")
        }
        if data_fields:
            await feishu.update_record(config.T_UPLOAD_WORK, record_id, data_fields)
    for field_name in SELECT_FIELDS:
        value = planned_fields.get(field_name)
        if value not in (None, ""):
            await feishu.update_record(config.T_UPLOAD_WORK, record_id, {field_name: value})


async def _recover_uncertain_create(source_key: str) -> dict | None:
    """Read after an uncertain non-idempotent create instead of retrying POST."""
    refreshed = await feishu.fetch_all_records(
        config.T_UPLOAD_WORK, field_names=WORK_SYNC_FIELDS, page_size=200,
    )
    matches = [
        record for record in refreshed
        if _is_sync_owned_exact_row(record, source_key)
    ]
    if len(matches) > 1:
        raise RuntimeError(f"multiple work rows found after uncertain create: {source_key}")
    return matches[0] if matches else None


async def _sync_unlocked(commit: bool = False, source_record_id: str = "", max_creates: int = 1,
                         created_not_before_ms: int = 0, allow_batch: bool = False) -> dict:
    sources = await feishu.fetch_all_records(
        config.T_UPLOAD_INTAKE, field_names=SOURCE_FIELDS, page_size=200, automatic_fields=True,
    )
    if source_record_id:
        sources = [record for record in sources if str(record.get("record_id") or "") == source_record_id]
    works = await feishu.fetch_all_records(config.T_UPLOAD_WORK, field_names=WORK_SYNC_FIELDS, page_size=200)
    kols = await feishu.fetch_all_records(config.T_KOL, field_names=KOL_SYNC_FIELDS, page_size=500)
    products = await feishu.fetch_all_records(config.T_PRODUCT, field_names=PRODUCT_SYNC_FIELDS, page_size=200)
    result = plan_sync(
        sources, works, kols, products, created_not_before_ms=created_not_before_ms,
    )
    result.update({
        "commit": commit,
        "source_records": len(sources),
        "created": 0,
        "created_record_ids": [],
        "deferred_creates": 0,
        "deduped_after_recheck": 0,
        "repaired_record_ids": [],
    })
    if not commit:
        return result
    if not source_record_id and not allow_batch:
        raise ValueError("commit requires one explicit source_record_id during grey release")

    creates = sorted(
        (item for item in result["items"] if item["action"] == "create"),
        key=lambda item: (item["source_record_id"], item["source_key"]),
    )
    create_limit = max(0, int(max_creates))
    if len(creates) > create_limit and not allow_batch:
        raise ValueError(f"planned creates exceed grey limit: {len(creates)} > {max_creates}")
    if len(creates) > create_limit:
        result["deferred_creates"] = len(creates) - create_limit
        creates = creates[:create_limit]
    source_by_id = {str(record.get("record_id") or ""): record for record in sources}
    for item in creates:
        # Recheck immediately before the non-idempotent POST.  Together with
        # the process-wide commit lock this prevents overlapping ticks in the
        # single production service from creating the same work twice; the
        # recheck also catches a row written by another service instance.
        latest_works = await feishu.fetch_all_records(
            config.T_UPLOAD_WORK, field_names=WORK_SYNC_FIELDS, page_size=200,
        )
        latest_plan = plan_sync(
            [source_by_id[item["source_record_id"]]], latest_works, kols, products,
            created_not_before_ms=created_not_before_ms,
        )
        latest_item = next(
            (candidate for candidate in latest_plan["items"]
             if candidate["source_key"] == item["source_key"]),
            None,
        )
        if not latest_item or latest_item["action"] != "create":
            result["deduped_after_recheck"] += 1
            continue
        planned_fields = item["fields"]
        data_fields = {key: value for key, value in planned_fields.items() if key not in SELECT_FIELDS and value not in (None, "")}
        try:
            record_id = await feishu.create_record(config.T_UPLOAD_WORK, data_fields)
        except Exception:
            recovered = await _recover_uncertain_create(item["source_key"])
            if not recovered:
                raise
            record_id = str(recovered.get("record_id") or "")
        await _write_planned_fields(record_id, planned_fields, include_non_select=False)
        readback = await feishu.get_record(config.T_UPLOAD_WORK, record_id)
        if not _critical_readback_ok(readback, planned_fields):
            raise RuntimeError(f"created work readback failed: {record_id}")
        result["created"] += 1
        result["created_record_ids"].append(record_id)

    # A previous create may have landed while its response or a later field
    # update failed.  Only fill fields that are still blank.  In particular,
    # never roll back operator approval, archive progress, metrics state or
    # reuse decisions on an otherwise healthy work row.
    work_by_id = {str(record.get("record_id") or ""): record for record in works}
    for item in result["items"]:
        if item["action"] != "existing" or not item["existing_record_id"]:
            continue
        if item.get("repair_blocked_reasons"):
            continue
        existing = work_by_id.get(item["existing_record_id"])
        if not existing or not _is_sync_owned_exact_row(existing, item["source_key"]):
            continue
        repair_fields = _missing_initial_fields(existing, item["fields"])
        if not repair_fields:
            continue
        await _write_planned_fields(
            item["existing_record_id"], repair_fields, include_non_select=True,
        )
        readback = await feishu.get_record(config.T_UPLOAD_WORK, item["existing_record_id"])
        readback_fields = readback.get("fields") or {}
        if not all(
            _field_matches_plan(field_name, readback_fields.get(field_name), expected)
            for field_name, expected in repair_fields.items()
        ):
            raise RuntimeError(f"repaired work readback failed: {item['existing_record_id']}")
        result["repaired_record_ids"].append(item["existing_record_id"])
    return result


async def sync(commit: bool = False, source_record_id: str = "", max_creates: int = 1,
               created_not_before_ms: int = 0, allow_batch: bool = False) -> dict:
    """Fetch live tables and serialize every write-capable synchronization run."""
    kwargs = {
        "commit": commit,
        "source_record_id": source_record_id,
        "max_creates": max_creates,
        "created_not_before_ms": created_not_before_ms,
        "allow_batch": allow_batch,
    }
    if not commit:
        return await _sync_unlocked(**kwargs)
    async with _SYNC_COMMIT_LOCK:
        return await _sync_unlocked(**kwargs)
