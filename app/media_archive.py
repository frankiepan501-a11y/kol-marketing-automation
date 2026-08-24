"""KOL upload-work validation, grouping, naming, and YouTube metric mapping.

This module contains the deterministic cloud-side rules.  Large downloads and
Drive uploads live in ``tools.media_archive_worker`` and consume the plans
produced here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Iterable
from urllib.parse import parse_qs, urlparse


SUPPORTED_PLATFORMS = ("YouTube", "Instagram", "TikTok")
PLATFORM_PRIORITY = {"YouTube": 0, "Instagram": 1, "TikTok": 2}
PLATFORM_CODES = {"YouTube": "YT", "Instagram": "IG", "TikTok": "TK"}
METRIC_MILESTONES = (0, 1, 3, 7, 14, 30, 60, 90)
MAX_ARCHIVE_RETRIES = 3


@dataclass(frozen=True)
class WorkValidation:
    record_id: str
    valid: bool
    platform: str
    url: str
    platform_id: str
    errors: tuple[str, ...]


@dataclass(frozen=True)
class ArchiveGroupPlan:
    group_key: str
    master_record_id: str
    follower_record_ids: tuple[str, ...]
    action: str
    archive_file_url: str = ""


def field_text(value) -> str:
    """Extract a readable scalar from common Feishu Bitable field shapes."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("link", "text", "name"):
            if value.get(key) is not None:
                return str(value[key]).strip()
        return ""
    if isinstance(value, list):
        parts = [field_text(item) for item in value]
        return "".join(part for part in parts if part).strip()
    return str(value).strip()


def linked_record_ids(value) -> tuple[str, ...]:
    ids: list[str] = []
    values = value if isinstance(value, list) else [value]
    for item in values:
        if not isinstance(item, dict):
            continue
        direct = item.get("id")
        if direct:
            ids.append(str(direct))
        for key in ("record_ids", "link_record_ids"):
            for record_id in item.get(key) or []:
                ids.append(str(record_id))
    return tuple(dict.fromkeys(ids))


def _canonical_platform(value) -> str:
    text = field_text(value).lower()
    aliases = {
        "youtube": "YouTube",
        "youtube shorts": "YouTube",
        "yt": "YouTube",
        "tiktok": "TikTok",
        "tk": "TikTok",
        "instagram": "Instagram",
        "instagram reels": "Instagram",
        "ig": "Instagram",
    }
    return aliases.get(text, field_text(value))


def platform_and_id_from_url(url: str) -> tuple[str, str]:
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return "", ""
    host = (parsed.hostname or "").lower().removeprefix("www.")
    path = parsed.path.strip("/")

    if host in {"youtu.be", "youtube.com", "m.youtube.com", "music.youtube.com"}:
        if host == "youtu.be":
            video_id = path.split("/", 1)[0]
        else:
            video_id = (parse_qs(parsed.query).get("v") or [""])[0]
            if not video_id:
                match = re.match(r"(?:shorts|live|embed)/([^/?#]+)", path)
                video_id = match.group(1) if match else ""
        return "YouTube", video_id

    if host.endswith("tiktok.com"):
        match = re.search(r"/video/(\d+)", parsed.path)
        return "TikTok", match.group(1) if match else ""

    if host in {"instagram.com", "m.instagram.com"}:
        match = re.match(r"(?:reel|reels|p|tv)/([^/?#]+)", path)
        return "Instagram", match.group(1) if match else ""

    return "", ""


def validate_work_row(record: dict) -> WorkValidation:
    fields = record.get("fields") or {}
    record_id = str(record.get("record_id") or "")
    declared_platform = _canonical_platform(fields.get("发布平台"))
    url = field_text(fields.get("作品链接"))
    detected_platform, platform_id = platform_and_id_from_url(url)
    errors: list[str] = []

    if declared_platform not in SUPPORTED_PLATFORMS:
        errors.append("发布平台不支持")
    if not url or not detected_platform:
        errors.append("作品链接格式无效")
    elif declared_platform != detected_platform:
        errors.append("发布平台与作品链接不一致")
    kol_ids = linked_record_ids(fields.get("关联KOL"))
    product_ids = linked_record_ids(fields.get("关联产品"))
    if not kol_ids:
        errors.append("缺少关联KOL")
    elif len(kol_ids) != 1:
        errors.append("关联KOL必须且只能有一条")
    if not product_ids:
        errors.append("缺少关联产品")
    elif len(product_ids) != 1:
        errors.append("关联产品必须且只能有一条")
    if not field_text(fields.get("品牌")):
        errors.append("缺少品牌")

    return WorkValidation(
        record_id=record_id,
        valid=not errors,
        platform=declared_platform,
        url=url,
        platform_id=platform_id,
        errors=tuple(errors),
    )


def _enabled(value) -> bool:
    if isinstance(value, bool):
        return value
    return field_text(value).lower() in {"1", "true", "yes", "是", "勾选"}


def _group_key(record: dict) -> str:
    fields = record.get("fields") or {}
    return field_text(fields.get("同源作品组")) or str(record.get("record_id") or "")


def _priority(record: dict) -> tuple[int, str]:
    platform = _canonical_platform((record.get("fields") or {}).get("发布平台"))
    return PLATFORM_PRIORITY.get(platform, 99), str(record.get("record_id") or "")


def plan_archive_groups(records: Iterable[dict], max_retries: int = MAX_ARCHIVE_RETRIES) -> list[ArchiveGroupPlan]:
    grouped: dict[str, list[dict]] = {}
    for record in records:
        grouped.setdefault(_group_key(record), []).append(record)

    plans: list[ArchiveGroupPlan] = []
    for group_key in sorted(grouped):
        group = grouped[group_key]
        processing = any(
            field_text((record.get("fields") or {}).get("自动处理状态")) == "处理中"
            for record in group
        )
        if processing:
            plans.append(ArchiveGroupPlan(
                group_key=group_key,
                master_record_id="",
                follower_record_ids=tuple(sorted(str(r.get("record_id") or "") for r in group)),
                action="skip",
            ))
            continue
        archived = [
            record for record in group
            if field_text((record.get("fields") or {}).get("归档状态")) == "已归档"
            and field_text((record.get("fields") or {}).get("归档文件链接"))
        ]
        if archived:
            master = sorted(archived, key=_priority)[0]
            master_id = str(master.get("record_id") or "")
            followers = tuple(sorted(
                str(record.get("record_id") or "") for record in group
                if str(record.get("record_id") or "") != master_id
            ))
            plans.append(ArchiveGroupPlan(
                group_key=group_key,
                master_record_id=master_id,
                follower_record_ids=followers,
                action="propagate_existing",
                archive_file_url=field_text((master.get("fields") or {}).get("归档文件链接")),
            ))
            continue

        candidates = []
        for record in group:
            fields = record.get("fields") or {}
            retries = int(float(field_text(fields.get("重试次数")) or 0))
            exhausted = field_text(fields.get("归档状态")) == "下载失败" and retries >= max_retries
            if _enabled(fields.get("允许自动归档")) and not exhausted and validate_work_row(record).valid:
                candidates.append(record)

        if not candidates:
            plans.append(ArchiveGroupPlan(
                group_key=group_key,
                master_record_id="",
                follower_record_ids=tuple(sorted(str(r.get("record_id") or "") for r in group)),
                action="skip",
            ))
            continue

        master = sorted(candidates, key=_priority)[0]
        master_id = str(master.get("record_id") or "")
        followers = tuple(sorted(
            str(record.get("record_id") or "") for record in group
            if str(record.get("record_id") or "") != master_id
        ))
        plans.append(ArchiveGroupPlan(
            group_key=group_key,
            master_record_id=master_id,
            follower_record_ids=followers,
            action="queue_download",
        ))
    return plans


_WINDOWS_INVALID = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _filename_part(value: str) -> str:
    text = _WINDOWS_INVALID.sub(" ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip(" .-")
    return text or "unknown"


def build_archive_filename(platform: str, kol_name: str, product_name: str,
                           sequence: int, extension: str = "mp4") -> str:
    code = PLATFORM_CODES.get(_canonical_platform(platform))
    if not code:
        raise ValueError(f"unsupported platform: {platform}")
    sequence_i = max(1, int(sequence))
    ext = re.sub(r"[^A-Za-z0-9]", "", extension or "mp4").lower() or "mp4"
    return f"{code}-{_filename_part(kol_name)}-{_filename_part(product_name)}-{sequence_i:02d}.{ext}"


def map_youtube_video(item: dict) -> dict:
    snippet = item.get("snippet") or {}
    statistics = item.get("statistics") or {}
    published = snippet.get("publishedAt")
    published_ms = None
    if published:
        dt = datetime.fromisoformat(str(published).replace("Z", "+00:00"))
        published_ms = int(dt.timestamp() * 1000)

    def public_count(name: str):
        value = statistics.get(name)
        return int(value) if value not in (None, "") else None

    return {
        "平台作品ID": str(item.get("id") or ""),
        "作品名称": str(snippet.get("title") or ""),
        "上稿日期": published_ms,
        "播放量": public_count("viewCount"),
        "点赞量": public_count("likeCount"),
        "评论数": public_count("commentCount"),
    }


def next_metric_milestone(published_at: datetime, now: datetime,
                          captured_days: set[int] | frozenset[int]) -> int | None:
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_days = max(0, int((now - published_at).total_seconds() // 86400))
    for day in METRIC_MILESTONES:
        if day <= age_days and day not in captured_days:
            return day
    for day in METRIC_MILESTONES:
        if day > age_days and day not in captured_days:
            return day
    return None


def due_metric_milestone(published_at: datetime, now: datetime,
                         captured_days: set[int] | frozenset[int]) -> int | None:
    """Return an honest observation day, never invent a missed historical snapshot.

    A pre-existing work gets one baseline labelled with its real age.  After
    that, only an exact review milestone is captured.  Current counts must not
    be written back as fake D0/D1 history for an old video.
    """
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_days = max(0, int((now - published_at).total_seconds() // 86400))
    if not captured_days:
        return age_days
    if age_days in METRIC_MILESTONES and age_days not in captured_days:
        return age_days
    return None


def next_future_metric_milestone(published_at: datetime, now: datetime,
                                 captured_days: set[int] | frozenset[int]) -> int | None:
    """Return the next review day strictly after the current observation day."""
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_days = max(0, int((now - published_at).total_seconds() // 86400))
    return next(
        (day for day in METRIC_MILESTONES if day > age_days and day not in captured_days),
        None,
    )
