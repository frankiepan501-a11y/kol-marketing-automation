from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from .clients import ApiError, FeishuClient, YouTubeClient
from .constants import (
    BASE_TOKEN,
    BRAND,
    CONFIG_READ_FIELDS,
    CONFIG_RECORD_ID,
    EVENT_READ_FIELDS,
    KEYWORD,
    PLATFORM,
    POST_READ_FIELDS,
    POST_SINGLE_SELECT_FIELDS,
    TABLES,
)
from .core import (
    display_datetime,
    incremental_window,
    is_youtube_video_id,
    parse_datetime,
    query_groups,
    rfc3339,
    scalar,
    schedule_decision,
    split_terms,
    stable_hash,
    unique_lines,
)

_DURATION = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)
_COUNTER_FIELDS = ("粉丝数快照", "帖子数快照", "曝光量", "点赞数", "评论数")
_REFRESH_FIELDS = _COUNTER_FIELDS


def post_unique_key(video_id: str) -> str:
    """Return the existing Feishu identity encoding for YouTube + post_id."""
    if not is_youtube_video_id(video_id):
        raise ValueError("invalid YouTube video id")
    return f"5:{video_id}"


def index_youtube_rows_by_post_id(
    rows: list[dict[str, Any]], *, target_ids: set[str]
) -> dict[str, dict[str, Any]]:
    """Index only requested YouTube IDs and fail closed on an existing duplicate."""
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if _text(row.get("平台")) != PLATFORM:
            continue
        post_id = _text(row.get("帖子ID"))
        if post_id not in target_ids:
            continue
        if post_id in indexed:
            raise ApiError(
                "feishu",
                "duplicate_post_id",
                f"multiple YouTube records have post id {post_id}",
            )
        indexed[post_id] = row
    return indexed


def _text(value: Any) -> str:
    value = scalar(value)
    return value if isinstance(value, str) else ""


def _number(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _duration_seconds(value: Any) -> int:
    match = _DURATION.match(str(value or ""))
    if not match:
        return 0
    values = {name: int(raw or 0) for name, raw in match.groupdict().items()}
    return values["days"] * 86400 + values["hours"] * 3600 + values["minutes"] * 60 + values["seconds"]


def _published(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return display_datetime(parsed)
    except ValueError:
        return text[:19].replace("T", " ")


def _relevance(title: str, content: str, config: dict[str, Any]) -> tuple[str, int, str]:
    haystack = f"{title} {content}".casefold()
    for exclusion in split_terms(config.get("排除词")):
        if exclusion.casefold() in haystack:
            return "无关", 0, f"命中排除词：{exclusion}"
    signals = [KEYWORD, *split_terms(config.get("关键词别名"))]
    hits = [term for term in signals if term.casefold() in haystack]
    if hits:
        return "疑似", 60, f"命中竞品关键词：{', '.join(hits[:3])}；待AI复核"
    return "待分析", 20, "未命中明确排除词；待AI复核"


def _thumbnail(snippet: dict[str, Any]) -> str:
    thumbnails = snippet.get("thumbnails") if isinstance(snippet.get("thumbnails"), dict) else {}
    for size in ("maxres", "standard", "high", "medium", "default"):
        item = thumbnails.get(size)
        if isinstance(item, dict) and item.get("url"):
            return str(item["url"])
    return ""


def normalize_video(
    video: dict[str, Any],
    channel: dict[str, Any] | None,
    *,
    config: dict[str, Any],
    evidence: dict[str, list[str]],
    batch_id: str,
    captured_at: datetime,
) -> dict[str, Any]:
    video_id = str(video.get("id") or "")
    snippet = video.get("snippet") if isinstance(video.get("snippet"), dict) else {}
    details = video.get("contentDetails") if isinstance(video.get("contentDetails"), dict) else {}
    statistics = video.get("statistics") if isinstance(video.get("statistics"), dict) else {}
    channel = channel if isinstance(channel, dict) else {}
    channel_snippet = channel.get("snippet") if isinstance(channel.get("snippet"), dict) else {}
    channel_statistics = channel.get("statistics") if isinstance(channel.get("statistics"), dict) else {}
    channel_id = str(snippet.get("channelId") or channel.get("id") or "")
    custom_url = str(channel_snippet.get("customUrl") or "")
    if custom_url and not custom_url.startswith("@"):
        custom_url = f"@{custom_url.lstrip('/')}"
    title = str(snippet.get("title") or "")
    content = str(snippet.get("description") or "")
    relevance, score, reason = _relevance(title, content, config)
    sources = list(dict.fromkeys([*(evidence.get("sources") or []), "YouTube API"]))
    raw = {"video": video, "channel": channel, "evidence": evidence}
    fields: dict[str, Any] = {
        "唯一键": post_unique_key(video_id),
        "竞品品牌": BRAND,
        "命中关键词": KEYWORD,
        "平台": PLATFORM,
        "帖子ID": video_id,
        "帖子标题": title,
        "帖子内容": content,
        "发布时间": _published(snippet.get("publishedAt")),
        "帖子URL": f"https://www.youtube.com/watch?v={video_id}",
        "缩略图URL": _thumbnail(snippet),
        "附件类型": ["video"],
        "KOL平台ID": channel_id,
        "KOL账号名": str(channel_snippet.get("title") or snippet.get("channelTitle") or ""),
        "KOL账号Handle": custom_url,
        "KOL主页URL": f"https://www.youtube.com/channel/{channel_id}" if channel_id else "",
        "粉丝数快照": _number(channel_statistics.get("subscriberCount")),
        "帖子数快照": _number(channel_statistics.get("videoCount")),
        "曝光量": _number(statistics.get("viewCount")),
        "点赞数": _number(statistics.get("likeCount")),
        "评论数": _number(statistics.get("commentCount")),
        "抓取时间": display_datetime(captured_at),
        "采集批次ID": batch_id,
        "原始数据哈希": stable_hash(raw),
        "采集来源": sources,
        "YouTube命中查询词": "\n".join(evidence.get("queries") or []),
        "YouTube查询时间窗": "\n".join(evidence.get("windows") or []),
        "视频时长秒": _duration_seconds(details.get("duration")),
        "字幕可用": str(details.get("caption") or "").casefold() == "true",
        "视频标签": "\n".join(str(tag) for tag in snippet.get("tags", []) if str(tag)),
        "相关性": relevance,
        "AI相关性分": score,
        "AI判断依据": reason,
        "内容类型": "待分析",
        "合作信号": "待分析",
        "营销阶段": "待分析",
        "AI分析状态": "待分析",
        "人工复核状态": "待复核",
        "关联监控任务": [{"id": CONFIG_RECORD_ID}],
    }
    return {name: value for name, value in fields.items() if value not in (None, "")}


def _old_value(row: dict[str, Any], name: str) -> Any:
    value = scalar(row.get(name))
    if name == "发布时间" and value:
        try:
            return display_datetime(parse_datetime(value))
        except ValueError:
            return value
    if name in _COUNTER_FIELDS:
        return _number(value)
    if name in {"采集来源", "附件类型"}:
        return sorted(split_terms(value))
    if name in {"YouTube命中查询词", "YouTube查询时间窗", "视频标签"}:
        return unique_lines(value)
    if name in {"帖子URL", "缩略图URL", "KOL主页URL"} and isinstance(row.get(name), dict):
        return str(row[name].get("link") or row[name].get("text") or "")
    return value


def build_update(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    update: dict[str, Any] = {}
    for name in _REFRESH_FIELDS:
        if name not in incoming:
            continue
        new = incoming[name]
        if name in _COUNTER_FIELDS and new is None:
            continue
        if name == "采集来源":
            new = sorted(set(split_terms(existing.get(name))) | set(split_terms(new)))
        elif name in {"YouTube命中查询词", "YouTube查询时间窗"}:
            new = unique_lines(existing.get(name), new)
        if new not in (None, "") and new != _old_value(existing, name):
            update[name] = new
    if update:
        for name in ("抓取时间", "采集批次ID", "原始数据哈希"):
            if incoming.get(name) not in (None, ""):
                update[name] = incoming[name]
    return update


class IncrementalCollector:
    def __init__(self, feishu: FeishuClient, youtube: YouTubeClient):
        self.feishu = feishu
        self.youtube = youtube

    def _config(self) -> dict[str, Any]:
        config = self.feishu.get_record(BASE_TOKEN, TABLES["keyword_config"], CONFIG_RECORD_ID)
        if scalar(config.get("启用")) is not True:
            raise ValueError("NYXI YouTube config is disabled")
        if _text(config.get("平台")) != PLATFORM or _text(config.get("关键词")).casefold() != KEYWORD:
            raise ValueError("NYXI YouTube config identity mismatch")
        return {name: config.get(name) for name in CONFIG_READ_FIELDS} | {"_record_id": CONFIG_RECORD_ID}

    def mark_started(self, *, job_id: str, now: datetime) -> None:
        self.feishu.batch_update(
            BASE_TOKEN,
            TABLES["keyword_config"],
            [
                (
                    CONFIG_RECORD_ID,
                    {
                        "YouTube历史进度": (
                            f"云端增量运行中；job={job_id}；开始={display_datetime(now)}；"
                            "最近成功水位暂不变"
                        ),
                        "错误摘要": "",
                    },
                )
            ],
        )
        self.feishu.batch_update(
            BASE_TOKEN,
            TABLES["keyword_config"],
            [(CONFIG_RECORD_ID, {"运行状态": "待运行"})],
        )

    def _search(
        self, config: dict[str, Any], start: datetime, end: datetime
    ) -> tuple[dict[str, dict[str, list[str]]], int]:
        evidence: dict[str, dict[str, list[str]]] = {}
        calls = 0
        window = f"{rfc3339(start)}/{rfc3339(end)}"
        for terms in query_groups(config, KEYWORD).values():
            for query in terms:
                token: str | None = None
                for page_number in range(1, 11):
                    response = self.youtube.search(
                        query,
                        published_after=rfc3339(start),
                        published_before=rfc3339(end),
                        page_token=token,
                    )
                    calls += 1
                    for item in response.get("items", []) or []:
                        identifier = item.get("id") if isinstance(item, dict) else None
                        video_id = identifier.get("videoId") if isinstance(identifier, dict) else ""
                        if not is_youtube_video_id(video_id):
                            continue
                        item_evidence = evidence.setdefault(
                            video_id, {"sources": [], "queries": [], "windows": []}
                        )
                        for name, value in (
                            ("sources", "YouTube API"),
                            ("queries", query),
                            ("windows", window),
                        ):
                            if value not in item_evidence[name]:
                                item_evidence[name].append(value)
                    token = str(response.get("nextPageToken") or "") or None
                    if not token:
                        break
                    if page_number == 10:
                        raise ApiError("youtube", "incremental_page_cap", f"query exceeded 10 pages: {query}")
        return evidence, calls

    def run(
        self,
        *,
        now: datetime,
        commit: bool,
        force: bool = False,
        job_id: str = "",
    ) -> dict[str, Any]:
        config = self._config()
        events = self.feishu.list_records(
            BASE_TOKEN, TABLES["marketing_events"], field_names=EVENT_READ_FIELDS
        )
        decision = schedule_decision(now, events, brand=BRAND, force=force)
        if not decision.should_run:
            return {
                "ok": True,
                "status": "skipped",
                "reason": decision.reason,
                "active_events": list(decision.active_events),
                "mode": "commit" if commit else "preview",
            }

        if commit:
            self.mark_started(job_id=job_id or "direct-run", now=now)

        start, end = incremental_window(config, now)
        evidence, search_calls = self._search(config, start, end)
        all_rows = self.feishu.list_records(
            BASE_TOKEN, TABLES["competitor_posts"], field_names=POST_READ_FIELDS
        )
        existing_rows = [
            row
            for row in all_rows
            if _text(row.get("竞品品牌")).casefold() == BRAND.casefold()
            and _text(row.get("平台")) == PLATFORM
        ]
        existing = {
            _text(row.get("唯一键")): row
            for row in all_rows
            if _text(row.get("唯一键"))
        }
        current_ids = {
            _text(row.get("帖子ID"))
            for row in existing_rows
            if is_youtube_video_id(_text(row.get("帖子ID")))
        }
        requested_ids = sorted(current_ids | set(evidence))
        existing_by_post_id = index_youtube_rows_by_post_id(
            all_rows, target_ids=set(requested_ids)
        )
        videos = self.youtube.videos(requested_ids)
        found_ids = {str(video.get("id") or "") for video in videos}
        channel_ids = sorted(
            {
                str(video.get("snippet", {}).get("channelId") or "")
                for video in videos
                if isinstance(video.get("snippet"), dict)
                and video.get("snippet", {}).get("channelId")
            }
        )
        channels = {str(item.get("id") or ""): item for item in self.youtube.channels(channel_ids)}
        batch_id = f"ytinc-{now.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        normalized: list[dict[str, Any]] = []
        for video in videos:
            snippet = video.get("snippet") if isinstance(video.get("snippet"), dict) else {}
            channel_id = str(snippet.get("channelId") or "")
            video_id = str(video.get("id") or "")
            normalized.append(
                normalize_video(
                    video,
                    channels.get(channel_id),
                    config=config,
                    evidence=evidence.get(video_id, {"sources": [], "queries": [], "windows": []}),
                    batch_id=batch_id,
                    captured_at=now,
                )
            )

        new_rows = [
            row
            for row in normalized
            if row["唯一键"] not in existing
            and str(row.get("帖子ID") or "") not in existing_by_post_id
        ]
        updates: list[tuple[str, dict[str, Any]]] = []
        for row in normalized:
            old = existing.get(row["唯一键"]) or existing_by_post_id.get(
                str(row.get("帖子ID") or "")
            )
            if not old:
                continue
            change = build_update(old, row)
            if change:
                updates.append((str(old["_record_id"]), change))
        new_channels = {
            str(row.get("KOL平台ID") or "")
            for row in new_rows
            if row.get("KOL平台ID") and row.get("相关性") != "无关"
        }

        if commit:
            base_rows = [
                {name: value for name, value in row.items() if name not in POST_SINGLE_SELECT_FIELDS}
                for row in new_rows
            ]
            select_rows = [
                {name: value for name, value in row.items() if name in POST_SINGLE_SELECT_FIELDS}
                for row in new_rows
            ]
            created_ids = self.feishu.batch_create(
                BASE_TOKEN, TABLES["competitor_posts"], base_rows
            )
            self.feishu.batch_update(
                BASE_TOKEN,
                TABLES["competitor_posts"],
                [
                    (record_id, select_fields)
                    for record_id, select_fields in zip(created_ids, select_rows)
                    if select_fields
                ],
            )
            base_updates: list[tuple[str, dict[str, Any]]] = []
            select_updates: list[tuple[str, dict[str, Any]]] = []
            for record_id, fields in updates:
                base = {name: value for name, value in fields.items() if name not in POST_SINGLE_SELECT_FIELDS}
                selects = {name: value for name, value in fields.items() if name in POST_SINGLE_SELECT_FIELDS}
                if base:
                    base_updates.append((record_id, base))
                if selects:
                    select_updates.append((record_id, selects))
            self.feishu.batch_update(BASE_TOKEN, TABLES["competitor_posts"], base_updates)
            self.feishu.batch_update(BASE_TOKEN, TABLES["competitor_posts"], select_updates)
            config_fields = {
                "最近成功采集时间": display_datetime(now),
                "最近采集水位": display_datetime(end),
                "最近新增帖子数": len(new_rows),
                "最近新增KOL候选数": len(new_channels),
                "YouTube历史进度": (
                    "云端增量完成；调度=周一09:30+新品期周三/周五09:30；"
                    f"job={job_id or batch_id}；batch={batch_id}；"
                    f"窗口={rfc3339(start)}/{rfc3339(end)}；"
                    f"新增={len(new_rows)}；公开数据更新={len(updates)}；不可用={len(set(requested_ids) - found_ids)}"
                ),
                "错误摘要": "",
            }
            self.feishu.batch_update(
                BASE_TOKEN,
                TABLES["keyword_config"],
                [(CONFIG_RECORD_ID, config_fields)],
            )
            self.feishu.batch_update(
                BASE_TOKEN,
                TABLES["keyword_config"],
                [(CONFIG_RECORD_ID, {"运行状态": "正常"})],
            )

        return {
            "ok": True,
            "status": "completed",
            "mode": "commit" if commit else "preview",
            "reason": decision.reason,
            "active_events": list(decision.active_events),
            "batch_id": batch_id,
            "window_start": rfc3339(start),
            "window_end": rfc3339(end),
            "search_calls": search_calls,
            "searched_new_ids": len(evidence),
            "existing_rows": len(existing_rows),
            "refreshed_video_ids": len(found_ids),
            "unavailable_video_ids": len(set(requested_ids) - found_ids),
            "new_posts": len(new_rows),
            "updated_existing": len(updates),
            "candidate_new_kols": len(new_channels),
            "kol_master_writes": 0,
            "outbound_messages": 0,
        }

    def replay_video(
        self, video_id: str, *, now: datetime, commit: bool = False
    ) -> dict[str, Any]:
        """Re-run one public video without advancing the incremental waterline."""
        if not is_youtube_video_id(video_id):
            raise ValueError("invalid YouTube video id")
        config = self._config()
        rows = self.feishu.list_records(
            BASE_TOKEN, TABLES["competitor_posts"], field_names=POST_READ_FIELDS
        )
        matches = [
            row
            for row in rows
            if _text(row.get("竞品品牌")).casefold() == BRAND.casefold()
            and _text(row.get("平台")) == PLATFORM
            and _text(row.get("帖子ID")) == video_id
        ]
        if len(matches) > 1:
            raise ApiError(
                "feishu",
                "duplicate_post_id",
                f"multiple NYXI YouTube records have post id {video_id}",
            )
        existing = matches[0] if matches else None
        videos = self.youtube.videos([video_id])
        if not videos:
            return {
                "ok": True,
                "status": "unavailable",
                "mode": "commit" if commit else "preview",
                "video_id": video_id,
                "existing_before": existing is not None,
                "waterline_advanced": False,
                "kol_master_writes": 0,
                "outbound_messages": 0,
            }
        video = videos[0]
        snippet = video.get("snippet") if isinstance(video.get("snippet"), dict) else {}
        channel_id = str(snippet.get("channelId") or "")
        channels = self.youtube.channels([channel_id]) if channel_id else []
        channel = channels[0] if channels else None
        batch_id = f"ytreplay-{now.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        incoming = normalize_video(
            video,
            channel,
            config=config,
            evidence={"sources": ["YouTube API"], "queries": [], "windows": []},
            batch_id=batch_id,
            captured_at=now,
        )
        change = build_update(existing, incoming) if existing else incoming
        if commit and change:
            if existing:
                self.feishu.batch_update(
                    BASE_TOKEN,
                    TABLES["competitor_posts"],
                    [(str(existing["_record_id"]), change)],
                )
            else:
                base_fields = {
                    name: value
                    for name, value in incoming.items()
                    if name not in POST_SINGLE_SELECT_FIELDS
                }
                select_fields = {
                    name: value
                    for name, value in incoming.items()
                    if name in POST_SINGLE_SELECT_FIELDS
                }
                record_id = self.feishu.batch_create(
                    BASE_TOKEN, TABLES["competitor_posts"], [base_fields]
                )[0]
                if select_fields:
                    self.feishu.batch_update(
                        BASE_TOKEN,
                        TABLES["competitor_posts"],
                        [(record_id, select_fields)],
                    )
        return {
            "ok": True,
            "status": "completed",
            "mode": "commit" if commit else "preview",
            "video_id": video_id,
            "existing_before": existing is not None,
            "created": bool(commit and not existing),
            "changed_fields": sorted(change),
            "waterline_advanced": False,
            "kol_master_writes": 0,
            "outbound_messages": 0,
        }

    def mark_failure(self, error: Exception, *, job_id: str = "") -> None:
        code = error.code if isinstance(error, ApiError) else type(error).__name__
        self.feishu.batch_update(
            BASE_TOKEN,
            TABLES["keyword_config"],
            [
                (
                    CONFIG_RECORD_ID,
                    {
                        "错误摘要": f"云端增量失败：{code}",
                        "YouTube历史进度": (
                            f"云端增量失败；job={job_id or 'unknown'}；错误类型={code}；"
                            "最近成功水位未推进"
                        ),
                    },
                )
            ],
        )
        self.feishu.batch_update(
            BASE_TOKEN,
            TABLES["keyword_config"],
            [(CONFIG_RECORD_ID, {"运行状态": "失败"})],
        )
