"""集中上稿活动的承诺/实际上稿事实回填。

只接受三类可核验事实：
1. 达人回复正文里同时出现明确发布动作和具体日期；
2. 达人回复已被分类为 live_link_received 且正文含内容级公开链接；
3. “实际上稿时间”还必须能从同一回复提取“已经发布+具体日期”。

“感兴趣 / 要报价 / 洽谈中”不会被推断成承诺日期。
"""

from __future__ import annotations

import asyncio
import html
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from . import config, feishu, launch_evidence
from .feishu import ext, ext_url


DAY_MS = 24 * 60 * 60 * 1000
DRAFT_CACHE_SECONDS = 60
ACTIVE_STATES = {"已入围", "锁定准备中"}
SENT_STATES = {"已发", "已发送"}
SOCIAL_HOSTS = {
    "youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com",
    "tiktok.com", "www.tiktok.com", "instagram.com", "www.instagram.com",
    "facebook.com", "www.facebook.com", "x.com", "www.x.com",
    "twitter.com", "www.twitter.com", "twitch.tv", "www.twitch.tv",
    "clips.twitch.tv", "vm.tiktok.com", "vt.tiktok.com",
}
OWN_HOST_MARKERS = ("powkong", "funlab", "amazon.", "amzn.")

_MONTHS = {
    "jan": 1, "january": 1, "januar": 1, "enero": 1,
    "feb": 2, "february": 2, "februar": 2, "febrero": 2,
    "mar": 3, "march": 3, "märz": 3, "marz": 3, "marzo": 3,
    "apr": 4, "april": 4, "abril": 4,
    "may": 5, "mai": 5, "mayo": 5,
    "jun": 6, "june": 6, "juni": 6, "junio": 6,
    "jul": 7, "july": 7, "juli": 7, "julio": 7,
    "aug": 8, "august": 8, "agosto": 8,
    "sep": 9, "sept": 9, "september": 9, "septiembre": 9,
    "oct": 10, "october": 10, "oktober": 10, "octubre": 10,
    "nov": 11, "november": 11, "noviembre": 11,
    "dec": 12, "december": 12, "dezember": 12, "diciembre": 12,
}
_MONTH_PATTERN = "|".join(sorted((re.escape(x) for x in _MONTHS), key=len, reverse=True))
_COMMITMENT_PATTERNS = (
    re.compile(
        r"\b(?:i|we)\s+(?:can|will|plan\s+to|expect\s+to|intend\s+to|aim\s+to|"
        r"should\s+be\s+able\s+to)\s+(?:post|publish|upload|share|release)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:ich|wir)\s+(?:kann|können|werde|werden|plane|planen).{0,35}"
        r"\b(?:posten|veröffentlichen|hochladen|teilen)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:yo\s+|nosotros\s+)?(?:puedo|podemos|planeo|planeamos).{0,35}"
        r"\b(?:publicar|subir|compartir)\b",
        re.I,
    ),
    re.compile(r"\b(?:publicaré|publicaremos|subiré|subiremos|compartiré|compartiremos)\b", re.I),
)
_ACTUAL_PATTERNS = (
    re.compile(r"\b(?:i|we)\s+(?:posted|published|uploaded|shared|released)\b", re.I),
    re.compile(r"\b(?:the\s+(?:video|review|post)|it)\s+(?:went|is)\s+live\b", re.I),
    re.compile(r"\b(?:ich|wir)\s+(?:habe|haben).{0,35}\b(?:gepostet|veröffentlicht|hochgeladen|geteilt)\b", re.I),
    re.compile(r"\b(?:publiqué|publicamos|subí|subimos|compartí|compartimos)\b", re.I),
)
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
_DRAFT_CACHE_AT = 0.0
_DRAFT_CACHE_ROWS: list[dict] = []
_DRAFT_CACHE_LOCK: asyncio.Lock | None = None
_DRAFT_CACHE_LOCK_LOOP = None


def _ids(value) -> list[str]:
    return sorted(launch_evidence._ids(value))


def _ts(value) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _unquoted_reply_text(value: str) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"(?is)<blockquote\b.*?</blockquote>", "", text)
    text = re.sub(
        r"(?is)<(?:div|section)[^>]+class=[\"'][^\"']*"
        r"(?:gmail_quote|yahoo_quoted|protonmail_quote)[^\"']*[\"'][^>]*>.*$",
        "",
        text,
    )
    text = re.split(
        r"(?im)^\s*(?:"
        r"on .{0,220} wrote:|"
        r"am .{0,220} schrieb(?: .{0,100})?:|"
        r"el .{0,220} escribi[oó](?: .{0,100})?:|"
        r"-----original message-----|-----ursprüngliche nachricht-----|"
        r"-----mensaje original-----|from:\s.+|von:\s.+|de:\s.+)\s*$",
        text,
        maxsplit=1,
    )[0]
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith(">"))
    return re.sub(r"(?s)<[^>]+>", " ", text).strip()


def _date_ms(year: int, month: int, day: int) -> int:
    try:
        return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)
    except (TypeError, ValueError):
        return 0


def _extract_action_date(
    value: str, *, action_patterns: tuple[re.Pattern, ...], default_year: int,
    min_ts: int = 0, max_ts: int = 0,
) -> int:
    text = _unquoted_reply_text(value)
    action_matches = [
        match for pattern in action_patterns for match in pattern.finditer(text)
    ]
    if not text or not action_matches:
        return 0

    candidates: list[int] = []

    def same_clause(date_match: re.Match) -> bool:
        for action_match in action_matches:
            if action_match.end() <= date_match.start():
                between = text[action_match.end():date_match.start()]
            elif date_match.end() <= action_match.start():
                between = text[date_match.end():action_match.start()]
            else:
                between = ""
            if len(between) > 120:
                continue
            if not re.search(r"[.!?;\n]", between):
                return True
        return False

    for match in re.finditer(r"\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b", text):
        if same_clause(match):
            candidates.append(_date_ms(
                int(match.group(1)), int(match.group(2)), int(match.group(3)),
            ))
    for match in re.finditer(
        rf"\b({_MONTH_PATTERN})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(20\d{{2}}))?\b",
        text,
        re.I,
    ):
        if same_clause(match):
            candidates.append(_date_ms(
                int(match.group(3) or default_year),
                _MONTHS[match.group(1).casefold()],
                int(match.group(2)),
            ))
    for match in re.finditer(
        rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\.?\s+(?:de\s+)?({_MONTH_PATTERN})"
        rf"(?:\.?\s+(20\d{{2}}))?\b",
        text,
        re.I,
    ):
        if same_clause(match):
            candidates.append(_date_ms(
                int(match.group(3) or default_year),
                _MONTHS[match.group(2).casefold()],
                int(match.group(1)),
            ))
    valid = [
        item for item in candidates
        if item and (not min_ts or item >= min_ts) and (not max_ts or item <= max_ts)
    ]
    return min(valid) if valid else 0


def extract_explicit_commitment(
    value: str,
    *,
    default_year: int,
    min_ts: int = 0,
    max_ts: int = 0,
) -> int:
    """从未引用的本轮回复中提取“明确未来发布动作+具体日期”。"""
    return _extract_action_date(
        value, action_patterns=_COMMITMENT_PATTERNS, default_year=default_year,
        min_ts=min_ts, max_ts=max_ts,
    )


def extract_explicit_actual_date(
    value: str,
    *,
    default_year: int,
    min_ts: int = 0,
    max_ts: int = 0,
) -> int:
    """只从“已经发布+具体日期”的同一分句提取实际发布日期。"""
    return _extract_action_date(
        value, action_patterns=_ACTUAL_PATTERNS, default_year=default_year,
        min_ts=min_ts, max_ts=max_ts,
    )


def _is_content_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").casefold()
    path = parsed.path or ""
    query = parsed.query or ""
    if host in {"youtu.be"}:
        return bool(path.strip("/"))
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        return path.startswith(("/shorts/", "/live/")) or (
            path == "/watch" and bool(re.search(r"(?:^|&)v=[^&]+", query))
        )
    if host in {"tiktok.com", "www.tiktok.com"}:
        return bool(re.match(r"^/@[^/]+/video/\d+", path))
    if host in {"vm.tiktok.com", "vt.tiktok.com"}:
        return bool(path.strip("/"))
    if host in {"instagram.com", "www.instagram.com"}:
        return path.startswith(("/p/", "/reel/", "/tv/")) and len(path.strip("/").split("/")) >= 2
    if host in {"facebook.com", "www.facebook.com"}:
        return path.startswith(("/reel/", "/videos/", "/share/v/", "/share/r/")) or (
            path in {"/watch", "/watch/"} and bool(re.search(r"(?:^|&)v=[^&]+", query))
        )
    if host in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}:
        return bool(re.match(r"^/[^/]+/status/\d+", path))
    if host in {"twitch.tv", "www.twitch.tv"}:
        return bool(re.match(r"^/videos/\d+", path))
    if host == "clips.twitch.tv":
        return bool(path.strip("/"))
    return False


def _is_editor_content_url(url: str) -> bool:
    """媒体人只接受文章页；官网首页、栏目页和语言入口不算上稿。"""
    parsed = urlparse(url)
    host = (parsed.hostname or "").casefold()
    if host in SOCIAL_HOSTS:
        return _is_content_url(url)
    segments = [segment.casefold() for segment in parsed.path.split("/") if segment]
    if not segments:
        return False
    non_article_paths = {
        "article", "articles", "blog", "blogs", "de", "en", "es", "fr",
        "games", "gaming", "home", "index", "index.html", "news", "press",
        "review", "reviews", "tech", "technology",
    }
    navigation_segments = {
        "about", "author", "authors", "categories", "category", "contact",
        "people", "search", "staff", "tag", "tags", "team", "topics", "topic",
    }
    navigation_leafs = {
        "all", "archive", "archives", "featured", "latest", "popular", "recent",
        "trending",
    }
    fixed_page_markers = {
        "accessibility", "advertise", "careers", "conditions", "cookie", "jobs",
        "copyright", "disclaimer", "faq", "help", "legal", "newsletter", "policy",
        "privacy", "sitemap", "subscribe", "support", "terms",
    }
    if segments[-1] in non_article_paths or any(
        segment in navigation_segments for segment in segments
    ):
        return False
    last = segments[-1]
    last_stem = re.sub(r"\.(?:html?|php|aspx?)$", "", last)
    last_tokens = set(last_stem.replace("_", "-").split("-"))
    path_tokens = {
        token
        for segment in segments
        for token in re.sub(
            r"\.(?:html?|php|aspx?)$", "", segment,
        ).replace("_", "-").split("-")
    }
    if (
        last_stem.isdigit() or last_stem in non_article_paths
        or last_stem in navigation_leafs or re.fullmatch(r"page-?\d+", last_stem)
        or path_tokens & fixed_page_markers
        or path_tokens & navigation_segments
    ):
        return False
    content_sections = {
        "article", "articles", "blog", "blogs", "games", "gaming", "news",
        "post", "posts", "review", "reviews", "stories", "story", "tech",
        "technology",
    }
    if re.search(
        r"/(?:19|20)\d{2}/\d{1,2}/(?:\d{1,2}/)?[^/]+/?$",
        parsed.path,
    ):
        return True
    if (
        len(last_stem) >= 16 and last_stem.count("-") >= 2
        and bool(path_tokens & content_sections)
        and last_stem not in content_sections
    ):
        return True
    if len(last_stem) >= 20 and last_stem.count("-") >= 3:
        return True
    # 查询参数本身无法区分文章ID和栏目/固定页标识；不自动采信，交人工确认。
    return False


def extract_publication_url(value: str, *, object_type: str = "KOL") -> str:
    text = _unquoted_reply_text(value)
    for raw in _URL_RE.findall(text):
        url = raw.rstrip(".,;:!?)]}")
        host = (urlparse(url).hostname or "").casefold()
        if not host or any(marker in host for marker in OWN_HOST_MARKERS):
            continue
        if object_type == "KOL" and host not in SOCIAL_HOSTS:
            continue
        if object_type == "KOL" and not _is_content_url(url):
            continue
        if object_type != "KOL" and not _is_editor_content_url(url):
            continue
        return url
    return ""


def _eligible_participant(row: dict, draft_map: dict[str, dict]) -> bool:
    fields = row.get("fields") or {}
    if ext(fields.get("参与状态")) not in ACTIVE_STATES:
        return False
    if ext(fields.get("审核结论")) != "通过":
        return False
    return any(
        ext((draft_map.get(draft_id) or {}).get("fields", {}).get("发送状态")) in SENT_STATES
        for draft_id in _ids(fields.get("关联邮件草稿"))
    )


def _default_year(activity_fields: dict) -> int:
    for field_name in ("窗口开始", "窗口结束"):
        value = _ts(activity_fields.get(field_name))
        if value:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc).year
    return datetime.now(timezone.utc).year


async def draft_snapshot() -> list[dict]:
    """短时复用草稿快照，避免同一分钟两项活动重复扫描近2万行。"""
    global _DRAFT_CACHE_AT, _DRAFT_CACHE_ROWS, _DRAFT_CACHE_LOCK, _DRAFT_CACHE_LOCK_LOOP
    now = time.monotonic()
    if _DRAFT_CACHE_ROWS and now - _DRAFT_CACHE_AT < DRAFT_CACHE_SECONDS:
        return _DRAFT_CACHE_ROWS
    loop = asyncio.get_running_loop()
    if _DRAFT_CACHE_LOCK is None or _DRAFT_CACHE_LOCK_LOOP is not loop:
        _DRAFT_CACHE_LOCK = asyncio.Lock()
        _DRAFT_CACHE_LOCK_LOOP = loop
    async with _DRAFT_CACHE_LOCK:
        now = time.monotonic()
        if _DRAFT_CACHE_ROWS and now - _DRAFT_CACHE_AT < DRAFT_CACHE_SECONDS:
            return _DRAFT_CACHE_ROWS
        rows = await feishu.fetch_all_records(
            config.T_DRAFT,
            field_names=[
                "发送状态", "发送时间", "是否回复", "回复日期", "回复原文", "场景标签",
            ],
            page_size=500,
        )
        _DRAFT_CACHE_ROWS = rows
        _DRAFT_CACHE_AT = now
        return rows


async def _campaign_participants(campaign_id: str) -> list[dict]:
    rows = await feishu.search_records(
        config.T_LAUNCH_PARTICIPANT,
        [{"field_name": "活动ID", "operator": "is", "value": [campaign_id]}],
        field_names=[
            "活动ID", "对象类型", "参与状态", "审核结论", "关联KOL", "关联邮件草稿",
            "承诺上稿时间", "实际上稿时间", "上稿链接",
        ],
    )
    return [
        row for row in rows
        if ext((row.get("fields") or {}).get("活动ID")) == campaign_id
    ]


async def _draft_owner(
    draft_id: str, *, cache: dict[str, list[dict]],
) -> list[dict]:
    if draft_id not in cache:
        rows = await feishu.search_records(
            config.T_LAUNCH_PARTICIPANT,
            [{"field_name": "关联邮件草稿", "operator": "contains", "value": [draft_id]}],
            field_names=["活动ID", "关联邮件草稿"],
        )
        cache[draft_id] = [
            row for row in rows
            if draft_id in _ids((row.get("fields") or {}).get("关联邮件草稿"))
        ]
    return cache[draft_id]


async def _source_drafts_belong_only_to_participant(
    source_draft_ids: set[str], *, campaign_id: str, participant_id: str,
    cache: dict[str, list[dict]],
) -> tuple[bool, str]:
    for draft_id in sorted(source_draft_ids):
        owners = await _draft_owner(draft_id, cache=cache)
        if len(owners) != 1:
            return False, f"草稿{draft_id}关联{len(owners)}条活动参与记录"
        owner = owners[0]
        if owner.get("record_id") != participant_id or ext(
            (owner.get("fields") or {}).get("活动ID")
        ) != campaign_id:
            return False, f"草稿{draft_id}不唯一归属当前活动参与记录"
    return True, ""


def _fields_match(readback: dict, expected: dict) -> bool:
    for name, value in expected.items():
        actual = readback.get(name)
        if name in {"承诺上稿时间", "实际上稿时间"}:
            if _ts(actual) != _ts(value):
                return False
        elif name == "上稿链接":
            expected_url = ext_url(value) or ext(value)
            actual_url = ext_url(actual) or ext(actual)
            if actual_url != expected_url:
                return False
        elif ext(actual) != ext(value):
            return False
    return True


async def reconcile_campaign(
    campaign_id: str, *, dry_run: bool = True, activity: dict | None = None,
    participants: list[dict] | None = None, drafts: list[dict] | None = None,
) -> dict:
    if not config.T_LAUNCH_PARTICIPANT:
        raise RuntimeError("T_LAUNCH_PARTICIPANT 未配置")
    activity = activity or await launch_evidence.get_activity(campaign_id)
    activity_fields = activity.get("fields") or {}
    participants = participants if participants is not None else await _campaign_participants(campaign_id)
    rejected_wrong_campaign = sum(
        ext((row.get("fields") or {}).get("活动ID")) != campaign_id
        for row in participants
    )
    participants = [
        row for row in participants
        if ext((row.get("fields") or {}).get("活动ID")) == campaign_id
    ]
    drafts = drafts if drafts is not None else await draft_snapshot()
    draft_map = {row.get("record_id"): row for row in drafts if row.get("record_id")}

    default_year = _default_year(activity_fields)
    window_end = _ts(activity_fields.get("窗口结束"))
    planned, errors = [], []
    owner_cache: dict[str, list[dict]] = {}
    missing_live_links = 0
    for participant in participants:
        participant_id = participant.get("record_id")
        fields = participant.get("fields") or {}
        if not participant_id or not _eligible_participant(participant, draft_map):
            continue
        draft_rows = [
            draft_map[draft_id] for draft_id in _ids(fields.get("关联邮件草稿"))
            if draft_id in draft_map
            and ext((draft_map[draft_id].get("fields") or {}).get("发送状态")) in SENT_STATES
        ]
        draft_rows.sort(
            key=lambda row: _ts((row.get("fields") or {}).get("回复日期")), reverse=True,
        )
        update = {}
        commitment_source = ""
        actual_source = ""
        link_source = ""
        source_draft_ids: set[str] = set()

        if not _ts(fields.get("承诺上稿时间")):
            for draft in draft_rows:
                draft_fields = draft.get("fields") or {}
                if not draft_fields.get("是否回复"):
                    continue
                reply_ts = _ts(draft_fields.get("回复日期"))
                commitment = extract_explicit_commitment(
                    ext(draft_fields.get("回复原文")),
                    default_year=default_year,
                    min_ts=max(0, reply_ts - DAY_MS),
                    max_ts=(window_end + 90 * DAY_MS) if window_end else 0,
                )
                if commitment:
                    update["承诺上稿时间"] = commitment
                    commitment_source = "explicit_reply_date"
                    source_draft_ids.add(draft.get("record_id"))
                    break

        if not _ts(fields.get("实际上稿时间")):
            object_type = ext(fields.get("对象类型")) or "KOL"
            for draft in draft_rows:
                draft_fields = draft.get("fields") or {}
                if ext(draft_fields.get("场景标签")) != "live_link_received":
                    continue
                link = extract_publication_url(
                    ext(draft_fields.get("回复原文")), object_type=object_type,
                )
                if not link:
                    missing_live_links += 1
                    continue
                current_link = ext_url(fields.get("上稿链接")) or ext(fields.get("上稿链接"))
                if current_link != link:
                    update["上稿链接"] = {"link": link, "text": "打开上稿内容"}
                    link_source = "reply_content_link"
                    source_draft_ids.add(draft.get("record_id"))
                reply_ts = _ts(draft_fields.get("回复日期"))
                sent_times = [
                    value for value in (
                        _ts((row.get("fields") or {}).get("发送时间"))
                        for row in draft_rows
                    ) if value
                ]
                actual_date = extract_explicit_actual_date(
                    ext(draft_fields.get("回复原文")),
                    default_year=default_year,
                    min_ts=min(sent_times, default=0),
                    max_ts=(reply_ts + DAY_MS) if reply_ts else (
                        (window_end + 90 * DAY_MS) if window_end else 0
                    ),
                )
                if actual_date:
                    update["实际上稿时间"] = actual_date
                    actual_source = "explicit_published_date_in_reply"
                    source_draft_ids.add(draft.get("record_id"))
                break

        if not update:
            continue
        source_draft_ids.discard(None)
        unique_owner, owner_error = await _source_drafts_belong_only_to_participant(
            source_draft_ids, campaign_id=campaign_id, participant_id=participant_id,
            cache=owner_cache,
        )
        if not unique_owner:
            errors.append({"participant_id": participant_id, "error": owner_error})
            continue
        planned.append({
            "participant_id": participant_id,
            "fields": update,
            "commitment_source": commitment_source,
            "actual_source": actual_source,
            "link_source": link_source,
            "source_draft_ids": sorted(source_draft_ids),
        })
        if dry_run:
            continue
        try:
            await feishu.update_record(
                config.T_LAUNCH_PARTICIPANT, participant_id, update,
            )
            readback = await feishu.get_record(
                config.T_LAUNCH_PARTICIPANT, participant_id,
            )
            if not _fields_match(readback.get("fields") or {}, update):
                raise RuntimeError("飞书写入后回读不一致")
        except Exception as exc:
            errors.append({"participant_id": participant_id, "error": str(exc)[:160]})

    failed_ids = {item["participant_id"] for item in errors}
    successful_plans = [
        item for item in planned if not dry_run and item["participant_id"] not in failed_ids
    ]
    return {
        "ok": not errors,
        "degraded": bool(errors),
        "campaign_id": campaign_id,
        "dry_run": dry_run,
        "participants_scanned": len(participants),
        "participants_rejected_wrong_campaign": rejected_wrong_campaign,
        "updates_planned": len(planned),
        "updates_written": len(successful_plans),
        "commitments_planned": sum(bool(item["fields"].get("承诺上稿时间")) for item in planned),
        "commitments_written": sum(bool(item["fields"].get("承诺上稿时间")) for item in successful_plans),
        "actuals_planned": sum(bool(item["fields"].get("实际上稿时间")) for item in planned),
        "actuals_written": sum(bool(item["fields"].get("实际上稿时间")) for item in successful_plans),
        "links_planned": sum(bool(item["fields"].get("上稿链接")) for item in planned),
        "links_written": sum(bool(item["fields"].get("上稿链接")) for item in successful_plans),
        "ambiguous_manual_uploads": 0,
        "missing_live_links": missing_live_links,
        "planned": planned,
        "errors": errors,
    }
