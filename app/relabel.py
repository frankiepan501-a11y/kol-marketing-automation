"""A 阶段: 重打 KOL 标签 (基于近期视频标题, 不只是 about 描述)

链路:
1. 拉飞书 KOL 主表里 主平台=YouTube 且 (标签版本!=v2 OR 为空) 的 KOL
2. 爬频道 /videos 页, 提取近 10 条视频标题
3. 把 视频标题 + 现有 about 描述 一起喂 DeepSeek classifier_v2
4. 写回飞书: 近期视频标题 / 近期视频抓取时间 / 标签版本=v2 / 内容风格 / IP喜好

Zeabur 云端跑 (D3=c). 反爬命中率 < 80% → 切回本地 daemon.
失败的 KOL 标记 标签版本=待手工校验, 不动旧 v1 标签 (D2=b).
"""
import asyncio
import html as html_lib
import json
import os
import re
import time
from collections import Counter
import httpx
from . import config, feishu, deepseek
from .feishu import ext


# 用 httpx + 真实浏览器 UA 爬 YouTube
HEADERS_YT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DAY_MS = 86_400_000
PROFILE_FRESH_DAYS = 60
PROFILE_ACTIVE_DAYS = 90
VALID_VERTICALS = {
    "游戏硬件评测", "主机游戏", "PC游戏", "手游",
    "泛游戏娱乐", "科技非游戏", "影视娱乐", "其他",
}
VALID_ECOSYSTEMS = {
    "Switch", "Switch 2", "PlayStation", "Xbox",
    "PC-Steam", "Mobile", "跨平台", "未知",
}
VALID_STYLES = {"游戏", "生活娱乐", "SETUP", "科技测评", "UNBOX", "硬件改装", "测评", "教程", "综合"}
ECOSYSTEM_ORDER = [
    "Switch", "Switch 2", "PlayStation", "Xbox",
    "PC-Steam", "Mobile", "跨平台", "未知",
]
PROFILE_MIN_TARGET_TITLES = 3
PROFILE_GAME_OR_HARDWARE_CUES = (
    "game", "gaming", "videojuego", "videojuegos", "nintendo", "switch",
    "mario", "zelda", "yoshi", "splatoon", "bananza", "pokemon", "pokémon",
    "jrpg", "rpg", "console", "consola", "consolas", "retro", "gameboy",
    "game boy", "snes", "3ds", "playstation", "ps5", "ps4", "xbox",
    "steam", "controller", "gamepad", "dock", "hardware", "accessory",
    "setup", "gameroom", "game room", "boss", "手柄", "底座", "硬件",
    "配件", "游戏", "任天堂", "马里奥", "塞尔达", "耀西",
)

THREAD_STATES = {
    "待回复", "建联中", "洽谈中", "样品评估", "未产出",
    "已合作", "已合作-免费", "已合作-免费(多次)", "已合作-付费",
}
BLOCK_STATES = {"不合适", "黑名单", "低ROI"}


def _extract_channel_id(main_link: str) -> str:
    """主链接 → channel_id (UC...) 或 @handle"""
    if not main_link:
        return ""
    # https://www.youtube.com/channel/UCxxx
    m = re.search(r"youtube\.com/channel/(UC[\w-]+)", main_link)
    if m:
        return m.group(1)
    # https://www.youtube.com/@handle
    m = re.search(r"youtube\.com/@([\w.\-]+)", main_link)
    if m:
        return "@" + m.group(1)
    # https://www.youtube.com/c/customname  → 以 /c/ 形式访问
    m = re.search(r"youtube\.com/c/([\w.\-]+)", main_link)
    if m:
        return "@" + m.group(1)  # /c/X 通常等价 @X
    # https://www.youtube.com/user/legacyname
    m = re.search(r"youtube\.com/user/([\w.\-]+)", main_link)
    if m:
        return "@" + m.group(1)
    return ""


def _decode_json_text(value: str) -> str:
    try:
        return html_lib.unescape(json.loads(f'"{value}"'))
    except (json.JSONDecodeError, TypeError):
        return html_lib.unescape(value)


YOUTUBE_COUNTRY_CODES = {
    "united states": "US", "美国": "US",
    "united kingdom": "UK", "英国": "UK",
    "germany": "DE", "德国": "DE",
    "spain": "ES", "西班牙": "ES",
    "france": "FR", "法国": "FR",
    "italy": "IT", "意大利": "IT",
    "netherlands": "NL", "荷兰": "NL",
    "portugal": "PT", "葡萄牙": "PT",
    "sweden": "SE", "瑞典": "SE",
    "canada": "CA", "加拿大": "CA",
    "thailand": "TH", "泰国": "TH",
    "malaysia": "MY", "马来西亚": "MY",
    "singapore": "SG", "新加坡": "SG",
    "mexico": "MX", "墨西哥": "MX",
    "brazil": "BR", "巴西": "BR",
    "australia": "AU", "澳大利亚": "AU",
}

X_API = "https://api.x.com/2"
X_TARGET_LANGUAGES = {"en", "de", "es"}
X_COUNTRY_PATTERNS = {
    "US": ("united states", "united states of america", "usa", "u.s.a.", "u.s."),
    "UK": ("united kingdom", "great britain", "uk", "u.k."),
    "DE": ("germany", "deutschland"),
    "ES": ("spain", "españa"),
    "FR": ("france",),
    "IT": ("italy", "italia"),
    "NL": ("netherlands", "the netherlands"),
    "PT": ("portugal",),
    "SE": ("sweden",),
    "CA": ("canada",),
    "MX": ("mexico", "méxico"),
    "BR": ("brazil", "brasil"),
    "AU": ("australia",),
}


def _balanced_json_object(source: str, start: int) -> str:
    """从 start 的左花括号读取一个完整 JSON 对象。"""
    if start < 0 or start >= len(source) or source[start] != "{":
        return ""
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    return ""


def _youtube_text(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if not isinstance(value, dict):
        return ""
    if isinstance(value.get("content"), str):
        return value["content"].strip()
    if isinstance(value.get("simpleText"), str):
        return value["simpleText"].strip()
    runs = value.get("runs") or []
    if isinstance(runs, list):
        return "".join(
            str(item.get("text") or "") for item in runs if isinstance(item, dict)
        ).strip()
    return ""


def _parse_public_count(value: str) -> int:
    """把公开页面的 19.3K / 1.2M 等数量转成整数；无法确认则返回0。"""
    text = str(value or "").strip().upper().replace(",", "")
    match = re.search(r"(\d+(?:\.\d+)?)\s*([KMB]?)", text)
    if not match:
        return 0
    multiplier = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    try:
        return int(float(match.group(1)) * multiplier[match.group(2)])
    except (TypeError, ValueError, OverflowError):
        return 0


def parse_youtube_about_page(source: str) -> dict:
    """从公开 YouTube About 页读取可验证资料；缺失项保持为空，不猜测。"""
    marker = '"aboutChannelViewModel"'
    models = []
    cursor = 0
    while True:
        marker_index = (source or "").find(marker, cursor)
        if marker_index < 0:
            break
        object_start = (source or "").find("{", marker_index + len(marker))
        raw = _balanced_json_object(source or "", object_start)
        if raw:
            try:
                value = json.loads(raw)
                if isinstance(value, dict):
                    models.append(value)
            except json.JSONDecodeError:
                pass
            cursor = object_start + len(raw)
        else:
            cursor = marker_index + len(marker)
    model = max(models, key=lambda value: sum(bool(_youtube_text(value.get(name))) for name in (
        "description", "country", "canonicalChannelUrl", "channelId", "subscriberCountText",
    )), default={})

    description = _youtube_text(model.get("description"))
    country_raw = _youtube_text(model.get("country"))
    emails = []
    for match in re.findall(
        r"(?i)(?<![\w.+-])([\w.+-]+@[\w.-]+\.[a-z]{2,})(?![\w.-])",
        description,
    ):
        cleaned, _ = feishu.clean_email(match)
        if cleaned and cleaned not in emails:
            emails.append(cleaned)
    country = YOUTUBE_COUNTRY_CODES.get(country_raw.casefold(), "")
    canonical_url = _youtube_text(model.get("canonicalChannelUrl"))
    channel_id = _youtube_text(model.get("channelId"))
    subscribers = _youtube_text(model.get("subscriberCountText"))
    return {
        "retrieved": bool(model),
        "description": description,
        "country_raw": country_raw,
        "country": country,
        "emails": emails,
        "email": emails[0] if emails else "",
        "canonical_url": canonical_url,
        "channel_id": channel_id,
        "subscriber_count_text": subscribers,
        "followers": _parse_public_count(subscribers),
    }


async def fetch_youtube_public_profile(channel_id_or_handle: str) -> dict:
    """读取公开 About 页；只返回页面明示信息，不调用登录态或猜测模型。"""
    if channel_id_or_handle.startswith("@"):
        url = f"https://www.youtube.com/{channel_id_or_handle}/about"
    else:
        url = f"https://www.youtube.com/channel/{channel_id_or_handle}/about"
    try:
        async with httpx.AsyncClient(
            timeout=20.0, follow_redirects=True, headers=HEADERS_YT,
        ) as cli:
            response = await cli.get(url)
            if response.status_code != 200:
                return {"retrieved": False, "http_status": response.status_code}
            return {
                **parse_youtube_about_page(response.text),
                "http_status": response.status_code,
                "source_url": str(response.url),
            }
    except Exception as exc:
        return {"retrieved": False, "error": type(exc).__name__}


def _x_handle(value: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"(?:x|twitter)\.com/(@?[^/?#]+)", text, re.IGNORECASE)
    if match:
        text = match.group(1)
    return text.lstrip("@").strip()


def _x_country(location: str) -> str:
    """只认简介地点中明示的国家；城市、州和时区不做国家猜测。"""
    normalized = re.sub(r"\s+", " ", str(location or "").casefold()).strip()
    if not normalized:
        return ""
    pieces = {
        normalized,
        *(part.strip(" .") for part in re.split(r"[,/|·]", normalized) if part.strip()),
    }
    for code, labels in X_COUNTRY_PATTERNS.items():
        for label in labels:
            if label in pieces or re.search(rf"(?<!\w){re.escape(label)}(?!\w)", normalized):
                return code
    return ""


def parse_x_public_profile(user: dict, recent_posts: list[dict] | None = None) -> dict:
    """标准化X API公开字段；资料没有明示就保持为空。"""
    posts = [item for item in (recent_posts or []) if isinstance(item, dict)]
    description = str((user or {}).get("description") or "").strip()
    emails = []
    for match in re.findall(
        r"(?i)(?<![\w.+-])([\w.+-]+@[\w.-]+\.[a-z]{2,})(?![\w.-])",
        description,
    ):
        cleaned, _ = feishu.clean_email(match)
        if cleaned and cleaned not in emails:
            emails.append(cleaned)
    all_languages = [
        str(item.get("lang") or "").strip().lower()
        for item in posts if str(item.get("lang") or "").strip()
    ]
    languages = Counter(
        language for language in all_languages if language in X_TARGET_LANGUAGES
    )
    language = ""
    if languages:
        ranked = languages.most_common()
        # 目标语言必须在全部有语言标记的近期帖子里占严格多数；不能先丢掉
        # 日语/法语等非目标帖子，再把仅剩的一条英语误判成账号主语言。
        if ranked[0][1] > len(all_languages) / 2:
            language = ranked[0][0]
    username = str((user or {}).get("username") or "").strip()
    metrics = (user or {}).get("public_metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    return {
        "retrieved": bool((user or {}).get("id") and username),
        "description": description,
        "country_raw": str((user or {}).get("location") or "").strip(),
        "country": _x_country((user or {}).get("location") or ""),
        "language": language,
        "emails": emails,
        "email": emails[0] if emails else "",
        "canonical_url": f"https://x.com/{username}" if username else "",
        "creator_id": str((user or {}).get("id") or "").strip(),
        "handle": username,
        "followers": int(metrics.get("followers_count") or 0),
        "website_url": str((user or {}).get("url") or "").strip(),
        "recent_posts": [
            {"text": str(item.get("text") or "").strip(),
             "lang": str(item.get("lang") or "").strip().lower(),
             "created_at": str(item.get("created_at") or "").strip()}
            for item in posts
        ],
    }


async def fetch_x_public_profile(handle_or_url: str) -> dict:
    """用现有X API凭据读取公开资料与近期原创帖；不使用登录态。"""
    handle = _x_handle(handle_or_url)
    token = os.environ.get("X_BEARER_TOKEN") or os.environ.get("TWITTER_BEARER_TOKEN")
    if not handle:
        return {"retrieved": False, "error": "missing_x_handle"}
    if not token:
        return {"retrieved": False, "error": "missing_x_bearer_token"}
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as cli:
            response = await cli.get(
                f"{X_API}/users/by/username/{handle}",
                params={"user.fields": (
                    "id,name,username,description,location,url,public_metrics,"
                    "entities,verified,verified_type"
                )},
                headers=headers,
            )
            if response.status_code != 200:
                return {"retrieved": False, "http_status": response.status_code}
            user = (response.json() or {}).get("data") or {}
            posts = []
            user_id = str(user.get("id") or "")
            if user_id:
                post_response = await cli.get(
                    f"{X_API}/users/{user_id}/tweets",
                    params={
                        "max_results": 10, "exclude": "retweets,replies",
                        "tweet.fields": "id,text,lang,created_at",
                    },
                    headers=headers,
                )
                if post_response.status_code == 200:
                    posts = (post_response.json() or {}).get("data") or []
            return {
                **parse_x_public_profile(user, posts),
                "http_status": response.status_code,
                "source_url": f"{X_API}/users/by/username/{handle}",
            }
    except Exception as exc:
        return {"retrieved": False, "error": type(exc).__name__}


def _published_at_ms(text: str, *, now_ms: int) -> int:
    normalized = (text or "").strip().lower()
    if normalized == "yesterday":
        return now_ms - DAY_MS
    match = re.search(
        r"(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago",
        normalized,
    )
    if not match:
        return 0
    amount = int(match.group(1))
    unit_days = {
        "minute": 1 / 1440,
        "hour": 1 / 24,
        "day": 1,
        "week": 7,
        "month": 30,
        "year": 365,
    }
    return max(0, now_ms - int(amount * unit_days[match.group(2)] * DAY_MS))


def parse_recent_video_page(html: str, *, now_ms: int | None = None,
                            limit: int = 10) -> list[dict]:
    """把 YouTube /videos 页转成可审计的标题+发布时间列表。"""
    now_ms = int(now_ms or time.time() * 1000)
    title_pattern = re.compile(
        r'"lockupMetadataViewModel":\{"title":\{"content":"((?:[^"\\]|\\.){5,200})"\}',
    )
    matches = list(title_pattern.finditer(html or ""))
    videos: list[dict] = []
    seen: set[str] = set()
    relative_pattern = re.compile(
        r'(?:(?:publishedTimeText)[^\n]{0,500}?)?'
        r'(?:(?:simpleText|content)":")'
        r'((?:Streamed\s+|Premiered\s+)?\d+\s+'
        r'(?:minute|hour|day|week|month|year)s?\s+ago)"',
        re.IGNORECASE,
    )
    for index, match in enumerate(matches):
        title = _decode_json_text(match.group(1)).strip()
        if not title or title in seen:
            continue
        seen.add(title)
        end = matches[index + 1].start() if index + 1 < len(matches) else match.end() + 5000
        block = (html or "")[match.start():end]
        published_match = relative_pattern.search(block)
        published_text = _decode_json_text(published_match.group(1)) if published_match else ""
        videos.append({
            "title": title,
            "published_text": published_text,
            "published_at": _published_at_ms(published_text, now_ms=now_ms),
        })
        if len(videos) >= max(1, int(limit)):
            break

    # 老页面仍可能只有 videoRenderer；保留标题兜底，但无日期时资料状态不会变为有效。
    if len(videos) < 3:
        old_pattern = re.compile(
            r'"videoRenderer":\{"videoId":"[\w-]{11}"[^{}]{0,800}?'
            r'"title":\{"runs":\[\{"text":"((?:[^"\\]|\\.){5,200})"\}',
            re.DOTALL,
        )
        for match in old_pattern.finditer(html or ""):
            title = _decode_json_text(match.group(1)).strip()
            if not title or title in seen:
                continue
            seen.add(title)
            videos.append({"title": title, "published_text": "", "published_at": 0})
            if len(videos) >= max(1, int(limit)):
                break
    return videos


async def fetch_recent_videos(channel_id_or_handle: str, n: int = 10) -> list[dict]:
    """爬 YouTube 频道 /videos 页，返回标题和发布时间；失败返回空列表。"""
    if channel_id_or_handle.startswith("@"):
        url = f"https://www.youtube.com/{channel_id_or_handle}/videos"
    else:
        url = f"https://www.youtube.com/channel/{channel_id_or_handle}/videos"

    try:
        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers=HEADERS_YT,
        ) as cli:
            r = await cli.get(url)
            if r.status_code != 200:
                return []
            html = r.text
    except Exception:
        return []

    return parse_recent_video_page(html, limit=n)


async def fetch_recent_video_titles(channel_id_or_handle: str, n: int = 10) -> list:
    """兼容旧调用：只返回标题。"""
    return [video["title"] for video in await fetch_recent_videos(channel_id_or_handle, n=n)]


async def classify_v2(name: str, handle: str, description: str, sub: int,
                      recent_titles: list, *, model_budget=None,
                      task_id: str = "profile") -> dict:
    """v2 classifier: 加入近期视频标题作为 grounding signal.

    返回 {type, confidence, styles[], ip_tags[], country_guess, reason}
    """
    titles_str = "\n".join(f"- {t}" for t in recent_titles[:10]) if recent_titles else "(未抓到)"
    prompt = f"""你在审核一个 YouTube 频道, 一次性输出多个判断.

【频道信息】
名称: {name}
Handle: @{handle or 'unknown'}
描述: {(description or '(空)')[:400]}
订阅数: {sub or 0}

【近期视频标题】(最重要的判断依据 — 这是真实内容方向, 比描述靠谱)
{titles_str}

【判断1】身份分类
- KOL: 个人创作者/内容创作者/游戏主播
- 品牌商: 配件厂商/卖家官方账号
- 游戏厂商: 发行商/开发商/主机官方
- 媒体: 游戏媒体/评测网站官方
- 不确定: 信息不足

【判断2】内容风格 (多选, 最多 3 个)
只能从池子选: 游戏/生活娱乐/SETUP/科技测评/UNBOX/硬件改装/测评/教程/综合
🚨 必须基于"近期视频标题"判断, 不要看描述 自我介绍.
- 视频里反复出现 unboxing/开箱 → UNBOX
- 视频里反复出现 review/测评/比较 → 测评 / 科技测评
- 视频里反复出现 mod/改装/teardown → 硬件改装
- 视频里反复出现 setup tour/desk setup → SETUP
- 视频里反复出现 gameplay/walkthrough/let's play → 游戏

【判断3】IP喜好关键词 (最多 5 个短标签)
🚨 必须从"近期视频标题"里抽取真实出现的关键词, 不要从描述里抽.
例如视频标题里多次出现 "Switch 2" → "Switch 2"
不要笼统的 "PC游戏" — 要具体: "Steam Deck", "ROG Ally", "Cyberpunk 2077" 等

【判断4】国家推测
描述/标题语言/地名线索 → ISO 码 (US/UK/DE/JP/FR/ES/CA/BR/AU/NL/IT/MX/IN/TH/AE/ID/SE/PT/PH等). 无法确定输出 null.

【判断5】内容垂类 (单选)
只能从池子选: 游戏硬件评测/主机游戏/PC游戏/手游/泛游戏娱乐/科技非游戏/影视娱乐/其他

【判断6】主机生态 (多选)
只能从池子选: Switch/Switch 2/PlayStation/Xbox/PC-Steam/Mobile/跨平台/未知

只返回 JSON, 不要解释:
{{"type":"KOL|品牌商|游戏厂商|媒体|不确定","confidence":0.0-1.0,"reason":"基于哪几条视频判断的","styles":["游戏"],"ip_tags":["Switch 2"],"country_guess":"US|null","content_vertical":"主机游戏","ecosystems":["Switch 2"]}}"""

    if model_budget is not None:
        allowed, reason = model_budget.reserve(task_id)
        if not allowed:
            fallback = deterministic_profile_classification(
                name=name, description=description, recent_titles=recent_titles,
            )
            fallback["model_skip_reason"] = reason
            return fallback

    try:
        data = await deepseek.chat_json(prompt, max_tokens=400, temperature=0.1)
        if not isinstance(data, dict):
            raise ValueError("DeepSeek profile response must be a JSON object")
        if data.get("type") not in {"KOL", "品牌商", "游戏厂商", "媒体", "不确定"}:
            raise ValueError("DeepSeek profile response has no valid type")
    except Exception as exc:
        if model_budget is not None:
            model_budget.record_failure(terminal=deepseek.is_terminal_error(exc))
        fallback = deterministic_profile_classification(
            name=name, description=description, recent_titles=recent_titles,
        )
        fallback["model_fallback_reason"] = (
            "terminal_provider_failure"
            if deepseek.is_terminal_error(exc) else "model_error"
        )
        return fallback

    if model_budget is not None:
        model_budget.record_success()

    data.setdefault("styles", [])
    data.setdefault("ip_tags", [])
    data.setdefault("country_guess", None)
    data.setdefault("content_vertical", "")
    data.setdefault("ecosystems", [])
    if data.get("country_guess") in ("null", "None", ""):
        data["country_guess"] = None
    return data


def deterministic_profile_classification(*, name: str, description: str,
                                         recent_titles: list[str]) -> dict:
    """模型不可用时，用可回放的内容关键词完成保守画像。"""
    content_text = " ".join(recent_titles or []).lower()
    styles: list[str] = []
    style_cues = (
        ("UNBOX", ("unbox", "unboxing", "开箱")),
        ("测评", ("review", "comparison", "tested", "测评", "评测", "对比")),
        ("科技测评", ("controller", "gamepad", "dock", "hardware", "accessory", "手柄", "底座", "硬件", "配件")),
        ("SETUP", ("setup", "desk tour", "桌搭")),
        ("硬件改装", ("modding", "teardown", "repair", "改装", "拆解", "维修")),
        ("教程", ("how to", "guide", "tutorial", "教程", "攻略")),
        ("游戏", ("gameplay", "walkthrough", "boss", "full game", "gaming", "游戏", "通关")),
    )
    for style, cues in style_cues:
        if any(cue in content_text for cue in cues):
            styles.append(style)
        if len(styles) >= 3:
            break
    if not styles and any(cue in content_text for cue in (
        "nintendo", "switch", "mario", "zelda", "playstation", "xbox", "steam",
    )):
        styles.append("游戏")

    tags = []
    tag_cues = (
        ("Switch 2", ("switch 2",)), ("Nintendo", ("nintendo", "任天堂")),
        ("Mario", ("mario", "马里奥")), ("Zelda", ("zelda", "塞尔达")),
        ("Yoshi", ("yoshi", "耀西")), ("Splatoon", ("splatoon", "喷射战士")),
        ("Donkey Kong", ("donkey kong", "dk bananza")),
        ("Steam Deck", ("steam deck",)), ("PlayStation", ("playstation", "ps5", "ps4")),
        ("Xbox", ("xbox",)),
    )
    for tag, cues in tag_cues:
        if any(cue in content_text for cue in cues):
            tags.append(tag)
        if len(tags) >= 5:
            break

    normalized_name = re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()
    official_publishers = {
        "nintendo", "nintendo of america", "playstation", "xbox",
        "the pokemon company international", "sega", "ubisoft",
    }
    known_media = {"ign", "gamespot", "polygon", "the verge", "eurogamer"}
    if normalized_name in official_publishers or (
        "official" in normalized_name
        and any(cue in normalized_name for cue in ("nintendo", "playstation", "xbox", "games"))
    ):
        object_type = "游戏厂商"
    elif normalized_name in known_media:
        object_type = "媒体"
    else:
        # 对象已存在 KOL 主表；没有强官方/媒体信号时保守沿用 KOL 身份，
        # 但候选仍必须通过活动硬筛和本次人工审核。
        object_type = "KOL"

    target_title_count = sum(
        any(cue in (title or "").lower() for cue in PROFILE_GAME_OR_HARDWARE_CUES)
        for title in (recent_titles or [])
    )
    if target_title_count < PROFILE_MIN_TARGET_TITLES:
        # 搜索词可能只碰巧命中一两条视频；不能据此把整个频道写成 Switch/Nintendo。
        styles = []
        tags = []
        ecosystems = ["未知"]
        vertical = "其他"
        clear_profile_tags = True
        reason = (
            f"确定性兜底：近期目标游戏/主机内容仅 "
            f"{target_title_count}/{len(recent_titles or [])} 条，保守不贴游戏生态标签"
        )
    else:
        ecosystems = _derive_ecosystems(content_text, [])
        vertical = _derive_vertical(content_text, styles, "")
        clear_profile_tags = False
        reason = "确定性兜底：按近期标题关键词分类；外部模型不可用"
    return {
        "type": object_type, "confidence": 0.65,
        "reason": reason,
        "styles": styles, "ip_tags": tags, "country_guess": None,
        "content_vertical": vertical, "ecosystems": ecosystems,
        "clear_profile_tags": clear_profile_tags,
        "classification_source": "deterministic_fallback",
    }


def _timestamp_ms(value) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        raw = int(value)
        return raw * 1000 if 0 < raw < 10_000_000_000 else raw
    text = ext(value).strip()
    if text.isdigit():
        raw = int(text)
        return raw * 1000 if raw < 10_000_000_000 else raw
    try:
        from datetime import datetime
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return 0


def touch_route_for_status(status: str) -> str:
    normalized = (status or "").strip()
    if normalized == "未建联":
        return "可新开发"
    if normalized in THREAD_STATES:
        return "沿用原线程"
    if normalized in BLOCK_STATES:
        return "禁止新开发"
    return "待核对"


def touch_route_for_refresh(fields: dict) -> str:
    """刷新成功或失败都不得解除受控导入对象的人工审核锁。"""
    if (
        "[CONTROLLED_IMPORT]" in ext((fields or {}).get("迁移备注"))
        and ext((fields or {}).get("触达路由状态")) == "待核对"
    ):
        return "待核对"
    return touch_route_for_status(ext((fields or {}).get("合作状态")))


def _derive_ecosystems(text: str, explicit) -> list[str]:
    selected = [value for value in (explicit or []) if value in VALID_ECOSYSTEMS]
    lowered = (text or "").lower()
    cues = (
        ("Switch 2", ("switch 2",)),
        ("Switch", ("nintendo", "switch", "mario", "zelda", "yoshi", "任天堂", "马里奥", "塞尔达", "耀西")),
        ("PlayStation", ("playstation", "ps5", "ps4")),
        ("Xbox", ("xbox",)),
        ("PC-Steam", ("steam deck", "steam", "pc gaming", "rog ally", "windows gaming")),
        ("Mobile", ("mobile gaming", "android game", "ios game")),
    )
    if not selected:
        for ecosystem, terms in cues:
            if any(term in lowered for term in terms) and ecosystem not in selected:
                selected.append(ecosystem)
    concrete_groups = {
        "Switch" if value in {"Switch", "Switch 2"} else value
        for value in selected if value not in {"未知", "跨平台"}
    }
    if len(concrete_groups) >= 2 and "跨平台" not in selected:
        selected.append("跨平台")
    if not selected:
        selected = ["未知"]
    return [value for value in ECOSYSTEM_ORDER if value in selected]


def _derive_vertical(text: str, styles: list[str], explicit: str) -> str:
    if explicit in VALID_VERTICALS:
        return explicit
    lowered = (text or "").lower()
    style_set = set(styles or [])
    hardware = any(term in lowered for term in (
        "controller", "gamepad", "dock", "accessory", "hardware", "unbox",
        "setup", "手柄", "底座", "配件", "硬件", "开箱", "桌搭",
    )) or bool(style_set & {"科技测评", "UNBOX", "硬件改装", "SETUP"})
    game_cue = any(term in lowered for term in (
        "game", "gaming", "nintendo", "switch", "playstation", "xbox",
        "steam", "mario", "zelda", "游戏", "任天堂", "马里奥", "塞尔达",
    )) or "游戏" in style_set
    if hardware and game_cue:
        return "游戏硬件评测"
    if any(term in lowered for term in ("nintendo", "switch", "playstation", "xbox", "mario", "zelda", "任天堂", "马里奥", "塞尔达")):
        return "主机游戏"
    if any(term in lowered for term in ("steam", "pc game", "pc gaming", "rog ally")):
        return "PC游戏"
    if any(term in lowered for term in ("mobile gaming", "android game", "ios game")):
        return "手游"
    if any(term in lowered for term in ("movie", "cinema", "film review", "电影", "影视")):
        return "影视娱乐"
    if style_set & {"科技测评", "UNBOX", "硬件改装", "SETUP"}:
        return "科技非游戏"
    if game_cue:
        return "泛游戏娱乐"
    return "其他"


def plan_profile_update(fields: dict, recent_videos: list[dict], classification: dict,
                        *, now_ms: int | None = None) -> dict:
    """把一次抓取/分类结果转换为主表可直接写入的完整画像字段。"""
    now_ms = int(now_ms or time.time() * 1000)
    titles = [str(video.get("title") or "").strip() for video in recent_videos]
    titles = [title for title in titles if title]
    published = sorted(
        [int(video.get("published_at") or 0) for video in recent_videos if int(video.get("published_at") or 0) > 0],
        reverse=True,
    )
    styles = [value for value in (classification.get("styles") or []) if value in VALID_STYLES]
    tags = [str(value).strip() for value in (classification.get("ip_tags") or []) if str(value).strip()]
    combined_text = " ".join(
        titles + tags + [ext(fields.get("IP喜好")), " ".join(styles)]
    )
    ecosystems = _derive_ecosystems(combined_text, classification.get("ecosystems"))
    vertical = _derive_vertical(
        combined_text, styles, str(classification.get("content_vertical") or "").strip(),
    )
    latest = published[0] if published else 0
    cutoff = now_ms - PROFILE_ACTIVE_DAYS * DAY_MS
    posts_90d = sum(timestamp >= cutoff for timestamp in published)
    manual_at = _timestamp_ms(fields.get("资料核实时间"))
    manual_fresh = (
        ext(fields.get("资料可用状态")) == "人工核实有效"
        and manual_at > 0
        and now_ms - manual_at <= PROFILE_FRESH_DAYS * DAY_MS
    )
    if manual_fresh:
        readiness = "人工核实有效"
        verified_at = manual_at
    elif not latest:
        readiness = "缺资料"
        verified_at = now_ms
    elif posts_90d < 1 or latest < cutoff:
        readiness = "活跃度不足"
        verified_at = now_ms
    else:
        readiness = "有效"
        verified_at = now_ms
    if classification.get("type") not in {"", "KOL"}:
        readiness = "缺资料"

    route = touch_route_for_refresh(fields)
    update = {
        "标签版本": "v2",
        "近期视频标题": "\n".join(titles[:10]),
        "近期视频抓取时间": now_ms,
        "内容垂类": vertical,
        "主机生态": ecosystems,
        "近90天发布数": posts_90d,
        "资料可用状态": readiness,
        "资料核实时间": verified_at,
        "触达路由状态": route,
    }
    if latest:
        update["最近发布日"] = latest
    clear_profile_tags = bool(classification.get("clear_profile_tags")) and not manual_fresh
    if clear_profile_tags:
        # 只有本轮已经拿到足够近期标题、且明确证明旧机器画像是误判时才清理；
        # 近期人工核实的资料受保护，不被机器空结果覆盖。
        update["内容风格"] = []
        update["IP喜好"] = ""
    elif styles:
        update["内容风格"] = styles
    if tags and not clear_profile_tags:
        update["IP喜好"] = ", ".join(tags[:5])
    return update


async def relabel_one_kol(record: dict, *, dry_run: bool = False,
                          now_ms: int | None = None,
                          classification_mode: str = "deepseek",
                          model_budget=None) -> dict:
    """重打一个 KOL 的标签. 返回 {record_id, status, scrape_ok, classify_ok, titles_n, ...}"""
    rid = record["record_id"]
    f = record["fields"]
    now_ms = int(now_ms or time.time() * 1000)
    name = ext(f.get("账号名"))
    main_link = ""
    ml = f.get("主链接")
    if isinstance(ml, dict):
        main_link = ml.get("link", "")
    elif isinstance(ml, list) and ml:
        main_link = ml[0].get("link", "") if isinstance(ml[0], dict) else str(ml[0])

    cid = _extract_channel_id(main_link)
    if not cid:
        # 写"待手工校验"
        update_fields = {
            "标签版本": "待手工校验", "资料可用状态": "缺资料",
            "资料核实时间": now_ms,
            "触达路由状态": touch_route_for_refresh(f),
        }
        write_error = await _persist_profile_update(rid, update_fields, dry_run=dry_run)
        return _profile_result(
            rid, name, intended_status="no_channel_id", dry_run=dry_run,
            update_fields=update_fields, write_error=write_error,
        )

    # 1. 爬视频标题
    videos = await fetch_recent_videos(cid, n=10)
    titles = [video["title"] for video in videos]
    scrape_ok = len(titles) >= 3  # 拿到 ≥3 条算成功
    if not scrape_ok:
        update_fields = {
            "标签版本": "待手工校验", "近期视频抓取时间": now_ms,
            "资料可用状态": "待刷新", "资料核实时间": now_ms,
            "触达路由状态": touch_route_for_refresh(f),
        }
        write_error = await _persist_profile_update(rid, update_fields, dry_run=dry_run)
        return _profile_result(
            rid, name, intended_status="scrape_fail", dry_run=dry_run,
            update_fields=update_fields, write_error=write_error,
            channel_id=cid, titles_n=len(titles),
        )

    if classification_mode not in {"deepseek", "deterministic"}:
        raise ValueError("classification_mode must be deepseek or deterministic")

    # 2. 内容分类。活动零模型演练使用可回放的确定性规则；旧调用保持 DeepSeek 行为。
    handle = ""
    m = re.search(r"@([\w.\-]+)", main_link)
    if m:
        handle = m.group(1)
    description = ext(f.get("IP喜好"))[:400]  # 旧 IP喜好 字段是描述, 临时拿来用
    try:
        sub = max(0, int(float(str(f.get("粉丝数") or 0).replace(",", ""))))
    except (TypeError, ValueError):
        sub = 0

    if classification_mode == "deterministic":
        cls = deterministic_profile_classification(
            name=name, description=description, recent_titles=titles,
        )
    else:
        cls = await classify_v2(
            name, handle, description, sub, titles,
            model_budget=model_budget, task_id=f"profile:{rid}",
        )
    if cls.get("type") == "不确定" or "deepseek_err" in cls.get("reason", ""):
        update_fields = plan_profile_update(f, videos, cls, now_ms=now_ms)
        update_fields.update({"标签版本": "待手工校验", "资料可用状态": "缺资料"})
        write_error = await _persist_profile_update(rid, update_fields, dry_run=dry_run)
        return _profile_result(
            rid, name, intended_status="classify_fail", dry_run=dry_run,
            update_fields=update_fields, write_error=write_error,
            channel_id=cid, titles_n=len(titles),
            classify_reason=cls.get("reason", "")[:120],
        )

    # 3. 写回飞书 — 标签版本=v2 + 新标签 + 视频标题
    update_fields = plan_profile_update(f, videos, cls, now_ms=now_ms)
    styles = update_fields.get("内容风格") or []
    tags = cls.get("ip_tags") or []

    write_error = await _persist_profile_update(rid, update_fields, dry_run=dry_run)
    return _profile_result(
        rid, name, intended_status="ok", dry_run=dry_run,
        update_fields=update_fields, write_error=write_error,
        channel_id=cid, titles_n=len(titles), new_styles=styles, new_tags=tags,
        classify_reason=cls.get("reason", "")[:120],
        classification_source=cls.get("classification_source", classification_mode),
        model_skip_reason=cls.get("model_skip_reason", ""),
        model_fallback_reason=cls.get("model_fallback_reason", ""),
    )


async def _persist_profile_update(record_id: str, update_fields: dict, *,
                                  dry_run: bool) -> str:
    if dry_run:
        return ""
    try:
        await feishu.update_record(config.T_KOL, record_id, update_fields)
        return ""
    except Exception as exc:
        return str(exc)[:300]


def _profile_result(record_id: str, name: str, *, intended_status: str,
                    dry_run: bool, update_fields: dict, write_error: str = "",
                    **details) -> dict:
    result = {
        "record_id": record_id, "name": name,
        "status": "write_fail" if write_error else intended_status,
        "intended_status": intended_status,
        "dry_run": dry_run, "write_applied": bool(not dry_run and not write_error),
        "planned_fields": update_fields, **details,
    }
    if write_error:
        result["err"] = write_error
    return result


async def run_profile_records(record_ids: list[str], *, dry_run: bool = True,
                              limit: int = 100,
                              classification_mode: str = "deepseek",
                              model_budget=None) -> dict:
    """按明确记录 ID 后台刷新画像；默认只演练，不写主表。"""
    unique_ids = list(dict.fromkeys(str(value).strip() for value in record_ids if str(value).strip()))
    unique_ids = unique_ids[:max(1, min(int(limit), 100))]
    semaphore = asyncio.Semaphore(3)
    model_calls_before = (
        model_budget.snapshot()["run_calls"] if model_budget is not None else 0
    )

    async def one(record_id):
        async with semaphore:
            try:
                record = await feishu.get_record(config.T_KOL, record_id)
                return await relabel_one_kol(
                    record, dry_run=dry_run,
                    classification_mode=classification_mode,
                    model_budget=model_budget,
                )
            except Exception as exc:
                return {
                    "record_id": record_id, "name": "", "status": "processing_error",
                    "intended_status": "processing_error", "dry_run": dry_run,
                    "write_applied": False, "planned_fields": {},
                    "err": str(exc)[:300],
                }

    results = await asyncio.gather(*(one(record_id) for record_id in unique_ids))
    counts = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
    model_snapshot = model_budget.snapshot() if model_budget is not None else {}
    return {
        "dry_run": dry_run, "classification_mode": classification_mode,
        "model_calls": (
            0 if classification_mode == "deterministic"
            else model_snapshot.get("run_calls", 0) - model_calls_before
            if model_budget is not None else None
        ),
        "model_budget": model_snapshot,
        "requested": len(unique_ids), "processed": len(results),
        "writes": sum(bool(result.get("write_applied")) for result in results),
        "by_status": counts, "results": results,
    }


async def run_kol_test(limit: int = 10) -> dict:
    """从 KOL 主表挑前 N 个 主平台=YouTube 且 标签版本 != v2 的 KOL, 重打标签.

    返回总览 + 每条结果. 用于 A.4 验证 Zeabur 云端反爬命中率.
    """
    # 拉候选: 主平台=YouTube AND (标签版本 为空 OR != v2)
    items = await feishu.search_records(config.T_KOL, [
        {"field_name": "主平台", "operator": "is", "value": ["YouTube"]},
    ])
    # 过滤掉已 v2
    pending = []
    for rec in items:
        ver = ext(rec["fields"].get("标签版本"))
        if ver != "v2":
            pending.append(rec)
        if len(pending) >= limit:
            break

    results = []
    scrape_ok_n = 0
    classify_ok_n = 0
    for rec in pending:
        out = await relabel_one_kol(rec)
        results.append(out)
        if out["status"] in ("ok", "classify_fail"):
            scrape_ok_n += 1
        if out["status"] == "ok":
            classify_ok_n += 1

    return {
        "tried": len(results),
        "scrape_ok": scrape_ok_n,
        "scrape_ok_rate": f"{scrape_ok_n / max(1, len(results)) * 100:.0f}%",
        "classify_ok": classify_ok_n,
        "classify_ok_rate": f"{classify_ok_n / max(1, len(results)) * 100:.0f}%",
        "results": results,
    }
