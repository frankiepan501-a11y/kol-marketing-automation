"""A 阶段: 重打 KOL 标签 (基于近期视频标题, 不只是 about 描述)

链路:
1. 拉飞书 KOL 主表里 主平台=YouTube 且 (标签版本!=v2 OR 为空) 的 KOL
2. 爬频道 /videos 页, 提取近 10 条视频标题
3. 把 视频标题 + 现有 about 描述 一起喂 DeepSeek classifier_v2
4. 写回飞书: 近期视频标题 / 近期视频抓取时间 / 标签版本=v2 / 内容风格 / IP喜好

Zeabur 云端跑 (D3=c). 反爬命中率 < 80% → 切回本地 daemon.
失败的 KOL 标记 标签版本=待手工校验, 不动旧 v1 标签 (D2=b).
"""
import re
import time
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


async def fetch_recent_video_titles(channel_id_or_handle: str, n: int = 10) -> list:
    """爬 YouTube 频道 /videos 页, 提取近 N 条视频标题.

    返回标题 list. 失败返回 [].
    """
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

    # 2026 YouTube /videos 页结构: lockupMetadataViewModel.title.content
    # 旧 videoRenderer 已废弃, gridVideoRenderer 也不再用
    titles = []
    seen = set()
    pat = re.compile(
        r'"lockupMetadataViewModel":\{"title":\{"content":"((?:[^"\\]|\\.){5,200})"\}',
    )
    for m in pat.finditer(html):
        t = m.group(1).encode("utf-8").decode("unicode_escape", errors="ignore")
        if t in seen or len(t) < 5:
            continue
        seen.add(t)
        titles.append(t)
        if len(titles) >= n:
            break

    # 兜底: 旧版 videoRenderer (老页面/Shorts 频道偶尔仍用)
    if len(titles) < 3:
        pat_old = re.compile(
            r'"videoRenderer":\{"videoId":"[\w-]{11}"[^{}]{0,800}?"title":\{"runs":\[\{"text":"((?:[^"\\]|\\.){5,200})"\}',
            re.DOTALL,
        )
        for m in pat_old.finditer(html):
            t = m.group(1).encode("utf-8").decode("unicode_escape", errors="ignore")
            if t in seen or len(t) < 5:
                continue
            seen.add(t)
            titles.append(t)
            if len(titles) >= n:
                break
    return titles


async def classify_v2(name: str, handle: str, description: str, sub: int,
                      recent_titles: list) -> dict:
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

只返回 JSON, 不要解释:
{{"type":"KOL|品牌商|游戏厂商|媒体|不确定","confidence":0.0-1.0,"reason":"基于哪几条视频判断的","styles":["游戏"],"ip_tags":["Switch 2"],"country_guess":"US|null"}}"""

    try:
        data = await deepseek.chat_json(prompt, max_tokens=400, temperature=0.1)
    except Exception as e:
        return {"type": "不确定", "confidence": 0.0, "reason": f"deepseek_err: {str(e)[:80]}",
                "styles": [], "ip_tags": [], "country_guess": None}

    data.setdefault("styles", [])
    data.setdefault("ip_tags", [])
    data.setdefault("country_guess", None)
    if data.get("country_guess") in ("null", "None", ""):
        data["country_guess"] = None
    return data


VALID_STYLES = {"游戏", "生活娱乐", "SETUP", "科技测评", "UNBOX", "硬件改装", "测评", "教程", "综合"}


async def relabel_one_kol(record: dict) -> dict:
    """重打一个 KOL 的标签. 返回 {record_id, status, scrape_ok, classify_ok, titles_n, ...}"""
    rid = record["record_id"]
    f = record["fields"]
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
        try:
            await feishu.update_record(config.T_KOL, rid, {"标签版本": "待手工校验"})
        except Exception:
            pass
        return {"record_id": rid, "name": name, "status": "no_channel_id"}

    # 1. 爬视频标题
    titles = await fetch_recent_video_titles(cid, n=10)
    scrape_ok = len(titles) >= 3  # 拿到 ≥3 条算成功
    if not scrape_ok:
        try:
            await feishu.update_record(config.T_KOL, rid, {
                "标签版本": "待手工校验",
                "近期视频抓取时间": int(time.time() * 1000),
            })
        except Exception:
            pass
        return {"record_id": rid, "name": name, "status": "scrape_fail",
                "channel_id": cid, "titles_n": len(titles)}

    # 2. AI 分类
    handle = ""
    m = re.search(r"@([\w.\-]+)", main_link)
    if m:
        handle = m.group(1)
    description = ext(f.get("IP喜好"))[:400]  # 旧 IP喜好 字段是描述, 临时拿来用
    sub = int(f.get("粉丝数") or 0)

    cls = await classify_v2(name, handle, description, sub, titles)
    if cls.get("type") == "不确定" or "deepseek_err" in cls.get("reason", ""):
        try:
            await feishu.update_record(config.T_KOL, rid, {
                "标签版本": "待手工校验",
                "近期视频标题": "\n".join(titles),
                "近期视频抓取时间": int(time.time() * 1000),
            })
        except Exception:
            pass
        return {"record_id": rid, "name": name, "status": "classify_fail",
                "channel_id": cid, "titles_n": len(titles), "classify_reason": cls.get("reason", "")[:120]}

    # 3. 写回飞书 — 标签版本=v2 + 新标签 + 视频标题
    update_fields = {
        "标签版本": "v2",
        "近期视频标题": "\n".join(titles),
        "近期视频抓取时间": int(time.time() * 1000),
    }
    styles = [s for s in (cls.get("styles") or []) if s in VALID_STYLES]
    if styles:
        update_fields["内容风格"] = styles
    tags = cls.get("ip_tags") or []
    if tags:
        update_fields["IP喜好"] = ", ".join(tags[:5])

    try:
        await feishu.update_record(config.T_KOL, rid, update_fields)
    except Exception as e:
        return {"record_id": rid, "name": name, "status": "write_fail",
                "channel_id": cid, "titles_n": len(titles), "err": str(e)[:120]}

    return {
        "record_id": rid, "name": name, "status": "ok",
        "channel_id": cid, "titles_n": len(titles),
        "new_styles": styles, "new_tags": tags,
        "classify_reason": cls.get("reason", "")[:120],
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
