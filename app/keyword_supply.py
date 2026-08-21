# -*- coding: utf-8 -*-
"""自动关键词供给引擎 (2026-06-15; 2026-06-16 加多语言市场) — 让 YouTube daemon 持续有词抓.

背景(审计 project_kol_intake_audit_2026_06_11): 达人发现根因=关键词断供(脉冲式)。YouTube daemon
扫「爬虫任务台」(tblQnLHnBa1RjJUE) 的 爬虫类型=KOL-YouTube + 任务状态=1-待触发 + 触发=true 任务跑抓取。
本引擎定时按市场补词保持队列, 消除"人偶尔补词"L2 重复劳动。

2026-06-16 扩来源(方向1 多语言): 产品卖 DE/FR/ES/IT/BR/MX 等非英语市场, 但 KOL 库这些市场严重不足
(DE179/FR91/ES93 vs US1779)。本引擎除英语外, 用对应语言生成本地化游戏关键词补各市场队列
(德语词→德语创作者, daemon classify 判国家/语言入库)。零新凭据(复用 daemon+DeepSeek)。

边界: 只补 YouTube 爬虫任务台(KOL库同 app 可写; 96%产能)。TK/IG keywords_queue 在专题9app+Apify$5限暂不。
"""
import time
import math
from collections import Counter
from . import config, feishu, deepseek
from .feishu import ext

T_CRAWLER = "tblQnLHnBa1RjJUE"   # 爬虫任务台 (KOL 营销库内)
PER_BATCH_LIMIT = 50            # 每词 daemon 抓取上限
DISCOVERY_ACTIVE_TTL_MS = 2 * 60 * 60 * 1000

# 市场配置: 英语为主(水位15), 非英语市场(产品在卖+KOL库不足)各保持小水位
MARKETS = [
    {"lang": "en", "countries": ["US", "UK", "CA", "AU"], "target": 15, "name": "English"},
    {"lang": "de", "countries": ["DE"],        "target": 6, "name": "German (Deutsch)"},
    {"lang": "fr", "countries": ["FR"],        "target": 6, "name": "French (Français)"},
    {"lang": "es", "countries": ["ES", "MX"],  "target": 6, "name": "Spanish (Español)"},
    {"lang": "pt", "countries": ["BR"],        "target": 6, "name": "Portuguese (Brasil)"},
]

_AXES = """5 轴交叉生成:
  ① 游戏 IP/系列(IP 名保留通用拼写): super mario / zelda / pokemon / animal crossing / kirby /
     metroid / sonic / stardew valley / hollow knight / elden ring / final fantasy / splatoon 等
  ② 玩家身份/文化: cozy gamer / retro gamer / jrpg fan / speedrunner / indie gamer / handheld gamer 等
  ③ 平台/设备: steam deck / rog ally / switch 2 / gaming handheld 等
  ④ 场景/美学: cozy gaming room / aesthetic gaming setup / battlestation / desk makeover 等
  × 内容形式: themed gaming / setup / room / collection / fan / review / unboxing / haul / setup tour"""

_LOCALIZATION_MARKERS = {
    "pt": (
        "jogador", "jogos", "quarto", "coleção", "colecao",
        "análise", "analise", "avaliação", "avaliacao", "portátil", "portatil",
        "configuração", "configuracao", "dicas", "melhores", "acessórios", "acessorios",
        "em português", "em portugues",
    ),
}

_CAMPAIGN_KEYWORDS = {
    "dave": {
        "en": [
            "dave the diver gameplay", "cozy indie games channel",
            "nintendo switch indie games", "underwater adventure games",
            "switch 2 game reviews", "indie handheld gamer",
        ],
        "de": [
            "dave the diver deutsch", "indie spiele nintendo switch",
            "gemütliche spiele kanal", "nintendo switch spiele deutsch",
        ],
        "es": [
            "dave the diver español", "juegos indie nintendo switch",
            "canal de juegos acogedores", "juegos nintendo switch español",
        ],
    },
    "piranha": {
        "en": [
            "super mario gaming collection", "nintendo gaming room setup",
            "mario fan collection", "switch 2 setup tour",
            "nintendo fan gaming desk", "retro mario gamer room",
        ],
        "de": [
            "super mario sammlung deutsch", "nintendo spielzimmer deutsch",
            "mario fan zimmer", "nintendo switch setup deutsch",
        ],
        "es": [
            "colección super mario", "habitación gamer nintendo",
            "colección fan de mario", "setup nintendo switch español",
        ],
    },
}

# 食人花活动的首批固定词耗尽后，优先由 DeepSeek 生成更长尾的词；
# 外部模型欠费/不可用时，用这组经过品类约束的词继续建发现任务，避免补池停摆。
# 这些词只覆盖 Nintendo / Mario 收藏、游戏房、主机硬件评测四类目标受众。
_CAMPAIGN_FALLBACK_KEYWORDS = {
    "piranha": {
        "en": [
            "super mario collector room tour",
            "nintendo collection shelf showcase",
            "mario themed gaming setup tour",
            "nintendo fan game room makeover",
            "retro nintendo collection room",
            "switch 2 gaming desk setup",
            "nintendo hardware review channel",
            "switch gaming accessories reviewer",
            "mario memorabilia collection tour",
            "nintendo creator setup showcase",
            "super mario fan cave tour",
            "nintendo switch setup review channel",
        ],
        "de": [
            "super mario sammlerzimmer tour",
            "nintendo sammlung zimmer deutsch",
            "mario gaming setup deutsch",
            "nintendo spielzimmer tour deutsch",
            "nintendo hardware test kanal deutsch",
            "switch 2 gaming setup deutsch",
            "super mario fan sammlung deutsch",
            "nintendo zubehör review deutsch",
        ],
        "es": [
            "tour colección super mario",
            "habitación gamer nintendo español",
            "setup gamer mario español",
            "colección fan de nintendo",
            "canal reseñas hardware nintendo",
            "setup nintendo switch español",
            "colección retro nintendo tour",
            "reseñas accesorios nintendo español",
        ],
    },
}


def _multi_values(value) -> list[str]:
    if not isinstance(value, list):
        return [str(value)] if value else []
    out = []
    for item in value:
        if isinstance(item, dict):
            text = item.get("text") or item.get("name")
        else:
            text = item
        if text:
            out.append(str(text))
    return out


async def ensure_campaign_supply(*, campaign_id: str, activity: dict, product: dict,
                                 required_candidates: int, dry_run: bool = False) -> dict:
    """为单个活动补确定性 YouTube 发现任务；只建爬虫任务，不直接创建 KOL。"""
    rows = await feishu.fetch_all_records(T_CRAWLER)
    fields = activity.get("fields") or {}
    product_fields = product.get("fields") or {}
    language_aliases = {"英语": "en", "德语": "de", "西班牙语": "es"}
    languages = [
        language_aliases.get(value, value)
        for value in _multi_values(fields.get("活动目标语言"))
        if language_aliases.get(value, value) in {"en", "de", "es"}
    ] or ["en"]
    identity = f"{campaign_id} {ext(product_fields.get('产品英文名'))}".lower()
    theme = "dave" if "dave" in identity else "piranha" if "piranha" in identity else ""
    if not theme:
        return {"ok": False, "created": 0, "error": "当前活动缺少可验证的确定性拓词主题"}

    existing_keywords = {
        ext((row.get("fields") or {}).get("关键词列表")).strip().lower()
        for row in rows
        if ext((row.get("fields") or {}).get("关键词列表")).strip()
    }
    prefix = f"[活动补池:{campaign_id}]"
    now_ms = int(time.time() * 1000)
    active_pending_for_campaign = 0
    stale_pending_for_campaign = 0
    for row in rows:
        row_fields = row.get("fields") or {}
        if not ext(row_fields.get("任务名")).startswith(prefix):
            continue
        if ext(row_fields.get("任务状态")) not in {"1-待触发", "2-执行中"}:
            continue
        try:
            created_ms = int(float(row_fields.get("创建日期") or 0))
            if 0 < created_ms < 100_000_000_000:
                created_ms *= 1000
        except (TypeError, ValueError):
            created_ms = 0
        if created_ms and 0 <= now_ms - created_ms <= DISCOVERY_ACTIVE_TTL_MS:
            active_pending_for_campaign += 1
        else:
            stale_pending_for_campaign += 1
    target_tasks = max(3, min(9, math.ceil(max(1, int(required_candidates)) / 50)))
    need = max(0, target_tasks - active_pending_for_campaign)
    candidates = []
    while need and any(_CAMPAIGN_KEYWORDS[theme].get(lang) for lang in languages):
        progressed = False
        for lang in languages:
            for word in _CAMPAIGN_KEYWORDS[theme].get(lang, []):
                normalized = word.strip().lower()
                if normalized in existing_keywords or any(x[1] == normalized for x in candidates):
                    continue
                candidates.append((lang, normalized))
                need -= 1
                progressed = True
                break
            if need <= 0:
                break
        if not progressed:
            break

    keyword_source = "deterministic" if candidates else "none"
    generation_error = ""
    generation_warning = ""
    if need and theme == "piranha":
        prompt = f"""你是海外游戏KOL发现助手。为{theme}主题新品活动补充YouTube创作者搜索词。
目标语言只能从 {languages} 选择。搜索对象必须是Nintendo/Switch、Mario收藏、主机游戏房或游戏硬件评测创作者；
不要生成产品购买词、店铺词、官方频道词，不要重复这些词：{sorted(existing_keywords)[-80:]}。
返回JSON：{{"keywords":[{{"language":"en","keyword":"..."}}]}}，至少{max(need * 2, need)}条。"""
        try:
            generated = await deepseek.chat_json(prompt, max_tokens=1200, temperature=0.6)
            raw_words = (generated or {}).get("keywords") or []
            for index, item in enumerate(raw_words):
                if need <= 0:
                    break
                if isinstance(item, dict):
                    lang = str(item.get("language") or "").strip().lower()
                    word = str(item.get("keyword") or "").strip().lower()
                else:
                    lang = languages[index % len(languages)]
                    word = str(item or "").strip().lower()
                if lang not in languages or not (2 <= len(word) <= 80):
                    continue
                if word in existing_keywords or any(existing == word for _, existing in candidates):
                    continue
                market = next((m for m in MARKETS if m["lang"] == lang), {"lang": lang})
                if not _is_localized(word, market):
                    continue
                candidates.append((lang, word))
                need -= 1
            if candidates:
                keyword_source = "mixed" if keyword_source == "deterministic" else "dynamic"
        except Exception as exc:
            generation_warning = str(exc)[:160]

    if need and theme == "piranha":
        fallback_added = 0
        fallback_positions = {lang: 0 for lang in languages}
        while need:
            progressed = False
            for lang in languages:
                words = _CAMPAIGN_FALLBACK_KEYWORDS.get(theme, {}).get(lang, [])
                while fallback_positions[lang] < len(words):
                    word = words[fallback_positions[lang]].strip().lower()
                    fallback_positions[lang] += 1
                    if word in existing_keywords or any(
                        existing == word for _, existing in candidates
                    ):
                        continue
                    candidates.append((lang, word))
                    fallback_added += 1
                    need -= 1
                    progressed = True
                    break
                if need <= 0:
                    break
            if not progressed:
                break
        if fallback_added:
            keyword_source = (
                "curated_fallback"
                if fallback_added == len(candidates)
                else "mixed_curated_fallback"
            )

    if need and generation_warning:
        generation_error = generation_warning

    if dry_run:
        return {
            "ok": True, "created": 0, "would_create": len(candidates),
            "keywords": [{"language": lang, "keyword": word} for lang, word in candidates],
            "pending_before": active_pending_for_campaign + stale_pending_for_campaign,
            "active_pending_before": active_pending_for_campaign,
            "stale_pending_before": stale_pending_for_campaign,
            "target_tasks": target_tasks,
            "keyword_source": keyword_source, "shortfall_tasks": need,
            "generation_error": generation_error,
            "generation_warning": generation_warning,
        }

    now = int(time.time() * 1000)
    countries_by_language = {"en": ["US", "UK", "CA"], "de": ["DE"], "es": ["ES"]}
    created, errors = 0, []
    for lang, word in candidates:
        try:
            await feishu.create_record(T_CRAWLER, {
                "任务名": f"{prefix} YT KOL - {word}",
                "爬虫类型": "KOL-YouTube", "关键词列表": word,
                "筛选-国家": countries_by_language[lang], "筛选-语言": [lang],
                "每批数量上限": PER_BATCH_LIMIT, "任务状态": "1-待触发",
                "触发": True, "创建日期": now,
            })
            created += 1
        except Exception as exc:
            errors.append(f"{word}: {str(exc)[:100]}")
    if need and not generation_error:
        generation_error = f"仍缺{need}个未使用的活动发现关键词"
    return {
        "ok": not errors and not generation_error, "created": created, "errors": errors[:5],
        "pending_before": active_pending_for_campaign + stale_pending_for_campaign,
        "active_pending_before": active_pending_for_campaign,
        "stale_pending_before": stale_pending_for_campaign,
        "target_tasks": target_tasks,
        "keywords": [{"language": lang, "keyword": word} for lang, word in candidates],
        "keyword_source": keyword_source, "shortfall_tasks": need,
        "generation_error": generation_error,
        "generation_warning": generation_warning,
    }


def _is_localized(word: str, market: dict) -> bool:
    markers = _LOCALIZATION_MARKERS.get(market["lang"])
    if not markers:
        return True
    normalized = word.casefold()
    return any(marker in normalized for marker in markers)


def _build_prompt(market: dict, n: int, existing_sample: str, strict_retry: bool = False) -> str:
    lang_line = (
        "全小写英文自然短语。"
        if market["lang"] == "en"
        else f"**用 {market['name']} 书写**这些搜索词(游戏 IP 专有名保留通用拼写如 Zelda/Mario/Pokemon, 其余词本地化成 {market['name']}), 抓 {market['name']} 母语游戏创作者。"
    )
    pt_guard = ""
    if market["lang"] == "pt":
        pt_guard = """\n- 每个词必须含巴西葡语，不接受整句英文。可参考自然结构：quarto gamer Zelda、coleção Mario Brasil、análise Steam Deck em português、jogador de Nintendo Switch。"""
        if strict_retry:
            pt_guard += "\n- 上一批因全英文被系统拒绝；这次除游戏 IP 外，其余描述必须使用 Português do Brasil。"
    return f"""你是 KOL 达人发现的关键词拓展助手。为游戏配件品牌(Switch/PS/PC 手柄/收纳包/充电底座/RGB灯饰)
抓取 **YouTube 游戏创作者**, 生成 {n} 个长尾搜索词。

铁律(数据验证, 必须遵守):
- 按"受众/IP/主题向"拓词, **绝不用产品词**。产品词(switch dock/controller 等)实测新增=0; 受众/IP/主题词平均新增 78.6/词。
- {_AXES}
- **IP 轴优先**(高产+喂 IP 匹配评分)。
- {lang_line}{pt_guard}
- 不带 # 号。**不要和这些已有词重复**: {existing_sample}

只返回 JSON: {{"keywords": ["...", "..."]}} 共 {n} 个。"""


async def _load() -> tuple:
    """返回 (已有关键词小写集合, {lang: YouTube待触发数})"""
    recs = await feishu.fetch_all_records(T_CRAWLER)
    existing = set()
    pending = Counter()
    for r in recs:
        f = r["fields"]
        kw = ext(f.get("关键词列表"))
        if kw:
            existing.add(kw.strip().lstrip("#").lower())
        if ext(f.get("爬虫类型")) == "KOL-YouTube" and ext(f.get("任务状态")) == "1-待触发":
            langs = f.get("筛选-语言") or []
            lang = (langs[0].get("text") or langs[0].get("name")) if (langs and isinstance(langs[0], dict)) else (langs[0] if langs else "en")
            pending[lang] += 1
    return existing, pending


async def run(dry_run: bool = False) -> dict:
    existing, pending = await _load()
    summary = {"markets": {}, "total_added": 0}
    now = int(time.time() * 1000)

    for m in MARKETS:
        pend = pending.get(m["lang"], 0)
        if pend >= m["target"]:
            summary["markets"][m["lang"]] = {"pending": pend, "skip": "队列充足"}
            continue
        need = m["target"] - pend
        fresh, seen = [], set()
        rejected_localization = 0
        last_error = ""
        for attempt in range(2):
            remaining = need - len(fresh)
            prompt = _build_prompt(
                m, max(remaining * 2, 2), ", ".join(list(existing | seen)[:40]),
                strict_retry=attempt > 0,
            )
            try:
                res = await deepseek.chat_json(prompt, max_tokens=900, temperature=0.7)
            except Exception as e:
                last_error = f"deepseek: {str(e)[:80]}"
                continue
            words = (res or {}).get("keywords") or []
            for w in words:
                wn = (w or "").strip().lstrip("#").lower()
                if wn and wn not in existing and wn not in seen and 2 <= len(wn) <= 60:
                    if _is_localized(wn, m):
                        seen.add(wn)
                        fresh.append(wn)
                    else:
                        rejected_localization += 1
                if len(fresh) >= need:
                    break
            if len(fresh) >= need:
                break
        if not fresh:
            error = last_error or (
                f"localization: rejected {rejected_localization} non-{m['lang']} keywords after 2 attempts"
            )
            summary["markets"][m["lang"]] = {
                "pending": pend, "need": need, "error": error,
            }
            continue
        existing |= seen   # 跨市场防重复

        if dry_run:
            summary["markets"][m["lang"]] = {
                "pending": pend, "need": need, "would_add": fresh,
                "rejected_localization": rejected_localization,
            }
            continue

        created, errors = 0, []
        for w in fresh:
            try:
                await feishu.create_record(T_CRAWLER, {
                    "任务名": f"[自动] YT KOL - {w}",
                    "爬虫类型": "KOL-YouTube",
                    "关键词列表": w,
                    "筛选-国家": m["countries"],
                    "筛选-语言": [m["lang"]],
                    "每批数量上限": PER_BATCH_LIMIT,
                    "任务状态": "1-待触发",
                    "触发": True,
                    "创建日期": now,
                })
                created += 1
            except Exception as e:
                errors.append(f"{w}: {str(e)[:50]}")
        summary["markets"][m["lang"]] = {
            "pending_before": pend, "added": created, "keywords": fresh,
            "errors": errors[:3], "rejected_localization": rejected_localization,
        }
        summary["total_added"] += created

    ok = not any(
        "error" in market_result or bool(market_result.get("errors"))
        for market_result in summary["markets"].values()
    )
    return {"ok": ok, **summary}
