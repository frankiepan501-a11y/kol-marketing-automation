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
import re
import asyncio
import unicodedata
from collections import Counter
from . import config, feishu, deepseek, launch_evidence
from .feishu import ext

T_CRAWLER = "tblQnLHnBa1RjJUE"   # 爬虫任务台 (KOL 营销库内)
PER_BATCH_LIMIT = 50            # 每词 daemon 抓取上限
DISCOVERY_ACTIVE_TTL_MS = 2 * 60 * 60 * 1000
DISCOVERY_REUSE_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
LOW_YIELD_PROBE_COOLDOWN_MS = 2 * 60 * 60 * 1000
LOW_YIELD_RECENT_SAMPLE = 12
LOW_YIELD_MIN_EMAIL_SAMPLES = 6
LOW_YIELD_MIN_EMAIL_SIGNAL_COVERAGE = 0.80
LOW_YIELD_MIN_COMPLETED_FOR_CONVERSION = 6
LOW_YIELD_MIN_EMAILS_PER_TASK = 0.25
LOW_YIELD_MIN_APPROVED_PER_TASK = 0.10
DAVE_KEYWORD_PILOT_CAMPAIGN_ID = "launch-20260915-funlab-dave-ys11-5"
PIRANHA_SOURCE_BACKFILL_CAMPAIGN_ID = "launch-20260915-powkong-piranha-v2"
P0_SOURCE_BACKFILL_CAMPAIGNS = {
    DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
    PIRANHA_SOURCE_BACKFILL_CAMPAIGN_ID,
}
DAVE_KEYWORD_PILOT_TAG = "[灰度:dave-keyword-v1]"
DAVE_KEYWORD_PILOT_V2_TAG = "[灰度:dave-keyword-v2]"
DAVE_KEYWORD_PILOT_MAX_TASKS = 4
_CAMPAIGN_SUPPLY_LOCKS: dict[str, asyncio.Lock] = {}

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

# 食人花活动的首批固定词和确定性七层词耗尽后，才由 DeepSeek 生成更长尾的词；
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

_DISCOVERY_BAD_INTENT = re.compile(
    r"\b(?:"
    r"buy(?:ing)?|prices?|coupons?|discounts?|deals?|cheap|amazon|stores?|"
    r"shops?|shopping|sales?|official(?:ly)?|"
    r"kaufen|preise?|gutscheine?|rabatte?|angebote?|guenstig\w*|günstig\w*|"
    r"l[aä]den?|offiziell\w*|"
    r"compr(?:ar|a|as|ando)|precios?|cup[oó]nes?|descuentos?|ofertas?|"
    r"barat[oa]s?|tiendas?|oficial(?:es)?"
    r")\b",
    re.I,
)
_CATEGORY_EN = {
    "手柄": "controller", "游戏手柄": "controller", "controller": "controller",
    "底座": "dock", "充电底座": "charging dock", "dock": "dock",
    "收纳包": "carrying case", "case": "carrying case",
}
_PLATFORM_EN = {
    "switch 2": "nintendo switch 2", "switch": "nintendo switch",
    "pc": "pc gaming", "steam deck": "steam deck", "ps5": "playstation 5",
    "xbox": "xbox",
}
_PILOT_COUNTRIES_BY_LANGUAGE = {
    "en": ["US", "UK", "CA", "AU", "IE", "NZ"],
    "de": ["DE", "AT", "CH"],
    "es": ["ES", "MX"],
}


def _split_phrases(value) -> list[str]:
    values = _multi_values(value)
    out = []
    for raw in values:
        for part in re.split(r"[;,，；|/\n]+", raw):
            phrase = re.sub(r"\s+", " ", part).strip(" -")
            if phrase:
                out.append(phrase)
    return list(dict.fromkeys(out))


def _english_phrase(value: str) -> str:
    value = re.sub(r"\s+", " ", str(value or "")).strip()
    if not value or not re.search(r"[A-Za-z]", value):
        return ""
    # 搜索词不能混入中文；字段中若是“中文（English）”，优先取括号内英文。
    bracketed = re.findall(r"[（(]([^()（）]*[A-Za-z][^()（）]*)[)）]", value)
    if bracketed:
        value = bracketed[-1]
    value = re.sub(r"[^A-Za-z0-9+&'’\- ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip().lower()


def _discovery_item(*, language: str, keyword: str, source: str,
                    reason: str, axes: list[str] | None = None,
                    evidence_mode: str = "") -> dict | None:
    word = re.sub(r"\s+", " ", keyword).strip().lower()
    if not (2 <= len(word) <= 80) or _DISCOVERY_BAD_INTENT.search(word):
        return None
    return {
        "language": language, "keyword": word, "source": source,
        "axes": list(dict.fromkeys(
            (axes or [source]) + (["localization"] if language != "en" else [])
        )),
        "reason": reason, "evidence_mode": evidence_mode,
    }


_PIRANHA_SEVEN_LAYER_TERMS = {
    "en": {
        "ip": ["piranha plant", "super mario", "mario"],
        "platform": ["nintendo switch 2", "nintendo switch"],
        "category": ["gaming dock", "switch dock", "gaming accessory"],
        "problem": ["organized gaming desk", "game room display", "charging setup"],
        "format": ["review", "unboxing", "setup tour", "collection showcase"],
        "adjacent": ["nintendo collector", "mario fan", "retro gamer"],
    },
    "de": {
        "ip": ["piranha plant", "super mario", "mario"],
        "platform": ["nintendo switch 2", "nintendo switch"],
        "category": ["gaming dock", "switch dock", "nintendo zubehör"],
        "problem": ["ordentlicher gaming tisch", "spielzimmer vitrine", "lade setup"],
        "format": ["test deutsch", "unboxing deutsch", "setup tour deutsch", "sammlung deutsch"],
        "adjacent": ["nintendo sammler", "mario fan", "retro gamer deutsch"],
    },
    "es": {
        "ip": ["piranha plant", "super mario", "mario"],
        "platform": ["nintendo switch 2", "nintendo switch"],
        "category": ["dock gamer", "base switch", "accesorios nintendo"],
        "problem": ["escritorio gamer ordenado", "vitrina gamer", "setup de carga"],
        "format": ["reseña español", "unboxing español", "tour setup español", "colección español"],
        "adjacent": ["coleccionista nintendo", "fan de mario", "gamer retro español"],
    },
}


def _piranha_seven_layer_candidates(*, activity_fields: dict, product_fields: dict,
                                     languages: list[str], existing_keywords: set[str],
                                     limit: int = 100) -> list[dict]:
    """按活动配置编译七层可追溯词；竞品层默认关闭，绝不绑定某个品牌。"""
    mode = ext(activity_fields.get("竞品证据模式")).strip()
    evidence_status = ext(activity_fields.get("竞品分析状态")).strip()
    competitor = _english_phrase(ext(activity_fields.get("竞品品牌")))
    competitor_enabled = bool(
        competitor
        and mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE}
        and evidence_status == "已就绪"
    )
    product_ip = ext(product_fields.get("适配IP")).strip() or "Piranha Plant"
    product_platform = ext(product_fields.get("适配主机")).strip() or "Switch 2"
    product_category = ext(product_fields.get("品类")).strip() or "gaming dock"
    product_anchor = f"{product_ip}/{product_platform}/{product_category}"
    out: list[dict] = []
    seen = set(existing_keywords)
    max_items = max(0, min(int(limit), 100))
    for language in languages:
        terms = _PIRANHA_SEVEN_LAYER_TERMS.get(language)
        if not terms:
            continue
        sources: list[tuple[str, list[str], str]] = []
        if competitor_enabled:
            sources.append(("competitor", [
                f"{competitor} {terms['platform'][0]} {terms['format'][0]}",
                f"{competitor} {terms['category'][0]} {terms['format'][1]}",
            ], "本活动已选择且已就绪的竞品证据层"))
        sources.extend([
            ("ip_theme", [
                f"{ip_name} {terms['adjacent'][index % len(terms['adjacent'])]} {terms['format'][index % len(terms['format'])]}"
                for index, ip_name in enumerate(terms["ip"])
            ], "产品/IP层"),
            ("platform_ecosystem", [
                f"{platform} {terms['category'][index % len(terms['category'])]} {terms['format'][index % len(terms['format'])]}"
                for index, platform in enumerate(terms["platform"])
            ], "主机生态层"),
            ("category_function", [
                f"{category} {terms['platform'][index % len(terms['platform'])]} {terms['format'][(index + 1) % len(terms['format'])]}"
                for index, category in enumerate(terms["category"])
            ], "品类/功能层"),
            ("user_problem", [
                f"{problem} {terms['platform'][index % len(terms['platform'])]} {terms['format'][(index + 2) % len(terms['format'])]}"
                for index, problem in enumerate(terms["problem"])
            ], "用户问题层"),
            ("content_format", [
                f"{terms['ip'][index % len(terms['ip'])]} {terms['category'][index % len(terms['category'])]} {content_format}"
                for index, content_format in enumerate(terms["format"])
            ], "内容形态层"),
            ("adjacent_audience", [
                f"{audience} {terms['problem'][index % len(terms['problem'])]} {terms['format'][(index + 1) % len(terms['format'])]}"
                for index, audience in enumerate(terms["adjacent"])
            ], "邻近受众层"),
        ])
        # 先每层取一条，再取各层第二条；避免一个来源先占满整批。
        max_variants = max((len(words) for _, words, _ in sources), default=0)
        for variant_index in range(max_variants):
            for source, words, reason in sources:
                if variant_index >= len(words):
                    continue
                item = _discovery_item(
                    language=language, keyword=words[variant_index], source=source,
                    reason=f"{reason}（产品锚点：{product_anchor}）",
                    axes=[source, product_anchor, "creator_content"],
                    evidence_mode=mode,
                )
                if not item or item["keyword"] in seen:
                    continue
                seen.add(item["keyword"])
                out.append(item)
                if len(out) >= max_items:
                    return out
    return out


def _candidate_keyword(item) -> str:
    return str(item.get("keyword") if isinstance(item, dict) else item[1]).strip().lower()


def _dave_structured_candidates(*, activity_fields: dict, product_fields: dict,
                                languages: list[str], existing_keywords: set[str],
                                pilot_version: str = "v1") -> list[dict]:
    """Dave 灰度词池。只把活动/产品字段编译成词，不把 NYXI 写成系统默认。"""
    mode = ext(activity_fields.get("竞品证据模式")).strip()
    evidence_status = ext(activity_fields.get("竞品分析状态")).strip()
    competitor = _english_phrase(ext(activity_fields.get("竞品品牌")))
    competitor_enabled = bool(
        competitor
        and mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE}
        and evidence_status == "已就绪"
    )

    ips = [
        phrase for phrase in (
            _english_phrase(value)
            for value in _split_phrases(product_fields.get("适配IP"))
        ) if phrase
    ]
    if not ips and "dave" in ext(product_fields.get("产品英文名")).lower():
        ips = ["dave the diver"]

    platforms = []
    for value in _split_phrases(product_fields.get("适配主机")):
        normalized = _english_phrase(value)
        platform = _PLATFORM_EN.get(normalized, normalized)
        if platform:
            platforms.append(platform)
    platforms = list(dict.fromkeys(platforms)) or ["nintendo switch 2"]

    category_raw = ext(product_fields.get("品类")).strip().lower()
    category = _CATEGORY_EN.get(category_raw, _english_phrase(category_raw)) or "gaming accessory"
    benchmark = [
        phrase for phrase in (
            _english_phrase(value)
            for value in _split_phrases(product_fields.get("对标关键词"))
        ) if phrase and not _DISCOVERY_BAD_INTENT.search(phrase)
    ]
    audiences = [
        phrase for phrase in (
            _english_phrase(value)
            for value in _split_phrases(product_fields.get("目标人群"))
        ) if phrase
    ]

    by_source: dict[str, dict[str, list[str]]] = {
        source: {lang: [] for lang in languages}
        for source in ("competitor", "ip_theme", "platform", "category_feature",
                       "audience_scenario", "content_format")
    }
    if competitor_enabled:
        if "en" in languages:
            by_source["competitor"]["en"] += [
                f"{competitor} controller review", f"{competitor} switch controller review",
            ]
        if "de" in languages:
            by_source["competitor"]["de"].append(f"{competitor} controller test deutsch")
        if "es" in languages:
            by_source["competitor"]["es"].append(f"{competitor} mando reseña español")

    for ip in ips[:3]:
        if "en" in languages:
            by_source["ip_theme"]["en"] += [f"{ip} review", f"{ip} gameplay channel"]
        if "de" in languages:
            by_source["ip_theme"]["de"].append(f"{ip} review deutsch")
        if "es" in languages:
            by_source["ip_theme"]["es"].append(f"{ip} reseña español")

    for platform in platforms[:3]:
        if "en" in languages:
            by_source["platform"]["en"].append(f"{platform} {category} review")
        if "de" in languages:
            by_source["platform"]["de"].append(f"{platform} controller test deutsch")
        if "es" in languages:
            by_source["platform"]["es"].append(f"mando {platform} reseña español")

    category_bases = benchmark[:3] or [category]
    for base in category_bases:
        if "en" in languages:
            by_source["category_feature"]["en"].append(f"{base} review channel")
        if "de" in languages:
            by_source["category_feature"]["de"].append(f"{base} test deutsch")
        if "es" in languages:
            by_source["category_feature"]["es"].append(f"{base} reseña español")

    for audience in audiences[:3]:
        if "en" in languages:
            by_source["audience_scenario"]["en"].append(f"{audience} youtube channel")
    if "en" in languages:
        by_source["content_format"]["en"] += [
            f"{category} unboxing channel", f"gaming {category} review channel",
        ]
    if "de" in languages:
        by_source["content_format"]["de"].append(f"gaming {category} unboxing deutsch")
    if "es" in languages:
        by_source["content_format"]["es"].append(f"unboxing {category} español")

    if pilot_version == "v2":
        # v1真实灰度显示：IP泛词会把Dave人名/真实潜水/垃圾箱潜水混进来，
        # 德西语“test/reseña”也不足以锁定游戏创作者。v2至少交叉3个轴，
        # 仍保留同样的市场硬闸，方便做同批对比，不靠放宽筛选换数量。
        if competitor_enabled and "en" in languages:
            by_source["competitor"]["en"] = [f"{competitor} switch controller review"]
        if ips and "en" in languages:
            by_source["ip_theme"]["en"] = [
                f"{ips[0]} nintendo switch gameplay review",
            ]
        if platforms and "de" in languages:
            by_source["platform"]["de"] = [
                f"{platforms[0]} controller gaming test deutsch",
            ]
        if platforms and "es" in languages:
            by_source["platform"]["es"] = [
                f"mando {platforms[0]} review gaming españa",
            ]

    # 首批探测优先覆盖“竞品/IP/平台+本地化”，避免 4 条全压同一假设。
    preferred = []
    if competitor_enabled and "en" in languages:
        preferred.append(("competitor", "en"))
    if "en" in languages:
        preferred.append(("ip_theme", "en"))
    if "de" in languages:
        preferred.append(("platform", "de"))
    if "es" in languages:
        preferred.append(("platform", "es"))
    for source in by_source:
        for language in languages:
            preferred.append((source, language))

    items, seen = [], set(existing_keywords)
    for source, language in preferred:
        words = by_source.get(source, {}).get(language, [])
        while words:
            word = words.pop(0)
            item = _discovery_item(
                language=language, keyword=word, source=source,
                reason=(
                    f"由活动的{source}信息生成；"
                    f"用于验证该来源的有效邮箱和合格候选产出"
                ),
                axes={
                    "competitor": ["competitor", "category_feature", "content_format"],
                    "ip_theme": (
                        ["ip_theme", "platform", "content_format"]
                        if pilot_version == "v2" else ["ip_theme", "content_format"]
                    ),
                    "platform": ["platform", "category_feature", "content_format"],
                    "category_feature": ["category_feature", "content_format"],
                    "audience_scenario": ["audience_scenario", "content_format"],
                    "content_format": ["category_feature", "content_format"],
                }[source],
                evidence_mode=mode if source == "competitor" else "",
            )
            if not item or item["keyword"] in seen:
                continue
            seen.add(item["keyword"])
            items.append(item)
            break
    return items


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


def _created_ms(fields: dict) -> int:
    try:
        value = int(float(fields.get("创建日期") or 0))
    except (TypeError, ValueError):
        return 0
    return value * 1000 if 0 < value < 100_000_000_000 else value


def _timestamp_ms(value) -> int:
    try:
        parsed = int(float(value or 0))
    except (TypeError, ValueError):
        return 0
    return parsed * 1000 if 0 < parsed < 100_000_000_000 else parsed


def _last_modified_ms(row: dict) -> int:
    """飞书 automatic_fields=true 返回记录级 last_modified_time。"""
    return _timestamp_ms(row.get("last_modified_time"))


def _normalize_discovery_keyword(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"\s+", " ", normalized).strip()


def _normalized_countries(value) -> tuple[str, ...]:
    return tuple(sorted({
        text.strip().upper() for text in _multi_values(value) if text.strip()
    }))


def _normalized_language(value) -> str:
    values = _multi_values(value)
    return values[0].strip().lower() if values else ""


def _discovery_history_key(keyword: str, countries, language: str) -> tuple:
    return (
        _normalize_discovery_keyword(keyword),
        _normalized_countries(countries),
        str(language or "").strip().lower(),
    )


def _task_source(fields: dict) -> str:
    match = re.search(r"\[词源:([^\]]+)\]", ext(fields.get("任务名")))
    if match:
        return match.group(1).strip().lower()
    keyword = _normalize_discovery_keyword(fields.get("关键词列表"))
    legacy_words = {
        _normalize_discovery_keyword(word)
        for theme in ("dave", "piranha")
        for words in _CAMPAIGN_KEYWORDS.get(theme, {}).values()
        for word in words
    }
    # A2 之前的固定词没有词源标签，但来源可由受控常量唯一确定；
    # 其他无标签任务仍视为缺信号，不能笼统冒充 legacy_fixed。
    return "legacy_fixed" if keyword in legacy_words else ""


def plan_campaign_source_metadata_backfill(rows: list[dict], *, campaign_id: str) -> dict:
    """只按明确标签或受控固定词表，规划历史任务词源元数据回填。"""
    prefix = f"[活动补池:{campaign_id}]"
    updates = []
    already_attributed = 0
    unattributed_tasks = 0
    missing_output_signal = 0
    for row in rows:
        fields = row.get("fields") or {}
        name = ext(fields.get("任务名"))
        if (
            not name.startswith(prefix)
            or ext(fields.get("任务状态")) != "3-已完成"
        ):
            continue
        if re.search(r"\[词源:[^\]]+\]", name):
            already_attributed += 1
            continue
        source = _task_source(fields)
        if not source:
            unattributed_tasks += 1
            continue
        log = ext(fields.get("执行日志"))
        if _valid_email_output(fields) is None:
            missing_output_signal += 1
        marker = f"词源元数据回填: {source}（依据:受控固定词表精确命中）"
        updated_log = log if marker in log else "\n".join(
            part for part in (log.rstrip(), marker) if part
        )
        updates.append({
            "record_id": str(row.get("record_id") or ""),
            "source": source,
            "evidence": "controlled_fixed_keyword_exact_match",
            "fields": {
                "任务名": name.replace(prefix, f"{prefix}[词源:{source}]", 1),
                "执行日志": updated_log,
            },
        })
    return {
        "campaign_id": campaign_id,
        "planned_updates": len(updates),
        "updates": updates,
        "already_attributed": already_attributed,
        "unattributed_tasks": unattributed_tasks,
        "missing_output_signal": missing_output_signal,
        "guessed_attributions": 0,
    }


async def backfill_campaign_source_metadata(*, campaign_ids: list[str],
                                            dry_run: bool = True) -> dict:
    """受控 P0 回填；不建爬虫任务、不改筛选、不触发草稿或邮件。"""
    requested = list(dict.fromkeys(str(value or "").strip() for value in campaign_ids))
    unsupported = [value for value in requested if value not in P0_SOURCE_BACKFILL_CAMPAIGNS]
    if not requested or unsupported:
        raise ValueError(
            "campaign_ids 仅允许当前 Dave 与食人花集中宣发活动"
        )
    rows = await feishu.fetch_all_records(T_CRAWLER, automatic_fields=True)
    plans = [
        plan_campaign_source_metadata_backfill(rows, campaign_id=campaign_id)
        for campaign_id in requested
    ]
    applied = 0
    errors = []
    if not dry_run:
        for plan in plans:
            for update in plan["updates"]:
                try:
                    await feishu.update_record(
                        T_CRAWLER, update["record_id"], update["fields"],
                    )
                    applied += 1
                except Exception as exc:
                    errors.append({
                        "record_id": update["record_id"],
                        "error": str(exc)[:160],
                    })
    return {
        "ok": not errors,
        "dry_run": bool(dry_run),
        "campaigns": plans,
        "planned_updates": sum(plan["planned_updates"] for plan in plans),
        "applied_updates": applied,
        "errors": errors[:10],
        "crawler_tasks_created": 0,
        "drafts_created": 0,
        "emails_sent": 0,
        "quality_filters_lowered": False,
        "guessed_attributions": 0,
    }


def _task_matches_history_key(fields: dict, key: tuple) -> bool:
    keyword, countries, language = key
    if _normalize_discovery_keyword(fields.get("关键词列表")) != keyword:
        return False
    task_countries = _normalized_countries(fields.get("筛选-国家"))
    task_language = _normalized_language(fields.get("筛选-语言"))
    # 七天复用键必须是完整三元组。缺国家/语言的旧脏记录不能充当通配符，
    # 否则一个旧词会错误阻断所有目标市场。
    return bool(
        task_countries and task_language
        and task_countries == countries and task_language == language
    )


def _history_key_state(rows: list[dict], *, keyword: str, countries,
                       language: str, now_ms: int) -> dict:
    key = _discovery_history_key(keyword, countries, language)
    matched = [
        row for row in rows
        if _task_matches_history_key(row.get("fields") or {}, key)
    ]
    active = [
        row for row in matched
        if ext((row.get("fields") or {}).get("任务状态")) in {
            "1-待触发", "2-执行中", "2-运行中",
        }
    ]
    if active:
        return {"state": "active_duplicate", "last_completed_ms": 0}
    terminal = [
        row for row in matched
        if ext((row.get("fields") or {}).get("任务状态")) == "3-已完成"
    ]
    if not terminal:
        return {"state": "unseen", "last_completed_ms": 0, "outcome_rank": 1}
    completed_times = [_last_modified_ms(row) for row in terminal]
    if not all(completed_times):
        return {
            "state": "history_signal_unavailable", "last_completed_ms": 0,
            "outcome_rank": 1,
        }
    last_completed_ms = max(completed_times)
    if last_completed_ms > now_ms:
        return {
            "state": "history_signal_unavailable",
            "last_completed_ms": last_completed_ms,
            "outcome_rank": 1,
        }
    if 0 <= now_ms - last_completed_ms < DISCOVERY_REUSE_WINDOW_MS:
        return {
            "state": "recently_executed", "last_completed_ms": last_completed_ms,
            "outcome_rank": 1,
        }
    latest = max(terminal, key=_last_modified_ms)
    latest_output = _attributed_usable_output(latest.get("fields") or {})
    return {
        "state": "reusable_after_ttl", "last_completed_ms": last_completed_ms,
        # >7天组合：有可用对象优先、未知其次、明确零产出最后。
        "outcome_rank": 0 if (latest_output or 0) > 0 else 1 if latest_output is None else 2,
    }


def _valid_email_output(fields: dict) -> int | None:
    match = re.search(r"其中有邮箱\s*[:：]\s*(\d+)", ext(fields.get("执行日志")))
    return int(match.group(1)) if match else None


def _int_field(fields: dict, *names: str) -> int:
    for name in names:
        try:
            if fields.get(name) not in (None, ""):
                return max(0, int(float(fields.get(name))))
        except (TypeError, ValueError):
            continue
    return 0


def _attributed_usable_output(fields: dict) -> int | None:
    log = ext(fields.get("执行日志"))
    approved = re.search(r"(?:明确合格|自动通过)\s*[:：]\s*(\d+)", log)
    review = re.search(r"(?:可运营审核|待运营审核)\s*[:：]\s*(\d+)", log)
    if approved and review:
        return int(approved.group(1)) + int(review.group(1))
    valid_email = _valid_email_output(fields)
    # 没有有效邮箱时无需等待内部筛选，也能确定可用产出为零。
    return 0 if valid_email == 0 else None


def _source_health(rows: list[dict], *, prefix: str, now_ms: int,
                   source_outcomes: dict | None = None) -> dict:
    outcomes_by_task = (source_outcomes or {}).get("by_task") or {}
    by_source: dict[str, list[dict]] = {}
    for row in rows:
        fields = row.get("fields") or {}
        if not ext(fields.get("任务名")).startswith(prefix):
            continue
        if ext(fields.get("任务状态")) != "3-已完成":
            continue
        source = _task_source(fields)
        if source:
            by_source.setdefault(source, []).append(row)
    result = {}
    for source, source_rows in by_source.items():
        source_rows.sort(key=_last_modified_ms, reverse=True)
        known = []
        round_outputs = []
        pending_unknown = 0
        missing = 0
        # “连续两轮”只看最近两条已完成任务；未知轮次不能被跳过后，
        # 再拿更老的两个零产出拼成一段并不存在的连续低产。
        recent_rounds = source_rows[:2]
        for row in recent_rounds:
            fields = row.get("fields") or {}
            modified_ms = _last_modified_ms(row)
            task_metric = outcomes_by_task.get(str(row.get("record_id") or "")) or {}
            attributed_count = (
                _int_field(task_metric, "auto_approved")
                + _int_field(task_metric, "operator_review")
            )
            attributed_discovered = _int_field(task_metric, "discovered")
            if attributed_count > 0:
                output = attributed_count
            elif attributed_discovered > 0:
                output = 0
            else:
                output = _attributed_usable_output(fields)
            if not modified_ms or _valid_email_output(fields) is None:
                missing += 1
                round_outputs.append(None)
                continue
            if output is None:
                pending_unknown += 1
                round_outputs.append(None)
                continue
            known.append((modified_ms, output))
            round_outputs.append(output)
        mode = "healthy"
        cooldown_until_ms = 0
        # 最新一轮已产出可用对象即可恢复；否则连续最近两轮都为0才冷却。
        # 任何未知轮次都不能被更老的结果跨过去拼接。
        if round_outputs and (round_outputs[0] or 0) > 0:
            mode = "healthy"
        elif len(round_outputs) >= 2 and round_outputs[:2] == [0, 0]:
            second_zero_ms = _last_modified_ms(recent_rounds[0])
            cooldown_until_ms = second_zero_ms + LOW_YIELD_PROBE_COOLDOWN_MS
            mode = "cooldown" if now_ms < cooldown_until_ms else "probe"
        elif recent_rounds and (missing or pending_unknown):
            mode = "signal_missing"
        result[source] = {
            "mode": mode,
            "known_outcomes": len(known),
            "pending_or_unknown": pending_unknown,
            "signal_missing_tasks": missing,
            "last_completed_ms": max(
                (_last_modified_ms(row) for row in source_rows), default=0,
            ),
            "cooldown_until_ms": cooldown_until_ms,
        }
    return result


def _campaign_source_signal(rows: list[dict], *, prefix: str,
                            candidate_sources: set[str] | None = None) -> dict:
    """按来源检查审计信号；旧脏记录只能隔离自身，不能冻结整个活动。"""
    completed = [
        row for row in rows
        if ext((row.get("fields") or {}).get("任务名")).startswith(prefix)
        and ext((row.get("fields") or {}).get("任务状态")) == "3-已完成"
    ]
    missing = []
    isolated: dict[str, int] = Counter()
    observed_sources = set()
    unattributed_tasks = 0
    for row in completed:
        fields = row.get("fields") or {}
        source = _task_source(fields)
        reasons = []
        if not source:
            reasons.append("source")
            unattributed_tasks += 1
            isolated["unattributed_legacy"] += 1
        else:
            observed_sources.add(source)
        if not _last_modified_ms(row):
            reasons.append("last_modified_time")
        if _valid_email_output(fields) is None:
            reasons.append("execution_log")
        if reasons:
            missing.append({"record_id": row.get("record_id", ""), "missing": reasons})
            if source:
                isolated[source] += 1
    sources = set(candidate_sources or observed_sources)
    available_sources = sorted(sources - set(isolated))
    return {
        "available": bool(available_sources) if sources else not missing,
        "missing_tasks": len(missing),
        "unattributed_tasks": unattributed_tasks,
        "available_sources": available_sources,
        "isolated_sources": sorted(isolated),
        "isolated_source_tasks": dict(isolated),
        "details": missing[:5],
    }


def _active_sources(rows: list[dict], *, prefix: str) -> set[str]:
    return {
        source
        for row in rows
        for fields in [row.get("fields") or {}]
        for source in [_task_source(fields)]
        if source
        and ext(fields.get("任务名")).startswith(prefix)
        and ext(fields.get("任务状态")) in {"1-待触发", "2-执行中", "2-运行中"}
    }


def _raw_discovered_output(fields: dict) -> int | None:
    for name in ("原始发现数", "实际发现数量", "抓取数量", "实际产出总数"):
        if fields.get(name) not in (None, ""):
            return _int_field(fields, name)
    match = re.search(
        r"(?:原始发现|发现作者|发现KOL|抓取结果|共发现)\s*[:：]?\s*(\d+)",
        ext(fields.get("执行日志")), re.I,
    )
    return int(match.group(1)) if match else None


def _external_youtube_outcome(rows: list[dict], *, prefix: str) -> dict:
    completed = []
    for row in rows:
        fields = row.get("fields") or {}
        if (
            ext(fields.get("任务名")).startswith(prefix)
            and ext(fields.get("任务状态")) == "3-已完成"
        ):
            completed.append(fields)
    valid_samples = [
        value for value in (_valid_email_output(fields) for fields in completed)
        if value is not None
    ]
    attributed = [
        value for value in (_attributed_usable_output(fields) for fields in completed)
        if value is not None
    ]
    raw_samples = [
        value for value in (_raw_discovered_output(fields) for fields in completed)
        if value is not None
    ]
    new_records = sum(
        _int_field(fields, "实际产出-新增", "新增数量") for fields in completed
    )
    updated_records = sum(
        _int_field(fields, "实际产出-更新", "更新数量") for fields in completed
    )
    return {
        "completed_tasks": len(completed),
        "raw_discovered": sum(raw_samples),
        "raw_discovered_signal_tasks": len(raw_samples),
        "new_records": new_records,
        "updated_records": updated_records,
        "records_written": new_records + updated_records,
        "valid_email": sum(valid_samples),
        "valid_email_signal_tasks": len(valid_samples),
        "attributed_outcome_tasks": len(attributed),
        "pending_or_unknown_tasks": len(completed) - len(attributed),
        "usable_candidates": sum(attributed),
    }


def _campaign_countries_by_language(*, languages: list[str], target_countries: list[str],
                                    configured: dict[str, list[str]]) -> dict[str, list[str]]:
    if target_countries:
        return {
            language: list(configured.get(language) or [])
            for language in languages
        }
    defaults = {"en": ["US", "UK", "CA"], "de": ["DE"], "es": ["ES"]}
    return {language: list(defaults.get(language) or []) for language in languages}


def _round_robin_sources(items: list[dict], source_health: dict) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for item in items:
        buckets.setdefault(item["source"], []).append(item)
    for source in buckets:
        buckets[source].sort(key=lambda item: (
            int(item.get("outcome_rank", 1)),
            int(item.get("last_completed_ms") or 0),
            item.get("keyword", ""),
        ))
    sources = sorted(
        buckets,
        key=lambda source: (
            int((source_health.get(source) or {}).get("last_completed_ms") or 0),
            source,
        ),
    )
    out = []
    while sources:
        next_sources = []
        for source in sources:
            if buckets[source]:
                out.append(buckets[source].pop(0))
            if buckets[source]:
                next_sources.append(source)
        sources = next_sources
    return out


def _select_dave_external_candidates(*, rows: list[dict], activity_fields: dict,
                                     product_fields: dict, languages: list[str],
                                     countries_by_language: dict[str, list[str]],
                                     prefix: str, now_ms: int, limit: int,
                                     source_outcomes: dict | None = None) -> tuple:
    return _select_deterministic_external_candidates(
        theme="dave", rows=rows, activity_fields=activity_fields,
        product_fields=product_fields, languages=languages,
        countries_by_language=countries_by_language, prefix=prefix,
        now_ms=now_ms, limit=limit, source_outcomes=source_outcomes,
    )


def _select_piranha_external_candidates(*, rows: list[dict], activity_fields: dict,
                                         product_fields: dict, languages: list[str],
                                         countries_by_language: dict[str, list[str]],
                                         prefix: str, now_ms: int, limit: int,
                                         source_outcomes: dict | None = None) -> tuple:
    return _select_deterministic_external_candidates(
        theme="piranha", rows=rows, activity_fields=activity_fields,
        product_fields=product_fields, languages=languages,
        countries_by_language=countries_by_language, prefix=prefix,
        now_ms=now_ms, limit=limit, source_outcomes=source_outcomes,
    )


def _select_deterministic_external_candidates(
    *, theme: str, rows: list[dict], activity_fields: dict,
    product_fields: dict, languages: list[str],
    countries_by_language: dict[str, list[str]], prefix: str,
    now_ms: int, limit: int, source_outcomes: dict | None = None,
) -> tuple:
    """确定性词库共用七天复用、来源健康和在途去重。AI只负责增量。"""
    fixed = []
    positions = {language: 0 for language in languages}
    while True:
        progressed = False
        for language in languages:
            words = _CAMPAIGN_KEYWORDS[theme].get(language, [])
            if positions[language] >= len(words):
                continue
            word = words[positions[language]]
            positions[language] += 1
            item = _discovery_item(
                language=language, keyword=word, source="legacy_fixed",
                reason=f"{theme} 旧固定发现词；保留兼容并纳入七天轮转",
                axes=["legacy_fixed", "creator_content"],
            )
            if item:
                fixed.append(item)
            progressed = True
        if not progressed:
            break
    structured = (
        _dave_structured_candidates(
            activity_fields=activity_fields, product_fields=product_fields,
            languages=languages, existing_keywords=set(), pilot_version="v2",
        )
        if theme == "dave" else
        _piranha_seven_layer_candidates(
            activity_fields=activity_fields, product_fields=product_fields,
            languages=languages, existing_keywords=set(), limit=100,
        )
    )
    source_health = _source_health(
        rows, prefix=prefix, now_ms=now_ms, source_outcomes=source_outcomes,
    )
    candidate_sources = {
        item["source"] for item in fixed + structured if item.get("source")
    }
    source_signal = _campaign_source_signal(
        rows, prefix=prefix, candidate_sources=candidate_sources,
    )
    active_sources = _active_sources(rows, prefix=prefix)
    history_state = Counter({
        "unseen": 0, "active_duplicate": 0, "recently_executed": 0,
        "reusable_after_ttl": 0, "history_signal_unavailable": 0,
        "source_cooling": 0,
        "source_signal_unavailable": int(not source_signal["available"]),
    })
    unseen, reusable = [], []
    probe_used: set[str] = set()
    assessed_keys = set()
    # 旧任务缺国家/语言/完成时间时不能安全复用，也不能当作别的市场的通配符；
    # 仅跳过同活动同词，其他新七层组合仍可继续供给。
    untraceable_keywords = {
        _normalize_discovery_keyword(fields.get("关键词列表"))
        for row in rows
        for fields in [row.get("fields") or {}]
        if ext(fields.get("任务名")).startswith(prefix)
        and ext(fields.get("任务状态")) == "3-已完成"
        and (
            not _normalized_countries(fields.get("筛选-国家"))
            or not _normalized_language(fields.get("筛选-语言"))
            or not _last_modified_ms(row)
        )
    }
    for item in fixed + structured:
        countries = countries_by_language.get(item["language"]) or []
        if not countries:
            continue
        key = _discovery_history_key(item["keyword"], countries, item["language"])
        if key in assessed_keys:
            continue
        assessed_keys.add(key)
        if _normalize_discovery_keyword(item["keyword"]) in untraceable_keywords:
            history_state["history_signal_unavailable"] += 1
            continue
        state = _history_key_state(
            rows, keyword=item["keyword"], countries=countries,
            language=item["language"], now_ms=now_ms,
        )
        history_state[state["state"]] += 1
        if state["state"] in {
            "active_duplicate", "recently_executed", "history_signal_unavailable",
        }:
            continue
        if item["source"] in set(source_signal.get("isolated_sources") or []):
            history_state["source_signal_unavailable"] += 1
            continue
        health = source_health.get(item["source"]) or {"mode": "healthy"}
        if health.get("mode") == "cooldown":
            history_state["source_cooling"] += 1
            continue
        if health.get("mode") in {"probe", "signal_missing"}:
            if item["source"] in active_sources or item["source"] in probe_used:
                history_state["source_cooling"] += 1
                continue
            probe_used.add(item["source"])
        enriched = {
            **item, "countries": countries,
            "history_state": state["state"],
            "last_completed_ms": state["last_completed_ms"],
            "outcome_rank": int(state.get("outcome_rank", 1)),
        }
        if state["state"] == "unseen":
            unseen.append(enriched)
        else:
            reusable.append(enriched)
    # 新组合优先；旧组合重跑时按词源轮转，避免 legacy_fixed 独占整批。
    ordered = unseen + _round_robin_sources(reusable, source_health)
    effective_limit = max(0, int(limit))
    if theme == "dave" and not source_signal["available"]:
        # 只有所有候选来源都被隔离时才保留全活动单探测；
        # 单个来源或无归属旧记录异常不再压住其他健康来源。
        effective_limit = 0 if active_sources else min(1, effective_limit)
    selected = ordered[:effective_limit]
    history_state["deterministic_combinations_exhausted"] = max(
        0, int(limit) - len(selected),
    )
    return selected, dict(history_state), source_health, source_signal


def _campaign_discovery_quality(
    rows: list[dict], *, prefix: str, approved_candidates: int | None, now_ms: int,
) -> dict:
    campaign_rows = [
        row for row in rows
        if ext((row.get("fields") or {}).get("任务名")).startswith(prefix)
    ]
    completed = [
        row for row in campaign_rows
        if ext((row.get("fields") or {}).get("任务状态")) == "3-已完成"
    ]
    completed.sort(
        key=lambda row: _created_ms(row.get("fields") or {}), reverse=True,
    )
    recent = completed[:LOW_YIELD_RECENT_SAMPLE]
    recent_dated = [
        row for row in recent if _created_ms(row.get("fields") or {}) > 0
    ]
    email_outputs = [
        value for value in (
            _valid_email_output(row.get("fields") or {}) for row in recent
        ) if value is not None
    ]
    recent_valid_emails = sum(email_outputs)
    emails_per_task = (
        recent_valid_emails / len(email_outputs) if email_outputs else None
    )
    email_signal_coverage = (
        len(email_outputs) / len(recent_dated) if recent_dated else 0.0
    )
    approved_per_task = (
        max(0, int(approved_candidates)) / len(recent)
        if approved_candidates is not None and recent else None
    )
    reasons = []
    if (
        len(recent_dated) >= LOW_YIELD_MIN_EMAIL_SAMPLES
        and email_signal_coverage < LOW_YIELD_MIN_EMAIL_SIGNAL_COVERAGE
    ):
        # 当前任务表没有结构化“有效邮箱数”字段，只能读取版本化日志口径。
        # 一旦日志格式漂移，宁可降速探测，也不能静默恢复机械扩池。
        reasons.append("email_yield_signal_unavailable")
    if (
        len(email_outputs) >= LOW_YIELD_MIN_EMAIL_SAMPLES
        and emails_per_task is not None
        and emails_per_task < LOW_YIELD_MIN_EMAILS_PER_TASK
    ):
        reasons.append("recent_valid_email_yield_low")
    if (
        len(recent) >= LOW_YIELD_MIN_COMPLETED_FOR_CONVERSION
        and approved_per_task is not None
        and approved_per_task < LOW_YIELD_MIN_APPROVED_PER_TASK
    ):
        reasons.append("approved_candidate_conversion_low")
    last_created_ms = max(
        (_created_ms(row.get("fields") or {}) for row in campaign_rows), default=0,
    )
    low_yield = bool(reasons)
    cooling_down = bool(
        low_yield and last_created_ms
        and 0 <= now_ms - last_created_ms < LOW_YIELD_PROBE_COOLDOWN_MS
    )
    return {
        "mode": "cooldown" if cooling_down else "slow_probe" if low_yield else "normal",
        "reasons": reasons,
        "completed_tasks": len(completed),
        "recent_tasks_checked": len(recent),
        "recent_dated_tasks": len(recent_dated),
        "email_output_samples": len(email_outputs),
        "email_signal_source": "execution_log_v1",
        "email_signal_coverage": round(email_signal_coverage, 3),
        "recent_valid_emails": recent_valid_emails,
        "valid_emails_per_task": (
            round(emails_per_task, 3) if emails_per_task is not None else None
        ),
        "approved_candidates": approved_candidates,
        "approved_signal_source": "activity_new_development_approved_last_24h",
        "approved_per_recent_task": (
            round(approved_per_task, 3) if approved_per_task is not None else None
        ),
        "last_task_created_ms": last_created_ms,
        "cooldown_ms": LOW_YIELD_PROBE_COOLDOWN_MS,
    }


def _append_curated_candidates(
    *, theme: str, languages: list[str], existing_keywords: set[str],
    candidates: list, need: int,
) -> tuple[int, int]:
    added = 0
    positions = {lang: 0 for lang in languages}
    while need:
        progressed = False
        for lang in languages:
            words = _CAMPAIGN_FALLBACK_KEYWORDS.get(theme, {}).get(lang, [])
            while positions[lang] < len(words):
                word = words[positions[lang]].strip().lower()
                positions[lang] += 1
                if word in existing_keywords or any(
                    _candidate_keyword(existing) == word for existing in candidates
                ):
                    continue
                candidates.append((lang, word))
                added += 1
                need -= 1
                progressed = True
                break
            if need <= 0:
                break
        if not progressed:
            break
    return need, added


async def _ensure_campaign_supply_unlocked(
    *, campaign_id: str, activity: dict, product: dict,
    required_candidates: int, approved_candidates: int | None = None,
    dry_run: bool = False, max_tasks: int | None = None,
    structured_pilot: bool = False, pilot_version: str = "v1",
    allow_ai: bool = True, volume_priority: bool = False, model_budget=None,
    source_outcomes: dict | None = None,
) -> dict:
    """为单个活动补发现任务；体量优先时换词续跑，但不降低候选硬筛选。"""
    fields = activity.get("fields") or {}
    product_fields = product.get("fields") or {}
    language_aliases = {"英语": "en", "德语": "de", "西班牙语": "es"}
    languages = [
        language_aliases.get(value, value)
        for value in _multi_values(fields.get("活动目标语言"))
        if language_aliases.get(value, value) in {"en", "de", "es"}
    ] or ["en"]
    target_country_list = list(dict.fromkeys(
        value.strip().upper()
        for value in _multi_values(fields.get("活动目标国家"))
        if value.strip()
    ))
    target_countries = set(target_country_list)
    pilot_countries_by_language = {}
    locally_covered = set()
    for language in languages:
        if language == "en":
            continue
        matched = [
            country for country in _PILOT_COUNTRIES_BY_LANGUAGE[language]
            if country in target_countries
        ]
        pilot_countries_by_language[language] = matched
        locally_covered.update(matched)
    if "en" in languages:
        # 活动以英语区为主：未配置对应本地语种的目标国家交给英语组，
        # DE/ES 等已有本地语种的国家不重复抓。
        pilot_countries_by_language["en"] = [
            country for country in target_country_list
            if country not in locally_covered
        ]
    covered_target_countries = {
        country
        for countries in pilot_countries_by_language.values()
        for country in countries
    }
    uncovered_target_countries = [
        country for country in target_country_list
        if country not in covered_target_countries
    ]
    discovery_countries_by_language = _campaign_countries_by_language(
        languages=languages, target_countries=target_country_list,
        configured=pilot_countries_by_language,
    )
    if structured_pilot:
        languages = [
            language for language in languages
            if pilot_countries_by_language.get(language)
        ]
        if not languages:
            return {
                "ok": False, "created": 0, "would_create": 0,
                "error": "活动目标国家与活动目标语言没有可执行交集",
                "quality_filters_lowered": False,
                "uncovered_target_countries": uncovered_target_countries,
            }
    identity = f"{campaign_id} {ext(product_fields.get('产品英文名'))}".lower()
    theme = "dave" if "dave" in identity else "piranha" if "piranha" in identity else ""
    if not theme:
        return {"ok": False, "created": 0, "error": "当前活动缺少可验证的确定性拓词主题"}

    rows = (
        await feishu.fetch_all_records(T_CRAWLER, automatic_fields=True)
        if not structured_pilot
        else await feishu.fetch_all_records(T_CRAWLER)
    )

    existing_keywords = {
        ext((row.get("fields") or {}).get("关键词列表")).strip().lower()
        for row in rows
        if ext((row.get("fields") or {}).get("关键词列表")).strip()
    }
    prefix = f"[活动补池:{campaign_id}]"
    if pilot_version not in {"v1", "v2"}:
        raise ValueError("pilot_version must be v1 or v2")
    pilot_tag = (
        DAVE_KEYWORD_PILOT_V2_TAG if pilot_version == "v2" else DAVE_KEYWORD_PILOT_TAG
    )
    pilot_prefix = f"{prefix}{pilot_tag}"
    pilot_rows = [
        row for row in rows
        if ext((row.get("fields") or {}).get("任务名")).startswith(pilot_prefix)
    ]
    if structured_pilot and len(pilot_rows) >= DAVE_KEYWORD_PILOT_MAX_TASKS:
        return {
            "ok": True, "created": 0, "would_create": 0,
            "skipped": "pilot_already_created", "keywords": [],
            "keyword_source": f"structured_{pilot_version}", "shortfall_tasks": 0,
            "generation_error": "", "generation_warning": "",
            "pending_before": 0, "active_pending_before": 0,
            "stale_pending_before": 0, "target_tasks": 0,
            "quality_gate": {"mode": "not_recomputed"},
            "quality_filters_lowered": False,
            "uncovered_target_countries": uncovered_target_countries,
        }
    now_ms = int(time.time() * 1000)
    quality_gate = _campaign_discovery_quality(
        rows, prefix=prefix, approved_candidates=approved_candidates, now_ms=now_ms,
    )
    active_pending_for_campaign = 0
    stale_pending_for_campaign = 0
    for row in rows:
        row_fields = row.get("fields") or {}
        if not ext(row_fields.get("任务名")).startswith(prefix):
            continue
        if ext(row_fields.get("任务状态")) not in {
            "1-待触发", "2-执行中", "2-运行中",
        }:
            continue
        created_ms = _created_ms(row_fields)
        if created_ms and 0 <= now_ms - created_ms <= DISCOVERY_ACTIVE_TTL_MS:
            active_pending_for_campaign += 1
        else:
            stale_pending_for_campaign += 1
    target_tasks = max(3, min(9, math.ceil(max(1, int(required_candidates)) / 50)))
    if max_tasks is not None:
        target_tasks = min(target_tasks, max(1, int(max_tasks)))
    if structured_pilot:
        target_tasks = min(
            target_tasks,
            max(0, DAVE_KEYWORD_PILOT_MAX_TASKS - len(pilot_rows)),
        )
    quality_gate_enabled = theme == "piranha" or structured_pilot
    quality_cooldown_overridden = bool(
        volume_priority
        and quality_gate_enabled
        and quality_gate["mode"] in {"cooldown", "slow_probe"}
    )
    if (
        quality_gate_enabled
        and quality_gate["mode"] in {"cooldown", "slow_probe"}
        and not volume_priority
    ):
        target_tasks = 1
    common_result = {
        "pending_before": active_pending_for_campaign + stale_pending_for_campaign,
        "active_pending_before": active_pending_for_campaign,
        "stale_pending_before": stale_pending_for_campaign,
        "target_tasks": target_tasks,
        "quality_gate": quality_gate,
        "volume_priority": bool(volume_priority),
        "quality_cooldown_overridden": quality_cooldown_overridden,
        "quality_filters_lowered": False,
        "uncovered_target_countries": uncovered_target_countries,
        "model_calls": 0,
        "keyword_history_state": {},
        "source_health": {},
        "source_signal": {"available": True, "missing_tasks": 0, "details": []},
        "external_youtube_outcome": _external_youtube_outcome(rows, prefix=prefix),
    }
    if (
        quality_gate_enabled
        and quality_gate["mode"] == "cooldown"
        and not volume_priority
    ):
        return {
            "ok": True, "created": 0, "would_create": 0,
            "skipped": "quality_cooldown", "keywords": [],
            "keyword_source": "quality_cooldown", "shortfall_tasks": target_tasks,
            "generation_error": "", "generation_warning": "",
            "model_budget": model_budget.snapshot() if model_budget is not None else {},
            **common_result,
        }
    need = (
        target_tasks if structured_pilot
        else max(0, target_tasks - active_pending_for_campaign)
    )
    candidates = []
    if theme == "dave" and structured_pilot:
        candidates = _dave_structured_candidates(
            activity_fields=fields, product_fields=product_fields,
            languages=languages, existing_keywords=existing_keywords,
            pilot_version=pilot_version,
        )[:need]
        need -= len(candidates)
    if theme in {"dave", "piranha"} and not structured_pilot and need:
        selector = (
            _select_dave_external_candidates
            if theme == "dave" else _select_piranha_external_candidates
        )
        deterministic_candidates, history_state, source_health, source_signal = (
            selector(
            rows=rows, activity_fields=fields, product_fields=product_fields,
            languages=languages,
            countries_by_language=discovery_countries_by_language,
            prefix=prefix, now_ms=now_ms, limit=need,
            source_outcomes=source_outcomes,
        ))
        candidates.extend(deterministic_candidates)
        need -= len(deterministic_candidates)
        common_result["keyword_history_state"] = history_state
        common_result["source_health"] = source_health
        common_result["source_signal"] = source_signal
    if theme in {"dave", "piranha"} and not structured_pilot and candidates:
        sources = {item.get("source") for item in candidates if isinstance(item, dict)}
        keyword_source = (
            "deterministic" if sources == {"legacy_fixed"}
            else "seven_layer_deterministic" if "legacy_fixed" not in sources
            else "mixed_seven_layer_deterministic"
        )
    else:
        keyword_source = (
            f"structured_{pilot_version}" if structured_pilot and candidates
            else "deterministic" if candidates else "none"
        )
    generation_error = ""
    generation_warning = ""
    if need and theme == "piranha" and quality_gate["mode"] == "slow_probe":
        need, fallback_added = _append_curated_candidates(
            theme=theme, languages=languages, existing_keywords=existing_keywords,
            candidates=candidates, need=need,
        )
        if fallback_added:
            keyword_source = (
                "curated_fallback"
                if fallback_added == len(candidates) else "mixed_curated_fallback"
            )
    if need and theme == "piranha" and allow_ai:
        allowed, budget_reason = (
            model_budget.reserve(f"keyword:{campaign_id}")
            if model_budget is not None else (True, "ok")
        )
        if not allowed:
            generation_warning = budget_reason
        else:
            prompt = f"""你是海外游戏KOL发现助手。为{theme}主题新品活动补充YouTube创作者搜索词。
目标语言只能从 {languages} 选择。搜索对象必须是Nintendo/Switch、Mario收藏、主机游戏房或游戏硬件评测创作者；
不要生成产品购买词、店铺词、官方频道词，不要重复这些词：{sorted(existing_keywords)[-80:]}。
返回JSON：{{"keywords":[{{"language":"en","keyword":"..."}}]}}，至少{max(need * 2, need)}条。"""
            try:
                common_result["model_calls"] += 1
                accepted_before = len(candidates)
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
                    if word in existing_keywords or any(
                        _candidate_keyword(existing) == word for existing in candidates
                    ):
                        continue
                    market = next((m for m in MARKETS if m["lang"] == lang), {"lang": lang})
                    if not _is_localized(word, market):
                        continue
                    candidates.append((lang, word))
                    need -= 1
                if len(candidates) > accepted_before:
                    if model_budget is not None:
                        model_budget.record_success()
                    keyword_source = "mixed" if keyword_source == "deterministic" else "dynamic"
                elif model_budget is not None:
                    model_budget.record_failure()
            except Exception as exc:
                if model_budget is not None:
                    model_budget.record_failure(terminal=deepseek.is_terminal_error(exc))
                generation_warning = str(exc)[:160]

    if need and theme == "piranha":
        need, fallback_added = _append_curated_candidates(
            theme=theme, languages=languages, existing_keywords=existing_keywords,
            candidates=candidates, need=need,
        )
        if fallback_added:
            keyword_source = (
                "curated_fallback"
                if fallback_added == len(candidates)
                else "mixed_curated_fallback"
            )

    if need and generation_warning:
        generation_error = generation_warning

    skipped = (
        "source_signal_unavailable"
        if theme == "dave" and not structured_pilot
        and not common_result["source_signal"].get("available", True)
        else
        "deterministic_combinations_exhausted"
        if theme == "dave" and not structured_pilot and need else
        "fixed_keywords_exhausted" if need and not allow_ai else ""
    )
    if dry_run:
        external_discovery = {
            "created": 0, "would_create": len(candidates),
            "active_pending": active_pending_for_campaign,
            "stale_pending": stale_pending_for_campaign,
            "target_tasks": target_tasks,
            "shortfall_tasks": need,
            **common_result["keyword_history_state"],
        }
        return {
            "ok": True, "created": 0, "would_create": len(candidates),
            "keywords": [
                item if isinstance(item, dict)
                else {"language": item[0], "keyword": item[1]}
                for item in candidates
            ],
            "keyword_source": keyword_source, "shortfall_tasks": need,
            "generation_error": generation_error,
            "generation_warning": generation_warning,
            "skipped": skipped,
            "external_youtube_discovery": external_discovery,
            "model_budget": model_budget.snapshot() if model_budget is not None else {},
            **common_result,
        }

    now = int(time.time() * 1000)
    created, errors = 0, []
    for item in candidates:
        if isinstance(item, dict):
            lang, word = item["language"], item["keyword"]
            source = item.get("source") or "deterministic"
            task_countries = list(
                item.get("countries")
                or discovery_countries_by_language.get(lang)
                or []
            )
        else:
            lang, word = item
            source = "deterministic"
            task_countries = list(
                discovery_countries_by_language.get(lang) or []
            )
        try:
            task_name = (
                f"{pilot_prefix}[词源:{source}] YT KOL - {word}"
                if structured_pilot else
                f"{prefix}[词源:{source}] YT KOL - {word}"
                if isinstance(item, dict) else
                f"{prefix} YT KOL - {word}"
            )
            await feishu.create_record(T_CRAWLER, {
                "任务名": task_name,
                "爬虫类型": "KOL-YouTube", "关键词列表": word,
                "筛选-国家": task_countries, "筛选-语言": [lang],
                "每批数量上限": PER_BATCH_LIMIT, "任务状态": "1-待触发",
                "触发": True, "创建日期": now,
            })
            created += 1
        except Exception as exc:
            errors.append(f"{word}: {str(exc)[:100]}")
    if need and not generation_error:
        generation_error = f"仍缺{need}个符合历史窗口和来源健康规则的外部YouTube发现任务"
    total_shortfall = need + len(errors)
    external_discovery = {
        "created": created, "would_create": 0,
        "active_pending": active_pending_for_campaign,
        "stale_pending": stale_pending_for_campaign,
        "target_tasks": target_tasks,
        "shortfall_tasks": total_shortfall,
        **common_result["keyword_history_state"],
    }
    return {
        "ok": not errors and not generation_error, "created": created, "errors": errors[:5],
        "keywords": [
            item if isinstance(item, dict)
            else {"language": item[0], "keyword": item[1]}
            for item in candidates
        ],
        "keyword_source": keyword_source, "shortfall_tasks": total_shortfall,
        "generation_error": generation_error,
        "generation_warning": generation_warning,
        "skipped": skipped,
        "external_youtube_discovery": external_discovery,
        "model_budget": model_budget.snapshot() if model_budget is not None else {},
        **common_result,
    }


async def ensure_campaign_supply(
    *, campaign_id: str, activity: dict, product: dict,
    required_candidates: int, approved_candidates: int | None = None,
    dry_run: bool = False, max_tasks: int | None = None,
    structured_pilot: bool = False, pilot_version: str = "v1",
    allow_ai: bool = True, volume_priority: bool = False, model_budget=None,
    source_outcomes: dict | None = None,
) -> dict:
    """活动级唯一写入口；同一活动的自治补池和灰度共用一把读后写锁。"""
    kwargs = {
        "campaign_id": campaign_id, "activity": activity, "product": product,
        "required_candidates": required_candidates,
        "approved_candidates": approved_candidates, "dry_run": dry_run,
        "max_tasks": max_tasks, "structured_pilot": structured_pilot,
        "pilot_version": pilot_version, "allow_ai": allow_ai,
        "volume_priority": volume_priority, "model_budget": model_budget,
        "source_outcomes": source_outcomes,
    }
    if dry_run:
        return await _ensure_campaign_supply_unlocked(**kwargs)
    lock = _CAMPAIGN_SUPPLY_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        # 获取锁后才读取爬虫任务表，关闭两个入口并发“同时读旧快照再重复建任务”。
        return await _ensure_campaign_supply_unlocked(**kwargs)


async def run_campaign_pilot(*, campaign_id: str, required_candidates: int = 200,
                             max_tasks: int = 4, dry_run: bool = True,
                             pilot_version: str = "v1") -> dict:
    """读真实活动/产品后预演或创建小批发现任务；绝不生成草稿或发送邮件。"""
    activity = await launch_evidence.get_activity(campaign_id)
    fields = activity.get("fields") or {}
    if campaign_id != DAVE_KEYWORD_PILOT_CAMPAIGN_ID:
        raise ValueError("当前灰度只允许戴夫活动；其他产品须在灰度通过并定稿后启用")
    if (
        ext(fields.get("运行模式")) != "正式运行"
        or ext(fields.get("状态")) != "正式执行中"
    ):
        raise ValueError("戴夫活动不是正式运行/正式执行中，禁止创建灰度任务")
    product_id = ext(fields.get("产品主记录ID")).strip()
    if not product_id:
        linked = fields.get("关联产品主记录")
        if isinstance(linked, dict):
            ids = linked.get("link_record_ids") or linked.get("record_ids") or []
            product_id = str(ids[0]) if ids else ""
        elif isinstance(linked, list) and linked:
            product_id = str(linked[0])
    if not product_id:
        raise ValueError(f"活动缺少产品主记录ID: {campaign_id}")
    product = await feishu.get_record(config.T_PRODUCT, product_id)
    if ext((product.get("fields") or {}).get("派单模式")) != "活动专用":
        raise ValueError("戴夫产品未处于活动专用锁，禁止创建灰度任务")
    async def execute_pilot() -> dict:
        return await ensure_campaign_supply(
            campaign_id=campaign_id, activity=activity, product=product,
            required_candidates=max(1, int(required_candidates)),
            dry_run=dry_run, max_tasks=max(1, min(4, int(max_tasks))),
            structured_pilot=True, pilot_version=pilot_version,
        )

    result = await execute_pilot()
    writes = int(result.get("created") or 0)
    return {
        **result, "campaign_id": campaign_id, "product_id": product_id,
        "pilot_version": pilot_version,
        "read_only": bool(dry_run), "writes": writes,
        "drafts_created": 0, "emails_sent": 0,
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
