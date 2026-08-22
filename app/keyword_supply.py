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
from collections import Counter
from . import config, feishu, deepseek, launch_evidence
from .feishu import ext

T_CRAWLER = "tblQnLHnBa1RjJUE"   # 爬虫任务台 (KOL 营销库内)
PER_BATCH_LIMIT = 50            # 每词 daemon 抓取上限
DISCOVERY_ACTIVE_TTL_MS = 2 * 60 * 60 * 1000
LOW_YIELD_PROBE_COOLDOWN_MS = 2 * 60 * 60 * 1000
LOW_YIELD_RECENT_SAMPLE = 12
LOW_YIELD_MIN_EMAIL_SAMPLES = 6
LOW_YIELD_MIN_EMAIL_SIGNAL_COVERAGE = 0.80
LOW_YIELD_MIN_COMPLETED_FOR_CONVERSION = 6
LOW_YIELD_MIN_EMAILS_PER_TASK = 0.25
LOW_YIELD_MIN_APPROVED_PER_TASK = 0.10
DAVE_KEYWORD_PILOT_CAMPAIGN_ID = "launch-20260915-funlab-dave-ys11-5"
DAVE_KEYWORD_PILOT_TAG = "[灰度:dave-keyword-v1]"
DAVE_KEYWORD_PILOT_MAX_TASKS = 4
_DAVE_KEYWORD_PILOT_LOCK = asyncio.Lock()

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


def _dave_structured_candidates(*, activity_fields: dict, product_fields: dict,
                                languages: list[str], existing_keywords: set[str]) -> list[dict]:
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
                    "ip_theme": ["ip_theme", "content_format"],
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


def _valid_email_output(fields: dict) -> int | None:
    match = re.search(r"其中有邮箱\s*[:：]\s*(\d+)", ext(fields.get("执行日志")))
    return int(match.group(1)) if match else None


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
    candidates: list[tuple[str, str]], need: int,
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
                    existing == word for _, existing in candidates
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


async def ensure_campaign_supply(*, campaign_id: str, activity: dict, product: dict,
                                 required_candidates: int,
                                 approved_candidates: int | None = None,
                                 dry_run: bool = False,
                                 max_tasks: int | None = None,
                                 structured_pilot: bool = False) -> dict:
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

    existing_keywords = {
        ext((row.get("fields") or {}).get("关键词列表")).strip().lower()
        for row in rows
        if ext((row.get("fields") or {}).get("关键词列表")).strip()
    }
    prefix = f"[活动补池:{campaign_id}]"
    pilot_prefix = f"{prefix}{DAVE_KEYWORD_PILOT_TAG}"
    pilot_rows = [
        row for row in rows
        if ext((row.get("fields") or {}).get("任务名")).startswith(pilot_prefix)
    ]
    if structured_pilot and len(pilot_rows) >= DAVE_KEYWORD_PILOT_MAX_TASKS:
        return {
            "ok": True, "created": 0, "would_create": 0,
            "skipped": "pilot_already_created", "keywords": [],
            "keyword_source": "structured_v1", "shortfall_tasks": 0,
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
    if quality_gate_enabled and quality_gate["mode"] in {"cooldown", "slow_probe"}:
        target_tasks = 1
    common_result = {
        "pending_before": active_pending_for_campaign + stale_pending_for_campaign,
        "active_pending_before": active_pending_for_campaign,
        "stale_pending_before": stale_pending_for_campaign,
        "target_tasks": target_tasks,
        "quality_gate": quality_gate,
        "quality_filters_lowered": False,
        "uncovered_target_countries": uncovered_target_countries,
    }
    if quality_gate_enabled and quality_gate["mode"] == "cooldown":
        return {
            "ok": True, "created": 0, "would_create": 0,
            "skipped": "quality_cooldown", "keywords": [],
            "keyword_source": "quality_cooldown", "shortfall_tasks": target_tasks,
            "generation_error": "", "generation_warning": "",
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
        )[:need]
        need -= len(candidates)
    while (not structured_pilot and need
           and any(_CAMPAIGN_KEYWORDS[theme].get(lang) for lang in languages)):
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

    keyword_source = "structured_v1" if structured_pilot and candidates else (
        "deterministic" if candidates else "none"
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

    if dry_run:
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
            **common_result,
        }

    now = int(time.time() * 1000)
    countries_by_language = (
        pilot_countries_by_language if structured_pilot
        else {"en": ["US", "UK", "CA"], "de": ["DE"], "es": ["ES"]}
    )
    created, errors = 0, []
    for item in candidates:
        if isinstance(item, dict):
            lang, word = item["language"], item["keyword"]
            source = item.get("source") or "deterministic"
        else:
            lang, word = item
            source = "deterministic"
        try:
            task_name = (
                f"{pilot_prefix}[词源:{source}] YT KOL - {word}"
                if structured_pilot else f"{prefix} YT KOL - {word}"
            )
            await feishu.create_record(T_CRAWLER, {
                "任务名": task_name,
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
        "keywords": [
            item if isinstance(item, dict)
            else {"language": item[0], "keyword": item[1]}
            for item in candidates
        ],
        "keyword_source": keyword_source, "shortfall_tasks": need,
        "generation_error": generation_error,
        "generation_warning": generation_warning,
        **common_result,
    }


async def run_campaign_pilot(*, campaign_id: str, required_candidates: int = 200,
                             max_tasks: int = 4, dry_run: bool = True) -> dict:
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
            structured_pilot=True,
        )

    if dry_run:
        result = await execute_pilot()
    else:
        # 当前生产为单实例；锁内重新读取任务台，避免两个同时提交各写4条。
        async with _DAVE_KEYWORD_PILOT_LOCK:
            result = await execute_pilot()
    writes = int(result.get("created") or 0)
    return {
        **result, "campaign_id": campaign_id, "product_id": product_id,
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
