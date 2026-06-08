"""媒体人版富化 + 打分 + 生 PR 草稿 (复刻 enrich.py 流程, KOL→媒体人).

变更对照 KOL 版:
- 用 scoring.score_editor 6 维 (地区/语言/品类/媒体集团/触达稳定/防骚扰), 不是 score_kol
- 没有粉丝范围/平台筛选 (媒体人池子小, 让 score_editor 自然扣分)
- DeepSeek prompt 是 PR pitch 风格 (正式商务, 不是朋友 DM)
- 关联字段「关联媒体人」, 对象类型 "媒体人"
- 草稿表 6 维分字段: 地区分/语言分/品类分/媒体集团分/触达稳定分/防骚扰分

n8n cron 每 5 分钟扫 T_TASK_EDITOR 任务状态=2-待触发 + 触发=true 的任务."""
import re, time, asyncio, random
from . import config, feishu, deepseek, draft_router, snov
from .feishu import ext, xrid
from .scoring import score_editor, _parse_multiselect


COUNTRY_TO_LANG = {
    "US": "en", "UK": "en", "CA": "en", "AU": "en", "PH": "en", "IN": "en",
    "DE": "de", "FR": "fr", "ES": "es", "BR": "pt", "PT": "pt",
    "JP": "ja", "IT": "it", "NL": "nl", "MX": "es", "TH": "en", "AE": "en",
    "ID": "en", "SE": "sv",
}
LANG_DISPLAY = {
    "en": "English", "de": "German", "fr": "French", "es": "Spanish",
    "pt": "Portuguese", "ja": "Japanese", "it": "Italian", "nl": "Dutch", "sv": "Swedish",
}

# V1.5 直控筛选: 任务台「筛选-语言」中文 options → 媒体人主表「语言」字段 ISO 代码
LANG_CN_TO_ISO = {
    "英语": "en", "德语": "de", "日语": "ja", "法语": "fr",
    "西班牙语": "es", "葡萄牙语": "pt", "中文": "zh",
    "意大利语": "it", "荷兰语": "nl", "瑞典语": "sv", "其他": "其他",
    "en": "en", "de": "de", "ja": "ja", "fr": "fr",
    "es": "es", "pt": "pt", "zh": "zh", "it": "it",
    "nl": "nl", "sv": "sv",
}

# Ban phrases (媒体人 PR 也需要防 LLM 假装看过具体文章)
BAN_PHRASE_PATTERNS = [
    re.compile(r"\bI\s+(saw|read|caught|loved|enjoyed)\s+(your|the)\b", re.I),
    re.compile(r"\bjust\s+(read|saw)\b", re.I),
    re.compile(r"\byour\s+(latest|recent|last|new)\s+(article|piece|review|story|coverage|post)", re.I),
    re.compile(r"\byour\s+(articles?|pieces?|reviews?|stories?|posts?)\b", re.I),
    re.compile(r"\bthat\s+(article|piece|review|story)\s+you\b", re.I),
]


def _check_ban_phrases(body: str) -> list:
    hits = []
    for p in BAN_PHRASE_PATTERNS:
        m = p.search(body or "")
        if m:
            hits.append(m.group(0))
    return hits


SIGNATURE_POOL = {
    "FUNLAB": ["Tom from FUNLAB PR", "Mia @ FUNLAB Press", "Alex / FUNLAB Press Office"],
    "POWKONG": ["Lisa @ POWKONG PR", "Ryan from POWKONG Press", "Jamie / POWKONG Press"],
    "白牌": ["Emma @ LY Gamer", "Leo @ LY Gamer", "Mia @ LY Gamer"],
}

# 主题前缀池 — Python 端随机选 1 个传给 prompt, 避免 5 条全 "Exclusive:" 模板化
SUBJECT_PREFIXES = [
    {"prefix": "Exclusive:", "tone": "首发/独家发布感"},
    {"prefix": "First Look:", "tone": "提前看到/预览感"},
    {"prefix": "Preview:", "tone": "新品预览感"},
    {"prefix": "Heads up:", "tone": "友善提示感, 不像营销"},
    {"prefix": "Coming this week:", "tone": "时效紧迫感"},
    {"prefix": "", "tone": "不带前缀, 直接产品 news angle 陈述"},
]

# 开头句模板池 — Python 端随机选 1 个传给 prompt, 避免 4/5 都用 "Given your coverage of X at Y,"
OPENING_TEMPLATES = [
    "Given your coverage of {cat} at {media}, I thought this might interest you.",
    "Saw your work covers {cat} territory — wanted to flag this one.",
    "Your {media} beat overlaps with what we're launching, so reaching out directly.",
    "Quick one for someone covering {cat}: ",
    "Thought of you given your focus on {cat}.",
    "Reaching out because {media}'s readers tend to vibe with this kind of accessory.",
]


def _filter_endorsement_for_editor(p_media_endorse: str, editor_media: str) -> tuple:
    """检测产品「媒体报道」是否引用了收件编辑自家媒体的话.
    返回 (filtered_endorsement, was_filtered)
    - 编辑「所属媒体」字段可能是 'IGN | IGN UK' / 'IGN, Kotaku' 等组合, 拆分隔符取每个媒体 token
    - 任一 token (长度 ≥3, 跳过 stopword) 出现在背书原文 → 过滤含该 token 的整句"""
    if not p_media_endorse or not editor_media:
        return p_media_endorse, False
    import re as _re
    # 1. 拆 editor_media 为 token list (按 | / , ; 等分隔)
    raw_tokens = _re.split(r'[|/,;、]', editor_media)
    STOPWORDS = {"the", "a", "an", "of", "and", "or", "&", "-", "—", "uk", "us", "eu"}
    tokens = []
    for t in raw_tokens:
        t = t.strip().lower()
        if len(t) >= 3 and t not in STOPWORDS:
            tokens.append(t)
    if not tokens:
        return p_media_endorse, False

    endorse_lower = p_media_endorse.lower()
    matched_tokens = [t for t in tokens if t in endorse_lower]
    if not matched_tokens:
        return p_media_endorse, False

    # 2. 按句号/分号/管道分句, 移除含任一命中 token 的句
    sentences = _re.split(r'(?<=[.!?。!?])\s+|;\s*|；\s*|\|', p_media_endorse)
    kept = []
    for s in sentences:
        s_lower = s.lower()
        if any(t in s_lower for t in matched_tokens):
            continue
        if s.strip():
            kept.append(s.strip())
    filtered = " | ".join(kept).strip()
    return filtered, True

COUNTRY_TZ = {"US": -5, "UK": 0, "DE": 1, "CA": -5, "PH": 8, "FR": 1, "ES": 1,
              "BR": -3, "AU": 10, "NL": 1, "IT": 1, "MX": -6, "IN": 5.5,
              "JP": 9, "TH": 7, "AE": 4, "ID": 7, "SE": 1, "PT": 0}
APAC = {"JP", "TH", "PH", "ID", "IN", "AE"}


def _next_send_time(country_iso: str):
    from datetime import datetime, timedelta, timezone
    now_utc = datetime.now(timezone.utc)
    offset = COUNTRY_TZ.get(country_iso, 0)
    best_hour = 15 if country_iso in APAC else 10
    local = now_utc + timedelta(hours=offset)
    target = local.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    while True:
        wd = target.weekday()
        if country_iso in APAC:
            if wd == 3: break
        else:
            if wd in (1, 2, 3): break
        target += timedelta(days=1)
        target = target.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    target_utc = target - timedelta(hours=offset)
    desc = f"{['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][target.weekday()]} {best_hour:02d}:00 local ({country_iso})"
    return int(target_utc.timestamp() * 1000), desc


# ===== 1. 任务扫描 =====
async def find_pending_tasks() -> list:
    items = await feishu.search_records(config.T_TASK_EDITOR, [
        {"field_name": "任务状态", "operator": "is", "value": ["2-待触发"]},
        {"field_name": "触发", "operator": "is", "value": ["true"]},
    ])
    return items


# ===== 2. 媒体人候选筛选 =====
# 草稿状态白名单(可重发) — 已否决 / 发送失败 之外的状态视作"已锁定", 跳过
_DRAFT_REUSABLE_STATES = {"已否决", "发送失败"}

# === 媒体人 V2 — 品牌感知三层防御 ===
# 跨品牌可派 (媒体人合作完后状态会回 "已合作")
DISPATCHABLE_ANY_BRAND_EDITOR = {"", "未建联", "已合作"}
# 永久排除
EXCLUDE_HARD_EDITOR = {"不合适"}
# 流程中状态 (建联中 / 洽谈中 / 样品评估 / 未产出) → 仅同品牌排除

# 7 天同品牌轰炸窗
RECENT_BLAST_DAYS_EDITOR = 7


def _brand_from_email_editor(email: str) -> str:
    # 2026-06-08 改配置驱动(支持白牌)
    return config.brand_from_text(email)


async def filter_editors(task_fields: dict, product_rid: str = "", brand: str = "",
                         seen_kb: set = None) -> list:
    """按任务条件筛选媒体人 — 品牌感知三层防重派单 (KOL 资产化 V2).

    Layer 1: 合作状态品牌感知
        - 不合适 → 任何品牌都排除
        - 未建联 / 已合作 / 空 → 任何品牌都通过
        - 建联中 / 洽谈中 / 样品评估 / 未产出 → 仅排除"产生此状态的品牌"
    Layer 2: 同媒体人 × 同产品 永久不重派.
    Layer 3: 7 天内同媒体人同品牌任意产品已派 → 排除 (跨品牌不算).
    """
    import time as _time
    cats_want = task_fields.get("筛选-报道品类") or []
    types_want = task_fields.get("筛选-媒体类型") or []
    groups_want = task_fields.get("筛选-媒体集团") or []
    countries_want = task_fields.get("筛选-国家") or []
    langs_want = task_fields.get("筛选-语言") or []
    for x in (cats_want, types_want, groups_want, countries_want, langs_want):
        if x is None: x = []
    if not isinstance(cats_want, list): cats_want = [cats_want]
    if not isinstance(types_want, list): types_want = [types_want]
    if not isinstance(groups_want, list): groups_want = [groups_want]
    if not isinstance(countries_want, list): countries_want = [countries_want]
    if not isinstance(langs_want, list): langs_want = [langs_want]

    # 单选/多选字段统一抽 text
    def _opt_text(x):
        if isinstance(x, dict): return x.get("text") or x.get("name") or ""
        return str(x or "")
    cats_want = [_opt_text(x) for x in cats_want]
    types_want = [_opt_text(x) for x in types_want]
    groups_want = [_opt_text(x) for x in groups_want]
    countries_want = [_opt_text(x) for x in countries_want]
    langs_want = [_opt_text(x) for x in langs_want]
    # V1.5: 任务台 options 由 ISO 改成中文(如「英语」), 此处统一映射成 ISO 跟主表「语言」比对
    langs_want_iso = {LANG_CN_TO_ISO.get(l, l) for l in langs_want}

    batch_limit = int(task_fields.get("人数上限") or 30)
    hard_pool = max(batch_limit * 5, 100)

    # 一次性拉全量媒体人草稿 (Layer 1/2/3 共享)
    all_ed_drafts = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "对象类型", "operator": "is", "value": ["媒体人"]},
    ])

    # Layer 1 数据准备: 每个媒体人最近一次"非否决/失败"草稿 → 反推当前流程中是哪个品牌
    editor_active_brand = {}
    editor_latest_ms = {}
    for d in all_ed_drafts:
        f = d.get("fields", {})
        if ext(f.get("邮件草稿状态")) in _DRAFT_REUSABLE_STATES: continue
        ed_rid = xrid(f.get("关联媒体人"))
        if not ed_rid: continue
        gen_ms = f.get("生成时间", 0) or 0
        if not isinstance(gen_ms, (int, float)): gen_ms = 0
        if editor_latest_ms.get(ed_rid, 0) < gen_ms:
            editor_latest_ms[ed_rid] = gen_ms
            editor_active_brand[ed_rid] = _brand_from_email_editor(ext(f.get("发送邮箱")))

    # Layer 2: 同产品已存在媒体人草稿 → 永久排除
    exclude_editor_ids = set()
    if product_rid:
        for d in all_ed_drafts:
            f = d.get("fields", {})
            if ext(f.get("邮件草稿状态")) in _DRAFT_REUSABLE_STATES: continue
            link_products = f.get("关联产品") or {}
            link_ids = link_products.get("link_record_ids") or [] if isinstance(link_products, dict) else []
            if product_rid not in link_ids: continue
            ed_rid = xrid(f.get("关联媒体人"))
            if ed_rid: exclude_editor_ids.add(ed_rid)

    # Layer 3: 7 天内同品牌任意产品已派 → 排除 (跨品牌不算)
    recent_blasted_same_brand = set()
    cutoff_ms = int(_time.time() * 1000) - RECENT_BLAST_DAYS_EDITOR * 86400 * 1000
    for d in all_ed_drafts:
        f = d.get("fields", {})
        if ext(f.get("邮件草稿状态")) in _DRAFT_REUSABLE_STATES: continue
        gen_ms = f.get("生成时间")
        if not isinstance(gen_ms, (int, float)) or gen_ms < cutoff_ms: continue
        draft_brand = _brand_from_email_editor(ext(f.get("发送邮箱")))
        if brand and draft_brand and draft_brand != brand: continue
        ed_rid = xrid(f.get("关联媒体人"))
        if ed_rid: recent_blasted_same_brand.add(ed_rid)

    # 池: 仅 邮箱 isNotEmpty (放开 合作状态 严格 filter, 改 in-loop check)
    items = await feishu.search_records(config.T_EDITOR, [
        {"field_name": "邮箱", "operator": "isNotEmpty", "value": []},
    ])
    pool_total = len(items)

    hits = []
    skipped_status, skipped_dedup, skipped_blast, skipped_inrun = 0, 0, 0, 0
    for rec in items:
        f = rec.get("fields", {})
        rid = rec["record_id"]
        coop = ext(f.get("合作状态"))
        # Layer 1 (品牌感知)
        if coop in EXCLUDE_HARD_EDITOR:
            skipped_status += 1; continue
        if coop not in DISPATCHABLE_ANY_BRAND_EDITOR:
            active_brand = editor_active_brand.get(rid, "")
            if active_brand and brand and active_brand == brand:
                skipped_status += 1; continue
        # Layer 2: 同媒体人 × 同产品永久不重派
        if rid in exclude_editor_ids:
            skipped_dedup += 1; continue
        # Layer 3: 7 天同媒体人同品牌轰炸 (跨品牌不算)
        if rid in recent_blasted_same_brand:
            skipped_blast += 1; continue
        # P0 本轮内存去重: 同一 cron run 内,同媒体人同品牌已被前序任务派过 → 跳过(防 search 索引延迟漏防的并发重复)
        if seen_kb and (rid, brand) in seen_kb:
            skipped_inrun += 1; continue
        if countries_want and ext(f.get("国家")) not in countries_want: continue
        if langs_want_iso and ext(f.get("语言")) not in langs_want_iso: continue
        if types_want and ext(f.get("媒体类型")) not in types_want: continue
        if groups_want and ext(f.get("媒体集团")) not in groups_want: continue
        if cats_want:
            ed_cats = list(_parse_multiselect(f.get("报道品类")))
            if not any(c in ed_cats for c in cats_want): continue
        hits.append(rec)
        if len(hits) >= hard_pool: break

    print(f"[enrich_editor V2 品牌感知] filter: pool={pool_total} → hits={len(hits)} "
          f"brand={brand} (L1同品牌流程中/拒绝={skipped_status}, L2同产品永久={skipped_dedup}, "
          f"L3同品牌7天轰炸={skipped_blast}, P0本轮去重={skipped_inrun}, countries={countries_want}, langs={list(langs_want_iso)})")

    return hits[:hard_pool]


# ===== 3. DeepSeek 仅生 PR 草稿 (打分本地完成) =====
async def gen_pr_draft(editor_record: dict, product: dict, brand: str,
                        signature: str, breakdown: dict, total: float) -> dict:
    e = editor_record["fields"]
    name = ext(e.get("媒体人姓名"))
    media = ext(e.get("所属媒体")) or ext(e.get("主要媒体"))
    media_type = ext(e.get("媒体类型"))
    media_group = ext(e.get("媒体集团"))
    country = ext(e.get("国家"))
    country_cn = ext(e.get("国家原文"))
    lang = ext(e.get("语言")) or "en"
    email = ext(e.get("邮箱"))
    cats = list(_parse_multiselect(e.get("报道品类")))
    bio = ext(e.get("IP喜好"))
    recent = ext(e.get("最近文章标题"))
    if not email:
        return {"skip": "无邮箱"}

    pf = product["fields"]
    p_en = ext(pf.get("产品英文名"))
    if p_en:
        p_name = p_en
    else:
        p_name_raw = ext(pf.get("产品名"))
        p_name = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', p_name_raw).strip() or p_name_raw
        print(f"[WARN] 产品缺少「产品英文名」, 降级用中文剥前缀: {p_name}")
    p_brand = ext(pf.get("品牌"))
    p_cat = ext(pf.get("品类"))
    p_s1 = ext(pf.get("卖点1"))
    p_s2 = ext(pf.get("卖点2"))
    p_s3 = ext(pf.get("卖点3"))
    p_url_raw = feishu.product_url(pf)   # 官网链接优先, 缺则降级亚马逊链接(都是 URL 字段, 防死链)
    p_price = pf.get("报价(USD)", 0)
    p_audience = ext(pf.get("目标人群"))
    p_media_endorse_raw = ext(pf.get("媒体报道"))

    # 时效由头 / News Peg (2026-06-08): 媒体外联核心由头, 借游戏 IP 上线节点. 空=行为不变.
    news_hook = ext(pf.get("时效由头"))
    news_hook_block = ""
    if news_hook:
        news_hook_block = (
            "\n【时效由头 / News Peg】(本封 pitch 的核心由头, 但严禁暗示该 DLC/活动是我方制作)\n"
            f"{news_hook}\n"
            "→ 主题前缀优先用时效型 (Coming this week / Exclusive), "
            "中段\"为什么现在值得报道\"必须锚定到这个上线节点 "
            "(官方授权周边 + 玩家配合新内容的报道时机)。\n"
        )

    # UTM 注入
    from . import utm as _utm
    p_url = _utm.make_utm_link(p_url_raw, brand, p_name, name)
    utm_id_value = _utm.kol_utm_id(name)  # 复用 KOL UTM ID 体系 (媒体人也按名字 hash)

    # 防自家媒体引用: 如产品「媒体报道」含编辑所属媒体的引言, 过滤掉那句
    p_media_endorse, was_filtered = _filter_endorsement_for_editor(p_media_endorse_raw, media)
    if was_filtered:
        print(f"[enrich-editor] 过滤自家媒体引言 ({media}) 给 {name}")

    lang_display = LANG_DISPLAY.get(lang, "English")
    high_dims = sorted(breakdown.items(), key=lambda x: x[1]["score"], reverse=True)[:3]
    angle_hints = " / ".join(f"{k}:{v['reason']}" for k, v in high_dims)

    # 主题前缀 + 开头模板: Python 端随机选, 强制 5 条草稿用不同变体
    chosen_prefix = random.choice(SUBJECT_PREFIXES)
    primary_cat = cats[0] if cats else (p_cat or "gaming accessories")
    chosen_opening = random.choice(OPENING_TEMPLATES).format(
        cat=primary_cat, media=media or "your beat",
    )

    prompt = f"""你是游戏配件品牌的公关 PR. 为游戏/科技媒体编辑撰写 pitch 邮件 (媒体外联, 比 KOL 带货更正式).

【2026 媒体外联 PR pitch 黄金法则】(必须遵守)

📌 主题行 (<55 字符, 具体, 有新闻价值感)
  ✓ 点出产品 news angle, 不是 "collaboration" / "partnership"
  ✗ 禁: "Partnership", "Collab", "Looking to feature", "Quick question"
  ✓ **本封必须用主题前缀**: "{chosen_prefix['prefix']}" (调性: {chosen_prefix['tone']})
    - 如前缀为空, 不带前缀直接陈述 product news (例 "Piranha Plant dock for Switch 2 fans")
    - 主题示范: "{chosen_prefix['prefix']} Piranha Plant dock for Switch 2".strip()

📌 正文开头 (第 1 句, 必须用以下变体, **不要套路化**)
  ✓ **本封建议开头**: "{chosen_opening}"
  ✓ 你也可以基于编辑的 报道品类 / 媒体名 写一个语义类似的变体, 但**严禁**直接照搬下面这种已被滥用的开头:
    ❌ "Given your coverage of [X] at [Y]," (已用过太多次, 显得是模板)
  ✓ 模糊但真实 > 具体但编造 — 严禁 "I read your article on X" / "Your latest review" 等

📌 正文整体 (120-180 词, 全文 {lang_display})
  ✓ 必须以 "Hi {{编辑名}}," 或 "Dear {{编辑名}}," 开头 (从姓名提取 first name) — 严禁 "Hi there," / 任何匿名
  🚨 严禁编造具体文章/评测 — 我们只有标签, 没真看过内容
    ✗ 禁 "I saw/read your X article/review/piece"
    ✗ 禁 任何 "你最近的 [具体作品]" 句式 — LLM 幻觉, 客户一眼识破
    ✓ 用基于 报道品类 / 媒体名 的概括 (参考上面给的开头模板)
  ✓ 中段: 产品新闻角度 (为什么值得报道, 不是"产品多好"):
    - 设计亮点 / 限量 / 与游戏 IP 关联 / 已有媒体背书 (但**不要引收件人自家媒体**, 见下方)
  ✓ 提供记者需要的: 产品页 (含图) / 寄测样意向
  ✓ 1 行产品链接独立段落: <p>📎 Press kit: <a href="{p_url}">Product page →</a></p>
     - en: "Product page →"  / de: "Produktseite →" / fr: "Fiche produit →"
     - es: "Página del producto →"  / pt: "Página do produto →"  / ja: "製品ページ →"
     - it: "Pagina prodotto →" / nl: "Productpagina →"
  ✓ CTA: "Happy to send a review unit or additional assets if it's a good fit for your coverage."
  ✗ 严禁内部 SKU 代号 (YM24/PK02/FL-JC 等), 严禁 <img>, 严禁中文混杂
  ✗ 产品类型: 这是 Switch 2 无线手柄 (wireless controller / Switch 2 accessory), 充电底座只是随附配件 —
     必须称它为 controller / Switch 2 accessory, 严禁把整个产品叫成 "dock"
  ✗ 严禁在主题或正文出现任何价格 / 报价 / $ 金额

📌 透明度: 说清品牌, 不暗示佣金, 不"求报道"

【媒体编辑】
姓名: {name} | 媒体: {media} ({media_type or '?'}, 集团 {media_group or '?'})
国家: {country_cn or country} | 语言: {lang_display}
报道品类: {', '.join(cats) or '(待补)'}
风格摘要: {(bio or '(无)')[:300]}
最近文章 (仅作上下文, 不要直接引用具体标题):
{(recent or '(无)')[:400]}

【产品】
{p_name} ({p_brand} / {p_cat})
卖点: {p_s1} | {p_s2} | {p_s3}
官网: {p_url} | 目标人群: {p_audience or '游戏玩家'}
{"已有媒体背书 (注意: 已过滤掉收件人自家媒体的话, 安全引用即可): " + p_media_endorse if p_media_endorse else "已有媒体背书: (无, 不要硬编)"}
{news_hook_block}
【匹配亮点】(系统已确认 {total:.0f} 分, 基于以下维度)
{angle_hints}

【署名】{signature}

返回 JSON:
{{
  "email_subject": "主题行(<55字符, **必须**以 '{chosen_prefix['prefix']}' 开头或不带前缀)",
  "email_body": "<p>Dear/Hi {{first_name}},</p><p>开头(用建议的开头模板或语义类似变体)</p><p>新闻角度段</p><p>📎 Press kit: <a href='{p_url}'>...</a></p><p>CTA段</p><p>-- {signature}</p>",
  "highlights": "1句话总结这位编辑与产品的契合点",
  "angle": "建议 pitch 切入角度(英文,1句)"
}}"""

    try:
        r = await deepseek.chat_json(prompt, max_tokens=1000, temperature=0.4)
    except Exception as ex:
        return {"error": f"deepseek: {str(ex)[:100]}"}

    body = r.get("email_body", "")
    ban_phrase_failed = False
    hits = _check_ban_phrases(body)
    if hits:
        retry_prompt = (prompt + "\n\n"
            + f"⚠️ 上次生成命中禁用句式: {hits[:3]} 严禁再用任何"
            + " I read/saw your X article / your latest article/review/piece 等"
            + "假装看过具体作品的句式. 只能用基于「报道品类/媒体名」的概括.")
        try:
            r = await deepseek.chat_json(retry_prompt, max_tokens=1000, temperature=0.4)
            body = r.get("email_body", "")
            hits2 = _check_ban_phrases(body)
            if hits2:
                ban_phrase_failed = True
                print(f"[ban-phrase-editor] 重生后仍命中: {hits2[:3]} → 标人审")
        except Exception as ex:
            ban_phrase_failed = True
            print(f"[ban-phrase-editor] 重生异常: {ex}")

    return {
        "subject": r.get("email_subject", ""),
        "body": body,
        "highlights": r.get("highlights", ""),
        "angle": r.get("angle", ""),
        "ban_phrase_failed": ban_phrase_failed,
        "utm_url": p_url,
        "utm_id": utm_id_value,
    }


# ===== 4. 单媒体人: 本地打分 + 过阈值再生草稿 =====
async def score_and_draft_one(editor_record: dict, product: dict, brand: str,
                                signature: str, threshold: float,
                                expected_report_cats: set, expected_media_types: set) -> dict:
    e = editor_record["fields"]
    name = ext(e.get("媒体人姓名"))
    email = ext(e.get("邮箱"))
    country = ext(e.get("国家"))
    if not email:
        return {"skip": "无邮箱", "editor_record_id": editor_record["record_id"]}

    total, breakdown = score_editor(e, product["fields"], expected_report_cats, expected_media_types)
    lang = ext(e.get("语言")) or "en"

    out = {
        "editor_record_id": editor_record["record_id"],
        "name": name, "email": email, "country": country, "lang": lang,
        "total": total, "breakdown": breakdown,
        "passed": total >= threshold,
    }
    if not out["passed"]:
        return out

    # ── Snov 真邮箱解析 (2026-06-04, 治本替代 {fi}{last}@域名 猜测) ──
    # valid → 用真邮箱(可能纠正) + 标编辑「邮箱验真状态=有效」让域名守卫放行
    # unknown → 用找到的邮箱发(退信由 bounce_monitor 回标, 不增加人工)
    # not_found/unavailable → 降级现状(猜测邮箱 + 域名守卫照常). 纯加法 fail-safe.
    # 幂等省 credit: 编辑已知 有效/无效 → 跳过(无效本就被 auto_send gate 拦; 有效已解析过).
    if config.SNOV_EDITOR_FINDER_ENABLED and "@" in (email or ""):
        cur_verify = ext(e.get("邮箱验真状态"))
        if cur_verify not in ("有效", "无效"):
            domain = email.split("@", 1)[1]
            try:
                sv = await snov.find_email(name, domain)
            except Exception as ex:
                sv = {"status": "unavailable", "email": None, "raw": str(ex)[:80]}
            st_, snov_email = sv.get("status"), sv.get("email")
            out["snov_status"] = st_
            if st_ == "valid" and snov_email:
                out["email"] = snov_email
                # 回填主表: 纠正邮箱 + 标有效(idempotent + 守卫放行 + 下游复用)
                try:
                    upd = {"邮箱验真状态": "有效"}
                    if snov_email.lower() != email.lower():
                        upd["邮箱"] = snov_email
                    await feishu.update_record(config.T_EDITOR, editor_record["record_id"], upd)
                except Exception as ex:
                    print(f"[enrich-editor] Snov 回填失败 {name}: {ex}")
            elif st_ == "unknown" and snov_email:
                # 找到但未验证: 用找到的邮箱发(可能比猜测准), 不改主表 → 域名守卫照常治理
                out["email"] = snov_email
            # not_found / unavailable: 保持猜测邮箱不动

    draft = await gen_pr_draft(editor_record, product, brand, signature, breakdown, total)
    if "error" in draft or "skip" in draft:
        out["error"] = draft.get("error") or draft.get("skip")
        out["passed"] = False
        return out
    out.update({
        "subject": draft["subject"],
        "body": draft["body"],
        "highlights": draft["highlights"],
        "angle": draft["angle"],
        "ban_phrase_failed": draft.get("ban_phrase_failed", False),
        "utm_url": draft.get("utm_url", ""),
        "utm_id": draft.get("utm_id", ""),
    })
    return out


# ===== 5. 写草稿 + 调 router =====
async def write_drafts_and_route(task_rid: str, product_rid: str, brand: str,
                                  sender_alias: str, signature: str,
                                  passed_list: list) -> list:
    now_ms = int(time.time() * 1000)
    results = []
    for s in passed_list:
        if not s.get("passed"): continue
        bk = s["breakdown"]
        send_ms, send_desc = _next_send_time(s.get("country", "US") or "US")
        fields = {
            "邮件草稿ID": f"{task_rid[:8]}-ED-{s['name'][:18]}",
            "关联任务": [task_rid],
            "关联媒体人": [s["editor_record_id"]],
            "关联产品": [product_rid],
            "匹配度总分": s["total"],
            # 6 维分: 4 复用 KOL 字段名 + 2 媒体人专有 (Phase B.0 加的)
            "地区分": bk.get("地区", {}).get("score", 0),
            "语言分": bk.get("语言", {}).get("score", 0),
            "品类分": bk.get("品类", {}).get("score", 0),
            "媒体集团分": bk.get("媒体集团", {}).get("score", 0),
            "触达稳定分": bk.get("触达稳定", {}).get("score", 0),
            "防骚扰分": bk.get("防骚扰", {}).get("score", 0),
            "匹配亮点": (s.get("highlights", "") + " | 维度: " +
                       " / ".join(f"{k}:{v.get('reason','')[:40]}" for k, v in bk.items()))[:500],
            "建议切入点": s.get("angle", "")[:200],
            "收件邮箱": s["email"],
            "邮件主题": s["subject"],
            "邮件正文": s["body"],
            "邮件语言": s["lang"],
            "邮件草稿状态": "待审",
            "邮件草稿来源": "cold",
            "对象类型": "媒体人",
            "发送邮箱": sender_alias,
            "发送人署名": signature,
            "生成时间": now_ms,
            "建议发送时间": send_ms,
            "发送时区说明": send_desc,
            "重生次数": 0,
            "UTM 链接": s.get("utm_url", ""),
        }
        try:
            rid = await feishu.create_record(config.T_DRAFT, fields)
        except Exception as e:
            results.append({"name": s["name"], "error": f"write_draft: {str(e)[:120]}"})
            continue
        # UTM ID 写回媒体人主表 (idempotent)
        utm_id_val = s.get("utm_id", "")
        if utm_id_val:
            try:
                ed_rec = await feishu.get_record(config.T_EDITOR, s["editor_record_id"])
                cur_utm = ext(ed_rec["fields"].get("UTM ID"))
                if not cur_utm:
                    await feishu.update_record(config.T_EDITOR, s["editor_record_id"], {"UTM ID": utm_id_val})
            except Exception as e:
                print(f"[enrich-editor] write editor UTM ID fail rid={s['editor_record_id']}: {e}")
        # ban-phrase 失败 → 跳过 router 走人审
        if s.get("ban_phrase_failed"):
            try:
                await feishu.update_record(config.T_DRAFT, rid, {
                    "审核路径": "需人改",
                    "AI评分理由": "[ban-phrase-editor] 软幻觉重生后仍命中, 人工修正后再发",
                })
            except Exception:
                pass
            results.append({"name": s["name"], "rid": rid, "path": "需人改", "reason": "ban_phrase_failed"})
            continue
        try:
            route = await draft_router.route_draft(rid)
            results.append({"name": s["name"], "rid": rid, "score": route["score"], "path": route["path"]})
        except Exception as e:
            results.append({"name": s["name"], "rid": rid, "router_err": str(e)[:100]})
    return results


# ===== 6. 处理一个任务(主流程) =====
async def enrich_task(task_record: dict, seen_kb: set = None) -> dict:
    task_rid = task_record["record_id"]
    tf = task_record["fields"]
    task_name = ext(tf.get("任务名"))
    brand = ext(tf.get("品牌")) or "FUNLAB"
    threshold = float(tf.get("匹配度阈值") or 75)

    sender_choice = ext(tf.get("发送邮箱"))
    if "fireflyfunlab" in sender_choice or "FUNLAB" in sender_choice:
        sender_alias = "partner@fireflyfunlab.com"
    elif "powkong" in sender_choice or "POWKONG" in sender_choice:
        sender_alias = "partner@powkong.com"
    else:
        sender_alias = config.BRAND_CONFIG[brand]["alias_from"]

    signature = ext(tf.get("发送人署名")) or random.choice(SIGNATURE_POOL.get(brand, ["Frankie"]))

    prod_rid = xrid(tf.get("目标产品"))
    if not prod_rid:
        await feishu.update_record(config.T_TASK_EDITOR, task_rid, {
            "任务状态": "8-已取消", "备注": "未关联产品",
        })
        return {"task": task_name, "error": "无产品", "task_rid": task_rid}

    try:
        product = await feishu.get_record(config.T_PRODUCT, prod_rid)
    except Exception as e:
        await feishu.update_record(config.T_TASK_EDITOR, task_rid, {
            "任务状态": "8-已取消", "备注": f"读产品失败: {str(e)[:80]}",
        })
        return {"task": task_name, "error": f"读产品失败: {e}", "task_rid": task_rid}

    # 任务里设的报道品类 + 媒体类型 → set (传给 score_editor)
    def _to_set(v):
        s = set()
        if not v: return s
        if not isinstance(v, list): v = [v]
        for x in v:
            if isinstance(x, dict): s.add(x.get("text") or x.get("name") or "")
            else: s.add(str(x))
        s.discard("")
        return s
    expected_report_cats = _to_set(tf.get("筛选-报道品类"))
    expected_media_types = _to_set(tf.get("筛选-媒体类型"))

    await feishu.update_record(config.T_TASK_EDITOR, task_rid, {"任务状态": "3-富化中"})

    candidates = await filter_editors(tf, product_rid=prod_rid, brand=brand, seen_kb=seen_kb)
    if not candidates:
        await feishu.update_record(config.T_TASK_EDITOR, task_rid, {
            "任务状态": "7-已完成", "富化候选数": 0, "通过阈值数": 0, "备注": "无候选",
        })
        return {"task": task_name, "candidates": 0, "task_rid": task_rid}

    await feishu.update_record(config.T_TASK_EDITOR, task_rid, {
        "任务状态": "4-生成草稿中", "富化候选数": len(candidates),
    })

    batch_limit = int(tf.get("人数上限") or 30)

    # 第一轮: 本地打分
    scored_local = []
    for ed in candidates:
        total, bk = score_editor(ed["fields"], product["fields"],
                                  expected_report_cats, expected_media_types)
        scored_local.append({"editor": ed, "total": total, "breakdown": bk})

    scored_local.sort(key=lambda x: x["total"], reverse=True)
    top_pass = [x for x in scored_local if x["total"] >= threshold][:batch_limit]

    # 第二轮: 仅过阈值的 DeepSeek 生草稿 (并发 5)
    sem = asyncio.Semaphore(5)
    async def _gated(item):
        async with sem:
            return await score_and_draft_one(
                item["editor"], product, brand, signature, threshold,
                expected_report_cats, expected_media_types,
            )
    scored_raw = await asyncio.gather(
        *[_gated(item) for item in top_pass], return_exceptions=True,
    )
    scored = []
    for s in scored_raw:
        if isinstance(s, Exception): continue
        if isinstance(s, dict) and not s.get("error") and not s.get("skip"):
            scored.append(s)
    passed = [s for s in scored if s.get("passed")]

    routed = await write_drafts_and_route(task_rid, prod_rid, brand, sender_alias, signature, passed)

    # P0 本轮内存去重: 登记本任务实际生成草稿的媒体人(同品牌), 供同 cron 后续任务跳过(防并发重复)
    if seen_kb is not None:
        for s in passed:
            if s.get("passed") and s.get("editor_record_id"):
                seen_kb.add((s["editor_record_id"], brand))

    auto_count = sum(1 for r in routed if r.get("path") == "自动通过")
    human_count = sum(1 for r in routed if r.get("path") in ("待人审", "需人改"))
    retry_count = sum(1 for r in routed if r.get("path") == "退回重生")
    await feishu.update_record(config.T_TASK_EDITOR, task_rid, {
        "任务状态": "5-草稿待审",
        "通过阈值数": len(passed),
        "备注": (f"自动通过 {auto_count} / 待人审 {human_count} / 退回 {retry_count}")[:200],
    })

    return {
        "task": task_name, "task_rid": task_rid,
        "candidates": len(candidates),
        "local_pass": len(top_pass),
        "deepseek_ok": len(scored),
        "passed": len(passed),
        "auto_pass": auto_count,
        "human_review": human_count,
        "retry": retry_count,
    }


# ===== 7. 入口 =====
async def run() -> dict:
    tasks = await find_pending_tasks()
    if not tasks:
        return {"processed": 0, "message": "no pending editor task"}

    results = []
    seen_kb = set()  # P0: 本轮(本次cron)已派的 (editor_record_id, brand), 跨任务共享防并发重复
    for t in tasks:
        try:
            r = await enrich_task(t, seen_kb=seen_kb)
            results.append(r)
        except Exception as e:
            import traceback
            results.append({
                "task_rid": t["record_id"],
                "error": str(e)[:200],
                "trace": traceback.format_exc()[-500:],
            })
    return {"processed": len(results), "results": results}
