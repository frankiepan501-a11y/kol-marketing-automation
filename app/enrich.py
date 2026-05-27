"""阶段 3+5: KOL 营销任务台触发的富化 + 打分 + 生草稿 (v2)

变更:
- 打分本地化(scoring.score_kol),不再让 DeepSeek 算分,确定性 + 省 token
- DeepSeek 只负责生草稿(主题+正文)
- 新6维写入草稿表新字段:地区分/语言分/品类分/粉丝vs客单价分/平台分/防骚扰分

n8n cron 每 5 分钟扫 T_TASK_KOL 任务状态=2-待触发 + 触发=true 的任务,
对每个任务: 读关联产品+映射规则 → 筛 KOL 候选 → 本地打分 → 阈值过 → DeepSeek 生草稿 →
写「KOL·媒体人邮件草稿」 → 逐条调 draft_router 自审 → 更新任务状态。
"""
import re, time, asyncio, random
from . import config, feishu, deepseek, draft_router
from .feishu import ext, xrid
from .scoring import score_kol, _parse_multiselect


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

# V1.5 直控筛选: 任务台「筛选-语言」中文 options → KOL/媒体人主表「语言」字段 ISO 代码
# 双向兼容: 中文 options(运营友好) + ISO 代码(老数据/媒体人端原任务台 ISO options)
LANG_CN_TO_ISO = {
    "英语": "en", "德语": "de", "日语": "ja", "法语": "fr",
    "西班牙语": "es", "葡萄牙语": "pt", "中文": "zh",
    "意大利语": "it", "荷兰语": "nl", "瑞典语": "sv", "其他": "其他",
    "en": "en", "de": "de", "ja": "ja", "fr": "fr",
    "es": "es", "pt": "pt", "zh": "zh", "it": "it",
    "nl": "nl", "sv": "sv",
}

# ===== ban-phrase: 防 LLM 软幻觉(假装看过 KOL 具体作品) =====
# 触发任一 → 重生 1 次, 仍命中 → 标记 _ban_phrase_failed 走人审通道
BAN_PHRASE_PATTERNS = [
    re.compile(r"\bI\s+(saw|watched|caught|loved|enjoyed)\s+(your|the)\b", re.I),
    re.compile(r"\bjust\s+(saw|watched)\b", re.I),
    re.compile(r"\b(been\s+)?following\s+your\s+(channel|content|stream)\b", re.I),
    re.compile(r"\byour\s+(latest|recent|last|new)\s+(video|stream|episode|post|upload|clip)", re.I),
    re.compile(r"\byour\s+(streams?|videos?|episodes?|uploads?|clips?)\b", re.I),
    re.compile(r"\bloved\s+(how|the\s+way)\s+you\b", re.I),
    re.compile(r"\bthat\s+(video|stream|episode|post)\s+you\b", re.I),
]


def _check_ban_phrases(body: str) -> list:
    """返回命中的禁用句式列表(空 list = 干净)"""
    hits = []
    for p in BAN_PHRASE_PATTERNS:
        m = p.search(body or "")
        if m:
            hits.append(m.group(0))
    return hits


SIGNATURE_POOL = {
    "FUNLAB": ["Tom from FUNLAB Team", "Mia @ FUNLAB Outreach", "Alex / FUNLAB Partnership"],
    "POWKONG": ["Lisa @ POWKONG Team", "Ryan from POWKONG", "Jamie / POWKONG Partnership"],
}

# 2026-05-17 A6: 从写死 UTC offset 改成 IANA tz name, 用 zoneinfo 自动处理 DST
# 旧版 US=-5 永远是 EST, 但 3/9-11/2 应该是 EDT (-4) → 5/16 (夏令时) 发件全晚 1h
COUNTRY_TZ_IANA = {
    "US": "America/New_York", "UK": "Europe/London", "DE": "Europe/Berlin",
    "CA": "America/Toronto",  "PH": "Asia/Manila",  "FR": "Europe/Paris",
    "ES": "Europe/Madrid",    "BR": "America/Sao_Paulo", "AU": "Australia/Sydney",
    "NL": "Europe/Amsterdam", "IT": "Europe/Rome",  "MX": "America/Mexico_City",
    "IN": "Asia/Kolkata",     "JP": "Asia/Tokyo",   "TH": "Asia/Bangkok",
    "AE": "Asia/Dubai",       "ID": "Asia/Jakarta", "SE": "Europe/Stockholm",
    "PT": "Europe/Lisbon",
}
APAC = {"JP", "TH", "PH", "ID", "IN", "AE"}

# 映射规则表 ID
T_MAPPING = "tblA63dLsAYTwjT8"


def _next_send_time(country_iso: str):
    """返回 (target_utc_ms, human_desc). DST 自动处理 (Python 3.9+ zoneinfo)."""
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        # Python <3.9 fallback (Zeabur Python 3.11+ 应该有)
        from backports.zoneinfo import ZoneInfo

    tz_name = COUNTRY_TZ_IANA.get(country_iso, "UTC")
    tz = ZoneInfo(tz_name)
    local = datetime.now(tz)
    best_hour = 15 if country_iso in APAC else 10
    target = local.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    while True:
        wd = target.weekday()
        if country_iso in APAC:
            if wd == 3: break  # 周四
        else:
            if wd in (1, 2, 3): break  # 周二/三/四
        target += timedelta(days=1)
        target = target.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    target_utc = target.astimezone(timezone.utc)
    desc = f"{['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][target.weekday()]} {best_hour:02d}:00 {tz_name} ({country_iso})"
    return int(target_utc.timestamp() * 1000), desc


# ===== 1. 任务扫描 =====
async def find_pending_tasks() -> list:
    items = await feishu.search_records(config.T_TASK_KOL, [
        {"field_name": "任务状态", "operator": "is", "value": ["2-待触发"]},
        {"field_name": "触发", "operator": "is", "value": ["true"]},
    ])
    return items


# ===== 2. 读映射规则 =====
async def get_mapping_rules(category: str, hosts: list) -> dict:
    """根据产品的品类+适配主机,读映射规则,返回:
       {expected_kol_styles: set, expected_report_cats: set,
        expected_media_types: set, want_platforms: set}"""
    rules = await feishu.search_records(T_MAPPING, [
        {"field_name": "产品品类", "operator": "is", "value": [category]},
        {"field_name": "是否启用", "operator": "is", "value": ["true"]},
    ])
    if not hosts:
        hosts = ["通用"]
    expected_styles = set()
    expected_report_cats = set()
    expected_media_types = set()
    matched_rules = 0
    for rule in rules:
        f = rule.get("fields", {})
        rule_host = ext(f.get("适配主机"))
        if rule_host in hosts or rule_host == "通用":
            expected_styles |= _parse_multiselect(f.get("KOL内容风格"))
            expected_report_cats |= _parse_multiselect(f.get("媒体人报道品类"))
            expected_media_types |= _parse_multiselect(f.get("媒体人媒体类型"))
            matched_rules += 1
    if matched_rules == 0:
        # 兜底:走通用规则
        for rule in rules:
            f = rule.get("fields", {})
            if ext(f.get("适配主机")) == "通用":
                expected_styles |= _parse_multiselect(f.get("KOL内容风格"))
                expected_report_cats |= _parse_multiselect(f.get("媒体人报道品类"))
                expected_media_types |= _parse_multiselect(f.get("媒体人媒体类型"))
                break
    return {
        "expected_styles": expected_styles,
        "expected_report_cats": expected_report_cats,
        "expected_media_types": expected_media_types,
        "matched_rules": matched_rules,
    }


# ===== 3. KOL 候选筛选 =====
# 草稿状态白名单(可重发) — 已否决 / 发送失败 之外的状态都视作"已锁定 KOL", 跳过
_DRAFT_REUSABLE_STATES = {"已否决", "发送失败"}

# === KOL 资产化 V2 — 品牌感知三层防御 ===
# 跨品牌可派 (新人 + 已合作老朋友推下一款, 任何品牌都通过 Layer 1)
DISPATCHABLE_ANY_BRAND = {"", "未建联", "已合作-免费", "已合作-免费(多次)", "已合作-付费"}
# 永久排除 (拒绝 — 任何品牌都不派)
EXCLUDE_HARD = {"不合适", "黑名单"}
# 同品牌流程中 (含: 待回复 / 洽谈中 / 未产出 — 仅排除产生此状态的品牌, 跨品牌可派)
# 任何不在 DISPATCHABLE_ANY_BRAND 也不在 EXCLUDE_HARD 的状态, 默认按"同品牌流程中"处理

# 7 天同品牌轰炸窗 (跨品牌不算)
RECENT_BLAST_DAYS_SAME_BRAND = 7


def _brand_from_email(email: str) -> str:
    """通过 partner@ 邮箱反推品牌, 用于草稿表的品牌识别."""
    if not email: return ""
    e = str(email).lower()
    if "powkong" in e: return "POWKONG"
    if "funlab" in e or "firefly" in e: return "FUNLAB"
    return ""


async def filter_kols(task_fields: dict, product_rid: str = "", brand: str = "") -> list:
    """按任务条件筛选 KOL — 品牌感知三层防重派单 (KOL 资产化 V2).

    Layer 1 (合作状态品牌感知白名单):
        - 不合适/黑名单 → 任何品牌都排除
        - 未建联/已合作-免费/已合作-免费(多次)/已合作-付费/空 → 任何品牌都通过
        - 待回复/洽谈中/未产出 → 仅排除"产生此状态的品牌" (查最近草稿的发送邮箱反推)

    Layer 2 (同 KOL × 同产品永久去重): T_DRAFT 同产品已存在非否决/失败草稿 → 永久排除.
        保证老朋友不会被重推已合作过的产品.

    Layer 3 (7 天同品牌轰炸窗): T_DRAFT 同品牌任意产品 7 天内已派 → 排除.
        跨品牌可派 (POWKONG 派完 7 天内, FUNLAB 仍可派同 KOL).
    """
    import time as _time
    platforms_want = task_fields.get("筛选-平台") or []
    countries_want = task_fields.get("筛选-国家") or []
    styles_want = task_fields.get("筛选-内容风格") or []
    langs_want_raw = task_fields.get("筛选-语言") or []
    if not isinstance(platforms_want, list): platforms_want = [platforms_want]
    if not isinstance(countries_want, list): countries_want = [countries_want]
    if not isinstance(styles_want, list): styles_want = [styles_want]
    if not isinstance(langs_want_raw, list): langs_want_raw = [langs_want_raw]
    langs_want = {LANG_CN_TO_ISO.get(l, l) for l in langs_want_raw}

    f_min = int(task_fields.get("筛选-粉丝下限") or 0)
    f_max = int(task_fields.get("筛选-粉丝上限") or 10_000_000)
    batch_limit = int(task_fields.get("批量大小") or 50)
    hard_pool = max(batch_limit * 5, 200)

    # === 一次性拉全量 KOL 草稿 (Layer 1/2/3 共享, 避免多次 API) ===
    all_kol_drafts = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "对象类型", "operator": "is", "value": ["KOL"]},
    ])

    # Layer 1 数据准备: 每个 KOL 最近一次"非否决/失败"草稿 → 反推该 KOL 当前流程中是哪个品牌
    # 用于"待回复/洽谈中/未产出"状态时, 决定是否同品牌
    kol_active_brand = {}  # kol_rid → brand (POWKONG / FUNLAB / "")
    kol_latest_ms = {}
    for d in all_kol_drafts:
        f = d.get("fields", {})
        if ext(f.get("邮件草稿状态")) in _DRAFT_REUSABLE_STATES: continue
        kol_rid = xrid(f.get("关联KOL"))
        if not kol_rid: continue
        gen_ms = f.get("生成时间", 0) or 0
        if not isinstance(gen_ms, (int, float)): gen_ms = 0
        if kol_latest_ms.get(kol_rid, 0) < gen_ms:
            kol_latest_ms[kol_rid] = gen_ms
            kol_active_brand[kol_rid] = _brand_from_email(ext(f.get("发送邮箱")))

    # Layer 2: 同产品已存在草稿的 KOL → 永久排除
    exclude_kol_ids = set()
    if product_rid:
        for d in all_kol_drafts:
            f = d.get("fields", {})
            if ext(f.get("邮件草稿状态")) in _DRAFT_REUSABLE_STATES: continue
            # 检查关联产品是否包含 product_rid
            link_products = f.get("关联产品") or {}
            link_ids = link_products.get("link_record_ids") or [] if isinstance(link_products, dict) else []
            if product_rid not in link_ids: continue
            kol_rid = xrid(f.get("关联KOL"))
            if kol_rid: exclude_kol_ids.add(kol_rid)

    # Layer 3: 7 天内同品牌任意产品已派的 KOL → 排除 (跨品牌不算)
    recent_blasted_same_brand = set()
    cutoff_ms = int(_time.time() * 1000) - RECENT_BLAST_DAYS_SAME_BRAND * 86400 * 1000
    for d in all_kol_drafts:
        f = d.get("fields", {})
        if ext(f.get("邮件草稿状态")) in _DRAFT_REUSABLE_STATES: continue
        gen_ms = f.get("生成时间")
        if not isinstance(gen_ms, (int, float)) or gen_ms < cutoff_ms: continue
        draft_brand = _brand_from_email(ext(f.get("发送邮箱")))
        if brand and draft_brand and draft_brand != brand: continue  # 跨品牌不算轰炸
        kol_rid = xrid(f.get("关联KOL"))
        if kol_rid: recent_blasted_same_brand.add(kol_rid)

    # 池: 邮箱有值 (放开 合作状态 严格 filter, 改 in-loop check)
    items = await feishu.search_records(config.T_KOL, [
        {"field_name": "邮箱", "operator": "isNotEmpty", "value": []},
    ])
    pool_total = len(items)

    hits = []
    skipped_status, skipped_dedup, skipped_blast = 0, 0, 0
    for rec in items:
        f = rec.get("fields", {})
        rid = rec["record_id"]
        coop = ext(f.get("合作状态"))
        # Layer 1 (品牌感知):
        if coop in EXCLUDE_HARD:
            skipped_status += 1; continue
        if coop not in DISPATCHABLE_ANY_BRAND:
            # 流程中状态 (待回复/洽谈中/未产出) → 仅同品牌排除
            active_brand = kol_active_brand.get(rid, "")
            if active_brand and brand and active_brand == brand:
                skipped_status += 1; continue
            # 跨品牌或查不到品牌, 放行
        # Layer 2: 同 KOL × 同产品 永久不重派
        if rid in exclude_kol_ids:
            skipped_dedup += 1; continue
        # Layer 3: 7 天内同 KOL 同品牌任意产品已派 → 跳过 (跨品牌不算)
        if rid in recent_blasted_same_brand:
            skipped_blast += 1; continue
        if platforms_want:
            mp = ext(f.get("主平台"))
            if mp not in platforms_want: continue
        if countries_want:
            country = ext(f.get("国家"))
            if country not in countries_want: continue
        if langs_want:
            kol_lang = ext(f.get("语言"))
            if kol_lang not in langs_want: continue
        sub = f.get("粉丝数", 0) or 0
        try: sub = int(sub)
        except (ValueError, TypeError): sub = 0
        if sub < f_min or sub > f_max: continue
        if styles_want:
            styles_list = list(_parse_multiselect(f.get("内容风格")))
            if not any(s in styles_list for s in styles_want): continue
        hits.append(rec)
        if len(hits) >= hard_pool: break

    print(f"[enrich V2 品牌感知] filter: pool={pool_total} → hits={len(hits)} "
          f"brand={brand} (L1同品牌流程中/拒绝={skipped_status}, L2同产品永久={skipped_dedup}, "
          f"L3同品牌7天轰炸={skipped_blast}, countries={countries_want}, langs={list(langs_want)})")

    return hits[:hard_pool]


# ===== 4. DeepSeek 仅生草稿(打分本地完成) =====
async def gen_draft(kol_record: dict, product: dict, brand: str,
                    signature: str, breakdown: dict, total: float) -> dict:
    k = kol_record["fields"]
    kol_name = ext(k.get("账号名"))
    kol_country = ext(k.get("国家"))
    kol_country_cn = ext(k.get("国家原文"))
    kol_sub = k.get("粉丝数", 0) or 0
    kol_styles = ext(k.get("内容风格"))
    kol_ip = ext(k.get("IP喜好"))
    # 2026-05-16: 清洗 multi-email / "dm" / "待补" 等异常邮箱
    kol_email, _email_reason = feishu.clean_email(ext(k.get("邮箱")))
    kol_url = ext(k.get("主链接"))
    if not kol_email:
        return {"skip": f"无有效邮箱: {_email_reason}"}

    pf = product["fields"]
    # 海外营销邮件优先用「产品英文名」, 缺则降级中文剥前缀
    p_en = ext(pf.get("产品英文名"))
    if p_en:
        p_name = p_en
    else:
        p_name_raw = ext(pf.get("产品名"))
        p_name = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', p_name_raw).strip() or p_name_raw
        print(f"[WARN] 产品缺少「产品英文名」字段, 降级用中文剥前缀: {p_name}")
    p_brand = ext(pf.get("品牌"))
    p_cat = ext(pf.get("品类"))
    p_s1 = ext(pf.get("卖点1"))
    p_s2 = ext(pf.get("卖点2"))
    p_s3 = ext(pf.get("卖点3"))
    p_url_raw = feishu.ext_url(pf.get("官网链接"))   # ext_url 取 link 不取 text(防中文标签被当链接)
    p_price = pf.get("报价(USD)", 0)
    p_audience = ext(pf.get("目标人群"))
    p_media = ext(pf.get("媒体报道"))

    # UTM 注入 (Phase 1 ROI 闭环): 给产品链接加 utm_source/medium/campaign/content
    from . import utm as _utm
    p_url = _utm.make_utm_link(p_url_raw, brand, p_name, kol_name)
    utm_id_value = _utm.kol_utm_id(kol_name)

    lang = COUNTRY_TO_LANG.get(kol_country, "en")
    lang_display = LANG_DISPLAY.get(lang, "English")

    # 从 breakdown 抽出亮点(高分维度)
    high_dims = sorted(breakdown.items(), key=lambda x: x[1]["score"], reverse=True)[:3]
    angle_hints = " / ".join(f"{k}:{v['reason']}" for k, v in high_dims)

    prompt = f"""你是一个专业的海外 KOL 外联邮件撰稿人。请按以下 2026 年最新业内最佳实践生成邮件。

【2026 KOL 外联邮件黄金法则】(必须遵守)

📌 主题行 (<40 字符 / <=7 词): 像朋友 DM, 不像营销文案
  ✗ 禁: "partnership"/"collaboration"/"opportunity"/"your thoughts"/"quick question"
  ✗ 禁: 通用模板主题(如 "Piranha Plant dock for your retro setup")— 任何不带KOL名字或KOL专属关键词的主题都属于"通用模板"
  ✅ 必须包含 KOL 名字(从账号名 {kol_name} 抽取) OR KOL 频道/IP 喜好的专属关键词
  ✓ 例:
    - "{kol_name}, this fits your retro corner"
    - "{kol_name}'s Switch 2 needs this dock"
    - "For your GameCube nostalgia setup"  (有具体场景,非通用)

📌 正文 (100-150 词):
  ✅ 必须以 "Hey {{KOL名字}}," 开头(从账号名提取)— 不允许 "Hey," / "Hi there," / 任何匿名开头
  ✓ 第 1 句引用 KOL 内容方向(基于下方 IP喜好/风格 标签), 禁 [xxx 占位符]
  🚨 严禁编造具体作品/视频/直播 — 我们没真看过 KOL 任何具体内容,只有抽象标签
    ✗ 禁 "I saw your X video" / "I watched your latest stream" / "Your last episode about Y"
    ✗ 禁 任何"我看了你 [具体作品]"句式 — 这是 LLM 幻觉, 客户一眼识破
    ✓ 用 "Saw you're into {{IP喜好}} content" / "Your {{风格}} channel caught my eye"
    ✓ 用 "Your retro-gaming corner has serious vibes" (基于风格标签的概括)
    ✓ 模糊但真实 > 具体但编造 — 宁可泛泛而谈,也不要假装看过
  ✓ 中段强调"为什么契合他"(参考下方匹配亮点)
  ✓ 1 行产品链接 (独立段落): <p>👉 <a href="{p_url}">{{See it in action →}}</a></p>
     - en: "See it in action →"  / de: "Sieh es live →"  / fr: "À voir en action →"
     - es: "Míralo en acción →"  / pt: "Veja em ação →"   / ja: "実物を見る →"
     - it: "Guardalo in azione →" / nl: "Zie het in actie →"
  ✓ CTA 开放式: "Would you be curious to try one? Happy to send it over, no strings attached."
  ✗ 严禁内部 SKU 代号 (YM24/PK02/FL-JC 等), 严禁 <img>, 严禁中文混杂

📌 透明度: 说清品牌/产品, 不承诺佣金, 不推销腔
📌 语言: 全文 {lang_display} (KOL 国家: {kol_country_cn or kol_country})

【KOL】
账号: {kol_name} | 国家: {kol_country_cn or kol_country}
粉丝: {kol_sub:,} | 风格: {kol_styles} | IP喜好: {kol_ip}
主页: {kol_url}

【产品】
{p_name} ({p_brand} / {p_cat}) | 报价: ${p_price} USD
卖点: {p_s1} | {p_s2} | {p_s3}
官网: {p_url} | 目标人群: {p_audience}
媒体背书: {p_media or '(无)'}

【匹配亮点】(系统已确认 {total:.0f} 分,基于以下维度)
{angle_hints}

【署名】{signature}

返回 JSON:
{{
  "email_subject": "主题",
  "email_body": "<p>开头</p><p>中段</p><p>CTA段</p><p>-- {signature}</p>",
  "highlights": "1句话总结这位 KOL 与产品的契合点",
  "angle": "建议切入角度(英文,1句)"
}}"""

    try:
        r = await deepseek.chat_json(prompt, max_tokens=1000, temperature=0.4)
    except Exception as e:
        return {"error": f"deepseek: {str(e)[:100]}"}

    body = r.get("email_body", "")
    ban_phrase_failed = False
    hits = _check_ban_phrases(body)
    if hits:
        # 重生 1 次, 在原 prompt 后面追加 KOL_NAME + 上次命中片段警告
        retry_prompt = (
            prompt
            + "\n\n"
            + f"⚠️ 上次生成命中禁用句式: {hits[:3]} 严禁再用任何"
            + "I saw/watched your X / your latest/recent/last video/stream / your streams/videos 等"
            + "假装看过具体作品的句式。只能用基于 IP喜好/风格 的概括(如 'your retro-gaming corner' / "
            + "'Saw you're into PC gaming content')。"
        )
        try:
            r = await deepseek.chat_json(retry_prompt, max_tokens=1000, temperature=0.4)
            body = r.get("email_body", "")
            hits2 = _check_ban_phrases(body)
            if hits2:
                ban_phrase_failed = True
                print(f"[ban-phrase] 重生后仍命中: {hits2[:3]} → 标记需人审")
            else:
                print(f"[ban-phrase] 首生命中 {hits[:3]}, 重生后干净")
        except Exception as e:
            ban_phrase_failed = True
            print(f"[ban-phrase] 重生异常: {e}, 标记需人审")

    return {
        "subject": r.get("email_subject", ""),
        "body": body,
        "highlights": r.get("highlights", ""),
        "angle": r.get("angle", ""),
        "ban_phrase_failed": ban_phrase_failed,
        "utm_url": p_url,            # Phase 1: 实发产品链接 (含 UTM)
        "utm_id": utm_id_value,      # Phase 1: KOL UTM ID (= utm_content)
    }


# ===== 5. 单 KOL: 本地打分 + 过阈值再生草稿 =====
async def score_and_draft_one(kol_record: dict, product: dict, brand: str,
                                signature: str, threshold: float,
                                expected_styles: set, want_platforms: set) -> dict:
    k = kol_record["fields"]
    kol_name = ext(k.get("账号名"))
    # 2026-05-16: 清洗 multi-email / 异常邮箱
    kol_email, _email_reason = feishu.clean_email(ext(k.get("邮箱")))
    kol_country = ext(k.get("国家"))
    if not kol_email:
        return {"skip": f"无有效邮箱: {_email_reason}", "kol_record_id": kol_record["record_id"]}

    # 本地打分
    total, breakdown = score_kol(k, product["fields"], expected_styles, want_platforms)
    lang = COUNTRY_TO_LANG.get(kol_country, "en")

    out = {
        "kol_record_id": kol_record["record_id"],
        "kol_name": kol_name,
        "kol_email": kol_email,
        "kol_country": kol_country,
        "lang": lang,
        "total": total,
        "breakdown": breakdown,
        "passed": total >= threshold,
    }
    if not out["passed"]:
        return out

    # 过阈值才调 DeepSeek 生草稿
    draft = await gen_draft(kol_record, product, brand, signature, breakdown, total)
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


# ===== 6. 写草稿 + 调 router =====
async def write_drafts_and_route(task_rid: str, product_rid: str, brand: str,
                                  sender_alias: str, signature: str,
                                  passed_list: list) -> list:
    now_ms = int(time.time() * 1000)
    results = []
    for s in passed_list:
        if not s.get("passed"): continue
        bk = s["breakdown"]
        send_ms, send_desc = _next_send_time(s.get("kol_country", "US") or "US")
        fields = {
            "邮件草稿ID": f"{task_rid[:8]}-{s['kol_name'][:20]}",
            "关联任务": [task_rid],
            "关联KOL": [s["kol_record_id"]],
            "关联产品": [product_rid],
            "匹配度总分": s["total"],
            # 新 6 维分(对应 scoring.score_kol 输出)
            "地区分": bk.get("地区", {}).get("score", 0),
            "语言分": bk.get("语言", {}).get("score", 0),
            "品类分": bk.get("品类", {}).get("score", 0),
            "粉丝vs客单价分": bk.get("粉丝vs客单价", {}).get("score", 0),
            "平台分": bk.get("平台", {}).get("score", 0),
            "防骚扰分": bk.get("防骚扰", {}).get("score", 0),
            "匹配亮点": (s.get("highlights", "") + " | 维度: " +
                       " / ".join(f"{k}:{v.get('reason','')[:40]}" for k, v in bk.items()))[:500],
            "建议切入点": s.get("angle", "")[:200],
            "收件邮箱": s["kol_email"],
            "邮件主题": s["subject"],
            "邮件正文": s["body"],
            "邮件语言": s["lang"],
            "邮件草稿状态": "待审",
            "邮件草稿来源": "cold",
            "对象类型": "KOL",
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
            results.append({"kol": s["kol_name"], "error": f"write_draft: {str(e)[:100]}"})
            continue
        # Phase 1 ROI: 第一次给 KOL 派单时, 写 UTM ID 到 KOL 主表 (idempotent)
        utm_id_val = s.get("utm_id", "")
        if utm_id_val:
            try:
                kol_rec = await feishu.get_record(config.T_KOL, s["kol_record_id"])
                cur_utm = ext(kol_rec["fields"].get("UTM ID"))
                if not cur_utm:
                    await feishu.update_record(config.T_KOL, s["kol_record_id"], {"UTM ID": utm_id_val})
            except Exception as e:
                print(f"[enrich] write KOL UTM ID fail rid={s['kol_record_id']}: {e}")
        # ban-phrase 失败 → 跳过 auto router, 强制走人审通道
        if s.get("ban_phrase_failed"):
            try:
                await feishu.update_record(config.T_DRAFT, rid, {
                    "审核路径": "需人改",
                    "AI评分理由": "[ban-phrase] 软幻觉重生后仍命中, 人工修正后再发",
                })
            except Exception:
                pass
            results.append({"kol": s["kol_name"], "rid": rid, "path": "需人改", "reason": "ban_phrase_failed"})
            continue
        try:
            route = await draft_router.route_draft(rid)
            results.append({"kol": s["kol_name"], "rid": rid, "score": route["score"], "path": route["path"]})
        except Exception as e:
            results.append({"kol": s["kol_name"], "rid": rid, "router_err": str(e)[:100]})
    return results


# ===== 7. 处理一个任务(主流程) =====
async def enrich_task(task_record: dict) -> dict:
    task_rid = task_record["record_id"]
    tf = task_record["fields"]
    task_name = ext(tf.get("任务名"))
    brand = ext(tf.get("品牌")) or "FUNLAB"
    threshold = float(tf.get("匹配度阈值") or 80)
    batch_limit = int(tf.get("批量大小") or 50)

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
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "8-已取消", "备注": "未关联产品",
        })
        return {"task": task_name, "error": "无产品", "task_rid": task_rid}

    try:
        product = await feishu.get_record(config.T_PRODUCT, prod_rid)
    except Exception as e:
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "8-已取消", "备注": f"读产品失败: {str(e)[:80]}",
        })
        return {"task": task_name, "error": f"读产品失败: {e}", "task_rid": task_rid}

    # 2026-05-26 Bug B (戴夫派单事故): 产品无「官网链接」→ cold 会带死链 href="" (邮件铁律禁死链)
    # → 跳过整任务 + 告警, 零外发. 请先在产品库填官网链接再重跑.
    if not feishu.ext_url(product["fields"].get("官网链接")):
        p_disp = ext(product["fields"].get("产品英文名")) or ext(product["fields"].get("产品名"))
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "8-已取消",
            "备注": "产品缺「官网链接」→ cold 会带死链, 已跳过派单(零外发). 填官网链接后重跑.",
        })
        try:
            await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, {
                "header": {"template": "red", "title": {"tag": "plain_text",
                    "content": "🚨 KOL 派单跳过 — 产品缺官网链接(防死链)"}},
                "elements": [{"tag": "div", "text": {"tag": "lark_md",
                    "content": f"**任务**: {task_name}\n**产品**: {p_disp}\n**原因**: 产品库「官网链接」为空 → cold 邮件会带死链, 已跳过整批派单(零外发). 请先填官网链接再重跑。"}}],
            }, biz="KOL")
        except Exception:
            pass
        return {"task": task_name, "error": "产品缺官网链接,跳过防死链", "task_rid": task_rid}

    # 读映射规则
    p_cat = ext(product["fields"].get("品类"))
    p_hosts = list(_parse_multiselect(product["fields"].get("适配主机")))
    mapping = await get_mapping_rules(p_cat, p_hosts)

    # 任务的筛选-平台 → set
    want_platforms = set()
    plats = tf.get("筛选-平台") or []
    if not isinstance(plats, list): plats = [plats]
    for p in plats:
        if isinstance(p, dict): want_platforms.add(p.get("text") or p.get("name") or "")
        else: want_platforms.add(str(p))

    await feishu.update_record(config.T_TASK_KOL, task_rid, {"任务状态": "3-富化中"})

    candidates = await filter_kols(tf, product_rid=prod_rid, brand=brand)
    if not candidates:
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "7-已完成", "富化候选数": 0, "通过阈值数": 0, "备注": "无候选",
        })
        return {"task": task_name, "candidates": 0, "task_rid": task_rid}

    await feishu.update_record(config.T_TASK_KOL, task_rid, {
        "任务状态": "4-生成草稿中", "富化候选数": len(candidates),
    })

    # 第一轮:本地打分(不调 DeepSeek)
    scored_local = []
    for kol in candidates:
        total, bk = score_kol(kol["fields"], product["fields"],
                                mapping["expected_styles"], want_platforms)
        scored_local.append({"kol": kol, "total": total, "breakdown": bk})

    # 排序取 batch_limit 内通过阈值的
    scored_local.sort(key=lambda x: x["total"], reverse=True)
    top_pass = [x for x in scored_local if x["total"] >= threshold][:batch_limit]

    # 第二轮:DeepSeek 仅对过阈值的生草稿
    sem = asyncio.Semaphore(5)
    async def _gated(item):
        async with sem:
            kol = item["kol"]
            return await score_and_draft_one(
                kol, product, brand, signature, threshold,
                mapping["expected_styles"], want_platforms,
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

    # 写草稿 + 调 router
    routed = await write_drafts_and_route(task_rid, prod_rid, brand, sender_alias, signature, passed)

    auto_count = sum(1 for r in routed if r.get("path") == "自动通过")
    human_count = sum(1 for r in routed if r.get("path") in ("待人审", "需人改"))
    retry_count = sum(1 for r in routed if r.get("path") == "退回重生")
    await feishu.update_record(config.T_TASK_KOL, task_rid, {
        "任务状态": "5-草稿待审",
        "通过阈值数": len(passed),
        "备注": (f"映射规则{mapping['matched_rules']} / 自动通过 {auto_count} / 待人审 {human_count} / 退回 {retry_count}")[:200],
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


# ===== 8. 入口 =====
async def run() -> dict:
    tasks = await find_pending_tasks()
    if not tasks:
        return {"processed": 0, "message": "no pending KOL task"}

    results = []
    for t in tasks:
        try:
            r = await enrich_task(t)
            results.append(r)
        except Exception as e:
            import traceback
            results.append({
                "task_rid": t["record_id"],
                "error": str(e)[:200],
                "trace": traceback.format_exc()[-500:],
            })
    return {"processed": len(results), "results": results}
