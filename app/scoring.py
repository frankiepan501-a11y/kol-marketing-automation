"""6 维确定性评分函数 (派单 v2 — 配置驱动, 不调 DeepSeek)

权重:
  地区匹配 25 / 语言匹配 25 / 品类匹配 20 /
  粉丝量级 vs 客单价 10 / 平台匹配 10 / 历史防骚扰 10
"""
from typing import Tuple
from .feishu import ext


PRIMARY_COUNTRIES = {"US", "UK", "DE", "CA", "FR", "ES", "BR", "PT",
                     "AU", "NL", "IT", "MX", "JP"}
APAC_OTHER = {"PH", "TH", "ID", "IN", "AE"}
PRIMARY_LANGS = {"de", "fr", "es", "pt", "ja", "it", "nl", "sv"}


def _parse_multiselect(v):
    """飞书 MultiSelect 字段返回 list[str|dict|{text,name}],统一拆成 set[str]"""
    if not v:
        return set()
    if isinstance(v, str):
        return {v}
    if isinstance(v, dict):
        return {v.get("text") or v.get("name") or ""}
    out = set()
    for x in v:
        if isinstance(x, dict):
            n = x.get("text") or x.get("name")
            if n:
                out.add(n)
        elif isinstance(x, str):
            out.add(x)
    out.discard("")
    return out


def score_region(country_code: str) -> Tuple[float, str]:
    if country_code in PRIMARY_COUNTRIES:
        return 25, f"主销国 {country_code}"
    if country_code in APAC_OTHER:
        return 15, f"APAC {country_code}"
    if country_code in ("其他", "未知", "", None):
        return 5, "未知国家"
    return 10, country_code


def score_language(lang_code: str) -> Tuple[float, str]:
    if lang_code == "en":
        return 25, "英语母语 (全球通用)"
    if lang_code in PRIMARY_LANGS:
        return 18, f"主流语种 {lang_code}"
    if lang_code == "zh":
        return 5, "中文 (海外不通)"
    return 10, lang_code or "未知"


def score_category(kol_styles: set, expected_styles: set) -> Tuple[float, str]:
    """品类匹配 = KOL内容风格 ∩ 映射表期望风格"""
    if not expected_styles:
        return 10, "无映射规则,兜底 10 分"
    overlap = kol_styles & expected_styles
    n = len(overlap)
    if n >= 3:
        return 20, f"高度匹配({n}): {','.join(list(overlap)[:3])}"
    if n == 2:
        return 14, f"较匹配({n}): {','.join(overlap)}"
    if n == 1:
        return 8, f"部分匹配: {next(iter(overlap))}"
    return 0, "不匹配"


def score_fans_price(subs: int, price_usd: float) -> Tuple[float, str]:
    """粉丝量级 vs 产品客单价"""
    if not subs or subs <= 0:
        return 0, "无粉丝数据"
    if price_usd <= 30:
        ideal_min, ideal_max = 5_000, 100_000
        zone = "$≤30 / 1万-10万"
    elif price_usd <= 100:
        ideal_min, ideal_max = 50_000, 1_000_000
        zone = "$30-100 / 5万-100万"
    else:
        ideal_min, ideal_max = 500_000, 10_000_000
        zone = "$>100 / 50万+"
    if ideal_min <= subs <= ideal_max:
        return 10, f"匹配 {zone}"
    # 偏差扣分
    if subs < ideal_min:
        ratio = subs / ideal_min if ideal_min else 0
        return max(0, round(10 * ratio, 1)), f"粉丝偏少 ({subs:,} vs {zone})"
    return 5, f"粉丝偏多 ({subs:,} vs {zone},非最优)"


def score_platform(kol_platform: str, want_platforms: set) -> Tuple[float, str]:
    if not want_platforms:
        return 10, "无平台限制"
    if kol_platform in want_platforms:
        return 10, f"主战场命中 {kol_platform}"
    return 5, f"非主战场 ({kol_platform} not in {want_platforms})"


def score_anti_spam(kol_fields: dict) -> Tuple[float, str]:
    """半年内 0 次接触 = 10 / 1 次 = 5 / >=2 次 = 0
    简化: 用合作状态判断,后续如需精确可查跟进记录表"""
    coop = ext(kol_fields.get("合作状态"))
    if coop in ("未建联", "", None):
        return 10, "首次接触"
    if coop in ("待回复",):
        return 5, "已发过开发信,谨慎重发"
    return 0, f"已在 {coop} 流程,不重复 cold"


def score_kol(kol_fields: dict, product_fields: dict,
              expected_styles: set, want_platforms: set) -> Tuple[float, dict]:
    """KOL 综合 6 维打分,返回 (total, breakdown)"""
    country = ext(kol_fields.get("国家"))
    lang = ext(kol_fields.get("语言"))
    kol_styles = _parse_multiselect(kol_fields.get("内容风格"))
    try:
        subs = int(kol_fields.get("粉丝数", 0) or 0)
    except (ValueError, TypeError):
        subs = 0
    try:
        price = float(product_fields.get("报价(USD)", 0) or 0)
    except (ValueError, TypeError):
        price = 0
    platform = ext(kol_fields.get("主平台"))

    d_region, r_region = score_region(country)
    d_lang, r_lang = score_language(lang)
    d_cat, r_cat = score_category(kol_styles, expected_styles)
    d_fans, r_fans = score_fans_price(subs, price)
    d_plat, r_plat = score_platform(platform, want_platforms)
    d_anti, r_anti = score_anti_spam(kol_fields)

    total = d_region + d_lang + d_cat + d_fans + d_plat + d_anti
    return total, {
        "地区": {"score": d_region, "reason": r_region},
        "语言": {"score": d_lang, "reason": r_lang},
        "品类": {"score": d_cat, "reason": r_cat},
        "粉丝vs客单价": {"score": d_fans, "reason": r_fans},
        "平台": {"score": d_plat, "reason": r_plat},
        "防骚扰": {"score": d_anti, "reason": r_anti},
    }


def score_editor(editor_fields: dict, product_fields: dict,
                 expected_report_cats: set, expected_media_types: set) -> Tuple[float, dict]:
    """媒体人版评分:
    地区 25 / 语言 25 / 品类(报道品类+媒体类型综合) 20 /
    媒体集团权重 10 / 主链接稳定性 10 / 防骚扰 10
    """
    country = ext(editor_fields.get("国家"))
    lang = ext(editor_fields.get("语言"))
    report_cats = _parse_multiselect(editor_fields.get("报道品类"))
    media_type = ext(editor_fields.get("媒体类型"))
    media_group = ext(editor_fields.get("媒体集团"))

    d_region, r_region = score_region(country)
    d_lang, r_lang = score_language(lang)

    # 品类:报道品类交集 + 媒体类型加成
    cat_overlap = report_cats & expected_report_cats
    if len(cat_overlap) >= 3:
        d_cat = 20
    elif len(cat_overlap) == 2:
        d_cat = 14
    elif len(cat_overlap) == 1:
        d_cat = 8
    else:
        d_cat = 0
    if media_type in expected_media_types:
        d_cat = min(20, d_cat + 4)
    r_cat = f"报道品类匹配{len(cat_overlap)}/媒体类型{media_type}"

    # 媒体集团权重 (粗略: 头部 10 / 其他 6)
    TOP_GROUPS = {"IGN Entertainment", "Vox Media", "Future", "Condé Nast",
                  "Penske", "Yahoo", "Red Ventures", "NYT"}
    if media_group in TOP_GROUPS:
        d_grp, r_grp = 10, f"头部集团 {media_group}"
    elif media_group in ("独立", "其他", "", None):
        d_grp, r_grp = 6, "独立/未知集团"
    else:
        d_grp, r_grp = 8, media_group

    # 触达稳定性: 有作者主页 + 邮箱已验证 = 10
    has_url = bool(ext(editor_fields.get("作者主页URL")) or ext(editor_fields.get("主链接")))
    email_status = ext(editor_fields.get("邮箱验真状态"))
    if has_url and email_status == "有效":
        d_reach, r_reach = 10, "作者页+邮箱有效"
    elif has_url or email_status == "有效":
        d_reach, r_reach = 7, "部分触达信号"
    elif email_status == "未验":
        d_reach, r_reach = 5, "邮箱未验证"
    else:
        d_reach, r_reach = 3, "弱触达"

    # 防骚扰
    coop = ext(editor_fields.get("合作状态"))
    if coop in ("未建联", "", None):
        d_anti, r_anti = 10, "首次接触"
    elif coop == "建联中":
        d_anti, r_anti = 5, "建联中,谨慎"
    else:
        d_anti, r_anti = 0, f"已在 {coop} 流程"

    total = d_region + d_lang + d_cat + d_grp + d_reach + d_anti
    return total, {
        "地区": {"score": d_region, "reason": r_region},
        "语言": {"score": d_lang, "reason": r_lang},
        "品类": {"score": d_cat, "reason": r_cat},
        "媒体集团": {"score": d_grp, "reason": r_grp},
        "触达稳定": {"score": d_reach, "reason": r_reach},
        "防骚扰": {"score": d_anti, "reason": r_anti},
    }
