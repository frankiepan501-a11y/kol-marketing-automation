# -*- coding: utf-8 -*-
"""UTM 链接生成 helper — Phase 1 ROI 数据闭环

设计:
- utm_source = kol (固定, 区别于 paid/seo/direct)
- utm_medium = email (固定)
- utm_campaign = {brand_lower}_{product_en_slug}  例: powkong_piranha_plant_switch_2_dock
- utm_content = kol_{kol_handle_slug}             例: kol_ashtvn

KOL 主表「UTM ID」字段 = kol_{kol_handle_slug}, 与 utm_content 一致, 方便归因时一键 join。
"""
import re
from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse


def slugify(s: str) -> str:
    """URL-safe slug: lowercase + 非字母数字替换 _ + 去重 _."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def kol_utm_id(kol_handle: str) -> str:
    """KOL handle → UTM ID (= utm_content). 例: 'Ashtvn' → 'kol_ashtvn'"""
    return f"kol_{slugify(kol_handle)}"


def campaign_id(brand: str, product_en: str) -> str:
    """Brand + product → utm_campaign. 例: ('POWKONG','Piranha Plant Switch 2 Dock') → 'powkong_piranha_plant_switch_2_dock'"""
    return f"{slugify(brand)}_{slugify(product_en)}"


def make_utm_link(product_url: str, brand: str, product_en: str, kol_handle: str) -> str:
    """给 product_url 添加 UTM 参数, 保留原 querystring + fragment.

    返回带 UTM 的完整 URL; 如果 product_url 为空, 返回空字符串.
    """
    if not product_url:
        return ""
    try:
        parsed = urlparse(product_url)
    except Exception:
        return product_url

    existing = dict(parse_qsl(parsed.query, keep_blank_values=True))
    existing.update({
        "utm_source": "kol",
        "utm_medium": "email",
        "utm_campaign": campaign_id(brand, product_en),
        "utm_content": kol_utm_id(kol_handle),
    })
    new_query = urlencode(existing, doseq=True)
    return urlunparse(parsed._replace(query=new_query))
