"""View model 构造层. 把 collected raw dict 转成 jinja2 模板用的字典.

设计原则:
- 核心 sections (01 总览 / 02 PK GA4 / 03 PK Meta / 04 PK SEO / 06 FL GA4 / 07 FL SEO)
  用 collected 真实数据驱动
- 其他 sections (02.5/02.6/03.5/05/06.5+/08/09/10/11/12) 在 jinja2 模板里硬编码占位,
  view model 仅提供 ai_insights[section_id] 给底部 callout 嵌入
- 派生指标 (占比 / 达成率 / 客单价) 在这里算, 模板只负责渲染
- 缺数据时返回带 _missing=True 标记的 dict, 模板用条件渲染显示「⚠️ 数据缺口」

输出 dict 结构见各 _shape_xxx 函数的 docstring.
"""
import datetime
import logging
import re

log = logging.getLogger("weekly_report.data_shaper")


# ============ 格式化工具 ============

def fmt_int(n) -> str:
    """1234 -> "1,234" """
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "—"


def fmt_money(n, prefix: str = "$") -> str:
    """1234.56 -> "$1,234.56" or "$1,235" if integer-ish"""
    try:
        v = float(n)
        if v == int(v):
            return f"{prefix}{int(v):,}"
        return f"{prefix}{v:,.2f}"
    except (TypeError, ValueError):
        return "—"


def fmt_pct(n, decimals: int = 1) -> str:
    """0.034 -> "3.4%" """
    try:
        return f"{float(n) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"


def fmt_pct_value(n, decimals: int = 1) -> str:
    """已经是百分数的: 3.11 -> "3.11%". GA4 ctr / bounce 等已是 0-1 小数, 用 fmt_pct."""
    try:
        return f"{float(n):.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"


def fmt_seconds(s) -> str:
    """45.6 -> "46 秒". 130 -> "2 分 10 秒". """
    try:
        s = float(s)
    except (TypeError, ValueError):
        return "—"
    if s < 60:
        return f"{s:.0f} 秒"
    m = int(s // 60)
    rem = int(s % 60)
    return f"{m} 分 {rem} 秒"


def diff_pct(a, b) -> str:
    """两数差异: a - 本周 / b - 对比 (上周 or 双品牌的另一边). 返回"+57%" 或 "-12%"."""
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return "↑ 显著" if a > 0 else "—"
        d = (a - b) / b
        sign = "+" if d > 0 else ""
        return f"{sign}{d * 100:.0f}%"
    except (TypeError, ValueError):
        return "—"


def safe_get(d, *keys, default=None):
    """链式 get: safe_get(collected, 'ga4', 'data', 'powkong', 'core', 'sessions')."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


# ============ Section shapers ============

def _shape_header(start_date, end_date) -> dict:
    week = f"W{start_date.isocalendar()[1]}"
    return {
        "week_label": week,
        "date_range_start": start_date.strftime("%Y-%m-%d"),
        "date_range_end": end_date.strftime("%Y-%m-%d"),
        # Windows strftime 不支持 %-m / %-d, 直接用 month/day 数字属性
        "date_range_cn": (
            f"{start_date.year}年{start_date.month}月{start_date.day}日 – "
            f"{end_date.month}月{end_date.day}日"
        ),
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


def _shape_section_01_overview(collected: dict) -> dict:
    """01 双品牌总览: shopify / shopline / ga4_compare 表."""
    sf = safe_get(collected, "shopify", "data", default={}) or {}
    sl = safe_get(collected, "shopline", "data", default={}) or {}
    pk_ga4 = safe_get(collected, "ga4", "data", "powkong", "core", default={}) or {}
    fl_ga4 = safe_get(collected, "ga4", "data", "funlab", "core", default={}) or {}

    pk_paid = sf.get("paid_orders", 0)
    pk_fulfilled = sf.get("fulfilled_orders", 0)
    pk_aov = sf.get("net_sales", 0) / max(pk_paid, 1)

    fl_paid = sl.get("paid_orders", 0)
    fl_aov = sl.get("net_sales", 0) / max(fl_paid, 1)

    shopify_pk = {
        "_missing": sf == {} or "error" in safe_get(collected, "shopify", default={}),
        "gross": fmt_money(sf.get("gross_sales")),
        "orders": pk_paid,
        "fulfilled": pk_fulfilled,
        "fulfilled_pct": fmt_pct(pk_fulfilled / max(pk_paid, 1)),
        "refund_count": sf.get("refund_count", 0),
        "refund_rate": fmt_pct(sf.get("refund_rate", 0)),
        "discount_total": fmt_money(sf.get("total_discounts", 0)),
        "discount_orders": sf.get("discount_orders", 0),
        "abnormal_cancelled": (sf.get("abnormal") or {}).get("cancelled", 0),
        "abnormal_dup_email": (sf.get("abnormal") or {}).get("duplicate_email_24h", 0),
        "aov": fmt_money(pk_aov),
    }
    shopline_fl = {
        "_missing": sl == {} or "error" in safe_get(collected, "shopline", default={}),
        "gross": fmt_money(sl.get("gross_sales")),
        "orders": fl_paid,
        "fulfilled": sl.get("fulfilled_orders", 0),
        "fulfilled_pct": fmt_pct(sl.get("fulfilled_orders", 0) / max(fl_paid, 1)),
        "refund_count": sl.get("refund_count", 0),
        "refund_rate": fmt_pct(sl.get("refund_rate", 0)),
        "abnormal_cancelled": (sl.get("abnormal") or {}).get("cancelled", 0),
        "abnormal_dup_email": (sl.get("abnormal") or {}).get("duplicate_email_24h", 0),
        "aov": fmt_money(fl_aov),
    }

    # GA4 双品牌对比表
    def cmp_row(label, pk_v, fl_v, formatter=fmt_int, more_is_better=True, suffix=""):
        try:
            pk_n = float(pk_v) if pk_v is not None else 0
            fl_n = float(fl_v) if fl_v is not None else 0
        except (TypeError, ValueError):
            pk_n = fl_n = 0
        if pk_n == 0 and fl_n == 0:
            diff = "—"
            diff_class = "neutral"
        elif fl_n > pk_n:
            d = (fl_n - pk_n) / max(pk_n, 1)
            diff = f"FL +{d * 100:.0f}%"
            diff_class = "up" if more_is_better else "down"
        else:
            d = (pk_n - fl_n) / max(fl_n, 1)
            diff = f"PK +{d * 100:.0f}%"
            diff_class = "up" if more_is_better else "down"
        return {
            "label": label,
            "pk": formatter(pk_v) + suffix if pk_v is not None else "—",
            "fl": formatter(fl_v) + suffix if fl_v is not None else "—",
            "diff": diff,
            "diff_class": diff_class,
        }

    ga4_compare = [
        cmp_row("活跃用户", pk_ga4.get("active_users"), fl_ga4.get("active_users")),
        cmp_row("会话数", pk_ga4.get("sessions"), fl_ga4.get("sessions")),
        cmp_row("总收入", pk_ga4.get("total_revenue"), fl_ga4.get("total_revenue"),
                formatter=fmt_money),
        cmp_row("购买次数", pk_ga4.get("ecommerce_purchases"),
                fl_ga4.get("ecommerce_purchases")),
        cmp_row("转化率", pk_ga4.get("session_conversion_rate"),
                fl_ga4.get("session_conversion_rate"), formatter=fmt_pct),
        cmp_row("跳出率", pk_ga4.get("bounce_rate"),
                fl_ga4.get("bounce_rate"), formatter=fmt_pct, more_is_better=False),
        cmp_row("平均参与时长", pk_ga4.get("avg_engagement_time"),
                fl_ga4.get("avg_engagement_time"), formatter=fmt_seconds),
    ]

    # 跨平台口径差异
    pk_diff_pct = 0
    if sf.get("net_sales") and pk_ga4.get("total_revenue"):
        pk_diff_pct = abs(sf["net_sales"] - pk_ga4["total_revenue"]) / max(sf["net_sales"], 1)
    fl_diff_pct = 0
    if sl.get("net_sales") and fl_ga4.get("total_revenue"):
        fl_diff_pct = abs(sl["net_sales"] - fl_ga4["total_revenue"]) / max(sl["net_sales"], 1)

    return {
        "shopify_pk": shopify_pk,
        "shopline_fl": shopline_fl,
        "ga4_compare": ga4_compare,
        "cross_platform": {
            "pk_shopify_money": fmt_money(sf.get("net_sales", 0)),
            "pk_ga4_money": fmt_money(pk_ga4.get("total_revenue", 0)),
            "pk_diff_pct": fmt_pct(pk_diff_pct, decimals=0),
            "fl_shopline_money": fmt_money(sl.get("net_sales", 0)),
            "fl_ga4_money": fmt_money(fl_ga4.get("total_revenue", 0)),
            "fl_diff_pct": fmt_pct(fl_diff_pct, decimals=0),
        },
    }


def _shape_ga4_brand(brand_data: dict, brand_color: str = "pw") -> dict:
    """02 PK GA4 / 06 FL GA4: 6 个 metric cards + 流量来源 + 漏斗."""
    if not brand_data or "error" in brand_data:
        return {"_missing": True, "error": (brand_data or {}).get("error", "no data")}

    core = brand_data.get("core", {}) or {}
    channels_raw = brand_data.get("channels", []) or []
    funnel = brand_data.get("funnel", {}) or {}

    sessions = core.get("sessions", 0)
    purchases = core.get("ecommerce_purchases", 0)
    total_rev = core.get("total_revenue", 0)
    aov = total_rev / max(purchases, 1)

    metrics = [
        {"label": "活跃用户", "value": fmt_int(core.get("active_users")), "color": brand_color},
        {"label": "总收入 (GA4)", "value": fmt_money(total_rev), "color": "green"},
        {"label": "购买次数", "value": fmt_int(purchases),
         "color": "green", "sub": f"客单价 {fmt_money(aov)}"},
        {"label": "转化率", "value": fmt_pct(core.get("session_conversion_rate")),
         "color": "green" if core.get("session_conversion_rate", 0) > 0.005 else "amber"},
        {"label": "跳出率", "value": fmt_pct(core.get("bounce_rate")),
         "color": "amber" if core.get("bounce_rate", 0) > 0.6 else brand_color},
        {"label": "平均参与", "value": fmt_seconds(core.get("avg_engagement_time")),
         "color": brand_color, "value_size": "sm"},
    ]

    # 流量来源 top 6 + 占比
    total_sess = sum(c.get("sessions", 0) for c in channels_raw)
    max_sess = max((c.get("sessions", 0) for c in channels_raw), default=1)
    chan_color_map = {
        "Paid Social": f"var(--{brand_color})",
        "Direct": "var(--muted)",
        "Organic Search": "var(--teal)",
        "Organic Social": f"var(--{brand_color})",
        "Organic Shopping": "var(--green)",
        "Paid Search": "var(--amber)",
        "Referral": "var(--purple)",
        "Email": "var(--amber)",
    }
    chan_opacity_map = {
        "Paid Social": 0.85,
        "Organic Social": 0.5,
        "Organic Shopping": 0.6,
        "Paid Search": 0.7,
        "Referral": 0.6,
    }
    channels_view = []
    for c in channels_raw[:6]:
        name = c.get("channel", "?")
        sess = c.get("sessions", 0)
        channels_view.append({
            "label": name,
            "sessions": fmt_int(sess),
            "pct": fmt_pct(sess / max(total_sess, 1)),
            "bar_pct": int(round(sess / max(max_sess, 1) * 100)),
            "color_var": chan_color_map.get(name, "var(--muted)"),
            "opacity": chan_opacity_map.get(name, 1.0),
        })

    # 漏斗
    sessions_n = sessions or 1
    funnel_view = {
        "sessions": fmt_int(sessions),
        "add_to_cart": fmt_int(funnel.get("add_to_cart", 0)),
        "begin_checkout": fmt_int(funnel.get("begin_checkout", 0)),
        "purchase": fmt_int(funnel.get("purchase", 0)),
        "atc_pct": fmt_pct(funnel.get("add_to_cart", 0) / sessions_n, decimals=2),
        "atc_pct_num": funnel.get("add_to_cart", 0) / sessions_n * 100,
        "ic_pct": fmt_pct(funnel.get("begin_checkout", 0) / sessions_n, decimals=2),
        "ic_pct_num": funnel.get("begin_checkout", 0) / sessions_n * 100,
        "purchase_pct": fmt_pct(funnel.get("purchase", 0) / sessions_n, decimals=2),
        "purchase_pct_num": funnel.get("purchase", 0) / sessions_n * 100,
        "atc_to_ic": fmt_pct(funnel.get("begin_checkout", 0) / max(funnel.get("add_to_cart", 0), 1)),
        "ic_to_purchase": fmt_pct(funnel.get("purchase", 0) / max(funnel.get("begin_checkout", 0), 1)),
    }

    return {
        "_missing": False,
        "metrics": metrics,
        "channels": channels_view,
        "funnel": funnel_view,
        "total_sessions": fmt_int(sessions),
        "total_sessions_num": sessions,
        "social_breakdown": brand_data.get("social_breakdown", []),
        "countries": brand_data.get("countries", []),
        "utm": brand_data.get("utm_kol", {}),
    }


def _shape_meta_pk(collected: dict) -> dict:
    """03 PK Meta 广告: 指标矩阵 (3 组) + 7 天 ROAS + 漏斗."""
    pk = safe_get(collected, "meta_ads", "data", "powkong", default={}) or {}
    s = pk.get("summary", {}) or {}
    daily = pk.get("daily", []) or []
    if not s or s.get("empty") or "error" in pk:
        return {"_missing": True}

    spend = s.get("spend", 0)
    roas = s.get("roas", 0)
    purchases = s.get("purchases", 0)
    pv = s.get("purchase_value", 0)
    cpa = s.get("cpa", 0)
    impressions = s.get("impressions", 0)
    clicks = s.get("clicks", 0)
    cpc = s.get("cpc", 0)
    cpm = (spend / impressions * 1000) if impressions else 0
    reach = s.get("reach", 0)
    freq = s.get("frequency", 0)
    ctr = s.get("ctr", 0)  # 已是 0-100 的百分数 (Meta 接口返回如 3.11 = 3.11%)
    lpv = s.get("landing_page_view", 0)
    atc = s.get("add_to_cart", 0)
    atc_cost = s.get("add_to_cart_cost", 0)
    ic = s.get("initiate_checkout", 0)

    cost_metrics = [
        {"label": "总花费", "value": fmt_money(spend), "color": "pw", "badge": "7 天", "badge_class": "info"},
        {"label": "综合 ROAS", "value": f"{roas:.2f}", "color": "amber" if roas < 1.5 else "green",
         "badge": "低于盈亏" if roas < 1.5 else "盈利", "badge_class": "down" if roas < 1.5 else "up"},
        {"label": "购买金额", "value": fmt_money(pv), "color": "green" if roas >= 1.5 else "amber",
         "sub": f"{purchases} 单 · ROAS={roas:.2f}"},
        {"label": "CPA", "value": fmt_money(cpa), "color": "amber" if cpa > 50 else "green",
         "badge": "偏高" if cpa > 50 else "健康", "badge_class": "down" if cpa > 50 else "up"},
        {"label": "CPC", "value": fmt_money(cpc), "color": "pw",
         "badge": "健康" if cpc < 1 else "偏高", "badge_class": "up" if cpc < 1 else "down"},
        {"label": "CPM", "value": fmt_money(cpm), "color": "pw", "sub": "每千次曝光"},
    ]
    reach_metrics = [
        {"label": "曝光", "value": fmt_int(impressions), "color": "pw", "value_size": "sm"},
        {"label": "触达 (Reach)", "value": fmt_int(reach) if reach else "—", "color": "pw", "value_size": "sm"},
        {"label": "Frequency", "value": f"{freq:.2f}" if freq else "—", "color": "green" if freq < 3 else "amber",
         "badge": "健康 (<3)" if freq < 3 else "偏高", "badge_class": "up" if freq < 3 else "down"},
        {"label": "CTR", "value": fmt_pct_value(ctr, decimals=2), "color": "pw",
         "badge": "高于行业" if ctr > 2 else "—", "badge_class": "up" if ctr > 2 else "neutral"},
        {"label": "点击", "value": fmt_int(clicks), "color": "pw", "value_size": "sm"},
        {"label": "LPV/点击率", "value": fmt_pct(lpv / max(clicks, 1)), "color": "amber",
         "sub": f"{lpv} / {fmt_int(clicks)}"},
    ]
    funnel_metrics = [
        {"label": "落地页浏览", "value": fmt_int(lpv), "color": "amber",
         "value_size": "sm", "sub": f"LPV/曝光 {lpv / max(impressions, 1) * 100:.1f}%"},
        {"label": "加购", "value": fmt_int(atc), "color": "pw", "sub": f"加购成本 {fmt_money(atc_cost)}"},
        {"label": "发起结账 (IC)", "value": fmt_int(ic), "color": "pw",
         "sub": f"{ic / max(atc, 1) * 100:.1f}% 加购→IC"},
        {"label": "购买 CVR", "value": fmt_pct(purchases / max(clicks, 1), decimals=2),
         "color": "amber", "sub": "购买/点击"},
    ]

    # 7 天 ROAS
    daily_view = []
    max_roas = max((d.get("roas", 0) for d in daily), default=1) or 1
    for d in daily:
        r = d.get("roas", 0)
        if r >= 3:
            color, bg = "green", f"rgba(16,185,129,0.7)"
            text_color = "#d1fae5"
        elif r >= 2:
            color, bg = "blue", f"rgba(59,130,246,0.5)"
            text_color = "#dbeafe"
        elif r >= 1:
            color, bg = "amber", f"rgba(245,158,11,0.5)"
            text_color = "#fde68a"
        else:
            color, bg = "red", f"rgba(239,68,68,0.5)"
            text_color = "#fecaca"
        date_str = (d.get("date") or "")[5:].replace("-", "/")
        bar_w = max(int(r / max_roas * 70), 5) if r > 0 else 5
        daily_view.append({
            "date": date_str,
            "roas": f"{r:.2f}",
            "bar_w": bar_w,
            "bg": bg,
            "text_color": text_color,
            "color_class": f"num-{color}",
            "is_highlight": r == max_roas and r >= 2,
        })

    # 广告漏斗
    ad_funnel = {
        "impressions": fmt_int(impressions),
        "impressions_short": f"{impressions / 1000:.0f}K" if impressions > 1000 else fmt_int(impressions),
        "clicks": fmt_int(clicks),
        "lpv": fmt_int(lpv),
        "atc": fmt_int(atc),
        "ic": fmt_int(ic),
        "purchases": fmt_int(purchases),
    }

    # 零转化天数
    zero_days = [d for d in daily if d.get("roas", 0) == 0]
    zero_dates_str = "、".join((d.get("date") or "")[5:].replace("-", "/") for d in zero_days)
    zero_warning = f"⚠️ {zero_dates_str} {len(zero_days)} 天零转化" if zero_days else ""

    return {
        "_missing": False,
        "cost_metrics": cost_metrics,
        "reach_metrics": reach_metrics,
        "funnel_metrics": funnel_metrics,
        "daily": daily_view,
        "ad_funnel": ad_funnel,
        "zero_warning": zero_warning,
    }


def _shape_gsc(brand_data: dict, brand_label: str, site_url: str) -> dict:
    """04/07 GSC: 4 个核心指标 + 收录情况 (无收录数据时 placeholder) + 高展现低 CTR 关键词."""
    if not brand_data or "error" in brand_data:
        return {"_missing": True}

    summary = brand_data.get("summary", {}) or {}
    queries = brand_data.get("top_queries", []) or []

    clicks = summary.get("clicks", 0)
    impressions = summary.get("impressions", 0)
    ctr = summary.get("ctr", 0)
    pos = summary.get("position", 0)

    metrics = [
        {"label": "总点击", "value": fmt_int(clicks), "color": "green",
         "badge": "↑ 显著" if clicks > 50 else None, "badge_class": "up"},
        {"label": "总展现", "value": fmt_int(impressions), "color": brand_label,
         "value_size": "sm",
         "badge": "↑ 显著" if impressions > 5000 else None, "badge_class": "up"},
        {"label": "平均 CTR", "value": fmt_pct(ctr), "color": "amber" if ctr < 0.02 else "green"},
        {"label": "平均排名", "value": f"{pos:.1f}", "color": "amber" if pos > 10 else "green"},
    ]

    # 高展现低 CTR (展现 > 1000 且 ctr < 0.005)
    low_ctr = [q for q in queries if q.get("impressions", 0) > 1000 and q.get("ctr", 0) < 0.005]
    low_ctr_view = []
    for q in low_ctr[:5]:
        low_ctr_view.append({
            "query": q.get("key", ""),
            "impressions": fmt_int(q.get("impressions", 0)),
            "ctr": fmt_pct(q.get("ctr", 0), decimals=2),
            "position": f"{q.get('position', 0):.1f}",
            "issue": "意图不匹配",  # TODO: AI 后续判断
        })

    return {
        "_missing": False,
        "metrics": metrics,
        "site_url": site_url,
        "low_ctr_queries": low_ctr_view,
    }


# ============ 国家国旗映射 ============

COUNTRY_FLAG = {
    "United States": "🇺🇸", "United Kingdom": "🇬🇧", "Canada": "🇨🇦",
    "Germany": "🇩🇪", "France": "🇫🇷", "Australia": "🇦🇺", "Japan": "🇯🇵",
    "Netherlands": "🇳🇱", "Italy": "🇮🇹", "Spain": "🇪🇸", "Sweden": "🇸🇪",
    "Switzerland": "🇨🇭", "Belgium": "🇧🇪", "Austria": "🇦🇹", "Denmark": "🇩🇰",
    "Norway": "🇳🇴", "Finland": "🇫🇮", "Ireland": "🇮🇪", "Poland": "🇵🇱",
    "Mexico": "🇲🇽", "Brazil": "🇧🇷", "India": "🇮🇳",
    "Taiwan": "🇹🇼", "Hong Kong": "🇭🇰", "Singapore": "🇸🇬", "Malaysia": "🇲🇾",
    "Thailand": "🇹🇭", "Indonesia": "🇮🇩", "Philippines": "🇵🇭", "Vietnam": "🇻🇳",
    "South Korea": "🇰🇷", "China": "🇨🇳", "New Zealand": "🇳🇿",
}


def _country_flag(name: str) -> str:
    return COUNTRY_FLAG.get(name, "🌐")


def _shape_countries(brand_data: dict, brand_total_sessions: int) -> dict:
    """02.5 / 06.5 国家流量分布: top 10 + 其他汇总."""
    if not brand_data or "error" in brand_data:
        return {"_missing": True}
    countries = brand_data.get("countries", []) or []
    if not countries:
        return {"_missing": True}

    top = countries[:10]
    rows = []
    for i, c in enumerate(top, 1):
        sess = c.get("sessions", 0)
        pct = c.get("pct", 0)
        # 高亮 Top 1
        pct_class = "num-blue" if i == 1 else ""
        rev_class = "num-green" if c.get("revenue", 0) > 100 else ""
        rows.append({
            "rank": i,
            "country": c.get("country", "?"),
            "flag": _country_flag(c.get("country", "")),
            "sessions": fmt_int(sess),
            "pct": fmt_pct(pct),
            "pct_class": pct_class,
            "purchases": c.get("purchases", 0),
            "revenue": fmt_money(c.get("revenue", 0)),
            "revenue_class": rev_class,
            "cvr": fmt_pct(c.get("cvr", 0), decimals=2),
        })

    # 其他 (rank 11+) 汇总
    rest = countries[10:]
    if rest:
        rest_sess = sum(c.get("sessions", 0) for c in rest)
        rest_purch = sum(c.get("purchases", 0) for c in rest)
        rest_rev = sum(c.get("revenue", 0) for c in rest)
        other = {
            "sessions": fmt_int(rest_sess),
            "pct": fmt_pct(rest_sess / max(brand_total_sessions, 1)),
            "purchases": rest_purch,
            "revenue": fmt_money(rest_rev),
            "count": len(rest),
        }
    else:
        other = None

    return {
        "_missing": False,
        "rows": rows,
        "other": other,
        "total_sessions": fmt_int(brand_total_sessions),
    }


def _shape_top_products(platform_data: dict, currency: str = "USD") -> dict:
    """02.6 / 06.55 产品销量 top: 来自 shopify/shopline collector top_products."""
    if not platform_data or "error" in platform_data:
        return {"_missing": True}
    top = platform_data.get("top_products") or []
    if not top:
        return {"_missing": True}

    total_qty = sum(p.get("qty", 0) for p in top)
    total_rev = sum(p.get("revenue", 0) for p in top)
    paid_orders = platform_data.get("paid_orders", 0) or 0
    aov = total_rev / max(paid_orders, 1)

    rows = []
    for i, p in enumerate(top, 1):
        qty = p.get("qty", 0)
        rev = p.get("revenue", 0)
        # rank 1: 加 amber 高亮
        qty_pct_class = "num-amber" if i == 1 else ""
        rev_class = "num-green" if rev >= total_rev * 0.2 else ""
        rev_pct_class = "num-amber" if i == 1 else ""
        # 产品单价
        unit = rev / max(qty, 1)
        rows.append({
            "rank": i,
            "title": p.get("title") or "(unknown)",
            "sku": p.get("sku") or "",
            "qty": qty,
            "qty_pct": fmt_pct(p.get("qty_pct", 0)),
            "qty_pct_class": qty_pct_class,
            "revenue": fmt_money(rev, prefix="$" if currency == "USD" else f"{currency} "),
            "revenue_class": rev_class,
            "revenue_pct": fmt_pct(p.get("revenue_pct", 0)),
            "revenue_pct_class": rev_pct_class,
            "unit_price": fmt_money(unit, prefix="$" if currency == "USD" else f"{currency} "),
        })

    # 集中度: top 1 占比
    top1_rev_pct = top[0].get("revenue_pct", 0) if top else 0

    return {
        "_missing": False,
        "rows": rows,
        "total_qty": total_qty,
        "total_revenue": fmt_money(total_rev),
        "aov": fmt_money(aov),
        "sku_count": len(top),
        "top1_title": top[0].get("title") if top else "",
        "top1_pct": fmt_pct(top1_rev_pct),
    }


def _shape_section_09_kol(collected: dict) -> dict:
    """09 KOL/UTM: 双端发送数据 + UTM 流量贡献."""
    kol_data = safe_get(collected, "kol", "data", default={}) or {}
    pk_ga4 = safe_get(collected, "ga4", "data", "powkong", default={}) or {}
    fl_ga4 = safe_get(collected, "ga4", "data", "funlab", default={}) or {}

    kol = kol_data.get("kol") or {}
    editor = kol_data.get("editor") or {}
    has_kol_err = "error" in kol
    has_editor_err = "error" in editor

    pk_total_sess = safe_get(pk_ga4, "core", "sessions", default=0) or 0
    fl_total_sess = safe_get(fl_ga4, "core", "sessions", default=0) or 0
    pk_utm = pk_ga4.get("utm_kol") or {}
    fl_utm = fl_ga4.get("utm_kol") or {}

    def _utm_row(label, brand_class, utm, total_sess):
        sess = utm.get("sessions", 0)
        rev = utm.get("revenue", 0)
        purch = utm.get("purchases", 0)
        pct = sess / max(total_sess, 1)
        pct_class = "num-amber" if pct < 0.05 else "num-green"
        roas = "N/A" if sess == 0 else f"{rev / max(sess, 1) * 100:.2f} (per100)"
        return {
            "brand": label,
            "brand_class": brand_class,
            "sessions": fmt_int(sess),
            "pct_total": f"{fmt_pct(pct)} / {fmt_int(total_sess)}",
            "pct_class": pct_class,
            "revenue": fmt_money(rev),
            "purchases": purch,
            "roas": roas,
        }

    utm_rows = [
        _utm_row("Powkong", "brand-pw", pk_utm, pk_total_sess),
        _utm_row("FUNLAB", "brand-fl", fl_utm, fl_total_sess),
    ]

    total_utm_sess = pk_utm.get("sessions", 0) + fl_utm.get("sessions", 0)

    return {
        "_missing": has_kol_err and has_editor_err,
        "kol": {
            "_error": has_kol_err,
            "tasks_created": kol.get("tasks_created", 0),
            "qualified": kol.get("qualified", 0),
            "sent": kol.get("sent", 0),
            "replied": kol.get("replied", 0),
            "intent_replies": kol.get("intent_replies", 0),
            "interest_rate": fmt_pct(kol.get("interest_rate", 0)),
            "reply_rate": fmt_pct(kol.get("reply_rate", 0)),
        },
        "editor": {
            "_error": has_editor_err,
            "tasks_created": editor.get("tasks_created", 0),
            "qualified": editor.get("qualified", 0),
            "sent": editor.get("sent", 0),
            "replied": editor.get("replied", 0),
            "intent_replies": editor.get("intent_replies", 0),
            "interest_rate": fmt_pct(editor.get("interest_rate", 0)),
            "reply_rate": fmt_pct(editor.get("reply_rate", 0)),
        },
        "utm_rows": utm_rows,
        "total_utm_sessions": total_utm_sess,
        "no_activity": total_utm_sess == 0,
    }


def _shape_section_11_seo_prod(collected: dict) -> dict:
    """11 SEO 自动化产能: 6 工作流达成率 + 选题池状态."""
    n8n_data = safe_get(collected, "n8n", "data", default={}) or {}
    if "error" in safe_get(collected, "n8n", default={}):
        return {"_missing": True}
    workflows = n8n_data.get("workflows") or {}
    summary = n8n_data.get("summary") or {}

    # 表格 6 行: 按 collector WORKFLOWS 顺序
    wf_order = [
        ("ee779GzBI8Bj4Bx3", "SEO 新闻稿 (双站)"),
        ("bxqthAOVFjGviUEr", "SEO 商业意图"),
        ("PEzTmqGwOqcHOPfc", "SEO 周报"),
        ("xLEIAVos3YmynRsq", "竞品 Gap 扫描"),
        ("9gMvXqs3mjS1zBZJ", "PSI 月度审计"),
        ("z8OmSc1gWqc9cnsH", "GSC 排名追踪"),
    ]
    rows = []
    weekly_actual = 0
    weekly_target = 0
    news_actual = 0
    news_target = 12
    intent_actual = 0
    intent_target = 2
    for wf_id, name in wf_order:
        w = workflows.get(wf_id) or {}
        if "error" in w:
            rows.append({
                "name": name,
                "actual": "—",
                "actual_class": "num-red",
                "target": "—",
                "rate": "—",
                "rate_class": "num-red",
                "gap": "—",
                "gap_class": "down",
                "status": "API 错误",
                "status_class": "num-red",
            })
            continue
        expected = w.get("expected_per_week", 0)
        in_win = (w.get("in_window") or {})
        actual = in_win.get("success", 0)
        if wf_id == "ee779GzBI8Bj4Bx3":
            news_actual = actual
        if wf_id == "bxqthAOVFjGviUEr":
            intent_actual = actual

        if expected == 0:
            rate = "-"
            rate_class = ""
            gap = "-"
            gap_class = ""
            target_str = "月级"
            status = "月度"
            status_class = ""
        else:
            weekly_actual += actual
            weekly_target += expected
            rate_pct = actual / expected
            rate = fmt_pct(rate_pct)
            target_str = str(expected)
            if rate_pct >= 0.9:
                rate_class = "num-green"
                gap_class = "up"
                status = "达成"
                status_class = "num-green"
            elif rate_pct >= 0.5:
                rate_class = "num-amber"
                gap_class = "down"
                status = "不足"
                status_class = "num-amber"
            elif rate_pct > 0:
                rate_class = "num-red"
                gap_class = "down"
                status = "严重不足"
                status_class = "num-amber"
            else:
                rate_class = "num-red"
                gap_class = "down"
                status = "未跑" if w.get("health") != "stale" else "Stale"
                status_class = "num-red" if status == "未跑" else "num-amber"
            d = (actual - expected) / expected
            gap = f"{'+' if d >= 0 else ''}{d * 100:.0f}%"

        rows.append({
            "name": name,
            "actual": str(actual),
            "actual_class": rate_class or "",
            "target": target_str,
            "rate": rate,
            "rate_class": rate_class,
            "gap": gap,
            "gap_class": gap_class,
            "status": status,
            "status_class": status_class,
        })

    overall_rate_pct = weekly_actual / max(weekly_target, 1)
    overall_rate = fmt_pct(overall_rate_pct)
    overall_class = (
        "num-green" if overall_rate_pct >= 0.9
        else "num-amber" if overall_rate_pct >= 0.5
        else "num-red"
    )

    # 选题池
    kol_data = safe_get(collected, "kol", "data", default={}) or {}
    tp = kol_data.get("topic_pool") or {}
    has_tp_err = "error" in tp
    by_status = tp.get("by_status") or {}
    waiting = tp.get("waiting", 0)
    written_pk = by_status.get("已写 PK", 0)
    written_fl = by_status.get("已写 FL", 0)
    written_both = by_status.get("已写双站", 0)

    return {
        "_missing": False,
        "rows": rows,
        "overall_actual": weekly_actual,
        "overall_target": weekly_target,
        "overall_rate": overall_rate,
        "overall_rate_class": overall_class,
        "news_actual": news_actual,
        "news_target": news_target,
        "news_gap_pct": f"{(news_actual - news_target) / news_target * 100:.0f}%" if news_target else "—",
        "intent_actual": intent_actual,
        "intent_target": intent_target,
        "intent_gap_pct": f"{(intent_actual - intent_target) / intent_target * 100:.0f}%" if intent_target else "—",
        "topic_pool": {
            "_error": has_tp_err,
            "candidates_total": tp.get("candidates_total", 0),
            "waiting": waiting,
            "written_pk": written_pk,
            "written_fl": written_fl,
            "written_both": written_both,
            "new_this_week": tp.get("new_this_week", 0),
            "consumed_this_week": tp.get("consumed_this_week", 0),
            "low_stock_alert": tp.get("low_stock_alert", False),
        },
        "summary": summary,
    }


def _psi_perf_class(score: int) -> str:
    if score >= 90:
        return "num-green"
    if score >= 50:
        return "num-amber"
    return "num-red"


def _psi_perf_emoji(score: int) -> str:
    if score >= 90:
        return "🟢"
    if score >= 50:
        return "🟡"
    return "🔴"


def _psi_metric_class(key: str, value: float) -> str:
    """LCP/TBT/CLS/FCP/SI/INP 阈值上色."""
    thresholds = {
        "lcp_ms": (2500, 4000),
        "tbt_ms": (200, 600),
        "cls": (0.1, 0.25),
        "fcp_ms": (1800, 3000),
        "si_ms": (3400, 5800),
        "inp_ms": (200, 500),
    }
    good, ok = thresholds.get(key, (0, 0))
    if value <= good:
        return "num-green"
    if value <= ok:
        return "num-amber"
    return "num-red"


def _shape_section_12_psi(collected: dict) -> dict:
    """12 Lighthouse: 4 维度评分 + Performance 6 子项 + Top 3 优化建议."""
    psi_data = safe_get(collected, "psi", "data", default={}) or {}
    if "error" in safe_get(collected, "psi", default={}):
        return {"_missing": True}
    pages = psi_data.get("pages") or []
    if not pages:
        return {"_missing": True}

    # 4 维度评分表
    score_rows = []
    for p in pages:
        if "error" in p:
            score_rows.append({
                "name": p.get("name", "?"),
                "_error": p.get("error", "")[:60],
            })
            continue
        sc = p.get("scores") or {}
        score_rows.append({
            "name": p.get("name", "?"),
            "performance": sc.get("performance", 0),
            "performance_class": _psi_perf_class(sc.get("performance", 0)),
            "performance_emoji": _psi_perf_emoji(sc.get("performance", 0)),
            "accessibility": sc.get("accessibility", 0),
            "accessibility_class": _psi_perf_class(sc.get("accessibility", 0)),
            "accessibility_emoji": _psi_perf_emoji(sc.get("accessibility", 0)),
            "best_practices": sc.get("best_practices", 0),
            "best_practices_class": _psi_perf_class(sc.get("best_practices", 0)),
            "best_practices_emoji": _psi_perf_emoji(sc.get("best_practices", 0)),
            "seo": sc.get("seo", 0),
            "seo_class": _psi_perf_class(sc.get("seo", 0)),
            "seo_emoji": _psi_perf_emoji(sc.get("seo", 0)),
        })

    # Performance 6 子项 (LCP/TBT/CLS/FCP/SI/INP) × 4 页
    metric_rows = []
    metric_meta = [
        ("lcp_ms", "LCP (ms)", "25%", lambda v: fmt_int(v)),
        ("tbt_ms", "TBT (ms)", "25%", lambda v: fmt_int(v)),
        ("cls", "CLS", "25%", lambda v: f"{v:.2f}"),
        ("fcp_ms", "FCP (ms)", "10%", lambda v: fmt_int(v)),
        ("si_ms", "SI (ms)", "10%", lambda v: fmt_int(v)),
        ("inp_ms", "INP (ms)", "—", lambda v: fmt_int(v) if v else "—"),
    ]
    for key, label, weight, fmt in metric_meta:
        cells = []
        for p in pages:
            if "error" in p:
                cells.append({"value": "—", "class": ""})
                continue
            v = (p.get("metrics") or {}).get(key, 0)
            cells.append({"value": fmt(v), "class": _psi_metric_class(key, v)})
        metric_rows.append({"label": label, "weight": weight, "cells": cells})

    # Top 3 优化建议: 跨 4 页合并按 savings 降序取前 3
    all_opps = []
    for p in pages:
        if "error" in p:
            continue
        page_name = p.get("name", "?")
        for o in (p.get("opportunities_top3") or []):
            all_opps.append({
                "page": page_name,
                "title": o.get("title", ""),
                "savings_ms": o.get("savings_ms", 0),
            })
    all_opps.sort(key=lambda x: -x["savings_ms"])
    top3 = all_opps[:3]

    summary = psi_data.get("summary") or {}
    perf_avg = summary.get("performance_avg", 0)
    red_flags = summary.get("red_flags") or []

    # 主告警: 找最差的 LCP
    worst_lcp = 0
    worst_lcp_page = None
    for p in pages:
        if "error" in p:
            continue
        v = (p.get("metrics") or {}).get("lcp_ms", 0)
        if v > worst_lcp:
            worst_lcp = v
            worst_lcp_page = p.get("name")

    return {
        "_missing": False,
        "score_rows": score_rows,
        "metric_rows": metric_rows,
        "top3": top3,
        "perf_avg": perf_avg,
        "red_flags_count": len(red_flags),
        "worst_lcp": worst_lcp,
        "worst_lcp_page": worst_lcp_page,
        "page_count": len(pages),
    }


def _shape_section_08_actions(collected: dict, markdown_insights: str) -> dict:
    """08 下周任务清单: 优先用 integrator markdown 提取的 ## 08 段."""
    insights = _split_md_insights(markdown_insights or "")
    body = insights.get("08", "")
    return {
        "_missing": not body,
        "html": _md_to_html_simple(body) if body else "",
        "raw": body,
    }


# ============ AI insights split ============

SECTION_HEADER_RE = re.compile(r"^##\s*(\d{2})[^\n]*$", re.M)


def _split_md_insights(markdown: str) -> dict:
    """integrator 出的 markdown 按 ## 01 / ## 02 / ... 分割成 dict."""
    out = {}
    if not markdown:
        return out
    matches = list(SECTION_HEADER_RE.finditer(markdown))
    for i, m in enumerate(matches):
        sec_id = m.group(1)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(markdown)
        body = markdown[start:end].strip()
        out[sec_id] = body
    return out


def _md_to_html_simple(md: str) -> str:
    """简易 markdown → html (用于 callout 嵌入). 只支持基础 lib 调用."""
    if not md:
        return ""
    try:
        import markdown as md_lib
        return md_lib.markdown(md, extensions=["tables", "fenced_code", "nl2br"])
    except ImportError:
        import html as h
        return f"<pre>{h.escape(md)}</pre>"


# ============ 主入口 ============

def shape(collected: dict, start_date, end_date, gaps: list,
          markdown_insights: str = "") -> dict:
    """构造 view model.

    返回 dict, 由 jinja2 渲染时直接展开.
    """
    log.info("data_shaper.shape %s ~ %s, gaps=%d", start_date, end_date, len(gaps))

    insights_map = _split_md_insights(markdown_insights or "")

    pk_ga4 = safe_get(collected, "ga4", "data", "powkong", default={})
    fl_ga4 = safe_get(collected, "ga4", "data", "funlab", default={})
    pk_gsc = safe_get(collected, "gsc", "data", "powkong", default={})
    fl_gsc = safe_get(collected, "gsc", "data", "funlab", default={})
    sf = safe_get(collected, "shopify", "data", default={}) or {}
    sl = safe_get(collected, "shopline", "data", default={}) or {}

    pk_total_sess = safe_get(pk_ga4, "core", "sessions", default=0) or 0
    fl_total_sess = safe_get(fl_ga4, "core", "sessions", default=0) or 0

    return {
        # 顶部
        **_shape_header(start_date, end_date),
        "gaps": gaps,
        "gaps_count": len(gaps),

        # 核心 sections (数据驱动)
        "section_01": _shape_section_01_overview(collected),
        "section_02": _shape_ga4_brand(pk_ga4, brand_color="pw"),
        "section_025": _shape_countries(pk_ga4, pk_total_sess),
        "section_026": _shape_top_products(sf, currency="USD"),
        "section_03": _shape_meta_pk(collected),
        "section_04": _shape_gsc(pk_gsc, "pw", "powkong.com - sc-domain"),
        "section_06": _shape_ga4_brand(fl_ga4, brand_color="fl"),
        "section_065": _shape_countries(fl_ga4, fl_total_sess),
        "section_0655": _shape_top_products(sl, currency="USD"),
        "section_07": _shape_gsc(fl_gsc, "fl", "funlabswitch.com - URL prefix"),
        "section_08": _shape_section_08_actions(collected, markdown_insights),
        "section_09": _shape_section_09_kol(collected),
        "section_11": _shape_section_11_seo_prod(collected),
        "section_12": _shape_section_12_psi(collected),

        # AI 文字洞察 (jinja2 在每段底部 callout 嵌入)
        "insights_html": {
            sec: _md_to_html_simple(body) for sec, body in insights_map.items()
        },
    }
