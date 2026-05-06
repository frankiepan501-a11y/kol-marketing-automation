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

    return {
        # 顶部
        **_shape_header(start_date, end_date),
        "gaps": gaps,
        "gaps_count": len(gaps),

        # 核心 sections (数据驱动)
        "section_01": _shape_section_01_overview(collected),
        "section_02": _shape_ga4_brand(pk_ga4, brand_color="pw"),
        "section_03": _shape_meta_pk(collected),
        "section_04": _shape_gsc(pk_gsc, "pw", "powkong.com - sc-domain"),
        "section_06": _shape_ga4_brand(fl_ga4, brand_color="fl"),
        "section_07": _shape_gsc(fl_gsc, "fl", "funlabswitch.com - URL prefix"),

        # AI 文字洞察 (jinja2 在每段底部 callout 嵌入)
        "insights_html": {
            sec: _md_to_html_simple(body) for sec, body in insights_map.items()
        },
    }
