"""GA4 collector.

数据源: GA4 Data API v1beta
Property IDs: PK 512451371 / FL 399581026
Auth: Google service account (env GA4_SERVICE_ACCOUNT_JSON, 与 GSC 共用)

输出 collected.ga4.data:
{
  "powkong": {
    "core": {  # 7 个核心指标 (本周值, 上周对比由 history collector 给基线)
      "active_users": int,
      "sessions": int,
      "total_revenue": float,
      "ecommerce_purchases": int,
      "session_conversion_rate": float,
      "bounce_rate": float,
      "avg_engagement_time": float,  # 秒
    },
    "channels": [(channel, sessions), ...],  # 流量来源 6+ 类
    "utm_kol": {  # KOL/Editor UTM 过滤
      "sessions": int,
      "revenue": float,
      "purchases": int,
      "top5_campaigns": [(name, sessions), ...],
    },
    "funnel": {
      "sessions": int,
      "add_to_cart": int,
      "begin_checkout": int,
      "purchase": int,
    },
  },
  "funlab": {同上结构}
}
"""
import asyncio
import logging
import os
import json

log = logging.getLogger("weekly_report.ga4")

# Property IDs
# PK = powkong.com (Shopify)
# FL = funlabswitch.com (Shopline) — 旧 memory 写 399581026 是错的, 真实值见 GA4 后台 Shopline—FUNLAB 账号
PK_PROPERTY = "512451371"
FL_PROPERTY = "403214709"

UTM_KOL_VALUES = ["kol_dm", "editor_pr", "kol", "editor", "kol_email"]


def _get_credentials():
    """Lazy import + load GA4 service account."""
    from google.oauth2 import service_account
    creds_json = os.environ.get("GA4_SERVICE_ACCOUNT_JSON", "").strip()
    if not creds_json:
        raise RuntimeError("GA4_SERVICE_ACCOUNT_JSON env 未设")
    info = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/analytics.readonly",
            "https://www.googleapis.com/auth/webmasters.readonly",
        ],
    )


def _build_client():
    """同步构建 GA4 client. async 调用方用 asyncio.to_thread 包."""
    from googleapiclient.discovery import build
    creds = _get_credentials()
    return build("analyticsdata", "v1beta", credentials=creds, cache_discovery=False)


def _run_report_sync(client, property_id: str, body: dict) -> dict:
    """同步调 runReport."""
    return client.properties().runReport(property=f"properties/{property_id}", body=body).execute()


async def _run_report(property_id: str, body: dict) -> dict:
    client = await asyncio.to_thread(_build_client)
    return await asyncio.to_thread(_run_report_sync, client, property_id, body)


def _date_range(start_date, end_date) -> dict:
    return {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}


def _parse_metric(row, idx, default=0.0):
    try:
        return float(row["metricValues"][idx]["value"])
    except (KeyError, IndexError, ValueError, TypeError):
        return default


async def _fetch_brand(property_id: str, start_date, end_date) -> dict:
    dr = _date_range(start_date, end_date)

    # 1. 7 核心指标
    core_body = {
        "dateRanges": [dr],
        "metrics": [
            {"name": "activeUsers"},
            {"name": "sessions"},
            {"name": "totalRevenue"},
            {"name": "ecommercePurchases"},
            {"name": "sessionConversionRate"},
            {"name": "bounceRate"},
            {"name": "averageSessionDuration"},  # fallback if averageEngagementTime not available
        ],
    }
    # 单独拉 averageEngagementTime (新版 GA4 有, 老版回退)
    eng_body = {
        "dateRanges": [dr],
        "metrics": [{"name": "userEngagementDuration"}, {"name": "engagedSessions"}],
    }

    # 2. 流量来源 (粗分类)
    chan_body = {
        "dateRanges": [dr],
        "dimensions": [{"name": "sessionDefaultChannelGroup"}],
        "metrics": [{"name": "sessions"}, {"name": "totalRevenue"}],
        "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
        "limit": 12,
    }

    # 2.1 社媒平台细分 (sessionSource 含 social 类前缀, 后续过滤)
    social_body = {
        "dateRanges": [dr],
        "dimensions": [{"name": "sessionSource"}, {"name": "sessionMedium"}],
        "metrics": [{"name": "sessions"}, {"name": "totalRevenue"}],
        "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
        "limit": 30,
    }

    # 2.2 国家流量分布 Top 10
    country_body = {
        "dateRanges": [dr],
        "dimensions": [{"name": "country"}],
        "metrics": [{"name": "sessions"}, {"name": "totalRevenue"},
                    {"name": "ecommercePurchases"}],
        "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
        "limit": 10,
    }

    # 3. UTM KOL/Editor 过滤
    utm_filter = {
        "filter": {
            "fieldName": "sessionSource",
            "inListFilter": {"values": UTM_KOL_VALUES, "caseSensitive": False},
        }
    }
    utm_body = {
        "dateRanges": [dr],
        "metrics": [{"name": "sessions"}, {"name": "totalRevenue"}, {"name": "ecommercePurchases"}],
        "dimensionFilter": utm_filter,
    }
    utm_camp_body = {
        "dateRanges": [dr],
        "dimensions": [{"name": "sessionCampaignName"}],
        "metrics": [{"name": "sessions"}],
        "dimensionFilter": utm_filter,
        "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
        "limit": 5,
    }

    # 4. 漏斗事件
    funnel_body = {
        "dateRanges": [dr],
        "dimensions": [{"name": "eventName"}],
        "metrics": [{"name": "eventCount"}],
        "dimensionFilter": {
            "filter": {
                "fieldName": "eventName",
                "inListFilter": {"values": ["session_start", "add_to_cart", "begin_checkout", "purchase"]},
            }
        },
    }

    # 并发 8 个 reports (含新加 social + country)
    core_r, eng_r, chan_r, social_r, country_r, utm_r, utm_camp_r, funnel_r = await asyncio.gather(
        _run_report(property_id, core_body),
        _run_report(property_id, eng_body),
        _run_report(property_id, chan_body),
        _run_report(property_id, social_body),
        _run_report(property_id, country_body),
        _run_report(property_id, utm_body),
        _run_report(property_id, utm_camp_body),
        _run_report(property_id, funnel_body),
    )

    # 解析 core
    core_row = (core_r.get("rows") or [{}])[0] if core_r.get("rows") else {}
    core = {
        "active_users": int(_parse_metric(core_row, 0)),
        "sessions": int(_parse_metric(core_row, 1)),
        "total_revenue": round(_parse_metric(core_row, 2), 2),
        "ecommerce_purchases": int(_parse_metric(core_row, 3)),
        "session_conversion_rate": round(_parse_metric(core_row, 4), 4),
        "bounce_rate": round(_parse_metric(core_row, 5), 4),
        "avg_session_duration": round(_parse_metric(core_row, 6), 1),
    }
    # engagement (优先用 userEngagementDuration / engagedSessions)
    eng_row = (eng_r.get("rows") or [{}])[0] if eng_r.get("rows") else {}
    user_eng_dur = _parse_metric(eng_row, 0)
    engaged_sess = _parse_metric(eng_row, 1)
    if engaged_sess > 0:
        core["avg_engagement_time"] = round(user_eng_dur / engaged_sess, 1)
    else:
        core["avg_engagement_time"] = core["avg_session_duration"]
    core["engaged_sessions"] = int(engaged_sess)
    core["engagement_rate"] = round(engaged_sess / max(core["sessions"], 1), 4)

    # 流量来源 (粗分类)
    channels = []
    for row in (chan_r.get("rows") or []):
        name = (row.get("dimensionValues") or [{}])[0].get("value", "?")
        sess = int(_parse_metric(row, 0))
        rev = round(_parse_metric(row, 1), 2)
        channels.append({"channel": name, "sessions": sess, "revenue": rev})

    # 社媒平台细分 (按 sessionSource 关键字归类)
    SOCIAL_PLATFORMS = {
        "facebook": ["facebook", "fb", "m.facebook", "l.facebook"],
        "instagram": ["instagram", "ig", "l.instagram"],
        "tiktok": ["tiktok", "tt", "ads.tiktok"],
        "youtube": ["youtube", "yt", "m.youtube"],
        "twitter": ["twitter", "x.com", "t.co"],
        "pinterest": ["pinterest"],
        "reddit": ["reddit", "old.reddit"],
        "threads": ["threads"],
    }
    social_breakdown = {k: {"sessions": 0, "revenue": 0.0} for k in SOCIAL_PLATFORMS}
    social_breakdown["other_social"] = {"sessions": 0, "revenue": 0.0}
    for row in (social_r.get("rows") or []):
        dvs = row.get("dimensionValues") or []
        source = (dvs[0].get("value") if dvs else "").lower()
        medium = (dvs[1].get("value") if len(dvs) > 1 else "").lower()
        sess = int(_parse_metric(row, 0))
        rev = round(_parse_metric(row, 1), 2)
        # 仅算 social (medium 包含 social 或 source 命中已知社媒)
        is_social = "social" in medium or any(
            kw in source for kws in SOCIAL_PLATFORMS.values() for kw in kws
        )
        if not is_social:
            continue
        matched = None
        for plat, kws in SOCIAL_PLATFORMS.items():
            if any(kw in source for kw in kws):
                matched = plat
                break
        bucket = matched or "other_social"
        social_breakdown[bucket]["sessions"] += sess
        social_breakdown[bucket]["revenue"] = round(social_breakdown[bucket]["revenue"] + rev, 2)
    social_total = sum(v["sessions"] for v in social_breakdown.values())
    social_list = []
    for plat in ["facebook", "instagram", "tiktok", "youtube", "twitter",
                 "pinterest", "reddit", "threads", "other_social"]:
        v = social_breakdown[plat]
        if v["sessions"] > 0:
            social_list.append({
                "platform": plat,
                "sessions": v["sessions"],
                "revenue": v["revenue"],
                "pct": round(v["sessions"] / max(social_total, 1), 4),
            })

    # 国家流量分布 Top 10
    countries = []
    country_total_sess = 0
    for row in (country_r.get("rows") or []):
        name = (row.get("dimensionValues") or [{}])[0].get("value", "?")
        sess = int(_parse_metric(row, 0))
        rev = round(_parse_metric(row, 1), 2)
        purch = int(_parse_metric(row, 2))
        countries.append({"country": name, "sessions": sess, "revenue": rev,
                           "purchases": purch})
        country_total_sess += sess
    for c in countries:
        c["pct"] = round(c["sessions"] / max(country_total_sess, 1), 4)
        c["cvr"] = round(c["purchases"] / max(c["sessions"], 1), 4)

    # UTM KOL
    utm_row = (utm_r.get("rows") or [{}])[0] if utm_r.get("rows") else {}
    utm_camps = []
    for row in (utm_camp_r.get("rows") or []):
        name = (row.get("dimensionValues") or [{}])[0].get("value", "?")
        sess = int(_parse_metric(row, 0))
        utm_camps.append({"campaign": name, "sessions": sess})
    utm = {
        "sessions": int(_parse_metric(utm_row, 0)),
        "revenue": round(_parse_metric(utm_row, 1), 2),
        "purchases": int(_parse_metric(utm_row, 2)),
        "top5_campaigns": utm_camps,
    }

    # 漏斗
    funnel = {"sessions": core["sessions"]}  # 用 core 的 sessions 当漏斗起点
    for row in (funnel_r.get("rows") or []):
        ev = (row.get("dimensionValues") or [{}])[0].get("value", "?")
        cnt = int(_parse_metric(row, 0))
        if ev in ("add_to_cart", "begin_checkout", "purchase"):
            funnel[ev] = cnt
    funnel.setdefault("add_to_cart", 0)
    funnel.setdefault("begin_checkout", 0)
    funnel.setdefault("purchase", 0)

    return {
        "core": core,
        "channels": channels,
        "social_breakdown": social_list,
        "countries": countries,
        "utm_kol": utm,
        "funnel": funnel,
    }


async def collect(start_date, end_date) -> dict:
    """GA4 双站数据收集."""
    log.info("ga4.collect %s ~ %s", start_date, end_date)
    try:
        pk, fl = await asyncio.gather(
            _fetch_brand(PK_PROPERTY, start_date, end_date),
            _fetch_brand(FL_PROPERTY, start_date, end_date),
            return_exceptions=True,
        )
        if isinstance(pk, Exception):
            log.exception("ga4 PK fetch failed")
            pk = {"error": f"{type(pk).__name__}: {pk}"}
        if isinstance(fl, Exception):
            log.exception("ga4 FL fetch failed")
            fl = {"error": f"{type(fl).__name__}: {fl}"}
        return {"status": "ok", "data": {"powkong": pk, "funlab": fl,
                                          "window": f"{start_date}~{end_date}"}}
    except Exception as e:
        log.exception("ga4 collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))
