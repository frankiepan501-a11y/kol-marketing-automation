"""GSC collector.

数据源: Google Search Console API (复用 GA4 service account, 已加为 GSC user)
sites: PK https://powkong.com/ + FL https://funlabswitch.com/

输出 collected.gsc.data:
{
  "powkong": {
    "summary": {  # 周累计
      "clicks": int,
      "impressions": int,
      "ctr": float,        # 平均
      "position": float,   # 平均排名
    },
    "top_queries": [(query, clicks, impressions, ctr, position), ...] top 10,
    "top_pages": [(page, clicks, impressions, ctr, position), ...] top 10,
    "blogs": {  # /blogs/news/ 前缀过滤
      "clicks": int, "impressions": int, "ctr": float, "position": float,
      "top_articles": [...] top 10,
    },
  },
  "funlab": {同上结构}
}
"""
import asyncio
import logging

from .ga4 import _get_credentials  # 复用 service account loader

log = logging.getLogger("weekly_report.gsc")

# GSC site URL 格式必须与 GSC 后台 property 注册类型完全一致 (借鉴 D:/scripts/gsc-mcp.js):
# - powkong: domain property → "sc-domain:powkong.com" (不是 https://powkong.com/)
# - funlab: URL prefix property → "https://funlabswitch.com/"
# 之前用 https://powkong.com/ 报 403 - service account 加的是 sc-domain:powkong.com 的权限
PK_SITE = "sc-domain:powkong.com"
FL_SITE = "https://funlabswitch.com/"


def _build_client():
    from googleapiclient.discovery import build
    creds = _get_credentials()
    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


def _query_sync(client, site_url: str, body: dict) -> dict:
    return client.searchanalytics().query(siteUrl=site_url, body=body).execute()


async def _query(site_url: str, body: dict) -> dict:
    client = await asyncio.to_thread(_build_client)
    return await asyncio.to_thread(_query_sync, client, site_url, body)


async def _fetch_site(site_url: str, start_date, end_date) -> dict:
    sd = start_date.isoformat()
    ed = end_date.isoformat()

    # 1. 整站汇总 (无 dimensions)
    summary_body = {"startDate": sd, "endDate": ed, "type": "web"}

    # 2. Top 10 queries
    top_q_body = {
        "startDate": sd, "endDate": ed,
        "dimensions": ["query"],
        "rowLimit": 10,
        "type": "web",
    }

    # 3. Top 10 pages
    top_p_body = {
        "startDate": sd, "endDate": ed,
        "dimensions": ["page"],
        "rowLimit": 10,
        "type": "web",
    }

    # 4. /blogs/news/ 前缀汇总
    blogs_body = {
        "startDate": sd, "endDate": ed,
        "dimensionFilterGroups": [{
            "filters": [{"dimension": "page", "operator": "contains", "expression": "/blogs/news/"}]
        }],
        "type": "web",
    }
    blogs_top_body = {
        "startDate": sd, "endDate": ed,
        "dimensions": ["page"],
        "dimensionFilterGroups": [{
            "filters": [{"dimension": "page", "operator": "contains", "expression": "/blogs/news/"}]
        }],
        "rowLimit": 10,
        "type": "web",
    }

    summary, top_q, top_p, blogs, blogs_top = await asyncio.gather(
        _query(site_url, summary_body),
        _query(site_url, top_q_body),
        _query(site_url, top_p_body),
        _query(site_url, blogs_body),
        _query(site_url, blogs_top_body),
        return_exceptions=True,
    )

    def parse_summary(resp):
        if isinstance(resp, Exception):
            return {"error": str(resp)}
        rows = resp.get("rows") or []
        if not rows:
            return {"clicks": 0, "impressions": 0, "ctr": 0.0, "position": 0.0}
        r = rows[0]
        return {
            "clicks": int(r.get("clicks", 0)),
            "impressions": int(r.get("impressions", 0)),
            "ctr": round(r.get("ctr", 0), 4),
            "position": round(r.get("position", 0), 1),
        }

    def parse_rows(resp):
        if isinstance(resp, Exception):
            return [{"error": str(resp)}]
        out = []
        for r in (resp.get("rows") or []):
            out.append({
                "key": (r.get("keys") or [""])[0],
                "clicks": int(r.get("clicks", 0)),
                "impressions": int(r.get("impressions", 0)),
                "ctr": round(r.get("ctr", 0), 4),
                "position": round(r.get("position", 0), 1),
            })
        return out

    return {
        "summary": parse_summary(summary),
        "top_queries": parse_rows(top_q),
        "top_pages": parse_rows(top_p),
        "blogs": {
            **parse_summary(blogs),
            "top_articles": parse_rows(blogs_top),
        },
    }


async def collect(start_date, end_date) -> dict:
    log.info("gsc.collect %s ~ %s", start_date, end_date)
    try:
        pk, fl = await asyncio.gather(
            _fetch_site(PK_SITE, start_date, end_date),
            _fetch_site(FL_SITE, start_date, end_date),
            return_exceptions=True,
        )
        if isinstance(pk, Exception):
            pk = {"error": f"{type(pk).__name__}: {pk}"}
        if isinstance(fl, Exception):
            fl = {"error": f"{type(fl).__name__}: {fl}"}
        return {"status": "ok", "data": {"powkong": pk, "funlab": fl,
                                          "window": f"{start_date}~{end_date}"}}
    except Exception as e:
        log.exception("gsc collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))
