"""Google Search Console collector.

数据源: GSC API (复用 GA4 service account)
Phase 2 实现内容:
- search_analytics: query/page/country/clicks/impressions/ctr/position
- 双站 sitemap index_status
- url_inspection (核心页)
"""
import logging

log = logging.getLogger("weekly_report.gsc")


async def collect(start_date, end_date) -> dict:
    """Google Search Console 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] gsc.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
