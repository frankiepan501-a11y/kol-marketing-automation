"""GA4 collector.

数据源: GA4 Reporting API (Property IDs PK 512451371 / FL 399581026)
Phase 2 实现内容:
- activeUsers / sessions / totalRevenue / ecommercePurchases / sessionConversionRate / bounceRate / averageEngagementTime
- UTM 流量过滤: utm_source IN [kol_dm, editor_pr, kol, editor]
- sessionDefaultChannelGroup 6 类来源
- 电商漏斗事件 (add_to_cart / begin_checkout / purchase)
"""
import logging

log = logging.getLogger("weekly_report.ga4")


async def collect(start_date, end_date) -> dict:
    """GA4 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] ga4.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
