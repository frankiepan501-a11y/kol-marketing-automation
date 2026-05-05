"""Powkong Shopify collector.

数据源: Shopify Admin GraphQL (env SHOPIFY_PK_ACCESS_TOKEN)
Phase 2 实现内容:
- orders / refunds / discount_codes / inventory
- 关键字段: 销售/订单/客户/已发货/回头客率/访客
"""
import logging

log = logging.getLogger("weekly_report.shopify")


async def collect(start_date, end_date) -> dict:
    """Powkong Shopify 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] shopify.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
