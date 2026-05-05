"""Funlab Shopline collector.

数据源: Shopline Admin API (env SHOPLINE_FL_JWT, 2029 到期)
Phase 2 实现内容:
- orders 状态: cancelled/refunded/on_hold
- 注意中文显示 vs API 字段映射
"""
import logging

log = logging.getLogger("weekly_report.shopline")


async def collect(start_date, end_date) -> dict:
    """Funlab Shopline 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] shopline.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
