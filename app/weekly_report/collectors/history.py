"""上周 baseline collector.

数据源: 「SEO 周报历史数据」Bitable (KiQQbf7HxaT8TKsYToecfo86noc / tblp8TQhBnWI7Ax9)
Phase 2 实现内容:
- 拿上周 record 作环比基准
- 21 字段全填的话直接对比双站 9 个核心指标
"""
import logging

log = logging.getLogger("weekly_report.history")


async def collect(start_date, end_date) -> dict:
    """上周 baseline 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] history.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
