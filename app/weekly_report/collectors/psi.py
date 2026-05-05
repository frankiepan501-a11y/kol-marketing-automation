"""Lighthouse PageSpeed collector.

数据源: PSI API (env PSI_API_KEY, claude-analytics-489703)
Phase 2 实现内容:
- 4 核心页 x 4 维度 (Performance/A11y/BP/SEO) + Performance 6 子项
- Top 3 opportunities (按 overallSavingsMs 排序)
- 配额 25000/天, 4 页 ~2 分钟
"""
import logging

log = logging.getLogger("weekly_report.psi")


async def collect(start_date, end_date) -> dict:
    """Lighthouse PageSpeed 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] psi.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
