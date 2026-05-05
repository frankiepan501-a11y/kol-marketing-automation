"""Meta Ads collector.

数据源: Meta Marketing API (PK act_1498442934673297 / FL act_1705425610151698)
Phase 2 实现内容:
- account insights / 每日 ROAS / 漏斗 (曝光→点击→落地→加购→结账→购买)
- 与 meta_ads_s1_weekly.py 已有代码可复用
"""
import logging

log = logging.getLogger("weekly_report.meta_ads")


async def collect(start_date, end_date) -> dict:
    """Meta Ads 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] meta_ads.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
