"""KOL/媒体人 Bitable collector.

数据源: 复用 app.config 读 KOL 营销库 (KINabIENjak8fRsB6AHcIDALntc) + 选题池 (CPvwbGznza5L4ZsgBG8cULcinne)
Phase 2 实现内容:
- KOL 端: 派单/富化/已发/回复/意向率/拒绝率/寄样
- 媒体人端: 同上 + 媒体集团分布
- 选题池: 候选词按状态分布 + 本周新增/消费
"""
import logging

log = logging.getLogger("weekly_report.kol_bitable")


async def collect(start_date, end_date) -> dict:
    """KOL/媒体人 Bitable 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] kol_bitable.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
