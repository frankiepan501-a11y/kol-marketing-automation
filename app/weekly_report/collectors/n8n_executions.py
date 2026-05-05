"""n8n 工作流执行状态 collector.

数据源: n8n REST API (env N8N_API_KEY)
Phase 2 实现内容:
- 6 个工作流: ee779GzBI8Bj4Bx3 / bxqthAOVFjGviUEr / PEzTmqGwOqcHOPfc / xLEIAVos3YmynRsq / 9gMvXqs3mjS1zBZJ / z8OmSc1gWqc9cnsH
- ⚠️ Zeabur 默认 prune 2-3 天, 拿全周需调 EXECUTIONS_DATA_PRUNE_MAX_AGE > 7d
"""
import logging

log = logging.getLogger("weekly_report.n8n_executions")


async def collect(start_date, end_date) -> dict:
    """n8n 工作流执行状态 数据收集.

    Returns:
        {
            "status": "ok" | "stub" | "error",
            "data": { ... },  # 结构化数据供 integrator 用
            "error": str (如失败),
        }
    """
    log.info("[STUB] n8n_executions.collect %s ~ %s", start_date, end_date)
    return {"status": "stub", "data": {}, "note": "Phase 2 待实现"}


if __name__ == "__main__":
    import asyncio, datetime
    today = datetime.date.today()
    print(asyncio.run(collect(today, today)))
