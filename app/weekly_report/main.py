"""Weekly report 主入口.

流程: 时间区间 → 并发数据收集 (10 collectors) → AI 整合 → HTML 渲染 → 飞书发布

每个 collector 当前是 stub (Phase 1.1), 在 Phase 2 逐个实现.
"""
import asyncio
import datetime
import logging

from .collectors import (
    shopify, shopline, ga4, gsc, meta_ads,
    kol_bitable, zoho_complaints, n8n_executions, psi, history,
)
from . import integrator, renderer, publisher

log = logging.getLogger("weekly_report")


def _resolve_week(start_date=None, end_date=None):
    """默认上一完整周 (上周一 ~ 上周日)."""
    today = datetime.date.today()
    if end_date and start_date:
        return start_date, end_date
    last_sun = today - datetime.timedelta(days=today.weekday() + 1)  # 上周日
    last_mon = last_sun - datetime.timedelta(days=6)  # 上周一
    return last_mon, last_sun


async def run(dry_run: bool = False, start_date=None, end_date=None) -> dict:
    """主入口."""
    start, end = _resolve_week(start_date, end_date)
    log.info("weekly-report start: %s ~ %s (dry_run=%s)", start, end, dry_run)

    # 并发收集. return_exceptions=True 让单个 collector 失败不拖死全部.
    raw = await asyncio.gather(
        shopify.collect(start, end),
        shopline.collect(start, end),
        ga4.collect(start, end),
        gsc.collect(start, end),
        meta_ads.collect(start, end),
        kol_bitable.collect(start, end),
        zoho_complaints.collect(start, end),
        n8n_executions.collect(start, end),
        psi.collect(start, end),
        history.collect(start, end),
        return_exceptions=True,
    )
    keys = ["shopify", "shopline", "ga4", "gsc", "meta_ads",
            "kol", "complaints", "n8n", "psi", "history"]
    collected = {}
    gaps = []
    for k, r in zip(keys, raw):
        if isinstance(r, Exception):
            collected[k] = {"status": "error", "error": str(r)}
            gaps.append(f"{k}: {type(r).__name__}: {r}")
        else:
            collected[k] = r
            if r.get("status") not in ("ok", "stub"):
                gaps.append(f"{k}: {r.get('status')} - {r.get('error', '')}")

    log.info("collectors done. gaps=%d / %d", len(gaps), len(keys))

    # 整合
    md = await integrator.build_markdown(collected, start, end, gaps)

    # 渲染
    html = await renderer.render(md, collected, start, end)

    # dry-run 跳过发布
    if dry_run:
        return {
            "ok": True, "dry_run": True,
            "week": f"{start}~{end}",
            "html_size": len(html),
            "md_preview": md[:500],
            "gaps": gaps,
            "collectors_summary": {k: collected[k].get("status") for k in keys},
        }

    # 真发布
    result = await publisher.publish(html, md, collected, start, end, gaps)
    return {"ok": True, "week": f"{start}~{end}", "gaps_count": len(gaps), **result}


if __name__ == "__main__":
    # 本地 dry-run 测试: python -m app.weekly_report.main
    print(asyncio.run(run(dry_run=True)))
