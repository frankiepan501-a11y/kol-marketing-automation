"""上周 baseline collector.

数据源:「SEO 周报历史数据」Bitable
- app_token: KiQQbf7HxaT8TKsYToecfo86noc
- table_id:  tblp8TQhBnWI7Ax9
- 21 字段 (周次/起始日期/文档链接/双站 9 个核心指标含周变化%)

注意: 此 base 与 KOL 营销库不同 (KOL 营销库 = config.FEISHU_APP_TOKEN), 用 feishu.api 直调任意 path.

输出 collected.history.data:
{
  "last_week": {  # 上周 record 的所有字段值, 给 integrator 算环比
    "周次": "2026-04-20 ~ 2026-04-26",
    "起始日期": "2026-04-20",
    ... (双站 9 个核心指标)
  } | None,
  "two_weeks_ago": {同上} | None,  # 给 integrator 算"上周 vs 上上周变化"
  "available": bool,
}
"""
import asyncio
import datetime
import logging

log = logging.getLogger("weekly_report.history")

HISTORY_APP = "KiQQbf7HxaT8TKsYToecfo86noc"
HISTORY_TABLE = "tblp8TQhBnWI7Ax9"


def _ext(f):
    """飞书 field unwrap (inline copy)."""
    if f is None:
        return ""
    if isinstance(f, list):
        if not f:
            return ""
        if isinstance(f[0], dict):
            return f[0].get("text") or f[0].get("link") or f[0].get("name") or ""
        return str(f[0])
    if isinstance(f, dict):
        return f.get("text") or f.get("link") or ""
    return f or ""


async def _fetch_records() -> list:
    """拉所有历史 records (按起始日期排序)."""
    from app import feishu
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{HISTORY_APP}/tables/{HISTORY_TABLE}/records?page_size=100"
        if page_token:
            path += f"&page_token={page_token}"
        r = await feishu.api("GET", path)
        d = r.get("data") or {}
        items.extend(d.get("items") or [])
        if not d.get("has_more"):
            break
        page_token = d.get("page_token", "")
        if not page_token:
            break
    return items


def _parse_start_date(rec: dict) -> datetime.date:
    """从 record 里抽出起始日期. 飞书日期字段是 ms 时间戳."""
    f = rec.get("fields", {})
    raw = f.get("起始日期")
    if isinstance(raw, (int, float)):
        return datetime.date.fromtimestamp(raw / 1000)
    if isinstance(raw, str):
        try:
            return datetime.date.fromisoformat(raw[:10])
        except ValueError:
            return datetime.date.min
    return datetime.date.min


def _find_record_for_week(records: list, target_start: datetime.date) -> dict:
    """从 records 里找起始日期 = target_start 的那条 (容差 ±1 天)."""
    for rec in records:
        d = _parse_start_date(rec)
        if abs((d - target_start).days) <= 1:
            return rec
    return None


async def collect(start_date, end_date) -> dict:
    log.info("history.collect %s ~ %s", start_date, end_date)
    try:
        records = await _fetch_records()
        if not records:
            return {
                "status": "ok",
                "data": {"last_week": None, "two_weeks_ago": None,
                          "available": False, "note": "history bitable 空, 第一次跑没 baseline"},
            }

        # start_date 是本周一. 上周一 = start_date - 7d, 上上周一 = -14d.
        last_start = start_date - datetime.timedelta(days=7)
        two_start = start_date - datetime.timedelta(days=14)

        last_rec = _find_record_for_week(records, last_start)
        two_rec = _find_record_for_week(records, two_start)

        return {
            "status": "ok",
            "data": {
                "last_week": last_rec.get("fields") if last_rec else None,
                "two_weeks_ago": two_rec.get("fields") if two_rec else None,
                "available": last_rec is not None,
                "total_history_records": len(records),
                "window": f"{start_date}~{end_date}",
            },
        }
    except Exception as e:
        log.exception("history collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))
