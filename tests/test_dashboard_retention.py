import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

from app import dashboard


def _record(record_id, day, dimension="关键词"):
    dt = datetime.combine(day, datetime.min.time(), tzinfo=dashboard.BJ_TZ)
    return {
        "record_id": record_id,
        "fields": {"统计日期": int(dt.timestamp() * 1000), "维度类型": dimension},
    }


def test_retention_keeps_recent_detail_and_only_old_overview_checkpoints():
    now = datetime(2026, 8, 19, tzinfo=dashboard.BJ_TZ)
    today = now.date()
    cutoff = today - timedelta(days=29)
    old_month_end = datetime(2026, 6, 30).date()
    old_same_week_later = datetime(2026, 7, 2).date()
    records = [
        _record("today-old", today, "总览"),
        _record("recent-detail", cutoff, "关键词"),
        _record("old-month-overview", old_month_end, "总览"),
        _record("old-month-detail", old_month_end, "关键词"),
        _record("old-week-latest-overview", old_same_week_later, "总览"),
        _record("old-week-latest-detail", old_same_week_later, "关键词"),
        {"record_id": "invalid", "fields": {"统计日期": "bad", "维度类型": "关键词"}},
    ]

    plan = dashboard.plan_snapshot_retention(records, now)

    assert plan["cutoff_date"] == "2026-07-21"
    assert plan["today_delete_ids"] == ["today-old"]
    assert "recent-detail" not in plan["historical_delete_ids"]
    assert "old-month-overview" not in plan["historical_delete_ids"]
    assert "old-week-latest-overview" not in plan["historical_delete_ids"]
    assert set(plan["historical_delete_ids"]) == {"old-month-detail", "old-week-latest-detail"}
    assert plan["invalid_date_records"] == 1


def test_retention_deletes_older_non_checkpoint_overview():
    now = datetime(2026, 8, 19, tzinfo=dashboard.BJ_TZ)
    records = [
        _record("week-earlier", datetime(2026, 6, 23).date(), "总览"),
        _record("week-latest", datetime(2026, 6, 28).date(), "总览"),
    ]

    plan = dashboard.plan_snapshot_retention(records, now)

    assert plan["historical_delete_ids"] == ["week-earlier"]
    assert plan["kept_weekly_overview"] == 1


def test_snapshot_day_parses_utc_and_naive_iso_as_beijing_day():
    assert dashboard._snapshot_day("2026-08-18T16:00:00Z").isoformat() == "2026-08-19"
    assert dashboard._snapshot_day("2026-08-19T00:00:00").isoformat() == "2026-08-19"


def test_cleanup_retention_defaults_to_preview(monkeypatch):
    old = _record("old-detail", datetime(2026, 6, 1).date(), "关键词")
    fetch = AsyncMock(return_value=[old])
    delete = AsyncMock()
    monkeypatch.setattr(dashboard.feishu, "fetch_all_records", fetch)
    monkeypatch.setattr(dashboard, "_delete_dashboard_records", delete)

    result = asyncio.run(dashboard.cleanup_retention())

    assert result["commit"] is False
    assert result["planned_historical_delete"] == 1
    assert result["deleted"] == 0
    delete.assert_not_awaited()


def test_cleanup_retention_requires_explicit_commit_to_delete(monkeypatch):
    old = _record("old-detail", datetime(2026, 6, 1).date(), "关键词")
    monkeypatch.setattr(dashboard.feishu, "fetch_all_records", AsyncMock(return_value=[old]))
    delete = AsyncMock()
    monkeypatch.setattr(dashboard, "_delete_dashboard_records", delete)

    result = asyncio.run(dashboard.cleanup_retention(commit=True))

    assert result["deleted"] == 1
    delete.assert_awaited_once_with(["old-detail"])
