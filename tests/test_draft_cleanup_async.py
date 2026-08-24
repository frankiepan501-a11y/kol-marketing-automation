import asyncio
from unittest.mock import AsyncMock

import pytest

from app import draft_cleanup, main


@pytest.fixture(autouse=True)
def _reset_jobs(monkeypatch):
    old_token = main.config.INTERNAL_TOKEN
    main.config.INTERNAL_TOKEN = "unit-token"
    main._draft_cleanup_jobs.clear()
    yield
    main._draft_cleanup_jobs.clear()
    main.config.INTERNAL_TOKEN = old_token


def _draft(record_id, status, generated_at):
    return {
        "record_id": record_id,
        "fields": {"邮件草稿状态": status, "生成时间": generated_at},
    }


def test_dry_run_reports_candidates_without_deleting(monkeypatch):
    old = 1
    records = [
        _draft("reject-old", "已否决", old),
        _draft("failed-old", "发送失败", old),
        _draft("sent-old", "已发送", old),
    ]
    monkeypatch.setattr(draft_cleanup.feishu, "fetch_all_records", AsyncMock(return_value=records))
    delete = AsyncMock()
    monkeypatch.setattr(draft_cleanup.feishu, "api", delete)

    result = asyncio.run(draft_cleanup.run(days=30, dry_run=True))

    assert result["dry_run"] is True
    assert result["planned_delete"] == 2
    assert result["deleted"] == 0
    delete.assert_not_awaited()


def test_commit_keeps_existing_hard_protection(monkeypatch):
    records = [
        _draft("reject-old", "已否决", 1),
        _draft("sent-old", "已发送", 1),
    ]
    monkeypatch.setattr(draft_cleanup.feishu, "fetch_all_records", AsyncMock(return_value=records))
    delete = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr(draft_cleanup.feishu, "api", delete)

    result = asyncio.run(draft_cleanup.run(days=30, dry_run=False))

    assert result["planned_delete"] == 1
    assert result["deleted"] == 1
    payload = delete.await_args.args[2]
    assert payload == {"records": ["reject-old"]}


def test_endpoint_returns_trackable_background_job(monkeypatch):
    run = AsyncMock(return_value={"dry_run": True, "planned_delete": 7, "deleted": 0})
    monkeypatch.setattr(main.draft_cleanup, "run", run)

    async def exercise():
        accepted = await main.run_draft_cleanup(
            authorization="Bearer unit-token", days=30, dry_run=True,
        )
        await asyncio.sleep(0)
        status = await main.get_draft_cleanup_job(
            accepted["job_id"], authorization="Bearer unit-token",
        )
        return accepted, status

    accepted, status = asyncio.run(exercise())

    assert accepted["accepted"] is True
    assert accepted["status"] == "running"
    assert status["status"] == "success"
    assert status["result"]["planned_delete"] == 7
    run.assert_awaited_once_with(days=30, dry_run=True)


def test_duplicate_running_request_reuses_job(monkeypatch):
    release = asyncio.Event()

    async def slow_run(days=30, dry_run=False):
        await release.wait()
        return {"dry_run": dry_run, "planned_delete": 0, "deleted": 0}

    monkeypatch.setattr(main.draft_cleanup, "run", slow_run)

    async def exercise():
        first = await main.run_draft_cleanup(
            authorization="Bearer unit-token", days=30, dry_run=True,
        )
        second = await main.run_draft_cleanup(
            authorization="Bearer unit-token", days=30, dry_run=True,
        )
        release.set()
        await asyncio.sleep(0)
        return first, second

    first, second = asyncio.run(exercise())

    assert first["already_running"] is False
    assert second["already_running"] is True
    assert second["job_id"] == first["job_id"]
