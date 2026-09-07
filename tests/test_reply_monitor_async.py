import asyncio
from unittest.mock import AsyncMock

import pytest

from app import main


@pytest.fixture(autouse=True)
def _reset_jobs():
    old_token = main.config.INTERNAL_TOKEN
    main.config.INTERNAL_TOKEN = "unit-token"
    if hasattr(main, "_reply_monitor_jobs"):
        main._reply_monitor_jobs.clear()
    yield
    if hasattr(main, "_reply_monitor_jobs"):
        main._reply_monitor_jobs.clear()
    main.config.INTERNAL_TOKEN = old_token


def test_default_endpoint_returns_trackable_background_job(monkeypatch):
    run = AsyncMock(return_value={"processed": 0, "results": []})
    monkeypatch.setattr(main.reply_monitor, "run", run)

    async def exercise():
        accepted = await main.run_reply_monitor(authorization="Bearer unit-token")
        await asyncio.sleep(0.1)
        status = await main.get_reply_monitor_job(
            accepted["job_id"], authorization="Bearer unit-token",
        )
        return accepted, status

    accepted, status = asyncio.run(exercise())

    assert accepted["accepted"] is True
    assert accepted["status"] == "running"
    assert status["status"] == "success"
    assert status["result"]["processed"] == 0
    run.assert_awaited_once_with()


def test_duplicate_running_request_reuses_job(monkeypatch):
    release = asyncio.Event()

    async def slow_run():
        await release.wait()
        return {"processed": 0, "results": []}

    monkeypatch.setattr(main.reply_monitor, "run", slow_run)

    async def exercise():
        first = await main.run_reply_monitor(authorization="Bearer unit-token")
        second = await main.run_reply_monitor(authorization="Bearer unit-token")
        release.set()
        await asyncio.sleep(0)
        return first, second

    first, second = asyncio.run(exercise())

    assert first["already_running"] is False
    assert second["already_running"] is True
    assert second["job_id"] == first["job_id"]


def test_status_endpoint_rejects_wrong_token():
    with pytest.raises(main.HTTPException) as exc:
        asyncio.run(
            main.get_reply_monitor_job(
                "reply-monitor-missing", authorization="Bearer wrong-token",
            )
        )
    assert exc.value.status_code == 401


def test_single_sender_replay_is_passed_to_background_job(monkeypatch):
    run = AsyncMock(return_value={"processed": 0, "results": []})
    monkeypatch.setattr(main.reply_monitor, "run", run)

    async def exercise():
        accepted = await main.run_reply_monitor(
            authorization="Bearer unit-token",
            brand="FUNLAB",
            sender="known@example.com",
        )
        await asyncio.sleep(0.1)
        return await main.get_reply_monitor_job(
            accepted["job_id"], authorization="Bearer unit-token",
        )

    status = asyncio.run(exercise())

    assert status["status"] == "success"
    assert status["scope"] == {
        "brand": "FUNLAB",
        "sender": "known@example.com",
    }
    run.assert_awaited_once_with(
        only_brand="FUNLAB",
        only_sender="known@example.com",
    )


def test_timed_out_job_releases_running_slot(monkeypatch):
    release = asyncio.Event()

    async def stuck_run(**_kwargs):
        await release.wait()
        return {"processed": 0, "results": []}

    monkeypatch.setattr(main.reply_monitor, "run", stuck_run)
    monkeypatch.setattr(main, "_REPLY_MONITOR_JOB_TIMEOUT", 0.01)

    async def exercise():
        first = await main.run_reply_monitor(authorization="Bearer unit-token")
        await asyncio.sleep(0.03)
        first_status = await main.get_reply_monitor_job(
            first["job_id"], authorization="Bearer unit-token",
        )
        second = await main.run_reply_monitor(authorization="Bearer unit-token")
        release.set()
        await asyncio.sleep(0)
        return first, first_status, second

    first, first_status, second = asyncio.run(exercise())

    assert first_status["status"] == "error"
    assert first_status["error"] == "reply monitor exceeded 0.01 seconds"
    assert second["already_running"] is False
    assert second["job_id"] != first["job_id"]


def test_bounded_replay_rejects_invalid_sender_before_start(monkeypatch):
    run = AsyncMock(return_value={"processed": 0, "results": []})
    monkeypatch.setattr(main.reply_monitor, "run", run)

    with pytest.raises(main.HTTPException) as exc:
        asyncio.run(
            main.run_reply_monitor(
                authorization="Bearer unit-token",
                brand="FUNLAB",
                sender="not-an-email",
            )
        )

    assert exc.value.status_code == 400
    run.assert_not_awaited()


def test_different_scope_does_not_reuse_running_job(monkeypatch):
    release = asyncio.Event()

    async def slow_run(**_kwargs):
        await release.wait()
        return {"processed": 0, "results": []}

    monkeypatch.setattr(main.reply_monitor, "run", slow_run)

    async def exercise():
        first = await main.run_reply_monitor(authorization="Bearer unit-token")
        with pytest.raises(main.HTTPException) as exc:
            await main.run_reply_monitor(
                authorization="Bearer unit-token",
                brand="FUNLAB",
                sender="known@example.com",
            )
        release.set()
        await asyncio.sleep(0)
        return first, exc.value

    first, error = asyncio.run(exercise())

    assert first["already_running"] is False
    assert error.status_code == 409


def test_bounded_replay_fails_when_sender_not_in_recent_inbox(monkeypatch):
    monkeypatch.setattr(
        main.reply_monitor.zoho,
        "list_inbox",
        AsyncMock(return_value=[]),
    )

    with pytest.raises(ValueError, match="bounded replay matched 0 messages"):
        asyncio.run(
            main.reply_monitor.run(
                only_brand="FUNLAB",
                only_sender="known@example.com",
            )
        )
