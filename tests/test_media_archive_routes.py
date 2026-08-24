import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app import main


def test_scan_route_stays_dry_run_when_feature_flag_is_off(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", False)
    scan = AsyncMock(return_value={"commit": False, "queued_groups": 1})
    monkeypatch.setattr(main.media_archive_controller, "scan", scan)

    result = asyncio.run(main.run_media_archive_scan(
        authorization="Bearer test-token", commit=False, refresh_metrics=False,
    ))

    assert result["ok"] is True
    scan.assert_awaited_once_with(commit=False, refresh_metrics=False)


def test_scan_route_rejects_commit_when_feature_flag_is_off(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", False)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.run_media_archive_scan(
            authorization="Bearer test-token", commit=True, refresh_metrics=False,
        ))

    assert exc.value.status_code == 503


def test_archive_tick_is_a_safe_noop_until_grey_flag_is_enabled(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", False)
    scan = AsyncMock()
    monkeypatch.setattr(main.media_archive_controller, "scan", scan)

    result = asyncio.run(main.run_media_archive_tick(
        authorization="Bearer test-token",
    ))

    assert result == {"ok": True, "enabled": False, "skipped": True}
    scan.assert_not_awaited()


def test_archive_tick_commits_queue_scan_only_after_flag_is_enabled(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", True)
    scan = AsyncMock(return_value={"commit": True, "queued_groups": 1})
    monkeypatch.setattr(main.media_archive_controller, "scan", scan)

    result = asyncio.run(main.run_media_archive_tick(
        authorization="Bearer test-token",
    ))

    assert result["queued_groups"] == 1
    scan.assert_awaited_once_with(commit=True, refresh_metrics=False)


def test_worker_claim_requires_enabled_feature_and_returns_job(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", True)
    claim = AsyncMock(return_value={"record_id": "rec-1", "job_id": "archive-1"})
    monkeypatch.setattr(main.media_archive_controller, "claim_job", claim)

    result = asyncio.run(main.claim_media_archive_job(
        request=type("Request", (), {"json": AsyncMock(return_value={
            "worker_id": "old-terminal-grey", "record_id": "rec-1",
        })})(),
        authorization="Bearer test-token",
    ))

    assert result["ok"] is True
    assert result["job"]["record_id"] == "rec-1"
    claim.assert_awaited_once_with(
        worker_id="old-terminal-grey", record_id="rec-1", commit=True,
    )


def test_worker_complete_forwards_direct_result_fields(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", True)
    complete = AsyncMock(return_value={"updated_records": 2})
    monkeypatch.setattr(main.media_archive_controller, "complete_job", complete)
    payload = {
        "job_id": "archive-1",
        "record_id": "rec-1",
        "source_group": "source-1",
        "result_fields": {
            "飞书file_token": "file-1",
            "归档文件链接": {"link": "https://example.feishu.cn/file/file-1", "text": "video"},
        },
    }

    result = asyncio.run(main.complete_media_archive_job(
        request=type("Request", (), {"json": AsyncMock(return_value=payload)})(),
        authorization="Bearer test-token",
    ))

    assert result == {"ok": True, "updated_records": 2}
    complete.assert_awaited_once_with(
        record_id="rec-1",
        source_group="source-1",
        job_id="archive-1",
        result_fields=payload["result_fields"],
        commit=True,
    )
