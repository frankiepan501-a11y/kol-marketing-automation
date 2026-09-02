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


def test_youtube_tick_uses_its_own_flag_while_archive_downloads_stay_off(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", False)
    monkeypatch.setattr(main.config, "YOUTUBE_METRICS_ENABLED", True)
    monkeypatch.setattr(main.config, "YOUTUBE_DATA_API_KEY", "configured")
    refresh = AsyncMock(return_value={"commit": True, "updated": 1})
    monkeypatch.setattr(main.media_archive_controller, "refresh_youtube_metrics", refresh)

    result = asyncio.run(main.tick_media_archive_youtube_metrics(
        authorization="Bearer test-token",
    ))

    assert result["enabled"] is True
    assert result["updated"] == 1
    refresh.assert_awaited_once_with(commit=True)


def test_youtube_single_record_commit_uses_metrics_flag_and_filter(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", False)
    monkeypatch.setattr(main.config, "YOUTUBE_METRICS_ENABLED", True)
    refresh = AsyncMock(return_value={"commit": True, "updated": 1})
    monkeypatch.setattr(main.media_archive_controller, "refresh_youtube_metrics", refresh)

    result = asyncio.run(main.refresh_media_archive_youtube_metrics(
        authorization="Bearer test-token", commit=True, record_id="rec-1",
    ))

    assert result["updated"] == 1
    refresh.assert_awaited_once_with(commit=True, record_id="rec-1")


def test_intake_sync_preview_stays_available_when_write_flag_is_off(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_ENABLED", False)
    sync = AsyncMock(return_value={"commit": False, "created": 0})
    monkeypatch.setattr(main.upload_intake_sync, "sync", sync)

    result = asyncio.run(main.sync_media_archive_intake(
        authorization="Bearer test-token", commit=False, source_record_id="",
    ))

    assert result["ok"] is True
    sync.assert_awaited_once_with(commit=False, source_record_id="", max_creates=1)


def test_intake_sync_rejects_commit_when_write_flag_is_off(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_ENABLED", False)
    sync = AsyncMock()
    monkeypatch.setattr(main.upload_intake_sync, "sync", sync)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.sync_media_archive_intake(
            authorization="Bearer test-token", commit=True, source_record_id="src-1",
        ))

    assert exc.value.status_code == 503
    sync.assert_not_awaited()


def test_intake_tick_refuses_batch_write_without_a_nonzero_cutoff(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_ENABLED", True)
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_NOT_BEFORE_MS", 0)
    sync = AsyncMock()
    monkeypatch.setattr(main.upload_intake_sync, "sync", sync)

    result = asyncio.run(main.tick_media_archive_intake(authorization="Bearer test-token"))

    assert result["skipped"] is True
    assert result["reason"] == "cutoff_not_configured"
    sync.assert_not_awaited()


def test_intake_tick_passes_cutoff_and_batch_cap_after_both_gates_open(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_ENABLED", True)
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_NOT_BEFORE_MS", 123456)
    monkeypatch.setattr(main.config, "UPLOAD_INTAKE_SYNC_MAX_CREATES", 7)
    sync = AsyncMock(return_value={"commit": True, "created": 2})
    monkeypatch.setattr(main.upload_intake_sync, "sync", sync)

    result = asyncio.run(main.tick_media_archive_intake(authorization="Bearer test-token"))

    assert result["created"] == 2
    sync.assert_awaited_once_with(
        commit=True,
        source_record_id="",
        max_creates=7,
        created_not_before_ms=123456,
        allow_batch=True,
    )


def test_scan_commit_cannot_bypass_the_youtube_metrics_flag(monkeypatch):
    monkeypatch.setattr(main.config, "INTERNAL_TOKEN", "test-token")
    monkeypatch.setattr(main.config, "MEDIA_ARCHIVE_ENABLED", True)
    monkeypatch.setattr(main.config, "YOUTUBE_METRICS_ENABLED", False)
    scan = AsyncMock()
    monkeypatch.setattr(main.media_archive_controller, "scan", scan)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.run_media_archive_scan(
            authorization="Bearer test-token", commit=True, refresh_metrics=True,
        ))

    assert exc.value.status_code == 503
    scan.assert_not_awaited()


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
