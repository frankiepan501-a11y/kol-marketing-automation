import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from app import media_archive_controller as controller


def _row(record_id, platform="YouTube", url="https://youtu.be/nUkwNTRFJBc",
         group="source-1", state="", enabled=True, status="待下载"):
    return {
        "record_id": record_id,
        "fields": {
            "发布平台": platform,
            "作品链接": url,
            "同源作品组": group,
            "关联KOL": [{"record_ids": ["kol-1"]}],
            "关联产品": [{"record_ids": ["product-1"]}],
            "品牌": "FUNLAB",
            "允许自动归档": enabled,
            "自动处理状态": state,
            "归档状态": status,
            "重试次数": 0,
        },
    }


def test_scan_preview_plans_cross_platform_group_without_writes(monkeypatch):
    rows = [
        _row("yt"),
        _row("ig", "Instagram", "https://www.instagram.com/reel/ABC123/"),
        _row("tk", "TikTok", "https://www.tiktok.com/@creator/video/7491234567890123456"),
    ]
    monkeypatch.setattr(controller.feishu, "fetch_all_records", AsyncMock(return_value=rows))
    update = AsyncMock()
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.scan(commit=False, refresh_metrics=False))

    assert result["commit"] is False
    assert result["queued_groups"] == 1
    assert result["plans"][0]["master_record_id"] == "yt"
    update.assert_not_awaited()


def test_scan_commit_marks_one_master_and_waiting_followers(monkeypatch):
    rows = [
        _row("yt"),
        _row("ig", "Instagram", "https://www.instagram.com/reel/ABC123/"),
    ]
    monkeypatch.setattr(controller.feishu, "fetch_all_records", AsyncMock(return_value=rows))
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.scan(commit=True, refresh_metrics=False))

    assert result["queued_groups"] == 1
    calls = update.await_args_list
    assert any(call.args[1] == "yt" and call.args[2].get("同源主素材") is True for call in calls)
    assert any(call.args[1] == "yt" and call.args[2].get("自动处理状态") == "待执行" for call in calls)
    assert any(call.args[1] == "ig" and call.args[2].get("自动处理状态") == "等待同源母版" for call in calls)


def test_scan_does_not_requeue_a_group_that_is_already_processing(monkeypatch):
    master = _row("yt", state="处理中")
    follower = _row(
        "ig", "Instagram", "https://www.instagram.com/reel/ABC123/",
        state="等待同源母版",
    )
    monkeypatch.setattr(
        controller.feishu, "fetch_all_records", AsyncMock(return_value=[master, follower]),
    )
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.scan(commit=True, refresh_metrics=False))

    assert result["queued_groups"] == 0
    update.assert_not_awaited()


def test_refresh_youtube_metrics_writes_latest_and_due_snapshot(monkeypatch):
    row = _row("yt")
    row["fields"]["平台作品ID"] = "nUkwNTRFJBc"
    monkeypatch.setattr(controller.config, "T_MEDIA_ARCHIVE_SNAPSHOT", "tbl-snapshot")
    monkeypatch.setattr(controller.feishu, "fetch_all_records", AsyncMock(side_effect=[[row], []]))
    monkeypatch.setattr(controller, "fetch_youtube_videos", AsyncMock(return_value={
        "nUkwNTRFJBc": {
            "id": "nUkwNTRFJBc",
            "snippet": {"title": "A title", "publishedAt": "2026-08-20T12:00:00Z"},
            "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "7"},
        }
    }))
    update = AsyncMock(return_value={})
    create = AsyncMock(return_value="snapshot-1")
    monkeypatch.setattr(controller.feishu, "update_record", update)
    monkeypatch.setattr(controller.feishu, "create_record", create)

    result = asyncio.run(controller.refresh_youtube_metrics(
        commit=True,
        record_id="yt",
        now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
    ))

    assert result["updated"] == 1
    assert result["snapshots_created"] == 1
    latest_fields = update.await_args_list[0].args[2]
    assert latest_fields["播放量"] == 1000
    assert latest_fields["点赞量"] == 50
    assert latest_fields["评论数"] == 7
    assert update.await_args_list[1].args[2] == {"数据抓取状态": "已更新"}
    snapshot_fields = create.await_args.args[1]
    assert snapshot_fields["关联作品"] == ["yt"]
    assert snapshot_fields["上稿后天数"] == 1
    assert snapshot_fields["数据源"] == "YouTube Data API"


def test_refresh_youtube_metrics_can_limit_a_commit_to_one_record(monkeypatch):
    selected = _row("yt-selected")
    selected["fields"]["平台作品ID"] = "selected-id"
    other = _row("yt-other", url="https://youtu.be/other-id")
    other["fields"]["平台作品ID"] = "other-id"
    monkeypatch.setattr(controller.config, "T_MEDIA_ARCHIVE_SNAPSHOT", "")
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(return_value=[selected, other]),
    )
    fetch = AsyncMock(return_value={
        "selected-id": {
            "id": "selected-id",
            "snippet": {"title": "Selected", "publishedAt": "2026-08-20T12:00:00Z"},
            "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
        },
    })
    monkeypatch.setattr(controller, "fetch_youtube_videos", fetch)
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.refresh_youtube_metrics(
        commit=True, record_id="yt-selected",
        now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
    ))

    assert result["youtube_rows"] == 1
    assert result["updated"] == 1
    fetch.assert_awaited_once_with(["selected-id"])
    assert all(call.args[1] == "yt-selected" for call in update.await_args_list)


def test_full_youtube_refresh_uses_three_bounded_batch_writes(monkeypatch):
    first = _row("yt-1")
    first["fields"]["平台作品ID"] = "video-1"
    second = _row("yt-2", url="https://youtu.be/video-2")
    second["fields"]["平台作品ID"] = "video-2"
    monkeypatch.setattr(controller.config, "T_MEDIA_ARCHIVE_SNAPSHOT", "tbl-snapshot")
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[first, second], []]),
    )
    monkeypatch.setattr(controller, "fetch_youtube_videos", AsyncMock(return_value={
        "video-1": {
            "id": "video-1",
            "snippet": {"title": "One", "publishedAt": "2026-08-20T12:00:00Z"},
            "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
        },
        "video-2": {
            "id": "video-2",
            "snippet": {"title": "Two", "publishedAt": "2026-08-20T12:00:00Z"},
            "statistics": {"viewCount": "200", "likeCount": "9", "commentCount": "2"},
        },
    }))
    batch_update = AsyncMock(return_value={})
    batch_create = AsyncMock(return_value=[])
    monkeypatch.setattr(
        controller.feishu, "batch_update_records", batch_update, raising=False,
    )
    monkeypatch.setattr(
        controller.feishu, "batch_create_records", batch_create, raising=False,
    )
    update = AsyncMock()
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.refresh_youtube_metrics(
        commit=True,
        now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
    ))

    assert result["updated"] == 2
    assert result["snapshots_created"] == 2
    assert batch_update.await_count == 2
    data_rows = batch_update.await_args_list[0].args[1]
    status_rows = batch_update.await_args_list[1].args[1]
    assert {row["record_id"] for row in data_rows} == {"yt-1", "yt-2"}
    assert all("播放量" in row["fields"] for row in data_rows)
    assert all(row["fields"] == {"数据抓取状态": "已更新"} for row in status_rows)
    assert batch_create.await_count == 1
    update.assert_not_awaited()


def test_claim_job_builds_filename_and_marks_processing(monkeypatch):
    row = _row("yt", state="待执行")
    monkeypatch.setattr(controller.feishu, "fetch_all_records", AsyncMock(return_value=[row]))

    async def get_record(table_id, record_id):
        if record_id == "kol-1":
            return {"fields": {"账号名": "Amrie47"}}
        return {"fields": {"产品名": "Long product title", "素材归档名": "YS11-5-戴夫"}}

    monkeypatch.setattr(controller.feishu, "get_record", get_record)
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    job = asyncio.run(controller.claim_job(worker_id="old-terminal-grey", commit=True))

    assert job["record_id"] == "yt"
    assert job["filename"] == "YT-Amrie47-YS11-5-戴夫-01.mp4"
    assert job["product_folder_name"] == "YS11-5-戴夫"
    assert job["brand_folder_token"] == controller.BRAND_FOLDER_TOKENS["FUNLAB"]
    calls = update.await_args_list
    assert any(call.args[2].get("处理终端") == "old-terminal-grey" for call in calls)
    assert any(call.args[2] == {"自动处理状态": "处理中"} for call in calls)


def test_status_writer_clears_a_stale_failure_stage(monkeypatch):
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    asyncio.run(controller._update_status("rec-1", {
        "自动处理状态": "待执行",
        "失败环节": None,
        "失败原因": "",
    }))

    assert any(call.args[2] == {"失败环节": None} for call in update.await_args_list)


def test_worker_heartbeat_writes_data_before_status_select(monkeypatch):
    monkeypatch.setattr(controller.config, "T_MEDIA_ARCHIVE_WORKER", "tbl-worker")
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(return_value=[{
            "record_id": "worker-row",
            "fields": {"Worker ID": "old-terminal-grey"},
        }]),
    )
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    asyncio.run(controller.heartbeat(
        worker_id="old-terminal-grey",
        version="0.2.1",
        host="OLD-PC",
        status="忙碌",
        last_record_id="rec-1",
    ))

    assert update.await_count == 2
    assert "状态" not in update.await_args_list[0].args[2]
    assert update.await_args_list[1].args[2] == {"状态": "忙碌"}


def test_complete_job_backfills_every_source_row_in_two_phases(monkeypatch):
    rows = [_row("yt", group="source-1"), _row("ig", "Instagram", "https://www.instagram.com/reel/ABC123/", group="source-1")]
    rows[0]["fields"].update({"自动处理状态": "处理中", "归档任务ID": "archive-1"})
    monkeypatch.setattr(controller.feishu, "fetch_all_records", AsyncMock(return_value=rows))
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)
    result_fields = {
        "归档文件名": "YT-Amrie47-YS11-5-戴夫-01.mp4",
        "归档文件链接": {"link": "https://u1wpma3xuhr.feishu.cn/file/file123", "text": "video"},
        "飞书file_token": "file123",
        "视频分辨率": "1440×2560",
        "归档状态": "已归档",
        "自动处理状态": "已完成",
    }

    result = asyncio.run(controller.complete_job(
        record_id="yt", source_group="source-1", job_id="archive-1",
        result_fields=result_fields, commit=True,
    ))

    assert result["updated_records"] == 2
    assert update.await_count == 6
    for record_id in ("yt", "ig"):
        calls = [call.args[2] for call in update.await_args_list if call.args[1] == record_id]
        assert any(fields.get("飞书file_token") == "file123" for fields in calls)
        assert {"归档状态": "已归档"} in calls
        assert {"自动处理状态": "已完成"} in calls


def test_complete_job_limits_same_source_backfill_to_anchor_kol_and_product(monkeypatch):
    anchor = _row("yt", group="reused-label")
    anchor["fields"].update({"自动处理状态": "处理中", "归档任务ID": "archive-1"})
    follower = _row(
        "ig", "Instagram", "https://www.instagram.com/reel/ABC123/",
        group="reused-label",
    )
    unrelated = _row(
        "tk", "TikTok", "https://www.tiktok.com/@other/video/7491234567890123456",
        group="reused-label",
    )
    unrelated["fields"]["关联产品"] = [{"record_ids": ["product-2"]}]
    monkeypatch.setattr(
        controller.feishu, "fetch_all_records",
        AsyncMock(return_value=[anchor, follower, unrelated]),
    )
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.complete_job(
        record_id="yt",
        source_group="reused-label",
        job_id="archive-1",
        result_fields={
            "归档文件链接": {
                "link": "https://u1wpma3xuhr.feishu.cn/file/file123",
                "text": "video",
            },
            "飞书file_token": "file123",
        },
        commit=True,
    ))

    assert result["record_ids"] == ["yt", "ig"]
    assert all(call.args[1] != "tk" for call in update.await_args_list)


def test_complete_job_backfills_direct_file_link_to_unified_source_after_all_siblings_finish(monkeypatch):
    anchor = _row("yt", group="source-group", state="处理中")
    anchor["fields"].update({"归档任务ID": "archive-1", "来源记录ID": "src-1#YT"})
    follower = _row(
        "ig", "Instagram", "https://www.instagram.com/reel/ABC123/",
        group="source-group",
    )
    follower["fields"]["来源记录ID"] = "src-1#IG"
    source = {
        "record_id": "src-1",
        "fields": {
            "上稿平台链接": (
                "https://youtu.be/nUkwNTRFJBc\n"
                "https://www.instagram.com/reel/ABC123/"
            ),
            "飞书云盘链接": "",
            "素材情况": "已上稿",
        },
    }
    monkeypatch.setattr(controller.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[anchor, follower], [source]]),
    )
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.complete_job(
        record_id="yt",
        source_group="source-group",
        job_id="archive-1",
        result_fields={
            "归档文件名": "YT-Creator-Product-01.mp4",
            "归档文件链接": "https://u1wpma3xuhr.feishu.cn/file/file123",
            "飞书file_token": "file123",
        },
        commit=True,
    ))

    source_calls = [call.args for call in update.await_args_list if call.args[0] == "source-table"]
    assert source_calls == [
        (
            "source-table",
            "src-1",
            {"飞书云盘链接": {
                "link": "https://u1wpma3xuhr.feishu.cn/file/file123",
                "text": "YT-Creator-Product-01.mp4",
            }},
        ),
        ("source-table", "src-1", {"素材情况": "已下载"}),
    ]
    assert result["source_backfill"]["updated"] == 1


def test_source_backfill_reconciliation_does_not_mark_a_pending_source_complete(monkeypatch):
    archived = _row("yt", group="group-1", state="已完成", status="已归档")
    archived["fields"].update({
        "来源记录ID": "src-1#YT",
        "归档文件链接": "https://u1wpma3xuhr.feishu.cn/file/file123",
        "归档文件名": "YT-Creator-Product-01.mp4",
    })
    pending = _row(
        "tk", "TikTok", "https://www.tiktok.com/@creator/video/7491234567890123456",
        group="group-2", state="", status="待下载",
    )
    pending["fields"]["来源记录ID"] = "src-1#TK"
    monkeypatch.setattr(controller.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(return_value=[{
            "record_id": "src-1",
            "fields": {"上稿平台链接": (
                "https://youtu.be/nUkwNTRFJBc\n"
                "https://www.tiktok.com/@creator/video/7491234567890123456"
            )},
        }]),
    )
    update = AsyncMock(return_value={})
    monkeypatch.setattr(controller.feishu, "update_record", update)

    result = asyncio.run(controller.reconcile_source_backfills([archived, pending], commit=True))

    assert result["updated"] == 0
    assert result["pending"] == 1
    update.assert_not_awaited()


def test_complete_job_rejects_a_stale_worker_callback(monkeypatch):
    anchor = _row("yt", group="source-1", state="处理中")
    anchor["fields"]["归档任务ID"] = "archive-current"
    monkeypatch.setattr(
        controller.feishu, "fetch_all_records", AsyncMock(return_value=[anchor]),
    )

    try:
        asyncio.run(controller.complete_job(
            record_id="yt",
            source_group="source-1",
            job_id="archive-old",
            result_fields={
                "归档文件链接": {
                    "link": "https://u1wpma3xuhr.feishu.cn/file/file123",
                    "text": "video",
                },
                "飞书file_token": "file123",
            },
            commit=True,
        ))
    except ValueError as exc:
        assert "job_id" in str(exc)
    else:
        raise AssertionError("stale callbacks must not overwrite a newer archive job")


def test_complete_job_rejects_worker_attempt_to_write_unrelated_business_fields(monkeypatch):
    anchor = _row("yt", group="source-1", state="处理中")
    anchor["fields"]["归档任务ID"] = "archive-1"
    monkeypatch.setattr(
        controller.feishu, "fetch_all_records", AsyncMock(return_value=[anchor]),
    )

    try:
        asyncio.run(controller.complete_job(
            record_id="yt",
            source_group="source-1",
            job_id="archive-1",
            result_fields={
                "归档文件链接": {
                    "link": "https://u1wpma3xuhr.feishu.cn/file/file123",
                    "text": "video",
                },
                "飞书file_token": "file123",
                "作品名称": "must not be changed by worker",
            },
            commit=True,
        ))
    except ValueError as exc:
        assert "unexpected result fields" in str(exc)
    else:
        raise AssertionError("worker result writes must be restricted to archive fields")


def test_fail_job_rejects_a_stale_worker_callback(monkeypatch):
    anchor = _row("yt", group="source-1", state="处理中")
    anchor["fields"].update({"归档任务ID": "archive-current", "重试次数": 1})
    monkeypatch.setattr(controller.feishu, "get_record", AsyncMock(return_value=anchor))

    try:
        asyncio.run(controller.fail_job(
            record_id="yt",
            job_id="archive-old",
            stage="高画质下载",
            error="old attempt failed late",
            commit=True,
        ))
    except ValueError as exc:
        assert "job_id" in str(exc)
    else:
        raise AssertionError("a stale failure must not reset a newer archive job")


def test_worker_health_audit_flags_a_queue_without_a_fresh_terminal(monkeypatch):
    queued = _row("yt", state="待执行", enabled=True)
    stale_heartbeat = {
        "record_id": "worker-1",
        "fields": {
            "Worker ID": "old-terminal-grey",
            "最后心跳": 1787579400000,
            "状态": "在线",
        },
    }
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[queued], [stale_heartbeat]]),
    )

    result = asyncio.run(controller.audit_worker_health(
        now=datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc),
        stale_minutes=10,
    ))

    assert result["ok"] is False
    assert result["active_queue"] == 1
    assert result["fresh_workers"] == 0
    assert result["issues"][0]["code"] == "NO_FRESH_WORKER"


def test_worker_health_audit_is_quiet_when_the_queue_is_empty(monkeypatch):
    monkeypatch.setattr(
        controller.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[], []]),
    )

    result = asyncio.run(controller.audit_worker_health(
        now=datetime(2026, 8, 24, 15, 0, tzinfo=timezone.utc),
        stale_minutes=10,
    ))

    assert result["ok"] is True
    assert result["active_queue"] == 0
    assert result["issues"] == []
