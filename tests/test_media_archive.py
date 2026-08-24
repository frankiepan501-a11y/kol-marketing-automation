from datetime import datetime, timezone

from app import media_archive


def _row(record_id, platform, url, group="group-1", *, status="待下载", retries=0,
         enabled=True, kol="kol-1", product="product-1", file_url=""):
    return {
        "record_id": record_id,
        "fields": {
            "发布平台": platform,
            "作品链接": url,
            "同源作品组": group,
            "关联KOL": [{"record_ids": [kol]}] if kol else [],
            "关联产品": [{"record_ids": [product]}] if product else [],
            "品牌": "FUNLAB",
            "归档状态": status,
            "允许自动归档": enabled,
            "重试次数": retries,
            "归档文件链接": file_url,
        },
    }


def test_validate_work_accepts_supported_platform_urls_and_extracts_ids():
    cases = [
        ("YouTube", "https://youtu.be/nUkwNTRFJBc", "nUkwNTRFJBc"),
        ("TikTok", "https://www.tiktok.com/@creator/video/7491234567890123456", "7491234567890123456"),
        ("Instagram", "https://www.instagram.com/reel/DbJJ6p9xjXp/", "DbJJ6p9xjXp"),
    ]

    for platform, url, expected_id in cases:
        result = media_archive.validate_work_row(_row("rec1", platform, url))
        assert result.valid is True
        assert result.platform_id == expected_id
        assert result.errors == ()


def test_validate_work_rejects_platform_mismatch_and_missing_relations():
    row = _row(
        "rec1",
        "TikTok",
        "https://www.youtube.com/watch?v=nUkwNTRFJBc",
        kol="",
        product="",
    )

    result = media_archive.validate_work_row(row)

    assert result.valid is False
    assert "发布平台与作品链接不一致" in result.errors
    assert "缺少关联KOL" in result.errors
    assert "缺少关联产品" in result.errors


def test_group_plan_selects_one_youtube_master_for_cross_posted_work():
    rows = [
        _row("rec-ig", "Instagram", "https://www.instagram.com/reel/ABC123/"),
        _row("rec-tk", "TikTok", "https://www.tiktok.com/@creator/video/7491234567890123456"),
        _row("rec-yt", "YouTube", "https://youtu.be/nUkwNTRFJBc"),
    ]

    plan = media_archive.plan_archive_groups(rows)

    assert len(plan) == 1
    assert plan[0].master_record_id == "rec-yt"
    assert plan[0].follower_record_ids == ("rec-ig", "rec-tk")
    assert plan[0].action == "queue_download"


def test_group_plan_falls_back_after_youtube_exhausts_retries():
    rows = [
        _row(
            "rec-yt", "YouTube", "https://youtu.be/nUkwNTRFJBc",
            status="下载失败", retries=3,
        ),
        _row("rec-ig", "Instagram", "https://www.instagram.com/reel/ABC123/"),
        _row("rec-tk", "TikTok", "https://www.tiktok.com/@creator/video/7491234567890123456"),
    ]

    plan = media_archive.plan_archive_groups(rows, max_retries=3)

    assert plan[0].master_record_id == "rec-ig"
    assert plan[0].action == "queue_download"


def test_group_plan_propagates_existing_master_without_second_download():
    rows = [
        _row(
            "rec-yt", "YouTube", "https://youtu.be/nUkwNTRFJBc",
            status="已归档", file_url="https://u1wpma3xuhr.feishu.cn/file/file123",
        ),
        _row("rec-ig", "Instagram", "https://www.instagram.com/reel/ABC123/"),
    ]

    plan = media_archive.plan_archive_groups(rows)

    assert plan[0].master_record_id == "rec-yt"
    assert plan[0].action == "propagate_existing"
    assert plan[0].archive_file_url.endswith("/file/file123")


def test_filename_uses_platform_kol_product_and_two_digit_sequence():
    assert media_archive.build_archive_filename(
        platform="YouTube",
        kol_name="Amrie47",
        product_name="YS11-5-戴夫",
        sequence=1,
        extension="mp4",
    ) == "YT-Amrie47-YS11-5-戴夫-01.mp4"

    assert media_archive.build_archive_filename(
        platform="Instagram",
        kol_name="Creator: One",
        product_name="Product/A",
        sequence=12,
        extension="mp4",
    ) == "IG-Creator One-Product A-12.mp4"


def test_youtube_statistics_keep_missing_public_counts_blank():
    item = {
        "id": "nUkwNTRFJBc",
        "snippet": {
            "title": "Sample title",
            "publishedAt": "2026-08-20T12:00:00Z",
        },
        "statistics": {"viewCount": "12345"},
    }

    mapped = media_archive.map_youtube_video(item)

    assert mapped["平台作品ID"] == "nUkwNTRFJBc"
    assert mapped["作品名称"] == "Sample title"
    assert mapped["播放量"] == 12345
    assert mapped["点赞量"] is None
    assert mapped["评论数"] is None
    assert mapped["上稿日期"] == 1787227200000


def test_metric_schedule_uses_review_milestones_and_stops_after_day_90():
    published = datetime(2026, 8, 1, tzinfo=timezone.utc)
    now = datetime(2026, 8, 8, 1, tzinfo=timezone.utc)

    assert media_archive.next_metric_milestone(published, now, captured_days={0, 1, 3}) == 7
    assert media_archive.next_metric_milestone(published, now, captured_days={0, 1, 3, 7}) == 14
    assert media_archive.next_metric_milestone(
        published, datetime(2026, 11, 15, tzinfo=timezone.utc),
        captured_days={0, 1, 3, 7, 14, 30, 60, 90},
    ) is None


def test_due_metric_milestone_does_not_create_a_future_snapshot_early():
    published = datetime(2026, 8, 1, tzinfo=timezone.utc)
    now = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)

    assert media_archive.due_metric_milestone(
        published, now, captured_days={0, 1},
    ) is None
    assert media_archive.next_metric_milestone(
        published, now, captured_days={0, 1},
    ) == 3


def test_old_work_gets_one_honest_baseline_instead_of_fake_historical_milestones():
    published = datetime(2026, 1, 1, tzinfo=timezone.utc)
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)

    assert media_archive.due_metric_milestone(
        published, now, captured_days=set(),
    ) == 235
    assert media_archive.next_future_metric_milestone(
        published, now, captured_days={235},
    ) is None
