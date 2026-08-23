import unittest
from datetime import datetime, timedelta, timezone

from app.clients import ApiError
from app.collector import (
    IncrementalCollector,
    build_update,
    index_rows_by_unique_key,
    index_youtube_rows_by_post_id,
    is_youtube_identity,
    post_unique_key,
    repair_interrupted_insert_fields,
    select_enabled_configs,
    normalize_video,
)
from app.core import (
    BEIJING,
    active_launch_events,
    backfill_window,
    incremental_window,
    query_groups,
    schedule_decision,
)


class FakeJobTests(unittest.TestCase):
    def test_assert_endpoint_contract_is_present(self):
        from pathlib import Path

        source = (Path(__file__).parents[1] / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn('@app.get("/runs/{job_id}/assert")', source)

    def test_durable_assert_recovers_completed_skipped_failed_and_running(self):
        from app.job_status import durable_job_snapshot, finished_status

        for prefix, expected_code in (
            ("云端增量完成", 200),
            ("云端增量跳过", 200),
            ("云端增量失败", 500),
            ("云端增量运行中", 409),
        ):
            job = durable_job_snapshot(
                "ytinc-abc", {"YouTube历史进度": f"{prefix}；job=ytinc-abc；x"}
            )
            self.assertEqual(finished_status(job)[0], expected_code)

    def test_durable_assert_does_not_match_a_job_id_prefix(self):
        from app.job_status import durable_job_snapshot, durable_job_snapshot_many

        self.assertIsNone(
            durable_job_snapshot(
                "ytinc-abc", {"YouTube历史进度": "云端增量完成；job=ytinc-abcd；x"}
            )
        )

    def test_durable_assert_recovers_completed_backfill_json(self):
        from app.job_status import durable_job_snapshot

        snapshot = durable_job_snapshot(
            "ytbackfill-abc",
            {
                "YouTube历史进度": (
                    '{"version":"yt-backfill-v1","last_job_id":"ytbackfill-abc",'
                    '"status":"ready","next_end":"2026-08-16T00:00:00Z"}'
                )
            },
        )
        self.assertEqual(snapshot["status"], "completed")
        self.assertEqual(snapshot["operation"], "backfill")

    def test_durable_assert_aggregates_multi_brand_config_rows(self):
        from app.job_status import durable_job_snapshot_many

        rows = [
            {"YouTube历史进度": "云端增量完成；job=ytinc-abc；x"},
            {"YouTube历史进度": "云端增量跳过；job=ytinc-abc；x"},
        ]
        snapshot = durable_job_snapshot_many("ytinc-abc", rows)
        self.assertEqual(snapshot["status"], "completed")
        self.assertEqual(snapshot["config_count"], 2)


def event(day, *, source="官方确认", confirmed="已确认", brand="NYXI"):
    return {
        "事件名称": "Launch",
        "竞品品牌": brand,
        "正式开售日期": day,
        "来源类型": source,
        "人工确认状态": confirmed,
    }


class ScheduleTests(unittest.TestCase):
    def test_monday_runs_without_launch_event(self):
        now = datetime(2026, 9, 7, 9, 30, tzinfo=BEIJING)
        decision = schedule_decision(now, [], brand="NYXI")
        self.assertTrue(decision.should_run)
        self.assertEqual(decision.reason, "weekly_monday")

    def test_wednesday_and_friday_run_only_inside_confirmed_d0_plus_minus_30(self):
        wednesday = datetime(2026, 8, 12, 9, 30, tzinfo=BEIJING)
        friday = datetime(2026, 8, 14, 9, 30, tzinfo=BEIJING)
        rows = [event("2026-08-05 00:00:00")]
        self.assertTrue(schedule_decision(wednesday, rows, brand="NYXI").should_run)
        self.assertTrue(schedule_decision(friday, rows, brand="NYXI").should_run)
        self.assertFalse(
            schedule_decision(
                datetime(2026, 9, 9, 9, 30, tzinfo=BEIJING), rows, brand="NYXI"
            ).should_run
        )

    def test_launch_window_includes_both_30_day_boundaries(self):
        center = datetime(2026, 8, 5, 9, 30, tzinfo=BEIJING)
        for offset in (-30, 30):
            now = center + timedelta(days=offset)
            self.assertEqual(active_launch_events([event("2026-08-05 00:00:00")], now, brand="NYXI"), ("Launch",))

    def test_unconfirmed_or_nonofficial_event_never_enables_boost(self):
        now = datetime(2026, 8, 12, 9, 30, tzinfo=BEIJING)
        rows = [
            event("2026-08-05 00:00:00", source="AI推测待确认"),
            event("2026-08-05 00:00:00", confirmed="待确认"),
            event("2026-08-05 00:00:00", brand="Other"),
        ]
        self.assertFalse(schedule_decision(now, rows, brand="NYXI").should_run)

    def test_feishu_select_lists_and_millisecond_dates_are_supported(self):
        now = datetime(2026, 8, 12, 9, 30, tzinfo=BEIJING)
        launch = int(datetime(2026, 8, 5, tzinfo=BEIJING).timestamp() * 1000)
        row = event(launch)
        row["竞品品牌"] = ["NYXI"]
        row["来源类型"] = ["官方确认"]
        row["人工确认状态"] = ["已确认"]
        self.assertTrue(schedule_decision(now, [row], brand="NYXI").should_run)


class IncrementalTests(unittest.TestCase):
    def test_backfill_window_moves_backward_from_now(self):
        now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
        start, end, history_start, done, progress = backfill_window(
            {"历史回溯起始日期": "2026-08-01T00:00:00Z"}, now, window_days=7
        )
        self.assertEqual(end, now)
        self.assertEqual(start, datetime(2026, 8, 16, 12, tzinfo=timezone.utc))
        self.assertEqual(history_start, datetime(2026, 8, 1, tzinfo=timezone.utc))
        self.assertFalse(done)
        self.assertEqual(progress, {})

    def test_backfill_window_uses_cursor_and_stops_at_history_start(self):
        now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
        config = {
            "历史回溯起始日期": "2026-08-01T00:00:00Z",
            "YouTube历史进度": '{"version":"yt-backfill-v1","next_end":"2026-08-05T00:00:00Z"}',
        }
        start, end, _, done, _ = backfill_window(config, now, window_days=7)
        self.assertEqual(end, datetime(2026, 8, 5, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 8, 1, tzinfo=timezone.utc))
        self.assertFalse(done)
        config["YouTube历史进度"] = '{"version":"yt-backfill-v1","next_end":"2026-08-01T00:00:00Z"}'
        _, _, _, done, _ = backfill_window(config, now, window_days=7)
        self.assertTrue(done)

    def test_window_overlaps_last_success_by_48_hours(self):
        now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
        start, end = incremental_window(
            {"最近成功采集时间": "2026-08-11T21:45:39+08:00"}, now
        )
        self.assertEqual(start, datetime(2026, 8, 9, 13, 45, 39, tzinfo=timezone.utc))
        self.assertEqual(end, now)

    def test_unique_key_encodes_youtube_and_post_id(self):
        self.assertEqual(post_unique_key("dQw4w9WgXcQ"), "5:dQw4w9WgXcQ")

    def test_platform_and_post_id_dedup_does_not_depend_on_unique_key(self):
        row = {"平台": ["YouTube"], "帖子ID": "dQw4w9WgXcQ", "唯一键": ""}
        indexed = index_youtube_rows_by_post_id(
            [row], target_ids={"dQw4w9WgXcQ"}
        )
        self.assertIs(indexed["dQw4w9WgXcQ"], row)

    def test_duplicate_platform_and_post_id_fails_closed(self):
        rows = [
            {"平台": "YouTube", "帖子ID": "dQw4w9WgXcQ"},
            {"平台": "YouTube", "帖子ID": "dQw4w9WgXcQ"},
        ]
        with self.assertRaises(ApiError):
            index_youtube_rows_by_post_id(
                rows, target_ids={"dQw4w9WgXcQ"}
            )

    def test_duplicate_unique_key_fails_closed(self):
        with self.assertRaises(ApiError):
            index_rows_by_unique_key(
                [{"唯一键": "5:dQw4w9WgXcQ"}, {"唯一键": "5:dQw4w9WgXcQ"}]
            )

    def test_unrelated_duplicate_unique_key_does_not_block_target(self):
        target = {"唯一键": "5:dQw4w9WgXcQ"}
        indexed = index_rows_by_unique_key(
            [target, {"唯一键": "other"}, {"唯一键": "other"}],
            target_keys={"5:dQw4w9WgXcQ"},
        )
        self.assertIs(indexed["5:dQw4w9WgXcQ"], target)

    def test_partial_insert_is_included_in_next_refresh_for_any_brand(self):
        self.assertTrue(
            is_youtube_identity(
                {
                    "竞品品牌": "GameSir",
                    "唯一键": "5:dQw4w9WgXcQ",
                    "平台": "",
                    "帖子ID": "dQw4w9WgXcQ",
                },
                brand="GameSir",
            )
        )

    def test_enabled_configs_are_selected_by_platform_and_optional_brand(self):
        rows = [
            {"_record_id": "nyxi-yt", "启用": True, "平台": "YouTube", "竞品品牌": "NYXI", "关键词": "nyxi"},
            {"_record_id": "gamesir-yt", "启用": True, "平台": "YouTube", "竞品品牌": "GameSir", "关键词": "gamesir"},
            {"_record_id": "nyxi-x", "启用": True, "平台": "X", "竞品品牌": "NYXI", "关键词": "nyxi"},
            {"_record_id": "off-yt", "启用": False, "平台": "YouTube", "竞品品牌": "Other", "关键词": "other"},
        ]
        self.assertEqual(
            [row["_record_id"] for row in select_enabled_configs(rows, platform="YouTube")],
            ["nyxi-yt", "gamesir-yt"],
        )
        self.assertEqual(
            [row["_record_id"] for row in select_enabled_configs(rows, platform="YouTube", brand="GameSir")],
            ["gamesir-yt"],
        )

    def test_normalize_video_uses_configured_brand_keyword_and_relation(self):
        video = {
            "id": "dQw4w9WgXcQ",
            "snippet": {
                "channelId": "UCgamesir123",
                "channelTitle": "GameSir Review",
                "title": "GameSir G8 review",
                "description": "GameSir G8 review",
                "publishedAt": "2026-08-20T00:00:00Z",
            },
            "statistics": {"viewCount": "123"},
        }
        fields = normalize_video(
            video,
            None,
            config={"竞品品牌": "GameSir", "关键词": "gamesir", "关键词别名": "GameSir Gaming"},
            evidence={"sources": [], "queries": [], "windows": []},
            batch_id="batch",
            captured_at=datetime(2026, 8, 23, tzinfo=timezone.utc),
            config_record_id="gamesir-yt",
        )
        self.assertEqual(fields["竞品品牌"], "GameSir")
        self.assertEqual(fields["命中关键词"], "gamesir")
        self.assertEqual(fields["关联监控任务"], [{"id": "gamesir-yt"}])

    def test_query_matrix_is_grouped_and_deduplicated(self):
        config = {
            "竞品品牌": "NYXI",
            "关键词别名": "NYXI, NYXI Gaming, nyxi hyperion",
            "排除词": "nyxi leon, lady nyxi",
            "产品系列词": "Hyperion\nWizard",
            "产品型号词": "Hyperion 3\nNJ12",
        }
        groups = query_groups(config, "nyxi")
        self.assertEqual(set(groups), {"brand", "series", "model"})
        self.assertIn("NYXI Hyperion 3", groups["model"])
        self.assertEqual(len(groups["brand"]), len(set(term.casefold() for term in groups["brand"])))

    def test_unchanged_row_does_not_refresh_capture_metadata(self):
        old = {
            "帖子标题": "Same",
            "曝光量": 100,
            "抓取时间": "old",
            "采集批次ID": "old-batch",
            "原始数据哈希": "old-hash",
        }
        incoming = dict(old, 抓取时间="new", 采集批次ID="new-batch", 原始数据哈希="new-hash")
        self.assertEqual(build_update(old, incoming), {})

    def test_changed_metric_adds_capture_metadata_but_preserves_review_fields(self):
        old = {"曝光量": 100, "合作信号": "A", "人工复核状态": "已确认"}
        incoming = {
            "曝光量": 110,
            "抓取时间": "2026-08-12 09:30:00",
            "采集批次ID": "batch",
            "原始数据哈希": "hash",
            "合作信号": "待分析",
            "人工复核状态": "待复核",
        }
        update = build_update(old, incoming)
        self.assertEqual(update["曝光量"], 110)
        self.assertNotIn("合作信号", update)
        self.assertNotIn("人工复核状态", update)

    def test_existing_post_refresh_is_limited_to_public_metrics_and_author_snapshot(self):
        old = {
            "帖子标题": "Human-reviewed title",
            "帖子内容": "Human-reviewed content",
            "视频标签": "reviewed-tag",
            "曝光量": 100,
        }
        incoming = {
            "帖子标题": "Changed upstream title",
            "帖子内容": "Changed upstream content",
            "视频标签": "changed-tag",
            "曝光量": 110,
            "抓取时间": "2026-08-12 09:30:00",
            "采集批次ID": "batch",
            "原始数据哈希": "hash",
        }
        update = build_update(old, incoming)
        self.assertEqual(update["曝光量"], 110)
        self.assertNotIn("帖子标题", update)
        self.assertNotIn("帖子内容", update)
        self.assertNotIn("视频标签", update)

    def test_blank_existing_review_fields_are_not_auto_filled(self):
        update = build_update(
            {"曝光量": 100},
            {
                "曝光量": 100,
                "合作信号": "待分析",
                "营销阶段": "待分析",
                "人工复核状态": "待复核",
            },
        )
        self.assertEqual(update, {})

    def test_interrupted_insert_repairs_only_missing_selects_for_nyxi(self):
        old = {
            "竞品品牌": "NYXI",
            "唯一键": "5:dQw4w9WgXcQ",
            "平台": "",
            "合作信号": "A",
        }
        incoming = {
            "唯一键": "5:dQw4w9WgXcQ",
            "平台": "YouTube",
            "合作信号": "待分析",
            "人工复核状态": "待复核",
        }
        repair = repair_interrupted_insert_fields(old, incoming)
        self.assertEqual(repair["平台"], "YouTube")
        self.assertEqual(repair["人工复核状态"], "待复核")
        self.assertNotIn("合作信号", repair)

    def test_reviewed_row_never_enters_interrupted_insert_repair(self):
        self.assertEqual(
            repair_interrupted_insert_fields(
                {
                    "竞品品牌": "NYXI",
                    "唯一键": "5:dQw4w9WgXcQ",
                    "平台": "YouTube",
                },
                {
                    "唯一键": "5:dQw4w9WgXcQ",
                    "平台": "YouTube",
                    "人工复核状态": "待复核",
                },
            ),
            {},
        )

    def test_success_marks_normal_before_advancing_waterline(self):
        class FakeFeishu:
            def __init__(self):
                self.calls = []

            def batch_update(self, app_token, table_id, records):
                self.calls.append(records[0][1])

        fake = FakeFeishu()
        IncrementalCollector(fake, None).mark_success("gamesir-yt", {"最近采集水位": "new"})
        self.assertEqual(fake.calls, [{"运行状态": "正常"}, {"最近采集水位": "new"}])


if __name__ == "__main__":
    unittest.main()
