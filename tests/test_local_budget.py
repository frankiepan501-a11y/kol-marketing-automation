import json
import unittest
from datetime import datetime, timezone

from app.clients import ApiError
from app.collector import IncrementalCollector
from app.quota import DailySearchBudget


NOW = datetime(2026, 9, 20, 8, 30, tzinfo=timezone.utc)


class FakeFeishu:
    def __init__(self, config):
        self.config = config
        self.saved = []

    def batch_update(self, _app, _table, updates):
        self.saved.extend(updates)
        self.config.update(updates[0][1])

    def list_records(self, *_args, **_kwargs):
        return []


class LocalBudgetTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "_record_id": "cfg",
            "竞品品牌": "8BitDo",
            "平台": "YouTube",
            "关键词": "8bitdo",
            "历史回溯起始日期": "2020-01-01T00:00:00Z",
            "YouTube历史游标": json.dumps(
                {"version": "yt-backfill-v1", "next_end": "2026-07-13T11:00:42Z"}
            ),
        }
        self.feishu = FakeFeishu(self.config)
        self.collector = IncrementalCollector(self.feishu, object())
        self.collector._config = lambda **_kwargs: self.config

    def test_nyxi_calls_are_charged_before_backfill(self):
        budget = DailySearchBudget.from_nyxi({"search_calls": 17})
        self.assertEqual(budget.limit, 100)
        self.assertEqual(budget.reserve, 20)
        self.assertEqual(budget.used, 17)
        self.assertEqual(budget.remaining, 83)
        self.assertTrue(budget.can_start_segment())

    def test_missing_nyxi_call_count_fails_closed(self):
        budget = DailySearchBudget.from_nyxi({})
        self.assertEqual(budget.used, 100)
        self.assertFalse(budget.can_start_segment())

    def test_successful_segments_consume_actual_calls_and_stop_at_safe_reserve(self):
        calls = []

        def collect(**kwargs):
            calls.append(kwargs)
            return {"search_calls": 10, "new_posts": 1}

        self.collector._collect_window = collect
        budget = DailySearchBudget(limit=50, reserve=20, used=20)
        result = self.collector.backfill_budgeted(
            now=NOW, job_id="budget", budget=budget, brand="8BitDo"
        )

        self.assertEqual(result["status"], "quota_paused")
        self.assertEqual(result["search_calls"], 10)
        self.assertEqual(result["budget_used"], 30)
        self.assertEqual(result["budget_remaining"], 20)
        self.assertEqual(len(calls), 1)
        state = json.loads(self.config["YouTube历史游标"])
        self.assertEqual(state["status"], "partial")
        self.assertEqual(state["next_end"], "2026-07-13T11:00:42Z")

    def test_overflow_charges_ten_calls_before_split_retry(self):
        calls = []

        def collect(**kwargs):
            calls.append(kwargs)
            raise ApiError("youtube", "incremental_page_cap", "overflow")

        self.collector._collect_window = collect
        budget = DailySearchBudget(limit=40, reserve=20, used=10)
        result = self.collector.backfill_budgeted(
            now=NOW, job_id="overflow", budget=budget, brand="8BitDo"
        )

        self.assertEqual(result["status"], "quota_paused")
        self.assertEqual(result["search_calls"], 10)
        self.assertEqual(result["budget_remaining"], 20)
        self.assertEqual(len(calls), 1)
        state = json.loads(self.config["YouTube历史游标"])
        self.assertGreater(len(state["pending_segments"]), 2)

    def test_youtube_daily_quota_error_pauses_without_advancing_cursor(self):
        self.collector._collect_window = lambda **_kwargs: (_ for _ in ()).throw(
            ApiError(
                "youtube", "quotaExceeded", "daily quota exhausted",
                metadata={"search_calls": "4"},
            )
        )
        budget = DailySearchBudget()
        result = self.collector.backfill_budgeted(
            now=NOW, job_id="exhausted", budget=budget, brand="8BitDo"
        )

        self.assertEqual(result["status"], "quota_exhausted")
        self.assertEqual(result["search_calls"], 4)
        self.assertEqual(result["budget_used"], 4)
        self.assertEqual(
            json.loads(self.config["YouTube历史游标"])["next_end"],
            "2026-07-13T11:00:42Z",
        )

    def test_search_error_carries_attempted_page_count(self):
        class FailingYouTube:
            def __init__(self):
                self.calls = 0

            def search(self, *_args, **_kwargs):
                self.calls += 1
                if self.calls == 3:
                    raise ApiError("youtube", "quotaExceeded", "daily quota exhausted")
                return {"items": [], "nextPageToken": "next"}

        collector = IncrementalCollector(self.feishu, FailingYouTube())
        with self.assertRaises(ApiError) as caught:
            collector._search(
                self.config,
                datetime(2026, 7, 1, tzinfo=timezone.utc),
                datetime(2026, 7, 2, tzinfo=timezone.utc),
                queries=["8bitdo"],
            )

        self.assertEqual(caught.exception.code, "quotaExceeded")
        self.assertEqual(caught.exception.metadata["search_calls"], "3")

    def test_detail_api_error_preserves_prior_search_count(self):
        class DetailFailingYouTube:
            def search(self, *_args, **_kwargs):
                return {"items": [], "nextPageToken": ""}

            def videos(self, _video_ids):
                raise ApiError("youtube", "quotaExceeded", "daily quota exhausted")

        collector = IncrementalCollector(self.feishu, DetailFailingYouTube())
        with self.assertRaises(ApiError) as caught:
            collector._collect_window(
                config=self.config,
                config_record_id="cfg",
                brand="8BitDo",
                platform="YouTube",
                now=NOW,
                start=datetime(2026, 7, 1, tzinfo=timezone.utc),
                end=datetime(2026, 7, 2, tzinfo=timezone.utc),
                commit=False,
                job_id="detail-error",
                refresh_existing_ids=False,
                queries=["8bitdo"],
            )

        self.assertEqual(caught.exception.metadata["search_calls"], "1")


if __name__ == "__main__":
    unittest.main()
