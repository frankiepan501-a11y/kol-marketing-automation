import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import x_history


class XHistoryProbeTests(unittest.TestCase):
    def setUp(self):
        self.old_token = x_history.config.INTERNAL_TOKEN
        x_history.config.INTERNAL_TOKEN = "unit-token"
        app = FastAPI()
        app.include_router(x_history.router)
        self.client = TestClient(app)

    def tearDown(self):
        x_history.config.INTERNAL_TOKEN = self.old_token

    def test_probe_requires_internal_bearer(self):
        self.assertEqual(401, self.client.get("/x-history/probe").status_code)
        self.assertEqual(
            401,
            self.client.get(
                "/x-history/probe",
                headers={"Authorization": "Bearer wrong"},
            ).status_code,
        )

    @patch("app.x_history._x_search_all", new_callable=AsyncMock)
    def test_probe_reports_full_archive_supported_without_exposing_posts(self, search_all):
        search_all.return_value = {
            "data": [{"id": "123", "text": "secret test body"}],
            "meta": {"result_count": 1, "next_token": "private-page-token"},
        }
        response = self.client.get(
            "/x-history/probe",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["full_archive_supported"])
        self.assertEqual(1, body["result_count"])
        self.assertNotIn("secret test body", str(body))
        self.assertNotIn("private-page-token", str(body))

    @patch("app.x_history._x_search_all", new_callable=AsyncMock)
    def test_probe_classifies_permission_error_and_redacts_token(self, search_all):
        search_all.side_effect = x_history.XApiError(
            status_code=403,
            category="full_archive_not_authorized",
            message="Bearer x-secret-token is not allowed",
        )
        response = self.client.get(
            "/x-history/probe",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertFalse(body["full_archive_supported"])
        self.assertEqual("full_archive_not_authorized", body["reason"])
        self.assertNotIn("x-secret-token", str(body))


class XHistoryCollectorTests(unittest.IsolatedAsyncioTestCase):
    def test_history_end_stays_behind_x_server_clock(self):
        now = datetime(2026, 8, 12, 15, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(
            datetime(2026, 8, 12, 14, 59, 30, tzinfo=timezone.utc),
            x_history.history_end(now),
        )

    def test_year_windows_are_contiguous_and_end_exclusive(self):
        windows = x_history.build_year_windows(
            datetime(2023, 6, 1, tzinfo=timezone.utc),
            datetime(2025, 2, 1, tzinfo=timezone.utc),
            [x_history.QuerySpec("brand", "NYXI品牌词", '"NYXI" -is:retweet')],
        )
        self.assertEqual(3, len(windows))
        self.assertEqual(windows[0].end, windows[1].start)
        self.assertEqual(windows[1].end, windows[2].start)
        self.assertEqual("2023-06-01T00:00:00Z", windows[0].start_iso)
        self.assertEqual("2025-02-01T00:00:00Z", windows[-1].end_iso)

    async def test_collect_window_paginates_and_enriches_author(self):
        pages = [
            {
                "data": [{
                    "id": "100",
                    "author_id": "u1",
                    "created_at": "2024-01-03T00:00:00Z",
                    "text": "NYXI Hyperion review",
                    "public_metrics": {"like_count": 4, "impression_count": 80},
                }],
                "includes": {"users": [{
                    "id": "u1",
                    "name": "Reviewer",
                    "username": "reviewer",
                    "public_metrics": {"followers_count": 1200, "following_count": 20, "tweet_count": 50},
                }]},
                "meta": {"result_count": 1, "next_token": "page-2"},
            },
            {
                "data": [{
                    "id": "101",
                    "author_id": "u2",
                    "created_at": "2024-01-04T00:00:00Z",
                    "text": "NYXI Wizard review",
                    "public_metrics": {"like_count": 5},
                }],
                "includes": {"users": [{"id": "u2", "name": "Creator", "username": "creator"}]},
                "meta": {"result_count": 1},
            },
        ]
        spec = x_history.QuerySpec("brand", "NYXI品牌词", '"NYXI" -is:retweet')
        window = x_history.build_year_windows(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            [spec],
        )[0]
        with patch("app.x_history._x_search_all", new=AsyncMock(side_effect=pages)) as search:
            rows, calls = await x_history.collect_window(window, "batch-1", "2026-08-12T00:00:00Z")

        self.assertEqual(2, calls)
        self.assertEqual(2, len(rows))
        self.assertEqual("7:100", rows[0]["唯一键"])
        self.assertEqual("Reviewer", rows[0]["KOL账号名"])
        self.assertEqual(1200, rows[0]["粉丝数快照"])
        self.assertEqual(80, rows[0]["曝光量"])
        self.assertEqual("page-2", search.await_args_list[1].args[0]["next_token"])

    def test_merge_candidates_unions_query_and_window_evidence(self):
        first = {"唯一键": "7:100", "X命中查询词": "NYXI品牌词", "X查询时间窗": "2024", "点赞数": 1}
        second = {"唯一键": "7:100", "X命中查询词": "Hyperion型号词", "X查询时间窗": "2024", "点赞数": 2}
        merged = x_history.merge_candidate_rows([first, second])
        self.assertEqual(1, len(merged))
        self.assertEqual("NYXI品牌词；Hyperion型号词", merged[0]["X命中查询词"])
        self.assertEqual("2024", merged[0]["X查询时间窗"])
        self.assertEqual(2, merged[0]["点赞数"])

    async def test_dry_run_plan_never_calls_feishu_write(self):
        rows = [{"唯一键": "7:100", "采集来源": ["X API"], "点赞数": 2}]
        existing = {
            "7:100": {
                "record_id": "rec1",
                "fields": {"唯一键": "7:100", "采集来源": ["SocialEcho"], "点赞数": 1},
            }
        }
        with patch("app.x_history._batch_create_once", new=AsyncMock()) as create, patch(
            "app.x_history._batch_update_once", new=AsyncMock()
        ) as update:
            result = await x_history.upsert_rows(rows, existing, commit=False)
        self.assertEqual(0, result["created"])
        self.assertEqual(0, result["updated"])
        self.assertEqual(1, result["would_update"])
        create.assert_not_awaited()
        update.assert_not_awaited()

    async def test_commit_merges_existing_sources_before_update(self):
        rows = [{"唯一键": "7:100", "采集来源": ["X API"], "X命中查询词": "NYXI品牌词", "点赞数": 2}]
        existing = {
            "7:100": {
                "record_id": "rec1",
                "fields": {"唯一键": "7:100", "采集来源": ["SocialEcho"], "点赞数": 1},
            }
        }
        with patch("app.x_history._batch_create_once", new=AsyncMock(return_value=[])) as create, patch(
            "app.x_history._batch_update_once", new=AsyncMock()
        ) as update:
            result = await x_history.upsert_rows(rows, existing, commit=True)
        self.assertEqual(0, result["created"])
        self.assertEqual(1, result["updated"])
        create.assert_not_awaited()
        payload = update.await_args.args[0][0]
        self.assertEqual(["SocialEcho", "X API"], payload["fields"]["采集来源"])

    def test_commit_rejects_page_cap_that_would_skip_history(self):
        old_token = x_history.config.INTERNAL_TOKEN
        x_history.config.INTERNAL_TOKEN = "unit-token"
        try:
            app = FastAPI()
            app.include_router(x_history.router)
            client = TestClient(app)
            response = client.post(
                "/x-history/run?commit=true&max_pages_per_window=1",
                headers={"Authorization": "Bearer unit-token"},
            )
            self.assertEqual(400, response.status_code)
        finally:
            x_history.config.INTERNAL_TOKEN = old_token


if __name__ == "__main__":
    unittest.main()
