import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from app.clients import ApiError
from app.collector import IncrementalCollector
from app.core import rfc3339
from app.quota import (
    GoogleQuotaReader,
    QuotaUnavailable,
    MONITORING_TYPE,
    OAUTH_SCOPES,
    PROJECT_ID,
)


NOW = datetime(2026, 9, 17, 8, 30, tzinfo=timezone.utc)


class QuotaTests(unittest.TestCase):
    def test_oauth_scopes_cover_key_lookup_and_monitoring_read(self):
        self.assertEqual(
            OAUTH_SCOPES,
            (
                "https://www.googleapis.com/auth/cloud-platform.read-only",
                "https://www.googleapis.com/auth/monitoring.read",
            ),
        )

    def reader(self, *, used=60, sampled_at=None, parent="projects/123/locations/global", window_start="2026-09-17T07:00:00Z"):
        sampled_at = sampled_at or NOW - timedelta(minutes=1)

        def get(url):
            if "lookupKey" in url:
                return {"parent": parent}
            if "consumerQuotaMetrics" in url:
                return {"metrics": [{
                    "metric": "youtube.googleapis.com/search_list",
                    "consumerQuotaLimits": [{
                        "name": "x/limits/%2Fday%2Fproject", "unit": "1/d/{project}",
                        "quotaBuckets": [{"effectiveLimit": "100"}],
                    }],
                }]}
            if "timeSeries" in url:
                return {"timeSeries": [{
                    "resource": {"labels": {"service": "youtube.googleapis.com"}},
                    "metric": {"type": MONITORING_TYPE, "labels": {
                        "quota_metric": "youtube.googleapis.com/search_list",
                        "limit_name": "defaultSearchListPerDayPerProject",
                        "window_size": "86400s", "window_start_time": window_start,
                    }},
                    "points": [{"interval": {"endTime": rfc3339(sampled_at)}, "value": {"int64Value": str(used)}}],
                }]}
            raise AssertionError(url)

        return GoogleQuotaReader(
            project_number="123", service_account_json=json.dumps({"project_id": PROJECT_ID}),
            youtube_api_key="test", get_json=get,
        )

    def test_same_project_fresh_usage(self):
        snapshot = self.reader().snapshot(after=NOW - timedelta(minutes=2), now=NOW)
        self.assertEqual(snapshot.remaining, 40)
        self.assertEqual(snapshot.quota_day, "2026-09-17")

    def test_audit_allows_older_same_day_sample_without_relaxing_budget_gate(self):
        reader = self.reader(sampled_at=NOW - timedelta(minutes=30))
        self.assertEqual(reader.audit_snapshot(now=NOW).used, 60)
        with self.assertRaisesRegex(QuotaUnavailable, "sample has not caught up"):
            reader.snapshot(after=NOW - timedelta(minutes=31), now=NOW)

    def test_wrong_project_and_stale_usage_fail_closed(self):
        with self.assertRaises(QuotaUnavailable):
            self.reader(parent="projects/999").snapshot(after=NOW - timedelta(minutes=2), now=NOW)
        with self.assertRaises(QuotaUnavailable):
            self.reader(sampled_at=NOW - timedelta(minutes=6)).snapshot(after=NOW - timedelta(minutes=7), now=NOW)
        with self.assertRaises(QuotaUnavailable):
            self.reader().snapshot(after=NOW - timedelta(seconds=30), now=NOW)
        with self.assertRaises(QuotaUnavailable):
            self.reader(window_start="2026-09-16T07:00:00Z").snapshot(
                after=NOW - timedelta(minutes=2), now=NOW
            )

    def test_credentials_alone_do_not_enable_backfill(self):
        env = {
            "GOOGLE_QUOTA_SERVICE_ACCOUNT_JSON": json.dumps({"project_id": PROJECT_ID}),
            "GOOGLE_QUOTA_PROJECT_NUMBER": "123",
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertIsNone(GoogleQuotaReader.from_environment("test"))

    def test_api_error_pauses_backfill_as_unknown_quota(self):
        reader = GoogleQuotaReader(
            project_number="123", service_account_json=json.dumps({"project_id": PROJECT_ID}),
            youtube_api_key="test", get_json=lambda url: (_ for _ in ()).throw(ApiError("http", "403", "denied")),
        )
        with self.assertRaisesRegex(QuotaUnavailable, "key project lookup failed \\(403\\)"):
            reader.snapshot(after=NOW - timedelta(minutes=2), now=NOW)

    def test_usage_api_error_reports_safe_stage(self):
        def get(url):
            if "lookupKey" in url:
                return {"parent": "projects/123/locations/global"}
            if "consumerQuotaMetrics" in url:
                return {"metrics": [{
                    "metric": "youtube.googleapis.com/search_list",
                    "consumerQuotaLimits": [{
                        "unit": "1/d/{project}",
                        "quotaBuckets": [{"effectiveLimit": "100"}],
                    }],
                }]}
            raise ApiError("http", "403", "denied")

        reader = GoogleQuotaReader(
            project_number="123", service_account_json=json.dumps({"project_id": PROJECT_ID}),
            youtube_api_key="test", get_json=get,
        )
        with self.assertRaisesRegex(QuotaUnavailable, "quota usage read failed \\(403\\)"):
            reader.audit_snapshot(now=NOW)

    def test_usage_api_error_reports_only_known_permission_hint(self):
        def get(url):
            if "lookupKey" in url:
                return {"parent": "projects/123/locations/global"}
            if "consumerQuotaMetrics" in url:
                return {"metrics": [{
                    "metric": "youtube.googleapis.com/search_list",
                    "consumerQuotaLimits": [{
                        "unit": "1/d/{project}",
                        "quotaBuckets": [{"effectiveLimit": "100"}],
                    }],
                }]}
            raise ApiError(
                "http",
                "403",
                "Permission monitoring.timeSeries.list denied for project secret-project-name",
            )

        reader = GoogleQuotaReader(
            project_number="123", service_account_json=json.dumps({"project_id": PROJECT_ID}),
            youtube_api_key="test", get_json=get,
        )
        with self.assertRaisesRegex(
            QuotaUnavailable,
            r"quota usage read failed \(403; permission=monitoring.timeSeries.list\)$",
        ):
            reader.audit_snapshot(now=NOW)

    def test_usage_api_error_does_not_expose_unknown_response_or_secrets(self):
        secret = "oauth-token api-key private_key raw-response secret-project"

        def get(url):
            if "lookupKey" in url:
                return {"parent": "projects/123/locations/global"}
            if "consumerQuotaMetrics" in url:
                return {"metrics": [{
                    "metric": "youtube.googleapis.com/search_list",
                    "consumerQuotaLimits": [{
                        "unit": "1/d/{project}",
                        "quotaBuckets": [{"effectiveLimit": "100"}],
                    }],
                }]}
            raise ApiError("http", "403", secret)

        reader = GoogleQuotaReader(
            project_number="123", service_account_json=json.dumps({"project_id": PROJECT_ID}),
            youtube_api_key="test", get_json=get,
        )
        with self.assertRaises(QuotaUnavailable) as caught:
            reader.audit_snapshot(now=NOW)
        self.assertEqual(str(caught.exception), "Google quota usage read failed (403)")
        self.assertNotIn(secret, str(caught.exception))

    def test_usage_api_error_labels_scope_reason_without_permission_tag(self):
        def get(url):
            if "lookupKey" in url:
                return {"parent": "projects/123/locations/global"}
            if "consumerQuotaMetrics" in url:
                return {"metrics": [{
                    "metric": "youtube.googleapis.com/search_list",
                    "consumerQuotaLimits": [{
                        "unit": "1/d/{project}",
                        "quotaBuckets": [{"effectiveLimit": "100"}],
                    }],
                }]}
            raise ApiError(
                "http", "ACCESS_TOKEN_SCOPE_INSUFFICIENT", "scope rejected"
            )

        reader = GoogleQuotaReader(
            project_number="123", service_account_json=json.dumps({"project_id": PROJECT_ID}),
            youtube_api_key="test", get_json=get,
        )
        with self.assertRaisesRegex(
            QuotaUnavailable,
            r"quota usage read failed \(ACCESS_TOKEN_SCOPE_INSUFFICIENT\)$",
        ):
            reader.audit_snapshot(now=NOW)


class FakeFeishu:
    def __init__(self, config):
        self.config = config
        self.saved = []

    def batch_update(self, app, table, updates):
        self.saved.extend(updates)
        self.config.update(updates[0][1])


class SegmentTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "_record_id": "cfg", "竞品品牌": "8BitDo", "平台": "YouTube", "关键词": "8bitdo",
            "历史回溯起始日期": "2020-01-01T00:00:00Z",
            "YouTube历史游标": json.dumps({"version": "yt-backfill-v1", "next_end": "2026-07-13T11:00:42Z"}),
        }
        self.feishu = FakeFeishu(self.config)
        self.collector = IncrementalCollector(self.feishu, object())
        self.collector._config = lambda **kwargs: self.config

    def test_budget_pause_does_not_advance_cursor(self):
        self.collector._collect_window = lambda **kwargs: self.fail("search must not run")
        result = self.collector.backfill_budgeted(
            now=NOW, job_id="a", quota_remaining=lambda after: 29, after=NOW,
        )
        self.assertEqual(result["status"], "quota_paused")
        self.assertEqual(len(self.feishu.saved), 0)
        self.assertEqual(json.loads(self.config["YouTube历史游标"])["next_end"], "2026-07-13T11:00:42Z")

    def test_completed_segment_is_checkpointed_and_resumed(self):
        calls = []

        def collect(**kwargs):
            calls.append((kwargs["queries"], kwargs["start"], kwargs["end"]))
            return {"search_calls": 1, "new_posts": 1}

        self.collector._collect_window = collect
        budgets = iter([31, 29])
        first = self.collector.backfill_budgeted(
            now=NOW, job_id="first", quota_remaining=lambda after: next(budgets), after=NOW,
        )
        self.assertEqual(first["status"], "quota_paused")
        state = json.loads(self.config["YouTube历史游标"])
        self.assertEqual(state["next_end"], "2026-07-13T11:00:42Z")
        self.assertEqual(state["status"], "partial")
        self.assertEqual(len(calls), 1)
        second = self.collector.backfill_budgeted(
            now=NOW, job_id="second", quota_remaining=lambda after: 100, after=NOW,
        )
        self.assertEqual(second["status"], "completed")
        self.assertEqual(json.loads(self.config["YouTube历史游标"])["next_end"], "2026-07-06T11:00:42Z")
        self.assertEqual(len(calls), len(set(map(str, calls))))

    def test_overflow_splits_and_checkpoints_before_retry(self):
        calls = []

        def collect(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise ApiError("youtube", "incremental_page_cap", "overflow")
            return {"search_calls": 1, "new_posts": 0}

        self.collector._collect_window = collect
        budget = iter([40, 29])
        result = self.collector.backfill_budgeted(
            now=NOW, job_id="split", quota_remaining=lambda after: next(budget), after=NOW,
        )
        self.assertEqual(result["status"], "quota_paused")
        state = json.loads(self.config["YouTube历史游标"])
        self.assertEqual(state["next_end"], "2026-07-13T11:00:42Z")
        self.assertGreater(len(state["pending_segments"]), 2)
        self.assertEqual(state["pending_segments"][0]["query"], calls[0]["queries"][0])


if __name__ == "__main__":
    unittest.main()
