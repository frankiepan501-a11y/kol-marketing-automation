import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.main import _quota_preflight_payload
from app.quota import QuotaUnavailable


NOW = datetime(2026, 9, 19, 8, 30, tzinfo=timezone.utc)


class QuotaPreflightTests(unittest.TestCase):
    def test_returns_only_verified_quota_summary(self):
        snapshot = SimpleNamespace(
            project_id="powkong-funlab-ads-api",
            project_number="77860794092",
            quota_day="2026-09-19",
            limit=100,
            used=20,
            remaining=80,
            sampled_at=NOW - timedelta(minutes=1),
        )
        reader = Mock()
        reader.audit_snapshot.return_value = snapshot
        env = {
            "GOOGLE_QUOTA_SERVICE_ACCOUNT_JSON": '{"project_id":"powkong-funlab-ads-api"}',
            "GOOGLE_QUOTA_PROJECT_NUMBER": "77860794092",
            "GOOGLE_QUOTA_VERIFIED": "sentinel",
        }
        with (
            patch.dict(os.environ, env, clear=True),
            patch("app.main.YouTubeClient", return_value=SimpleNamespace(api_key="secret-key")),
            patch("app.main.GoogleQuotaReader", return_value=reader),
        ):
            result = _quota_preflight_payload(now=NOW)
            self.assertEqual(os.environ["GOOGLE_QUOTA_VERIFIED"], "sentinel")

        self.assertEqual(
            result,
            {
                "project_match": True,
                "project_id": "powkong-funlab-ads-api",
                "project_number": "77860794092",
                "quota_day": "2026-09-19",
                "limit": 100,
                "used": 20,
                "remaining": 80,
                "sampled_at": (NOW - timedelta(minutes=1)).isoformat(),
            },
        )
        reader.audit_snapshot.assert_called_once_with(now=NOW)

    def test_missing_configuration_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(QuotaUnavailable, "configuration is missing"):
                _quota_preflight_payload(now=NOW)

    def test_unexpected_failure_log_excludes_exception_message(self):
        with (
            patch("app.main._quota_preflight_payload", side_effect=RuntimeError("secret-value")),
            self.assertLogs("socialecho-youtube-incremental", level="WARNING") as captured,
        ):
            from app.main import _log_quota_preflight

            _log_quota_preflight()

        text = "\n".join(captured.output)
        self.assertIn("diagnostic=quota_preflight_unexpected", text)
        self.assertNotIn("secret-value", text)


if __name__ == "__main__":
    unittest.main()
