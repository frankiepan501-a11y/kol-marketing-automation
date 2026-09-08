import tempfile
import unittest
from pathlib import Path

from app.endpoint_alert_dedup import EndpointAlertDedup


class EndpointAlertDedupTests(unittest.TestCase):
    def test_new_store_instance_is_suppressed_within_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "endpoint-alerts.sqlite3"
            first = EndpointAlertDedup(path, cooldown_seconds=3600)
            restarted = EndpointAlertDedup(path, cooldown_seconds=3600)

            self.assertTrue(first.claim("/launch/runtime/autonomous", now=1000.0))
            self.assertFalse(restarted.claim("/launch/runtime/autonomous", now=1001.0))

    def test_release_allows_retry_after_delivery_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "endpoint-alerts.sqlite3"
            store = EndpointAlertDedup(path, cooldown_seconds=3600)

            self.assertTrue(store.claim("/launch/runtime/autonomous", now=1000.0))
            store.release("/launch/runtime/autonomous")
            self.assertTrue(store.claim("/launch/runtime/autonomous", now=1001.0))

    def test_snapshot_reports_unavailable_persistent_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            blocked_parent = Path(tmp) / "not-a-directory"
            blocked_parent.write_text("blocked", encoding="utf-8")
            store = EndpointAlertDedup(
                blocked_parent / "endpoint-alerts.sqlite3",
                cooldown_seconds=3600,
            )

            self.assertEqual(
                {
                    "backend": "sqlite",
                    "persistent": False,
                    "state_available": False,
                    "cooldown_seconds": 3600,
                },
                store.snapshot(),
            )

    def test_alert_is_allowed_again_after_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "endpoint-alerts.sqlite3"
            store = EndpointAlertDedup(path, cooldown_seconds=3600)

            self.assertTrue(store.claim("/launch/runtime/autonomous", now=1000.0))
            self.assertTrue(store.claim("/launch/runtime/autonomous", now=4601.0))

    def test_different_endpoints_are_independent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "endpoint-alerts.sqlite3"
            store = EndpointAlertDedup(path, cooldown_seconds=3600)

            self.assertTrue(store.claim("/launch/runtime/autonomous", now=1000.0))
            self.assertTrue(store.claim("/launch/runtime/feedback", now=1001.0))


if __name__ == "__main__":
    unittest.main()
