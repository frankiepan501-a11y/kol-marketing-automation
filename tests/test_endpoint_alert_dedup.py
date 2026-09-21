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

    def test_transient_incident_waits_30_minutes_and_merges_endpoints(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EndpointAlertDedup(Path(tmp) / "endpoint-alerts.sqlite3")

            self.assertIsNone(store.record_incident_failure(
                "feishu-bitable:1254607", "/auto-send/run", now=1000.0,
            ))
            self.assertIsNone(store.record_incident_failure(
                "feishu-bitable:1254607", "/dashboard/refresh", now=2700.0,
            ))
            incident = store.record_incident_failure(
                "feishu-bitable:1254607", "/auto-send/run", now=2801.0,
            )

            self.assertEqual(
                ["/auto-send/run", "/dashboard/refresh"], incident["endpoints"],
            )
            self.assertEqual(1801.0, incident["duration_seconds"])
            self.assertEqual(1, incident["alert_count"])

    def test_transient_incident_reminds_only_every_six_hours(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EndpointAlertDedup(Path(tmp) / "endpoint-alerts.sqlite3")
            key = "feishu-bitable:1254607"
            store.record_incident_failure(key, "/auto-send/run", now=1000.0)
            self.assertIsNotNone(store.record_incident_failure(
                key, "/auto-send/run", now=2801.0,
            ))
            self.assertIsNone(store.record_incident_failure(
                key, "/launch/runtime/autonomous", now=2802.0,
            ))
            reminder = store.record_incident_failure(
                key, "/launch/runtime/autonomous", now=24402.0,
            )
            self.assertEqual(2, reminder["alert_count"])

    def test_transient_incident_recovery_is_silent_until_an_alert_was_sent(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EndpointAlertDedup(Path(tmp) / "endpoint-alerts.sqlite3")
            key = "feishu-bitable:1254607"
            store.record_incident_failure(key, "/auto-send/run", now=1000.0)
            self.assertIsNone(store.resolve_incident_endpoint(
                key, "/auto-send/run", now=1100.0,
            ))

    def test_transient_incident_recovers_after_all_failed_endpoints_succeed(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EndpointAlertDedup(Path(tmp) / "endpoint-alerts.sqlite3")
            key = "feishu-bitable:1254607"
            store.record_incident_failure(key, "/auto-send/run", now=1000.0)
            store.record_incident_failure(key, "/dashboard/refresh", now=2801.0)

            self.assertIsNone(store.resolve_incident_endpoint(
                key, "/auto-send/run", now=2900.0,
            ))
            recovered = store.resolve_incident_endpoint(
                key, "/dashboard/refresh", now=3000.0,
            )
            self.assertEqual(
                ["/auto-send/run", "/dashboard/refresh"], recovered["endpoints"],
            )
            self.assertEqual(1, recovered["alert_count"])


if __name__ == "__main__":
    unittest.main()
