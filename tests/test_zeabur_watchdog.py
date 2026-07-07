import argparse
import os
import tempfile
import unittest
from unittest import mock

from scripts import zeabur_watchdog as zw


class ZeaburWatchdogTests(unittest.TestCase):
    def test_evaluate_server_resource_thresholds(self):
        server = {
            "name": "tokyo",
            "status": {
                "isOnline": True,
                "vmStatus": "RUNNING",
                "totalCPU": 100,
                "usedCPU": 10,
                "totalMemory": 100,
                "usedMemory": 96,
                "totalDisk": 100,
                "usedDisk": 86,
            },
        }
        issues = zw.evaluate_server(server)
        self.assertIn(("memory_high", "critical"), [(i.key, i.severity) for i in issues])
        self.assertIn(("disk_high", "warning"), [(i.key, i.severity) for i in issues])

    def test_server_offline_short_circuits_resource_checks(self):
        issues = zw.evaluate_server(
            {
                "name": "tokyo",
                "status": {
                    "isOnline": False,
                    "vmStatus": "RUNNING",
                    "totalMemory": 100,
                    "usedMemory": 99,
                },
            }
        )
        self.assertEqual([i.key for i in issues], ["server_offline"])

    @mock.patch.dict(os.environ, {"ZEABUR_API_KEY": "dummy"}, clear=True)
    @mock.patch("scripts.zeabur_watchdog.save_state")
    @mock.patch("scripts.zeabur_watchdog.send_feishu", return_value=True)
    @mock.patch("scripts.zeabur_watchdog.restart_service", return_value=True)
    @mock.patch("scripts.zeabur_watchdog.probe_url")
    @mock.patch("scripts.zeabur_watchdog.zeabur_graphql")
    def test_health_failure_restarts_service_when_server_online(
        self, graphql, probe_url, restart_service, send_feishu, save_state
    ):
        graphql.return_value = {
            "servers": [
                {
                    "_id": zw.DEFAULT_SERVER_ID,
                    "name": "tokyo",
                    "status": {"isOnline": True, "vmStatus": "RUNNING"},
                }
            ],
            "project": {
                "services": [
                    {"_id": "svc1", "name": "svc", "status": "RUNNING", "suspendedAt": None}
                ]
            },
        }
        probe_url.return_value = zw.ProbeResult(ok=False, status=None, elapsed_ms=100, error="timeout")
        args = argparse.Namespace(
            project_id=zw.DEFAULT_PROJECT_ID,
            environment_id=zw.DEFAULT_ENVIRONMENT_ID,
            server_id=zw.DEFAULT_SERVER_ID,
            state_file="",
            health_timeout=1,
            alert_cooldown=60,
            restart_cooldown=60,
            auto_restart_services=True,
            dry_run=False,
        )
        with mock.patch(
            "scripts.zeabur_watchdog.load_json_env",
            return_value=[
                {
                    "name": "svc",
                    "service_id": "svc1",
                    "health_url": "https://svc/health",
                    "restart_on_fail": True,
                }
            ],
        ):
            summary = zw.run_once(args)
        self.assertFalse(summary["ok"])
        restart_service.assert_called_once_with("svc1", zw.DEFAULT_ENVIRONMENT_ID, dry_run=False)
        send_feishu.assert_called_once()

    def test_cooldown_blocks_repeated_restart(self):
        state = {"service:svc1": 1000}
        self.assertFalse(zw.should_fire(state, "service:svc1", cooldown_minutes=60, now=1200))
        self.assertTrue(zw.should_fire(state, "service:svc1", cooldown_minutes=60, now=5000))

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_send_feishu_missing_config_returns_false(self):
        self.assertFalse(zw.send_feishu("hello", dry_run=False))


if __name__ == "__main__":
    unittest.main()
