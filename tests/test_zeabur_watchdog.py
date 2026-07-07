import argparse
import os
import tempfile
import unittest
import urllib.error
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

    def test_evaluate_deployments_reports_unseen_recent_failure(self):
        issues = zw.evaluate_deployments(
            {"_id": "svc_ml", "name": "ml-sync"},
            [
                {
                    "_id": "dep_run",
                    "status": "RUNNING",
                    "createdAt": "2026-07-07T06:00:00Z",
                },
                {
                    "_id": "dep_fail",
                    "status": "FAILED",
                    "createdAt": "2026-07-07T05:44:21Z",
                    "commitSHA": "abcdef123456",
                    "commitMessage": "Fix close loop",
                },
                {
                    "_id": "dep_seen",
                    "status": "FAILED",
                    "createdAt": "2026-07-07T05:43:00Z",
                },
                {
                    "_id": "dep_old",
                    "status": "FAILED",
                    "createdAt": "2026-07-05T05:43:00Z",
                },
            ],
            seen_deployments={"dep_seen": 1},
            now=zw.parse_utc_ts("2026-07-07T06:10:00Z"),
            lookback_minutes=24 * 60,
            failure_statuses={"FAILED"},
            include_build_logs=False,
        )
        self.assertEqual(1, len(issues))
        self.assertEqual("deployment_failed:svc_ml:dep_fail", issues[0].key)
        self.assertIn("ml-sync deployment dep_fail status=FAILED", issues[0].message)

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

    @mock.patch.dict(os.environ, {"ZEABUR_API_KEY": "dummy"}, clear=True)
    @mock.patch("scripts.zeabur_watchdog.save_state")
    @mock.patch("scripts.zeabur_watchdog.send_feishu", return_value=True)
    @mock.patch("scripts.zeabur_watchdog.probe_url", return_value=zw.ProbeResult(True, 200, 50))
    @mock.patch("scripts.zeabur_watchdog.zeabur_graphql")
    def test_run_once_alerts_any_project_service_failed_deployment(
        self, graphql, probe_url, send_feishu, save_state
    ):
        def fake_graphql(query, variables):
            if "buildLogs" in query:
                return {"buildLogs": [{"message": "ERROR failed to download source code"}]}
            if "deployments" in query:
                if variables["serviceID"] == "svc_ml":
                    return {
                        "deployments": {
                            "edges": [
                                {
                                    "node": {
                                        "_id": "dep_fail",
                                        "serviceID": "svc_ml",
                                        "status": "FAILED",
                                        "createdAt": "2026-07-07T05:44:21Z",
                                        "commitSHA": "abcdef123456",
                                        "commitMessage": "Fix close loop",
                                    }
                                }
                            ]
                        }
                    }
                return {"deployments": {"edges": []}}
            return {
                "servers": [
                    {
                        "_id": zw.DEFAULT_SERVER_ID,
                        "name": "tokyo",
                        "status": {"isOnline": True, "vmStatus": "RUNNING"},
                    }
                ],
                "project": {
                    "services": [
                        {"_id": "svc1", "name": "svc", "status": "RUNNING", "suspendedAt": None},
                        {"_id": "svc_ml", "name": "ml-sync", "status": "RUNNING", "suspendedAt": None},
                    ]
                },
            }

        graphql.side_effect = fake_graphql
        args = argparse.Namespace(
            project_id=zw.DEFAULT_PROJECT_ID,
            environment_id=zw.DEFAULT_ENVIRONMENT_ID,
            server_id=zw.DEFAULT_SERVER_ID,
            state_file="",
            health_timeout=1,
            alert_cooldown=60,
            restart_cooldown=60,
            auto_restart_services=True,
            check_deployments=True,
            deployment_per_page=10,
            deployment_lookback=24 * 60,
            deployment_seen_retention=7 * 24 * 60,
            deployment_failure_statuses={"FAILED"},
            include_build_logs=True,
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
        self.assertEqual(1, summary["deployment_issue_count"])
        self.assertEqual("deployment_failed:svc_ml:dep_fail", summary["issues"][0]["key"])
        self.assertIn("failed to download source code", summary["issues"][0]["message"])
        send_feishu.assert_called_once()
        saved_state = save_state.call_args.args[1]
        self.assertIn("dep_fail", saved_state["deployments"])

    def test_cooldown_blocks_repeated_restart(self):
        state = {"service:svc1": 1000}
        self.assertFalse(zw.should_fire(state, "service:svc1", cooldown_minutes=60, now=1200))
        self.assertTrue(zw.should_fire(state, "service:svc1", cooldown_minutes=60, now=5000))

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_send_feishu_missing_config_returns_false(self):
        self.assertFalse(zw.send_feishu("hello", dry_run=False))

    def test_split_targets_accepts_multiple_separators(self):
        self.assertEqual(
            zw.split_targets("ou_a, ou_b;ou_c\nou_d"),
            ["ou_a", "ou_b", "ou_c", "ou_d"],
        )

    @mock.patch.dict(
        os.environ,
        {"ZEABUR_API_KEY": "dummy", "WATCHDOG_ZEABUR_API_RETRIES": "2"},
        clear=True,
    )
    @mock.patch("scripts.zeabur_watchdog.time.sleep")
    @mock.patch("scripts.zeabur_watchdog.http_json")
    def test_zeabur_graphql_retries_transient_errors(self, http_json, sleep):
        http_json.side_effect = [
            RuntimeError("URL error https://api.zeabur.com/graphql: EOF"),
            {"data": {"ok": True}},
        ]
        self.assertEqual({"ok": True}, zw.zeabur_graphql("query {}", {}))
        self.assertEqual(2, http_json.call_count)
        sleep.assert_called_once()

    @mock.patch.dict(os.environ, {"WATCHDOG_HEALTH_RETRIES": "2"}, clear=True)
    @mock.patch("scripts.zeabur_watchdog.time.sleep")
    @mock.patch("scripts.zeabur_watchdog.urllib.request.urlopen")
    def test_probe_url_retries_transient_errors(self, urlopen, sleep):
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        urlopen.side_effect = [urllib.error.URLError("EOF"), Response()]
        result = zw.probe_url("https://svc/health", timeout=1)
        self.assertTrue(result.ok)
        self.assertEqual(2, urlopen.call_count)
        sleep.assert_called_once()

    @mock.patch.dict(
        os.environ,
        {
            "FEISHU_NOTIFY_APP_ID": "app",
            "FEISHU_NOTIFY_APP_SECRET": "secret",
            "FEISHU_NOTIFY_OPEN_ID": "ou_a,ou_b",
            "FEISHU_NOTIFY_CHAT_ID": "oc_c",
        },
        clear=True,
    )
    @mock.patch("scripts.zeabur_watchdog.http_json")
    @mock.patch("scripts.zeabur_watchdog.feishu_token", return_value="tenant-token")
    def test_send_feishu_sends_to_all_users_and_chats(self, feishu_token, http_json):
        self.assertTrue(zw.send_feishu("hello", dry_run=False))
        self.assertEqual(http_json.call_count, 3)
        urls = [call.args[0] for call in http_json.call_args_list]
        payloads = [call.args[1] for call in http_json.call_args_list]
        self.assertEqual(
            urls,
            [
                "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
                "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
                "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            ],
        )
        self.assertEqual([payload["receive_id"] for payload in payloads], ["ou_a", "ou_b", "oc_c"])


if __name__ == "__main__":
    unittest.main()
