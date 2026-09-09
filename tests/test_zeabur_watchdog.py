import argparse
import json
import os
import tempfile
import unittest
import urllib.error
from unittest import mock

from scripts import zeabur_watchdog as zw


class ZeaburWatchdogTests(unittest.TestCase):
    @mock.patch.dict(os.environ, {"CS_AUDIT_NOT_BEFORE_MS": "2000"}, clear=True)
    def test_cs_outbound_audit_uses_cutover_and_tracks_anomaly_until_fixed(self):
        legacy = {
            "record_id": "rec_legacy",
            "last_modified_time": "3000",
            "fields": {"状态": "已回复", "回复时间": 1999, "最近出站Message-ID": ""},
        }
        state = {}

        issues, summary = zw.evaluate_cs_outbound_records([legacy], state)
        self.assertEqual([], issues)
        self.assertEqual(0, summary["records_after_cutover"])

        new_bad = {
            "record_id": "rec_new",
            "last_modified_time": "2000",
            "fields": {
                "状态": "待客户补充",
                "补充信息请求时间": 2000,
                "最近出站Message-ID": "",
                "客户标识": "customer@example.com",
                "原文": "private body",
            },
        }
        issues, summary = zw.evaluate_cs_outbound_records([legacy, new_bad], state)
        self.assertEqual(["cs_outbound_missing_proof:rec_new"], [issue.key for issue in issues])
        self.assertEqual(1, summary["tracked_count"])
        self.assertIn("rec_new", issues[0].message)
        self.assertNotIn("customer@example.com", issues[0].message)
        self.assertNotIn("private body", issues[0].message)

        issues, summary = zw.evaluate_cs_outbound_records([legacy], state)
        self.assertEqual([], issues)
        self.assertEqual(0, summary["tracked_count"])

        issues, summary = zw.evaluate_cs_outbound_records([legacy, new_bad], state)
        self.assertEqual(["cs_outbound_missing_proof:rec_new"], [issue.key for issue in issues])
        self.assertEqual(1, summary["tracked_count"])

    def test_cs_outbound_audit_ignores_states_that_do_not_require_customer_send(self):
        records = [
            {"record_id": "rec_pending", "last_modified_time": str(zw.CS_AUDIT_NOT_BEFORE_MS_DEFAULT), "fields": {"状态": "待回", "最近出站Message-ID": ""}},
            {"record_id": "rec_escalated", "last_modified_time": str(zw.CS_AUDIT_NOT_BEFORE_MS_DEFAULT), "fields": {"状态": "已升级", "最近出站Message-ID": ""}},
            {"record_id": "rec_archived", "last_modified_time": str(zw.CS_AUDIT_NOT_BEFORE_MS_DEFAULT), "fields": {"状态": "归档非客服", "最近出站Message-ID": ""}},
            {"record_id": "rec_proven", "last_modified_time": str(zw.CS_AUDIT_NOT_BEFORE_MS_DEFAULT), "fields": {"状态": "已回复", "回复时间": zw.CS_AUDIT_NOT_BEFORE_MS_DEFAULT, "最近出站Message-ID": "<mid>"}},
        ]
        state = {}
        issues, summary = zw.evaluate_cs_outbound_records(records, state)
        self.assertEqual([], issues)
        self.assertEqual(0, summary["current_anomaly_count"])

    @mock.patch.dict(os.environ, {"CS_AUDIT_NOT_BEFORE_MS": "2000"}, clear=True)
    def test_cs_outbound_audit_does_not_mislabel_unknown_transition_as_missing_proof(self):
        record = {
            "record_id": "rec_unknown",
            "created_time": "3000",
            "last_modified_time": "3000",
            "fields": {"状态": "已回复", "回复时间": "", "最近出站Message-ID": ""},
        }
        issues, summary = zw.evaluate_cs_outbound_records([record], {})
        self.assertEqual(["cs_outbound_audit_time_missing:rec_unknown"], [issue.key for issue in issues])
        self.assertIn("无法确认", issues[0].message)
        self.assertNotIn("状态=已回复，但最近出站Message-ID为空", issues[0].message)
        self.assertEqual(1, summary["records_with_unknown_transition"])
        card = zw.build_alert_card(
            issues, [], {"name": "tokyo", "status": {"isOnline": True, "vmStatus": "RUNNING"}}, {}
        )
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("客服工单出站凭证异常", rendered)
        self.assertIn("record=rec_unknown", rendered)

    @mock.patch.dict(os.environ, {"CS_AUDIT_NOT_BEFORE_MS": "2000"}, clear=True)
    def test_cs_outbound_audit_ignores_unrelated_edit_to_legacy_record(self):
        record = {
            "record_id": "rec_legacy_edited",
            "created_time": "1000",
            "last_modified_time": "3000",
            "fields": {"状态": "已回复", "回复时间": "", "最近出站Message-ID": ""},
        }
        issues, summary = zw.evaluate_cs_outbound_records([record], {})
        self.assertEqual([], issues)
        self.assertEqual(0, summary["records_with_unknown_transition"])

    @mock.patch.dict(os.environ, {"CS_AUDIT_NOT_BEFORE_MS": "2000"}, clear=True)
    def test_cs_outbound_audit_catches_old_ticket_reentering_required_status(self):
        state = {"status_by_record": {"rec_reused": "待回"}}
        record = {
            "record_id": "rec_reused",
            "created_time": "1000",
            "last_modified_time": "3000",
            "fields": {"状态": "已回复", "回复时间": "", "最近出站Message-ID": ""},
        }
        issues, summary = zw.evaluate_cs_outbound_records([record], state)
        self.assertEqual(["cs_outbound_audit_time_missing:rec_reused"], [issue.key for issue in issues])
        self.assertEqual(1, summary["records_with_unknown_transition"])
        self.assertEqual("已回复", state["status_by_record"]["rec_reused"])

    @mock.patch.dict(os.environ, {"CS_AUDIT_NOT_BEFORE_MS": "2000"}, clear=True)
    def test_cs_outbound_audit_fails_closed_when_automatic_times_are_missing(self):
        record = {
            "record_id": "rec_no_clock",
            "fields": {"状态": "已回复", "回复时间": "", "最近出站Message-ID": ""},
        }
        issues, summary = zw.evaluate_cs_outbound_records([record], {})
        self.assertEqual(["cs_outbound_audit_source_time_missing"], [issue.key for issue in issues])
        self.assertIn("不能视为巡检健康", issues[0].message)
        self.assertEqual(1, summary["records_without_automatic_time"])
        card = zw.build_alert_card(
            issues, [], {"name": "tokyo", "status": {"isOnline": True, "vmStatus": "RUNNING"}}, {}
        )
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("客服工单出站凭证异常", rendered)
        self.assertIn("automatic_fields", rendered)
        self.assertNotIn("优先打开 Zeabur 构建日志", rendered)

    @mock.patch.dict(
        os.environ,
        {
            "FEISHU_NOTIFY_APP_ID": "app",
            "FEISHU_NOTIFY_APP_SECRET": "secret",
            "CS_AUDIT_APP_TOKEN": "base",
            "CS_AUDIT_TABLE_ID": "table",
        },
        clear=True,
    )
    @mock.patch("scripts.zeabur_watchdog.http_get_json")
    @mock.patch("scripts.zeabur_watchdog.feishu_token", return_value="tenant-token")
    def test_fetch_cs_ticket_records_uses_list_api_and_query_page_token(self, feishu_token, http_get):
        http_get.side_effect = [
            {"code": 0, "data": {"items": [{"record_id": "r1", "fields": {}}],
                                   "has_more": True, "page_token": "next token"}},
            {"code": 0, "data": {"items": [{"record_id": "r2", "fields": {}}],
                                   "has_more": False, "page_token": ""}},
        ]
        records = zw.fetch_cs_ticket_records()
        self.assertEqual(["r1", "r2"], [record["record_id"] for record in records])
        self.assertNotIn("page_token=", http_get.call_args_list[0].args[0])
        self.assertIn("automatic_fields=true", http_get.call_args_list[0].args[0])
        self.assertIn("field_names=", http_get.call_args_list[0].args[0])
        self.assertNotIn("%E5%AE%A2%E6%88%B7%E6%A0%87%E8%AF%86", http_get.call_args_list[0].args[0])
        self.assertIn("page_token=next+token", http_get.call_args_list[1].args[0])
        for call in http_get.call_args_list:
            self.assertEqual("Bearer tenant-token", call.kwargs["headers"]["Authorization"])

    def test_cs_only_alert_card_is_business_readable_and_actionable(self):
        issue = zw.Issue(
            "cs_outbound_missing_proof:rec_new",
            "critical",
            "工单 CSF-new 状态=已回复，但最近出站Message-ID为空",
            "CSF-new",
        )
        card = zw.build_alert_card(
            [issue], [], {"name": "tokyo", "status": {"isOnline": True, "vmStatus": "RUNNING"}}, {}
        )
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertEqual(
            "🟠 [AUDIT·P1] 客服工单出站凭证异常 · 1条",
            card["header"]["title"]["content"],
        )
        self.assertIn("Message-ID", rendered)
        self.assertIn("record=rec_new", rendered)

    def test_cs_alert_cards_include_every_record_link(self):
        issues = [
            zw.Issue(
                f"cs_outbound_missing_proof:rec_{index}",
                "critical",
                f"客服工单记录 rec_{index} 状态=已回复，但最近出站Message-ID为空；"
                f"https://example.invalid/?record=rec_{index}",
                f"rec_{index}",
            )
            for index in range(8)
        ]
        cards = zw.build_alert_cards(
            issues,
            [],
            {"name": "tokyo", "status": {"isOnline": True, "vmStatus": "RUNNING"}},
            {},
        )
        self.assertEqual(2, len(cards))
        rendered = "\n".join(json.dumps(card, ensure_ascii=False) for card in cards)
        for index in range(8):
            self.assertIn(f"record=rec_{index}", rendered)

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
        ), mock.patch(
            "scripts.zeabur_watchdog.utc_now_ts",
            return_value=zw.parse_utc_ts("2026-07-07T06:10:00Z"),
        ):
            summary = zw.run_once(args)
        self.assertFalse(summary["ok"])
        self.assertEqual(1, summary["deployment_issue_count"])
        self.assertEqual("deployment_failed:svc_ml:dep_fail", summary["issues"][0]["key"])
        self.assertIn("failed to download source code", summary["issues"][0]["message"])
        send_feishu.assert_called_once()
        self.assertIn("card", send_feishu.call_args.kwargs)
        self.assertEqual(
            "[AUDIT·P1] Zeabur 构建/运行告警",
            send_feishu.call_args.kwargs["card"]["header"]["title"]["content"],
        )
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

    def test_build_alert_card_compacts_deployment_details(self):
        issue = zw.Issue(
            "deployment_failed:svc_ml:dep_fail123456789",
            "critical",
            'ml-sync deployment dep_fail123456789 status=FAILED at 2026-07-07T05:44:21Z; commit=abcdef1 Fix close loop; log=ERROR failed to download source code err=fetch git ref: Get "https://github.com/frankiepan501-a11y/ml-data-sync.git/info/refs?service=git-upload-pack": dial tcp 20.205.243.166:443: i/o timeout',
            "ml-sync",
        )
        card = zw.build_alert_card(
            [issue],
            [],
            {"name": "tokyo", "status": {"isOnline": True, "vmStatus": "RUNNING"}},
            {"kol-automation": zw.ProbeResult(True, 200, 50)},
        )
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertEqual("interactive", "interactive")
        self.assertIn("ml-sync", rendered)
        self.assertIn("dep_fail123456789"[-8:], rendered)
        self.assertIn("[github.com]", rendered)
        self.assertNotIn("https://github.com", rendered)
        self.assertIn("本次需要处理", rendered)

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

    @mock.patch.dict(
        os.environ,
        {
            "FEISHU_NOTIFY_APP_ID": "app",
            "FEISHU_NOTIFY_APP_SECRET": "secret",
            "FEISHU_NOTIFY_OPEN_ID": "ou_a",
        },
        clear=True,
    )
    @mock.patch("scripts.zeabur_watchdog.http_json")
    @mock.patch("scripts.zeabur_watchdog.feishu_token", return_value="tenant-token")
    def test_send_feishu_sends_interactive_card(self, feishu_token, http_json):
        card = {"config": {"wide_screen_mode": True}, "elements": []}
        self.assertTrue(zw.send_feishu("fallback text", dry_run=False, card=card))
        payload = http_json.call_args.args[1]
        self.assertEqual("interactive", payload["msg_type"])
        self.assertEqual(card, json.loads(payload["content"]))


if __name__ == "__main__":
    unittest.main()
