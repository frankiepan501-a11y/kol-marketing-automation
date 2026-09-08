import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch

for _key in (
    "FEISHU_NOTIFY_APP_ID",
    "FEISHU_NOTIFY_APP_SECRET",
    "FEISHU_APP3_ID",
    "FEISHU_APP3_SECRET",
    "FEISHU_APP_TOKEN",
    "T_KOL",
    "T_EDITOR",
    "T_DRAFT",
    "T_KOL_FU",
    "T_EDITOR_FU",
    "T_DASH",
    "T_PRODUCT",
    "T_TASK_KOL",
    "T_TASK_EDITOR",
    "SNOV_CLIENT_ID",
    "SNOV_CLIENT_SECRET",
    "INTERNAL_TOKEN",
):
    os.environ.setdefault(_key, "test")

from app import main


class EndpointFailureCardTests(unittest.TestCase):
    def test_health_fails_closed_when_real_kol_base_probe_fails(self):
        with patch.object(main.config, "KOL_DEEPSEEK_API_KEY", "test-key"), \
             patch.object(main.config, "KOL_FEISHU_CONFIG_READY", True), \
             patch.object(
                 main.feishu,
                 "probe_kol_bitable_access",
                 new=AsyncMock(return_value={
                     "ok": False,
                     "status_code": 400,
                     "feishu_code": 99991672,
                     "message": "Access denied",
                 }),
             ), patch.object(
                 main._endpoint_alert_dedup,
                 "snapshot",
                 return_value={"state_available": True, "persistent": True},
             ):
            result = asyncio.run(main.health())

        self.assertEqual("degraded", result["status"])
        self.assertFalse(result["kol_feishu_migration"]["ready"])
        self.assertFalse(result["kol_feishu_migration"]["base_access"]["ok"])
        self.assertEqual(
            99991672,
            result["kol_feishu_migration"]["base_access"]["feishu_code"],
        )

    def test_health_is_ready_only_after_real_kol_base_probe_succeeds(self):
        with patch.object(main.config, "KOL_DEEPSEEK_API_KEY", "test-key"), \
             patch.object(main.config, "KOL_FEISHU_CONFIG_READY", True), \
             patch.object(
                 main.feishu,
                 "probe_kol_bitable_access",
                 new=AsyncMock(return_value={"ok": True, "records_visible": True}),
             ), patch.object(
                 main._endpoint_alert_dedup,
                 "snapshot",
                 return_value={"state_available": True, "persistent": True},
             ):
            result = asyncio.run(main.health())

        self.assertEqual("ok", result["status"])
        self.assertTrue(result["kol_feishu_migration"]["ready"])
        self.assertTrue(result["kol_feishu_migration"]["base_access"]["ok"])

    def test_failed_kol_alert_delivery_releases_persistent_claim(self):
        claim = Mock(return_value=True)
        release = Mock()
        sender = AsyncMock(side_effect=RuntimeError("temporary send failure"))
        with patch.object(main._endpoint_alert_dedup, "claim", new=claim), \
             patch.object(main._endpoint_alert_dedup, "release", new=release), \
             patch.object(main.config, "KOL_NOTIFY_USERS", [("潘志聪", "on_frankie")]), \
             patch.object(main.feishu, "send_card_message", new=sender):
            asyncio.run(main._alert_endpoint_failure("/launch/runtime/autonomous", "boom"))

        claim.assert_called_once_with("/launch/runtime/autonomous")
        release.assert_called_once_with("/launch/runtime/autonomous")

    def test_non_kol_alert_keeps_in_process_dedup(self):
        claim = Mock(return_value=True)
        sender = AsyncMock(return_value="om_b2b")
        main._alert_last.clear()
        with patch.object(main._endpoint_alert_dedup, "claim", new=claim), \
             patch.object(main.config, "NOTIFY_USERS", [("潘志聪", "ou_frankie")]), \
             patch.object(main.feishu, "send_card_message", new=sender):
            asyncio.run(main._alert_endpoint_failure("/b2b-mail-reminder/run", "boom"))

        claim.assert_not_called()
        sender.assert_awaited_once()

    def test_auto_send_data_not_ready_card_is_operator_readable(self):
        error = (
            'GET /bitable/v1/apps/KIN/tables/tblpWteXNX34vds4/records?page_size=100'
            ' → 400: {"code":1254607,"msg":"Data not ready, please try again later",'
            '"error":{"log_id":"202607231202194A328CC"}}'
        )
        trace = "Traceback...\n" + error

        card, level = main._build_endpoint_failure_card("/auto-send/run", error, trace)
        text = json.dumps(card, ensure_ascii=False)

        self.assertEqual(level, "P2")
        self.assertEqual(card["header"]["template"], "yellow")
        self.assertTrue(card["config"]["wide_screen_mode"])
        self.assertIn("[KOL·P2]", card["header"]["title"]["content"])
        self.assertIn("KOL 发信链读表暂时失败", card["header"]["title"]["content"])
        self.assertIn("飞书数据未就绪", text)
        self.assertIn("运营无需处理草稿", text)
        self.assertIn("没有证据显示已误发邮件", text)
        self.assertIn("下次 cron 会自动再试", text)
        self.assertIn("飞书 log_id", text)
        self.assertIn("202607231202194A328CC", text)
        self.assertNotIn("Trace 末段", text)

    def test_non_transient_failure_card_uses_p1_and_clear_action(self):
        card, level = main._build_endpoint_failure_card(
            "/auto-send/run",
            "Zoho OAuth failed",
            "RuntimeError: Zoho OAuth failed",
        )
        text = json.dumps(card, ensure_ascii=False)

        self.assertEqual(level, "P1")
        self.assertEqual(card["header"]["template"], "red")
        self.assertIn("[KOL·P1]", card["header"]["title"]["content"])
        self.assertIn("KOL 发信链运行失败", card["header"]["title"]["content"])
        self.assertIn("技术侧检查", text)
        self.assertIn("运营先不要手动改草稿状态", text)


if __name__ == "__main__":
    unittest.main()
