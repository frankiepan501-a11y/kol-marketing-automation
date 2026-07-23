import json
import os
import unittest

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
