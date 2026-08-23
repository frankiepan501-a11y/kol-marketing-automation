import asyncio
import datetime as dt
import json
import os
import tempfile
import unittest
from zoneinfo import ZoneInfo


for key in [
    "FEISHU_BITABLE_APP_ID",
    "FEISHU_BITABLE_APP_SECRET",
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
]:
    os.environ.setdefault(key, "test")

from app import feishu, sla_check  # noqa: E402


SHANGHAI = ZoneInfo("Asia/Shanghai")


def at_local(year, month, day, hour):
    return int(dt.datetime(year, month, day, hour, tzinfo=SHANGHAI).timestamp() * 1000)


def draft(rid, source, age_hours, now_ms, *, status="待审", subject=None, card_sent_ms=0):
    return {
        "record_id": rid,
        "fields": {
            "邮件草稿状态": status,
            "邮件草稿来源": source,
            "对象类型": "KOL",
            "邮件主题": subject or f"subject-{rid}",
            "AI评分": 8,
            "生成时间": now_ms - age_hours * 3600 * 1000,
            "卡片发送时间": card_sent_ms,
        },
    }


class SlaDigestTests(unittest.TestCase):
    def setUp(self):
        self.originals = {
            "search_records": feishu.search_records,
            "resolve_notify_targets": feishu.resolve_notify_targets,
            "send_card_message": feishu.send_card_message,
            "update_record": feishu.update_record,
            "nudge": sla_check._layer_soft_nudge,
            "layer3": sla_check._layer3_no_content_30d,
            "layer4": sla_check._layer4_low_roi_60d,
            "frankie_only": sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY,
            "state_dir": sla_check.config.KOL_SLA_STATE_DIR,
        }
        self.state_tmp = tempfile.TemporaryDirectory()
        sla_check.config.KOL_SLA_STATE_DIR = self.state_tmp.name

    def tearDown(self):
        feishu.search_records = self.originals["search_records"]
        feishu.resolve_notify_targets = self.originals["resolve_notify_targets"]
        feishu.send_card_message = self.originals["send_card_message"]
        feishu.update_record = self.originals["update_record"]
        sla_check._layer_soft_nudge = self.originals["nudge"]
        sla_check._layer3_no_content_30d = self.originals["layer3"]
        sla_check._layer4_low_roi_60d = self.originals["layer4"]
        sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY = self.originals["frankie_only"]
        sla_check.config.KOL_SLA_STATE_DIR = self.originals["state_dir"]
        self.state_tmp.cleanup()

    def _install_fakes(self, waiting_review, waiting_tracking=()):
        sent = []
        updated = []

        async def fake_search(table_id, filters, field_names=None):
            conditions = {x["field_name"]: x["value"] for x in filters}
            if conditions.get("SLA已升级") == ["true"]:
                return [
                    rec for rec in list(waiting_review) + list(waiting_tracking)
                    if (rec.get("fields") or {}).get("SLA已升级")
                ]
            if conditions.get("邮件草稿状态") == ["待修改"]:
                return list(waiting_tracking)
            if conditions.get("邮件草稿状态") == ["待审"]:
                return list(waiting_review)
            return []

        async def fake_targets(role):
            if role == "ship_main":
                return [("运营负责人", "ou_ops")]
            if role == "frankie":
                return [("Frankie", "ou_frankie")]
            raise AssertionError(f"unexpected role: {role}")

        async def fake_send(receive_type, receive_id, card, biz="KOL", level="P1"):
            sent.append({
                "receive_type": receive_type,
                "receive_id": receive_id,
                "card": card,
                "biz": biz,
                "level": level,
            })
            return f"om_{len(sent)}"

        async def fake_update(table_id, record_id, fields):
            updated.append((record_id, fields))

        async def noop(now_ms):
            return {"layer": "noop", "checked": 0}

        feishu.search_records = fake_search
        feishu.resolve_notify_targets = fake_targets
        feishu.send_card_message = fake_send
        feishu.update_record = fake_update
        sla_check._layer_soft_nudge = noop
        sla_check._layer3_no_content_30d = noop
        sla_check._layer4_low_roi_60d = noop
        return sent, updated

    def test_p1_digest_is_one_card_to_reviewer_and_one_48h_exception_to_frankie(self):
        sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY = False
        now_ms = at_local(2026, 8, 23, 18)
        rows = [
            draft("rec_reply", "reply", 29, now_ms),
            draft("rec_quote", "affiliate_quote", 52, now_ms),
            draft("rec_ship", "ship_confirm", 31, now_ms),
            draft("rec_cold", "cold", 40, now_ms),
        ]
        tracking = [draft(
            "rec_tracking", "tracking_followup", 27, now_ms, status="待修改"
        )]
        sent, _ = self._install_fakes(rows, tracking)

        result = asyncio.run(sla_check.run(now_ms=now_ms))

        self.assertEqual(len(sent), 2)
        self.assertEqual(
            [(x["receive_type"], x["receive_id"], x["level"]) for x in sent],
            [("open_id", "ou_ops", "P1"), ("open_id", "ou_frankie", "P1")],
        )
        self.assertNotIn("chat_id", [x["receive_type"] for x in sent])

        reviewer_text = str(sent[0]["card"])
        self.assertIn("独立站运营专员", reviewer_text)
        self.assertIn("有 4 封重要合作邮件待审核", reviewer_text)
        self.assertIn("现在需要你做什么", reviewer_text)
        self.assertIn("今天 4 小时内", reviewer_text)
        self.assertIn("先补齐运单号和物流商", reviewer_text)
        self.assertIn("点「通过」后，邮件会进入真实发送流程", reviewer_text)
        self.assertIn("打开待审核邮件列表", reviewer_text)
        self.assertIn("record=rec_quote", reviewer_text)
        self.assertIn("record=rec_reply", reviewer_text)
        self.assertNotIn("record=rec_cold", reviewer_text)
        for jargon in ["SLA", "`reply`", "`affiliate_quote`", "系统已检查", "在途草稿队列"]:
            self.assertNotIn(jargon, reviewer_text)

        frankie_text = str(sent[1]["card"])
        self.assertIn("你不用逐封审核", frankie_text)
        self.assertIn("超过 48 小时没处理", frankie_text)
        self.assertIn("record=rec_quote", frankie_text)

        layer = result["layer_1"]
        self.assertEqual(layer["p1_overdue"], 4)
        self.assertEqual(layer["p1_over_48h"], 1)
        self.assertEqual(layer["p2_overdue"], 1)
        self.assertEqual(layer["reviewer_digest_sent"], 1)
        self.assertEqual(layer["frankie_digest_sent"], 1)
        self.assertEqual(layer["p2_digest_sent"], 0)

    def test_p2_digest_is_sent_once_in_the_daily_hour_only(self):
        sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY = False
        noon_ms = at_local(2026, 8, 23, 12)
        rows = [
            draft("rec_a", "cold", 30, noon_ms),
            draft("rec_b", "followup", 36, noon_ms),
            draft("rec_c", "secondary_outreach", 50, noon_ms),
        ]
        sent, updated = self._install_fakes(rows)

        result = asyncio.run(sla_check.run(now_ms=noon_ms))

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["receive_id"], "ou_ops")
        self.assertEqual(sent[0]["level"], "P2")
        card_text = str(sent[0]["card"])
        self.assertIn("有 3 封日常合作邮件待审核", card_text)
        self.assertIn("明天前", card_text)
        self.assertIn("首次合作邀请", card_text)
        self.assertIn("未回复跟进", card_text)
        self.assertIn("二次联系", card_text)
        for jargon in ["cold", "followup", "secondary_outreach", "SLA", "P2"]:
            self.assertNotIn(jargon, card_text)
        self.assertEqual(result["layer_1"]["p2_digest_sent"], 1)
        self.assertEqual(
            updated,
            [
                ("rec_a", {"SLA已升级": True, "卡片发送时间": noon_ms}),
                ("rec_b", {"卡片发送时间": noon_ms}),
                ("rec_c", {"卡片发送时间": noon_ms}),
            ],
        )

    def test_p2_digest_does_not_repeat_when_today_was_already_recorded(self):
        sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY = False
        noon_ms = at_local(2026, 8, 23, 12)
        rows = [
            draft("rec_cold", "cold", 30, noon_ms, card_sent_ms=noon_ms - 60_000),
            draft("rec_new", "followup", 31, noon_ms),
        ]
        sent, updated = self._install_fakes(rows)

        result = asyncio.run(sla_check.run(now_ms=noon_ms))

        self.assertEqual(sent, [])
        self.assertEqual(updated, [])
        self.assertEqual(result["layer_1"]["p2_overdue"], 2)
        self.assertEqual(result["layer_1"]["p2_due_today"], 0)

    def test_p2_daily_run_claim_is_atomic_and_independent_of_queue_records(self):
        noon_ms = at_local(2026, 8, 23, 12)

        self.assertTrue(sla_check._claim_p2_daily_run(noon_ms))
        self.assertFalse(sla_check._claim_p2_daily_run(noon_ms))

    def test_frankie_only_gate_routes_reviewer_digest_to_explicit_frankie(self):
        sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY = True
        now_ms = at_local(2026, 8, 23, 18)
        rows = [draft("rec_reply", "reply", 29, now_ms)]
        sent, _ = self._install_fakes(rows)

        result = asyncio.run(sla_check.run(now_ms=now_ms))

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["receive_id"], "ou_frankie")
        self.assertEqual(result["layer_1"]["reviewer_digest_sent"], 1)

    def test_card_uses_plain_operational_language_and_one_clear_action(self):
        now_ms = at_local(2026, 8, 23, 18)
        rows = [
            draft("rec_a", "reply", 29, now_ms),
            draft("rec_b", "affiliate_quote", 31, now_ms),
        ]

        card = sla_check.build_sla_digest_card(rows, now_ms, audience="reviewer", level="P1")
        payload = json.dumps(card, ensure_ascii=False)

        self.assertIn("先看对方原邮件", payload)
        self.assertIn("没问题点「通过」", payload)
        self.assertIn("需要修改就先改正文", payload)
        self.assertIn("不适合发送就点「否决」或「退回重做」", payload)
        self.assertIn("系统已自动排除处理完的邮件", payload)
        self.assertEqual(payload.count("打开待审核邮件列表"), 1)
        for jargon in ["SLA", "超时", "队列", "reply", "affiliate_quote", "幂等", "元数据"]:
            self.assertNotIn(jargon, payload)

    def test_p2_digest_is_silent_outside_the_daily_hour(self):
        sla_check.config.KOL_SLA_CARD_FRANKIE_ONLY = False
        evening_ms = at_local(2026, 8, 23, 18)
        rows = [draft("rec_cold", "cold", 30, evening_ms)]
        sent, updated = self._install_fakes(rows)

        result = asyncio.run(sla_check.run(now_ms=evening_ms))

        self.assertEqual(sent, [])
        self.assertEqual(result["layer_1"]["p2_overdue"], 1)
        self.assertEqual(result["layer_1"]["p2_digest_sent"], 0)
        self.assertEqual(updated, [])


if __name__ == "__main__":
    unittest.main()
