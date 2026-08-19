import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_outreach
from app import auto_send


class LaunchOutreachTests(unittest.TestCase):
    def setUp(self):
        launch_outreach._LOCKS.clear()

    def test_requires_exact_single_real_confirmation_and_no_dry_run(self):
        with patch.dict(os.environ, {}, clear=True):
            launch_outreach.require_real_one_only("REAL_ONE_ONLY")
            with self.assertRaisesRegex(RuntimeError, "REAL_ONE_ONLY"):
                launch_outreach.require_real_one_only("yes")
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankie@example.com"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "DRY-RUN"):
                launch_outreach.require_real_one_only("REAL_ONE_ONLY")

    def test_participant_gate_requires_human_approval_and_exact_profile(self):
        activity = {
            "record_id": "activity1",
            "fields": {
                "活动ID": "campaign1", "产品主记录ID": "product1",
                "KOL已锁定名单版本": "evidence-v4", "KOL名单阻塞代码": "",
            },
        }
        participant = {
            "record_id": "participant1",
            "fields": {
                "活动ID": "campaign1", "产品家族ID": "product1", "对象类型": "KOL",
                "关联KOL": {"link_record_ids": ["kol1"]}, "参与状态": "已入围",
                "名单版本": "evidence-v4", "排序版本": "evidence-v4",
                "审核结论": "通过", "进入方式": "新开发", "活动分池": "新开发池",
                "达人主页": {"link": "https://www.youtube.com/@IndieAlpaca", "text": "主页"},
            },
        }
        launch_outreach.validate_participant_gate(
            activity, participant, campaign_id="campaign1", product_id="product1",
            contact_id="kol1", expected_profile_url="https://youtube.com/@indiealpaca/",
            expected_ranking_version="evidence-v4",
        )
        participant["fields"]["审核结论"] = "待审核"
        with self.assertRaisesRegex(RuntimeError, "审核结论"):
            launch_outreach.validate_participant_gate(
                activity, participant, campaign_id="campaign1", product_id="product1",
                contact_id="kol1", expected_profile_url="https://youtube.com/@indiealpaca/",
                expected_ranking_version="evidence-v4",
            )

    def test_profile_gate_accepts_same_youtube_channel_id_and_reviewed_handle(self):
        kol = {"fields": {"账号名": "IndieAlpaca", "YouTube频道ID": "UC_O4asJTG4G3PpNsA5Tkw3Q"}}
        self.assertTrue(launch_outreach._profile_identity_matches(
            "https://www.youtube.com/channel/UC_O4asJTG4G3PpNsA5Tkw3Q",
            "https://www.youtube.com/@IndieAlpaca",
            kol,
        ))
        self.assertFalse(launch_outreach._profile_identity_matches(
            "https://www.youtube.com/channel/UC_O4asJTG4G3PpNsA5Tkw3Q",
            "https://www.youtube.com/@AnotherCreator",
            kol,
        ))

    def test_existing_release_draft_never_automatically_resends(self):
        existing = {
            "record_id": "draft1",
            "fields": {"邮件草稿ID": "launch-run-0001", "邮件草稿状态": "发送失败"},
        }
        with patch.dict(os.environ, {}, clear=True), patch.object(
            launch_outreach, "_find_release_draft", new=AsyncMock(return_value=existing)
        ), patch.object(
            launch_outreach.auto_send, "send_one", new=AsyncMock()
        ) as send_mock:
            result = asyncio.run(launch_outreach.send_one_real(
                campaign_id="campaign1", participant_record_id="participant1",
                product_id="product1", contact_id="kol1", brand="FUNLAB",
                expected_profile_url="https://youtube.com/@IndieAlpaca",
                expected_ranking_version="evidence-v4", nonce="run-0001",
                approved_by="Frankie", confirm="REAL_ONE_ONLY",
            ))
        self.assertTrue(result["reused"])
        self.assertFalse(result["resent"])
        send_mock.assert_not_awaited()

    def test_successful_flow_creates_one_draft_sends_once_and_clears_global_gate(self):
        activity = {
            "record_id": "activity1",
            "fields": {
                "活动ID": "campaign1", "产品主记录ID": "product1",
                "KOL已锁定名单版本": "evidence-v4", "KOL名单阻塞代码": "",
                "发送邮件授权": False,
            },
        }
        participant = {
            "record_id": "participant1",
            "fields": {
                "活动ID": "campaign1", "产品家族ID": "product1", "对象类型": "KOL",
                "关联KOL": {"link_record_ids": ["kol1"]}, "参与状态": "已入围",
                "名单版本": "evidence-v4", "排序版本": "evidence-v4",
                "审核结论": "通过", "进入方式": "新开发", "活动分池": "新开发池",
                "基础评分快照": 84,
                "达人主页": {"link": "https://www.youtube.com/@IndieAlpaca", "text": "主页"},
            },
        }
        product = {
            "record_id": "product1",
            "fields": {
                "产品英文名": "FUNLAB Luminex Dave THE DIVER Edition - Switch 2 Pro Controller",
                "品牌": "FUNLAB", "派单模式": "活动专用",
                "官网链接": {"link": "https://example.com/dave", "text": "Dave"},
            },
        }
        kol = {
            "record_id": "kol1",
            "fields": {
                "账号名": "IndieAlpaca", "邮箱": "contact@indiealpa.ca",
                "YouTube频道ID": "UC_O4asJTG4G3PpNsA5Tkw3Q",
            },
        }
        replay = {"candidate": {"decision": "eligible_new_cold", "base_filter_passed": True}}
        draft_copy = {}
        update_calls = []

        async def fake_create(table_id, fields):
            draft_copy.update(fields)
            return "draft1"

        async def fake_update(table_id, record_id, fields):
            update_calls.append((table_id, record_id, fields))
            return {"record_id": record_id, "fields": fields}

        async def fake_get(table_id, record_id):
            return {
                "activity1": activity, "participant1": participant,
                "product1": product, "kol1": kol,
                "draft1": {"record_id": "draft1", "fields": draft_copy},
            }[record_id]

        with patch.dict(os.environ, {}, clear=True), \
             patch.object(launch_outreach, "_find_release_draft", new=AsyncMock(return_value=None)), \
             patch.object(launch_outreach.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(launch_outreach.launch_candidate_preview, "replay_candidate", new=AsyncMock(return_value=replay)), \
             patch.object(launch_outreach.feishu, "get_record", new=fake_get), \
             patch.object(launch_outreach.feishu, "create_record", new=fake_create), \
             patch.object(launch_outreach.feishu, "update_record", new=fake_update), \
             patch.object(launch_outreach.enrich, "gen_draft", new=AsyncMock(return_value={
                 "subject": "IndieAlpaca, dive into this",
                 "body": '<p>FUNLAB Luminex Dave THE DIVER Edition - Switch 2 Pro Controller.</p>'
                         '<p><a href="https://example.com/dave?utm_source=kol">See it</a></p>',
                 "highlights": "Dave fit", "angle": "licensed controller",
                 "utm_url": "https://example.com/dave?utm_source=kol", "utm_id": "indiealpaca",
             })), \
             patch.object(launch_outreach.auto_send, "pause_state", return_value={"paused": False, "paused_brands": {}}), \
             patch.object(launch_outreach.auto_send, "send_one", new=AsyncMock(return_value={
                 "ok": True, "msg_id": "msg1", "to": "contact@indiealpa.ca", "brand": "FUNLAB",
             })) as send_mock, \
             patch.object(launch_outreach, "validate_sent_raw", new=AsyncMock(return_value={
                 "passed": True, "checks": {"recipient": True}, "subject": "IndieAlpaca, dive into this",
             })):
            result = asyncio.run(launch_outreach.send_one_real(
                campaign_id="campaign1", participant_record_id="participant1",
                product_id="product1", contact_id="kol1", brand="FUNLAB",
                expected_profile_url="https://youtube.com/@indiealpaca",
                expected_ranking_version="evidence-v4", nonce="run-0002",
                approved_by="Frankie", confirm="REAL_ONE_ONLY",
            ))

        self.assertTrue(result["ok"])
        self.assertEqual("draft1", result["draft_id"])
        send_mock.assert_awaited_once()
        self.assertIs(
            auto_send._LAUNCH_ACTIVITY_RELEASE,
            send_mock.await_args.kwargs["activity_release"],
        )
        self.assertIn("launch-release:run-0002", draft_copy["邮件正文"])
        gate_values = [fields["发送邮件授权"] for _, _, fields in update_calls if "发送邮件授权" in fields]
        self.assertEqual([True, False], gate_values)

    def test_regular_sender_still_denies_activity_locked_cold_draft(self):
        draft = {
            "record_id": "draft-locked",
            "fields": {
                "邮件草稿来源": "cold", "关联产品": {"link_record_ids": ["product1"]},
            },
        }
        with patch.object(auto_send, "_locked_product_mode", new=AsyncMock(return_value="活动专用")), \
             patch.object(auto_send, "_deny_product_locked_ready", new=AsyncMock()) as deny_mock, \
             patch.object(auto_send.zoho, "send_email", new=AsyncMock()) as send_mock:
            result = asyncio.run(auto_send.send_one(draft))
        self.assertFalse(result["ok"])
        self.assertTrue(result["skipped"])
        deny_mock.assert_awaited_once()
        send_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
