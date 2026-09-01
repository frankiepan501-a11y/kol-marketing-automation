import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from app import kol_no_email_outreach as outreach


class KolNoEmailOutreachTests(unittest.TestCase):
    def _kol(self, **overrides):
        fields = {
            "账号名": "Game Dock Lab",
            "主平台": "YouTube",
            "主链接": {"link": "https://youtube.com/@gamedocklab", "text": "YouTube"},
            "国家": "United States",
            "语言": "English",
            "粉丝数": 42000,
            "近期视频标题": "Best Switch 2 desk setup",
            "聚合页URL": {"link": "https://linktr.ee/gamedocklab", "text": "Linktree"},
            "其他链接": "https://beacons.ai/gamedocklab",
            "邮箱": "",
            "邮箱验真状态": "未验",
            "资料可用状态": "缺资料",
            "触达路由状态": "待核对",
            "合作状态": "未建联",
            "决策反哺日志": "",
        }
        fields.update(overrides)
        return {"record_id": "kol-1", "fields": fields}

    def _activity(self):
        return {
            "record_id": "activity-1",
            "fields": {
                "活动ID": "launch-piranha",
                "活动名称": "9·15 POWKONG 食人花二代集中上稿",
                "品牌": "POWKONG",
                "产品英文名": "Piranha Plant Switch 2 Dock",
                "ERP SKU": "PK02-S2",
                "运行模式": "正式运行",
                "状态": "正式执行中",
                "窗口开始": 1787932800000,
                "发布窗口中点": 1788364800000,
                "窗口结束": 1788796800000,
                "目标上稿数": 20,
            },
        }

    def _participant(self):
        return {
            "record_id": "participant-1",
            "fields": {
                "活动ID": "launch-piranha",
                "参与状态": "已入围",
                "审核结论": "通过",
                "关联KOL": ["kol-1"],
                "产品家族ID": "YM24食人花二代",
                "对象类型": "KOL",
                "进入方式": "新开发",
                "活动分池": "新开发池",
                "计划上稿时间": 1788364800000,
                "承诺上稿时间": 1788451200000,
            },
        }

    def _case(self):
        return outreach.build_case(
            kol=self._kol(), activity=self._activity(), participant=self._participant()
        )

    def _event(self, action, *, email=""):
        return {
            "operator": {"name": "潘志聪", "open_id": "ou_frankie"},
            "context": {"open_message_id": "om_card"},
            "action": {
                "value": {
                    "action": action,
                    "kol_record_id": "kol-1",
                    "campaign_id": "launch-piranha",
                    "platform": "YouTube",
                },
                "form_value": {"business_email": email} if email else {},
            },
        }

    def test_card_is_evidence_rich_and_has_safe_structure(self):
        card = outreach.build_outreach_card(self._case())
        rendered = json.dumps(card, ensure_ascii=False)

        for expected in (
            "无邮箱私信获取联系邮箱", "Game Dock Lab", "YouTube",
            "United States", "English", "42000", "近期内容",
            "公开页面检查", "新开发池", "规定时间上稿", "POWKONG",
            "系统不会代发私信", outreach.DM_TEMPLATE_EN,
            outreach.DM_TEMPLATE_ZH, "business_email",
        ):
            self.assertIn(expected, rendered)
        self.assertEqual([], outreach.validate_outreach_card(card))

        nodes = list(outreach.card_nodes(card))
        forms = [node for node in nodes if node.get("tag") == "form"]
        self.assertEqual(1, len(forms))
        self.assertEqual(1, sum(
            1 for node in nodes
            if node.get("tag") == "button" and node.get("action_type") == "form_submit"
        ))
        self.assertFalse(any(
            isinstance(item, dict) and item.get("tag") == "action"
            for item in forms[0].get("elements", [])
        ))
        self.assertEqual(3, sum(1 for node in nodes if node.get("tag") == "action"))

    def test_candidate_without_participant_uses_activity_product_and_states_gap(self):
        case = outreach.build_case(
            kol=self._kol(), activity=self._activity(), participant=None,
        )

        self.assertEqual("Piranha Plant Switch 2 Dock", case["product"])
        self.assertIn("未建参与记录", case["fit_reason"])
        self.assertFalse(case["task_nature"].startswith("新开发"))

    def test_email_capture_returns_to_quality_gate_without_sending(self):
        update = AsyncMock(return_value={"record_id": "kol-1"})
        patch_card = AsyncMock(return_value=True)
        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=self._kol())), \
             patch.object(outreach.feishu, "update_record", update), \
             patch.object(outreach.feishu, "update_card_message_with_app", patch_card):
            result = asyncio.run(outreach.handle_callback(
                self._event(outreach.ACTION_EMAIL_CAPTURED, email="creator@example.com")
            ))

        self.assertTrue(result["ok"])
        self.assertFalse(result["idempotent"])
        written = update.await_args.args[2]
        self.assertEqual("creator@example.com", written["邮箱"])
        self.assertEqual("未验", written["邮箱验真状态"])
        self.assertEqual("人工核实有效", written["资料可用状态"])
        self.assertEqual("可新开发", written["触达路由状态"])
        self.assertEqual("建联中", written["合作状态"])
        self.assertIn("KOL本人提供", written["决策反哺日志"])
        self.assertIn("YouTube", written["决策反哺日志"])
        patch_card.assert_awaited_once()

    def test_invalid_email_is_rejected_without_writing(self):
        update = AsyncMock()
        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=self._kol())), \
             patch.object(outreach.feishu, "update_record", update):
            result = asyncio.run(outreach.handle_callback(
                self._event(outreach.ACTION_EMAIL_CAPTURED, email="not-an-email")
            ))

        self.assertFalse(result["ok"])
        self.assertIn("邮箱格式", result["error"])
        update.assert_not_awaited()

    def test_platform_ongoing_hands_off_without_enabling_new_email(self):
        update = AsyncMock(return_value={"record_id": "kol-1"})
        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=self._kol())), \
             patch.object(outreach.feishu, "update_record", update), \
             patch.object(outreach.feishu, "update_card_message_with_app", AsyncMock(return_value=True)):
            result = asyncio.run(outreach.handle_callback(
                self._event(outreach.ACTION_PLATFORM_ONGOING)
            ))

        self.assertTrue(result["ok"])
        written = update.await_args.args[2]
        self.assertEqual("沿用原线程", written["触达路由状态"])
        self.assertEqual("建联中", written["合作状态"])
        self.assertNotIn("邮箱", written)

    def test_not_fit_and_no_response_have_different_business_meaning(self):
        async def run(action):
            update = AsyncMock(return_value={"record_id": "kol-1"})
            with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=self._kol())), \
                 patch.object(outreach.feishu, "update_record", update), \
                 patch.object(outreach.feishu, "update_card_message_with_app", AsyncMock(return_value=True)):
                result = await outreach.handle_callback(self._event(action))
            return result, update.await_args.args[2]

        not_fit, not_fit_fields = asyncio.run(run(outreach.ACTION_NOT_FIT))
        no_reply, no_reply_fields = asyncio.run(run(outreach.ACTION_NO_RESPONSE))

        self.assertTrue(not_fit["ok"])
        self.assertEqual("不合适", not_fit_fields["合作状态"])
        self.assertEqual("禁止新开发", not_fit_fields["触达路由状态"])
        self.assertTrue(no_reply["ok"])
        self.assertEqual("缺资料", no_reply_fields["资料可用状态"])
        self.assertEqual("禁止新开发", no_reply_fields["触达路由状态"])
        self.assertNotIn("合作状态", no_reply_fields)

    def test_callback_is_idempotent_by_stable_log_marker(self):
        existing = self._kol(
            邮箱="creator@example.com",
            邮箱验真状态="未验",
            资料可用状态="人工核实有效",
            触达路由状态="可新开发",
            合作状态="建联中",
            决策反哺日志="[NO_EMAIL_DM:launch-piranha:email_captured] source=KOL本人提供",
        )
        update = AsyncMock()
        patch_card = AsyncMock(return_value=True)
        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=existing)), \
             patch.object(outreach.feishu, "update_record", update), \
             patch.object(outreach.feishu, "update_card_message_with_app", patch_card):
            result = asyncio.run(outreach.handle_callback(
                self._event(outreach.ACTION_EMAIL_CAPTURED, email="creator@example.com")
            ))

        self.assertTrue(result["ok"])
        self.assertTrue(result["idempotent"])
        update.assert_not_awaited()
        patch_card.assert_awaited_once()

    def test_first_outcome_wins_when_a_stale_card_submits_another_action(self):
        existing = self._kol(
            邮箱="creator@example.com",
            邮箱验真状态="未验",
            资料可用状态="人工核实有效",
            触达路由状态="可新开发",
            合作状态="建联中",
            决策反哺日志="[NO_EMAIL_DM:launch-piranha:email_captured] source=KOL本人提供",
        )
        update = AsyncMock()
        patch_card = AsyncMock(return_value=True)
        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=existing)), \
             patch.object(outreach.feishu, "update_record", update), \
             patch.object(outreach.feishu, "update_card_message_with_app", patch_card):
            result = asyncio.run(outreach.handle_callback(
                self._event(outreach.ACTION_NOT_FIT)
            ))

        self.assertTrue(result["ok"])
        self.assertTrue(result["idempotent"])
        self.assertTrue(result["conflict_ignored"])
        self.assertEqual(outreach.ACTION_EMAIL_CAPTURED, result["action"])
        update.assert_not_awaited()
        patch_card.assert_awaited_once()

    def test_callback_accepts_event_hub_normalized_payload(self):
        event = {
            "sender_open_id": "ou_frankie",
            "sender_name": "潘志聪",
            "message_id": "om_card",
            "card_action": {
                "action": outreach.ACTION_PLATFORM_ONGOING,
                "kol_record_id": "kol-1",
                "campaign_id": "launch-piranha",
                "platform": "YouTube",
            },
            "card_form_value": {},
        }
        update = AsyncMock(return_value={"record_id": "kol-1"})
        patch_card = AsyncMock(return_value=True)
        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=self._kol())), \
             patch.object(outreach.feishu, "update_record", update), \
             patch.object(outreach.feishu, "update_card_message_with_app", patch_card):
            result = asyncio.run(outreach.handle_callback(event))

        self.assertTrue(result["ok"])
        self.assertEqual("潘志聪", update.await_args.args[2]["决策反哺日志"].split("operator=")[1].split(";")[0])
        patch_card.assert_awaited_once()

    def test_targeted_load_refuses_to_guess_between_campaigns(self):
        other = self._activity()
        other["record_id"] = "activity-2"
        other["fields"] = {**other["fields"], "活动ID": "launch-dave", "活动名称": "Dave 集中上稿"}
        participant2 = self._participant()
        participant2["record_id"] = "participant-2"
        participant2["fields"] = {**participant2["fields"], "活动ID": "launch-dave"}

        async def fetch(table_id, **_kwargs):
            if table_id == outreach.config.T_LAUNCH_CAMPAIGN:
                return [self._activity(), other]
            if table_id == outreach.config.T_LAUNCH_PARTICIPANT:
                return [self._participant(), participant2]
            self.fail(table_id)

        with patch.object(outreach.feishu, "get_record", AsyncMock(return_value=self._kol())), \
             patch.object(outreach.feishu, "fetch_all_records", side_effect=fetch):
            with self.assertRaisesRegex(ValueError, "多个活动"):
                asyncio.run(outreach.load_case(kol_record_id="kol-1"))

    def test_real_card_rollout_is_frankie_only_until_flag_enabled(self):
        with patch.object(outreach, "load_case", AsyncMock(return_value=self._case())):
            with self.assertRaisesRegex(ValueError, "只允许 Frankie"):
                asyncio.run(outreach.send_card(
                    kol_record_id="kol-1", campaign_id="launch-piranha",
                    dry_run=False, frankie_only=False,
                ))


if __name__ == "__main__":
    unittest.main()
