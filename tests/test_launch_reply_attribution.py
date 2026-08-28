import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_reply_attribution as attribution


class LaunchReplyAttributionTests(unittest.TestCase):
    def _activity(self, campaign_id="launch-piranha", name="POWKONG 食人花二代集中上稿"):
        return {
            "record_id": f"activity-{campaign_id}",
            "fields": {
                "活动ID": campaign_id,
                "活动名称": name,
                "品牌": "POWKONG",
                "运行模式": "正式运行",
                "状态": "正式执行中",
                "窗口开始": 1787932800000,
                "发布窗口中点": 1788364800000,
                "窗口结束": 1788796800000,
            },
        }

    def _participant(self, campaign_id="launch-piranha"):
        return {
            "record_id": "participant-1",
            "fields": {
                "活动ID": campaign_id,
                "参与状态": "已入围",
                "审核结论": "通过",
                "关联KOL": ["kol-1"],
                "产品家族ID": "product-1",
                "进入方式": "继续洽谈",
                "活动分池": "现有流程贡献池",
                "对象类型": "KOL",
                "计划上稿时间": 1788364800000,
                "承诺上稿时间": 1788451200000,
                "关联邮件草稿": ["cold-1"],
            },
        }

    def _reply(self, intent="感兴趣", body="Sounds good. Which campaign is this for?"):
        return {
            "record_id": "reply-1",
            "fields": {
                "邮件草稿来源": "reply",
                "邮件草稿状态": "待审",
                "审核路径": "待人审",
                "卡片已标记已审": False,
                "关联KOL": ["kol-1"],
                "关联产品": ["product-1"],
                "回复意图": intent,
                "回复原文": body,
                "回复目标MsgID": "orphan-mid",
            },
        }

    def test_unmatched_reply_becomes_actionable_campaign_choice(self):
        cases = attribution.collect_unmatched_reply_cases(
            activities=[self._activity()],
            participants=[self._participant()],
            drafts=[self._reply()],
            contacts={"kol-1": {"record_id": "kol-1", "fields": {"账号名": "Just the Gems"}}},
            products={"product-1": {"record_id": "product-1", "fields": {"产品名": "食人花二代"}}},
        )

        self.assertEqual(1, len(cases))
        self.assertEqual("reply-1", cases[0]["reply_record_id"])
        self.assertEqual("launch-piranha", cases[0]["candidates"][0]["campaign_id"])
        self.assertEqual("继续洽谈", cases[0]["candidates"][0]["entry_mode"])
        self.assertEqual("现有流程贡献池", cases[0]["candidates"][0]["task_nature"])
        self.assertTrue(cases[0]["candidates"][0]["timed_upload_required"])
        self.assertEqual("待运营确认", cases[0]["attribution_status"])

    def test_rejection_and_post_cooperation_pure_thanks_do_not_open_attribution_card(self):
        rejected = self._reply(intent="委婉拒绝", body="No thanks, please close this out.")
        thanks = self._reply(intent="不明意图", body="Thank you again!")

        self.assertTrue(attribution.is_terminal_without_attribution(rejected, cooperation_status="洽谈中"))
        self.assertTrue(attribution.is_terminal_without_attribution(thanks, cooperation_status="已合作-免费"))
        self.assertFalse(
            attribution.is_terminal_without_attribution(
                self._reply(intent="不明意图", body="Thanks, the sample arrived. When should I post?"),
                cooperation_status="已合作-免费",
            )
        )

    def test_card_shows_task_nature_and_upload_deadline_with_one_submit(self):
        case = attribution.collect_unmatched_reply_cases(
            activities=[self._activity()],
            participants=[self._participant()],
            drafts=[self._reply()],
            contacts={"kol-1": {"record_id": "kol-1", "fields": {"账号名": "Just the Gems"}}},
            products={"product-1": {"record_id": "product-1", "fields": {"产品名": "食人花二代"}}},
        )[0]
        card = attribution.build_attribution_card(case)
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("活动归属待确认", rendered)
        self.assertIn("继续洽谈", rendered)
        self.assertIn("现有流程贡献池", rendered)
        self.assertIn("规定时间上稿", rendered)
        self.assertIn("launch-piranha", rendered)
        self.assertIn("select_static", rendered)
        self.assertEqual([], attribution.validate_attribution_card(card))

        nodes = list(attribution.card_nodes(card))
        self.assertEqual(1, sum(
            1 for node in nodes
            if node.get("tag") == "button" and node.get("action_type") == "form_submit"
        ))

    def test_callback_is_idempotent_and_patches_original_card(self):
        event = {
            "operator": {"name": "张佳烨", "open_id": "ou_operator"},
            "context": {"open_message_id": "om_card"},
            "action": {
                "value": {
                    "action": attribution.ACTION_CONFIRM,
                    "reply_record_id": "reply-1",
                    "allowed_campaign_ids": ["launch-piranha"],
                },
                "form_value": {"campaign_id": "launch-piranha"},
            },
        }
        existing = self._reply()
        get_record = AsyncMock(side_effect=[existing, {
            **existing,
            "fields": {
                **existing["fields"],
                "集中宣发活动ID": "launch-piranha",
                "活动归属状态": "已确认",
            },
        }])
        update = AsyncMock(return_value={"record_id": "reply-1"})
        patch_card = AsyncMock(return_value=True)

        with patch.object(attribution.feishu, "get_record", get_record), \
             patch.object(attribution.feishu, "update_record", update), \
             patch.object(attribution.feishu, "update_card_message_with_app", patch_card):
            first = asyncio.run(attribution.handle_callback(event))
            second = asyncio.run(attribution.handle_callback(event))

        self.assertTrue(first["ok"])
        self.assertEqual("launch-piranha", first["campaign_id"])
        self.assertTrue(second["ok"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(1, update.await_count)
        self.assertGreaterEqual(patch_card.await_count, 1)

    def test_callback_accepts_event_hub_normalized_payload(self):
        event = {
            "sender_open_id": "ou_operator",
            "sender_name": "张佳烨",
            "message_id": "om_card",
            "card_action": {
                "action": attribution.ACTION_CONFIRM,
                "reply_record_id": "reply-1",
                "allowed_campaign_ids": ["launch-piranha"],
            },
            "card_form_value": {"campaign_id": "launch-piranha"},
        }
        update = AsyncMock(return_value={"record_id": "reply-1"})
        patch_card = AsyncMock(return_value=True)

        with patch.object(attribution.feishu, "get_record", AsyncMock(return_value=self._reply())), \
             patch.object(attribution.feishu, "update_record", update), \
             patch.object(attribution.feishu, "update_card_message_with_app", patch_card):
            result = asyncio.run(attribution.handle_callback(event))

        self.assertTrue(result["ok"])
        self.assertEqual("launch-piranha", result["campaign_id"])
        written = update.await_args.args[2]
        self.assertEqual("张佳烨", written["活动归属确认人"])
        patch_card.assert_awaited_once()

    def test_targeted_load_avoids_full_draft_table_scan(self):
        reply = self._reply()
        cold = {
            "record_id": "cold-1",
            "fields": {
                "邮件草稿来源": "cold",
                "关联KOL": ["kol-1"],
                "关联产品": ["product-1"],
                "是否回复": True,
                "回复原文": "[MID:source-mid]",
            },
        }
        activity = self._activity()
        participant = self._participant()

        async def fetch_all(table_id, **_kwargs):
            if table_id == attribution.config.T_LAUNCH_CAMPAIGN:
                return [activity]
            if table_id == attribution.config.T_LAUNCH_PARTICIPANT:
                return [participant]
            self.fail(f"targeted load must not scan full table: {table_id}")

        records = {
            (attribution.config.T_DRAFT, "reply-1"): reply,
            (attribution.config.T_DRAFT, "cold-1"): cold,
            (attribution.config.T_KOL, "kol-1"): {
                "record_id": "kol-1", "fields": {"账号名": "Just the Gems"},
            },
            (attribution.config.T_PRODUCT, "product-1"): {
                "record_id": "product-1", "fields": {"产品名": "食人花二代"},
            },
        }

        async def get_record(table_id, record_id):
            return records[(table_id, record_id)]

        with patch.object(attribution.feishu, "fetch_all_records", side_effect=fetch_all), \
             patch.object(attribution.feishu, "get_record", side_effect=get_record):
            source = asyncio.run(attribution._load_source(reply_record_id="reply-1"))

        self.assertEqual({"reply-1", "cold-1"}, {
            row["record_id"] for row in source["drafts"]
        })
        self.assertEqual("Just the Gems", source["contacts"]["kol-1"]["fields"]["账号名"])
        self.assertEqual("食人花二代", source["products"]["product-1"]["fields"]["产品名"])


if __name__ == "__main__":
    unittest.main()
