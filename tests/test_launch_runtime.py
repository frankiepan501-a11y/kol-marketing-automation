import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from app import auto_send, enrich, launch_runtime


def _certificate():
    return json.dumps({
        "campaign_id": "campaign1", "product_id": "product1", "brand": "FUNLAB",
        "template_version": "launch-queue-v1", "passed": True,
    })


class LaunchRuntimeTests(unittest.TestCase):
    def test_activity_queue_gate_requires_formal_authorization_and_exact_links(self):
        draft = {
            "record_id": "draft1",
            "fields": {
                "邮件草稿ID": "launchq-abcd",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["product1"]},
                "发送邮箱": "FUNLAB",
            },
        }
        participant = {
            "record_id": "part1",
            "fields": {
                "活动ID": "campaign1", "产品家族ID": "product1", "对象类型": "KOL",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联邮件草稿": {"link_record_ids": ["draft1"]},
                "参与状态": "已入围", "审核结论": "通过",
                "进入方式": "新开发", "活动分池": "新开发池",
                "名单版本": "evidence-v1", "排序版本": "evidence-v1",
            },
        }
        activity = {
            "record_id": "activity1",
            "fields": {
                "活动ID": "campaign1", "产品主记录ID": "product1",
                "运行模式": "正式运行", "状态": "正式执行中",
                "发送邮件授权": True, "KOL已锁定名单版本": "evidence-v1",
                "证据排序版本": "evidence-v1", "名单锁定授权": True,
                "邮件Raw验证证书": _certificate(),
                "KOL名单阻塞代码": "",
            },
        }
        ok, reasons = auto_send.validate_activity_queue_gate(activity, participant, draft)
        self.assertTrue(ok, reasons)

        activity["fields"]["发送邮件授权"] = False
        ok, reasons = auto_send.validate_activity_queue_gate(activity, participant, draft)
        self.assertFalse(ok)
        self.assertIn("发送邮件授权", reasons)

    def test_regular_locked_draft_is_denied_but_valid_activity_draft_is_released(self):
        activity_draft = {
            "record_id": "draft1",
            "fields": {
                "邮件草稿ID": "launchq-abcd", "邮件草稿来源": "cold",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["product1"]},
                "发送邮箱": "FUNLAB",
            },
        }
        regular_draft = {
            "record_id": "draft2",
            "fields": {
                "邮件草稿ID": "regular1", "邮件草稿来源": "cold",
                "关联KOL": {"link_record_ids": ["kol2"]},
                "关联产品": {"link_record_ids": ["product1"]},
            },
        }
        participant = {
            "record_id": "part1",
            "fields": {
                "活动ID": "campaign1", "产品家族ID": "product1", "对象类型": "KOL",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联邮件草稿": {"link_record_ids": ["draft1"]},
                "参与状态": "已入围", "审核结论": "通过", "进入方式": "新开发",
                "活动分池": "新开发池", "名单版本": "evidence-v1", "排序版本": "evidence-v1",
            },
        }
        activity = {
            "record_id": "activity1",
            "fields": {
                "活动ID": "campaign1", "产品主记录ID": "product1",
                "运行模式": "正式运行", "状态": "正式执行中",
                "发送邮件授权": True, "KOL已锁定名单版本": "evidence-v1",
                "证据排序版本": "evidence-v1", "名单锁定授权": True,
                "邮件Raw验证证书": _certificate(),
                "KOL名单阻塞代码": "",
            },
        }

        with patch.object(
            auto_send.feishu, "fetch_all_records",
            new=AsyncMock(side_effect=[[participant], [activity]]),
        ):
            released, failures = asyncio.run(auto_send.resolve_activity_queue_releases(
                [activity_draft, regular_draft]
            ))
        self.assertEqual({"draft1"}, released)
        self.assertEqual(0, failures)

    def test_zoho_count_uses_sent_folder_timestamp(self):
        now_ms = 1_800_000_000_000
        with patch.object(auto_send.time, "time", return_value=now_ms / 1000), patch.object(
            auto_send.zoho, "list_sent_messages", new=AsyncMock(return_value={
                "messages": [
                    {"messageId": "new", "sentDateInGMT": now_ms - 1000},
                    {"messageId": "old", "sentDateInGMT": now_ms - 90_000_000},
                ]
            }),
        ):
            counts, errors = asyncio.run(auto_send.zoho_sent_counts_24h(["FUNLAB"]))
        self.assertEqual(1, counts["FUNLAB"])
        self.assertEqual({}, errors)

    def test_feedback_control_uses_configured_commitment_target(self):
        self.assertEqual(
            "expand",
            launch_runtime.recommend_feedback_action(
                target_posts=20, target_commitments=29, commitments=4, sent=30, replies=3,
            )["action"],
        )
        self.assertEqual(
            "hold",
            launch_runtime.recommend_feedback_action(
                target_posts=20, target_commitments=29, commitments=27, sent=90, replies=20,
            )["action"],
        )
        self.assertEqual(
            "stop",
            launch_runtime.recommend_feedback_action(
                target_posts=20, target_commitments=29, commitments=29, sent=100, replies=30,
            )["action"],
        )

    def test_gate_fails_without_exact_lock_version_or_raw_certificate(self):
        draft = {"record_id": "draft1", "fields": {"邮件草稿ID": "launchq-a", "关联KOL": {"link_record_ids": ["kol1"]}, "关联产品": {"link_record_ids": ["product1"]}, "发送邮箱": "FUNLAB"}}
        participant = {"record_id": "part1", "fields": {"活动ID": "campaign1", "产品家族ID": "product1", "关联KOL": {"link_record_ids": ["kol1"]}, "关联邮件草稿": {"link_record_ids": ["draft1"]}, "参与状态": "已入围", "审核结论": "通过", "进入方式": "新开发", "活动分池": "新开发池", "名单版本": "v1", "排序版本": "v1"}}
        activity = {"record_id": "a1", "fields": {"活动ID": "campaign1", "产品主记录ID": "product1", "运行模式": "正式运行", "状态": "正式执行中", "发送邮件授权": True, "名单锁定授权": True, "KOL已锁定名单版本": "v1", "证据排序版本": "v2", "KOL名单阻塞代码": ""}}
        ok, reasons = auto_send.validate_activity_queue_gate(activity, participant, draft)
        self.assertFalse(ok)
        self.assertIn("锁定版本一致", reasons)
        self.assertIn("Raw验证证书", reasons)

    def test_product_type_instruction_distinguishes_dock_from_controller(self):
        dock = enrich._product_type_instruction("POWKONG Piranha Plant 2 Dock", "充电底座")
        controller = enrich._product_type_instruction("FUNLAB Dave Controller", "游戏手柄")
        self.assertIn("dock", dock.lower())
        self.assertIn("严禁称为 controller", dock)
        self.assertIn("controller", controller.lower())
        self.assertIn("严禁把整个产品叫成 dock", controller)


if __name__ == "__main__":
    unittest.main()
