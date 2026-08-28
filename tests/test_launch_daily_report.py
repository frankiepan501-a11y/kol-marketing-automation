import asyncio
import json
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from app import feishu


BJ = timezone(timedelta(hours=8))


def _ms(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp() * 1000)


class LaunchDailyReportTests(unittest.TestCase):
    def setUp(self):
        from app import launch_daily_report

        self.report = launch_daily_report
        self.report._SEND_RECEIPTS.clear()
        self.real_persist_receipt = self.report._persist_report_receipt
        self.persist_receipt = patch.object(
            self.report, "_persist_report_receipt", new=AsyncMock(),
        ).start()
        self.addCleanup(patch.stopall)
        self.day = date(2026, 8, 21)
        self.start = _ms("2026-08-21T00:00:00+08:00")
        self.end = _ms("2026-08-22T00:00:00+08:00")
        self.now = _ms("2026-08-21T17:15:00+08:00")
        self.window_end = _ms("2026-09-15T23:59:59+08:00")
        self.activity = {
            "record_id": "act1",
            "fields": {
                "活动ID": "launch-dave",
                "活动名称": "9·15 FUNLAB 潜水员戴夫联名款",
                "品牌": "FUNLAB",
                "运行模式": "正式运行",
                "状态": "正式执行中",
                "目标上稿数": 20,
                "窗口结束": self.window_end,
            },
        }

    def _participant(self, rid, **fields):
        base = {
            "活动ID": "launch-dave",
            "参与状态": "已入围",
            "审核结论": "通过",
        }
        base.update(fields)
        return {"record_id": rid, "fields": base}

    def test_metrics_are_cold_only_deduplicated_and_use_on_time_posts(self):
        participants = [
            self._participant(
                "p1",
                **{
                    "系统审核分流": "系统建议通过",
                    "关联邮件草稿": {"link_record_ids": ["d1", "d1"]},
                    "承诺上稿时间": _ms("2026-09-10T12:00:00+08:00"),
                    "实际上稿时间": _ms("2026-09-14T12:00:00+08:00"),
                },
            ) | {"created_time": self.start + 1_000},
            self._participant(
                "p2",
                **{
                    "审核时间": self.start + 2_000,
                    "关联邮件草稿": ["d2"],
                },
            ),
            self._participant(
                "p3",
                **{
                    "审核时间": self.start - 1_000,
                    "关联邮件草稿": ["d3"],
                    "实际上稿时间": _ms("2026-09-16T12:00:00+08:00"),
                },
            ),
            self._participant("p4", **{"审核时间": self.start - 2_000, "关联邮件草稿": ["d4"]}),
            self._participant("p5"),
            self._participant("p6", **{"参与状态": "已取消", "审核时间": self.start + 3_000}),
        ]
        drafts = {
            "d1": {"record_id": "d1", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发送",
                "发送时间": self.start + 10_000, "是否回复": True,
                "回复日期": self.start + 20_000,
            }},
            "d2": {"record_id": "d2", "fields": {
                "邮件草稿来源": "followup", "发送状态": "已发送",
                "发送时间": self.start + 30_000, "是否回复": True,
                "回复日期": self.start + 40_000,
            }},
            "d3": {"record_id": "d3", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "发送时间": self.start - 50_000, "是否回复": True,
                "回复日期": self.start + 60_000,
            }},
            "d4": {"record_id": "d4", "fields": {
                "邮件草稿来源": "cold", "发送状态": "未发",
                "邮件草稿状态": "自动通过", "建议发送时间": self.now - 1,
            }},
        }

        snap = self.report.summarize_campaign(
            self.activity,
            participants,
            drafts,
            quota={"sent_24h": 56, "cap": 80},
            now_ms=self.now,
            day_start_ms=self.start,
            day_end_ms=self.end,
        )

        self.assertEqual(2, snap["today_eligible"])
        self.assertEqual(5, snap["eligible_total"])
        self.assertEqual(1, snap["eligible_time_missing"])
        self.assertEqual(1, snap["ready_due"])
        self.assertEqual(1, snap["sent_today"])
        self.assertEqual(2, snap["sent_total"])
        self.assertEqual(2, snap["replies_today"])
        self.assertEqual(2, snap["replies_total"])
        self.assertEqual(1, snap["commitments"])
        self.assertEqual(1, snap["on_time_posts"])
        self.assertEqual(2, snap["actual_posts"])

    def test_funnel_phases_use_explicit_reply_intent_commitment_shipping_and_post_facts(self):
        participants = [
            self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1", "关联邮件草稿": ["d1"],
            }),
            self._participant("p2", **{
                "关联KOL": ["k2"], "产品家族ID": "product1", "关联邮件草稿": ["d2"],
            }),
            self._participant("p3", **{
                "关联邮件草稿": ["d3"],
                "承诺上稿时间": _ms("2026-09-10T12:00:00+08:00"),
            }),
            self._participant("p4", **{"关联邮件草稿": ["d4"]}),
            self._participant("p5", **{
                "关联邮件草稿": ["d5"],
                "实际上稿时间": _ms("2026-09-12T12:00:00+08:00"),
            }),
        ]
        drafts = {
            "d1": {"record_id": "d1", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "感兴趣",
            }},
            "d2": {"record_id": "d2", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "质疑/澄清",
                "回复原文": "[MID:mid-r2] Could you clarify?",
                "关联KOL": ["k2"], "关联产品": ["product1"],
            }},
            "d3": {"record_id": "d3", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "感兴趣",
            }},
            "d4": {"record_id": "d4", "fields": {
                "邮件草稿来源": "ship_confirm", "发送状态": "已发",
                "寄样阶段": "在途",
            }},
            "d5": {"record_id": "d5", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "感兴趣",
            }},
            "r2": {"record_id": "r2", "fields": {
                "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                "审核路径": "待人审", "生成时间": self.start + 50_000,
                "关联KOL": ["k2"],
                "关联产品": ["product1"], "卡片已标记已审": False,
                "回复目标MsgID": "mid-r2",
            }},
        }

        snap = self.report.summarize_campaign(
            self.activity, participants, drafts,
            quota={"sent_24h": 10, "cap": 80}, now_ms=self.now,
            day_start_ms=self.start, day_end_ms=self.end,
        )

        self.assertEqual(1, snap["reply_pending"])
        self.assertEqual(1, snap["awaiting_post_date"])
        self.assertEqual(1, snap["commitments"])
        self.assertEqual(1, snap["shipped"])
        self.assertEqual(1, snap["actual_posts"])
        self.assertEqual("独立站运营专员", snap["next_action_owner"])
        self.assertIn("回复", snap["funnel_next_action"])

    def test_reply_pending_counts_live_review_draft_even_when_cold_reply_is_positive(self):
        participants = [
            self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            }),
        ]
        drafts = {
            "cold1": {"record_id": "cold1", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "感兴趣",
                "回复原文": "[MID:mid-live] Sounds good",
                "关联KOL": ["k1"], "关联产品": ["product1"],
            }},
            "reply1": {"record_id": "reply1", "fields": {
                "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                "审核路径": "待人审", "生成时间": self.start + 20_000,
                "关联KOL": ["k1"],
                "关联产品": ["product1"], "卡片已标记已审": False,
                "回复目标MsgID": "mid-live",
            }},
        }

        snap = self.report.summarize_campaign(
            self.activity, participants, drafts,
            quota={"sent_24h": 10, "cap": 80}, now_ms=self.now,
            day_start_ms=self.start, day_end_ms=self.end,
        )

        self.assertEqual(1, snap["reply_pending"])
        self.assertEqual(1, snap["awaiting_post_date"])

    def test_reply_pending_ignores_historical_nonpositive_reply_after_review_is_resolved(self):
        participants = [
            self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            }),
        ]
        drafts = {
            "cold1": {"record_id": "cold1", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "不明意图",
                "关联KOL": ["k1"], "关联产品": ["product1"],
            }},
            "reply1": {"record_id": "reply1", "fields": {
                "邮件草稿来源": "reply", "邮件草稿状态": "已发送",
                "审核路径": "待人审", "关联KOL": ["k1"],
                "关联产品": ["product1"], "卡片已标记已审": True,
            }},
        }

        snap = self.report.summarize_campaign(
            self.activity, participants, drafts,
            quota={"sent_24h": 10, "cap": 80}, now_ms=self.now,
            day_start_ms=self.start, day_end_ms=self.end,
        )

        self.assertEqual(0, snap["reply_pending"])

    def test_late_old_reply_is_not_reassigned_to_reused_kol_product(self):
        participants = [
            self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["current-cold"],
            }),
        ]
        drafts = {
            "current-cold": {"record_id": "current-cold", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "发送时间": self.start + 20_000,
                "回复原文": "[MID:mid-current] Current campaign reply",
                "关联KOL": ["k1"], "关联产品": ["product1"],
            }},
            "old-reply": {"record_id": "old-reply", "fields": {
                "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                "审核路径": "待人审", "生成时间": self.start + 30_000,
                "关联KOL": ["k1"], "关联产品": ["product1"],
                "卡片已标记已审": False,
                "回复目标MsgID": "mid-old-campaign",
            }},
        }

        snap = self.report.summarize_campaign(
            self.activity, participants, drafts,
            quota={"sent_24h": 10, "cap": 80}, now_ms=self.now,
            day_start_ms=self.start, day_end_ms=self.end,
        )

        self.assertEqual(0, snap["reply_pending"])

    def test_replied_cold_missing_identity_is_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "是否回复": True, "回复原文": "[MID:mid-1] Reply",
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "关联KOL": ["k1"],
                    "关联产品": ["product1"], "卡片已标记已审": False,
                    "回复目标MsgID": "mid-1",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any("cold草稿归属" in error for error in snapshot["data_errors"]))

    def test_cold_identity_mismatch_with_participant_is_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "是否回复": True, "回复原文": "[MID:mid-1] Reply",
                    "关联KOL": ["k-other"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "关联KOL": ["k-other"],
                    "关联产品": ["product1"], "卡片已标记已审": False,
                    "回复目标MsgID": "mid-1",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any("cold草稿归属" in error for error in snapshot["data_errors"]))

    def test_live_reply_with_multivalue_identity_is_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [
                self._participant("p1", **{
                    "关联KOL": ["k1"], "产品家族ID": "product1",
                    "关联邮件草稿": ["cold1"],
                }),
            ],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "发送时间": self.start + 10_000,
                    "回复原文": "[MID:mid-multi] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "生成时间": self.start + 20_000,
                    "关联KOL": ["k1", "k2"], "关联产品": ["product1"],
                    "卡片已标记已审": False,
                    "回复目标MsgID": "mid-multi",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any("缺少唯一KOL/产品" in error for error in snapshot["data_errors"]))

    def test_live_reply_missing_identity_with_exact_mid_is_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "是否回复": True, "回复原文": "[MID:mid-missing] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "回复目标MsgID": "mid-missing",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any("缺少唯一KOL/产品" in error for error in snapshot["data_errors"]))

    def test_live_reply_missing_mid_and_partial_identity_is_never_silent(self):
        cases = [
            ("missing_product", {"关联KOL": ["k1"]}),
            ("missing_kol", {"关联产品": ["product1"]}),
            ("missing_both", {}),
        ]
        for label, reply_identity in cases:
            with self.subTest(label=label):
                source = {
                    "activities": [self.activity],
                    "participants": [self._participant("p1", **{
                        "关联KOL": ["k1"], "产品家族ID": "product1",
                        "关联邮件草稿": ["cold1"],
                    })],
                    "drafts": {
                        "cold1": {"record_id": "cold1", "fields": {
                            "邮件草稿来源": "cold", "发送状态": "已发",
                            "回复原文": "[MID:mid-source] Reply",
                            "关联KOL": ["k1"], "关联产品": ["product1"],
                        }},
                        "reply1": {"record_id": "reply1", "fields": {
                            "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                            "审核路径": "待人审", "卡片已标记已审": False,
                            **reply_identity,
                        }},
                    },
                    "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
                    "quota_errors": {},
                }
                with patch.object(
                    self.report, "_load_report_source", new=AsyncMock(return_value=source),
                ):
                    result = asyncio.run(self.report.run(day=self.day))

                snapshot = result["snapshots"][0]
                self.assertEqual(0, snapshot["reply_pending"])
                self.assertTrue(any(
                    "缺少唯一KOL/产品" in error
                    for error in snapshot["data_errors"]
                ))

    def test_live_reply_with_unknown_mid_is_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-current] Current reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-orphan",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any("活动cold归属不一致" in error for error in snapshot["data_errors"]))

    def test_operator_confirmed_attribution_restores_unknown_mid_to_campaign(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-current] Current reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-orphan",
                    "集中宣发活动ID": "launch-dave",
                    "活动归属状态": "已确认",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(1, snapshot["reply_pending"])
        self.assertEqual([], snapshot["data_errors"])

    def test_live_reply_mid_identity_conflict_is_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-conflict] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k-other"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-conflict",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any("活动cold归属不一致" in error for error in snapshot["data_errors"]))

    def test_duplicate_live_replies_for_same_mid_are_excluded_and_reported(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-duplicate] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                **{
                    f"reply{index}": {"record_id": f"reply{index}", "fields": {
                        "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                        "审核路径": "待人审", "卡片已标记已审": False,
                        "关联KOL": ["k1"], "关联产品": ["product1"],
                        "回复目标MsgID": "mid-duplicate",
                    }}
                    for index in (1, 2)
                },
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any(
            "原邮件ID对应2条待审reply" in error
            for error in snapshot["data_errors"]
        ))
        duplicate_error = next(
            error for error in snapshot["data_errors"]
            if "原邮件ID对应2条待审reply" in error
        )
        self.assertIn("mid-duplicate", duplicate_error)
        self.assertIn("reply1", duplicate_error)
        self.assertIn("reply2", duplicate_error)
        duplicate_detail = next(
            detail for detail in snapshot["data_error_details"]
            if detail["kind"] == "duplicate_live_reply"
        )
        self.assertEqual(["mid-duplicate"], duplicate_detail["message_ids"])
        self.assertEqual(["reply1", "reply2"], duplicate_detail["draft_ids"])
        self.assertFalse(any(
            "缺少唯一KOL/产品" in error
            for error in snapshot["data_errors"]
        ))

    def test_same_cold_shared_by_two_participants_in_one_campaign_is_invalid(self):
        source = {
            "activities": [self.activity],
            "participants": [
                self._participant("p1", **{
                    "关联KOL": ["k1"], "产品家族ID": "product1",
                    "关联邮件草稿": ["cold-shared"],
                }),
                self._participant("p2", **{
                    "关联KOL": ["k2"], "产品家族ID": "product2",
                    "关联邮件草稿": ["cold-shared"],
                }),
            ],
            "drafts": {
                "cold-shared": {"record_id": "cold-shared", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "是否回复": True, "回复意图": "感兴趣", "寄样阶段": "在途",
                    "回复原文": "[MID:mid-shared-cold] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply-shared": {"record_id": "reply-shared", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-shared-cold",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertEqual(0, snapshot["sent_total"])
        self.assertEqual(0, snapshot["replies_total"])
        self.assertEqual(0, snapshot["awaiting_post_date"])
        self.assertEqual(0, snapshot["shipped"])
        shared_error = next(
            error for error in snapshot["data_errors"]
            if "cold草稿关联多个参与记录" in error
        )
        self.assertIn("cold-shared", shared_error)
        shared_detail = next(
            detail for detail in snapshot["data_error_details"]
            if detail["kind"] == "shared_cold_source"
        )
        self.assertEqual(["mid-shared-cold"], shared_detail["message_ids"])
        self.assertEqual(["cold-shared"], shared_detail["draft_ids"])

    def test_duplicate_active_campaign_ids_fail_closed_with_record_ids(self):
        duplicate_activity = {
            "record_id": "act2",
            "fields": {
                **self.activity["fields"],
                "活动名称": "重复活动记录",
            },
        }
        source = {
            "activities": [self.activity, duplicate_activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1"],
            })],
            "drafts": {
                "cold1": {"record_id": "cold1", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "是否回复": True, "回复意图": "感兴趣", "寄样阶段": "在途",
                    "回复原文": "[MID:mid-duplicate-campaign] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-duplicate-campaign",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        self.assertEqual(2, len(result["snapshots"]))
        for snapshot in result["snapshots"]:
            self.assertEqual("数据异常", snapshot["status"]["label"])
            self.assertEqual(0, snapshot["eligible_total"])
            self.assertEqual(0, snapshot["sent_total"])
            self.assertEqual(0, snapshot["replies_total"])
            self.assertEqual(0, snapshot["reply_pending"])
            self.assertEqual(0, snapshot["awaiting_post_date"])
            self.assertEqual(0, snapshot["shipped"])
            self.assertTrue(any("活动ID重复" in error for error in snapshot["data_errors"]))
            duplicate_detail = next(
                detail for detail in snapshot["data_error_details"]
                if detail["kind"] == "duplicate_active_campaign_id"
            )
            self.assertEqual(["act1", "act2"], duplicate_detail["activity_record_ids"])

    def test_exact_mid_match_does_not_mark_same_key_other_campaign_invalid(self):
        second_activity = {
            "record_id": "act2",
            "fields": {
                **self.activity["fields"],
                "活动ID": "launch-piranha",
                "活动名称": "9·15 POWKONG 食人花二代",
                "品牌": "POWKONG",
            },
        }
        source = {
            "activities": [self.activity, second_activity],
            "participants": [
                self._participant("p1", **{
                    "关联KOL": ["k-shared"], "产品家族ID": "product-shared",
                    "关联邮件草稿": ["cold-a"],
                }),
                self._participant("p2", **{
                    "活动ID": "launch-piranha",
                    "关联KOL": ["k-shared"], "产品家族ID": "product-shared",
                    "关联邮件草稿": ["cold-b"],
                }),
            ],
            "drafts": {
                "cold-a": {"record_id": "cold-a", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-a] Reply",
                    "关联KOL": ["k-shared"], "关联产品": ["product-shared"],
                }},
                "cold-b": {"record_id": "cold-b", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-b] Reply",
                    "关联KOL": ["k-shared"], "关联产品": ["product-shared"],
                }},
                "reply-a": {"record_id": "reply-a", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k-shared"], "关联产品": ["product-shared"],
                    "回复目标MsgID": "mid-a",
                }},
            },
            "quotas": {
                "FUNLAB": {"sent_24h": 1, "cap": 80},
                "POWKONG": {"sent_24h": 1, "cap": 80},
            },
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        dave, piranha = result["snapshots"]
        self.assertEqual(1, dave["reply_pending"])
        self.assertEqual(0, piranha["reply_pending"])
        self.assertEqual([], dave["data_errors"])
        self.assertEqual([], piranha["data_errors"])

    def test_valid_reply_evidence_wins_without_false_exclusion_message(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold-good", "cold-bad"],
            })],
            "drafts": {
                "cold-good": {"record_id": "cold-good", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-valid] Reply",
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                }},
                "cold-bad": {"record_id": "cold-bad", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "回复原文": "[MID:mid-valid] Reply",
                    "关联产品": ["product1"],
                }},
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-valid",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(1, snapshot["reply_pending"])
        self.assertTrue(any("cold草稿归属" in error for error in snapshot["data_errors"]))
        self.assertFalse(any(
            "相关reply已从统计中排除" in error
            for error in snapshot["data_errors"]
        ))

    def test_funnel_phases_exclude_drafts_linked_to_multiple_campaigns(self):
        participants = [
            self._participant("p1", **{"关联邮件草稿": ["ambiguous"]}),
        ]
        drafts = {
            "ambiguous": {"record_id": "ambiguous", "fields": {
                "邮件草稿来源": "cold", "发送状态": "已发",
                "是否回复": True, "回复意图": "感兴趣", "寄样阶段": "在途",
            }},
        }

        snap = self.report.summarize_campaign(
            self.activity, participants, drafts,
            quota={"sent_24h": 10, "cap": 80}, now_ms=self.now,
            day_start_ms=self.start, day_end_ms=self.end,
            excluded_draft_ids={"ambiguous"},
        )

        self.assertEqual(0, snap["sent_total"])
        self.assertEqual(0, snap["reply_pending"])
        self.assertEqual(0, snap["awaiting_post_date"])
        self.assertEqual(0, snap["shipped"])

    def test_status_priority_and_thresholds_are_deterministic(self):
        common = {
            "target_posts": 20,
            "on_time_posts": 1,
            "quota_cap": 80,
            "quota_sent_24h": 20,
            "quota_remaining": 60,
            "data_errors": [],
        }
        red = self.report.status_for({**common, "ready_due": 20, "quota_sent_24h": 81})
        green = self.report.status_for({**common, "ready_due": 12})
        orange = self.report.status_for({**common, "ready_due": 0})
        yellow = self.report.status_for({**common, "ready_due": 5})

        self.assertEqual("red", red["color"])
        self.assertEqual("green", green["color"])
        self.assertEqual("orange", orange["color"])
        self.assertEqual("yellow", yellow["color"])
        self.assertIn("补池", orange["next_action"])

    def test_card_is_json2_with_real_colored_tags_and_compact_progress(self):
        snap = {
            "campaign_id": "launch-dave",
            "name": "9·15 FUNLAB 潜水员戴夫联名款",
            "brand": "FUNLAB",
            "today_eligible": 12,
            "eligible_total": 47,
            "eligible_time_missing": 0,
            "ready_due": 47,
            "sent_today": 28,
            "sent_total": 91,
            "replies_today": 4,
            "replies_total": 17,
            "reply_pending": 3,
            "awaiting_post_date": 5,
            "commitments": 6,
            "shipped": 4,
            "on_time_posts": 2,
            "actual_posts": 3,
            "target_posts": 20,
            "quota_sent_24h": 56,
            "quota_cap": 80,
            "quota_remaining": 24,
            "post_progress_pct": 10,
            "quota_progress_pct": 70,
            "status": {"label": "进度正常", "color": "green", "next_action": "继续自动发送。"},
            "next_action_owner": "独立站运营专员",
            "funnel_next_action": "先处理3条真实回复。",
            "data_errors": [],
        }
        card = self.report.build_card([snap], day=self.day)
        result = self.report.validate_card(card, [snap], day=self.day)

        self.assertTrue(result["ok"], result)
        self.assertEqual("2.0", card["schema"])
        self.assertNotEqual("fill", (card.get("config") or {}).get("width_mode"))
        self.assertEqual("KOL集中宣发任务日报 · 2026-08-21", card["header"]["title"]["content"])
        self.assertEqual("green", card["header"]["text_tag_list"][1]["color"])
        encoded = json.dumps(card, ensure_ascii=False)
        markdown = next(e["content"] for e in card["body"]["elements"] if e.get("tag") == "markdown")
        self.assertIn('<text_tag color="green">进度正常</text_tag>', markdown)
        self.assertNotIn('"tag": "button"', encoded)
        self.assertNotIn('"tag": "form"', encoded)
        self.assertNotIn('"tag": "note"', encoded)
        charts = [e for e in card["body"]["elements"] if e.get("tag") == "chart"]
        self.assertEqual([], charts, "聊天卡不得使用会按卡片宽度放大的 chart 画布")
        progress = next(
            e["content"] for e in card["body"]["elements"]
            if e.get("element_id") == "progress_0"
        )
        self.assertIn("**上稿进度**　2 / 20　10%", progress)
        self.assertIn("**邮箱额度**　56 / 80　70%", progress)
        self.assertIn("█", progress)
        self.assertIn("░", progress)
        encoded = json.dumps(card, ensure_ascii=False)
        self.assertIn("回复待处理", encoded)
        self.assertIn("待确认上稿日", encoded)
        self.assertIn("已寄样", encoded)
        self.assertIn("独立站运营专员", encoded)

    def test_run_is_read_only_and_frankie_sample_never_touches_group(self):
        source = {
            "activities": [self.activity],
            "participants": [],
            "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}},
            "quota_errors": {},
        }
        sender = AsyncMock(return_value="om_sample")
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            result = asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

        self.assertTrue(result["validation"]["ok"])
        self.assertEqual(["om_sample"], result["message_ids"])
        self.assertEqual(0, result["business_writes"])
        self.assertEqual(2, result["operational_receipt_writes"])
        sender.assert_awaited_once()
        args = sender.await_args.args
        kwargs = sender.await_args.kwargs
        self.assertEqual("open_id", args[0])
        self.assertNotEqual("chat_id", args[0])
        self.assertFalse(kwargs["format_title"])

    def test_notify_false_never_calls_feishu_sender(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        sender = AsyncMock()
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            result = asyncio.run(self.report.run(day=self.day, notify=False, frankie_only=True))
        self.assertTrue(result["validation"]["ok"])
        self.assertFalse(result["notified"])
        self.assertEqual(0, result["business_writes"])
        self.assertEqual(0, result["operational_receipt_writes"])
        sender.assert_not_awaited()

    def test_cross_campaign_draft_is_excluded_and_marks_both_campaigns_red(self):
        second_activity = {
            "record_id": "act2",
            "fields": {
                **self.activity["fields"],
                "活动ID": "launch-piranha",
                "活动名称": "9·15 POWKONG 食人花二代",
                "品牌": "POWKONG",
            },
        }
        source = {
            "activities": [self.activity, second_activity],
            "participants": [
                self._participant("p1", **{"关联邮件草稿": ["d-shared"]}),
                self._participant(
                    "p2", **{"活动ID": "launch-piranha", "关联邮件草稿": ["d-shared"]},
                ),
            ],
            "drafts": {
                "d-shared": {"record_id": "d-shared", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发送",
                    "发送时间": self.start + 1_000,
                }},
            },
            "quotas": {
                "FUNLAB": {"sent_24h": 1, "cap": 80},
                "POWKONG": {"sent_24h": 1, "cap": 80},
            },
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        self.assertEqual([0, 0], [row["sent_total"] for row in result["snapshots"]])
        self.assertEqual(["red", "red"], [row["status"]["color"] for row in result["snapshots"]])
        self.assertTrue(all(
            any("跨活动重复关联" in error for error in row["data_errors"])
            for row in result["snapshots"]
        ))

    def test_live_reply_matching_two_campaigns_is_excluded_from_both(self):
        second_activity = {
            "record_id": "act2",
            "fields": {
                **self.activity["fields"],
                "活动ID": "launch-piranha",
                "活动名称": "9·15 POWKONG 食人花二代",
                "品牌": "POWKONG",
            },
        }
        source = {
            "activities": [self.activity, second_activity],
            "participants": [
                self._participant("p1", **{
                    "关联KOL": ["k-shared"], "产品家族ID": "product-shared",
                    "关联邮件草稿": ["cold-dave"],
                }),
                self._participant("p2", **{
                    "活动ID": "launch-piranha", "关联KOL": ["k-other"],
                    "产品家族ID": "product-other",
                    "关联邮件草稿": ["cold-piranha"],
                }),
            ],
            "drafts": {
                "cold-dave": {"record_id": "cold-dave", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "发送时间": self.start + 10_000,
                    "回复原文": "[MID:mid-shared] Shared reply",
                    "关联KOL": ["k-shared"], "关联产品": ["product-shared"],
                }},
                "cold-piranha": {"record_id": "cold-piranha", "fields": {
                    "邮件草稿来源": "cold", "发送状态": "已发",
                    "发送时间": self.start + 10_000,
                    "回复原文": "[MID:mid-shared] Shared reply",
                    "关联KOL": ["k-other"], "关联产品": ["product-other"],
                }},
                "reply-shared": {"record_id": "reply-shared", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "生成时间": self.start + 20_000,
                    "关联KOL": ["k-other"],
                    "关联产品": ["product-other"], "卡片已标记已审": False,
                    "回复目标MsgID": "mid-shared",
                }},
            },
            "quotas": {
                "FUNLAB": {"sent_24h": 1, "cap": 80},
                "POWKONG": {"sent_24h": 1, "cap": 80},
            },
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        self.assertEqual([0, 0], [row["reply_pending"] for row in result["snapshots"]])
        self.assertTrue(all(
            any("跨活动重复关联" in error for error in row["data_errors"])
            for row in result["snapshots"]
        ))

    def test_duplicate_cold_sources_same_mid_are_excluded_with_precise_error(self):
        source = {
            "activities": [self.activity],
            "participants": [self._participant("p1", **{
                "关联KOL": ["k1"], "产品家族ID": "product1",
                "关联邮件草稿": ["cold1", "cold2"],
            })],
            "drafts": {
                **{
                    f"cold{index}": {"record_id": f"cold{index}", "fields": {
                        "邮件草稿来源": "cold", "发送状态": "已发",
                        "回复原文": "[MID:mid-duplicate-source] Reply",
                        "关联KOL": ["k1"], "关联产品": ["product1"],
                    }}
                    for index in (1, 2)
                },
                "reply1": {"record_id": "reply1", "fields": {
                    "邮件草稿来源": "reply", "邮件草稿状态": "待审",
                    "审核路径": "待人审", "卡片已标记已审": False,
                    "关联KOL": ["k1"], "关联产品": ["product1"],
                    "回复目标MsgID": "mid-duplicate-source",
                }},
            },
            "quotas": {"FUNLAB": {"sent_24h": 1, "cap": 80}},
            "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            result = asyncio.run(self.report.run(day=self.day))

        snapshot = result["snapshots"][0]
        self.assertEqual(0, snapshot["reply_pending"])
        self.assertTrue(any(
            "原邮件ID对应多条有效cold来源" in error
            for error in snapshot["data_errors"]
        ))
        self.assertFalse(any(
            "归属字段缺失" in error
            for error in snapshot["data_errors"]
        ))

    def test_same_day_same_recipient_and_campaign_set_sends_only_once(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        sender = AsyncMock(return_value="om_once")
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            first = asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))
            second = asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(["om_once"], second["message_ids"])
        sender.assert_awaited_once()

    def test_persisted_receipt_prevents_resend_after_memory_is_cleared(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            preview = asyncio.run(self.report.run(day=self.day))
        receipt_key, message_uuid = self.report._send_identity(
            report_day=self.day,
            recipient_type="open_id",
            recipient_id=self.report._frankie_open_id(),
            snapshots=preview["snapshots"],
        )
        self.activity["fields"]["数据口径备注"] = (
            self.report.REPORT_RECEIPT_PREFIX
            + json.dumps({
                "key": receipt_key, "uuid": message_uuid, "status": "sent",
                "message_id": "om_persisted",
            }, ensure_ascii=False, separators=(",", ":"))
        )
        self.report._SEND_RECEIPTS.clear()
        sender = AsyncMock()
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            result = asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

        self.assertTrue(result["deduplicated"])
        self.assertEqual(["om_persisted"], result["message_ids"])
        sender.assert_not_awaited()

    def test_persisted_sending_receipt_pauses_instead_of_resending(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)):
            preview = asyncio.run(self.report.run(day=self.day))
        receipt_key, message_uuid = self.report._send_identity(
            report_day=self.day,
            recipient_type="open_id",
            recipient_id=self.report._frankie_open_id(),
            snapshots=preview["snapshots"],
        )
        self.activity["fields"]["数据口径备注"] = (
            self.report.REPORT_RECEIPT_PREFIX
            + json.dumps({
                "key": receipt_key, "uuid": message_uuid, "status": "sending",
                "updated_ts": 1,
            }, ensure_ascii=False, separators=(",", ":"))
        )
        sender = AsyncMock()
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            with self.assertRaisesRegex(self.report.DailyReportError, "暂停自动重发"):
                asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))
        sender.assert_not_awaited()

    def test_receipt_persistence_only_updates_runtime_note_and_reads_it_back(self):
        payload = {
            "key": "abc123", "uuid": "kolrpt-abc123", "status": "sent",
            "day": "2026-08-21", "message_id": "om_saved", "updated_ts": 1,
        }
        current = {"record_id": "act1", "fields": {"数据口径备注": "既有业务备注"}}
        readback = {
            "record_id": "act1",
            "fields": {"数据口径备注": (
                "既有业务备注\n" + self.report.REPORT_RECEIPT_PREFIX
                + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            )},
        }
        get_record = AsyncMock(side_effect=[current, readback])
        update_record = AsyncMock(return_value={"record_id": "act1"})
        with patch.object(self.report.feishu, "get_record", new=get_record), patch.object(
            self.report.feishu, "update_record", new=update_record,
        ):
            asyncio.run(self.real_persist_receipt(self.activity, payload))

        update_record.assert_awaited_once()
        self.assertEqual(self.report.config.T_LAUNCH_CAMPAIGN, update_record.await_args.args[0])
        self.assertEqual("act1", update_record.await_args.args[1])
        self.assertEqual({"数据口径备注"}, set(update_record.await_args.args[2]))
        self.assertTrue(update_record.await_args.args[2]["数据口径备注"].startswith("既有业务备注\n"))

    def test_receipt_persistence_never_truncates_existing_business_note(self):
        payload = {
            "key": "abc123", "uuid": "kolrpt-abc123", "status": "sending",
            "day": "2026-08-21", "updated_ts": 1,
        }
        original_note = "业务说明" * 740
        get_record = AsyncMock(return_value={
            "record_id": "act1", "fields": {"数据口径备注": original_note},
        })
        update_record = AsyncMock()
        with patch.object(self.report.feishu, "get_record", new=get_record), patch.object(
            self.report.feishu, "update_record", new=update_record,
        ):
            with self.assertRaisesRegex(self.report.DailyReportError, "原有业务备注保持不变"):
                asyncio.run(self.real_persist_receipt(self.activity, payload))
        update_record.assert_not_awaited()

    def test_receipt_persistence_prunes_old_technical_days_but_keeps_business_note(self):
        old = {"key": "old", "day": "2026-08-20", "status": "sent", "message_id": "om_old"}
        same_day = {
            "key": "other", "day": "2026-08-21", "status": "sent",
            "message_id": "om_other",
        }
        payload = {
            "key": "new", "uuid": "kolrpt-new", "day": "2026-08-21",
            "status": "sent", "message_id": "om_new", "updated_ts": 1,
        }
        prefix = self.report.REPORT_RECEIPT_PREFIX
        current_note = "关键业务说明\n" + "\n".join(
            prefix + json.dumps(row, separators=(",", ":")) for row in (old, same_day)
        )
        expected_note = "关键业务说明\n" + "\n".join(
            prefix + json.dumps(row, separators=(",", ":")) for row in (same_day, payload)
        )
        get_record = AsyncMock(side_effect=[
            {"record_id": "act1", "fields": {"数据口径备注": current_note}},
            {"record_id": "act1", "fields": {"数据口径备注": expected_note}},
        ])
        update_record = AsyncMock(return_value={"record_id": "act1"})
        with patch.object(self.report.feishu, "get_record", new=get_record), patch.object(
            self.report.feishu, "update_record", new=update_record,
        ):
            asyncio.run(self.real_persist_receipt(self.activity, payload))

        written = update_record.await_args.args[2]["数据口径备注"]
        self.assertEqual(expected_note, written)
        self.assertNotIn("om_old", written)
        self.assertIn("关键业务说明", written)
        self.assertIn("om_other", written)

    def test_empty_state_builds_valid_card_but_does_not_send(self):
        source = {
            "activities": [], "participants": [], "drafts": {},
            "quotas": {}, "quota_errors": {},
        }
        sender = AsyncMock()
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            result = asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

        self.assertTrue(result["validation"]["ok"])
        self.assertTrue(result["empty_state_send_skipped"])
        self.assertFalse(result["notified"])
        sender.assert_not_awaited()

    def test_empty_message_id_is_a_hard_failure(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=AsyncMock(return_value=""),
        ):
            with self.assertRaisesRegex(self.report.DailyReportError, "message_id"):
                asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

    def test_definitive_card_schema_rejection_is_persisted_as_rejected(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        sender = AsyncMock(side_effect=RuntimeError(
            'POST /im/v1/messages → 400: {"code":230099,"msg":"Failed to create card content"}',
        ))
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            with self.assertRaisesRegex(RuntimeError, "Failed to create card content"):
                asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

        statuses = [call.args[1]["status"] for call in self.persist_receipt.await_args_list]
        self.assertEqual(["sending", "rejected"], statuses)

    def test_5xx_body_that_mentions_card_rejection_stays_sending(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        sender = AsyncMock(side_effect=RuntimeError(
            'POST /im/v1/messages → 500: upstream included 400: Failed to create card content',
        ))
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ):
            with self.assertRaisesRegex(RuntimeError, "500"):
                asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=True))

        statuses = [call.args[1]["status"] for call in self.persist_receipt.await_args_list]
        self.assertEqual(["sending"], statuses)

    def test_group_send_requires_both_flag_and_current_chat_whitelist(self):
        source = {
            "activities": [self.activity], "participants": [], "drafts": {},
            "quotas": {"FUNLAB": {"sent_24h": 0, "cap": 80}}, "quota_errors": {},
        }
        sender = AsyncMock(return_value="om_group")
        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ), patch.dict(os.environ, {"KOL_LAUNCH_DAILY_GROUP_ENABLED": "0"}, clear=False):
            with self.assertRaises(self.report.DailyReportError):
                asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=False))
        sender.assert_not_awaited()

        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ), patch.object(self.report.config, "NOTIFY_CHAT_ID", "oc_wrong"), patch.dict(
            os.environ, {"KOL_LAUNCH_DAILY_GROUP_ENABLED": "1"}, clear=False,
        ):
            with self.assertRaisesRegex(self.report.DailyReportError, "不是当前KOL运营群"):
                asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=False))
        sender.assert_not_awaited()

        with patch.object(self.report, "_load_report_source", new=AsyncMock(return_value=source)), patch.object(
            self.report.feishu, "send_card_message", new=sender,
        ), patch.object(
            self.report.config, "NOTIFY_CHAT_ID", self.report.CURRENT_GROUP_CHAT_ID,
        ), patch.dict(os.environ, {"KOL_LAUNCH_DAILY_GROUP_ENABLED": "1"}, clear=False):
            result = asyncio.run(self.report.run(day=self.day, notify=True, frankie_only=False))
        self.assertEqual(["om_group"], result["message_ids"])
        sender.assert_awaited_once()
        self.assertEqual("chat_id", sender.await_args.args[0])
        self.assertEqual(self.report.CURRENT_GROUP_CHAT_ID, sender.await_args.args[1])

    def test_feishu_title_formatting_can_be_disabled_without_changing_default(self):
        card = {
            "schema": "2.0",
            "header": {"title": {"tag": "plain_text", "content": "KOL集中宣发任务日报 · 2026-08-21"}},
            "body": {"elements": []},
        }
        api = AsyncMock(return_value={"data": {"message_id": "om1"}})
        with patch.object(feishu, "api", new=api):
            asyncio.run(feishu.send_card_message(
                "open_id", "ou_test", card, format_title=False, message_uuid="stable-uuid",
            ))
        body = api.await_args.args[2]
        sent = json.loads(body["content"])
        self.assertEqual("KOL集中宣发任务日报 · 2026-08-21", sent["header"]["title"]["content"])
        self.assertEqual("stable-uuid", body["uuid"])

        original = {
            "header": {"title": {"tag": "plain_text", "content": "旧卡"}},
            "elements": [],
        }
        with patch.object(feishu, "api", new=api):
            asyncio.run(feishu.send_card_message("open_id", "ou_test", original))
        body = api.await_args.args[2]
        sent = json.loads(body["content"])
        self.assertTrue(sent["header"]["title"]["content"].startswith("🟠 [KOL·P1]"))


if __name__ == "__main__":
    unittest.main()
