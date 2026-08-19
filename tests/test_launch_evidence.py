import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_evidence


class LaunchEvidenceContractTests(unittest.TestCase):
    def test_switching_active_evidence_to_none_requires_reason(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "证据配置版本": 4,
            "竞品证据模式": "引用历史证据",
        }}
        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(return_value=[activity])), \
             patch.object(launch_evidence.feishu, "update_record", new=AsyncMock()) as update:
            with self.assertRaises(launch_evidence.EvidenceValidationError):
                asyncio.run(launch_evidence.configure_evidence(
                    campaign_id="campaign-1", mode=launch_evidence.MODE_NONE,
                    expected_config_version=4,
                ))
        update.assert_not_awaited()

    def test_new_analysis_creates_research_node_before_activity_commit(self):
        activity = {
            "record_id": "act1",
            "fields": {"活动ID": "launch-1", "证据配置版本": 2},
        }
        calls = []

        async def fake_create(table_id, fields):
            calls.append(("create", table_id, fields))
            return "node1"

        async def fake_update(table_id, record_id, fields):
            calls.append(("update", table_id, record_id, fields))

        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.config, "T_LAUNCH_NODE", "nodes"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(
                 side_effect=[[activity], []]
             )), \
             patch.object(launch_evidence.feishu, "create_record", new=fake_create), \
             patch.object(launch_evidence.feishu, "update_record", new=fake_update):
            result = asyncio.run(launch_evidence.configure_evidence(
                campaign_id="launch-1", mode=launch_evidence.MODE_NEW,
                competitor_brand="NYXI", expected_config_version=2,
            ))

        self.assertEqual(result["config_version"], 3)
        self.assertEqual(calls[0][0:2], ("create", "nodes"))
        self.assertEqual(calls[0][2]["节点代码"], "competitor_research")
        self.assertEqual(calls[1][0:3], ("update", "activities", "act1"))

    def test_no_evidence_mode_clears_all_competitor_fields(self):
        target = launch_evidence.build_config_target(
            mode="不使用竞品证据",
            change_reason="活动不需要竞品证据",
        )

        self.assertEqual("不使用竞品证据", target["竞品证据模式"])
        self.assertEqual("不适用", target["竞品分析状态"])
        self.assertEqual("", target["竞品品牌"])
        self.assertEqual([], target["关联竞品帖子"])
        self.assertEqual([], target["关联竞品营销事件"])

    def test_no_evidence_mode_rejects_stale_competitor_fields(self):
        with self.assertRaises(launch_evidence.EvidenceValidationError):
            launch_evidence.build_config_target(
                mode="不使用竞品证据", competitor_brand="NYXI",
                post_record_ids=["post1"],
            )

    def test_start_failure_marks_node_and_activity_failed_for_retry(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "证据配置版本": 5,
            "竞品证据模式": "发起新分析", "竞品分析状态": "待分析",
        }}
        node = {"record_id": "node1", "fields": {"节点代码": "competitor_research"}}
        calls = []

        async def fake_update(table_id, record_id, fields):
            calls.append((table_id, record_id, dict(fields)))
            if len(calls) == 2:
                raise RuntimeError("start failed")

        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.config, "T_LAUNCH_NODE", "nodes"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(
                 side_effect=[[activity], [node], [node]]
             )), \
             patch.object(launch_evidence.feishu, "update_record", new=AsyncMock(side_effect=fake_update)):
            with self.assertRaises(RuntimeError):
                asyncio.run(launch_evidence.start_analysis(
                    campaign_id="campaign-1", expected_config_version=5,
                ))
        self.assertEqual("执行中", calls[0][2]["节点状态"])
        self.assertEqual("已阻塞", calls[2][2]["节点状态"])
        self.assertEqual("失败", calls[3][2]["竞品分析状态"])
        self.assertEqual(5, calls[3][2]["证据配置版本"])

    def test_reuse_confirmed_post_increments_config_version_once(self):
        activity = {
            "record_id": "act1",
            "fields": {"活动ID": "campaign-1", "证据配置版本": 4},
        }
        post = {
            "record_id": "post1",
            "fields": {
                "竞品品牌": "NYXI",
                "人工复核状态": "已确认",
                "相关性": "相关",
            },
        }

        async def fake_get(table_id, record_id):
            return activity if record_id == "act1" else post

        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.config, "T_COMPETITOR_POST", "posts"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(return_value=[activity])), \
             patch.object(launch_evidence.feishu, "get_record", new=AsyncMock(side_effect=fake_get)), \
             patch.object(launch_evidence.feishu, "update_record", new=AsyncMock()) as update:
            result = asyncio.run(launch_evidence.configure_evidence(
                campaign_id="campaign-1",
                mode="引用历史证据",
                competitor_brand="NYXI",
                post_record_ids=["post1"],
                event_record_ids=[],
                change_reason="使用已审核 NYXI 证据",
                expected_config_version=4,
            ))

        self.assertEqual(5, result["config_version"])
        update.assert_awaited_once()
        written = update.await_args.args[2]
        self.assertEqual(5, written["证据配置版本"])
        self.assertEqual(["post1"], written["关联竞品帖子"])

    def test_start_moves_waiting_analysis_to_running(self):
        activity = {
            "record_id": "act1",
            "fields": {
                "活动ID": "campaign-1",
                "竞品证据模式": "发起新分析",
                "竞品分析状态": "待分析",
                "证据配置版本": 2,
            },
        }
        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.config, "T_LAUNCH_NODE", "nodes"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(
                 side_effect=[[activity], [{"record_id": "node1", "fields": {}}]]
             )), \
             patch.object(launch_evidence.feishu, "update_record", new=AsyncMock()) as update:
            result = asyncio.run(launch_evidence.start_analysis(
                campaign_id="campaign-1", expected_config_version=2,
            ))

        self.assertEqual("分析中", result["status"])
        self.assertEqual(3, result["config_version"])
        self.assertEqual("执行中", update.await_args_list[0].args[2]["节点状态"])
        self.assertEqual(
            {"竞品分析状态": "分析中", "证据配置版本": 3},
            update.await_args_list[1].args[2],
        )

    def test_retry_reopens_node_then_commits_new_activity_version(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "证据配置版本": 5,
            "竞品证据模式": "发起新分析", "竞品分析状态": "失败",
        }}
        node = {"record_id": "node1", "fields": {"节点状态": "已阻塞"}}
        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.config, "T_LAUNCH_NODE", "nodes"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(
                 side_effect=[[activity], [node]]
             )), \
             patch.object(launch_evidence.feishu, "update_record", new=AsyncMock()) as update:
            result = asyncio.run(launch_evidence.retry_analysis(
                campaign_id="campaign-1", expected_config_version=5,
            ))

        self.assertEqual("待分析", result["status"])
        self.assertEqual(6, result["config_version"])
        self.assertEqual("待执行", update.await_args_list[0].args[2]["节点状态"])
        self.assertEqual("待分析", update.await_args_list[1].args[2]["竞品分析状态"])

    def test_submit_then_confirm_only_accepts_current_candidate_subset(self):
        activity = {
            "record_id": "act1",
            "fields": {
                "活动ID": "campaign-1", "竞品证据模式": "发起新分析",
                "竞品分析状态": "分析中", "竞品品牌": "NYXI",
                "证据配置版本": 3,
            },
        }
        node = {
            "record_id": "node1",
            "fields": {"活动ID": "campaign-1", "节点代码": "competitor_research"},
        }
        posts = {
            "post1": {"record_id": "post1", "fields": {
                "竞品品牌": "NYXI", "人工复核状态": "已确认", "相关性": "相关",
            }},
            "post2": {"record_id": "post2", "fields": {
                "竞品品牌": "NYXI", "人工复核状态": "待确认", "相关性": "相关",
            }},
        }

        async def fake_search(table_id, filters, field_names=None):
            return [activity] if table_id == "activities" else [node]

        async def fake_get(table_id, record_id):
            return posts[record_id]

        async def fake_update(table_id, record_id, fields):
            target = activity if table_id == "activities" else node
            target["fields"].update(fields)

        with patch.object(launch_evidence.config, "LAUNCH_EVIDENCE_ENABLED", True), \
             patch.object(launch_evidence.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_evidence.config, "T_LAUNCH_NODE", "nodes"), \
             patch.object(launch_evidence.config, "T_COMPETITOR_POST", "posts"), \
             patch.object(launch_evidence.feishu, "search_records", new=AsyncMock(side_effect=fake_search)), \
             patch.object(launch_evidence.feishu, "get_record", new=AsyncMock(side_effect=fake_get)), \
             patch.object(launch_evidence.feishu, "update_record", new=AsyncMock(side_effect=fake_update)):
            submitted = asyncio.run(launch_evidence.submit_analysis(
                campaign_id="campaign-1", candidate_post_ids=["post1", "post2"],
                candidate_event_ids=[], submission_note="两条候选",
                expected_config_version=3,
            ))
            confirmed = asyncio.run(launch_evidence.confirm_analysis(
                campaign_id="campaign-1", confirmed_post_ids=["post1"],
                confirmed_event_ids=[], expected_config_version=4,
            ))

        self.assertEqual("待人工确认", submitted["status"])
        self.assertEqual(4, submitted["config_version"])
        self.assertEqual(["post1", "post2"], node["fields"]["待确认竞品帖子"])
        self.assertEqual("已就绪", confirmed["status"])
        self.assertEqual(5, confirmed["config_version"])
        self.assertEqual(["post1"], activity["fields"]["关联竞品帖子"])


if __name__ == "__main__":
    unittest.main()
