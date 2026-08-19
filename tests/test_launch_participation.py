import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_participation


class LaunchParticipationTests(unittest.TestCase):
    def test_readback_accepts_feishu_numeric_strings_for_numeric_fields(self):
        launch_participation._assert_readback(
            {"fields": {"基础评分快照": "84", "最终优先级": "3084.00", "零分": 0}},
            {"基础评分快照": 84, "最终优先级": 3084, "零分": 0},
        )

    def test_readback_still_rejects_different_numeric_values(self):
        with self.assertRaises(launch_participation.ParticipantManualReviewError):
            launch_participation._assert_readback(
                {"fields": {"基础评分快照": "85"}},
                {"基础评分快照": 84},
            )

    def test_participant_key_is_stable(self):
        self.assertEqual(
            launch_participation.participant_key("c", "p", "KOL", "k"),
            "c|p|KOL|k",
        )

    def test_snapshot_keeps_identity_window_p75_and_base_filter_conclusions(self):
        snapshot = launch_participation._snapshot({
            "score": 91, "final_priority": 3091, "evidence_level": "A",
            "decision": "eligible_new_cold", "identity_paths": ["linked_kol"],
            "stable_identity_keys": ["kol_record:kol1"],
            "matched_post_ids": ["post1", "post2"],
            "evidence_posts": [
                {"post_id": "post2", "published_at": 2000},
                {"post_id": "post1", "published_at": 1000},
            ],
            "p75_thresholds": {"YouTube|长视频": 600},
            "p75_samples": {"YouTube|长视频": 8},
            "base_filter_passed": True,
            "base_filter_reasons": ["country_match", "language_match"],
        }, "evidence-v7")

        self.assertEqual("kol_record:kol1", snapshot["stable_identity_keys"][0])
        self.assertEqual("post1", snapshot["long_term_first_post"]["post_id"])
        self.assertEqual("post2", snapshot["long_term_last_post"]["post_id"])
        self.assertEqual(600, snapshot["p75_thresholds"]["YouTube|长视频"])
        self.assertTrue(snapshot["base_filter_passed"])
        self.assertEqual(["country_match", "language_match"], snapshot["base_filter_reasons"])

    def test_full_replacement_cancels_omitted_kol_without_touching_media_version(self):
        activity = {
            "record_id": "act1",
            "fields": {
                "活动ID": "campaign-1", "产品主记录ID": "product1",
                "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
                "证据排序版本": "v2", "名单锁定授权": True,
                "KOL已锁定名单版本": "v1", "媒体人已锁定名单版本": "media-v7",
                "KOL名单阻塞代码": "", "KOL失败锁定批次ID": "",
                "KOL阻塞待处理记录": "",
            },
        }
        records = [
            {"record_id": "part1", "fields": {
                "活动参与唯一键": "campaign-1|product1|KOL|kol1",
                "活动ID": "campaign-1", "产品主记录ID": "product1",
                "对象类型": "KOL", "联系人记录ID": "kol1",
                "参与状态": "已入围", "名单版本": "v1", "锁定批次ID": "old",
                "排序快照历史": "[]",
            }},
            {"record_id": "part2", "fields": {
                "活动参与唯一键": "campaign-1|product1|KOL|kol2",
                "活动ID": "campaign-1", "产品主记录ID": "product1",
                "对象类型": "KOL", "联系人记录ID": "kol2",
                "参与状态": "已入围", "名单版本": "v1", "锁定批次ID": "old",
                "排序快照历史": "[]",
            }},
        ]
        preview = {"candidates": [
            {"contact_id": "kol1", "decision": "eligible_new_cold", "score": 90,
             "final_priority": 90, "evidence_level": "无加分", "matched_post_ids": []},
            {"contact_id": "kol3", "decision": "reactivation_same_thread", "score": 88,
             "final_priority": 88, "evidence_level": "无加分", "matched_post_ids": []},
        ]}

        async def fake_create(table_id, fields):
            record = {"record_id": "part3", "fields": dict(fields)}
            records.append(record)
            return "part3"

        async def fake_update(table_id, record_id, fields):
            target = activity if table_id == "activities" else next(
                row for row in records if row["record_id"] == record_id
            )
            target["fields"].update(fields)

        async def fake_get(table_id, record_id):
            return activity if table_id == "activities" else next(
                row for row in records if row["record_id"] == record_id
            )

        with patch.object(launch_participation.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", True), \
             patch.object(launch_participation.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_participation.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_participation.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(launch_participation.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=preview)), \
             patch.object(launch_participation.feishu, "search_records", new=AsyncMock(return_value=records)), \
             patch.object(launch_participation.feishu, "create_record", new=AsyncMock(side_effect=fake_create)), \
             patch.object(launch_participation.feishu, "update_record", new=AsyncMock(side_effect=fake_update)), \
             patch.object(launch_participation.feishu, "get_record", new=AsyncMock(side_effect=fake_get)):
            result = asyncio.run(launch_participation.lock_participants(
                campaign_id="campaign-1", product_family_id="product1", object_type="KOL",
                contact_ids=["kol1", "kol3"], expected_ranking_version="v2",
                lock_batch_id="batch-2",
            ))

        by_contact = {}
        for row in records:
            fields = row["fields"]
            contact_id = fields.get("联系人记录ID")
            if not contact_id:
                contact_id = (fields.get("关联KOL") or [""])[0]
            by_contact[contact_id] = fields
        self.assertEqual("v2", activity["fields"]["KOL已锁定名单版本"])
        self.assertEqual("media-v7", activity["fields"]["媒体人已锁定名单版本"])
        self.assertEqual("v2", by_contact["kol1"]["名单版本"])
        self.assertEqual("已入围", by_contact["kol3"]["参与状态"])
        self.assertEqual("campaign-1|product1|KOL|kol3", by_contact["kol3"]["参与记录ID"])
        self.assertEqual("product1", by_contact["kol3"]["产品家族ID"])
        self.assertEqual("已取消", by_contact["kol2"]["参与状态"])
        self.assertEqual("不再符合", by_contact["kol2"]["取消原因代码"])
        self.assertEqual(1, result["created"])
        self.assertEqual(1, result["cancelled"])

    def test_partial_write_rolls_back_and_marks_only_kol_retryable(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "产品主记录ID": "product1",
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
            "证据排序版本": "v2", "名单锁定授权": True,
            "KOL已锁定名单版本": "v1", "媒体人已锁定名单版本": "media-v7",
            "KOL名单阻塞代码": "", "KOL失败锁定批次ID": "",
            "KOL阻塞待处理记录": "",
        }}
        record = {"record_id": "part1", "fields": {
            "活动参与唯一键": "campaign-1|product1|KOL|kol1",
            "活动ID": "campaign-1", "产品主记录ID": "product1",
            "对象类型": "KOL", "联系人记录ID": "kol1",
            "参与状态": "已入围", "名单版本": "v1", "锁定批次ID": "old",
            "取消原因代码": "", "排序快照历史": "[]",
        }}
        preview = {"candidates": [
            {"contact_id": "kol1", "decision": "eligible_new_cold", "score": 90,
             "final_priority": 90, "evidence_level": "无加分"},
            {"contact_id": "kol3", "decision": "eligible_new_cold", "score": 88,
             "final_priority": 88, "evidence_level": "无加分"},
        ]}

        async def fake_update(table_id, record_id, fields):
            target = activity if table_id == "activities" else record
            target["fields"].update(fields)

        with patch.object(launch_participation.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", True), \
             patch.object(launch_participation.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_participation.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_participation.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(launch_participation.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=preview)), \
             patch.object(launch_participation.feishu, "search_records", new=AsyncMock(return_value=[record])), \
             patch.object(launch_participation.feishu, "create_record", new=AsyncMock(side_effect=RuntimeError("POST records → 400 rejected"))), \
             patch.object(launch_participation.feishu, "update_record", new=AsyncMock(side_effect=fake_update)), \
             patch.object(launch_participation.feishu, "get_record", new=AsyncMock(return_value=record)):
            with self.assertRaises(launch_participation.ParticipantRetryableError):
                asyncio.run(launch_participation.lock_participants(
                    campaign_id="campaign-1", product_family_id="product1", object_type="KOL",
                    contact_ids=["kol1", "kol3"], expected_ranking_version="v2",
                    lock_batch_id="batch-failed",
                ))

        self.assertEqual("v1", record["fields"]["名单版本"])
        self.assertEqual("LOCK_BATCH_RETRYABLE", activity["fields"]["KOL名单阻塞代码"])
        self.assertEqual("batch-failed", activity["fields"]["KOL失败锁定批次ID"])
        self.assertEqual("media-v7", activity["fields"]["媒体人已锁定名单版本"])

    def test_create_rechecks_unique_key_and_blocks_duplicate_for_manual_review(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "产品主记录ID": "product1",
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
            "证据排序版本": "v2", "名单锁定授权": True,
            "KOL已锁定名单版本": "v1", "KOL名单阻塞代码": "",
        }}
        preview = {"candidates": [{
            "contact_id": "kol1", "decision": "eligible_new_cold", "score": 90,
            "final_priority": 90, "evidence_level": "无加分",
        }]}
        created = {"record_id": "part1", "fields": {}}
        duplicate = {"record_id": "part2", "fields": {}}
        calls = 0

        async def fake_search(table_id, filters):
            nonlocal calls
            calls += 1
            return [] if calls == 1 else [created, duplicate]

        async def fake_create(table_id, fields):
            created["fields"].update(fields)
            duplicate["fields"].update(fields)
            return "part1"

        async def fake_update(table_id, record_id, fields):
            target = activity if table_id == "activities" else (
                created if record_id == "part1" else duplicate
            )
            target["fields"].update(fields)

        with patch.object(launch_participation.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", True), \
             patch.object(launch_participation.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_participation.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_participation.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(launch_participation.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=preview)), \
             patch.object(launch_participation.feishu, "search_records", new=AsyncMock(side_effect=fake_search)), \
             patch.object(launch_participation.feishu, "create_record", new=AsyncMock(side_effect=fake_create)), \
             patch.object(launch_participation.feishu, "update_record", new=AsyncMock(side_effect=fake_update)), \
             patch.object(launch_participation.feishu, "get_record", new=AsyncMock(return_value=created)):
            with self.assertRaises(launch_participation.ParticipantManualReviewError):
                asyncio.run(launch_participation.lock_participants(
                    campaign_id="campaign-1", product_family_id="product1",
                    object_type="KOL", contact_ids=["kol1"],
                    expected_ranking_version="v2", lock_batch_id="batch-duplicate",
                ))

        self.assertEqual("LOCK_BATCH_MANUAL_REVIEW", activity["fields"]["KOL名单阻塞代码"])
        self.assertEqual("batch-duplicate", activity["fields"]["KOL失败锁定批次ID"])
        self.assertEqual("已取消", created["fields"]["参与状态"])

    def test_uncertain_create_and_failed_unique_query_blocks_automatic_retry(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "产品主记录ID": "product1",
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
            "证据排序版本": "v2", "名单锁定授权": True,
            "KOL已锁定名单版本": "v1", "KOL名单阻塞代码": "",
        }}
        preview = {"candidates": [{
            "contact_id": "kol1", "decision": "eligible_new_cold", "score": 90,
            "final_priority": 90, "evidence_level": "无加分",
        }]}
        calls = 0

        async def fake_search(table_id, filters):
            nonlocal calls
            calls += 1
            if calls == 1:
                return []
            raise RuntimeError("unique query unavailable")

        async def fake_update(table_id, record_id, fields):
            if table_id == "activities":
                activity["fields"].update(fields)

        with patch.object(launch_participation.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", True), \
             patch.object(launch_participation.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_participation.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_participation.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(launch_participation.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=preview)), \
             patch.object(launch_participation.feishu, "search_records", new=AsyncMock(side_effect=fake_search)), \
             patch.object(launch_participation.feishu, "create_record", new=AsyncMock(side_effect=TimeoutError("unknown commit"))), \
             patch.object(launch_participation.feishu, "update_record", new=AsyncMock(side_effect=fake_update)), \
             patch.object(launch_participation.feishu, "get_record", new=AsyncMock()):
            with self.assertRaises(launch_participation.ParticipantManualReviewError):
                asyncio.run(launch_participation.lock_participants(
                    campaign_id="campaign-1", product_family_id="product1",
                    object_type="KOL", contact_ids=["kol1"],
                    expected_ranking_version="v2", lock_batch_id="batch-uncertain",
                ))

        self.assertEqual("LOCK_BATCH_MANUAL_REVIEW", activity["fields"]["KOL名单阻塞代码"])
        self.assertEqual("batch-uncertain", activity["fields"]["KOL失败锁定批次ID"])
        self.assertIn("key:campaign-1|product1|KOL|kol1", activity["fields"]["KOL阻塞待处理记录"])

    def test_validation_checks_entire_batch_before_changing_first_record(self):
        activity = {"record_id": "act1", "fields": {
            "活动ID": "campaign-1", "产品主记录ID": "product1",
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
            "证据排序版本": "v2", "名单锁定授权": True,
            "KOL已锁定名单版本": "v1", "KOL名单阻塞代码": "",
        }}
        records = [
            {"record_id": "part1", "fields": {
                "活动参与唯一键": "campaign-1|product1|KOL|kol1",
                "联系人记录ID": "kol1", "参与状态": "已入围",
                "名单版本": "v1", "锁定批次ID": "old", "排序快照历史": "[]",
            }},
            {"record_id": "part2", "fields": {
                "活动参与唯一键": "campaign-1|product1|KOL|kol2",
                "联系人记录ID": "kol2", "参与状态": "已取消",
                "取消原因代码": "运营取消", "名单版本": "v1",
                "锁定批次ID": "old", "排序快照历史": "[]",
            }},
        ]
        preview = {"candidates": [
            {"contact_id": "kol1", "decision": "eligible_new_cold", "score": 90,
             "final_priority": 90, "evidence_level": "无加分"},
            {"contact_id": "kol2", "decision": "eligible_new_cold", "score": 80,
             "final_priority": 80, "evidence_level": "无加分"},
        ]}

        async def fake_update(table_id, record_id, fields):
            target = activity if table_id == "activities" else next(
                row for row in records if row["record_id"] == record_id
            )
            target["fields"].update(fields)

        with patch.object(launch_participation.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", True), \
             patch.object(launch_participation.config, "T_LAUNCH_CAMPAIGN", "activities"), \
             patch.object(launch_participation.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_participation.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(launch_participation.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=preview)), \
             patch.object(launch_participation.feishu, "search_records", new=AsyncMock(return_value=records)), \
             patch.object(launch_participation.feishu, "create_record", new=AsyncMock()), \
             patch.object(launch_participation.feishu, "update_record", new=AsyncMock(side_effect=fake_update)), \
             patch.object(launch_participation.feishu, "get_record", new=AsyncMock()):
            with self.assertRaises(launch_participation.ParticipantValidationError):
                asyncio.run(launch_participation.lock_participants(
                    campaign_id="campaign-1", product_family_id="product1", object_type="KOL",
                    contact_ids=["kol1", "kol2"], expected_ranking_version="v2",
                    lock_batch_id="batch-invalid",
                ))

        self.assertEqual("v1", records[0]["fields"]["名单版本"])
        self.assertEqual("old", records[0]["fields"]["锁定批次ID"])


if __name__ == "__main__":
    unittest.main()
