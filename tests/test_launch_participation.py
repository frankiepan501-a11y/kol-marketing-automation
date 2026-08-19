import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_participation


class LaunchParticipationTests(unittest.TestCase):
    def test_readback_retries_feishu_eventual_consistency_before_blocking(self):
        stale = {"fields": {"锁定批次ID": "old"}}
        fresh = {"fields": {"锁定批次ID": "new"}}
        with patch.object(
            launch_participation.feishu, "get_record",
            new=AsyncMock(side_effect=[stale, fresh]),
        ) as get_mock, patch.object(
            launch_participation.asyncio, "sleep", new=AsyncMock()
        ) as sleep_mock:
            actual = asyncio.run(launch_participation._verified_readback(
                "participants", "part1", {"锁定批次ID": "new"},
            ))

        self.assertEqual(fresh, actual)
        self.assertEqual(2, get_mock.await_count)
        sleep_mock.assert_awaited_once()

    def test_update_uses_authoritative_put_response_instead_of_stale_followup_get(self):
        updated = {"record_id": "part1", "fields": {"锁定批次ID": "new"}}
        with patch.object(
            launch_participation.feishu, "update_record",
            new=AsyncMock(return_value=updated),
        ) as update_mock, patch.object(
            launch_participation.feishu, "get_record", new=AsyncMock()
        ) as get_mock:
            actual = asyncio.run(launch_participation._update_and_verify(
                "participants", "part1", {"锁定批次ID": "new"},
            ))

        self.assertEqual(updated, actual)
        update_mock.assert_awaited_once()
        get_mock.assert_not_awaited()

    def test_partial_put_response_is_marked_for_deferred_batch_readback(self):
        accepted = {"record_id": "part1"}
        with patch.object(
            launch_participation.feishu, "update_record",
            new=AsyncMock(return_value=accepted),
        ), patch.object(
            launch_participation.feishu, "get_record", new=AsyncMock()
        ) as get_mock:
            actual = asyncio.run(launch_participation._update_and_verify(
                "participants", "part1", {"锁定批次ID": "new"},
            ))
        self.assertTrue(actual["_deferred_verification"])
        get_mock.assert_not_awaited()

    def test_update_and_confirm_reads_back_accepted_response_before_returning(self):
        accepted = {"record_id": "part1", "_deferred_verification": True}
        fresh = {"record_id": "part1", "fields": {"参与状态": "已取消"}}
        with patch.object(
            launch_participation, "_update_and_verify", new=AsyncMock(return_value=accepted),
        ), patch.object(
            launch_participation, "_verified_readback", new=AsyncMock(return_value=fresh),
        ) as readback_mock:
            actual = asyncio.run(launch_participation._update_and_confirm(
                "participants", "part1", {"参与状态": "已取消"},
            ))

        self.assertEqual(fresh, actual)
        readback_mock.assert_awaited_once()

    def test_deferred_batch_readback_waits_then_verifies_all_expected_fields(self):
        fresh = {"record_id": "part1", "fields": {"锁定批次ID": "new"}}
        with patch.object(
            launch_participation.feishu, "get_record",
            new=AsyncMock(return_value=fresh),
        ), patch.object(
            launch_participation.asyncio, "sleep", new=AsyncMock()
        ) as sleep_mock:
            asyncio.run(launch_participation._verify_deferred_batch([
                ("part1", {"锁定批次ID": "new"}),
            ]))
        sleep_mock.assert_awaited_once_with(5)

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

    def test_history_parses_feishu_rich_text_segments_instead_of_treating_them_as_snapshots(self):
        value = [{"text": '[{"ranking_version":"evidence-v1","final_priority":84}]',
                  "type": "text"}]
        history = launch_participation._history(value)
        self.assertEqual(1, len(history))
        self.assertEqual("evidence-v1", history[0]["ranking_version"])

    def test_history_recovers_previously_nested_feishu_text_wrapper(self):
        nested = '[{"text":"[{\\"ranking_version\\":\\"evidence-v1\\",\\"final_priority\\":84}]","type":"text"},{"ranking_version":"evidence-v2","final_priority":95}]'
        history = launch_participation._history(nested)
        self.assertEqual(["evidence-v1", "evidence-v2"], [
            item["ranking_version"] for item in history
        ])

    def test_with_snapshot_replaces_same_ranking_version_instead_of_growing_forever(self):
        existing = '[{"ranking_version":"evidence-v2","final_priority":80}]'
        updated = launch_participation._with_snapshot(
            {"排序快照历史": existing},
            {"score": 90, "final_priority": 95, "evidence_level": "B"},
            "evidence-v2",
        )
        parsed = launch_participation._history(updated)
        self.assertEqual(1, len(parsed))
        self.assertEqual(95, parsed[0]["final_priority"])

    def test_ranking_fields_include_actionable_human_review_snapshot(self):
        fields = launch_participation._ranking_fields({
            "decision": "eligible_new_cold", "score": 84, "final_priority": 3084,
            "evidence_level": "A", "matched_post_ids": ["post1"],
            "profile_url": "https://youtube.com/@creator", "platform": "YouTube",
            "country": "US", "language": "en", "followers": 100000,
            "content_summary": "Controller review", "content_updated_at": 1_800_000_000_000,
            "relationship_summary": "合作状态=未建联",
            "evidence_summary": "NYXI证据1条",
            "primary_evidence_url": "https://youtube.com/watch?v=nyxi",
            "review_route": "KOL运营审核",
            "review_instruction": "请打开主页确认内容适配性",
            "review_decision": "待审核",
        }, "evidence-v2")

        self.assertEqual("https://youtube.com/@creator", fields["达人主页"]["link"])
        self.assertEqual("KOL运营审核", fields["系统审核分流"])
        self.assertEqual("待审核", fields["审核结论"])
        self.assertIn("主页", fields["系统审核说明"])
        self.assertEqual("https://youtube.com/watch?v=nyxi", fields["主证据帖子"]["link"])

    def test_missing_review_snapshot_fails_closed_and_clears_old_audit_signature(self):
        fields = launch_participation._ranking_fields({
            "decision": "eligible_new_cold", "score": 84, "final_priority": 84,
            "evidence_level": "无加分", "matched_post_ids": [],
        }, "evidence-v2")

        self.assertEqual("KOL运营审核", fields["系统审核分流"])
        self.assertEqual("待审核", fields["审核结论"])
        self.assertIn("未生成", fields["系统审核说明"])
        self.assertEqual("", fields["审核原因"])
        self.assertIsNone(fields["审核人"])
        self.assertIsNone(fields["审核时间"])

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
