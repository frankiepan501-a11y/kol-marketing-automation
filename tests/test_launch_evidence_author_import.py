import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_evidence_author_import as importer


class LaunchEvidenceAuthorImportTests(unittest.TestCase):
    def _candidate(self):
        return {
            "author_key": "youtube|handle:mekelkasanova", "name": "Mekel Kasanova",
            "platform": "YouTube", "handle": "MekelKasanova", "creator_id": "",
            "profile_url": "https://youtube.com/@MekelKasanova",
            "public_profile_url": "https://youtube.com/@MekelKasanova",
            "country": "US", "country_raw": "United States", "language": "en",
            "followers": 12000, "email_verified": True,
            "_verified_email": "new@example.com", "eligible_for_master_write": True,
            "write_block_reasons": [], "matched_post_ids": ["post1"],
            "evidence_strength_score": 88,
        }

    def test_dry_run_plans_both_records_but_never_writes(self):
        candidate = self._candidate()
        enrichment = {
            "campaign_id": importer.DAVE_CAMPAIGN_ID,
            "ranking_version": "evidence-v4", "candidates": [candidate],
        }
        enrich = AsyncMock(return_value=enrichment)
        with patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=enrich,
        ), patch.object(
            importer.launch_evidence, "get_activity",
            new=AsyncMock(return_value={"record_id": "activity1", "fields": {
                "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
                "证据排序版本": "evidence-v4",
            }}),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(return_value=([], [])),
        ), patch.object(importer.feishu, "create_record", new=AsyncMock()) as create:
            result = asyncio.run(importer.run_controlled_import(
                campaign_id=importer.DAVE_CAMPAIGN_ID,
                seed_candidates=[candidate], source_job_id="launchruntime-source",
                expected_handles=["mekelkasanova"], commit=False,
            ))

        self.assertTrue(result["read_only"])
        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["planned"])
        self.assertTrue(enrich.await_args.kwargs["_reattach_server_evidence"])
        create.assert_not_awaited()

    def test_commit_creates_pending_participant_without_draft_and_is_replay_safe(self):
        candidate = self._candidate()
        enrichment = {
            "campaign_id": importer.DAVE_CAMPAIGN_ID,
            "ranking_version": "evidence-v4", "candidates": [candidate],
        }
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        master = {"record_id": "kol1", "fields": {
            "账号名": "New Creator", "邮箱": "new@example.com",
            "合作状态": "未建联", "触达路由状态": "待核对",
            "资料可用状态": "有效",
            "迁移备注": importer.controlled_marker(
                importer.DAVE_CAMPAIGN_ID, "youtube|handle:mekelkasanova",
            ),
        }}
        participant = {"record_id": "participant1", "fields": {
            "参与记录ID": importer.participant_key(
                importer.DAVE_CAMPAIGN_ID, "product1", "kol1",
            ),
            "审核结论": "待审核", "参与状态": "已入围",
            "关联KOL": ["kol1"],
        }}
        created_tables = []

        async def create_record(table, fields):
            created_tables.append((table, fields))
            return "kol1" if table == importer.config.T_KOL else "participant1"

        async def get_record(table, record_id):
            return master if table == importer.config.T_KOL else participant

        participant_lookups = [[], [participant]]
        with patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value=enrichment),
        ), patch.object(
            importer.config, "T_KOL", "kol-table",
        ), patch.object(
            importer.config, "T_LAUNCH_PARTICIPANT", "participant-table",
        ), patch.object(
            importer.config, "T_DRAFT", "draft-table",
        ), patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(return_value=([], [])),
        ), patch.object(
            importer.feishu, "create_record", side_effect=create_record,
        ), patch.object(
            importer.feishu, "get_record", side_effect=get_record,
        ), patch.object(
            importer, "_participants_by_unique_key_strong",
            new=AsyncMock(side_effect=participant_lookups),
        ), patch.object(
            importer.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ):
            result = asyncio.run(importer.run_controlled_import(
                campaign_id=importer.DAVE_CAMPAIGN_ID,
                seed_candidates=[candidate], source_job_id="launchruntime-source",
                expected_handles=["mekelkasanova"], commit=True,
            ))

        self.assertEqual(2, result["writes"])
        self.assertEqual(0, result["drafts_created"])
        self.assertEqual(0, result["emails_sent"])
        master_fields = created_tables[0][1]
        self.assertEqual("待核对", master_fields["触达路由状态"])
        self.assertEqual("有效", master_fields["资料可用状态"])
        participant_fields = created_tables[1][1]
        self.assertEqual("待审核", participant_fields["审核结论"])
        self.assertNotIn("关联邮件草稿", participant_fields)

    def test_commit_replay_reuses_controlled_master_and_participant(self):
        candidate = self._candidate()
        enrichment = {
            "campaign_id": importer.DAVE_CAMPAIGN_ID,
            "ranking_version": "evidence-v4", "candidates": [candidate],
        }
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        master = {"record_id": "kol1", "fields": {
            "账号名": "New Creator", "邮箱": "new@example.com",
            "迁移备注": importer.controlled_marker(
                importer.DAVE_CAMPAIGN_ID, "youtube|handle:mekelkasanova",
            ),
            "主平台": "YouTube", "主链接": {"link": "https://youtube.com/@MekelKasanova"},
        }}
        participant = {"record_id": "participant1", "fields": {
            "参与记录ID": importer.participant_key(
                importer.DAVE_CAMPAIGN_ID, "product1", "kol1",
            ),
            "审核结论": "待审核", "参与状态": "已入围",
            "关联KOL": ["kol1"],
        }}

        with patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value=enrichment),
        ), patch.object(
            importer.config, "T_KOL", "kol-table",
        ), patch.object(
            importer.config, "T_LAUNCH_PARTICIPANT", "participant-table",
        ), patch.object(
            importer.config, "T_DRAFT", "draft-table",
        ), patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(return_value=([master], [])),
        ), patch.object(
            importer, "_participants_by_unique_key_strong",
            new=AsyncMock(return_value=[participant]),
        ), patch.object(
            importer.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            importer.feishu, "create_record", new=AsyncMock(),
        ) as create:
            result = asyncio.run(importer.run_controlled_import(
                campaign_id=importer.DAVE_CAMPAIGN_ID,
                seed_candidates=[candidate], source_job_id="launchruntime-source",
                expected_handles=["mekelkasanova"], commit=True,
            ))

        self.assertEqual(0, result["writes"])
        self.assertEqual("reused", result["results"][0]["master_action"])
        self.assertEqual("reused", result["results"][0]["participant_action"])
        create.assert_not_awaited()

    def test_dry_run_counts_existing_controlled_master_as_reusable(self):
        candidate = self._candidate()
        candidate.update({
            "eligible_for_master_write": False,
            "write_block_reasons": [
                "creator_identity_already_in_kol_or_media_master",
                "email_already_in_kol_or_media_master",
            ],
        })
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        master = {"record_id": "kol1", "fields": {
            "邮箱": "new@example.com",
            "迁移备注": importer.controlled_marker(
                importer.DAVE_CAMPAIGN_ID, "youtube|handle:mekelkasanova",
            ),
            "主链接": {"link": "https://youtube.com/@MekelKasanova"},
        }}
        candidate["existing_identity_owners"] = [{"record_id": "kol1"}]
        candidate["existing_email_owners"] = [{"record_id": "kol1"}]

        with patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value={
                "ranking_version": "evidence-v4", "candidates": [candidate],
            }),
        ), patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(return_value=([master], [])),
        ), patch.object(importer.feishu, "create_record", new=AsyncMock()) as create:
            result = asyncio.run(importer.run_controlled_import(
                campaign_id=importer.DAVE_CAMPAIGN_ID,
                seed_candidates=[candidate], source_job_id="launchruntime-source",
                expected_handles=["mekelkasanova"], commit=False,
            ))

        self.assertEqual(1, result["planned"])
        self.assertEqual(1, result["reusable"])
        self.assertEqual([], result["blocked"])
        create.assert_not_awaited()

    def test_commit_replay_blocks_new_duplicate_owner_detected_before_write(self):
        candidate = self._candidate()
        candidate.update({
            "eligible_for_master_write": False,
            "write_block_reasons": [
                "creator_identity_already_in_kol_or_media_master",
                "email_already_in_kol_or_media_master",
            ],
        })
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        master = {"record_id": "kol1", "fields": {
            "邮箱": "new@example.com",
            "迁移备注": importer.controlled_marker(
                importer.DAVE_CAMPAIGN_ID, "youtube|handle:mekelkasanova",
            ),
            "主链接": {"link": "https://youtube.com/@MekelKasanova"},
        }}
        duplicate = {"record_id": "kol2", "fields": {
            "邮箱": "new@example.com",
            "主链接": {"link": "https://youtube.com/@MekelKasanova"},
        }}
        candidate["existing_identity_owners"] = [{"record_id": "kol1"}]
        candidate["existing_email_owners"] = [{"record_id": "kol1"}]

        with patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value={
                "ranking_version": "evidence-v4", "candidates": [candidate],
            }),
        ), patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(side_effect=[([master], []), ([master, duplicate], [])]),
        ), patch.object(importer.feishu, "create_record", new=AsyncMock()) as create:
            with self.assertRaises(importer.ControlledImportError) as ctx:
                asyncio.run(importer.run_controlled_import(
                    campaign_id=importer.DAVE_CAMPAIGN_ID,
                    seed_candidates=[candidate], source_job_id="launchruntime-source",
                    expected_handles=["mekelkasanova"], commit=True,
                ))

        self.assertIn("changed_before_write", str(ctx.exception))
        create.assert_not_awaited()

    def test_commit_replay_still_blocks_when_current_public_gate_fails(self):
        candidate = self._candidate()
        candidate.update({
            "eligible_for_master_write": False,
            "write_block_reasons": [
                "country_not_target", "creator_identity_already_in_kol_or_media_master",
                "email_already_in_kol_or_media_master",
            ],
        })
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        master = {"record_id": "kol1", "fields": {
            "邮箱": "new@example.com",
            "迁移备注": importer.controlled_marker(
                importer.DAVE_CAMPAIGN_ID, "youtube|handle:mekelkasanova",
            ),
            "主链接": {"link": "https://youtube.com/@MekelKasanova"},
        }}
        candidate["existing_identity_owners"] = [{"record_id": "kol1"}]
        candidate["existing_email_owners"] = [{"record_id": "kol1"}]

        with patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value={
                "ranking_version": "evidence-v4", "candidates": [candidate],
            }),
        ), patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(return_value=([master], [])),
        ), patch.object(importer.feishu, "create_record", new=AsyncMock()) as create:
            with self.assertRaises(importer.ControlledImportError) as ctx:
                asyncio.run(importer.run_controlled_import(
                    campaign_id=importer.DAVE_CAMPAIGN_ID,
                    seed_candidates=[candidate], source_job_id="launchruntime-source",
                    expected_handles=["mekelkasanova"], commit=True,
                ))

        self.assertIn("country_not_target", str(ctx.exception))
        create.assert_not_awaited()

    def test_master_safety_fields_are_repaired_one_by_one_and_read_back(self):
        marker = importer.controlled_marker(
            importer.DAVE_CAMPAIGN_ID, "youtube|handle:mekelkasanova",
        )
        initial = {"record_id": "kol1", "fields": {
            "邮箱": "new@example.com", "迁移备注": marker,
        }}
        repaired = {"record_id": "kol1", "fields": {
            "邮箱": "new@example.com", "迁移备注": marker,
            "合作状态": "未建联", "触达路由状态": "待核对",
            "资料可用状态": "有效",
        }}
        with patch.object(
            importer.feishu, "get_record", new=AsyncMock(side_effect=[initial, repaired]),
        ), patch.object(
            importer.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(importer._readback_master_safety(
                "kol1", "new@example.com", importer.DAVE_CAMPAIGN_ID,
                "youtube|handle:mekelkasanova",
            ))

        self.assertEqual(repaired, result)
        self.assertEqual(3, update.await_count)
        self.assertEqual(
            [{"合作状态": "未建联"}, {"触达路由状态": "待核对"},
             {"资料可用状态": "有效"}],
            [call.args[2] for call in update.await_args_list],
        )

    def test_draft_guard_uses_strong_list_readback(self):
        rows = [
            {"record_id": "draft1", "fields": {"关联KOL": ["kol1"]}},
            {"record_id": "draft2", "fields": {"关联KOL": ["kol2"]}},
        ]
        with patch.object(
            importer.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ) as fetch:
            result = asyncio.run(importer._drafts_for_kol("kol1"))

        self.assertEqual(["draft1"], [row["record_id"] for row in result])
        fetch.assert_awaited_once()

    def test_continuation_import_selects_only_current_server_verified_candidates(self):
        eligible = self._candidate()
        eligible.update({"handle": "nextcreator", "author_key": "youtube|handle:nextcreator"})
        blocked = self._candidate()
        blocked.update({
            "handle": "blockedcreator", "author_key": "youtube|handle:blockedcreator",
            "eligible_for_master_write": False,
            "write_block_reasons": ["missing_valid_email"],
        })
        sample = {
            "candidates": [
                {"author_key": f"server-only-{index}"} for index in range(20)
            ],
            "unmatched_authors": 100,
        }
        activity_ctx = {"ranking_version": "evidence-v4"}
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        with patch.object(
            importer.preview, "_build_unmatched_evidence_author_sample",
            new=AsyncMock(return_value=(sample, activity_ctx, [], [])),
        ) as build, patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value={
                "ranking_version": "evidence-v4", "candidates": [blocked, eligible],
            }),
        ) as enrich, patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(importer, "_commit_selected", new=AsyncMock()) as commit:
            result = asyncio.run(importer.run_continuation_import(
                campaign_id=importer.DAVE_CAMPAIGN_ID, offset=17,
                sample_limit=20, import_limit=3, commit=False,
                source_job_id="launchruntime-current-job",
            ))

        self.assertEqual(1, result["planned"])
        self.assertEqual(36, result["next_offset"])
        self.assertEqual(["nextcreator"], result["selected_handles"])
        build.assert_awaited_once_with(
            campaign_id=importer.DAVE_CAMPAIGN_ID, limit=20, offset=17,
        )
        enrich.assert_awaited_once_with(
            campaign_id=importer.DAVE_CAMPAIGN_ID, limit=20,
            seed_candidates=sample["candidates"],
            source_job_id="launchruntime-current-job",
            _include_verified_email=True, _reattach_server_evidence=True,
        )
        commit.assert_not_awaited()

    def test_continuation_import_recovers_committed_reviews_after_partial_failure(self):
        eligible = []
        for index in range(3):
            candidate = self._candidate()
            candidate.update({
                "handle": f"nextcreator{index}",
                "author_key": f"youtube|handle:nextcreator{index}",
            })
            eligible.append(candidate)
        sample = {
            "candidates": [{"author_key": f"server-only-{i}"} for i in range(20)],
            "unmatched_authors": 100,
        }
        activity = {"record_id": "activity1", "fields": {
            "活动ID": importer.DAVE_CAMPAIGN_ID, "产品主记录ID": "product1",
            "证据排序版本": "evidence-v4",
        }}
        durable = {
            "results": [{
                "author_key": "youtube|handle:nextcreator0",
                "source_job_id": "launchruntime-current-job",
                "handle": "nextcreator0", "kol_id": "kol1",
                "participant_ids": ["participant1"], "participant_count": 1,
                "review_statuses": ["待审核"], "draft_count": 0,
                "participant_source_job_ids": ["launchruntime-current-job"],
            }, {
                "author_key": "youtube|handle:nextcreator1",
                "source_job_id": "launchruntime-current-job",
                "handle": "nextcreator1", "kol_id": "kol2",
                "participant_ids": [], "participant_count": 0,
                "review_statuses": [], "draft_count": 0,
                "participant_source_job_ids": [],
            }, {
                "author_key": "youtube|handle:nextcreator2",
                "source_job_id": "launchruntime-other-job",
                "handle": "nextcreator2", "kol_id": "kol3",
                "participant_ids": ["participant3"], "participant_count": 1,
                "review_statuses": ["待审核"], "draft_count": 0,
                "participant_source_job_ids": ["launchruntime-other-job"],
            }],
        }
        with patch.object(
            importer.preview, "_build_unmatched_evidence_author_sample",
            new=AsyncMock(return_value=(sample, {"ranking_version": "evidence-v4"}, [], [])),
        ), patch.object(
            importer.preview, "enrich_unmatched_evidence_authors",
            new=AsyncMock(return_value={
                "ranking_version": "evidence-v4", "candidates": eligible,
            }),
        ), patch.object(
            importer.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            importer, "_commit_selected",
            new=AsyncMock(side_effect=importer.ControlledImportError("third candidate failed")),
        ), patch.object(
            importer, "audit_controlled_import_progress", new=AsyncMock(return_value=durable),
        ) as audit:
            result = asyncio.run(importer.run_continuation_import(
                campaign_id=importer.DAVE_CAMPAIGN_ID, offset=17,
                sample_limit=20, import_limit=3, commit=True,
                source_job_id="launchruntime-current-job",
            ))

        self.assertTrue(result["partial_failure"])
        self.assertEqual(1, result["participation_writes"])
        self.assertEqual(1, result["imported"])
        self.assertEqual(1, result["incomplete_controlled_imports"])
        self.assertEqual(17, result["next_offset"])
        self.assertEqual(["third candidate failed"], result["errors"])
        audit.assert_awaited_once_with(importer.DAVE_CAMPAIGN_ID)

    def test_durable_audit_reads_participant_source_job_from_fact_record(self):
        campaign_id = importer.DAVE_CAMPAIGN_ID
        source_job_id = "launchruntime-current-job"
        author_key = "youtube|handle:nextcreator"
        master = {
            "record_id": "kol1",
            "fields": {
                "迁移备注": (
                    importer.controlled_marker(campaign_id, author_key)
                    + f"; source_job={source_job_id}"
                ),
            },
        }
        participant = {
            "record_id": "participant1",
            "fields": {
                "参与记录ID": "unique1", "审核结论": "待审核",
                "关联KOL": ["kol1"], "关联邮件草稿": [],
                "竞品证据摘要": (
                    f"NYXI公开帖子作者；source_job={source_job_id}；证据帖子=1条"
                ),
            },
        }

        async def fetch_records(table_id, **_kwargs):
            if table_id == importer.config.T_LAUNCH_PARTICIPANT:
                return [participant]
            if table_id == importer.config.T_DRAFT:
                return []
            raise AssertionError(table_id)

        with patch.object(
            importer.launch_evidence, "get_activity",
            new=AsyncMock(return_value={"fields": {"产品主记录ID": "product1"}}),
        ), patch.object(
            importer.preview, "_load_evidence_identity_contacts",
            new=AsyncMock(return_value=([master], [])),
        ), patch.object(
            importer.feishu, "fetch_all_records", new=AsyncMock(side_effect=fetch_records),
        ), patch.object(importer, "participant_key", return_value="unique1"):
            result = asyncio.run(importer.audit_controlled_import_progress(campaign_id))

        self.assertEqual(1, result["participation_records"])
        self.assertEqual(source_job_id, result["results"][0]["source_job_id"])
        self.assertEqual(
            [source_job_id], result["results"][0]["participant_source_job_ids"],
        )


if __name__ == "__main__":
    unittest.main()
