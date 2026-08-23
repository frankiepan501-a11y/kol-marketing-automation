import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from app import main


class FakeRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


class LaunchRouteTests(unittest.TestCase):
    def test_keyword_pilot_defaults_to_read_only_and_caps_preview_at_four(self):
        result = {"read_only": True, "writes": 0, "would_create": 4}
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.keyword_supply, "run_campaign_pilot", new=AsyncMock(return_value=result),
        ) as run:
            response = asyncio.run(main.launch_keyword_supply_pilot(
                FakeRequest({"campaign_id": "c1", "max_tasks": 99}),
                authorization="Bearer secret",
            ))

        self.assertTrue(response["read_only"])
        self.assertEqual(0, response["writes"])
        run.assert_awaited_once_with(
            campaign_id="c1", required_candidates=200, max_tasks=4, dry_run=True,
            pilot_version="v1",
        )

    def test_keyword_pilot_commit_requires_exact_confirmation(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            main.keyword_supply, "run_campaign_pilot", new=AsyncMock(),
        ) as run:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_keyword_supply_pilot(
                    FakeRequest({"campaign_id": "c1", "dry_run": False}),
                    authorization="Bearer secret",
                ))

        self.assertEqual(400, ctx.exception.status_code)
        run.assert_not_awaited()

    def test_keyword_pilot_commit_creates_only_discovery_tasks(self):
        result = {"read_only": False, "writes": 4, "created": 4, "emails_sent": 0}
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            main.keyword_supply, "run_campaign_pilot", new=AsyncMock(return_value=result),
        ) as run:
            response = asyncio.run(main.launch_keyword_supply_pilot(
                FakeRequest({
                    "campaign_id": "c1", "dry_run": False,
                    "confirm": "CREATE_MAX_4_DISCOVERY_TASKS", "required_candidates": 500,
                }),
                authorization="Bearer secret",
            ))

        self.assertEqual(4, response["writes"])
        self.assertEqual(0, response["emails_sent"])
        run.assert_awaited_once_with(
            campaign_id="c1", required_candidates=500, max_tasks=4, dry_run=False,
            pilot_version="v1",
        )

    def test_keyword_pilot_replay_is_scoped_and_read_only(self):
        result = {
            "read_only": True, "writes": 0, "drafts_created": 0,
            "emails_sent": 0, "candidates": [{"contact_id": "kol1"}],
        }
        request = FakeRequest({
            "campaign_id": main.keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
            "contact_ids": ["kol1"],
        })
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.launch_candidate_preview, "replay_candidates_targeted",
            new=AsyncMock(return_value=result),
        ) as replay:
            response = asyncio.run(main.launch_keyword_supply_pilot_replay(
                request, authorization="Bearer secret",
            ))

        self.assertTrue(response["read_only"])
        self.assertEqual(0, response["writes"])
        replay.assert_awaited_once_with(
            campaign_id=main.keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
            contact_ids=["kol1"], object_type="KOL",
        )

    def test_keyword_pilot_replay_rejects_other_campaigns(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_keyword_supply_pilot_replay(
                    FakeRequest({"campaign_id": "other", "contact_ids": ["kol1"]}),
                    authorization="Bearer secret",
                ))
        self.assertEqual(422, ctx.exception.status_code)

    def test_evidence_author_pilot_runs_in_background_without_write_switch(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            result = {
                "read_only": True, "writes": 0, "drafts_created": 0,
                "emails_sent": 0, "sample_size": 20,
            }
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", False,
            ), patch.object(
                main.launch_candidate_preview, "preview_unmatched_evidence_authors",
                new=AsyncMock(return_value=result),
            ) as preview_authors:
                accepted = await main.launch_evidence_author_pilot(
                    FakeRequest({
                        "campaign_id": main.keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
                        "limit": 99,
                    }),
                    authorization="Bearer secret",
                )
                await asyncio.sleep(0)
                status = await main.get_launch_runtime_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._launch_runtime_jobs.clear()
            return accepted, status, preview_authors

        accepted, status, preview_authors = asyncio.run(exercise())

        self.assertTrue(accepted["accepted"])
        self.assertEqual("success", status["status"])
        self.assertEqual(0, status["result"]["writes"])
        preview_authors.assert_awaited_once_with(
            campaign_id=main.keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
            limit=20,
        )

    def test_evidence_author_enrichment_runs_in_background_without_write_switch(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            result = {
                "read_only": True, "writes": 0, "drafts_created": 0,
                "emails_sent": 0, "summary": {"sample_size": 20},
            }
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", False,
            ), patch.object(
                main.launch_candidate_preview, "enrich_unmatched_evidence_authors",
                new=AsyncMock(return_value=result),
            ) as enrich:
                accepted = await main.launch_evidence_author_enrichment(
                    FakeRequest({
                        "campaign_id": main.keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
                        "limit": 99,
                    }),
                    authorization="Bearer secret",
                )
                await asyncio.sleep(0)
                status = await main.get_launch_runtime_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._launch_runtime_jobs.clear()
            return status, enrich

        status, enrich = asyncio.run(exercise())

        self.assertEqual("success", status["status"])
        self.assertEqual(0, status["result"]["writes"])
        enrich.assert_awaited_once_with(
            campaign_id=main.keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
            limit=20,
        )

    def test_outcome_reconcile_defaults_to_dry_run(self):
        result = {"campaign_id": "c1", "dry_run": True, "updates_planned": 1}
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.launch_outcomes, "reconcile_campaign", new=AsyncMock(return_value=result),
        ) as reconcile:
            response = asyncio.run(main.launch_outcomes_reconcile(
                FakeRequest({"campaign_id": "c1"}), authorization="Bearer secret",
            ))

        self.assertTrue(response["ok"])
        self.assertTrue(response["dry_run"])
        reconcile.assert_awaited_once_with("c1", dry_run=True)

    def test_outcome_reconcile_commit_requires_activity_queue_switch(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", False,
        ), patch.object(
            main.launch_outcomes, "reconcile_campaign", new=AsyncMock(),
        ) as reconcile:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_outcomes_reconcile(
                    FakeRequest({"campaign_id": "c1", "dry_run": False}),
                    authorization="Bearer secret",
                ))

        self.assertEqual(403, ctx.exception.status_code)
        reconcile.assert_not_awaited()

    def test_autonomous_job_persists_actual_completion_and_survives_memory_miss(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            persisted = {"status": "success", "campaign_id": "c1", "mode": "autonomous",
                         "result": {"inventory_after": 40}}
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
            ), patch.object(
                main.launch_runtime, "load_runtime_job", new=AsyncMock(return_value=persisted),
            ), patch.object(
                main.launch_runtime, "persist_runtime_job", new=AsyncMock(),
            ) as persist, patch.object(
                main.launch_runtime, "autonomous_refill",
                new=AsyncMock(return_value={"inventory_after": 40}),
            ):
                accepted = await main.launch_runtime_autonomous_refill(
                    FakeRequest({"campaign_id": "c1"}), authorization="Bearer secret",
                )
                await asyncio.sleep(0)
                main._launch_runtime_jobs.clear()
                status = await main.get_launch_runtime_job(
                    accepted["job_id"], campaign_id="c1", authorization="Bearer secret",
                )
            return accepted, status, persist

        accepted, status, persist = asyncio.run(exercise())

        self.assertTrue(accepted["accepted"])
        self.assertEqual("success", status["status"])
        states = [call.kwargs["status"] for call in persist.await_args_list]
        self.assertEqual(["running", "success"], states)

    def test_latest_autonomous_job_status_reads_campaign_durable_state(self):
        persisted = {
            "job_id": "launchruntime-real", "status": "success",
            "campaign_id": "c1", "mode": "autonomous", "started_ts": 123,
        }
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
            main.launch_runtime, "load_runtime_job", new=AsyncMock(return_value=persisted),
        ) as load:
            status = asyncio.run(main.get_launch_runtime_job(
                "latest", campaign_id="c1", authorization="Bearer secret",
            ))

        self.assertEqual("launchruntime-real", status["job_id"])
        self.assertEqual("success", status["status"])
        load.assert_awaited_once_with("c1")

    def test_autonomous_job_restarts_when_durable_running_job_is_not_in_this_process(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            durable = {
                "job_id": "launchruntime-interrupted", "status": "running",
                "campaign_id": "c1", "mode": "autonomous",
                "updated_ts": 9_999_999_999,
            }
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
            ), patch.object(
                main.launch_runtime, "load_runtime_job", new=AsyncMock(return_value=durable),
            ), patch.object(
                main.launch_runtime, "persist_runtime_job", new=AsyncMock(),
            ), patch.object(
                main.launch_runtime, "autonomous_refill",
                new=AsyncMock(return_value={"business_outcome": "supply_in_progress"}),
            ) as refill:
                accepted = await main.launch_runtime_autonomous_refill(
                    FakeRequest({"campaign_id": "c1"}), authorization="Bearer secret",
                )
                await asyncio.sleep(0)
            main._launch_runtime_jobs.clear()
            return accepted, refill

        accepted, refill = asyncio.run(exercise())

        self.assertFalse(accepted["already_running"])
        self.assertNotEqual("launchruntime-interrupted", accepted["job_id"])
        refill.assert_awaited_once_with(campaign_id="c1")

    def test_autonomous_job_persists_degraded_when_supply_is_blocked(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            blocked = {
                "action": "expand", "business_outcome": "supply_blocked",
                "made_supply_progress": False,
                "supply_progress_breakdown": {}, "inventory_after": 0,
                "quota": {"remaining": 107},
            }
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
            ), patch.object(
                main.launch_runtime, "load_runtime_job", new=AsyncMock(return_value=None),
            ), patch.object(
                main.launch_runtime, "persist_runtime_job", new=AsyncMock(),
            ) as persist, patch.object(
                main.launch_runtime, "autonomous_refill", new=AsyncMock(return_value=blocked),
            ):
                accepted = await main.launch_runtime_autonomous_refill(
                    FakeRequest({"campaign_id": "c1"}), authorization="Bearer secret",
                )
                await asyncio.sleep(0)
                status = await main.get_launch_runtime_job(
                    accepted["job_id"], campaign_id="c1", authorization="Bearer secret",
                )
            main._launch_runtime_jobs.clear()
            return status, persist

        status, persist = asyncio.run(exercise())

        self.assertEqual("degraded", status["status"])
        states = [call.kwargs["status"] for call in persist.await_args_list]
        self.assertEqual(["running", "degraded"], states)

    def test_profile_backfill_defaults_to_background_dry_run(self):
        async def exercise():
            main._relabel_profile_jobs.clear()
            result = {"dry_run": True, "processed": 1, "writes": 0}
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.relabel, "run_profile_records", new=AsyncMock(return_value=result),
            ) as run:
                accepted = await main.start_relabel_kol_profiles(
                    FakeRequest({"record_ids": ["kol1"]}), authorization="Bearer secret",
                )
                await asyncio.sleep(0)
                status = await main.get_relabel_kol_profile_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._relabel_profile_jobs.clear()
            return accepted, status, run

        accepted, status, run = asyncio.run(exercise())

        self.assertTrue(accepted["dry_run"])
        self.assertEqual("success", status["status"])
        self.assertEqual(0, status["result"]["writes"])
        run.assert_awaited_once_with(["kol1"], dry_run=True, limit=1)

    def test_profile_backfill_commit_requires_explicit_confirmation(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.start_relabel_kol_profiles(
                    FakeRequest({"record_ids": ["kol1"], "dry_run": False}),
                    authorization="Bearer secret",
                ))
        self.assertEqual(400, ctx.exception.status_code)

    def test_profile_backfill_rejects_string_boolean(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.start_relabel_kol_profiles(
                    FakeRequest({"record_ids": ["kol1"], "dry_run": "false"}),
                    authorization="Bearer secret",
                ))
        self.assertEqual(400, ctx.exception.status_code)

    def test_review_pool_job_never_calls_queue(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
            ), patch.object(
                main.launch_runtime, "append_review_candidates",
                new=AsyncMock(return_value={"created": 3, "drafts_created": 0, "emails_sent": 0}),
            ) as append_review, patch.object(
                main.launch_runtime, "queue_approved", new=AsyncMock(),
            ) as queue:
                accepted = await main.launch_runtime_review_pool(
                    FakeRequest({"campaign_id": "c1", "review_target": 20}),
                    authorization="Bearer secret",
                )
                await asyncio.sleep(0)
                status = await main.get_launch_runtime_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._launch_runtime_jobs.clear()
            return status, append_review, queue

        status, append_review, queue = asyncio.run(exercise())

        self.assertEqual("success", status["status"])
        self.assertEqual(0, status["result"]["emails_sent"])
        append_review.assert_awaited_once_with(campaign_id="c1", review_target=20)
        queue.assert_not_awaited()

    def test_write_route_requires_internal_auth(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(main.launch_evidence_start(FakeRequest({}), authorization=""))
        self.assertEqual(ctx.exception.status_code, 401)

    def test_evidence_switch_defaults_to_denied(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
             patch.object(main.config, "LAUNCH_EVIDENCE_ENABLED", False), \
             patch.object(main.launch_evidence, "start_analysis", new=AsyncMock()) as start:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_evidence_start(
                    FakeRequest({"campaign_id": "c1", "expected_config_version": 1}),
                    authorization="Bearer secret",
                ))
        self.assertEqual(ctx.exception.status_code, 403)
        start.assert_not_awaited()

    def test_not_found_conflict_and_validation_have_stable_status_codes(self):
        cases = [
            (main.launch_evidence.EvidenceNotFoundError("missing"), 404),
            (main.launch_evidence.EvidenceVersionConflict("old"), 409),
            (main.launch_evidence.EvidenceValidationError("bad state"), 422),
        ]
        for error, expected in cases:
            with self.subTest(expected=expected), \
                 patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
                 patch.object(main.config, "LAUNCH_EVIDENCE_ENABLED", True), \
                 patch.object(main.launch_evidence, "start_analysis", new=AsyncMock(side_effect=error)):
                with self.assertRaises(HTTPException) as ctx:
                    asyncio.run(main.launch_evidence_start(
                        FakeRequest({"campaign_id": "c1", "expected_config_version": 1}),
                        authorization="Bearer secret",
                    ))
                self.assertEqual(ctx.exception.status_code, expected)

    def test_preview_accepts_campaign_without_product_and_stays_read_only(self):
        result = {"read_only": True, "writes": 0, "campaign_id": "c1"}
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
             patch.object(main.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=result)) as preview:
            response = asyncio.run(main.launch_candidates_preview(
                authorization="Bearer secret", product_id="", campaign_id="c1",
                object_type="KOL", limit=20, async_mode=False,
            ))
        self.assertTrue(response["read_only"])
        self.assertEqual(response["writes"], 0)
        preview.assert_awaited_once_with("", object_type="KOL", limit=20, campaign_id="c1")

    def test_preview_defaults_to_background_job_and_exposes_result(self):
        async def exercise():
            main._launch_preview_jobs.clear()
            result = {"read_only": True, "writes": 0, "campaign_id": "c1"}
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
                 patch.object(main.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=result)):
                accepted = await main.launch_candidates_preview(
                    authorization="Bearer secret", campaign_id="c1",
                    object_type="KOL", limit=20,
                )
                await asyncio.sleep(0)
                status = await main.get_launch_preview_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._launch_preview_jobs.clear()
            return accepted, status

        accepted, status = asyncio.run(exercise())

        self.assertTrue(accepted["accepted"])
        self.assertFalse(accepted["already_running"])
        self.assertEqual(status["status"], "success")
        self.assertTrue(status["result"]["read_only"])
        self.assertEqual(status["result"]["writes"], 0)

    def test_preview_job_status_requires_known_job(self):
        main._launch_preview_jobs.clear()
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.get_launch_preview_job(
                    "launchpreview-missing", authorization="Bearer secret",
                ))
        self.assertEqual(ctx.exception.status_code, 404)

    def test_duplicate_preview_click_reuses_running_job(self):
        async def exercise():
            main._launch_preview_jobs.clear()
            gate = asyncio.Event()

            async def slow_preview(*args, **kwargs):
                await gate.wait()
                return {"read_only": True, "writes": 0, "campaign_id": "c1"}

            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
                 patch.object(main.launch_candidate_preview, "preview_candidates", new=AsyncMock(side_effect=slow_preview)) as preview:
                first = await main.launch_candidates_preview(
                    authorization="Bearer secret", campaign_id="c1", limit=20,
                )
                second = await main.launch_candidates_preview(
                    authorization="Bearer secret", campaign_id="c1", limit=20,
                )
                gate.set()
                await asyncio.sleep(0)
            main._launch_preview_jobs.clear()
            return first, second, preview.await_count

        first, second, await_count = asyncio.run(exercise())

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertTrue(second["already_running"])
        self.assertEqual(await_count, 1)

    def test_participant_switch_denies_before_any_write(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
             patch.object(main.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", False), \
             patch.object(main.launch_participation, "lock_participants", new=AsyncMock()) as lock:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_participants_lock(
                    FakeRequest({"campaign_id": "c1"}), authorization="Bearer secret",
                ))
        self.assertEqual(ctx.exception.status_code, 403)
        lock.assert_not_awaited()

    def test_auto_send_async_mode_returns_job_and_exposes_result(self):
        async def exercise():
            main._auto_send_jobs.clear()
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
                 patch.object(main.auto_send, "run", new=AsyncMock(return_value={"sent": 2, "fail": 0})):
                accepted = await main.run_auto_send(
                    authorization="Bearer secret", async_mode=True,
                )
                await asyncio.sleep(0)
                status = await main.get_auto_send_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._auto_send_jobs.clear()
            return accepted, status

        accepted, status = asyncio.run(exercise())

        self.assertTrue(accepted["accepted"])
        self.assertFalse(accepted["already_running"])
        self.assertEqual(status["status"], "success")
        self.assertEqual(status["result"]["sent"], 2)

    def test_auto_send_async_mode_reuses_running_job(self):
        async def exercise():
            main._auto_send_jobs.clear()
            gate = asyncio.Event()

            async def slow_send():
                await gate.wait()
                return {"sent": 0, "fail": 0}

            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
                 patch.object(main.auto_send, "run", new=AsyncMock(side_effect=slow_send)) as run:
                first = await main.run_auto_send(
                    authorization="Bearer secret", async_mode=True,
                )
                second = await main.run_auto_send(
                    authorization="Bearer secret", async_mode=True,
                )
                gate.set()
                await asyncio.sleep(0)
            main._auto_send_jobs.clear()
            return first, second, run.await_count

        first, second, await_count = asyncio.run(exercise())

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertTrue(second["already_running"])
        self.assertEqual(await_count, 1)

    def test_launch_daily_report_defaults_to_background_and_exposes_result(self):
        async def exercise():
            main._launch_daily_report_jobs.clear()
            result = {"ok": True, "campaigns": 2, "notified": False}
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch(
                "app.launch_daily_report.active_campaign_ids",
                new=AsyncMock(return_value=("c1", "c2")),
            ), patch(
                "app.launch_daily_report.run", new=AsyncMock(return_value=result),
            ):
                accepted = await main.run_launch_daily_report(
                    authorization="Bearer secret", day="2026-08-21",
                )
                await asyncio.sleep(0)
                status = await main.get_launch_daily_report_job(
                    accepted["job_id"], authorization="Bearer secret",
                )
            main._launch_daily_report_jobs.clear()
            return accepted, status

        accepted, status = asyncio.run(exercise())

        self.assertTrue(accepted["accepted"])
        self.assertFalse(accepted["already_running"])
        self.assertEqual("success", status["status"])
        self.assertEqual(2, status["result"]["campaigns"])

    def test_launch_daily_report_reuses_running_job_for_same_request(self):
        async def exercise():
            main._launch_daily_report_jobs.clear()
            gate = asyncio.Event()

            async def slow_report(**kwargs):
                await gate.wait()
                return {"ok": True, "campaigns": 2, "notified": kwargs["notify"]}

            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch(
                "app.launch_daily_report.active_campaign_ids",
                new=AsyncMock(return_value=("c1", "c2")),
            ), patch(
                "app.launch_daily_report.run", new=AsyncMock(side_effect=slow_report),
            ) as run:
                first = await main.run_launch_daily_report(
                    authorization="Bearer secret", day="2026-08-21", notify=True,
                )
                second = await main.run_launch_daily_report(
                    authorization="Bearer secret", day="2026-08-21", notify=True,
                )
                gate.set()
                await asyncio.sleep(0)
            main._launch_daily_report_jobs.clear()
            return first, second, run.await_count

        first, second, await_count = asyncio.run(exercise())

        self.assertEqual(first["job_id"], second["job_id"])
        self.assertTrue(second["already_running"])
        self.assertEqual(1, await_count)

    def test_launch_daily_report_new_activity_set_creates_new_job(self):
        async def exercise():
            main._launch_daily_report_jobs.clear()
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch(
                "app.launch_daily_report.active_campaign_ids",
                new=AsyncMock(side_effect=[("c1",), ("c1", "c2")]),
            ), patch(
                "app.launch_daily_report.run",
                new=AsyncMock(return_value={"ok": True, "campaigns": 2}),
            ):
                first = await main.run_launch_daily_report(
                    authorization="Bearer secret", day="2026-08-21",
                )
                await asyncio.sleep(0)
                second = await main.run_launch_daily_report(
                    authorization="Bearer secret", day="2026-08-21",
                )
                await asyncio.sleep(0)
            main._launch_daily_report_jobs.clear()
            return first, second

        first, second = asyncio.run(exercise())

        self.assertNotEqual(first["job_id"], second["job_id"])
        self.assertFalse(second["reused"])

    def test_launch_daily_report_job_status_requires_known_job(self):
        main._launch_daily_report_jobs.clear()
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.get_launch_daily_report_job(
                    "launchreport-missing", authorization="Bearer secret",
                ))
        self.assertEqual(404, ctx.exception.status_code)


if __name__ == "__main__":
    unittest.main()
