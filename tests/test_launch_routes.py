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
    def test_autonomous_job_persists_actual_completion_and_survives_memory_miss(self):
        async def exercise():
            main._launch_runtime_jobs.clear()
            persisted = {"status": "success", "campaign_id": "c1", "mode": "autonomous",
                         "result": {"inventory_after": 40}}
            with patch.object(main.config, "INTERNAL_TOKEN", "secret"), patch.object(
                main.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
            ), patch.object(
                main.launch_runtime, "load_runtime_job", new=AsyncMock(side_effect=[None, persisted]),
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


if __name__ == "__main__":
    unittest.main()
