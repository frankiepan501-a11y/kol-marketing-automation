import asyncio
import unittest

from app import main


class DraftRegenAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main.config.INTERNAL_TOKEN = "test-token"
        main._draft_regen_jobs.clear()
        self._orig_regen = main.draft_regen.regen_draft

    async def asyncTearDown(self):
        main.draft_regen.regen_draft = self._orig_regen
        main._draft_regen_jobs.clear()

    async def test_default_async_returns_job_and_finishes_in_background(self):
        async def fake_regen(record_id, feedback=""):
            await asyncio.sleep(0)
            return {"ok": True, "old_rid": record_id, "new_rid": "rec_new", "retries": 1}

        main.draft_regen.regen_draft = fake_regen

        resp = await main.run_draft_regen(
            record_id="rec_old",
            feedback="make it warmer",
            authorization="Bearer test-token",
        )

        self.assertTrue(resp["accepted"])
        self.assertEqual("rec_old", resp["record_id"])
        job_id = resp["job_id"]

        for _ in range(20):
            if main._draft_regen_jobs[job_id]["status"] != "running":
                break
            await asyncio.sleep(0.01)

        job = main._draft_regen_jobs[job_id]
        self.assertEqual("success", job["status"])
        self.assertEqual("rec_new", job["result"]["new_rid"])

    async def test_duplicate_async_click_reuses_running_job(self):
        release = asyncio.Event()

        async def slow_regen(record_id, feedback=""):
            await release.wait()
            return {"ok": True, "old_rid": record_id, "new_rid": "rec_new", "retries": 1}

        main.draft_regen.regen_draft = slow_regen

        first = await main.run_draft_regen(
            record_id="rec_old",
            feedback="first",
            authorization="Bearer test-token",
        )
        second = await main.run_draft_regen(
            record_id="rec_old",
            feedback="second",
            authorization="Bearer test-token",
        )

        self.assertFalse(first["already_running"])
        self.assertTrue(second["already_running"])
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(1, len(main._draft_regen_jobs))

        release.set()
        for _ in range(20):
            if main._draft_regen_jobs[first["job_id"]]["status"] != "running":
                break
            await asyncio.sleep(0.01)
