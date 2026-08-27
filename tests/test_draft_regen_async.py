import asyncio
import unittest

from app import main


class DraftRegenAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main.config.INTERNAL_TOKEN = "test-token"
        self._orig_kol_key = main.config.KOL_DEEPSEEK_API_KEY
        main.config.KOL_DEEPSEEK_API_KEY = "test-key"
        main._draft_regen_jobs.clear()
        self._orig_regen = main.draft_regen.regen_draft
        self._orig_card_update = main.feishu.update_card_message_with_app
        self._orig_card_send = main.feishu.send_card_via_app3
        self._orig_alert = main._alert_endpoint_failure

    async def test_health_reports_kol_ai_unconfigured(self):
        original = main.config.KOL_DEEPSEEK_API_KEY
        try:
            main.config.KOL_DEEPSEEK_API_KEY = ""
            result = await main.health()
        finally:
            main.config.KOL_DEEPSEEK_API_KEY = original

        self.assertEqual("degraded", result["status"])
        self.assertFalse(result["kol_ai_configured"])

    async def test_regen_rejects_before_accepting_when_kol_ai_is_unconfigured(self):
        original = main.config.KOL_DEEPSEEK_API_KEY
        try:
            main.config.KOL_DEEPSEEK_API_KEY = ""
            with self.assertRaises(main.HTTPException) as caught:
                await main.run_draft_regen(
                    record_id="rec_old",
                    feedback="make it warmer",
                    authorization="Bearer test-token",
                )
        finally:
            main.config.KOL_DEEPSEEK_API_KEY = original

        self.assertEqual(503, caught.exception.status_code)
        self.assertEqual(0, len(main._draft_regen_jobs))

    async def asyncTearDown(self):
        main.draft_regen.regen_draft = self._orig_regen
        main.feishu.update_card_message_with_app = self._orig_card_update
        main.feishu.send_card_via_app3 = self._orig_card_send
        main._alert_endpoint_failure = self._orig_alert
        main.config.KOL_DEEPSEEK_API_KEY = self._orig_kol_key
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
        self.assertTrue(second["suppress_reply"])
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(1, len(main._draft_regen_jobs))

        release.set()
        for _ in range(20):
            if main._draft_regen_jobs[first["job_id"]]["status"] != "running":
                break
            await asyncio.sleep(0.01)

    async def test_duplicate_after_completion_reuses_terminal_job(self):
        calls = 0

        async def fake_regen(record_id, feedback=""):
            nonlocal calls
            calls += 1
            return {"ok": True, "old_rid": record_id, "new_rid": "rec_new", "retries": 1}

        main.draft_regen.regen_draft = fake_regen
        first = await main.run_draft_regen(
            record_id="rec_old",
            feedback="make it warmer",
            authorization="Bearer test-token",
        )
        for _ in range(20):
            if main._draft_regen_jobs[first["job_id"]]["status"] != "running":
                break
            await asyncio.sleep(0.01)

        second = await main.run_draft_regen(
            record_id="rec_old",
            feedback="make it warmer",
            authorization="Bearer test-token",
        )

        self.assertEqual(1, calls)
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertTrue(second["already_processed"])
        self.assertTrue(second["suppress_reply"])
        self.assertEqual("success", second["status"])
        self.assertEqual("rec_new", second["result"]["new_rid"])

    async def test_successful_background_regen_patches_original_card_to_success(self):
        updates = []

        async def fake_regen(record_id, feedback=""):
            return {"ok": True, "old_rid": record_id, "new_rid": "rec_new", "retries": 1}

        async def fake_update(message_id, card, *, which):
            updates.append((message_id, card, which))
            return True

        main.draft_regen.regen_draft = fake_regen
        main.feishu.update_card_message_with_app = fake_update

        response = await main.run_draft_regen(
            record_id="rec_old",
            feedback="make it warmer",
            message_id="om_test",
            approver="测试人",
            authorization="Bearer test-token",
        )
        for _ in range(50):
            if "card_updated" in main._draft_regen_jobs[response["job_id"]]:
                break
            await asyncio.sleep(0.01)

        self.assertEqual(1, len(updates))
        message_id, card, which = updates[0]
        self.assertEqual("om_test", message_id)
        self.assertEqual("app3", which)
        self.assertEqual("green", card["header"]["template"])
        self.assertIn("重生完成", card["header"]["title"]["content"])
        self.assertIn("rec_new", card["elements"][0]["text"]["content"])

    async def test_failed_background_regen_patches_original_card_to_failure(self):
        updates = []

        async def fake_regen(record_id, feedback=""):
            return {"ok": False, "error": "deepseek fail: missing KOL_DEEPSEEK_API_KEY"}

        async def fake_update(message_id, card, *, which):
            updates.append((message_id, card, which))
            return True

        main.draft_regen.regen_draft = fake_regen
        main.feishu.update_card_message_with_app = fake_update

        response = await main.run_draft_regen(
            record_id="rec_old",
            feedback="make it warmer",
            message_id="om_test",
            approver="测试人",
            authorization="Bearer test-token",
        )
        for _ in range(50):
            if "card_updated" in main._draft_regen_jobs[response["job_id"]]:
                break
            await asyncio.sleep(0.01)

        self.assertEqual("done_with_issue", main._draft_regen_jobs[response["job_id"]]["status"])
        self.assertEqual(1, len(updates))
        _, card, which = updates[0]
        self.assertEqual("app3", which)
        self.assertEqual("red", card["header"]["template"])
        self.assertIn("重生失败", card["header"]["title"]["content"])
        self.assertIn("KOL AI 配置缺失", card["elements"][0]["text"]["content"])

    async def test_background_exception_patches_original_card_to_failure(self):
        updates = []

        async def fake_regen(record_id, feedback=""):
            raise RuntimeError("provider timeout")

        async def fake_update(message_id, card, *, which):
            updates.append((message_id, card, which))
            return True

        async def fake_alert(*args, **kwargs):
            return None

        main.draft_regen.regen_draft = fake_regen
        main.feishu.update_card_message_with_app = fake_update
        main._alert_endpoint_failure = fake_alert

        response = await main.run_draft_regen(
            record_id="rec_old",
            feedback="",
            message_id="om_test",
            approver="测试人",
            authorization="Bearer test-token",
        )
        for _ in range(50):
            if "card_updated" in main._draft_regen_jobs[response["job_id"]]:
                break
            await asyncio.sleep(0.01)

        self.assertEqual("error", main._draft_regen_jobs[response["job_id"]]["status"])
        self.assertEqual(1, len(updates))
        _, card, which = updates[0]
        self.assertEqual("app3", which)
        self.assertEqual("red", card["header"]["template"])
        self.assertIn("重生失败", card["header"]["title"]["content"])

    async def test_card_patch_failure_retries_then_sends_app3_fallback(self):
        update_calls = []
        fallback_calls = []

        async def fake_regen(record_id, feedback=""):
            return {"ok": True, "old_rid": record_id, "new_rid": "rec_new", "retries": 1}

        async def fake_update(message_id, card, *, which):
            update_calls.append((message_id, which))
            return False

        async def fake_send(receive_type, receive_id, card):
            fallback_calls.append((receive_type, receive_id, card))
            return "om_fallback"

        main.draft_regen.regen_draft = fake_regen
        main.feishu.update_card_message_with_app = fake_update
        main.feishu.send_card_via_app3 = fake_send

        response = await main.run_draft_regen(
            record_id="rec_old",
            feedback="",
            message_id="om_test",
            operator_open_id="ou_test",
            authorization="Bearer test-token",
        )
        for _ in range(50):
            if main._draft_regen_jobs[response["job_id"]].get("fallback_message_id"):
                break
            await asyncio.sleep(0.01)

        job = main._draft_regen_jobs[response["job_id"]]
        self.assertEqual(2, len(update_calls))
        self.assertEqual(1, len(fallback_calls))
        self.assertEqual(("open_id", "ou_test"), fallback_calls[0][:2])
        self.assertFalse(job["card_updated"])
        self.assertEqual("om_fallback", job["fallback_message_id"])
