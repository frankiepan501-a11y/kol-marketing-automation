import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app import enrich
from app.enrich_model_guard import EnrichModelBudget


def _product():
    return {
        "record_id": "prod_1",
        "fields": {
            "产品英文名": "FUNLAB Switch 2 Controller",
            "品牌": "FUNLAB",
            "品类": "controller",
            "卖点1": "hall-effect sticks",
            "卖点2": "programmable back buttons",
            "卖点3": "wireless play",
            "目标人群": "Switch 2 players",
            "官网链接": {"link": "https://example.com/controller", "text": "Product"},
        },
    }


def _kol(record_id="kol_1", country="US"):
    return {
        "record_id": record_id,
        "fields": {
            "账号名": f"Creator {record_id}",
            "国家": country,
            "国家原文": country,
            "粉丝数": 25000,
            "内容风格": ["gaming"],
            "IP喜好": "Switch",
            "邮箱": f"{record_id}@example.com",
            "主链接": "https://youtube.com/example",
        },
    }


class EnrichTemplateModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_regular_english_candidate_uses_safe_template_without_model(self):
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("app.enrich.deepseek.chat_json", new=mock.AsyncMock()) as chat:
            budget = EnrichModelBudget(
                per_task=2, per_run=4, daily=10, failure_threshold=2,
                state_path=Path(tmp) / "budget.json",
            )

            result = await enrich.score_and_draft_one(
                _kol(), _product(), "FUNLAB", "Mia @ FUNLAB Outreach", 0,
                set(), set(), model_budget=budget, task_id="task_1",
            )

        self.assertTrue(result["passed"])
        self.assertEqual("template", result["generation_mode"])
        self.assertEqual(0, result["model_calls"])
        self.assertIn("Creator kol_1", result["subject"])
        self.assertIn("https://example.com/controller?", result["body"])
        self.assertEqual(0, chat.await_count)

    async def test_two_model_failures_open_circuit_and_third_candidate_skips_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=10, per_run=10, daily=10, failure_threshold=2,
                state_path=Path(tmp) / "budget.json",
            )
            chat = mock.AsyncMock(side_effect=RuntimeError("provider unavailable"))
            with mock.patch("app.enrich.deepseek.chat_json", new=chat):
                first = await enrich.score_and_draft_one(
                    _kol("de_1", "DE"), _product(), "FUNLAB", "Mia", 0,
                    set(), set(), model_budget=budget, task_id="task_1",
                )
                second = await enrich.score_and_draft_one(
                    _kol("de_2", "DE"), _product(), "FUNLAB", "Mia", 0,
                    set(), set(), model_budget=budget, task_id="task_1",
                )
                third = await enrich.score_and_draft_one(
                    _kol("de_3", "DE"), _product(), "FUNLAB", "Mia", 0,
                    set(), set(), model_budget=budget, task_id="task_1",
                )

        self.assertFalse(first["passed"])
        self.assertFalse(second["passed"])
        self.assertFalse(third["passed"])
        self.assertEqual(1, first["model_calls"])
        self.assertEqual(1, second["model_calls"])
        self.assertEqual(0, third["model_calls"])
        self.assertEqual("circuit_open", third["model_skip_reason"])
        self.assertEqual(2, chat.await_count)

    async def test_two_invalid_model_outputs_also_open_circuit(self):
        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=10, per_run=10, daily=10, failure_threshold=2,
                state_path=Path(tmp) / "budget.json",
            )
            chat = mock.AsyncMock(return_value={})
            with mock.patch("app.enrich.deepseek.chat_json", new=chat):
                first = await enrich.score_and_draft_one(
                    _kol("de_1", "DE"), _product(), "FUNLAB", "Mia", 0,
                    set(), set(), model_budget=budget, task_id="task_1",
                )
                second = await enrich.score_and_draft_one(
                    _kol("de_2", "DE"), _product(), "FUNLAB", "Mia", 0,
                    set(), set(), model_budget=budget, task_id="task_1",
                )
                third = await enrich.score_and_draft_one(
                    _kol("de_3", "DE"), _product(), "FUNLAB", "Mia", 0,
                    set(), set(), model_budget=budget, task_id="task_1",
                )

        self.assertFalse(first["passed"])
        self.assertFalse(second["passed"])
        self.assertFalse(third["passed"])
        self.assertEqual("circuit_open", third["model_skip_reason"])
        self.assertEqual(2, chat.await_count)

    async def test_template_rejects_current_brand_model_sku_format(self):
        product = _product()
        product["fields"]["产品英文名"] = "FUNLAB Switch 2 Controller FF01A-07"
        result = await enrich.score_and_draft_one(
            _kol(), product, "FUNLAB", "Mia", 0, set(), set(),
        )

        self.assertFalse(result["passed"])
        self.assertIn("internal_sku", result["error"])


class EnrichModelBudgetTests(unittest.TestCase):
    def test_task_run_daily_budgets_and_failure_circuit_are_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=2, per_run=3, daily=4, failure_threshold=2,
                state_path=Path(tmp) / "budget.json",
            )
            self.assertEqual((True, "ok"), budget.reserve("task_a"))
            budget.record_success()
            self.assertEqual((True, "ok"), budget.reserve("task_a"))
            budget.record_failure()
            self.assertEqual((False, "task_budget_exhausted"), budget.reserve("task_a"))
            self.assertEqual((True, "ok"), budget.reserve("task_b"))
            budget.record_failure()
            self.assertTrue(budget.circuit_open)
            self.assertEqual((False, "circuit_open"), budget.reserve("task_c"))
            self.assertEqual(3, budget.snapshot()["run_calls"])

    def test_run_and_daily_limits_survive_new_budget_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "budget.json"
            first = EnrichModelBudget(
                per_task=10, per_run=1, daily=2, failure_threshold=5,
                state_path=state,
            )
            self.assertEqual((True, "ok"), first.reserve("task_a"))
            first.record_success()
            self.assertEqual((False, "run_budget_exhausted"), first.reserve("task_b"))

            second = EnrichModelBudget(
                per_task=10, per_run=5, daily=2, failure_threshold=5,
                state_path=state,
            )
            self.assertEqual((True, "ok"), second.reserve("task_c"))
            second.record_success()

            third = EnrichModelBudget(
                per_task=10, per_run=5, daily=2, failure_threshold=5,
                state_path=state,
            )
            self.assertEqual((False, "daily_budget_exhausted"), third.reserve("task_d"))


class EnrichTemplateRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_safe_template_is_deterministically_approved_without_ai_reviewer(self):
        passed = [{
            "passed": True,
            "generation_mode": "template",
            "template_version": "kol-cold-template-v1",
            "template_validation": {"passed": True, "reasons": []},
            "kol_record_id": "kol_1",
            "kol_name": "Creator",
            "kol_email": "creator@example.com",
            "kol_country": "US",
            "lang": "en",
            "total": 95,
            "breakdown": {},
            "subject": "Creator, a controller for your setup",
            "body": "<p>Hey Creator,</p><p>Safe fixed template.</p>",
            "highlights": "Template match",
            "angle": "Product fit",
            "utm_url": "https://example.com/controller?utm_source=kol",
            "utm_id": "creator-kol-1",
        }]
        with mock.patch("app.enrich.feishu.search_records", new=mock.AsyncMock(return_value=[])), \
             mock.patch("app.enrich.feishu.create_record", new=mock.AsyncMock(return_value="draft_1")) as create, \
             mock.patch("app.enrich.feishu.get_record", new=mock.AsyncMock(return_value={"fields": {}})), \
             mock.patch("app.enrich.feishu.update_record", new=mock.AsyncMock()) as update, \
             mock.patch("app.enrich._next_send_time", return_value=(1234567890, "test-window")), \
             mock.patch("app.enrich.draft_router.route_draft", new=mock.AsyncMock()) as route:
            result = await enrich.write_drafts_and_route(
                "task_1", "product_1", "FUNLAB", "partner@example.com", "Mia", passed,
            )

        self.assertEqual("自动通过", result[0]["path"])
        self.assertEqual(0, route.await_count)
        self.assertEqual("自动通过", create.await_args.args[1]["审核路径"])
        self.assertEqual("自动通过", create.await_args.args[1]["邮件草稿状态"])
        self.assertTrue(any(
            call.args[:2] == (enrich.config.T_KOL, "kol_1")
            and call.args[2].get("UTM ID") == "creator-kol-1"
            for call in update.await_args_list
        ))

    async def test_ai_exception_goes_to_human_review_without_second_model_reviewer(self):
        passed = [{
            "passed": True, "generation_mode": "ai",
            "kol_record_id": "kol_2", "kol_name": "Creator Two",
            "kol_email": "creator2@example.com", "kol_country": "DE", "lang": "de",
            "total": 98, "breakdown": {}, "subject": "Creator Two, neues Produkt",
            "body": "<p>Hallo Creator Two,</p><p>Geprüfter Entwurf.</p>",
            "highlights": "Localized exception", "angle": "Localization",
            "utm_url": "https://example.com/controller?utm_source=kol", "utm_id": "",
        }]
        with mock.patch("app.enrich.feishu.search_records", new=mock.AsyncMock(return_value=[])), \
             mock.patch("app.enrich.feishu.create_record", new=mock.AsyncMock(return_value="draft_2")) as create, \
             mock.patch("app.enrich.feishu.update_record", new=mock.AsyncMock()) as update, \
             mock.patch("app.enrich._next_send_time", return_value=(1234567890, "test-window")), \
             mock.patch("app.enrich.draft_router.route_draft", new=mock.AsyncMock()) as route:
            result = await enrich.write_drafts_and_route(
                "task_2", "product_1", "FUNLAB", "partner@example.com", "Mia", passed,
            )

        self.assertEqual("待人审", result[0]["path"])
        self.assertEqual(0, route.await_count)
        self.assertEqual("待人审", create.await_args.args[1]["审核路径"])
        self.assertEqual("待审", create.await_args.args[1]["邮件草稿状态"])


if __name__ == "__main__":
    unittest.main()
