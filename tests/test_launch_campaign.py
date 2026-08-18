import unittest
from datetime import date

from app.launch_campaign import (
    CampaignSpec,
    build_campaign_plan,
    build_portfolio_plan,
)
from scripts.launch_campaign_shadow import build_pilot


class LaunchCampaignPlanTests(unittest.TestCase):
    def test_pilot_budget_and_break_even_are_reproducible(self):
        plan = build_pilot(date(2026, 8, 19))
        self.assertEqual(50_000, plan["budget"]["portfolio_ceiling_cny"])
        self.assertEqual(23_000, plan["budget"]["reserve_budget_cny"])
        self.assertFalse(plan["budget"]["reserve_auto_allocated"])

        powkong, dave = plan["products"]
        self.assertEqual(81, powkong["economics"]["break_even_orders"])
        self.assertEqual(385_715, powkong["economics"]["break_even_exposure"])
        self.assertEqual(85, dave["economics"]["break_even_orders"])
        self.assertEqual(404_762, dave["economics"]["break_even_exposure"])

    def test_pilot_uses_commitment_buffer_and_scenario_range(self):
        plan = build_pilot(date(2026, 8, 19))
        for product in plan["products"]:
            scope = product["capacity"]["target_scope"]
            self.assertEqual(20, scope["kol_posts"])
            self.assertIsNone(scope["media_reports"])
            self.assertFalse(scope["media_reports_count_toward_kol_target"])
            self.assertEqual(29, product["capacity"]["required_commitments"])
            self.assertEqual(29, product["capacity"]["planned_commitments"])
            self.assertEqual(16, product["capacity"]["paid_warm_candidates_required"])
            scenarios = product["capacity"]["funnel_scenarios"]
            self.assertEqual(["保守", "中性", "进取"], [x["scenario"] for x in scenarios])
            self.assertEqual(700, scenarios[0]["cold_emails_required"])
            self.assertEqual(427, scenarios[1]["cold_emails_required"])
            self.assertEqual(280, scenarios[2]["cold_emails_required"])
            self.assertEqual(950, scenarios[0]["total_candidate_contacts_required"])
            self.assertEqual(550, scenarios[1]["total_candidate_contacts_required"])
            self.assertEqual(346, scenarios[2]["total_candidate_contacts_required"])

    def test_shadow_plan_cannot_authorize_external_actions(self):
        plan = build_pilot(date(2026, 8, 19))
        for product in plan["products"]:
            gates = product["execution_gates"]
            self.assertEqual("shadow", gates["mode"])
            self.assertFalse(gates["email_send"])
            self.assertFalse(gates["paid_commit"])
            self.assertFalse(gates["sample_ship"])
            self.assertFalse(gates["operator_bulk_card"])
            self.assertFalse(gates["reserve_release"])
            self.assertTrue(gates["live_release_requires_human_approval"])
            budget = product["budget_control"]
            self.assertEqual(0, budget["available_without_approval_cny"])
            self.assertTrue(all(not gate["eligible"] for gate in budget["release_gates"]))

    def test_current_pilot_is_flagged_fast_track(self):
        plan = build_pilot(date(2026, 8, 19))
        for product in plan["products"]:
            self.assertEqual("2026-08-04", product["timing"]["ideal_start_date"])
            self.assertEqual(27, product["timing"]["days_to_launch"])
            self.assertTrue(product["timing"]["fast_track"])
            self.assertEqual("P0-时间不足", product["timing"]["risk"])
            states = [x["state"] for x in product["timing"]["milestones"]]
            self.assertEqual(
                ["shadow_overdue", "shadow_overdue", "shadow_overdue"],
                states[:3],
            )
            self.assertTrue(all(x == "shadow_pending" for x in states[3:]))

    def test_portfolio_reports_pre_live_contract_and_locked_reserve(self):
        plan = build_pilot(date(2026, 8, 19))
        self.assertEqual(
            "planner_and_shadow_tables_only",
            plan["integration_contract"]["implementation_state"],
        )
        self.assertEqual(0, plan["budget"]["reserve_approved_to_release_cny"])

    def test_portfolio_rejects_duplicate_campaign_ids(self):
        spec = CampaignSpec(
            campaign_id="duplicate",
            brand="FUNLAB",
            product_name="test",
            erp_sku="SKU",
            package_version="标准包装",
            launch_date=date(2026, 9, 15),
            target_posts=1,
            paid_warm_commitments=1,
            gifting_commitments=1,
            on_time_post_rate=1,
            budget_cap_cny=100,
            contribution_per_order_cny=100,
        )
        with self.assertRaisesRegex(ValueError, "must be unique"):
            build_portfolio_plan(
                [spec, spec], as_of=date(2026, 8, 19), reserve_budget_cny=0
            )

    def test_rejects_too_few_commitment_slots(self):
        spec = CampaignSpec(
            campaign_id="bad",
            brand="FUNLAB",
            product_name="test",
            erp_sku="SKU",
            package_version="标准包装",
            launch_date=date(2026, 9, 15),
            target_posts=20,
            paid_warm_commitments=2,
            gifting_commitments=2,
            on_time_post_rate=0.70,
            budget_cap_cny=1_000,
            contribution_per_order_cny=100,
        )
        with self.assertRaisesRegex(ValueError, "below required"):
            build_campaign_plan(spec, as_of=date(2026, 8, 19))


if __name__ == "__main__":
    unittest.main()
