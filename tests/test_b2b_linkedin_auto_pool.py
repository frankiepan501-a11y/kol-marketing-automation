import asyncio
import os
import unittest
from collections import Counter

from app import b2b_linkedin_auto_pool as pool


class B2BLinkedInAutoPoolTest(unittest.TestCase):
    def setUp(self):
        self.old_env = os.environ.get("B2B_LINKEDIN_COMPANY_URLS_JSON")
        os.environ.pop("B2B_LINKEDIN_COMPANY_URLS_JSON", None)

    def tearDown(self):
        if self.old_env is None:
            os.environ.pop("B2B_LINKEDIN_COMPANY_URLS_JSON", None)
        else:
            os.environ["B2B_LINKEDIN_COMPANY_URLS_JSON"] = self.old_env

    def test_known_company_mapping_sets_linkedin_company(self):
        lead = pool._seed_to_lead({
            "company": "PCComponentes",
            "domain": "pccomponentes.com",
            "country": "Spain",
            "company_type": "电商卖家",
            "channels": ["本地电商"],
            "category": "gaming accessories",
        })
        self.assertEqual("https://www.linkedin.com/company/pccomponentes", lead["linkedin_company"])
        self.assertEqual("已确认", lead["linkedin_company_status"])
        self.assertEqual("known_map", lead["linkedin_company_source"])

    def test_env_mapping_can_fill_new_high_confidence_company(self):
        os.environ["B2B_LINKEDIN_COMPANY_URLS_JSON"] = '{"example.com":"https://www.linkedin.com/company/example"}'
        lead = pool._seed_to_lead({
            "company": "Example Retail",
            "domain": "example.com",
            "country": "United States",
            "company_type": "零售商",
            "channels": ["本地电商"],
            "category": "gaming accessories",
        })
        self.assertEqual("https://www.linkedin.com/company/example", lead["linkedin_company"])
        self.assertEqual("已确认", lead["linkedin_company_status"])
        self.assertEqual("env", lead["linkedin_company_source"])

    def test_unknown_company_is_marked_for_manual_review_without_url(self):
        lead = pool._seed_to_lead({
            "company": "Unknown Games Retailer",
            "domain": "unknown-games.example",
            "country": "United States",
            "company_type": "零售商",
            "channels": ["本地电商"],
            "category": "gaming accessories",
            "notes": "seed note",
        })
        self.assertEqual("", lead["linkedin_company"])
        self.assertEqual("待人工确认", lead["linkedin_company_status"])
        self.assertIn("LinkedIn公司页待人工确认", lead["notes"])

        score = pool._score_lead(lead)
        copy = pool._copy_for_lead(lead, score)
        fields = pool._lead_fields(
            lead,
            score,
            copy,
            batch="unit",
            snov_status="无结果",
            snov_source="Company seed fallback",
            snov_summary="",
        )
        self.assertNotIn("LinkedIn公司页", fields)
        self.assertIn("先人工确认企业LinkedIn公司页", fields["下一步行动"])
        self.assertIn("LinkedIn公司页待人工确认", fields["备注"])

    def test_run_reports_resolved_and_pending_company_pages(self):
        original_load_seeds = pool._load_seeds
        original_load_existing = pool._load_existing_keys
        original_snov = pool._snov_prospects
        original_load_candidates = pool._load_pending_candidate_seeds
        try:
            pool._load_seeds = lambda: [
                {
                    "company": "PCComponentes",
                    "domain": "pccomponentes.com",
                    "country": "Spain",
                    "company_type": "电商卖家",
                    "channels": ["本地电商"],
                    "category": "gaming accessories",
                },
                {
                    "company": "Unknown Games Retailer",
                    "domain": "unknown-games.example",
                    "country": "United States",
                    "company_type": "零售商",
                    "channels": ["本地电商"],
                    "category": "gaming accessories",
                },
            ]

            async def fake_existing():
                return set(), set(), set(), set()

            async def fake_snov(domain, *, max_prospects):
                return [], "{}"

            pool._load_existing_keys = fake_existing
            pool._snov_prospects = fake_snov
            pool._load_pending_candidate_seeds = lambda: _async_value(([], Counter()))

            result = asyncio.run(pool.run(commit=False, domain_limit=2, record_limit=2))
        finally:
            pool._load_seeds = original_load_seeds
            pool._load_existing_keys = original_load_existing
            pool._snov_prospects = original_snov
            pool._load_pending_candidate_seeds = original_load_candidates

        self.assertEqual(2, result["planned_records"])
        self.assertEqual(1, result["linkedin_company_resolved"])
        self.assertEqual(1, result["linkedin_company_pending"])
        self.assertEqual("Unknown Games Retailer", result["linkedin_company_pending_preview"][0]["company"])
        self.assertEqual("seed_fallback", result["candidate_source"])

    def test_refill_candidates_plans_new_candidate_inventory(self):
        original_load_seeds = pool._load_seeds
        original_load_existing = pool._load_existing_keys
        original_list_candidates = pool._list_candidate_records
        try:
            pool._load_seeds = lambda: [{
                "company": "Example Games Distributor",
                "domain": "example-distributor.com",
                "country": "United States",
                "company_type": "分销商",
                "channels": ["分销"],
                "category": "gaming accessories distributor",
            }]

            async def fake_existing():
                return set(), set(), set(), set()

            pool._load_existing_keys = fake_existing
            pool._list_candidate_records = lambda: _async_value([])

            result = asyncio.run(pool.refill_candidates(commit=False, limit=10))
        finally:
            pool._load_seeds = original_load_seeds
            pool._load_existing_keys = original_load_existing
            pool._list_candidate_records = original_list_candidates

        self.assertEqual(1, result["planned_candidates"])
        self.assertEqual(0, result["created_candidates"])
        self.assertEqual("Example Games Distributor", result["planned_preview"][0]["company"])
        self.assertEqual("系统种子", result["planned_preview"][0]["source"])

    def test_run_uses_candidate_pool_before_seed_fallback(self):
        original_load_seeds = pool._load_seeds
        original_load_existing = pool._load_existing_keys
        original_snov = pool._snov_prospects
        original_load_candidates = pool._load_pending_candidate_seeds
        try:
            pool._load_seeds = lambda: [{
                "company": "Should Not Be Used",
                "domain": "unused.example",
                "country": "United States",
                "company_type": "分销商",
                "channels": ["分销"],
                "category": "gaming accessories",
            }]

            async def fake_existing():
                return set(), set(), set(), set()

            async def fake_snov(domain, *, max_prospects):
                return [], "{}"

            async def fake_candidates():
                return ([{
                    "company": "Candidate Pool Co",
                    "domain": "candidatepool.example",
                    "country": "United States",
                    "company_type": "分销商",
                    "channels": ["分销"],
                    "category": "gaming accessories distributor",
                    "source": "LinkedIn-现有客户相似",
                    "notes": "",
                    "_candidate_record_id": "recCandidate",
                    "_candidate_query_count": 0,
                    "_priority_score": 90,
                }], Counter({"待入池": 1}))

            pool._load_existing_keys = fake_existing
            pool._snov_prospects = fake_snov
            pool._load_pending_candidate_seeds = fake_candidates

            result = asyncio.run(pool.run(commit=False, domain_limit=1, record_limit=1))
        finally:
            pool._load_seeds = original_load_seeds
            pool._load_existing_keys = original_load_existing
            pool._snov_prospects = original_snov
            pool._load_pending_candidate_seeds = original_load_candidates

        self.assertEqual("candidate_pool", result["candidate_source"])
        self.assertEqual(1, result["candidate_pending_total"])
        self.assertEqual("Candidate Pool Co", result["planned_preview"][0]["company"])


async def _async_value(value):
    return value


if __name__ == "__main__":
    unittest.main()
