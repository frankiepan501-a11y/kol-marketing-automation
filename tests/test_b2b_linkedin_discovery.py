import asyncio
import os
import unittest
from collections import Counter

from app import b2b_linkedin_discovery as discovery


class B2BLinkedInDiscoveryTest(unittest.TestCase):
    def setUp(self):
        self.old_env = {
            key: os.environ.get(key)
            for key in [
                "B2B_DISCOVERY_PENDING_TARGET",
                "B2B_DISCOVERY_DAILY_CREATE_LIMIT",
                "B2B_DISCOVERY_MIN_SCORE",
                "GOOGLE_SEARCH_PROVIDER",
                "B2B_DISCOVERY_MANUAL_RESULTS_JSON",
            ]
        }
        os.environ["B2B_DISCOVERY_PENDING_TARGET"] = "200"
        os.environ["B2B_DISCOVERY_DAILY_CREATE_LIMIT"] = "100"
        os.environ["B2B_DISCOVERY_MIN_SCORE"] = "55"
        os.environ["GOOGLE_SEARCH_PROVIDER"] = "manual"
        os.environ.pop("B2B_DISCOVERY_MANUAL_RESULTS_JSON", None)

    def tearDown(self):
        for key, value in self.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_blocks_platform_domains(self):
        self.assertEqual(
            "blocked_platform_domain",
            discovery._domain_blocked("amazon.com", title="Nintendo Switch accessories"),
        )
        self.assertEqual("", discovery._domain_blocked("example-distributor.com", title="Gaming distributor"))

    def test_result_to_seed_normalizes_company_domain(self):
        seed, skip = discovery._result_to_seed({
            "title": "Example Games Distribution - Nintendo Switch Accessories",
            "link": "https://www.example-games.com/catalog/switch",
            "snippet": "Wholesale gaming accessories and console products.",
            "provider": "unit",
        }, {
            "query": "gaming accessories distributor",
            "country": "United States",
            "company_type": "分销商",
            "channels": ["分销"],
            "category": "gaming accessories distributor",
        })
        self.assertEqual("", skip)
        self.assertEqual("example-games.com", seed["domain"])
        self.assertEqual("搜索补给", seed["candidate_source"])
        self.assertIn("query=gaming accessories distributor", seed["notes"])

    def test_high_waterline_skips_search(self):
        original_indexes = discovery._candidate_indexes
        original_search = discovery._search_items
        try:
            async def fake_indexes():
                return Counter({"待入池": 220}), set(), set()

            async def fail_search(*args, **kwargs):
                raise AssertionError("search should not run when waterline is full")

            discovery._candidate_indexes = fake_indexes
            discovery._search_items = fail_search
            result = asyncio.run(discovery.run(commit=False, provider="all", limit=50))
        finally:
            discovery._candidate_indexes = original_indexes
            discovery._search_items = original_search

        self.assertEqual("skip_target_met", result["waterline_status"])
        self.assertEqual(0, result["planned_candidates"])
        self.assertEqual({"candidate_pool_target_met": 1}, result["skip_reasons"])

    def test_run_plans_valid_search_result_with_snov_summary(self):
        original_indexes = discovery._candidate_indexes
        original_search = discovery._search_items
        original_existing = discovery.pool._load_existing_keys
        original_snov = discovery.pool._snov_prospects
        try:
            async def fake_indexes():
                return Counter({"待入池": 50}), set(), set()

            async def fake_search(query, *, num, provider):
                return [
                    {
                        "title": "Example Games Distribution - Gaming Accessories",
                        "link": "https://example-games.com",
                        "snippet": "Distributor and wholesale supplier for Nintendo Switch accessories.",
                        "provider": "unit",
                    },
                    {
                        "title": "Nintendo Switch accessories on Amazon",
                        "link": "https://amazon.com/example",
                        "snippet": "Marketplace listing.",
                        "provider": "unit",
                    },
                ]

            async def fake_existing():
                return set(), set(), set(), set()

            async def fake_snov(domain, *, max_prospects):
                self.assertEqual("example-games.com", domain)
                return ([{"first_name": "Ada", "last_name": "Buyer", "position": "Purchasing Manager"}], '{"prospects":1}')

            discovery._candidate_indexes = fake_indexes
            discovery._search_items = fake_search
            discovery.pool._load_existing_keys = fake_existing
            discovery.pool._snov_prospects = fake_snov

            result = asyncio.run(discovery.run(commit=False, provider="all", limit=5, min_score=55))
        finally:
            discovery._candidate_indexes = original_indexes
            discovery._search_items = original_search
            discovery.pool._load_existing_keys = original_existing
            discovery.pool._snov_prospects = original_snov

        self.assertEqual("refill_needed", result["waterline_status"])
        self.assertEqual(1, result["planned_candidates"])
        self.assertEqual(0, result["created_candidates"])
        self.assertEqual(1, result["snov_available_candidates"])
        preview = result["planned_preview"][0]
        self.assertEqual("example-games.com", preview["domain"])
        self.assertEqual("查询成功", preview["snov_status"])
        self.assertEqual("搜索补给", preview["source"])
        self.assertGreaterEqual(result["skip_reasons"]["blocked_platform_domain"], 1)

    def test_duplicates_are_reported_before_planning(self):
        original_indexes = discovery._candidate_indexes
        original_search = discovery._search_items
        original_existing = discovery.pool._load_existing_keys
        try:
            async def fake_indexes():
                return Counter({"待入池": 10}), {"duplicate.com"}, set()

            async def fake_search(query, *, num, provider):
                return [{
                    "title": "Duplicate Distributor",
                    "link": "https://duplicate.com",
                    "snippet": "Gaming accessories distributor.",
                    "provider": "unit",
                }]

            async def fake_existing():
                return set(), set(), set(), set()

            discovery._candidate_indexes = fake_indexes
            discovery._search_items = fake_search
            discovery.pool._load_existing_keys = fake_existing

            result = asyncio.run(discovery.run(commit=False, provider="google", limit=5))
        finally:
            discovery._candidate_indexes = original_indexes
            discovery._search_items = original_search
            discovery.pool._load_existing_keys = original_existing

        self.assertEqual(0, result["planned_candidates"])
        self.assertEqual(1, result["skip_reasons"]["duplicate_candidate_domain"])


if __name__ == "__main__":
    unittest.main()
