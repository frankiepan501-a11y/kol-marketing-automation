import unittest
from unittest import mock

from app import enrich


class EnrichCandidateGateTests(unittest.TestCase):
    def test_explicit_pending_route_is_not_eligible_for_regular_cold_outreach(self):
        self.assertFalse(enrich._allows_new_cold_outreach({
            "合作状态": "未建联", "触达路由状态": "待核对",
        }))

    def test_new_outreach_and_legacy_blank_route_remain_compatible(self):
        self.assertTrue(enrich._allows_new_cold_outreach({"触达路由状态": "可新开发"}))
        self.assertTrue(enrich._allows_new_cold_outreach({}))

    def test_controlled_marker_with_silently_missing_route_fails_closed(self):
        self.assertFalse(enrich._allows_new_cold_outreach({
            "迁移备注": "[CONTROLLED_IMPORT] campaign=c1",
            "触达路由状态": "",
        }))


class EnrichFilterIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @mock.patch("app.enrich.feishu.search_records")
    async def test_regular_filter_excludes_pending_route(self, search_records):
        search_records.side_effect = [
            [],
            [
                {"record_id": "kol_pending", "fields": {
                    "邮箱": "pending@example.com", "合作状态": "未建联",
                    "触达路由状态": "待核对", "粉丝数": 10000,
                }},
                {"record_id": "kol_controlled_missing_route", "fields": {
                    "邮箱": "controlled@example.com", "合作状态": "未建联",
                    "触达路由状态": "", "粉丝数": 10000,
                    "迁移备注": "[CONTROLLED_IMPORT] campaign=c1",
                }},
                {"record_id": "kol_ready", "fields": {
                    "邮箱": "ready@example.com", "合作状态": "未建联",
                    "触达路由状态": "可新开发", "粉丝数": 10000,
                }},
            ],
        ]

        rows = await enrich.filter_kols({"筛选-粉丝下限": 1}, brand="FUNLAB")

        self.assertEqual([row["record_id"] for row in rows], ["kol_ready"])


if __name__ == "__main__":
    unittest.main()
