import unittest
from unittest.mock import AsyncMock, patch

from app.product_dispatch_mode import (
    ACTIVITY_MODE,
    PAUSED_MODE,
    build_activity_group,
    is_proactive_product_outreach_source,
    partition_regular_products,
)


def _product(record_id: str, mode: str = "", **fields):
    data = {"派单模式": mode}
    data.update(fields)
    return {"record_id": record_id, "fields": data}


class ProductDispatchModeTests(unittest.TestCase):
    def test_empty_mode_remains_backward_compatible_regular_dispatch(self):
        regular, locked = partition_regular_products([
            _product("rec-legacy"),
        ])

        self.assertEqual(["rec-legacy"], [p["record_id"] for p in regular])
        self.assertEqual([], locked)

    def test_activity_and_paused_products_are_excluded_from_regular_dispatch(self):
        regular, locked = partition_regular_products([
            _product("rec-normal", "常规派单"),
            _product("rec-campaign", ACTIVITY_MODE, 活动归并键="piranha-v2"),
            _product("rec-paused", PAUSED_MODE),
        ])

        self.assertEqual(["rec-normal"], [p["record_id"] for p in regular])
        self.assertEqual(
            [("rec-campaign", ACTIVITY_MODE), ("rec-paused", PAUSED_MODE)],
            [(x["record_id"], x["mode"]) for x in locked],
        )

    def test_unknown_non_empty_mode_is_fail_closed(self):
        regular, locked = partition_regular_products([
            _product("rec-typo", "活动专用（拼错）"),
        ])

        self.assertEqual([], regular)
        self.assertEqual("活动专用（拼错）", locked[0]["mode"])

    def test_only_initial_cold_source_is_blockable(self):
        self.assertTrue(is_proactive_product_outreach_source(None))
        self.assertTrue(is_proactive_product_outreach_source("cold"))
        self.assertTrue(is_proactive_product_outreach_source("secondary_outreach"))
        for source in ("followup", "reply", "tracking_followup"):
            self.assertFalse(is_proactive_product_outreach_source(source))

    def test_activity_aliases_resolve_to_one_canonical_product(self):
        products = [
            _product(
                "rec-main",
                ACTIVITY_MODE,
                活动归并键="launch-20260915-powkong-piranha-v2",
                活动主记录=True,
                活动主记录ID="rec-main",
            ),
            _product(
                "rec-alias",
                ACTIVITY_MODE,
                活动归并键="launch-20260915-powkong-piranha-v2",
                活动主记录=False,
                活动主记录ID="rec-main",
            ),
        ]

        group = build_activity_group(products)

        self.assertEqual("launch-20260915-powkong-piranha-v2", group["merge_key"])
        self.assertEqual("rec-main", group["canonical_record_id"])
        self.assertEqual(["rec-main", "rec-alias"], group["member_record_ids"])

    def test_activity_group_rejects_inconsistent_canonical_ids(self):
        products = [
            _product(
                "rec-a",
                ACTIVITY_MODE,
                活动归并键="same-key",
                活动主记录=True,
                活动主记录ID="rec-a",
            ),
            _product(
                "rec-b",
                ACTIVITY_MODE,
                活动归并键="same-key",
                活动主记录ID="rec-b",
            ),
        ]

        with self.assertRaisesRegex(ValueError, "主记录ID不一致"):
            build_activity_group(products)


class ProductDispatchIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_dispatch_filters_activity_products_before_task_creation(self):
        from app import dispatch

        products = [
            _product("rec-normal", "常规派单"),
            _product("rec-campaign", ACTIVITY_MODE, 产品名="食人花二代"),
        ]
        with patch.object(
            dispatch.feishu, "search_records", AsyncMock(return_value=products)
        ):
            result = await dispatch.fetch_main_push_products()

        self.assertEqual(["rec-normal"], [p["record_id"] for p in result])

    async def test_secondary_outreach_never_falls_back_to_activity_products(self):
        from app import secondary_outreach

        products = [
            _product(
                "rec-campaign",
                ACTIVITY_MODE,
                产品名="食人花二代",
                **{
                    "上架状态": "主推",
                    "派单-库存OK": True,
                    "派单-素材OK": True,
                    "派单-文案OK": True,
                    "派单-价格OK": True,
                },
            )
        ]
        with patch.object(
            secondary_outreach.feishu,
            "search_records",
            AsyncMock(return_value=products),
        ):
            result = await secondary_outreach._find_main_products()

        self.assertEqual([], result)

    async def test_auto_send_lock_lookup_blocks_activity_but_allows_regular(self):
        from app import auto_send

        with patch.object(
            auto_send.feishu,
            "get_record",
            AsyncMock(side_effect=[
                _product("rec-campaign", ACTIVITY_MODE),
                _product("rec-normal", "常规派单"),
            ]),
        ):
            self.assertEqual(ACTIVITY_MODE, await auto_send._locked_product_mode("rec-campaign"))
            self.assertEqual("", await auto_send._locked_product_mode("rec-normal"))

    async def test_auto_send_lock_lookup_rejects_empty_product_response(self):
        from app import auto_send

        with patch.object(auto_send.feishu, "get_record", AsyncMock(return_value={})):
            with self.assertRaisesRegex(RuntimeError, "回读为空"):
                await auto_send._locked_product_mode("rec-missing")


if __name__ == "__main__":
    unittest.main()
