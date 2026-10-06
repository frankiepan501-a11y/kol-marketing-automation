import unittest
from unittest.mock import AsyncMock, patch

from app import config, feishu, launch_runtime


class KolTempOwnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_campaign_boundary_card_routes_to_frankie(self):
        with patch.object(config, "KOL_TEMP_FRANKIE_OWNER", True), patch.object(
            feishu, "resolve_notify_targets",
            new=AsyncMock(return_value=[("Frankie", "on_frankie")]),
        ) as route, patch.object(
            feishu, "fetch_users_by_job_title", new=AsyncMock()
        ) as lookup, patch.object(
            feishu, "send_card_message", new=AsyncMock(return_value="om_test")
        ) as send:
            result = await launch_runtime._notify_operator_review(
                campaign_id="test", activity={"fields": {"活动名称": "测试"}}, created=1,
            )
        self.assertEqual(1, result["sent"])
        route.assert_awaited_once_with("frankie")
        lookup.assert_not_awaited()
        self.assertEqual(("union_id", "on_frankie"), send.await_args.args[:2])
