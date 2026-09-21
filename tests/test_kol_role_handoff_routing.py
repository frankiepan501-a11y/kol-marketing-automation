import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("INTERNAL_TOKEN", "test-token")
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_ID", "cli_test_kol")
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_SECRET", "secret_test_kol")
os.environ.setdefault("KOL_ASSISTANT_FRANKIE_UNION_ID", "on_frankie")

from app import auto_send, config, feishu, zoho


class KolRoleHandoffRoutingTests(unittest.TestCase):
    def test_needs_rewrite_uses_current_job_title_and_frankie_not_static_people(self):
        with patch.object(
            feishu,
            "fetch_users_by_job_title",
            new=AsyncMock(return_value=[("张佳烨", "on_zhang"), ("叶星", "on_ye")]),
        ), patch.object(config, "KOL_NOTIFY_USERS", [
            ("潘志聪-Frankie", "on_frankie"),
            ("吴晓丹", "on_wu"),
            ("张佳烨-独立站运营", "on_static_zhang"),
        ]), patch.object(config, "KOL_ASSISTANT_FRANKIE_UNION_ID", "on_frankie"):
            targets = asyncio.run(feishu.resolve_notify_targets("needs_rewrite"))

        self.assertEqual(
            [("张佳烨", "on_zhang"), ("叶星", "on_ye"), (config.KOL_FRANKIE_NAME, "on_frankie")],
            targets,
        )
        self.assertNotIn("on_static_zhang", [uid for _, uid in targets])
        self.assertNotIn("on_wu", [uid for _, uid in targets])

    def test_missing_job_title_alerts_safely_and_fails_closed(self):
        alert = AsyncMock()
        with patch.object(
            feishu, "fetch_users_by_job_title", new=AsyncMock(return_value=[])
        ), patch.object(feishu, "_alert_role_resolution_failure", new=alert):
            with self.assertRaisesRegex(RuntimeError, "no active KOL reviewer"):
                asyncio.run(feishu.resolve_notify_targets("reviewer"))

        alert.assert_awaited_once_with("独立站运营专员")

    def test_job_title_token_failure_alerts_and_fails_closed(self):
        alert = AsyncMock()
        feishu._job_title_cache.clear()
        with patch.object(
            feishu, "token", new=AsyncMock(side_effect=RuntimeError("token unavailable"))
        ), patch.object(feishu, "_alert_role_resolution_failure", new=alert):
            with self.assertRaisesRegex(RuntimeError, "no active KOL reviewer"):
                asyncio.run(feishu.resolve_notify_targets("reviewer"))

        alert.assert_awaited_once_with("独立站运营专员")

    def test_job_title_partial_department_failure_discards_partial_people(self):
        class Response:
            def __init__(self, payload, status_code=200):
                self._payload = payload
                self.status_code = status_code

            def json(self):
                return self._payload

        class Client:
            def __init__(self):
                self.get = AsyncMock(side_effect=[
                    Response({"code": 0, "data": {"items": [{
                        "name": "张佳烨",
                        "union_id": "on_zhang",
                        "job_title": "独立站运营专员",
                        "status": {"is_activated": True, "is_resigned": False, "is_frozen": False},
                    }], "has_more": False}}),
                    Response({}, status_code=500),
                ])

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

        feishu._job_title_cache.clear()
        with patch.object(feishu, "token", new=AsyncMock(return_value="token")), patch.object(
            feishu.httpx, "AsyncClient", return_value=Client()
        ), patch.object(config, "KOL_CONTACT_DEPARTMENT_IDS", ["od_first", "od_second"]):
            result = asyncio.run(feishu.fetch_users_by_job_title("独立站运营专员"))

        self.assertEqual([], result)

    def test_reviewer_route_rejects_missing_frankie_union_id(self):
        with patch.object(
            feishu,
            "fetch_users_by_job_title",
            new=AsyncMock(return_value=[("叶星", "on_ye")]),
        ), patch.object(config, "KOL_ASSISTANT_FRANKIE_UNION_ID", ""):
            with self.assertRaisesRegex(RuntimeError, "FRANKIE_UNION_ID"):
                asyncio.run(feishu.resolve_notify_targets("reviewer"))

    def test_auto_send_pause_alert_goes_to_group_and_frankie_only(self):
        auto_send._paused_brands.clear()
        auto_send._pause_alerted.clear()
        sender = AsyncMock(return_value="om_test")
        with patch.object(
            feishu,
            "resolve_notify_targets",
            new=AsyncMock(return_value=[("潘志聪-Frankie", "on_frankie")]),
        ) as resolver, patch.object(feishu, "send_card_message", new=sender), patch.object(
            config,
            "NOTIFY_USERS",
            [("潘志聪-Frankie", "ou_old"), ("张佳烨", "ou_zhang"), ("余琦华", "ou_yu")],
        ):
            asyncio.run(auto_send._trigger_pause("FUNLAB", "Zoho unavailable"))

        resolver.assert_awaited_once_with("frankie")
        self.assertEqual(2, sender.await_count)
        self.assertEqual("chat_id", sender.await_args_list[0].args[0])
        self.assertEqual(("union_id", "on_frankie"), sender.await_args_list[1].args[:2])

    def test_zoho_sent_integrity_alert_uses_role_targets_not_static_people(self):
        class Response:
            status_code = 200

            @staticmethod
            def json():
                return {"data": {"content": "<p>short</p>"}}

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def get(self, *_args, **_kwargs):
                return Response()

        sender = AsyncMock(return_value="om_test")
        with patch.object(zoho.asyncio, "sleep", new=AsyncMock()), patch.object(
            zoho, "access", new=AsyncMock(return_value="token")
        ), patch.object(zoho.httpx, "AsyncClient", return_value=Client()), patch.object(
            config, "BRAND_CONFIG", {"FUNLAB": {"account_id": "acct"}}
        ), patch.object(
            config,
            "NOTIFY_USERS",
            [("张佳烨", "ou_zhang"), ("余琦华", "ou_yu")],
        ), patch.object(
            feishu,
            "resolve_notify_targets",
            new=AsyncMock(return_value=[("张佳烨", "on_zhang"), ("叶星", "on_ye"), ("Frankie", "on_frankie")]),
        ) as resolver, patch.object(feishu, "send_card_message", new=sender):
            asyncio.run(
                zoho.verify_sent_after(
                    "FUNLAB", "msg1", "sent", expected_text_len=100, delay=0
                )
            )

        resolver.assert_awaited_once_with("reviewer")
        self.assertEqual(
            [("union_id", "on_zhang"), ("union_id", "on_ye"), ("union_id", "on_frankie")],
            [(call.args[0], call.args[1]) for call in sender.await_args_list],
        )


if __name__ == "__main__":
    unittest.main()
