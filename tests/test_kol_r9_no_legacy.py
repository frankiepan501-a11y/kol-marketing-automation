import asyncio
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_ID", "cli_test_kol")
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_SECRET", "secret_test_kol")
os.environ.setdefault("KOL_ASSISTANT_FRANKIE_UNION_ID", "on_frankie")
os.environ.setdefault(
    "KOL_NOTIFY_USERS",
    "潘志聪-Frankie:on_frankie,吴晓丹:on_wu,张佳烨-独立站运营:on_zhang",
)
os.environ.setdefault("FEISHU_APP_TOKEN", "kol_test_base")

from app import config, feishu


class KolR9NoLegacyTests(unittest.TestCase):
    def test_kol_base_is_target_only(self):
        path = "/bitable/v1/apps/kol_test_base/tables/tbl/records"
        with patch.object(config, "FEISHU_APP_TOKEN", "kol_test_base"):
            self.assertEqual("kol_assistant", feishu._resolve_identity(path, "bitable"))

    def test_kol_card_is_target_only_and_maps_old_recipient_by_name(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **_kwargs):
            calls.append((path, body, which))
            return {"data": {"message_id": "om_target"}}

        with patch.object(
            config,
            "NOTIFY_USERS",
            [("潘志聪-Frankie", "ou_old_frankie")],
        ), patch.object(
            config,
            "KOL_NOTIFY_USERS",
            [("潘志聪-Frankie", "on_frankie")],
        ), patch.object(feishu, "api", new=fake_api):
            result = asyncio.run(
                feishu.send_card_message(
                    "open_id", "ou_old_frankie", {"elements": []}, biz="KOL"
                )
            )

        self.assertEqual("om_target", result)
        self.assertIn("receive_id_type=union_id", calls[0][0])
        self.assertEqual("on_frankie", calls[0][1]["receive_id"])
        self.assertEqual("kol_assistant", calls[0][2])

    def test_missing_kol_recipient_mapping_fails_without_notify_lookup(self):
        calls = []

        async def sender(_method, _path, _body=None, which="bitable", **_kwargs):
            calls.append(which)
            return {}

        with patch.object(config, "NOTIFY_USERS", []), patch.object(
            config, "KOL_NOTIFY_USERS", []
        ), patch.object(feishu, "api", new=sender):
            with self.assertRaisesRegex(RuntimeError, "专属收件人"):
                asyncio.run(
                    feishu.send_card_message(
                        "open_id", "ou_unknown", {"elements": []}, biz="KOL"
                    )
                )
        self.assertEqual(["kol_assistant"], calls)
        self.assertNotIn("notify", calls)

    def test_legacy_named_sender_alias_never_uses_app3_for_new_cards(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **_kwargs):
            calls.append(which)
            return {"data": {"message_id": "om_target"}}

        with patch.object(feishu, "api", new=fake_api):
            result = asyncio.run(
                feishu.send_card_via_app3("union_id", "on_user", {"elements": []})
            )
        self.assertEqual("om_target", result)
        self.assertEqual(["kol_assistant"], calls)

    def test_new_card_patch_does_not_fallback_to_app3(self):
        patcher = AsyncMock(return_value=False)
        with patch.object(feishu, "update_card_message_with_app", new=patcher):
            result = asyncio.run(feishu.update_kol_card(
                "om_new", {"elements": []}, which="kol_assistant",
            ))
        self.assertFalse(result)
        self.assertEqual(1, patcher.await_count)
        self.assertEqual("kol_assistant", patcher.await_args.kwargs["which"])

    def test_unowned_persisted_card_patch_fails_instead_of_guessing(self):
        with self.assertRaisesRegex(ValueError, "owner identity"):
            asyncio.run(feishu.update_kol_card("om_unknown", {"elements": []}))

    def test_message_refs_persist_exact_sender_and_plain_values_are_app3(self):
        packed = feishu.pack_kol_message_ref("om_new")
        self.assertEqual("kol_assistant:om_new", packed)
        self.assertEqual(("om_new", "kol_assistant"), feishu.unpack_kol_message_ref(packed))
        self.assertEqual(("om_old", "app3"), feishu.unpack_kol_message_ref("om_old"))

    def test_unmarked_historical_card_still_uses_app3(self):
        patcher = AsyncMock(return_value=True)
        with patch.object(feishu, "update_card_message_with_app", new=patcher):
            result = asyncio.run(
                feishu.update_kol_card(
                    "om_old", {"elements": []},
                    event={"card_action": {"action": "draft_approve"}},
                )
            )
        self.assertTrue(result)
        self.assertEqual("app3", patcher.await_args.kwargs["which"])

    def test_job_title_lookup_uses_kol_app_and_union_ids(self):
        class Response:
            def __init__(self, payload, status_code=200):
                self._payload = payload
                self.status_code = status_code

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(self.status_code)

            def json(self):
                return self._payload

        responses = [
            Response({"code": 0, "data": {"items": [{
                "name": "运营",
                "union_id": "on_operator",
                "job_title": "独立站运营专员",
                "status": {"is_activated": True, "is_resigned": False, "is_frozen": False},
            }], "has_more": False}}),
        ]

        class Client:
            def __init__(self):
                self.get = AsyncMock(side_effect=responses)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

        client = Client()
        feishu._job_title_cache.clear()
        with patch.object(feishu, "token", new=AsyncMock(return_value="tok")) as token_mock, \
             patch.object(feishu.httpx, "AsyncClient", return_value=client), \
             patch.object(config, "KOL_CONTACT_DEPARTMENT_IDS", ["od_ops"]):
            result = asyncio.run(feishu.fetch_users_by_job_title("独立站运营专员"))

        token_mock.assert_awaited_once_with("kol_assistant")
        self.assertEqual([("运营", "on_operator")], result)
        self.assertEqual("union_id", client.get.await_args_list[0].kwargs["params"]["user_id_type"])

    def test_empty_kol_notify_users_fail_closed(self):
        with patch.object(config, "KOL_NOTIFY_USERS", []):
            with self.assertRaisesRegex(RuntimeError, "refusing silent notification loss"):
                asyncio.run(feishu.resolve_notify_targets("needs_rewrite"))

    def test_missing_reviewer_and_fallback_fail_closed(self):
        with patch.object(config, "KOL_NOTIFY_USERS", []), \
             patch.object(feishu, "fetch_users_by_job_title", new=AsyncMock(return_value=[])):
            with self.assertRaisesRegex(RuntimeError, "no active KOL reviewer"):
                asyncio.run(feishu.resolve_notify_targets("reviewer"))


if __name__ == "__main__":
    unittest.main()
