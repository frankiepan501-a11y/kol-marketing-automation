import asyncio
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_ID", "cli_test_kol")
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_SECRET", "secret_test_kol")
os.environ.setdefault("KOL_ASSISTANT_FRANKIE_UNION_ID", "on_test_frankie")
os.environ.setdefault("KOL_EVENT_HUB_URL", "https://n8n.example/webhook/feishu-event-hub")
os.environ.setdefault("FEISHU_APP_TOKEN", "kol_test_base")

from app import config, feishu, kol_callback


class KolR8IdentityTests(unittest.TestCase):
    def test_health_probe_reads_real_kol_table_with_dedicated_identity(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **kwargs):
            calls.append((method, path, which, kwargs))
            return {"code": 0, "data": {"items": [{"record_id": "rec1"}]}}

        with patch.object(config, "FEISHU_APP_TOKEN", "kol_test_base"), \
             patch.object(config, "T_KOL", "tbl_kol"), \
             patch.object(feishu, "api", new=fake_api):
            result = asyncio.run(feishu.probe_kol_bitable_access())

        self.assertTrue(result["ok"])
        self.assertEqual("kol_assistant", calls[0][2])
        self.assertIn("/apps/kol_test_base/tables/tbl_kol/records", calls[0][1])
        self.assertFalse(calls[0][3]["retry_transient"])

    def test_kol_base_paths_use_dedicated_identity(self):
        with patch.object(config, "FEISHU_APP_TOKEN", "kol_test_base"):
            path = "/bitable/v1/apps/kol_test_base/tables/tbl_test/records"
            self.assertEqual("kol_assistant", feishu._resolve_identity(path, "bitable"))
            self.assertEqual("notify", feishu._resolve_identity(path, "notify"))
            self.assertEqual(
                "bitable",
                feishu._resolve_identity(
                    "/bitable/v1/apps/another-app/tables/tbl/records", "bitable"
                ),
            )

    def test_kol_business_cards_use_dedicated_identity(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **_kwargs):
            calls.append((method, path, body, which))
            return {"data": {"message_id": "om_kol"}}

        with patch.object(feishu, "api", new=fake_api):
            message_id = asyncio.run(
                feishu.send_card_message(
                    "union_id", "on_target", {"header": {}, "elements": []}, biz="KOL"
                )
            )

        self.assertEqual("om_kol", message_id)
        self.assertEqual("kol_assistant", calls[0][3])

    def test_old_open_id_is_resolved_then_sent_as_union_id(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **_kwargs):
            calls.append((path, body, which))
            return {"data": {"message_id": "om_kol"}}

        with patch.object(feishu, "open_id_to_union_id", new=AsyncMock(return_value="on_target")), \
             patch.object(feishu, "api", new=fake_api):
            asyncio.run(
                feishu.send_card_message(
                    "open_id", "ou_old", {"header": {}, "elements": []}, biz="KOL"
                )
            )

        self.assertIn("receive_id_type=union_id", calls[0][0])
        self.assertEqual("on_target", calls[0][1]["receive_id"])
        self.assertEqual("kol_assistant", calls[0][2])

    def test_non_kol_cards_keep_existing_identity(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **_kwargs):
            calls.append(which)
            return {"data": {"message_id": "om_old"}}

        with patch.object(feishu, "api", new=fake_api):
            asyncio.run(
                feishu.send_card_message(
                    "open_id", "ou_target", {"header": {}, "elements": []}, biz="AUDIT"
                )
            )

        self.assertEqual(["notify"], calls)

    def test_r9_target_only_routes_ignore_removed_r8_switches(self):
        path = "/bitable/v1/apps/kol_test_base/tables/tbl/records"
        calls = []

        async def fake_api(method, api_path, body=None, which="bitable", **_kwargs):
            calls.append(which)
            return {"data": {"message_id": "om_old"}}

        with patch.object(config, "FEISHU_APP_TOKEN", "kol_test_base"), \
             patch.object(feishu, "api", new=fake_api):
            self.assertEqual("kol_assistant", feishu._resolve_identity(path, "bitable"))
            asyncio.run(feishu.send_card_via_app3("union_id", "on_test", {"elements": []}))

        self.assertEqual(["kol_assistant"], calls)

    def test_kol_interactive_sender_uses_dedicated_identity(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable", **_kwargs):
            calls.append(which)
            return {"data": {"message_id": "om_interactive"}}

        with patch.object(feishu, "api", new=fake_api):
            result = asyncio.run(
                feishu.send_card_via_kol_assistant(
                    "union_id", "on_target", {"elements": []}
                )
            )

        self.assertEqual("om_interactive", result)
        self.assertEqual(["kol_assistant"], calls)

    def test_callback_marker_selects_same_app_for_patch(self):
        self.assertEqual(
            "kol_assistant",
            feishu.kol_callback_identity(
                {"card_action": {"action": "draft_approve", "_delivery_identity": "kol_assistant"}}
            ),
        )
        self.assertEqual("app3", feishu.kol_callback_identity({"card_action": {"action": "draft_approve"}}))


class KolR8CallbackTests(unittest.TestCase):
    @staticmethod
    def _event(action="draft_approve"):
        return SimpleNamespace(
            message_id="om_test",
            chat_id="oc_test",
            operator=SimpleNamespace(open_id="ou_test"),
            action=SimpleNamespace(value={"action": action, "draft_record_id": "rec1"}, form_value={"note": "ok"}),
            raw={"header": {"event_id": "evt_test_1"}},
        )

    def test_only_kol_actions_are_allowlisted(self):
        self.assertTrue(kol_callback.is_allowed_action("draft_approve"))
        self.assertTrue(kol_callback.is_allowed_action("warm_recap_send"))
        self.assertTrue(kol_callback.is_allowed_action("kol_roi_map_confirm"))
        self.assertFalse(kol_callback.is_allowed_action("cs_reply_send"))
        self.assertFalse(kol_callback.is_allowed_action("hr_r7_verify"))

    def test_normalized_event_hub_payload_has_delivery_marker(self):
        payload = kol_callback.build_event_hub_payload(self._event())
        self.assertEqual("card.action.trigger", payload["header"]["event_type"])
        self.assertEqual("kol_assistant", payload["event"]["action"]["value"]["_delivery_identity"])
        self.assertEqual("feishu:evt_test_1", payload["event"]["action"]["value"]["_kol_idempotency_key"])
        self.assertEqual("om_test", payload["event"]["context"]["open_message_id"])
        self.assertEqual("ou_test", payload["event"]["operator"]["open_id"])

    def test_derived_idempotency_key_is_stable_without_event_id(self):
        event = self._event()
        event.raw = {}
        self.assertEqual(
            kol_callback.callback_idempotency_key(event),
            kol_callback.callback_idempotency_key(event),
        )

    def test_relay_rejects_non_kol_action_without_http_call(self):
        sender = AsyncMock()
        with patch.object(kol_callback, "_post_event", new=sender):
            result = asyncio.run(kol_callback.handle_card_action(self._event("cs_reply_send")))
        self.assertEqual("action_not_allowed", result["error"])
        sender.assert_not_awaited()

    def test_relay_posts_kol_action_once(self):
        sender = AsyncMock(return_value=None)
        with patch.object(kol_callback, "_post_event", new=sender):
            result = asyncio.run(kol_callback.handle_card_action(self._event()))
        self.assertTrue(result["ok"])
        sender.assert_awaited_once()
        sent_payload = sender.await_args.args[0]
        self.assertEqual("draft_approve", sent_payload["event"]["action"]["value"]["action"])

    def test_failed_relay_is_spooled_for_replay(self):
        sender = AsyncMock(side_effect=RuntimeError("hub down"))
        spool = Mock()
        with patch.object(kol_callback, "_post_event", new=sender), \
             patch.object(kol_callback, "_append_spool", new=spool):
            result = asyncio.run(kol_callback.handle_card_action(self._event()))
        self.assertTrue(result["queued"])
        spool.assert_called_once()

    def test_snapshot_reports_initial_websocket_connection(self):
        connection = SimpleNamespace(state="idle", ready=False, reconnect_attempts=0)
        channel = SimpleNamespace(
            _ws_client=SimpleNamespace(_conn=object()),
            connection_snapshot=lambda: connection,
        )
        with patch.object(kol_callback, "_CHANNEL", channel), \
             patch.dict(kol_callback.STATE, {"error": "ConnectionError"}):
            result = kol_callback.snapshot()
        self.assertEqual("connected", result["connection"])
        self.assertTrue(result["ready"])
        self.assertIsNone(result["error"])


if __name__ == "__main__":
    unittest.main()
