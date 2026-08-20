import unittest
from unittest.mock import AsyncMock, patch

from app import discord_tester_program as program
from app import discord_tester_routes as routes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


class DiscordTesterInteractionTests(unittest.IsolatedAsyncioTestCase):
    async def test_discord_get_request_does_not_send_a_json_body(self):
        response = unittest.mock.Mock(status_code=200, content=b"[]")
        response.json.return_value = []
        client = AsyncMock()
        client.request.return_value = response
        context = AsyncMock()
        context.__aenter__.return_value = client
        with (patch("app.discord_tester_routes.httpx.AsyncClient", return_value=context),
              patch.dict("os.environ", {"DISCORD_BOT_TOKEN": "test-token"})):
            await routes._discord_request("GET", "/guilds/123/roles")

        _, kwargs = client.request.call_args
        self.assertNotIn("json", kwargs)

    async def test_emergency_notification_counts_only_confirmed_message_ids(self):
        sender = AsyncMock(side_effect=["", "om_confirmed"])
        with (patch("app.config.NOTIFY_CHAT_ID", "oc_ops"),
              patch("app.config.NOTIFY_USERS", [("潘志聪", "ou_frankie")]),
              patch("app.feishu.send_card_message", sender)):
            sent = await routes._notify_emergency("rec123", "discord123", "Unusual odor")

        self.assertEqual(1, sent)
        self.assertEqual(2, sender.await_count)

    async def test_emergency_notification_returns_zero_when_all_message_ids_are_empty(self):
        sender = AsyncMock(return_value="")
        with (patch("app.config.NOTIFY_CHAT_ID", "oc_ops"),
              patch("app.config.NOTIFY_USERS", [("潘志聪", "ou_frankie")]),
              patch("app.feishu.send_card_message", sender)):
            sent = await routes._notify_emergency("rec123", "discord123", "Unusual odor")

        self.assertEqual(0, sent)

    async def test_apply_button_opens_first_application_modal(self):
        payload = {
            "type": 3,
            "data": {"custom_id": "tester_apply_start"},
            "member": {"user": {"id": "123", "username": "tester"}},
        }

        outcome = await program.build_interaction_outcome(payload)

        self.assertEqual(9, outcome.response["type"])
        self.assertEqual("tester_apply_step1", outcome.response["data"]["custom_id"])
        self.assertIn("Step 1 Of 3", outcome.response["data"]["title"])

    async def test_valid_step1_returns_private_continue_button(self):
        payload = _modal_submit("tester_apply_step1", {
            "country": "United States",
            "age": "YES",
            "devices": "Switch 2, Steam Deck, PC Steam",
            "amazon_24m": "YES",
            "commit": "YES",
        })

        outcome = await program.build_interaction_outcome(payload, signing_secret="test-secret")

        self.assertEqual(4, outcome.response["type"])
        self.assertEqual(64, outcome.response["data"]["flags"])
        button = outcome.response["data"]["components"][0]["components"][0]
        self.assertTrue(button["custom_id"].startswith("tester_apply_continue2."))
        self.assertLessEqual(len(button["custom_id"]), 100)

    async def test_ineligible_step1_stops_before_collecting_more_data(self):
        payload = _modal_submit("tester_apply_step1", {
            "country": "United States",
            "age": "YES",
            "devices": "Steam Deck and PC",
            "amazon_24m": "YES",
            "commit": "YES",
        })

        outcome = await program.build_interaction_outcome(payload, signing_secret="test-secret")

        self.assertEqual(4, outcome.response["type"])
        self.assertIn("not eligible", outcome.response["data"]["content"].lower())
        self.assertNotIn("components", outcome.response["data"])

    async def test_signed_continue_button_opens_second_modal(self):
        first = await program.build_interaction_outcome(_modal_submit("tester_apply_step1", {
            "country": "Canada", "age": "YES", "devices": "Switch 2",
            "amazon_24m": "YES", "commit": "YES",
        }), signing_secret="test-secret")
        custom_id = first.response["data"]["components"][0]["components"][0]["custom_id"]

        outcome = await program.build_interaction_outcome({
            "type": 3, "data": {"custom_id": custom_id},
            "member": {"user": {"id": "123", "username": "tester"}},
        }, signing_secret="test-secret")

        self.assertEqual(9, outcome.response["type"])
        self.assertTrue(outcome.response["data"]["custom_id"].startswith("tester_apply_step2."))
        self.assertIn("Step 2 Of 3", outcome.response["data"]["title"])

    async def test_tampered_continue_state_is_rejected(self):
        outcome = await program.build_interaction_outcome({
            "type": 3,
            "data": {"custom_id": "tester_apply_continue2.1-US-f.invalid"},
        }, signing_secret="test-secret")

        self.assertEqual(4, outcome.response["type"])
        self.assertIn("expired or invalid", outcome.response["data"]["content"].lower())

    async def test_step2_submission_returns_continue_to_final_step(self):
        step2_custom_id = await _open_step2_custom_id()
        outcome = await program.build_interaction_outcome(_modal_submit(step2_custom_id, {
            "purchase_count": "4-6",
            "product_types": "Controllers, games, cases",
            "funlab_prime": "FUNLAB=YES; PRIME=YES",
            "play_hours": "SWITCH=6-10; PC=2-5; CROSS=YES",
            "cross_platform": "Compared latency on Switch 2 and Steam Deck.",
        }), signing_secret="test-secret")

        self.assertEqual(4, outcome.response["type"])
        button = outcome.response["data"]["components"][0]["components"][0]
        self.assertTrue(button["custom_id"].startswith("tester_apply_continue3."))
        self.assertLessEqual(len(button["custom_id"]), 100)

        final = await program.build_interaction_outcome({
            "type": 3, "data": {"custom_id": button["custom_id"]}
        }, signing_secret="test-secret")
        self.assertEqual(9, final.response["type"])
        self.assertIn("Step 3 Of 3", final.response["data"]["title"])

    async def test_final_submission_saves_one_application_without_sensitive_shipping_data(self):
        ledger = FakeLedger()
        notices = []

        async def notify(message):
            notices.append(message)

        final_custom_id = await _open_final_custom_id()

        outcome = await program.build_interaction_outcome(_modal_submit(final_custom_id, {
            "games": "Mario Kart World (Switch 2), Hades (Steam Deck), Rocket League (PC)",
            "controllers": "Nintendo Pro Controller and 8BitDo Ultimate",
            "disconnect": "Record OS, connection mode, battery, game, steps, frequency, expected and actual result.",
            "feature_test": "Test latency in the same race on Switch 2 and PC, five runs each.",
            "route_agree": "ROUTE=SWITCH + STEAM DECK; RULES=YES; ALUMNI=NO",
        }), signing_secret="test-secret", ledger=ledger, completion_notifier=notify)

        self.assertEqual(5, outcome.response["type"])
        self.assertEqual(64, outcome.response["data"]["flags"])
        self.assertIsNone(ledger.saved)
        await outcome.work()
        self.assertIn("Application received", notices[0])
        self.assertEqual("123", ledger.saved["Discord用户ID"])
        self.assertEqual("已提交", ledger.saved["报名状态"])
        self.assertIn("Switch 2", ledger.saved["设备"])
        self.assertNotIn("收件地址", ledger.saved)
        self.assertNotIn("联系电话", ledger.saved)
        self.assertGreaterEqual(ledger.saved["筛选分数"], 30)
        self.assertIn("/47", ledger.saved["筛选结论"])

    async def test_final_submission_reports_background_save_failure(self):
        notices = []

        async def notify(message):
            notices.append(message)

        outcome = await program.build_interaction_outcome(
            _modal_submit(await _open_final_custom_id(), {
                "games": "Mario Kart World on Switch 2",
                "controllers": "Nintendo Pro Controller",
                "disconnect": "Record device, game, steps, frequency, expected and actual result.",
                "feature_test": "Run five matched races and compare input response.",
                "route_agree": "ROUTE=SWITCH; RULES=YES; ALUMNI=NO",
            }),
            signing_secret="test-secret",
            ledger=FailingLedger(),
            completion_notifier=notify,
        )

        await outcome.work()
        self.assertIn("could not save", notices[0])

    async def test_final_route_must_match_declared_devices(self):
        outcome = await program.build_interaction_outcome(
            _modal_submit(await _open_final_custom_id(), {
                "games": "Mario Kart World",
                "controllers": "Nintendo Pro Controller",
                "disconnect": "Record platform, game, steps, expected and actual result.",
                "feature_test": "Compare five matched runs.",
                "route_agree": "ROUTE=SWITCH + PC STEAM; RULES=YES; ALUMNI=NO",
            }),
            signing_secret="test-secret",
        )

        # The helper applicant declares Switch 2 + Steam Deck, but no PC Steam.
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("does not match", outcome.response["data"]["content"])

    def test_discord_signature_verification_rejects_tampering(self):
        private_key = Ed25519PrivateKey.generate()
        public_hex = private_key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        ).hex()
        timestamp = "1724130000"
        body = b'{"type":1}'
        signature = private_key.sign(timestamp.encode("ascii") + body).hex()

        self.assertTrue(routes.verify_discord_signature(public_hex, signature, timestamp, body))
        self.assertFalse(routes.verify_discord_signature(public_hex, signature, timestamp, body + b" "))

    def test_secure_form_token_is_bound_to_kind_user_record_and_expiry(self):
        with patch.object(program.time, "time", return_value=1_700_000_000):
            token = program.issue_form_token(
                "verification", "rec123", "discord123", 3600, "form-secret"
            )
            claims = program.read_form_token(token, "verification", "form-secret")
        self.assertEqual("rec123", claims["record_id"])
        self.assertEqual("discord123", claims["discord_user_id"])
        self.assertEqual({}, program.read_form_token(token, "shipping", "form-secret", now=1_700_000_010))
        self.assertEqual({}, program.read_form_token(token + "x", "verification", "form-secret", now=1_700_000_010))
        self.assertEqual({}, program.read_form_token(token, "verification", "form-secret", now=1_700_003_601))

    def test_sensitive_forms_have_separate_fields_and_safety_instruction(self):
        verification = routes.form_html("verification", "signed-token")
        shipping = routes.form_html("shipping", "signed-token")
        emergency = routes.form_html("emergency", "signed-token")
        day7 = routes.form_html("checkpoint2", "signed-token")
        receipt = routes.form_html("receipt", "signed-token")

        self.assertIn("Redact", verification)
        self.assertIn('type="file"', verification)
        self.assertNotIn('name="address_line_1"', verification)
        self.assertIn('name="address_line_1"', shipping)
        self.assertNotIn('type="file"', shipping)
        self.assertIn("Stop using", emergency)
        self.assertIn("Day 7 Stability Test", day7)
        self.assertIn("First successful connection", receipt)
        self.assertIn("First impressions", receipt)
        self.assertIn("no safety issue", receipt)

    def test_form_writes_keep_shipping_and_feedback_in_their_own_fields(self):
        claims = {"record_id": "rec123", "discord_user_id": "discord123"}
        app_fields, feedback = program.build_form_writes("shipping", {
            "legal_name": "Test User", "phone": "+1 555 0100", "email": "test@example.com",
            "address_line_1": "123 Main St", "address_line_2": "Apt 4", "city": "Austin",
            "region": "TX", "postal_code": "78701", "country": "US", "carrier_notes": "",
        }, claims)
        self.assertEqual("待发货", app_fields["配送状态"])
        self.assertIn("123 Main St", app_fields["收件地址"])
        self.assertEqual({}, feedback)

        app_fields, feedback = program.build_form_writes("emergency", {
            "platforms": "Switch 2", "summary": "Unusual odor", "steps": "After charging",
            "frequency": "Once",
        }, claims)
        self.assertIn("立即停用", app_fields["问题与异常"])
        self.assertEqual("P0-立即停用", feedback["严重度"])
        self.assertEqual("紧急安全上报", feedback["反馈类型"])

        app_fields, feedback = program.build_form_writes("receipt", {
            "condition": "Outer box crushed and controller damaged",
            "notes": "Button shell is cracked",
        }, claims)
        self.assertEqual("暂停", app_fields["测试进度"])
        self.assertFalse(app_fields["签收确认"])
        self.assertEqual("P1-阻断测试", feedback["严重度"])

    def test_form_status_gates_and_retention_scopes(self):
        self.assertTrue(program.status_allows_form("verification", "Shortlisted"))
        self.assertFalse(program.status_allows_form("verification", "Selected"))
        self.assertTrue(program.status_allows_form("shipping", "Selected"))
        self.assertFalse(program.status_allows_form("shipping", "已提交"))

        proof_clear = program.retention_clear_fields("verification")
        self.assertEqual([], proof_clear["购买凭证"])
        self.assertIsNone(proof_clear["核验资料删除日"])
        self.assertNotIn("收件地址", proof_clear)
        selected_clear = program.retention_clear_fields("selected")
        self.assertEqual("", selected_clear["收件地址"])
        self.assertEqual("", selected_clear["Discord用户ID"])
        self.assertIsNone(selected_clear["配送资料删除日"])
        self.assertEqual("未入选资料删除日", program.retention_date_field("unselected"))

    def test_prelaunch_setup_is_private_and_rehearsal_message_has_apply_button(self):
        plan = routes.discord_setup_plan()
        self.assertTrue(plan["prelaunch_private"])
        self.assertIn("tester-feedback", plan["channels"])
        self.assertIn("tester-office-hours", plan["channels"])
        self.assertIn("tester-staff-rehearsal", plan["channels"])
        self.assertNotIn("tester-announcements", plan["channels"])
        self.assertNotIn("public_announcement", plan["actions"])

        message = routes.rehearsal_message_payload()
        button = message["components"][0]["components"][0]
        self.assertEqual("tester_apply_start", button["custom_id"])
        self.assertIn("STAFF TEST", message["content"])


def _modal_submit(custom_id, values):
    return {
        "type": 5,
        "data": {
            "custom_id": custom_id,
            "components": [
                {"type": 1, "components": [{"type": 4, "custom_id": key, "value": value}]}
                for key, value in values.items()
            ],
        },
        "member": {"user": {"id": "123", "username": "tester"}},
    }


async def _open_step2_custom_id():
    first = await program.build_interaction_outcome(_modal_submit("tester_apply_step1", {
        "country": "United States", "age": "YES", "devices": "Switch 2, Steam Deck",
        "amazon_24m": "YES", "commit": "YES",
    }), signing_secret="test-secret")
    continue_id = first.response["data"]["components"][0]["components"][0]["custom_id"]
    second = await program.build_interaction_outcome({"type": 3, "data": {"custom_id": continue_id}}, signing_secret="test-secret")
    return second.response["data"]["custom_id"]


async def _open_final_custom_id():
    step2_custom_id = await _open_step2_custom_id()
    step2 = await program.build_interaction_outcome(_modal_submit(step2_custom_id, {
        "purchase_count": "4-6",
        "product_types": "Controllers, games, cases",
        "funlab_prime": "FUNLAB=YES; PRIME=YES",
        "play_hours": "SWITCH=6-10; PC=2-5; CROSS=YES",
        "cross_platform": "Compared latency on Switch 2 and Steam Deck.",
    }), signing_secret="test-secret")
    continue_id = step2.response["data"]["components"][0]["components"][0]["custom_id"]
    final = await program.build_interaction_outcome({"type": 3, "data": {"custom_id": continue_id}}, signing_secret="test-secret")
    return final.response["data"]["custom_id"]


class FakeLedger:
    def __init__(self):
        self.saved = None

    async def save_application(self, fields):
        self.saved = fields
        return "rec_test_123"


class FailingLedger:
    async def save_application(self, fields):
        raise RuntimeError("simulated ledger failure")


if __name__ == "__main__":
    unittest.main()
