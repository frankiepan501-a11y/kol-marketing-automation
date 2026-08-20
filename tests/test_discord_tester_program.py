import unittest
import time
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
        outcome = await program.build_interaction_outcome({
            "type": 3, "data": {"custom_id": "tester_apply_start"},
            "member": {"user": {"id": "123", "username": "tester"}},
        })
        self.assertEqual(9, outcome.response["type"])
        self.assertEqual("tester_apply_v2_step1", outcome.response["data"]["custom_id"])
        self.assertIn("Step 1 Of 2", outcome.response["data"]["title"])
        self.assertEqual(5, len(outcome.response["data"]["components"]))

    async def test_valid_step1_returns_private_continue_button(self):
        outcome = await program.build_interaction_outcome(_modal_submit("tester_apply_v2_step1", {
            "country": "United States", "age": "YES",
            "devices": "Switch 2, Steam Deck, PC Steam",
            "amazon_24m": "YES", "commit": "YES",
        }), signing_secret="test-secret")
        self.assertEqual(4, outcome.response["type"])
        self.assertEqual(64, outcome.response["data"]["flags"])
        button = outcome.response["data"]["components"][0]["components"][0]
        self.assertTrue(button["custom_id"].startswith("tester_apply_v2_continue2."))
        self.assertEqual("Continue To Final Step", button["label"])
        self.assertLessEqual(len(button["custom_id"]), 100)

    async def test_ineligible_step1_stops_before_collecting_more_data(self):
        outcome = await program.build_interaction_outcome(_modal_submit("tester_apply_v2_step1", {
            "country": "United States", "age": "YES", "devices": "Steam Deck and PC",
            "amazon_24m": "YES", "commit": "YES",
        }), signing_secret="test-secret")
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("not eligible", outcome.response["data"]["content"].lower())
        restart = outcome.response["data"]["components"][0]["components"][0]
        self.assertEqual("tester_apply_start", restart["custom_id"])

    async def test_signed_continue_button_opens_second_modal(self):
        custom_id = (await program.build_interaction_outcome(_modal_submit("tester_apply_v2_step1", {
            "country": "Canada", "age": "YES", "devices": "Switch 2",
            "amazon_24m": "YES", "commit": "YES",
        }), signing_secret="test-secret")).response["data"]["components"][0]["components"][0]["custom_id"]
        outcome = await program.build_interaction_outcome({
            "type": 3, "data": {"custom_id": custom_id},
            "member": {"user": {"id": "123", "username": "tester"}},
        }, signing_secret="test-secret")
        self.assertEqual(9, outcome.response["type"])
        self.assertTrue(outcome.response["data"]["custom_id"].startswith("tester_apply_v2_step2."))
        self.assertIn("Step 2 Of 2", outcome.response["data"]["title"])
        self.assertEqual(5, len(outcome.response["data"]["components"]))
        labels = [row["components"][0]["label"] for row in outcome.response["data"]["components"]]
        self.assertEqual([
            "Amazon, FUNLAB And Prime Profile",
            "Weekly Play Profile",
            "Favorite Game IPs Or Franchises",
            "Games, Platforms And Controllers",
            "What Matters Most In Gaming Accessories?",
        ], labels)
        self.assertTrue(all(len(label) <= 45 for label in labels))

    async def test_tampered_continue_state_is_rejected(self):
        outcome = await program.build_interaction_outcome({
            "type": 3, "data": {"custom_id": "tester_apply_v2_continue2.1-US-f.invalid"},
        }, signing_secret="test-secret")
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("expired or invalid", outcome.response["data"]["content"].lower())

    async def test_second_submission_saves_application_and_infers_route(self):
        ledger, notices = FakeLedger(), []

        async def notify(message):
            notices.append(message)

        outcome = await program.build_interaction_outcome(_modal_submit(await _open_step2_custom_id(), {
            "purchase_profile": "COUNT=4-6; FUNLAB=YES; PRIME=YES",
            "play_profile": "SWITCH=6-10; PC=2-5; CROSS=YES",
            "favorite_ips": "Pokémon； Zelda, mario; POKÉMON",
            "usage": "Mario Kart World on Switch 2; Hades on Steam Deck; Nintendo Pro Controller",
            "priorities": "Comfort； low latency, durability; COMFORT",
        }), signing_secret="test-secret", ledger=ledger, completion_notifier=notify)
        self.assertEqual(5, outcome.response["type"])
        self.assertIsNone(ledger.saved)
        await outcome.work()
        self.assertIn("Application received", notices[0])
        self.assertEqual("123", ledger.saved["Discord用户ID"])
        self.assertEqual(["Switch 2", "Steam Deck"], ledger.saved["设备"])
        self.assertEqual("Pokémon; Zelda; mario", ledger.saved["喜爱游戏IP"])
        self.assertEqual("Comfort; low latency; durability", ledger.saved["配件关注点"])
        self.assertEqual(ledger.saved["游戏与手柄使用经验"], ledger.saved["拟测试场景"])
        self.assertEqual("Switch + Steam Deck", ledger.saved["主测试路线"])
        for old_field in ("Amazon购买品类", "断连问题回答", "功能测试回答", "申请理由"):
            self.assertEqual("", ledger.saved[old_field])
        self.assertFalse(ledger.saved["可选加入Tester Alumni"])
        self.assertNotIn("收件地址", ledger.saved)
        self.assertIn("/47", ledger.saved["筛选结论"])

    async def test_second_submission_reports_background_save_failure(self):
        notices = []

        async def notify(message):
            notices.append(message)

        outcome = await program.build_interaction_outcome(_modal_submit(await _open_step2_custom_id(), {
            "purchase_profile": "COUNT=1; FUNLAB=NO; PRIME=NO",
            "play_profile": "SWITCH=2-5; PC=0; CROSS=NO",
            "favorite_ips": "Mario", "usage": "Mario Kart 8 Deluxe on Switch",
            "priorities": "Comfort",
        }), signing_secret="test-secret", ledger=FailingLedger(), completion_notifier=notify)
        await outcome.work()
        self.assertIn("could not save", notices[0])
        self.assertIn("marketing@fireflyfunlab.com", notices[0])

    async def test_pc_steam_hours_keep_score_and_infer_pc_route(self):
        ledger = FakeLedger()
        step2_custom_id = await _open_step2_custom_id(devices="Switch 2, PC Steam")
        outcome = await program.build_interaction_outcome(_modal_submit(step2_custom_id, {
            "purchase_profile": "COUNT=4-6; FUNLAB=YES; PRIME=YES",
            "play_profile": "SWITCH=6-10; PC=2-5; CROSS=YES",
            "favorite_ips": "Mario; Zelda",
            "usage": "Mario Kart World on Switch 2 and Rocket League on PC; Nintendo Pro and Xbox controllers",
            "priorities": "Low latency; comfort",
        }), signing_secret="test-secret", ledger=ledger)
        await outcome.work()
        self.assertEqual(39, ledger.saved["筛选分数"])
        self.assertEqual("Switch + PC Steam", ledger.saved["主测试路线"])

    async def test_preference_lists_reject_more_than_three_unique_items(self):
        outcome = await program.build_interaction_outcome(_modal_submit(await _open_step2_custom_id(), {
            "purchase_profile": "COUNT=2-3; FUNLAB=NO; PRIME=NO",
            "play_profile": "SWITCH=2-5; PC=0; CROSS=NO",
            "favorite_ips": "Mario; Zelda; Pokémon; Kirby",
            "usage": "Mario Kart 8 Deluxe on Switch", "priorities": "Comfort",
        }), signing_secret="test-secret")
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("up to 3", outcome.response["data"]["content"])
        restart = outcome.response["data"]["components"][0]["components"][0]
        self.assertEqual("tester_apply_start", restart["custom_id"])

    def test_route_inference_uses_the_approved_precedence(self):
        self.assertEqual("Switch + Steam Deck", program._infer_route(
            ["Switch 2", "Steam Deck", "PC / Steam"], "6–10"
        ))
        self.assertEqual("Switch + PC Steam", program._infer_route(
            ["Switch 2", "PC / Steam"], "2–5"
        ))
        self.assertEqual("Switch 2", program._infer_route(
            ["Switch 2", "PC / Steam"], "Under 2"
        ))
        self.assertEqual("Switch", program._infer_route(["Switch 1"], "0"))
        self.assertEqual(14, program._device_mask("Switch 2, Steam Deck, Steam"))

    async def test_old_v1_interactions_are_rejected_without_writing(self):
        ledger = FakeLedger()
        for interaction_type, custom_id in [
            (3, "tester_apply_continue2.old"), (5, "tester_apply_step1"),
            (5, "tester_apply_step2.old"), (3, "tester_apply_continue3.old"),
            (5, "tester_apply_step3.old"),
        ]:
            outcome = await program.build_interaction_outcome(
                {"type": interaction_type, "data": {"custom_id": custom_id}}, ledger=ledger
            )
            self.assertEqual(4, outcome.response["type"])
            self.assertIn("updated", outcome.response["data"]["content"].lower())
            self.assertIsNone(outcome.work)
        self.assertIsNone(ledger.saved)

    async def test_expired_v2_draft_is_rejected_without_writing(self):
        ledger = FakeLedger()
        custom_id = await _open_step2_custom_id()
        program._drafts.clear()
        outcome = await program.build_interaction_outcome(_modal_submit(custom_id, {
            "purchase_profile": "COUNT=1; FUNLAB=NO; PRIME=NO",
            "play_profile": "SWITCH=2-5; PC=0; CROSS=NO",
            "favorite_ips": "Mario", "usage": "Mario Kart 8 Deluxe on Switch",
            "priorities": "Comfort",
        }), signing_secret="test-secret", ledger=ledger)
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("expired", outcome.response["data"]["content"].lower())
        self.assertIsNone(outcome.work)
        self.assertIsNone(ledger.saved)

    async def test_v2_draft_expires_after_thirty_minutes(self):
        program._drafts.clear()
        with patch.object(program.time, "time", return_value=1_000):
            first = await program.build_interaction_outcome(_modal_submit("tester_apply_v2_step1", {
                "country": "United States", "age": "YES", "devices": "Switch 2",
                "amazon_24m": "YES", "commit": "YES",
            }), signing_secret="test-secret")
        continue_id = first.response["data"]["components"][0]["components"][0]["custom_id"]
        with patch.object(program.time, "time", return_value=2_801):
            outcome = await program.build_interaction_outcome(
                {"type": 3, "data": {"custom_id": continue_id}}, signing_secret="test-secret"
            )
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("expired", outcome.response["data"]["content"].lower())

    def test_preference_boundaries_reject_empty_item_and_total_overflow(self):
        base = {
            "purchase_profile": "COUNT=2-3; FUNLAB=NO; PRIME=NO",
            "play_profile": "SWITCH=2-5; PC=0; CROSS=NO",
            "favorite_ips": "Mario", "usage": "Mario Kart 8 Deluxe on Switch",
            "priorities": "Comfort",
        }
        cases = [
            ({"favorite_ips": ""}, "at least one game IP"),
            ({"favorite_ips": "x" * 81}, "80 characters"),
            ({"favorite_ips": ";".join(["a" * 80, "b" * 80, "c" * 80])}, "too long"),
            ({"priorities": ""}, "at least one accessory priority"),
            ({"priorities": "x" * 61}, "60 characters"),
            ({"priorities": ";".join(["a" * 60, "b" * 60, "c" * 60])}, "too long"),
            ({"priorities": "Comfort; latency; durability; weight"}, "up to 3"),
        ]
        for override, expected in cases:
            with self.subTest(expected=expected):
                parsed, error = program._step2_values({**base, **override})
                self.assertEqual({}, parsed)
                self.assertIn(expected, error)

    def test_purchase_and_pc_score_boundaries(self):
        base = {"pc_hours": "0", "cross": False}
        expected = {"1": 18, "2–3": 21, "4–6": 24, "7+": 27}
        for purchase_count, score in expected.items():
            with self.subTest(purchase_count=purchase_count):
                self.assertEqual(score, program._provisional_score(
                    ["Switch 1"], {**base, "purchase_count": purchase_count}
                ))
        for hours in ("2–5", "6–10", "10+"):
            with self.subTest(hours=hours):
                self.assertEqual(32, program._provisional_score(
                    ["Switch 1", "PC / Steam"],
                    {"purchase_count": "7+", "pc_hours": hours, "cross": False},
                ))
        self.assertEqual(27, program._provisional_score(
            ["Switch 1", "PC / Steam"],
            {"purchase_count": "7+", "pc_hours": "Under 2", "cross": False},
        ))
        self.assertEqual(47, program._provisional_score(
            ["Switch 2", "Steam Deck", "PC / Steam"],
            {"purchase_count": "7+", "pc_hours": "10+", "cross": True},
        ))

    async def test_ledger_updates_same_record_for_same_user_and_batch(self):
        api = AsyncMock(side_effect=[
            {"code": 0, "data": {"items": []}},
            {"code": 0, "data": {"record": {"record_id": "rec_same"}}},
            {"code": 0, "data": {"items": [{"record_id": "rec_same"}]}},
            {"code": 0, "data": {}},
        ])
        retired = {
            "Amazon购买品类": "", "断连问题回答": "",
            "功能测试回答": "", "申请理由": "",
        }
        fields = {
            "Discord用户ID": "123", "活动批次": "batch",
            "喜爱游戏IP": "Mario", **retired,
        }
        with patch("app.feishu.api", api):
            ledger = program.DiscordTesterLedger()
            first_id = await ledger.save_application(fields)
            second_id = await ledger.save_application({**fields, "喜爱游戏IP": "Zelda"})
        self.assertEqual("rec_same", first_id)
        self.assertEqual(first_id, second_id)
        self.assertEqual("PUT", api.await_args_list[-1].args[0])
        self.assertEqual("Zelda", api.await_args_list[-1].args[2]["fields"]["喜爱游戏IP"])
        for field in retired:
            self.assertEqual("", api.await_args_list[-1].args[2]["fields"][field])

    async def test_interaction_ack_returns_within_three_seconds_before_ledger_write(self):
        ledger = FakeLedger()
        payload = _modal_submit(await _open_step2_custom_id(), {
            "purchase_profile": "COUNT=1; FUNLAB=NO; PRIME=NO",
            "play_profile": "SWITCH=2-5; PC=0; CROSS=NO",
            "favorite_ips": "Mario", "usage": "Mario Kart 8 Deluxe on Switch",
            "priorities": "Comfort",
        })
        started = time.perf_counter()
        outcome = await program.build_interaction_outcome(
            payload, signing_secret="test-secret", ledger=ledger
        )
        self.assertLess(time.perf_counter() - started, 3)
        self.assertEqual(5, outcome.response["type"])
        self.assertIsNone(ledger.saved)

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
        self.assertIn("2-step application", message["content"])


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


async def _open_step2_custom_id(devices="Switch 2, Steam Deck"):
    first = await program.build_interaction_outcome(_modal_submit("tester_apply_v2_step1", {
        "country": "United States", "age": "YES", "devices": devices,
        "amazon_24m": "YES", "commit": "YES",
    }), signing_secret="test-secret")
    continue_id = first.response["data"]["components"][0]["components"][0]["custom_id"]
    second = await program.build_interaction_outcome({"type": 3, "data": {"custom_id": continue_id}}, signing_secret="test-secret")
    return second.response["data"]["custom_id"]


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
