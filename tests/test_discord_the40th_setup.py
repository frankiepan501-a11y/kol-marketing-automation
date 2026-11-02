import unittest
from unittest.mock import Mock

from app import discord_the40th_setup as setup


class The40thDiscordSetupTests(unittest.TestCase):
    def test_fetch_snapshot_is_read_only_and_uses_authenticated_bot_identity(self):
        client = Mock()
        responses = {
            "/users/@me": {"id": "bot"},
            "/guilds/guild/roles": [{"id": "guild", "name": "@everyone"}],
            "/guilds/guild/channels": [],
            "/guilds/guild/members/bot": {"roles": []},
        }

        def request(method, url, **kwargs):
            response = Mock()
            response.status_code = 200
            response.content = b"{}"
            response.json.return_value = responses[url.removeprefix(setup.DISCORD_API)]
            return response

        client.request.side_effect = request
        snapshot = setup.fetch_discord_snapshot(
            client=client, token="secret", guild_id="guild"
        )

        self.assertEqual("bot", snapshot["bot_user_id"])
        self.assertEqual(4, client.request.call_count)
        self.assertEqual(
            {"GET"},
            {call.args[0] for call in client.request.call_args_list},
        )
        for call in client.request.call_args_list:
            self.assertEqual("Bot secret", call.kwargs["headers"]["Authorization"])

    def test_audit_blocks_admin_bot_and_tester_selected_channel_access(self):
        roles = [
            {"id": "guild", "name": "@everyone", "position": 0, "permissions": "0"},
            {"id": "bot-role", "name": "FUN Bot", "position": 10,
             "permissions": str(setup.ADMINISTRATOR | setup.MANAGE_ROLES)},
            {"id": "selected", "name": "Tester Selected", "position": 2, "permissions": "0"},
        ]
        channels = [{
            "id": "private", "name": "legacy-private", "type": 0,
            "permission_overwrites": [
                {"id": "selected", "type": 0, "allow": str(setup.VIEW_CHANNEL), "deny": "0"},
            ],
        }]
        audit = setup.audit_discord_state(
            guild_id="guild", roles=roles, channels=channels,
            bot_member={"roles": ["bot-role"]},
        )
        self.assertTrue(audit["bot_has_administrator"])
        self.assertEqual(["legacy-private"], audit["tester_selected_view_channels"])
        self.assertFalse(audit["safe_to_commit"])

    def test_audit_requires_bot_above_all_activity_roles_and_minimum_permissions(self):
        roles = [
            {"id": "guild", "name": "@everyone", "position": 0, "permissions": "0"},
            {"id": "bot-role", "name": "FUN Bot", "position": 4,
             "permissions": str(setup.MANAGE_ROLES | setup.MANAGE_CHANNELS)},
            {"id": "applicant", "name": "Applicant", "position": 1, "permissions": "0"},
            {"id": "first", "name": "The First 20", "position": 2, "permissions": "0"},
            {"id": "selected", "name": "Tester Selected", "position": 3, "permissions": "0"},
            {"id": "forty", "name": "The 40", "position": 5, "permissions": "0"},
        ]
        audit = setup.audit_discord_state(
            guild_id="guild", roles=roles, channels=[],
            bot_member={"roles": ["bot-role"]},
        )
        self.assertFalse(audit["bot_above_activity_roles"])
        self.assertTrue(audit["bot_can_manage_roles"])
        self.assertTrue(audit["bot_can_manage_channels"])
        self.assertFalse(audit["safe_to_commit"])

    def test_plan_uses_three_private_areas_and_keeps_tracks_separate(self):
        role_ids = {
            "Applicant": "applicant",
            "The First 20": "first",
            "Tester Selected": "selected",
            "The 40": "forty",
        }
        plan = setup.build_resource_plan(
            guild_id="guild", bot_user_id="bot", role_ids=role_ids,
            product_role_ids=["product"], operations_role_ids=["ops"],
        )
        self.assertEqual(
            ["the-first-20", "feedback", "the-40"],
            [channel["name"] for channel in plan["channels"]],
        )
        by_name = {channel["name"]: channel for channel in plan["channels"]}
        first_overwrites = {item["id"]: item for item in by_name["the-first-20"]["permission_overwrites"]}
        forty_overwrites = {item["id"]: item for item in by_name["the-40"]["permission_overwrites"]}
        self.assertTrue(int(first_overwrites["first"]["allow"]) & setup.VIEW_CHANNEL)
        self.assertTrue(int(first_overwrites["forty"]["deny"]) & setup.VIEW_CHANNEL)
        self.assertTrue(int(forty_overwrites["forty"]["allow"]) & setup.VIEW_CHANNEL)
        self.assertTrue(int(forty_overwrites["first"]["deny"]) & setup.VIEW_CHANNEL)
        self.assertNotIn("selected", first_overwrites)
        self.assertNotIn("selected", forty_overwrites)

    def test_plan_enforces_the_full_private_area_permission_matrix(self):
        role_ids = {
            "Applicant": "applicant",
            "The First 20": "first",
            "Tester Selected": "selected",
            "The 40": "forty",
        }
        plan = setup.build_resource_plan(
            guild_id="guild", bot_user_id="bot", role_ids=role_ids,
            product_role_ids=["product"], operations_role_ids=["ops"],
        )
        by_name = {
            channel["name"]: {
                item["id"]: item for item in channel["permission_overwrites"]
            }
            for channel in plan["channels"]
        }

        for overwrites in by_name.values():
            self.assertTrue(int(overwrites["guild"]["deny"]) & setup.VIEW_CHANNEL)
            self.assertTrue(int(overwrites["bot"]["allow"]) & setup.VIEW_CHANNEL)
            self.assertNotIn("applicant", overwrites)
            self.assertNotIn("selected", overwrites)

        self.assertTrue(int(by_name["the-first-20"]["product"]["allow"]) & setup.VIEW_CHANNEL)
        self.assertTrue(int(by_name["feedback"]["product"]["allow"]) & setup.VIEW_CHANNEL)
        self.assertTrue(int(by_name["the-40"]["product"]["deny"]) & setup.VIEW_CHANNEL)
        for overwrites in by_name.values():
            self.assertTrue(int(overwrites["ops"]["allow"]) & setup.VIEW_CHANNEL)


    def test_plan_refuses_missing_product_or_operations_roles(self):
        role_ids = {name: name for name in setup.ACTIVITY_ROLE_NAMES}
        with self.assertRaisesRegex(ValueError, "product/technical"):
            setup.build_resource_plan(
                guild_id="guild", bot_user_id="bot", role_ids=role_ids,
                product_role_ids=[], operations_role_ids=["ops"],
            )
        with self.assertRaisesRegex(ValueError, "operations"):
            setup.build_resource_plan(
                guild_id="guild", bot_user_id="bot", role_ids=role_ids,
                product_role_ids=["product"], operations_role_ids=[],
            )


if __name__ == "__main__":
    unittest.main()
