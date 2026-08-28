import unittest

import httpx

from app.discord_tester_role_sync import (
    DiscordSelectedRoleAssigner,
    SelectedRoleSync,
    build_runtime,
)


class FakeLedger:
    def __init__(self, records):
        self.records = records

    async def list_applications(self):
        return self.records


class FakeAssigner:
    def __init__(self, *, existing=(), fail_for=()):
        self.existing = set(existing)
        self.fail_for = set(fail_for)
        self.calls = []

    async def ensure_selected(self, user_id):
        self.calls.append(user_id)
        if user_id in self.fail_for:
            raise RuntimeError("simulated Discord error")
        if user_id in self.existing:
            return False
        self.existing.add(user_id)
        return True


def record(user_id, *, application="已提交", verification="未开始"):
    return {"record_id": f"rec-{user_id}", "fields": {
        "Discord用户ID": user_id,
        "报名状态": application,
        "核验状态": verification,
    }}


class SelectedRoleSyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_verified_and_selected_records_are_assigned(self):
        ledger = FakeLedger([
            record("eligible", application="已入选", verification="已通过"),
            record("unverified", application="已入选", verification="未开始"),
            record("not-selected", application="已提交", verification="已通过"),
        ])
        assigner = FakeAssigner()

        result = await SelectedRoleSync(ledger=ledger, assigner=assigner).run_once()

        self.assertEqual(["eligible"], assigner.calls)
        self.assertEqual({"scanned": 3, "eligible": 1, "added": 1,
                          "already_present": 0, "failed": 0}, result)

    async def test_existing_role_is_idempotent_and_one_failure_does_not_block_others(self):
        ledger = FakeLedger([
            record("existing", application="已入选", verification="已通过"),
            record("failed", application="已入选", verification="已通过"),
            record("ok", application="已入选", verification="已通过"),
        ])
        assigner = FakeAssigner(existing={"existing"}, fail_for={"failed"})

        result = await SelectedRoleSync(ledger=ledger, assigner=assigner).run_once()

        self.assertEqual(1, result["already_present"])
        self.assertEqual(1, result["failed"])
        self.assertEqual(1, result["added"])

    async def test_discord_write_is_confirmed_by_member_readback(self):
        calls = []
        reads = 0

        def handler(request):
            nonlocal reads
            calls.append((request.method, request.url.path))
            if request.url.path.endswith("/roles") and request.method == "GET":
                return httpx.Response(200, json=[{"id": "selected", "name": "Tester Selected"}])
            if request.url.path.endswith("/members/user-1") and request.method == "GET":
                reads += 1
                return httpx.Response(200, json={"roles": [] if reads == 1 else ["selected"]})
            if request.url.path.endswith("/members/user-1/roles/selected"):
                return httpx.Response(204)
            return httpx.Response(404)

        assigner = DiscordSelectedRoleAssigner(
            token="token", guild_id="guild", transport=httpx.MockTransport(handler),
        )

        self.assertTrue(await assigner.ensure_selected("user-1"))
        self.assertEqual(2, reads)
        self.assertIn(("PUT", "/api/v10/guilds/guild/members/user-1/roles/selected"), calls)

    def test_runtime_is_default_off_and_requires_bot_configuration(self):
        self.assertIsNone(build_runtime({}))
        with self.assertRaises(ValueError):
            build_runtime({"DISCORD_TESTER_ROLE_SYNC_ENABLED": "1"})
        self.assertIsNotNone(build_runtime({
            "DISCORD_TESTER_ROLE_SYNC_ENABLED": "1",
            "DISCORD_BOT_TOKEN": "token",
            "DISCORD_FUNLAB_GUILD_ID": "guild",
        }))


if __name__ == "__main__":
    unittest.main()
