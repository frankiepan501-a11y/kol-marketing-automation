import asyncio
import os
import sys
import unittest

from fastapi import HTTPException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

for _key in (
    "FEISHU_NOTIFY_APP_ID",
    "FEISHU_NOTIFY_APP_SECRET",
    "FEISHU_APP3_ID",
    "FEISHU_APP3_SECRET",
    "FEISHU_APP_TOKEN",
    "T_KOL",
    "T_EDITOR",
    "T_DRAFT",
    "T_KOL_FU",
    "T_EDITOR_FU",
    "T_DASH",
    "T_PRODUCT",
    "T_TASK_KOL",
    "T_TASK_EDITOR",
    "SNOV_CLIENT_ID",
    "SNOV_CLIENT_SECRET",
    "INTERNAL_TOKEN",
):
    os.environ.setdefault(_key, "test")

from app import main  # noqa: E402


class AmzMessageRevokeTests(unittest.TestCase):
    def setUp(self):
        self.old_token = main.config.INTERNAL_TOKEN
        main.config.INTERNAL_TOKEN = "unit-token"

    def tearDown(self):
        main.config.INTERNAL_TOKEN = self.old_token

    def test_revoke_requires_explicit_confirm(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                main.revoke_amz_message(
                    authorization="Bearer unit-token",
                    message_id="om_test",
                    confirm=False,
                )
            )
        self.assertEqual(400, ctx.exception.status_code)

    def test_revoke_rejects_non_message_id(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                main.revoke_amz_message(
                    authorization="Bearer unit-token",
                    message_id="bad_id",
                    confirm=True,
                )
            )
        self.assertEqual(400, ctx.exception.status_code)

    def test_revoke_returns_feishu_status(self):
        original = main.amz_assistant.delete_message

        async def fake_delete(message_id):
            return {"code": 0, "msg": "success"}

        try:
            main.amz_assistant.delete_message = fake_delete
            result = asyncio.run(
                main.revoke_amz_message(
                    authorization="Bearer unit-token",
                    message_id="om_test",
                    confirm=True,
                    reason="unit test",
                )
            )
        finally:
            main.amz_assistant.delete_message = original

        self.assertTrue(result["ok"])
        self.assertEqual("om_test", result["message_id"])
        self.assertEqual(0, result["feishu_code"])


if __name__ == "__main__":
    unittest.main()
