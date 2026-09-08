import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import b2b_linkedin_daily_card as daily_card


class B2BLinkedInPoolSummaryRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.old_env = {
            key: os.environ.get(key)
            for key in [
                "B2B_LINKEDIN_OWNER_NOTIFY_JSON",
                "B2B_LINKEDIN_POOL_SUMMARY_OWNER",
            ]
        }
        self.original_list_records = daily_card._list_records
        self.original_send_card = daily_card.feishu.send_card_via_b2b_assistant
        self.sent = []

        async def fake_list_records(**_kwargs):
            return []

        async def fake_send_card(receive_type, receive_id, card):
            self.sent.append((receive_type, receive_id, card))
            return "om_pool_summary"

        daily_card._list_records = fake_list_records
        daily_card.feishu.send_card_via_b2b_assistant = fake_send_card

    async def asyncTearDown(self):
        daily_card._list_records = self.original_list_records
        daily_card.feishu.send_card_via_b2b_assistant = self.original_send_card
        for key, value in self.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    async def test_summary_sends_only_to_configured_owner(self):
        os.environ["B2B_LINKEDIN_POOL_SUMMARY_OWNER"] = "吴晓丹"
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {"吴晓丹": "open_id:ou_b2b_owner"},
            ensure_ascii=False,
        )

        result = await daily_card.run_pool_summary(commit=True, notify=True)

        self.assertEqual("om_pool_summary", result["message_id"])
        self.assertEqual("", result["send_error"])
        self.assertEqual(1, len(self.sent))
        self.assertEqual(("open_id", "ou_b2b_owner"), self.sent[0][:2])
        self.assertNotEqual(daily_card.B2B_GROUP_CHAT_ID, self.sent[0][1])

    async def test_missing_owner_mapping_never_falls_back_to_group(self):
        os.environ["B2B_LINKEDIN_POOL_SUMMARY_OWNER"] = "吴晓丹"
        os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)

        result = await daily_card.run_pool_summary(commit=True, notify=True)

        self.assertEqual("", result["message_id"])
        self.assertIn("missing private notify target", result["send_error"])
        self.assertEqual([], self.sent)

    async def test_group_target_is_rejected_for_summary(self):
        os.environ["B2B_LINKEDIN_POOL_SUMMARY_OWNER"] = "吴晓丹"
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {"吴晓丹": "chat_id:oc_any_group_or_chat"},
            ensure_ascii=False,
        )

        result = await daily_card.run_pool_summary(commit=True, notify=True)

        self.assertEqual("", result["message_id"])
        self.assertIn("private target required", result["send_error"])
        self.assertEqual([], self.sent)

    async def test_wildcard_mapping_is_rejected_for_summary(self):
        os.environ["B2B_LINKEDIN_POOL_SUMMARY_OWNER"] = "吴晓丹"
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {"*": "open_id:ou_someone_else"},
            ensure_ascii=False,
        )

        result = await daily_card.run_pool_summary(commit=True, notify=True)

        self.assertEqual("", result["message_id"])
        self.assertIn("missing private notify target", result["send_error"])
        self.assertEqual([], self.sent)

    async def test_frankie_only_preview_stays_private(self):
        os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)

        result = await daily_card.run_pool_summary(
            commit=True,
            notify=True,
            frankie_only=True,
        )

        self.assertEqual("om_pool_summary", result["message_id"])
        self.assertEqual("", result["send_error"])
        self.assertEqual(
            ("email", daily_card.B2B_LINKEDIN_FRANKIE_EMAIL),
            self.sent[0][:2],
        )


if __name__ == "__main__":
    unittest.main()
