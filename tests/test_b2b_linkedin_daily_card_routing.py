import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import b2b_linkedin_daily_card as daily_card


def _lead(index: int) -> dict:
    return {
        "record_id": f"rec_{index}",
        "company": f"Company {index}",
        "contact": f"Contact {index}",
        "position": "Buyer",
        "country": "United States",
        "company_type": "Distributor",
        "grade": "A-优先开发",
        "score": 90,
        "dev_status": "待开发",
        "reach_status": "待触达",
        "crm_match": "新线索",
        "icp": "是",
        "linkedin_company": "https://www.linkedin.com/company/example",
        "linkedin_profile": "https://www.linkedin.com/in/example",
        "website": "https://example.com",
        "url": f"https://example.test/records/rec_{index}",
        "reason": "Matches the B2B development criteria.",
        "connect_copy": "Hello, open to connect?",
        "message_copy": "Thanks for connecting.",
        "owner": "未分配",
        "next_action": "",
        "note": "",
        "batch": "test-batch",
    }


class B2BLinkedInDailyCardRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_missing_private_mapping_fails_before_any_write_or_send(self):
        old_mapping = os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1), _lead(2), _lead(3)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})) as api,
                patch.object(
                    daily_card.feishu,
                    "send_card_via_b2b_assistant",
                    AsyncMock(return_value="om_unexpected"),
                ) as send_card,
            ):
                with self.assertRaisesRegex(ValueError, "missing private notify target"):
                    await daily_card.run(commit=True, notify=True, limit=1)

            api.assert_not_awaited()
            send_card.assert_not_awaited()
        finally:
            if old_mapping is not None:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_group_mapping_is_rejected_before_any_write_or_send(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {"吴晓丹": "chat_id:oc_group_must_not_receive_daily_cards"},
            ensure_ascii=False,
        )
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})) as api,
                patch.object(
                    daily_card.feishu,
                    "send_card_via_b2b_assistant",
                    AsyncMock(return_value="om_unexpected"),
                ) as send_card,
            ):
                with self.assertRaisesRegex(ValueError, "private target required"):
                    await daily_card.run(commit=True, notify=True, limit=1)

            api.assert_not_awaited()
            send_card.assert_not_awaited()
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_complete_owner_mapping_sends_each_card_to_a_private_target(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {
                "吴晓丹": "union_id:on_owner_one",
                "冼浩华": "union_id:on_owner_two",
                "李桐欣": "union_id:on_owner_three",
            },
            ensure_ascii=False,
        )
        send_card = AsyncMock(side_effect=["om_one", "om_two", "om_three"])
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1), _lead(2), _lead(3)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})),
                patch.object(daily_card.feishu, "send_card_via_b2b_assistant", send_card),
            ):
                result = await daily_card.run(commit=True, notify=True, limit=1)

            self.assertEqual(3, len(result["message_ids"]))
            self.assertEqual([], result["send_errors"])
            self.assertEqual(
                {
                    ("union_id", "on_owner_one"),
                    ("union_id", "on_owner_two"),
                    ("union_id", "on_owner_three"),
                },
                {(call.args[0], call.args[1]) for call in send_card.await_args_list},
            )
            self.assertNotIn("chat_id", {call.args[0] for call in send_card.await_args_list})
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_partial_or_wildcard_mapping_fails_closed_for_the_whole_batch(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        cases = {
            "partial": {
                "吴晓丹": "union_id:on_owner_one",
                "冼浩华": "union_id:on_owner_two",
            },
            "wildcard": {"*": "union_id:on_someone_else"},
        }
        try:
            for label, mapping in cases.items():
                with self.subTest(label=label):
                    os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
                        mapping,
                        ensure_ascii=False,
                    )
                    api = AsyncMock(return_value={"code": 0})
                    send_card = AsyncMock(return_value="om_unexpected")
                    with (
                        patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1), _lead(2), _lead(3)])),
                        patch.object(daily_card.feishu, "api", api),
                        patch.object(daily_card.feishu, "send_card_via_b2b_assistant", send_card),
                    ):
                        with self.assertRaisesRegex(ValueError, "missing private notify target"):
                            await daily_card.run(commit=True, notify=True, limit=1)

                    api.assert_not_awaited()
                    send_card.assert_not_awaited()
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_duplicate_private_target_fails_before_any_write_or_send(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {
                "吴晓丹": "union_id:on_shared_target",
                "冼浩华": "union_id:on_shared_target",
                "李桐欣": "union_id:on_owner_three",
            },
            ensure_ascii=False,
        )
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1), _lead(2), _lead(3)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})) as api,
                patch.object(
                    daily_card.feishu,
                    "send_card_via_b2b_assistant",
                    AsyncMock(return_value="om_unexpected"),
                ) as send_card,
            ):
                with self.assertRaisesRegex(ValueError, "duplicate private notify target"):
                    await daily_card.run(commit=True, notify=True, limit=1)

            api.assert_not_awaited()
            send_card.assert_not_awaited()
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_missing_idle_owner_mapping_still_fails_before_any_write_or_send(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {"吴晓丹": "union_id:on_owner_one"},
            ensure_ascii=False,
        )
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})) as api,
                patch.object(
                    daily_card.feishu,
                    "send_card_via_b2b_assistant",
                    AsyncMock(return_value="om_unexpected"),
                ) as send_card,
            ):
                with self.assertRaisesRegex(ValueError, "missing private notify target"):
                    await daily_card.run(
                        commit=True,
                        notify=True,
                        limit=1,
                        owner="吴晓丹",
                    )

            api.assert_not_awaited()
            send_card.assert_not_awaited()
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_invalid_dict_target_fails_before_any_write_or_send(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = json.dumps(
            {
                "吴晓丹": {"receive_type": "open_id", "receive_id": "   "},
                "冼浩华": "union_id:on_owner_two",
                "李桐欣": "union_id:on_owner_three",
            },
            ensure_ascii=False,
        )
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})) as api,
                patch.object(
                    daily_card.feishu,
                    "send_card_via_b2b_assistant",
                    AsyncMock(return_value="om_unexpected"),
                ) as send_card,
            ):
                with self.assertRaisesRegex(ValueError, "private notify target"):
                    await daily_card.run(
                        commit=True,
                        notify=True,
                        limit=1,
                        owner="吴晓丹",
                    )

            api.assert_not_awaited()
            send_card.assert_not_awaited()
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_dry_run_still_works_without_notification_mapping(self):
        old_mapping = os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        send_card = AsyncMock(return_value="om_unexpected")
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1)])),
                patch.object(daily_card.feishu, "send_card_via_b2b_assistant", send_card),
            ):
                result = await daily_card.run(commit=False, notify=False, limit=1)

            self.assertEqual(1, result["eligible_total"])
            self.assertEqual([], result["assignment_updates"])
            self.assertEqual([], result["message_ids"])
            send_card.assert_not_awaited()
        finally:
            if old_mapping is not None:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_frankie_only_stays_private_without_owner_mapping(self):
        old_mapping = os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        send_card = AsyncMock(side_effect=["om_one", "om_two", "om_three"])
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1), _lead(2), _lead(3)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})),
                patch.object(daily_card.feishu, "send_card_via_b2b_assistant", send_card),
            ):
                result = await daily_card.run(
                    commit=True,
                    notify=True,
                    limit=1,
                    frankie_only=True,
                )

            self.assertEqual(3, len(result["message_ids"]))
            self.assertEqual([], result["send_errors"])
            self.assertEqual(
                {
                    ("email", daily_card.B2B_LINKEDIN_FRANKIE_EMAIL),
                },
                {(call.args[0], call.args[1]) for call in send_card.await_args_list},
            )
        finally:
            if old_mapping is not None:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners

    async def test_invalid_mapping_json_fails_before_any_write_or_send(self):
        old_mapping = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON")
        old_owners = os.environ.pop("B2B_LINKEDIN_DISPATCH_OWNERS", None)
        os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = "{not-valid-json"
        try:
            with (
                patch.object(daily_card, "_eligible_rows", AsyncMock(return_value=[_lead(1)])),
                patch.object(daily_card.feishu, "api", AsyncMock(return_value={"code": 0})) as api,
                patch.object(
                    daily_card.feishu,
                    "send_card_via_b2b_assistant",
                    AsyncMock(return_value="om_unexpected"),
                ) as send_card,
            ):
                with self.assertRaisesRegex(ValueError, "missing private notify target"):
                    await daily_card.run(commit=True, notify=True, limit=1)

            api.assert_not_awaited()
            send_card.assert_not_awaited()
        finally:
            if old_mapping is None:
                os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            else:
                os.environ["B2B_LINKEDIN_OWNER_NOTIFY_JSON"] = old_mapping
            if old_owners is not None:
                os.environ["B2B_LINKEDIN_DISPATCH_OWNERS"] = old_owners


if __name__ == "__main__":
    unittest.main()
