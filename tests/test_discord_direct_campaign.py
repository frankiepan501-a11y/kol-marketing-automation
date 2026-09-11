import os
import unittest
from unittest.mock import MagicMock, patch

from app import discord_direct_campaign as campaign
from app import discord_direct_campaign_config as campaign_config

EXPECTED_POLL_QUESTION = (
    "Two Directs in two days — which reveal made you reach for your controller first?"
)
EXPECTED_POLL_ANSWERS = [
    "Ocarina of Time remake",
    "Metroid Ravenous",
    "Kirby and the World Beyond",
    "Monster Hunter Wilds",
    "Persona 6 / Persona 4 Revival",
    "Mario Kart World update",
    "Something else — reply below",
]


class DiscordDirectCampaignTests(unittest.TestCase):
    def test_public_payload_combines_original_visual_links_and_opt_in_button(self):
        payload = campaign.public_message_payload("funlab-direct-community-vote-1200x675.png")

        self.assertIn(campaign.PUBLIC_MARKER, payload["content"])
        self.assertIn("https://www.nintendo.com/us/nintendo-direct/9-8-2026/", payload["content"])
        self.assertIn("https://www.nintendo.com/us/nintendo-direct/9-9-2026/", payload["content"])
        self.assertIn("not affiliated with or endorsed by Nintendo", payload["content"])
        self.assertNotIn("@everyone", payload["content"])
        self.assertEqual({"parse": []}, payload["allowed_mentions"])
        self.assertEqual("tester_direct_dm", payload["components"][0]["components"][0]["custom_id"])
        self.assertEqual(0, payload["attachments"][0]["id"])
        self.assertNotIn("poll", payload)
        self.assertLessEqual(len(payload["nonce"]), 25)

    def test_poll_payload_is_a_separate_attachment_free_message(self):
        payload = campaign.poll_message_payload(channel_name=campaign_config.PUBLIC_CHANNEL)

        self.assertIn(campaign.POLL_MARKER, payload["content"])
        self.assertNotIn("attachments", payload)
        self.assertNotIn("components", payload)
        self.assertEqual({"parse": []}, payload["allowed_mentions"])
        self.assertEqual(72, payload["poll"]["duration"])
        self.assertFalse(payload["poll"]["allow_multiselect"])
        self.assertEqual(7, len(payload["poll"]["answers"]))
        self.assertNotIn("attachment_ids", payload["poll"])
        self.assertLessEqual(len(payload["nonce"]), 25)

    def test_channel_resolution_requires_one_exact_text_channel(self):
        channels = [
            {"id": "1", "name": "general-chat", "type": 0},
            {"id": "2", "name": "general", "type": 0},
            {"id": "3", "name": "general", "type": 4},
        ]
        self.assertEqual("2", campaign.find_channel_id(channels, "general"))
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            campaign.find_channel_id(channels, "missing")

    def test_rehearsal_public_and_poll_posts_use_distinct_stable_nonces(self):
        rehearsal_main = campaign.public_message_payload(
            "visual.png", channel_name=campaign_config.REHEARSAL_CHANNEL
        )["nonce"]
        rehearsal_poll = campaign.poll_message_payload(
            channel_name=campaign_config.REHEARSAL_CHANNEL
        )["nonce"]
        public_main = campaign.public_message_payload(
            "visual.png", channel_name=campaign_config.PUBLIC_CHANNEL
        )["nonce"]
        public_poll = campaign.poll_message_payload(
            channel_name=campaign_config.PUBLIC_CHANNEL
        )["nonce"]

        self.assertEqual(4, len({rehearsal_main, rehearsal_poll, public_main, public_poll}))
        self.assertEqual(
            public_main,
            campaign.public_message_payload(
                "visual.png", channel_name=campaign_config.PUBLIC_CHANNEL
            )["nonce"],
        )
        self.assertTrue(all(len(item) <= 25 for item in {
            rehearsal_main, rehearsal_poll, public_main, public_poll
        }))

    def test_existing_public_marker_prevents_duplicate_post(self):
        messages = [
            {"id": "old", "author": {"id": "other"}, "content": campaign.PUBLIC_MARKER},
            {"id": "current", "author": {"id": campaign.DEFAULT_BOT_USER_ID},
             "content": f"**{campaign.PUBLIC_MARKER}**"},
        ]
        self.assertEqual(
            "current",
            campaign.existing_campaign_message_id(messages, marker=campaign.PUBLIC_MARKER),
        )

    def test_poll_readback_requires_native_poll_without_attachment(self):
        stored = {
            "id": "poll-message",
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.POLL_MARKER,
            "mention_everyone": False,
            "attachments": [],
            "components": [],
            "poll": {
                "question": {"text": EXPECTED_POLL_QUESTION},
                "answers": [
                    {"answer_id": index + 1, "poll_media": {"text": answer}}
                    for index, answer in enumerate(EXPECTED_POLL_ANSWERS)
                ],
                "allow_multiselect": False,
                "layout_type": 1,
            },
        }

        campaign._verify_stored_poll_message(
            stored,
            message_id="poll-message",
            bot_user_id=campaign.DEFAULT_BOT_USER_ID,
        )

    def test_main_readback_rejects_legacy_combined_poll(self):
        stored = {
            "id": "main-message",
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.PUBLIC_MARKER,
            "mention_everyone": False,
            "attachments": [{"filename": "visual.png"}],
            "components": [{"components": [{"custom_id": "tester_direct_dm"}]}],
            "poll": {"question": {"text": EXPECTED_POLL_QUESTION}},
        }

        with self.assertRaisesRegex(RuntimeError, "unexpectedly contains a poll"):
            campaign._verify_stored_message(
                stored,
                message_id="main-message",
                bot_user_id=campaign.DEFAULT_BOT_USER_ID,
                attachment_filename="visual.png",
            )

    def test_poll_readback_rejects_components(self):
        stored = {
            "id": "poll-message",
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.POLL_MARKER,
            "mention_everyone": False,
            "attachments": [],
            "components": [{"components": [{"custom_id": "unexpected"}]}],
            "poll": {
                "question": {"text": EXPECTED_POLL_QUESTION},
                "answers": [{"poll_media": {"text": "Wrong option"}}],
                "allow_multiselect": True,
                "layout_type": 1,
            },
        }

        with self.assertRaisesRegex(RuntimeError, "unexpectedly contains components"):
            campaign._verify_stored_poll_message(
                stored,
                message_id="poll-message",
                bot_user_id=campaign.DEFAULT_BOT_USER_ID,
            )

    def test_poll_readback_rejects_wrong_options(self):
        stored = {
            "id": "poll-message",
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.POLL_MARKER,
            "mention_everyone": False,
            "attachments": [],
            "components": [],
            "poll": {
                "question": {"text": EXPECTED_POLL_QUESTION},
                "answers": [{"poll_media": {"text": "Wrong option"}}],
                "allow_multiselect": False,
                "layout_type": 1,
            },
        }

        with self.assertRaisesRegex(RuntimeError, "wrong answer choices"):
            campaign._verify_stored_poll_message(
                stored,
                message_id="poll-message",
                bot_user_id=campaign.DEFAULT_BOT_USER_ID,
            )

    def test_partial_recovery_validates_existing_half_before_creating_missing_half(self):
        channels = [{"id": "hidden", "name": campaign_config.REHEARSAL_CHANNEL, "type": 0}]
        existing_main_id = str(campaign.CAMPAIGN_START_SNOWFLAKE + 1)
        existing_main = {
            "id": existing_main_id,
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.PUBLIC_MARKER,
        }
        invalid_main_readback = {
            "id": existing_main_id,
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.PUBLIC_MARKER,
            "mention_everyone": False,
            "attachments": [{"filename": campaign.DEFAULT_IMAGE.name}],
            "components": [],
        }
        responses = [
            {"id": campaign.DEFAULT_BOT_USER_ID},
            channels,
            [existing_main],
            [],
            invalid_main_readback,
        ]

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "test-token"}), \
                patch.object(campaign, "_request", side_effect=responses) as request:
            with self.assertRaisesRegex(RuntimeError, "opt-in DM button"):
                campaign.publish(channel_name=campaign_config.REHEARSAL_CHANNEL, commit=True)

        post_calls = [call for call in request.call_args_list if call.args[1] == "POST"]
        self.assertEqual([], post_calls)

    def test_poll_only_orphan_fails_closed_instead_of_reversing_message_order(self):
        channels = [{"id": "hidden", "name": campaign_config.REHEARSAL_CHANNEL, "type": 0}]
        existing_poll_id = str(campaign.CAMPAIGN_START_SNOWFLAKE + 2)
        existing_poll = {
            "id": existing_poll_id,
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.POLL_MARKER,
        }
        poll_readback = {
            **existing_poll,
            "mention_everyone": False,
            "attachments": [],
            "components": [],
            "poll": {
                "question": {"text": EXPECTED_POLL_QUESTION},
                "answers": [
                    {"answer_id": index + 1, "poll_media": {"text": answer}}
                    for index, answer in enumerate(EXPECTED_POLL_ANSWERS)
                ],
                "allow_multiselect": False,
                "layout_type": 1,
            },
        }
        created_main = {"id": "new-main"}
        main_readback = {
            "id": "new-main",
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": campaign.PUBLIC_MARKER,
            "mention_everyone": False,
            "attachments": [{"filename": campaign.DEFAULT_IMAGE.name}],
            "components": [{"components": [{"custom_id": "tester_direct_dm"}]}],
            "embeds": [{"url": url} for url in campaign.OFFICIAL_DIRECT_URLS],
        }
        responses = [
            {"id": campaign.DEFAULT_BOT_USER_ID},
            channels,
            [],
            [existing_poll],
            poll_readback,
            created_main,
            main_readback,
        ]

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "test-token"}), \
                patch.object(campaign, "_request", side_effect=responses) as request:
            with self.assertRaisesRegex(RuntimeError, "orphan poll"):
                campaign.publish(channel_name=campaign_config.REHEARSAL_CHANNEL, commit=True)

        post_calls = [call for call in request.call_args_list if call.args[1] == "POST"]
        self.assertEqual([], post_calls)

    def test_official_preview_urls_requires_both_direct_pages(self):
        stored = {
            "embeds": [
                {"url": "https://www.nintendo.com/us/nintendo-direct/9-8-2026/"},
                {"url": "https://www.nintendo.com/us/nintendo-direct/9-9-2026/"},
            ]
        }
        self.assertEqual(set(campaign.OFFICIAL_DIRECT_URLS), campaign._official_preview_urls(stored))

    def test_public_commit_requires_verified_rehearsal_message(self):
        with self.assertRaisesRegex(RuntimeError, "rehearsal message ID"):
            campaign.publish(channel_name="general", commit=True)

    def test_public_commit_requires_rehearsal_member_dm_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "rehearsal member ID"):
            campaign.publish(
                channel_name="general",
                commit=True,
                rehearsal_message_id="hidden-message",
            )

    def test_rehearsal_dm_requires_answered_interest_and_apply_button(self):
        client = MagicMock()
        answered = {
            "id": "dm-answer",
            "author": {"id": campaign.DEFAULT_BOT_USER_ID},
            "content": f"**{campaign_config.DM_MARKER}**\n\nThanks",
            "components": [{"type": 1, "components": [{
                "type": 2,
                "custom_id": "tester_apply_start.direct.oot",
            }]}],
        }
        with patch.object(campaign, "_request", side_effect=[{"id": "dm-channel"}, [answered]]):
            self.assertEqual(
                "dm-answer",
                campaign._verify_rehearsal_dm(
                    client,
                    token="token",
                    member_id="member",
                    bot_user_id=campaign.DEFAULT_BOT_USER_ID,
                ),
            )

    def test_duplicate_scan_pages_back_to_campaign_start(self):
        client = MagicMock()
        recent = [
            {"id": str(campaign.CAMPAIGN_START_SNOWFLAKE + 100 + index),
             "author": {"id": "other"}, "content": "chat"}
            for index in range(100)
        ]
        older = [{"id": str(campaign.CAMPAIGN_START_SNOWFLAKE + 50),
                  "author": {"id": campaign.DEFAULT_BOT_USER_ID},
                  "content": campaign.PUBLIC_MARKER}]
        with patch.object(campaign, "_request", side_effect=[recent, older]) as request:
            found = campaign._find_existing_campaign_message(
                client,
                token="token",
                channel_id="general",
                bot_user_id=campaign.DEFAULT_BOT_USER_ID,
                max_pages=2,
            )
        self.assertEqual(older[0]["id"], found)
        self.assertIn("before=", request.call_args_list[1].args[2])

    def test_metrics_subject_does_not_expose_discord_user_id(self):
        subject = campaign_config.event_subject("932448074931011606")
        self.assertEqual(16, len(subject))
        self.assertNotIn("932448074931011606", subject)

    def test_discord_validation_error_preserves_nested_field_path(self):
        response = MagicMock()
        response.status_code = 400
        response.json.return_value = {
            "code": 50035,
            "message": "Invalid Form Body",
            "errors": {
                "poll": {
                    "question": {
                        "text": {
                            "_errors": [{"code": "BASE_TYPE_MAX_LENGTH", "message": "Too long."}]
                        }
                    }
                }
            },
        }
        client = MagicMock()
        client.request.return_value = response

        with self.assertRaisesRegex(RuntimeError, r'errors=.*"poll".*"question".*"text"'):
            campaign._request(client, "POST", "/channels/123/messages", token="secret-token")


if __name__ == "__main__":
    unittest.main()
