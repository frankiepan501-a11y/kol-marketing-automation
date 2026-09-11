import unittest
from unittest.mock import MagicMock, patch

from app import discord_direct_campaign as campaign
from app import discord_direct_campaign_config as campaign_config


class DiscordDirectCampaignTests(unittest.TestCase):
    def test_public_payload_combines_original_visual_poll_links_and_opt_in_button(self):
        payload = campaign.public_message_payload("funlab-direct-community-vote-1200x675.png")

        self.assertIn(campaign.PUBLIC_MARKER, payload["content"])
        self.assertIn("https://www.nintendo.com/us/nintendo-direct/9-8-2026/", payload["content"])
        self.assertIn("https://www.nintendo.com/us/nintendo-direct/9-9-2026/", payload["content"])
        self.assertIn("not affiliated with or endorsed by Nintendo", payload["content"])
        self.assertNotIn("@everyone", payload["content"])
        self.assertEqual({"parse": []}, payload["allowed_mentions"])
        self.assertEqual("tester_direct_dm", payload["components"][0]["components"][0]["custom_id"])
        self.assertEqual(72, payload["poll"]["duration"])
        self.assertFalse(payload["poll"]["allow_multiselect"])
        self.assertEqual(7, len(payload["poll"]["answers"]))
        self.assertEqual(0, payload["attachments"][0]["id"])
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

    def test_rehearsal_and_public_posts_use_distinct_stable_nonces(self):
        rehearsal = campaign.public_message_payload(
            "visual.png", channel_name=campaign_config.REHEARSAL_CHANNEL
        )["nonce"]
        public = campaign.public_message_payload(
            "visual.png", channel_name=campaign_config.PUBLIC_CHANNEL
        )["nonce"]

        self.assertNotEqual(rehearsal, public)
        self.assertEqual(
            public,
            campaign.public_message_payload(
                "visual.png", channel_name=campaign_config.PUBLIC_CHANNEL
            )["nonce"],
        )
        self.assertLessEqual(len(rehearsal), 25)
        self.assertLessEqual(len(public), 25)

    def test_existing_public_marker_prevents_duplicate_post(self):
        messages = [
            {"id": "old", "author": {"id": "other"}, "content": campaign.PUBLIC_MARKER},
            {"id": "current", "author": {"id": campaign.DEFAULT_BOT_USER_ID},
             "content": f"**{campaign.PUBLIC_MARKER}**"},
        ]
        self.assertEqual("current", campaign.existing_campaign_message_id(messages))

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


if __name__ == "__main__":
    unittest.main()
