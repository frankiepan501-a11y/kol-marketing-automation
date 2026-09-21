import os
import unittest
from unittest.mock import MagicMock, patch

from app import discord_final10_campaign as campaign


class DiscordFinal10CampaignTests(unittest.TestCase):
    def test_payload_uses_approved_copy_and_existing_application_button(self):
        payload = campaign.message_payload("visual.png")

        self.assertEqual(campaign.MESSAGE, payload["content"])
        self.assertTrue(payload["content"].startswith("🗳️ THE RESULTS ARE IN\n\n"))
        self.assertNotIn("@everyone", payload["content"])
        self.assertNotIn("No purchase", payload["content"])
        self.assertNotIn("Amazon review", payload["content"])
        self.assertEqual({"parse": []}, payload["allowed_mentions"])
        button = payload["components"][0]["components"][0]
        self.assertEqual("Check If I Qualify", button["label"])
        self.assertEqual("tester_apply_start", button["custom_id"])
        self.assertLessEqual(len(payload["nonce"]), 25)

    def test_only_general_is_allowed(self):
        with self.assertRaisesRegex(RuntimeError, "only be published to general"):
            campaign.publish(channel_name="announcement", commit=False)

    def test_dry_run_checks_bot_channel_image_and_duplicate_without_posting(self):
        channels = [{"id": "general-id", "name": "general", "type": 0}]
        responses = [{"id": campaign.BOT_USER_ID}, channels, []]
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "test"}), \
                patch.object(campaign, "_request", side_effect=responses) as request:
            result = campaign.publish(commit=False)

        self.assertTrue(result["ok"])
        self.assertFalse(result["commit"])
        self.assertEqual("general-id", result["channel_id"])
        self.assertEqual(64, len(result["image_sha256"]))
        self.assertFalse(any(call.args[1] == "POST" for call in request.call_args_list))

    @staticmethod
    def _stored(local_bytes: bytes) -> dict:
        return {
            "id": "message-id",
            "author": {"id": campaign.BOT_USER_ID},
            "content": campaign.MESSAGE,
            "mention_everyone": False,
            "attachments": [{
                "filename": campaign.DEFAULT_IMAGE.name,
                "size": len(local_bytes),
                "width": 1200,
                "height": 630,
                "url": "https://cdn.example/visual.png",
            }],
            "components": [{"components": [{
                "custom_id": "tester_apply_start",
                "label": "Check If I Qualify",
            }]}],
        }

    def test_commit_posts_once_then_verifies_exact_readback_and_attachment_hash(self):
        local_bytes = campaign.DEFAULT_IMAGE.read_bytes()
        channels = [{"id": "general-id", "name": "general", "type": 0}]
        responses = [
            {"id": campaign.BOT_USER_ID}, channels, [], {"id": "message-id"},
            self._stored(local_bytes),
        ]
        download = MagicMock(content=local_bytes)
        download.raise_for_status.return_value = None
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = download
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "test"}), \
                patch.object(campaign.httpx, "Client", return_value=client), \
                patch.object(campaign, "_request", side_effect=responses) as request:
            result = campaign.publish(commit=True)

        self.assertEqual("message-id", result["message_id"])
        self.assertFalse(result["duplicate"])
        self.assertFalse(result["mention_everyone"])
        post_calls = [call for call in request.call_args_list if call.args[1] == "POST"]
        self.assertEqual(1, len(post_calls))

    def test_existing_marker_is_read_back_without_duplicate_post(self):
        local_bytes = campaign.DEFAULT_IMAGE.read_bytes()
        channels = [{"id": "general-id", "name": "general", "type": 0}]
        existing = {
            "id": str(campaign.CAMPAIGN_START_SNOWFLAKE + 1),
            "author": {"id": campaign.BOT_USER_ID},
            "content": campaign.MESSAGE,
        }
        stored = self._stored(local_bytes)
        stored["id"] = existing["id"]
        responses = [{"id": campaign.BOT_USER_ID}, channels, [existing], stored]
        download = MagicMock(content=local_bytes)
        download.raise_for_status.return_value = None
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = download
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "test"}), \
                patch.object(campaign.httpx, "Client", return_value=client), \
                patch.object(campaign, "_request", side_effect=responses) as request:
            result = campaign.publish(commit=True)

        self.assertTrue(result["duplicate"])
        self.assertFalse(any(call.args[1] == "POST" for call in request.call_args_list))
