"""Duplicate-safe publisher for the final ten FUNLAB tester places."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx

from .discord_direct_campaign_config import BOT_USER_ID, PUBLIC_CHANNEL, component_custom_ids


DISCORD_API = "https://discord.com/api/v10"
DEFAULT_GUILD_ID = "1009762946437619742"
CAMPAIGN_KEY = "discord-final10-20260922"
DISCORD_EPOCH_MS = 1420070400000
CAMPAIGN_START_MS = int(datetime(2026, 9, 21, tzinfo=UTC).timestamp() * 1000)
CAMPAIGN_START_SNOWFLAKE = (CAMPAIGN_START_MS - DISCORD_EPOCH_MS) << 22
DEFAULT_IMAGE = Path(__file__).parent / "assets" / "funlab-final10-facebook-x-1200x630.png"
EXPECTED_IMAGE_WIDTH = 1200
EXPECTED_IMAGE_HEIGHT = 630

MESSAGE = (
    "🗳️ THE RESULTS ARE IN\n\n"
    "Ocarina of Time Remake took the top spot in our community vote.\n\n"
    "Now we’re opening the final 10 places in the first FUNLAB Private Product Test group.\n\n"
    "We’re looking for Switch players who care about:\n"
    "• precise, responsive controls\n"
    "• comfort during longer play sessions\n"
    "• reliable performance across Switch and PC / Steam\n\n"
    "Switch ownership is required. Switch 2, Steam Deck and PC / Steam experience are a plus. "
    "Recent Amazon Video Games shopping experience is preferred.\n\n"
    "Think you’re a fit? Tap the button below and FUN Bot will send the application privately.\n\n"
    "Applications will remain open until the final 10 qualified testers are confirmed."
)


def _nonce(channel_name: str) -> str:
    digest = hashlib.sha256(f"{CAMPAIGN_KEY}:{channel_name}".encode("utf-8")).hexdigest()[:10]
    return f"fn10-{digest}"


def message_payload(filename: str, *, channel_name: str = PUBLIC_CHANNEL) -> dict:
    return {
        "content": MESSAGE,
        "allowed_mentions": {"parse": []},
        "attachments": [{
            "id": 0,
            "filename": filename,
            "description": "FUNLAB final ten private product tester places",
        }],
        "components": [{
            "type": 1,
            "components": [{
                "type": 2,
                "style": 1,
                "label": "Check If I Qualify",
                "custom_id": "tester_apply_start",
            }],
        }],
        "nonce": _nonce(channel_name),
        "enforce_nonce": True,
    }


def _request(client: httpx.Client, method: str, path: str, *, token: str,
             json_body: dict | None = None, data: dict | None = None,
             files: dict | None = None):
    kwargs: dict = {
        "headers": {
            "Authorization": f"Bot {token}",
            "User-Agent": "FUNLAB-Final10-Campaign/1.0",
        }
    }
    if json_body is not None:
        kwargs["json"] = json_body
    if data is not None:
        kwargs["data"] = data
    if files is not None:
        kwargs["files"] = files
    response = client.request(method, f"{DISCORD_API}{path}", **kwargs)
    if response.status_code >= 400:
        detail = ""
        try:
            body = response.json()
            detail = f" code={body.get('code', '')} message={str(body.get('message') or '')[:240]}"
            if body.get("errors"):
                detail += " errors=" + json.dumps(
                    body["errors"], ensure_ascii=True, separators=(",", ":")
                )[:1500]
        except ValueError:
            pass
        raise RuntimeError(f"Discord API {method} {path} failed: HTTP {response.status_code}{detail}")
    return response.json() if response.content else {}


def _channel_id(channels: list[dict], channel_name: str) -> str:
    matches = [
        str(channel.get("id") or "")
        for channel in channels
        if channel.get("name") == channel_name and int(channel.get("type", -1)) in {0, 5}
    ]
    matches = [item for item in matches if item]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one Discord text channel named {channel_name!r}; found {len(matches)}")
    return matches[0]


def _find_existing(client: httpx.Client, *, token: str, channel_id: str,
                   bot_user_id: str, max_pages: int = 20) -> str:
    before = ""
    for _ in range(max_pages):
        path = f"/channels/{channel_id}/messages?limit=100"
        if before:
            path += f"&before={before}"
        messages = _request(client, "GET", path, token=token)
        if not isinstance(messages, list) or not messages:
            return ""
        for message in messages:
            author_id = str(((message.get("author") or {}).get("id")) or "")
            if author_id == bot_user_id and str(message.get("content") or "") == MESSAGE:
                return str(message.get("id") or "")
        snowflakes = [int(item["id"]) for item in messages if str(item.get("id") or "").isdigit()]
        if not snowflakes:
            raise RuntimeError("Discord history scan returned no usable message IDs")
        oldest = min(snowflakes)
        if oldest <= CAMPAIGN_START_SNOWFLAKE or len(messages) < 100:
            return ""
        before = str(oldest)
    raise RuntimeError("Discord history is too active to prove this campaign was not already published")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _verify(message: dict, *, message_id: str, bot_user_id: str,
            filename: str, local_bytes: bytes, client: httpx.Client) -> dict:
    if str(message.get("id") or "") != message_id:
        raise RuntimeError("Discord readback returned a different message ID")
    if str(((message.get("author") or {}).get("id")) or "") != bot_user_id:
        raise RuntimeError("Discord readback author is not FUN Bot")
    if str(message.get("content") or "") != MESSAGE:
        raise RuntimeError("Discord readback content does not match the approved copy")
    if message.get("mention_everyone") is not False:
        raise RuntimeError("Discord readback did not confirm mention_everyone=false")
    ids = component_custom_ids(message)
    if ids != {"tester_apply_start"}:
        raise RuntimeError("Discord readback has the wrong application button")
    buttons = [
        component
        for row in message.get("components") or []
        for component in row.get("components") or []
    ]
    if len(buttons) != 1 or buttons[0].get("label") != "Check If I Qualify":
        raise RuntimeError("Discord readback has the wrong application button label")
    attachments = [item for item in message.get("attachments") or [] if item.get("filename") == filename]
    if len(attachments) != 1:
        raise RuntimeError("Discord readback is missing the approved visual")
    attachment = attachments[0]
    if int(attachment.get("size") or 0) != len(local_bytes):
        raise RuntimeError("Discord attachment size does not match the approved visual")
    if int(attachment.get("width") or 0) != EXPECTED_IMAGE_WIDTH or int(attachment.get("height") or 0) != EXPECTED_IMAGE_HEIGHT:
        raise RuntimeError("Discord attachment dimensions do not match 1200x630")
    response = client.get(str(attachment.get("url") or ""), follow_redirects=True)
    response.raise_for_status()
    local_sha = _sha256(local_bytes)
    remote_sha = _sha256(response.content)
    if remote_sha != local_sha:
        raise RuntimeError("Discord attachment SHA-256 does not match the approved visual")
    if message.get("poll"):
        raise RuntimeError("Discord final-ten message unexpectedly contains a poll")
    return {"attachment_sha256": remote_sha, "attachment_bytes": len(local_bytes)}


def publish(*, channel_name: str = PUBLIC_CHANNEL, image_path: Path = DEFAULT_IMAGE,
            commit: bool = False) -> dict:
    if channel_name != PUBLIC_CHANNEL:
        raise RuntimeError("The final-ten campaign may only be published to general")
    token = os.environ.get("DISCORD_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not configured")
    guild_id = os.environ.get("DISCORD_FUNLAB_GUILD_ID", DEFAULT_GUILD_ID)
    expected_bot_id = os.environ.get("DISCORD_FUN_BOT_USER_ID", BOT_USER_ID)
    image_path = image_path.resolve()
    if not image_path.is_file():
        raise RuntimeError(f"Campaign image not found: {image_path}")
    local_bytes = image_path.read_bytes()
    payload = message_payload(image_path.name, channel_name=channel_name)

    with httpx.Client(timeout=30.0) as client:
        bot = _request(client, "GET", "/users/@me", token=token)
        bot_user_id = str(bot.get("id") or "")
        if bot_user_id != expected_bot_id:
            raise RuntimeError(f"Unexpected Discord bot identity: {bot_user_id or 'missing'}")
        channels = _request(client, "GET", f"/guilds/{guild_id}/channels", token=token)
        channel_id = _channel_id(channels, channel_name)
        existing_id = _find_existing(
            client, token=token, channel_id=channel_id, bot_user_id=bot_user_id
        )
        if not commit:
            return {
                "ok": True,
                "commit": False,
                "channel_id": channel_id,
                "bot_user_id": bot_user_id,
                "existing_message_id": existing_id,
                "payload": payload,
                "image_bytes": len(local_bytes),
                "image_sha256": _sha256(local_bytes),
            }

        if existing_id:
            message_id = existing_id
        else:
            created = _request(
                client,
                "POST",
                f"/channels/{channel_id}/messages",
                token=token,
                data={"payload_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
                files={"files[0]": (image_path.name, local_bytes, "image/png")},
            )
            message_id = str(created.get("id") or "")
            if not message_id:
                raise RuntimeError("Discord did not return the final-ten message ID")
        stored = _request(
            client, "GET", f"/channels/{channel_id}/messages/{message_id}", token=token
        )
        verified = _verify(
            stored,
            message_id=message_id,
            bot_user_id=bot_user_id,
            filename=image_path.name,
            local_bytes=local_bytes,
            client=client,
        )
        return {
            "ok": True,
            "commit": True,
            "duplicate": bool(existing_id),
            "channel_id": channel_id,
            "message_id": message_id,
            "bot_user_id": bot_user_id,
            "mention_everyone": stored.get("mention_everyone"),
            **verified,
        }
