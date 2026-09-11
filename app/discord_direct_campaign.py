"""One-off, duplicate-safe publisher for the September Direct community vote."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import httpx

from . import discord_direct_campaign_config as campaign_config


DISCORD_API = "https://discord.com/api/v10"
DEFAULT_GUILD_ID = "1009762946437619742"
DEFAULT_BOT_USER_ID = campaign_config.BOT_USER_ID
PUBLIC_MARKER = campaign_config.PUBLIC_MARKER
OFFICIAL_DIRECT_URLS = campaign_config.OFFICIAL_DIRECT_URLS
CAMPAIGN_START_SNOWFLAKE = campaign_config.CAMPAIGN_START_SNOWFLAKE
DEFAULT_IMAGE = Path(__file__).parent / "assets" / "funlab-direct-community-vote-1200x675.png"


def public_message_payload(attachment_filename: str, *, channel_name: str = campaign_config.PUBLIC_CHANNEL) -> dict:
    return {
        "content": (
            f"🎮 **{PUBLIC_MARKER}**\n\n"
            "**TWO DIRECTS. ONE FIRST PICK.**\n\n"
            "Nintendo gave Switch players a lot to talk about on September 8 and 9. "
            "Vote for the reveal that made you want to grab your controller first — "
            "then tell us why in the replies.\n\n"
            "Want to hear about a limited FUNLAB controller test? Tap "
            "**DM Me Tester Details**. FUN Bot will send one private follow-up and the "
            "optional application link.\n\n"
            "**Watch the official presentations:**\n"
            "The Legend of Zelda 40th Anniversary Direct:\n"
            f"{OFFICIAL_DIRECT_URLS[0]}\n\n"
            "Nintendo Direct 9.9.2026:\n"
            f"{OFFICIAL_DIRECT_URLS[1]}\n\n"
            "*This community discussion and FUNLAB product test are independently organized "
            "and are not affiliated with or endorsed by Nintendo.*"
        ),
        "allowed_mentions": {"parse": []},
        "attachments": [{
            "id": 0,
            "filename": attachment_filename,
            "description": "FUNLAB original mystery controller community vote artwork",
        }],
        "components": [{
            "type": 1,
            "components": [{
                "type": 2,
                "style": 1,
                "label": "DM Me Tester Details",
                "custom_id": "tester_direct_dm",
            }],
        }],
        "poll": {
            "question": {
                "text": "Two Directs in two days — which reveal made you reach for your controller first?"
            },
            "answers": [
                {"poll_media": {"text": "Ocarina of Time remake"}},
                {"poll_media": {"text": "Metroid Ravenous"}},
                {"poll_media": {"text": "Kirby and the World Beyond"}},
                {"poll_media": {"text": "Monster Hunter Wilds"}},
                {"poll_media": {"text": "Persona 6 / Persona 4 Revival"}},
                {"poll_media": {"text": "Mario Kart World update"}},
                {"poll_media": {"text": "Something else — reply below"}},
            ],
            "duration": 72,
            "allow_multiselect": False,
            "layout_type": 1,
        },
        "nonce": campaign_config.public_nonce(channel_name),
        "enforce_nonce": True,
    }


def find_channel_id(channels: list[dict], channel_name: str) -> str:
    matches = [
        str(channel.get("id") or "")
        for channel in channels
        if channel.get("name") == channel_name and int(channel.get("type", -1)) in {0, 5}
    ]
    matches = [item for item in matches if item]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one Discord text channel named {channel_name!r}; found {len(matches)}")
    return matches[0]


def existing_campaign_message_id(messages: list[dict], *, bot_user_id: str = DEFAULT_BOT_USER_ID) -> str:
    for message in messages:
        author_id = str(((message.get("author") or {}).get("id")) or "")
        if author_id == bot_user_id and PUBLIC_MARKER in str(message.get("content") or ""):
            return str(message.get("id") or "")
    return ""


def _find_existing_campaign_message(client: httpx.Client, *, token: str,
                                    channel_id: str, bot_user_id: str,
                                    max_pages: int = 20) -> str:
    """Scan safely back to campaign start; fail closed if the scan cannot finish."""
    before = ""
    for _ in range(max_pages):
        path = f"/channels/{channel_id}/messages?limit=100"
        if before:
            path += f"&before={before}"
        messages = _request(client, "GET", path, token=token)
        if not isinstance(messages, list) or not messages:
            return ""
        existing_id = existing_campaign_message_id(messages, bot_user_id=bot_user_id)
        if existing_id:
            return existing_id
        snowflakes = [int(item["id"]) for item in messages if str(item.get("id") or "").isdigit()]
        if not snowflakes:
            raise RuntimeError("Discord history scan returned no usable message IDs")
        oldest = min(snowflakes)
        if oldest <= CAMPAIGN_START_SNOWFLAKE or len(messages) < 100:
            return ""
        before = str(oldest)
    raise RuntimeError(
        "Discord history is too active to prove this campaign was not already published; refusing to post"
    )


def _request(client: httpx.Client, method: str, path: str, *, token: str,
             json_body: dict | None = None, data: dict | None = None,
             files: dict | None = None):
    kwargs: dict = {
        "headers": {
            "Authorization": f"Bot {token}",
            "User-Agent": "FUNLAB-Direct-Community-Vote/1.0",
        },
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
        except ValueError:
            pass
        raise RuntimeError(f"Discord API {method} {path} failed: HTTP {response.status_code}{detail}")
    return response.json() if response.content else {}


def _verify_stored_message(message: dict, *, message_id: str, bot_user_id: str,
                           attachment_filename: str) -> None:
    if str(message.get("id") or "") != message_id:
        raise RuntimeError("Discord readback returned a different message ID")
    if str(((message.get("author") or {}).get("id")) or "") != bot_user_id:
        raise RuntimeError("Discord readback author is not FUN Bot")
    if PUBLIC_MARKER not in str(message.get("content") or ""):
        raise RuntimeError("Discord readback is missing the campaign marker")
    if message.get("mention_everyone") is not False:
        raise RuntimeError("Discord readback did not confirm mention_everyone=false")
    if "tester_direct_dm" not in campaign_config.component_custom_ids(message):
        raise RuntimeError("Discord readback is missing the opt-in DM button")
    if not any(item.get("filename") == attachment_filename for item in message.get("attachments") or []):
        raise RuntimeError("Discord readback is missing the FUNLAB original visual")
    poll_question = (((message.get("poll") or {}).get("question") or {}).get("text") or "")
    if not poll_question.startswith("Two Directs in two days"):
        raise RuntimeError("Discord readback is missing the native poll")


def _official_preview_urls(message: dict) -> set[str]:
    found: set[str] = set()
    for embed in message.get("embeds") or []:
        actual = str(embed.get("url") or "").rstrip("/")
        for expected in OFFICIAL_DIRECT_URLS:
            if actual == expected.rstrip("/"):
                found.add(expected)
    return found


def _verify_rehearsal_dm(client: httpx.Client, *, token: str, member_id: str,
                         bot_user_id: str) -> str:
    """Require evidence that the hidden CTA reached the answered DM and apply button."""
    dm = _request(
        client,
        "POST",
        "/users/@me/channels",
        token=token,
        json_body={"recipient_id": member_id},
    )
    channel_id = str(dm.get("id") or "")
    if not channel_id:
        raise RuntimeError("Discord did not return the rehearsal member DM channel")
    messages = _request(client, "GET", f"/channels/{channel_id}/messages?limit=100", token=token)
    for message in messages if isinstance(messages, list) else []:
        author_id = str(((message.get("author") or {}).get("id")) or "")
        custom_ids = campaign_config.component_custom_ids(message)
        if (
            author_id == bot_user_id
            and campaign_config.DM_MARKER in str(message.get("content") or "")
            and any(item.startswith("tester_apply_start.direct.") for item in custom_ids)
        ):
            return str(message.get("id") or "")
    raise RuntimeError(
        "Hidden rehearsal is incomplete: click the CTA, answer the Zelda DM question, "
        "and confirm the application button appears"
    )


def _readback_with_previews(client: httpx.Client, *, token: str, channel_id: str,
                            message_id: str, bot_user_id: str,
                            attachment_filename: str) -> tuple[dict, set[str]]:
    stored = _request(client, "GET", f"/channels/{channel_id}/messages/{message_id}", token=token)
    _verify_stored_message(
        stored,
        message_id=message_id,
        bot_user_id=bot_user_id,
        attachment_filename=attachment_filename,
    )
    previews = _official_preview_urls(stored)
    for _ in range(6):
        if previews == set(OFFICIAL_DIRECT_URLS):
            break
        time.sleep(5)
        stored = _request(client, "GET", f"/channels/{channel_id}/messages/{message_id}", token=token)
        previews = _official_preview_urls(stored)
    missing = set(OFFICIAL_DIRECT_URLS) - previews
    if missing:
        raise RuntimeError(
            f"Discord message {message_id} is missing official Direct previews: {sorted(missing)}"
        )
    return stored, previews


def publish(*, channel_name: str, image_path: Path = DEFAULT_IMAGE, commit: bool = False,
            rehearsal_message_id: str = "", rehearsal_user_id: str = "") -> dict:
    if channel_name == campaign_config.PUBLIC_CHANNEL and commit and not rehearsal_message_id:
        raise RuntimeError("Public commit requires the verified rehearsal message ID")
    if channel_name == campaign_config.PUBLIC_CHANNEL and commit and not rehearsal_user_id:
        raise RuntimeError("Public commit requires the rehearsal member ID")
    token = os.environ.get("DISCORD_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not configured")
    guild_id = os.environ.get("DISCORD_FUNLAB_GUILD_ID", DEFAULT_GUILD_ID)
    expected_bot_id = os.environ.get("DISCORD_FUN_BOT_USER_ID", DEFAULT_BOT_USER_ID)
    image_path = image_path.resolve()
    if not image_path.is_file():
        raise RuntimeError(f"Campaign image not found: {image_path}")
    filename = image_path.name
    payload = public_message_payload(filename, channel_name=channel_name)

    with httpx.Client(timeout=30.0) as client:
        bot = _request(client, "GET", "/users/@me", token=token)
        bot_user_id = str(bot.get("id") or "")
        if bot_user_id != expected_bot_id:
            raise RuntimeError(f"Unexpected Discord bot identity: {bot_user_id or 'missing'}")
        channels = _request(client, "GET", f"/guilds/{guild_id}/channels", token=token)
        channel_id = find_channel_id(channels, channel_name)
        if channel_name == campaign_config.PUBLIC_CHANNEL and commit:
            rehearsal_channel_id = find_channel_id(channels, campaign_config.REHEARSAL_CHANNEL)
            _readback_with_previews(
                client,
                token=token,
                channel_id=rehearsal_channel_id,
                message_id=rehearsal_message_id,
                bot_user_id=bot_user_id,
                attachment_filename=filename,
            )
            rehearsal_dm_id = _verify_rehearsal_dm(
                client,
                token=token,
                member_id=rehearsal_user_id,
                bot_user_id=bot_user_id,
            )
        else:
            rehearsal_dm_id = ""
        existing_id = _find_existing_campaign_message(
            client,
            token=token,
            channel_id=channel_id,
            bot_user_id=bot_user_id,
        )
        if existing_id:
            stored, previews = _readback_with_previews(
                client,
                token=token,
                channel_id=channel_id,
                message_id=existing_id,
                bot_user_id=bot_user_id,
                attachment_filename=filename,
            )
            return {
                "ok": True,
                "commit": commit,
                "duplicate_prevented": True,
                "channel_id": channel_id,
                "message_id": existing_id,
                "mention_everyone": stored.get("mention_everyone"),
                "official_preview_urls": sorted(previews),
                "rehearsal_dm_id": rehearsal_dm_id,
            }
        if not commit:
            return {
                "ok": True,
                "commit": False,
                "channel_id": channel_id,
                "bot_user_id": bot_user_id,
                "payload": payload,
                "image_bytes": image_path.stat().st_size,
            }

        with image_path.open("rb") as handle:
            created = _request(
                client,
                "POST",
                f"/channels/{channel_id}/messages",
                token=token,
                data={"payload_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
                files={"files[0]": (filename, handle, "image/png")},
            )
        message_id = str(created.get("id") or "")
        if not message_id:
            raise RuntimeError("Discord did not return a message ID")

        stored, previews = _readback_with_previews(
            client,
            token=token,
            channel_id=channel_id,
            message_id=message_id,
            bot_user_id=bot_user_id,
            attachment_filename=filename,
        )
        return {
            "ok": True,
            "commit": True,
            "duplicate_prevented": False,
            "channel_id": channel_id,
            "message_id": message_id,
            "mention_everyone": stored.get("mention_everyone"),
            "official_preview_count": len(previews),
            "official_preview_urls": sorted(previews),
            "attachment_count": len(stored.get("attachments") or []),
            "poll_question": (((stored.get("poll") or {}).get("question") or {}).get("text") or ""),
            "rehearsal_dm_id": rehearsal_dm_id,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish the FUNLAB Direct community vote safely")
    parser.add_argument("--channel-name", required=True)
    parser.add_argument("--image", type=Path, default=DEFAULT_IMAGE)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--rehearsal-message-id", default="")
    parser.add_argument("--rehearsal-user-id", default="")
    args = parser.parse_args(argv)
    try:
        result = publish(
            channel_name=args.channel_name,
            image_path=args.image,
            commit=args.commit,
            rehearsal_message_id=args.rehearsal_message_id,
            rehearsal_user_id=args.rehearsal_user_id,
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=True))
        return 1
    print(json.dumps(result, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
