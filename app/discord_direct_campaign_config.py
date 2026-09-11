"""Shared, immutable settings and helpers for the 2026-09-11 Direct campaign."""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import Enum


CAMPAIGN_KEY = "discord-direct-20260911"
CAMPAIGN_DATE = "2026-09-11"
BOT_USER_ID = "1485906070248493116"
PUBLIC_MARKER = f"FUNLAB DIRECT COMMUNITY VOTE • {CAMPAIGN_DATE}"
DM_MARKER = f"FUNLAB DIRECT CHECK-IN • {CAMPAIGN_DATE}"
REHEARSAL_CHANNEL = "tester-staff-rehearsal"
PUBLIC_CHANNEL = "general"
OFFICIAL_DIRECT_URLS = (
    "https://www.nintendo.com/us/nintendo-direct/9-8-2026/",
    "https://www.nintendo.com/us/nintendo-direct/9-9-2026/",
)
DISCORD_EPOCH_MS = 1420070400000
CAMPAIGN_START_MS = int(
    datetime.fromisoformat(CAMPAIGN_DATE).replace(tzinfo=UTC).timestamp() * 1000
)
CAMPAIGN_START_SNOWFLAKE = (CAMPAIGN_START_MS - DISCORD_EPOCH_MS) << 22
INTEREST_CHOICES = {
    "oot": "Ocarina of Time remake",
    "system": "Anniversary Switch 2",
    "pro_controller": "Anniversary Pro Controller",
    "case": "Anniversary carrying case",
    "other": "Another Zelda reveal",
}


class DirectMessageStatus(str, Enum):
    SENT = "sent"
    ALREADY_SENT = "already_sent"


def component_custom_ids(message: dict) -> set[str]:
    return {
        str(component.get("custom_id"))
        for row in message.get("components") or []
        for component in row.get("components") or []
        if component.get("custom_id")
    }


def event_subject(discord_user_id: str) -> str:
    """Return a stable campaign-local pseudonymous member key for metrics."""
    return hashlib.sha256(f"{CAMPAIGN_KEY}:{discord_user_id}".encode("utf-8")).hexdigest()[:16]


def dm_nonce(discord_user_id: str) -> str:
    return f"dir911-{event_subject(discord_user_id)}"


def public_nonce(channel_name: str) -> str:
    """Return a stable channel-scoped nonce so rehearsal cannot suppress public posting."""
    channel_key = hashlib.sha256(channel_name.encode("utf-8")).hexdigest()[:10]
    return f"dir911-{channel_key}"


def log_event(event: str, *, discord_user_id: str = "", **fields: object) -> None:
    """Emit structured, content-free funnel telemetry to the production runtime log."""
    data: dict[str, object] = {"campaign": CAMPAIGN_KEY, "event": event}
    if discord_user_id:
        data["subject"] = event_subject(discord_user_id)
    data.update({key: value for key, value in fields.items() if value not in (None, "")})
    print("FUNLAB_DIRECT_EVENT " + json.dumps(data, ensure_ascii=True, separators=(",", ":")))
