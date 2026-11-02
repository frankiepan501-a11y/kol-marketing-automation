"""Read-only audit and deterministic resource plan for THE 40TH CREW.

This module deliberately contains no Discord mutation calls.  It turns a
Discord snapshot into an auditable decision and a preview of the minimum
roles/channels required by the campaign.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


DISCORD_API = "https://discord.com/api/v10"


# Discord permission bits from the public API reference.
ADMINISTRATOR = 1 << 3
MANAGE_CHANNELS = 1 << 4
VIEW_CHANNEL = 1 << 10
SEND_MESSAGES = 1 << 11
EMBED_LINKS = 1 << 14
ATTACH_FILES = 1 << 15
READ_MESSAGE_HISTORY = 1 << 16
MANAGE_MESSAGES = 1 << 13
MANAGE_ROLES = 1 << 28
CREATE_PUBLIC_THREADS = 1 << 35
SEND_MESSAGES_IN_THREADS = 1 << 38


ACTIVITY_ROLE_NAMES = (
    "Applicant",
    "The First 20",
    "Tester Selected",
    "The 40",
)

_TESTER_SELECTED_DANGEROUS = (
    ADMINISTRATOR | MANAGE_CHANNELS | MANAGE_MESSAGES | MANAGE_ROLES
)


def _permission_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _role_map(roles: Iterable[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(role.get("id")): role for role in roles if role.get("id") is not None}


def _read_discord(client: Any, *, token: str, path: str) -> Any:
    response = client.request(
        "GET",
        f"{DISCORD_API}{path}",
        headers={
            "Authorization": f"Bot {token}",
            "User-Agent": "FUNLAB-The40th-Preflight/1.0",
        },
    )
    if response.status_code >= 400:
        detail = ""
        try:
            body = response.json()
            detail = f" code={body.get('code', '')} message={str(body.get('message') or '')[:240]}"
        except (AttributeError, ValueError):
            pass
        raise RuntimeError(f"Discord API GET {path} failed: HTTP {response.status_code}{detail}")
    return response.json() if response.content else {}


def fetch_discord_snapshot(*, client: Any, token: str, guild_id: str) -> dict[str, Any]:
    """Fetch only the four resources required by the permission audit."""

    if not token:
        raise ValueError("Discord bot token is required")
    bot = _read_discord(client, token=token, path="/users/@me")
    bot_user_id = str(bot.get("id") or "")
    if not bot_user_id:
        raise RuntimeError("Discord bot identity response is missing id")
    roles = _read_discord(client, token=token, path=f"/guilds/{guild_id}/roles")
    channels = _read_discord(client, token=token, path=f"/guilds/{guild_id}/channels")
    bot_member = _read_discord(
        client,
        token=token,
        path=f"/guilds/{guild_id}/members/{bot_user_id}",
    )
    return {
        "guild_id": str(guild_id),
        "bot_user_id": bot_user_id,
        "roles": roles,
        "channels": channels,
        "bot_member": bot_member,
    }


def audit_discord_state(
    *,
    guild_id: str,
    roles: Sequence[Mapping[str, Any]],
    channels: Sequence[Mapping[str, Any]],
    bot_member: Mapping[str, Any],
    allow_administrator: bool = False,
) -> dict[str, Any]:
    """Audit a guild snapshot without changing Discord.

    ``bot_member`` is the response shape returned by Discord's guild member
    endpoint.  The audit intentionally checks direct Tester Selected channel
    grants separately so an old label cannot silently become an access role.
    """

    roles_by_id = _role_map(roles)
    everyone = roles_by_id.get(str(guild_id), {})
    bot_role_ids = [str(role_id) for role_id in bot_member.get("roles", [])]
    bot_roles = [roles_by_id[role_id] for role_id in bot_role_ids if role_id in roles_by_id]
    bot_permissions = _permission_value(everyone.get("permissions"))
    for role in bot_roles:
        bot_permissions |= _permission_value(role.get("permissions"))

    activity_roles = {
        str(role.get("name")): role
        for role in roles
        if str(role.get("name")) in ACTIVITY_ROLE_NAMES
    }
    missing_activity_roles = [
        name for name in ACTIVITY_ROLE_NAMES if name not in activity_roles
    ]
    bot_top_position = max(
        (_permission_value(role.get("position")) for role in bot_roles),
        default=0,
    )
    target_top_position = max(
        (_permission_value(role.get("position")) for role in activity_roles.values()),
        default=-1,
    )
    bot_above_activity_roles = bot_top_position > target_top_position

    selected = activity_roles.get("Tester Selected")
    selected_id = str(selected.get("id")) if selected else None
    selected_permissions = _permission_value(selected.get("permissions")) if selected else 0
    selected_view_channels: list[str] = []
    if selected_id:
        for channel in channels:
            for overwrite in channel.get("permission_overwrites", []) or []:
                if (
                    str(overwrite.get("id")) == selected_id
                    and _permission_value(overwrite.get("type")) == 0
                    and _permission_value(overwrite.get("allow")) & VIEW_CHANNEL
                ):
                    selected_view_channels.append(str(channel.get("name") or channel.get("id")))
                    break

    bot_has_administrator = bool(bot_permissions & ADMINISTRATOR)
    bot_can_manage_roles = bot_has_administrator or bool(bot_permissions & MANAGE_ROLES)
    bot_can_manage_channels = bot_has_administrator or bool(bot_permissions & MANAGE_CHANNELS)
    selected_has_dangerous_permissions = bool(
        selected_permissions & _TESTER_SELECTED_DANGEROUS
    )
    safe_to_commit = all(
        (
            allow_administrator or not bot_has_administrator,
            bot_can_manage_roles,
            bot_can_manage_channels,
            bot_above_activity_roles,
            not selected_has_dangerous_permissions,
            not selected_view_channels,
        )
    )

    return {
        "bot_has_administrator": bot_has_administrator,
        "administrator_retention_approved": bool(allow_administrator),
        "bot_can_manage_roles": bot_can_manage_roles,
        "bot_can_manage_channels": bot_can_manage_channels,
        "bot_top_role_position": bot_top_position,
        "activity_role_ids": {
            name: str(role.get("id")) for name, role in activity_roles.items()
        },
        "missing_activity_roles": missing_activity_roles,
        "bot_above_activity_roles": bot_above_activity_roles,
        "tester_selected_has_dangerous_permissions": selected_has_dangerous_permissions,
        "tester_selected_view_channels": selected_view_channels,
        "safe_to_commit": safe_to_commit,
    }


def _overwrite(role_id: str, *, allow: int = 0, deny: int = 0, kind: int = 0) -> dict[str, str | int]:
    return {
        "id": str(role_id),
        "type": kind,
        "allow": str(allow),
        "deny": str(deny),
    }


def build_resource_plan(
    *,
    guild_id: str,
    bot_user_id: str,
    role_ids: Mapping[str, str],
    product_role_ids: Sequence[str],
    operations_role_ids: Sequence[str],
) -> dict[str, Any]:
    """Build the exact three-area permission preview for the campaign."""

    missing = [name for name in ACTIVITY_ROLE_NAMES if not role_ids.get(name)]
    if missing:
        raise ValueError(f"missing activity role IDs: {', '.join(missing)}")
    if not product_role_ids:
        raise ValueError("at least one product/technical role ID is required")
    if not operations_role_ids:
        raise ValueError("at least one operations role ID is required")

    first_id = str(role_ids["The First 20"])
    forty_id = str(role_ids["The 40"])
    everyone_deny = _overwrite(str(guild_id), deny=VIEW_CHANNEL)
    bot_allow = (
        VIEW_CHANNEL
        | SEND_MESSAGES
        | READ_MESSAGE_HISTORY
        | EMBED_LINKS
        | ATTACH_FILES
    )
    member_allow = VIEW_CHANNEL | SEND_MESSAGES | READ_MESSAGE_HISTORY
    forum_member_allow = member_allow | CREATE_PUBLIC_THREADS | SEND_MESSAGES_IN_THREADS

    def common_overwrites(
        *, first_allow: int = 0, first_deny: int = 0,
        forty_allow: int = 0, forty_deny: int = 0,
        product_allow: int = 0, product_deny: int = 0,
        operations_allow: int,
    ) -> list[dict[str, str | int]]:
        overwrites = [
            everyone_deny,
            _overwrite(str(bot_user_id), allow=bot_allow, kind=1),
            _overwrite(first_id, allow=first_allow, deny=first_deny),
            _overwrite(forty_id, allow=forty_allow, deny=forty_deny),
        ]
        overwrites.extend(
            _overwrite(str(role_id), allow=product_allow, deny=product_deny)
            for role_id in product_role_ids
        )
        overwrites.extend(
            _overwrite(str(role_id), allow=operations_allow)
            for role_id in operations_role_ids
        )
        return overwrites

    channels = [
        {
            "name": "the-first-20",
            "type": 0,
            "permission_overwrites": common_overwrites(
                first_allow=member_allow,
                forty_deny=VIEW_CHANNEL,
                product_allow=member_allow,
                operations_allow=member_allow,
            ),
        },
        {
            "name": "feedback",
            "type": 15,
            "permission_overwrites": common_overwrites(
                first_allow=forum_member_allow,
                forty_deny=VIEW_CHANNEL,
                product_allow=forum_member_allow,
                operations_allow=forum_member_allow,
            ),
        },
        {
            "name": "the-40",
            "type": 0,
            "permission_overwrites": common_overwrites(
                first_deny=VIEW_CHANNEL,
                forty_allow=member_allow,
                product_deny=VIEW_CHANNEL,
                operations_allow=member_allow,
            ),
        },
    ]

    return {
        "category": {"name": "THE 40TH CREW", "type": 4},
        "channels": channels,
        "guardrails": {
            "tester_selected_is_label_only": True,
            "applicant_has_no_private_area_access": True,
            "first_20_and_the_40_are_separate": True,
        },
    }
