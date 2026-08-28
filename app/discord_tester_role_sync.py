"""Keep the public Tester Selected role in sync with approved Feishu records."""
from __future__ import annotations

import asyncio
from contextlib import suppress
import os
import time
from typing import Mapping, Protocol

import httpx

from .discord_tester_program import DiscordTesterLedger


class ApplicationLedger(Protocol):
    async def list_applications(self) -> list[dict]: ...


class RoleAssigner(Protocol):
    async def ensure_selected(self, discord_user_id: str) -> bool: ...


def _eligible(fields: dict) -> bool:
    application = str(fields.get("报名状态") or "").strip()
    verification = str(fields.get("核验状态") or "").strip()
    return (
        application in {"已入选", "入选", "Selected"}
        and verification in {"已通过", "通过", "Verified"}
    )


class DiscordSelectedRoleAssigner:
    """Add Tester Selected once and require a successful Discord readback."""

    def __init__(self, *, token: str, guild_id: str,
                 transport: httpx.AsyncBaseTransport | None = None):
        self.token = token
        self.guild_id = guild_id
        self.transport = transport
        self.base_url = "https://discord.com/api/v10"

    async def ensure_selected(self, discord_user_id: str) -> bool:
        async with httpx.AsyncClient(
            headers={"Authorization": f"Bot {self.token}"},
            timeout=20.0,
            transport=self.transport,
        ) as client:
            roles_response = await client.get(
                f"{self.base_url}/guilds/{self.guild_id}/roles"
            )
            roles_response.raise_for_status()
            selected = next(
                (role for role in roles_response.json()
                 if str(role.get("name") or "") == "Tester Selected"),
                None,
            )
            if not selected:
                raise RuntimeError("Discord role not found: Tester Selected")
            role_id = str(selected["id"])
            member_url = (
                f"{self.base_url}/guilds/{self.guild_id}/members/{discord_user_id}"
            )
            member_response = await client.get(member_url)
            member_response.raise_for_status()
            current_roles = {
                str(value) for value in member_response.json().get("roles", [])
            }
            if role_id in current_roles:
                return False
            add_response = await client.put(f"{member_url}/roles/{role_id}")
            add_response.raise_for_status()
            readback_response = await client.get(member_url)
            readback_response.raise_for_status()
            readback_roles = {
                str(value) for value in readback_response.json().get("roles", [])
            }
            if role_id not in readback_roles:
                raise RuntimeError("Discord role assignment readback failed")
            return True


class SelectedRoleSync:
    def __init__(self, *, ledger: ApplicationLedger, assigner: RoleAssigner):
        self.ledger = ledger
        self.assigner = assigner

    async def run_once(self) -> dict:
        records = await self.ledger.list_applications()
        result = {
            "scanned": len(records), "eligible": 0, "added": 0,
            "already_present": 0, "failed": 0,
        }
        processed: set[str] = set()
        for record in records:
            fields = record.get("fields") or {}
            discord_user_id = str(fields.get("Discord用户ID") or "").strip()
            if (not discord_user_id or discord_user_id in processed
                    or not _eligible(fields)):
                continue
            processed.add(discord_user_id)
            result["eligible"] += 1
            try:
                added = await self.assigner.ensure_selected(discord_user_id)
            except Exception:
                result["failed"] += 1
                continue
            result["added"] += int(added)
            result["already_present"] += int(not added)
        return result


class SelectedRoleSyncRuntime:
    def __init__(self, sync: SelectedRoleSync, *, interval_seconds: int = 5 * 60):
        self.sync = sync
        self.interval_seconds = interval_seconds
        self.last_success_ms = 0
        self.last_error = ""
        self.last_result: dict = {}
        self._task: asyncio.Task | None = None

    async def run_once(self) -> dict:
        result = await self.sync.run_once()
        self.last_success_ms = int(time.time() * 1000)
        self.last_result = result
        self.last_error = "role_assignment_failed" if result["failed"] else ""
        return dict(result)

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(
                self._loop(), name="discord-tester-selected-role-sync"
            )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = type(exc).__name__
            await asyncio.sleep(self.interval_seconds)


def build_runtime(env: Mapping[str, str] | None = None) -> SelectedRoleSyncRuntime | None:
    values = os.environ if env is None else env
    if values.get("DISCORD_TESTER_ROLE_SYNC_ENABLED", "0") != "1":
        return None
    token = values.get("DISCORD_BOT_TOKEN", "").strip()
    guild_id = values.get("DISCORD_FUNLAB_GUILD_ID", "").strip()
    missing = [
        name for name, value in {
            "DISCORD_BOT_TOKEN": token,
            "DISCORD_FUNLAB_GUILD_ID": guild_id,
        }.items() if not value
    ]
    if missing:
        raise ValueError(f"Discord tester role sync missing: {', '.join(missing)}")
    return SelectedRoleSyncRuntime(SelectedRoleSync(
        ledger=DiscordTesterLedger(),
        assigner=DiscordSelectedRoleAssigner(token=token, guild_id=guild_id),
    ))
