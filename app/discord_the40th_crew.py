"""Deterministic rules for the FUNLAB THE 40TH CREW campaign.

Eligibility, allocation and action transitions are pure and replayable.  The
Feishu adapter is kept in this module so the interaction handler can enforce
the central rule: write durably before acknowledging an application.
"""
from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import logging
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Mapping, Optional, Protocol, Sequence


CAMPAIGN_ID = "FUNLAB-THE40TH-CREW-2026"
CONTENT_AUTHORIZATION_VERSION = "the40th-content-v1"
TARGET_MARKETS = frozenset({"US", "DE", "FR", "CA", "MX"})
SUBMISSION_CUTOFF_AT = "2026-11-23T05:00:00Z"
PROMOTION_INVITE_CUTOFF_AT = "2026-12-02T05:00:00Z"


def _utc_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


SUBMISSION_CUTOFF_MS = _utc_ms(SUBMISSION_CUTOFF_AT)
PROMOTION_INVITE_CUTOFF_MS = _utc_ms(PROMOTION_INVITE_CUTOFF_AT)
CONTROLLER_SELECTED_LIMIT = 20
CONTROLLER_RESERVE_LIMIT = 10
CARDCASE_SELECTED_LIMIT = 40
CARDCASE_RESERVE_LIMIT = 10
RETRY_DELAYS_SECONDS = (60, 300, 1800, 7200, 21600)
_DRAFT_TTL_MS = 30 * 60 * 1000
_drafts: dict[str, dict] = {}
_CLAIM_LEASE_MS = 15 * 60 * 1000
THE40TH_PRIVACY_MARKER = "[THE40TH-PRIVACY-V1]"
logger = logging.getLogger(__name__)

_PROTECTED_APPLICATION_STATUSES = {
    "selected", "confirmed", "testing", "deliverables_complete", "withdrawn_or_expired",
}
_PROTECTED_TRACK_STATUSES = {
    "selected", "reserve", "confirmed", "testing", "deliverables_complete",
    "withdrawn_or_expired", "promoted_to_first20",
}
_SELECTED_TRACK_STATUSES = {
    "selected", "confirmed", "testing", "deliverables_complete", "promoted_to_first20",
}
_RESERVE_TRACK_STATUSES = {"reserve"}
_ALLOCATION_WRITTEN_STATUSES = _SELECTED_TRACK_STATUSES | _RESERVE_TRACK_STATUSES


@dataclass(frozen=True)
class Eligibility:
    base_eligible: bool
    controller_eligible: bool
    cardcase_eligible: bool
    invalid_reasons: tuple[str, ...]


@dataclass(frozen=True)
class Allocation:
    controller_selected: list[str]
    controller_reserve: list[str]
    cardcase_selected: list[str]
    cardcase_reserve: list[str]


@dataclass(frozen=True)
class RetryDecision:
    retryable: bool
    delay_seconds: int
    manual_action_required: bool
    failure_class: str


@dataclass(frozen=True)
class ExternalAction:
    idempotency_key: str
    campaign_id: str
    discord_user_id: str
    action_type: str
    action_version: str
    status: str
    retry_count: int
    next_retry_at: int
    failure_class: str
    manual_action_required: bool
    evidence: str
    created_at: int
    updated_at: int


class SubmissionLedger(Protocol):
    async def save_submission(
        self, *, discord_user_id: str, discord_username: str,
        submitted_answers: Mapping[str, object], now_ms: int,
    ) -> tuple[str, dict]: ...


@dataclass
class InteractionOutcome:
    response: dict
    work: Optional[Callable[[], Awaitable[None]]] = None


class ApplicationClaimStore:
    """Persistent single-writer claim for Feishu records.

    Coolify must mount the configured path on persistent storage and keep this
    service at one replica.  A crashed writer leaves a short lease; the next
    request first reconciles Feishu by deterministic application_id before it
    may create anything.
    """

    def __init__(self, path: str | Path | None = None):
        configured = path or os.environ.get(
            "DISCORD_THE40TH_CLAIM_DB", "/data/the40th_application_claims.sqlite3"
        )
        self.path = Path(configured)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.path), timeout=10)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS application_claims (
                application_id TEXT PRIMARY KEY,
                owner_token TEXT NOT NULL,
                record_id TEXT NOT NULL DEFAULT '',
                lease_until INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS allocation_runs (
                campaign_id TEXT PRIMARY KEY,
                snapshot_json TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )"""
        )
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(application_claims)")}
        if "lease_until" not in columns:
            connection.execute("ALTER TABLE application_claims ADD COLUMN lease_until INTEGER NOT NULL DEFAULT 0")
        return connection

    def allocation_state(self, campaign_id: str) -> dict | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT snapshot_json, status FROM allocation_runs WHERE campaign_id=?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                return None
            return {"updates": json.loads(str(row[0])), "status": str(row[1])}
        finally:
            connection.close()

    def begin_allocation(self, campaign_id: str, updates: Mapping[str, object]) -> dict:
        clock_ms = self._clock_ms()
        snapshot_json = json.dumps(updates, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """INSERT OR IGNORE INTO allocation_runs(campaign_id, snapshot_json, status, updated_at)
                   VALUES(?, ?, 'pending', ?)""",
                (campaign_id, snapshot_json, clock_ms),
            )
            row = connection.execute(
                "SELECT snapshot_json, status FROM allocation_runs WHERE campaign_id=?",
                (campaign_id,),
            ).fetchone()
            connection.commit()
            if row is None:
                raise RuntimeError("allocation snapshot could not be persisted")
            return {"updates": json.loads(str(row[0])), "status": str(row[1])}
        finally:
            connection.close()

    def finalize_allocation(self, campaign_id: str) -> None:
        clock_ms = self._clock_ms()
        connection = self._connect()
        try:
            changed = connection.execute(
                "UPDATE allocation_runs SET status='finalized', updated_at=? WHERE campaign_id=?",
                (clock_ms, campaign_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("allocation snapshot is missing")
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _clock_ms(now_ms: int | None = None) -> int:
        return int(time.time() * 1000) if now_ms is None else int(now_ms)

    def acquire(
        self, application_id_value: str, *, owner_token: str, now_ms: int | None = None,
    ) -> tuple[bool, str]:
        clock_ms = self._clock_ms(now_ms)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT owner_token, record_id, lease_until FROM application_claims WHERE application_id=?",
                (application_id_value,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO application_claims(application_id, owner_token, record_id, lease_until, updated_at) VALUES(?,?,?,?,?)",
                    (application_id_value, owner_token, "", clock_ms + _CLAIM_LEASE_MS, clock_ms),
                )
                connection.commit()
                return True, ""
            current_owner, record_id, lease_until = str(row[0]), str(row[1]), int(row[2])
            if current_owner == owner_token or not current_owner or clock_ms >= lease_until:
                connection.execute(
                    "UPDATE application_claims SET owner_token=?, lease_until=?, updated_at=? WHERE application_id=?",
                    (owner_token, clock_ms + _CLAIM_LEASE_MS, clock_ms, application_id_value),
                )
                connection.commit()
                return True, ""
            connection.commit()
            return False, record_id
        finally:
            connection.close()

    def renew(
        self, application_id_value: str, *, owner_token: str, now_ms: int | None = None,
    ) -> None:
        clock_ms = self._clock_ms(now_ms)
        connection = self._connect()
        try:
            changed = connection.execute(
                """UPDATE application_claims SET lease_until=?, updated_at=?
                   WHERE application_id=? AND owner_token=?""",
                (clock_ms + _CLAIM_LEASE_MS, clock_ms, application_id_value, owner_token),
            ).rowcount
            if changed != 1:
                raise RuntimeError("application claim lease was lost")
            connection.commit()
        finally:
            connection.close()

    def bind(
        self, application_id_value: str, *, owner_token: str, record_id: str,
        now_ms: int | None = None,
    ) -> None:
        clock_ms = self._clock_ms(now_ms)
        connection = self._connect()
        try:
            changed = connection.execute(
                """UPDATE application_claims SET record_id=?, updated_at=?
                   WHERE application_id=? AND owner_token=?""",
                (record_id, clock_ms, application_id_value, owner_token),
            ).rowcount
            if changed != 1:
                raise RuntimeError("application claim ownership changed before record binding")
            connection.commit()
        finally:
            connection.close()

    def release(
        self, application_id_value: str, *, owner_token: str, now_ms: int | None = None,
    ) -> None:
        clock_ms = self._clock_ms(now_ms)
        connection = self._connect()
        try:
            connection.execute(
                """UPDATE application_claims SET owner_token='', lease_until=0, updated_at=?
                   WHERE application_id=? AND owner_token=?""",
                (clock_ms, application_id_value, owner_token),
            )
            connection.commit()
        finally:
            connection.close()


def _truth(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y", "i agree"}


def _country(value: object) -> str:
    key = "".join(ch for ch in str(value or "").casefold() if ch.isalnum())
    aliases = {
        "us": "US", "usa": "US", "unitedstates": "US", "unitedstatesofamerica": "US",
        "de": "DE", "germany": "DE", "fr": "FR", "france": "FR",
        "ca": "CA", "canada": "CA", "mx": "MX", "mexico": "MX",
    }
    return aliases.get(key, "")


def _items(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        source: Iterable[object] = value.replace(",", ";").split(";")
    elif isinstance(value, Sequence):
        source = value
    else:
        source = ()
    return tuple(dict.fromkeys(str(item).strip() for item in source if str(item).strip()))


def _has_switch(value: object) -> bool:
    normalized = {item.casefold().replace(" ", "") for item in _items(value)}
    return bool(normalized & {"switch", "switch1", "switch2", "both"})


def evaluate_eligibility(answers: Mapping[str, object]) -> Eligibility:
    reasons: list[str] = []
    if _country(answers.get("country")) not in TARGET_MARKETS:
        reasons.append("unsupported_country")
    if not _truth(answers.get("age_confirmed")):
        reasons.append("under_18_or_unconfirmed")
    if not _has_switch(answers.get("devices")):
        reasons.append("switch_device_required")
    if not _truth(answers.get("truth_confirmed")):
        reasons.append("truth_confirmation_required")
    if not _truth(answers.get("privacy_confirmed")):
        reasons.append("privacy_confirmation_required")

    base = not reasons
    controller = base and all(
        _truth(answers.get(field))
        for field in (
            "can_complete_14_day_test",
            "controller_tasks_confirmed",
            "disclosure_confirmed",
            "content_authorization_confirmed",
        )
    )
    cardcase = base and _truth(answers.get("cardcase_opt_in"))
    return Eligibility(base, controller, cardcase, tuple(reasons))


def _select(custom_id: str, label: str, options: Sequence[tuple[str, str]], *, max_values: int = 1) -> dict:
    return {
        "type": 18,
        "label": label,
        "component": {
            "type": 3,
            "custom_id": custom_id,
            "options": [{"label": text, "value": value} for text, value in options],
            "min_values": 1,
            "max_values": max_values,
            "required": True,
        },
    }


def step1_modal(source: str = "Discord") -> dict:
    """Return the first Discord modal without legacy commerce questions."""
    return {
        "type": 9,
        "data": {
            "custom_id": f"the40th_apply_v1_step1.{source}",
            "title": "Step 1 Of 2 — Eligibility",
            "components": [
                _select("country", "Country Or Region", [(code, code) for code in ("US", "DE", "FR", "CA", "MX")]),
                _select(
                    "age_confirmed",
                    "18+, Privacy Notice And Truthful Answers",
                    [("I confirm all three", "yes")],
                ),
                _select("devices", "Nintendo Switch Device", [("Switch", "Switch"), ("Switch 2", "Switch 2"), ("Both", "Both")]),
                _select(
                    "game_preferences",
                    "Game Preferences",
                    [("Zelda", "Zelda"), ("Open-world", "open-world"),
                     ("Action-adventure", "action-adventure"), ("Other", "other")],
                    max_values=4,
                ),
                _select("can_complete_14_day_test", "Can You Complete A 14-Day Test?", [("Yes", "yes"), ("No", "no")]),
            ],
        },
    }


def step2_modal(signed_state: str) -> dict:
    """Return the second modal with separate task, authorization and pool consent."""
    return {
        "type": 9,
        "data": {
            "custom_id": f"the40th_apply_v1_step2.{signed_state}",
            "title": "Step 2 Of 2 — Tasks And Consent",
            "components": [
                _select(
                    "controller_tasks_confirmed",
                    "Controller Test Tasks",
                    [("Agree: 2 posts + Day 7 Discord update + feedback", "yes"), ("Not for me", "no")],
                ),
                _select("cardcase_opt_in", "Consider Me For A Game Card Case", [("Yes", "yes"), ("No", "no")]),
                _select("disclosure_confirmed", "Public Content Disclosure", [("I agree to disclose", "yes"), ("No", "no")]),
                _select(
                    "content_authorization_confirmed",
                    "Separate Content Use Authorization",
                    [("Agree: owned channels + 12-month global paid ads; no whitelisting", "yes"),
                     ("I do not agree", "no")],
                ),
                _select(
                    "future_test_contact",
                    "May FUNLAB Contact You About Future Tests?",
                    [("Yes, optional future test contact is okay", "yes"),
                     ("No, contact me only about this application", "no")],
                ),
            ],
        },
    }


def privacy_dm_payload(source: str = "Discord", form_custom_id: str = "") -> dict:
    """Explain the application boundary before the private form is opened."""
    return {
        "content": (
            f"{THE40TH_PRIVACY_MARKER}\n"
            "FUNLAB THE 40TH CREW application details:\n"
            "• Open to adults (18+) in US, DE, FR, CA and MX who actively use Switch/Switch 2.\n"
            "• We collect your Discord identity, country, device, game preferences, task choices, "
            "consent choices and campaign source to operate this test.\n"
            "• We do not ask for an address, phone number, purchase proof or Amazon activity in this form.\n"
            "• Application and campaign-status data are retained until 12 months after campaign close, "
            "then deleted or anonymized. Shipping details are collected separately and deleted 90 days "
            "after delivery and shipping disputes close.\n"
            "• Optional future-test contact can be declined or withdrawn. Content-use authorization is "
            "separate and can be declined or withdrawn; minimum authorization evidence is retained until "
            "12 months after the authorization period ends.\n"
            "• FUNLAB acknowledges takedown requests within 2 business days and completes controllable "
            "owned-channel/active-paid-use removal within 10 business days.\n"
            "• Submission does not guarantee selection. You may ask a moderator to withdraw before testing begins."
        ),
        "components": [{"type": 1, "components": [{
            "type": 2,
            "style": 1,
            "label": "Open Private Application",
            "custom_id": form_custom_id or f"the40th_form_start.{source}.invalid",
        }]}],
    }


def application_id(discord_user_id: str) -> str:
    digest = hashlib.sha256(f"{CAMPAIGN_ID}:{discord_user_id}".encode("utf-8")).hexdigest()[:20]
    return f"the40th-{digest}"


def privacy_dm_nonce(discord_user_id: str, request_key: str = "") -> str:
    """Campaign-specific Discord nonce; never overlaps the 2026-09 Direct campaign."""
    digest = hashlib.sha256(
        f"{CAMPAIGN_ID}:privacy-dm:{discord_user_id}:{request_key}".encode("utf-8")
    ).hexdigest()
    return str(int(digest[:16], 16))


_BITABLE_FIELDS = {
    "discord_user_id": "Discord用户ID",
    "discord_username": "Discord用户名",
    "campaign_id": "活动批次",
    "application_status": "报名状态",
    "country": "国家或地区",
    "age_confirmed": "年满18岁",
    "devices": "设备",
    "game_preferences": "喜爱游戏IP",
    "can_complete_14_day_test": "14天体验确认",
    "controller_tasks_confirmed": "手柄任务确认",
    "cardcase_opt_in": "卡盒后备选择",
    "disclosure_confirmed": "内容披露确认",
    "content_authorization_confirmed": "内容授权结果",
    "content_authorization_version": "内容授权版本",
    "content_authorization_consented_at": "内容授权同意时间",
    "content_authorization_last_granted_at": "内容授权最近同意时间",
    "content_authorization_revoked_at": "内容授权撤回时间",
    "content_authorization_last_decision_at": "内容授权最近决定时间",
    "content_authorization_event_log": "内容授权事件记录",
    "application_source": "报名来源",
    "future_test_contact_opt_in": "future_test_contact_opt_in",
    "future_test_contact_opted_in_at": "future_test_contact_opted_in_at",
    "future_test_contact_revoked_at": "future_test_contact_revoked_at",
    "future_test_contact_event_log": "future_test_contact_event_log",
    "first_submitted_at": "first_submitted_at",
    "first_valid_submitted_at": "first_valid_submitted_at",
    "first_controller_eligible_at": "first_controller_eligible_at",
    "first_cardcase_eligible_at": "first_cardcase_eligible_at",
    "updated_at": "updated_at",
    "submission_cutoff_at": "submission_cutoff_at",
    "promotion_invite_cutoff_at": "promotion_invite_cutoff_at",
    "late_submission": "late_submission",
    "controller_late_submission": "controller_late_submission",
    "cardcase_late_submission": "cardcase_late_submission",
    "base_eligible": "base_eligible",
    "controller_eligible": "controller_eligible",
    "cardcase_eligible": "cardcase_eligible",
    "invalid_reason": "invalid_reason",
    "controller_status": "controller_status",
    "controller_reserve_rank": "controller_reserve_rank",
    "cardcase_status": "cardcase_status",
    "cardcase_reserve_rank": "cardcase_reserve_rank",
    "application_id": "application_id",
    "withdrawn_at": "withdrawn_at",
}


def to_bitable_fields(application: Mapping[str, object]) -> dict:
    """Map only fields approved for this campaign; legacy scoring is excluded."""
    fields: dict[str, object] = {}
    for internal, external in _BITABLE_FIELDS.items():
        if internal not in application:
            continue
        value = application[internal]
        if internal in {"devices", "game_preferences"}:
            value = list(_items(value))
        fields[external] = value
    return fields


def from_bitable_fields(fields: Mapping[str, object]) -> dict:
    reverse = {external: internal for internal, external in _BITABLE_FIELDS.items()}
    return {reverse[key]: value for key, value in fields.items() if key in reverse}


def _require_feishu_ok(result: Mapping[str, object], action: str) -> Mapping[str, object]:
    if result.get("code", 0) not in (0, None):
        raise RuntimeError(f"Feishu {action} failed: code={result.get('code')} msg={result.get('msg', '')}")
    return result


class The40thCrewLedger:
    """Feishu-backed, one-record-per-user campaign ledger.

    The table schema is configured separately.  This adapter never falls back
    to the legacy purchase/scoring fields.
    """

    def __init__(
        self,
        *,
        base_token: str = "KINabIENjak8fRsB6AHcIDALntc",
        table_id: str = "tblt8oRYMtaa8B4v",
        claim_store: ApplicationClaimStore | None = None,
    ):
        self.base_token = os.environ.get("DISCORD_THE40TH_BASE_TOKEN", base_token)
        self.table_id = os.environ.get("DISCORD_THE40TH_APPLICATION_TABLE_ID", table_id)
        self.claim_store = claim_store or ApplicationClaimStore()

    async def _find_application_records(self, app_id: str) -> list[dict]:
        """Use the non-indexed list path so crash recovery does not trust search freshness."""
        from . import feishu

        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records"
        matches: list[dict] = []
        page_token = ""
        while True:
            query = "?page_size=500" + (f"&page_token={page_token}" if page_token else "")
            result = _require_feishu_ok(
                await feishu.api("GET", path + query, which="bitable"),
                "THE 40TH CREW application list reconciliation",
            )
            data = result.get("data") or {}
            for item in data.get("items") or []:
                if str((item.get("fields") or {}).get("application_id") or "") == app_id:
                    matches.append(item)
            if not data.get("has_more") or not data.get("page_token"):
                return matches
            page_token = str(data["page_token"])

    async def save_submission(
        self, *, discord_user_id: str, discord_username: str,
        submitted_answers: Mapping[str, object], now_ms: int,
    ) -> tuple[str, dict]:
        from . import feishu

        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records"
        app_id = application_id(str(discord_user_id))
        owner_token = secrets.token_hex(12)
        is_owner, claimed_record_id = self.claim_store.acquire(
            app_id, owner_token=owner_token
        )
        if not is_owner:
            raise RuntimeError("application is already being processed; retry shortly")
        self.claim_store.renew(app_id, owner_token=owner_token)
        items = await self._find_application_records(app_id)
        if len(items) > 1:
            raise RuntimeError("duplicate THE 40TH CREW application records require manual review")
        if claimed_record_id and items and str(items[0].get("record_id") or "") != claimed_record_id:
            raise RuntimeError("application claim and Feishu record disagree; manual review required")
        existing = from_bitable_fields((items[0].get("fields") or {})) if items else None
        merged = merge_submission(
            existing=existing,
            discord_user_id=str(discord_user_id),
            discord_username=discord_username,
            submitted_answers=submitted_answers,
            now_ms=now_ms,
        )
        payload = {"fields": to_bitable_fields(merged)}
        if items:
            record_id = str(items[0].get("record_id") or "")
            if not claimed_record_id:
                self.claim_store.bind(app_id, owner_token=owner_token, record_id=record_id)
            self.claim_store.renew(app_id, owner_token=owner_token)
            _require_feishu_ok(
                await feishu.api("PUT", f"{path}/{record_id}", payload, which="bitable"),
                "THE 40TH CREW application update",
            )
        else:
            if claimed_record_id:
                raise RuntimeError("claimed Feishu record is not currently readable; manual review required")
            self.claim_store.renew(app_id, owner_token=owner_token)
            created = _require_feishu_ok(
                await feishu.api("POST", path, payload, which="bitable"),
                "THE 40TH CREW application create",
            )
            data = created.get("data") or {}
            record_id = str(((data.get("record") or {}).get("record_id")) or data.get("record_id") or "")
            if not record_id:
                raise RuntimeError("Feishu application create returned no record_id")
            self.claim_store.bind(app_id, owner_token=owner_token, record_id=record_id)
        self.claim_store.release(app_id, owner_token=owner_token)
        return record_id, merged

    async def list_campaign_applications(self) -> list[dict]:
        from . import feishu

        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records"
        rows: list[dict] = []
        page_token = ""
        while True:
            query = "?page_size=500" + (f"&page_token={page_token}" if page_token else "")
            result = _require_feishu_ok(
                await feishu.api("GET", path + query, which="bitable"),
                "THE 40TH CREW application list",
            )
            data = result.get("data") or {}
            for item in data.get("items") or []:
                application = from_bitable_fields(item.get("fields") or {})
                if application.get("campaign_id") != CAMPAIGN_ID:
                    continue
                application["record_id"] = str(item.get("record_id") or "")
                rows.append(application)
            if not data.get("has_more") or not data.get("page_token"):
                return rows
            page_token = str(data["page_token"])

    async def update_application(self, record_id: str, fields: Mapping[str, object]) -> None:
        from . import feishu

        if not record_id:
            raise ValueError("record_id is required")
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records/{record_id}"
        _require_feishu_ok(
            await feishu.api("PUT", path, {"fields": to_bitable_fields(fields)}, which="bitable"),
            "THE 40TH CREW allocation update",
        )

    async def apply_allocation(self, *, commit: bool = False) -> dict:
        rows = validate_unique_applications(await self.list_campaign_applications())
        state = self.claim_store.allocation_state(CAMPAIGN_ID)
        if state is None and _is_allocation_rerun(rows):
            raise RuntimeError("allocation state is missing for existing allocated records; manual review required")
        if state is None:
            updates = allocation_updates(rows, rerun=False)
            if commit:
                state = self.claim_store.begin_allocation(CAMPAIGN_ID, updates)
                updates = dict(state["updates"])
        elif state["status"] == "pending":
            updates = dict(state["updates"])
            current_users = {str(row.get("discord_user_id") or "") for row in rows}
            missing_users = set(updates) - current_users
            if missing_users:
                raise RuntimeError(
                    f"pending allocation records are missing; manual review required: {sorted(missing_users)}"
                )
            for row in rows:
                user_id = str(row.get("discord_user_id") or "")
                if user_id not in updates:
                    continue
                current = dict(updates[user_id])
                withdrawn = bool(row.get("withdrawn_at"))
                for status_field, rank_field in (
                    ("controller_status", "controller_reserve_rank"),
                    ("cardcase_status", "cardcase_reserve_rank"),
                ):
                    current_status = str(row.get(status_field) or "")
                    if withdrawn or current_status == "withdrawn_or_expired":
                        current[status_field] = "withdrawn_or_expired"
                        current[rank_field] = None
                    elif current_status in {
                        "confirmed",
                        "testing",
                        "deliverables_complete",
                        "promoted_to_first20",
                    }:
                        # A recovery replay may fill missing writes, but it must never
                        # move an application backwards from later operational work.
                        current[status_field] = current_status
                        current[rank_field] = row.get(rank_field)
                updates[user_id] = current
        else:
            updates = allocation_updates(rows, rerun=True)
        record_by_user = {
            str(row.get("discord_user_id") or ""): str(row.get("record_id") or "")
            for row in rows
        }
        if commit:
            for user_id, fields in updates.items():
                await self.update_application(record_by_user[user_id], fields)
            self.claim_store.finalize_allocation(CAMPAIGN_ID)
        selected_controller = sum(
            1 for fields in updates.values()
            if fields["controller_status"] in _SELECTED_TRACK_STATUSES
        )
        reserve_controller = sum(1 for fields in updates.values() if fields["controller_status"] == "reserve")
        selected_cardcase = sum(
            1 for fields in updates.values()
            if fields["cardcase_status"] in (_SELECTED_TRACK_STATUSES - {"promoted_to_first20"})
        )
        reserve_cardcase = sum(1 for fields in updates.values() if fields["cardcase_status"] == "reserve")
        return {
            "ok": True,
            "mode": "commit" if commit else "preview",
            "campaign_id": CAMPAIGN_ID,
            "application_count": len(rows),
            "controller_selected_count": selected_controller,
            "controller_reserve_count": reserve_controller,
            "cardcase_selected_count": selected_cardcase,
            "cardcase_reserve_count": reserve_cardcase,
            "controller_vacancy_count": max(0, CONTROLLER_SELECTED_LIMIT - selected_controller),
            "cardcase_vacancy_count": max(0, CARDCASE_SELECTED_LIMIT - selected_cardcase),
            "promotion_required": (
                selected_controller < CONTROLLER_SELECTED_LIMIT
                or selected_cardcase < CARDCASE_SELECTED_LIMIT
            ),
            "updates": updates,
        }


def merge_submission(
    *,
    existing: Mapping[str, object] | None,
    discord_user_id: str,
    discord_username: str,
    submitted_answers: Mapping[str, object],
    now_ms: int,
) -> dict:
    """Merge a submission while preserving all first-qualified timestamps."""
    if existing and existing.get("withdrawn_at"):
        raise ValueError("withdrawn applications cannot rejoin the current campaign")
    current = dict(existing or {})
    if current and (
        str(current.get("discord_user_id") or "") != str(discord_user_id)
        or str(current.get("campaign_id") or CAMPAIGN_ID) != CAMPAIGN_ID
    ):
        raise ValueError("existing application does not match this user and campaign")

    eligibility = evaluate_eligibility(submitted_answers)
    previous_application_status = str(current.get("application_status") or "")
    previous_controller_status = str(current.get("controller_status") or "")
    previous_cardcase_status = str(current.get("cardcase_status") or "")
    previous_authorization = _truth(current.get("content_authorization_confirmed"))
    authorization_seen_before = bool(existing) and "content_authorization_confirmed" in (existing or {})
    future_contact_seen_before = bool(existing) and "future_test_contact_opt_in" in (existing or {})
    current.update(dict(submitted_answers))
    current.update({
        "discord_user_id": str(discord_user_id),
        "discord_username": str(discord_username),
        "campaign_id": CAMPAIGN_ID,
        "application_id": str(current.get("application_id") or application_id(str(discord_user_id))),
        "first_submitted_at": current.get("first_submitted_at") or now_ms,
        "updated_at": now_ms,
        "submission_cutoff_at": SUBMISSION_CUTOFF_MS,
        "promotion_invite_cutoff_at": PROMOTION_INVITE_CUTOFF_MS,
        "base_eligible": eligibility.base_eligible,
        "controller_eligible": eligibility.controller_eligible,
        "cardcase_eligible": eligibility.cardcase_eligible,
        "invalid_reason": ";".join(eligibility.invalid_reasons),
    })
    if eligibility.base_eligible and not current.get("first_valid_submitted_at"):
        current["first_valid_submitted_at"] = now_ms
    if eligibility.controller_eligible and not current.get("first_controller_eligible_at"):
        current["first_controller_eligible_at"] = now_ms
    if eligibility.cardcase_eligible and not current.get("first_cardcase_eligible_at"):
        current["first_cardcase_eligible_at"] = now_ms
    first_valid = current.get("first_valid_submitted_at")
    current["late_submission"] = not (
        isinstance(first_valid, (int, float)) and int(first_valid) < SUBMISSION_CUTOFF_MS
    )
    controller_first = current.get("first_controller_eligible_at")
    cardcase_first = current.get("first_cardcase_eligible_at")
    current["controller_late_submission"] = bool(
        isinstance(controller_first, (int, float)) and int(controller_first) >= SUBMISSION_CUTOFF_MS
    )
    current["cardcase_late_submission"] = bool(
        isinstance(cardcase_first, (int, float)) and int(cardcase_first) >= SUBMISSION_CUTOFF_MS
    )

    current["application_status"] = (
        previous_application_status
        if previous_application_status in _PROTECTED_APPLICATION_STATUSES
        else ("submitted_valid" if eligibility.base_eligible else "submitted_invalid")
    )
    current["controller_status"] = (
        previous_controller_status
        if previous_controller_status in _PROTECTED_TRACK_STATUSES
        else ("eligible_unallocated" if eligibility.controller_eligible else "not_eligible")
    )
    current["cardcase_status"] = (
        previous_cardcase_status
        if previous_cardcase_status in _PROTECTED_TRACK_STATUSES
        else ("eligible_unallocated" if eligibility.cardcase_eligible else "not_eligible")
    )

    authorization_now = _truth(submitted_answers.get("content_authorization_confirmed"))
    current["content_authorization_last_decision_at"] = now_ms
    if authorization_now:
        current["content_authorization_version"] = str(
            submitted_answers.get("content_authorization_version") or CONTENT_AUTHORIZATION_VERSION
        )
        if not current.get("content_authorization_consented_at"):
            current["content_authorization_consented_at"] = now_ms
        if not previous_authorization:
            current["content_authorization_last_granted_at"] = now_ms
        current["content_authorization_revoked_at"] = None
    elif previous_authorization:
        current["content_authorization_revoked_at"] = now_ms
    else:
        current["content_authorization_version"] = str(
            (existing or {}).get("content_authorization_version") or ""
        )
    if not authorization_seen_before or authorization_now != previous_authorization:
        previous_log = str((existing or {}).get("content_authorization_event_log") or "").strip()
        event = (
            f"{now_ms}|{'consented' if authorization_now else 'declined_or_withdrawn'}|"
            f"{CONTENT_AUTHORIZATION_VERSION}|source=discord_interaction|actor={discord_user_id}"
        )
        current["content_authorization_event_log"] = "\n".join(filter(None, (previous_log, event)))

    previous_pool = _truth((existing or {}).get("future_test_contact_opt_in"))
    current_pool = _truth(submitted_answers.get("future_test_contact_opt_in"))
    current["future_test_contact_opt_in"] = current_pool
    if current_pool and not current.get("future_test_contact_opted_in_at"):
        current["future_test_contact_opted_in_at"] = now_ms
        current["future_test_contact_revoked_at"] = None
    elif current_pool:
        current["future_test_contact_revoked_at"] = None
    elif previous_pool and not current_pool:
        current["future_test_contact_revoked_at"] = now_ms
    if not future_contact_seen_before or current_pool != previous_pool:
        previous_log = str((existing or {}).get("future_test_contact_event_log") or "").strip()
        event = (
            f"{now_ms}|{'opted_in' if current_pool else 'declined_or_revoked'}|"
            f"source=discord_interaction|actor={discord_user_id}"
        )
        current["future_test_contact_event_log"] = "\n".join(filter(None, (previous_log, event)))
    return current


def _eligible_rows(records: Iterable[Mapping[str, object]], *, track: str) -> list[Mapping[str, object]]:
    eligible_field = f"{track}_eligible"
    time_field = f"first_{track}_eligible_at"
    rows = []
    for row in records:
        timestamp = row.get(time_field)
        if (
            str(row.get("campaign_id") or "") == CAMPAIGN_ID
            and _truth(row.get(eligible_field))
            and not row.get("withdrawn_at")
            and isinstance(timestamp, (int, float))
            and int(timestamp) < SUBMISSION_CUTOFF_MS
        ):
            rows.append(row)
    return sorted(
        rows,
        key=lambda row: (
            int(row[time_field]),
            str(row.get("record_id") or ""),
            str(row.get("discord_user_id") or ""),
        ),
    )


def validate_unique_applications(records: Iterable[Mapping[str, object]]) -> list[Mapping[str, object]]:
    """Fail closed when one campaign user/application maps to multiple records."""
    rows = list(records)
    users: dict[str, list[str]] = {}
    applications: dict[str, list[str]] = {}
    for row in rows:
        if str(row.get("campaign_id") or "") != CAMPAIGN_ID:
            continue
        record_id = str(row.get("record_id") or "<missing-record-id>")
        user_id = str(row.get("discord_user_id") or "")
        app_id = str(row.get("application_id") or "")
        if user_id:
            users.setdefault(user_id, []).append(record_id)
        if app_id:
            applications.setdefault(app_id, []).append(record_id)
    duplicate_users = {key: ids for key, ids in users.items() if len(ids) > 1}
    duplicate_applications = {key: ids for key, ids in applications.items() if len(ids) > 1}
    if duplicate_users or duplicate_applications:
        raise RuntimeError(
            "duplicate THE 40TH CREW allocation records require manual review: "
            f"users={duplicate_users}; applications={duplicate_applications}"
        )
    return rows


def _allocate_track(
    rows: list[Mapping[str, object]], *, track: str, selected_limit: int,
    reserve_limit: int, excluded_selected: set[str] | None = None,
) -> tuple[list[str], list[str]]:
    eligible = [
        row for row in _eligible_rows(rows, track=track)
        if str(row.get("discord_user_id") or "") not in (excluded_selected or set())
    ]
    status_field = f"{track}_status"
    frozen_selected = [
        str(row["discord_user_id"]) for row in eligible
        if str(row.get(status_field) or "") in _SELECTED_TRACK_STATUSES
    ]
    if len(frozen_selected) > selected_limit:
        raise RuntimeError(f"existing {track} selected records exceed the configured limit")
    selected = list(frozen_selected)
    selected_set = set(selected)
    for row in eligible:
        user_id = str(row["discord_user_id"])
        if len(selected) >= selected_limit:
            break
        if user_id not in selected_set:
            selected.append(user_id)
            selected_set.add(user_id)

    frozen_reserve = [
        str(row["discord_user_id"]) for row in eligible
        if str(row.get(status_field) or "") in _RESERVE_TRACK_STATUSES
        and str(row["discord_user_id"]) not in selected_set
    ]
    if len(frozen_reserve) > reserve_limit:
        raise RuntimeError(f"existing {track} reserve records exceed the configured limit")
    reserve = list(frozen_reserve)
    reserve_set = set(reserve)
    for row in eligible:
        user_id = str(row["discord_user_id"])
        if len(reserve) >= reserve_limit:
            break
        if user_id not in selected_set and user_id not in reserve_set:
            reserve.append(user_id)
            reserve_set.add(user_id)
    return selected, reserve


def _is_allocation_rerun(rows: Iterable[Mapping[str, object]]) -> bool:
    return any(
        str(row.get(field) or "") in _ALLOCATION_WRITTEN_STATUSES
        for row in rows
        for field in ("controller_status", "cardcase_status")
    )


def _existing_track_allocation(
    rows: list[Mapping[str, object]], *, track: str, selected_limit: int,
    reserve_limit: int,
) -> tuple[list[str], list[str]]:
    status_field = f"{track}_status"
    rank_field = f"{track}_reserve_rank"
    selected_rows = [
        row for row in rows
        if not row.get("withdrawn_at")
        and str(row.get(status_field) or "") in _SELECTED_TRACK_STATUSES
        and not (track == "cardcase" and str(row.get(status_field) or "") == "promoted_to_first20")
    ]
    reserve_rows = [
        row for row in rows
        if not row.get("withdrawn_at") and str(row.get(status_field) or "") == "reserve"
    ]
    selected = [str(row.get("discord_user_id") or "") for row in selected_rows]
    reserve = [
        str(row.get("discord_user_id") or "")
        for row in sorted(
            reserve_rows,
            key=lambda row: (
                int(row.get(rank_field) or reserve_limit + 1),
                str(row.get("record_id") or ""),
            ),
        )
    ]
    if len(selected) > selected_limit or len(reserve) > reserve_limit:
        raise RuntimeError(f"existing {track} allocation exceeds the configured limit")
    return selected, reserve


def allocate(
    records: Iterable[Mapping[str, object]], *, rerun: bool = False,
) -> Allocation:
    rows = validate_unique_applications(records)
    if rerun:
        controller_selected, controller_reserve = _existing_track_allocation(
            rows, track="controller", selected_limit=CONTROLLER_SELECTED_LIMIT,
            reserve_limit=CONTROLLER_RESERVE_LIMIT,
        )
        cardcase_selected, cardcase_reserve = _existing_track_allocation(
            rows, track="cardcase", selected_limit=CARDCASE_SELECTED_LIMIT,
            reserve_limit=CARDCASE_RESERVE_LIMIT,
        )
        overlap = set(controller_selected) & set(cardcase_selected)
        if overlap:
            raise RuntimeError(
                f"existing dual-track selected records require manual review: {sorted(overlap)}"
            )
        return Allocation(
            controller_selected, controller_reserve, cardcase_selected, cardcase_reserve,
        )
    controller_selected, controller_reserve = _allocate_track(
        rows, track="controller", selected_limit=CONTROLLER_SELECTED_LIMIT,
        reserve_limit=CONTROLLER_RESERVE_LIMIT,
    )
    cardcase_selected, cardcase_reserve = _allocate_track(
        rows, track="cardcase", selected_limit=CARDCASE_SELECTED_LIMIT,
        reserve_limit=CARDCASE_RESERVE_LIMIT, excluded_selected=set(controller_selected),
    )
    return Allocation(controller_selected, controller_reserve, cardcase_selected, cardcase_reserve)


def allocation_updates(
    records: Iterable[Mapping[str, object]], *, rerun: bool = False,
) -> dict[str, dict]:
    """Return reproducible per-user status/rank writes for a result preview."""
    rows = validate_unique_applications(records)
    allocation = allocate(rows, rerun=rerun)
    controller_selected = set(allocation.controller_selected)
    controller_reserve = {user_id: rank for rank, user_id in enumerate(allocation.controller_reserve, 1)}
    cardcase_selected = set(allocation.cardcase_selected)
    cardcase_reserve = {user_id: rank for rank, user_id in enumerate(allocation.cardcase_reserve, 1)}
    updates: dict[str, dict] = {}
    for row in rows:
        if str(row.get("campaign_id") or "") != CAMPAIGN_ID:
            continue
        user_id = str(row.get("discord_user_id") or "")
        if not user_id:
            continue
        withdrawn = bool(row.get("withdrawn_at"))
        controller_time = row.get("first_controller_eligible_at")
        cardcase_time = row.get("first_cardcase_eligible_at")
        controller_expired = isinstance(controller_time, (int, float)) and int(controller_time) >= SUBMISSION_CUTOFF_MS
        cardcase_expired = isinstance(cardcase_time, (int, float)) and int(cardcase_time) >= SUBMISSION_CUTOFF_MS

        controller_status = "withdrawn_or_expired" if withdrawn or controller_expired else "not_eligible"
        controller_rank = None
        if not withdrawn and not controller_expired and _truth(row.get("controller_eligible")):
            controller_status = "eligible_unallocated"
        if user_id in controller_selected:
            controller_status = "selected"
        elif user_id in controller_reserve:
            controller_status = "reserve"
            controller_rank = controller_reserve[user_id]

        cardcase_status = "withdrawn_or_expired" if withdrawn or cardcase_expired else "not_eligible"
        cardcase_rank = None
        if not withdrawn and not cardcase_expired and _truth(row.get("cardcase_eligible")):
            cardcase_status = "eligible_unallocated"
        if user_id in cardcase_selected:
            cardcase_status = "selected"
        elif user_id in cardcase_reserve:
            cardcase_status = "reserve"
            cardcase_rank = cardcase_reserve[user_id]
        previous_controller_status = str(row.get("controller_status") or "")
        previous_cardcase_status = str(row.get("cardcase_status") or "")
        if rerun and not withdrawn and previous_controller_status in _ALLOCATION_WRITTEN_STATUSES:
            controller_status = previous_controller_status
            controller_rank = row.get("controller_reserve_rank") if controller_status == "reserve" else None
        elif user_id in controller_selected and previous_controller_status in _SELECTED_TRACK_STATUSES:
            controller_status = previous_controller_status
        if rerun and not withdrawn and previous_cardcase_status in _ALLOCATION_WRITTEN_STATUSES:
            cardcase_status = previous_cardcase_status
            cardcase_rank = row.get("cardcase_reserve_rank") if cardcase_status == "reserve" else None
        elif user_id in cardcase_selected and previous_cardcase_status in _SELECTED_TRACK_STATUSES:
            cardcase_status = previous_cardcase_status
        updates[user_id] = {
            "controller_status": controller_status,
            "controller_reserve_rank": controller_rank,
            "cardcase_status": cardcase_status,
            "cardcase_reserve_rank": cardcase_rank,
        }
    return updates


def confirmation_due_at(selected_at_ms: int) -> int:
    return int(selected_at_ms) + 72 * 60 * 60 * 1000


def promote_controller(
    allocation: Allocation,
    *,
    discord_user_id: str,
    replacing_user_id: str,
    invited_at_ms: int,
) -> Allocation:
    if invited_at_ms >= PROMOTION_INVITE_CUTOFF_MS:
        raise ValueError("promotion invitation cutoff has passed")
    if discord_user_id not in allocation.controller_reserve:
        raise ValueError("controller promotion must use the next recorded reserve")
    if replacing_user_id not in allocation.controller_selected:
        raise ValueError("controller promotion requires a selected vacancy")
    if allocation.controller_reserve[0] != discord_user_id:
        raise ValueError("controller promotion must preserve reserve order")

    controller_selected = [
        discord_user_id if user_id == replacing_user_id else user_id
        for user_id in allocation.controller_selected
    ]
    controller_reserve = [user_id for user_id in allocation.controller_reserve if user_id != discord_user_id]
    cardcase_selected = [user_id for user_id in allocation.cardcase_selected if user_id != discord_user_id]
    cardcase_reserve = list(allocation.cardcase_reserve)
    if len(cardcase_selected) < CARDCASE_SELECTED_LIMIT and cardcase_reserve:
        cardcase_selected.append(cardcase_reserve.pop(0))
    return Allocation(controller_selected, controller_reserve, cardcase_selected, cardcase_reserve)


def action_idempotency_key(discord_user_id: str, action_type: str, action_version: str) -> str:
    return f"{CAMPAIGN_ID}:{discord_user_id}:{action_type}:{action_version}"


def new_action(
    discord_user_id: str, action_type: str, action_version: str, *, now_ms: int,
) -> ExternalAction:
    return ExternalAction(
        idempotency_key=action_idempotency_key(discord_user_id, action_type, action_version),
        campaign_id=CAMPAIGN_ID,
        discord_user_id=str(discord_user_id),
        action_type=action_type,
        action_version=action_version,
        status="pending",
        retry_count=0,
        next_retry_at=0,
        failure_class="",
        manual_action_required=False,
        evidence="",
        created_at=int(now_ms),
        updated_at=int(now_ms),
    )


def mark_action_applied(action: ExternalAction, *, now_ms: int, evidence: str) -> ExternalAction:
    return replace(
        action,
        status="applied",
        next_retry_at=0,
        failure_class="",
        manual_action_required=False,
        evidence=evidence,
        updated_at=int(now_ms),
    )


def retry_decision(failure_class: str, *, attempt_count: int, retry_after_seconds: int | None = None) -> RetryDecision:
    retryable_classes = {"discord_429", "discord_5xx", "network_timeout", "feishu_5xx"}
    if failure_class not in retryable_classes or attempt_count > len(RETRY_DELAYS_SECONDS):
        return RetryDecision(False, 0, True, failure_class)
    delay = RETRY_DELAYS_SECONDS[max(0, attempt_count - 1)]
    if retry_after_seconds is not None:
        delay = max(delay, max(0, int(retry_after_seconds)))
    return RetryDecision(True, delay, False, failure_class)


def mark_action_failed(
    action: ExternalAction,
    failure_class: str,
    *,
    now_ms: int,
    retry_after_seconds: int | None = None,
) -> ExternalAction:
    attempt_count = action.retry_count + 1
    decision = retry_decision(
        failure_class,
        attempt_count=attempt_count,
        retry_after_seconds=retry_after_seconds,
    )
    return replace(
        action,
        status="failed",
        retry_count=attempt_count,
        next_retry_at=(int(now_ms) + decision.delay_seconds * 1000) if decision.retryable else 0,
        failure_class=failure_class,
        manual_action_required=decision.manual_action_required,
        updated_at=int(now_ms),
    )


def requeue_action(action: ExternalAction, *, now_ms: int) -> ExternalAction:
    if not action.manual_action_required:
        raise ValueError("only a manually gated action can be explicitly requeued")
    return replace(
        action,
        status="pending",
        next_retry_at=0,
        failure_class="",
        manual_action_required=False,
        updated_at=int(now_ms),
    )


def _extract_values(data: Mapping[str, object]) -> dict[str, str]:
    values: dict[str, str] = {}

    def walk(node: object) -> None:
        if isinstance(node, Mapping):
            custom_id = node.get("custom_id")
            if custom_id and "values" in node:
                values[str(custom_id)] = ";".join(str(item).strip() for item in (node.get("values") or []))
            elif custom_id and "value" in node:
                values[str(custom_id)] = str(node.get("value") or "").strip()
            for child in node.values():
                walk(child)
        elif isinstance(node, Sequence) and not isinstance(node, (str, bytes)):
            for child in node:
                walk(child)

    walk(data.get("components") or [])
    return values


def _user(payload: Mapping[str, object]) -> tuple[str, str]:
    member = payload.get("member") or {}
    user = member.get("user") or payload.get("user") or {}
    return str(user.get("id") or ""), str(user.get("username") or user.get("global_name") or "")


def _sign_reference(reference: str, secret: str) -> str:
    signature = hmac.new(secret.encode("utf-8"), reference.encode("ascii"), hashlib.sha256).hexdigest()[:20]
    return f"{reference}.{signature}"


def _privacy_form_custom_id(
    *, discord_user_id: str, source: str, expires_at_ms: int, secret: str,
) -> str:
    material = f"{discord_user_id}|{source}|{int(expires_at_ms)}"
    signature = hmac.new(secret.encode("utf-8"), material.encode("utf-8"), hashlib.sha256).hexdigest()[:20]
    return f"the40th_form_start.{source}.{int(expires_at_ms)}.{signature}"


def _verify_privacy_form_custom_id(
    custom_id: str, *, discord_user_id: str, now_ms: int, secret: str,
) -> str:
    try:
        prefix, source, expires_text, supplied = custom_id.split(".", 3)
        expires_at_ms = int(expires_text)
    except (ValueError, TypeError):
        return ""
    if prefix != "the40th_form_start" or source not in {"Discord", "Instagram", "Facebook", "X", "Other"}:
        return ""
    if now_ms >= expires_at_ms:
        return ""
    expected = _privacy_form_custom_id(
        discord_user_id=discord_user_id,
        source=source,
        expires_at_ms=expires_at_ms,
        secret=secret,
    ).rsplit(".", 1)[1]
    return source if hmac.compare_digest(supplied, expected) else ""


def _store_draft(data: Mapping[str, object], secret: str) -> str:
    now_ms = int(datetime.now().timestamp() * 1000)
    for stale_reference, state in list(_drafts.items()):
        if int(state.get("expires_at_ms") or 0) < now_ms:
            _drafts.pop(stale_reference, None)
    reference = secrets.token_urlsafe(9)
    _drafts[reference] = dict(data)
    return _sign_reference(reference, secret)


def _read_state(token: str, secret: str, *, discord_user_id: str, now_ms: int) -> dict:
    try:
        reference, supplied = token.rsplit(".", 1)
        expected = _sign_reference(reference, secret).rsplit(".", 1)[1]
        if not hmac.compare_digest(supplied, expected):
            return {}
        state = _drafts.get(reference) or {}
        if str(state.get("discord_user_id") or "") != discord_user_id:
            return {}
        if int(state.get("expires_at_ms") or 0) < now_ms:
            _drafts.pop(reference, None)
            return {}
        return state
    except (ValueError, TypeError, UnicodeDecodeError):
        return {}


def _consume_state(token: str) -> None:
    try:
        reference = token.rsplit(".", 1)[0]
    except (ValueError, TypeError):
        return
    _drafts.pop(reference, None)


def _error_response(message: str) -> InteractionOutcome:
    return InteractionOutcome({"type": 4, "data": {"content": message, "flags": 64}})


def _parse_step1(values: Mapping[str, str]) -> tuple[dict, str]:
    country = _country(values.get("country"))
    if country not in TARGET_MARKETS:
        return {}, "This campaign is currently open only in US, DE, FR, CA and MX."
    if not _truth(values.get("age_confirmed")):
        return {}, "You must confirm that you are 18+, read the privacy notice, and answered truthfully."
    if not _has_switch(values.get("devices")):
        return {}, "An actively used Nintendo Switch or Switch 2 is required."
    return {
        "country": country,
        "age_confirmed": True,
        "privacy_confirmed": True,
        "truth_confirmed": True,
        "devices": list(_items(values.get("devices"))),
        "game_preferences": list(_items(values.get("game_preferences"))),
        "can_complete_14_day_test": _truth(values.get("can_complete_14_day_test")),
    }, ""


def _parse_step2(values: Mapping[str, str]) -> tuple[dict, str]:
    future = values.get("future_test_contact", "")
    if future not in {"yes", "no"}:
        return {}, "Choose whether FUNLAB may contact you about future tests."
    return {
        "controller_tasks_confirmed": _truth(values.get("controller_tasks_confirmed")),
        "cardcase_opt_in": _truth(values.get("cardcase_opt_in")),
        "disclosure_confirmed": _truth(values.get("disclosure_confirmed")),
        "content_authorization_confirmed": _truth(values.get("content_authorization_confirmed")),
        "content_authorization_version": CONTENT_AUTHORIZATION_VERSION,
        "future_test_contact_opt_in": future == "yes",
    }, ""


async def build_interaction_outcome(
    payload: Mapping[str, object],
    *,
    signing_secret: str,
    ledger: SubmissionLedger | None = None,
    completion_notifier: Callable[[str], Awaitable[None]] | None = None,
    direct_message_sender: Callable[[str, dict], Awaitable[object]] | None = None,
    now_ms: int | None = None,
) -> InteractionOutcome:
    """Handle only THE 40TH CREW custom IDs; callers route by prefix."""
    if not signing_secret:
        return _error_response("Application signing is temporarily unavailable. Please try again later.")
    ledger = ledger or The40thCrewLedger()
    now_ms = int(datetime.now().timestamp() * 1000) if now_ms is None else int(now_ms)
    custom_id = str((payload.get("data") or {}).get("custom_id") or "")
    discord_user_id, discord_username = _user(payload)
    if not discord_user_id:
        return _error_response("We could not confirm your Discord identity. Please reopen the application in the server.")

    if custom_id == "the40th_apply_start" or custom_id.startswith("the40th_apply_start."):
        source = custom_id.removeprefix("the40th_apply_start.") if "." in custom_id else "Discord"
        if source not in {"Discord", "Instagram", "Facebook", "X", "Other"}:
            return _error_response("This campaign source is not valid. Please reopen the official FUNLAB post.")
        if direct_message_sender is None:
            return _error_response("FUN Bot private application delivery is temporarily unavailable. Please try again later.")

        async def send_privacy_notice() -> None:
            try:
                form_custom_id = _privacy_form_custom_id(
                    discord_user_id=discord_user_id,
                    source=source,
                    expires_at_ms=now_ms + _DRAFT_TTL_MS,
                    secret=signing_secret,
                )
                await direct_message_sender(
                    discord_user_id,
                    privacy_dm_payload(source, form_custom_id),
                )
                message = "Check your DMs — FUN Bot sent the private application details and privacy notice."
            except Exception as exc:
                logger.exception(
                    "the40th_privacy_dm_failed user=%s error_type=%s",
                    discord_user_id,
                    type(exc).__name__,
                )
                message = (
                    "FUN Bot could not deliver the private application. Enable direct messages from "
                    "FUNLAB server members, then tap Apply again."
                )
            if completion_notifier:
                try:
                    await completion_notifier(message)
                except Exception:
                    logger.exception("the40th_privacy_dm_completion_notice_failed user=%s", discord_user_id)

        return InteractionOutcome({"type": 5, "data": {"flags": 64}}, send_privacy_notice)

    if custom_id.startswith("the40th_form_start."):
        source = _verify_privacy_form_custom_id(
            custom_id,
            discord_user_id=discord_user_id,
            now_ms=now_ms,
            secret=signing_secret,
        )
        if not source:
            return _error_response(
                "This private application link is invalid or expired. Reopen the official FUNLAB post to receive a new one."
            )
        return InteractionOutcome(step1_modal(source))

    if custom_id.startswith("the40th_apply_v1_step1."):
        source = custom_id.removeprefix("the40th_apply_v1_step1.")
        if source not in {"Discord", "Instagram", "Facebook", "X", "Other"}:
            return _error_response("This campaign source is not valid. Please reopen the official FUNLAB post.")
        step1, error = _parse_step1(_extract_values(payload.get("data") or {}))
        if error:
            return _error_response(error)
        token = _store_draft(
            {
                "discord_user_id": discord_user_id,
                "step1": {**step1, "application_source": source},
                "expires_at_ms": now_ms + _DRAFT_TTL_MS,
            },
            signing_secret,
        )
        return InteractionOutcome({
            "type": 4,
            "data": {
                "content": "Step 1 is complete. Continue to tasks and consent.",
                "flags": 64,
                "components": [{"type": 1, "components": [{
                    "type": 2, "style": 1, "label": "Continue To Step 2",
                    "custom_id": f"the40th_apply_continue.{token}",
                }]}],
            },
        })

    if custom_id.startswith("the40th_apply_continue."):
        token = custom_id.removeprefix("the40th_apply_continue.")
        if not _read_state(token, signing_secret, discord_user_id=discord_user_id, now_ms=now_ms):
            return _error_response("This application step expired or belongs to another account. Please start again.")
        return InteractionOutcome(step2_modal(token))

    if custom_id.startswith("the40th_apply_v1_step2."):
        token = custom_id.removeprefix("the40th_apply_v1_step2.")
        state = _read_state(token, signing_secret, discord_user_id=discord_user_id, now_ms=now_ms)
        if not state:
            return _error_response("This application step expired or belongs to another account. Please start again.")
        step2, error = _parse_step2(_extract_values(payload.get("data") or {}))
        if error:
            return _error_response(error)
        submitted_answers = {**(state.get("step1") or {}), **step2}

        async def save_and_confirm() -> None:
            notify = completion_notifier
            async def send_notice(message: str) -> None:
                if not notify:
                    return
                pending = notify(message)
                if inspect.isawaitable(pending):
                    await pending
            try:
                _, merged = await ledger.save_submission(
                    discord_user_id=discord_user_id,
                    discord_username=discord_username,
                    submitted_answers=submitted_answers,
                    now_ms=now_ms,
                )
            except Exception as exc:
                logger.exception(
                    "the40th_submission_save_failed user=%s application_id=%s error_type=%s",
                    discord_user_id,
                    application_id(discord_user_id),
                    type(exc).__name__,
                )
                try:
                    await send_notice(
                        "Your application was not saved, so no queue position was assigned. "
                        "Please try again later or contact a moderator."
                    )
                except Exception:
                    logger.exception("the40th_submission_failure_notice_failed user=%s", discord_user_id)
                return

            _consume_state(token)
            current_tracks = []
            if merged.get("controller_eligible") and not merged.get("controller_late_submission"):
                current_tracks.append("controller")
            if merged.get("cardcase_eligible") and not merged.get("cardcase_late_submission"):
                current_tracks.append("game-card case")
            if current_tracks:
                status = f"eligible for current {' and '.join(current_tracks)} allocation"
            elif merged.get("base_eligible"):
                status = "received after the current allocation deadline; no current queue position"
            else:
                status = "needs correction; no current queue position"
            try:
                await send_notice(
                    f"Application submitted successfully. Reference: {merged['application_id']}. "
                    f"Current status: {status}."
                )
            except Exception:
                logger.exception(
                    "the40th_submission_saved_notice_failed user=%s application_id=%s",
                    discord_user_id,
                    merged.get("application_id"),
                )

        return InteractionOutcome({"type": 5, "data": {"flags": 64}}, save_and_confirm)

    return _error_response("This campaign action is no longer available.")
