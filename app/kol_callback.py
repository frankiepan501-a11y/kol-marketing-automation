"""Stable long-connection callback consumer for KOL媒体助手.

The Feishu App receives only KOL card actions. Business handling remains in the
existing n8n Event Hub during R8; this module normalizes and relays the event,
without changing any email or Base business rule.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path

import httpx

from . import config


STATE = {
    "enabled": False,
    "connection": "idle",
    "started_at": None,
    "last_event_at": None,
    "last_action": None,
    "last_relay_ok": None,
    "pending_count": 0,
    "error": None,
}

_ACTION_PREFIXES = ("draft_", "tp_", "kol_roi_map_", "kol_no_email_")
_EXACT_ACTIONS = frozenset({"warm_recap_send", "launch_reply_attribution_confirm"})
_CHANNEL = None
_THREAD = None
_SPOOL_LOCK = threading.Lock()


def _action_value(event) -> dict:
    value = getattr(getattr(event, "action", None), "value", None)
    return copy.deepcopy(value) if isinstance(value, dict) else {}


def action_name(event) -> str:
    return str(_action_value(event).get("action") or "")


def is_allowed_action(action: str) -> bool:
    action = str(action or "")
    return action in _EXACT_ACTIONS or action.startswith(_ACTION_PREFIXES)


def callback_idempotency_key(event) -> str:
    raw = getattr(event, "raw", None)
    if isinstance(raw, dict):
        header = raw.get("header") or {}
        event_id = str(header.get("event_id") or raw.get("event_id") or "").strip()
        if event_id:
            return "feishu:" + event_id
    stable = {
        "message_id": getattr(event, "message_id", "") or "",
        "chat_id": getattr(event, "chat_id", "") or "",
        "operator_open_id": getattr(getattr(event, "operator", None), "open_id", "") or "",
        "action_value": _action_value(event),
        "form_value": getattr(getattr(event, "action", None), "form_value", None) or {},
    }
    raw_key = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "derived:" + hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def build_event_hub_payload(event) -> dict:
    value = _action_value(event)
    value["_delivery_identity"] = "kol_assistant"
    value["_kol_idempotency_key"] = callback_idempotency_key(event)
    form_value = getattr(getattr(event, "action", None), "form_value", None)
    if not isinstance(form_value, dict):
        form_value = {}
    return {
        "header": {
            "event_type": "card.action.trigger",
            "event_id": value["_kol_idempotency_key"],
        },
        "event": {
            "action": {"value": value, "form_value": copy.deepcopy(form_value)},
            "context": {
                "open_message_id": getattr(event, "message_id", "") or "",
                "open_chat_id": getattr(event, "chat_id", "") or "",
            },
            "operator": {
                "open_id": getattr(getattr(event, "operator", None), "open_id", "") or "",
            },
        },
    }


async def _post_event(payload: dict) -> None:
    """Deliver with bounded retries before the event is durably spooled."""
    if not config.KOL_EVENT_HUB_URL:
        raise RuntimeError("KOL_EVENT_HUB_URL is not configured")
    last_error = None
    for attempt, delay in enumerate((0, 1, 3, 8)):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(config.KOL_EVENT_HUB_URL, json=payload)
                response.raise_for_status()
            return
        except Exception as exc:
            last_error = exc
            if attempt == 3:
                raise
    raise last_error


def _spool_path() -> Path:
    return Path(config.KOL_CALLBACK_SPOOL_PATH)


def _append_spool(payload: dict) -> None:
    path = _spool_path()
    with _SPOOL_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {"queue_id": uuid.uuid4().hex, "queued_at": int(time.time()), "payload": payload}
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        STATE["pending_count"] = STATE.get("pending_count", 0) + 1


def _read_spool() -> list[dict]:
    path = _spool_path()
    with _SPOOL_LOCK:
        if not path.exists():
            return []
        rows = []
        bad_lines = []
        for index, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
            try:
                row = json.loads(line)
                if isinstance(row.get("payload"), dict):
                    row.setdefault("queue_id", f"legacy-{row.get('queued_at', 0)}-{index}")
                    rows.append(row)
            except (json.JSONDecodeError, AttributeError):
                bad_lines.append(line)
        if bad_lines:
            deadletter = path.with_suffix(path.suffix + ".deadletter")
            with deadletter.open("a", encoding="utf-8") as handle:
                for line in bad_lines:
                    handle.write(json.dumps({"detected_at": int(time.time()), "raw": line}) + "\n")
            STATE["error"] = f"deadletter:{len(bad_lines)}"
        return rows


def _remove_spool(queue_ids: set[str]) -> int:
    path = _spool_path()
    with _SPOOL_LOCK:
        if not path.exists():
            STATE["pending_count"] = 0
            return 0
        current = []
        for index, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
            try:
                row = json.loads(line)
                row.setdefault("queue_id", f"legacy-{row.get('queued_at', 0)}-{index}")
                if row.get("queue_id") not in queue_ids and isinstance(row.get("payload"), dict):
                    current.append(row)
            except (json.JSONDecodeError, AttributeError):
                # _read_spool already copied malformed rows to the persistent
                # dead-letter file; do not keep retrying an unreadable row.
                continue
        if not current:
            path.unlink(missing_ok=True)
            STATE["pending_count"] = 0
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        text = "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            for row in current
        )
        temp.write_text(text, encoding="utf-8")
        os.replace(temp, path)
        STATE["pending_count"] = len(current)
        return len(current)


async def replay_spool_once() -> dict:
    rows = await asyncio.to_thread(_read_spool)
    delivered_ids = set()
    delivered = 0
    for row in rows:
        try:
            await _post_event(row["payload"])
            delivered += 1
            delivered_ids.add(row["queue_id"])
        except Exception:
            pass
    pending = await asyncio.to_thread(_remove_spool, delivered_ids)
    if delivered and pending == 0:
        STATE.update(last_relay_ok=True, error=None)
    return {"delivered": delivered, "pending": pending}


async def _replay_spool_forever() -> None:
    while True:
        try:
            await replay_spool_once()
        except Exception as exc:
            STATE["error"] = f"replay:{type(exc).__name__}"
        await asyncio.sleep(30)


async def handle_card_action(event) -> dict:
    action = action_name(event)
    STATE["last_event_at"] = int(time.time())
    STATE["last_action"] = action or "unknown"
    if not is_allowed_action(action):
        STATE.update(last_relay_ok=False, error="action_not_allowed")
        return {"ok": False, "error": "action_not_allowed", "action": action}
    payload = build_event_hub_payload(event)
    try:
        await _post_event(payload)
    except Exception as exc:
        await asyncio.to_thread(_append_spool, payload)
        STATE.update(last_relay_ok=False, error=f"queued:{type(exc).__name__}")
        return {"ok": False, "queued": True, "action": action}
    STATE.update(last_relay_ok=True, error=None)
    return {"ok": True, "action": action}


def _run(app_id: str, app_secret: str) -> None:
    global _CHANNEL
    from lark_channel import Events, FeishuChannel, SecurityConfig

    async def main() -> None:
        global _CHANNEL
        channel = FeishuChannel(
            app_id=app_id,
            app_secret=app_secret,
            security=SecurityConfig(mode="strict"),
        )
        _CHANNEL = channel
        channel.on(Events.CARD_ACTION, lambda event: handle_card_action(event))
        channel.on(Events.RECONNECTED, lambda *_: STATE.update(connection="connected", error=None))
        channel.on(Events.ERROR, lambda err: STATE.update(connection="error", error=type(err).__name__))
        STATE.update(enabled=True, connection="connecting", started_at=int(time.time()), error=None)
        asyncio.create_task(_replay_spool_forever())
        await channel.connect()

    try:
        asyncio.run(main())
    except Exception as exc:
        STATE.update(connection="error", error=type(exc).__name__)


def start() -> None:
    global _THREAD
    if not config.KOL_CALLBACK_ENABLED:
        STATE.update(enabled=False, connection="disabled", error="disabled_by_config")
        return
    app_id = config.FEISHU_KOL_ASSISTANT_APP_ID
    app_secret = config.FEISHU_KOL_ASSISTANT_APP_SECRET
    if not app_id or not app_secret:
        STATE.update(enabled=False, connection="disabled", error="missing_credentials")
        return
    if _THREAD and _THREAD.is_alive():
        return
    _THREAD = threading.Thread(
        target=_run,
        args=(app_id, app_secret),
        daemon=True,
        name="kol-card-callback",
    )
    _THREAD.start()


def snapshot() -> dict:
    out = dict(STATE)
    if _CHANNEL is not None:
        try:
            conn = _CHANNEL.connection_snapshot()
            # lark-channel-sdk 1.4.0 marks the public snapshot ready only after
            # ``Client.start()`` returns. In websocket mode that call blocks
            # for the lifetime of the connection, so the first successful
            # connection otherwise appears as ``idle`` forever. The transport
            # connection is set only after the websocket handshake succeeds.
            ws_client = getattr(_CHANNEL, "_ws_client", None)
            transport_connected = getattr(ws_client, "_conn", None) is not None
            out.update(
                connection="connected" if transport_connected else conn.state,
                ready=bool(conn.ready or transport_connected),
                reconnect_attempts=conn.reconnect_attempts,
            )
        except Exception:
            pass
    return out
