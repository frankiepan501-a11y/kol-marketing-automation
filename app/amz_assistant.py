"""Feishu Amazon assistant app helpers.

This app owns Amazon-operation cards and their card.action.trigger callbacks.
Cards must be sent and patched by the same Feishu app, so AMZ cards should use
this module instead of the customer-service assistant once configured.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx

from . import cs_dispatch


APP_ID = os.environ.get("FEISHU_AMZ_ASSISTANT_APP_ID", "")
APP_SECRET = os.environ.get("FEISHU_AMZ_ASSISTANT_APP_SECRET", "")
VERIFICATION_TOKEN = os.environ.get("FEISHU_AMZ_ASSISTANT_VERIFICATION_TOKEN", "")
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", cs_dispatch.OBSERVE_UNION)

_tok = {"v": "", "exp": 0.0}


def is_configured() -> bool:
    return bool(APP_ID and APP_SECRET)


async def _token() -> str:
    if not is_configured():
        raise RuntimeError("FEISHU_AMZ_ASSISTANT_APP_ID/SECRET not configured")
    if _tok["v"] and _tok["exp"] > time.time():
        return _tok["v"]
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": APP_ID, "app_secret": APP_SECRET},
        )
        resp.raise_for_status()
        data = resp.json()
    _tok["v"] = data.get("tenant_access_token", "")
    _tok["exp"] = time.time() + (int(data.get("expire", 3600)) - 300)
    return _tok["v"]


async def send_card_to_union(union_id: str, card: dict) -> str:
    if not is_configured():
        return await cs_dispatch._send_card(union_id, card)
    token = await _token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=union_id",
            headers={"Authorization": f"Bearer {token}"},
            json={"receive_id": union_id, "msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)},
        )
        data = resp.json()
    return data.get("data", {}).get("message_id", "") if data.get("code") == 0 else ""


async def send_card_to_chat(chat_id: str, card: dict) -> str:
    if not is_configured():
        token = await cs_dispatch._token()
    else:
        token = await _token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            headers={"Authorization": f"Bearer {token}"},
            json={"receive_id": chat_id, "msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)},
        )
        data = resp.json()
    return data.get("data", {}).get("message_id", "") if data.get("code") == 0 else ""


async def update_card(message_id: str, card: dict) -> bool:
    if not is_configured():
        return await cs_dispatch._update_card(message_id, card)
    if not message_id:
        return False
    try:
        token = await _token()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.patch(
                f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}",
                headers={"Authorization": f"Bearer {token}"},
                json={"content": json.dumps(card, ensure_ascii=False)},
            )
            data = resp.json()
        return data.get("code") == 0
    except Exception as exc:
        print(f"[amz_assistant.update_card] {message_id} fail: {exc}")
        return False


async def notify_frankie(text: str) -> str:
    if not is_configured():
        await cs_dispatch._notify_frankie(text)
        return ""
    token = await _token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=union_id",
            headers={"Authorization": f"Bearer {token}"},
            json={"receive_id": FRANKIE_UNION_ID, "msg_type": "text", "content": json.dumps({"text": text}, ensure_ascii=False)},
        )
        data = resp.json()
    return data.get("data", {}).get("message_id", "") if data.get("code") == 0 else ""


def _callback_token(payload: dict) -> str:
    header = payload.get("header") or {}
    event = payload.get("event") or {}
    return str(payload.get("token") or header.get("token") or event.get("token") or "")


def _token_ok(payload: dict) -> bool:
    return not VERIFICATION_TOKEN or _callback_token(payload) == VERIFICATION_TOKEN


def _challenge(payload: dict) -> str:
    event = payload.get("event") or {}
    return str(payload.get("challenge") or event.get("challenge") or "")


def _event_type(payload: dict) -> str:
    header = payload.get("header") or {}
    event = payload.get("event") or {}
    return str(header.get("event_type") or payload.get("type") or event.get("type") or "")


def _card_event(payload: dict) -> dict:
    event = payload.get("event")
    if isinstance(event, dict):
        out = dict(event)
        out["_header"] = payload.get("header") or {}
        return out
    return payload


async def handle_feishu_callback(payload: dict[str, Any]) -> dict:
    if payload.get("encrypt"):
        return {"code": 400, "msg": "encrypted callbacks are not enabled for this endpoint yet"}
    if _event_type(payload) == "url_verification" or _challenge(payload):
        if not _token_ok(payload):
            return {"code": 403, "msg": "invalid verification token"}
        return {"challenge": _challenge(payload)}
    if not _token_ok(payload):
        return {"toast": {"type": "error", "content": "无效的飞书回调 token"}}
    event_type = _event_type(payload)
    if event_type and event_type != "card.action.trigger":
        return {"code": 0, "msg": "ignored"}
    from . import amz_review_audit

    return await amz_review_audit.handle_callback(_card_event(payload))
