"""KOL媒体助手专属飞书发送器。

R7 仅承接任务完成情况周报的 Frankie-only 灰度。这里故意不提供旧 App
回退：新身份缺配置或发送失败时必须显式失败，避免表面迁移、实际仍由旧 App 发送。
"""
from __future__ import annotations

import json
import os
import time

import httpx


APP_ID = (os.environ.get("FEISHU_KOL_ASSISTANT_APP_ID") or "").strip()
APP_SECRET = (os.environ.get("FEISHU_KOL_ASSISTANT_APP_SECRET") or "").strip()
FRANKIE_UNION_ID = (os.environ.get("KOL_ASSISTANT_FRANKIE_UNION_ID") or "").strip()

_token_cache = {"value": "", "expires_at": 0.0}


def is_configured() -> bool:
    return bool(APP_ID and APP_SECRET and FRANKIE_UNION_ID)


async def _token() -> str:
    if not is_configured():
        raise RuntimeError(
            "KOL媒体助手未配置：需要 FEISHU_KOL_ASSISTANT_APP_ID/SECRET "
            "和 Frankie union_id"
        )
    if _token_cache["value"] and _token_cache["expires_at"] > time.time():
        return str(_token_cache["value"])
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": APP_ID, "app_secret": APP_SECRET},
        )
        response.raise_for_status()
        payload = response.json()
    token = str(payload.get("tenant_access_token") or "")
    if not token:
        raise RuntimeError(
            f"KOL媒体助手获取 token 失败：code={payload.get('code')} "
            f"msg={str(payload.get('msg') or '')[:120]}"
        )
    _token_cache["value"] = token
    _token_cache["expires_at"] = time.time() + max(60, int(payload.get("expire", 3600)) - 300)
    return token


async def send_card_to_frankie(card: dict, *, message_uuid: str) -> str:
    """只用 KOL媒体助手向 Frankie 的 union_id 发送一张卡。"""
    token = await _token()
    body = {
        "receive_id": FRANKIE_UNION_ID,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
        "uuid": message_uuid[:50],
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=union_id",
            headers={"Authorization": f"Bearer {token}"},
            json=body,
        )
        response.raise_for_status()
        payload = response.json()
    if payload.get("code") != 0:
        raise RuntimeError(
            f"KOL媒体助手发送失败：code={payload.get('code')} "
            f"msg={str(payload.get('msg') or '')[:120]}"
        )
    message_id = str((payload.get("data") or {}).get("message_id") or "")
    if not message_id:
        raise RuntimeError("KOL媒体助手发送响应缺少 message_id")
    return message_id
