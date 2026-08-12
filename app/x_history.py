"""Read-only X full-archive capability probe for competitor history capture.

The probe intentionally returns only capability metadata. It never returns post
content, pagination tokens, or credential material.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException

from . import config


X_API = "https://api.x.com/2"
PROBE_QUERY = "from:NyxiGaming -is:retweet"
PROBE_START = "2024-01-01T00:00:00Z"
PROBE_END = "2024-01-02T00:00:00Z"

router = APIRouter(prefix="/x-history", tags=["x-history"])


@dataclass
class XApiError(RuntimeError):
    status_code: int
    category: str
    message: str = ""

    def __str__(self) -> str:
        return f"X API error {self.status_code}: {self.category}"


def _check_auth(authorization: str) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if authorization[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")


def _x_headers() -> dict[str, str]:
    token = os.environ.get("X_BEARER_TOKEN") or os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        raise XApiError(0, "missing_x_bearer_token")
    return {"Authorization": f"Bearer {token}"}


def _error_category(status_code: int) -> str:
    return {
        401: "credential_invalid",
        402: "credits_required",
        403: "full_archive_not_authorized",
        429: "rate_limited",
    }.get(status_code, "x_api_error")


async def _x_search_all(params: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=45.0) as client:
        response = await client.get(
            f"{X_API}/tweets/search/all",
            params=params,
            headers=_x_headers(),
        )
    if response.status_code >= 400:
        raise XApiError(
            status_code=response.status_code,
            category=_error_category(response.status_code),
        )
    return response.json()


@router.get("/probe")
async def probe_full_archive(authorization: str = Header(default="")) -> dict[str, Any]:
    """Probe X full-archive access without writing data or exposing results."""
    _check_auth(authorization)
    try:
        payload = await _x_search_all({
            "query": PROBE_QUERY,
            "start_time": PROBE_START,
            "end_time": PROBE_END,
            "max_results": 10,
            "tweet.fields": "id",
        })
    except XApiError as exc:
        return {
            "ok": False,
            "full_archive_supported": False,
            "reason": exc.category,
            "http_status": exc.status_code,
            "writes_performed": 0,
        }

    meta = payload.get("meta") or {}
    return {
        "ok": True,
        "full_archive_supported": True,
        "result_count": int(meta.get("result_count") or 0),
        "writes_performed": 0,
    }
