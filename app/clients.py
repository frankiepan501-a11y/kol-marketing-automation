from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .constants import DATETIME_FIELDS, RELATION_FIELDS, URL_FIELDS
from .core import chunks, parse_datetime


class ApiError(RuntimeError):
    def __init__(self, service: str, code: str, message: str):
        self.service = service
        self.code = str(code or "unknown")
        super().__init__(f"{service}:{self.code}:{str(message)[:300]}")


def _json_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    data = None
    request_headers = {"Accept": "application/json", **(headers or {})}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json; charset=utf-8"
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        payload = error.read(65536).decode("utf-8", errors="replace")
        try:
            parsed = json.loads(payload)
            details = parsed.get("error", {}).get("errors", [])
            reason = details[0].get("reason") if details and isinstance(details[0], dict) else None
            code = reason or parsed.get("code") or parsed.get("error", {}).get("code") or error.code
            message = parsed.get("msg") or parsed.get("error", {}).get("message") or "http error"
        except (json.JSONDecodeError, AttributeError):
            code, message = error.code, "http error"
        raise ApiError("http", str(code), str(message)) from None
    except (urllib.error.URLError, TimeoutError) as error:
        raise ApiError("network", type(error).__name__, "request failed") from None
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        raise ApiError("http", "invalid_json", "response was not JSON") from None
    if not isinstance(value, dict):
        raise ApiError("http", "invalid_shape", "response was not an object")
    return value


class YouTubeClient:
    BASE = "https://youtube.googleapis.com/youtube/v3"

    def __init__(self, api_key: str | None = None):
        self.api_key = str(api_key or os.environ.get("YOUTUBE_API_KEY") or "").strip()
        if not self.api_key:
            raise RuntimeError("YOUTUBE_API_KEY is required")

    def _get(self, resource: str, params: dict[str, Any]) -> dict[str, Any]:
        query = {**params, "key": self.api_key}
        url = f"{self.BASE}/{resource}?{urllib.parse.urlencode(query)}"
        try:
            result = _json_request("GET", url)
        except ApiError as error:
            raise ApiError("youtube", error.code, "YouTube API request failed") from None
        if "error" in result:
            error = result.get("error") if isinstance(result.get("error"), dict) else {}
            reasons = error.get("errors") if isinstance(error.get("errors"), list) else []
            reason = reasons[0].get("reason") if reasons and isinstance(reasons[0], dict) else error.get("code")
            raise ApiError("youtube", str(reason or "unknown"), "YouTube API request failed")
        return result

    def search(
        self,
        query: str,
        *,
        published_after: str,
        published_before: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "part": "snippet",
            "type": "video",
            "maxResults": 50,
            "order": "date",
            "q": query,
            "publishedAfter": published_after,
            "publishedBefore": published_before,
        }
        if page_token:
            params["pageToken"] = page_token
        return self._get("search", params)

    def videos(self, video_ids: list[str]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for batch in chunks(video_ids, 50):
            response = self._get(
                "videos",
                {
                    "part": "snippet,contentDetails,statistics,status",
                    "id": ",".join(batch),
                    "maxResults": len(batch),
                },
            )
            items.extend(item for item in response.get("items", []) if isinstance(item, dict))
        return items

    def channels(self, channel_ids: list[str]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for batch in chunks(channel_ids, 50):
            response = self._get(
                "channels",
                {"part": "snippet,statistics", "id": ",".join(batch), "maxResults": len(batch)},
            )
            items.extend(item for item in response.get("items", []) if isinstance(item, dict))
        return items


class FeishuClient:
    BASE = "https://open.feishu.cn/open-apis"

    def __init__(self, app_id: str | None = None, app_secret: str | None = None):
        self.app_id = str(app_id or os.environ.get("FEISHU_APP_ID") or "").strip()
        self.app_secret = str(app_secret or os.environ.get("FEISHU_APP_SECRET") or "").strip()
        if not self.app_id or not self.app_secret:
            raise RuntimeError("FEISHU_APP_ID and FEISHU_APP_SECRET are required")
        self._token = ""

    def _refresh(self) -> None:
        result = _json_request(
            "POST",
            f"{self.BASE}/auth/v3/tenant_access_token/internal",
            body={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        token = str(result.get("tenant_access_token") or "")
        if not token:
            raise ApiError("feishu", str(result.get("code") or "auth_failed"), str(result.get("msg") or "token missing"))
        self._token = token

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        retry_auth: bool = True,
    ) -> dict[str, Any]:
        if not self._token:
            self._refresh()
        result = _json_request(
            method,
            f"{self.BASE}{path}",
            headers={"Authorization": f"Bearer {self._token}"},
            body=body,
        )
        code = result.get("code", 0)
        if code == 99991663 and retry_auth:
            self._refresh()
            return self.request(method, path, body=body, retry_auth=False)
        if code not in (0, None):
            raise ApiError("feishu", str(code), str(result.get("msg") or "request failed"))
        return result

    def get_record(self, base_token: str, table_id: str, record_id: str) -> dict[str, Any]:
        path = f"/bitable/v1/apps/{base_token}/tables/{table_id}/records/{record_id}"
        result = self.request("GET", path)
        record = result.get("data", {}).get("record", {})
        return {"_record_id": record.get("record_id", record_id), **dict(record.get("fields") or {})}

    def list_records(
        self,
        base_token: str,
        table_id: str,
        *,
        field_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page_token = ""
        while True:
            query: dict[str, Any] = {"page_size": 500}
            if page_token:
                query["page_token"] = page_token
            if field_names:
                query["field_names"] = json.dumps(field_names, ensure_ascii=False, separators=(",", ":"))
            path = (
                f"/bitable/v1/apps/{base_token}/tables/{table_id}/records?"
                + urllib.parse.urlencode(query)
            )
            data = self.request("GET", path).get("data", {})
            for record in data.get("items", []) or []:
                if isinstance(record, dict):
                    rows.append({"_record_id": record.get("record_id"), **dict(record.get("fields") or {})})
            if not data.get("has_more"):
                break
            next_token = str(data.get("page_token") or "")
            if not next_token or next_token == page_token:
                raise ApiError("feishu", "pagination_stalled", "record list cursor did not advance")
            page_token = next_token
        return rows

    @staticmethod
    def api_fields(fields: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, value in fields.items():
            if value is None:
                continue
            if name in DATETIME_FIELDS:
                result[name] = int(parse_datetime(value).timestamp() * 1000)
            elif name in RELATION_FIELDS:
                values = value if isinstance(value, list) else []
                result[name] = [
                    str(item.get("id") or item.get("record_id") or "") if isinstance(item, dict) else str(item)
                    for item in values
                    if (isinstance(item, str) and item) or (isinstance(item, dict) and (item.get("id") or item.get("record_id")))
                ]
            elif name in URL_FIELDS:
                if isinstance(value, dict):
                    link = str(value.get("link") or "")
                    text = str(value.get("text") or link)
                else:
                    link, text = str(value), str(value)
                if link:
                    result[name] = {"link": link, "text": text}
            else:
                result[name] = value
        return result

    def batch_create(self, base_token: str, table_id: str, rows: list[dict[str, Any]]) -> list[str]:
        ids: list[str] = []
        for batch in chunks(rows, 200):
            path = f"/bitable/v1/apps/{base_token}/tables/{table_id}/records/batch_create"
            body = {"records": [{"fields": self.api_fields(fields)} for fields in batch]}
            returned = self.request("POST", path, body=body).get("data", {}).get("records", [])
            if len(returned) != len(batch):
                raise ApiError("feishu", "batch_create_short", "not all records were created")
            ids.extend(str(item.get("record_id") or "") for item in returned)
        return ids

    def batch_update(
        self,
        base_token: str,
        table_id: str,
        updates: list[tuple[str, dict[str, Any]]],
    ) -> None:
        for batch in chunks(updates, 200):
            path = f"/bitable/v1/apps/{base_token}/tables/{table_id}/records/batch_update"
            body = {
                "records": [
                    {"record_id": record_id, "fields": self.api_fields(fields)}
                    for record_id, fields in batch
                ]
            }
            returned = self.request("POST", path, body=body).get("data", {}).get("records", [])
            if len(returned) != len(batch):
                raise ApiError("feishu", "batch_update_short", "not all records were updated")
