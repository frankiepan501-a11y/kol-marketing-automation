"""Read the same project's YouTube search quota; never estimate from API calls."""

from __future__ import annotations

import json
import os
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .clients import ApiError, _json_request
from .core import rfc3339

PROJECT_ID = "powkong-funlab-ads-api"
QUOTA_METRIC = "youtube.googleapis.com/search_list"
LIMIT_NAME = "defaultSearchListPerDayPerProject"
SERVICE = "youtube.googleapis.com"
MONITORING_TYPE = "serviceruntime.googleapis.com/quota/ratev2/net_usage"
PACIFIC = ZoneInfo("America/Los_Angeles")
SAFETY_RESERVE = 20
SEGMENT_MAX_CALLS = 10
MAX_SAMPLE_AGE = timedelta(minutes=5)
OAUTH_SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform.read-only",
    "https://www.googleapis.com/auth/monitoring.read",
)


class QuotaUnavailable(RuntimeError):
    """No trustworthy current reading is available; backfill must pause."""


@dataclass(frozen=True)
class QuotaSnapshot:
    project_id: str
    project_number: str
    quota_day: str
    limit: int
    used: int
    sampled_at: datetime

    @property
    def remaining(self) -> int:
        return self.limit - self.used


class GoogleQuotaReader:
    """Service-account-only read path; all fetched values are checked against fixed identities."""

    def __init__(
        self,
        *,
        project_number: str,
        service_account_json: str,
        youtube_api_key: str,
        get_json: Callable[[str], dict[str, Any]] | None = None,
    ):
        if not project_number.isdecimal() or not youtube_api_key:
            raise QuotaUnavailable("project number or YouTube key is missing")
        try:
            info = json.loads(service_account_json)
        except (TypeError, ValueError):
            raise QuotaUnavailable("service account configuration is invalid") from None
        if not isinstance(info, dict) or info.get("project_id") != PROJECT_ID:
            raise QuotaUnavailable("service account belongs to a different project")
        self.project_number = project_number
        self.youtube_api_key = youtube_api_key
        self._get_json = get_json or self._authenticated_get(info)

    @staticmethod
    def _authenticated_get(info: dict[str, Any]) -> Callable[[str], dict[str, Any]]:
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=list(OAUTH_SCOPES)
        )

        def get_json(url: str) -> dict[str, Any]:
            if not credentials.valid:
                credentials.refresh(Request())
            return _json_request("GET", url, headers={"Authorization": f"Bearer {credentials.token}"})

        return get_json

    @classmethod
    def from_environment(cls, youtube_api_key: str) -> GoogleQuotaReader | None:
        # Credentials alone must not enable spending quota before the console smoke check.
        if os.environ.get("GOOGLE_QUOTA_VERIFIED") != "1":
            return None
        raw = os.environ.get("GOOGLE_QUOTA_SERVICE_ACCOUNT_JSON", "")
        number = os.environ.get("GOOGLE_QUOTA_PROJECT_NUMBER", "").strip()
        if not raw or not number:
            return None
        return cls(project_number=number, service_account_json=raw, youtube_api_key=youtube_api_key)

    def _verify_key_project(self) -> None:
        url = "https://apikeys.googleapis.com/v2/keys:lookupKey?" + urllib.parse.urlencode(
            {"keyString": self.youtube_api_key}
        )
        result = self._get_json(url)
        if result.get("parent") != f"projects/{self.project_number}/locations/global":
            raise QuotaUnavailable("YouTube key project does not match quota project")

    def _limit(self) -> int:
        base = (
            f"https://serviceusage.googleapis.com/v1beta1/projects/{PROJECT_ID}"
            f"/services/{SERVICE}/consumerQuotaMetrics"
        )
        page = ""
        matches: list[int] = []
        while True:
            result = self._get_json(base + "?" + urllib.parse.urlencode({"view": "FULL", "pageToken": page}))
            for metric in result.get("metrics", []):
                if metric.get("metric") != QUOTA_METRIC:
                    continue
                for limit in metric.get("consumerQuotaLimits", []):
                    if limit.get("metric") not in (None, QUOTA_METRIC):
                        raise QuotaUnavailable("search quota limit metric does not match")
                    if not str(limit.get("unit", "")).startswith("1/d/"):
                        continue
                    buckets = limit.get("quotaBuckets", [])
                    global_buckets = [b for b in buckets if not b.get("dimensions")]
                    if len(global_buckets) != 1:
                        raise QuotaUnavailable("search quota has no unique global bucket")
                    matches.append(int(global_buckets[0]["effectiveLimit"]))
            page = str(result.get("nextPageToken") or "")
            if not page:
                break
        if len(matches) != 1 or matches[0] <= 0:
            raise QuotaUnavailable("search quota limit was not uniquely identified")
        return matches[0]

    def _usage(self, now: datetime) -> tuple[int, datetime]:
        day_start = now.astimezone(PACIFIC).replace(hour=0, minute=0, second=0, microsecond=0)
        filter_text = (
            f'metric.type="{MONITORING_TYPE}" AND resource.type="consumer_quota" '
            f'AND resource.label.service="{SERVICE}" '
            f'AND metric.label.quota_metric="{QUOTA_METRIC}"'
        )
        params = {
            "filter": filter_text,
            "interval.startTime": rfc3339(day_start),
            "interval.endTime": rfc3339(now),
            "view": "FULL",
        }
        url = (
            f"https://monitoring.googleapis.com/v3/projects/{PROJECT_ID}/timeSeries?"
            + urllib.parse.urlencode(params)
        )
        result = self._get_json(url)
        series = result.get("timeSeries", [])
        if result.get("nextPageToken") or len(series) != 1:
            raise QuotaUnavailable("search usage is missing or ambiguous")
        item = series[0]
        if item.get("metric", {}).get("type") != MONITORING_TYPE:
            raise QuotaUnavailable("usage metric type does not match daily rate quota")
        if item.get("resource", {}).get("labels", {}).get("service") != SERVICE:
            raise QuotaUnavailable("usage service does not match YouTube")
        labels = item.get("metric", {}).get("labels", {})
        if labels.get("quota_metric") != QUOTA_METRIC or labels.get("limit_name") != LIMIT_NAME:
            raise QuotaUnavailable("usage metric does not match search.list")
        if labels.get("window_size") != "86400s":
            raise QuotaUnavailable("search usage is not a daily rate window")
        try:
            window_start = datetime.fromisoformat(labels["window_start_time"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            raise QuotaUnavailable("search usage window start is missing") from None
        if window_start != day_start.astimezone(timezone.utc):
            raise QuotaUnavailable("search usage is from a different quota window")
        points = item.get("points", [])
        if not points:
            raise QuotaUnavailable("search usage has no data points")
        latest = max(points, key=lambda p: p.get("interval", {}).get("endTime", ""))
        sampled_at = datetime.fromisoformat(latest["interval"]["endTime"].replace("Z", "+00:00"))
        used = int(latest["value"]["int64Value"])
        if sampled_at.astimezone(PACIFIC).date() != now.astimezone(PACIFIC).date() or used < 0:
            raise QuotaUnavailable("search usage is from another quota day")
        return used, sampled_at

    def snapshot(self, *, after: datetime, now: datetime | None = None) -> QuotaSnapshot:
        now = now or datetime.now(timezone.utc)
        snapshot = self.audit_snapshot(now=now)
        if snapshot.sampled_at <= after or now - snapshot.sampled_at > MAX_SAMPLE_AGE:
            raise QuotaUnavailable("search usage sample has not caught up")
        return snapshot

    def audit_snapshot(self, *, now: datetime | None = None) -> QuotaSnapshot:
        """Return the latest validated sample for console reconciliation only."""
        now = now or datetime.now(timezone.utc)
        try:
            self._verify_key_project()
            limit = self._limit()
            used, sampled_at = self._usage(now)
        except ApiError as error:
            raise QuotaUnavailable(f"Google quota read failed ({error.code})") from None
        if sampled_at > now:
            raise QuotaUnavailable("search usage sample is from the future")
        if used > limit:
            raise QuotaUnavailable("search usage exceeds configured limit")
        return QuotaSnapshot(
            project_id=PROJECT_ID,
            project_number=self.project_number,
            quota_day=now.astimezone(PACIFIC).date().isoformat(),
            limit=limit,
            used=used,
            sampled_at=sampled_at,
        )

    def wait_for_snapshot(
        self, *, after: datetime, max_wait_seconds: int = 300, poll_seconds: int = 30
    ) -> QuotaSnapshot:
        deadline = time.monotonic() + max_wait_seconds
        while True:
            try:
                return self.snapshot(after=after)
            except QuotaUnavailable as error:
                if "sample has not caught up" not in str(error) or time.monotonic() + poll_seconds > deadline:
                    raise
                time.sleep(poll_seconds)
