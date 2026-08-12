from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable
BEIJING = timezone(timedelta(hours=8), name="Asia/Shanghai")
VIDEO_ID = re.compile(r"^[0-9A-Za-z_-]{11}$")


def split_terms(value: Any) -> list[str]:
    if isinstance(value, list):
        values = value
    elif isinstance(value, str):
        values = re.split(r"[,，|｜\n]+", value)
    else:
        return []
    return [str(part).strip() for part in values if str(part).strip()]


def scalar(value: Any) -> Any:
    if isinstance(value, list) and len(value) == 1:
        return scalar(value[0])
    if isinstance(value, dict):
        if "text" in value and len(value) <= 3:
            return value.get("text")
        if "value" in value and len(value) <= 3:
            return value.get("value")
    return value


def parse_datetime(value: Any, *, fallback: datetime | None = None) -> datetime:
    value = scalar(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(value or "").strip()
    if text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=BEIJING) if parsed.tzinfo is None else parsed
        except ValueError:
            pass
    if fallback is not None:
        return fallback
    raise ValueError("datetime value is missing or invalid")


def rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def display_datetime(value: datetime) -> str:
    return value.astimezone(BEIJING).strftime("%Y-%m-%d %H:%M:%S")


def incremental_window(config: dict[str, Any], now: datetime) -> tuple[datetime, datetime]:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    last_success = parse_datetime(config.get("最近成功采集时间"), fallback=now - timedelta(days=7))
    start = last_success.astimezone(timezone.utc) - timedelta(hours=48)
    end = now.astimezone(timezone.utc)
    if start >= end:
        start = end - timedelta(hours=48)
    return start, end


def _select_text(value: Any) -> str:
    value = scalar(value)
    return str(value or "").strip()


@dataclass(frozen=True)
class ScheduleDecision:
    should_run: bool
    reason: str
    active_events: tuple[str, ...] = ()


def active_launch_events(
    rows: Iterable[dict[str, Any]], now: datetime, *, brand: str
) -> tuple[str, ...]:
    today = now.astimezone(BEIJING).date()
    active: list[str] = []
    for row in rows:
        if _select_text(row.get("竞品品牌")).casefold() != brand.casefold():
            continue
        if _select_text(row.get("来源类型")) != "官方确认":
            continue
        if _select_text(row.get("人工确认状态")) != "已确认":
            continue
        raw_date = row.get("正式开售日期")
        try:
            launch_date = parse_datetime(raw_date).astimezone(BEIJING).date()
        except ValueError:
            continue
        if abs((today - launch_date).days) <= 30:
            name = _select_text(row.get("事件名称")) or launch_date.isoformat()
            if name not in active:
                active.append(name)
    return tuple(active)


def schedule_decision(
    now: datetime,
    event_rows: Iterable[dict[str, Any]],
    *,
    brand: str,
    force: bool = False,
) -> ScheduleDecision:
    if force:
        return ScheduleDecision(True, "manual_force")
    weekday = now.astimezone(BEIJING).weekday()
    if weekday == 0:
        return ScheduleDecision(True, "weekly_monday")
    if weekday not in {2, 4}:
        return ScheduleDecision(False, "not_scheduled_day")
    events = active_launch_events(event_rows, now, brand=brand)
    if events:
        return ScheduleDecision(True, "launch_window", events)
    return ScheduleDecision(False, "no_confirmed_launch_window")


def query_groups(config: dict[str, Any], keyword: str) -> dict[str, tuple[str, ...]]:
    brand = str(scalar(config.get("竞品品牌")) or keyword).strip()
    aliases = [
        term
        for term in split_terms(config.get("关键词别名"))
        if brand.casefold() in term.casefold()
    ]
    negative_words: list[str] = []
    for phrase in split_terms(config.get("排除词")):
        for word in re.findall(r"[0-9A-Za-z]+", phrase):
            token = f"-{word}"
            if (
                word.casefold() != brand.casefold()
                and len(word) >= 4
                and token.casefold() not in {item.casefold() for item in negative_words}
            ):
                negative_words.append(token)
    negative_suffix = " ".join(negative_words)
    brand_terms = [
        f"{term} {negative_suffix}".strip()
        for term in [keyword, brand, f"{brand} Gaming", *aliases]
    ]
    series = [f"{brand} {term}" for term in split_terms(config.get("产品系列词"))]
    models = [f"{brand} {term}" for term in split_terms(config.get("产品型号词"))]

    def unique(values: Iterable[str]) -> tuple[str, ...]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            cleaned = " ".join(str(value).split())
            key = cleaned.casefold()
            if cleaned and key not in seen:
                seen.add(key)
                result.append(cleaned)
        return tuple(result)

    return {
        "brand": unique(brand_terms),
        "series": unique(series),
        "model": unique(models),
    }


def stable_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def unique_lines(*values: Any) -> str:
    result: list[str] = []
    for value in values:
        for item in split_terms(value):
            if item not in result:
                result.append(item)
    return "\n".join(result)


def is_youtube_video_id(value: Any) -> bool:
    return bool(VIDEO_ID.fullmatch(str(value or "").strip()))


def date_only(value: Any) -> date | None:
    try:
        return parse_datetime(value).astimezone(BEIJING).date()
    except ValueError:
        return None
