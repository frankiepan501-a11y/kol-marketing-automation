"""Conservative per-run YouTube search budget for the daily collector."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DAILY_SEARCH_LIMIT = 100
SAFETY_RESERVE = 20
SEGMENT_MAX_CALLS = 10
YOUTUBE_DAILY_QUOTA_CODES = frozenset(
    {"quotaExceeded", "dailyLimitExceeded", "dailyLimitExceededUnreg"}
)


@dataclass
class DailySearchBudget:
    """Count only this daily job's search.list calls; never claim project-wide usage."""

    limit: int = DAILY_SEARCH_LIMIT
    reserve: int = SAFETY_RESERVE
    used: int = 0

    def __post_init__(self) -> None:
        if self.limit <= 0 or self.reserve < 0 or self.reserve >= self.limit:
            raise ValueError("invalid daily search budget")
        if self.used < 0:
            raise ValueError("used search calls cannot be negative")

    @classmethod
    def from_nyxi(cls, nyxi: dict[str, Any]) -> DailySearchBudget:
        value = nyxi.get("search_calls")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return cls(used=DAILY_SEARCH_LIMIT)
        return cls(used=value)

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def can_start_segment(self, max_calls: int = SEGMENT_MAX_CALLS) -> bool:
        return self.remaining >= self.reserve + max_calls

    def consume(self, calls: int) -> None:
        if isinstance(calls, bool) or not isinstance(calls, int) or calls < 0:
            raise ValueError("search calls must be a non-negative integer")
        self.used += calls

    def summary(self) -> dict[str, int]:
        return {
            "budget_limit": self.limit,
            "budget_reserve": self.reserve,
            "budget_used": self.used,
            "budget_remaining": self.remaining,
        }
