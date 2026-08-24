"""KOL enrich DeepSeek budgets and consecutive-failure circuit breaker."""
from __future__ import annotations

import json
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


_STATE_LOCKS: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)


class EnrichModelBudget:
    """Reserve calls before the provider request and persist the daily count."""

    def __init__(self, *, per_task: int, per_run: int, daily: int,
                 failure_threshold: int, state_path: Path | str):
        self.per_task = max(0, int(per_task))
        self.per_run = max(0, int(per_run))
        self.daily = max(0, int(daily))
        self.failure_threshold = max(1, int(failure_threshold))
        self.state_path = Path(state_path)
        self._state_lock = _STATE_LOCKS[str(self.state_path.resolve())]
        self.run_calls = 0
        self.task_calls = defaultdict(int)
        self.consecutive_failures = 0
        self.circuit_open = False
        self._day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
        self.daily_calls = self._read_daily_calls()

    def _read_daily_calls(self, *, strict: bool = False) -> int:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("budget state must be a JSON object")
            if data.get("date") == self._day:
                return max(0, int(data.get("calls") or 0))
        except FileNotFoundError:
            return 0
        except (ValueError, TypeError, OSError):
            if strict:
                raise
        return 0

    def _save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(
            json.dumps({"date": self._day, "calls": self.daily_calls}, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(self.state_path)

    def reserve(self, task_id: str) -> tuple[bool, str]:
        if self.circuit_open:
            return False, "circuit_open"
        with self._state_lock:
            try:
                self.daily_calls = self._read_daily_calls(strict=True)
            except (ValueError, TypeError, OSError):
                return False, "budget_state_unavailable"
            if self.per_task == 0 or self.task_calls[task_id] >= self.per_task:
                return False, "task_budget_exhausted"
            if self.per_run == 0 or self.run_calls >= self.per_run:
                return False, "run_budget_exhausted"
            if self.daily == 0 or self.daily_calls >= self.daily:
                return False, "daily_budget_exhausted"
            self.daily_calls += 1
            try:
                self._save()
            except OSError:
                self.daily_calls -= 1
                return False, "budget_state_unavailable"
            self.task_calls[task_id] += 1
            self.run_calls += 1
            return True, "ok"

    def record_success(self) -> None:
        self.consecutive_failures = 0

    def record_failure(self, *, terminal: bool = False) -> None:
        self.consecutive_failures += 1
        if terminal or self.consecutive_failures >= self.failure_threshold:
            self.circuit_open = True

    def snapshot(self) -> dict:
        with self._state_lock:
            try:
                self.daily_calls = self._read_daily_calls(strict=True)
            except (ValueError, TypeError, OSError):
                pass
        return {
            "run_calls": self.run_calls,
            "daily_calls": self.daily_calls,
            "per_task_limit": self.per_task,
            "per_run_limit": self.per_run,
            "daily_limit": self.daily,
            "consecutive_failures": self.consecutive_failures,
            "failure_threshold": self.failure_threshold,
            "circuit_open": self.circuit_open,
        }
