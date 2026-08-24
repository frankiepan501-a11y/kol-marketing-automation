"""KOL enrich DeepSeek budgets and consecutive-failure circuit breaker."""
from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


_STATE_LOCKS: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)
_BEIJING_TZ = timezone(timedelta(hours=8))
_SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}


class EnrichModelBudget:
    """Reserve calls before the provider request and persist the daily count."""

    def __init__(self, *, per_task: int, per_run: int, daily: int,
                 failure_threshold: int, state_path: Path | str,
                 now_fn: Callable[[], datetime] | None = None):
        self.per_task = max(0, int(per_task))
        self.per_run = max(0, int(per_run))
        self.daily = max(0, int(daily))
        self.failure_threshold = max(1, int(failure_threshold))
        self.state_path = Path(state_path)
        self.state_backend = (
            "sqlite" if self.state_path.suffix.lower() in _SQLITE_SUFFIXES else "json"
        )
        self._state_lock = _STATE_LOCKS[str(self.state_path.resolve())]
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.run_calls = 0
        self.task_calls = defaultdict(int)
        self.consecutive_failures = 0
        self.circuit_open = False
        self.state_available = True
        self.last_denial_reason: str | None = None
        self._day = self._current_day()
        self.daily_calls = self._read_daily_calls()

    def _current_day(self) -> str:
        now = self._now_fn()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.astimezone(_BEIJING_TZ).date().isoformat()

    def _refresh_day(self) -> None:
        self._day = self._current_day()

    def _sqlite_connection(self) -> sqlite3.Connection:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.state_path), timeout=5)
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS model_budget_daily ("
                "day TEXT PRIMARY KEY, calls INTEGER NOT NULL)"
            )
            connection.commit()
            return connection
        except Exception:
            connection.close()
            raise

    def _read_daily_calls(self, *, strict: bool = False) -> int:
        try:
            if self.state_backend == "sqlite":
                connection = self._sqlite_connection()
                try:
                    row = connection.execute(
                        "SELECT calls FROM model_budget_daily WHERE day = ?",
                        (self._day,),
                    ).fetchone()
                finally:
                    connection.close()
                self.state_available = True
                return max(0, int(row[0])) if row else 0
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("budget state must be a JSON object")
            if data.get("date") == self._day:
                self.state_available = True
                return max(0, int(data.get("calls") or 0))
        except FileNotFoundError:
            self.state_available = True
            return 0
        except (ValueError, TypeError, OSError, sqlite3.Error):
            self.state_available = False
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

    def _reserve_sqlite_daily(self) -> tuple[bool, int]:
        connection = self._sqlite_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT calls FROM model_budget_daily WHERE day = ?",
                (self._day,),
            ).fetchone()
            calls = max(0, int(row[0])) if row else 0
            if calls >= self.daily:
                connection.rollback()
                return False, calls
            calls += 1
            connection.execute(
                "INSERT INTO model_budget_daily(day, calls) VALUES(?, ?) "
                "ON CONFLICT(day) DO UPDATE SET calls = excluded.calls",
                (self._day, calls),
            )
            connection.commit()
            return True, calls
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _deny(self, reason: str) -> tuple[bool, str]:
        self.last_denial_reason = reason
        return False, reason

    def reserve(self, task_id: str) -> tuple[bool, str]:
        with self._state_lock:
            self._refresh_day()
            if self.circuit_open:
                return self._deny("circuit_open")
            if self.per_task == 0 or self.task_calls[task_id] >= self.per_task:
                return self._deny("task_budget_exhausted")
            if self.per_run == 0 or self.run_calls >= self.per_run:
                return self._deny("run_budget_exhausted")
            if self.daily == 0:
                return self._deny("daily_budget_exhausted")
            try:
                if self.state_backend == "sqlite":
                    reserved, self.daily_calls = self._reserve_sqlite_daily()
                    if not reserved:
                        self.state_available = True
                        return self._deny("daily_budget_exhausted")
                else:
                    self.daily_calls = self._read_daily_calls(strict=True)
                    if self.daily_calls >= self.daily:
                        return self._deny("daily_budget_exhausted")
                    self.daily_calls += 1
                    self._save()
                self.state_available = True
            except (ValueError, TypeError, OSError, sqlite3.Error):
                self.state_available = False
                return self._deny("budget_state_unavailable")
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
            self._refresh_day()
            try:
                self.daily_calls = self._read_daily_calls(strict=True)
                self.state_available = True
            except (ValueError, TypeError, OSError, sqlite3.Error):
                self.state_available = False
        return {
            "run_calls": self.run_calls,
            "daily_calls": self.daily_calls,
            "budget_day": self._day,
            "per_task_limit": self.per_task,
            "per_run_limit": self.per_run,
            "daily_limit": self.daily,
            "consecutive_failures": self.consecutive_failures,
            "failure_threshold": self.failure_threshold,
            "circuit_open": self.circuit_open,
            "state_backend": self.state_backend,
            "state_available": self.state_available,
            "last_denial_reason": self.last_denial_reason,
        }
