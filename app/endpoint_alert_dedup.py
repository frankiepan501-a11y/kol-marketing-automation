"""Durable cooldown claims for endpoint-failure alerts."""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


class EndpointAlertDedup:
    """Claim one alert per endpoint and cooldown across process restarts."""

    def __init__(self, state_path: str | Path, *, cooldown_seconds: int = 3600):
        self.state_path = Path(state_path)
        self.cooldown_seconds = max(1, int(cooldown_seconds))
        self._lock = threading.Lock()
        self._fallback: dict[str, float] = {}
        self.state_available: bool | None = None

    def _connect(self) -> sqlite3.Connection:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.state_path), timeout=5)
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS endpoint_alert_claims ("
                "endpoint TEXT PRIMARY KEY, last_alert_ts REAL NOT NULL)"
            )
            connection.commit()
            return connection
        except Exception:
            connection.close()
            raise

    def _claim_sqlite(self, endpoint: str, now: float) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT last_alert_ts FROM endpoint_alert_claims WHERE endpoint = ?",
                (endpoint,),
            ).fetchone()
            last = float(row[0]) if row else None
            if last is not None and now - last < self.cooldown_seconds:
                connection.rollback()
                return False
            connection.execute(
                "INSERT INTO endpoint_alert_claims(endpoint, last_alert_ts) VALUES(?, ?) "
                "ON CONFLICT(endpoint) DO UPDATE SET last_alert_ts = excluded.last_alert_ts",
                (endpoint, now),
            )
            connection.execute(
                "DELETE FROM endpoint_alert_claims WHERE last_alert_ts < ?",
                (now - 7 * 24 * 3600,),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def claim(self, endpoint: str, *, now: float | None = None) -> bool:
        endpoint = str(endpoint or "unknown")
        current = float(time.time() if now is None else now)
        with self._lock:
            try:
                claimed = self._claim_sqlite(endpoint, current)
                self.state_available = True
                return claimed
            except (OSError, sqlite3.Error):
                # Keep the old in-process protection if the persistent volume is
                # temporarily unavailable; the health response exposes this state.
                self.state_available = False
                last = self._fallback.get(endpoint)
                if last is not None and current - last < self.cooldown_seconds:
                    return False
                self._fallback[endpoint] = current
                return True

    def release(self, endpoint: str) -> None:
        """Release a reservation when no alert was actually delivered."""
        endpoint = str(endpoint or "unknown")
        with self._lock:
            try:
                connection = self._connect()
                try:
                    connection.execute(
                        "DELETE FROM endpoint_alert_claims WHERE endpoint = ?",
                        (endpoint,),
                    )
                    connection.commit()
                    self.state_available = True
                finally:
                    connection.close()
            except (OSError, sqlite3.Error):
                self.state_available = False
            self._fallback.pop(endpoint, None)

    def snapshot(self) -> dict:
        with self._lock:
            try:
                connection = self._connect()
                connection.close()
                self.state_available = True
            except (OSError, sqlite3.Error):
                self.state_available = False
        return {
            "backend": "sqlite",
            "persistent": self.state_available is True,
            "state_available": self.state_available,
            "cooldown_seconds": self.cooldown_seconds,
        }
