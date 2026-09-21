"""Durable cooldown claims for endpoint-failure alerts."""
from __future__ import annotations

import json
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
        self._incident_fallback: dict[str, dict] = {}
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
            connection.execute(
                "CREATE TABLE IF NOT EXISTS endpoint_alert_incidents ("
                "incident_key TEXT PRIMARY KEY, first_failure_ts REAL NOT NULL, "
                "last_failure_ts REAL NOT NULL, last_alert_ts REAL, "
                "alert_count INTEGER NOT NULL DEFAULT 0, endpoints_json TEXT NOT NULL)"
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

    @staticmethod
    def _incident_result(
        incident_key: str, first_failure_ts: float, last_failure_ts: float,
        alert_count: int, endpoints: dict, now: float,
    ) -> dict:
        return {
            "incident_key": incident_key,
            "first_failure_ts": float(first_failure_ts),
            "last_failure_ts": float(last_failure_ts),
            "duration_seconds": max(0.0, float(now) - float(first_failure_ts)),
            "alert_count": int(alert_count),
            "endpoints": sorted(endpoints),
        }

    def _record_incident_sqlite(
        self, incident_key: str, endpoint: str, now: float,
        initial_delay_seconds: int, reminder_seconds: int,
        reset_after_seconds: int,
    ) -> dict | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT first_failure_ts, last_failure_ts, last_alert_ts, "
                "alert_count, endpoints_json FROM endpoint_alert_incidents "
                "WHERE incident_key = ?",
                (incident_key,),
            ).fetchone()
            if row and now - float(row[1]) <= reset_after_seconds:
                first_failure_ts = float(row[0])
                last_alert_ts = float(row[2]) if row[2] is not None else None
                alert_count = int(row[3])
                try:
                    endpoints = json.loads(row[4]) or {}
                except (TypeError, ValueError):
                    endpoints = {}
            else:
                first_failure_ts = now
                last_alert_ts = None
                alert_count = 0
                endpoints = {}

            endpoints[endpoint] = {"last_failure_ts": now, "healthy": False}
            should_alert = (
                now - first_failure_ts >= initial_delay_seconds
                and (
                    last_alert_ts is None
                    or now - last_alert_ts >= reminder_seconds
                )
            )
            if should_alert:
                last_alert_ts = now
                alert_count += 1
            connection.execute(
                "INSERT INTO endpoint_alert_incidents(incident_key, first_failure_ts, "
                "last_failure_ts, last_alert_ts, alert_count, endpoints_json) "
                "VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(incident_key) DO UPDATE SET "
                "first_failure_ts=excluded.first_failure_ts, "
                "last_failure_ts=excluded.last_failure_ts, "
                "last_alert_ts=excluded.last_alert_ts, alert_count=excluded.alert_count, "
                "endpoints_json=excluded.endpoints_json",
                (
                    incident_key, first_failure_ts, now, last_alert_ts, alert_count,
                    json.dumps(endpoints, ensure_ascii=False, separators=(",", ":")),
                ),
            )
            connection.commit()
            if not should_alert:
                return None
            return self._incident_result(
                incident_key, first_failure_ts, now, alert_count, endpoints, now,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def record_incident_failure(
        self, incident_key: str, endpoint: str, *, now: float | None = None,
        initial_delay_seconds: int = 30 * 60,
        reminder_seconds: int = 6 * 3600,
        reset_after_seconds: int = 12 * 3600,
    ) -> dict | None:
        """Track one root-cause incident and claim only actionable reminders."""
        incident_key = str(incident_key or "unknown")
        endpoint = str(endpoint or "unknown")
        current = float(time.time() if now is None else now)
        with self._lock:
            try:
                result = self._record_incident_sqlite(
                    incident_key, endpoint, current, max(1, initial_delay_seconds),
                    max(1, reminder_seconds), max(1, reset_after_seconds),
                )
                self.state_available = True
                return result
            except (OSError, sqlite3.Error):
                self.state_available = False
                state = self._incident_fallback.get(incident_key)
                if not state or current - state["last_failure_ts"] > reset_after_seconds:
                    state = {
                        "first_failure_ts": current, "last_alert_ts": None,
                        "alert_count": 0, "endpoints": {},
                    }
                state["last_failure_ts"] = current
                state["endpoints"][endpoint] = {
                    "last_failure_ts": current, "healthy": False,
                }
                should_alert = (
                    current - state["first_failure_ts"] >= initial_delay_seconds
                    and (
                        state["last_alert_ts"] is None
                        or current - state["last_alert_ts"] >= reminder_seconds
                    )
                )
                if should_alert:
                    state["last_alert_ts"] = current
                    state["alert_count"] += 1
                self._incident_fallback[incident_key] = state
                if not should_alert:
                    return None
                return self._incident_result(
                    incident_key, state["first_failure_ts"], current,
                    state["alert_count"], state["endpoints"], current,
                )

    def resolve_incident_endpoint(
        self, incident_key: str, endpoint: str, *, now: float | None = None,
    ) -> dict | None:
        """Mark one failed endpoint healthy; return once when all have recovered."""
        incident_key = str(incident_key or "unknown")
        endpoint = str(endpoint or "unknown")
        current = float(time.time() if now is None else now)
        with self._lock:
            try:
                connection = self._connect()
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    row = connection.execute(
                        "SELECT first_failure_ts, last_failure_ts, alert_count, "
                        "endpoints_json FROM endpoint_alert_incidents "
                        "WHERE incident_key = ?", (incident_key,),
                    ).fetchone()
                    if not row:
                        connection.rollback()
                        return None
                    endpoints = json.loads(row[3]) or {}
                    if endpoint not in endpoints:
                        connection.rollback()
                        return None
                    endpoints[endpoint]["healthy"] = True
                    if not all(bool(item.get("healthy")) for item in endpoints.values()):
                        connection.execute(
                            "UPDATE endpoint_alert_incidents SET endpoints_json = ? "
                            "WHERE incident_key = ?",
                            (json.dumps(endpoints, ensure_ascii=False, separators=(",", ":")), incident_key),
                        )
                        connection.commit()
                        return None
                    connection.execute(
                        "DELETE FROM endpoint_alert_incidents WHERE incident_key = ?",
                        (incident_key,),
                    )
                    connection.commit()
                    if int(row[2]) < 1:
                        return None
                    return self._incident_result(
                        incident_key, float(row[0]), float(row[1]), int(row[2]),
                        endpoints, current,
                    )
                finally:
                    connection.close()
            except (OSError, sqlite3.Error, TypeError, ValueError):
                self.state_available = False
                state = self._incident_fallback.get(incident_key)
                if not state or endpoint not in state["endpoints"]:
                    return None
                state["endpoints"][endpoint]["healthy"] = True
                if not all(bool(item.get("healthy")) for item in state["endpoints"].values()):
                    return None
                self._incident_fallback.pop(incident_key, None)
                if state["alert_count"] < 1:
                    return None
                return self._incident_result(
                    incident_key, state["first_failure_ts"], state["last_failure_ts"],
                    state["alert_count"], state["endpoints"], current,
                )

    def release_incident_alert(self, incident_key: str) -> None:
        """Allow the next failure to retry when no incident card was delivered."""
        incident_key = str(incident_key or "unknown")
        with self._lock:
            try:
                connection = self._connect()
                try:
                    connection.execute(
                        "UPDATE endpoint_alert_incidents SET last_alert_ts = NULL, "
                        "alert_count = CASE WHEN alert_count > 0 THEN alert_count - 1 ELSE 0 END "
                        "WHERE incident_key = ?", (incident_key,),
                    )
                    connection.commit()
                    self.state_available = True
                finally:
                    connection.close()
            except (OSError, sqlite3.Error):
                self.state_available = False
            state = self._incident_fallback.get(incident_key)
            if state:
                state["last_alert_ts"] = None
                state["alert_count"] = max(0, state["alert_count"] - 1)

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
