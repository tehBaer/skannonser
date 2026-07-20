"""Single choke point for paid Google APIs: rate limiting, monthly budget
enforcement, warn-threshold notifications, and a call ledger in api_usage."""
import sqlite3
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TypeVar

from skannonser.config.domain import Budget
from skannonser.config.settings import get_secrets

T = TypeVar("T")

_APIS = {"routes", "geocode"}


class BudgetExceeded(RuntimeError):
    def __init__(self, api: str, usage: int, cap: int):
        self.api = api
        self.usage = usage
        self.cap = cap
        super().__init__(f"{api}: monthly budget exceeded ({usage}/{cap} calls)")


def _default_notify(message: str) -> None:
    try:
        subprocess.run(
            [get_secrets().notify_bin, "send", message], check=False, timeout=10
        )
    except Exception:
        pass


def _default_clock() -> str:
    # UTC to match the api_usage.called_at SQL default (datetime('now') is UTC in SQLite).
    return datetime.now(timezone.utc).strftime("%Y-%m")


class Gateway:
    def __init__(
        self,
        conn: sqlite3.Connection,
        budget: Budget,
        notify: Callable[[str], None] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], str] | None = None,
    ):
        self.conn = conn
        self.budget = budget
        self.notify = notify or _default_notify
        self.sleeper = sleeper
        self.clock = clock or _default_clock
        self._last_call: dict[str, float] = {}

    def call(self, api: str, fn: Callable[[], T], finnkode: str | None = None) -> T:
        self._check_known(api)
        self._rate_limit(api)

        cap = getattr(self.budget, f"{api}_monthly_cap")
        usage = self.month_usage(api)
        if usage >= cap:
            self._record(api, "blocked", finnkode)
            raise BudgetExceeded(api, usage, cap)

        self._maybe_warn(api, usage, cap)

        try:
            result = fn()
        except Exception:
            self._record(api, "error", finnkode)
            raise
        self._record(api, "ok", finnkode)
        return result

    def month_usage(self, api: str) -> int:
        self._check_known(api)
        month = self.clock()
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM api_usage "
            "WHERE api = ? AND outcome IN ('ok', 'error') "
            "AND strftime('%Y-%m', called_at) = ?",
            (api, month),
        ).fetchone()
        return row["c"]

    def _check_known(self, api: str) -> None:
        if api not in _APIS:
            raise ValueError(f"unknown api: {api!r} (expected one of {sorted(_APIS)})")

    def _rate_limit(self, api: str) -> None:
        rpm = getattr(self.budget, f"{api}_rpm")
        min_interval = 60.0 / rpm
        last = self._last_call.get(api)
        if last is not None:
            remaining = min_interval - (time.monotonic() - last)
            if remaining > 0:
                self.sleeper(remaining)
        self._last_call[api] = time.monotonic()

    def _maybe_warn(self, api: str, usage: int, cap: int) -> None:
        month = self.clock()
        for pct in self.budget.warn_pcts:
            if usage * 100 < pct * cap:
                continue
            if self._already_warned(api, pct, month):
                continue
            self._record(api, f"warn:{pct}", None)
            self._safe_notify(f"{api}: {usage}/{cap} calls this month ({pct}% of budget)")

    def _already_warned(self, api: str, pct: int, month: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM api_usage "
            "WHERE api = ? AND outcome = ? AND strftime('%Y-%m', called_at) = ? LIMIT 1",
            (api, f"warn:{pct}", month),
        ).fetchone()
        return row is not None

    def _safe_notify(self, message: str) -> None:
        try:
            self.notify(message)
        except Exception:
            pass

    def _record(self, api: str, outcome: str, finnkode: str | None) -> None:
        self.conn.execute(
            "INSERT INTO api_usage (api, outcome, finnkode) VALUES (?, ?, ?)",
            (api, outcome, finnkode),
        )
        self.conn.commit()
