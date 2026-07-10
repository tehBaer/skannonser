# Phone Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send Pushover push-notifications to the owner's phone for daily listing churn, a weekly added/sold summary, battery/power events, and (via a Healthchecks.io heartbeat) server-offline detection.

**Architecture:** A new `main/notify/` Python package plus two additive SQLite tables. Pure logic (metric diffs, battery state machine, message formatting) is separated from I/O (a single Pushover sender, DB queries) so it can be unit-tested without network. Cron on the `mbp` box drives the jobs; a cron `curl` handles the heartbeat.

**Tech Stack:** Python 3.12, stdlib `unittest`, `requests`, SQLite (`properties.db`), cron, Makefile, Pushover API, Healthchecks.io.

## Global Constraints

- Python 3.12; tests use stdlib `unittest` only (no pytest). Run: `.venv/bin/python -m unittest tests.<module> -v` from repo root `~/kode/skannonser`.
- Tests put repo root on `sys.path`: `REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`.
- New DB tables are additive, created with `CREATE TABLE IF NOT EXISTS` in `PropertyDatabase._init_db`.
- "Active tracked listings" = `eiendom.active = 1` AND `pris <= SHEETS_MAX_PRICE (7500000)` AND `CAST(info_usable_i_area AS REAL) >= MIN_BRA_I (70)` (values from `main/config/filters.py`).
- Secrets live only in a gitignored `main/config/notify_secrets.py` on the box — never committed.
- Runnable modules are invoked as `.venv/bin/python -m main.notify.<module>` from repo root.
- Commit messages end with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Battery thresholds: 50, 20, 10 (percent). Weekly summary window: 7 days inclusive, sent Sundays 08:00. Daily summary 07:00. Battery + heartbeat every 10 min.

---

## File Structure

- `main/notify/__init__.py` — package marker.
- `main/notify/config.py` — loads Pushover/Healthchecks secrets.
- `main/notify/pushover.py` — `send(title, message, priority)` (the only network sender).
- `main/notify/listing_metrics.py` — `DailyMetrics`, `compute_daily_metrics`, `format_daily_message` (pure).
- `main/notify/daily_summary.py` — daily orchestrator (`run`).
- `main/notify/weekly_summary.py` — weekly orchestrator (`run`, `format_weekly_message`).
- `main/notify/battery.py` — `read_battery`, `decide_alerts` (pure), `run`.
- `main/database/db.py` — MODIFY: two new tables + accessor methods.
- `main/config/notify_secrets.py` — gitignored secrets (created on box).
- `.gitignore` — MODIFY: add `main/config/notify_secrets.py`.
- `Makefile` — MODIFY: `notify-daily`, `notify-weekly`, `notify-battery` targets.
- Tests: `tests/test_notify_db.py`, `tests/test_notify_pushover.py`, `tests/test_listing_metrics.py`, `tests/test_daily_summary.py`, `tests/test_weekly_summary.py`, `tests/test_battery.py`.

---

### Task 1: DB layer — snapshot & metrics tables + accessors

**Files:**
- Modify: `main/database/db.py` (add tables in `_init_db`; add methods on `PropertyDatabase`)
- Test: `tests/test_notify_db.py`

**Interfaces:**
- Produces:
  - `get_active_tracked_finnkodes() -> set[str]`
  - `get_finnkodes_with_status(finnkodes: Iterable[str], status: str) -> set[str]`
  - `get_previous_active_snapshot() -> set[str]`
  - `replace_active_snapshot(finnkodes: Iterable[str]) -> None`
  - `record_daily_metrics(metric_date: str, added: int, removed_sold: int, removed_delisted: int, total_active: int) -> None`
  - `sum_daily_metrics_between(start_date: str, end_date: str) -> dict` (keys: `added`, `removed_sold`, `removed_delisted`)
  - `count_sold_between(start_date: str, end_date: str) -> int`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_notify_db.py`:

```python
import os, sqlite3, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase


class NotifyDbTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = PropertyDatabase(os.path.join(self._tmp.name, "t.db"))
        self._conn = sqlite3.connect(self.db.db_path)

    def tearDown(self):
        self._conn.close()
        self._tmp.cleanup()

    def _insert(self, finnkode, active=1, pris=3000000, brai=80, status=None):
        self._conn.execute(
            "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
            "VALUES (?,?,?,?,?,?)",
            (finnkode, active, pris, brai, status, f"https://finn.no/{finnkode}"),
        )
        self._conn.commit()

    def test_active_tracked_respects_filters(self):
        self._insert("1", active=1, pris=3000000, brai=80)   # pass
        self._insert("2", active=0, pris=3000000, brai=80)   # inactive -> excluded
        self._insert("3", active=1, pris=9000000, brai=80)   # too pricey -> excluded
        self._insert("4", active=1, pris=3000000, brai=50)   # too small -> excluded
        self.assertEqual(self.db.get_active_tracked_finnkodes(), {"1"})

    def test_finnkodes_with_status(self):
        self._insert("10", status="Solgt")
        self._insert("11", status=None)
        self.assertEqual(self.db.get_finnkodes_with_status(["10", "11", "99"], "Solgt"), {"10"})

    def test_snapshot_round_trip_and_replace(self):
        self.db.replace_active_snapshot({"a", "b"})
        self.assertEqual(self.db.get_previous_active_snapshot(), {"a", "b"})
        self.db.replace_active_snapshot({"b", "c"})
        self.assertEqual(self.db.get_previous_active_snapshot(), {"b", "c"})

    def test_daily_metrics_record_and_sum(self):
        self.db.record_daily_metrics("2026-07-06", 5, 2, 1, 100)
        self.db.record_daily_metrics("2026-07-07", 3, 0, 4, 101)
        got = self.db.sum_daily_metrics_between("2026-07-06", "2026-07-07")
        self.assertEqual(got, {"added": 8, "removed_sold": 2, "removed_delisted": 5})

    def test_record_daily_metrics_is_idempotent_per_date(self):
        self.db.record_daily_metrics("2026-07-06", 5, 2, 1, 100)
        self.db.record_daily_metrics("2026-07-06", 9, 9, 9, 200)  # replace same date
        self.assertEqual(self.db.sum_daily_metrics_between("2026-07-06", "2026-07-06"),
                         {"added": 9, "removed_sold": 9, "removed_delisted": 9})

    def test_count_sold_between(self):
        self.db.record_status_change_if_changed("50", "", "Solgt")   # observed_at = now
        self.db.record_status_change_if_changed("51", "", "Reservert")
        from datetime import date
        today = date.today().isoformat()
        self.assertEqual(self.db.count_sold_between(today, today), 1)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m unittest tests.test_notify_db -v`
Expected: FAIL — `AttributeError: 'PropertyDatabase' object has no attribute 'get_active_tracked_finnkodes'`.

- [ ] **Step 3: Add the two tables**

In `main/database/db.py`, inside `_init_db`, immediately after the `eiendom_status_history` table + index block (before `conn.commit()` at the end of that first connection block), add:

```python
        # Notification support: daily active-set snapshot + per-day metrics.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_listing_snapshot (
                finnkode TEXT PRIMARY KEY
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_metrics (
                metric_date TEXT PRIMARY KEY,
                added INTEGER NOT NULL DEFAULT 0,
                removed_sold INTEGER NOT NULL DEFAULT 0,
                removed_delisted INTEGER NOT NULL DEFAULT 0,
                total_active INTEGER NOT NULL DEFAULT 0
            )
        ''')
```

- [ ] **Step 4: Add the accessor methods**

In `main/database/db.py`, add these methods to `PropertyDatabase` (after `get_status_history`):

```python
    def _sheet_filters(self):
        try:
            from main.config.filters import SHEETS_MAX_PRICE, MIN_BRA_I
        except ImportError:
            try:
                from config.filters import SHEETS_MAX_PRICE, MIN_BRA_I
            except ImportError:
                SHEETS_MAX_PRICE, MIN_BRA_I = None, None
        return SHEETS_MAX_PRICE, MIN_BRA_I

    def get_active_tracked_finnkodes(self) -> set:
        """Active listings passing the sheet price/area filters (the tracked set)."""
        max_price, min_brai = self._sheet_filters()
        query = "SELECT finnkode FROM eiendom WHERE active = 1"
        params = []
        if max_price is not None:
            query += " AND pris <= ?"
            params.append(max_price)
        if min_brai is not None:
            query += " AND CAST(info_usable_i_area AS REAL) >= ?"
            params.append(min_brai)
        conn = self.get_connection()
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()
        return {str(r[0]).strip() for r in rows}

    def get_finnkodes_with_status(self, finnkodes, status: str) -> set:
        """Subset of finnkodes whose current tilgjengelighet == status."""
        fk = [str(f).strip() for f in finnkodes]
        if not fk:
            return set()
        conn = self.get_connection()
        try:
            result = set()
            for i in range(0, len(fk), 500):
                chunk = fk[i:i + 500]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"SELECT finnkode FROM eiendom WHERE tilgjengelighet = ? "
                    f"AND finnkode IN ({placeholders})",
                    [status, *chunk],
                ).fetchall()
                result.update(str(r[0]).strip() for r in rows)
            return result
        finally:
            conn.close()

    def get_previous_active_snapshot(self) -> set:
        conn = self.get_connection()
        try:
            rows = conn.execute("SELECT finnkode FROM daily_listing_snapshot").fetchall()
        finally:
            conn.close()
        return {str(r[0]).strip() for r in rows}

    def replace_active_snapshot(self, finnkodes) -> None:
        conn = self.get_connection()
        try:
            conn.execute("DELETE FROM daily_listing_snapshot")
            conn.executemany(
                "INSERT OR IGNORE INTO daily_listing_snapshot (finnkode) VALUES (?)",
                [(str(f).strip(),) for f in finnkodes],
            )
            conn.commit()
        finally:
            conn.close()

    def record_daily_metrics(self, metric_date, added, removed_sold, removed_delisted, total_active) -> None:
        conn = self.get_connection()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO daily_metrics "
                "(metric_date, added, removed_sold, removed_delisted, total_active) "
                "VALUES (?, ?, ?, ?, ?)",
                (metric_date, added, removed_sold, removed_delisted, total_active),
            )
            conn.commit()
        finally:
            conn.close()

    def sum_daily_metrics_between(self, start_date, end_date) -> dict:
        conn = self.get_connection()
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(added),0), COALESCE(SUM(removed_sold),0), "
                "COALESCE(SUM(removed_delisted),0) FROM daily_metrics "
                "WHERE metric_date >= ? AND metric_date <= ?",
                (start_date, end_date),
            ).fetchone()
        finally:
            conn.close()
        return {"added": row[0], "removed_sold": row[1], "removed_delisted": row[2]}

    def count_sold_between(self, start_date, end_date) -> int:
        """Count status->Solgt transitions with observed_at date in [start_date, end_date]."""
        conn = self.get_connection()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM eiendom_status_history "
                "WHERE new_status = 'Solgt' AND date(observed_at) >= ? AND date(observed_at) <= ?",
                (start_date, end_date),
            ).fetchone()
        finally:
            conn.close()
        return row[0]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m unittest tests.test_notify_db -v`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add main/database/db.py tests/test_notify_db.py
git commit -m "notify: add daily snapshot + metrics tables and accessors" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Config + Pushover sender

**Files:**
- Create: `main/notify/__init__.py` (empty)
- Create: `main/notify/config.py`
- Create: `main/notify/pushover.py`
- Test: `tests/test_notify_pushover.py`

**Interfaces:**
- Produces: `pushover.send(title: str, message: str, priority: int = 0, *, app_token=None, user_key=None, timeout=10) -> bool`
- Consumes: `config.PUSHOVER_APP_TOKEN`, `config.PUSHOVER_USER_KEY`, `config.HEALTHCHECKS_URL`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_notify_pushover.py`:

```python
import os, sys, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify import pushover


class PushoverTests(unittest.TestCase):
    def test_send_posts_expected_payload_and_returns_true(self):
        fake_resp = mock.Mock()
        fake_resp.raise_for_status.return_value = None
        with mock.patch.object(pushover.requests, "post", return_value=fake_resp) as post:
            ok = pushover.send("Title", "Body", priority=1, app_token="A", user_key="U")
        self.assertTrue(ok)
        args, kwargs = post.call_args
        self.assertEqual(args[0], "https://api.pushover.net/1/messages.json")
        self.assertEqual(kwargs["data"]["token"], "A")
        self.assertEqual(kwargs["data"]["user"], "U")
        self.assertEqual(kwargs["data"]["title"], "Title")
        self.assertEqual(kwargs["data"]["message"], "Body")
        self.assertEqual(kwargs["data"]["priority"], 1)

    def test_send_returns_false_when_credentials_missing(self):
        with mock.patch.object(pushover.requests, "post") as post:
            ok = pushover.send("T", "B", app_token="", user_key="")
        self.assertFalse(ok)
        post.assert_not_called()

    def test_send_returns_false_on_http_error(self):
        with mock.patch.object(pushover.requests, "post", side_effect=Exception("boom")):
            ok = pushover.send("T", "B", app_token="A", user_key="U")
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m unittest tests.test_notify_pushover -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main.notify'`.

- [ ] **Step 3: Create the package + config**

Create `main/notify/__init__.py` (empty file).

Create `main/notify/config.py`:

```python
"""Loads notification secrets from a gitignored module or environment variables."""
import os

try:
    from main.config import notify_secrets as _secrets
except Exception:
    try:
        from config import notify_secrets as _secrets
    except Exception:
        _secrets = None


def _get(name, default=""):
    if _secrets is not None and hasattr(_secrets, name):
        return getattr(_secrets, name)
    return os.environ.get(name, default)


PUSHOVER_APP_TOKEN = _get("PUSHOVER_APP_TOKEN")
PUSHOVER_USER_KEY = _get("PUSHOVER_USER_KEY")
HEALTHCHECKS_URL = _get("HEALTHCHECKS_URL")
```

- [ ] **Step 4: Create the sender**

Create `main/notify/pushover.py`:

```python
"""Single choke-point for sending Pushover notifications."""
import requests

from main.notify import config

API_URL = "https://api.pushover.net/1/messages.json"


def send(title, message, priority=0, *, app_token=None, user_key=None, timeout=10) -> bool:
    token = app_token if app_token is not None else config.PUSHOVER_APP_TOKEN
    user = user_key if user_key is not None else config.PUSHOVER_USER_KEY
    if not token or not user:
        print("[pushover] missing credentials; not sending")
        return False
    payload = {
        "token": token,
        "user": user,
        "title": title,
        "message": message,
        "priority": priority,
    }
    try:
        resp = requests.post(API_URL, data=payload, timeout=timeout)
        resp.raise_for_status()
        return True
    except Exception as exc:
        print(f"[pushover] send failed: {exc}")
        return False
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m unittest tests.test_notify_pushover -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add main/notify/__init__.py main/notify/config.py main/notify/pushover.py tests/test_notify_pushover.py
git commit -m "notify: add config loader and Pushover sender" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Listing metrics (pure)

**Files:**
- Create: `main/notify/listing_metrics.py`
- Test: `tests/test_listing_metrics.py`

**Interfaces:**
- Produces:
  - `DailyMetrics` dataclass: `added: int, removed_sold: int, removed_delisted: int, total_active: int, added_finnkodes: set, removed_finnkodes: set`
  - `compute_daily_metrics(current: set, previous: set, sold_removed: set) -> DailyMetrics`
  - `format_daily_message(m: DailyMetrics) -> str`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_listing_metrics.py`:

```python
import os, sys, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify.listing_metrics import compute_daily_metrics, format_daily_message


class ListingMetricsTests(unittest.TestCase):
    def test_added_and_removed_split_sold_vs_delisted(self):
        previous = {"a", "b", "c", "d"}
        current = {"c", "d", "e"}           # added e; removed a, b
        sold_removed = {"a"}                # a was sold; b delisted
        m = compute_daily_metrics(current, previous, sold_removed)
        self.assertEqual(m.added, 1)
        self.assertEqual(m.removed_sold, 1)
        self.assertEqual(m.removed_delisted, 1)
        self.assertEqual(m.total_active, 3)
        self.assertEqual(m.added_finnkodes, {"e"})
        self.assertEqual(m.removed_finnkodes, {"a", "b"})

    def test_no_change(self):
        m = compute_daily_metrics({"a", "b"}, {"a", "b"}, set())
        self.assertEqual((m.added, m.removed_sold, m.removed_delisted, m.total_active), (0, 0, 0, 2))

    def test_format_message(self):
        m = compute_daily_metrics({"c", "d", "e"}, {"a", "b", "c", "d"}, {"a"})
        self.assertEqual(
            format_daily_message(m),
            "\U0001F3E0 Today: +1 added, -2 removed (1 sold, 1 delisted). Active: 3.",
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m unittest tests.test_listing_metrics -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main.notify.listing_metrics'`.

- [ ] **Step 3: Implement**

Create `main/notify/listing_metrics.py`:

```python
"""Pure daily added/removed metrics from active-set diffs."""
from dataclasses import dataclass, field


@dataclass
class DailyMetrics:
    added: int
    removed_sold: int
    removed_delisted: int
    total_active: int
    added_finnkodes: set = field(default_factory=set)
    removed_finnkodes: set = field(default_factory=set)


def compute_daily_metrics(current: set, previous: set, sold_removed: set) -> DailyMetrics:
    added = set(current) - set(previous)
    removed = set(previous) - set(current)
    removed_sold = removed & set(sold_removed)
    removed_delisted = removed - set(sold_removed)
    return DailyMetrics(
        added=len(added),
        removed_sold=len(removed_sold),
        removed_delisted=len(removed_delisted),
        total_active=len(current),
        added_finnkodes=added,
        removed_finnkodes=removed,
    )


def format_daily_message(m: DailyMetrics) -> str:
    removed_total = m.removed_sold + m.removed_delisted
    return (
        f"\U0001F3E0 Today: +{m.added} added, -{removed_total} removed "
        f"({m.removed_sold} sold, {m.removed_delisted} delisted). "
        f"Active: {m.total_active}."
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m unittest tests.test_listing_metrics -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add main/notify/listing_metrics.py tests/test_listing_metrics.py
git commit -m "notify: add pure daily listing-metrics computation" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Daily summary orchestrator

**Files:**
- Create: `main/notify/daily_summary.py`
- Test: `tests/test_daily_summary.py`

**Interfaces:**
- Consumes: Task 1 DB methods; Task 3 `compute_daily_metrics`, `format_daily_message`; `pushover.send`.
- Produces: `run(db_path=None, today=None, send=pushover.send) -> bool`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_daily_summary.py`:

```python
import os, sqlite3, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase
from main.notify import daily_summary


class DailySummaryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "t.db")
        self.db = PropertyDatabase(self.db_path)
        self._conn = sqlite3.connect(self.db_path)
        self.sent = []

    def tearDown(self):
        self._conn.close()
        self._tmp.cleanup()

    def _fake_send(self, title, message, priority=0):
        self.sent.append((title, message, priority))
        return True

    def _insert(self, finnkode, active=1, status=None):
        self._conn.execute(
            "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
            "VALUES (?,?,?,?,?,?)",
            (finnkode, active, 3000000, 80, status, f"https://finn.no/{finnkode}"),
        )
        self._conn.commit()

    def test_first_run_sets_baseline_without_diff(self):
        self._insert("1"); self._insert("2")
        ok = daily_summary.run(db_path=self.db_path, today="2026-07-10", send=self._fake_send)
        self.assertTrue(ok)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("Baseline", self.sent[0][1])
        self.assertEqual(self.db.get_previous_active_snapshot(), {"1", "2"})

    def test_second_run_reports_added_and_removed(self):
        # Seed baseline of {1,2}
        self.db.replace_active_snapshot({"1", "2"})
        # Now active tracked = {2,3}; 1 removed (sold), 3 added
        self._insert("2"); self._insert("3")
        self._insert("1", active=0, status="Solgt")  # in db but not active -> removed, sold
        ok = daily_summary.run(db_path=self.db_path, today="2026-07-11", send=self._fake_send)
        self.assertTrue(ok)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0][1],
                         "\U0001F3E0 Today: +1 added, -1 removed (1 sold, 0 delisted). Active: 2.")
        self.assertEqual(self.db.get_previous_active_snapshot(), {"2", "3"})
        self.assertEqual(self.db.sum_daily_metrics_between("2026-07-11", "2026-07-11"),
                         {"added": 1, "removed_sold": 1, "removed_delisted": 0})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m unittest tests.test_daily_summary -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main.notify.daily_summary'`.

- [ ] **Step 3: Implement**

Create `main/notify/daily_summary.py`:

```python
"""Daily orchestrator: diff active set, notify, persist metrics + snapshot."""
from datetime import date

from main.database.db import PropertyDatabase
from main.notify import pushover
from main.notify.listing_metrics import compute_daily_metrics, format_daily_message


def run(db_path=None, today=None, send=pushover.send) -> bool:
    db = PropertyDatabase(db_path)
    today = today or date.today().isoformat()
    previous = db.get_previous_active_snapshot()
    current = db.get_active_tracked_finnkodes()

    if not previous:
        # First run: establish baseline; do not report a misleading full diff.
        db.replace_active_snapshot(current)
        db.record_daily_metrics(today, 0, 0, 0, len(current))
        return send("Listings baseline",
                    f"\U0001F4CA Baseline set: {len(current)} active listings tracked.", 0)

    removed = previous - current
    sold = db.get_finnkodes_with_status(removed, "Solgt")
    metrics = compute_daily_metrics(current, previous, sold)
    ok = send("Daily listings", format_daily_message(metrics), 0)
    db.record_daily_metrics(today, metrics.added, metrics.removed_sold,
                            metrics.removed_delisted, metrics.total_active)
    db.replace_active_snapshot(current)
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m unittest tests.test_daily_summary -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add main/notify/daily_summary.py tests/test_daily_summary.py
git commit -m "notify: add daily summary orchestrator with baseline handling" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Weekly summary orchestrator

**Files:**
- Create: `main/notify/weekly_summary.py`
- Test: `tests/test_weekly_summary.py`

**Interfaces:**
- Consumes: Task 1 `sum_daily_metrics_between`, `count_sold_between`; `pushover.send`.
- Produces: `format_weekly_message(added: int, sold: int) -> str`; `run(db_path=None, today=None, send=pushover.send) -> bool`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_weekly_summary.py`:

```python
import os, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.database.db import PropertyDatabase
from main.notify import weekly_summary


class WeeklySummaryTests(unittest.TestCase):
    def test_format_message(self):
        self.assertEqual(weekly_summary.format_weekly_message(48, 19),
                         "\U0001F4C5 This week: +48 added, 19 sold.")

    def test_run_aggregates_added_over_window_and_counts_sold(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "t.db")
        db = PropertyDatabase(db_path)
        # Window Sunday 2026-07-12 covers 2026-07-06..2026-07-12
        db.record_daily_metrics("2026-07-06", 5, 0, 0, 100)
        db.record_daily_metrics("2026-07-10", 7, 0, 0, 101)
        db.record_daily_metrics("2026-06-30", 99, 0, 0, 90)  # outside window
        db.record_status_change_if_changed("1", "", "Solgt")  # observed today (within window if run today)

        sent = []
        weekly_summary.run(db_path=db_path, today="2026-07-12",
                           send=lambda t, m, p=0: sent.append((t, m, p)) or True)
        self.assertEqual(len(sent), 1)
        self.assertIn("+12 added", sent[0][1])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m unittest tests.test_weekly_summary -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main.notify.weekly_summary'`.

- [ ] **Step 3: Implement**

Create `main/notify/weekly_summary.py`:

```python
"""Weekly Sunday summary: added over the past 7 days + sold transitions."""
from datetime import date, timedelta

from main.database.db import PropertyDatabase
from main.notify import pushover


def format_weekly_message(added: int, sold: int) -> str:
    return f"\U0001F4C5 This week: +{added} added, {sold} sold."


def run(db_path=None, today=None, send=pushover.send) -> bool:
    db = PropertyDatabase(db_path)
    end = date.fromisoformat(today) if today else date.today()
    start = end - timedelta(days=6)  # inclusive 7-day window
    metrics = db.sum_daily_metrics_between(start.isoformat(), end.isoformat())
    sold = db.count_sold_between(start.isoformat(), end.isoformat())
    return send("Weekly summary", format_weekly_message(metrics["added"], sold), 0)


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m unittest tests.test_weekly_summary -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add main/notify/weekly_summary.py tests/test_weekly_summary.py
git commit -m "notify: add weekly summary orchestrator" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Battery monitor

**Files:**
- Create: `main/notify/battery.py`
- Test: `tests/test_battery.py`

**Interfaces:**
- Consumes: `pushover.send`.
- Produces:
  - `BatteryState = namedtuple("BatteryState", ["percent", "on_ac", "status"])`
  - `Alert = namedtuple("Alert", ["title", "message", "priority"])`
  - `read_battery(power_supply_dir="/sys/class/power_supply") -> BatteryState`
  - `decide_alerts(current: BatteryState, prev: dict, thresholds=(50, 20, 10)) -> (list[Alert], dict)`
  - `run(state_path, power_supply_dir="/sys/class/power_supply", send=pushover.send) -> bool`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_battery.py`:

```python
import os, sys, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify import battery
from main.notify.battery import BatteryState, decide_alerts


class DecideAlertsTests(unittest.TestCase):
    def test_first_observation_seeds_state_without_alert(self):
        alerts, state = decide_alerts(BatteryState(95, True, "Charging"), {})
        self.assertEqual(alerts, [])
        self.assertEqual(state, {"power": "ac", "last_threshold_alerted": None})

    def test_unplug_sends_power_lost_high_priority(self):
        prev = {"power": "ac", "last_threshold_alerted": None}
        alerts, state = decide_alerts(BatteryState(80, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].title, "Power lost")
        self.assertEqual(alerts[0].priority, 1)
        self.assertEqual(state["power"], "battery")

    def test_threshold_50_fires_once_then_suppressed(self):
        prev = {"power": "battery", "last_threshold_alerted": None}
        alerts, state = decide_alerts(BatteryState(50, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertIn("50%", alerts[0].message)
        self.assertEqual(state["last_threshold_alerted"], 50)
        # Same level again -> no repeat
        alerts2, _ = decide_alerts(BatteryState(48, False, "Discharging"), state)
        self.assertEqual(alerts2, [])

    def test_ten_percent_is_high_priority(self):
        prev = {"power": "battery", "last_threshold_alerted": 20}
        alerts, _ = decide_alerts(BatteryState(9, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].priority, 1)

    def test_unplug_at_15_does_not_refire_50_or_20_but_fires_10_later(self):
        prev = {"power": "ac", "last_threshold_alerted": None}
        alerts, state = decide_alerts(BatteryState(15, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)               # only power-lost
        self.assertEqual(alerts[0].title, "Power lost")
        alerts2, state2 = decide_alerts(BatteryState(9, False, "Discharging"), state)
        self.assertEqual(len(alerts2), 1)
        self.assertIn("9%", alerts2[0].message)

    def test_restore_sends_power_restored_and_resets(self):
        prev = {"power": "battery", "last_threshold_alerted": 20}
        alerts, state = decide_alerts(BatteryState(30, True, "Charging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].title, "Power restored")
        self.assertEqual(state, {"power": "ac", "last_threshold_alerted": None})


class ReadBatteryTests(unittest.TestCase):
    def _make(self, tmp, bat_pct, bat_status, ac_online):
        os.makedirs(os.path.join(tmp, "BAT0"))
        os.makedirs(os.path.join(tmp, "AC"))
        with open(os.path.join(tmp, "BAT0", "type"), "w") as f: f.write("Battery\n")
        with open(os.path.join(tmp, "BAT0", "capacity"), "w") as f: f.write(f"{bat_pct}\n")
        with open(os.path.join(tmp, "BAT0", "status"), "w") as f: f.write(f"{bat_status}\n")
        with open(os.path.join(tmp, "AC", "type"), "w") as f: f.write("Mains\n")
        with open(os.path.join(tmp, "AC", "online"), "w") as f: f.write(f"{ac_online}\n")

    def test_reads_percent_and_ac(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            self._make(tmp, 42, "Discharging", 0)
            st = battery.read_battery(tmp)
            self.assertEqual(st.percent, 42)
            self.assertFalse(st.on_ac)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m unittest tests.test_battery -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main.notify.battery'`.

- [ ] **Step 3: Implement**

Create `main/notify/battery.py`:

```python
"""Battery/power monitor with an anti-spam alert state machine."""
import json
import os
from collections import namedtuple

from main.notify import pushover

BatteryState = namedtuple("BatteryState", ["percent", "on_ac", "status"])
Alert = namedtuple("Alert", ["title", "message", "priority"])


def read_battery(power_supply_dir="/sys/class/power_supply") -> BatteryState:
    percent, status, on_ac = None, "Unknown", None
    try:
        names = sorted(os.listdir(power_supply_dir))
    except OSError:
        return BatteryState(percent=None, on_ac=True, status="Unknown")
    for name in names:
        path = os.path.join(power_supply_dir, name)
        try:
            with open(os.path.join(path, "type")) as f:
                dev_type = f.read().strip()
        except OSError:
            continue
        if dev_type == "Battery":
            try:
                with open(os.path.join(path, "capacity")) as f:
                    percent = int(f.read().strip())
                with open(os.path.join(path, "status")) as f:
                    status = f.read().strip()
            except OSError:
                continue
        elif dev_type == "Mains":
            try:
                with open(os.path.join(path, "online")) as f:
                    on_ac = f.read().strip() == "1"
            except OSError:
                pass
    if on_ac is None:
        on_ac = status != "Discharging"
    return BatteryState(percent=percent, on_ac=on_ac, status=status)


def _seed_ceiling(pct, thresholds):
    at_or_above = [t for t in thresholds if t >= pct]
    return min(at_or_above) if at_or_above else None


def decide_alerts(current: BatteryState, prev: dict, thresholds=(50, 20, 10)):
    thresholds = sorted(thresholds, reverse=True)
    pct = current.percent if current.percent is not None else 100
    new_power = "ac" if current.on_ac else "battery"
    prev_power = prev.get("power")
    last = prev.get("last_threshold_alerted")
    alerts = []

    if prev_power is None:
        last = _seed_ceiling(pct, thresholds) if new_power == "battery" else None
        return alerts, {"power": new_power, "last_threshold_alerted": last}

    if prev_power == "ac" and new_power == "battery":
        alerts.append(Alert("Power lost", f"⚡ Power lost - on battery ({pct}%)", 1))
        last = _seed_ceiling(pct, thresholds)
    elif prev_power == "battery" and new_power == "ac":
        alerts.append(Alert("Power restored", f"\U0001F50C Power restored ({pct}%)", 0))
        last = None
    elif new_power == "battery":
        ceiling = last if last is not None else 101
        for t in thresholds:
            if pct <= t < ceiling:
                alerts.append(Alert("Battery low", f"\U0001F50B Battery low: {pct}%", 1 if t <= 10 else 0))
                last = t
                ceiling = t
    return alerts, {"power": new_power, "last_threshold_alerted": last}


def _load_state(state_path):
    try:
        with open(state_path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_state(state_path, state):
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    with open(state_path, "w") as f:
        json.dump(state, f)


def run(state_path, power_supply_dir="/sys/class/power_supply", send=pushover.send) -> bool:
    current = read_battery(power_supply_dir)
    prev = _load_state(state_path)
    alerts, new_state = decide_alerts(current, prev)
    for a in alerts:
        send(a.title, a.message, a.priority)
    _save_state(state_path, new_state)
    return True


if __name__ == "__main__":
    import sys
    default_state = os.path.expanduser("~/skannonser-notify-state/battery.json")
    sys.exit(0 if run(default_state) else 1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m unittest tests.test_battery -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add main/notify/battery.py tests/test_battery.py
git commit -m "notify: add battery/power monitor with anti-spam state machine" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: Wiring — secrets, Makefile, cron, heartbeat, deploy

**Files:**
- Modify: `.gitignore`
- Modify: `Makefile`
- Create (on box only, gitignored): `main/config/notify_secrets.py`
- Modify (on box): `~/run_skannonser_daily.sh` is NOT touched here — these are separate schedules.

**Interfaces:** none (integration/deploy).

- [ ] **Step 1: Gitignore the secrets file**

Add to `.gitignore` (new line):

```
main/config/notify_secrets.py
```

- [ ] **Step 2: Add Makefile targets**

In `Makefile`, add to the `.PHONY` list: `notify-daily notify-weekly notify-battery`. Then add these targets (near `sold-sync`):

```make
notify-daily:
	@$(PYTHON) -m main.notify.daily_summary

notify-weekly:
	@$(PYTHON) -m main.notify.weekly_summary

notify-battery:
	@$(PYTHON) -m main.notify.battery
```

- [ ] **Step 3: Run the full test suite**

Run: `.venv/bin/python -m unittest discover -s tests -v`
Expected: PASS (all notify tests + the pre-existing 11).

- [ ] **Step 4: Commit the repo changes**

```bash
git add .gitignore Makefile
git commit -m "notify: gitignore secrets and add make targets" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git push origin master
```

- [ ] **Step 5: Deploy to the box and verify battery reads there**

```bash
ssh mbp 'cd ~/kode/skannonser && git pull --ff-only && ls /sys/class/power_supply/'
```
Expected: a `BAT0` (or `BAT1`) and an AC/`ADP1` entry appear. If the battery entry differs, note the name — `read_battery` auto-detects by `type` so no code change is needed.

- [ ] **Step 6: Create the secrets file on the box (owner supplies values)**

On the box, create `main/config/notify_secrets.py` (gitignored):

```python
PUSHOVER_APP_TOKEN = "<app token from pushover.net/apps/build>"
PUSHOVER_USER_KEY = "<user key from pushover dashboard>"
HEALTHCHECKS_URL = "<ping URL from healthchecks.io>"
```

- [ ] **Step 7: Smoke-test a real notification**

```bash
ssh mbp 'cd ~/kode/skannonser && .venv/bin/python -c "from main.notify import pushover; print(pushover.send(\"Test\", \"Hello from mbp\", 0))"'
```
Expected: prints `True` and the phone receives a "Test" push.

- [ ] **Step 8: Install cron entries on the box**

Add to the box crontab (`crontab -e`), keeping the existing daily pipeline line:

```
0 7 * * *  cd /home/mbp2016/kode/skannonser && /home/mbp2016/kode/skannonser/.venv/bin/python -m main.notify.daily_summary  >> /home/mbp2016/skannonser-logs/notify.log 2>&1
0 8 * * 0  cd /home/mbp2016/kode/skannonser && /home/mbp2016/kode/skannonser/.venv/bin/python -m main.notify.weekly_summary >> /home/mbp2016/skannonser-logs/notify.log 2>&1
*/10 * * * * cd /home/mbp2016/kode/skannonser && /home/mbp2016/kode/skannonser/.venv/bin/python -m main.notify.battery >> /home/mbp2016/skannonser-logs/notify.log 2>&1
*/10 * * * * curl -fsS -m 10 --retry 3 "$(/home/mbp2016/kode/skannonser/.venv/bin/python -c 'from main.notify import config; print(config.HEALTHCHECKS_URL)')" >/dev/null 2>&1
```

- [ ] **Step 9: Verify cron + heartbeat**

```bash
ssh mbp 'crontab -l | grep notify'
```
Expected: the four lines above. Confirm on healthchecks.io that the check flips to "up" within ~10 min.

---

## Self-Review

**Spec coverage:**
- Offline detection → Task 7 heartbeat cron + Healthchecks (owns alerting). ✓
- Daily added/removed (sold vs delisted, tracked set) → Task 1 (queries) + Task 3 (compute) + Task 4 (orchestrate). ✓
- Weekly added/sold (Sunday) → Task 5 + Task 7 cron `0 8 * * 0`. ✓
- Battery 50/20/10 + unplug/restore + anti-spam → Task 6. ✓
- Pushover sender + secrets → Task 2 + Task 7. ✓
- Additive tables → Task 1. ✓
- TDD, temp sqlite, mocked HTTP → every task. ✓

**Placeholder scan:** Only intentional owner-supplied `<...>` values in the secrets file (Task 7 Step 6) — these are runtime secrets, not plan placeholders. No code placeholders.

**Type consistency:** `send(title, message, priority)` signature is consistent across `pushover.send`, the `send=` injection points in daily/weekly/battery, and the fake sends in tests. `BatteryState(percent, on_ac, status)` and `decide_alerts -> (list, dict)` consistent across battery module and tests. DB method names match between Task 1 definitions and their consumers in Tasks 4/5.
