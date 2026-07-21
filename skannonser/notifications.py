"""Notify: daily/weekly listing summaries pushed via the ``notify`` CLI.

Ports (byte-identical message text and baseline-handling semantics -- the
user's PHONE is the consumer of these messages):

  * ``main/notify/listing_metrics.py`` -- pure added/removed-sold/
    removed-delisted/total-active diff, adapted from a ``DailyMetrics``
    dataclass to a plain ``dict`` per this module's interface contract.
  * ``main/notify/daily_summary.py`` -- baseline-on-first-run handling,
    the daily message format, and the record-then-replace-snapshot order.
  * ``main/notify/weekly_summary.py`` -- 7-day trailing window aggregation
    and the weekly message format.
  * ``main/notify/send.py`` -- the exact ``notify`` binary CLI invocation.

Adapted from ``PropertyDatabase``/``db_path`` to a plain ``sqlite3.Connection``
+ ``ListingsRepo``, matching the rest of this rebuild's repository pattern.
Two legacy DB accessors used by ``daily_summary.run`` fall OUTSIDE the
db.py:729-786 range this task's ``ListingsRepo`` additions cover, so they are
reproduced here as private module-level helpers instead of repo methods:

  * ``get_active_tracked_finnkodes`` (db.py:690-706) -> ``_active_tracked_finnkodes``:
    active rows passing the domain's ``sheets_max_price``/``min_bra_i``
    filters (the same filters ``skannonser.publish.export``/
    ``skannonser.ingest.finn.refresh`` already apply, sourced from
    ``load_domain().filters`` rather than legacy's ``main.config.filters``
    import-with-fallback -- those two ints are always present in
    ``config/domain.toml``, so there is no ``None`` case to handle here).
  * ``get_finnkodes_with_status`` (db.py:708-727) -> ``_finnkodes_with_status``:
    chunked (500-at-a-time) ``IN (...)`` status lookup.
"""
from __future__ import annotations

import subprocess
from datetime import date, timedelta
from typing import Callable

from skannonser.config.domain import load_domain
from skannonser.config.settings import get_secrets
from skannonser.store.repositories.listings import ListingsRepo

Send = Callable[[str, str, int], bool]


# ---------------------------------------------------------------------------
# Pure diff logic (main/notify/listing_metrics.py)
# ---------------------------------------------------------------------------


def compute_daily_metrics(previous: set, current: set, sold_finnkodes: set) -> dict:
    """Port of ``main/notify/listing_metrics.py:compute_daily_metrics``,
    EXACTLY -- the set arithmetic (added = current - previous; removed =
    previous - current; split by membership in ``sold_finnkodes``) is
    unchanged. Only the parameter order is renamed to (previous, current,
    sold_finnkodes) per this module's interface contract, and the return
    value is a plain ``dict`` instead of a ``DailyMetrics`` dataclass.
    """
    added = set(current) - set(previous)
    removed = set(previous) - set(current)
    removed_sold = removed & set(sold_finnkodes)
    removed_delisted = removed - set(sold_finnkodes)
    return {
        "added": len(added),
        "removed_sold": len(removed_sold),
        "removed_delisted": len(removed_delisted),
        "total_active": len(current),
        "added_finnkodes": added,
        "removed_finnkodes": removed,
    }


def format_daily_message(m: dict) -> str:
    """Port of ``main/notify/listing_metrics.py:format_daily_message``,
    byte-identical output."""
    removed_total = m["removed_sold"] + m["removed_delisted"]
    return (
        f"\U0001F3E0 Today: +{m['added']} added, -{removed_total} removed "
        f"({m['removed_sold']} sold, {m['removed_delisted']} delisted). "
        f"Active: {m['total_active']}."
    )


def format_weekly_message(added: int, sold: int) -> str:
    """Port of ``main/notify/weekly_summary.py:format_weekly_message``,
    byte-identical output."""
    return f"\U0001F4C5 This week: +{added} added, {sold} sold."


# ---------------------------------------------------------------------------
# DB accessors outside the ported ListingsRepo range (db.py:690-727)
# ---------------------------------------------------------------------------


def _active_tracked_finnkodes(conn) -> set[str]:
    """Port of ``get_active_tracked_finnkodes`` (db.py:690-706): active
    listings passing the domain's sheet price/area filters (the tracked
    set)."""
    filters = load_domain().filters
    rows = conn.execute(
        "SELECT finnkode FROM eiendom WHERE active = 1 "
        "AND pris <= ? AND CAST(info_usable_i_area AS REAL) >= ?",
        (filters.sheets_max_price, filters.min_bra_i),
    ).fetchall()
    return {str(r["finnkode"]).strip() for r in rows}


def _finnkodes_with_status(conn, finnkodes, status: str) -> set[str]:
    """Port of ``get_finnkodes_with_status`` (db.py:708-727): subset of
    ``finnkodes`` whose CURRENT ``tilgjengelighet`` equals ``status``,
    chunked at 500 to stay under SQLite's variable-count limit."""
    fk = [str(f).strip() for f in finnkodes]
    if not fk:
        return set()
    result: set[str] = set()
    for i in range(0, len(fk), 500):
        chunk = fk[i : i + 500]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT finnkode FROM eiendom WHERE tilgjengelighet = ? "
            f"AND finnkode IN ({placeholders})",
            [status, *chunk],
        ).fetchall()
        result.update(str(r["finnkode"]).strip() for r in rows)
    return result


# ---------------------------------------------------------------------------
# Send (main/notify/send.py)
# ---------------------------------------------------------------------------


def default_send(title: str, message: str, priority: int = 0) -> bool:
    """Port of ``main/notify/send.py:send``, EXACTLY -- same ``notify`` CLI
    invocation (``<binary> send <title> <message> --priority <priority>``,
    15s timeout), same best-effort behavior (any exception -> ``False``,
    never raises). Uses ``get_secrets().notify_bin`` (env-var-backed via
    ``NOTIFY_BIN``, default ``"notify"``) in place of legacy's direct
    ``os.environ.get('NOTIFY_BIN', 'notify')`` read -- same effective value,
    routed through this rebuild's config system.
    """
    binary = get_secrets().notify_bin
    try:
        return (
            subprocess.run(
                [binary, "send", title, message, "--priority", str(priority)],
                timeout=15,
            ).returncode
            == 0
        )
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Orchestrators (main/notify/daily_summary.py, weekly_summary.py)
# ---------------------------------------------------------------------------


def daily_summary(conn, send: Send = default_send, today: str | None = None) -> dict:
    """Port of ``main/notify/daily_summary.py:run``, EXACTLY -- baseline
    handling on first run (no previous snapshot: set the baseline, record
    zeroed metrics, send the baseline message, and do NOT report a
    misleading full diff), then the normal diff/notify/persist path,
    including the record-then-replace-snapshot order.

    Returns a dict instead of legacy's bare ``bool`` -- ``{"sent": ok}`` plus
    ``{"baseline": True, "total_active": N}`` on a baseline run, or
    ``{"baseline": False, "added", "removed_sold", "removed_delisted",
    "total_active"}`` on a normal run.
    """
    repo = ListingsRepo(conn)
    today = today or date.today().isoformat()
    previous = repo.previous_active_snapshot()
    current = _active_tracked_finnkodes(conn)

    if not previous:
        repo.replace_active_snapshot(current)
        repo.record_daily_metrics(today, 0, 0, 0, len(current))
        ok = send(
            "Listings baseline",
            f"\U0001F4CA Baseline set: {len(current)} active listings tracked.",
            0,
        )
        return {"baseline": True, "total_active": len(current), "sent": ok}

    removed = previous - current
    sold = _finnkodes_with_status(conn, removed, "Solgt")
    metrics = compute_daily_metrics(previous, current, sold)
    ok = send("Daily listings", format_daily_message(metrics), 0)
    repo.record_daily_metrics(
        today,
        metrics["added"],
        metrics["removed_sold"],
        metrics["removed_delisted"],
        metrics["total_active"],
    )
    repo.replace_active_snapshot(current)
    return {
        "baseline": False,
        "added": metrics["added"],
        "removed_sold": metrics["removed_sold"],
        "removed_delisted": metrics["removed_delisted"],
        "total_active": metrics["total_active"],
        "sent": ok,
    }


def weekly_summary(conn, send: Send = default_send, today: str | None = None) -> dict:
    """Port of ``main/notify/weekly_summary.py:run``, EXACTLY -- inclusive
    7-day trailing window (``end - 6 days`` .. ``end``), added summed from
    ``daily_metrics`` + sold counted from ``eiendom_status_history``.

    Returns a dict instead of legacy's bare ``bool``:
    ``{"added", "removed_sold", "removed_delisted", "sold", "sent"}``.
    """
    repo = ListingsRepo(conn)
    end = date.fromisoformat(today) if today else date.today()
    start = end - timedelta(days=6)  # inclusive 7-day window
    metrics = repo.sum_daily_metrics_between(start.isoformat(), end.isoformat())
    sold = repo.count_sold_between(start.isoformat(), end.isoformat())
    ok = send("Weekly summary", format_weekly_message(metrics["added"], sold), 0)
    return {
        "added": metrics["added"],
        "removed_sold": metrics["removed_sold"],
        "removed_delisted": metrics["removed_delisted"],
        "sold": sold,
        "sent": ok,
    }
