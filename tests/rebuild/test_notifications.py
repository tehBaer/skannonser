"""Tests for `skannonser/notifications.py` and the `skannonser notify`
CLI (`skannonser/commands/notify_cmd.py`).

Mirrors every case in legacy's `tests/test_listing_metrics.py`,
`tests/test_daily_summary.py`, and `tests/test_weekly_summary.py`
(message text pinned as literal expected strings -- the user's PHONE reads
these). No subprocess is ever invoked in a test: `default_send` is only
exercised with `subprocess.run` monkeypatched, and every summary/CLI test
injects a fake `send`. No `api_usage` row is ever written by anything this
module touches.
"""
import pytest
from typer.testing import CliRunner

from skannonser.cli import app
from skannonser.commands import notify_cmd
from skannonser.config.settings import Secrets
from skannonser.store import connection, migrations
from skannonser.store.repositories.listings import ListingsRepo
from skannonser import notifications
from skannonser.notifications import (
    compute_daily_metrics,
    daily_summary,
    default_send,
    format_daily_message,
    format_weekly_message,
    weekly_summary,
)


# ---------------------------------------------------------------------------
# Fixtures / seeding helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


@pytest.fixture()
def repo(conn):
    return ListingsRepo(conn)


def _insert(conn, finnkode, active=1, status=None, pris=3_000_000, info_usable_i_area=80):
    """Raw-SQL row insert, matching legacy's `test_daily_summary.py:_insert`
    exactly (same columns, same default price/area so the domain's
    sheets_max_price=7500000/min_bra_i=70 filters trivially pass)."""
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, pris, info_usable_i_area, tilgjengelighet, url) "
        "VALUES (?,?,?,?,?,?)",
        (finnkode, active, pris, info_usable_i_area, status, f"https://finn.no/{finnkode}"),
    )
    conn.commit()


def _fake_send(sent: list):
    def _send(title, message, priority=0):
        sent.append((title, message, priority))
        return True
    return _send


# ---------------------------------------------------------------------------
# compute_daily_metrics / format_daily_message
# (mirrors tests/test_listing_metrics.py exactly, adapted to the new
# (previous, current, sold_finnkodes) parameter order and dict return.)
# ---------------------------------------------------------------------------


def test_added_and_removed_split_sold_vs_delisted():
    previous = {"a", "b", "c", "d"}
    current = {"c", "d", "e"}  # added e; removed a, b
    sold_finnkodes = {"a"}  # a was sold; b delisted
    m = compute_daily_metrics(previous, current, sold_finnkodes)
    assert m["added"] == 1
    assert m["removed_sold"] == 1
    assert m["removed_delisted"] == 1
    assert m["total_active"] == 3
    assert m["added_finnkodes"] == {"e"}
    assert m["removed_finnkodes"] == {"a", "b"}


def test_no_change():
    m = compute_daily_metrics({"a", "b"}, {"a", "b"}, set())
    assert (m["added"], m["removed_sold"], m["removed_delisted"], m["total_active"]) == (0, 0, 0, 2)


def test_format_daily_message():
    m = compute_daily_metrics({"a", "b", "c", "d"}, {"c", "d", "e"}, {"a"})
    assert (
        format_daily_message(m)
        == "\U0001F3E0 Today: +1 added, -2 removed (1 sold, 1 delisted). Active: 3."
    )


def test_format_weekly_message():
    assert format_weekly_message(48, 19) == "\U0001F4C5 This week: +48 added, 19 sold."


# ---------------------------------------------------------------------------
# ListingsRepo notify additions (db.py:729-786 port)
# ---------------------------------------------------------------------------


def test_previous_active_snapshot_empty_by_default(repo):
    assert repo.previous_active_snapshot() == set()


def test_replace_active_snapshot_wholesale_replace(repo):
    repo.replace_active_snapshot({"1", "2"})
    assert repo.previous_active_snapshot() == {"1", "2"}
    repo.replace_active_snapshot({"3"})
    assert repo.previous_active_snapshot() == {"3"}
    repo.replace_active_snapshot(set())
    assert repo.previous_active_snapshot() == set()


def test_record_daily_metrics_is_upsert_by_date(repo):
    repo.record_daily_metrics("2026-07-10", 1, 2, 3, 10)
    assert repo.sum_daily_metrics_between("2026-07-10", "2026-07-10") == {
        "added": 1,
        "removed_sold": 2,
        "removed_delisted": 3,
    }
    row = repo.conn.execute("SELECT COUNT(*) AS c FROM daily_metrics").fetchone()
    assert row["c"] == 1

    # Re-running for the same date overwrites, not duplicates (INSERT OR REPLACE).
    repo.record_daily_metrics("2026-07-10", 9, 9, 9, 99)
    row = repo.conn.execute("SELECT COUNT(*) AS c FROM daily_metrics").fetchone()
    assert row["c"] == 1
    assert repo.sum_daily_metrics_between("2026-07-10", "2026-07-10") == {
        "added": 9,
        "removed_sold": 9,
        "removed_delisted": 9,
    }


def test_sum_daily_metrics_between_missing_rows_sum_to_zero(repo):
    assert repo.sum_daily_metrics_between("2026-01-01", "2026-01-31") == {
        "added": 0,
        "removed_sold": 0,
        "removed_delisted": 0,
    }


def test_sum_daily_metrics_between_range_boundaries(repo):
    repo.record_daily_metrics("2026-06-30", 99, 0, 0, 90)  # outside
    repo.record_daily_metrics("2026-07-06", 5, 0, 0, 100)  # start (inclusive)
    repo.record_daily_metrics("2026-07-10", 7, 0, 0, 101)  # inside
    repo.record_daily_metrics("2026-07-12", 3, 0, 0, 102)  # end (inclusive)
    repo.record_daily_metrics("2026-07-13", 88, 0, 0, 103)  # outside
    assert repo.sum_daily_metrics_between("2026-07-06", "2026-07-12") == {
        "added": 15,
        "removed_sold": 0,
        "removed_delisted": 0,
    }


def test_count_sold_between(repo):
    conn = repo.conn
    conn.execute(
        "INSERT INTO eiendom_status_history (finnkode, old_status, new_status, observed_at) "
        "VALUES ('1', 'Aktiv', 'Solgt', '2026-07-06 10:00:00')"
    )
    conn.execute(
        "INSERT INTO eiendom_status_history (finnkode, old_status, new_status, observed_at) "
        "VALUES ('2', 'Aktiv', 'Solgt', '2026-07-12 23:59:00')"
    )
    conn.execute(
        "INSERT INTO eiendom_status_history (finnkode, old_status, new_status, observed_at) "
        "VALUES ('3', 'Aktiv', 'Solgt', '2026-07-13 00:00:01')"  # outside range
    )
    conn.execute(
        "INSERT INTO eiendom_status_history (finnkode, old_status, new_status, observed_at) "
        "VALUES ('4', 'Aktiv', 'Inaktiv', '2026-07-06 10:00:00')"  # not Solgt
    )
    conn.commit()
    assert repo.count_sold_between("2026-07-06", "2026-07-12") == 2


# ---------------------------------------------------------------------------
# _active_tracked_finnkodes / _finnkodes_with_status (db.py:690-727 ports,
# isolated -- mirrors legacy tests/test_notify_db.py's
# test_active_tracked_respects_filters / test_finnkodes_with_status)
# ---------------------------------------------------------------------------


def test_active_tracked_finnkodes_respects_price_and_area_filters(conn):
    _insert(conn, "1")  # passes both sheets_max_price/min_bra_i filters
    _insert(conn, "2", pris=99_000_000)  # over sheets_max_price -> excluded
    assert notifications._active_tracked_finnkodes(conn) == {"1"}


def test_finnkodes_with_status_returns_only_requested_status_subset(conn):
    _insert(conn, "1", active=0, status="Solgt")
    _insert(conn, "2", active=0, status="Inaktiv")
    _insert(conn, "3", active=0, status="Solgt")
    assert notifications._finnkodes_with_status(conn, {"1", "2", "3"}, "Solgt") == {"1", "3"}


# ---------------------------------------------------------------------------
# daily_summary (mirrors tests/test_daily_summary.py exactly)
# ---------------------------------------------------------------------------


def test_first_run_sets_baseline_without_diff(conn, repo):
    _insert(conn, "1")
    _insert(conn, "2")
    sent: list = []
    result = daily_summary(conn, send=_fake_send(sent), today="2026-07-10")
    assert result["sent"] is True
    assert result["baseline"] is True
    assert result["total_active"] == 2
    assert len(sent) == 1
    assert sent[0][0] == "Listings baseline"
    assert sent[0][1] == "\U0001F4CA Baseline set: 2 active listings tracked."
    assert repo.previous_active_snapshot() == {"1", "2"}


def test_second_run_reports_added_and_removed(conn, repo):
    repo.replace_active_snapshot({"1", "2"})
    _insert(conn, "2")
    _insert(conn, "3")
    _insert(conn, "1", active=0, status="Solgt")  # removed, sold
    sent: list = []
    result = daily_summary(conn, send=_fake_send(sent), today="2026-07-11")
    assert result["sent"] is True
    assert result["baseline"] is False
    assert len(sent) == 1
    assert sent[0][1] == (
        "\U0001F3E0 Today: +1 added, -1 removed (1 sold, 0 delisted). Active: 2."
    )
    assert repo.previous_active_snapshot() == {"2", "3"}
    assert repo.sum_daily_metrics_between("2026-07-11", "2026-07-11") == {
        "added": 1,
        "removed_sold": 1,
        "removed_delisted": 0,
    }


def test_second_run_removed_delisted_when_not_solgt(conn, repo):
    """Extra case beyond legacy's own tests: a removed listing whose current
    status is NOT 'Solgt' counts as delisted, not sold."""
    repo.replace_active_snapshot({"1", "2"})
    _insert(conn, "2")
    _insert(conn, "1", active=0, status="Inaktiv")
    sent: list = []
    result = daily_summary(conn, send=_fake_send(sent), today="2026-07-11")
    assert result == {
        "baseline": False,
        "added": 0,
        "removed_sold": 0,
        "removed_delisted": 1,
        "total_active": 1,
        "sent": True,
    }


def test_daily_summary_excludes_listings_outside_price_area_filters(conn, repo):
    """Active-tracked set (`_active_tracked_finnkodes`) applies the domain's
    sheets_max_price/min_bra_i filters, same as legacy's
    get_active_tracked_finnkodes -- a listing failing either filter must not
    count toward the tracked/active total."""
    _insert(conn, "1", pris=99_000_000)  # over sheets_max_price
    _insert(conn, "2", info_usable_i_area=10)  # under min_bra_i
    _insert(conn, "3")  # passes both
    sent: list = []
    result = daily_summary(conn, send=_fake_send(sent), today="2026-07-10")
    assert result["total_active"] == 1
    assert repo.previous_active_snapshot() == {"3"}


def test_daily_summary_default_send_used_when_not_injected(conn, monkeypatch):
    """`daily_summary`'s default `send` parameter is `default_send` -- verify
    it's actually wired (via a monkeypatched `subprocess.run`, never a real
    call)."""
    calls = []

    class FakeCompleted:
        returncode = 0

    def fake_run(args, timeout=None):
        calls.append(args)
        return FakeCompleted()

    monkeypatch.setattr(notifications.subprocess, "run", fake_run)
    _insert(conn, "1")
    result = daily_summary(conn, today="2026-07-10")
    assert result["sent"] is True
    assert len(calls) == 1
    assert calls[0][1] == "send"
    assert calls[0][2] == "Listings baseline"


# ---------------------------------------------------------------------------
# weekly_summary (mirrors tests/test_weekly_summary.py exactly)
# ---------------------------------------------------------------------------


def test_run_aggregates_added_over_window_and_counts_sold(conn, repo):
    # Window ending Sunday 2026-07-12 covers 2026-07-06..2026-07-12
    repo.record_daily_metrics("2026-07-06", 5, 0, 0, 100)
    repo.record_daily_metrics("2026-07-10", 7, 0, 0, 101)
    repo.record_daily_metrics("2026-06-30", 99, 0, 0, 90)  # outside window

    sent: list = []
    result = weekly_summary(conn, send=_fake_send(sent), today="2026-07-12")
    assert len(sent) == 1
    assert "+12 added" in sent[0][1]
    assert result["added"] == 12
    assert result["sold"] == 0
    assert result["sent"] is True


def test_weekly_summary_message_includes_sold_count(conn, repo):
    repo.record_daily_metrics("2026-07-06", 3, 0, 0, 50)
    repo.conn.execute(
        "INSERT INTO eiendom_status_history (finnkode, old_status, new_status, observed_at) "
        "VALUES ('1', 'Aktiv', 'Solgt', '2026-07-08 10:00:00')"
    )
    repo.conn.commit()
    sent: list = []
    weekly_summary(conn, send=_fake_send(sent), today="2026-07-12")
    assert sent[0][1] == "\U0001F4C5 This week: +3 added, 1 sold."


# ---------------------------------------------------------------------------
# default_send (main/notify/send.py port) -- subprocess mocked, never real.
# ---------------------------------------------------------------------------


def test_default_send_builds_legacy_cli_args(monkeypatch):
    calls = []

    class FakeCompleted:
        returncode = 0

    def fake_run(args, timeout=None):
        calls.append((args, timeout))
        return FakeCompleted()

    monkeypatch.setenv("NOTIFY_BIN", "my-notify")
    notifications.get_secrets.cache_clear()
    monkeypatch.setattr(notifications.subprocess, "run", fake_run)

    ok = default_send("Daily listings", "hello", 5)
    assert ok is True
    assert calls == [(["my-notify", "send", "Daily listings", "hello", "--priority", "5"], 15)]


def test_default_send_uses_notify_default_when_notify_bin_unset(monkeypatch):
    """Pins the DEFAULT binary literal: with NOTIFY_BIN unset entirely (not
    just absent from this test's env -- also drop it from the process env so
    get_secrets() can't pick it up from outside), default_send must invoke
    argv[0] == "notify" (Secrets.notify_bin's class default)."""
    calls = []

    class FakeCompleted:
        returncode = 0

    def fake_run(args, timeout=None):
        calls.append(args)
        return FakeCompleted()

    monkeypatch.delenv("NOTIFY_BIN", raising=False)
    # Monkeypatch get_secrets to return Secrets with _env_file=None to properly
    # isolate the class default (ensures .env repo file doesn't interfere).
    monkeypatch.setattr(notifications, "get_secrets", lambda: Secrets(_env_file=None))
    monkeypatch.setattr(notifications.subprocess, "run", fake_run)

    ok = default_send("t", "m", 0)
    assert ok is True
    assert calls[0][0] == "notify"


def test_default_send_returns_false_on_nonzero_exit(monkeypatch):
    class FakeCompleted:
        returncode = 1

    monkeypatch.setattr(notifications.subprocess, "run", lambda *a, **kw: FakeCompleted())
    assert default_send("t", "m", 0) is False


def test_default_send_never_raises(monkeypatch):
    def raising_run(*a, **kw):
        raise OSError("binary not found")

    monkeypatch.setattr(notifications.subprocess, "run", raising_run)
    assert default_send("t", "m", 0) is False


# ---------------------------------------------------------------------------
# CLI: `skannonser notify daily|weekly`
# ---------------------------------------------------------------------------


def test_cli_notify_daily_missing_db_exits_nonzero(tmp_path):
    missing = tmp_path / "nope.db"
    result = CliRunner().invoke(app, ["notify", "daily", "--db", str(missing)])
    assert result.exit_code != 0


def test_cli_notify_daily_exits_nonzero_when_migrations_pending(tmp_path):
    db = tmp_path / "t.db"
    connection.connect(db).close()  # no migrations applied
    result = CliRunner().invoke(app, ["notify", "daily", "--db", str(db)])
    assert result.exit_code != 0
    assert "pending migrations" in result.output


def test_cli_notify_daily_routes_to_daily_summary(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    conn = connection.connect(db)
    migrations.migrate(conn)
    conn.close()

    calls = []

    def fake_send(title, message, priority=0):
        calls.append((title, message, priority))
        return True

    monkeypatch.setattr(notify_cmd, "default_send", fake_send)
    result = CliRunner().invoke(app, ["notify", "daily", "--db", str(db)])
    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0][0] == "Listings baseline"


def test_cli_notify_daily_exits_nonzero_when_send_fails(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    conn = connection.connect(db)
    migrations.migrate(conn)
    conn.close()

    monkeypatch.setattr(notify_cmd, "default_send", lambda *a, **kw: False)
    result = CliRunner().invoke(app, ["notify", "daily", "--db", str(db)])
    assert result.exit_code != 0


def test_cli_notify_weekly_routes_to_weekly_summary(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    conn = connection.connect(db)
    migrations.migrate(conn)
    ListingsRepo(conn).record_daily_metrics("2026-07-10", 4, 0, 0, 40)
    conn.close()

    calls = []

    def fake_send(title, message, priority=0):
        calls.append((title, message, priority))
        return True

    monkeypatch.setattr(notify_cmd, "default_send", fake_send)
    result = CliRunner().invoke(app, ["notify", "weekly", "--db", str(db)])
    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0][0] == "Weekly summary"


def test_cli_notify_weekly_missing_db_exits_nonzero(tmp_path):
    missing = tmp_path / "nope.db"
    result = CliRunner().invoke(app, ["notify", "weekly", "--db", str(missing)])
    assert result.exit_code != 0


def test_cli_notify_weekly_exits_nonzero_when_migrations_pending(tmp_path):
    db = tmp_path / "t.db"
    connection.connect(db).close()
    result = CliRunner().invoke(app, ["notify", "weekly", "--db", str(db)])
    assert result.exit_code != 0
    assert "pending migrations" in result.output


# ---------------------------------------------------------------------------
# api_usage must stay untouched by anything notify-related.
# ---------------------------------------------------------------------------


def test_no_api_usage_rows_written(conn):
    _insert(conn, "1")
    daily_summary(conn, send=_fake_send([]), today="2026-07-10")
    row = conn.execute("SELECT COUNT(*) AS c FROM api_usage").fetchone()
    assert row["c"] == 0
