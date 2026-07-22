"""Tests for the nightly orchestrator (`skannonser/nightly.py`) -- the
`make full` cron replacement -- and its two CLI commands, `skannonser run
sheets` / `skannonser run nightly` (`skannonser/commands/run_cmd.py`).

No network, no google imports: every pipeline step is a fake monkeypatched
onto `skannonser.nightly`'s module namespace (matching the existing
convention -- e.g. `tests/rebuild/test_dnb_travel.py` monkeypatches
`run_cmd.run_dnb_travel`), and the Sheets side is either a recording fake
client or the `--dry-run-sheets` JSON-file hook. Every DB is a migrated tmp
sqlite file (`connection.connect` + `migrations.migrate`) so `run_sheets`'s
real export builders run against real (if mostly empty) tables.
"""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from skannonser import nightly as nightly_module
from skannonser.cli import app
from skannonser.commands import run_cmd
from skannonser.config.domain import Budget, load_domain
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.nightly import run_nightly, run_sheets
from skannonser.store import connection, migrations

# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------


def make_budget(**overrides) -> Budget:
    defaults = dict(
        routes_monthly_cap=9000,
        geocode_monthly_cap=9000,
        warn_pcts=[50, 80],
        routes_rpm=6000,
        geocode_rpm=6000,
    )
    defaults.update(overrides)
    return Budget(**defaults)


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "nightly.db")
    migrations.migrate(c)
    return c


@pytest.fixture()
def domain():
    return load_domain()


@pytest.fixture()
def gateway(conn):
    return Gateway(conn, make_budget(), notify=lambda m: None, sleeper=lambda s: None)


_INGEST_OK = {"crawled": 1, "parsed": 1, "failed": 0, "upserted": 1, "deactivated": 0}
_ENRICH_OK = {
    "derived": 0,
    "api_calls": 0,
    "donor_skipped": 0,
    "mvv_uni_donor_written": 0,
    "sentinels_written": 0,
    "metadata_refreshed": 0,
    "budget_exhausted": False,
}
_DNB_TRAVEL_OK = {
    "candidates": 0,
    "api_calls": 0,
    "brj_written": 0,
    "mvv_written": 0,
    "sentinels_written": 0,
}
_REFRESH_OK = {"candidates": 0, "refreshed": 0, "status_changed": 0, "errors": 0}
_GEOCODE_OK = {"candidates": 0, "geocoded": 0, "failed": 0}
_THUMBS_OK = {"candidates": 0, "downloaded": 0, "skipped_existing": 0, "failed": 0}


class RecordingClient:
    """Fake `SheetsClient`: records every `rewrite_tab(tab, rows)` call and
    reports a deterministic cell count."""

    def __init__(self, order=None):
        self.calls = []
        self.order = order

    def rewrite_tab(self, tab, rows):
        self.calls.append((tab, len(rows)))
        if self.order is not None:
            self.order.append(f"sheets:{tab}")
        return len(rows)


def _install_happy_fakes(monkeypatch, order):
    """Monkeypatch every nightly.<fn> to append its step name to `order` and
    return a canned OK stats dict -- the "everything succeeds" baseline
    fixture shared by several tests below."""

    def fake_finn(
        domain, conn, project_dir, fetch=None, archive_dir=None,
        page_delay=None, fetch_delay=None,
    ):
        order.append("ingest_finn")
        return dict(_INGEST_OK)

    def fake_dnb(domain, conn, fetch=None):
        order.append("ingest_dnb")
        return dict(_INGEST_OK)

    def fake_geocode(conn, domain, gateway, api_key, get=None):
        order.append("geocode")
        return dict(_GEOCODE_OK)

    def fake_enrich(conn, domain, gateway, api_key, targets="all", post=None):
        order.append(f"enrich_{targets}")
        return dict(_ENRICH_OK)

    def fake_dnb_travel(conn, domain, gateway, api_key, post=None):
        order.append("enrich_dnb")
        return dict(_DNB_TRAVEL_OK)

    def fake_refresh(
        conn, domain, project_dir, mode, fetch=None,
        fetch_delay=None, listing_delay=None,
    ):
        order.append(f"refresh:{mode}")
        return dict(_REFRESH_OK)

    def fake_thumbs(conn, dest_dir, fetch=None, fetch_delay=None, limit=0):
        order.append("thumbs")
        return dict(_THUMBS_OK)

    monkeypatch.setattr(nightly_module, "run_finn_ingest", fake_finn)
    monkeypatch.setattr(nightly_module, "run_dnb_ingest", fake_dnb)
    monkeypatch.setattr(nightly_module, "run_geocode", fake_geocode)
    monkeypatch.setattr(nightly_module, "run_enrich", fake_enrich)
    monkeypatch.setattr(nightly_module, "run_dnb_travel", fake_dnb_travel)
    monkeypatch.setattr(nightly_module, "refresh_listings", fake_refresh)
    monkeypatch.setattr(nightly_module, "cache_thumbnails", fake_thumbs)


# ---------------------------------------------------------------------------
# Full happy-path: step ordering pinned, sheets called last.
# ---------------------------------------------------------------------------


def test_full_nightly_happy_path_step_order_and_sheets_last(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert order[:8] == [
        "ingest_finn",
        "ingest_dnb",
        "geocode",
        "enrich_all",
        "enrich_mvv_uni",
        "enrich_dnb",
        "refresh:stale-open",
        "thumbs",
    ]
    # Sheets writes 4 tabs, all strictly after every pipeline step (including
    # thumbs), in the documented tab order.
    assert order[8:] == ["sheets:Eie", "sheets:Sold", "sheets:DNB", "sheets:Stations"]

    assert result["failed"] == []
    assert result["budget_exhausted"] == []
    for name in (
        "ingest_finn",
        "ingest_dnb",
        "geocode",
        "enrich_all",
        "enrich_mvv_uni",
        "enrich_dnb",
        "refresh",
        "thumbs",
        "sheets",
    ):
        assert result["steps"][name]["ok"] is True, result["steps"][name]
    # RecordingClient.rewrite_tab returns len(rows) of the [header]+data
    # payload it was given -- an empty tab still carries the header row.
    assert result["steps"]["sheets"]["stats"]["Eie"] == {"rows": 0, "cells": 1}


# ---------------------------------------------------------------------------
# Section independence.
# ---------------------------------------------------------------------------


def test_enrich_failure_does_not_skip_refresh_or_sheets(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)

    def failing_enrich(conn, domain, gateway, api_key, targets="all", post=None):
        order.append(f"enrich_{targets}(FAIL)")
        raise RuntimeError("boom")

    monkeypatch.setattr(nightly_module, "run_enrich", failing_enrich)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert "enrich_all" in result["failed"]
    assert "enrich_mvv_uni" in result["failed"]
    assert result["steps"]["enrich_all"] == {"ok": False, "error": "boom"}
    # Later steps still ran (in order), despite both enrich steps failing.
    assert "enrich_dnb" in order
    assert "refresh:stale-open" in order
    assert result["steps"]["refresh"]["ok"] is True
    assert "thumbs" in order
    assert result["steps"]["thumbs"]["ok"] is True
    assert result["steps"]["sheets"]["ok"] is True
    assert any(o.startswith("sheets:") for o in order)


def test_refresh_failure_does_not_skip_thumbs_or_sheets(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)

    def failing_refresh(
        conn, domain, project_dir, mode, fetch=None,
        fetch_delay=None, listing_delay=None,
    ):
        order.append(f"refresh:{mode}(FAIL)")
        raise RuntimeError("refresh boom")

    monkeypatch.setattr(nightly_module, "refresh_listings", failing_refresh)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert "refresh" in result["failed"]
    assert result["steps"]["refresh"] == {"ok": False, "error": "refresh boom"}
    assert "thumbs" in order  # ran anyway
    assert result["steps"]["thumbs"]["ok"] is True
    assert result["steps"]["sheets"]["ok"] is True


def test_thumbs_failure_does_not_skip_sheets(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)

    def failing_thumbs(conn, dest_dir, fetch=None, fetch_delay=None, limit=0):
        order.append("thumbs(FAIL)")
        raise RuntimeError("thumbs boom")

    monkeypatch.setattr(nightly_module, "cache_thumbnails", failing_thumbs)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert "thumbs" in result["failed"]
    assert result["steps"]["thumbs"] == {"ok": False, "error": "thumbs boom"}
    assert result["steps"]["sheets"]["ok"] is True
    assert any(o.startswith("sheets:") for o in order)


def test_thumbs_step_wired_with_thumbs_dir_default_and_override(conn, domain, gateway, monkeypatch):
    """`run_nightly`'s `thumbs_dir` param (default `data/thumbs/`) is
    forwarded verbatim to `cache_thumbnails` -- proves the wiring, not just
    that the step runs."""
    order = []
    _install_happy_fakes(monkeypatch, order)

    seen = {}

    def recording_thumbs(conn, dest_dir, fetch=None, fetch_delay=None, limit=0):
        seen["dest_dir"] = dest_dir
        return dict(_THUMBS_OK)

    monkeypatch.setattr(nightly_module, "cache_thumbnails", recording_thumbs)
    client = RecordingClient(order)

    run_nightly(conn, domain, gateway, "K", client)
    assert seen["dest_dir"] == Path("data/thumbs/")

    custom_dir = Path("some/custom/thumbs")
    run_nightly(conn, domain, gateway, "K", client, thumbs_dir=custom_dir)
    assert seen["dest_dir"] == custom_dir


def test_ingest_finn_failure_does_not_skip_ingest_dnb(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)

    def failing_finn(
        domain, conn, project_dir, fetch=None, archive_dir=None,
        page_delay=None, fetch_delay=None,
    ):
        order.append("ingest_finn(FAIL)")
        raise RuntimeError("network exploded")

    monkeypatch.setattr(nightly_module, "run_finn_ingest", failing_finn)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert result["steps"]["ingest_finn"] == {"ok": False, "error": "network exploded"}
    assert "ingest_finn" in result["failed"]
    assert "ingest_dnb" in order  # ran anyway
    assert result["steps"]["ingest_dnb"]["ok"] is True


def test_ingest_zero_url_crawl_recorded_as_failure_not_exception(conn, domain, gateway, monkeypatch):
    """run_finn_ingest itself already skips mark_inactive on a zero-url
    crawl (pipeline.py guard 1) -- it does NOT raise. Nightly must still
    classify this as a step failure (mirrors run_cmd.py's `_crawled_ok`)."""
    order = []
    _install_happy_fakes(monkeypatch, order)

    def zero_url_finn(
        domain, conn, project_dir, fetch=None, archive_dir=None,
        page_delay=None, fetch_delay=None,
    ):
        order.append("ingest_finn")
        return {"crawled": 0, "parsed": 0, "failed": 0, "upserted": 0, "deactivated": 0}

    monkeypatch.setattr(nightly_module, "run_finn_ingest", zero_url_finn)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert "ingest_finn" in result["failed"]
    assert result["steps"]["ingest_finn"]["ok"] is False
    assert "zero URLs" in result["steps"]["ingest_finn"]["error"]
    assert "ingest_dnb" in order


def test_ingest_high_failure_rate_recorded_as_failure(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)

    def flaky_dnb(domain, conn, fetch=None):
        order.append("ingest_dnb")
        # 3/10 = 30% > 20% threshold.
        return {"crawled": 10, "parsed": 7, "failed": 3, "upserted": 7, "deactivated": 0}

    monkeypatch.setattr(nightly_module, "run_dnb_ingest", flaky_dnb)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert "ingest_dnb" in result["failed"]
    assert "exceeds" in result["steps"]["ingest_dnb"]["error"]
    assert result["steps"]["geocode"]["ok"] is True  # later steps still ran


# ---------------------------------------------------------------------------
# BudgetExceeded normalization: both shapes (raised, and caught-internally
# with a `budget_exhausted` stats key) must be recorded as budget_exhausted,
# never as a failure -- and later steps must still run.
# ---------------------------------------------------------------------------


def test_budget_exceeded_raised_is_not_a_failure_and_later_steps_run(
    conn, domain, gateway, monkeypatch
):
    order = []
    _install_happy_fakes(monkeypatch, order)

    def budget_geocode(conn, domain, gateway, api_key, get=None):
        order.append("geocode(BUDGET)")
        raise BudgetExceeded("geocode", 100, 100)

    monkeypatch.setattr(nightly_module, "run_geocode", budget_geocode)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert result["failed"] == []
    assert result["budget_exhausted"] == ["geocode"]
    assert result["steps"]["geocode"]["ok"] is True
    assert "enrich_all" in order  # ran anyway
    assert result["steps"]["sheets"]["ok"] is True


def test_budget_exhausted_stats_flag_from_run_enrich_is_not_a_failure(
    conn, domain, gateway, monkeypatch
):
    """`run_enrich` catches BudgetExceeded internally and returns a stats
    dict with `budget_exhausted=True` instead of raising -- nightly must
    detect this shape too."""
    order = []
    _install_happy_fakes(monkeypatch, order)

    def budget_enrich(conn, domain, gateway, api_key, targets="all", post=None):
        order.append(f"enrich_{targets}(BUDGET)")
        stats = dict(_ENRICH_OK)
        stats["budget_exhausted"] = True
        return stats

    monkeypatch.setattr(nightly_module, "run_enrich", budget_enrich)
    client = RecordingClient(order)

    result = run_nightly(conn, domain, gateway, "K", client)

    assert result["failed"] == []
    assert set(result["budget_exhausted"]) == {"enrich_all", "enrich_mvv_uni"}
    assert result["steps"]["enrich_all"]["ok"] is True
    assert "enrich_dnb" in order
    assert result["steps"]["sheets"]["ok"] is True


# ---------------------------------------------------------------------------
# --dry-run-sheets hook: run_nightly(sheets_writer=...) writes payloads
# instead of touching the client.
# ---------------------------------------------------------------------------


def test_sheets_writer_used_instead_of_client(conn, domain, gateway, monkeypatch):
    order = []
    _install_happy_fakes(monkeypatch, order)

    class ExplodingClient:
        def rewrite_tab(self, tab, rows):
            raise AssertionError("client must never be called when sheets_writer is given")

    written = []

    def writer(tab, header, rows):
        written.append((tab, header, rows))

    result = run_nightly(
        conn, domain, gateway, "K", ExplodingClient(), sheets_writer=writer
    )

    assert result["steps"]["sheets"]["ok"] is True
    assert [t for t, _h, _r in written] == ["Eie", "Sold", "DNB", "Stations"]


# ---------------------------------------------------------------------------
# run_sheets: per-tab {rows, cells} counts.
# ---------------------------------------------------------------------------


def _ins_eiendom(conn, finnkode, **kw):
    defaults = dict(
        tilgjengelighet="Til salgs",
        active=1,
        adresse="Gata 1",
        postnummer="0581",
        pris=5_000_000,
        url=f"https://www.finn.no/{finnkode}",
        info_usable_i_area=80,
    )
    defaults.update(kw)
    conn.execute(
        """
        INSERT INTO eiendom (
            finnkode, tilgjengelighet, active, adresse, postnummer, pris, url,
            info_usable_i_area
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            finnkode,
            defaults["tilgjengelighet"],
            defaults["active"],
            defaults["adresse"],
            defaults["postnummer"],
            defaults["pris"],
            defaults["url"],
            defaults["info_usable_i_area"],
        ),
    )
    conn.commit()


class FailingOnSecondTabClient:
    """Fake `SheetsClient` that writes the 1st tab fine, then raises on the
    2nd -- proves `_publish` preserves the already-completed tab's stats
    instead of discarding them when a later tab's write blows up."""

    def __init__(self):
        self.calls = []

    def rewrite_tab(self, tab, rows):
        self.calls.append(tab)
        if tab == "Sold":
            raise RuntimeError("sheets API exploded")
        return len(rows)


def test_publish_mid_loop_failure_preserves_completed_tab_stats(conn, domain, gateway, monkeypatch):
    """A client that raises on the 2nd tab (Sold) must not wipe out the 1st
    tab's (Eie) already-written stats -- the sheets step still fails, but
    the report shows exactly what got published before the blowup, plus
    which tabs were never attempted."""
    order = []
    _install_happy_fakes(monkeypatch, order)
    client = FailingOnSecondTabClient()

    result = run_nightly(conn, domain, gateway, "K", client)

    assert "sheets" in result["failed"]
    assert result["steps"]["sheets"]["ok"] is False

    stats = result["steps"]["sheets"]["stats"]
    assert stats["tabs"]["Eie"] == {"rows": 0, "cells": 1}
    assert "Sold" not in stats["tabs"]
    assert "DNB" not in stats["tabs"]
    assert "Stations" not in stats["tabs"]
    assert stats["failed_tab"] == "Sold"
    assert "sheets API exploded" in stats["error"]
    assert stats["unattempted"] == ["DNB", "Stations"]
    assert "sheets API exploded" in result["steps"]["sheets"]["error"]


def test_run_sheets_per_tab_counts(conn):
    _ins_eiendom(conn, "111")
    _ins_eiendom(conn, "222")

    client = RecordingClient()
    result = run_sheets(conn, client)

    assert set(result.keys()) == {"Eie", "Sold", "DNB", "Stations"}
    assert result["Eie"]["rows"] == 2
    assert result["Eie"]["cells"] == 3  # header + 2 data rows
    assert result["Sold"]["rows"] == 0
    assert ("Eie", 3) in client.calls  # header + 2 data rows


# ---------------------------------------------------------------------------
# CLI: `skannonser run sheets`
# ---------------------------------------------------------------------------


def _seeded_db(tmp_path) -> Path:
    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()
    return db


def test_cli_sheets_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    result = CliRunner().invoke(app, ["run", "sheets"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_sheets_exits_nonzero_when_migrations_pending(tmp_path):
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()
    result = CliRunner().invoke(app, ["run", "sheets", "--db", str(db)])
    assert result.exit_code == 1
    assert "pending migrations" in result.output


def test_cli_sheets_missing_spreadsheet_config_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("SPREADSHEET_ID", "")
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "sheets", "--db", str(db)])
    assert result.exit_code == 1
    assert "spreadsheet_id" in result.output


def test_cli_sheets_routes_to_run_sheets(tmp_path, monkeypatch):
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_path))
    db = _seeded_db(tmp_path)
    calls = []

    def fake_run_sheets(conn, client):
        calls.append(client.spreadsheet_id)
        return {"Eie": {"rows": 0, "cells": 0}}

    monkeypatch.setattr(run_cmd, "run_sheets", fake_run_sheets)

    result = CliRunner().invoke(app, ["run", "sheets", "--db", str(db)])
    assert result.exit_code == 0, result.output
    assert calls == ["SHEET1"]


# ---------------------------------------------------------------------------
# CLI: `skannonser run nightly`
# ---------------------------------------------------------------------------


def test_cli_nightly_missing_db_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "nope.db"))
    result = CliRunner().invoke(app, ["run", "nightly"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_nightly_exits_nonzero_when_migrations_pending(tmp_path):
    db = tmp_path / "unmigrated.db"
    connection.connect(db).close()
    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1
    assert "pending migrations" in result.output


def test_cli_nightly_missing_api_key_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "")
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1
    assert "GOOGLE_MAPS_API_KEY not set" in result.output


def test_cli_nightly_missing_spreadsheet_config_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "")
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    db = _seeded_db(tmp_path)
    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1
    assert "spreadsheet_id" in result.output


def test_cli_nightly_real_step_failure_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_path))
    db = _seeded_db(tmp_path)

    def fake_run_nightly(conn, domain, gateway, api_key, client, fetch=None, post=None, sheets_writer=None):
        return {
            "steps": {"ingest_finn": {"ok": False, "error": "boom"}},
            "failed": ["ingest_finn"],
            "budget_exhausted": [],
        }

    monkeypatch.setattr(run_cmd, "run_nightly", fake_run_nightly)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 1, result.output


def test_cli_nightly_budget_exhausted_only_exits_0(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_path))
    db = _seeded_db(tmp_path)

    def fake_run_nightly(conn, domain, gateway, api_key, client, fetch=None, post=None, sheets_writer=None):
        return {
            "steps": {"geocode": {"ok": True, "stats": {"budget_exhausted": True}}},
            "failed": [],
            "budget_exhausted": ["geocode"],
        }

    monkeypatch.setattr(run_cmd, "run_nightly", fake_run_nightly)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])
    assert result.exit_code == 0, result.output
    assert "budget exhausted" in result.output


def test_cli_nightly_dry_run_sheets_writes_four_json_files_and_skips_config_check(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "")  # not configured -- must not matter
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    db = _seeded_db(tmp_path)
    out_dir = tmp_path / "dryrun"

    # The real run_nightly() runs here (only run_sheets/run_nightly itself
    # are faked in the other CLI tests) -- so every pipeline step needs a
    # network-free fake, exactly like the unit-level tests above.
    _install_happy_fakes(monkeypatch, [])

    client_built = []
    monkeypatch.setattr(
        run_cmd.SheetsClient,
        "__init__",
        lambda self, *a, **k: client_built.append(True),
    )

    result = CliRunner().invoke(
        app, ["run", "nightly", "--db", str(db), "--dry-run-sheets", str(out_dir)]
    )
    assert result.exit_code == 0, result.output
    assert client_built == []  # SheetsClient never constructed in dry-run mode

    for tab in ("eie", "sold", "dnb", "stations"):
        path = out_dir / f"{tab}.json"
        assert path.exists(), f"missing {path}"
        payload = json.loads(path.read_text())
        assert "header" in payload
        assert "rows" in payload


def test_cli_nightly_missing_service_account_file_exits_1_before_any_step_runs(
    tmp_path, monkeypatch
):
    """`spreadsheet_id`/`google_service_account_file` being *set* isn't
    enough -- a stale/typo'd path must be caught before the (potentially
    hours-long, budget-consuming) pipeline runs at all, not discovered only
    when the sheets step tries to use it at the very end."""
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "K")
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    monkeypatch.setenv(
        "GOOGLE_SERVICE_ACCOUNT_FILE", str(tmp_path / "does-not-exist-sa.json")
    )
    db = _seeded_db(tmp_path)

    order = []
    _install_happy_fakes(monkeypatch, order)

    result = CliRunner().invoke(app, ["run", "nightly", "--db", str(db)])

    assert result.exit_code == 1, result.output
    assert "not found" in result.output
    assert order == []  # no pipeline step executed -- the config check ran first


def test_cli_sheets_missing_service_account_file_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    monkeypatch.setenv(
        "GOOGLE_SERVICE_ACCOUNT_FILE", str(tmp_path / "does-not-exist-sa.json")
    )
    db = _seeded_db(tmp_path)

    result = CliRunner().invoke(app, ["run", "sheets", "--db", str(db)])

    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_sheets_exits_nonzero_on_publish_failure(tmp_path, monkeypatch):
    """When run_sheets returns a partial-failure dict (failed_tab key), the
    CLI must exit with code 1 and surface the failure."""
    monkeypatch.setenv("SPREADSHEET_ID", "SHEET1")
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa_path))
    db = _seeded_db(tmp_path)

    def fake_run_sheets_failing(conn, client):
        # Simulate mid-tab failure: Eie completed, Sold raised, DNB/Stations not attempted
        return {
            "tabs": {"Eie": {"rows": 0, "cells": 1}},
            "failed_tab": "Sold",
            "error": "sheets API exploded",
            "unattempted": ["DNB", "Stations"],
        }

    monkeypatch.setattr(run_cmd, "run_sheets", fake_run_sheets_failing)

    result = CliRunner().invoke(app, ["run", "sheets", "--db", str(db)])
    assert result.exit_code == 1, result.output
    assert "failed_tab" in result.output


# ---------------------------------------------------------------------------
# Polite-access wiring: browser UA + jittered pacing threaded into the FINN
# crawl/refresh (see skannonser/http.py, config [crawl]).
# ---------------------------------------------------------------------------


def test_run_nightly_paces_and_uses_browser_fetch(conn, domain, gateway, monkeypatch):
    from skannonser.http import browser_get

    order = []
    _install_happy_fakes(monkeypatch, order)

    captured = {}

    def recording_finn(
        domain, conn, project_dir, fetch=None, archive_dir=None,
        page_delay=None, fetch_delay=None,
    ):
        captured["finn_fetch"] = fetch
        captured["page_delay"] = page_delay
        captured["fetch_delay"] = fetch_delay
        return dict(_INGEST_OK)

    def recording_refresh(
        conn, domain, project_dir, mode, fetch=None,
        fetch_delay=None, listing_delay=None,
    ):
        captured["refresh_fetch"] = fetch
        captured["listing_delay"] = listing_delay
        return dict(_REFRESH_OK)

    monkeypatch.setattr(nightly_module, "run_finn_ingest", recording_finn)
    monkeypatch.setattr(nightly_module, "refresh_listings", recording_refresh)

    run_nightly(conn, domain, gateway, "K", RecordingClient())

    # Default fetch on the scraping paths is the browser-UA getter, not the
    # bare python-requests default.
    assert captured["finn_fetch"] is browser_get
    assert captured["refresh_fetch"] is browser_get
    # Pacing callables are wired from domain.crawl (non-None, callable).
    assert callable(captured["page_delay"])
    assert callable(captured["fetch_delay"])
    assert callable(captured["listing_delay"])
