"""Tests for FINN sold-price enrichment (`skannonser/enrich/sold.py` +
`store/repositories/sold.py`).

The feature is built but DORMANT -- it is not wired into `nightly.py`. These
tests drive it entirely offline (fake `fetch`); the one real network call
lives outside the suite.

Endpoint shape (captured live 2026-07): the FINN sold map's
`soldpropertiescard?bbox=minLon,minLat,maxLon,maxLat` returns
`{"docs":[{...}]}`, each doc keyed by `adId` (== finnkode) with
`cadastralSoldPrice` (the tinglyst sale price), `cadastralSoldDate`
(registration), `soldDate` (sale), and `priceSuggestion` (asking).
"""

import pytest

from skannonser.enrich import sold as sold_mod
from skannonser.enrich.sold import fetch_sold_cards, parse_sold_card, run_sold_enrich
from skannonser.store import connection, migrations
from skannonser.store.repositories.sold import SoldPricesRepo

# A doc shaped exactly like the live endpoint (trimmed to the fields we read).
_CARD = {
    "adId": 463400207,
    "address": "Hennumveien 2",
    "cadastralSoldPrice": 6450000,
    "cadastralSoldDate": "2026-07-02",
    "priceSuggestion": 6500000,
    "soldDate": "2026-05-21",
    "propertyType": "DETACHED",
    "size": 150,
}


class FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "sold.db")
    migrations.migrate(c)
    return c


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------


def test_parse_sold_card_extracts_normalized_fields():
    rec = parse_sold_card(_CARD)
    assert rec == {
        "finnkode": "463400207",
        "sold_price": 6450000,
        "sold_date": "2026-05-21",
        "cadastral_sold_date": "2026-07-02",
        "price_suggestion": 6500000,
        "address": "Hennumveien 2",
    }


def test_parse_sold_card_without_adid_returns_none():
    assert parse_sold_card({"cadastralSoldPrice": 1}) is None


def test_parse_sold_card_tolerates_missing_price_fields():
    rec = parse_sold_card({"adId": 111})
    assert rec["finnkode"] == "111"
    assert rec["sold_price"] is None
    assert rec["price_suggestion"] is None


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


def test_fetch_sold_cards_formats_bbox_and_returns_docs():
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append((url, kwargs))
        return FakeResp({"docs": [_CARD]})

    docs = fetch_sold_cards((10.26, 59.80, 10.264, 59.81), fetch=fake_fetch)

    assert docs == [_CARD]
    url, kwargs = calls[0]
    assert url == sold_mod.SOLD_CARD_URL
    # bbox passed as a single comma-joined param: minLon,minLat,maxLon,maxLat.
    assert kwargs["params"] == {"bbox": "10.26,59.8,10.264,59.81"}


def test_fetch_sold_cards_non_throttle_non_200_returns_empty():
    # A 404/500 is a dud tile, not a throttle -> empty, no alarm.
    docs = fetch_sold_cards(
        (0, 0, 1, 1), fetch=lambda url, **k: FakeResp({"docs": [_CARD]}, status=404)
    )
    assert docs == []


@pytest.mark.parametrize("status", [429, 403, 503])
def test_fetch_sold_cards_raises_throttled_on_rate_limit_status(status):
    from skannonser.enrich.sold import Throttled

    with pytest.raises(Throttled):
        fetch_sold_cards((0, 0, 1, 1), fetch=lambda url, **k: FakeResp({}, status=status))


def test_fetch_sold_cards_raises_throttled_on_non_json_body():
    from skannonser.enrich.sold import Throttled

    class HtmlResp:
        status_code = 200

        def json(self):
            raise ValueError("not json")  # a block/challenge page, not our JSON

    with pytest.raises(Throttled):
        fetch_sold_cards((0, 0, 1, 1), fetch=lambda url, **k: HtmlResp())


# ---------------------------------------------------------------------------
# repo
# ---------------------------------------------------------------------------


def test_repo_upsert_inserts_then_reads_back(conn):
    repo = SoldPricesRepo(conn)
    repo.upsert([parse_sold_card(_CARD)])

    row = conn.execute(
        "SELECT sold_price, sold_date, cadastral_sold_date, price_suggestion "
        "FROM sold_prices WHERE finnkode = '463400207'"
    ).fetchone()
    assert row["sold_price"] == 6450000
    assert row["sold_date"] == "2026-05-21"
    assert row["cadastral_sold_date"] == "2026-07-02"
    assert row["price_suggestion"] == 6500000


def test_repo_upsert_is_fill_only_for_sold_price(conn):
    repo = SoldPricesRepo(conn)
    repo.upsert([parse_sold_card(_CARD)])
    # A later re-fetch that somehow lacks the price must NOT wipe the stored one.
    repo.upsert([{"finnkode": "463400207", "sold_price": None, "sold_date": None,
                  "cadastral_sold_date": None, "price_suggestion": None, "address": None}])

    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode = '463400207'"
    ).fetchone()
    assert row["sold_price"] == 6450000


# ---------------------------------------------------------------------------
# orchestrator
# ---------------------------------------------------------------------------


def test_run_sold_enrich_stores_only_our_listings(conn):
    # We track finnkode 463400207 but NOT 999999999.
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('463400207')")
    conn.commit()

    other = {**_CARD, "adId": 999999999, "address": "Elsewhere 1"}

    stats = run_sold_enrich(
        conn,
        [(10.26, 59.80, 10.264, 59.81)],
        fetch=lambda url, **k: FakeResp({"docs": [_CARD, other]}),
    )

    stored = {
        r["finnkode"]
        for r in conn.execute("SELECT finnkode FROM sold_prices")
    }
    assert stored == {"463400207"}
    assert stats["tiles"] == 1
    assert stats["cards_seen"] == 2
    assert stats["stored"] == 1


# ---------------------------------------------------------------------------
# CLI wiring: manual command routes to run_sold_enrich; NOT in nightly.
# ---------------------------------------------------------------------------


def test_cli_enrich_sold_routes_to_run_sold_enrich(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app
    from skannonser.commands import run_cmd

    db = tmp_path / "cli.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()

    calls = []

    def fake_run(conn, bboxes, restrict=True):
        calls.append((bboxes, restrict))
        return {"tiles": 1, "cards_seen": 0, "matched": 0, "stored": 0}

    monkeypatch.setattr(run_cmd, "run_sold_enrich", fake_run)

    result = CliRunner().invoke(
        app, ["run", "enrich-sold", "--bbox", "10.26,59.80,10.264,59.81", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert calls == [([(10.26, 59.80, 10.264, 59.81)], True)]


def test_cli_enrich_sold_rejects_bad_bbox(tmp_path):
    from typer.testing import CliRunner

    from skannonser.cli import app

    result = CliRunner().invoke(app, ["run", "enrich-sold", "--bbox", "1,2,3"])
    assert result.exit_code == 2


def test_sold_enrich_is_not_wired_into_nightly():
    # The dormancy guarantee: nightly must not import or call the sold sweep.
    import inspect

    from skannonser import nightly

    src = inspect.getsource(nightly)
    assert "run_sold_enrich" not in src
    assert "enrich.sold" not in src


# ---------------------------------------------------------------------------
# Area tiler: select targets -> cover with grid cells -> adaptive sweep.
# ---------------------------------------------------------------------------


def _seed(conn, finnkode, status="Solgt", lat=None, lng=None):
    conn.execute(
        "INSERT INTO eiendom (finnkode, tilgjengelighet) VALUES (?, ?)",
        (finnkode, status),
    )
    if lat is not None:
        conn.execute(
            "INSERT INTO eiendom_processed (finnkode, lat, lng) VALUES (?, ?, ?)",
            (finnkode, lat, lng),
        )
    conn.commit()


def test_select_sold_targets_picks_solgt_with_coords_and_no_price(conn):
    from skannonser.enrich.sold import select_sold_targets

    _seed(conn, "A", "Solgt", 59.805, 10.261)      # target
    _seed(conn, "B", "", 59.805, 10.261)           # not sold
    _seed(conn, "C", "Solgt", None, None)          # no coords
    _seed(conn, "D", "Solgt", 59.806, 10.262)      # already priced -> excluded
    conn.execute(
        "INSERT INTO sold_prices (finnkode, sold_price) VALUES ('D', 5000000)"
    )
    _seed(conn, "E", "Solgt", 59.807, 10.263)      # row exists but price NULL -> target
    conn.execute("INSERT INTO sold_prices (finnkode, sold_price) VALUES ('E', NULL)")
    conn.commit()

    got = {t["finnkode"] for t in select_sold_targets(conn)}
    assert got == {"A", "E"}


def test_run_sold_sweep_stores_matched_targets(conn):
    from skannonser.enrich.sold import run_sold_sweep

    _seed(conn, "463400207", "Solgt", 59.805, 10.261)

    stats = run_sold_sweep(
        conn,
        fetch=lambda url, **k: FakeResp({"docs": [_CARD]}),
    )

    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode = '463400207'"
    ).fetchone()
    assert row["sold_price"] == 6450000
    assert stats["stored"] == 1


def test_run_sold_sweep_matches_target_despite_15_cap_in_dense_area(conn):
    # The failure the grid approach hit: a target surrounded by many other sales.
    # The endpoint returns only the 15 NEAREST to the box center; because the box
    # is centered on the target, the target must be among them.
    from skannonser.enrich.sold import run_sold_sweep

    _seed(conn, "500001", "Solgt", 59.805, 10.261)

    def fake_fetch(url, **kwargs):
        lon0, lat0, lon1, lat1 = (float(x) for x in kwargs["params"]["bbox"].split(","))
        cx, cy = (lon0 + lon1) / 2, (lat0 + lat1) / 2
        # 25 sales incl. our target, all within the box; return the 15 nearest
        # to the box center (the real endpoint's behaviour).
        sales = [(500001, 59.805, 10.261)] + [
            (900000 + i, 59.805 + 0.00005 * i, 10.261 + 0.00005 * i) for i in range(1, 25)
        ]
        inside = [s for s in sales if lon0 <= s[2] <= lon1 and lat0 <= s[1] <= lat1]
        inside.sort(key=lambda s: (s[1] - cy) ** 2 + (s[2] - cx) ** 2)
        docs = [{"adId": s[0], "cadastralSoldPrice": 4200000} for s in inside[:15]]
        return FakeResp({"docs": docs})

    stats = run_sold_sweep(conn, fetch=fake_fetch)

    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode = '500001'"
    ).fetchone()
    assert row["sold_price"] == 4200000      # matched despite 24 competing sales
    assert stats["tiles_queried"] == 1       # one request, no subdivision


def test_cli_enrich_sold_default_runs_budgeted_backlog(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app
    from skannonser.commands import run_cmd

    db = tmp_path / "sweep.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()

    calls = []

    def fake_backlog(conn, notify=None, max_requests=4, force=False, delay=None):
        calls.append({"max_requests": max_requests, "notify": notify, "delay": delay})
        return {"suspended": False, "coverage": {"priced": 0, "total": 0, "fraction": 0.0}}

    monkeypatch.setattr(run_cmd, "run_sold_backlog", fake_backlog)

    result = CliRunner().invoke(
        app, ["run", "enrich-sold", "--requests", "4", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["max_requests"] == 4
    assert callable(calls[0]["notify"])   # Pushover sink wired
    assert callable(calls[0]["delay"])    # paced


def test_cli_enrich_sold_resume_clears_suspension(tmp_path):
    from typer.testing import CliRunner

    from skannonser.cli import app
    from skannonser.enrich.sold import is_suspended, suspend

    db = tmp_path / "resume.db"
    c = connection.connect(db)
    migrations.migrate(c)
    suspend(c, "test throttle")
    assert is_suspended(c) is True
    c.close()

    result = CliRunner().invoke(app, ["run", "enrich-sold", "--resume", "--db", str(db)])
    assert result.exit_code == 0, result.output

    c2 = connection.connect(db)
    assert is_suspended(c2) is False


def test_cli_enrich_sold_status_makes_no_requests(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app
    from skannonser.commands import run_cmd

    db = tmp_path / "status.db"
    c = connection.connect(db)
    migrations.migrate(c)
    c.close()

    def boom(*a, **k):
        raise AssertionError("--status must not sweep")

    monkeypatch.setattr(run_cmd, "run_sold_backlog", boom)

    result = CliRunner().invoke(app, ["run", "enrich-sold", "--status", "--db", str(db)])
    assert result.exit_code == 0, result.output
    assert "coverage" in result.output.lower()


# ---------------------------------------------------------------------------
# Phase B: suspend state, aging, coverage, density+budget, backlog orchestrator.
# ---------------------------------------------------------------------------


def _seed_aged(conn, finnkode, days_ago, lat=59.80, lng=10.26, status="Solgt"):
    conn.execute(
        "INSERT INTO eiendom (finnkode, tilgjengelighet, updated_at) "
        "VALUES (?, ?, datetime('now', ?))",
        (finnkode, status, f"-{days_ago} days"),
    )
    conn.execute(
        "INSERT INTO eiendom_processed (finnkode, lat, lng) VALUES (?, ?, ?)",
        (finnkode, lat, lng),
    )
    conn.commit()


def _card_fetch(coords: dict):
    """Fake fetch: returns a priced card for each seeded finnkode whose coords
    fall inside the requested bbox."""
    def fetch(url, **kwargs):
        lon0, lat0, lon1, lat1 = (float(x) for x in kwargs["params"]["bbox"].split(","))
        docs = [
            {"adId": int(fk), "cadastralSoldPrice": 5000000, "soldDate": "2026-01-01"}
            for fk, (la, ln) in coords.items()
            if lon0 <= ln <= lon1 and lat0 <= la <= lat1
        ]
        return FakeResp({"docs": docs})
    return fetch


def test_suspend_is_persistent_and_resumable(conn):
    from skannonser.enrich.sold import is_suspended, resume, suspend

    assert is_suspended(conn) is False
    suspend(conn, "throttled 429")
    assert is_suspended(conn) is True
    resume(conn)
    assert is_suspended(conn) is False


def test_select_sold_targets_min_age_excludes_recent(conn):
    from skannonser.enrich.sold import select_sold_targets

    _seed_aged(conn, "101", days_ago=200)   # old enough
    _seed_aged(conn, "102", days_ago=5)     # too recent

    got = {t["finnkode"] for t in select_sold_targets(conn, min_age_days=100)}
    assert got == {"101"}


def test_sold_coverage_counts_only_aged_and_priced(conn):
    from skannonser.enrich.sold import sold_coverage

    _seed_aged(conn, "101", 200)                       # aged, unpriced
    _seed_aged(conn, "102", 200)                       # aged, priced
    conn.execute("INSERT INTO sold_prices (finnkode, sold_price) VALUES ('102', 5000000)")
    _seed_aged(conn, "103", 5)                          # recent -> not in denominator
    conn.commit()

    cov = sold_coverage(conn, min_age_days=100)
    assert cov["total"] == 2
    assert cov["priced"] == 1
    assert cov["fraction"] == 0.5


def test_run_sold_sweep_density_first_within_request_budget(conn):
    from skannonser.enrich.sold import run_sold_sweep, select_sold_targets

    # 101 and 102 sit within one tight box (a cluster); 201 is far away. A
    # 1-request budget must spend it on the cluster, catching both in one box.
    _seed_aged(conn, "101", 200, lat=59.8010, lng=10.2610)
    _seed_aged(conn, "102", 200, lat=59.8013, lng=10.2613)   # ~35 m from 101
    _seed_aged(conn, "201", 200, lat=59.900, lng=10.500)     # far away

    coords = {"101": (59.8010, 10.2610), "102": (59.8013, 10.2613), "201": (59.900, 10.500)}
    stats = run_sold_sweep(
        conn,
        fetch=_card_fetch(coords),
        targets=select_sold_targets(conn, min_age_days=100),
        max_requests=1,
        order_by_density=True,
    )

    stored = {r["finnkode"] for r in conn.execute("SELECT finnkode FROM sold_prices")}
    assert stored == {"101", "102"}          # the cluster won the single request
    assert stats["tiles_queried"] == 1


def test_run_sold_sweep_tightens_box_when_target_hidden_by_cap(conn):
    from skannonser.enrich.sold import run_sold_sweep

    # The dense-area failure mode: a full-size box comes back CAPPED with the
    # target missing (nearer sales crowded it out); a half-size box surfaces it.
    _seed(conn, "500001", "Solgt", 59.805, 10.261)
    filler = [{"adId": 900000 + i} for i in range(15)]   # capped, none ours

    def fake_fetch(url, **kwargs):
        lon0, lat0, lon1, lat1 = (float(x) for x in kwargs["params"]["bbox"].split(","))
        width = lon1 - lon0
        if width > 0.0012:                                # full-size box
            return FakeResp({"docs": filler})
        return FakeResp({"docs": [{"adId": 500001, "cadastralSoldPrice": 4200000}]})

    stats = run_sold_sweep(conn, fetch=fake_fetch)

    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode = '500001'"
    ).fetchone()
    assert row["sold_price"] == 4200000
    assert stats["tiles_queried"] == 2       # full box, then one tighter retry


def test_backlog_is_noop_when_suspended(conn):
    from skannonser.enrich.sold import run_sold_backlog, suspend

    _seed_aged(conn, "101", 200)
    suspend(conn, "earlier throttle")
    calls = []

    stats = run_sold_backlog(
        conn, fetch=lambda url, **k: calls.append(1) or FakeResp({"docs": []})
    )
    assert stats["suspended"] is True
    assert calls == []                       # no network while suspended


def test_backlog_is_noop_when_coverage_target_reached(conn):
    from skannonser.enrich.sold import run_sold_backlog

    _seed_aged(conn, "101", 200)
    conn.execute("INSERT INTO sold_prices (finnkode, sold_price) VALUES ('101', 5000000)")
    conn.commit()
    calls = []

    stats = run_sold_backlog(
        conn,
        fetch=lambda url, **k: calls.append(1) or FakeResp({"docs": []}),
        coverage_target=0.80,
    )
    assert stats["target_reached"] is True
    assert calls == []                       # already >=80% covered


def test_backlog_suspends_and_notifies_on_throttle(conn):
    from skannonser.enrich.sold import is_suspended, run_sold_backlog

    _seed_aged(conn, "101", 200)
    notes = []

    stats = run_sold_backlog(
        conn,
        fetch=lambda url, **k: FakeResp({}, status=429),
        notify=notes.append,
    )
    assert stats.get("throttled") is True
    assert is_suspended(conn) is True
    assert notes and "throttl" in notes[0].lower()


def test_backlog_sweeps_and_reports_coverage(conn):
    from skannonser.enrich.sold import run_sold_backlog

    _seed_aged(conn, "500001", 200, lat=59.805, lng=10.261)

    stats = run_sold_backlog(
        conn,
        fetch=_card_fetch({"500001": (59.805, 10.261)}),
        max_requests=4,
    )
    assert stats["suspended"] is False
    assert stats["coverage"]["fraction"] == 1.0
    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode = '500001'"
    ).fetchone()
    assert row["sold_price"] == 5000000


# ---------------------------------------------------------------------------
# Daily-digest progress: how many priced recently + coverage.
# ---------------------------------------------------------------------------


def test_sold_progress_counts_recently_priced_and_coverage(conn):
    from skannonser.enrich.sold import sold_progress

    # Two aged sold listings; one priced just now, one still unpriced.
    _seed_aged(conn, "101", 200)
    _seed_aged(conn, "102", 200)
    conn.execute(
        "INSERT INTO sold_prices (finnkode, sold_price, updated_at) "
        "VALUES ('101', 5000000, datetime('now'))"
    )
    # An older price (2 days ago) must NOT count as "new today".
    _seed_aged(conn, "103", 200)
    conn.execute(
        "INSERT INTO sold_prices (finnkode, sold_price, updated_at) "
        "VALUES ('103', 4000000, datetime('now', '-2 days'))"
    )
    conn.commit()

    p = sold_progress(conn, since_hours=24, min_age_days=100)
    assert p["new_priced"] == 1                 # only the one priced today
    assert p["suspended"] is False
    assert p["coverage"]["total"] == 3
    assert p["coverage"]["priced"] == 2         # 101 + 103


def test_sold_progress_reports_suspension(conn):
    from skannonser.enrich.sold import sold_progress, suspend

    suspend(conn, "429")
    assert sold_progress(conn)["suspended"] is True


# ---------------------------------------------------------------------------
# Per-target attempt tracking (starvation guard)
# ---------------------------------------------------------------------------


def _attempts(conn) -> dict:
    return {
        r["finnkode"]: (r["attempts"], r["last_attempted_at"])
        for r in conn.execute(
            "SELECT finnkode, attempts, last_attempted_at FROM sold_price_attempts"
        )
    }


def test_sweep_records_one_attempt_per_target_it_queries(conn):
    from skannonser.enrich.sold import run_sold_sweep, select_sold_targets

    _seed_aged(conn, "101", 200, lat=59.8010, lng=10.2610)

    run_sold_sweep(
        conn,
        fetch=_card_fetch({}),  # no cards come back -- a miss
        targets=select_sold_targets(conn, min_age_days=100),
    )

    got = _attempts(conn)
    assert got["101"][0] == 1
    assert got["101"][1] is not None  # last_attempted_at stamped


def test_sweep_counts_one_attempt_even_when_box_is_tightened(conn):
    from skannonser.enrich.sold import run_sold_sweep

    # A capped-but-missed box costs TWO requests (full, then the adaptive
    # shrink) for ONE target -- the attempt count tracks targets, not requests.
    _seed(conn, "500001", "Solgt", 59.805, 10.261)
    filler = [{"adId": 900000 + i} for i in range(15)]

    stats = run_sold_sweep(conn, fetch=lambda url, **kw: FakeResp({"docs": filler}))

    assert stats["tiles_queried"] == 2
    assert _attempts(conn)["500001"][0] == 1


def test_target_caught_by_a_neighbour_box_records_no_attempt(conn):
    from skannonser.enrich.sold import run_sold_sweep, select_sold_targets

    # 102 sits inside 101's box, so it is matched without a request of its own
    # -- it must not be charged an attempt.
    _seed_aged(conn, "101", 200, lat=59.8010, lng=10.2610)
    _seed_aged(conn, "102", 200, lat=59.8013, lng=10.2613)
    coords = {"101": (59.8010, 10.2610), "102": (59.8013, 10.2613)}

    run_sold_sweep(
        conn,
        fetch=_card_fetch(coords),
        targets=select_sold_targets(conn, min_age_days=100),
        order_by_density=True,
    )

    got = _attempts(conn)
    assert got["101"][0] == 1
    assert "102" not in got


def test_select_sold_targets_carries_attempt_counts(conn):
    from skannonser.enrich.sold import record_attempts, select_sold_targets

    _seed_aged(conn, "101", 200)
    _seed_aged(conn, "102", 200)
    record_attempts(conn, ["101"])
    record_attempts(conn, ["101"])

    by_kode = {t["finnkode"]: t for t in select_sold_targets(conn, min_age_days=100)}
    assert by_kode["101"]["attempts"] == 2
    assert by_kode["102"]["attempts"] == 0


def test_repeatedly_missed_targets_yield_to_untried_ones(conn):
    from skannonser.enrich.sold import (
        record_attempts,
        run_sold_sweep,
        select_sold_targets,
    )

    # The starvation case: a DENSE cluster (101+102) that has been queried five
    # times without ever producing a card -- those sales may never be tinglyst.
    # A lone, never-tried target (201) must win the single request, even though
    # density alone would keep handing it to the cluster forever.
    _seed_aged(conn, "101", 200, lat=59.8010, lng=10.2610)
    _seed_aged(conn, "102", 200, lat=59.8013, lng=10.2613)
    _seed_aged(conn, "201", 200, lat=59.900, lng=10.500)
    for _ in range(5):
        record_attempts(conn, ["101", "102"])

    stats = run_sold_sweep(
        conn,
        fetch=_card_fetch({"201": (59.900, 10.500)}),
        targets=select_sold_targets(conn, min_age_days=100),
        max_requests=1,
        order_by_density=True,
    )

    stored = {r["finnkode"] for r in conn.execute("SELECT finnkode FROM sold_prices")}
    assert stored == {"201"}
    assert stats["tiles_queried"] == 1
