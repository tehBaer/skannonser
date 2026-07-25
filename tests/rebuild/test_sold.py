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

    def fake_backlog(conn, notify=None, max_requests=4, force=False, delay=None,
                      grace_days=-1, max_attempts=-1, inaktiv_reserve=-1):
        calls.append({
            "max_requests": max_requests,
            "notify": notify,
            "delay": delay,
            "grace_days": grace_days,
            "max_attempts": max_attempts,
            "inaktiv_reserve": inaktiv_reserve,
        })
        return {"suspended": False, "coverage": {"priced": 0, "total": 0, "fraction": 0.0}}

    monkeypatch.setattr(run_cmd, "run_sold_backlog", fake_backlog)

    result = CliRunner().invoke(
        app, ["run", "enrich-sold", "--requests", "4", "--db", str(db)]
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["max_requests"] == 4
    assert calls[0]["grace_days"] == 180  # config [sold] trukket_grace_days threaded through
    assert calls[0]["max_attempts"] == 5  # config [sold] max_attempts threaded through
    assert calls[0]["inaktiv_reserve"] == 2  # config [sold] inaktiv_reserve_requests threaded through
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


def test_backlog_no_longer_early_returns_at_high_coverage(conn):
    # Regression: the old 80% coverage gate used to early-return once solgt
    # coverage crossed 80%. That gate is deleted -- the sweep must still run
    # against remaining targets even when coverage is already >= 80% (the
    # exact threshold that used to trip the old gate).
    from skannonser.enrich.sold import run_sold_backlog

    # 4 already-priced solgt listings + 1 still-unpriced target = 80% coverage,
    # precisely the old gate's trip point (fraction >= 0.80).
    for i in range(4):
        fk = f"20{i}"
        _seed_aged(conn, fk, 200)
        conn.execute(
            "INSERT INTO sold_prices (finnkode, sold_price) VALUES (?, 5000000)", (fk,)
        )
    _seed_aged(conn, "500001", 200, lat=59.805, lng=10.261)   # still needs a price
    conn.commit()

    from skannonser.enrich.sold import sold_coverage
    assert sold_coverage(conn, min_age_days=100)["fraction"] >= 0.80  # sanity: gate would trip

    stats = run_sold_backlog(
        conn,
        fetch=_card_fetch({"500001": (59.805, 10.261)}),
        max_requests=4,
    )

    assert "target_reached" not in stats      # the gate is gone entirely
    assert stats["tiles_queried"] == 1         # the sweep actually ran
    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode = '500001'"
    ).fetchone()
    assert row["sold_price"] == 5000000


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


# ---------------------------------------------------------------------------
# Inaktiv sweep tier (2026-07-24 closed-status spec): widen the target set to
# in-grace Inaktiv listings, with strict Solgt-first priority -- Solgt is far
# likelier to have a tinglyst price, so a tight budget must go there first,
# ahead of both density and the attempts sub-order.
# ---------------------------------------------------------------------------


def _attempted_finnkodes(conn) -> list[str]:
    """Finnkodes with at least one recorded sweep attempt (see `_attempts`)."""
    return sorted(_attempts(conn))


def test_targets_include_inaktiv_within_grace(conn):
    from skannonser.enrich.sold import select_sold_targets

    # solgt row, inaktiv row closed 10 days ago, inaktiv row closed 200 days ago
    _seed_aged(conn, "111", 10, status="Solgt")
    _seed_aged(conn, "222", 10, status="Inaktiv")
    _seed_aged(conn, "333", 200, status="Inaktiv")

    targets = select_sold_targets(conn, grace_days=180)
    by_fk = {t["finnkode"]: t for t in targets}
    assert set(by_fk) == {"111", "222"}          # aged-out inaktiv excluded
    assert by_fk["111"]["status"] == "solgt"
    assert by_fk["222"]["status"] == "inaktiv"


def test_targets_inaktiv_with_price_excluded(conn):
    from skannonser.enrich.sold import select_sold_targets

    _seed_aged(conn, "222", 10, status="Inaktiv")
    conn.execute("INSERT INTO sold_prices (finnkode, sold_price) VALUES ('222', 5000000)")
    conn.commit()
    assert select_sold_targets(conn, grace_days=180) == []


def test_sweep_orders_solgt_tier_first(conn):
    from skannonser.enrich.sold import record_attempts, run_sold_sweep, select_sold_targets

    # Two inaktiv targets DENSER than the solgt target (a tight neighbour pair
    # vs. an isolated point) -- AND the solgt target has MORE prior attempts
    # than either of them, so the old (attempts-then-density) order would have
    # ranked it last. With a 1-request budget the solgt target must still be
    # attempted first: the status tier beats both density and attempts.
    _seed_aged(conn, "202", 10, lat=59.8010, lng=10.2610, status="Inaktiv")
    _seed_aged(conn, "203", 10, lat=59.8013, lng=10.2613, status="Inaktiv")  # ~35 m from 202
    _seed_aged(conn, "111", 10, lat=59.900, lng=10.500, status="Solgt")      # isolated
    for _ in range(3):
        record_attempts(conn, ["111"])

    targets = select_sold_targets(conn, grace_days=180)
    run_sold_sweep(
        conn,
        fetch=_card_fetch({}),   # no cards -- only attempt-charging matters here
        targets=targets,
        max_requests=1,
        order_by_density=True,
    )

    assert _attempted_finnkodes(conn) == ["111"]


def test_sweep_stores_price_for_inaktiv_target(conn):
    from skannonser.enrich.sold import run_sold_sweep, select_sold_targets

    # An inaktiv in-grace target whose bbox returns a card with a price: the
    # price must be stored (this is the whole point of the widening).
    _seed_aged(conn, "222", 10, lat=59.805, lng=10.261, status="Inaktiv")

    targets = select_sold_targets(conn, grace_days=180)
    run_sold_sweep(
        conn,
        fetch=lambda url, **k: FakeResp(
            {"docs": [{"adId": 222, "cadastralSoldPrice": 4500000}]}
        ),
        targets=targets,
    )

    row = conn.execute(
        "SELECT sold_price FROM sold_prices WHERE finnkode='222'"
    ).fetchone()
    assert row["sold_price"] == 4500000


def test_inaktiv_pending_counts(conn):
    from skannonser.enrich.sold import inaktiv_pending

    _seed_aged(conn, "222", 10, status="Inaktiv")    # pending
    _seed_aged(conn, "333", 200, status="Inaktiv")   # aged out
    _seed_aged(conn, "444", 10, status="Inaktiv")
    conn.execute("INSERT INTO sold_prices (finnkode, sold_price) VALUES ('444', 1)")  # priced
    conn.commit()

    out = inaktiv_pending(conn, grace_days=180)
    assert out == {"pending": 1, "priced": 1}


# ---------------------------------------------------------------------------
# Attempts ceiling (replaces the 80% coverage gate): a target only reaches
# max_attempts after the whole eligible backlog has been combed that many
# times (fewest-attempts-first ordering), so the sweep provably goes quiet.
# ---------------------------------------------------------------------------


def test_select_sold_targets_excludes_target_at_attempts_ceiling(conn):
    from skannonser.enrich.sold import record_attempts, select_sold_targets

    _seed_aged(conn, "101", 200)   # will sit AT the ceiling -> excluded
    _seed_aged(conn, "102", 200)   # one attempt short of the ceiling -> included
    for _ in range(5):
        record_attempts(conn, ["101"])
    for _ in range(4):
        record_attempts(conn, ["102"])

    got = {
        t["finnkode"]
        for t in select_sold_targets(conn, min_age_days=100, max_attempts=5)
    }
    assert got == {"102"}


def test_select_sold_targets_ceiling_applies_to_both_tiers(conn):
    from skannonser.enrich.sold import record_attempts, select_sold_targets

    _seed_aged(conn, "111", 10, status="Solgt")
    _seed_aged(conn, "222", 10, status="Inaktiv")
    for _ in range(5):
        record_attempts(conn, ["111", "222"])

    assert select_sold_targets(conn, grace_days=180, max_attempts=5) == []


def test_given_up_targets_counts_priceless_at_or_above_ceiling(conn):
    from skannonser.enrich.sold import given_up_targets, record_attempts

    _seed_aged(conn, "101", 200)   # reaches the ceiling, still priceless -> given up
    _seed_aged(conn, "102", 200)   # below the ceiling -> not given up
    _seed_aged(conn, "103", 200)   # reaches the ceiling but IS priced -> not given up
    conn.execute("INSERT INTO sold_prices (finnkode, sold_price) VALUES ('103', 5000000)")
    conn.commit()
    for _ in range(5):
        record_attempts(conn, ["101", "103"])
    for _ in range(4):
        record_attempts(conn, ["102"])

    assert given_up_targets(conn, max_attempts=5) == 1


# ---------------------------------------------------------------------------
# Inaktiv reserve: strict Solgt-first ordering starves the Inaktiv tier
# completely at any realistic budget (1022 solgt vs 178 inaktiv live). These
# targets are handed to run_sold_sweep directly (bypassing select_sold_targets
# / the eiendom table entirely -- run_sold_sweep only needs finnkode/lat/lng/
# status/attempts dicts), spaced far enough apart that no box ever catches a
# neighbour, so every non-skipped target costs exactly one request. The fake
# fetch never returns a card, so "matched" never fires and only the tier/cap
# bookkeeping is under test.
# ---------------------------------------------------------------------------


def _no_match_fetch(url, **kwargs):
    return FakeResp({"docs": []})


def _spread(prefix, n, status, lat0=59.0, lng0=10.0, step=0.05):
    """``n`` targets far enough apart (step >> the ~0.0008/0.0005 deg pad) that
    no box ever catches a neighbour -- one request per non-skipped target."""
    return [
        {
            "finnkode": f"{prefix}{i}",
            "lat": lat0 + i * step,
            "lng": lng0 + i * step,
            "status": status,
            "attempts": 0,
        }
        for i in range(n)
    ]


def test_inaktiv_reserve_carves_out_budget_from_solgt_first_ordering(conn):
    # The regression this exists to fix: with 20 solgt targets and strict
    # Solgt-first ordering, pre-change code would spend the ENTIRE 15-request
    # budget on solgt, leaving inaktiv at 0 (position 1023 in the live-DB
    # numbers this fix addresses). With the reserve, solgt is capped at
    # max(1, 15 - 2) = 13 and the freed 2 requests go to inaktiv.
    from skannonser.enrich.sold import run_sold_sweep

    solgt = _spread("s", 20, "solgt")
    inaktiv = _spread("i", 3, "inaktiv", lat0=61.0, lng0=12.0)

    stats = run_sold_sweep(
        conn,
        fetch=_no_match_fetch,
        targets=solgt + inaktiv,
        max_requests=15,
        inaktiv_reserve=2,
    )

    attempted = _attempted_finnkodes(conn)
    solgt_attempted = [fk for fk in attempted if fk.startswith("s")]
    inaktiv_attempted = [fk for fk in attempted if fk.startswith("i")]
    assert len(solgt_attempted) == 13
    assert len(inaktiv_attempted) == 2
    assert stats["tiles_queried"] == 15


def test_inaktiv_reserve_not_wasted_when_no_inaktiv_targets(conn):
    # No eligible inaktiv targets at all -> solgt may use the FULL budget, not
    # max_requests - inaktiv_reserve. The reserve is never wasted on an empty
    # tier.
    from skannonser.enrich.sold import run_sold_sweep

    solgt = _spread("s", 20, "solgt")

    stats = run_sold_sweep(
        conn,
        fetch=_no_match_fetch,
        targets=solgt,
        max_requests=15,
        inaktiv_reserve=2,
    )

    attempted = _attempted_finnkodes(conn)
    assert len(attempted) == 15          # not capped to 13
    assert stats["tiles_queried"] == 15


def test_inaktiv_gets_all_leftover_when_solgt_tier_runs_dry(conn):
    # Few solgt targets (fewer than the 13-request solgt cap) + many inaktiv:
    # once solgt exhausts its own backlog, ALL remaining budget -- not just
    # the 2-request reserve -- flows downhill to inaktiv.
    from skannonser.enrich.sold import run_sold_sweep

    solgt = _spread("s", 3, "solgt")
    inaktiv = _spread("i", 20, "inaktiv", lat0=61.0, lng0=12.0)

    stats = run_sold_sweep(
        conn,
        fetch=_no_match_fetch,
        targets=solgt + inaktiv,
        max_requests=15,
        inaktiv_reserve=2,
    )

    attempted = _attempted_finnkodes(conn)
    solgt_attempted = [fk for fk in attempted if fk.startswith("s")]
    inaktiv_attempted = [fk for fk in attempted if fk.startswith("i")]
    assert len(solgt_attempted) == 3     # all of them, tier ran dry
    assert len(inaktiv_attempted) == 12  # 15 - 3, not just the 2-request reserve
    assert stats["tiles_queried"] == 15


def test_inaktiv_reserve_tiny_budget_favors_solgt(conn):
    # max_requests=1 with both tiers pending: max(1, 1 - 2) == 1, so solgt
    # gets the single request and inaktiv gets none -- Solgt is never fully
    # starved even when the nominal reserve would exceed the whole budget.
    from skannonser.enrich.sold import run_sold_sweep

    solgt = _spread("s", 5, "solgt")
    inaktiv = _spread("i", 5, "inaktiv", lat0=61.0, lng0=12.0)

    stats = run_sold_sweep(
        conn,
        fetch=_no_match_fetch,
        targets=solgt + inaktiv,
        max_requests=1,
        inaktiv_reserve=2,
    )

    attempted = _attempted_finnkodes(conn)
    assert attempted == ["s0"]
    assert stats["tiles_queried"] == 1


def test_inaktiv_reserve_charges_no_attempt_to_solgt_skipped_by_subcap(conn):
    # A solgt target skipped because the solgt sub-cap was reached must have
    # NO row in the attempts ledger at all -- not attempts == 0, no row,
    # since record_attempts is only called for targets a box was actually
    # centered on.
    from skannonser.enrich.sold import run_sold_sweep

    solgt = _spread("s", 20, "solgt")
    inaktiv = _spread("i", 3, "inaktiv", lat0=61.0, lng0=12.0)

    run_sold_sweep(
        conn,
        fetch=_no_match_fetch,
        targets=solgt + inaktiv,
        max_requests=15,
        inaktiv_reserve=2,
    )

    ledger = _attempts(conn)
    # s13..s19 sit past the 13-request solgt cap -> skipped, never charged.
    for i in range(13, 20):
        assert f"s{i}" not in ledger
    # i2 sits past the 2-request inaktiv share -> skipped, never charged.
    assert "i2" not in ledger
    # The ones actually attempted ARE charged exactly once.
    for i in range(13):
        assert ledger[f"s{i}"][0] == 1
    for i in range(2):
        assert ledger[f"i{i}"][0] == 1
