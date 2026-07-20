from datetime import datetime, timezone

import pytest

import main.location_features as legacy_location_features
from main.location_features import PublicTransitCommuteTime

from skannonser.config.domain import Budget
from skannonser.enrich.sentinels import TRAVEL_API_ERROR, TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC
from skannonser.enrich.travel_api import TransitCommute
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.store import connection, migrations

WORK_ADDRESS = "Rådmann Halmrasts Vei 5"
CURRENT_UTC_MONTH = datetime.now(timezone.utc).strftime("%Y-%m")


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


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


@pytest.fixture
def gateway(tmp_path):
    conn = connection.connect(tmp_path / "travel.db")
    migrations.migrate(conn)
    return Gateway(conn, make_budget(), notify=lambda m: None, sleeper=lambda s: None)


def _routes_row_count(gateway: Gateway) -> int:
    return gateway.conn.execute(
        "SELECT COUNT(*) AS c FROM api_usage WHERE api='routes'"
    ).fetchone()["c"]


def _seed_ok_row(conn, api, month=CURRENT_UTC_MONTH):
    """Seed a single 'ok' api_usage row directly, dated inside `month` (UTC,
    matching Gateway's default clock), so it counts toward month_usage."""
    called_at = f"{month}-01 00:00:00"
    conn.execute(
        "INSERT INTO api_usage (api, outcome, called_at) VALUES (?, ?, ?)",
        (api, "ok", called_at),
    )
    conn.commit()


# --- Step 1: pin test — request construction equals legacy -----------------


def test_build_request_matches_legacy_byte_for_byte(monkeypatch):
    """Capture the real requests.post call made by the legacy
    PublicTransitCommuteTime.calculate() and assert our ported build_request()
    produces the identical url/headers/body, with no network involved on
    either side."""
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        return FakeResponse(200, {"routes": []})

    monkeypatch.setattr(legacy_location_features.requests, "post", fake_post)

    legacy = PublicTransitCommuteTime(WORK_ADDRESS, config={"api_key": "K"})
    legacy.calculate("Storgata 1", "0155")

    assert captured, "legacy .calculate() never called requests.post"

    ported = TransitCommute(WORK_ADDRESS, gateway=None, api_key="K")
    url, headers, body = ported.build_request("Storgata 1", "0155")

    assert url == captured["url"]
    assert headers["X-Goog-Api-Key"] == captured["headers"]["X-Goog-Api-Key"]
    assert headers["X-Goog-FieldMask"] == captured["headers"]["X-Goog-FieldMask"]
    assert body == captured["json"]


def test_build_request_omits_postnummer_when_absent():
    ported = TransitCommute(WORK_ADDRESS, gateway=None, api_key="K")
    _, _, body = ported.build_request("Storgata 1", None)
    assert body["origin"]["address"] == "Storgata 1, Norway"


# --- Step 2: behavior / sentinel matrix -------------------------------------


def test_minutes_parses_valid_duration(gateway):
    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gateway,
        api_key="K",
        post=lambda *a, **k: FakeResponse(200, {"routes": [{"duration": "1800s"}]}),
    )
    assert commute.minutes("Storgata 1", "0155") == 30
    assert _routes_row_count(gateway) == 1


def test_minutes_rejects_unrealistic_duration(gateway):
    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gateway,
        api_key="K",
        max_minutes=360,
        post=lambda *a, **k: FakeResponse(200, {"routes": [{"duration": "90000s"}]}),
    )
    assert commute.minutes("Storgata 1", "0155") == TRAVEL_UNREALISTIC
    assert _routes_row_count(gateway) == 1


def test_minutes_no_routes(gateway):
    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gateway,
        api_key="K",
        post=lambda *a, **k: FakeResponse(200, {"routes": []}),
    )
    assert commute.minutes("Storgata 1", "0155") == TRAVEL_NO_ROUTES
    assert _routes_row_count(gateway) == 1


def test_minutes_route_without_duration_key(gateway):
    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gateway,
        api_key="K",
        post=lambda *a, **k: FakeResponse(200, {"routes": [{}]}),
    )
    assert commute.minutes("Storgata 1", "0155") == TRAVEL_NO_ROUTES
    assert _routes_row_count(gateway) == 1


def test_minutes_post_raises(gateway):
    def boom(*a, **k):
        raise RuntimeError("network down")

    commute = TransitCommute(WORK_ADDRESS, gateway=gateway, api_key="K", post=boom)
    assert commute.minutes("Storgata 1", "0155") == TRAVEL_API_ERROR
    assert _routes_row_count(gateway) == 1


def test_minutes_http_error_status(gateway):
    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gateway,
        api_key="K",
        post=lambda *a, **k: FakeResponse(500, {}),
    )
    assert commute.minutes("Storgata 1", "0155") is None
    assert _routes_row_count(gateway) == 1


def test_minutes_missing_api_key_short_circuits_before_gateway(gateway):
    calls = []

    def should_not_run(*a, **k):
        calls.append(1)
        return FakeResponse(200, {"routes": [{"duration": "60s"}]})

    commute = TransitCommute(WORK_ADDRESS, gateway=gateway, api_key="", post=should_not_run)
    assert commute.minutes("Storgata 1", "0155") is None
    assert calls == []
    assert _routes_row_count(gateway) == 0


# --- Regression: exception scope + budget propagation -----------------------


def test_malformed_response_degrades_to_api_error(gateway):
    """A 200 response whose body isn't valid JSON must degrade to
    TRAVEL_API_ERROR like legacy, not crash minutes() with an unhandled
    ValueError. Legacy's try/except (main/location_features.py:415-437) wraps
    the request through the end of parsing; ours must match that scope."""

    class BadJSONResponse:
        status_code = 200

        def json(self):
            raise ValueError("not valid json")

    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gateway,
        api_key="K",
        post=lambda *a, **k: BadJSONResponse(),
    )
    assert commute.minutes("Storgata 1", "0155") == TRAVEL_API_ERROR
    assert _routes_row_count(gateway) == 1


def test_budget_exceeded_propagates_not_sentinel(tmp_path):
    """BudgetExceeded is an administrative stop, not a per-row API failure.
    It must propagate out of minutes() so callers halt and leave rows
    untouched, instead of being laundered into a permanent TRAVEL_API_ERROR
    sentinel by the blanket except."""
    conn = connection.connect(tmp_path / "travel_budget.db")
    migrations.migrate(conn)
    budget = make_budget(routes_monthly_cap=1)
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None)
    _seed_ok_row(conn, "routes")  # this month's usage is already at the cap

    commute = TransitCommute(
        WORK_ADDRESS,
        gateway=gw,
        api_key="K",
        post=lambda *a, **k: FakeResponse(200, {"routes": [{"duration": "1800s"}]}),
    )

    with pytest.raises(BudgetExceeded):
        commute.minutes("Storgata 1", "0155")
