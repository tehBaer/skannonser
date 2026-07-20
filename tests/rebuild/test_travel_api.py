import pytest

import main.location_features as legacy_location_features
from main.location_features import PublicTransitCommuteTime

from skannonser.config.domain import Budget
from skannonser.enrich.sentinels import TRAVEL_API_ERROR, TRAVEL_NO_ROUTES, TRAVEL_UNREALISTIC
from skannonser.enrich.travel_api import TransitCommute
from skannonser.gateway import Gateway
from skannonser.store import connection, migrations

WORK_ADDRESS = "Rådmann Halmrasts Vei 5"


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
