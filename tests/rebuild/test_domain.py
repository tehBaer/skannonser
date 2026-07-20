import pytest
from pydantic import ValidationError

from skannonser.config.domain import DomainConfig, load_domain


def test_config_show_masks_secrets(monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app

    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "super-secret-value")
    result = CliRunner().invoke(app, ["config", "show"])
    assert result.exit_code == 0, result.output
    assert "super-secret-value" not in result.output
    assert "google_maps_api_key: set" in result.output


def test_load_domain_matches_legacy_values():
    d = load_domain()
    assert d.filters.sheets_max_price == 7_500_000
    assert d.filters.min_bra_i == 70
    assert d.travel.reuse_within_meters == 300
    assert [dest.key for dest in d.destinations] == ["brj", "mvv", "mvv_uni"]
    assert len(d.polygon_points) == 10
    lng, lat = d.polygon_points[0]
    assert d.coords.lng_min <= lng <= d.coords.lng_max
    assert d.coords.lat_min <= lat <= d.coords.lat_max


def test_polygon_must_have_three_points():
    with pytest.raises(ValidationError):
        DomainConfig(
            filters=dict(sheets_max_price=1, url_max_price=1, min_bra_i=1, include_unlisted=True),
            coords=dict(lat_min=57.0, lat_max=72.0, lng_min=4.0, lng_max=32.0),
            travel=dict(reuse_within_meters=300, max_travel_minutes=360),
            destinations=[dict(key="a", label="A", address="x", df_column="X", db_column="x")],
            polygon_points=[(10.0, 59.0), (10.1, 59.1)],
            budget=dict(routes_monthly_cap=9000, geocode_monthly_cap=9000, warn_pcts=[50, 80]),
            dnb=dict(region_guids=[], max_pages=1),
        )


def test_polygon_points_outside_coord_bounds_rejected():
    with pytest.raises(ValidationError):
        DomainConfig(
            filters=dict(sheets_max_price=1, url_max_price=1, min_bra_i=1, include_unlisted=True),
            coords=dict(lat_min=57.0, lat_max=72.0, lng_min=4.0, lng_max=32.0),
            travel=dict(reuse_within_meters=300, max_travel_minutes=360),
            destinations=[dict(key="a", label="A", address="x")],
            polygon_points=[(100.0, 59.0), (10.1, 59.1), (10.2, 59.2)],
            budget=dict(routes_monthly_cap=9000, geocode_monthly_cap=9000, warn_pcts=[50, 80]),
            dnb=dict(region_guids=[], max_pages=1),
        )


def test_load_domain_has_budget_config():
    d = load_domain()
    assert d.budget.routes_monthly_cap == 9000
    assert d.budget.geocode_monthly_cap == 9000
    assert d.budget.warn_pcts == [50, 80]
    assert d.budget.routes_rpm == 60
    assert d.budget.geocode_rpm == 60


def test_load_domain_destinations_have_column_config():
    d = load_domain()
    brj = next(dest for dest in d.destinations if dest.key == "brj")
    assert brj.df_column == "PENDL RUSH BRJ"
    assert brj.db_column == "pendl_rush_brj"
    assert brj.exclusive is False

    mvv_uni = next(dest for dest in d.destinations if dest.key == "mvv_uni")
    assert mvv_uni.df_column == "MVV UNI RUSH"
    assert mvv_uni.db_column == "pendl_rush_mvv_uni_rush"
    assert mvv_uni.exclusive is True
