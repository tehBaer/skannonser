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
            destinations=[dict(key="a", label="A", address="x")],
            polygon_points=[(10.0, 59.0), (10.1, 59.1)],
        )


def test_polygon_points_outside_coord_bounds_rejected():
    with pytest.raises(ValidationError):
        DomainConfig(
            filters=dict(sheets_max_price=1, url_max_price=1, min_bra_i=1, include_unlisted=True),
            coords=dict(lat_min=57.0, lat_max=72.0, lng_min=4.0, lng_max=32.0),
            travel=dict(reuse_within_meters=300, max_travel_minutes=360),
            destinations=[dict(key="a", label="A", address="x")],
            polygon_points=[(100.0, 59.0), (10.1, 59.1), (10.2, 59.2)],
        )
