"""Unit tests for skannonser.ingest.finn.parse_details -- the group-A/B/C
field parser that sits BESIDE the frozen legacy parse_ad (see the
2026-07-23 listing-details design spec)."""
import json

from skannonser.ingest.finn.parse_details import ListingDetails, parse_details


def _gam_html(targeting: list[dict]) -> str:
    state = {"config": {"adServer": {"gam": {"targeting": targeting}}}}
    return (
        "<html><head><script type=\"application/json\" "
        "id=\"advertising-initial-state\">"
        + json.dumps(state)
        + "</script></head><body></body></html>"
    )


def test_gam_int_fields():
    html = _gam_html(
        [
            {"key": "bedrooms", "value": ["2"]},
            {"key": "rooms", "value": ["3"]},
            {"key": "floor", "value": ["5"]},
        ]
    )
    d = parse_details(html, "123")
    assert d.finnkode == "123"
    assert d.bedrooms == 2
    assert d.rooms == 3
    assert d.floor == 5


def test_missing_gam_script_yields_all_none():
    d = parse_details("<html><body><p>hei</p></body></html>", "123")
    assert d.bedrooms is None and d.rooms is None and d.floor is None


def test_malformed_gam_json_yields_none_without_raising():
    html = (
        "<html><script type=\"application/json\" "
        "id=\"advertising-initial-state\">{not json</script></html>"
    )
    d = parse_details(html, "123")
    assert d.bedrooms is None


def test_non_numeric_gam_value_yields_none():
    html = _gam_html([{"key": "bedrooms", "value": ["mange"]}])
    assert parse_details(html, "123").bedrooms is None


def test_garbage_html_never_raises():
    d = parse_details("<<<>>>\x00????", "123")
    assert isinstance(d, ListingDetails)
