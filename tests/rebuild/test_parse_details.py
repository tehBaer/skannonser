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


def _pricing_html(pairs: list[tuple[str, str]]) -> str:
    dl = "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in pairs)
    return f'<html><body><div data-testid="pricing-details"><dl>{dl}</dl></div></body></html>'


def test_money_fields_parse():
    html = _pricing_html(
        [
            ("Totalpris", "4\xa0944\xa0646 kr"),
            ("Omkostninger", "9\xa0646 kr"),
            ("Fellesgjeld", "1\xa0945\xa0000 kr"),
            ("Felleskost/mnd.", "13\xa0813 kr"),
            ("Fellesformue", "20\xa0178 kr"),
            ("Formuesverdi", "1\xa0139\xa0380 kr"),
        ]
    )
    d = parse_details(html, "123")
    assert d.totalpris == 4944646
    assert d.omkostninger == 9646
    assert d.fellesgjeld == 1945000
    assert d.felleskost_mnd == 13813
    assert d.fellesformue == 20178
    assert d.formuesverdi == 1139380


def test_kommunale_avg_per_aar_suffix():
    html = _pricing_html([("Kommunale avg.", "15\xa0088 kr per år")])
    assert parse_details(html, "123").kommunale_avg_aar == 15088


def test_zero_kr_parses_as_zero():
    html = _pricing_html([("Fellesgjeld", "0 kr")])
    assert parse_details(html, "123").fellesgjeld == 0


def test_unknown_dt_label_ignored():
    html = _pricing_html([("Prisantydning", "2\xa0990\xa0000 kr")])
    d = parse_details(html, "123")
    assert d.totalpris is None


def test_missing_pricing_section_all_money_none():
    d = parse_details("<html><body></body></html>", "123")
    assert d.totalpris is None and d.felleskost_mnd is None
