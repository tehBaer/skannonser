import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from skannonser.ingest.finn.payload import Section
from skannonser.enrich.tilstand import (
    GRID, BYGNINGSDEL, classify_input, content_sha, select_sections,
    TILSTAND_SCHEMA, TilstandResponse, cache_get, cache_put, classify_one,
    _response_text,
)

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def test_select_sections_keeps_condition_headings_and_tg_bodies():
    secs = [
        Section("Tilstandsrapport", "Rapport fra bygningssakkyndig."),
        Section("Beliggenhet", "Kort vei til butikk."),
        Section("Standard", "Bad har TG3 og krever utbedring."),  # body marker
        Section("Egenerklæring", "Selger opplyser om fuktskade."),
    ]
    kept = select_sections(secs)
    assert [s.heading for s in kept] == ["Tilstandsrapport", "Standard", "Egenerklæring"]


def test_classify_input_none_when_nothing_selected():
    # fixture 424071751 decodes to 38 sections, none condition-related
    html = (FIXTURES / "424071751.html").read_text(encoding="utf-8", errors="replace")
    assert classify_input(html) is None


def test_classify_input_selects_condition_text_from_real_ad():
    # fixture 432672475: 15 sections, 2 condition sections, ~9.5k chars
    html = (FIXTURES / "432672475.html").read_text(encoding="utf-8", errors="replace")
    text = classify_input(html)
    assert text is not None
    assert "TG" in text or "tilstand" in text.lower()
    assert len(text) < len(html)


def test_classify_input_never_raises_on_junk():
    for junk in ("", "<html></html>", "\x00\xff", "a" * 10):
        assert classify_input(junk) is None


def test_content_sha_is_stable_hex():
    assert content_sha("abc") == content_sha("abc")
    assert len(content_sha("abc")) == 64


def test_grid_is_the_spec_grid():
    assert GRID == (0, 10_000, 20_000, 50_000, 100_000, 200_000, 300_000, 500_000, 1_000_000)
    assert len(BYGNINGSDEL) == 18 and "annet" in BYGNINGSDEL


GOOD_RESPONSE = {
    "findings": [
        {"tg": 3, "bygningsdel": "vatrom", "tiltak": "utskiftning",
         "alvorlighet": "alvorlig", "kostnad_lav": 200_000, "kostnad_hoy": 500_000,
         "kostnad_kilde": "takst"},
    ],
    "egenerklaering_present": True,
    "egenerklaering": ["fuktskade"],
    "tilstandsrapport_dato": "2026-05-01",
    "tilstandsrapport_utsteder": "anticimex",
}


def test_classify_one_parses_via_injected_call():
    resp = classify_one("some text", _call=lambda text: json.dumps(GOOD_RESPONSE))
    assert resp.findings[0].bygningsdel == "vatrom"
    assert resp.egenerklaering == ["fuktskade"]


def test_response_model_rejects_off_vocab_and_off_grid():
    with pytest.raises(ValidationError):
        TilstandResponse.model_validate(
            {**GOOD_RESPONSE, "findings": [{**GOOD_RESPONSE["findings"][0], "bygningsdel": "badekar"}]}
        )
    with pytest.raises(ValidationError):
        TilstandResponse.model_validate(
            {**GOOD_RESPONSE, "findings": [{**GOOD_RESPONSE["findings"][0], "kostnad_lav": 137_500}]}
        )
    with pytest.raises(ValidationError):
        TilstandResponse.model_validate({**GOOD_RESPONSE, "egenerklaering": ["badekar"]})


def test_schema_declares_enums_at_the_wire():
    f = TILSTAND_SCHEMA["properties"]["findings"]["items"]
    assert f["properties"]["bygningsdel"]["enum"][0] == "vatrom"
    assert 137_500 not in f["properties"]["kostnad_lav"]["anyOf"][0]["enum"]
    assert TILSTAND_SCHEMA["additionalProperties"] is False


def test_cache_roundtrip(tmp_path):
    from skannonser.store import connection, migrations
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    assert cache_get(conn, "deadbeef") is None
    cache_put(conn, "deadbeef", json.dumps(GOOD_RESPONSE))
    assert json.loads(cache_get(conn, "deadbeef")) == GOOD_RESPONSE


def test_response_text_raises_on_refusal():
    response = SimpleNamespace(stop_reason="refusal", content=[])
    with pytest.raises(RuntimeError, match="refused"):
        _response_text(response)


def test_response_text_raises_on_truncation():
    response = SimpleNamespace(stop_reason="max_tokens", content=[])
    with pytest.raises(RuntimeError, match="truncated"):
        _response_text(response)


def test_response_text_returns_text_on_normal_stop():
    response = SimpleNamespace(
        stop_reason="end_turn",
        content=[SimpleNamespace(type="text", text="hello")],
    )
    assert _response_text(response) == "hello"
