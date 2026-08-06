import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from skannonser.ingest.finn.payload import Section
from skannonser.enrich.tilstand import (
    GRID, BYGNINGSDEL, classify_input, content_sha, select_sections,
    TILSTAND_SCHEMA, TilstandResponse, cache_get, cache_put, classify_one,
    _response_text, compute_rollup,
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


def _resp(findings, egen_present=True, egen=()):
    return TilstandResponse.model_validate({
        "findings": findings,
        "egenerklaering_present": egen_present,
        "egenerklaering": list(egen),
        "tilstandsrapport_dato": None,
        "tilstandsrapport_utsteder": None,
    })


F_BAD = {"tg": 3, "bygningsdel": "vatrom", "tiltak": None, "alvorlighet": "alvorlig",
         "kostnad_lav": 200_000, "kostnad_hoy": 500_000, "kostnad_kilde": "takst"}
F_TAK = {"tg": 2, "bygningsdel": "tak", "tiltak": None, "alvorlighet": "mindre",
         "kostnad_lav": 10_000, "kostnad_hoy": 50_000, "kostnad_kilde": "estimat"}


def test_rollup_sums_and_worst():
    r = compute_rollup(_resp([F_BAD, F_TAK], egen=["fuktskade"]))
    assert (r["tg2_count"], r["tg3_count"]) == (1, 1)
    assert (r["reparasjon_lav"], r["reparasjon_hoy"]) == (210_000, 550_000)
    # midpoints 350k + 30k = 380k, rounded to nearest 10k
    assert r["reparasjon_est"] == 380_000
    assert r["alvorlighet"] == "alvorlig"
    assert r["verste_bygningsdel"] == "vatrom"
    assert r["reparasjon_kilde"] == "blandet"
    assert r["egenerklaering_antall"] == 1


def test_rollup_severity_tie_broken_by_kostnad_hoy():
    a = {**F_BAD, "bygningsdel": "tak", "kostnad_hoy": 300_000}
    b = {**F_BAD, "bygningsdel": "vatrom", "kostnad_hoy": 500_000}
    assert compute_rollup(_resp([a, b]))["verste_bygningsdel"] == "vatrom"


def test_rollup_zero_findings_is_counts_zero_not_null():
    r = compute_rollup(_resp([]))
    assert (r["tg2_count"], r["tg3_count"]) == (0, 0)
    assert r["reparasjon_lav"] is None and r["reparasjon_est"] is None
    assert r["alvorlighet"] is None and r["reparasjon_kilde"] is None


def test_rollup_egen_absent_section_is_null_not_zero():
    assert compute_rollup(_resp([], egen_present=False))["egenerklaering_antall"] is None
    assert compute_rollup(_resp([], egen_present=True))["egenerklaering_antall"] == 0


def test_rollup_kilde_uniform():
    assert compute_rollup(_resp([F_BAD]))["reparasjon_kilde"] == "takst"
    assert compute_rollup(_resp([F_TAK]))["reparasjon_kilde"] == "estimat"
