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
    # fixture 442178886 decodes with no condition-related sections at all
    html = (FIXTURES / "442178886.html").read_text(encoding="utf-8", errors="replace")
    assert classify_input(html) is None


def test_classify_input_selects_a_radon_only_ad():
    """424071751 has no condition report -- before the 2026-08-06 widening it
    selected nothing and was skipped. Its `Radon` section says "Radonmåling er
    ikke gjennomført", which is precisely the substantive statement this change
    exists to reach, so being classifiable now is the point rather than a
    regression. Measured over 400 real ads this flips 0 of them, so it does not
    move the corpus size: the ad is an edge case, not a pattern."""
    html = (FIXTURES / "424071751.html").read_text(encoding="utf-8", errors="replace")
    text = classify_input(html)
    assert text is not None
    assert "Radon" in text


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
    # Required, not defaulted -- see the note on TilstandResponse. Null here is
    # the common case: most prospectuses carry only generic radon advice.
    "radon_status": None,
    "radonsperre": None,
    "radon_bq": None,
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


def _resp(findings, egen_present=True, egen=(), **radon):
    return TilstandResponse.model_validate({
        "findings": findings,
        "egenerklaering_present": egen_present,
        "egenerklaering": list(egen),
        "tilstandsrapport_dato": None,
        "tilstandsrapport_utsteder": None,
        "radon_status": None,
        "radonsperre": None,
        "radon_bq": None,
        **radon,
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


def test_rollup_est_rounds_half_up_not_bankers():
    f = {"tg": 2, "bygningsdel": "tak", "tiltak": None, "alvorlighet": "mindre",
         "kostnad_lav": 0, "kostnad_hoy": 10_000, "kostnad_kilde": "estimat"}
    assert compute_rollup(_resp([f]))["reparasjon_est"] == 10_000


# --- cache export / import -------------------------------------------------
# Moving classifier results between machines (local classify -> server) needs
# only the CACHE: everything else is derived from it. These two functions are
# what the `tools export-tilstand-cache` / `import-tilstand-cache` pair wrap.

def _cache_db(tmp_path, name="c.db"):
    from skannonser.store import connection, migrations

    conn = connection.connect(tmp_path / name)
    migrations.migrate(conn)
    return conn


def test_export_cache_returns_all_rows(tmp_path):
    from skannonser.enrich.tilstand import cache_put, export_cache

    conn = _cache_db(tmp_path)
    assert export_cache(conn) == []
    cache_put(conn, "a" * 64, '{"x": 1}', model="m1")
    cache_put(conn, "b" * 64, '{"x": 2}', model="m2")
    rows = export_cache(conn)
    assert len(rows) == 2
    assert {r["content_sha256"] for r in rows} == {"a" * 64, "b" * 64}
    assert set(rows[0]) == {
        "content_sha256", "response_json", "model", "effort", "created_at",
        "schema_version"}


def test_import_cache_roundtrips_and_preserves_model_and_timestamp(tmp_path):
    from skannonser.enrich.tilstand import cache_put, export_cache, import_cache

    src = _cache_db(tmp_path, "src.db")
    cache_put(src, "a" * 64, '{"x": 1}', model="claude-opus-5")
    exported = export_cache(src)

    dst = _cache_db(tmp_path, "dst.db")
    assert import_cache(dst, exported) == {"imported": 1, "replaced": 0}
    got = export_cache(dst)[0]
    # created_at travels with the row: it records when the response was PAID
    # for, not when it was copied, so a re-export is idempotent.
    assert got == exported[0]


def test_import_cache_reports_replacements_separately(tmp_path):
    from skannonser.enrich.tilstand import (
        _SCHEMA_VERSION, cache_get, cache_put, import_cache,
    )

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"old": true}', model="m")
    # Rows carry the CURRENT schema_version: this test is about replacement
    # accounting, and an imported row on an older version would (correctly)
    # read back as a miss, which is what the versioning tests below assert.
    result = import_cache(conn, [
        {"content_sha256": "a" * 64, "response_json": '{"new": true}',
         "model": "m2", "created_at": "2026-01-01T00:00:00",
         "schema_version": _SCHEMA_VERSION},
        {"content_sha256": "b" * 64, "response_json": '{"x": 1}',
         "model": "m2", "created_at": "2026-01-01T00:00:00",
         "schema_version": _SCHEMA_VERSION},
    ])
    assert result == {"imported": 1, "replaced": 1}
    assert cache_get(conn, "a" * 64) == '{"new": true}'


def test_import_cache_rejects_rows_missing_required_keys(tmp_path):
    import pytest

    from skannonser.enrich.tilstand import export_cache, import_cache

    conn = _cache_db(tmp_path)
    with pytest.raises(ValueError):
        import_cache(conn, [{"content_sha256": "a" * 64}])
    # a rejected batch writes nothing at all
    assert export_cache(conn) == []


def test_import_cache_accepts_pre_017_exports_without_effort(tmp_path):
    """Export files written before migration 017 have no `effort` key. Rejecting
    them would strand every cache file exported earlier -- including ones
    already copied to the server. Missing means NOT RECORDED, i.e. NULL."""
    from skannonser.enrich.tilstand import export_cache, import_cache

    conn = _cache_db(tmp_path)
    legacy = [{"content_sha256": "a" * 64, "response_json": '{"x": 1}',
               "model": "claude-opus-5", "created_at": "2026-08-06T20:22:01"}]
    assert import_cache(conn, legacy) == {"imported": 1, "replaced": 0}
    got = export_cache(conn)[0]
    assert got["effort"] is None
    assert got["model"] == "claude-opus-5"


# --- cache schema versioning ------------------------------------------------
# Adding fields to the output schema makes every cached response incomplete.
# Version the cache so that shows up as a MISS (re-classify) rather than as
# silent NULLs that are indistinguishable from "the document said nothing".

def test_cache_get_ignores_rows_from_an_older_schema(tmp_path):
    from skannonser.enrich.tilstand import _SCHEMA_VERSION, cache_get, cache_put

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"old": true}', version=_SCHEMA_VERSION - 1)
    assert cache_get(conn, "a" * 64) is None                       # current version: miss
    assert cache_get(conn, "a" * 64, version=_SCHEMA_VERSION - 1) == '{"old": true}'


def test_cache_put_stamps_the_current_version(tmp_path):
    from skannonser.enrich.tilstand import _SCHEMA_VERSION, cache_put, export_cache

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"x": 1}')
    assert export_cache(conn)[0]["schema_version"] == _SCHEMA_VERSION


def test_a_version_bump_keeps_the_old_row(tmp_path):
    """Superseded rows stay on disk: they cost nothing and they document what
    was actually paid for."""
    from skannonser.enrich.tilstand import _SCHEMA_VERSION, cache_put, export_cache

    conn = _cache_db(tmp_path)
    cache_put(conn, "a" * 64, '{"old": true}', version=_SCHEMA_VERSION - 1)
    cache_put(conn, "b" * 64, '{"new": true}')
    assert len(export_cache(conn)) == 2


def test_export_import_carries_the_version(tmp_path):
    from skannonser.enrich.tilstand import (
        _SCHEMA_VERSION, cache_get, cache_put, export_cache, import_cache,
    )

    src = _cache_db(tmp_path, "src.db")
    cache_put(src, "a" * 64, '{"x": 1}', version=_SCHEMA_VERSION - 1)
    dst = _cache_db(tmp_path, "dst.db")
    import_cache(dst, export_cache(src))
    # a stale row must still read as stale on the receiving side, or a
    # server import would resurrect responses the local side had retired
    assert cache_get(dst, "a" * 64) is None
    assert export_cache(dst)[0]["schema_version"] == _SCHEMA_VERSION - 1


def test_import_cache_accepts_pre_018_exports_without_schema_version(tmp_path):
    """Same reasoning as the `effort` case above, with sharper teeth: the column
    is NOT NULL, so a missing key that reached the INSERT as None would raise
    IntegrityError and strand every export file written before this change --
    including the ones already on the server. Missing means version 1, which is
    what those responses actually are."""
    from skannonser.enrich.tilstand import cache_get, export_cache, import_cache

    conn = _cache_db(tmp_path)
    legacy = [{"content_sha256": "a" * 64, "response_json": '{"x": 1}',
               "model": "claude-opus-5", "created_at": "2026-08-06T20:22:01"}]
    assert import_cache(conn, legacy) == {"imported": 1, "replaced": 0}
    assert export_cache(conn)[0]["schema_version"] == 1
    assert cache_get(conn, "a" * 64) is None   # version 1 is stale, so: a miss


def test_select_sections_keeps_radon_and_hms_headings():
    """Radon statements often sit under their own heading or under "Helse,
    miljø og sikkerhet" -- neither matches the condition-report vocabulary, so
    without this the classifier is asked about text it was never shown."""
    secs = [
        Section("Radonmåling", "Det er ikke foretatt radonmålinger."),
        Section("Helse, miljø og sikkerhet", "Bygget er ikke oppført med radonsperre."),
        Section("Beliggenhet", "Kort vei til butikk."),
    ]
    kept = [s.heading for s in select_sections(secs)]
    assert kept == ["Radonmåling", "Helse, miljø og sikkerhet"]


# --- radon fields -----------------------------------------------------------

def test_radon_fields_parse_and_reject_off_vocab():
    from skannonser.enrich.tilstand import RADON_STATUS, RADONSPERRE

    base = {**GOOD_RESPONSE, "radon_status": "malt_over_grense",
            "radonsperre": "mangler", "radon_bq": 280}
    resp = TilstandResponse.model_validate(base)
    assert resp.radon_status == "malt_over_grense"
    assert resp.radonsperre == "mangler"
    assert resp.radon_bq == 280

    for bad in ({"radon_status": "kanskje"}, {"radonsperre": "delvis"}):
        with pytest.raises(ValidationError):
            TilstandResponse.model_validate({**base, **bad})

    assert len(RADON_STATUS) == 4 and len(RADONSPERRE) == 2


def test_radon_fields_are_nullable():
    """NULL is the common case: most ads carry only generic advice, which is
    not a statement about this property."""
    resp = TilstandResponse.model_validate(
        {**GOOD_RESPONSE, "radon_status": None, "radonsperre": None, "radon_bq": None})
    assert resp.radon_status is None and resp.radon_bq is None


def test_radon_bq_is_a_free_integer_not_a_grid_value():
    """Unlike repair costs, this is a measured quantity whose whole value is
    its position relative to the 100 and 200 Bq/m3 thresholds -- snapping it
    to the cost grid would destroy that."""
    resp = TilstandResponse.model_validate({**GOOD_RESPONSE, "radon_bq": 137})
    assert resp.radon_bq == 137


def test_schema_declares_the_radon_enums_at_the_wire():
    from skannonser.enrich.tilstand import RADON_STATUS

    props = TILSTAND_SCHEMA["properties"]
    assert props["radon_status"]["anyOf"][0]["enum"] == list(RADON_STATUS)
    assert props["radon_bq"]["anyOf"][0]["type"] == "integer"
    for key in ("radon_status", "radonsperre", "radon_bq"):
        assert key in TILSTAND_SCHEMA["required"]


def test_prompt_names_both_documented_traps():
    """The two failure modes measured in the spec. If these instructions are
    ever dropped the model reverts to extracting the statutory limit as the
    property's radon level."""
    from skannonser.enrich.tilstand import _SYSTEM_PROMPT

    p = _SYSTEM_PROMPT.lower()
    assert "grenseverdi" in p          # trap 1: quoted threshold, not a measurement
    assert "radonsperre" in p          # trap 2: negation around the barrier
    assert "aktsomhet" in p            # the risk-map paragraphs, also not a measurement
