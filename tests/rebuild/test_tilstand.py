from pathlib import Path

from skannonser.ingest.finn.payload import Section
from skannonser.enrich.tilstand import (
    GRID, BYGNINGSDEL, classify_input, content_sha, select_sections,
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
