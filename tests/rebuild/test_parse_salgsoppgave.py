"""Rules-extracted salgsoppgave fields. Every field is optional and typed;
a parse failure yields None for that field only, never an exception."""
from pathlib import Path

import pytest

from skannonser.ingest.finn.parse_salgsoppgave import (
    Salgsoppgave,
    parse_salgsoppgave,
)

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def _parse(name):
    html = (FIXTURES / f"{name}.html").read_text(encoding="utf-8", errors="replace")
    return parse_salgsoppgave(html, name)


def test_extracts_at_least_one_field_from_a_real_ad():
    parsed = _parse("448347467")
    populated = [
        k for k, v in parsed.model_dump().items() if k != "finnkode" and v is not None
    ]
    assert populated, "no rule fired on a real salgsoppgave — regexes are dead"


def test_returns_model_with_finnkode_even_for_junk():
    result = parse_salgsoppgave("<html></html>", "999")
    assert isinstance(result, Salgsoppgave)
    assert result.finnkode == "999"
    assert result.ferdigattest is None
    assert result.eiendomsskatt_kr is None


def test_absent_topic_is_false_not_null_when_a_salgsoppgave_was_read():
    """NULL means 'no salgsoppgave text'; False means 'read it, not mentioned'.
    Conflating the two would make an unparsed listing indistinguishable from
    one that simply never discusses radon."""
    parsed = _parse("448347467")
    assert parsed.radon_omtalt in (True, False)
    assert parsed.heftelser in (True, False)

    unparsed = parse_salgsoppgave("<html></html>", "999")
    assert unparsed.radon_omtalt is None
    assert unparsed.heftelser is None


@pytest.mark.parametrize(
    "junk", ["", "<html>", "<script>enqueue(</script>", "not html at all"]
)
def test_never_raises_on_arbitrary_input(junk):
    assert parse_salgsoppgave(junk, "1").finnkode == "1"


def test_enum_fields_only_emit_known_values():
    """No free text: every enum column is a member of its vocabulary or None."""
    allowed = {
        "ferdigattest": {"ferdigattest", "midlertidig", "ingen"},
        "utleie": {"tillatt", "ikke_tillatt", "egen_enhet"},
        "husdyr": {"tillatt", "krever_godkjenning", "ikke_tillatt"},
    }
    for name in ("448347467", "432672475", "451631591", "466043223"):
        parsed = _parse(name)
        for field, vocabulary in allowed.items():
            value = getattr(parsed, field)
            assert value is None or value in vocabulary, (name, field, value)


@pytest.mark.parametrize(
    "prose, expected",
    [
        ("Selger har tegnet Boligselgerforsikring levert av Gjensidige.", True),
        ("Det har tegnet boligselgerforsikring.", True),
        ("Selger har ikke tegnet boligselgerforsikring.", False),
        ("Ingen boligselgerforsikring er tegnet for eiendommen.", False),
        ("Ingenting om forsikring her.", None),
    ],
)
def test_boligselgerforsikring_from_prose(prose, expected):
    """Comes from prose, never from ad.changeOfOwnershipInsurance -- that flag
    reads False on ~96% of ads regardless of what the prose says (verified over
    300 ads, 2026-07-27).

    The negative pattern must be checked first: 'har ikke tegnet' contains
    'har tegnet', so testing the positive first would invert every negative.
    """
    from skannonser.ingest.finn.parse_salgsoppgave import _boligselgerforsikring

    assert _boligselgerforsikring(prose) is expected


def test_kr_amounts_are_ints_not_strings():
    for name in ("448347467", "432672475"):
        value = _parse(name).eiendomsskatt_kr
        assert value is None or isinstance(value, int)
