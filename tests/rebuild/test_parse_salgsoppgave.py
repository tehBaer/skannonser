"""Rules-extracted salgsoppgave fields. Every field is optional and typed;
a parse failure yields None for that field only, never an exception."""
from pathlib import Path

import pytest

from skannonser.ingest.finn.parse_salgsoppgave import (
    Salgsoppgave,
    parse_salgsoppgave,
)
from skannonser.ingest.finn.payload import Section

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


def test_parse_never_propagates_when_internal_extraction_raises(monkeypatch):
    """Finding 1: `payload._MAX_DEPTH` bounds `_resolve`'s own recursion but
    not the ambient stack already spent getting here -- inside an ordinary
    Typer/FastAPI call chain, a sufficiently nested turbo-stream payload can
    still raise RecursionError out of `_resolve` -> `_from_turbostream` ->
    `decode_ad` -> `parse_salgsoppgave`. Any exception reaching that last
    hop must degrade to the all-NULL row for this finnkode, not propagate
    and abort the caller's batch of thousands."""
    import skannonser.ingest.finn.parse_salgsoppgave as mod

    def boom(html):
        raise RecursionError("simulated: ambient stack + nested payload")

    monkeypatch.setattr(mod, "decode_ad", boom)
    result = parse_salgsoppgave("<html><script>irrelevant</script></html>", "42")
    assert result == Salgsoppgave(finnkode="42")


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


@pytest.mark.parametrize(
    "prose, expected",
    [
        # Brief's own flagged precedence case: an unlisted negation phrasing
        # ('foreligger ikke') co-occurring with 'midlertidig brukstillatelse'
        # must still resolve to 'midlertidig', not 'ingen'.
        (
            "Det foreligger ikke ferdigattest, men midlertidig brukstillatelse foreligger.",
            "midlertidig",
        ),
        # Bug: this negation isn't one of the three allowlisted phrasings
        # ('foreligger ikke ferdigattest', 'ingen ferdigattest', 'ferdigattest
        # foreligger ikke'), so it fell through to the bare 'ferdigattest'
        # match and came out as the opposite of what the text says.
        (
            "Vi har dessverre ikke mottatt ferdigattest fra kommunen ennå.",
            "ingen",
        ),
        # Positive case, to prove the fix hasn't inverted the field.
        ("Ferdigattest foreligger for eiendommen.", "ferdigattest"),
        ("Ingenting relevant her.", None),
        # Regression: a common boilerplate disclaimer asserts the
        # certificate EXISTS ("ferdigattest eksisterer") but happens to
        # share a sentence with "ingen"/"ikke" about something unrelated
        # (that existence gives no guarantee about undocumented work). A
        # naive same-sentence negation search inverts this to 'ingen'.
        (
            "At ferdigattest eksisterer, gir ingen garanti for at det ikke "
            "er utført arbeid på boligen som ikke er byggemeldt eller godkjent.",
            "ferdigattest",
        ),
        # Regression: "Ferdigattesten omfatter ikke <X>" means a certificate
        # exists but its scope excludes a later addition -- not that no
        # certificate exists. This phrasing recurs across many ads, almost
        # always a few sentences after an explicit "Det foreligger
        # ferdigattest ... datert ...". Also exercises that the suffixed
        # form ("ferdigattesten", definite) is recognised as the same word.
        (
            "Ferdigattesten omfatter ikke endringer utført i etterkant.",
            "ferdigattest",
        ),
        # Regression: a cross-reference to a document heading ("se punktet
        # Ferdigattest") is not an assertion about the property; the nearby
        # "ikke" describes something else (the room), not the certificate.
        (
            "Rommet er ikke godkjent som rom for varig opphold, se punktet "
            "Ferdigattest for mer informasjon.",
            "ferdigattest",
        ),
        # Regression: a fixed statutory-citation disclaimer ("no completion
        # certificates are issued any more for pre-1998 construction, per
        # law") recurs near-verbatim across ads regardless of this specific
        # property's status, usually right after an explicit "Ferdigattest
        # er utstedt: <date>" for the property itself.
        (
            "Ferdigattest er utstedt: 19.10.2021. Ferdigattest utstedes "
            "ikke lenger for tiltak det er søkt om før 01.01.1998, jf. "
            "plan og bygningsloven § 21-10 femte ledd.",
            "ferdigattest",
        ),
        # Contrast: a property-specific negation that also happens to
        # mention 1998 must NOT be caught by the statutory-disclaimer
        # exclusion above -- the negated subject here is "Eiendommen", not
        # a generic "tiltak"/"bygg" class. (Resolves to 'midlertidig', not
        # 'ingen', because the sentence also names 'midlertidig
        # brukstillatelse' -- same precedence rule as the brief's case.)
        (
            "Eiendommen er oppført før 1998 og har ikke ferdigattest "
            "eller midlertidig brukstillatelse for eiendommen.",
            "midlertidig",
        ),
        # Regression: the same statutory disclaimer also appears in mirrored
        # word order ("På bygg ... ikke lenger utstedes ferdigattest").
        (
            "Boligen er godkjent 28.03.1989. På bygg som er oppført "
            "tidligere enn 1998, vil det ikke lenger utstedes ferdigattest, "
            "jfr. nye bestemmelser i plan- og bygningsloven fra 01.07.2015.",
            "ferdigattest",
        ),
        # Regression: "eller ikke" ('or not') is an indifference idiom, not
        # an assertion that no certificate exists.
        (
            "Boligen kan brukes som den fremstår i dag uansett om det "
            "foreligger ferdigattest eller ikke.",
            "ferdigattest",
        ),
        # Regression: sections are joined as "heading\ntext\nheading\ntext"
        # with no sentence-ending punctuation between them. A short negation
        # window ('uten' + up to 25 chars) must not leak across that
        # boundary and misattribute an unrelated 'uten samtykke' to a
        # 'Ferdigattest' heading that starts the very next section.
        (
            "Bruksrett til 6 bilparkeringsplasser\n"
            "Kan ikke endres uten samtykke fra Oslo kommune\n"
            "Ferdigattest / brukstillatelse\n"
            'Det foreligger ferdigattest for "Oppføring av kvartalsbebyggelse" datert 15.09.2017.',
            "ferdigattest",
        ),
        # Regression (finding 1, third round): the statutory-disclaimer
        # exclusion used to key off the negated noun's word class
        # ("tiltak"/"bygg\w*"), but "bygget" is ALSO the ordinary definite
        # form of "the building" -- a genuine, property-specific negation,
        # not the generic class the heuristic meant to catch. That
        # swallowed real negations and inverted them to "ferdigattest".
        # Only the sibling sentence naming "boligen" escaped, proving the
        # bug was the word class, not the surrounding grammar.
        (
            "Ferdigattest foreligger ikke for bygget, som ble oppført i 2015.",
            "ingen",
        ),
        (
            "Ferdigattest foreligger ikke for boligen, som ble oppført i 2015.",
            "ingen",
        ),
    ],
)
def test_ferdigattest_negation_handling(prose, expected):
    from skannonser.ingest.finn.parse_salgsoppgave import _ferdigattest

    assert _ferdigattest(prose) == expected


def test_ferdigattest_neg_checked_before_assert():
    """Pins the ordering (finding 2): '_FERDIGATTEST_NEG' must run before
    '_FERDIGATTEST_ASSERT' because ASSERT's 'foreligger...ferdigattest'
    pattern also matches negated text -- 'foreligger ikke ferdigattest'
    satisfies both regexes. If a future reorder checked ASSERT first, this
    would silently flip to 'ferdigattest' instead of 'ingen'."""
    from skannonser.ingest.finn.parse_salgsoppgave import (
        _FERDIGATTEST_ASSERT,
        _FERDIGATTEST_NEG,
        _ferdigattest,
    )

    prose = "Det foreligger ikke ferdigattest for eiendommen."
    assert _FERDIGATTEST_NEG.search(prose)
    assert _FERDIGATTEST_ASSERT.search(prose)
    assert _ferdigattest(prose) == "ingen"


@pytest.mark.parametrize(
    "prose, expected",
    [
        # Positive case, to prove the fix hasn't inverted the field.
        ("Utleie er tillatt for hele boligen.", "tillatt"),
        # Genuine negation must still be caught.
        (
            "Det er ikke tillatt med utleie av sokkelleiligheten i boligen.",
            "ikke_tillatt",
        ),
        # Bug: 'ei' is the feminine indefinite article ('a/an') in modern
        # Norwegian, not a negation. This sentence permits the rental; the
        # old '(?:ikke|ei)' alternation misread 'ei' as 'not' and returned
        # ikke_tillatt.
        (
            "Det er ei tillatt utleie av sokkelleiligheten i boligen.",
            None,
        ),
        ("Boligen har egen utleiedel i kjelleren.", "egen_enhet"),
    ],
)
def test_utleie_negation_handling(prose, expected):
    from skannonser.ingest.finn.parse_salgsoppgave import _utleie

    assert _utleie(prose) == expected


@pytest.mark.parametrize(
    "prose, expected",
    [
        # Bug: 'ikke tillatt uten styrets samtykke' is a conditional permit
        # (allowed with board consent), but _HUSDYR_NOT ('ikke tillatt') was
        # tested before _HUSDYR_GODKJENNING and matched first, collapsing
        # this common housing-co-op phrasing to a bare prohibition.
        (
            "Dyrehold er ikke tillatt uten styrets samtykke.",
            "krever_godkjenning",
        ),
        # Genuine prohibition must still be caught -- the precedence fix
        # must not make every 'ikke tillatt' resolve to godkjenning.
        ("Dyrehold er ikke tillatt i bygget.", "ikke_tillatt"),
        # Positive case, to prove the fix hasn't inverted the field.
        ("Dyrehold er tillatt.", "tillatt"),
    ],
)
def test_husdyr_negation_handling(prose, expected):
    from skannonser.ingest.finn.parse_salgsoppgave import _husdyr

    assert _husdyr(prose) == expected


# Regressions for the heading/list misclassification (measured over 300
# cached ads, seed 77: 132/300 classified "midlertidig", 90 of those with a
# body that explicitly asserted a full certificate exists). Root cause: the
# old `_flat_text` glued section HEADINGS onto bodies and searched the
# result for bare word presence. Norwegian brokers use a fixed heading that
# names both documents regardless of which the property actually has
# ("Midlertidig brukstillatelse og ferdigattest" / "Ferdigattest/
# midlertidig brukstillatelse"), so that heading alone short-circuited the
# field to "midlertidig" even when the body underneath clearly said
# otherwise. These exercise `_ferdigattest_scope` + `_ferdigattest` together
# via `parse_salgsoppgave`-shaped `Section` lists, since the bug was
# structural (which text reaches the classifier), not a phrasing gap.
def _ferdigattest_from_sections(sections):
    from skannonser.ingest.finn.parse_salgsoppgave import (
        _ferdigattest,
        _ferdigattest_scope,
    )

    return _ferdigattest(_ferdigattest_scope(sections))


def test_ferdigattest_ignores_the_standard_dual_heading():
    """Corpus case (464524286-style): the heading names both documents, but
    the body underneath unambiguously says a certificate was issued -- the
    heading must not be read as its own assertion."""
    secs = [
        Section(
            "Midlertidig brukstillatelse og ferdigattest",
            "Det foreligger ferdigattest datert 27.11.1974 som omhandler nybygg.",
        ),
    ]
    assert _ferdigattest_from_sections(secs) == "ferdigattest"


def test_ferdigattest_none_when_only_named_in_an_attachment_list():
    """Corpus case (405235986-style): 'Vedlegg til salgsoppgaven' sections
    enumerate document *names* the salgsoppgave includes, one per line, no
    verb -- not a statement about which document the property has."""
    secs = [
        Section(
            "Vedlegg til salgsoppgaven",
            "Vedtekter og eventuelle husordensregler\n"
            "Ferdigattest/midlertidig brukstillatelse\n"
            "Arealbekreftelse",
        ),
    ]
    assert _ferdigattest_from_sections(secs) is None


def test_ferdigattest_prefers_unnegated_certificate_over_historical_midlertidig():
    """Corpus case (449531664): a historical, superseded 'midlertidig
    brukstillatelse' from years earlier must not preempt a later, un-negated,
    repeated assertion that a full certificate now exists."""
    secs = [
        Section(
            "Midlertidig brukstillatelse og ferdigattest",
            "Det foreligger ferdigattest for boligblokk (hus A+B) datert 12.08.2025\n"
            "Det foreligger midlertidig brukstillatelse datert 05.07.2007. "
            "Og ferdigattesten kom 12.08.2025.\n"
            "Det foreligger ferdigattest for rehabilitering av fasader 02.02.2024.",
        ),
    ]
    assert _ferdigattest_from_sections(secs) == "ferdigattest"


def test_ferdigattest_scope_falls_back_when_no_dedicated_section():
    """Not every ad has a section headed about the topic; the field must
    still fire from a scattered mention rather than going permanently None,
    as long as it isn't inside an attachment list."""
    secs = [
        Section("Innhold", "Boligen er et rekkehus med ferdigattest datert 10.04.1957."),
        Section("Vedlegg til salgsoppgaven", "Ferdigattest/midlertidig brukstillatelse"),
    ]
    assert _ferdigattest_from_sections(secs) == "ferdigattest"


@pytest.mark.parametrize(
    "prose, expected",
    [
        # Must-not-break cases from the brief, re-asserted directly against
        # `_ferdigattest` (independent of the section-scoping fix above).
        (
            "Det foreligger ikke ferdigattest, men midlertidig brukstillatelse.",
            "midlertidig",
        ),
        (
            "Vi har dessverre ikke mottatt ferdigattest fra kommunen ennå.",
            "ingen",
        ),
        ("Ferdigattest er utstedt 12.05.2019.", "ferdigattest"),
    ],
)
def test_ferdigattest_must_not_break_cases(prose, expected):
    from skannonser.ingest.finn.parse_salgsoppgave import _ferdigattest

    assert _ferdigattest(prose) == expected
