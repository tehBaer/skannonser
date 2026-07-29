"""Decoding the app-state payload FINN ships on every ad page, both formats."""
import json
from pathlib import Path

import pytest

from skannonser.ingest.finn.payload import Section, _resolve, decode_ad, sections

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


def _load(name):
    return (FIXTURES / f"{name}.html").read_text(encoding="utf-8", errors="replace")


def test_decodes_turbostream_format():
    ad = decode_ad(_load("448347467"))
    assert ad is not None
    assert ad["constructionYear"]
    assert isinstance(ad["generalText"], list)


def test_decodes_remix_format():
    """Older cached pages use window.__remixContext = {...} instead."""
    ad = decode_ad(_load("432672475"))
    assert ad is not None
    assert isinstance(ad["generalText"], list)
    assert len(ad["generalText"]) > 0


def test_sections_are_plaintext_with_headings():
    secs = sections(decode_ad(_load("448347467")))
    assert secs
    assert all(isinstance(s, Section) for s in secs)
    assert all("<" not in s.text for s in secs), "HTML tags must be stripped"
    assert any(s.heading for s in secs)


def test_sections_unescape_entities():
    """Labels like 'Utvendig &gt; Veggkonstruksjon' must come back as '>'."""
    for name in ("448347467", "432672475"):
        for s in sections(decode_ad(_load(name))):
            assert "&gt;" not in s.text
            assert "&amp;" not in s.text


def test_development_project_has_no_sections():
    """New-build projects genuinely carry no salgsoppgave; not a parse failure."""
    ad = decode_ad(_load("445242445"))
    assert ad is not None
    assert sections(ad) == []


def test_page_without_payload_returns_none():
    """A FINN search-results page carries no ad[.html]/homes.ad loaderData route.

    NOTE: the brief's own draft named "211471492" here, but that fixture is a
    fully-decodable ordinary ad (its title/price match its golden
    211471492.expected.json byte for byte) -- not a payload-less page. Swapped
    to result_page1.html, an already-present, structurally-different fixture
    (a search-results page, used by test_finn_crawl.py) confirmed to have no
    ad loaderData route at all.
    """
    assert decode_ad(_load("result_page1")) is None


@pytest.mark.parametrize(
    "junk",
    ["", "<html></html>", "<script>window.__remixContext = {broken</script>",
     '<script>enqueue("[not json")</script>', "<script>enqueue(</script>"],
)
def test_malformed_input_returns_none_never_raises(junk):
    assert decode_ad(junk) is None


def test_sections_tolerates_garbage_ad():
    assert sections({}) == []
    assert sections({"generalText": None}) == []
    assert sections({"generalText": ["not-a-dict"]}) == []


def test_sections_tolerates_non_string_text_unsafe():
    """textUnsafe of the wrong type must degrade like heading already does,
    not raise TypeError out of the regex substitutions."""
    secs = sections({"generalText": [{"textUnsafe": 123, "heading": "h"}]})
    assert secs == [Section("h", "123")]

    secs = sections({"generalText": [{"textUnsafe": ["x"], "heading": "h"}]})
    assert secs == [Section("h", "['x']")]

    secs = sections({"generalText": [{"textUnsafe": None, "heading": "h"}]})
    assert secs == [Section("h", "")]


def test_resolve_cycle_protection_returns_none_not_infinite_loop():
    """Pin the existing `seen` guard: a self-referential list resolves its
    cyclic slot to None instead of recursing forever."""
    arr = [[0]]  # index 0 is a list containing a reference to itself
    assert _resolve(arr, 0, frozenset()) == [None]


def test_resolve_bounds_chain_depth_without_recursion_error():
    """A turbo-stream payload with a long chain of distinct nested lists never
    revisits an index, so the cycle-detecting `seen` set does not stop it --
    only an explicit depth bound does. Reproduces the RecursionError."""
    n = 5000
    arr = [[i + 1] for i in range(n - 1)] + [[]]
    result = _resolve(arr, 0, frozenset())  # must not raise RecursionError
    assert result is None or isinstance(result, list)


def test_decode_ad_survives_deeply_nested_turbostream_payload():
    """End-to-end: decode_ad must degrade to None, never raise, on a
    pathologically deep turbo-stream payload."""
    n = 5000
    arr = [[i + 1] for i in range(n - 1)] + [[]]
    inner = json.dumps(arr)
    script = f"window.__reactRouterContext.streamController.enqueue({json.dumps(inner)})"
    html = f"<html><body><script>{script}</script></body></html>"
    assert decode_ad(html) is None


def test_resolve_rejects_boolean_index():
    """bool is an int subclass in Python; True/False must not be accepted as
    turbo-stream array indices even though isinstance(True, int) is True."""
    arr = [10, 20, 30]
    assert _resolve(arr, True, frozenset()) is None
    assert _resolve(arr, False, frozenset()) is None
