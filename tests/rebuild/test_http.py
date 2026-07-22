"""Tests for the shared polite-HTTP helpers (`skannonser/http.py`): the
browser User-Agent fetch used on the FINN/DNB scraping paths, and the
jittered inter-request delay factory used to pace crawls.
"""

import pytest

from skannonser import http as http_mod
from skannonser.http import BROWSER_USER_AGENT, browser_get, jittered_delay


def test_browser_user_agent_is_a_real_browser_string():
    # The whole point: never send the self-identifying `python-requests/x.y`
    # default UA to a classifieds site.
    assert "Mozilla" in BROWSER_USER_AGENT
    assert "python-requests" not in BROWSER_USER_AGENT.lower()


def test_browser_get_sends_browser_user_agent_and_default_timeout():
    calls = []

    def fake_transport(url, **kwargs):
        calls.append((url, kwargs))
        return "resp"

    result = browser_get("https://example.test/a", _transport=fake_transport)

    assert result == "resp"
    (url, kwargs) = calls[0]
    assert url == "https://example.test/a"
    assert kwargs["headers"]["User-Agent"] == BROWSER_USER_AGENT
    assert kwargs["timeout"] == http_mod.BROWSER_TIMEOUT


def test_browser_get_forwards_params_and_respects_explicit_timeout():
    calls = []

    def fake_transport(url, **kwargs):
        calls.append(kwargs)
        return "resp"

    browser_get(
        "https://example.test/geo",
        params={"address": "x"},
        timeout=10.0,
        _transport=fake_transport,
    )

    kwargs = calls[0]
    assert kwargs["params"] == {"address": "x"}
    # An explicitly passed timeout must win over the browser default.
    assert kwargs["timeout"] == 10.0
    assert kwargs["headers"]["User-Agent"] == BROWSER_USER_AGENT


def test_browser_get_lets_caller_headers_override_user_agent():
    calls = []

    def fake_transport(url, **kwargs):
        calls.append(kwargs)
        return "resp"

    browser_get(
        "https://example.test/thumb",
        headers={"User-Agent": "thumbs/1.0"},
        _transport=fake_transport,
    )

    # A caller that sets its own UA (e.g. the thumbnail fetch) keeps it.
    assert calls[0]["headers"]["User-Agent"] == "thumbs/1.0"


def test_jittered_delay_sleeps_a_value_within_range():
    slept = []
    delay = jittered_delay(
        2.0,
        8.0,
        _sleep=slept.append,
        _rand=lambda lo, hi: (lo + hi) / 2,
    )

    delay()

    assert slept == [5.0]


def test_jittered_delay_draws_from_the_configured_bounds():
    drawn = []

    def fake_rand(lo, hi):
        drawn.append((lo, hi))
        return lo

    delay = jittered_delay(1.5, 4.5, _sleep=lambda s: None, _rand=fake_rand)
    delay()

    assert drawn == [(1.5, 4.5)]


def test_jittered_delay_rejects_inverted_bounds():
    with pytest.raises(ValueError):
        jittered_delay(5.0, 1.0)
