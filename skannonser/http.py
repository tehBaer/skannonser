"""Shared polite-HTTP helpers for the scraping paths.

Two concerns live here, both about *how* the scanner talks to third-party
sites (FINN, DNB) rather than *what* it fetches:

- `browser_get` sends a real browser `User-Agent` instead of the default
  `python-requests/x.y` string. That default is the single most
  bot-identifying signal a low-volume personal scraper emits; a residential
  IP plus a browser UA plus slow, jittered pacing is what makes the traffic
  read as a person rather than a script. `browser_get` is signature-
  compatible with `requests.get` (forwards `params`/`timeout`/etc.), so it
  drops in as the default `fetch` on any path that currently uses
  `requests.get`.

- `jittered_delay` builds the inter-request pacing callables (`page_delay`,
  `fetch_delay`, `listing_delay`) the crawl/refresh code already accepts,
  from a min/max range. Wide, randomized gaps keep the footprint gentle and
  human-shaped; the ranges are configured in `domain.toml`'s `[crawl]`
  section (see `skannonser.config.domain.Crawl`).
"""

import random
import time
from typing import Callable

import requests

# A current, ordinary desktop-Chrome UA. Kept deliberately unremarkable --
# the goal is to look like a normal browser, not to fingerprint-evade.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Generous default so a slow, politely-paced fetch is never cut short.
BROWSER_TIMEOUT = 30.0


def browser_get(url, *, _transport=requests.get, **kwargs):
    """GET `url` with a browser `User-Agent`, forwarding any `requests.get`
    kwargs (`params`, `timeout`, ...).

    A caller-supplied `headers` mapping is merged over the browser UA, so a
    path that sets its own `User-Agent` (e.g. the thumbnail fetch) keeps it.
    An explicit `timeout` wins over `BROWSER_TIMEOUT`. `_transport` is a
    private injection seam for tests.
    """
    headers = {"User-Agent": BROWSER_USER_AGENT}
    headers.update(kwargs.pop("headers", None) or {})
    kwargs.setdefault("timeout", BROWSER_TIMEOUT)
    return _transport(url, headers=headers, **kwargs)


def jittered_delay(
    min_seconds: float,
    max_seconds: float,
    *,
    _sleep: Callable[[float], None] = time.sleep,
    _rand: Callable[[float, float], float] = random.uniform,
) -> Callable[[], None]:
    """Return a no-arg callable that sleeps a uniformly-random duration in
    `[min_seconds, max_seconds]` each time it is called.

    Shaped to plug straight into the `page_delay`/`fetch_delay`/
    `listing_delay` hooks in `finn.crawl`/`finn.refresh`/`html_cache`.
    `_sleep`/`_rand` are private injection seams for tests.
    """
    if min_seconds < 0 or max_seconds < min_seconds:
        raise ValueError(
            f"invalid delay range: min={min_seconds}, max={max_seconds}"
        )

    def _delay() -> None:
        _sleep(_rand(min_seconds, max_seconds))

    return _delay
