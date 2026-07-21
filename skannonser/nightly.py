"""Nightly orchestrator: the `make full` / `~/run_skannonser_daily.sh`
replacement, plus the Sheets publish step it ends with.

Legacy's cron wrapper (`~/run_skannonser_daily.sh`, phase-2 log knowledge)
ran three INDEPENDENT sections -- [A] full (crawl+enrich), [B]
refresh-stale-open, [C] sold-sync -- such that a failure in one section was
recorded but never prevented the next section from running (a crawl outage
must not also skip the day's stale-listing refresh or sheet publish). This
module mirrors that at the level of eight concrete steps, run strictly in
order but each independently try/except'd: ingest_finn, ingest_dnb, geocode,
enrich(targets=all), enrich(targets=mvv_uni), enrich_dnb, refresh
(stale-open), sheets. No step's failure skips a LATER step -- none of these
eight steps has a same-run data dependency on an earlier one succeeding (each
one operates on whatever is currently in the DB), so "intra-section
dependencies skip conservatively" (the general wrapper policy) never actually
fires for this concrete step list; it is called out here for the next
maintainer who adds a step that DOES depend on an earlier one.

Two distinct BudgetExceeded shapes are normalized into one outcome
("budget_exhausted", not a failure -- the monthly Routes/Geocode cap is an
administrative stop, not a bug):

  * `run_geocode` / `run_dnb_travel` let `BudgetExceeded` propagate straight
    out of the row loop (row being processed when the cap hit is left
    untouched).
  * `run_enrich` catches it internally and returns its stats dict with
    `stats["budget_exhausted"] = True` instead of raising.

`_run_step` below handles both: it catches a raised `BudgetExceeded`, AND
inspects a normally-returned stats dict for the `budget_exhausted` key.

`notify-daily` is NOT a nightly step -- legacy ran it off its own separate
07:00 cron (`skannonser notify daily`, Task 5), and stays that way here.

Zero-url ingest guard (pipeline.py's guard 1/2, `run_finn_ingest`/
`run_dnb_ingest`'s own module docstring): those two functions ALREADY skip
`mark_inactive`/`deactivate_missing` internally when the crawl finds zero
urls or the parse-failure rate exceeds `FAILURE_RATE_THRESHOLD` -- that
protection has already happened by the time this module ever sees the
returned stats. This module's only remaining job, mirroring
`skannonser/commands/run_cmd.py`'s `_crawled_ok`/`_failure_rate_ok` CLI-level
checks, is to classify that outcome as a nightly STEP failure too (so
`skannonser run nightly`'s exit code and step report surface it exactly like
`skannonser run ingest` already does) -- no additional "skip mark-dependent
steps" logic is needed here, because no later nightly step reads
`mark_inactive`'s output as an input.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable

import requests

from skannonser.config.domain import DomainConfig
from skannonser.enrich.dnb_travel import run_dnb_travel
from skannonser.enrich.geocode import run_geocode
from skannonser.enrich.travel import run_enrich
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.ingest.finn.refresh import refresh_listings
from skannonser.pipeline import FAILURE_RATE_THRESHOLD, run_dnb_ingest, run_finn_ingest
from skannonser.publish.export import dnb_rows, eie_rows, sold_rows, stations_rows

# Matches skannonser/commands/run_cmd.py's `ingest` command defaults exactly
# (separate archive dir from legacy's own, see that module's docstring).
_FINN_PROJECT_DIR = Path("data/eiendom")
_FINN_ARCHIVE_DIR = _FINN_PROJECT_DIR / "html_crawled_rebuild"

# ---------------------------------------------------------------------------
# run_sheets / the shared publish step
# ---------------------------------------------------------------------------


def _publish(conn: sqlite3.Connection, *, client=None, sheets_writer=None) -> dict:
    """Build every tab's (header, rows) payload and either rewrite it via
    `client` or hand it to `sheets_writer(tab, header, rows)` (the
    `--dry-run-sheets` hook -- Task 10 uses this to write JSON payload files
    instead of touching a real spreadsheet). Exactly one of the two is used
    per call; `sheets_writer` wins when given.

    The builder list is assembled here (call time), not at module import, so
    monkeypatching `skannonser.nightly.eie_rows` etc. (the same pattern used
    for every other pipeline function this module calls) takes effect --
    a module-level constant built from the imported names once at import
    time would freeze stale references instead.

    Each tab's write is its own try/except -- a mid-loop exception (e.g. the
    3rd of 4 tabs) must not discard the tabs already rewritten in the live
    spreadsheet. On success this returns a flat `{tab: {"rows", "cells"}}`
    mapping, same as before. On a tab failure it instead returns
    `{"tabs": {...tabs completed before the failure...}, "failed_tab": name,
    "error": str, "unattempted": [names not yet tried]}` -- `_run_step`
    recognizes this shape and records the step as failed while still
    surfacing exactly what did and didn't get published.
    """
    builders: list[tuple[str, Callable[[sqlite3.Connection], tuple[list[str], list[list]]]]] = [
        ("Eie", eie_rows),
        ("Sold", sold_rows),
        ("DNB", dnb_rows),
        ("Stations", stations_rows),
    ]
    tabs: dict = {}
    for i, (tab, builder) in enumerate(builders):
        header, rows = builder(conn)
        try:
            if sheets_writer is not None:
                sheets_writer(tab, header, rows)
                tabs[tab] = {"rows": len(rows), "cells": 0}
            else:
                cells = client.rewrite_tab(tab, [header] + rows)
                tabs[tab] = {"rows": len(rows), "cells": cells}
        except Exception as exc:  # noqa: BLE001 - recorded, not swallowed silently
            return {
                "tabs": tabs,
                "failed_tab": tab,
                "error": str(exc),
                "unattempted": [t for t, _ in builders[i + 1 :]],
            }
    return tabs


def run_sheets(conn: sqlite3.Connection, client) -> dict:
    """Rewrite the Eie, Sold, DNB, and Stations tabs from the current DB
    state (Task 3's builders), via `client.rewrite_tab`. Returns per-tab
    `{"rows": n, "cells": n}` counts on success, or on mid-tab failure
    returns `{"tabs": {...tabs completed...}, "failed_tab": name,
    "error": str, "unattempted": [tab names not yet tried]}` -- the partial
    failure shape ensures already-published tabs' stats are preserved and
    exactly which tabs didn't get attempted are clear."""
    return _publish(conn, client=client)


# ---------------------------------------------------------------------------
# run_nightly
# ---------------------------------------------------------------------------


def _record_exception(steps: dict, failed: list, name: str, exc: Exception) -> None:
    steps[name] = {"ok": False, "error": str(exc)}
    failed.append(name)


def _record_budget_exhausted(steps: dict, budget_exhausted: list, name: str, stats: dict) -> None:
    steps[name] = {"ok": True, "stats": stats}
    budget_exhausted.append(name)


def _run_step(
    steps: dict, failed: list, budget_exhausted: list, name: str, fn: Callable[[], dict]
) -> None:
    """Run one nightly step, normalizing both BudgetExceeded shapes (see
    module docstring) into a `budget_exhausted` outcome, and any other
    exception into a recorded failure -- never re-raising, so the caller
    always proceeds to the next step."""
    try:
        stats = fn()
    except BudgetExceeded:
        _record_budget_exhausted(steps, budget_exhausted, name, {"budget_exhausted": True})
        return
    except Exception as exc:  # noqa: BLE001 - deliberately broad, see docstring
        _record_exception(steps, failed, name, exc)
        return

    if isinstance(stats, dict) and stats.get("budget_exhausted"):
        _record_budget_exhausted(steps, budget_exhausted, name, stats)
        return

    # `_publish`'s partial-failure shape (see its docstring): a mid-loop tab
    # write blew up, but `stats` still carries the tabs completed before it.
    # Record the step as failed WITHOUT collapsing that partial state down
    # to just an error string (that's exactly what Fix 1 corrects).
    if isinstance(stats, dict) and "failed_tab" in stats:
        steps[name] = {"ok": False, "error": stats["error"], "stats": stats}
        failed.append(name)
        return

    steps[name] = {"ok": True, "stats": stats}


def _run_ingest_step(steps: dict, failed: list, name: str, fn: Callable[[], dict]) -> None:
    """Ingest steps (finn/dnb) don't raise BudgetExceeded, but a zero-url
    crawl or a too-high parse-failure rate is a step failure for reporting
    purposes even though `run_finn_ingest`/`run_dnb_ingest` themselves
    already protected the active set internally (see module docstring)."""
    try:
        stats = fn()
    except Exception as exc:  # noqa: BLE001
        _record_exception(steps, failed, name, exc)
        return

    crawled = stats.get("crawled", 0)
    if crawled == 0:
        steps[name] = {"ok": False, "error": f"{name}: crawl returned zero URLs"}
        failed.append(name)
        return

    rate = stats.get("failed", 0) / crawled
    if rate > FAILURE_RATE_THRESHOLD:
        steps[name] = {
            "ok": False,
            "error": (
                f"{name}: parse-failure rate {stats['failed']}/{crawled} "
                f"({rate:.0%}) exceeds {FAILURE_RATE_THRESHOLD:.0%} threshold"
            ),
        }
        failed.append(name)
        return

    steps[name] = {"ok": True, "stats": stats}


def run_nightly(
    conn: sqlite3.Connection,
    domain: DomainConfig,
    gateway: Gateway,
    api_key: str,
    client,
    fetch=requests.get,
    post=requests.post,
    sheets_writer=None,
) -> dict:
    """The legacy `make full` replacement: ingest finn -> ingest dnb ->
    geocode -> enrich(all) -> enrich(mvv_uni) -> enrich_dnb ->
    refresh(stale-open) -> sheets, run strictly in this order. Every step is
    independently try/except'd (see module docstring) -- no step's failure
    prevents any later step from running.

    `client` is the `SheetsClient` used for the sheets step, UNLESS
    `sheets_writer` is given, in which case it is used instead (the
    `--dry-run-sheets` hook; `client` may then be `None`).

    Returns `{"steps": {name: {"ok": bool, "stats": {...}} | {"ok": False,
    "error": str}}, "failed": [names], "budget_exhausted": [names]}`.
    """
    steps: dict = {}
    failed: list = []
    budget_exhausted: list = []

    _run_ingest_step(
        steps,
        failed,
        "ingest_finn",
        lambda: run_finn_ingest(
            domain, conn, _FINN_PROJECT_DIR, fetch=fetch, archive_dir=_FINN_ARCHIVE_DIR
        ),
    )
    _run_ingest_step(
        steps,
        failed,
        "ingest_dnb",
        lambda: run_dnb_ingest(domain, conn, fetch=fetch),
    )
    _run_step(
        steps,
        failed,
        budget_exhausted,
        "geocode",
        lambda: run_geocode(conn, domain, gateway, api_key, get=fetch),
    )
    _run_step(
        steps,
        failed,
        budget_exhausted,
        "enrich_all",
        lambda: run_enrich(conn, domain, gateway, api_key, targets="all", post=post),
    )
    _run_step(
        steps,
        failed,
        budget_exhausted,
        "enrich_mvv_uni",
        lambda: run_enrich(conn, domain, gateway, api_key, targets="mvv_uni", post=post),
    )
    _run_step(
        steps,
        failed,
        budget_exhausted,
        "enrich_dnb",
        lambda: run_dnb_travel(conn, domain, gateway, api_key, post=post),
    )
    _run_step(
        steps,
        failed,
        budget_exhausted,
        "refresh",
        lambda: refresh_listings(
            conn, domain, _FINN_PROJECT_DIR, mode="stale-open", fetch=fetch
        ),
    )
    # Sheets ALWAYS attempts -- it publishes whatever state the DB is
    # currently in, regardless of how many earlier steps failed.
    _run_step(
        steps,
        failed,
        budget_exhausted,
        "sheets",
        lambda: _publish(conn, client=client, sheets_writer=sheets_writer),
    )

    return {"steps": steps, "failed": failed, "budget_exhausted": budget_exhausted}
