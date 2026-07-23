"""Status refresh: re-download listings from FINN and record status changes.

Port of `main/sync/refresh_listings.py` (`refresh_listing`, `_normalize_status`,
`refresh_all_listings`, lines 24-213) plus the row-selection semantics of
`main/database/db.py:926-1006` (`get_eiendom_for_status_refresh`,
`get_stale_eiendom_for_status_refresh`).

Three selection modes (`skannonser run refresh --mode ...`):

- "all": every listing with a non-empty URL (active or not), no price/area
  filter. Port of `get_eiendom_for_status_refresh(only_inactive=False)`
  (db.py:936-956), ordered `active ASC, scraped_at DESC`.
- "inactive": only `active = 0` listings, scoped by the domain's
  `sheets_max_price`/`min_bra_i` filters (`load_domain().filters`, same
  values as legacy's `SHEETS_MAX_PRICE`/`MIN_BRA_I`). Port of
  `get_stale_eiendom_for_status_refresh` (db.py:958-1006), ordered
  `scraped_at DESC`. This mode does NOT exclude already-closed
  (Solgt/Inaktiv) listings -- that's what "stale-open" is for.
- "stale-open": the "inactive" scope, further excluding listings whose
  CURRENT (pre-refresh) status is already 'Solgt' or 'Inaktiv'
  (case-insensitive) -- those are already known-closed, so re-checking them
  wastes a request. Port of the composition legacy used for this purpose:
  `refresh_all_listings(only_inactive=True, exclude_statuses=['Solgt',
  'Inaktiv'])` (refresh_listings.py:90-101) layered on the same
  `get_stale_eiendom_for_status_refresh` scope.

Every mode force-refetches via `html_cache.load_or_fetch(..., force=True)`
(legacy's `force_save=True` in `refresh_listing`) -- bypassing the cache is
the entire point of a refresh run, since the cache would otherwise mask a
status change.

`refresh_listings` never touches `active` / `mark_inactive` -- that lifecycle
belongs exclusively to `run_finn_ingest` (see `skannonser/pipeline.py`'s
module docstring). This function only updates `tilgjengelighet` and appends
to `eiendom_status_history`.
"""

import sqlite3
import time
from pathlib import Path
from typing import Callable

from skannonser.config.domain import DomainConfig
from skannonser.http import browser_get
from skannonser.ingest.finn import html_cache
from skannonser.ingest.finn import parse as finn_parse
from skannonser.ingest.finn import parse_details as finn_parse_details
from skannonser.store.repositories.details import DetailsRepo
from skannonser.store.repositories.listings import ListingsRepo

MODES: tuple[str, ...] = ("all", "inactive", "stale-open")

# Case-insensitive, matching db.py:1074's
# `LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) IN ('solgt', 'inaktiv')`.
_CLOSED_STATUSES_SQL = "('solgt', 'inaktiv')"


def _select_rows(conn: sqlite3.Connection, domain: DomainConfig, mode: str):
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES} (got {mode!r})")

    if mode == "all":
        # Port of get_eiendom_for_status_refresh(only_inactive=False)
        # (db.py:936-956): every listing with a non-empty url, regardless of
        # active/status, no price/area filter.
        query = (
            "SELECT finnkode, url, tilgjengelighet FROM eiendom "
            "WHERE url IS NOT NULL AND TRIM(url) != '' "
            "ORDER BY active ASC, scraped_at DESC"
        )
        return conn.execute(query).fetchall()

    # "inactive" and "stale-open" both start from
    # get_stale_eiendom_for_status_refresh's scope (db.py:958-1006):
    # active=0, a non-empty url, and the domain's sheet price/area filters.
    query = (
        "SELECT finnkode, url, tilgjengelighet FROM eiendom "
        "WHERE active = 0 AND url IS NOT NULL AND TRIM(url) != '' "
        "AND pris <= ? AND CAST(info_usable_i_area AS REAL) >= ?"
    )
    params: list = [domain.filters.sheets_max_price, domain.filters.min_bra_i]

    if mode == "stale-open":
        query += (
            f" AND LOWER(TRIM(COALESCE(tilgjengelighet, ''))) NOT IN {_CLOSED_STATUSES_SQL}"
        )

    query += " ORDER BY scraped_at DESC"

    return conn.execute(query, params).fetchall()


def refresh_listings(
    conn: sqlite3.Connection,
    domain: DomainConfig,
    project_dir: Path,
    mode: str,
    fetch=browser_get,
    fetch_delay: Callable[[], None] | None = None,
    listing_delay: Callable[[], None] | None = None,
) -> dict:
    """Re-download every selected listing's ad page, update its
    `tilgjengelighet`, and append to `eiendom_status_history` only where the
    status actually changed.

    Port of `refresh_all_listings` (main/sync/refresh_listings.py:63-189).
    A fetch/parse failure for one listing (network error, unparsable page)
    is caught and counted in `errors` -- mirroring `refresh_listing`'s
    try/except -- without updating that listing's status or aborting the
    rest of the batch.

    `listing_delay` paces BETWEEN listings, in addition to `fetch_delay`'s
    own per-fetch pacing inside `html_cache.load_or_fetch` -- legacy runs
    both: a 0.1s force-fetch sleep inside `load_or_fetch_ad_html`, plus a
    separate `time.sleep(delay)` (default 0.2s) between listings in
    `refresh_all_listings` itself (main/sync/refresh_listings.py:65,167-168).
    It fires after every listing except the last, matching legacy's
    `if current_num < total: time.sleep(delay)` placement exactly.

    Returns `{"candidates", "refreshed", "status_changed", "errors"}`.
    """
    project_dir = Path(project_dir)
    rows = _select_rows(conn, domain, mode)
    repo = ListingsRepo(conn)
    details_repo = DetailsRepo(conn)

    candidates = len(rows)
    refreshed = 0
    status_changed = 0
    errors = 0

    for i, row in enumerate(rows):
        finnkode = str(row["finnkode"]).strip()
        url = row["url"]
        old_status = row["tilgjengelighet"]

        try:
            html = html_cache.load_or_fetch(
                url, project_dir, finnkode, fetch=fetch, fetch_delay=fetch_delay, force=True
            )
            listing = finn_parse.parse_ad(html, finnkode, url)
            new_status = listing.Tilgjengelighet
        except Exception:
            errors += 1
        else:
            repo.update_status(finnkode, new_status)
            if repo.record_status_change_if_changed(finnkode, old_status, new_status):
                status_changed += 1
            refreshed += 1

            # Re-parse details off the fresh HTML too -- felleskost/totalpris
            # changes ride along with the status refresh for free. Best-effort.
            try:
                details_repo.upsert_details(
                    [finn_parse_details.parse_details(html, finnkode)]
                )
            except Exception:
                pass

        if i < candidates - 1:
            if listing_delay is not None:
                listing_delay()
            else:
                time.sleep(0.2)

    return {
        "candidates": candidates,
        "refreshed": refreshed,
        "status_changed": status_changed,
        "errors": errors,
    }
