"""DNB Eiendom travel enrichment: fill missing BRJ/MVV commute times directly
on the ``dnbeiendom`` table.

Ports ``scripts/backfill_dnbeiendom_travel_to_sheet.py`` (387 lines), whose
default (and only production-used, per the plan brief) invocation is
``--target all`` -- resolved by ``TRAVEL_COLS_BY_TARGET["all"] = ["PENDL RUSH
BRJ", "PENDL RUSH MVV"]`` (script.py:17-22) to BRJ+MVV only. ``mvv_uni`` is a
selectable ``--target`` on that script, but nothing in the repo invokes it for
DNB, and ``post_process_eiendom``'s own "all" resolution (post_process.py:293-
299: ``run_mvv_uni = target_value == "mvv_uni"``, never true for "all") agrees
-- mirrors ``skannonser.enrich.travel``'s ``Destination.exclusive`` handling,
which excludes ``mvv_uni`` from "all" for the exact same reason. This port
hard-codes BRJ+MVV; it never touches ``mvv_uni``.

Candidate selection (script.py:236-252, its only SQL query):

    SELECT d.* FROM dnbeiendom d
    WHERE d.active = 1
      AND (d.duplicate_of_finnkode IS NULL OR TRIM(d.duplicate_of_finnkode) = '')

i.e. active, NOT matched to a FINN listing. A DNB row WITH a
``duplicate_of_finnkode`` is a duplicate of an already-tracked FINN listing --
it inherits FINN's travel values at export time (Task 3's sheet-merge logic),
never gets its own row computed, and is excluded here exactly as legacy
excludes it from this SQL in the first place (not filtered out later --
never selected at all). Within that candidate set, per-destination candidacy
is "value is missing" (``PENDL RUSH BRJ``/``PENDL RUSH MVV`` is NaN in the
work frame built at script.py:95-135, straight off ``dnbeiendom``/joined
``eiendom_processed`` columns) -- the same missing-value-only candidacy
``skannonser.enrich.travel`` documents for the main ``eiendom`` table
(``is_candidate = row["values"].get(df_col) is None``).

Address construction (script.py:113-114): ``Adresse``/``Postnummer`` come
straight off the DNB row's own ``adresse``/``postnummer`` columns (already
normalized by ``DnbRepo`` on ingest) -- no DNB-specific ``StreetAddress``/
``PostalCode`` fields are re-read. The travel call itself
(``main.location_features.PublicTransitCommuteTime`` in legacy) is the exact
same public-transit Routes-API client ``skannonser.enrich.travel_api
.TransitCommute`` already ports for the main ``eiendom`` enrichment
(``skannonser.enrich.travel``) -- same request shape
(``f"{address}, {postnummer}, Norway"``), same BRJ/MVV destination addresses
(``config/domain.toml``'s ``[[destinations]]`` entries, shared across both
tables), same sentinel semantics.

Donor/reuse participation -- INTENTIONALLY DROPPED (sanctioned scope
narrowing, see the report for the full writeup): legacy's script *does* wire
this DNB backfill into the shared donor-reuse system
(``post_process_eiendom(..., donor_seed_df=_build_shared_donor_seed(db),
travel_targets=args.target)`` at script.py:262-270, followed by its own
``_fill_missing_from_donor_seed`` all-or-nothing donor copy at script.py:173-
203). Tracing that machinery end-to-end for the BRJ+MVV-only case that this
script always runs surfaces a real, provable legacy gap rather than behavior
worth mirroring:

  * For plain (non-``mvv_uni``) destinations, ``post_process_eiendom`` never
    writes a donor's *value* into the acceptor's own ``PENDL RUSH BRJ``/``PENDL
    RUSH MVV`` cell -- it only sets ``TRAVEL_COPY_FROM_FINNKODE`` (post_process
    .py:891-898/990-996 assign a link; the value stays NaN; only the
    ``mvv_uni`` branch, post_process.py:1136-1176, resolves+writes a donor
    value in-loop). Legacy's *other* caller of this same function
    (the main FINN/eiendom pipeline) relies on ``db.py``'s
    ``get_eiendom_for_sheets`` CASE/COALESCE join (db.py:829-856) to resolve
    that link into a value at sheet-export time -- but this DNB script never
    calls that helper. It builds its own ``processed_map`` straight off the
    in-memory frame ``post_process_eiendom`` returned (script.py:276-281), so
    a donor-linked-but-unresolved row's cell is empty there.
  * The script's own resolution attempt, ``_fill_missing_from_donor_seed``
    (script.py:173-203), is a *separate*, stricter, all-or-nothing lookup:
    it only fires against the pre-run ``donor_seed_df`` snapshot (never the
    same-run ``processed`` frame, so a sibling DNB row's freshly-successful
    API call this same run is never available as a donor for a later row --
    it would only become visible on a *subsequent* run, once persisted), and
    it requires the donor to have *both* BRJ and MVV already complete. But
    the assignment step that decided to skip the BRJ API call in the first
    place used ``donor_cache_brj`` -- a BRJ-*only*-complete cache
    (post_process.py:815-816, unconditional, independent of whether MVV is
    also running) -- not the BRJ+MVV-complete ``donor_cache_all`` that
    resolution actually requires. So a BRJ-only-complete donor legitimately
    skips the BRJ API call at assignment time, then fails the all-or-nothing
    check at resolution time whenever that donor lacks MVV -- the row is left
    with the API call skipped AND no value written, silently. Every run this
    keeps recurring for that row.
  * On top of that, ``dnbeiendom`` (migration 004) carries no
    ``travel_copy_from_finnkode`` column at all -- there is nowhere in the
    new schema to persist a donor pointer for a DNB row, so even the "works
    when the donor turns out to already be BRJ+MVV-complete" happy path has
    no cross-run analogue here (legacy only made this workable at all because
    it persisted DNB rows back into ``eiendom_processed`` under a synthetic
    ``dnb_id``/``DNB-<id>`` finnkode -- migration 004 retires that hack by
    giving DNB its own real columns; re-introducing the synthetic-finnkode
    write-back to keep donor bookkeeping alive would resurrect exactly the
    awkwardness migration 004 exists to remove).

Given (a) the brief's own candidacy description for this port never mentions
donor reuse ("active, unmatched-to-finn, missing values"), (b) the schema
this port targets has no slot to store a donor pointer, and (c) the
mechanism, traced fully, mostly produces skip-with-no-value dead ends for the
one destination (BRJ) this script always runs -- ``run_dnb_travel`` makes a
straight Routes API call for every missing-value candidate, no donor lookup,
no reuse-within-meters check. This is a deliberate simplification, not an
oversight; see the report for the full trace and the option to revisit if a
controller wants the (broken) legacy behavior mirrored byte-for-byte anyway.

Sentinels (``skannonser.enrich.sentinels``) are stored like any other value.
``DnbRepo.set_travel``'s COALESCE fill-only semantics then make storing a
sentinel equivalent to "don't retry": once a column is non-NULL (real value
or sentinel), the candidate query below never selects that destination on
that row again.
"""

import sqlite3

import requests

from skannonser.config.domain import DomainConfig
from skannonser.enrich.sentinels import is_travel_sentinel
from skannonser.enrich.travel_api import TransitCommute
from skannonser.gateway import Gateway
from skannonser.store.repositories.dnb import DnbRepo

_CANDIDATE_SQL = """
    SELECT url, adresse, postnummer, pendl_rush_brj, pendl_rush_mvv
    FROM dnbeiendom
    WHERE active = 1
      AND (duplicate_of_finnkode IS NULL OR TRIM(duplicate_of_finnkode) = '')
      AND url IS NOT NULL AND TRIM(url) != ''
    ORDER BY scraped_at DESC
"""


def run_dnb_travel(
    conn: sqlite3.Connection,
    domain: DomainConfig,
    gateway: Gateway,
    api_key: str,
    post=requests.post,
    limit: int = 0,
) -> dict:
    """Fill missing ``pendl_rush_brj``/``pendl_rush_mvv`` on active,
    FINN-unmatched ``dnbeiendom`` rows via the Routes API.

    ``limit`` caps the TOTAL number of API calls made this run (across both
    destinations, across all rows) at 0 = unlimited; once the cap is hit, the
    run stops before starting any further call -- rows not yet reached stay
    exactly as they were, ready for the next window.

    Each row's ``DnbRepo.set_travel`` write happens once, after every
    destination that row needed has been attempted -- so a
    :class:`~skannonser.gateway.BudgetExceeded` raised mid-row (out of
    ``TransitCommute.minutes``) always propagates before that row's write,
    leaving it completely untouched, exactly like ``run_geocode``.
    """
    by_key = {d.key: d for d in domain.destinations}
    brj_dest = by_key["brj"]
    mvv_dest = by_key["mvv"]
    max_min = int(domain.travel.max_travel_minutes)

    commute_brj = TransitCommute(brj_dest.address, gateway, api_key, post=post, max_minutes=max_min)
    commute_mvv = TransitCommute(mvv_dest.address, gateway, api_key, post=post, max_minutes=max_min)

    repo = DnbRepo(conn)
    rows = conn.execute(_CANDIDATE_SQL).fetchall()

    stats = {
        "candidates": 0,
        "api_calls": 0,
        "brj_written": 0,
        "mvv_written": 0,
        "sentinels_written": 0,
    }

    calls_made = 0
    for row in rows:
        needs_brj = row["pendl_rush_brj"] is None
        needs_mvv = row["pendl_rush_mvv"] is None
        if not needs_brj and not needs_mvv:
            continue
        if limit and calls_made >= limit:
            break

        stats["candidates"] += 1
        brj_val = mvv_val = None

        if needs_brj and (not limit or calls_made < limit):
            minutes = commute_brj.minutes(row["adresse"], row["postnummer"])
            calls_made += 1
            stats["api_calls"] += 1
            if minutes is not None:
                brj_val = int(minutes)
                if is_travel_sentinel(brj_val):
                    stats["sentinels_written"] += 1

        if needs_mvv and (not limit or calls_made < limit):
            minutes = commute_mvv.minutes(row["adresse"], row["postnummer"])
            calls_made += 1
            stats["api_calls"] += 1
            if minutes is not None:
                mvv_val = int(minutes)
                if is_travel_sentinel(mvv_val):
                    stats["sentinels_written"] += 1

        if brj_val is not None or mvv_val is not None:
            repo.set_travel(row["url"], brj=brj_val, mvv=mvv_val)
            if brj_val is not None:
                stats["brj_written"] += 1
            if mvv_val is not None:
                stats["mvv_written"] += 1

    return stats
