"""Listings/meta read API (Phase 5 Task 3) + annotations CRUD (Phase 5 Task 4).

Listings/meta/missing-coords are all GETs served off ``ro_conn`` (never
write). The annotations routes at the bottom of this module are the
exception: they use ``rw_conn`` to create/update/delete a single
``annotations`` row -- see the "Annotations CRUD" section below for the
import-protection contract those writes must uphold.

Three listing "buckets" are merged into ``/api/listings``:

* **Eie visible** -- ``rows.listing_rows(conn, include_hidden_fields=True)``,
  unchanged (the same visibility-filtered, donor-resolved query the Eie sheet
  tab uses). ``source: "eie"``, ``sold: false``.
* **Eie sold** -- only when ``?sold=1``: a private query
  (``_SOLD_SQL``) built from the SAME shared fragments
  (``rows._EIE_SELECT_HEAD``/``_DONOR_TRAVEL_SQL``/``_EIE_SELECT_TAIL``/
  ``_EIE_JOINS``) ``export.sold_rows`` uses, PLUS an ``annotations`` join
  (Sold sheet tab never had Kommentar/Tag, but there's no reason the API
  should withhold them if a sold listing happens to carry one) and the same
  hidden-field enrichment as ``listing_rows`` (via ``rows._add_hidden_fields``,
  extracted for exactly this reuse). ``source: "eie"``, ``sold: true``.
  Sold items additionally carry the tinglyst outcome (``sold_price``/
  ``sold_date``/``price_suggestion``, LEFT-joined off ``sold_prices``) --
  null when the sold-price backlog hasn't covered that finnkode yet.
  ``?bucket=sold`` returns ONLY this bucket (no re-shipped actives; the lazy
  sold toggle in the map/table uses it). Every item also carries
  ``scraped_at`` (first-seen; ``eiendom.updated_at`` is NOT last-seen, see
  README).
* **DNB-unique** -- a fresh query against ``dnbeiendom`` mirroring
  ``export.dnb_rows``'s selection/filters (active, price-capped,
  ``duplicate_of_finnkode`` excluded -- see that function's docstring for the
  "no double-pin" rationale) PLUS its own travel columns
  (``pendl_rush_brj``/``pendl_rush_mvv`` -- no ``mvv_uni`` column exists on
  ``dnbeiendom``, so that destination is always ``None`` for DNB rows).
  ``source: "dnb"``, ``sold: false`` (DNB never has a sold concept here).

DNB IDENTIFIER DECISION: ``dnbeiendom`` has no ``finnkode``, so DNB rows need
a synthetic, STABLE, path-safe id for the ``finnkode`` field (used both in
the listing payload and as the ``/api/listings/{finnkode}`` detail path
param): ``f"dnb:{sha1(url)[:16]}"`` unconditionally (``url`` is the table's
identity/upsert-match key, UNIQUE on ``dnbeiendom`` and never changes for a
given crawled listing). This derivation is provably stable across scrapes
regardless of whether ``dnb_id`` is ever populated. A raw url can't be used
directly as a path segment (embedded ``/`` would break FastAPI's path-param
matching). The derivation itself lives in ``skannonser.ids.dnb_identifier``
(shared with ``skannonser.enrich.thumbs``'s nightly thumbnail cache, so both
call sites can never disagree on a DNB row's identifier/filename).

BOLIGTYPE TRIM DECISION: the API serves ``boligtype`` (and ``/api/meta``'s
``boligtyper`` list) TRIMMED of surrounding whitespace, unlike the raw
``_boligtype_raw`` hidden field ``rows.listing_rows`` exposes. Rationale:
whitespace-inconsistent scraped values (e.g. ``"Leilighet "``) would
otherwise silently fracture client-side filter grouping ("Leilighet" and
"Leilighet " would count as different filter buckets). See ``_clean_boligtype``.

IMAGE DECISION (Phase 5 Task 5 update): ``image`` is now thumbnail-FILE-
existence-based -- ``{thumbs_dir}/{identifier}.jpg``'s presence on disk --
whenever the app was built with a ``thumbs_dir`` (``create_app``'s default:
``data/thumbs``, wired by the nightly ``thumbs`` step,
``skannonser.enrich.thumbs.cache_thumbnails``). Only when the app was
explicitly built WITHOUT a thumbs dir (``thumbs_dir=None``) does this fall
back to the original placeholder, ``bool(image_url)`` -- see ``_has_thumb``.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from skannonser.config.domain import DomainConfig, load_domain
from skannonser.ids import IDENTIFIER_RE, dnb_identifier
from skannonser.publish.rows import (
    _DONOR_TRAVEL_SQL,
    _EIE_JOINS,
    _EIE_SELECT_HEAD,
    _EIE_SELECT_TAIL,
    _add_hidden_fields,
    _as_float,
    _rows_from_cursor,
    _sheet_filters,
    listing_rows,
)
from skannonser.web.app import ro_conn, rw_conn

router = APIRouter(prefix="/api")


def _domain(request: Request) -> DomainConfig:
    """``app.state.domain`` when the app was built with an override (tests
    may want a custom domain.toml); otherwise the real ``config/domain.toml``
    -- same fallback ``rows._sheet_filters`` already relies on."""
    configured = getattr(request.app.state, "domain", None)
    return configured or load_domain()


# ---------------------------------------------------------------------------
# Shared scalar helpers
# ---------------------------------------------------------------------------

def _clean_boligtype(raw: Any) -> str | None:
    """Trim whitespace off a boligtype value; ``None``/blank -> ``None``.

    See module docstring "BOLIGTYPE TRIM DECISION".
    """
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _travel_from_record(rec: dict, domain: DomainConfig) -> dict[str, Any]:
    """``{dest.key: rec[dest.df_column]}`` for every configured destination.

    ``rec.get(...)`` returns ``None`` for a destination column absent from
    the record entirely (e.g. DNB rows have no ``"MVV UNI RUSH"`` key) --
    same effect as an explicit per-source special case, no branching needed.
    Values pass through as SQLite already returned them (``int``/``None`` for
    the INTEGER travel columns) -- no re-coercion.
    """
    return {dest.key: rec.get(dest.df_column) for dest in domain.destinations}


def _thumbs_dir(request: Request) -> Path | None:
    """``app.state.thumbs_dir`` as set by ``create_app`` (default:
    ``data/thumbs``), or ``None`` if the app was explicitly built without
    one -- see module docstring "IMAGE DECISION"."""
    return getattr(request.app.state, "thumbs_dir", None)


def _has_thumb(thumbs_dir: Path | None, identifier: str, image_url_present: bool) -> bool:
    """File-existence-based ``image`` bool when `thumbs_dir` is configured
    (Task 5); falls back to the pre-Task-5 ``image_url``-non-empty
    placeholder when it isn't (see module docstring "IMAGE DECISION").
    Shared by both `_eie_item` and `_dnb_item` so a future migration that
    gives `dnbeiendom` an `image_url` column (see
    `skannonser.enrich.thumbs`'s "DNB image_url column" note) needs no
    change here -- the identifier-keyed file check already works for any
    source."""
    if thumbs_dir is None:
        return image_url_present
    return (Path(thumbs_dir) / f"{identifier}.jpg").is_file()


# ---------------------------------------------------------------------------
# Eie sold-rows query (API-shaped: annotations + hidden fields, unlike
# export.sold_rows which is sheet-shaped)
# ---------------------------------------------------------------------------

# Tinglyst sold-price columns (migration 006) joined onto sold rows so the web
# UI can show the actual sale outcome next to the last-seen asking price.
_SOLD_PRICE_COLS = (
    ', sp.sold_price AS "SOLD_PRICE", sp.sold_date AS "SOLD_DATE"'
    ', sp.price_suggestion AS "PRICE_SUGGESTION"'
)
_SOLD_PRICE_JOIN = " LEFT JOIN sold_prices sp ON sp.finnkode = e.finnkode"

_SOLD_API_SQL = (
    "SELECT "
    + _EIE_SELECT_HEAD
    + _DONOR_TRAVEL_SQL
    + ", "
    + _EIE_SELECT_TAIL
    + ', a.kommentar AS "Kommentar", a.tag AS "Tag"'
    + _SOLD_PRICE_COLS
    + _EIE_JOINS
    + " LEFT JOIN annotations a ON a.finnkode = e.finnkode"
    + _SOLD_PRICE_JOIN
    + " WHERE e.active = 0"
    + " AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) IN ('solgt', 'inaktiv')"
    + " AND COALESCE(e.pris, 0) <= ?"
    + " AND CAST(e.info_usable_i_area AS REAL) >= ?"
    + " ORDER BY e.scraped_at DESC"
)


def _sold_records(conn: sqlite3.Connection) -> list[dict]:
    """Same visibility predicate as ``export.sold_rows`` (active=0, status in
    solgt/inaktiv, price/BRA filters -- see that function's docstring), but
    additionally joined against ``annotations`` and hidden-field-enriched,
    for direct consumption by ``_eie_item``."""
    max_price, min_bra_i = _sheet_filters()
    records = _rows_from_cursor(conn.execute(_SOLD_API_SQL, (max_price, min_bra_i)))
    return _add_hidden_fields(records)


# ---------------------------------------------------------------------------
# Listing-details enrichment (migration 010; Task 9)
# ---------------------------------------------------------------------------

def _facilities_by_finnkode(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Every listing's facility strings in one query, alphabetical -- grouped
    in Python rather than GROUP_CONCAT to avoid delimiter games."""
    out: dict[str, list[str]] = {}
    for row in conn.execute(
        "SELECT finnkode, facility FROM listing_facilities ORDER BY finnkode, facility"
    ):
        out.setdefault(str(row["finnkode"]), []).append(row["facility"])
    return out


def _pris_kvm_totalpris(rec: dict) -> int | None:
    """totalpris / BRA-i, rounded. Derived at query time, never stored
    (design spec: stored copies go stale silently). None unless both inputs
    are present and positive."""
    try:
        totalpris = float(rec.get("TOTALPRIS"))
        bra_i = float(rec.get("Internt bruksareal (BRA-i)"))
    except (TypeError, ValueError):
        return None
    if totalpris <= 0 or bra_i <= 0:
        return None
    return round(totalpris / bra_i)


def _maanedskost(rec: dict) -> int | None:
    """felleskost/mnd + kommunale avg/12. None when felleskost is unknown;
    a missing kommunale-avg term contributes 0 (spec's NULL rule)."""
    try:
        felleskost = int(rec.get("FELLESKOST_MND"))
    except (TypeError, ValueError):
        return None
    try:
        kommunale_mnd = round(int(rec.get("KOMMUNALE_AVG_AAR")) / 12)
    except (TypeError, ValueError):
        kommunale_mnd = 0
    return felleskost + kommunale_mnd


# ---------------------------------------------------------------------------
# DNB-unique query
# ---------------------------------------------------------------------------

_DNB_API_SQL = (
    "SELECT "
    '    d.dnb_id AS dnb_id,'
    '    d.url AS "URL",'
    '    d.adresse AS "Adresse",'
    '    d.postnummer AS "Postnummer",'
    '    d.pris AS "Pris",'
    '    d.property_type AS "Boligtype",'
    '    d.lat AS "LAT",'
    '    d.lng AS "LNG",'
    '    d.pendl_rush_brj AS "PENDL RUSH BRJ",'
    '    d.pendl_rush_mvv AS "PENDL RUSH MVV",'
    '    d.scraped_at AS "SCRAPED_AT"'
    + " FROM dnbeiendom d"
    + " WHERE d.active = 1 AND COALESCE(d.pris, 0) <= ?"
    + " AND (d.duplicate_of_finnkode IS NULL OR TRIM(d.duplicate_of_finnkode) = '')"
    + " ORDER BY d.scraped_at DESC"
)


def _dnb_records(conn: sqlite3.Connection) -> list[dict]:
    """DNB-unique rows -- identical scope to ``export.dnb_rows`` (see its
    docstring for the "no double-pin" exclusion rationale), plus its own
    travel columns for ``_travel_from_record``."""
    (max_price, _min_bra_i) = _sheet_filters()
    return _rows_from_cursor(conn.execute(_DNB_API_SQL, (max_price,)))


def _dnb_records_all(conn: sqlite3.Connection) -> list[dict]:
    """EVERY dnbeiendom row (no active/price/duplicate filter) -- used only
    by the detail endpoint, which must resolve a listing regardless of
    whether it's currently visible on the DNB-unique listing bucket."""
    sql = (
        "SELECT "
        '    d.dnb_id AS dnb_id,'
        '    d.url AS "URL",'
        '    d.adresse AS "Adresse",'
        '    d.postnummer AS "Postnummer",'
        '    d.pris AS "Pris",'
        '    d.property_type AS "Boligtype",'
        '    d.lat AS "LAT",'
        '    d.lng AS "LNG",'
        '    d.pendl_rush_brj AS "PENDL RUSH BRJ",'
        '    d.pendl_rush_mvv AS "PENDL RUSH MVV",'
        '    d.scraped_at AS "SCRAPED_AT"'
        + " FROM dnbeiendom d"
    )
    return _rows_from_cursor(conn.execute(sql))


# ---------------------------------------------------------------------------
# Record -> API item
# ---------------------------------------------------------------------------

def _eie_item(
    rec: dict,
    domain: DomainConfig,
    *,
    sold: bool,
    thumbs_dir: Path | None = None,
    facilities: list[str] | None = None,
) -> dict:
    finnkode = rec.get("_finnkode")
    item = {
        "finnkode": finnkode,
        "adresse": rec.get("ADRESSE"),
        "postnummer": rec.get("Postnummer"),
        "pris": rec.get("Pris"),
        "pris_kvm": rec.get("PRIS KVM"),
        "boligtype": _clean_boligtype(rec.get("_boligtype_raw")),
        "tilgjengelighet": rec.get("Tilgjengelighet"),
        "lat": rec.get("_lat"),
        "lng": rec.get("_lng"),
        "travel": _travel_from_record(rec, domain),
        "bra_i": rec.get("Internt bruksareal (BRA-i)"),
        "byggeaar": rec.get("Byggeår"),
        "url": rec.get("URL"),
        "image": _has_thumb(thumbs_dir, finnkode, bool(rec.get("_image_url"))),
        "kommentar": rec.get("Kommentar"),
        "tag": rec.get("Tag"),
        "scraped_at": rec.get("SCRAPED_AT"),
        "source": "eie",
        "sold": sold,
        # Listing-details enrichment (migration 010; None/[] when unparsed).
        "soverom": rec.get("SOVEROM"),
        "rom": rec.get("ROM"),
        "etasje": rec.get("ETASJE"),
        "eieform": rec.get("EIEFORM"),
        "nabolag": rec.get("NABOLAG"),
        "energimerke": rec.get("ENERGIMERKE"),
        "energifarge": rec.get("ENERGIFARGE"),
        "totalpris": rec.get("TOTALPRIS"),
        "omkostninger": rec.get("OMKOSTNINGER"),
        "fellesgjeld": rec.get("FELLESGJELD"),
        "felleskost_mnd": rec.get("FELLESKOST_MND"),
        "fellesformue": rec.get("FELLESFORMUE"),
        "formuesverdi": rec.get("FORMUESVERDI"),
        "kommunale_avg_aar": rec.get("KOMMUNALE_AVG_AAR"),
        "facilities": facilities or [],
        "pris_kvm_totalpris": _pris_kvm_totalpris(rec),
        "maanedskost": _maanedskost(rec),
    }
    if sold:
        # Only sold items carry the tinglyst outcome (see _SOLD_PRICE_COLS);
        # actives omit the keys entirely rather than shipping always-null noise.
        item["sold_price"] = rec.get("SOLD_PRICE")
        item["sold_date"] = rec.get("SOLD_DATE")
        item["price_suggestion"] = rec.get("PRICE_SUGGESTION")
    return item


def _dnb_item(
    rec: dict,
    domain: DomainConfig,
    annotation: tuple[str | None, str | None] | None = None,
    *,
    thumbs_dir: Path | None = None,
) -> dict:
    """``annotation``, when given, is the ``(kommentar, tag)`` pair already
    looked up by the caller for this row's synthetic id (see
    ``_dnb_annotations``/callers) -- DNB rows have no FK to join against, so
    the lookup can't happen in the SQL that builds ``rec``. NOTE: the sheet
    export (``export.dnb_rows``) stays untouched -- the DNB tab has no
    Kommentar/Tag columns (legacy parity), so these annotations are web-only
    by design."""
    kommentar, tag = annotation if annotation is not None else (None, None)
    identifier = dnb_identifier(rec.get("URL"))
    return {
        "finnkode": identifier,
        "adresse": rec.get("Adresse"),
        "postnummer": rec.get("Postnummer"),
        "pris": rec.get("Pris"),
        "pris_kvm": None,  # dnbeiendom has no pris_kvm column
        "boligtype": _clean_boligtype(rec.get("Boligtype")),
        "tilgjengelighet": None,  # dnbeiendom has no tilgjengelighet column
        "lat": _as_float(rec.get("LAT")),
        "lng": _as_float(rec.get("LNG")),
        "travel": _travel_from_record(rec, domain),
        "bra_i": None,  # dnbeiendom has no BRA-i column
        "byggeaar": None,  # dnbeiendom has no construction-year column
        "url": rec.get("URL"),
        # dnbeiendom has no image_url column today, so `image_url_present`
        # is always False here -- but `_has_thumb` still checks the
        # identifier-keyed file when `thumbs_dir` is configured, so this
        # starts working automatically once a migration adds one (see
        # `skannonser.enrich.thumbs`'s DNB candidate-query note).
        "image": _has_thumb(thumbs_dir, identifier, False),
        "kommentar": kommentar,
        "tag": tag,
        "scraped_at": rec.get("SCRAPED_AT"),
        "source": "dnb",
        "sold": False,
    }


def _dnb_annotations(
    conn: sqlite3.Connection,
) -> dict[str, tuple[str | None, str | None]]:
    """``{dnb-synthetic-id: (kommentar, tag)}`` for every annotation row keyed
    by a ``dnb:...`` id -- fetched once so ``/api/listings`` doesn't issue one
    query per DNB row. The annotations PK is the synthetic id itself (the PUT
    route already stores it verbatim, same as any other finnkode), so a
    ``LIKE 'dnb:%'`` scan of the small ``annotations`` table is all that's
    needed; no join is possible since the id is a hash, not a stored column
    on ``dnbeiendom`` (same rationale as ``_find_dnb_record``)."""
    rows = conn.execute(
        "SELECT finnkode, kommentar, tag FROM annotations WHERE finnkode LIKE 'dnb:%'"
    ).fetchall()
    return {r["finnkode"]: (r["kommentar"], r["tag"]) for r in rows}


def _sold_from_hidden(rec: dict) -> bool:
    """Same predicate as the Sold visibility filter (active=0 AND status in
    solgt/inaktiv), applied to an already-fetched hidden-field-enriched
    record -- used by the detail endpoint, which fetches by finnkode with no
    visibility WHERE clause at all."""
    active = rec.get("_active")
    status = str(rec.get("Tilgjengelighet") or "").strip().lower()
    return (not active) and status in ("solgt", "inaktiv")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/listings")
def get_listings(
    request: Request,
    sold: int = 0,
    bucket: str | None = None,
    conn: sqlite3.Connection = Depends(ro_conn),
) -> dict:
    domain = _domain(request)
    thumbs_dir = _thumbs_dir(request)
    facs = _facilities_by_finnkode(conn)

    # `bucket=sold` returns ONLY the sold rows -- the map/table load actives
    # up front and lazily fetch sold on first toggle, so re-shipping the
    # actives they already hold (the `sold=1` merged shape) is pure waste.
    # `sold=1` keeps its original merged behavior for compatibility.
    if bucket is not None:
        if bucket != "sold":
            raise HTTPException(status_code=400, detail=f"unknown bucket: {bucket!r}")
        return {
            "listings": [
                _eie_item(
                    rec, domain, sold=True, thumbs_dir=thumbs_dir,
                    facilities=facs.get(rec.get("_finnkode")),
                )
                for rec in _sold_records(conn)
            ]
        }

    items = [
        _eie_item(
            rec, domain, sold=False, thumbs_dir=thumbs_dir,
            facilities=facs.get(rec.get("_finnkode")),
        )
        for rec in listing_rows(conn, include_hidden_fields=True)
    ]
    dnb_annotations = _dnb_annotations(conn)
    items += [
        _dnb_item(
            rec,
            domain,
            dnb_annotations.get(dnb_identifier(rec.get("URL"))),
            thumbs_dir=thumbs_dir,
        )
        for rec in _dnb_records(conn)
    ]
    if sold:
        items += [
            _eie_item(
                rec, domain, sold=True, thumbs_dir=thumbs_dir,
                facilities=facs.get(rec.get("_finnkode")),
            )
            for rec in _sold_records(conn)
        ]
    return {"listings": items}


def _eie_full_row(conn: sqlite3.Connection, finnkode: str) -> dict | None:
    """Single Eie row (any visibility -- active, sold, inactive) by raw
    ``finnkode``, hidden-field-enriched. ``None`` if unknown."""
    sql = (
        "SELECT "
        + _EIE_SELECT_HEAD
        + _DONOR_TRAVEL_SQL
        + ", "
        + _EIE_SELECT_TAIL
        + ', a.kommentar AS "Kommentar", a.tag AS "Tag"'
        + _SOLD_PRICE_COLS
        + _EIE_JOINS
        + " LEFT JOIN annotations a ON a.finnkode = e.finnkode"
        + _SOLD_PRICE_JOIN
        + " WHERE e.finnkode = ?"
    )
    records = _rows_from_cursor(conn.execute(sql, (finnkode,)))
    if not records:
        return None
    return _add_hidden_fields(records)[0]


def _find_dnb_record(conn: sqlite3.Connection, finnkode: str) -> dict | None:
    """Linear scan over every dnbeiendom row matching the synthetic id --
    unavoidable since the id is a hash, not a stored column. Fine at current
    DNB row counts (hundreds); revisit if that changes."""
    for rec in _dnb_records_all(conn):
        if dnb_identifier(rec.get("URL")) == finnkode:
            return rec
    return None


@router.get("/listings/{finnkode}")
def get_listing_detail(
    finnkode: str,
    request: Request,
    conn: sqlite3.Connection = Depends(ro_conn),
) -> dict:
    domain = _domain(request)
    thumbs_dir = _thumbs_dir(request)

    rec = _eie_full_row(conn, finnkode)
    if rec is not None:
        fac_rows = conn.execute(
            "SELECT facility FROM listing_facilities WHERE finnkode = ? ORDER BY facility",
            (finnkode,),
        ).fetchall()
        item = _eie_item(
            rec, domain, sold=_sold_from_hidden(rec), thumbs_dir=thumbs_dir,
            facilities=[r["facility"] for r in fac_rows],
        )
        raw = {k: v for k, v in rec.items() if not k.startswith("_")}
        return {**raw, **item}

    dnb_rec = _find_dnb_record(conn, finnkode)
    if dnb_rec is not None:
        ann_row = conn.execute(
            "SELECT kommentar, tag FROM annotations WHERE finnkode = ?", (finnkode,)
        ).fetchone()
        annotation = (ann_row["kommentar"], ann_row["tag"]) if ann_row is not None else None
        item = _dnb_item(dnb_rec, domain, annotation, thumbs_dir=thumbs_dir)
        raw = {k: v for k, v in dnb_rec.items() if k != "dnb_id"}
        return {**raw, **item}

    raise HTTPException(status_code=404, detail=f"listing {finnkode!r} not found")


@router.get("/meta")
def get_meta(request: Request, conn: sqlite3.Connection = Depends(ro_conn)) -> dict:
    domain = _domain(request)
    visible = listing_rows(conn, include_hidden_fields=True)
    boligtyper = sorted(
        {
            b
            for b in (_clean_boligtype(rec.get("_boligtype_raw")) for rec in visible)
            if b
        }
    )
    return {
        "polygon": [list(p) for p in domain.polygon_points],
        # Only client-relevant filters are exposed (sheets_max_price, min_bra_i);
        # url_max_price/include_unlisted are internal to export queries and are
        # the ones withheld from the UI.
        "filters": {
            "sheets_max_price": domain.filters.sheets_max_price,
            "min_bra_i": domain.filters.min_bra_i,
        },
        "boligtyper": boligtyper,
        "destinations": [{"key": d.key, "label": d.label} for d in domain.destinations],
        "stations": _stations_meta(conn),
        "facilities": [
            {"name": row["facility"], "count": row["n"]}
            for row in conn.execute(
                "SELECT facility, COUNT(*) AS n FROM listing_facilities "
                "GROUP BY facility ORDER BY n DESC, facility"
            )
        ],
        "energimerker": [
            row["energimerke"]
            for row in conn.execute(
                "SELECT DISTINCT energimerke FROM listing_details "
                "WHERE energimerke IS NOT NULL ORDER BY energimerke"
            )
        ],
        "eieformer": [
            row["eieform"]
            for row in conn.execute(
                "SELECT DISTINCT eieform FROM listing_details "
                "WHERE eieform IS NOT NULL ORDER BY eieform"
            )
        ],
    }


def _stations_meta(conn: sqlite3.Connection) -> list[dict]:
    """One entry per station: ``lines`` = distinct line names,
    ``travel`` = ``{destination: minutes}`` merged across all of the
    station's lines (the minimum minutes when lines disagree on the same
    destination -- a station-level summary, not a per-line one; ``/api/meta``
    has no per-line slot in its contract). ``radius_m`` straight off
    ``stations``. Ported from the query shape ``export.stations_rows`` uses
    (station_lines/station_travel joins), grouped by station instead of by
    (station, line)."""
    line_rows = _rows_from_cursor(
        conn.execute(
            """
            SELECT
                s.id AS station_id,
                s.name AS name,
                s.lat AS lat,
                s.lng AS lng,
                s.radius_m AS radius_m,
                sl.id AS station_line_id,
                sl.line AS line
            FROM stations s
            LEFT JOIN station_lines sl ON sl.station_id = s.id
            ORDER BY s.name, sl.line
            """
        )
    )

    line_ids = [
        int(r["station_line_id"]) for r in line_rows if r["station_line_id"] is not None
    ]
    travel_by_line: dict[int, dict[str, Any]] = {}
    if line_ids:
        placeholders = ",".join("?" for _ in line_ids)
        for r in _rows_from_cursor(
            conn.execute(
                f"SELECT station_line_id, destination, minutes FROM station_travel "
                f"WHERE station_line_id IN ({placeholders})",
                line_ids,
            )
        ):
            travel_by_line.setdefault(int(r["station_line_id"]), {})[
                str(r["destination"])
            ] = r["minutes"]

    stations: dict[int, dict] = {}
    order: list[int] = []
    for r in line_rows:
        sid = int(r["station_id"])
        if sid not in stations:
            stations[sid] = {
                "name": r["name"],
                "lat": _as_float(r["lat"]),
                "lng": _as_float(r["lng"]),
                "radius_m": _as_float(r["radius_m"]),
                "lines": [],
                "travel": {},
            }
            order.append(sid)
        if r["line"] is not None:
            stations[sid]["lines"].append(r["line"])
        slid = r["station_line_id"]
        if slid is not None:
            for dest, minutes in travel_by_line.get(int(slid), {}).items():
                current = stations[sid]["travel"].get(dest)
                if current is None or (minutes is not None and minutes < current):
                    stations[sid]["travel"][dest] = minutes

    return [stations[sid] for sid in order]


@router.get("/missing-coords")
def get_missing_coords(conn: sqlite3.Connection = Depends(ro_conn)) -> dict:
    visible = listing_rows(conn, include_hidden_fields=True)
    rows = [
        {
            "finnkode": rec.get("_finnkode"),
            "adresse": rec.get("ADRESSE"),
            "postnummer": rec.get("Postnummer"),
        }
        for rec in visible
        if rec.get("_lat") is None or rec.get("_lng") is None
    ]
    return {"rows": rows}


# ---------------------------------------------------------------------------
# Annotations CRUD (Phase 5 Task 4)
#
# `annotations` is keyed by `finnkode` (PK, see migration 005): `kommentar`,
# `tag`, `imported_at`, `updated_at`. `skannonser.publish.annotations`'s
# sheet-import upsert only ever (re-)writes a row whose `updated_at` is NULL
# or equal to its own `imported_at` -- see that module's "Never-clobber-the-
# web-UI contract" docstring. The PUT route below is the web-edit side of
# that contract: it bumps `updated_at` to now while leaving `imported_at`
# untouched (or NULL, on a fresh insert -- never equal to a fresh non-NULL
# `updated_at`), so a row this route ever writes is permanently protected
# from being overwritten by a subsequent sheet import.
#
# TOMBSTONE DECISION (controller ruling): a both-null/both-empty PUT used to
# unconditionally DELETE the row. But for a row an import created
# (`imported_at IS NOT NULL`), physically deleting it re-opens the door for
# the NEXT sheet import to resurrect the old value -- the row is gone, so
# `updated_at IS NULL OR updated_at = imported_at` is vacuously satisfied by
# a fresh INSERT, and the import's upsert has no memory that a human ever
# cleared it. Instead: if the existing row has `imported_at IS NOT NULL`,
# TOMBSTONE it -- UPDATE `kommentar = NULL, tag = NULL, updated_at = now`,
# leaving `imported_at` exactly as stored. The row survives as a web-edit
# marker: `updated_at` (now) can never again equal `imported_at` (frozen at
# whatever the last import set), so the import-protection WHERE clause
# permanently blocks that finnkode from being touched again. A row that was
# NEVER imported (`imported_at IS NULL`) has nothing to protect against --
# there's no import that could resurrect it -- so it's still physically
# DELETEd (a no-op if the row is already absent), same as before.
# ---------------------------------------------------------------------------

def _validate_finnkode(finnkode: str) -> None:
    if not IDENTIFIER_RE.match(finnkode or ""):
        raise HTTPException(status_code=400, detail=f"invalid finnkode: {finnkode!r}")


def _norm_text(value: str | None) -> str | None:
    """``None``/blank-after-strip -> ``None``; otherwise the stripped text.
    Lets a PUT body send either ``null`` or ``""`` to mean "no value" --
    the contract treats them interchangeably for both the delete-trigger
    check and what gets stored/returned."""
    if value is None:
        return None
    text = value.strip()
    return text or None


class AnnotationBody(BaseModel):
    """Both fields are required in the request body but nullable -- a client
    must send `{"kommentar": ..., "tag": ...}` (possibly `null` values), not
    omit either key."""

    kommentar: str | None
    tag: str | None


_ANNOTATION_UPSERT_SQL = """
INSERT INTO annotations (finnkode, kommentar, tag, imported_at, updated_at)
VALUES (?, ?, ?, NULL, ?)
ON CONFLICT(finnkode) DO UPDATE SET
    kommentar = excluded.kommentar,
    tag = excluded.tag,
    updated_at = excluded.updated_at
"""


@router.get("/annotations/{finnkode}")
def get_annotation(finnkode: str, conn: sqlite3.Connection = Depends(ro_conn)) -> dict:
    _validate_finnkode(finnkode)
    row = conn.execute(
        "SELECT kommentar, tag FROM annotations WHERE finnkode = ?", (finnkode,)
    ).fetchone()
    if row is None:
        return {"finnkode": finnkode, "kommentar": None, "tag": None}
    return {
        "finnkode": finnkode,
        "kommentar": _norm_text(row["kommentar"]),
        "tag": _norm_text(row["tag"]),
    }


@router.put("/annotations/{finnkode}")
def put_annotation(
    finnkode: str,
    body: AnnotationBody,
    conn: sqlite3.Connection = Depends(rw_conn),
) -> dict:
    _validate_finnkode(finnkode)
    kommentar = _norm_text(body.kommentar)
    tag = _norm_text(body.tag)

    if kommentar is None and tag is None:
        existing = conn.execute(
            "SELECT imported_at FROM annotations WHERE finnkode = ?", (finnkode,)
        ).fetchone()
        if existing is not None and existing["imported_at"] is not None:
            # TOMBSTONE (see "TOMBSTONE DECISION" above): keep the row so a
            # later sheet import can never resurrect the cleared value.
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE annotations SET kommentar = NULL, tag = NULL, updated_at = ? "
                "WHERE finnkode = ?",
                (now, finnkode),
            )
        else:
            conn.execute("DELETE FROM annotations WHERE finnkode = ?", (finnkode,))
        conn.commit()
        return {"finnkode": finnkode, "kommentar": None, "tag": None}

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(_ANNOTATION_UPSERT_SQL, (finnkode, kommentar, tag, now))
    conn.commit()
    return {"finnkode": finnkode, "kommentar": kommentar, "tag": tag}


__all__ = ["router"]
