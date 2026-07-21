"""Listings/meta read API (Phase 5 Task 3).

Everything here is a GET, served off ``ro_conn`` (never writes). Three
listing "buckets" are merged into ``/api/listings``:

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
param): ``f"dnb:{dnb_id}"`` when ``dnb_id`` is populated, else
``f"dnb:{sha1(url)[:16]}"`` (``url`` is UNIQUE on ``dnbeiendom`` and never
changes for a given crawled listing, so the hash is stable across requests).
A raw url can't be used directly as a path segment (embedded ``/`` would
break FastAPI's path-param matching).

BOLIGTYPE TRIM DECISION: the API serves ``boligtype`` (and ``/api/meta``'s
``boligtyper`` list) TRIMMED of surrounding whitespace, unlike the raw
``_boligtype_raw`` hidden field ``rows.listing_rows`` exposes. Rationale:
whitespace-inconsistent scraped values (e.g. ``"Leilighet "``) would
otherwise silently fracture client-side filter grouping ("Leilighet" and
"Leilighet " would count as different filter buckets). See ``_clean_boligtype``.

IMAGE DECISION: ``image`` is simply ``bool(image_url)`` for every row today
(EIE and DNB) regardless of ``app.state.thumbs_dir`` -- ports the sheet's
existing ``IMAGE_URL`` presence check verbatim. A follow-up "thumbs" task
owns actually checking thumbnail-file existence under ``thumbs_dir``; wiring
that in now would be speculative (no thumbs-download path exists yet).
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from skannonser.config.domain import DomainConfig, load_domain
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
from skannonser.web.app import ro_conn

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


def _dnb_identifier(dnb_id: Any, url: Any) -> str:
    """Stable, path-safe synthetic id for a DNB row -- see module docstring
    "DNB IDENTIFIER DECISION"."""
    if dnb_id is not None and str(dnb_id).strip():
        return f"dnb:{dnb_id}"
    digest = hashlib.sha1(str(url or "").encode("utf-8")).hexdigest()[:16]
    return f"dnb:{digest}"


# ---------------------------------------------------------------------------
# Eie sold-rows query (API-shaped: annotations + hidden fields, unlike
# export.sold_rows which is sheet-shaped)
# ---------------------------------------------------------------------------

_SOLD_API_SQL = (
    "SELECT "
    + _EIE_SELECT_HEAD
    + _DONOR_TRAVEL_SQL
    + ", "
    + _EIE_SELECT_TAIL
    + ', a.kommentar AS "Kommentar", a.tag AS "Tag"'
    + _EIE_JOINS
    + " LEFT JOIN annotations a ON a.finnkode = e.finnkode"
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
    '    d.pendl_rush_mvv AS "PENDL RUSH MVV"'
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
        '    d.pendl_rush_mvv AS "PENDL RUSH MVV"'
        + " FROM dnbeiendom d"
    )
    return _rows_from_cursor(conn.execute(sql))


# ---------------------------------------------------------------------------
# Record -> API item
# ---------------------------------------------------------------------------

def _eie_item(rec: dict, domain: DomainConfig, *, sold: bool) -> dict:
    return {
        "finnkode": rec.get("_finnkode"),
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
        "image": bool(rec.get("_image_url")),
        "kommentar": rec.get("Kommentar"),
        "tag": rec.get("Tag"),
        "source": "eie",
        "sold": sold,
    }


def _dnb_item(rec: dict, domain: DomainConfig) -> dict:
    return {
        "finnkode": _dnb_identifier(rec.get("dnb_id"), rec.get("URL")),
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
        "image": False,  # dnbeiendom has no image_url column
        "kommentar": None,
        "tag": None,
        "source": "dnb",
        "sold": False,
    }


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
    conn: sqlite3.Connection = Depends(ro_conn),
) -> dict:
    domain = _domain(request)
    items = [
        _eie_item(rec, domain, sold=False)
        for rec in listing_rows(conn, include_hidden_fields=True)
    ]
    items += [_dnb_item(rec, domain) for rec in _dnb_records(conn)]
    if sold:
        items += [_eie_item(rec, domain, sold=True) for rec in _sold_records(conn)]
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
        + _EIE_JOINS
        + " LEFT JOIN annotations a ON a.finnkode = e.finnkode"
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
        if _dnb_identifier(rec.get("dnb_id"), rec.get("URL")) == finnkode:
            return rec
    return None


@router.get("/listings/{finnkode}")
def get_listing_detail(
    finnkode: str,
    request: Request,
    conn: sqlite3.Connection = Depends(ro_conn),
) -> dict:
    domain = _domain(request)

    rec = _eie_full_row(conn, finnkode)
    if rec is not None:
        item = _eie_item(rec, domain, sold=_sold_from_hidden(rec))
        raw = {k: v for k, v in rec.items() if not k.startswith("_")}
        return {**raw, **item}

    dnb_rec = _find_dnb_record(conn, finnkode)
    if dnb_rec is not None:
        item = _dnb_item(dnb_rec, domain)
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
        "filters": {
            "sheets_max_price": domain.filters.sheets_max_price,
            "min_bra_i": domain.filters.min_bra_i,
        },
        "boligtyper": boligtyper,
        "destinations": [{"key": d.key, "label": d.label} for d in domain.destinations],
        "stations": _stations_meta(conn),
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


__all__ = ["router"]
