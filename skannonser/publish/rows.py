"""Shared listing-row query: the ``Eie`` visibility-filtered, donor-resolved
row builder, factored out of ``export.eie_rows`` so a second consumer (the
web view) can reuse the identical SQL without re-deriving it.

``export.eie_rows`` is now a THIN CONSUMER of ``listing_rows`` -- it calls
``listing_rows(conn)`` (the default path, no hidden fields) and applies its
existing cell normalization/ordering (``_norm_base_cell`` over ``EIE_HEADER``)
on top, unchanged. This module owns the raw SQL fragments
(``_EIE_SELECT_HEAD``/``_EIE_SELECT_TAIL``/``_EIE_JOINS``/``_DONOR_TRAVEL_SQL``)
and the small query helpers (``_rows_from_cursor``, ``_sheet_filters``) --
``export.py`` re-imports them from here for ``sold_rows`` (same shape,
different visibility filter), which is why they moved here rather than being
duplicated: one source of truth for the SQL both tabs share, exactly as
before the split (just relocated).

See ``export.py``'s module docstring ("FIDELITY TO LEGACY") for the full
citation trail (db.py line numbers etc.) backing every column/filter below --
none of that changed, only *where* the code lives.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Any

from skannonser.config.domain import load_domain

# ---------------------------------------------------------------------------
# Shared SQL fragments (moved verbatim from export.py)
# ---------------------------------------------------------------------------

# Donor-resolved travel columns, reused verbatim from
# ProcessedRepo.sheet_travel_values (processed.py:390-407) / the CASE blocks in
# get_eiendom_for_sheets (db.py:829-846). `ep` is the listing's processed row,
# `ep_src` the donor pointed at by ep.travel_copy_from_finnkode.
_DONOR_TRAVEL_SQL = """
    CASE
        WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
             AND ep_src.pendl_rush_brj IS NOT NULL
        THEN ep_src.pendl_rush_brj
        ELSE ep.pendl_rush_brj
    END AS "PENDL RUSH BRJ",
    CASE
        WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
             AND ep_src.pendl_rush_mvv IS NOT NULL
        THEN ep_src.pendl_rush_mvv
        ELSE ep.pendl_rush_mvv
    END AS "PENDL RUSH MVV",
    CASE
        WHEN ep.travel_copy_from_finnkode IS NOT NULL AND TRIM(ep.travel_copy_from_finnkode) != ''
             AND ep_src.pendl_rush_mvv_uni_rush IS NOT NULL
        THEN ep_src.pendl_rush_mvv_uni_rush
        ELSE ep.pendl_rush_mvv_uni_rush
    END AS "MVV UNI RUSH"
"""

# The non-travel Eie/Sold payload columns, aliased exactly as legacy.
# (Order here need not match the header -- rows are assembled by header name.)
_EIE_SELECT_HEAD = """
    e.finnkode AS "Finnkode",
    e.tilgjengelighet AS "Tilgjengelighet",
    e.active AS "active",
    COALESCE(ep.adresse_cleaned, e.adresse) AS "ADRESSE",
    e.postnummer AS "Postnummer",
    e.pris AS "Pris",
    e.url AS "URL",
    e.image_url AS "IMAGE_URL",
    e.image_hosted_url AS "IMAGE_HOSTED_URL",
    e.info_usable_area AS "Bruksareal",
    e.info_usable_i_area AS "Internt bruksareal (BRA-i)",
    e.info_primary_area AS "Primærrom",
    e.info_gross_area AS "Bruttoareal",
    e.info_usable_e_area AS "Eksternt bruksareal (BRA-e)",
    e.info_usable_b_area AS "Innglasset balkong (BRA-b)",
    e.info_open_area AS "Balkong/Terrasse (TBA)",
    e.info_plot_area AS "Tomteareal",
    e.info_plot_ownership AS "Eierskap, tomt",
    e.info_property_type AS "Boligtype",
    e.info_construction_year AS "Byggeår",
    ep.lat AS "LAT",
    ep.lng AS "LNG",
    e.pris_kvm AS "PRIS KVM",
"""

_EIE_SELECT_TAIL = """
    ep.travel_copy_from_finnkode AS "TRAVEL_COPY_FROM_FINNKODE",
    ep.google_maps_url AS "GOOGLE_MAPS_URL",
    e.scraped_at AS "SCRAPED_AT"
"""

_EIE_JOINS = """
    FROM eiendom e
    LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
    LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
"""


def _sheet_filters() -> tuple[int, int]:
    """(sheets_max_price, min_bra_i) from the domain config == legacy constants
    (main/config/filters.py: SHEETS_MAX_PRICE=7500000, MIN_BRA_I=70)."""
    f = load_domain().filters
    return f.sheets_max_price, f.min_bra_i


def _rows_from_cursor(cur: sqlite3.Cursor) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _as_float(v: Any) -> float | None:
    """Coerce a raw SQL value to ``float`` or ``None`` (NULL/non-numeric/NaN)."""
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return f


# ---------------------------------------------------------------------------
# listing_rows
# ---------------------------------------------------------------------------

# Eie visibility filter (== get_eiendom_for_sheets SQL filters db.py:861-866 +
# filter_rows_for_sheet_visibility helper:43-105): active = 1 AND
# tilgjengelighet NOT in {solgt, inaktiv} AND pris <= SHEETS_MAX_PRICE AND
# CAST(info_usable_i_area AS REAL) >= MIN_BRA_I. Kommentar/Tag re-exported
# from annotations (NULL when absent).
_EIE_SQL = (
    "SELECT "
    + _EIE_SELECT_HEAD
    + _DONOR_TRAVEL_SQL
    + ", "
    + _EIE_SELECT_TAIL
    + ', a.kommentar AS "Kommentar", a.tag AS "Tag"'
    + _EIE_JOINS
    + " LEFT JOIN annotations a ON a.finnkode = e.finnkode"
    + " WHERE e.active = 1"
    + " AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) NOT IN ('solgt', 'inaktiv')"
    + " AND e.pris <= ?"
    + " AND CAST(e.info_usable_i_area AS REAL) >= ?"
    + " ORDER BY e.active DESC, e.scraped_at DESC"
)


def listing_rows(
    conn: sqlite3.Connection, *, include_hidden_fields: bool = False
) -> list[dict]:
    """Eie visibility-filtered, donor-resolved row dicts.

    Identical SQL/filters/ordering to the query formerly inlined in
    ``export.eie_rows`` (see ``_EIE_SQL`` above). Each dict's keys are the
    ``EIE_HEADER`` sheet column names (``export.EIE_HEADER``) holding the RAW
    (un-normalized) SQL values -- ``export.eie_rows`` applies
    ``_norm_base_cell`` on top of exactly this to build the sheet payload.

    When ``include_hidden_fields=True``, each dict additionally carries
    underscore-prefixed keys that can never collide with a sheet header name,
    for a web-view consumer that needs typed/raw values the sheet's
    string/number cell-normalization would otherwise obscure. All of these
    are read off columns already present in the SELECT above (no query
    extension needed):

        _finnkode      str | None   -- raw e.finnkode
        _active        int | None   -- raw e.active
        _lat           float | None -- ep.lat coerced to float
        _lng           float | None -- ep.lng coerced to float
        _boligtype_raw Any          -- raw e.info_property_type
        _image_url     str | None   -- raw e.image_url

    Adding these extras must never alter the default-path (``EIE_HEADER``
    keyed) output -- they are additional keys on the same dict, nothing is
    removed or renamed.
    """
    max_price, min_bra_i = _sheet_filters()
    records = _rows_from_cursor(conn.execute(_EIE_SQL, (max_price, min_bra_i)))

    if not include_hidden_fields:
        return records

    for rec in records:
        rec["_finnkode"] = rec.get("Finnkode")
        rec["_active"] = rec.get("active")
        rec["_lat"] = _as_float(rec.get("LAT"))
        rec["_lng"] = _as_float(rec.get("LNG"))
        rec["_boligtype_raw"] = rec.get("Boligtype")
        rec["_image_url"] = rec.get("IMAGE_URL")

    return records
