"""Export payload builders: pure-Python header + row construction for the
``Eie`` / ``Sold`` / ``DNB`` / ``Stations`` Google Sheets tabs.

These builders replace the legacy DB->Sheets sync (``main/tools/manual_sheet_update.py``,
which chains ``sync_eiendom_to_sheets`` + ``update_rows_in_sheet`` +
``sync_stale_eiendom_to_sheets`` + ``sync_stations_to_sheet``) and the
DataFrame-era ``full_sync_eiendom_to_sheets``. They take a raw ``sqlite3``
connection and return ``(header, rows)`` payloads that Task 9's ``run_sheets``
writes through ``SheetsClient.rewrite_tab`` (``valueInputOption="USER_ENTERED"``).

FIDELITY TO LEGACY (the whole game -- the Apps Script map is an UNCHANGED
consumer, and Task 4's ``verify sheets`` enforces cell-by-cell zero-diff parity
on Eie/Sold/Stations):

* **Header + column order** for Eie/Sold reproduce
  ``db.py:get_eiendom_for_sheets`` (788-880) / ``get_stale_eiendom_for_sheets``
  (1008-1086) AFTER ``helper_sync_to_sheets.filter_hidden_sheet_columns``
  (108-119) drops the four internal ``*_CNTR`` columns and
  ``dedupe_and_canonicalize_dataframe_columns`` (122-151) canonicalizes/de-dupes
  (a no-op here: every name is already canonical and unique). Eie additionally
  appends ``Kommentar``/``Tag`` re-exported from the ``annotations`` table
  (migration 005) -- these are new-in-rebuild and excluded from Task 4 parity.

* **Finnkode is a RAW string, NOT a ``=HYPERLINK(...)`` formula.** Verified by
  exhaustive grep: the ONLY place the codebase ever *constructs* a HYPERLINK
  formula is ``main/googleUtils.py:117`` (``read_csv``, the dead CSV-export
  pipeline behind ``main/export.py``) with the format
  ``=HYPERLINK("<url>", "<finnkode>")`` (comma+space). No DB->Sheets sync path
  (``full_sync_eiendom_to_sheets`` helper:764-798, ``sync_eiendom_to_sheets``
  helper:411-522, ``update_rows_in_sheet``, ``sync_stale_eiendom_to_sheets``)
  ever calls it -- they all write the bare ``e.finnkode`` digits plus a separate
  ``URL`` column, then ``sanitize_for_sheets`` string-cleans it. So we emit the
  raw finnkode string + a URL column. (``main/sync/helper_sync_to_sheets.py:352``
  and Code.gs ``normalizeFinnkode_``/``parseUrl_`` are READ-side unwrappers that
  tolerate legacy rows the old CSV path left behind -- they are not writers.)

* **Cell normalization** mirrors ``sanitize_for_sheets`` (helper:154-199) per
  column class: ``Pris``/``PRIS KVM`` are ``fillna(0).astype(int)`` in the SQL
  layer (NULL -> ``0``); the area/year/commute columns are
  ``to_numeric(coerce).round()`` -> int, blank when non-numeric/NULL; text NULLs
  render ``""``; other cells string-cleaned (newlines->space, stripped).

* **postnummer** carries the DB's 4-digit zero-padded string with NO
  apostrophe-escaping -- the controller's bug-compatible ruling (commit
  ``d3eda31``): ``USER_ENTERED`` coerces ``"0581"`` -> ``581`` sheet-side exactly
  like legacy, reproducing today's (truncated) live display byte-for-byte. The
  display fix is post-cutover backlog.

* **Donor-resolved travel** reuses the read-time CASE/COALESCE pattern of
  ``ProcessedRepo.sheet_travel_values`` (processed.py:373-417), extended into the
  full row query so it stays a single query (no per-row N+1): when a listing's
  ``travel_copy_from_finnkode`` is set AND the donor row's value is non-null, the
  donor's value wins; single hop only.

* **LAT/LNG** come from ``eiendom_processed`` -- DB-authoritative. Legacy also
  read LAT/LNG straight out of ``eiendom_processed`` (``ep.lat``/``ep.lng`` in
  the same SELECTs), and the DB is where the sheet's own LAT/LNG originated, so
  one-way export re-emitting them is a no-op round-trip, not a data change.

STATIONS -- RAW vs USER_ENTERED (decision, from Code.gs evidence): legacy
``sync_stations_to_sheet`` (sync_stations_to_sheet.py:76,102-107) wrote
``str(...)``-ified values with ``valueInputOption="RAW"`` (so ``"59.9"`` stayed a
string). ``SheetsClient.rewrite_tab`` only offers ``USER_ENTERED``, which would
coerce numeric-looking strings to numbers. That coercion is HARMLESS here: the
map parses every station numeric field leniently -- LAT/LNG via
``toNumberOrNull_`` (Code.gs:633-634, accepts string OR number, even localized
``"59,9"``) and the ``TO_*`` minute columns via ``sanitizeForClientValue_``
passthrough (Code.gs:648, string|number|boolean returned as-is). Name/Line are
non-numeric text that ``USER_ENTERED`` leaves untouched. So we keep the legacy
``str()`` payload and let Task 9 send it via ``USER_ENTERED`` -- no ``RAW``
extension to ``SheetsClient`` needed, and Task 4 still sees identical string
values on both sides.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Any

from skannonser.config.domain import load_domain

# ---------------------------------------------------------------------------
# Headers (transcribed from legacy; see per-column citations below)
# ---------------------------------------------------------------------------

# Eie/Sold shared column order == get_eiendom_for_sheets SELECT aliases
# (db.py:806-853) MINUS the four *_CNTR columns dropped by
# filter_hidden_sheet_columns (helper:108-113): 'PENDL MORN CNTR',
# 'BIL MORN CNTR', 'PENDL DAG CNTR', 'BIL DAG CNTR'.
_BASE_HEADER: list[str] = [
    "Finnkode",                       # db.py:806
    "Tilgjengelighet",                # db.py:807
    "active",                         # db.py:808
    "ADRESSE",                        # db.py:809 (COALESCE(ep.adresse_cleaned, e.adresse))
    "Postnummer",                     # db.py:810
    "Pris",                           # db.py:811
    "URL",                            # db.py:812
    "IMAGE_URL",                      # db.py:813
    "IMAGE_HOSTED_URL",               # db.py:814
    "Bruksareal",                     # db.py:815
    "Internt bruksareal (BRA-i)",     # db.py:816
    "Primærrom",                      # db.py:817
    "Bruttoareal",                    # db.py:818
    "Eksternt bruksareal (BRA-e)",    # db.py:819
    "Innglasset balkong (BRA-b)",     # db.py:820
    "Balkong/Terrasse (TBA)",         # db.py:821
    "Tomteareal",                     # db.py:822
    "Eierskap, tomt",                 # db.py:823
    "Boligtype",                      # db.py:824
    "Byggeår",                        # db.py:825
    "LAT",                            # db.py:826
    "LNG",                            # db.py:827
    "PRIS KVM",                       # db.py:828
    "PENDL RUSH BRJ",                 # db.py:829-834 (donor CASE)
    "PENDL RUSH MVV",                 # db.py:835-840 (donor CASE)
    "MVV UNI RUSH",                   # db.py:841-846 (donor CASE)
    # 'PENDL MORN CNTR'/'BIL MORN CNTR'/'PENDL DAG CNTR'/'BIL DAG CNTR'
    # (db.py:847-850) are dropped by filter_hidden_sheet_columns.
    "TRAVEL_COPY_FROM_FINNKODE",      # db.py:851
    "GOOGLE_MAPS_URL",                # db.py:852
    "SCRAPED_AT",                     # db.py:853
]

# Eie appends the manually-typed columns, now re-exported from `annotations`.
EIE_HEADER: list[str] = _BASE_HEADER + ["Kommentar", "Tag"]

# Sold tab carries no annotations (get_stale_eiendom_for_sheets, db.py:1020-1069,
# is the same column set; sync_stale_eiendom_to_sheets never adds Kommentar/Tag).
SOLD_HEADER: list[str] = list(_BASE_HEADER)

# DNB tab: sync_dnbeiendom_sheet.py FULL_COL_ORDER (25-28) ==
# export_dnbeiendom_to_sheet.py ALL_EXPORT_COLS (21-22).
DNB_HEADER: list[str] = [
    "Adresse",
    "Postnummer",
    "Pris",
    "Boligtype",
    "URL",
    "LAT",
    "LNG",
    "PENDL RUSH BRJ",
    "PENDL RUSH MVV",
    "MVV UNI RUSH",
]

# Stations tab: sync_stations_to_sheet.py:71 headers for the default
# destination="Sandvika" (travel_col "TO_SANDVIKA") + the "Sandvika Transfer"
# extra destination it adds (sync_stations_to_sheet.py:54-57 -> "TO_SANDVIKA_TRANSFER").
_STATIONS_DESTINATION = "Sandvika"
_STATIONS_TRANSFER = "Sandvika Transfer"
STATIONS_HEADER: list[str] = [
    "Name",
    "LAT",
    "LNG",
    "Line",
    "TO_SANDVIKA",
    "TO_SANDVIKA_TRANSFER",
]

# Column-class sets for Eie/Sold cell normalization (see sanitize_for_sheets).
# Pris/PRIS KVM: fillna(0).astype(int) at the SQL layer -> NULL renders 0.
_INT_ZERO_COLS = {"Pris", "PRIS KVM"}
# area_cols (helper:158-167) + year_cols (helper:168) + commute_cols (helper:157):
# to_numeric(coerce).round() -> int, blank when NULL/non-numeric.
_INT_EMPTY_COLS = {
    "Bruksareal",
    "Internt bruksareal (BRA-i)",
    "Primærrom",
    "Bruttoareal",
    "Eksternt bruksareal (BRA-e)",
    "Innglasset balkong (BRA-b)",
    "Balkong/Terrasse (TBA)",
    "Tomteareal",
    "Byggeår",
    "PENDL RUSH BRJ",
    "PENDL RUSH MVV",
    "MVV UNI RUSH",
}


# ---------------------------------------------------------------------------
# Scalar normalizers
# ---------------------------------------------------------------------------

def norm_cell(v: Any) -> Any:
    """Normalize one non-postnummer cell to its sheet payload form.

    Mirrors ``sanitize_for_sheets`` (helper:180-199) for the generic case:
    ``None``/NaN render ``""``; numbers pass through unchanged (Sheets keeps
    them numeric under ``USER_ENTERED``); every other value is stringified and
    string-cleaned (newlines -> spaces, then stripped), matching the object-column
    branch ``str(x).replace('\\n',' ').replace('\\r',' ').strip() if x else ''``.
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        # No boolean columns reach here in practice; keep the raw value rather
        # than coercing to 0/1 or "".
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        return v
    if isinstance(v, int):
        return v
    s = str(v)
    if not s:
        return ""
    return s.replace("\n", " ").replace("\r", " ").strip()


def norm_postnummer(v: Any) -> str:
    """4-digit zero-padded postnummer STRING for the payload; ``""`` when blank.

    Payload form only -- NO apostrophe escaping (controller bug-compat ruling,
    commit ``d3eda31``): ``USER_ENTERED`` coerces ``"0581"`` -> ``581`` sheet-side
    exactly like legacy, so the live (truncated) display is reproduced
    byte-for-byte. Ports the zero-pad of ``db.py``'s inline postnummer block
    (1564-1572) / ``DnbRepo._to_postnummer``: numeric values zero-pad to 4 digits
    (leading zeros preserved); a non-numeric value is returned stripped as-is;
    ``None``/NaN/blank render ``""``.
    """
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    s = str(v).strip()
    if not s:
        return ""
    try:
        f = float(s)
    except (ValueError, TypeError):
        return s
    if math.isnan(f):
        return ""
    try:
        return str(int(f)).zfill(4)
    except (ValueError, TypeError):
        return s


def _to_number(v: Any) -> float | None:
    """Scalar stand-in for ``pd.to_numeric(errors='coerce')``: ``None`` on
    None/NaN/non-numeric, else the ``float`` value."""
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (ValueError, TypeError):
        return None
    if math.isnan(f):
        return None
    return f


def _int_or_empty(v: Any) -> Any:
    """``to_numeric(coerce).round()`` -> int, or ``""`` when not numeric.

    Matches the area/year/commute column branch of ``sanitize_for_sheets``.
    """
    f = _to_number(v)
    if f is None:
        return ""
    return int(round(f))


def _int_or_zero(v: Any) -> int:
    """``fillna(0).astype(int)`` -> int (NULL/non-numeric -> ``0``).

    Matches the ``numeric_columns = ['Pris', 'PRIS KVM']`` coercion applied in
    ``get_eiendom_for_sheets`` (db.py:875-878) / ``get_stale_eiendom_for_sheets``
    (1081-1084) before sanitize.
    """
    f = _to_number(v)
    if f is None:
        return 0
    return int(f)


def _norm_base_cell(header: str, value: Any) -> Any:
    """Apply the right column-class normalizer for an Eie/Sold column."""
    if header == "Postnummer":
        return norm_postnummer(value)
    if header in _INT_ZERO_COLS:
        return _int_or_zero(value)
    if header in _INT_EMPTY_COLS:
        return _int_or_empty(value)
    return norm_cell(value)


# ---------------------------------------------------------------------------
# Shared SQL fragments
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


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def eie_rows(conn: sqlite3.Connection) -> tuple[list[str], list[list]]:
    """Build the ``Eie`` tab payload: ``(header, rows)``.

    Visibility (== get_eiendom_for_sheets SQL filters db.py:861-866 +
    filter_rows_for_sheet_visibility helper:43-105): ``active = 1`` AND
    tilgjengelighet NOT in {solgt, inaktiv} AND ``pris <= SHEETS_MAX_PRICE`` AND
    ``CAST(info_usable_i_area AS REAL) >= MIN_BRA_I``. Kommentar/Tag are
    re-exported from ``annotations`` (empty string when absent).
    """
    max_price, min_bra_i = _sheet_filters()
    sql = (
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
    records = _rows_from_cursor(conn.execute(sql, (max_price, min_bra_i)))
    rows = [[_norm_base_cell(h, rec.get(h)) for h in EIE_HEADER] for rec in records]
    return list(EIE_HEADER), rows


def sold_rows(conn: sqlite3.Connection) -> tuple[list[str], list[list]]:
    """Build the ``Sold`` tab payload: ``(header, rows)``.

    Scope (== get_stale_eiendom_for_sheets db.py:1073-1074 + the price/area
    filters sync_stale_eiendom_to_sheets applies afterwards helper:648-675):
    ``active = 0`` AND tilgjengelighet in {solgt, inaktiv} AND (with Pris already
    ``fillna(0)``) ``COALESCE(pris, 0) <= SHEETS_MAX_PRICE`` AND
    ``CAST(info_usable_i_area AS REAL) >= MIN_BRA_I``.
    """
    max_price, min_bra_i = _sheet_filters()
    sql = (
        "SELECT "
        + _EIE_SELECT_HEAD
        + _DONOR_TRAVEL_SQL
        + ", "
        + _EIE_SELECT_TAIL
        + _EIE_JOINS
        + " WHERE e.active = 0"
        + " AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) IN ('solgt', 'inaktiv')"
        + " AND COALESCE(e.pris, 0) <= ?"
        + " AND CAST(e.info_usable_i_area AS REAL) >= ?"
        + " ORDER BY e.scraped_at DESC"
    )
    records = _rows_from_cursor(conn.execute(sql, (max_price, min_bra_i)))
    rows = [[_norm_base_cell(h, rec.get(h)) for h in SOLD_HEADER] for rec in records]
    return list(SOLD_HEADER), rows


def dnb_rows(conn: sqlite3.Connection) -> tuple[list[str], list[list]]:
    """Build the ``DNB`` tab payload: ``(header, rows)``.

    The first WORKING DNB export (legacy's ``sync_dnbeiendom_to_sheets`` is
    unreachable dead code; the ``scripts/`` variants only ever wrote DNB-only
    rows). Scope: ``active = 1`` DNB rows within ``SHEETS_MAX_PRICE`` (DNB has no
    ``info_usable_i_area`` column, so no MIN_BRA_I filter). Travel columns come
    from the ``dnbeiendom`` travel columns added in migration 004
    (``pendl_rush_brj``, ``pendl_rush_mvv``; there is no ``mvv_uni`` column on
    ``dnbeiendom`` so ``MVV UNI RUSH`` is blank for DNB-only rows). A row matched
    to a FINN listing (``duplicate_of_finnkode`` set) instead INHERITS that FINN
    row's donor-resolved travel (same CASE/COALESCE as Eie), which also supplies
    ``MVV UNI RUSH``.
    """
    max_price, _min_bra_i = _sheet_filters()
    sql = (
        "SELECT "
        '    d.adresse AS "Adresse",'
        '    d.postnummer AS "Postnummer",'
        '    d.pris AS "Pris",'
        '    d.property_type AS "Boligtype",'
        '    d.url AS "URL",'
        '    d.lat AS "LAT",'
        '    d.lng AS "LNG",'
        '    d.duplicate_of_finnkode AS "duplicate_of_finnkode",'
        '    d.pendl_rush_brj AS "dnb_brj",'
        '    d.pendl_rush_mvv AS "dnb_mvv",'
        "    " + _DONOR_TRAVEL_SQL.replace('"PENDL RUSH BRJ"', '"finn_brj"')
        .replace('"PENDL RUSH MVV"', '"finn_mvv"')
        .replace('"MVV UNI RUSH"', '"finn_mvv_uni"')
        + " FROM dnbeiendom d"
        + " LEFT JOIN eiendom_processed ep ON ep.finnkode = d.duplicate_of_finnkode"
        + " LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode"
        + " WHERE d.active = 1 AND COALESCE(d.pris, 0) <= ?"
        + " ORDER BY d.scraped_at DESC"
    )
    records = _rows_from_cursor(conn.execute(sql, (max_price,)))

    rows: list[list] = []
    for rec in records:
        matched = bool(str(rec.get("duplicate_of_finnkode") or "").strip())
        if matched:
            brj = rec.get("finn_brj")
            mvv = rec.get("finn_mvv")
            mvv_uni = rec.get("finn_mvv_uni")
        else:
            brj = rec.get("dnb_brj")
            mvv = rec.get("dnb_mvv")
            mvv_uni = None  # no dnbeiendom source column for mvv_uni
        row = [
            norm_cell(rec.get("Adresse")),
            norm_postnummer(rec.get("Postnummer")),
            _int_or_empty(rec.get("Pris")),
            norm_cell(rec.get("Boligtype")),
            norm_cell(rec.get("URL")),
            norm_cell(rec.get("LAT")),
            norm_cell(rec.get("LNG")),
            _int_or_empty(brj),
            _int_or_empty(mvv),
            _int_or_empty(mvv_uni),
        ]
        rows.append(row)
    return list(DNB_HEADER), rows


def stations_rows(conn: sqlite3.Connection) -> tuple[list[str], list[list]]:
    """Build the ``Stations`` tab payload: ``(header, rows)``.

    Direct port of ``sync_stations_to_sheet`` (sync_stations_to_sheet.py:44-76)
    + ``StationDatabase.get_all_for_export`` (stations.py:393-466) for the
    default ``destination="Sandvika"`` (+ the "Sandvika Transfer" extra it adds).
    One row per (station, line), ordered by name then line. Values are
    ``str(...)``-ified exactly as legacy (``sync_stations_to_sheet.py:76``); the
    empty-string fallback for a missing coord/minute is preserved.
    """
    line_rows = _rows_from_cursor(
        conn.execute(
            """
            SELECT
                sl.id AS station_line_id,
                s.name AS station_name,
                s.lat AS lat,
                s.lng AS lng,
                sl.line AS line
            FROM station_lines sl
            JOIN stations s ON s.id = sl.station_id
            ORDER BY s.name, sl.line
            """
        )
    )

    line_ids = [int(r["station_line_id"]) for r in line_rows]
    travel: dict[tuple[int, str], Any] = {}
    if line_ids:
        placeholders = ",".join("?" for _ in line_ids)
        for r in _rows_from_cursor(
            conn.execute(
                f"SELECT station_line_id, destination, minutes FROM station_travel "
                f"WHERE station_line_id IN ({placeholders})",
                line_ids,
            )
        ):
            travel[(int(r["station_line_id"]), str(r["destination"]))] = r["minutes"]

    rows: list[list] = []
    for r in line_rows:
        slid = int(r["station_line_id"])
        record = {
            "Name": r["station_name"],
            "LAT": r["lat"] if r["lat"] is not None else "",
            "LNG": r["lng"] if r["lng"] is not None else "",
            "Line": r["line"],
            "TO_SANDVIKA": _minutes_or_blank(travel.get((slid, _STATIONS_DESTINATION))),
            "TO_SANDVIKA_TRANSFER": _minutes_or_blank(
                travel.get((slid, _STATIONS_TRANSFER))
            ),
        }
        rows.append([str(record.get(h, "")) for h in STATIONS_HEADER])
    return list(STATIONS_HEADER), rows


def _minutes_or_blank(minutes: Any) -> Any:
    """``minutes if minutes is not None else ""`` -- stations.py:463 semantics."""
    return minutes if minutes is not None else ""
