"""Golden-master harness: compare the rebuilt Sheets export payload builders
(``skannonser.publish.export``'s ``eie_rows``/``sold_rows``/``stations_rows``)
against LEGACY's Eie/Sold/Stations DataFrame-to-sheet pipeline, over a
caller-supplied COPY of the property DB.

Three comparisons -- ``eie_diffs``, ``sold_diffs``, ``stations_diffs`` -- each
built by driving the REAL legacy row-building code (never a
reimplementation) up to, but never including, any Google Sheets service
call. No ``SheetsClient``/``get_sheets_service`` is ever constructed here;
these functions only ever compute the pandas ``DataFrame``/list-of-lists
payload legacy WOULD have written, so the comparison is zero-network,
zero-API-key.

Legacy call sequences driven, one per tab (see each ``_legacy_*`` function's
docstring for the exact source-line citations):

  * Eie -- ``main/sync/helper_sync_to_sheets.py:full_sync_eiendom_to_sheets``
    (729-814): ``PropertyDatabase.get_eiendom_for_sheets`` ->
    ``filter_rows_for_sheet_visibility`` -> ``filter_hidden_sheet_columns`` ->
    ``dedupe_and_canonicalize_dataframe_columns`` -> ``sanitize_for_sheets``.
  * Sold -- ``helper_sync_to_sheets.py:sync_stale_eiendom_to_sheets``
    (535-726): ``PropertyDatabase.get_stale_eiendom_for_sheets`` -> the
    inline ``SHEETS_MAX_PRICE``/``MIN_BRA_I`` post-filter (633-675,
    reproduced verbatim -- it is inline logic in the sync function, not a
    standalone helper to import) -> Finnkode normalize/dedupe (699-700) ->
    the same ``filter_hidden_sheet_columns`` /
    ``dedupe_and_canonicalize_dataframe_columns`` / ``sanitize_for_sheets``
    trio as Eie.
  * Stations -- ``main/sync/sync_stations_to_sheet.py:sync_stations_to_sheet``
    (44-76): drives the REAL ``StationDatabase.get_all_for_export``
    (stations.py:393-466) -- the only heavy-lifting legacy code involved --
    then reproduces the handful of trivial inline lines around it (header
    list, ``str(...)``-ifying each cell, sync_stations_to_sheet.py:68-76)
    verbatim, since ``sync_stations_to_sheet`` itself has no split between
    "build the payload" and "write it" -- calling the whole function would
    construct a real Sheets service (``get_sheets_service()`` at line 78).

DNB is excluded entirely: legacy's DNB sync (the nested
``sync_dnbeiendom_to_sheets`` function defined inside
``sync_stale_eiendom_to_sheets``, helper:554-629) is unreachable dead code --
it is defined but the enclosing function's own body never calls it. There is
no legacy baseline to drive for DNB; Task 3's unit tests on
``skannonser.publish.export.dnb_rows`` are the only coverage for that tab.

Legacy (``main.database.db.PropertyDatabase``, ``main.database.stations.StationDatabase``,
``main.sync.helper_sync_to_sheets``) is imported lazily inside
``verify_sheets`` -- mirrors ``skannonser/verify/parse.py`` /
``skannonser/verify/enrich.py``.
"""
from __future__ import annotations

import math
import numbers
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from skannonser.publish.export import eie_rows, sold_rows, stations_rows
from skannonser.store import connection as skconn

_REPO_ROOT = Path(__file__).resolve().parents[2]

# New-only Eie columns (re-exported from the `annotations` table, migration
# 005) with no legacy baseline -- stripped from the new side before compare.
_EIE_NEW_ONLY_COLUMNS = ("Kommentar", "Tag")

# Columns where legacy's `sanitize_for_sheets` can emit a numeric-looking
# STRING instead of a native number, purely as a side effect of pandas
# dtype mechanics -- NOT a real data difference. Mechanism (verified
# empirically while building this harness): for LAT/LNG and every
# area/commute/year column in `sanitize_for_sheets`'s `commute_cols`/
# `area_cols`/`year_cols` lists (helper:156-168), if EVEN ONE row in the
# (already visibility-filtered) frame has a missing value in that column,
# the whole-dataframe `df.fillna('')` (helper:181) upcasts that column's
# dtype from float64 to `object` (mixing real floats with `''`). The
# column-specific `.apply(lambda x: int(x) ... )` re-cast (helper:184-192)
# that follows then produces a Series of Python `int`s mixed with `''` --
# still heterogeneous, so pandas cannot infer it back to a native int64
# dtype -- so it STAYS `object`. The final generic "stringify every object
# column" loop (helper:195-197) then catches it and turns every value,
# including the real numbers, into a string (e.g. `59.91` -> `"59.91"`).
# A column with NO missing values in the frame skips this entirely (the
# re-cast Series is homogeneous ints, pandas infers int64, the generic loop
# skips it since dtype != 'object') -- so this is real-data-dependent, not
# a constant offset, and will very likely appear on any DB where some
# visible listing is still missing a coordinate or a travel/area value.
# It is MOOT for the actual rendered sheet: both the legacy and the new
# payload go through Sheets' `USER_ENTERED` value-input option, which
# parses a numeric-looking string right back into the same number -- so
# `"59.91"` and `59.91` land in the identical cell. Comparing them exactly
# (the general `everything else exact` rule) would therefore flag a false
# positive on essentially every real-world DB snapshot; comparing them
# numerically here reproduces what the sheet actually ends up showing.
#
# `Postnummer` is included too, on the strength of the SAME mechanism plus
# an existing controller ruling: Task 1 empirically established that the
# live sheet's Postnummer column IS truncated ("581" not "0581") because
# `USER_ENTERED` strips a numeric-looking string's leading zero, ruled
# "phase 4 bug-compatible... display fix backlogged" (`.superpowers/sdd/
# progress.md`, P4 Task 1 entry) -- `export.py`'s `norm_postnummer` was
# deliberately built to zero-pad and NOT apostrophe-escape so this
# truncation reproduces byte-for-byte. Running `verify sheets` against a
# real DB copy (this module's own checkpoint) surfaced the deeper reason:
# ~2000 of ~5860 `eiendom.postnummer` values are ALREADY stored WITHOUT a
# leading zero (e.g. `"581"`, confirmed by direct SQL:
# `SELECT length(postnummer), COUNT(*) FROM eiendom GROUP BY 1` ->
# `{3: 2008, 4: 3855}`) -- legacy's `sanitize_for_sheets` never touches
# Postnummer (it isn't in any of the numeric column lists), so it emits
# that raw, sometimes-unpadded DB string verbatim, while `norm_postnummer`
# always zero-pads. Both forms parse to the identical number under
# `USER_ENTERED` (`float("581") == float("0581")`), so this is the same
# "moot at the rendered layer" situation as LAT/LNG, not the "silently mask
# a real zero-pad regression" case originally worried about here -- a
# genuine wrong-postal-code difference (e.g. "581" vs "582") still fails
# numerically and is still caught.
_NUMERIC_STRING_FIELDS = {
    "LAT",
    "LNG",
    "Postnummer",
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


@dataclass(frozen=True)
class SheetDiff:
    tab: str
    key: str
    field: str
    legacy_value: object
    new_value: object


@dataclass
class VerifySheetsResult:
    eie_diffs: list = field(default_factory=list)
    sold_diffs: list = field(default_factory=list)
    stations_diffs: list = field(default_factory=list)


def _import_legacy():
    """Import legacy's DB/station accessors + the sheet-sync helper module,
    adding the repo root to `sys.path` first if needed (installed
    console-script case) -- same pattern as `verify/parse.py`'s
    `_import_legacy`."""
    try:
        from main.database.db import PropertyDatabase
        from main.database.stations import StationDatabase
        from main.sync import helper_sync_to_sheets as helper
    except ModuleNotFoundError:
        root = str(_REPO_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)
        from main.database.db import PropertyDatabase
        from main.database.stations import StationDatabase
        from main.sync import helper_sync_to_sheets as helper
    return PropertyDatabase, StationDatabase, helper


def _import_legacy_filters():
    try:
        from main.config.filters import MIN_BRA_I, SHEETS_MAX_PRICE
    except ImportError:
        from config.filters import MIN_BRA_I, SHEETS_MAX_PRICE
    return SHEETS_MAX_PRICE, MIN_BRA_I


# ---------------------------------------------------------------------------
# Legacy row-building (real legacy functions; no Sheets service constructed)
# ---------------------------------------------------------------------------


def _legacy_eie_df(PropertyDatabase, helper, db_path: Path) -> pd.DataFrame:
    """Reproduces `full_sync_eiendom_to_sheets`
    (helper_sync_to_sheets.py:729-814) up to (not including) the Sheets-
    service calls: builds the exact DataFrame the real full sync would hand
    to `service.spreadsheets().values().update(...)`. Every step below is a
    REAL legacy function call, in the real call order:

      1. `db.get_eiendom_for_sheets()` (db.py:788) -- SQL query with
         SHEETS_MAX_PRICE/MIN_BRA_I applied inside the query itself.
      2. `helper.filter_rows_for_sheet_visibility(df, db)` (helper:43) --
         active + Tilgjengelighet-hidden-status filter (internally re-queries
         `db.get_eiendom_for_status_refresh`).
      3. `helper.filter_hidden_sheet_columns(df)` (helper:116) -- drops the
         four internal `*_CNTR` columns.
      4. `helper.dedupe_and_canonicalize_dataframe_columns(df)` (helper:122)
         -- a no-op on this data (every column name is already canonical and
         unique) but run for fidelity to the real call sequence.
      5. `helper.sanitize_for_sheets(df)` (helper:154) -- the cell
         normalization (None/NaN -> "", area/year/commute -> int-or-blank,
         Pris/PRIS KVM already int from step 1, text cleaned).
    """
    db = PropertyDatabase(str(db_path))
    df = db.get_eiendom_for_sheets()
    df = helper.filter_rows_for_sheet_visibility(df, db)
    df = helper.filter_hidden_sheet_columns(df)
    df = helper.dedupe_and_canonicalize_dataframe_columns(df)
    df = helper.sanitize_for_sheets(df)
    return df


def _legacy_sold_df(PropertyDatabase, helper, db_path: Path) -> pd.DataFrame:
    """Reproduces `sync_stale_eiendom_to_sheets`
    (helper_sync_to_sheets.py:535-726) up to (not including) the Sheets-
    service calls:

      1. `db.get_stale_eiendom_for_sheets()` (db.py:1008) -- active=0 AND
         Tilgjengelighet in {solgt, inaktiv}; no price/BRA filter in SQL.
      2. The inline SHEETS_MAX_PRICE/MIN_BRA_I post-filter (helper:633-675),
         reproduced verbatim below -- it is inline logic in the sync
         function itself, not a standalone helper importable on its own.
      3. Finnkode normalize + dedupe (helper:699-700) via the real
         `helper._normalize_finnkode`.
      4. The same `filter_hidden_sheet_columns` /
         `dedupe_and_canonicalize_dataframe_columns` / `sanitize_for_sheets`
         trio as Eie.

    Deliberate simplification for verification purposes: legacy's early
    "df.empty -> clear the Sold sheet, return" branch (helper:681-697) is
    Sheets-I/O-only and is not reproduced here -- when the post-filter frame
    is empty we still run it through steps 3-4 above (all safe/no-op on an
    empty frame; verified in this module's tests) rather than short-
    circuiting, since a zero-row DataFrame is what matters for comparison,
    not which unreachable-here code path technically produced it.
    """
    db = PropertyDatabase(str(db_path))
    df = db.get_stale_eiendom_for_sheets()

    sheets_max_price, min_bra_i = _import_legacy_filters()
    include_mask = pd.Series(True, index=df.index)
    if sheets_max_price is not None and "Pris" in df.columns:
        price_vals = pd.to_numeric(df["Pris"], errors="coerce")
        include_mask &= (price_vals <= float(sheets_max_price)).fillna(False)
    if min_bra_i is not None and "Internt bruksareal (BRA-i)" in df.columns:
        bra_vals = pd.to_numeric(df["Internt bruksareal (BRA-i)"], errors="coerce")
        include_mask &= (bra_vals >= float(min_bra_i)).fillna(False)
    df = df.loc[include_mask].copy()

    df["Finnkode"] = df["Finnkode"].apply(helper._normalize_finnkode)
    df = df[df["Finnkode"] != ""].drop_duplicates(subset=["Finnkode"], keep="first")

    df = helper.filter_hidden_sheet_columns(df)
    df = helper.dedupe_and_canonicalize_dataframe_columns(df)
    df = helper.sanitize_for_sheets(df)
    return df


def _legacy_stations_payload(StationDatabase, db_path: Path) -> tuple[list[str], list[list]]:
    """Reproduces the row-building of `sync_stations_to_sheet`
    (sync_stations_to_sheet.py:44-76) up to (not including) the
    `get_sheets_service()`/Sheets-write calls, for the default
    `destination="Sandvika"` (+ its "Sandvika Transfer" extra, added
    automatically at line 56-57 of the legacy function).

    Drives the REAL `StationDatabase.get_all_for_export`
    (stations.py:393-466) -- the only legacy code with actual logic in this
    path. The few lines around it (header list, `str(...)`-ifying every
    cell) are trivial inline list construction, reproduced verbatim from
    `sync_stations_to_sheet.py:68-76` -- there is no standalone function to
    import for just that part, and `skannonser.publish.export`'s own
    docstring already characterizes this as a "direct port".
    """
    db = StationDatabase(str(db_path))
    destination = "Sandvika"
    extra_destinations = ["Sandvika Transfer"]
    export_rows = db.get_all_for_export(
        destination=destination, extra_destinations=extra_destinations
    )

    travel_col = "TO_SANDVIKA"
    transfer_cols = ["TO_SANDVIKA_TRANSFER"]
    headers = ["Name", "LAT", "LNG", "Line", travel_col, *transfer_cols]

    data = [[str(row.get(h, "")) for h in headers] for row in export_rows]
    return headers, data


# ---------------------------------------------------------------------------
# Normalization + comparison (applied identically to BOTH sides)
# ---------------------------------------------------------------------------


def _blank(v):
    """None/NaN -> "" -- the sheet renders both empty."""
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return v


def _is_number(v) -> bool:
    return isinstance(v, numbers.Number) and not isinstance(v, bool)


def _maybe_float(v):
    """Parse a numeric-looking string to float; `None` if it isn't one.
    Only consulted for `_NUMERIC_STRING_FIELDS` -- see that set's docstring
    for why this must not be a blanket rule."""
    if not isinstance(v, str):
        return None
    s = v.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _cells_equal(lv, nv, *, field: str | None = None) -> bool:
    """Cell equality: None/NaN normalized to "" on both sides first; if both
    remaining values are numbers, compare numerically (42 == 42.0 passes);
    for `field`s in `_NUMERIC_STRING_FIELDS`, a numeric-looking string on
    either side is also parsed and compared numerically (see that set's
    docstring for the legacy dtype-mixing mechanism this covers, and why
    Postnummer is deliberately included); otherwise exact equality (covers
    every other text field, where a numeric-looking value must NOT be
    coerced through float since no `USER_ENTERED`-moots-it precedent has
    been established for it)."""
    lv = _blank(lv)
    nv = _blank(nv)
    if lv == "" or nv == "":
        return lv == nv
    if _is_number(lv) and _is_number(nv):
        return float(lv) == float(nv)
    if field in _NUMERIC_STRING_FIELDS:
        ln = float(lv) if _is_number(lv) else _maybe_float(lv)
        nn = float(nv) if _is_number(nv) else _maybe_float(nv)
        if ln is not None and nn is not None:
            return ln == nn
    return lv == nv


def _records_from_df(df: pd.DataFrame, key_col: str = "Finnkode") -> dict:
    records: dict = {}
    for rec in df.to_dict("records"):
        key = str(rec.get(key_col, "") or "").strip()
        records[key] = rec
    return records


def _records_from_rows(
    header: list, rows: list, key_col: str = "Finnkode", exclude: tuple = ()
) -> tuple:
    """Build a `{key: {col: value}}` dict from a (header, rows) payload,
    dropping any `exclude`d columns first (used to strip Eie's new-only
    Kommentar/Tag before comparison)."""
    keep_idx = [i for i, h in enumerate(header) if h not in exclude]
    stripped_header = [header[i] for i in keep_idx]
    key_idx = header.index(key_col)
    records: dict = {}
    for row in rows:
        key = str(row[key_idx] or "").strip()
        records[key] = {stripped_header[j]: row[keep_idx[j]] for j in range(len(keep_idx))}
    return stripped_header, records


def _compare_records(
    tab: str,
    legacy_header: list,
    legacy_records: dict,
    new_header: list,
    new_records: dict,
) -> list:
    """Header row + cell-by-cell comparison for one tab. A header mismatch
    is reported as a single `<header>` diff but does not stop row
    comparison -- only columns common to both sides are compared per row,
    so a genuine cell-value desync is still caught even if, say, a column
    got renamed. A key present on only one side is reported as a `<row>`
    diff."""
    diffs: list = []
    if legacy_header != new_header:
        diffs.append(SheetDiff(tab, "<header>", "header", legacy_header, new_header))

    common_cols = [c for c in legacy_header if c in new_header]

    for key in sorted(set(legacy_records) | set(new_records)):
        lrec = legacy_records.get(key)
        nrec = new_records.get(key)
        if lrec is None:
            diffs.append(SheetDiff(tab, key, "<row>", None, "present in new only"))
            continue
        if nrec is None:
            diffs.append(SheetDiff(tab, key, "<row>", "present in legacy only", None))
            continue
        for col in common_cols:
            lv, nv = lrec.get(col), nrec.get(col)
            if not _cells_equal(lv, nv, field=col):
                diffs.append(SheetDiff(tab, key, col, lv, nv))
    return diffs


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def verify_sheets(db_path: Path) -> VerifySheetsResult:
    """Run the Eie/Sold/Stations golden-master comparisons against `db_path`
    (a COPY the caller made -- see `skannonser.commands.verify_cmd`). Never
    constructs a Sheets service; only DataFrame/row-list computation, zero
    network calls, zero API key reads.
    """
    PropertyDatabase, StationDatabase, helper = _import_legacy()
    conn = skconn.connect(db_path)
    try:
        # -- Eie -------------------------------------------------------
        legacy_eie_df = _legacy_eie_df(PropertyDatabase, helper, db_path)
        new_eie_header, new_eie_rows = eie_rows(conn)
        stripped_eie_header, new_eie_records = _records_from_rows(
            new_eie_header, new_eie_rows, exclude=_EIE_NEW_ONLY_COLUMNS
        )
        eie_diffs = _compare_records(
            "Eie",
            list(legacy_eie_df.columns),
            _records_from_df(legacy_eie_df),
            stripped_eie_header,
            new_eie_records,
        )

        # -- Sold --------------------------------------------------------
        legacy_sold_df = _legacy_sold_df(PropertyDatabase, helper, db_path)
        new_sold_header, new_sold_rows = sold_rows(conn)
        stripped_sold_header, new_sold_records = _records_from_rows(
            new_sold_header, new_sold_rows
        )
        sold_diffs = _compare_records(
            "Sold",
            list(legacy_sold_df.columns),
            _records_from_df(legacy_sold_df),
            stripped_sold_header,
            new_sold_records,
        )

        # -- Stations ------------------------------------------------------
        legacy_stations_header, legacy_stations_rows = _legacy_stations_payload(
            StationDatabase, db_path
        )
        legacy_name_idx = legacy_stations_header.index("Name")
        legacy_line_idx = legacy_stations_header.index("Line")
        legacy_stations_records = {
            f"{row[legacy_name_idx]}|{row[legacy_line_idx]}": dict(
                zip(legacy_stations_header, row)
            )
            for row in legacy_stations_rows
        }

        new_stations_header, new_stations_rows = stations_rows(conn)
        new_name_idx = new_stations_header.index("Name")
        new_line_idx = new_stations_header.index("Line")
        new_stations_records = {
            f"{row[new_name_idx]}|{row[new_line_idx]}": dict(zip(new_stations_header, row))
            for row in new_stations_rows
        }

        stations_diffs = _compare_records(
            "Stations",
            legacy_stations_header,
            legacy_stations_records,
            new_stations_header,
            new_stations_records,
        )

        return VerifySheetsResult(
            eie_diffs=eie_diffs, sold_diffs=sold_diffs, stations_diffs=stations_diffs
        )
    finally:
        conn.close()
