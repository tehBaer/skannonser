#!/usr/bin/env python3
"""Sync DNB sheet from DB:
  - Update mutable fields (Adresse, Postnummer, Pris, Boligtype, LAT, LNG) on existing rows
  - Delete rows whose URL is no longer active in the DB
  - Append new active DNB-only rows not yet in the sheet (base fields; no travel API)

Travel columns are not touched — use 'make dnb-backfill-travel' to fill those.
"""
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main.database.db import PropertyDatabase
from main.config.filters import SHEETS_MAX_PRICE
from main.sync import helper_sync_to_sheets as helper

MUTABLE_COLS = ["Adresse", "Postnummer", "Pris", "Boligtype", "LAT", "LNG"]
TRAVEL_COLS = ["PENDL RUSH BRJ", "PENDL RUSH MVV", "MVV UNI RUSH"]
SYNC_CHECK_COLS = MUTABLE_COLS + TRAVEL_COLS
FULL_COL_ORDER = [
    "Adresse", "Postnummer", "Pris", "Boligtype", "URL", "LAT", "LNG",
    "PENDL RUSH BRJ", "PENDL RUSH MVV", "MVV UNI RUSH",
]


def _normalize_url(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
        scheme = (parsed.scheme or "https").lower()
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip("/")
        return urlunsplit((scheme, netloc, path, "", ""))
    except Exception:
        return raw.rstrip("/")


def _normalize_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:  # NaN check (NaN != NaN)
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        num = float(text)
        if num != num:  # "nan" string parsed as float NaN
            return ""
        return str(int(num)) if num.is_integer() else str(num)
    except Exception:
        return text


def _is_placeholder_cell(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"na", "n/a", "nan", "none", "null", "#n/a", "<na>"}


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to A1 letter notation (handles >26 columns)."""
    result = ""
    n = idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


def _get_sheet_id(service, sheet_name: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=helper.SPREADSHEET_ID).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            return s["properties"]["sheetId"]
    raise ValueError(f"Sheet '{sheet_name}' not found")


def _append_rows(service, sheet_name: str, rows: list, col_headers: list) -> None:
    if not rows:
        return
    data = [[str(row_dict.get(h, "")) for h in col_headers] for row_dict in rows]
    service.spreadsheets().values().append(
        spreadsheetId=helper.SPREADSHEET_ID,
        range=sheet_name,
        valueInputOption="RAW",
        body={"values": data},
    ).execute()


def main() -> None:
    db = PropertyDatabase()
    conn = db.get_connection()
    try:
        if SHEETS_MAX_PRICE is None:
            src = pd.read_sql_query(
                """
                SELECT * FROM dnbeiendom
                WHERE active = 1
                  AND (duplicate_of_finnkode IS NULL OR TRIM(duplicate_of_finnkode) = '')
                ORDER BY scraped_at DESC
                """,
                conn,
            )
        else:
            src = pd.read_sql_query(
                """
                SELECT * FROM dnbeiendom
                WHERE active = 1
                  AND (duplicate_of_finnkode IS NULL OR TRIM(duplicate_of_finnkode) = '')
                  AND COALESCE(pris, 0) <= ?
                ORDER BY scraped_at DESC
                """,
                conn,
                params=(int(SHEETS_MAX_PRICE),),
            )
        total_active_dnb_only = pd.read_sql_query(
            """
            SELECT COUNT(*) AS count
            FROM dnbeiendom
            WHERE active = 1
              AND (duplicate_of_finnkode IS NULL OR TRIM(duplicate_of_finnkode) = '')
            """,
            conn,
        ).iloc[0]["count"]
    finally:
        conn.close()

    excluded_by_price = (int(total_active_dnb_only) - len(src)) if SHEETS_MAX_PRICE is not None else 0

    db_by_url: dict = {}
    for _, row in src.iterrows():
        url = _normalize_url(row.get("url"))
        if url:
            db_by_url[url] = {
                "Adresse": _normalize_cell(row.get("adresse")),
                "Postnummer": _normalize_cell(row.get("postnummer")),
                "Pris": _normalize_cell(row.get("pris")),
                "Boligtype": _normalize_cell(row.get("property_type")),
                "URL": str(row.get("url") or ""),
                "LAT": _normalize_cell(row.get("lat")),
                "LNG": _normalize_cell(row.get("lng")),
            }

    service = helper.get_sheets_service()
    sheet_name = "DNB"
    helper.ensure_sheet_exists(service, sheet_name)
    helper.ensure_sheet_headers(service, sheet_name, FULL_COL_ORDER)

    res = service.spreadsheets().values().get(
        spreadsheetId=helper.SPREADSHEET_ID,
        range=f"{sheet_name}!A1:AZ20000",
    ).execute()
    values = res.get("values", [])

    if not values:
        print(f"[DNB sheet] empty sheet -> append={len(db_by_url)}")
        _append_rows(service, sheet_name, list(db_by_url.values()), FULL_COL_ORDER)
        return

    headers = [helper.canonicalize_header_name(h) for h in values[0]]
    if "URL" not in headers:
        print("[DNB sheet] URL column missing; sync aborted")
        return

    url_idx = headers.index("URL")
    sheet_urls: set = set()
    cell_updates: list = []
    rows_to_delete: list = []  # 0-based data row indices (0 = first data row after header)

    for data_idx, row in enumerate(values[1:]):
        url = _normalize_url(row[url_idx] if url_idx < len(row) else "")
        if not url:
            continue
        sheet_urls.add(url)

        db_row = db_by_url.get(url)
        if db_row is None:
            rows_to_delete.append(data_idx)
            continue

        sheet_row_num = data_idx + 2  # 1-based spreadsheet row number
        for col in SYNC_CHECK_COLS:
            if col not in headers:
                continue
            col_idx = headers.index(col)
            old_raw = row[col_idx] if col_idx < len(row) else ""
            old = _normalize_cell(old_raw)
            new = _normalize_cell(db_row.get(col, "")) if col in MUTABLE_COLS else ""
            should_clear_placeholder = not new and _is_placeholder_cell(old_raw)
            if (new != old and new) or should_clear_placeholder:
                cell_updates.append({
                    "range": f"{sheet_name}!{_col_letter(col_idx)}{sheet_row_num}",
                    "values": [[new]],
                })

    new_urls = [u for u in db_by_url if u not in sheet_urls]

    print(
        "[DNB sheet] "
        f"db_active={len(src)} "
        f"excluded_by_price={excluded_by_price} "
        f"sheet_rows={len(values) - 1} "
        f"delete={len(rows_to_delete)} "
        f"cell_updates={len(cell_updates)} "
        f"append={len(new_urls)}"
    )

    # 1. Cell updates
    if cell_updates:
        for i in range(0, len(cell_updates), 500):
            chunk = cell_updates[i:i + 500]
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=helper.SPREADSHEET_ID,
                body={"valueInputOption": "RAW", "data": chunk},
            ).execute()
        print(f"[DNB sheet] updated_cells={len(cell_updates)}")

    # 2. Delete stale rows (descending order so earlier indices stay valid)
    if rows_to_delete:
        sheet_id = _get_sheet_id(service, sheet_name)
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": data_idx + 1,  # +1 to skip header row
                        "endIndex": data_idx + 2,
                    }
                }
            }
            for data_idx in sorted(rows_to_delete, reverse=True)
        ]
        service.spreadsheets().batchUpdate(
            spreadsheetId=helper.SPREADSHEET_ID,
            body={"requests": requests},
        ).execute()
        print(f"[DNB sheet] deleted_rows={len(rows_to_delete)}")

    # 3. Append new rows (base fields only; travel times filled by make dnb-backfill-travel)
    if new_urls:
        new_rows = [db_by_url[u] for u in new_urls]
        _append_rows(service, sheet_name, new_rows, headers)
        print(f"[DNB sheet] appended_rows={len(new_urls)}")


if __name__ == "__main__":
    main()
