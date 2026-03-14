#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main.database.db import PropertyDatabase
from main.post_process import post_process_eiendom
from main.sync import helper_sync_to_sheets as helper


TRAVEL_COLS_BY_TARGET = {
    "all": ["PENDL RUSH BRJ", "PENDL RUSH MVV"],
    "brj": ["PENDL RUSH BRJ"],
    "mvv": ["PENDL RUSH MVV"],
}

FULL_DNB_COL_ORDER = [
    "Adresse",
    "Postnummer",
    "Pris",
    "Boligtype",
    "URL",
    "LAT",
    "LNG",
    "PENDL RUSH BRJ",
    "PENDL RUSH MVV",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill travel columns into existing DNB sheet rows by URL"
    )
    parser.add_argument(
        "--target",
        choices=["all", "brj", "mvv"],
        default="all",
        help="Which travel destination group to compute/update",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned updates without writing to Sheets",
    )
    return parser.parse_args()


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


def _normalize_cell_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text == "":
        return ""
    if text.lower() in {"na", "n/a", "nan", "none", "null", "#n/a", "<na>"}:
        return ""
    try:
        num = float(text)
        if num != num:
            return ""
        if num.is_integer():
            return str(int(num))
        return str(num)
    except Exception:
        return text


def _is_placeholder_cell(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"na", "n/a", "nan", "none", "null", "#n/a", "<na>"}


def _build_work_df(df: pd.DataFrame, travel_cols: list[str]) -> pd.DataFrame:
    out = pd.DataFrame()

    def _make_finnkode(row: pd.Series) -> str:
        dnb_id = row.get("dnb_id")
        if pd.notna(dnb_id) and str(dnb_id).strip():
            return str(dnb_id).strip()

        row_id = row.get("id")
        if pd.notna(row_id):
            try:
                return f"DNB-{int(float(row_id))}"
            except (TypeError, ValueError):
                pass

        return f"DNB-ROW-{row.name}"

    out["Finnkode"] = df.apply(_make_finnkode, axis=1)
    out["Adresse"] = df.get("adresse", "")
    out["Postnummer"] = df.get("postnummer", "")
    out["Pris"] = df.get("pris", "")
    out["Boligtype"] = df.get("property_type", "")
    out["URL"] = df.get("url", "")
    out["LAT"] = df.get("lat", "")
    out["LNG"] = df.get("lng", "")

    existing_col_map = {
        "PENDL RUSH BRJ": "existing_pendl_rush_brj",
        "PENDL RUSH MVV": "existing_pendl_rush_mvv",
    }
    for col in travel_cols:
        src_col = existing_col_map.get(col)
        if src_col and src_col in df.columns:
            out[col] = df[src_col]
        else:
            out[col] = pd.NA

    out["TRAVEL_COPY_FROM_FINNKODE"] = df.get("existing_travel_copy_from_finnkode", pd.NA)

    return out


def _build_shared_donor_seed(db: PropertyDatabase) -> pd.DataFrame:
    seed = db.get_travel_donor_seed().copy()
    if seed.empty:
        return seed

    needed_cols = [
        "Finnkode",
        "LAT",
        "LNG",
            "PENDL RUSH BRJ",
            "PENDL RUSH MVV",
    ]
    for col in needed_cols:
        if col not in seed.columns:
            seed[col] = pd.NA

    return seed[needed_cols].copy()


def _normalize_finnkode_key(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        num = float(text)
        if num.is_integer():
            return str(int(num))
    except Exception:
        pass
    return text


def _fill_missing_from_donor_seed(processed: pd.DataFrame, donor_seed_df: pd.DataFrame, travel_cols: list[str]) -> None:
    if processed.empty or donor_seed_df is None or donor_seed_df.empty:
        return

    donor_map: dict[str, dict] = {}
    for _, row in donor_seed_df.iterrows():
        key = _normalize_finnkode_key(row.get("Finnkode"))
        if not key:
            continue
        donor_map[key] = {col: row.get(col, pd.NA) for col in travel_cols}

    copied_rows = 0
    for idx, row in processed.iterrows():
        donor_key = _normalize_finnkode_key(row.get("TRAVEL_COPY_FROM_FINNKODE"))
        if not donor_key:
            continue

        donor_vals = donor_map.get(donor_key)
        if not donor_vals:
            continue

        # All-or-nothing donor rule: only use donor when every travel column exists.
        if any(pd.isna(donor_vals.get(col, pd.NA)) for col in travel_cols):
            continue

        for col in travel_cols:
            processed.at[idx, col] = donor_vals[col]
        copied_rows += 1

    if copied_rows:
        print(f"Copied complete donor travel sets into {copied_rows} row(s)")


def _persist_shared_travel_seed(db: PropertyDatabase, processed: pd.DataFrame) -> None:
    def _db_value(value):
        return None if pd.isna(value) else value

    for _, row in processed.iterrows():
        finnkode = str(row.get("Finnkode", "") or "").strip()
        if not finnkode:
            continue

        db.insert_or_update_eiendom_processed(
            finnkode=finnkode,
            adresse=str(row.get("Adresse", "") or ""),
            postnummer=str(row.get("Postnummer", "") or ""),
            lat=_db_value(row.get("LAT", None)),
            lng=_db_value(row.get("LNG", None)),
                pendl_rush_brj=_db_value(row.get("PENDL RUSH BRJ", None)),
                pendl_rush_mvv=_db_value(row.get("PENDL RUSH MVV", None)),
            travel_copy_from_finnkode=_db_value(row.get("TRAVEL_COPY_FROM_FINNKODE", None)),
        )


def main() -> int:
    args = parse_args()
    travel_cols = TRAVEL_COLS_BY_TARGET[args.target]

    db = PropertyDatabase()
    conn = db.get_connection()
    try:
        # Backfill against rows that should exist in DNB sheet (active DNB-only rows).
        src = pd.read_sql_query(
            """
                        SELECT
                                d.*,
                                ep.pendl_rush_brj AS existing_pendl_rush_brj,
                                ep.pendl_rush_mvv AS existing_pendl_rush_mvv,
                                ep.travel_copy_from_finnkode AS existing_travel_copy_from_finnkode
                        FROM dnbeiendom d
                        LEFT JOIN eiendom_processed ep
                            ON ep.finnkode = COALESCE(d.dnb_id, 'DNB-' || d.id)
                        WHERE d.active = 1
                            AND (d.duplicate_of_finnkode IS NULL OR TRIM(d.duplicate_of_finnkode) = '')
            ORDER BY scraped_at DESC
            """,
            conn,
        )
    finally:
        conn.close()

    if src.empty:
        print("No active DNB-only rows found for backfill.")
        return 0

    work_df = _build_work_df(src, travel_cols)
    donor_seed_df = _build_shared_donor_seed(db)
    processed = post_process_eiendom(
        work_df,
        projectName="data/dnbeiendom",
        db=None,
        calculate_location_features=not args.dry_run,
        calculate_google_directions=not args.dry_run,
        travel_targets=args.target,
        donor_seed_df=donor_seed_df,
    )

    _fill_missing_from_donor_seed(processed, donor_seed_df, travel_cols)

    _persist_shared_travel_seed(db, processed)

    processed_map = {}
    for _, row in processed.iterrows():
        url = _normalize_url(row.get("URL"))
        if not url:
            continue
        processed_map[url] = {col: row.get(col, "") for col in travel_cols}

    if not processed_map:
        print("No processed rows with URL found.")
        return 0

    service = helper.get_sheets_service()
    sheet_name = "DNB"
    helper.ensure_sheet_exists(service, sheet_name)
    helper.ensure_sheet_headers(service, sheet_name, FULL_DNB_COL_ORDER)

    read_range = f"{sheet_name}!A1:AZ20000"
    res = service.spreadsheets().values().get(
        spreadsheetId=helper.SPREADSHEET_ID,
        range=read_range,
    ).execute()
    values = res.get("values", [])

    if not values:
        print("DNB sheet is empty; nothing to backfill.")
        return 0

    headers = [helper.canonicalize_header_name(h) for h in values[0]]
    if "URL" not in headers:
        print("DNB sheet has no URL header; cannot backfill by URL.")
        return 1

    url_idx = headers.index("URL")
    travel_indices = {}
    for col in travel_cols:
        if col in headers:
            travel_indices[col] = headers.index(col)

    if len(travel_indices) != len(travel_cols):
        missing = [c for c in travel_cols if c not in travel_indices]
        print(f"Missing travel headers in DNB sheet after ensure: {missing}")
        return 1

    updates = []
    matched_rows = 0

    for row_num, row in enumerate(values[1:], start=2):
        if len(row) <= url_idx:
            continue
        url = _normalize_url(row[url_idx])
        if not url:
            continue

        new_values = processed_map.get(url)
        if not new_values:
            continue

        matched_rows += 1
        row_updates = []
        for col in travel_cols:
            col_idx = travel_indices[col]
            old = row[col_idx] if col_idx < len(row) else ""
            new = new_values.get(col, "")

            old_norm = _normalize_cell_value(old)
            new_norm = _normalize_cell_value(new)
            should_clear_placeholder = new_norm == "" and _is_placeholder_cell(old)
            if not should_clear_placeholder and (new_norm == "" or new_norm == old_norm):
                continue

            row_updates.append((col_idx, new_norm))

        if not row_updates:
            continue

        for col_idx, new_value in row_updates:
            col_letter = chr(ord("A") + col_idx)
            updates.append(
                {
                    "range": f"{sheet_name}!{col_letter}{row_num}",
                    "values": [[new_value]],
                }
            )

    print(f"Processed rows in memory: {len(processed_map)}")
    print(f"Matched DNB sheet rows by URL: {matched_rows}")
    print(f"Cell updates needed: {len(updates)}")

    if args.dry_run or not updates:
        if args.dry_run:
            print("Dry run enabled; no sheet updates written.")
        return 0

    batch_size = 500
    applied = 0
    for i in range(0, len(updates), batch_size):
        chunk = updates[i:i + batch_size]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=helper.SPREADSHEET_ID,
            body={
                "valueInputOption": "RAW",
                "data": chunk,
            },
        ).execute()
        applied += len(chunk)

    print(f"Applied cell updates: {applied}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
