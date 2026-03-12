#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
# Ensure project root on sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main.database.db import PropertyDatabase
from main.post_process import post_process_eiendom
from main.sync import helper_sync_to_sheets as helper
import pandas as pd


TRAVEL_EXPORT_COLS = [
    'PENDL MORN BRJ',
    'PENDL DAG BRJ',
    'PENDL MORN MVV',
    'PENDL DAG MVV',
]

BASE_EXPORT_COLS = ['Adresse', 'Postnummer', 'Pris', 'Boligtype', 'URL', 'LAT', 'LNG']
ALL_EXPORT_COLS = BASE_EXPORT_COLS + TRAVEL_EXPORT_COLS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export new DNB listings to Google Sheets, including travel fields"
    )
    parser.add_argument(
        "--skip-travel-api",
        action="store_true",
        help="Do not call Google Routes API; export travel columns as-is",
    )
    parser.add_argument(
        "--target",
        choices=["all", "brj", "mvv"],
        default="all",
        help="Select which transit destination group to calculate",
    )
    return parser.parse_args()


def _build_work_df(dnb_only: pd.DataFrame) -> pd.DataFrame:
    def _make_finnkode(row: pd.Series) -> str:
        dnb_id = row.get('dnb_id')
        if pd.notna(dnb_id) and str(dnb_id).strip():
            return str(dnb_id).strip()

        row_id = row.get('id')
        if pd.notna(row_id):
            try:
                return f"DNB-{int(float(row_id))}"
            except (TypeError, ValueError):
                pass

        return f"DNB-ROW-{row.name}"

    work_df = pd.DataFrame()
    # Synthetic Finnkode makes donor-link logic deterministic within this batch.
    work_df['Finnkode'] = dnb_only.apply(_make_finnkode, axis=1)
    work_df['Adresse'] = dnb_only.get('adresse', '')
    work_df['Postnummer'] = dnb_only.get('postnummer', '')
    work_df['Pris'] = dnb_only.get('pris', '')
    work_df['Boligtype'] = dnb_only.get('property_type', '')
    work_df['URL'] = dnb_only.get('url', '')
    work_df['LAT'] = dnb_only.get('lat', '')
    work_df['LNG'] = dnb_only.get('lng', '')

    for col in TRAVEL_EXPORT_COLS:
        if col not in work_df.columns:
            work_df[col] = pd.NA

    return work_df


def _build_shared_donor_seed(db: PropertyDatabase) -> pd.DataFrame:
    seed = db.get_travel_donor_seed().copy()
    if seed.empty:
        return seed

    needed_cols = [
        'Finnkode',
        'LAT',
        'LNG',
        'PENDL MORN BRJ',
        'PENDL DAG BRJ',
        'PENDL MORN MVV',
        'PENDL DAG MVV',
    ]
    for col in needed_cols:
        if col not in seed.columns:
            seed[col] = pd.NA

    return seed[needed_cols].copy()


def _persist_shared_travel_seed(db: PropertyDatabase, processed: pd.DataFrame) -> None:
    def _db_value(value):
        return None if pd.isna(value) else value

    for _, row in processed.iterrows():
        finnkode = str(row.get('Finnkode', '') or '').strip()
        if not finnkode:
            continue

        db.insert_or_update_eiendom_processed(
            finnkode=finnkode,
            adresse=str(row.get('Adresse', '') or ''),
            postnummer=str(row.get('Postnummer', '') or ''),
            lat=_db_value(row.get('LAT', None)),
            lng=_db_value(row.get('LNG', None)),
            pendl_morn_brj=_db_value(row.get('PENDL MORN BRJ', None)),
            pendl_dag_brj=_db_value(row.get('PENDL DAG BRJ', None)),
            pendl_morn_mvv=_db_value(row.get('PENDL MORN MVV', None)),
            pendl_dag_mvv=_db_value(row.get('PENDL DAG MVV', None)),
            travel_copy_from_finnkode=_db_value(row.get('TRAVEL_COPY_FROM_FINNKODE', None)),
        )


def main():
    args = parse_args()
    db = PropertyDatabase()
    service = helper.get_sheets_service()
    sheet_name = 'DNB'
    helper.ensure_sheet_exists(service, sheet_name)
    helper.ensure_sheet_headers(service, sheet_name, ALL_EXPORT_COLS)

    df = db.get_new_dnbeiendom_for_export()
    print(f"New dnbeiendom rows for export (pre-filter): {len(df)}")
    if df.empty:
        print("No new DNB rows to export. Headers were still ensured.")
        return

    mask = df['duplicate_of_finnkode'].isnull() | (df['duplicate_of_finnkode'].astype(str).str.strip() == '')
    dnb_only = df.loc[mask].copy()
    print(f"DNB-only rows to export: {len(dnb_only)}")
    if dnb_only.empty:
        print("No DNB-only rows to export. Headers were still ensured.")
        return

    work_df = _build_work_df(dnb_only)
    donor_seed_df = _build_shared_donor_seed(db)

    calculate_directions = not args.skip_travel_api
    processed = post_process_eiendom(
        work_df,
        projectName='data/dnbeiendom',
        db=None,
        calculate_location_features=calculate_directions,
        calculate_google_directions=calculate_directions,
        travel_targets=args.target,
        donor_seed_df=donor_seed_df,
    )

    _persist_shared_travel_seed(db, processed)

    export_df = pd.DataFrame()
    export_df['Adresse'] = processed.get('Adresse', '')
    export_df['Postnummer'] = processed.get('Postnummer', '')
    export_df['Pris'] = processed.get('Pris', '')
    export_df['URL'] = processed.get('URL', '')
    export_df['LAT'] = processed.get('LAT', '')
    export_df['LNG'] = processed.get('LNG', '')
    for col in TRAVEL_EXPORT_COLS:
        export_df[col] = processed.get(col, pd.NA)

    # Avoid nullable Int64 fillna('') issues during sheet sanitization.
    export_df = export_df.astype(object)

    export_df = helper.dedupe_and_canonicalize_dataframe_columns(export_df)
    export_df = helper.sanitize_for_sheets(export_df)

    headers = list(export_df.columns)
    helper.ensure_sheet_headers(service, sheet_name, headers)

    body = {'values': export_df.values.tolist()}
    res = service.spreadsheets().values().append(spreadsheetId=helper.SPREADSHEET_ID, range=sheet_name, valueInputOption='RAW', body=body).execute()
    appended = res.get('updates', {}).get('updatedRows', 0)
    print(f"Appended rows: {appended}")

    urls = dnb_only['url'].fillna('').astype(str).tolist()
    if urls:
        marked = db.mark_dnbeiendom_as_exported(urls)
        print(f"Marked {marked} rows exported in DB.")


if __name__ == '__main__':
    main()
