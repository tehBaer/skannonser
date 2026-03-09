#!/usr/bin/env python3
import sys
from pathlib import Path
# Ensure project root on sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main.database.db import PropertyDatabase
from main.sync import helper_sync_to_sheets as helper
import pandas as pd


def main():
    db = PropertyDatabase()
    df = db.get_new_dnbeiendom_for_export()
    print(f"New dnbeiendom rows for export (pre-filter): {len(df)}")
    if df.empty:
        return

    mask = df['duplicate_of_finnkode'].isnull() | (df['duplicate_of_finnkode'].astype(str).str.strip() == '')
    dnb_only = df.loc[mask].copy()
    print(f"DNB-only rows to export: {len(dnb_only)}")
    if dnb_only.empty:
        return

    export_df = pd.DataFrame()
    export_df['Adresse'] = dnb_only.get('adresse', '')
    export_df['Postnummer'] = dnb_only.get('postnummer', '')
    export_df['Pris'] = dnb_only.get('pris', '')
    export_df['URL'] = dnb_only.get('url', '')
    export_df['LAT'] = dnb_only.get('lat', '')
    export_df['LNG'] = dnb_only.get('lng', '')

    export_df = helper.dedupe_and_canonicalize_dataframe_columns(export_df)
    export_df = helper.sanitize_for_sheets(export_df)

    service = helper.get_sheets_service()
    sheet_name = 'DNB'
    helper.ensure_sheet_exists(service, sheet_name)
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
