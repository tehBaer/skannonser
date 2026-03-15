"""
Sync database data to Google Sheets for visualization.
This maintains the Google Sheets interface while using a database backend.
"""
import sys
import os
import pandas as pd
from typing import List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import get_sheets_service, SPREADSHEET_ID, download_sheet_as_csv
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_sheets_service, SPREADSHEET_ID, download_sheet_as_csv
from googleapiclient.errors import HttpError


HIDDEN_TILGJENGELIGHET_STATUSES = {"solgt", "inaktiv"}

# Canonical header names to avoid duplicate legacy/new variants in Sheets.
HEADER_ALIASES = {
    'lat': 'LAT',
    'lng': 'LNG',
    'latitude': 'LAT',
    'longitude': 'LNG',
    'pendl rush mvv uni rush': 'MVV UNI RUSH',
}


def canonicalize_header_name(name: str) -> str:
    """Map legacy header variants to a canonical name."""
    raw = str(name or '').strip()
    if not raw:
        return raw
    alias = HEADER_ALIASES.get(raw.lower())
    return alias if alias else raw


def filter_rows_for_sheet_visibility(df: pd.DataFrame, db: PropertyDatabase) -> pd.DataFrame:
    """Exclude rows that should be hidden in sheets for now.

    Hidden rules:
    - active != 1
    - Tilgjengelighet in {Solgt, Inaktiv} (case-insensitive)
    """
    if df.empty:
        return df

    out = df.copy()
    out['Finnkode'] = out['Finnkode'].astype(str).str.strip()

    status_df = db.get_eiendom_for_status_refresh(only_inactive=False)
    if status_df.empty:
        return out

    status_df['Finnkode'] = status_df['Finnkode'].astype(str).str.strip()
    # Support databases that still have legacy `stale` column or the new `active` column.
    active_col = 'active' if 'active' in status_df.columns else None

    if active_col is not None:
        active_lookup = status_df.set_index('Finnkode')[active_col].to_dict()
    else:
        # If neither column exists, assume everything is active.
        active_lookup = {}

    tilgjengelighet_lookup = status_df.set_index('Finnkode')['Tilgjengelighet'].to_dict()

    # Map active lookup; default to 1 (active) when unknown.
    out['_sync_active'] = pd.to_numeric(
        out['Finnkode'].map(active_lookup), errors='coerce'
    ).fillna(1).astype(int)

    out['_sync_tilg'] = out['Finnkode'].map(tilgjengelighet_lookup)
    if 'Tilgjengelighet' in out.columns:
        out['_sync_tilg'] = out['_sync_tilg'].combine_first(out['Tilgjengelighet'])

    normalized_status = (
        out['_sync_tilg']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.lower()
    )

    active_mask = out['_sync_active'] == 1
    status_hidden_mask = normalized_status.isin(HIDDEN_TILGJENGELIGHET_STATUSES)
    visible_mask = active_mask & (~status_hidden_mask)

    excluded = int((~visible_mask).sum())
    if excluded > 0:
        active_hidden = int((~active_mask).sum())
        print(
            "Excluded "
            f"{excluded} rows from Eie sync "
            f"(active=0: {active_hidden}, "
            f"Tilgjengelighet=Solgt/Inaktiv: {int(status_hidden_mask.sum())})"
        )

    out = out.loc[visible_mask].copy()
    out.drop(columns=['_sync_active', '_sync_tilg'], inplace=True, errors='ignore')
    return out


HIDDEN_SHEET_COLUMNS = {
    'PENDL MORN CNTR',
    'BIL MORN CNTR',
    'PENDL DAG CNTR',
    'BIL DAG CNTR',
}


def filter_hidden_sheet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove internal-only columns that should not be written to Google Sheets."""
    visible_columns = [c for c in df.columns if c not in HIDDEN_SHEET_COLUMNS]
    return df[visible_columns].copy()


def dedupe_and_canonicalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate/alias columns into one canonical column.

    If multiple source columns map to the same canonical name, keep the first
    non-empty value from left-to-right for each row.
    """
    if df.empty:
        return df

    canonical_groups = {}
    ordered_canonical = []

    for idx, col in enumerate(df.columns):
        canon = canonicalize_header_name(col)
        if not canon:
            continue
        if canon not in canonical_groups:
            canonical_groups[canon] = []
            ordered_canonical.append(canon)
        canonical_groups[canon].append(idx)

    merged = pd.DataFrame(index=df.index)
    for canon in ordered_canonical:
        idxs = canonical_groups[canon]
        subset = df.iloc[:, idxs].copy()
        # Treat empty strings as missing so we can take first non-empty value.
        subset = subset.replace('', pd.NA)
        merged[canon] = subset.bfill(axis=1).iloc[:, 0]

    return merged


def sanitize_for_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data for Google Sheets export."""
    # Convert commute columns to integers without decimals before fillna
    commute_cols = ['PENDL RUSH BRJ', 'PENDL RUSH MVV', 'MVV UNI RUSH']
    area_cols = [
        'Bruksareal',
        'Internt bruksareal (BRA-i)',
        'Primærrom',
        'Bruttoareal',
        'Eksternt bruksareal (BRA-e)',
        'Innglasset balkong (BRA-b)',
        'Balkong/Terrasse (TBA)',
        'Tomteareal'
    ]
    year_cols = ['Byggeår']
    for col in commute_cols:
        if col in df.columns:
            # Convert to numeric, round, and convert to int (NaN becomes empty string in next step)
            df[col] = pd.to_numeric(df[col], errors='coerce').round()
    for col in area_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round()
    for col in year_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round()
    
    # Replace NaN with empty string
    df = df.fillna('')
    
    # Convert numeric columns that should be integers (after fillna converted NaN to '')
    for col in commute_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if x != '' and pd.notna(x) else x)
    for col in area_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if x != '' and pd.notna(x) else x)
    for col in year_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if x != '' and pd.notna(x) else x)
    
    # Clean strings: replace newlines and strip whitespace
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').strip() if x else '')
    
    return df


def ensure_sheet_headers(service, sheet_name: str, desired_columns: List[str]) -> List[str]:
    """Ensure sheet header is canonical and includes all desired columns.

    Important: preserve existing sheet column order to avoid row-value drift.
    We only canonicalize/de-duplicate existing header names and append missing
    desired columns to the end.
    """
    range_name = f"{sheet_name}!A1:AZ1"
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name
    ).execute()

    header_row = result.get("values", [[]])
    header = header_row[0] if header_row else []

    # Canonicalize and de-duplicate existing headers (e.g. lat/LAT -> LAT).
    updated_header = []
    seen = set()
    for col in header:
        canon = canonicalize_header_name(col)
        if not canon:
            continue
        if canon in seen:
            continue
        seen.add(canon)
        updated_header.append(canon)

    header_normalized = [col.strip() for col in updated_header]

    desired_canonical = []
    seen_desired = set()
    for col in desired_columns:
        canon = canonicalize_header_name(col)
        if canon and canon not in seen_desired:
            desired_canonical.append(canon)
            seen_desired.add(canon)

    # Preserve current order; append only truly missing desired columns.
    for col in desired_canonical:
        if col not in seen:
            updated_header.append(col)
            seen.add(col)

    if updated_header != header:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": [updated_header]}
        ).execute()
        print(f"✓ Updated sheet headers with {len(updated_header)} columns")

    return [col.strip() for col in updated_header]


def ensure_sheet_exists(service, sheet_name: str) -> None:
    """Create sheet tab when missing."""
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(title))"
    ).execute()

    existing_titles = {
        s.get("properties", {}).get("title", "")
        for s in spreadsheet.get("sheets", [])
    }
    if sheet_name in existing_titles:
        return

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name,
                        }
                    }
                }
            ]
        },
    ).execute()
    print(f"✓ Created sheet tab: {sheet_name}")


def rename_sheet_if_exists(service, old_name: str, new_name: str) -> bool:
    """Rename a sheet tab when old exists and new does not."""
    if not old_name or not new_name or old_name == new_name:
        return False

    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title))"
    ).execute()

    sheets = spreadsheet.get("sheets", [])
    by_title = {
        s.get("properties", {}).get("title", ""): s.get("properties", {})
        for s in sheets
    }

    if new_name in by_title or old_name not in by_title:
        return False

    old_props = by_title[old_name]
    sheet_id = old_props.get("sheetId")
    if sheet_id is None:
        return False

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "title": new_name,
                        },
                        "fields": "title",
                    }
                }
            ]
        },
    ).execute()

    print(f"✓ Renamed sheet tab: {old_name} -> {new_name}")
    return True


def _column_number_to_letter(col_num: int) -> str:
    """Convert 1-based column number to Excel/Sheets column letters."""
    result = ""
    while col_num > 0:
        col_num, rem = divmod(col_num - 1, 26)
        result = chr(65 + rem) + result
    return result


def _normalize_finnkode(value) -> str:
    """Normalize Finnkode values from DB/Sheets for stable duplicate checks."""
    if value is None:
        return ""

    finnkode = str(value).strip()
    if not finnkode:
        return ""

    # If it's a HYPERLINK formula, extract the display text
    if 'HYPERLINK' in finnkode.upper():
        parts = finnkode.split('"')
        if len(parts) >= 4:
            finnkode = parts[3].strip()

    # Normalize integer-like float representations (e.g. 123456789.0)
    try:
        as_float = float(finnkode)
        if as_float.is_integer():
            return str(int(as_float))
    except (ValueError, TypeError):
        pass

    return finnkode


def get_existing_finnkodes_from_sheet(service, sheet_name: str) -> List[str]:
    """Get all existing Finnkodes from the Google Sheet."""
    try:
        header_result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!1:1"
        ).execute()
        header_row = [str(col).strip() for col in header_result.get("values", [[]])[0]]

        if not header_row:
            return []

        # Prefer actual Finnkode column by header name; fallback to column A for legacy sheets.
        if "Finnkode" in header_row:
            finnkode_col_idx = header_row.index("Finnkode") + 1
        else:
            finnkode_col_idx = 1

        finnkode_col_letter = _column_number_to_letter(finnkode_col_idx)
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!{finnkode_col_letter}2:{finnkode_col_letter}"
        ).execute()

        values = result.get("values", [])

        finnkodes = []
        seen = set()
        for row in values:
            if row and len(row) > 0:
                normalized = _normalize_finnkode(row[0])
                if normalized and normalized not in seen:
                    finnkodes.append(normalized)
                    seen.add(normalized)

        return finnkodes
        
    except HttpError as e:
        print(f"Error reading from sheet: {e}")
        return []


def sync_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Eie"):
    """
    Sync property listings from database to Google Sheets.
    Includes all listings: active, unlisted/inactive, and sold.
    Only adds new listings that aren't already in the sheet.
    
    Args:
        db_path: Optional path to database file
        sheet_name: Name of the sheet to sync to (default: "Eie")
    """
    print(f"\n{'='*60}")
    print(f"Syncing eiendom data to Google Sheets")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*60}\n")
    
    # Initialize database
    db = PropertyDatabase(db_path)
    
    # Get credentials and service
    try:
        service = get_sheets_service()
        ensure_sheet_exists(service, sheet_name)
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return False
    
    # Get all listings from database then apply temporary sheet visibility rules.
    df = db.get_eiendom_for_sheets()
    df = filter_rows_for_sheet_visibility(df, db)
    
    if df.empty:
        print("No listings to sync")
        return True
    
    print(f"Found {len(df)} listings in database after sheet visibility filters")
    
    # Get existing Finnkodes from sheet
    existing_finnkodes = get_existing_finnkodes_from_sheet(service, sheet_name)
    print(f"Found {len(existing_finnkodes)} existing listings in Google Sheets")
    
    # Normalize and de-duplicate source data before filtering
    df['Finnkode'] = df['Finnkode'].apply(_normalize_finnkode)
    before_dedup = len(df)
    df = df[df['Finnkode'] != ''].drop_duplicates(subset=['Finnkode'], keep='first')
    removed_duplicates = before_dedup - len(df)
    if removed_duplicates > 0:
        print(f"Removed {removed_duplicates} duplicate/empty Finnkoder from database result before sync")

    existing_finnkodes_set = set(existing_finnkodes)
    new_listings = df[~df['Finnkode'].isin(existing_finnkodes_set)]
    
    if new_listings.empty:
        print("No new listings to add to Google Sheets")
        return True
    
    print(f"Found {len(new_listings)} new listings to add")
    
    # Keep internal-only columns in DB, but hide them from Sheets.
    new_listings = filter_hidden_sheet_columns(new_listings)
    # Normalize aliases and remove duplicate columns before building headers.
    new_listings = dedupe_and_canonicalize_dataframe_columns(new_listings)
    # Sanitize data
    new_listings = sanitize_for_sheets(new_listings)

    # Ensure headers contain new columns and align row order
    desired_columns = list(new_listings.columns)
    header_row = ensure_sheet_headers(service, sheet_name, desired_columns)
    new_rows = []
    for _, row in new_listings.iterrows():
        new_rows.append([row.get(col, '') for col in header_row])
    
    # Find the next available row in the sheet
    try:
        range_name = f"{sheet_name}!A1:H10000"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range=range_name
        ).execute()
        existing_data = result.get("values", [])
        next_row = len(existing_data) + 1
    except HttpError as e:
        print(f"Error finding next row: {e}")
        next_row = 2  # Default to row 2 if error
    
    # Append new rows to the sheet
    try:
        body = {"values": new_rows}
        result = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A{next_row}",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        
        updates = result.get('updates', {})
        updated_rows = updates.get('updatedRows', 0)
        
        print(f"\n✓ Successfully added {updated_rows} new listings to Google Sheets")
        
        # Mark as exported in database
        exported_finnkodes = new_listings['Finnkode'].tolist()
        clean_finnkodes = [str(fk) for fk in exported_finnkodes]
        
        marked = db.mark_as_exported('eiendom', clean_finnkodes)
        print(f"✓ Marked {marked} listings as exported in database")
        
        return True
        
    except HttpError as e:
        print(f"Error appending to sheet: {e}")
        return False


def sync_unlisted_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Eie(unlisted)"):
    """
    DEPRECATED: Unlisted listings are now included in the main sheet.
    This function is kept for backward compatibility but does nothing.
    """
    print("\n⚠️  Note: Unlisted/inactive listings are now included in the main 'Eie' sheet.")
    print("This function is no longer needed.")
    return True


def sync_stale_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Sold"):
    """Full-sync sold/inactive listings to dedicated sheet tab."""
    print(f"\n{'='*60}")
    print("Syncing sold/inactive eiendom listings to Google Sheets")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*60}\n")

    db = PropertyDatabase(db_path)

    try:
        service = get_sheets_service()
        if sheet_name == "Sold":
            rename_sheet_if_exists(service, "Stale", "Sold")
        ensure_sheet_exists(service, sheet_name)
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return False


    def sync_dnbeiendom_to_sheets(db_path: str = None, sheet_name: str = "DNB"):
        """
        Sync DNB-only listings (those without a mapped FINN duplicate) to a separate sheet.
        This keeps FINN as canonical while still preserving DNB-only listings in Sheets.
        """
        print(f"\n{'='*60}")
        print(f"Syncing DNB-only listings to Google Sheets")
        print(f"Sheet: {sheet_name}")
        print(f"{'='*60}\n")

        db = PropertyDatabase(db_path)

        try:
            service = get_sheets_service()
            ensure_sheet_exists(service, sheet_name)
        except Exception as e:
            print(f"Error connecting to Google Sheets: {e}")
            return False

        # Get new DNB rows to export
        df = db.get_new_dnbeiendom_for_export()
        if df.empty:
            print("No new DNB listings to export")
            return True

        # Keep only DNB rows that are not mapped to a FINN listing
        mask = df['duplicate_of_finnkode'].isnull() | (df['duplicate_of_finnkode'] == '')
        df = df.loc[mask].copy()
        if df.empty:
            print("No DNB-only listings to export (all mapped to FINN)")
            return True

        # Map dnbeiendom columns to export-friendly column names
        # Minimal mapping: Adresse, Postnummer, Pris, URL, LAT, LNG
        export_df = pd.DataFrame()
        export_df['Adresse'] = df.get('adresse', '')
        export_df['Postnummer'] = df.get('postnummer', '')
        export_df['Pris'] = df.get('pris', '')
        export_df['URL'] = df.get('url', '')
        export_df['LAT'] = df.get('lat', '')
        export_df['LNG'] = df.get('lng', '')

        # Prepare and sanitize
        export_df = dedupe_and_canonicalize_dataframe_columns(export_df)
        export_df = sanitize_for_sheets(export_df)

        # Ensure headers and append rows
        desired_columns = list(export_df.columns)
        header_row = ensure_sheet_headers(service, sheet_name, desired_columns)
        new_rows = [[row.get(col, '') for col in header_row] for _, row in export_df.iterrows()]

        try:
            # Append
            body = {"values": new_rows}
            range_start = f"{sheet_name}!A1"
            result = service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=range_start,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()

            updates = result.get('updates', {})
            updated_rows = updates.get('updatedRows', 0)
            print(f"✓ Added {updated_rows} DNB listings to sheet '{sheet_name}'")

            # Mark exported by URL
            urls = df['url'].fillna('').astype(str).tolist()
            marked = db.mark_dnbeiendom_as_exported(urls)
            print(f"✓ Marked {marked} DNB listings as exported in DB")
            return True

        except HttpError as e:
            print(f"Error appending DNB listings to sheet: {e}")
            return False

    df = db.get_stale_eiendom_for_sheets()

    # Apply the same export filters (price/BRA-i) used elsewhere before writing Sold.
    # This keeps sold/inactive scope aligned with sheet-visible listing rules.
    try:
        from main.config.filters import SHEETS_MAX_PRICE, MIN_BRA_I
    except ImportError:
        try:
            from config.filters import SHEETS_MAX_PRICE, MIN_BRA_I
        except ImportError:
            SHEETS_MAX_PRICE = None
            MIN_BRA_I = None

    prefilter_count = len(df)
    excluded_by_price = 0
    excluded_by_bra = 0

    if not df.empty:
        include_mask = pd.Series(True, index=df.index)

        if SHEETS_MAX_PRICE is not None and 'Pris' in df.columns:
            price_vals = pd.to_numeric(df['Pris'], errors='coerce')
            price_mask = price_vals <= float(SHEETS_MAX_PRICE)
            excluded_by_price = int((~price_mask.fillna(False)).sum())
            include_mask &= price_mask.fillna(False)

        if MIN_BRA_I is not None and 'Internt bruksareal (BRA-i)' in df.columns:
            bra_vals = pd.to_numeric(df['Internt bruksareal (BRA-i)'], errors='coerce')
            bra_mask = bra_vals >= float(MIN_BRA_I)
            excluded_by_bra = int((~bra_mask.fillna(False)).sum())
            include_mask &= bra_mask.fillna(False)

        excluded_total = int((~include_mask).sum())
        if excluded_total > 0:
            print(
                f"Applying Sold filters: excluding {excluded_total} listing(s) "
                f"(MAX_PRICE fails: {excluded_by_price}, MIN_BRA_I fails: {excluded_by_bra})"
            )
            print(
                f"Filters: SHEETS_MAX_PRICE={SHEETS_MAX_PRICE}, MIN_BRA_I={MIN_BRA_I}"
            )



        df = df.loc[include_mask].copy()

        kept_count = len(df)
        if excluded_total > 0:
            print(f"Sold rows kept after filters: {kept_count}/{prefilter_count}")

    if df.empty:
        print("No sold/inactive listings found. Clearing Sold sheet.")
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!A1:ZZ10000"
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body={"values": [["Finnkode"]]}
            ).execute()
            return True
        except HttpError as e:
            print(f"Error clearing Sold sheet: {e}")
            return False

    df['Finnkode'] = df['Finnkode'].apply(_normalize_finnkode)
    df = df[df['Finnkode'] != ''].drop_duplicates(subset=['Finnkode'], keep='first')

    df = filter_hidden_sheet_columns(df)
    df = dedupe_and_canonicalize_dataframe_columns(df)
    df = sanitize_for_sheets(df)

    all_rows = [df.columns.tolist()] + df.values.tolist()

    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1:ZZ10000"
        ).execute()

        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": all_rows}
        ).execute()

        updated_cells = result.get('updatedCells', 0)
        print(f"✓ Synced {len(df)} sold/inactive listings to '{sheet_name}' ({updated_cells} cells updated)")
        return True
    except HttpError as e:
        print(f"Error syncing Sold sheet: {e}")
        return False


def full_sync_eiendom_to_sheets(db_path: str = None, sheet_name: str = "Eie"):
    """
    Perform a full sync - replace all data in Google Sheets with database data.
    WARNING: This will overwrite the entire sheet!
    
    Args:
        db_path: Optional path to database file
        sheet_name: Name of the sheet to sync to (default: "Eie")
    """
    print(f"\n{'='*60}")
    print(f"FULL SYNC: Replacing all data in Google Sheets")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*60}\n")
    
    response = input("Proceed with Google Sheets API calls? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("✗ Full sync cancelled. No changes made.")
        return False

    response = input("This will OVERWRITE all data in the sheet. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Sync cancelled")
        return False
    
    # Initialize database
    db = PropertyDatabase(db_path)
    
    # Get credentials and service
    try:
        service = get_sheets_service()
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return False
    
    # Get all listings from database then apply temporary sheet visibility rules.
    df = db.get_eiendom_for_sheets()
    df = filter_rows_for_sheet_visibility(df, db)
    
    if df.empty:
        print("No visible listings to sync")
        return True
    
    print(f"Syncing {len(df)} listings to Google Sheets")
    
    # Keep internal-only columns in DB, but hide them from Sheets.
    df = filter_hidden_sheet_columns(df)
    # Normalize aliases and remove duplicate columns before writing full sheet.
    df = dedupe_and_canonicalize_dataframe_columns(df)
    # Sanitize data
    df = sanitize_for_sheets(df)
    
    # Convert to list format (header + data)
    all_rows = [df.columns.tolist()] + df.values.tolist()
    
    # Clear existing data and write new data
    try:
        # Clear the sheet
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1:ZZ10000"
        ).execute()
        
        # Write new data
        body = {"values": all_rows}
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        updated_cells = result.get('updatedCells', 0)
        print(f"\n✓ Successfully synced {len(df)} listings to Google Sheets ({updated_cells} cells updated)")
        
        # Mark all as exported
        all_finnkodes = df['Finnkode'].tolist()
        clean_finnkodes = [str(fk) for fk in all_finnkodes]
        
        marked = db.mark_as_exported('eiendom', clean_finnkodes)
        print(f"✓ Marked {marked} listings as exported in database")
        
        return True
        
    except HttpError as e:
        print(f"Error syncing to sheet: {e}")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync database to Google Sheets')
    parser.add_argument('--full', action='store_true', 
                       help='Perform full sync (overwrite all data)')
    parser.add_argument('--sheet', default='Eie',
                       help='Sheet name to sync to (default: Eie)')
    
    args = parser.parse_args()
    
    if args.full:
        full_sync_eiendom_to_sheets(sheet_name=args.sheet)
    else:
        sync_eiendom_to_sheets(sheet_name=args.sheet)
