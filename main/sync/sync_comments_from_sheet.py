"""Sync comment columns from Google Sheet back into the database.

Reads the `Finnkode`, `Kommentar`, and `Tag` columns from the specified sheet
and inserts a new row into `listing_comments` for any changed value (keeps history).
"""
import sys
import os
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import get_sheets_service, SPREADSHEET_ID
    from main.sync.helper_sync_to_sheets import canonicalize_header_name
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_sheets_service, SPREADSHEET_ID
    from sync.helper_sync_to_sheets import canonicalize_header_name

from googleapiclient.errors import HttpError


def _normalize_finnkode(value) -> str:
    if value is None:
        return ""
    v = str(value).strip()
    if not v:
        return ""
    if 'HYPERLINK' in v.upper():
        parts = v.split('"')
        if len(parts) >= 4:
            v = parts[3].strip()
    try:
        f = float(v)
        if f.is_integer():
            return str(int(f))
    except (ValueError, TypeError):
        pass
    return v


def sync_comments_from_sheet(sheet_name: str = "Eie", db_path: Optional[str] = None, user_id: Optional[str] = None, force: bool = False):
    print(f"\nSyncing comment columns from sheet '{sheet_name}' to DB...\n")

    db = PropertyDatabase(db_path)
    try:
        service = get_sheets_service()
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return False

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1:ZZ10000"
        ).execute()
    except HttpError as e:
        print(f"Error reading sheet: {e}")
        return False

    values = result.get('values', [])
    if not values:
        print("No data found in sheet")
        return True

    header = [canonicalize_header_name(c) for c in values[0]]
    # Find columns
    try:
        finn_idx = header.index('Finnkode')
    except ValueError:
        finn_idx = 0

    kommentar_idx = None
    tag_idx = None
    if 'Kommentar' in header:
        kommentar_idx = header.index('Kommentar')
    if 'Tag' in header:
        tag_idx = header.index('Tag')

    # Optional edited metadata columns created by Apps Script
    kommentar_at_idx = header.index('Kommentar__edited_at') if 'Kommentar__edited_at' in header else None
    kommentar_by_idx = header.index('Kommentar__edited_by') if 'Kommentar__edited_by' in header else None
    tag_at_idx = header.index('Tag__edited_at') if 'Tag__edited_at' in header else None
    tag_by_idx = header.index('Tag__edited_by') if 'Tag__edited_by' in header else None

    if kommentar_idx is None and tag_idx is None:
        print("No Kommentar/Tag columns found in sheet header; nothing to sync.")
        return True

    # Read rows and collect finnkodes
    rows = values[1:]
    finnkodes = []
    row_map = []  # tuples of (finnkode, kommentar_text, tag_text)
    for row in rows:
        finn_raw = row[finn_idx] if len(row) > finn_idx else ''
        finnkode = _normalize_finnkode(finn_raw)
        if not finnkode:
            continue
        kommentar = row[kommentar_idx] if (kommentar_idx is not None and len(row) > kommentar_idx) else ''
        tag = row[tag_idx] if (tag_idx is not None and len(row) > tag_idx) else ''
        finnkodes.append(finnkode)
        row_map.append((finnkode, str(kommentar).strip(), str(tag).strip()))

    if not finnkodes:
        print("No valid Finnkodes found in sheet rows")
        return True

    # Get current latest comments from DB (with metadata)
    db_comments = db.get_latest_comments_for_finnkodes(finnkodes)

    import pandas as pd
    inserted = 0
    conflicts = 0
    for i, (finnkode, kommentar_text, tag_text) in enumerate(row_map):
        existing = db_comments.get(finnkode, {})

        # Helper to parse sheet timestamp values robustly
        def _parse_sheet_ts(val):
            if val is None or val == '':
                return None
            try:
                return pd.to_datetime(val)
            except Exception:
                try:
                    # If Google returns serial number (Excel date), pandas can handle floats
                    return pd.to_datetime(float(val), unit='D', origin='1899-12-30')
                except Exception:
                    return None

        # Helper to parse db timestamp
        def _parse_db_ts(val):
            if not val:
                return None
            try:
                return pd.to_datetime(val)
            except Exception:
                return None

        # Kommentar: decide whether to accept sheet value
        if kommentar_idx is not None:
            sheet_at = None
            if kommentar_at_idx is not None and len(values[1 + i]) > kommentar_at_idx:
                sheet_at = _parse_sheet_ts(values[1 + i][kommentar_at_idx])

            prev_meta = existing.get('Kommentar') or {'text': '', 'updated_at': None}
            prev_text = (prev_meta.get('text') or '')
            prev_db_ts = _parse_db_ts(prev_meta.get('updated_at'))

            accept = False
            if (kommentar_text or '') == (prev_text or ''):
                accept = False
            else:
                if sheet_at is not None:
                    # Prefer sheet when sheet edit timestamp exists and is newer
                    if prev_db_ts is None or sheet_at > prev_db_ts:
                        accept = True
                else:
                    # Fallback: if DB empty or force, accept; otherwise skip to avoid overwrite
                    if not prev_text or force:
                        accept = True

            if accept:
                db.insert_listing_comment(finnkode, 'Kommentar', user_id, kommentar_text)
                inserted += 1
            else:
                if (kommentar_text or '') != (prev_text or ''):
                    conflicts += 1
                    if not sheet_at:
                        print(f"Conflict (Kommentar) for {finnkode}: sheet changed but DB is newer — skipping (use --force to override)")

        # Tag: decide whether to accept sheet value
        if tag_idx is not None:
            sheet_at = None
            if tag_at_idx is not None and len(values[1 + i]) > tag_at_idx:
                sheet_at = _parse_sheet_ts(values[1 + i][tag_at_idx])

            prev_meta = existing.get('Tag') or {'text': '', 'updated_at': None}
            prev_text = (prev_meta.get('text') or '')
            prev_db_ts = _parse_db_ts(prev_meta.get('updated_at'))

            accept = False
            if (tag_text or '') == (prev_text or ''):
                accept = False
            else:
                if sheet_at is not None:
                    if prev_db_ts is None or sheet_at > prev_db_ts:
                        accept = True
                else:
                    if not prev_text or force:
                        accept = True

            if accept:
                db.insert_listing_comment(finnkode, 'Tag', user_id, tag_text)
                inserted += 1
            else:
                if (tag_text or '') != (prev_text or ''):
                    conflicts += 1
                    if not sheet_at:
                        print(f"Conflict (Tag) for {finnkode}: sheet changed but DB is newer — skipping (use --force to override)")

    print(f"✓ Inserted {inserted} comment rows into listing_comments")
    if conflicts:
        print(f"⚠ {conflicts} conflicts skipped. Use --force to overwrite DB values from sheet.")
    return True


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync Kommentar and Tag columns from sheet into DB')
    parser.add_argument('--sheet', default='Eie', help='Sheet tab name (default: Eie)')
    parser.add_argument('--db', default=None, help='Optional database file path')
    parser.add_argument('--user', default=None, help='Optional user id to record')
    parser.add_argument('--force', action='store_true', help='Force overwrite DB values from sheet')
    args = parser.parse_args()
    sync_comments_from_sheet(sheet_name=args.sheet, db_path=args.db, user_id=args.user, force=args.force)
