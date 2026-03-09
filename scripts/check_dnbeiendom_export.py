#!/usr/bin/env python3
from main.database.db import PropertyDatabase
from main.sync import helper_sync_to_sheets as helper


def main():
    db = PropertyDatabase()
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dnbeiendom")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dnbeiendom WHERE duplicate_of_finnkode IS NOT NULL AND duplicate_of_finnkode != ''")
    matched = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dnbeiendom WHERE exported_to_sheets = 1")
    exported = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dnbeiendom WHERE exported_to_sheets = 0")
    not_exported = cur.fetchone()[0]
    print(f"dnbeiendom total: {total}, matched: {matched}, exported: {exported}, not_exported: {not_exported}")
    cur.execute("SELECT url, adresse, postnummer, duplicate_of_finnkode FROM dnbeiendom WHERE (duplicate_of_finnkode IS NULL OR duplicate_of_finnkode = '') AND exported_to_sheets = 0 LIMIT 10")
    rows = cur.fetchall()
    print('Sample not-exported DNB-only rows:')
    for r in rows:
        print(r)
    try:
        service = helper.get_sheets_service()
        sheet = helper.SPREADSHEET_ID
        rng = 'DNB!A1:F20'
        res = service.spreadsheets().values().get(spreadsheetId=sheet, range=rng).execute()
        values = res.get('values', [])
        print(f"Read {len(values)} rows from sheet 'DNB' (showing up to 20).")
        for v in values[:5]:
            print(v)
    except Exception as e:
        print('Could not read sheet:', e)
    conn.close()


if __name__ == "__main__":
    main()
