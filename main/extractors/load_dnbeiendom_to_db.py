#!/usr/bin/env python3
"""Annotate filtered DNB listings with matching FINN finnkode and load into DB.

- Reads: data/dnbeiendom/A_live_filtered.csv
- Reads: data/eiendom/A_live.csv
- Writes: inserts/updates into `dnbeiendom` table using PropertyDatabase
"""
from pathlib import Path
import pandas as pd
import re
import sys

# Ensure project root is on sys.path so `main` package imports work when run as a script
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from main.database.db import PropertyDatabase


def normalize_addr(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"[.,©()\"'\\/]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_pc(pc) -> str:
    if pd.isna(pc):
        return ""
    s = str(pc).strip()
    s = re.sub(r"\.0$", "", s)
    return s


def main():
    dnb_path = Path('data/dnbeiendom/A_live_filtered.csv')
    finn_path = Path('data/eiendom/A_live.csv')

    if not dnb_path.exists():
        print(f"DNB file not found: {dnb_path}")
        raise SystemExit(1)
    if not finn_path.exists():
        print(f"FINN file not found: {finn_path}")
        raise SystemExit(1)

    dnb = pd.read_csv(dnb_path)
    finn = pd.read_csv(finn_path)

    dnb['__addr_norm'] = dnb.get('StreetAddress', '').fillna('').apply(normalize_addr)
    dnb['__pc_norm'] = dnb.get('PostalCode', '').apply(normalize_pc)

    finn['__addr_norm'] = finn.get('Adresse', '').fillna('').apply(normalize_addr)
    finn['__pc_norm'] = finn.get('Postnummer', '').apply(normalize_pc)

    # Build lookup of (addr_norm, pc) -> finnkode
    finn_lookup = {}
    for idx, row in finn.iterrows():
        key = (row['__addr_norm'], row['__pc_norm'])
        if key not in finn_lookup:
            finn_lookup[key] = row.get('Finnkode')

    matched = []
    for idx, row in dnb.iterrows():
        key = (row['__addr_norm'], row['__pc_norm'])
        finnkode = finn_lookup.get(key)
        matched.append(finnkode if finnkode is not None else '')

    dnb['MatchedFinn_Finnkode'] = matched

    # Load into DB
    db = PropertyDatabase()
    inserted, updated = db.insert_or_update_dnbeiendom(dnb)

    print(f"Inserted {inserted}, Updated {updated} into dnbeiendom")

if __name__ == '__main__':
    main()
