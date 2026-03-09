#!/usr/bin/env python3
"""Compare DNB Eiendom (filtered) against FINN A_live and list DNB-only ads.

Matching strategy:
- Normalize `StreetAddress`/`Adresse` by removing punctuation and lowercasing.
- Require matching `PostalCode`/`Postnummer` to be equal.

Outputs:
- data/dnbeiendom/matched_with_finn.csv  (DNB rows with matched Finn info)
- data/dnbeiendom/unique_vs_finn.csv    (DNB rows with no match in Finn)
"""
from __future__ import annotations

from pathlib import Path
import re
import sys
import pandas as pd


def normalize_addr(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    # remove common punctuation
    s = re.sub(r"[.,©()\"'\\/]+", "", s)
    # collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_pc(pc) -> str:
    if pd.isna(pc):
        return ""
    s = str(pc).strip()
    # strip trailing .0 from numeric csv reads
    s = re.sub(r"\.0$", "", s)
    return s


def main():
    dnb_path = Path("data/dnbeiendom/A_live_filtered.csv")
    finn_path = Path("data/eiendom/A_live.csv")
    if not dnb_path.exists():
        print(f"DNB file not found: {dnb_path}")
        raise SystemExit(1)
    if not finn_path.exists():
        print(f"FINN file not found: {finn_path}")
        raise SystemExit(1)

    dnb = pd.read_csv(dnb_path)
    finn = pd.read_csv(finn_path)

    # Normalize columns
    dnb['__addr_norm'] = dnb.get('StreetAddress', '').fillna('').apply(normalize_addr)
    dnb['__pc_norm'] = dnb.get('PostalCode', '').apply(normalize_pc)

    finn['__addr_norm'] = finn.get('Adresse', '').fillna('').apply(normalize_addr)
    finn['__pc_norm'] = finn.get('Postnummer', '').apply(normalize_pc)

    # Build lookup of (addr_norm, pc) -> rows in finn
    finn_lookup = {}
    for idx, row in finn.iterrows():
        key = (row['__addr_norm'], row['__pc_norm'])
        finn_lookup.setdefault(key, []).append(row)

    matched_rows = []
    unique_rows = []

    for idx, row in dnb.iterrows():
        key = (row['__addr_norm'], row['__pc_norm'])
        matches = finn_lookup.get(key)
        if matches:
            # attach first matching finn row info
            first = matches[0]
            out = row.to_dict()
            out['MatchedFinn_Finnkode'] = first.get('Finnkode', '')
            out['MatchedFinn_URL'] = first.get('URL', '')
            matched_rows.append(out)
        else:
            unique_rows.append(row.to_dict())

    matched_df = pd.DataFrame(matched_rows)
    unique_df = pd.DataFrame(unique_rows)

    out_matched = Path('data/dnbeiendom/matched_with_finn.csv')
    out_unique = Path('data/dnbeiendom/unique_vs_finn.csv')
    matched_df.to_csv(out_matched, index=False)
    unique_df.to_csv(out_unique, index=False)

    print(f"DNB filtered rows: {len(dnb)}")
    print(f"Matched with FINN: {len(matched_df)}")
    print(f"Unique to DNB (not in FINN): {len(unique_df)}")
    print(f"Wrote: {out_matched} and {out_unique}")


if __name__ == '__main__':
    main()
