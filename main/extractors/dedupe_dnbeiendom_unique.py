#!/usr/bin/env python3
"""Deduplicate DNB unique CSV by canonical URL/listing id and write deduped CSV.

Saves: data/dnbeiendom/unique_vs_finn_deduped.csv
"""
from pathlib import Path
import pandas as pd
import urllib.parse
import sys

IN = Path('data/dnbeiendom/unique_vs_finn.csv')
OUT = Path('data/dnbeiendom/unique_vs_finn_deduped.csv')

if not IN.exists():
    print(f"Input not found: {IN}")
    sys.exit(1)


def canonical(url):
    try:
        if not isinstance(url, str) or not url.strip():
            return ''
        p = urllib.parse.urlparse(url.strip())
        scheme = p.scheme or 'https'
        netloc = p.netloc.lower()
        path = p.path.rstrip('/')
        # normalize default ports
        if netloc.endswith(':80') and scheme == 'http':
            netloc = netloc.rsplit(':', 1)[0]
        if netloc.endswith(':443') and scheme == 'https':
            netloc = netloc.rsplit(':', 1)[0]
        return urllib.parse.urlunparse((scheme, netloc, path, '', '', ''))
    except Exception:
        return ''


df = pd.read_csv(IN)
orig = len(df)
# Prefer URL column, fall back to 'Url' or 'url', else DNB id/listing id
url_col = None
for c in ('URL', 'Url', 'url'):
    if c in df.columns:
        url_col = c
        break

if url_col is None:
    # use any likely id column
    for c in ('DNB_ID', 'ListingId', 'dnb_id'):
        if c in df.columns:
            url_col = c
            break

if url_col is None:
    # Nothing to dedupe on: write input back and exit
    print('No URL or ID column found to dedupe on; copying input to output')
    df.to_csv(OUT, index=False)
    print(f'Wrote {len(df)} rows to {OUT}')
    sys.exit(0)

# create canonical key
df['__canon'] = df[url_col].fillna('').apply(canonical)
# For empty canon values, use raw value to avoid dropping
mask_empty = df['__canon'] == ''
if mask_empty.any():
    df.loc[mask_empty, '__canon'] = df.loc[mask_empty, url_col].fillna('').astype(str)

# drop duplicates keeping first
df2 = df.drop_duplicates('__canon').copy()
# remove helper column
df2.drop(columns=['__canon'], inplace=True)

out = len(df2)
# ensure output dir exists
OUT.parent.mkdir(parents=True, exist_ok=True)
df2.to_csv(OUT, index=False)
print(f'Read {orig} rows from {IN}')
print(f'Wrote {out} deduplicated rows to {OUT}')
