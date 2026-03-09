#!/usr/bin/env python3
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import pandas as pd
from main.database.db import PropertyDatabase

p = Path('data/dnbeiendom/A_live_filtered_no_buffer.csv')
if not p.exists():
    print('Missing', p)
    raise SystemExit(1)

df = pd.read_csv(p)
print('Rows to load:', len(df))

db = PropertyDatabase()
ins, upd = db.insert_or_update_dnbeiendom(df)
print('Inserted', ins, 'Updated', upd)
