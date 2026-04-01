#!/usr/bin/env python3
import sqlite3
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main.database.db import PropertyDatabase

db = PropertyDatabase()
conn = sqlite3.connect(db.db_path)
ids = [403064914, 399737917, 457898250, 457884031, 457883057, 457830220, 457682464,
       457557308, 457553930, 457543749, 457542883, 457534376, 457457330, 457455504,
       457455242, 457447683, 457441546, 457431023, 457321265, 457289605, 457164799,
       457144114, 456098215, 455225294, 454596194, 353742977]
placeholders = ','.join('?' * len(ids))
rows = conn.execute(
    f'''SELECT e.finnkode, ep.geocode_failed, ep.lat, ep.lng,
               COALESCE(ep.adresse_cleaned, e.adresse), e.active
        FROM eiendom e
        LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
        WHERE e.finnkode IN ({placeholders})''',
    [str(i) for i in ids]
).fetchall()
conn.close()

geocode_failed = [r for r in rows if r[1]]
no_ep_row = [r for r in rows if r[1] is None and r[2] is None]
print(f'geocode_failed=1 : {len(geocode_failed)}')
for r in geocode_failed:
    print(f'  {r[0]}: {r[4]}')
print()
print(f'No eiendom_processed row (fresh/never attempted): {len(no_ep_row)}')
for r in no_ep_row:
    print(f'  {r[0]}: active={r[5]}  addr={r[4]}')
