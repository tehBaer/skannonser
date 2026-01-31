"""Quick script to view database contents."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import PropertyDatabase

db = PropertyDatabase()

# View stats
print("=== DATABASE STATISTICS ===")
stats = db.get_stats('eiendom')
for key, value in stats.items():
    print(f"{key}: {value}")

# View active eiendom data
print("\n\n=== EIENDOM (Active Listings) ===")
df = db.get_active_listings('eiendom', as_dataframe=True)
if not df.empty:
    print(f"\nTotal: {len(df)} active listings")
    print(df.to_string())
else:
    print("No active listings")

# View data ready for sheets export
print("\n\n=== READY FOR GOOGLE SHEETS EXPORT ===")
df_sheets = db.get_eiendom_for_sheets()
if not df_sheets.empty:
    print(f"\nTotal: {len(df_sheets)} listings ready for export")
    print(df_sheets.to_string())
else:
    print("No listings ready for export")
