import sqlite3, os

db_path = os.path.join(os.path.dirname(__file__), '..', 'main', 'database', 'properties.db')
conn = sqlite3.connect(db_path)

row = conn.execute("""
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN ep.pendl_rush_mvv_uni_rush IS NULL THEN 1 ELSE 0 END) as missing_uni,
  SUM(CASE WHEN ep.pendl_rush_mvv IS NULL THEN 1 ELSE 0 END) as missing_mvv
FROM eiendom e
LEFT JOIN eiendom_processed ep ON ep.finnkode = e.finnkode
WHERE e.active = 1 AND e.search_hit = 1
""").fetchone()

print(f"Active+search_hit listings : {row[0]}")
print(f"  Missing pendl_rush_mvv_uni_rush : {row[1]}")
print(f"  Missing pendl_rush_mvv          : {row[2]}")

missing = conn.execute("""
SELECT e.finnkode, e.adresse, ep.pendl_rush_mvv, ep.pendl_rush_mvv_uni_rush
FROM eiendom e
LEFT JOIN eiendom_processed ep ON ep.finnkode = e.finnkode
WHERE e.active = 1 AND e.search_hit = 1
  AND ep.pendl_rush_mvv_uni_rush IS NULL
ORDER BY e.adresse
""").fetchall()

if missing:
    print(f"\nListings missing uni_rush ({len(missing)}):")
    for r in missing:
        mvv_str = f"mvv={r[2]}" if r[2] is not None else "mvv=None"
        print(f"  {r[0]}  {mvv_str}  addr={r[1]}")
else:
    print("\nAll active+search_hit listings have pendl_rush_mvv_uni_rush.")
