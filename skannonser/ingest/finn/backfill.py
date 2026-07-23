"""Local re-parse of cached ad HTML into listing_details/listing_facilities.

The recovery/bootstrap path for the details cache (2026-07-23 design spec):
iterate every `eiendom` finnkode, read
`{project_dir}/html_extracted/{finnkode}.html` where present, `parse_details`
it, upsert. Purely offline -- reads only the on-disk cache, never FINN.
"""
import sqlite3
from pathlib import Path

from skannonser.ingest.finn.parse_details import parse_details
from skannonser.store.repositories.details import DetailsRepo

_BATCH_SIZE = 200


def backfill_details(
    conn: sqlite3.Connection, project_dir: Path, wipe: bool = False
) -> dict:
    repo = DetailsRepo(conn)
    if wipe:
        repo.wipe()

    finnkodes = [
        str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")
    ]
    parsed = missing = upserted = 0
    batch = []
    for finnkode in finnkodes:
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            missing += 1
            continue
        html = path.read_text(encoding="utf-8", errors="replace")
        batch.append(parse_details(html, finnkode))
        parsed += 1
        if len(batch) >= _BATCH_SIZE:
            upserted += repo.upsert_details(batch)["upserted"]
            batch = []
    if batch:
        upserted += repo.upsert_details(batch)["upserted"]

    return {
        "eiendom_rows": len(finnkodes),
        "parsed": parsed,
        "missing_html": missing,
        "upserted": upserted,
    }
