"""Local re-parse of cached ad HTML into listing_salgsoppgave.

The recovery/bootstrap path for the salgsoppgave cache (2026-07-27 design
spec), mirroring `backfill_details`: iterate every `eiendom` finnkode, read
`{project_dir}/html_extracted/{finnkode}.html` where present,
`parse_salgsoppgave` it, upsert. Purely offline -- reads only the on-disk
cache, never FINN, and makes no API calls.
"""
import sqlite3
from pathlib import Path

from skannonser.ingest.finn.parse_salgsoppgave import parse_salgsoppgave
from skannonser.store.repositories.salgsoppgave import SalgsoppgaveRepo

_BATCH_SIZE = 200


def backfill_salgsoppgave(
    conn: sqlite3.Connection, project_dir: Path, wipe: bool = False
) -> dict:
    repo = SalgsoppgaveRepo(conn)
    if wipe:
        repo.wipe()

    finnkodes = [str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")]
    parsed = missing = upserted = 0
    batch = []
    for finnkode in finnkodes:
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            missing += 1
            continue
        html = path.read_text(encoding="utf-8", errors="replace")
        batch.append(parse_salgsoppgave(html, finnkode))
        parsed += 1
        if len(batch) >= _BATCH_SIZE:
            upserted += repo.upsert(batch)["upserted"]
            batch = []
    if batch:
        upserted += repo.upsert(batch)["upserted"]

    return {
        "eiendom_rows": len(finnkodes),
        "parsed": parsed,
        "missing_html": missing,
        "upserted": upserted,
    }
