#!/usr/bin/env python3
"""Backfill IMAGE_URL in eiendom DB from cached ad HTML files."""
import argparse
import os
import sqlite3
import sys
from typing import Optional

from bs4 import BeautifulSoup

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
    from main.extractors.parsing_helpers_common import getImageUrl
except ImportError:
    from database.db import PropertyDatabase
    from extractors.parsing_helpers_common import getImageUrl


def _extract_image_from_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    return getImageUrl(soup)


def _normalize(value: Optional[str]) -> str:
    return str(value or "").strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill eiendom.image_url from cached HTML files")
    parser.add_argument("--db", help="Optional path to properties.db")
    parser.add_argument(
        "--html-dir",
        default="data/eiendom/html_extracted",
        help="Directory with cached ad HTML files (default: data/eiendom/html_extracted)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = no limit)")
    parser.add_argument("--overwrite", action="store_true", help="Also overwrite existing IMAGE_URL values")
    args = parser.parse_args()

    db = PropertyDatabase(args.db)
    conn = sqlite3.connect(db.db_path)
    cur = conn.cursor()

    where = "1=1" if args.overwrite else "(image_url IS NULL OR TRIM(image_url) = '')"
    query = f"""
        SELECT finnkode, url, image_url
        FROM eiendom
        WHERE {where}
        ORDER BY scraped_at DESC
    """
    if args.limit and args.limit > 0:
        query += " LIMIT ?"
        rows = cur.execute(query, (args.limit,)).fetchall()
    else:
        rows = cur.execute(query).fetchall()

    total = len(rows)
    updated = 0
    missing_html = 0
    no_image = 0

    print(f"Candidates: {total}")
    for idx, (finnkode, _url, existing) in enumerate(rows, start=1):
        fk = _normalize(finnkode)
        if not fk:
            continue

        html_path = os.path.join(args.html_dir, f"{fk}.html")
        if not os.path.exists(html_path):
            missing_html += 1
            continue

        try:
            image_url = _normalize(_extract_image_from_file(html_path))
        except Exception:
            no_image += 1
            continue

        if not image_url:
            no_image += 1
            continue

        if (not args.overwrite) and _normalize(existing):
            continue

        cur.execute(
            """
            UPDATE eiendom
            SET image_url = ?, updated_at = CURRENT_TIMESTAMP
            WHERE finnkode = ?
            """,
            (image_url, fk),
        )
        if cur.rowcount > 0:
            updated += 1

        if idx % 200 == 0:
            conn.commit()
            print(f"Processed {idx}/{total}, updated {updated}")

    conn.commit()
    conn.close()

    print("\nBackfill complete")
    print(f"Updated IMAGE_URL: {updated}")
    print(f"Missing HTML file: {missing_html}")
    print(f"No image found in HTML: {no_image}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
