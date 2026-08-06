"""Local classification driver over cached ad HTML (2026-08-05 design spec).

Mirrors `backfill_salgsoppgave`'s walk, plus the cache/spend logic:
- `salgsoppgave_llm_cache` hit -> free replay, always processed.
- Miss -> one API call, bounded by `limit` (the spend control; the Batch API
  has none of its own) unless `cache_only`.
Responses are validated BEFORE caching so a malformed response never poisons
the cache. Purely local: reads the on-disk HTML cache, never FINN.
"""
import sqlite3
from pathlib import Path

from skannonser.enrich.tilstand import (
    TilstandResponse, _anthropic_call, cache_get, cache_put, classify_input,
    compute_rollup, content_sha,
)
from skannonser.store.repositories.tilstand import TilstandRepo


def classify_tilstand(
    conn: sqlite3.Connection,
    project_dir: Path,
    *,
    limit: int | None = None,
    wipe: bool = False,
    cache_only: bool = False,
    _call=None,
    _input_fn=None,
) -> dict:
    call = _call or _anthropic_call
    input_fn = _input_fn or classify_input
    repo = TilstandRepo(conn)
    if wipe:
        repo.wipe()

    finnkodes = [str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")]
    counts = {
        "eiendom_rows": len(finnkodes), "missing_html": 0, "empty_input": 0,
        "cached": 0, "called": 0, "limit_skipped": 0, "uncached_skipped": 0,
        "errors": 0, "upserted": 0,
    }
    for finnkode in finnkodes:
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            counts["missing_html"] += 1
            continue
        try:
            text = input_fn(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            counts["errors"] += 1
            continue
        if text is None:
            counts["empty_input"] += 1
            continue
        sha = content_sha(text)
        raw = cache_get(conn, sha)
        if raw is None:
            if cache_only:
                counts["uncached_skipped"] += 1
                continue
            if limit is not None and counts["called"] >= limit:
                counts["limit_skipped"] += 1
                continue
            try:
                raw = call(text)
                counts["called"] += 1
                resp = TilstandResponse.model_validate_json(raw)
            except Exception:
                counts["errors"] += 1
                continue
            cache_put(conn, sha, raw)
        else:
            counts["cached"] += 1
            try:
                resp = TilstandResponse.model_validate_json(raw)
            except Exception:
                counts["errors"] += 1
                continue
        repo.upsert_ad(
            finnkode,
            [f.model_dump() for f in resp.findings],
            resp.egenerklaering,
            compute_rollup(resp),
        )
        counts["upserted"] += 1
    return counts
