"""Local classification driver over cached ad HTML (2026-08-05 design spec).

Mirrors `backfill_salgsoppgave`'s walk, plus the cache/spend logic:
- `salgsoppgave_llm_cache` hit -> free replay, always processed.
- Miss -> one API call, bounded by `limit` (the spend control; the Batch API
  has none of its own) unless `cache_only`.
Responses are validated BEFORE caching so a malformed response never poisons
the cache. Purely local: reads the on-disk HTML cache, never FINN.
"""
import sqlite3
import time
from pathlib import Path

from skannonser.enrich.tilstand import (
    TilstandResponse, _MODEL, _SYSTEM_PROMPT, TILSTAND_SCHEMA, _anthropic_call,
    cache_get, cache_put, classify_input, compute_rollup, content_sha,
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


def _default_client():
    import anthropic  # lazy: only where classification actually runs

    return anthropic.Anthropic()


def _pending_inputs(conn, project_dir, input_fn, limit) -> dict[str, str]:
    """sha -> input text for every ad whose input is not yet cached.
    Dedup by sha is automatic (dict key); `limit` bounds the request count."""
    pending: dict[str, str] = {}
    for (finnkode,) in conn.execute("SELECT finnkode FROM eiendom"):
        if limit is not None and len(pending) >= limit:
            break
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            continue
        try:
            text = input_fn(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue
        if text is None:
            continue
        sha = content_sha(text)
        if sha not in pending and cache_get(conn, sha) is None:
            pending[sha] = text
    return pending


def classify_tilstand_batch(
    conn: sqlite3.Connection,
    project_dir: Path,
    *,
    limit: int | None = None,
    _client=None,
    _sleep=None,
    _input_fn=None,
) -> dict:
    """Backfill via the Batch API (50% cheaper). Fills the cache, then the
    sync driver derives rows from it -- so an interrupted run loses nothing
    already paid for."""
    input_fn = _input_fn or classify_input
    sleep = _sleep or time.sleep
    pending = _pending_inputs(conn, project_dir, input_fn, limit)
    counts = {"submitted": len(pending), "succeeded": 0, "failed": 0}
    if pending:
        client = _client or _default_client()
        requests = [
            {
                "custom_id": sha,  # sha256 hex = 64 chars = the API's cap, exactly
                "params": {
                    "model": _MODEL,
                    # Mirrors the sync seam: on claude-opus-5, adaptive thinking
                    # shares the max_tokens budget, so this must stay at 32000.
                    "max_tokens": 32000,
                    "system": _SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": text}],
                    "output_config": {
                        "format": {"type": "json_schema", "schema": TILSTAND_SCHEMA}
                    },
                },
            }
            for sha, text in pending.items()
        ]
        batch = client.messages.batches.create(requests=requests)
        while True:
            batch = client.messages.batches.retrieve(batch.id)
            if batch.processing_status == "ended":
                break
            sleep(60)
        for result in client.messages.batches.results(batch.id):
            # An errored/canceled/expired result has NO `.message` attribute at
            # all -- fetch it via getattr FIRST so a batch of mixed outcomes
            # never raises mid-loop (that would abort after the batch was
            # already paid for, losing every uncollected success -- and
            # double-billing them on re-run).
            msg = getattr(result.result, "message", None)
            stop_reason = getattr(msg, "stop_reason", None) if msg is not None else None
            ok = (
                result.result.type == "succeeded"
                and msg is not None
                and stop_reason not in ("refusal", "max_tokens")
            )
            raw = None
            if ok:
                raw = next(
                    (b.text for b in msg.content if b.type == "text"),
                    None,
                )
            if raw is not None:
                try:
                    TilstandResponse.model_validate_json(raw)
                except Exception:
                    raw = None
            if raw is None:
                counts["failed"] += 1
                continue
            cache_put(conn, result.custom_id, raw)
            counts["succeeded"] += 1
    derive = classify_tilstand(conn, project_dir, cache_only=True, _input_fn=_input_fn)
    counts.update({f"derive_{k}": v for k, v in derive.items()})
    return counts
