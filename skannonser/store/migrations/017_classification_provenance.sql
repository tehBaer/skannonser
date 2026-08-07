-- 017_classification_provenance.sql
-- Records WHICH model produced each classification, and at what reasoning
-- effort, so a later model comparison can tell old output from new.
--
-- `salgsoppgave_llm_cache` already carried `model`. Two things were missing:
-- the effort setting, and any link from a listing back to the response that
-- produced its rows. Both are added here; the model itself stays in the cache
-- row (one place, so a relabel propagates to every listing that shares it).

-- NULL means "not recorded", not "none". No effort has ever been captured:
-- `_anthropic_call` passes only model and max_tokens, so every existing row is
-- honestly unknown, and will stay so until the classifier specifies one.
ALTER TABLE salgsoppgave_llm_cache ADD COLUMN effort TEXT;

-- The join key. `classify_tilstand` already computes this sha to look the
-- response up; storing it makes per-listing provenance a plain join instead of
-- re-deriving classify_input() for all 5863 ads.
ALTER TABLE listing_tilstand ADD COLUMN content_sha256 TEXT;

CREATE INDEX IF NOT EXISTS idx_listing_tilstand_sha
    ON listing_tilstand(content_sha256);

-- One-time relabel. The 150 responses loaded on 2026-08-06 were produced in an
-- interactive session (the assistant classifying the text directly), not
-- through the API seam -- but the loader used cache_put's default `model`, so
-- they claim a provenance they do not have. Batch 1's 20 rows were already
-- labelled honestly by hand.
--
-- The date guard is what makes this safe to ship as a migration: it can only
-- ever match rows that predate 2026-08-07, so a genuine API run afterwards
-- keeps its own label. Lives here rather than in a one-off script so local and
-- the server both get it exactly once, without anyone remembering to do it
-- twice.
UPDATE salgsoppgave_llm_cache
   SET model = 'claude-opus-5 (interactive session)'
 WHERE model = 'claude-opus-5'
   AND created_at < '2026-08-07';
