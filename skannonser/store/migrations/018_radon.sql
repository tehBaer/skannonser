-- 018_radon.sql
-- Radon facts in the tilstand classifier (2026-08-06 design spec), plus the
-- cache versioning that makes changing the output schema safe.
--
-- Numbered 018, not the spec's 017: 017_classification_provenance landed first.
--
-- `radon_omtalt` on listing_salgsoppgave stays: it is regex over prose, free,
-- and covers every parsed ad including ones classification has not reached.
-- These columns answer a different, much stronger question.

ALTER TABLE listing_tilstand ADD COLUMN radon_status TEXT;
-- 'ikke_malt' | 'malt_under_grense' | 'malt_over_grense' | 'malt_ukjent_verdi'
ALTER TABLE listing_tilstand ADD COLUMN radonsperre TEXT;   -- 'finnes' | 'mangler'
ALTER TABLE listing_tilstand ADD COLUMN radon_bq INTEGER;   -- the property's OWN measurement

-- The cache is keyed by sha256 of the INPUT text; the prompt and output schema
-- are not in the key. Without this column, adding a field means every cached
-- response silently yields NULL for it forever -- indistinguishable from "the
-- document said nothing". Version mismatch is treated as a cache miss.
-- Existing rows predate radon, so they default to version 1.
ALTER TABLE salgsoppgave_llm_cache ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
