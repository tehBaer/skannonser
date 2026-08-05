-- 016_tilstand.sql
-- Tilstand classifier storage (2026-08-05 design spec). Classifier-owned,
-- separate from Phase-1's listing_salgsoppgave so a routine Phase-1 re-parse
-- or `backfill-salgsoppgave --wipe` can never touch classifier output.
-- Derived + disposable: rebuildable via `tools classify-tilstand --wipe`,
-- with salgsoppgave_llm_cache replaying paid responses for free.

-- Rebuild listing_tg_findings: the old UNIQUE (finnkode, tg, bygningsdel)
-- collapsed two TG3 bathrooms into one row -- harmless for counting parts,
-- silently halves the repair bill once findings carry costs. Table is empty
-- everywhere (0 rows, by design), so DROP is safe.
DROP TABLE IF EXISTS listing_tg_findings;
CREATE TABLE listing_tg_findings (
    id            INTEGER PRIMARY KEY,
    finnkode      TEXT NOT NULL REFERENCES eiendom(finnkode),
    tg            INTEGER NOT NULL,        -- 2 | 3
    bygningsdel   TEXT NOT NULL,           -- 18-value enum, or 'annet'
    tiltak        TEXT,
    alvorlighet   TEXT NOT NULL,           -- kosmetisk|mindre|vesentlig|alvorlig
    kostnad_lav   INTEGER,                 -- kr, grid value (see design spec)
    kostnad_hoy   INTEGER,                 -- kr, grid value; 1000000 = "1M+"
    kostnad_kilde TEXT                     -- 'takst' | 'estimat'
);
CREATE INDEX IF NOT EXISTS idx_tg_findings_finnkode
    ON listing_tg_findings (finnkode);

-- Per-listing rollups, denormalised so the web app can sort/filter.
CREATE TABLE IF NOT EXISTS listing_tilstand (
    finnkode              TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    tg2_count             INTEGER NOT NULL,
    tg3_count             INTEGER NOT NULL,
    reparasjon_lav        INTEGER,   -- SUM(kostnad_lav)   -> filter floor
    reparasjon_hoy        INTEGER,   -- SUM(kostnad_hoy)   -> filter ceiling
    reparasjon_est        INTEGER,   -- SUM(midpoints)     -> sort key
    alvorlighet           TEXT,      -- worst tier across findings
    verste_bygningsdel    TEXT,      -- bygningsdel of the worst finding
    reparasjon_kilde      TEXT,      -- 'takst' | 'blandet' | 'estimat'
    tilstandsrapport_dato TEXT,
    tilstandsrapport_utsteder TEXT,
    egenerklaering_antall INTEGER,
    classified_at         TEXT NOT NULL
);

-- Phase-2 columns on listing_salgsoppgave: NULL on every live row, now dead.
ALTER TABLE listing_salgsoppgave DROP COLUMN tg2_count;
ALTER TABLE listing_salgsoppgave DROP COLUMN tg3_count;
ALTER TABLE listing_salgsoppgave DROP COLUMN tilstandsrapport_dato;
ALTER TABLE listing_salgsoppgave DROP COLUMN tilstandsrapport_utsteder;
ALTER TABLE listing_salgsoppgave DROP COLUMN egenerklaering_antall;
