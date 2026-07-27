-- 015_salgsoppgave.sql
-- Salgsoppgave extraction (2026-07-27 design spec). Like migration 010 these
-- are a DERIVED, DISPOSABLE cache -- fully rebuildable from
-- data/eiendom/html_extracted/ via `skannonser tools backfill-salgsoppgave --wipe`.
-- Every column is typed or enum-constrained; no free-text columns by design.

-- No byggeaar column on purpose: eiendom.info_construction_year already
-- carries it on 99% of live rows and the API already exposes it as `byggeaar`.
CREATE TABLE IF NOT EXISTS listing_salgsoppgave (
    finnkode TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    boligselgerforsikring BOOLEAN,
    eiendomsskatt_kr INTEGER,
    ferdigattest TEXT,          -- 'ferdigattest' | 'midlertidig' | 'ingen'
    radon_omtalt BOOLEAN,
    utleie TEXT,                -- 'tillatt' | 'ikke_tillatt' | 'egen_enhet'
    husdyr TEXT,                -- 'tillatt' | 'krever_godkjenning' | 'ikke_tillatt'
    heftelser BOOLEAN,
    -- Phase 2 (classifier) fills these; NULL until then.
    tg2_count INTEGER,
    tg3_count INTEGER,
    tilstandsrapport_dato TEXT,
    tilstandsrapport_utsteder TEXT,
    egenerklaering_antall INTEGER,
    parsed_at TEXT
);

CREATE TABLE IF NOT EXISTS listing_tg_findings (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    tg INTEGER NOT NULL,
    bygningsdel TEXT NOT NULL,
    tiltak TEXT,
    UNIQUE (finnkode, tg, bygningsdel)
);

CREATE TABLE IF NOT EXISTS listing_egenerklaering (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    forhold TEXT NOT NULL,
    UNIQUE (finnkode, forhold)
);

CREATE TABLE IF NOT EXISTS salgsoppgave_llm_cache (
    content_sha256 TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- The two pricing-<dl> labels parse_details.py currently drops. Deterministic
-- DOM parsing, so they belong alongside the other <dl> money fields.
ALTER TABLE listing_details ADD COLUMN eiendomsskatt_kr INTEGER;
ALTER TABLE listing_details ADD COLUMN verditakst INTEGER;
