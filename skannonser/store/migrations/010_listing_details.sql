-- 010_listing_details.sql
-- Listing-details enrichment (2026-07-23 design spec): group-A/B/C fields
-- parsed from cached FINN ad HTML by skannonser/ingest/finn/parse_details.py.
-- Both tables are a DERIVED, DISPOSABLE cache -- fully rebuildable from
-- data/eiendom/html_extracted/ via `skannonser tools backfill-details --wipe`.
-- Full-row REPLACE semantics, no fill-only columns.
CREATE TABLE IF NOT EXISTS listing_details (
    finnkode TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    bedrooms INTEGER, rooms INTEGER, floor INTEGER,
    eieform TEXT, nabolag TEXT,
    totalpris INTEGER, omkostninger INTEGER, fellesgjeld INTEGER,
    felleskost_mnd INTEGER, fellesformue INTEGER, formuesverdi INTEGER,
    kommunale_avg_aar INTEGER,
    energimerke TEXT, energifarge TEXT,
    kommunenr TEXT, gardsnr TEXT, bruksnr TEXT, seksjonsnr TEXT,
    borettslag_navn TEXT, borettslag_orgnr TEXT, borettslag_andelsnr TEXT,
    parsed_at TEXT
);
CREATE TABLE IF NOT EXISTS listing_facilities (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    facility TEXT NOT NULL,
    UNIQUE (finnkode, facility)
);
