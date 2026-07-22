-- Single-row control state for the FINN sold-price sweep. When FINN throttles
-- (429/403/503 or a challenge page), the sweep sets suspended_at and STOPS --
-- it stays suspended until a human resumes it (`skannonser run enrich-sold
-- --resume`). No auto-retry: recognizing pushback and backing off is the whole
-- safety posture for scraping a robots-disallowed path.
CREATE TABLE IF NOT EXISTS sold_sweep_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    suspended_at TEXT,          -- NULL = active; non-NULL = suspended since this time
    suspend_reason TEXT,
    last_run_at TEXT
);
INSERT OR IGNORE INTO sold_sweep_state (id) VALUES (1);
