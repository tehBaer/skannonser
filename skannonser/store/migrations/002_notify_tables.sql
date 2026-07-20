CREATE TABLE IF NOT EXISTS eiendom_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    finnkode TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_status_history_finnkode ON eiendom_status_history(finnkode);
CREATE TABLE IF NOT EXISTS daily_listing_snapshot (
    finnkode TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date TEXT PRIMARY KEY,
    added INTEGER NOT NULL DEFAULT 0,
    removed_sold INTEGER NOT NULL DEFAULT 0,
    removed_delisted INTEGER NOT NULL DEFAULT 0,
    total_active INTEGER NOT NULL DEFAULT 0
);
