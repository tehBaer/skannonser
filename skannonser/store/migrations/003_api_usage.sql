CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    called_at TEXT NOT NULL DEFAULT (datetime('now')),
    api TEXT NOT NULL,
    outcome TEXT NOT NULL,
    finnkode TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_usage_called_at ON api_usage(called_at);
