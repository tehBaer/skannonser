-- FINN sold-price enrichment (dormant feature): the actual tinglyst sale
-- price for a listing, keyed by finnkode (== FINN adId). Populated by
-- skannonser/enrich/sold.py from the FINN sold-map card endpoint. Separate
-- table (not columns on eiendom) because only sold listings have a row and
-- the price registers ~100 days after sale, so it fills in over time.
CREATE TABLE IF NOT EXISTS sold_prices (
    finnkode TEXT PRIMARY KEY,
    sold_price INTEGER,            -- cadastralSoldPrice (kr); the tinglyst sale sum
    sold_date TEXT,               -- soldDate (bidding/sale date, ISO)
    cadastral_sold_date TEXT,     -- cadastralSoldDate (tinglysing/registration date, ISO)
    price_suggestion INTEGER,     -- priceSuggestion (prisantydning / asking price)
    address TEXT,
    source TEXT NOT NULL DEFAULT 'finn_map',
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
