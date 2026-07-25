-- 012_neighbour_sold_index.sql
-- Index the neighbour-sold lookup (2026-07-25 follow-up to migration 011).
-- GET /api/listings/{finnkode}/nabolag's only predicate is
-- `WHERE s.discovered_near_finnkode = ?` on a table this branch is designed
-- to grow by roughly an order of magnitude (every ~120 m sweep box now keeps
-- every card, not just its target's). Mirrors the precedent at
-- idx_dnbeiendom_duplicate_finnkode (001_adopt_live_schema.sql) for the same
-- "lookup column on a foreign-key-shaped TEXT column" shape.
--
-- NEW migration rather than an addition to 011: 011 has already been applied
-- to real databases, and the migration runner (skannonser/store/migrations.py)
-- records applied migrations by filename stem in `schema_migrations` -- an
-- edit to an already-applied file would never re-run on those databases.
CREATE INDEX IF NOT EXISTS idx_sold_prices_discovered_near
    ON sold_prices(discovered_near_finnkode);
