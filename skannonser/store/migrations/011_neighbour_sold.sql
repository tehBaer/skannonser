-- 011_neighbour_sold.sql
-- Neighbour sold prices (2026-07-25 spec): the sweep now keeps EVERY card a
-- response carries, not just tracked targets -- sold data is the one dataset
-- that's expensive to re-acquire, and neighbouring sales are the best signal
-- for how a neighbourhood is priced. SAME table on purpose: a neighbour card
-- is the same entity as a tracked one (same endpoint, same fields, keyed by
-- finnkode); only its relationship to `eiendom` differs, and that is derived
-- via EXISTS, never stored (a flag could drift, a join can't). Every existing
-- consumer joins FROM eiendom, so untracked rows are invisible to the sold
-- bucket / promotion / coverage / budpremie by construction.
-- price_suggestion for neighbours is the asking price AT SALE TIME (possibly
-- reduced) -- tracked listings' first-seen asking lives in eiendom.pris.
ALTER TABLE sold_prices ADD COLUMN size INTEGER;
ALTER TABLE sold_prices ADD COLUMN property_type TEXT;
ALTER TABLE sold_prices ADD COLUMN bedrooms INTEGER;
ALTER TABLE sold_prices ADD COLUMN collective_debt INTEGER;
ALTER TABLE sold_prices ADD COLUMN ownership_type TEXT;
-- The tracked listing whose ~120 m query box surfaced this card ("sales near
-- X" lookup, no geocoding spend). NULL for --bbox probes and targets' own
-- cards. Fill-only: the FIRST discovery anchor wins.
ALTER TABLE sold_prices ADD COLUMN discovered_near_finnkode TEXT;
