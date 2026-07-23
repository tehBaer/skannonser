-- Per-target attempt ledger for the FINN sold-price sweep.
--
-- WHY: `sold_prices` only records SUCCESSES, so nothing distinguished "never
-- queried" from "queried forty times, no card exists". A sale that never gets
-- tinglyst (borettslag/share sales whose cadastral record differs, fall-through
-- sales, ads marked Solgt that are never registered) stays in the target set
-- forever -- and because the sweep orders by neighbour density with a stable
-- sort, the same permanently-ungettable targets led every session and re-burned
-- the daily request budget on the same dead boxes.
--
-- With this ledger the sweep tiers by attempt count first (untried before
-- once-tried before twice-tried ...), keeping density as the tiebreak within a
-- tier. Repeatedly-missed targets sink instead of starving the rest, and they
-- are never dropped -- tinglysing can lag by months, so a target that misses
-- today may well hit on a much later pass.
CREATE TABLE IF NOT EXISTS sold_price_attempts (
    finnkode TEXT PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempted_at TEXT
);
