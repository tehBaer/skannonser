// Shared listing-derived helpers: freshness ("Ny"), sold-price premium and
// travel-time sentinels. Used by popup.js (map popups), table.js (table cells)
// and app.js (map feature properties + status line) so the three views can
// never disagree.

// First seen within this window -> "Ny" badge.
export const NEW_WINDOW_MS = 3 * 24 * 60 * 60 * 1000;

// scraped_at comes as SQLite "YYYY-MM-DD HH:MM:SS" (UTC) or ISO with a T;
// normalize so every browser parses it, and parse as UTC.
export function parseScrapedAt(value) {
  if (!value) return null;
  let s = String(value).replace(" ", "T");
  if (!/[zZ+]/.test(s.slice(10))) s += "Z";
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : null;
}

export function isNew(item, now = Date.now()) {
  const t = parseScrapedAt(item.scraped_at);
  return t != null && now - t <= NEW_WINDOW_MS;
}

// dd.m.yyyy for a scraped_at/sold_date-style value, or null.
export function fmtDate(value) {
  const t = parseScrapedAt(value);
  if (t == null) return null;
  const d = new Date(t);
  return d.getDate() + "." + (d.getMonth() + 1) + "." + d.getFullYear();
}

// --- Travel-time sentinels --------------------------------------------------
// The enrichment pipeline stores NEGATIVE failure codes in the travel columns
// rather than NULL, so a row it already gave up on is not retried forever:
// -1 no routes, -2 unrealistic (over domain.travel.max_travel_minutes), -3 API
// error. See skannonser/enrich/sentinels.py -- that module is the source of
// truth; this mirrors only the "negative means failure" invariant, so a fourth
// code needs no change here.
//
// These are not minutes, and they are not NULL either, which is the trap: a
// raw -1 satisfies `mins > max` for every slider position and sorts ahead of a
// 12-minute commute. Every read of `item.travel` must go through these.

// Substituted for a sentinel when filtering: an out-of-range "unreachable"
// distance. Must stay above filterstate.js's TRAVEL_MAX or an active slider
// stops excluding these rows (asserted in tests/web/travelsentinel.test.mjs).
export const TRAVEL_UNREACHABLE = 999;

export function isTravelSentinel(value) {
  if (value === null || value === undefined || value === "") return false;
  const n = Number(value);
  return Number.isFinite(n) && n < 0;
}

// Minutes for display and sorting, or null when the commute is unknown --
// which a sentinel is: the pipeline never computed one. Returning null puts
// these rows in the table's existing "blank" path, so the cell renders empty
// (matching popup.js) and `compareItems` sorts them last in both directions.
export function travelMinutes(item, key) {
  const raw = (item.travel || {})[key];
  if (isTravelSentinel(raw)) return null;
  if (raw === null || raw === undefined || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

// Percent over/under prisantydning for a sold item, or null when either the
// tinglyst price or the asking price is missing.
export function premiumPct(item) {
  const soldPrice = Number(item.sold_price);
  const asking = Number(item.price_suggestion);
  if (!Number.isFinite(soldPrice) || !Number.isFinite(asking) || asking <= 0) {
    return null;
  }
  return (soldPrice / asking - 1) * 100;
}

// "+7,2 %" / "−3,1 %" (nb-NO decimals) for a premium percent.
export function fmtPremium(pct) {
  return (
    (pct >= 0 ? "+" : "−") +
    Math.abs(pct).toLocaleString("nb-NO", { maximumFractionDigits: 1 }) +
    " %"
  );
}
