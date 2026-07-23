// Shared listing-derived helpers: freshness ("Ny") and sold-price premium.
// Used by popup.js (map popups), table.js (table cells) and app.js (map
// feature properties + status line) so the three views can never disagree.

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
