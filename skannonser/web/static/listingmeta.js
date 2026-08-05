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

// --- External map links -----------------------------------------------------
// Built from the geocode rather than `adresse`, because `adresse` is
// street-only ("Vinterkroken 75") -- no city, no postcode -- so handing it to
// Google's geocoder would be ambiguous nationwide. We already hold a precise
// lat/lng for ~93% of listings; the rest get no link at all.

// Earth Web's camera syntax, appended after the coordinates: 0a ground
// altitude, 300d range from target, 35y field of view, 0h north-up heading,
// 60t tilt off vertical, 0r roll. Close enough to read the building, tilted
// enough to show the terrain and neighbours a flat map cannot.
const EARTH_CAMERA = "0a,300d,35y,0h,60t,0r";

// [lat, lng] as finite numbers, or null when this listing was never geocoded.
// Both the empty string and null coerce to 0 through Number(), so the check
// has to be "is it finite" applied to a value we already know is non-blank --
// otherwise an un-geocoded row plots the Gulf of Guinea.
function coords(item) {
  const rawLat = item.lat;
  const rawLng = item.lng;
  if (rawLat === null || rawLat === undefined || rawLat === "") return null;
  if (rawLng === null || rawLng === undefined || rawLng === "") return null;
  const lat = Number(rawLat);
  const lng = Number(rawLng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return [lat, lng];
}

export function mapsUrl(item) {
  const c = coords(item);
  return c && "https://www.google.com/maps?q=" + c[0] + "," + c[1];
}

export function earthUrl(item) {
  const c = coords(item);
  return c && "https://earth.google.com/web/@" + c[0] + "," + c[1] + "," + EARTH_CAMERA;
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

// ---------------------------------------------------------------------------
// Salgsoppgave display (migration 015)
//
// The API serves these as enum keys (`ikke_tillatt`) and raw booleans. Both
// need formatting before a human sees them: popup.js's `addRow` only skips
// null/undefined/"", so a boolean `false` would render as the string "false",
// and an enum key is not Norwegian.
//
// All four return `null` for an absent value so `addRow` drops the row. That
// matters most for the booleans: `null` means "no salgsoppgave text existed"
// while `false` means "the text was read and the topic wasn't mentioned" --
// rendering "Nei" for the former would assert something never checked.
// ---------------------------------------------------------------------------

export function fmtJaNei(value) {
  if (value === null || value === undefined) return null;
  return value ? "Ja" : "Nei";
}

// An unmapped key is returned as-is rather than dropped: it means the parser
// grew a value the UI hasn't caught up with, and hiding it would make that
// invisible. Ugly beats silent.
function fromVocab(vocab, value) {
  if (value === null || value === undefined || value === "") return null;
  return vocab[value] !== undefined ? vocab[value] : value;
}

const FERDIGATTEST = {
  ferdigattest: "Ja",
  midlertidig: "Midlertidig brukstillatelse",
  ingen: "Nei",
};

const UTLEIE = {
  tillatt: "Tillatt",
  ikke_tillatt: "Ikke tillatt",
  egen_enhet: "Egen utleiedel",
};

const HUSDYR = {
  tillatt: "Tillatt",
  krever_godkjenning: "Krever godkjenning",
  ikke_tillatt: "Ikke tillatt",
};

// Chip/popover options, built from the same vocabularies the formatters use so
// a label can never drift between the popup, the map filter and the table
// filter. The trailing "" is the Ukjent bucket: a null here means the
// prospectus was never parsed, and it must be selectable in its own right
// rather than riding along with a real value (see filters.js).
function optionsOf(vocab) {
  return [
    ...Object.entries(vocab).map(([key, label]) => ({ key, label })),
    { key: "", label: "Ukjent" },
  ];
}

export const FERDIGATTEST_OPTIONS = optionsOf(FERDIGATTEST);
export const UTLEIE_OPTIONS = optionsOf(UTLEIE);
export const HUSDYR_OPTIONS = optionsOf(HUSDYR);

export function fmtFerdigattest(value) {
  return fromVocab(FERDIGATTEST, value);
}

export function fmtUtleie(value) {
  return fromVocab(UTLEIE, value);
}

export function fmtHusdyr(value) {
  return fromVocab(HUSDYR, value);
}

// Which table columns start hidden, given a reader's stored preferences.
//
// Extracted from table.js's localStorage read so the decision is testable:
// it has to distinguish "no opinion yet" from "deliberately shown", and
// getting that wrong either buries new columns forever or re-hides one the
// reader just chose to show.
//
//   stored == null / no hiddenColumns -> first run: the defaults
//   stored predating the migration    -> their set, plus the new columns
//   stored carrying the flag          -> their set, exactly as-is
export function resolveHiddenColumns(stored, defaults, newColumns) {
  const list = stored && Array.isArray(stored.hiddenColumns)
    ? stored.hiddenColumns
    : null;
  if (!list) return new Set(defaults);
  const hidden = new Set(list);
  if (!stored.salgsoppgaveColumnsDefaulted) {
    newColumns.forEach((key) => hidden.add(key));
  }
  return hidden;
}
