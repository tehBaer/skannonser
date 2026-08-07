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
//
// Blankness is rejected BEFORE Number(), not after: Number(null) is 0 and
// Number("") is 0, both of which satisfy Number.isFinite, so a closed listing
// whose sale is not yet tinglyst used to compute as a sale for 0 kr and render
// "-100 %". Same trap `coords()` documents above. An explicit numeric 0 is
// still a real (if implausible) sale and keeps computing.
export function premiumPct(item) {
  const rawSold = item.sold_price;
  const rawAsking = item.price_suggestion;
  if (rawSold === null || rawSold === undefined || rawSold === "") return null;
  if (rawAsking === null || rawAsking === undefined || rawAsking === "") return null;
  const soldPrice = Number(rawSold);
  const asking = Number(rawAsking);
  if (!Number.isFinite(soldPrice) || !Number.isFinite(asking) || asking <= 0) {
    return null;
  }
  return (soldPrice / asking - 1) * 100;
}

// A number fit for arithmetic, or null when the field is blank. Same
// blank-before-Number() discipline premiumPct spells out inline above: null
// and "" both coerce to a finite 0, so the blankness test has to happen first.
function numOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

// An OPTIONAL addend: blank, absent or unparseable all contribute 0.
//
// Deliberately the opposite rule from the sold price itself, because the two
// blanks mean different things. A missing `fellesgjeld` is not unknown -- it is
// zero: selveier ads simply omit the Fellesgjeld line (2 867 of 6 258
// listing_details rows have it NULL), and coalescing those to 0 reconciles
// `totalpris - omkostninger - fellesgjeld` against FINN's own priceSuggestion
// on 82 of the 85 sold rows where it is NULL. Treating it as unknown would
// blank this column for nearly every selveier.
function addend(value) {
  const n = numOrNull(value);
  return n === null ? 0 : n;
}

// The sold-side counterpart of `totalpris`: what the buyer actually committed
// to. Exists because the table sits `Totalpris` next to `Solgt for` on two
// different bases -- the first includes omkostninger and fellesgjeld, the
// second (FINN's tinglyst `cadastralSoldPrice`) does not -- so the gap between
// them reads as a discount when it is nothing of the sort.
//
// TWO CAVEATS, both sub-1 % and in opposite directions:
//   * `omkostninger` is quoted on the PRISANTYDNING. For selveier the
//     dokumentavgift is 2,5 % of the actual sale price, so a sale over asking
//     understates by ~2,5 % of the overage (250k over ask ~ 6 000 kr short).
//     Borettslag omkostninger are a flat fee, so those are exact.
//   * `fellesgjeld` is as of the ad; tinglysing lands ~100 days later, by
//     which point it has amortised slightly.
// Good enough to compare listings, not a settlement statement.
export function soldTotalpris(item) {
  const sold = numOrNull(item.sold_price);
  if (sold === null) return null;
  return sold + addend(item.omkostninger) + addend(item.fellesgjeld);
}

// The sold analogue of Total/kvm (api.py's `_pris_kvm_totalpris`) -- the number
// that actually compares two flats. Same divisor (BRA-i) and same rounding, so
// a sold row and an active row can be read down the same mental column.
export function soldPrisKvmTotalpris(item) {
  const total = soldTotalpris(item);
  if (total === null || total <= 0) return null;
  const braI = numOrNull(item.bra_i);
  if (braI === null || braI <= 0) return null;
  return Math.round(total / braI);
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

// `heftelser` and `radon_omtalt` are MENTION detectors -- the parser searches
// the prospectus for `servitutt|heftelse` and `radon`. True therefore means
// "the document discusses this", not "the property has it". Rendering them as
// Ja/Nei was actively misleading: "Radon: Ja" reads as a radon problem, while
// half of all listings mention servitutter purely as boilerplate about a right
// of way. Say what was observed instead.
export function fmtOmtalt(value) {
  if (value === null || value === undefined) return null;
  return value ? "Omtalt" : "Ikke omtalt";
}

// Fields whose value is extracted from the salgsoppgave PROSE rather than read
// from structured markup, so the UI can mark them as softer than the rest. A
// blank here means "the prospectus did not say", which is not the same as
// "no" -- and for the mention-detectors above, a value means even less.
//
// `eiendomsskatt_kr` is included because it falls back to prose when the
// pricing <dl> has no figure. `verditakst` is NOT: it comes only from that
// <dl>, exactly like totalpris, which carries no marker.
export const SALGSOPPGAVE_DERIVED = new Set([
  "ferdigattest",
  "utleie",
  "husdyr",
  "heftelser",
  "radon_omtalt",
  "boligselgerforsikring",
  "eiendomsskatt_kr",
]);

// Appended to every derived field's label. Applied from SALGSOPPGAVE_DERIVED
// at render time rather than baked into each label, so the marker and the set
// cannot drift apart.
export const SALGSOPPGAVE_SUFFIX = " (s)";

export const SALGSOPPGAVE_HINT =
  "(s) = hentet fra teksten i salgsoppgaven \u2014 tomt felt betyr at " +
  "salgsoppgaven ikke sa noe, ikke at svaret er nei";

// ---------------------------------------------------------------------------
// Computed columns -- the third provenance class
//
// SALGSOPPGAVE_DERIVED above marks regex over prospectus prose, and
// TILSTAND_DERIVED below marks a model's judgement. Both answer "how reliable
// is this value". THIS set answers a different question: is the number in the
// cell one FINN published, or one this UI worked out from several others?
//
// It matters most where the two sit side by side. `Solgt tot/kvm` combines a
// tinglyst sale price from FINN's sold-map card, omkostninger and fellesgjeld
// from the ad's pricing <dl>, and BRA-i from the key-info block -- four fields,
// three sources, one cell. A blank there can mean any of four things missing,
// and the figure inherits every one of their caveats (see `soldTotalpris`).
//
// NOT included: `reparasjon_est` and `reparasjon_usikkerhet`, which are also
// arithmetic but already carry TILSTAND_DERIVED's colour. That marker is the
// louder and more important one, and double-marking would render "Utbedring
// (b)" in violet -- two signals competing to say the same "treat with care".
export const COMPUTED_COLUMNS = new Set([
  "pris_kvm", // compute_pris_kvm: pris / area, over a P-ROM -> BRA-i -> BRA fallback chain
  "pris_kvm_totalpris", // totalpris / BRA-i
  "maanedskost", // felleskost/mnd + kommunale avgifter/12
  "premium", // sold_price / price_suggestion - 1
  "sold_totalpris", // sold_price + omkostninger + fellesgjeld
  "sold_pris_kvm_totalpris", // that / BRA-i
]);

export const COMPUTED_SUFFIX = " (b)";

export const COMPUTED_HINT =
  "(b) = beregnet av denne appen fra andre felt — ikke et tall FINN har " +
  "oppgitt. Tomt felt betyr at ett av leddene manglet.";

// `label` with the marker appended when the field is prose-derived or
// computed. Applied at render time from the sets above rather than baked into
// each label, so a key added to a set is marked everywhere at once. The two
// sets are disjoint (asserted in tests/web/soldtotal.test.mjs) -- a key in both
// would render "Foo (s) (b)" -- so the order of these branches is arbitrary.
export function labelWithSource(key, label) {
  if (SALGSOPPGAVE_DERIVED.has(key)) return label + SALGSOPPGAVE_SUFFIX;
  if (COMPUTED_COLUMNS.has(key)) return label + COMPUTED_SUFFIX;
  return label;
}

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

// ---------------------------------------------------------------------------
// Status (tilgjengelighet) vocabulary
//
// FIXED, not derived. `deriveVocabs` counts the statuses present in loaded
// items, and on a cold load only actives are loaded -- so "Solgt" would be
// missing from the vocabulary, leaving no control to click to trigger the very
// fetch that would produce it. Same reasoning as FERDIGATTEST_OPTIONS above:
// the value list is known ahead of the data.
//
// "" is "Til salgs", a real status and the most common one -- the backend only
// fills tilgjengelighet for CLOSED listings. It leads the list rather than
// being sorted to the end like other "" buckets (see selectionChipRow's
// emptyIsRealValue).
export const TILGJENGELIGHET_OPTIONS = [
  { key: "", label: "Til salgs" },
  { key: "Solgt", label: "Solgt" },
  { key: "Inaktiv", label: "Inaktiv" },
  { key: "Trukket", label: "Trukket" },
];

export function fmtFerdigattest(value) {
  return fromVocab(FERDIGATTEST, value);
}

export function fmtUtleie(value) {
  return fromVocab(UTLEIE, value);
}

export function fmtHusdyr(value) {
  return fromVocab(HUSDYR, value);
}

// ---------------------------------------------------------------------------
// Tilstand classifier (migration 016)
//
// Same discipline as the salgsoppgave fields: null means "never classified",
// which addRow must skip -- never render as "0 kr" or "Nei".
// ---------------------------------------------------------------------------

const ALVORLIGHET_LABELS = {
  kosmetisk: "Kosmetisk",
  mindre: "Mindre",
  vesentlig: "Vesentlig",
  alvorlig: "Alvorlig",
};

export const BYGNINGSDEL_LABELS = {
  vatrom: "Våtrom",
  kjokken: "Kjøkken",
  tak: "Tak",
  vinduer_dorer: "Vinduer/dører",
  yttervegg: "Yttervegg",
  etasjeskille: "Etasjeskille",
  grunn_drenering: "Grunn/drenering",
  vvs: "VVS",
  elektrisk: "Elektrisk",
  ventilasjon: "Ventilasjon",
  overflater: "Overflater",
  balkong_terrasse: "Balkong/terrasse",
  trapp: "Trapp",
  radon: "Radon",
  vaskerom: "Vaskerom",
  utvendig_annet: "Utvendig annet",
  helhet: "Helhet",
  annet: "Annet",
};

const RADON_STATUS_LABELS = {
  ikke_malt: "Ikke målt",
  // Distinct from "Ikke målt": the document gave a reason radon does not apply
  // (typically a flat several storeys up), rather than leaving a gap.
  ikke_relevant: "Ikke relevant",
  malt_under_grense: "Målt, under grense",
  // Shouted deliberately: this is the one radon state that costs money.
  malt_over_grense: "Målt, OVER grense",
  malt_ukjent_verdi: "Målt, verdi ikke oppgitt",
};

const RADONSPERRE_LABELS = { finnes: "Radonsperre", mangler: "Ingen radonsperre" };

export function fmtRadonStatus(value) {
  return fromVocab(RADON_STATUS_LABELS, value);
}

export function fmtRadonsperre(value) {
  return fromVocab(RADONSPERRE_LABELS, value);
}

// The single radon answer, merged from the classifier and the mention flag in
// descending order of confidence.
//
// 1. A classifier verdict, plus the measured value when one was stated.
//    radon_bq is populated on only ~2% of ads by design -- most Bq figures in
//    a prospectus are the statutory thresholds, and the classifier is told not
//    to extract those.
// 2. Otherwise, if the prospectus never says "radon" at all, then nobody
//    measured it: a measurement is the kind of thing a document reports.
//    Labelled "Ikke nevnt" rather than "Ikke målt" because it is inferred from
//    silence, where "Ikke målt" is the surveyor stating it outright.
// 3. Otherwise blank. "Mentioned, but only the statutory boilerplate" and "not
//    classified yet" and "no prospectus parsed" are all genuinely no
//    information, and must not masquerade as an answer.
//
// Step 2 is only sound because `radon_omtalt` was fixed on 2026-08-07 -- the
// old \bradon\b matched headings only, and inferring from ITS silence would
// have overwritten four real measurements with "not measured".
export function fmtRadon(item) {
  const s = fmtRadonStatus(item.radon_status);
  if (s) {
    const bq = item.radon_bq;
    return bq === null || bq === undefined ? s : s + " (" + bq + " Bq/m³)";
  }
  return item.radon_omtalt === false ? "Ikke nevnt" : null;
}

export function fmtAlvorlighet(value) {
  return fromVocab(ALVORLIGHET_LABELS, value);
}

export function fmtBygningsdel(value) {
  return fromVocab(BYGNINGSDEL_LABELS, value);
}

// nb-NO's grouping separator is U+00A0 (NBSP), not a plain space -- \s
// matches it, so this normalizes to a plain space for deterministic output
// (tests, and consistent copy/paste).
function fmtKr(n) {
  return n.toLocaleString("nb-NO").replace(/\s/g, " ");
}

// Cost band with provenance: `takst` is the surveyor's own figure and renders
// plain; `estimat`/`blandet` carry model judgment and are hedged with "~".
// The grid's 1 000 000 ceiling means "1M+", so a band touching it is open.
export function fmtKostnadBand(lav, hoy, kilde) {
  if (lav === null || lav === undefined || hoy === null || hoy === undefined) return null;
  if (lav === 0 && hoy === 1000000) return null; // fully unbounded band: no real signal
  // A report read with no TG2/TG3 findings rolls up to (0, 0). That is a known
  // zero, not a band and not a guess -- so no "under" wording and no ~ hedge.
  if (lav === 0 && hoy === 0) return "0 kr";
  const hedge = kilde === "takst" ? "" : "~";
  if (lav === 0) return hedge + "under " + fmtKr(hoy) + " kr";
  if (hoy === 1000000) return hedge + "over " + fmtKr(lav) + " kr";
  if (lav === hoy) return hedge + fmtKr(lav) + " kr";
  return hedge + fmtKr(lav) + " – " + fmtKr(hoy) + " kr";
}

export const TILSTAND_HINT =
  "Fra tilstandsrapporten, KI-klassifisert. ~ = kostnadsanslag fra modellen, " +
  "ikke takstmannens tall. 0 betyr at rapporten ble lest uten TG2/TG3-funn. " +
  "Tomt felt betyr at ingen tilstandsrapport ble lest.";

// Columns whose values a LANGUAGE MODEL produced, as opposed to
// SALGSOPPGAVE_DERIVED above, which is regex over the same prose. The
// distinction is worth a separate colour rather than a shared marker: a regex
// is wrong in ways you can predict from reading it, while a model can be
// fluently and confidently wrong about a real defect -- most of all for
// `reparasjon_est`, where an unstated cost is the model's own guess. Any
// future LLM-backed column belongs in this set, and in no other.
export const TILSTAND_DERIVED = new Set([
  "tg3_count",
  "reparasjon_est",
  "reparasjon_usikkerhet",
  "alvorlighet",
  "radon_status",
]);

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

// Second, independent one-time migration for migration 016's tilstand
// columns, applied on top of resolveHiddenColumns's result. Split out (rather
// than folded into resolveHiddenColumns's single flag) because a reader can
// have passed the 015 migration (salgsoppgaveColumnsDefaulted: true) before
// 016 existed, and still needs THIS migration to run once for them.
//
// `stored` null/falsy is a no-op: resolveHiddenColumns already returned the
// full defaults (which include the tilstand columns) for a fresh reader, so
// there is nothing more to add. Mutates and returns `hidden` for convenience.
export function applyTilstandColumnsMigration(hidden, stored, tilstandColumns) {
  if (stored && !stored.tilstandColumnsDefaulted) {
    tilstandColumns.forEach((key) => hidden.add(key));
  }
  return hidden;
}
