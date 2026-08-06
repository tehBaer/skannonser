// skannonser/web/static/filterstate.js
// Canonical shared filter state for the map AND the table (2026-07-24
// unified-filtering spec). Both pages read/write the `filters` object inside
// the one skannonser.ui.v1 localStorage blob through THIS module only.
// Cross-tab sync rides the `storage` event (fires in every OTHER tab on a
// write); same-tab flows call their own onChange directly after saving.

const STORAGE_KEY = "skannonser.ui.v1";

// Slider bounds — a slider AT its bound means "filter off".
export const BRA_I_SLIDER_MAX = 250;
export const TRAVEL_MAX = 120;
export const TOTALPRIS_MAX = 10_000_000;
export const FELLESKOST_MAX = 15000;
export const BYGGEAAR_FLOOR = 1900;
export const BYGGEAAR_CEIL = 2030;
export const TOTAL_KVM_MAX = 120_000;
export const MAANEDSKOST_MAX = 20_000;
export const PRIS_KVM_MAX = 150_000;
export const SOLD_PRICE_MAX = 10_000_000;
export const PREMIUM_MAX = 30; // percent over prisantydning
// Tilstand classifier (migration 016) rollup: reparasjon_est sums midpoints
// across every TG2/TG3 finding, so it can exceed the 1M grid ceiling other
// money sliders use -- a single roof + drainage + electrical estimate alone
// can clear it.
export const REPARASJON_MAX = 2_000_000;

export function priceBoundOf(meta) {
  return Number((meta.filters && meta.filters.sheets_max_price) || 7500000);
}

export function defaultFilters(meta) {
  const travelMax = {};
  (meta.destinations || []).forEach((d) => {
    travelMax[d.key] = TRAVEL_MAX;
  });
  return {
    // sliders
    priceMax: priceBoundOf(meta),
    braIMin: 0,
    travelMax,
    soveromMin: 0,
    totalprisMax: TOTALPRIS_MAX,
    felleskostMax: FELLESKOST_MAX,
    byggeaarMin: BYGGEAAR_FLOOR,
    totalKvmMax: TOTAL_KVM_MAX,
    maanedskostMax: MAANEDSKOST_MAX,
    prisKvmMax: PRIS_KVM_MAX,
    soldPriceMax: SOLD_PRICE_MAX,
    premiumMax: PREMIUM_MAX,
    reparasjonMax: REPARASJON_MAX,
    // selected sets: [] = off; non-empty => ONLY these values pass.
    boligtypeSelected: [],
    eieformSelected: [],
    energiSelected: [],
    tilgjengelighetSelected: [],
    tagSelected: [],
    postnummerSelected: [],
    nabolagSelected: [],
    // Tilstand classifier (migration 016). Routed like energiSelected, not
    // eieformSelected: a null alvorlighet means the listing was never
    // classified (no tilstandsrapport read), so the "" bucket must be an
    // explicit, selectable choice rather than deferring to includeUnknown.
    alvorlighetSelected: [],
    // Salgsoppgave enums (migration 015). Routed like energiSelected, not
    // eieformSelected: a missing value here means the prospectus was never
    // parsed (~36 % of listings), so deferring it to `includeUnknown` would
    // make picking a real value return every unparsed listing too.
    ferdigattestSelected: [],
    utleieSelected: [],
    husdyrSelected: [],
    // special
    facilitiesRequired: {},
    includeUnknown: true,
  };
}

function readBlob() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (_) {
    return {};
  }
}

// Stored-over-default merge + one-time migrations of legacy key shapes:
//  * the hidden-sets of the six value filters are dropped (see below);
//  * the legacy single-select `filters.eieform` string becomes a one-element
//    eieformSelected -- an exact restatement of the same intent, needing no
//    vocabulary.
export function loadFilters(meta) {
  const base = defaultFilters(meta);
  const blob = readBlob();
  const stored = blob.filters || {};
  const filters = {
    ...base,
    ...stored,
    travelMax: { ...base.travelMax, ...(stored.travelMax || {}) },
    boligtypeSelected: [...(stored.boligtypeSelected || [])],
    eieformSelected: [...(stored.eieformSelected || [])],
    energiSelected: [...(stored.energiSelected || [])],
    tilgjengelighetSelected: [...(stored.tilgjengelighetSelected || [])],
    tagSelected: [...(stored.tagSelected || [])],
    postnummerSelected: [...(stored.postnummerSelected || [])],
    nabolagSelected: [...(stored.nabolagSelected || [])],
    ferdigattestSelected: [...(stored.ferdigattestSelected || [])],
    utleieSelected: [...(stored.utleieSelected || [])],
    husdyrSelected: [...(stored.husdyrSelected || [])],
    alvorlighetSelected: [...(stored.alvorlighetSelected || [])],
    facilitiesRequired: { ...(stored.facilitiesRequired || {}) },
  };
  // The 2026-07-26 conversion from hidden-sets to selections. Inverting a
  // hidden set needs the COMPLETE value list to select everything else, and for
  // tags and tilgjengelighet that list is not known until listings load --
  // acting on a partial vocabulary is exactly what silently destroyed saved
  // filters before. So the five converted filters reset once; nothing else in
  // the blob is touched. Unconditional and after the merge, so a stale key
  // cannot survive `...stored` and be written back by saveFilters.
  ["boligtypeHidden", "eieformHidden", "energiHidden",
   "tilgjengelighetHidden", "tagHidden"].forEach((k) => delete filters[k]);
  if (typeof stored.eieform === "string") {
    if (stored.eieform) filters.eieformSelected = [stored.eieform];
    delete filters.eieform;
  }
  return filters;
}

// Read-modify-write of ONLY the blob's `filters` key (the table page has no
// whole-ui object of its own). The root-key deletes below are plain garbage
// collection: loadFilters stopped reading those legacy roots in the 2026-07-26
// conversion, so there is no shadowing left to prevent -- they are removed only
// so the blob does not carry dead weight forever.
export function saveFilters(filters) {
  try {
    const blob = readBlob();
    blob.filters = filters;
    delete blob.boligtypeHidden;
    delete blob.tagHidden;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(blob));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

export function subscribeOtherTabs(cb) {
  window.addEventListener("storage", (ev) => {
    if (ev.key === STORAGE_KEY) cb();
  });
}

// One entry per ACTIVE filter dimension: {key, label, valueText, clear(f)}.
// Drives the expandable active-filter list (map sidebar). clear() mutates
// sub-objects IN PLACE (delete keys / splice) so live component references
// into the shared filters object stay valid.
export function activeFilterEntries(filters, meta) {
  const NOK = new Intl.NumberFormat("nb-NO");
  const entries = [];
  const kr = (v) => NOK.format(v) + " kr";
  const maxSlider = (key, label, ceiling, fmt) => {
    if (filters[key] < ceiling) {
      entries.push({
        key,
        label,
        valueText: fmt(filters[key]),
        clear: (f) => {
          f[key] = ceiling;
        },
      });
    }
  };
  const minSlider = (key, label, floor, fmt) => {
    if (filters[key] > floor) {
      entries.push({
        key,
        label,
        valueText: fmt(filters[key]),
        clear: (f) => {
          f[key] = floor;
        },
      });
    }
  };

  maxSlider("priceMax", "Maks pris", priceBoundOf(meta), kr);
  maxSlider("totalprisMax", "Maks totalpris", TOTALPRIS_MAX, kr);
  maxSlider("felleskostMax", "Maks felleskost", FELLESKOST_MAX, kr);
  maxSlider("maanedskostMax", "Maks mnd-kost", MAANEDSKOST_MAX, kr);
  maxSlider("totalKvmMax", "Maks total/kvm", TOTAL_KVM_MAX, kr);
  maxSlider("prisKvmMax", "Maks pris/kvm", PRIS_KVM_MAX, kr);
  maxSlider("soldPriceMax", "Maks solgt-pris", SOLD_PRICE_MAX, kr);
  maxSlider("premiumMax", "Maks budpremie", PREMIUM_MAX, (v) => "≤ +" + v + " %");
  maxSlider("reparasjonMax", "Maks utbedring", REPARASJON_MAX, kr);
  minSlider("braIMin", "Min BRA-i", 0, (v) => "≥ " + v + " m²");
  minSlider("soveromMin", "Min soverom", 0, (v) => "≥ " + v);
  minSlider("byggeaarMin", "Min byggeår", BYGGEAAR_FLOOR, (v) => "≥ " + v);

  Object.keys(filters.travelMax || {}).forEach((destKey) => {
    if (filters.travelMax[destKey] < TRAVEL_MAX) {
      entries.push({
        key: "travelMax." + destKey,
        label: "Maks " + destKey.split("_").pop().toUpperCase(),
        valueText: "≤ " + filters.travelMax[destKey] + " min",
        clear: (f) => {
          f.travelMax[destKey] = TRAVEL_MAX;
        },
      });
    }
  });

  const selectedSet = (key, label) => {
    const n = (filters[key] || []).length;
    if (n) {
      entries.push({
        key,
        label,
        valueText: n + " valgt",
        count: n,
        clear: (f) => {
          f[key].splice(0, f[key].length);
        },
      });
    }
  };
  selectedSet("boligtypeSelected", "Boligtype");
  selectedSet("eieformSelected", "Eieform");
  selectedSet("energiSelected", "Energimerking");
  // tilgjengelighetSelected skips the shared selectedSet helper: [""] is the
  // seedStatus floor (see seedStatus above), not a user choice, and counting
  // it here would mean activeFilterCount can never reach 0 -- the map would
  // permanently read "1 filtre aktive" at rest, and a "disable Nullstill when
  // nothing is active" behaviour could never fire. Any OTHER non-empty
  // selection (e.g. ["", "Solgt"]) still counts as active; [] itself does
  // NOT -- tilg.length is falsy for it, same as the floor case above.
  const tilg = filters.tilgjengelighetSelected || [];
  if (tilg.length && !(tilg.length === 1 && tilg[0] === "")) {
    entries.push({
      key: "tilgjengelighetSelected",
      label: "Tilgjengelighet",
      valueText: tilg.length + " valgt",
      count: tilg.length,
      clear: (f) => {
        f.tilgjengelighetSelected.splice(0, f.tilgjengelighetSelected.length);
      },
    });
  }
  selectedSet("tagSelected", "Tag");
  selectedSet("postnummerSelected", "Postnummer");
  selectedSet("nabolagSelected", "Nabolag");
  selectedSet("ferdigattestSelected", "Ferdigattest");
  selectedSet("utleieSelected", "Utleie");
  selectedSet("husdyrSelected", "Husdyr");
  selectedSet("alvorlighetSelected", "Alvorlighet");

  const nFac = Object.keys(filters.facilitiesRequired || {}).length;
  if (nFac) {
    entries.push({
      key: "facilitiesRequired",
      label: "Fasiliteter",
      valueText: nFac + " krav",
      count: nFac,
      clear: (f) => {
        Object.keys(f.facilitiesRequired).forEach((k) => delete f.facilitiesRequired[k]);
      },
    });
  }
  return entries;
}

export function activeFilterCount(filters, meta) {
  return activeFilterEntries(filters, meta).length;
}

// Drop selected values that no longer exist in the current vocabulary.
// Without this, selecting a chip for a value that later leaves the
// vocabulary (a tag only closed listings carried, say) leaves an entry that
// filters nothing but keeps counting toward "N filtre aktive" forever -- and
// persists to localStorage. Returns true when something was removed so the
// caller can save. Only the deriveVocabs-backed sets are pruned; boligtype,
// eieform and energimerke come from the server's static meta vocabulary.
//
// `vocabComplete` GATES THE DELETION, and defaults to false because a
// speculative prune is unrecoverable. Both pages derive their vocabulary from
// the listings currently VISIBLE, which on the load path and after any layer
// toggle is a strict subset of the data:
//   * the map builds its UI before the lazily-fetched closed bucket arrives,
//     so a tag or nabolag carried only by closed listings looked "gone";
//   * the table reads its "Vis solgte" pref after the first prune has already
//     run against actives only;
//   * switching every layer off narrows the vocabulary to nothing, which used
//     to wipe the whole selection in two clicks;
//   * and the two pages prune ONE shared blob against DIFFERENT vocabularies
//     (the map has two closed buckets, the table one), so with both tabs open
//     one page deleted a filter the other was actively using.
// Callers pass true only when their vocabulary covers every listing the app
// has -- all buckets enabled AND the closed bucket fetched -- which is the
// same full dataset on both pages, so neither can undercut the other. When it
// is false this is a total no-op: it does not mutate either, because the
// caller's filters object is saved wholesale by unrelated flows and an
// "unpersisted" deletion would ride along on the next write.
export function pruneFilterSets(filters, vocabs, vocabComplete = false) {
  if (!filters || !vocabs || !vocabComplete) return false;
  let changed = false;
  const keysOf = (list) => new Set((list || []).map((o) => o.key));

  const pruneSelected = (arrKey, allowed) => {
    const arr = filters[arrKey];
    if (!Array.isArray(arr)) return;
    for (let i = arr.length - 1; i >= 0; i--) {
      if (!allowed.has(arr[i])) {
        arr.splice(i, 1);
        changed = true;
      }
    }
  };

  pruneSelected("tagSelected", keysOf(vocabs.tags));
  // tilgjengelighetSelected is DELIBERATELY exempt. Its four checkboxes come
  // from the fixed TILGJENGELIGHET_OPTIONS list now, not from deriveVocabs, so
  // a status with zero OBSERVED listings (Trukket, currently zero in
  // production) would never appear in vocabs.tilgjengelighet and would be
  // pruned the instant a user selected it with all four boxes checked --
  // permanently and silently, since pruning is unrecoverable by design. Status
  // is a closed domain; pruning it against observed data is simply wrong.
  pruneSelected("postnummerSelected", keysOf(vocabs.postnummer));
  pruneSelected("nabolagSelected", keysOf(vocabs.nabolag));
  return changed;
}

// Reset IN PLACE (both pages hold live references into this object),
// preserving only the includeUnknown policy choice.
export function resetFilters(filters, meta) {
  const keep = filters.includeUnknown;
  const fresh = defaultFilters(meta);
  Object.keys(filters).forEach((k) => delete filters[k]);
  Object.assign(filters, fresh, { includeUnknown: keep });
  return filters;
}

// The status floor. An empty selection means "unfiltered" everywhere else in
// this module, but for status that collides with the lazy closed-bucket fetch:
// a cold load with an empty selection shows only actives (the closed rows were
// never fetched), while an empty selection reached by selecting Solgt and then
// pressing Nullstill shows everything. Same stored value, two different views.
//
// So the pages never hold an empty status selection. Applied on load and again
// after resetFilters, which keeps ONE definition of "default" in defaultFilters
// and layers this floor on top rather than forking it.
export function seedStatus(filters) {
  // MUTATE, never reassign: wireStatusToggles' checkbox handlers close over
  // `state.ui.filters.tilgjengelighetSelected` by reference. Reassigning here
  // (the array used to be replaced wholesale) would detach every already-bound
  // closure from state on the very next empty-selection tick -- the handlers
  // keep splicing a now-orphaned array while state.ui.filters points at a new
  // one, silently bricking all four checkboxes for the rest of the session.
  if (!Array.isArray(filters.tilgjengelighetSelected)) {
    filters.tilgjengelighetSelected = [];
  }
  if (!filters.tilgjengelighetSelected.length) {
    filters.tilgjengelighetSelected.push("");
  }
  return filters;
}
