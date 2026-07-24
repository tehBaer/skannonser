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
    // hidden sets: {} = off; value key present => that value is excluded.
    boligtypeHidden: {},
    tagHidden: {},
    energiHidden: {},
    eieformHidden: {},
    tilgjengelighetHidden: {},
    // selected sets: [] = off; non-empty => ONLY these values pass.
    postnummerSelected: [],
    nabolagSelected: [],
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
//  * root-level ui.boligtypeHidden / ui.tagHidden (pre-2026-07-24) move into
//    filters.* (adopted only when filters.* doesn't have them yet);
//  * the legacy single-select `filters.eieform` string becomes an
//    eieformHidden set hiding every OTHER observed eieform.
export function loadFilters(meta) {
  const base = defaultFilters(meta);
  const blob = readBlob();
  const stored = blob.filters || {};
  const filters = {
    ...base,
    ...stored,
    travelMax: { ...base.travelMax, ...(stored.travelMax || {}) },
    boligtypeHidden: { ...(stored.boligtypeHidden || blob.boligtypeHidden || {}) },
    tagHidden: { ...(stored.tagHidden || blob.tagHidden || {}) },
    energiHidden: { ...(stored.energiHidden || {}) },
    eieformHidden: { ...(stored.eieformHidden || {}) },
    tilgjengelighetHidden: { ...(stored.tilgjengelighetHidden || {}) },
    postnummerSelected: [...(stored.postnummerSelected || [])],
    nabolagSelected: [...(stored.nabolagSelected || [])],
    facilitiesRequired: { ...(stored.facilitiesRequired || {}) },
  };
  if (typeof stored.eieform === "string") {
    if (stored.eieform) {
      (meta.eieformer || []).forEach((v) => {
        if (v !== stored.eieform) filters.eieformHidden[v] = true;
      });
    }
    delete filters.eieform;
  }
  return filters;
}

// Read-modify-write of ONLY the blob's `filters` key (the table page has no
// whole-ui object of its own), stripping the migrated legacy root keys so
// they can't shadow the new location on the next load.
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

  const hiddenSet = (key, label) => {
    const n = Object.keys(filters[key] || {}).length;
    if (n) {
      entries.push({
        key,
        label,
        valueText: n + " skjult",
        clear: (f) => {
          Object.keys(f[key]).forEach((k) => delete f[key][k]);
        },
      });
    }
  };
  hiddenSet("boligtypeHidden", "Boligtype");
  hiddenSet("eieformHidden", "Eieform");
  hiddenSet("energiHidden", "Energimerking");
  hiddenSet("tilgjengelighetHidden", "Tilgjengelighet");
  hiddenSet("tagHidden", "Tag");

  const selectedSet = (key, label) => {
    const n = (filters[key] || []).length;
    if (n) {
      entries.push({
        key,
        label,
        valueText: n + " valgt",
        clear: (f) => {
          f[key].splice(0, f[key].length);
        },
      });
    }
  };
  selectedSet("postnummerSelected", "Postnummer");
  selectedSet("nabolagSelected", "Nabolag");

  const nFac = Object.keys(filters.facilitiesRequired || {}).length;
  if (nFac) {
    entries.push({
      key: "facilitiesRequired",
      label: "Fasiliteter",
      valueText: nFac + " krav",
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

// Reset IN PLACE (both pages hold live references into this object),
// preserving only the includeUnknown policy choice.
export function resetFilters(filters, meta) {
  const keep = filters.includeUnknown;
  const fresh = defaultFilters(meta);
  Object.keys(filters).forEach((k) => delete filters[k]);
  Object.assign(filters, fresh, { includeUnknown: keep });
  return filters;
}
