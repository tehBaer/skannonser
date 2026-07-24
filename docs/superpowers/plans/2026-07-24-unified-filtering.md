# Unified Map/Table Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One shared, localStorage-synced filter state driving both the map (dim/hide) and the table (Notion-style header popovers), with a single predicate.

**Architecture:** New `filterstate.js` owns the canonical filters object (defaults, load+migrate, save, cross-tab subscribe, active-count, reset) inside the existing `skannonser.ui.v1` blob. `filters.js` gains the single predicate `listingExcluded` plus three reusable UI components (range row, checkbox group, searchable multi-select) that both the map sidebar and the new `tablefilters.js` header popovers mount. No backend changes.

**Tech Stack:** Plain browser ES modules (no build step), localStorage + `storage` events, existing MapLibre map page and plain-JS table page.

**Spec:** `docs/superpowers/specs/2026-07-24-unified-filtering-design.md`

## Global Constraints

- ZERO backend changes: no Python, no API, no SQL. The pytest suite (616) must stay green untouched — run it in the final task as proof.
- No JS test harness exists. Gate every JS task on `node --check <file>` for each touched file, plus the node-driven predicate spot-checks in Task 2. Browser verification is done by the controller (implementers must NOT start servers/browsers).
- Slider-at-bound = off, everywhere. Bounds (exact values): `TRAVEL_MAX = 120`, `TOTALPRIS_MAX = 10_000_000`, `FELLESKOST_MAX = 15000`, `BRA_I_SLIDER_MAX = 250`, `BYGGEAAR_FLOOR = 1900`, `BYGGEAAR_CEIL = 2030`, `TOTAL_KVM_MAX = 120_000`, `MAANEDSKOST_MAX = 20_000`.
- Hidden-set semantics ({} = off, key present = excluded) for boligtype/tag/energi/eieform/tilgjengelighet; selected-set semantics ([] = off, non-empty = only these) for postnummer/nabolag.
- `includeUnknown` (default true) is the single null policy for every numeric filter and for null energimerke/eieform/postnummer/nabolag/facilities. Exceptions that do NOT follow it: travel minutes (missing travel NEVER excludes — legacy rule), and the "" buckets of boligtype/tag/tilgjengelighet (explicit toggle rows).
- Approved behavior changes (do not "fix" them back): boligtype/tag unchecking now DIMS on the map (Nedtoning 100 % = hide); eieform is now a multi-select; pris/BRA-i missing values now follow includeUnknown (drops the old DNB special-case); sold map dots now follow the filters too (excluded sold dots dim/hide; passing sold dots keep the separate "Solgt nedtoning" opacity).
- Norwegian UI copy, matching existing style ("Av", "Nullstill filtre", "Inkluder ukjent verdi", "Flere filtre", "Ingen status", "(uten tag)", "N filtre aktive").
- Commit messages: conventional prefixes as used in this repo (`feat(web): …`, `fix(web): …`).

---

### Task 1: `filterstate.js` — shared state module

**Files:**
- Create: `skannonser/web/static/filterstate.js`

**Interfaces:**
- Consumes: nothing (reads/writes localStorage key `skannonser.ui.v1`).
- Produces (all later tasks import from here): the eight bound consts above, `priceBoundOf(meta)`, `defaultFilters(meta)`, `loadFilters(meta)`, `saveFilters(filters)`, `subscribeOtherTabs(cb)`, `activeFilterCount(filters, meta) -> number`, `resetFilters(filters, meta) -> filters` (in-place).

- [ ] **Step 1: Write the module**

```js
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

// Number of ACTIVE filter dimensions (each narrowed slider, each non-empty
// set, facilities as one) — drives the "N filtre aktive" line on both pages.
export function activeFilterCount(filters, meta) {
  let n = 0;
  if (filters.priceMax < priceBoundOf(meta)) n++;
  if (filters.braIMin > 0) n++;
  Object.keys(filters.travelMax || {}).forEach((k) => {
    if (filters.travelMax[k] < TRAVEL_MAX) n++;
  });
  if (filters.soveromMin > 0) n++;
  if (filters.totalprisMax < TOTALPRIS_MAX) n++;
  if (filters.felleskostMax < FELLESKOST_MAX) n++;
  if (filters.byggeaarMin > BYGGEAAR_FLOOR) n++;
  if (filters.totalKvmMax < TOTAL_KVM_MAX) n++;
  if (filters.maanedskostMax < MAANEDSKOST_MAX) n++;
  [
    "boligtypeHidden",
    "tagHidden",
    "energiHidden",
    "eieformHidden",
    "tilgjengelighetHidden",
  ].forEach((k) => {
    if (Object.keys(filters[k] || {}).length) n++;
  });
  if ((filters.postnummerSelected || []).length) n++;
  if ((filters.nabolagSelected || []).length) n++;
  if (Object.keys(filters.facilitiesRequired || {}).length) n++;
  return n;
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
```

- [ ] **Step 2: Syntax check**

Run: `node --check skannonser/web/static/filterstate.js`
Expected: exit 0, no output.

- [ ] **Step 3: Node sanity run of the pure functions** (temp script, not committed)

```bash
node --input-type=module -e "
import('file://' + process.cwd() + '/skannonser/web/static/filterstate.js').then((m) => {
  const meta = { filters: { sheets_max_price: 7500000 }, destinations: [{key:'brj'},{key:'mvv'}], eieformer: ['Aksje','Andel','Selveier'] };
  const d = m.defaultFilters(meta);
  console.assert(d.priceMax === 7500000 && d.travelMax.brj === 120 && d.includeUnknown === true, 'defaults');
  console.assert(m.activeFilterCount(d, meta) === 0, 'count=0 at defaults');
  d.felleskostMax = 5000; d.postnummerSelected = ['2000']; d.energiHidden = {G: true};
  console.assert(m.activeFilterCount(d, meta) === 3, 'count=3');
  m.resetFilters(d, meta);
  console.assert(m.activeFilterCount(d, meta) === 0 && d.includeUnknown === true, 'reset');
  console.log('OK');
});
"
```

Expected: `OK`, no assertion messages. (`loadFilters`/`saveFilters` touch localStorage and are exercised in the browser tasks; the migration branch is additionally covered by the Task 6 checklist.)

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/filterstate.js
git commit -m "feat(web): filterstate.js -- shared filter state, migration, cross-tab subscribe"
```

---

### Task 2: `filters.js` — one predicate, vocab derivation, three reusable components, rebuilt sidebar builders

**Files:**
- Modify: `skannonser/web/static/filters.js` (full rework — the file's current content is shown inline below where kept)

**Interfaces:**
- Consumes: all consts + `priceBoundOf` from `./filterstate.js` (Task 1).
- Produces: `listingExcluded(item, filters, meta) -> bool`, `residualOpacity(ui)` (unchanged), `deriveVocabs(items) -> {postnummer, nabolag, tilgjengelighet, tags}` (each `[{key, label, count}]`), `rangeRow(parent, opts) -> input` (now exported), `checkboxGroup(parent, {label, options, hidden, onChange})`, `searchableMultiSelect(parent, {label, options, selected, onChange})`, `buildMetricFilterUI(container, meta, ui, onChange)`, `buildBoligtypeFilterUI(container, meta, colorByType, filters, onChange)` (4th param is now the FILTERS object, not ui), `buildMoreFiltersUI(container, vocabs, filters, onChange)`.
- Removed exports (Task 3 updates the importer): `defaultFilterState`, `metricDimmed`, `boligtypeHidden`, `TRAVEL_MAX`/`TOTALPRIS_MAX`/`FELLESKOST_MAX` (now from filterstate.js).

- [ ] **Step 1: Rewrite the module header + imports + predicate.** Replace everything from the top of the file down to (and including) the current `boligtypeHidden` function (lines 1–186) with:

```js
// Shared filtering for BOTH pages (2026-07-24 unified-filtering spec):
// one predicate (`listingExcluded`), vocab derivation, and three reusable
// UI components (rangeRow / checkboxGroup / searchableMultiSelect) mounted
// by the map sidebar here and by the table's header popovers
// (tablefilters.js). State shape and bounds live in ./filterstate.js.
//
// Null policy: `filters.includeUnknown` (default true) governs every numeric
// filter and null energimerke/eieform/postnummer/nabolag/facilities.
// Deliberate exceptions: missing TRAVEL minutes never exclude (legacy rule,
// apps_script map.html 3824-3826), and the "" buckets of boligtype/tag/
// tilgjengelighet are explicit toggle rows, not "unknown".

import {
  BRA_I_SLIDER_MAX,
  TRAVEL_MAX,
  TOTALPRIS_MAX,
  FELLESKOST_MAX,
  BYGGEAAR_FLOOR,
  BYGGEAAR_CEIL,
  TOTAL_KVM_MAX,
  MAANEDSKOST_MAX,
  priceBoundOf,
} from "./filterstate.js";

const NOK = new Intl.NumberFormat("nb-NO");

// null/undefined/"" stay null (unknown) instead of coercing to 0 -- the
// filters must distinguish "unknown" from an actual zero.
function numOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function hiddenSetExcludes(set, key) {
  return Boolean(set && set[key]);
}

function selectedSetExcludes(selected, raw, unknownFails) {
  if (!selected || !selected.length) return false;
  if (raw === null || raw === undefined || raw === "") return unknownFails;
  return !selected.includes(String(raw));
}

// THE predicate: true when `item` fails the current filters. Map renders
// excluded items dimmed (hidden at Nedtoning 100 %); table hides their rows.
export function listingExcluded(item, filters, meta) {
  const f = filters;
  const unknownFails = !f.includeUnknown;

  const overMax = (raw, max, ceiling) => {
    if (max >= ceiling) return false; // slider at bound = off
    const v = numOrNull(raw);
    if (v == null) return unknownFails;
    return v > max;
  };
  const underMin = (raw, min, floor) => {
    if (min <= floor) return false;
    const v = numOrNull(raw);
    if (v == null) return unknownFails;
    return v < min;
  };

  // Sliders. NOTE pris/BRA-i now use the same unknown policy as everything
  // else (approved change; drops the old always-dim-Eie / never-dim-DNB
  // special-case for missing values).
  if (overMax(item.pris, f.priceMax, priceBoundOf(meta))) return true;
  if (underMin(item.bra_i, f.braIMin, 0)) return true;
  const travel = item.travel || {};
  for (const key of Object.keys(f.travelMax || {})) {
    const max = f.travelMax[key];
    if (max >= TRAVEL_MAX) continue;
    const mins = numOrNull(travel[key]);
    if (mins == null) continue; // missing travel never excludes (legacy rule)
    if (mins > max) return true;
  }
  if (underMin(item.soverom, f.soveromMin, 0)) return true;
  if (overMax(item.totalpris, f.totalprisMax, TOTALPRIS_MAX)) return true;
  if (overMax(item.felleskost_mnd, f.felleskostMax, FELLESKOST_MAX)) return true;
  if (underMin(item.byggeaar, f.byggeaarMin, BYGGEAAR_FLOOR)) return true;
  if (overMax(item.pris_kvm_totalpris, f.totalKvmMax, TOTAL_KVM_MAX)) return true;
  if (overMax(item.maanedskost, f.maanedskostMax, MAANEDSKOST_MAX)) return true;

  // Hidden sets with explicit "" buckets.
  if (hiddenSetExcludes(f.boligtypeHidden, item.boligtype || "")) return true;
  if (hiddenSetExcludes(f.tagHidden, item.tag ? String(item.tag).trim() : "")) return true;
  if (hiddenSetExcludes(f.tilgjengelighetHidden, item.tilgjengelighet || "")) return true;

  // Hidden sets where null = unknown (governed by includeUnknown once the
  // set is non-empty).
  const energiHidden = f.energiHidden || {};
  if (Object.keys(energiHidden).length) {
    const letter = item.energimerke || null;
    if (letter == null) {
      if (unknownFails) return true;
    } else if (energiHidden[letter]) {
      return true;
    }
  }
  const eieformHidden = f.eieformHidden || {};
  if (Object.keys(eieformHidden).length) {
    const v = item.eieform || null;
    if (v == null) {
      if (unknownFails) return true;
    } else if (eieformHidden[v]) {
      return true;
    }
  }

  // Selected sets (empty = off; non-empty = only these pass).
  if (selectedSetExcludes(f.postnummerSelected, item.postnummer, unknownFails)) return true;
  if (selectedSetExcludes(f.nabolagSelected, item.nabolag, unknownFails)) return true;

  // Required facilities (AND); missing/empty list = unknown as a whole.
  const required = Object.keys(f.facilitiesRequired || {});
  if (required.length) {
    const has = item.facilities;
    if (!Array.isArray(has) || has.length === 0) {
      if (unknownFails) return true;
    } else if (!required.every((r) => has.includes(r))) {
      return true;
    }
  }
  return false;
}

// Vocabularies derived from the loaded listing set (client-side by design --
// zero API changes; sold-only values join when the sold bucket loads).
export function deriveVocabs(items) {
  const post = new Map();
  const nab = new Map();
  const tilg = new Map();
  const tags = new Map();
  const bump = (m, k) => m.set(k, (m.get(k) || 0) + 1);
  items.forEach((it) => {
    if (it.postnummer !== null && it.postnummer !== undefined && it.postnummer !== "") {
      bump(post, String(it.postnummer));
    }
    if (it.nabolag) bump(nab, it.nabolag);
    bump(tilg, it.tilgjengelighet || "");
    bump(tags, it.tag ? String(it.tag).trim() : "");
  });
  const byKey = (m) =>
    [...m.entries()]
      .sort((a, b) => a[0].localeCompare(b[0], "nb"))
      .map(([key, count]) => ({ key, label: key, count }));
  const byCount = (m) =>
    [...m.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "nb"))
      .map(([key, count]) => ({ key, label: key, count }));
  const tilgList = byCount(tilg).map((o) => (o.key === "" ? { ...o, label: "Ingen status" } : o));
  const tagList = byKey(tags).map((o) => (o.key === "" ? { ...o, label: "(uten tag)" } : o));
  return { postnummer: byKey(post), nabolag: byCount(nab), tilgjengelighet: tilgList, tags: tagList };
}
```

- [ ] **Step 2: Keep `residualOpacity` and `rangeRow` (export the latter), keep `shortDest`.** Directly after the Step 1 block, the file continues with the existing `residualOpacity` function (unchanged, lines 188–192 of the old file), then the existing `rangeRow` (old lines 196–230) changed only by adding `export` before `function rangeRow(`, then the existing `const shortDest = …` line.

- [ ] **Step 3: Add the two set components** (after `shortDest`):

```js
// Checkbox group over a small vocabulary, HIDDEN-set semantics: every option
// rendered, checked = visible; unchecking writes {key: true} into `hidden`.
export function checkboxGroup(parent, { label, options, hidden, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "filter-row checkbox-group";
  if (label) {
    const head = document.createElement("div");
    head.className = "filter-head";
    head.textContent = label;
    wrap.appendChild(head);
  }
  options.forEach((opt) => {
    const row = document.createElement("label");
    row.className = "toggle";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !hidden[opt.key];
    cb.addEventListener("change", () => {
      if (cb.checked) delete hidden[opt.key];
      else hidden[opt.key] = true;
      onChange();
    });
    row.appendChild(cb);
    if (opt.swatch) {
      const sw = document.createElement("span");
      sw.className = "legend-swatch";
      sw.style.background = opt.swatch;
      row.appendChild(sw);
    }
    const text = opt.count != null ? `${opt.label} (${opt.count})` : opt.label;
    row.appendChild(document.createTextNode(text));
    wrap.appendChild(row);
  });
  parent.appendChild(wrap);
  return wrap;
}

// Search box + checkbox list over a large vocabulary, SELECTED-set semantics:
// `selected` is an ARRAY mutated in place; empty = filter off; checking a
// value means "only the checked values pass".
export function searchableMultiSelect(parent, { label, options, selected, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "filter-row search-multi";
  const head = document.createElement("div");
  head.className = "filter-head";
  const name = document.createElement("span");
  name.textContent = label;
  const val = document.createElement("span");
  val.className = "filter-val";
  head.appendChild(name);
  head.appendChild(val);
  wrap.appendChild(head);

  const search = document.createElement("input");
  search.type = "text";
  search.placeholder = "Søk …";
  search.className = "multi-search";
  wrap.appendChild(search);

  const list = document.createElement("div");
  list.className = "multi-list";
  wrap.appendChild(list);

  const clear = document.createElement("button");
  clear.type = "button";
  clear.className = "multi-clear";
  clear.textContent = "Tøm";
  wrap.appendChild(clear);

  const paintHead = () => {
    val.textContent = selected.length ? selected.length + " valgt" : "Av";
    clear.hidden = !selected.length;
  };
  const render = () => {
    const q = search.value.trim().toLowerCase();
    list.innerHTML = "";
    options
      .filter((o) => !q || o.label.toLowerCase().includes(q))
      .forEach((opt) => {
        const row = document.createElement("label");
        row.className = "toggle";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = selected.includes(opt.key);
        cb.addEventListener("change", () => {
          const i = selected.indexOf(opt.key);
          if (cb.checked && i === -1) selected.push(opt.key);
          if (!cb.checked && i !== -1) selected.splice(i, 1);
          paintHead();
          onChange();
        });
        row.appendChild(cb);
        row.appendChild(document.createTextNode(`${opt.label} (${opt.count})`));
        list.appendChild(row);
      });
  };
  search.addEventListener("input", render);
  clear.addEventListener("click", () => {
    selected.splice(0, selected.length);
    paintHead();
    render();
    onChange();
  });
  paintHead();
  render();
  parent.appendChild(wrap);
  return wrap;
}
```

- [ ] **Step 4: Rebuild the sidebar builders.** Replace the current `buildMetricFilterUI` and `buildBoligtypeFilterUI` entirely, and add `buildMoreFiltersUI`:

`buildMetricFilterUI(container, meta, ui, onChange)` — keeps its signature (`ui` still carries the map-only `dimIntensity`/`soldDim` plus `ui.filters`). Contents, in order:
1. The five existing sliders (Maks pris / Min BRA-i / Maks totalpris / Maks felleskost/mnd / Min soverom) — code unchanged from the current file (old lines 238–301).
2. Three NEW sliders right after "Min soverom", built with the same `rangeRow` pattern:

```js
  rangeRow(container, {
    label: "Min byggeår",
    min: BYGGEAAR_FLOOR,
    max: BYGGEAAR_CEIL,
    step: 1,
    value: ui.filters.byggeaarMin,
    fmt: (v) => (v <= BYGGEAAR_FLOOR ? "Av" : "≥ " + v),
    onInput: (v) => {
      ui.filters.byggeaarMin = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Maks total/kvm",
    min: 0,
    max: TOTAL_KVM_MAX,
    step: 1000,
    value: ui.filters.totalKvmMax,
    fmt: (v) => (v >= TOTAL_KVM_MAX ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.totalKvmMax = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Maks mnd-kost",
    min: 0,
    max: MAANEDSKOST_MAX,
    step: 250,
    value: ui.filters.maanedskostMax,
    fmt: (v) => (v >= MAANEDSKOST_MAX ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.maanedskostMax = v;
      onChange();
    },
  });
```

3. The travel sliders + Nedtoning + Solgt nedtoning — unchanged (old lines 303–342).
4. The eieform single-select, energi block, and facilities block are REPLACED by the shared component (approved multi-select change):

```js
  checkboxGroup(container, {
    label: "Eieform",
    options: (meta.eieformer || []).map((v) => ({ key: v, label: v })),
    hidden: ui.filters.eieformHidden,
    onChange,
  });

  if ((meta.energimerker || []).length) {
    checkboxGroup(container, {
      label: "Energimerking",
      options: (meta.energimerker || []).map((v) => ({ key: v, label: v })),
      hidden: ui.filters.energiHidden,
      onChange,
    });
  }
```

5. The facilities block keeps its REQUIRED (not hidden) semantics — it stays hand-rolled exactly as in the current file (old lines 393–416), unchanged.
6. The "Inkluder ukjent verdi" toggle — unchanged (old lines 419–430).

`buildBoligtypeFilterUI(container, meta, colorByType, filters, onChange)` — 4th param renamed from `ui` to `filters`; body becomes:

```js
export function buildBoligtypeFilterUI(container, meta, colorByType, filters, onChange) {
  container.innerHTML = "";
  checkboxGroup(container, {
    options: [
      ...(meta.boligtyper || []).map((t) => ({
        key: t,
        label: t,
        swatch: (colorByType && colorByType[t]) || "#6f7e76",
      })),
      { key: "", label: "Ukjent boligtype", swatch: (colorByType && colorByType[""]) || "#6f7e76" },
    ],
    hidden: filters.boligtypeHidden,
    onChange,
  });
}
```

`buildMoreFiltersUI(container, vocabs, filters, onChange)` — NEW:

```js
// The "Flere filtre" sidebar panel: status + place vocabularies derived from
// the loaded listings (deriveVocabs), same components the table popovers use.
export function buildMoreFiltersUI(container, vocabs, filters, onChange) {
  container.innerHTML = "";
  container.classList.remove("muted");
  checkboxGroup(container, {
    label: "Tilgjengelighet",
    options: vocabs.tilgjengelighet,
    hidden: filters.tilgjengelighetHidden,
    onChange,
  });
  searchableMultiSelect(container, {
    label: "Postnummer",
    options: vocabs.postnummer,
    selected: filters.postnummerSelected,
    onChange,
  });
  searchableMultiSelect(container, {
    label: "Nabolag",
    options: vocabs.nabolag,
    selected: filters.nabolagSelected,
    onChange,
  });
}
```

- [ ] **Step 5: Syntax check**

Run: `node --check skannonser/web/static/filters.js`
Expected: exit 0. (app.js still imports the removed names at this point — that breakage is Task 3's first step; `node --check` is per-file so it does not fail here.)

- [ ] **Step 6: Node spot-checks of the predicate** (temp, not committed)

```bash
node --input-type=module -e "
Promise.all([
  import('file://' + process.cwd() + '/skannonser/web/static/filterstate.js'),
  import('file://' + process.cwd() + '/skannonser/web/static/filters.js'),
]).then(([fs, f]) => {
  const meta = { filters: { sheets_max_price: 7500000 }, destinations: [{key:'brj'}], eieformer: ['Andel','Selveier'] };
  const d = () => fs.defaultFilters(meta);
  const item = { pris: 3000000, bra_i: 80, travel: {brj: 40}, soverom: 2, totalpris: 3100000,
                 felleskost_mnd: 4000, byggeaar: 1980, pris_kvm_totalpris: 38750, maanedskost: 4200,
                 boligtype: 'Leilighet', eieform: 'Andel', energimerke: 'C', tilgjengelighet: null,
                 postnummer: '2000', nabolag: 'Sentrum', facilities: ['Heis'], tag: null, source: 'eie' };
  const ex = (over) => f.listingExcluded(item, Object.assign(d(), over), meta);
  console.assert(ex({}) === false, 'defaults pass');
  console.assert(ex({ felleskostMax: 3000 }) === true, 'felleskost excl');
  console.assert(ex({ byggeaarMin: 1990 }) === true, 'byggeaar excl');
  console.assert(ex({ postnummerSelected: ['2000'] }) === false, 'postnr pass');
  console.assert(ex({ postnummerSelected: ['0181'] }) === true, 'postnr excl');
  console.assert(ex({ nabolagSelected: ['Sentrum'] }) === false, 'nabolag pass');
  console.assert(ex({ tilgjengelighetHidden: {'': true} }) === true, 'ingen-status bucket excl');
  console.assert(ex({ eieformHidden: {Selveier: true} }) === false, 'eieform other hidden -> pass');
  console.assert(ex({ eieformHidden: {Andel: true} }) === true, 'eieform hidden excl');
  const nullPris = { ...item, pris: null };
  console.assert(f.listingExcluded(nullPris, Object.assign(d(), { priceMax: 2000000 }), meta) === false, 'null pris + includeUnknown passes');
  console.assert(f.listingExcluded(nullPris, Object.assign(d(), { priceMax: 2000000, includeUnknown: false }), meta) === true, 'null pris + !includeUnknown fails');
  const noTravel = { ...item, travel: {} };
  console.assert(f.listingExcluded(noTravel, Object.assign(d(), { travelMax: {brj: 30}, includeUnknown: false }), meta) === false, 'missing travel NEVER excludes');
  const v = f.deriveVocabs([item, { ...item, postnummer: '2000' }, { ...item, postnummer: '0181', tag: 'hot' }]);
  console.assert(v.postnummer[0].key === '0181' && v.postnummer[1].count === 2, 'vocab counts');
  console.assert(v.tilgjengelighet[0].label === 'Ingen status', 'ingen-status label');
  console.log('OK');
});
"
```

Expected: `OK` with no assertion output.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/filters.js
git commit -m "feat(web): listingExcluded predicate + vocab derivation + shared filter components"
```

---

### Task 3: Map wiring — app.js + index.html

**Files:**
- Modify: `skannonser/web/static/app.js`
- Modify: `skannonser/web/static/index.html`

**Interfaces:**
- Consumes: filterstate.js (`defaultFilters`, `loadFilters`, `activeFilterCount`, `subscribeOtherTabs`, `resetFilters`), filters.js (`listingExcluded`, `residualOpacity`, `buildMetricFilterUI`, `buildBoligtypeFilterUI`, `buildMoreFiltersUI`, `deriveVocabs`).
- Produces: nothing new for later tasks; the map now renders exclusively off the shared state.

- [ ] **Step 1: index.html additions.** Inside the `#filter-panel` `<details>`, directly before `<div id="metric-filters" …>`, add:

```html
        <div id="active-filters" class="muted active-filters"></div>
```

Directly after the `#filter-panel` `</details>`, add a new panel:

```html
      <details class="panel" id="more-filters-panel" open>
        <summary>Flere filtre</summary>
        <div id="more-filters" class="muted">Laster …</div>
        <button type="button" id="reset-filters" class="reset-filters">Nullstill filtre</button>
      </details>
```

- [ ] **Step 2: app.js imports.** Replace the `./filters.js` import block (current lines 22–29) with:

```js
import {
  listingExcluded,
  residualOpacity,
  buildMetricFilterUI,
  buildBoligtypeFilterUI,
  buildMoreFiltersUI,
  deriveVocabs,
} from "./filters.js";
import {
  defaultFilters,
  loadFilters,
  activeFilterCount,
  subscribeOtherTabs,
  resetFilters,
} from "./filterstate.js";
```

- [ ] **Step 3: `defaultUi`.** Change `filters: defaultFilterState(meta),` to `filters: defaultFilters(meta),` and DELETE the two lines `boligtypeHidden: {},` and `tagHidden: {},` (they live inside `filters` now).

- [ ] **Step 4: `loadUi`.** Replace the whole `filters:` merge expression (current lines 105–111) with `filters: loadFilters(meta),` and delete the `boligtypeHidden: …` / `tagHidden: …` root merge lines (112–113). After the `soldDimNudged` block and before `return ui;`, add:

```js
      // Legacy roots migrated into filters by loadFilters -- strip them so
      // saveUi can never re-persist the old shape.
      delete ui.boligtypeHidden;
      delete ui.tagHidden;
```

- [ ] **Step 5: predicate swap.** In `isDimmed` (line 160): `if (metricDimmed(item, state.ui, state.meta)) return true;` → `if (listingExcluded(item, state.ui.filters, state.meta)) return true;`.
In `featureCollectionsByGroup` (lines 221–229), replace the loop body's filter/opacity section:

```js
  const hideExcluded = Number(state.ui.dimIntensity) >= 100;
  state.itemsById.forEach((item) => {
    if (item.lat == null || item.lng == null) return;
    if (!state.ui[bucketOf(item)]) return; // layer toggle (eie/dnb/sold)
    const excluded = isDimmed(item, ctx);
    // Nedtoning at 100 % = today's hard-hide (incl. cluster counts).
    if (excluded && hideExcluded) return;
    const gid = groupIdForItem(item, state.validGroupIds, state.ui.combineSold);
    if (!byGroup[gid]) return; // safety: no source for this group
    // Sold dots follow the filters too now (approved change): excluded ->
    // filter dim; passing sold dots keep the separate "Solgt nedtoning".
    const op = excluded ? residual : item.sold ? soldOpacity : 1;
    byGroup[gid].push(itemToFeature(item, op));
  });
```

(The `hideExcluded` const goes right above the `state.itemsById.forEach`; the old `boligtypeHidden(...)` and `state.ui.tagHidden[...]` early-returns are deleted.)

- [ ] **Step 6: tag + boligtype writers.** In `buildTagFilterUI`, replace both `state.ui.tagHidden` reads/writes with `state.ui.filters.tagHidden` (three places: `cb.checked = !state.ui.filters.tagHidden[key]`, the delete, the set) and route the change through the shared handler from Step 7 (`onFilterChange()` instead of `saveUi(); applyAll();`). In `init`, the `buildBoligtypeFilterUI(...)` call's 4th argument changes from `state.ui` to `state.ui.filters`.

- [ ] **Step 7: shared change handler, active line, more-filters panel, reset, cross-tab.** In `init`, replace the two `() => { saveUi(); applyAll(); }` callbacks and add the new wiring. Insert these functions above `init`:

```js
function renderActiveFilterLine() {
  const node = document.getElementById("active-filters");
  if (!node) return;
  const n = activeFilterCount(state.ui.filters, state.meta);
  node.textContent = n ? n + " filtre aktive" : "Ingen aktive filtre";
}

function onFilterChange() {
  saveUi();
  renderActiveFilterLine();
  applyAll();
}

// (Re)build every filter control that renders shared state -- used at init,
// after reset, and when another tab changes the filters.
function rebuildFilterUIs() {
  buildBoligtypeFilterUI(
    document.getElementById("boligtype-filter"),
    state.meta,
    { ...state.colorByType, "": DEFAULT_UNKNOWN_TYPE_COLOR },
    state.ui.filters,
    onFilterChange
  );
  buildMetricFilterUI(
    document.getElementById("metric-filters"),
    state.meta,
    state.ui,
    onFilterChange
  );
  buildMoreFiltersUI(
    document.getElementById("more-filters"),
    deriveVocabs([...state.itemsById.values()]),
    state.ui.filters,
    onFilterChange
  );
  buildTagFilterUI();
  renderActiveFilterLine();
}
```

In `init`, replace the existing `buildBoligtypeFilterUI(...)` and `buildMetricFilterUI(...)` calls (and their inline callbacks) with a single `rebuildFilterUIs();` (placed where `buildBoligtypeFilterUI` is today, after `state.groups` is set — note `renderSourceLegend()` stays), and after `wireDrawer();` add:

```js
  const resetBtn = document.getElementById("reset-filters");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      resetFilters(state.ui.filters, state.meta);
      rebuildFilterUIs();
      onFilterChange();
    });
  }
  // Live cross-tab sync: another tab (e.g. the table) changed the filters.
  subscribeOtherTabs(() => {
    state.ui.filters = loadFilters(state.meta);
    rebuildFilterUIs();
    applyAll();
  });
```

In `ensureSoldLoaded`'s success path, replace `buildTagFilterUI();` with `rebuildFilterUIs();` (sold items can add tags AND grow the postnummer/nabolag/tilgjengelighet vocabularies). The `sk-annotation-saved` listener keeps calling `buildTagFilterUI()` + `applyAll()` — but `buildTagFilterUI` must not double-bind; it already fully rebuilds its container, unchanged behavior.

- [ ] **Step 8: Syntax check both files**

Run: `node --check skannonser/web/static/app.js`
Expected: exit 0. (index.html has no checker; reviewed by eye + browser.)

- [ ] **Step 9: Commit** (controller browser-verifies after this task lands)

```bash
git add skannonser/web/static/app.js skannonser/web/static/index.html
git commit -m "feat(web): map renders off shared filter state -- unified predicate, Flere filtre panel, reset + cross-tab sync"
```

---

### Task 4: `tablefilters.js` + popover styles

**Files:**
- Create: `skannonser/web/static/tablefilters.js`
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: filterstate.js consts + `priceBoundOf`; filters.js `rangeRow`, `checkboxGroup`, `searchableMultiSelect`.
- Produces (Task 5 imports): `COLUMN_FILTERS` (descriptor map), `isColumnFilterActive(colKey, ctx) -> bool`, `makeFilterButton(colKey, ctx) -> HTMLButtonElement | null`, `openFacilitiesPopover(anchorEl, ctx)`, `closePopover()`. `ctx = { filters, meta, vocabs, onChange }`.

- [ ] **Step 1: Write the module**

```js
// skannonser/web/static/tablefilters.js
// Notion-style column-header filter popovers for the table page
// (2026-07-24 unified-filtering spec). Pure UI: state lives in the shared
// filters object (filterstate.js), controls are the same components the map
// sidebar mounts (filters.js) -- one implementation, two mounts.

import {
  BRA_I_SLIDER_MAX,
  TRAVEL_MAX,
  TOTALPRIS_MAX,
  FELLESKOST_MAX,
  BYGGEAAR_FLOOR,
  BYGGEAAR_CEIL,
  TOTAL_KVM_MAX,
  MAANEDSKOST_MAX,
  priceBoundOf,
} from "./filterstate.js";
import { rangeRow, checkboxGroup, searchableMultiSelect } from "./filters.js";

const NOK = new Intl.NumberFormat("nb-NO");
const fmtKr = (bound) => (v) => (v >= bound ? "Av" : NOK.format(v) + " kr");

// Column key -> filter descriptor. `kind` picks the popover body; stateKey
// addresses the shared filters object. Travel columns map onto
// filters.travelMax[destKey]. Vocab sources: "meta:<key>" reads ctx.meta,
// "vocab:<key>" reads ctx.vocabs (deriveVocabs output).
export const COLUMN_FILTERS = {
  pris: { kind: "slider-max", stateKey: "priceMax", bound: (ctx) => priceBoundOf(ctx.meta), step: 50000, fmt: "kr" },
  totalpris: { kind: "slider-max", stateKey: "totalprisMax", bound: () => TOTALPRIS_MAX, step: 100000, fmt: "kr" },
  pris_kvm_totalpris: { kind: "slider-max", stateKey: "totalKvmMax", bound: () => TOTAL_KVM_MAX, step: 1000, fmt: "kr" },
  felleskost_mnd: { kind: "slider-max", stateKey: "felleskostMax", bound: () => FELLESKOST_MAX, step: 250, fmt: "kr" },
  maanedskost: { kind: "slider-max", stateKey: "maanedskostMax", bound: () => MAANEDSKOST_MAX, step: 250, fmt: "kr" },
  bra_i: { kind: "slider-min", stateKey: "braIMin", floor: 0, max: BRA_I_SLIDER_MAX, step: 5, suffix: " m²" },
  soverom: { kind: "slider-min", stateKey: "soveromMin", floor: 0, max: 6, step: 1, suffix: "" },
  byggeaar: { kind: "slider-min", stateKey: "byggeaarMin", floor: BYGGEAAR_FLOOR, max: BYGGEAAR_CEIL, step: 1, suffix: "" },
  brj: { kind: "slider-travel", destKey: "brj" },
  mvv: { kind: "slider-travel", destKey: "mvv" },
  mvv_uni: { kind: "slider-travel", destKey: "mvv_uni" },
  boligtype: { kind: "set", stateKey: "boligtypeHidden", vocab: "meta:boligtyper", unknownBucket: "Ukjent boligtype" },
  eieform: { kind: "set", stateKey: "eieformHidden", vocab: "meta:eieformer" },
  energimerke: { kind: "set", stateKey: "energiHidden", vocab: "meta:energimerker" },
  tilgjengelighet: { kind: "set", stateKey: "tilgjengelighetHidden", vocab: "vocab:tilgjengelighet" },
  tag: { kind: "set", stateKey: "tagHidden", vocab: "vocab:tags" },
  postnummer: { kind: "search-set", stateKey: "postnummerSelected", vocab: "vocab:postnummer" },
  nabolag: { kind: "search-set", stateKey: "nabolagSelected", vocab: "vocab:nabolag" },
};

function vocabOptions(desc, ctx) {
  const [src, key] = desc.vocab.split(":");
  if (src === "meta") {
    const options = (ctx.meta[key] || []).map((v) => ({ key: v, label: v }));
    if (desc.unknownBucket) options.push({ key: "", label: desc.unknownBucket });
    return options;
  }
  return ctx.vocabs[key] || [];
}

export function isColumnFilterActive(colKey, ctx) {
  const desc = COLUMN_FILTERS[colKey];
  if (!desc) return false;
  const f = ctx.filters;
  switch (desc.kind) {
    case "slider-max":
      return f[desc.stateKey] < desc.bound(ctx);
    case "slider-min":
      return f[desc.stateKey] > desc.floor;
    case "slider-travel":
      return (f.travelMax[desc.destKey] ?? TRAVEL_MAX) < TRAVEL_MAX;
    case "set":
      return Object.keys(f[desc.stateKey] || {}).length > 0;
    case "search-set":
      return (f[desc.stateKey] || []).length > 0;
    default:
      return false;
  }
}

// --- popover singleton ---

let popoverEl = null;
let popoverAnchor = null;

export function closePopover() {
  if (popoverEl) popoverEl.remove();
  popoverEl = null;
  popoverAnchor = null;
}

function placePopover(pop, anchor) {
  const r = anchor.getBoundingClientRect();
  // position:fixed -> viewport coords; clamp horizontally.
  pop.style.top = r.bottom + 4 + "px";
  const width = Math.min(280, window.innerWidth - 16);
  pop.style.width = width + "px";
  pop.style.left = Math.max(8, Math.min(r.left, window.innerWidth - width - 8)) + "px";
}

function openPopover(anchor, build) {
  if (popoverAnchor === anchor) {
    closePopover(); // toggling the same funnel closes it
    return;
  }
  closePopover();
  popoverEl = document.createElement("div");
  popoverEl.className = "th-popover";
  build(popoverEl);
  document.body.appendChild(popoverEl);
  placePopover(popoverEl, anchor);
  popoverAnchor = anchor;
}

// One document-level dismiss wiring (idempotent via module init).
document.addEventListener("click", (ev) => {
  if (!popoverEl) return;
  if (popoverEl.contains(ev.target)) return;
  if (popoverAnchor && popoverAnchor.contains(ev.target)) return;
  closePopover();
});
document.addEventListener("keydown", (ev) => {
  if (ev.key === "Escape") closePopover();
});

function buildBody(pop, desc, ctx) {
  const f = ctx.filters;
  switch (desc.kind) {
    case "slider-max": {
      const bound = desc.bound(ctx);
      rangeRow(pop, {
        label: "Maks",
        min: 0,
        max: bound,
        step: desc.step,
        value: f[desc.stateKey],
        fmt: fmtKr(bound),
        onInput: (v) => {
          f[desc.stateKey] = v;
          ctx.onChange();
        },
      });
      break;
    }
    case "slider-min": {
      rangeRow(pop, {
        label: "Min",
        min: desc.floor,
        max: desc.max,
        step: desc.step,
        value: f[desc.stateKey],
        fmt: (v) => (v <= desc.floor ? "Av" : "≥ " + v + desc.suffix),
        onInput: (v) => {
          f[desc.stateKey] = v;
          ctx.onChange();
        },
      });
      break;
    }
    case "slider-travel": {
      rangeRow(pop, {
        label: "Maks reisetid",
        min: 0,
        max: TRAVEL_MAX,
        step: 1,
        value: f.travelMax[desc.destKey] ?? TRAVEL_MAX,
        fmt: (v) => (v >= TRAVEL_MAX ? "Av" : "≤ " + v + " min"),
        onInput: (v) => {
          f.travelMax[desc.destKey] = v;
          ctx.onChange();
        },
      });
      break;
    }
    case "set": {
      checkboxGroup(pop, {
        options: vocabOptions(desc, ctx),
        hidden: f[desc.stateKey],
        onChange: ctx.onChange,
      });
      break;
    }
    case "search-set": {
      searchableMultiSelect(pop, {
        label: "Vis kun",
        options: vocabOptions(desc, ctx),
        selected: f[desc.stateKey],
        onChange: ctx.onChange,
      });
      break;
    }
  }
}

// Funnel button for a column header, or null when the column has no filter.
export function makeFilterButton(colKey, ctx) {
  const desc = COLUMN_FILTERS[colKey];
  if (!desc) return null;
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "th-filter";
  btn.setAttribute("aria-label", "Filtrer kolonne");
  btn.innerHTML =
    '<svg viewBox="0 0 12 12" width="11" height="11" aria-hidden="true">' +
    '<path d="M1 2h10L7.5 6.5V10l-3 1V6.5Z" fill="currentColor"/></svg>';
  if (isColumnFilterActive(colKey, ctx)) btn.classList.add("active");
  btn.addEventListener("click", (ev) => {
    ev.stopPropagation(); // never trigger the header's sort
    openPopover(btn, (pop) => buildBody(pop, desc, ctx));
  });
  return btn;
}

// Toolbar "Fasiliteter" popover (no natural column) -- required-set semantics.
export function openFacilitiesPopover(anchorEl, ctx) {
  openPopover(anchorEl, (pop) => {
    checkboxGroup(pop, {
      label: "Må ha fasiliteter",
      options: (ctx.meta.facilities || []).map((o) => ({
        key: o.name,
        label: o.name,
        count: o.count,
      })),
      hidden: null, // not used -- see below
      onChange: ctx.onChange,
    });
  });
}
```

**Correction to the facilities popover** (checkboxGroup is hidden-set; facilities is required-set): instead of the broken `hidden: null` call above, `openFacilitiesPopover` builds its rows inline with required-semantics — replace its body with:

```js
export function openFacilitiesPopover(anchorEl, ctx) {
  openPopover(anchorEl, (pop) => {
    const wrap = document.createElement("div");
    wrap.className = "filter-row checkbox-group";
    const head = document.createElement("div");
    head.className = "filter-head";
    head.textContent = "Må ha fasiliteter";
    wrap.appendChild(head);
    (ctx.meta.facilities || []).forEach((o) => {
      const row = document.createElement("label");
      row.className = "toggle";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = Boolean(ctx.filters.facilitiesRequired[o.name]);
      cb.addEventListener("change", () => {
        if (cb.checked) ctx.filters.facilitiesRequired[o.name] = true;
        else delete ctx.filters.facilitiesRequired[o.name];
        ctx.onChange();
      });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(`${o.name} (${o.count})`));
      wrap.appendChild(row);
    });
    pop.appendChild(wrap);
  });
}
```

- [ ] **Step 2: Append styles to `skannonser/web/static/style.css`**

```css
/* --- table header filters + popovers (unified filtering, 2026-07-24) --- */
.th-filter {
  background: none;
  border: 0;
  padding: 2px 3px;
  margin-left: 4px;
  cursor: pointer;
  color: var(--muted, #8a938c);
  vertical-align: middle;
}
.th-filter:hover { color: var(--text, #222); }
.th-filter.active { color: var(--accent, #2f6f4f); }
#listings-table th.filter-active { background: rgba(47, 111, 79, 0.08); }
.th-popover {
  position: fixed;
  z-index: 60;
  background: var(--panel-bg, #fff);
  border: 1px solid rgba(0, 0, 0, 0.15);
  border-radius: 6px;
  box-shadow: 0 6px 24px rgba(0, 0, 0, 0.18);
  padding: 10px 12px;
  max-height: 60vh;
  overflow-y: auto;
  font-size: 13px;
}
.multi-search { width: 100%; margin: 4px 0 6px; padding: 4px 6px; }
.multi-list { max-height: 200px; overflow-y: auto; }
.multi-clear { margin-top: 6px; font-size: 12px; cursor: pointer; }
.active-filters { margin: 4px 0 8px; font-size: 12px; }
.reset-filters { margin-top: 8px; font-size: 12px; cursor: pointer; }
.toolbar-filter-btn { font-size: 13px; cursor: pointer; }
@media (max-width: 480px) {
  .th-popover { left: 8px !important; right: 8px; width: auto !important; }
}
```

- [ ] **Step 3: Syntax check**

Run: `node --check skannonser/web/static/tablefilters.js`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/tablefilters.js skannonser/web/static/style.css
git commit -m "feat(web): header-filter popover machinery + styles (tablefilters.js)"
```

---

### Task 5: Table wiring — table.js + table.html

**Files:**
- Modify: `skannonser/web/static/table.js`
- Modify: `skannonser/web/static/table.html`

**Interfaces:**
- Consumes: filterstate.js (`loadFilters`, `saveFilters`, `activeFilterCount`, `subscribeOtherTabs`, `resetFilters`), filters.js (`listingExcluded`, `deriveVocabs`), tablefilters.js (`COLUMN_FILTERS`, `isColumnFilterActive`, `makeFilterButton`, `openFacilitiesPopover`, `closePopover`).

- [ ] **Step 1: table.html toolbar.** Replace the `.table-toolbar` div content with:

```html
    <div class="table-toolbar">
      <input type="text" id="table-filter" class="table-filter-input"
             placeholder="Filtrer adresse, postnummer, boligtype …">
      <label class="toggle"><input type="checkbox" id="table-sold"> Vis solgte</label>
      <button type="button" id="facilities-filter-btn" class="toolbar-filter-btn">Fasiliteter</button>
      <label class="toggle"><input type="checkbox" id="table-include-unknown"> Inkluder ukjent verdi</label>
      <button type="button" id="table-reset-filters" class="toolbar-filter-btn">Nullstill filtre</button>
      <span id="table-status" class="muted"></span>
    </div>
```

- [ ] **Step 2: table.js imports + state.** After the existing imports add:

```js
import { listingExcluded, deriveVocabs } from "./filters.js";
import {
  loadFilters,
  saveFilters,
  activeFilterCount,
  subscribeOtherTabs,
  resetFilters,
} from "./filterstate.js";
import {
  COLUMN_FILTERS,
  isColumnFilterActive,
  makeFilterButton,
  openFacilitiesPopover,
  closePopover,
} from "./tablefilters.js";
```

Extend the `state` object with `meta: null, filters: null, vocabs: null,` and add below it:

```js
function filterCtx() {
  return {
    filters: state.filters,
    meta: state.meta,
    vocabs: state.vocabs,
    onChange: onFilterChange,
  };
}

function onFilterChange() {
  saveFilters(state.filters);
  render();
}

function refreshVocabs() {
  state.vocabs = deriveVocabs(state.items);
}
```

- [ ] **Step 3: `visibleRows` applies the shared predicate.** Replace the filter expression inside `visibleRows`:

```js
function visibleRows() {
  const filtered = state.items.filter((item) => {
    if (!state.showSold && item.sold) return false;
    if (listingExcluded(item, state.filters, state.meta)) return false;
    return matchesFilter(item, state.filterText);
  });
  filtered.sort((a, b) => compareItems(a, b, state.sortKey, state.sortDir));
  return filtered;
}
```

- [ ] **Step 4: `renderHead` gains funnel buttons + active tint.** In the `COLUMNS.forEach` loop, after the sort wiring and before `row.appendChild(th)`:

```js
    const filterBtn = makeFilterButton(col.key, filterCtx());
    if (filterBtn) {
      th.appendChild(filterBtn);
      if (isColumnFilterActive(col.key, filterCtx())) th.classList.add("filter-active");
    }
```

- [ ] **Step 5: status line.** In `render()`, replace the `setStatus(...)` call:

```js
  const n = activeFilterCount(state.filters, state.meta);
  setStatus(
    rows.length + " av " + state.items.length + " annonser" +
    (n ? " · " + n + " filtre aktive" : "")
  );
```

- [ ] **Step 6: toolbar wiring.** In `wireToolbar()` append:

```js
  const facBtn = document.getElementById("facilities-filter-btn");
  if (facBtn) {
    facBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openFacilitiesPopover(facBtn, filterCtx());
    });
  }

  const unk = document.getElementById("table-include-unknown");
  if (unk) {
    unk.checked = state.filters.includeUnknown !== false;
    unk.addEventListener("change", () => {
      state.filters.includeUnknown = unk.checked;
      onFilterChange();
    });
  }

  const reset = document.getElementById("table-reset-filters");
  if (reset) {
    reset.addEventListener("click", () => {
      resetFilters(state.filters, state.meta);
      if (unk) unk.checked = state.filters.includeUnknown !== false;
      closePopover();
      onFilterChange();
    });
  }
```

(Note: `wireToolbar` must therefore run AFTER `state.filters` exists — see Step 7's init ordering.)

- [ ] **Step 7: init loads meta + filters + vocabs, subscribes cross-tab.** Replace `init()`:

```js
async function init() {
  setStatus("Laster …");
  try {
    const [meta, items] = await Promise.all([
      fetch("/api/meta").then((r) => {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      }),
      fetchListings(0),
    ]);
    state.meta = meta;
    state.filters = loadFilters(meta);
    state.items = items;
  } catch (err) {
    setStatus("Kunne ikke laste data: " + err.message);
    return;
  }
  refreshVocabs();
  wireToolbar();
  const soldToggle = document.getElementById("table-sold");
  if (soldToggle.checked) {
    try {
      state.items = state.items.concat(await fetchListings(1));
      state.soldLoaded = true;
      state.showSold = true;
      refreshVocabs();
    } catch (_) {
      /* fall through with just the non-sold rows loaded */
    }
  }
  // Live cross-tab sync: the map (or another table tab) changed the filters.
  subscribeOtherTabs(() => {
    state.filters = loadFilters(state.meta);
    closePopover();
    const unk = document.getElementById("table-include-unknown");
    if (unk) unk.checked = state.filters.includeUnknown !== false;
    render();
  });
  render();
}
```

Also: in the sold-toggle `change` handler inside `wireToolbar`, after `state.soldLoaded = true;` add `refreshVocabs();` (sold rows grow the vocabularies).

- [ ] **Step 8: Syntax check**

Run: `node --check skannonser/web/static/table.js`
Expected: exit 0.

- [ ] **Step 9: Commit** (controller browser-verifies after this task)

```bash
git add skannonser/web/static/table.js skannonser/web/static/table.html
git commit -m "feat(web): Notion-style header filters on the table, wired to shared state"
```

---

### Task 6: Full verification + README

**Files:**
- Modify: `README.md` (web bullet: unified filtering; drop/adjust the sentence describing map-only filters if it contradicts)

- [ ] **Step 1: pytest suite (proves zero backend impact)**

Run: `.venv/bin/pytest tests/rebuild -q`
Expected: 616 passed, zero warnings.

- [ ] **Step 2: Controller browser verification** (spec §8 checklist, against the details-populated dev DB):
  1. Table: each popover kind works — slider (Totalpris), set (Energi), search-set (Postnummer: search, select 2, "Tøm") — rows filter immediately; funnel fills + header tints.
  2. Map reflects the same filters (dimmed dots); Nedtoning 100 % fully hides incl. cluster counts.
  3. Boligtype/tag unchecking dims (not hides) at default Nedtoning; eieform is a multi-select in the sidebar.
  4. Null-pris listing: dims only when "Inkluder ukjent verdi" is off while Maks pris is narrowed (approved pris/BRA-i change).
  5. Cross-tab: map and table in two tabs; narrowing felleskost in the table dims dots in the map tab without reload, and vice versa; active-filter counts match.
  6. Legacy migration: seed localStorage with a pre-change blob (root `boligtypeHidden: {"Enebolig": true}`, `filters.eieform: "Andel"`), reload → Enebolig unchecked in sidebar, eieform shows only Andel checked, and re-saving strips the legacy root keys from the stored blob.
  7. "Nullstill filtre" on either page clears everything except "Inkluder ukjent verdi"; both pages agree after sync.
  8. Mobile width (375px): popover clamps to viewport; sidebar drawer still works.
  9. No console errors on either page throughout.

- [ ] **Step 3: README.** Update the `web/` architecture bullet: the filter description becomes "shared map+table filtering (one predicate, localStorage-synced across tabs): sliders for continuous values, header-popover checkbox/search filters on the table, sidebar panels on the map; filtered-out listings dim on the map (hide at 100 % nedtoning) and hide in the table". Remove any now-false claims (e.g. that the table only has a text filter).

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: unified map/table filtering -- README web bullet"
```

---

## Self-review notes (already applied)

- **Spec coverage:** §2 state/migration → Task 1; §3 predicate incl. the three approved behavior changes + sold-follows-filters refinement → Tasks 2–3; §4 popovers → Tasks 4–5; §5 sidebar additions → Tasks 2–3; §6 client-side vocabs → Task 2 (`deriveVocabs`) consumed in 3 & 5; §7 cross-tab → Tasks 3 & 5; §8 verification → Task 6 (+ node spot-checks in 1–2).
- **Type consistency:** `ctx = {filters, meta, vocabs, onChange}` is identical in Tasks 4 and 5; builder signatures (`buildBoligtypeFilterUI` 4th param = filters) match between Tasks 2 and 3; `vocabs` keys (`postnummer`, `nabolag`, `tilgjengelighet`, `tags`) match `COLUMN_FILTERS`' `vocab:` references.
- **Known sequencing hazard:** after Task 2, app.js imports names filters.js no longer exports — the map page is broken until Task 3 lands. Tasks 2 and 3 must land in the same session without a deploy between them (they're consecutive; the branch isn't deployed mid-feature anyway).
- The facilities popover deliberately does NOT reuse `checkboxGroup` (required-set ≠ hidden-set semantics); the corrected inline implementation in Task 4 Step 1 governs.
