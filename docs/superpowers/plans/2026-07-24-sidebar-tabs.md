# Map Sidebar Overhaul + Kart/Tabell Tabs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Shorten and regroup the map sidebar (Notion-style select-fields, Visning panel for display settings, three collapsible slider groups, expandable active-filter list) and connect the two pages with a shared Kart | Tabell tab header.

**Architecture:** Purely presentational over the existing unified-filtering machinery: `filterstate.js` gains `activeFilterEntries` (the data behind the expandable active line), the popover primitives move from `tablefilters.js` into `filters.js` so the new sidebar `selectField` component can share them, and `filters.js`'s monolithic `buildMetricFilterUI` is replaced by `buildFilterPanelUI` (fields + slider sub-groups) + `buildDisplayUI` (nedtoning sliders). index.html restructures the panels; table.html only swaps its header. **Zero changes to the filter state schema, `listingExcluded`, migration, or anything in Python.**

**Tech Stack:** Plain browser ES modules (no build step), existing shared components (`rangeRow`, `checkboxGroup`), CSS in the one stylesheet.

**Spec:** `docs/superpowers/specs/2026-07-24-sidebar-tabs-design.md`

## Global Constraints

- ZERO backend/schema changes: no Python, no `defaultFilters`/`loadFilters`/`saveFilters` shape change, no predicate change. Final task proves it with the pytest suite (616) untouched-green.
- No JS harness: gate each task on `node --check` per touched JS file (+ the node script in Task 1). Browser verification is the controller's job — implementers must NOT start servers or browsers.
- Storage semantics unchanged: select-fields render the existing hidden-sets (`checked = visible`); `clear()` functions mutate sub-objects IN PLACE (delete keys / splice) so live component references stay valid.
- Norwegian copy, exact strings: "Alle", "Ingen", "N av M", "Ingen aktive filtre", "N filtre aktive · vis/skjul", "Nullstill filtre", "Inkluder ukjent verdi", "Pris og kostnad", "Bolig", "Reisetid", "Visning", "Filtret nedtoning", "Solgt nedtoning", "Ukjent boligtype", "Søk …", tab labels "Kart"/"Tabell".
- Slider bounds/steps are EXACTLY today's (from filterstate.js): they move between builders but never change values.
- The table page keeps its popovers/toolbar/columns unchanged — only its header is touched.
- The real static dir is `skannonser/web/static/` — NEVER create a root-level `web/` directory (a previous implementer did; it was a review finding).
- Commit prefixes as in this repo (`feat(web): …`).

---

### Task 1: `activeFilterEntries` in filterstate.js

**Files:**
- Modify: `skannonser/web/static/filterstate.js` (replace `activeFilterCount`, lines ~118-148)

**Interfaces:**
- Consumes: the module's own consts + `priceBoundOf`.
- Produces: `activeFilterEntries(filters, meta) -> [{key: string, label: string, valueText: string, clear(filters): void}]` — one entry per active filter dimension, in a stable order (money sliders, size sliders, travel, hidden sets, selected sets, facilities); `activeFilterCount(filters, meta)` reimplemented as `activeFilterEntries(...).length` so the two can never drift.

- [ ] **Step 1: Replace the existing `activeFilterCount` function** (keep its position in the file) with:

```js
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
```

- [ ] **Step 2: Syntax check**

Run: `node --check skannonser/web/static/filterstate.js`
Expected: exit 0.

- [ ] **Step 3: Node sanity run** (temp, not committed)

```bash
node --input-type=module -e "
import('file://' + process.cwd() + '/skannonser/web/static/filterstate.js').then((m) => {
  const meta = { filters: { sheets_max_price: 7500000 }, destinations: [{key:'brj'},{key:'mvv'}], eieformer: ['Andel','Selveier'] };
  const f = m.defaultFilters(meta);
  console.assert(m.activeFilterEntries(f, meta).length === 0, 'empty at defaults');
  f.felleskostMax = 5000;
  f.travelMax.brj = 40;
  f.boligtypeHidden.Enebolig = true;
  f.postnummerSelected.push('2000');
  f.facilitiesRequired.Heis = true;
  const entries = m.activeFilterEntries(f, meta);
  console.assert(entries.length === 5 && m.activeFilterCount(f, meta) === 5, 'count parity');
  const labels = entries.map((e) => e.label);
  console.assert(labels.includes('Maks felleskost') && labels.includes('Maks BRJ') && labels.includes('Boligtype') && labels.includes('Postnummer') && labels.includes('Fasiliteter'), 'labels');
  const boligRef = f.boligtypeHidden;
  entries.forEach((e) => e.clear(f));
  console.assert(m.activeFilterCount(f, meta) === 0, 'cleared');
  console.assert(f.boligtypeHidden === boligRef, 'in-place mutation (same object ref)');
  console.assert(f.felleskostMax === 15000 && f.travelMax.brj === 120, 'slider resets to bounds');
  console.log('OK');
});
"
```

Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/filterstate.js
git commit -m "feat(web): activeFilterEntries -- per-filter active list with in-place clear"
```

---

### Task 2: Popover primitives move to filters.js

**Files:**
- Modify: `skannonser/web/static/filters.js` (append the popover block)
- Modify: `skannonser/web/static/tablefilters.js` (delete its copy, import instead; lines ~78-121)

**Interfaces:**
- Produces: `filters.js` exports `openPopover(anchor, build)` and `closePopover()` (plus module-private `placePopover` and the document-level dismiss listeners). `tablefilters.js` re-exports `closePopover` so `table.js`'s existing import keeps working unchanged.

- [ ] **Step 1: Append to `filters.js`** (after `searchableMultiSelect`, before `buildMetricFilterUI`) — this block is MOVED VERBATIM from tablefilters.js (its current lines ~78-121) with `export` added to `closePopover` and `openPopover`:

```js
// --- shared popover singleton (moved from tablefilters.js 2026-07-24) ---
// Used by the table's header filters AND the sidebar's select-fields.

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

export function openPopover(anchor, build) {
  if (popoverAnchor === anchor) {
    closePopover(); // toggling the same anchor closes it
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

// One document-level dismiss wiring (module init).
document.addEventListener("click", (ev) => {
  if (!popoverEl) return;
  if (popoverEl.contains(ev.target)) return;
  if (popoverAnchor && popoverAnchor.contains(ev.target)) return;
  closePopover();
});
document.addEventListener("keydown", (ev) => {
  if (ev.key === "Escape") closePopover();
});
```

- [ ] **Step 2: In `tablefilters.js`:** delete the moved block (the `let popoverEl` … both `document.addEventListener` listeners, currently lines ~78-121, including the `--- popover singleton ---` comment). Add `openPopover` and `closePopover` to the existing `from "./filters.js"` import list. Add this line next to the other exports so table.js's `import { closePopover } from "./tablefilters.js"` keeps resolving:

```js
export { closePopover } from "./filters.js";
```

(`makeFilterButton` and `openFacilitiesPopover` now call the imported `openPopover` — the call sites themselves need no edit.)

- [ ] **Step 3: Syntax check both**

Run: `node --check skannonser/web/static/filters.js && node --check skannonser/web/static/tablefilters.js`
Expected: exit 0 for both. Also `grep -c "popoverEl" skannonser/web/static/tablefilters.js` → 0.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/filters.js skannonser/web/static/tablefilters.js
git commit -m "feat(web): share popover primitives from filters.js (moved out of tablefilters)"
```

---

### Task 3: `selectField` + `buildFilterPanelUI` + `buildDisplayUI`; retire the old builders

**Files:**
- Modify: `skannonser/web/static/filters.js` (replace `buildMetricFilterUI`/`buildBoligtypeFilterUI`/`buildMoreFiltersUI` with the new builders)

**Interfaces:**
- Consumes: `openPopover` (Task 2), existing `rangeRow`/`checkboxGroup`, bounds consts.
- Produces: `selectField(parent, {label, options, hidden, swatches?, searchable?, onChange})`; `buildFilterPanelUI(container, {meta, vocabs, colorByType, filters, collapsed, onChange, onCollapse})`; `buildDisplayUI(container, ui, onChange)`. REMOVED exports (Task 4 fixes the importer): `buildMetricFilterUI`, `buildBoligtypeFilterUI`, `buildMoreFiltersUI`. The map page is import-broken between this task and Task 4 — expected, do not deploy mid-feature.

- [ ] **Step 1: Add `selectField`** (after the popover block from Task 2):

```js
// Notion-style compact select-field over a HIDDEN-set: closed it shows a
// summary ("Alle" when nothing is hidden, chips of the visible values when
// ≤3 remain, else "N av M"); clicking opens the shared popover with the
// familiar checkbox rows (checked = visible). Storage semantics unchanged.
export function selectField(parent, { label, options, hidden, swatches, searchable, onChange }) {
  const field = document.createElement("button");
  field.type = "button";
  field.className = "select-field";
  const name = document.createElement("span");
  name.className = "select-field-label";
  name.textContent = label;
  const value = document.createElement("span");
  value.className = "select-field-value";
  field.appendChild(name);
  field.appendChild(value);

  const paint = () => {
    value.innerHTML = "";
    value.classList.remove("muted");
    const visible = options.filter((o) => !hidden[o.key]);
    if (visible.length === options.length) {
      value.textContent = "Alle";
      value.classList.add("muted");
    } else if (visible.length === 0) {
      value.textContent = "Ingen";
    } else if (visible.length <= 3) {
      visible.forEach((o) => {
        const chip = document.createElement("span");
        chip.className = "chip";
        if (swatches && o.swatch) {
          const dot = document.createElement("span");
          dot.className = "chip-dot";
          dot.style.background = o.swatch;
          chip.appendChild(dot);
        }
        chip.appendChild(document.createTextNode(o.label));
        value.appendChild(chip);
      });
    } else {
      value.textContent = visible.length + " av " + options.length;
    }
  };

  const buildBody = (pop) => {
    if (searchable) {
      const search = document.createElement("input");
      search.type = "text";
      search.placeholder = "Søk …";
      search.className = "multi-search";
      pop.appendChild(search);
      const listWrap = document.createElement("div");
      listWrap.className = "multi-list";
      pop.appendChild(listWrap);
      const render = () => {
        const q = search.value.trim().toLowerCase();
        listWrap.innerHTML = "";
        checkboxGroup(listWrap, {
          options: options.filter((o) => !q || o.label.toLowerCase().includes(q)),
          hidden,
          onChange: () => {
            paint();
            onChange();
          },
        });
      };
      search.addEventListener("input", render);
      render();
      return;
    }
    checkboxGroup(pop, {
      options,
      hidden,
      onChange: () => {
        paint();
        onChange();
      },
    });
  };

  field.addEventListener("click", (ev) => {
    ev.stopPropagation();
    openPopover(field, buildBody);
  });
  paint();
  parent.appendChild(field);
  return field;
}
```

- [ ] **Step 2: Replace `buildMetricFilterUI` with `buildFilterPanelUI` + `buildDisplayUI`, and DELETE `buildBoligtypeFilterUI` and `buildMoreFiltersUI` entirely.** The five select-fields, three slider sub-groups (order and slider bounds/steps identical to today's), and the unknown-toggle:

```js
// The whole "Filtre" panel body: five select-fields, three collapsible
// slider sub-groups (collapse state persisted via ui.collapsed through
// onCollapse), and the unknown-value policy toggle. Replaces the old
// buildMetricFilterUI/buildBoligtypeFilterUI/buildMoreFiltersUI trio --
// facilities/postnummer/nabolag deliberately have NO sidebar UI (2026-07-24
// sidebar-tabs spec §2): they are edited from the table popovers and
// surfaced via the active-filter list.
export function buildFilterPanelUI(
  container,
  { meta, vocabs, colorByType, filters, collapsed, onChange, onCollapse }
) {
  container.innerHTML = "";
  container.classList.remove("muted");

  const fields = document.createElement("div");
  fields.className = "filter-fields";
  selectField(fields, {
    label: "Boligtype",
    options: [
      ...(meta.boligtyper || []).map((t) => ({
        key: t,
        label: t,
        swatch: (colorByType && colorByType[t]) || "#6f7e76",
      })),
      { key: "", label: "Ukjent boligtype", swatch: (colorByType && colorByType[""]) || "#6f7e76" },
    ],
    hidden: filters.boligtypeHidden,
    swatches: true,
    onChange,
  });
  selectField(fields, {
    label: "Eieform",
    options: (meta.eieformer || []).map((v) => ({ key: v, label: v })),
    hidden: filters.eieformHidden,
    onChange,
  });
  selectField(fields, {
    label: "Energimerking",
    options: (meta.energimerker || []).map((v) => ({ key: v, label: v })),
    hidden: filters.energiHidden,
    onChange,
  });
  selectField(fields, {
    label: "Tilgjengelighet",
    options: vocabs.tilgjengelighet,
    hidden: filters.tilgjengelighetHidden,
    onChange,
  });
  selectField(fields, {
    label: "Tags",
    options: vocabs.tags,
    hidden: filters.tagHidden,
    searchable: true,
    onChange,
  });
  container.appendChild(fields);

  const group = (id, title) => {
    const det = document.createElement("details");
    det.className = "subgroup";
    det.id = id;
    det.open = !collapsed[id];
    const sum = document.createElement("summary");
    sum.textContent = title;
    det.appendChild(sum);
    det.addEventListener("toggle", () => {
      if (det.open) delete collapsed[id];
      else collapsed[id] = true;
      onCollapse();
    });
    container.appendChild(det);
    return det;
  };
  const kr = (bound) => (v) => (v >= bound ? "Av" : NOK.format(v) + " kr");
  const priceBound = priceBoundOf(meta);

  const pris = group("grp-pris", "Pris og kostnad");
  rangeRow(pris, {
    label: "Maks pris", min: 0, max: priceBound, step: 50000,
    value: filters.priceMax, fmt: kr(priceBound),
    onInput: (v) => { filters.priceMax = v; onChange(); },
  });
  rangeRow(pris, {
    label: "Maks totalpris", min: 0, max: TOTALPRIS_MAX, step: 100000,
    value: filters.totalprisMax, fmt: kr(TOTALPRIS_MAX),
    onInput: (v) => { filters.totalprisMax = v; onChange(); },
  });
  rangeRow(pris, {
    label: "Maks felleskost/mnd", min: 0, max: FELLESKOST_MAX, step: 250,
    value: filters.felleskostMax, fmt: kr(FELLESKOST_MAX),
    onInput: (v) => { filters.felleskostMax = v; onChange(); },
  });
  rangeRow(pris, {
    label: "Maks mnd-kost", min: 0, max: MAANEDSKOST_MAX, step: 250,
    value: filters.maanedskostMax, fmt: kr(MAANEDSKOST_MAX),
    onInput: (v) => { filters.maanedskostMax = v; onChange(); },
  });
  rangeRow(pris, {
    label: "Maks total/kvm", min: 0, max: TOTAL_KVM_MAX, step: 1000,
    value: filters.totalKvmMax, fmt: kr(TOTAL_KVM_MAX),
    onInput: (v) => { filters.totalKvmMax = v; onChange(); },
  });

  const bolig = group("grp-bolig", "Bolig");
  rangeRow(bolig, {
    label: "Min BRA-i", min: 0, max: BRA_I_SLIDER_MAX, step: 5,
    value: filters.braIMin, fmt: (v) => (v <= 0 ? "Av" : v + " m²"),
    onInput: (v) => { filters.braIMin = v; onChange(); },
  });
  rangeRow(bolig, {
    label: "Min soverom", min: 0, max: 6, step: 1,
    value: filters.soveromMin, fmt: (v) => (v <= 0 ? "Av" : "≥ " + v),
    onInput: (v) => { filters.soveromMin = v; onChange(); },
  });
  rangeRow(bolig, {
    label: "Min byggeår", min: BYGGEAAR_FLOOR, max: BYGGEAAR_CEIL, step: 1,
    value: filters.byggeaarMin, fmt: (v) => (v <= BYGGEAAR_FLOOR ? "Av" : "≥ " + v),
    onInput: (v) => { filters.byggeaarMin = v; onChange(); },
  });

  const reise = group("grp-reisetid", "Reisetid");
  (meta.destinations || []).forEach((d) => {
    rangeRow(reise, {
      label: "Maks " + shortDest(d.key) + " (min)", min: 0, max: TRAVEL_MAX, step: 1,
      value: filters.travelMax[d.key],
      fmt: (v) => (v >= TRAVEL_MAX ? "Av" : "≤ " + v + " min"),
      onInput: (v) => { filters.travelMax[d.key] = v; onChange(); },
    });
  });

  const unkRow = document.createElement("label");
  unkRow.className = "toggle";
  const unkCb = document.createElement("input");
  unkCb.type = "checkbox";
  unkCb.checked = filters.includeUnknown !== false;
  unkCb.addEventListener("change", () => {
    filters.includeUnknown = unkCb.checked;
    onChange();
  });
  unkRow.appendChild(unkCb);
  unkRow.appendChild(document.createTextNode("Inkluder ukjent verdi"));
  container.appendChild(unkRow);
}

// The "Visning" panel's sliders: display settings, NOT filters. (The
// klyng/budpremie checkboxes live in static HTML in the same panel, wired
// by app.js's existing wireLayerToggles/wirePremiumToggle.)
export function buildDisplayUI(container, ui, onChange) {
  container.innerHTML = "";
  rangeRow(container, {
    label: "Filtret nedtoning", min: 0, max: 100, step: 5,
    value: ui.dimIntensity, fmt: (v) => v + " %",
    onInput: (v) => { ui.dimIntensity = v; onChange(); },
  });
  rangeRow(container, {
    label: "Solgt nedtoning", min: 0, max: 100, step: 5,
    value: ui.soldDim || 0, fmt: (v) => (v <= 0 ? "Av" : v + " %"),
    onInput: (v) => { ui.soldDim = v; onChange(); },
  });
}
```

Note the old builders' contents this replaces: the five original + three details sliders and travel sliders (now in sub-groups, values/steps identical), the eieform/energi checkboxGroups (now select-fields), the facilities block (DROPPED from the sidebar per spec), the unknown toggle (kept), the two nedtoning sliders (moved to `buildDisplayUI`), the whole `buildBoligtypeFilterUI` (now the Boligtype select-field), and `buildMoreFiltersUI` (tilgjengelighet became a select-field; postnummer/nabolag DROPPED from the sidebar per spec).

- [ ] **Step 3: Syntax check + export audit**

Run: `node --check skannonser/web/static/filters.js`
Expected: exit 0. Then `grep -n "buildMetricFilterUI\|buildBoligtypeFilterUI\|buildMoreFiltersUI" skannonser/web/static/filters.js` → no matches.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/filters.js
git commit -m "feat(web): selectField + buildFilterPanelUI/buildDisplayUI; retire old sidebar builders"
```

---

### Task 4: Sidebar restructure — index.html + app.js + style.css

**Files:**
- Modify: `skannonser/web/static/index.html` (sidebar body)
- Modify: `skannonser/web/static/app.js` (imports, rebuildFilterUIs, active line, retired builders)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: Task 1 `activeFilterEntries`, Task 3 builders.
- Produces: the working new sidebar. The `.app-tabs` CSS also serves Task 5's table header.

- [ ] **Step 1: index.html.** Replace the sidebar contents between `<h1 class="brand">skannonser</h1>` and `<div id="status" …>` with (stations panel body is UNCHANGED — copy it verbatim from the current file where marked):

```html
      <nav class="app-tabs">
        <a href="/" class="tab active">Kart</a>
        <a href="/table" class="tab">Tabell</a>
      </nav>

      <details class="panel" id="lag-panel" open>
        <summary><h2>Lag</h2></summary>
        <label class="toggle"><input type="checkbox" id="toggle-eie" checked> Eie</label>
        <label class="toggle"><input type="checkbox" id="toggle-dnb" checked> DNB</label>
        <label class="toggle"><input type="checkbox" id="toggle-sold"> Solgt</label>
        <div id="source-legend" class="legend legend-keys"></div>
      </details>

      <details class="panel" id="filter-panel" open>
        <summary><h2>Filtre</h2></summary>
        <div id="active-filters" class="muted active-filters"></div>
        <button type="button" id="reset-filters" class="reset-filters">Nullstill filtre</button>
        <div id="filter-panel-body" class="muted">Laster …</div>
      </details>

      <details class="panel" id="visning-panel" open>
        <summary><h2>Visning</h2></summary>
        <div id="display-sliders"></div>
        <label class="toggle"><input type="checkbox" id="toggle-combine-sold"> Klyng solgte + aktive sammen</label>
        <label class="toggle"><input type="checkbox" id="toggle-sold-premium"> Farg solgte etter budpremie</label>
        <div id="premium-legend" class="legend" hidden></div>
      </details>

      <!-- stations panel: UNCHANGED, copy verbatim from the current file -->

      <details class="panel" id="missing-coords-panel">
        <summary><h2>Uten koordinater</h2></summary>
        <div id="missing-coords" class="muted">Laster …</div>
      </details>
```

Deleted relative to today: `#boligtype-filter-panel`, `#tag-panel`, `#more-filters-panel`, `#table-link-panel`, the `#metric-filters`/`#more-filters` divs; `#missing-coords-panel` loses its `open` attribute; the klyng/budpremie toggles + premium legend MOVED from Lag to Visning; `#source-legend` MOVED into Lag.

- [ ] **Step 2: app.js imports.** In the `./filters.js` import block replace `buildMetricFilterUI, buildBoligtypeFilterUI, buildMoreFiltersUI` with `buildFilterPanelUI, buildDisplayUI` (keep `listingExcluded, residualOpacity, deriveVocabs`). In the `./filterstate.js` block add `activeFilterEntries` (keep the rest).

- [ ] **Step 3: app.js — delete `buildTagFilterUI` entirely** (the Tags select-field replaces it) and update its two remaining call sites: the `sk-annotation-saved` listener becomes

```js
  document.addEventListener("sk-annotation-saved", () => {
    rebuildFilterUIs(); // tag vocab may have changed
    applyAll(); // tag rings / tag-visibility may have changed
  });
```

(`ensureSoldLoaded` already calls `rebuildFilterUIs()` — no change there.)

- [ ] **Step 4: app.js — replace `rebuildFilterUIs` and `renderActiveFilterLine`:**

```js
// Collapsed/expanded state of the active-filter list (session-local, not persisted).
let activeFiltersExpanded = false;

function renderActiveFilterLine() {
  const node = document.getElementById("active-filters");
  if (!node) return;
  node.innerHTML = "";
  const entries = activeFilterEntries(state.ui.filters, state.meta);
  const head = document.createElement("button");
  head.type = "button";
  head.className = "af-head";
  head.textContent = entries.length
    ? entries.length + " filtre aktive · " + (activeFiltersExpanded ? "skjul" : "vis")
    : "Ingen aktive filtre";
  head.disabled = !entries.length;
  head.addEventListener("click", () => {
    activeFiltersExpanded = !activeFiltersExpanded;
    renderActiveFilterLine();
  });
  node.appendChild(head);
  if (!activeFiltersExpanded || !entries.length) return;
  entries.forEach((entry) => {
    const row = document.createElement("div");
    row.className = "af-row";
    const text = document.createElement("span");
    text.textContent = entry.label + ": " + entry.valueText;
    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "af-clear";
    clearBtn.setAttribute("aria-label", "Fjern filter");
    clearBtn.textContent = "×";
    clearBtn.addEventListener("click", () => {
      entry.clear(state.ui.filters);
      rebuildFilterUIs(); // field summaries + sliders must reflect the clear
      onFilterChange();
    });
    row.appendChild(text);
    row.appendChild(clearBtn);
    node.appendChild(row);
  });
}

// (Re)build every filter/display control that renders shared state -- init,
// reset, cross-tab storage event, sold-load, annotation-save.
function rebuildFilterUIs() {
  buildFilterPanelUI(document.getElementById("filter-panel-body"), {
    meta: state.meta,
    vocabs: deriveVocabs([...state.itemsById.values()]),
    colorByType: { ...state.colorByType, "": DEFAULT_UNKNOWN_TYPE_COLOR },
    filters: state.ui.filters,
    collapsed: state.ui.collapsed,
    onChange: onFilterChange,
    onCollapse: saveUi,
  });
  buildDisplayUI(document.getElementById("display-sliders"), state.ui, onFilterChange);
  renderActiveFilterLine();
}
```

(`onFilterChange` itself is unchanged: `saveUi(); renderActiveFilterLine(); applyAll();`. The reset-button and `subscribeOtherTabs` handlers are unchanged — they already call `rebuildFilterUIs()`.)

- [ ] **Step 5: style.css append:**

```css
/* --- sidebar overhaul + tabs (2026-07-24) --- */
.app-tabs { display: flex; gap: 4px; margin: 6px 0 12px; }
.app-tabs .tab {
  padding: 5px 14px;
  text-decoration: none;
  color: var(--muted, #6f7e76);
  border-bottom: 2px solid transparent;
  font-weight: 600;
}
.app-tabs .tab.active { color: var(--accent, #2f6f4f); border-bottom-color: var(--accent, #2f6f4f); }
.app-tabs .tab:hover { color: var(--text, #222); }

.filter-fields { margin: 8px 0; }
.select-field {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  width: 100%;
  padding: 6px 8px;
  margin: 4px 0;
  background: none;
  border: 1px solid rgba(0, 0, 0, 0.12);
  border-radius: 6px;
  font: inherit;
  font-size: 13px;
  cursor: pointer;
  text-align: left;
}
.select-field:hover { border-color: var(--accent, #2f6f4f); }
.select-field-label { font-weight: 600; white-space: nowrap; }
.select-field-value { display: flex; flex-wrap: wrap; gap: 4px; justify-content: flex-end; }
.select-field-value.muted { color: var(--muted, #8a938c); }
.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: rgba(47, 111, 79, 0.1);
  border-radius: 10px;
  padding: 1px 8px;
  font-size: 12px;
}
.chip-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }

details.subgroup { margin: 8px 0; }
details.subgroup > summary {
  cursor: pointer;
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--muted, #6f7e76);
  margin: 6px 0 2px;
}

.af-head {
  background: none;
  border: 0;
  padding: 0;
  font: inherit;
  font-size: 12px;
  color: var(--accent, #2f6f4f);
  cursor: pointer;
}
.af-head:disabled { color: var(--muted, #8a938c); cursor: default; }
.af-row { display: flex; justify-content: space-between; align-items: center; font-size: 12px; margin: 3px 0; gap: 6px; }
.af-clear { background: none; border: 0; cursor: pointer; color: var(--muted, #8a938c); font-size: 14px; padding: 0 2px; }
.af-clear:hover { color: #b3402f; }
.reset-filters { margin: 4px 0 8px; }
```

- [ ] **Step 6: Syntax check**

Run: `node --check skannonser/web/static/app.js`
Expected: exit 0. Then `grep -n "buildTagFilterUI\|buildMetricFilterUI\|buildBoligtypeFilterUI\|buildMoreFiltersUI\|metric-filters\|more-filters\|tag-filter\b" skannonser/web/static/app.js` → no matches.

- [ ] **Step 7: Commit** (controller browser-verifies after this task)

```bash
git add skannonser/web/static/index.html skannonser/web/static/app.js skannonser/web/static/style.css
git commit -m "feat(web): sidebar overhaul -- tabs, select-fields, Visning panel, expandable active-filter list"
```

---

### Task 5: Table page tab header

**Files:**
- Modify: `skannonser/web/static/table.html` (header only)

**Interfaces:** consumes the `.app-tabs` CSS from Task 4. `table.js` is untouched.

- [ ] **Step 1: Replace the `.table-header` element** with:

```html
    <header class="table-header">
      <h1 class="brand">skannonser</h1>
      <nav class="app-tabs">
        <a href="/" class="tab">Kart</a>
        <a href="/table" class="tab active">Tabell</a>
      </nav>
    </header>
```

- [ ] **Step 2: Commit**

```bash
git add skannonser/web/static/table.html
git commit -m "feat(web): Kart/Tabell tab header on the table page"
```

---

### Task 6: Verification + README (controller-led)

**Files:**
- Modify: `README.md` (web bullet: sidebar structure sentence)

- [ ] **Step 1:** `.venv/bin/pytest tests/rebuild -q` → 616 passed (proves zero backend impact).
- [ ] **Step 2:** Controller browser verification (spec §8): tab header on both pages with correct active state; new panel order (Lag / Filtre / Visning / Stasjoner / Uten koordinater collapsed); each select-field opens/edits/summarizes ("Alle" → chips → "N av M", boligtype swatches, tag search); slider sub-groups collapse and persist; Visning sliders dim without changing the active count; active line expands, lists a table-set fasiliteter filter and clears it with ×; Nullstill clears everything; cross-tab sync still live; existing-blob filters render correct summaries; mobile drawer + popover clamp; zero console errors on both pages.
- [ ] **Step 3:** README: in the `web/` bullet, replace the sidebar-filters sentence to describe the new structure (tabs, select-fields, Visning panel, expandable active-filter list; fasiliteter/postnummer/nabolag edited from the table only). Commit as `docs: sidebar overhaul + tabs -- README web bullet`.

---

## Self-review notes (already applied)

- Spec §1→Tasks 4+5 (header), §2→Task 4 (structure) + Task 3 (builders), §3→Task 2, §4→Task 3, §5→Task 1 (entries) + Task 4 (UI), §6 file list matches tasks, §7 invariants pinned in Global Constraints, §8→Task 6.
- Type consistency: `buildFilterPanelUI`'s opts object identical between Tasks 3 and 4; `activeFilterEntries` consumed in Task 4 exactly as produced in Task 1; `closePopover` re-export keeps table.js untouched.
- Known sequencing hazard: map page import-broken between Tasks 3 and 4 (documented; no mid-feature deploy).
- `vocabs.tags`/`vocabs.tilgjengelighet` come from the existing `deriveVocabs` — including the "" bucket labels — no new vocab code needed.
