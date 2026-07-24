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

// residual opacity for a dimmed marker (see header dim-intensity note).
export function residualOpacity(ui) {
  const pct = Math.max(0, Math.min(100, Number(ui.dimIntensity)));
  return 1 - pct / 100;
}

// --- UI builders ---

export function rangeRow(parent, { label, min, max, step, value, fmt, onInput }) {
  const wrap = document.createElement("div");
  wrap.className = "filter-row";
  const head = document.createElement("div");
  head.className = "filter-head";
  const name = document.createElement("span");
  name.textContent = label;
  const val = document.createElement("span");
  val.className = "filter-val";
  head.appendChild(name);
  head.appendChild(val);
  const input = document.createElement("input");
  input.type = "range";
  input.min = String(min);
  input.max = String(max);
  input.step = String(step);
  input.value = String(value);
  const paint = () => {
    val.textContent = fmt(Number(input.value));
  };
  // The value label repaints on every tick; the actual onInput (which
  // triggers a full re-cluster of every source) is trailing-debounced so a
  // drag costs one rebuild, not one per pixel.
  let debounce = null;
  input.addEventListener("input", () => {
    paint();
    clearTimeout(debounce);
    debounce = setTimeout(() => onInput(Number(input.value)), 120);
  });
  paint();
  wrap.appendChild(head);
  wrap.appendChild(input);
  parent.appendChild(wrap);
  return input;
}

const shortDest = (key) => key.split("_").pop().toUpperCase();

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

// The whole "Filtre" panel body: five select-fields, three collapsible
// slider sub-groups (collapse state persisted via ui.collapsed through
// onCollapse), and the unknown-value policy toggle. Replaces the old
// metric-filter / boligtype-filter / more-filters builder trio --
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
