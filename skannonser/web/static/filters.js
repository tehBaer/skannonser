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

export function buildMetricFilterUI(container, meta, ui, onChange) {
  container.innerHTML = "";
  const priceBound = priceBoundOf(meta);

  rangeRow(container, {
    label: "Maks pris",
    min: 0,
    max: priceBound,
    step: 50000,
    value: ui.filters.priceMax,
    fmt: (v) => (v >= priceBound ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.priceMax = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Min BRA-i",
    min: 0,
    max: BRA_I_SLIDER_MAX,
    step: 5,
    value: ui.filters.braIMin,
    fmt: (v) => (v <= 0 ? "Av" : v + " m²"),
    onInput: (v) => {
      ui.filters.braIMin = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Maks totalpris",
    min: 0,
    max: TOTALPRIS_MAX,
    step: 100000,
    value: ui.filters.totalprisMax,
    fmt: (v) => (v >= TOTALPRIS_MAX ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.totalprisMax = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Maks felleskost/mnd",
    min: 0,
    max: FELLESKOST_MAX,
    step: 250,
    value: ui.filters.felleskostMax,
    fmt: (v) => (v >= FELLESKOST_MAX ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.felleskostMax = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Min soverom",
    min: 0,
    max: 6,
    step: 1,
    value: ui.filters.soveromMin,
    fmt: (v) => (v <= 0 ? "Av" : "≥ " + v),
    onInput: (v) => {
      ui.filters.soveromMin = v;
      onChange();
    },
  });

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

  (meta.destinations || []).forEach((d) => {
    rangeRow(container, {
      label: "Maks " + shortDest(d.key) + " (min)",
      min: 0,
      max: TRAVEL_MAX,
      step: 1,
      value: ui.filters.travelMax[d.key],
      fmt: (v) => (v >= TRAVEL_MAX ? "Av" : "≤ " + v + " min"),
      onInput: (v) => {
        ui.filters.travelMax[d.key] = v;
        onChange();
      },
    });
  });

  rangeRow(container, {
    label: "Filtret nedtoning",
    min: 0,
    max: 100,
    step: 5,
    value: ui.dimIntensity,
    fmt: (v) => v + " %",
    onInput: (v) => {
      ui.dimIntensity = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Solgt nedtoning",
    min: 0,
    max: 100,
    step: 5,
    value: ui.soldDim || 0,
    fmt: (v) => (v <= 0 ? "Av" : v + " %"),
    onInput: (v) => {
      ui.soldDim = v;
      onChange();
    },
  });

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

  // Required facilities (checked = must have), sorted by frequency from meta.
  if ((meta.facilities || []).length) {
    const facWrap = document.createElement("div");
    facWrap.className = "filter-row facilities-row";
    const facLabel = document.createElement("div");
    facLabel.className = "filter-head";
    facLabel.textContent = "Må ha fasiliteter";
    facWrap.appendChild(facLabel);
    (meta.facilities || []).forEach((f) => {
      const row = document.createElement("label");
      row.className = "toggle facility-toggle";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = Boolean(ui.filters.facilitiesRequired[f.name]);
      cb.addEventListener("change", () => {
        if (cb.checked) ui.filters.facilitiesRequired[f.name] = true;
        else delete ui.filters.facilitiesRequired[f.name];
        onChange();
      });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(f.name + " (" + f.count + ")"));
      facWrap.appendChild(row);
    });
    container.appendChild(facWrap);
  }

  // Unknown-value policy for every details filter above.
  const unkRow = document.createElement("label");
  unkRow.className = "toggle";
  const unkCb = document.createElement("input");
  unkCb.type = "checkbox";
  unkCb.checked = ui.filters.includeUnknown !== false;
  unkCb.addEventListener("change", () => {
    ui.filters.includeUnknown = unkCb.checked;
    onChange();
  });
  unkRow.appendChild(unkCb);
  unkRow.appendChild(document.createTextNode("Inkluder ukjent verdi"));
  container.appendChild(unkRow);
}

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
