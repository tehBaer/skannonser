// Metric filters (DIM, not hide) + per-boligtype visibility (Phase 5 Task 7).
//
// PORT PROVENANCE (apps_script/map/map.html):
//   * matchesMetricFilters ..... map.html 3813-3843 (per-metric pass test;
//                                missing travel metrics stay visible; missing
//                                price/BRA keep DNB rows visible).
//   * dim intensity ............ getDimIntensity 1454-1459. NOTE the legacy
//                                input was "Non-hit opacity" (the RESIDUAL
//                                opacity, default 10%). The brief asks for a
//                                "dim intensity" slider (the amount removed,
//                                default ~75%), so we expose the COMPLEMENT:
//                                residualOpacity = 1 - dimIntensity/100.
//   * per-type visibility ...... isBoligtypeEnabled / matchesBoligtypeToggle
//                                map.html 4256-4267.
//
// SIMPLIFICATIONS vs legacy: legacy carried full min+max ranges for BRA-i /
// pris-kvm / pris plus an age-in-days window and single-value travel sliders
// (metricFilters, map.html 1078-1167). The brief mandates a single-bound
// subset -- price MAX, BRA-i MIN, and a travel-minutes MAX per destination --
// so only those are built here (pris-kvm and age are dropped).

// Default upper bound for the BRA-i slider (m²) when we can't infer one.
const BRA_I_SLIDER_MAX = 250;
// Travel-minute sliders share this ceiling (a dest slider AT this value = off).
export const TRAVEL_MAX = 120;
// Details-filter ceilings (slider AT the ceiling = filter off, like TRAVEL_MAX).
export const TOTALPRIS_MAX = 10_000_000;
export const FELLESKOST_MAX = 15000;

// Fresh, fully-default filter sub-state derived from /api/meta.
export function defaultFilterState(meta) {
  const priceBound = Number((meta.filters && meta.filters.sheets_max_price) || 7500000);
  const travelMax = {};
  (meta.destinations || []).forEach((d) => {
    travelMax[d.key] = TRAVEL_MAX;
  });
  return {
    priceMax: priceBound, // == bound -> price filter off
    braIMin: 0, // == 0 -> BRA-i filter off
    travelMax, // per-dest, each == TRAVEL_MAX -> off
    // Listing-details filters (2026-07-23). Null/absent values on an item are
    // "unknown": they PASS every active details filter while includeUnknown
    // is on (default), and fail while it's off -- silently hiding sparse
    // older rows would be worse than showing them.
    soveromMin: 0,          // 0 = off
    totalprisMax: TOTALPRIS_MAX,
    felleskostMax: FELLESKOST_MAX,
    eieform: "",           // "" = any
    energiHidden: {},       // letter -> true (hidden), like boligtypeHidden
    facilitiesRequired: {}, // facility -> true (must have; AND semantics)
    includeUnknown: true,
  };
}

function priceBoundOf(meta) {
  return Number((meta.filters && meta.filters.sheets_max_price) || 7500000);
}

const NOK = new Intl.NumberFormat("nb-NO");
function isDnb(item) {
  return item.source === "dnb";
}
function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// matchesMetricFilters (map.html 3813-3843), inverted: returns TRUE when the
// listing should be DIMMED (fails at least one active metric filter).
export function metricDimmed(item, ui, meta) {
  const f = ui.filters;
  const priceBound = priceBoundOf(meta);
  const dnb = isDnb(item);

  // Price MAX (active only when narrowed below the bound).
  if (f.priceMax < priceBound) {
    const pris = num(item.pris);
    if (pris == null) {
      if (!dnb) return true; // missing price on a narrowed filter dims Eie/Sold
    } else if (pris > f.priceMax) {
      return true;
    }
  }

  // BRA-i MIN (active only when raised above 0).
  if (f.braIMin > 0) {
    const bra = num(item.bra_i);
    if (bra == null) {
      if (!dnb) return true;
    } else if (bra < f.braIMin) {
      return true;
    }
  }

  // Travel MAX per destination. Missing travel never dims (legacy 3824-3826).
  const travel = item.travel || {};
  for (const key of Object.keys(f.travelMax)) {
    const max = f.travelMax[key];
    if (max >= TRAVEL_MAX) continue; // off
    const mins = num(travel[key]);
    if (mins == null) continue; // keep visible
    if (mins > max) return true;
  }

  const unknownDims = !f.includeUnknown; // does an unknown value fail filters?

  // Min soverom.
  if (f.soveromMin > 0) {
    const soverom = num(item.soverom);
    if (soverom == null) {
      if (unknownDims) return true;
    } else if (soverom < f.soveromMin) {
      return true;
    }
  }

  // Max totalpris.
  if (f.totalprisMax < TOTALPRIS_MAX) {
    const totalpris = num(item.totalpris);
    if (totalpris == null) {
      if (unknownDims) return true;
    } else if (totalpris > f.totalprisMax) {
      return true;
    }
  }

  // Max felleskost/mnd.
  if (f.felleskostMax < FELLESKOST_MAX) {
    const felleskost = num(item.felleskost_mnd);
    if (felleskost == null) {
      if (unknownDims) return true;
    } else if (felleskost > f.felleskostMax) {
      return true;
    }
  }

  // Eieform (categorical; "" = any).
  if (f.eieform) {
    const eieform = item.eieform || null;
    if (eieform == null) {
      if (unknownDims) return true;
    } else if (eieform !== f.eieform) {
      return true;
    }
  }

  // Energimerke: hidden letters dim; unknown grade follows includeUnknown
  // once any letter is hidden.
  const energiHidden = f.energiHidden || {};
  if (Object.keys(energiHidden).length) {
    const letter = item.energimerke || null;
    if (letter == null) {
      if (unknownDims) return true;
    } else if (energiHidden[letter]) {
      return true;
    }
  }

  // Required facilities (AND). Items without facility data (DNB, unparsed)
  // are "unknown" as a whole.
  const required = Object.keys(f.facilitiesRequired || {});
  if (required.length) {
    const has = item.facilities;
    if (!Array.isArray(has) || has.length === 0) {
      if (unknownDims) return true;
    } else if (!required.every((r) => has.includes(r))) {
      return true;
    }
  }
  return false;
}

// isBoligtypeEnabled / matchesBoligtypeToggle (map.html 4256-4267): an
// unchecked type is hidden ENTIRELY (not merely dimmed).
export function boligtypeHidden(item, ui) {
  const hidden = ui.boligtypeHidden || {};
  const key = item.boligtype || ""; // "" == the "unknown" bucket
  return Boolean(hidden[key]);
}

// residual opacity for a dimmed marker (see header dim-intensity note).
export function residualOpacity(ui) {
  const pct = Math.max(0, Math.min(100, Number(ui.dimIntensity)));
  return 1 - pct / 100;
}

// --- UI builders ---

function rangeRow(parent, { label, min, max, step, value, fmt, onInput }) {
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
    label: "Nedtoning",
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

  // Eieform select ("" = any).
  const eieWrap = document.createElement("div");
  eieWrap.className = "filter-row";
  const eieLabel = document.createElement("div");
  eieLabel.className = "filter-head";
  eieLabel.textContent = "Eieform";
  const eieSelect = document.createElement("select");
  [["", "Alle"], ...(meta.eieformer || []).map((e) => [e, e])].forEach(([value, label]) => {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = label;
    eieSelect.appendChild(opt);
  });
  eieSelect.value = ui.filters.eieform || "";
  eieSelect.addEventListener("change", () => {
    ui.filters.eieform = eieSelect.value;
    onChange();
  });
  eieWrap.appendChild(eieLabel);
  eieWrap.appendChild(eieSelect);
  container.appendChild(eieWrap);

  // Energimerke checkboxes (checked = visible), one per observed letter.
  if ((meta.energimerker || []).length) {
    const energiWrap = document.createElement("div");
    energiWrap.className = "filter-row energi-row";
    const energiLabel = document.createElement("div");
    energiLabel.className = "filter-head";
    energiLabel.textContent = "Energimerking";
    energiWrap.appendChild(energiLabel);
    (meta.energimerker || []).forEach((letter) => {
      const row = document.createElement("label");
      row.className = "toggle energi-toggle";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = !ui.filters.energiHidden[letter];
      cb.addEventListener("change", () => {
        if (cb.checked) delete ui.filters.energiHidden[letter];
        else ui.filters.energiHidden[letter] = true;
        onChange();
      });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(letter));
      energiWrap.appendChild(row);
    });
    container.appendChild(energiWrap);
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

// Per-boligtype visibility checkboxes (checked = visible). `colorByType` gives
// each row its palette swatch; the "" key is the "Ukjent boligtype" bucket.
export function buildBoligtypeFilterUI(container, meta, colorByType, ui, onChange) {
  container.innerHTML = "";
  const rows = [...(meta.boligtyper || []).map((t) => [t, t]), ["", "Ukjent boligtype"]];
  rows.forEach(([key, label]) => {
    const row = document.createElement("label");
    row.className = "toggle boligtype-toggle";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !ui.boligtypeHidden[key];
    cb.addEventListener("change", () => {
      if (cb.checked) delete ui.boligtypeHidden[key];
      else ui.boligtypeHidden[key] = true;
      onChange();
    });
    const sw = document.createElement("span");
    sw.className = "legend-swatch";
    sw.style.background = (colorByType && colorByType[key]) || "#6f7e76";
    row.appendChild(cb);
    row.appendChild(sw);
    row.appendChild(document.createTextNode(label));
    container.appendChild(row);
  });
}
