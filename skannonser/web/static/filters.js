// Shared filtering for BOTH pages (2026-07-24 unified-filtering spec):
// one predicate (`listingExcluded`), vocab derivation, and three reusable
// UI components (rangeRow / checkboxGroup / searchableMultiSelect) mounted
// by the map sidebar here and by the table's header popovers
// (tablefilters.js). State shape and bounds live in ./filterstate.js.
//
// Null policy: `filters.includeUnknown` (default true) governs every numeric
// filter and null eieform/postnummer/nabolag/facilities.
// Deliberate exceptions: missing TRAVEL minutes never exclude (legacy rule,
// apps_script map.html 3824-3826), and the "" buckets of boligtype/tag/
// tilgjengelighet/energimerke are explicit toggle rows, not "unknown".
// A NEGATIVE travel value is not "missing" -- it is a pipeline failure code
// and an active slider drops it; see listingmeta.js's travel-sentinel section.

import {
  BRA_I_SLIDER_MAX,
  TRAVEL_MAX,
  TOTALPRIS_MAX,
  FELLESKOST_MAX,
  BYGGEAAR_FLOOR,
  BYGGEAAR_CEIL,
  TOTAL_KVM_MAX,
  MAANEDSKOST_MAX,
  PRIS_KVM_MAX,
  SOLD_PRICE_MAX,
  PREMIUM_MAX,
  priceBoundOf,
} from "./filterstate.js";
import { assignTagColors, colorForTag } from "./tagcolors.js";
import {
  premiumPct, isTravelSentinel, TRAVEL_UNREACHABLE,
  FERDIGATTEST_OPTIONS, UTLEIE_OPTIONS, HUSDYR_OPTIONS, SALGSOPPGAVE_HINT, SALGSOPPGAVE_SUFFIX,
} from "./listingmeta.js";

const NOK = new Intl.NumberFormat("nb-NO");

// null/undefined/"" stay null (unknown) instead of coercing to 0 -- the
// filters must distinguish "unknown" from an actual zero.
function numOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function selectedSetExcludes(selected, raw, unknownFails) {
  if (!selected || !selected.length) return false;
  if (raw === null || raw === undefined || raw === "") return unknownFails;
  return !selected.includes(String(raw));
}

// Selection over values the UI renders EXPLICITLY, including the "" bucket
// ("Ukjent boligtype" / "(uten tag)"). Distinct from
// selectedSetExcludes, which treats a missing value as *unknown* and defers to
// `includeUnknown`: here "" is a value the user can pick like any other, so
// routing it through the unknown policy would make the empty bucket
// unselectable. Empty selection = filter off.
export function selectionExcludes(selected, value) {
  if (!selected || !selected.length) return false;
  return !selected.includes(value);
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
    // A sentinel (-1/-2/-3) is a failure code, not a fast commute: count it as
    // unreachable so an active slider drops it. Without this a raw -1 clears
    // every "maks reisetid" -- a 70 km listing the router found no route to
    // survived a "<= 20 min" filter as if it were a 20-minute commute.
    const raw = travel[key];
    const mins = isTravelSentinel(raw) ? TRAVEL_UNREACHABLE : numOrNull(raw);
    if (mins == null) continue; // missing travel never excludes (legacy rule)
    if (mins > max) return true;
  }
  if (underMin(item.soverom, f.soveromMin, 0)) return true;
  if (overMax(item.totalpris, f.totalprisMax, TOTALPRIS_MAX)) return true;
  if (overMax(item.felleskost_mnd, f.felleskostMax, FELLESKOST_MAX)) return true;
  if (underMin(item.byggeaar, f.byggeaarMin, BYGGEAAR_FLOOR)) return true;
  if (overMax(item.pris_kvm_totalpris, f.totalKvmMax, TOTAL_KVM_MAX)) return true;
  if (overMax(item.maanedskost, f.maanedskostMax, MAANEDSKOST_MAX)) return true;
  if (overMax(item.pris_kvm, f.prisKvmMax, PRIS_KVM_MAX)) return true;
  // Sold-outcome filters apply ONLY to sold items -- actives structurally
  // lack these fields, and must never be swept out by includeUnknown=false.
  if (item.sold) {
    if (overMax(item.sold_price, f.soldPriceMax, SOLD_PRICE_MAX)) return true;
    if ((f.premiumMax ?? PREMIUM_MAX) < PREMIUM_MAX) {
      const pct = premiumPct(item);
      if (pct == null) {
        if (unknownFails) return true;
      } else if (pct > f.premiumMax) {
        return true;
      }
    }
  }

  // Selections over explicitly-rendered values, "" bucket included.
  if (selectionExcludes(f.boligtypeSelected, item.boligtype || "")) return true;
  if (selectionExcludes(f.tagSelected, item.tag ? String(item.tag).trim() : "")) return true;
  if (selectionExcludes(f.tilgjengelighetSelected, item.tilgjengelighet || "")) return true;
  // Energimerking joined these on 2026-07-27: an ungraded listing is now the
  // explicit "Ukjent" chip rather than an "unknown" deferring to
  // includeUnknown. Under the old routing, picking A returned every ungraded
  // listing too -- which was ~70 % of the data before the svg energy-label
  // parser fix, so "A" was mostly not-A.
  if (selectionExcludes(f.energiSelected, item.energimerke || "")) return true;

  // Salgsoppgave enums join energimerking's routing, NOT the selected-set
  // routing below: `null` here means the prospectus was never parsed, which is
  // ~36 % of listings. Under selectedSetExcludes that would defer to
  // `includeUnknown` (on by default), so picking "Ja" would return every
  // unparsed listing as well -- the same way "A" was mostly not-A before the
  // energimerking fix above.
  if (selectionExcludes(f.ferdigattestSelected, item.ferdigattest || "")) return true;
  if (selectionExcludes(f.utleieSelected, item.utleie || "")) return true;
  if (selectionExcludes(f.husdyrSelected, item.husdyr || "")) return true;

  // Selected sets (empty = off; non-empty = only these pass).
  if (selectedSetExcludes(f.eieformSelected, item.eieform, unknownFails)) return true;
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
  // "" here is NOT missing data: the backend only ever fills tilgjengelighet for
  // CLOSED listings (Solgt / Inaktiv / Trukket), so a null means the listing is
  // still being advertised. Every one of production's active listings is null.
  // Labelling that "Ingen status" read as "we don't know" when we know exactly.
  const tilgList = byCount(tilg).map((o) => (o.key === "" ? { ...o, label: "Til salgs" } : o));
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

// Checkbox group over a small vocabulary, SELECTED-set semantics: every option
// rendered, checked = selected; `selected` is an ARRAY mutated in place, and an
// empty one means the filter is off. Same rule as searchableMultiSelect and the
// chip rows -- this is the funnel-shaped mount for a column header, where a
// checkbox list reads better than chips (2026-07-26 selection conversion).
export function checkboxGroup(parent, { label, options, selected, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "filter-row checkbox-group";
  if (label) {
    const head = document.createElement("div");
    head.className = "filter-head";
    head.textContent = label;
    wrap.appendChild(head);
  }
  if (!options.length) {
    // Same reason the chip rows carry this: a value list can legitimately be
    // empty (eieform and energimerking are, in any dataset that lacks the
    // enrichment), and a popover containing only its own header reads as
    // broken rather than as "nothing to choose from".
    const empty = document.createElement("div");
    empty.className = "chip-row-empty muted";
    empty.textContent = "Ingen verdier";
    wrap.appendChild(empty);
    parent.appendChild(wrap);
    return wrap;
  }
  options.forEach((opt) => {
    const row = document.createElement("label");
    row.className = "toggle";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selected.includes(opt.key);
    cb.addEventListener("change", () => {
      const i = selected.indexOf(opt.key);
      if (cb.checked && i === -1) selected.push(opt.key);
      if (!cb.checked && i !== -1) selected.splice(i, 1);
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
// Used by the table's header filters and its column picker. The sidebar no
// longer opens popovers (its select-fields became chip rows), but this stays
// exported here because tablefilters.js and table.js must share ONE open
// popover and one dismiss wiring.

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

// One document-level dismiss wiring (module init). Guarded because this module
// holds the shared filter predicate and vocabulary derivation, which are unit
// tested under node -- where there is no document.
if (typeof document !== "undefined") {
  document.addEventListener("click", (ev) => {
    if (!popoverEl) return;
    if (popoverEl.contains(ev.target)) return;
    if (popoverAnchor && popoverAnchor.contains(ev.target)) return;
    closePopover();
  });
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") closePopover();
  });
}

// The one interaction rule: a chip toggles, EXCEPT that the first selection
// isolates. Returns the new selection; never mutates its input. `allKeys` is
// unused by the rule itself but pins the caller's vocabulary at click time so
// a future "select all" can share this function.
export function applyChipClick(selected, key, allKeys) {
  const current = selected || [];
  if (!current.length) return [key];
  if (current.includes(key)) return current.filter((k) => k !== key);
  return current.concat([key]);
}

// One selection control for every value list -- the five sidebar filters, the
// station lines, and the table toolbar's tags (which passes no `label` and so
// gets the bulk controls without a heading). Selected chips are
// FILLED and unselected ones outlined, so state reads without relying on the
// per-value colour -- tags and lines carry their own colours and cannot also
// use colour to mean "on".
export function selectionChipRow(parent, { label, labelHint, options, selected, colorFor, emptyIsRealValue, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "chip-row-block";

  const head = document.createElement("div");
  head.className = "filter-head chip-row-head";
  // `label` is optional: the table toolbar mounts this row beside already
  // labelled buttons and has no room for a heading. The head stays either way
  // so the bulk controls keep their place.
  if (label) {
    const name = document.createElement("span");
    // Same marker as the table header/picker: one field, one visual language.
    name.textContent = labelHint ? label + SALGSOPPGAVE_SUFFIX : label;
    // Salgsoppgave-derived rows carry the same dotted marker as their table
    // headers, so the softness is visible wherever the field is offered.
    if (labelHint) {
      name.classList.add("from-salgsoppgave");
      name.title = labelHint;
    }
    head.appendChild(name);
  }

  const bulkWrap = document.createElement("span");
  bulkWrap.className = "chip-bulk";
  const mkBulk = (text, fn) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "linkish";
    b.textContent = text;
    b.addEventListener("click", () => {
      fn();
      repaint();
      onChange();
    });
    bulkWrap.appendChild(b);
  };
  // Both controls reach the same resting state -- an empty selection shows
  // everything -- but they read differently to a user mid-filter, so both are
  // offered. "Alle" is the answer to "show me everything again"; "Tøm" is the
  // answer to "undo my picks".
  mkBulk("Alle", () => selected.splice(0, selected.length));
  mkBulk("Tøm", () => selected.splice(0, selected.length));
  head.appendChild(bulkWrap);
  wrap.appendChild(head);

  // Painting is per-chip and closes over its own key, so no step depends on
  // the chip's position in the row. Declared before the empty-list bail so
  // the bulk handlers above always have something to call.
  const paints = [];
  const repaint = () => paints.forEach((p) => p());

  if (!options.length) {
    const empty = document.createElement("div");
    empty.className = "chip-row-empty muted";
    empty.textContent = "Ingen verdier";
    wrap.appendChild(empty);
    parent.appendChild(wrap);
    return wrap;
  }

  const row = document.createElement("div");
  row.className = "tag-chip-row";
  // The "" bucket ("(uten tag)", "Ukjent boligtype") sorts LAST regardless of
  // the vocabulary's own order, which puts it first under localeCompare. It is
  // the absence of a choice rather than a choice, and it carries the largest
  // count, so leading with it buries the values the user actually picked.
  // `emptyIsRealValue` opts out: tilgjengelighet's "" means "Til salgs", which
  // is a genuine status and its most common one -- burying it would be wrong.
  const ordered = emptyIsRealValue
    ? options
    : [...options].sort((a, b) => (a.key === "" ? 1 : b.key === "" ? -1 : 0));
  const allKeys = ordered.map((o) => o.key);
  ordered.forEach((opt) => {
    const btn = document.createElement("button");
    btn.type = "button";
    const color = colorFor ? colorFor(opt.key) : null;
    btn.style.setProperty("--tag-color", color || "#6f7e76");
    const paint = () => {
      const on = selected.includes(opt.key);
      btn.className = "tag-chip" + (on ? "" : " off") + (opt.key === "" ? " untagged" : "");
      btn.setAttribute("aria-pressed", String(on));
    };
    paints.push(paint);
    btn.textContent = opt.count != null ? `${opt.label} (${opt.count})` : opt.label;
    btn.addEventListener("click", () => {
      const next = applyChipClick(selected, opt.key, allKeys);
      selected.splice(0, selected.length, ...next);
      repaint();
      onChange();
    });
    paint();
    row.appendChild(btn);
  });
  wrap.appendChild(row);
  parent.appendChild(wrap);
  return wrap;
}

// The whole "Filtre" panel body: five selection chip rows,
// three collapsible slider sub-groups (collapse state persisted via
// ui.collapsed through onCollapse), and the unknown-value policy toggle.
// Replaces the old
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
  // Boligtype's "" bucket is synthesised here: meta.boligtyper is the list of
  // KNOWN types, so without this row a listing with no type would be
  // unselectable. The vocab-derived rows below already carry their own "".
  selectionChipRow(fields, {
    label: "Boligtype",
    options: [
      ...(meta.boligtyper || []).map((t) => ({ key: t, label: t })),
      { key: "", label: "Ukjent boligtype" },
    ],
    selected: filters.boligtypeSelected,
    colorFor: (key) => (colorByType && colorByType[key]) || null,
    onChange,
  });
  selectionChipRow(fields, {
    label: "Eieform",
    options: (meta.eieformer || []).map((v) => ({ key: v, label: v })),
    selected: filters.eieformSelected,
    onChange,
  });
  // Same synthesis as Boligtype above: meta.energimerker is the list of KNOWN
  // grades (A-G), so the ungraded listings need a row of their own to be
  // selectable at all -- and, more to the point, to be EXCLUDABLE by picking a
  // grade without them.
  selectionChipRow(fields, {
    label: "Energimerking",
    options: [
      ...(meta.energimerker || []).map((v) => ({ key: v, label: v })),
      { key: "", label: "Ukjent" },
    ],
    selected: filters.energiSelected,
    onChange,
  });
  // Fixed vocabularies, not vocab-derived: these three are closed enums owned
  // by the parser (skannonser/ingest/finn/parse_salgsoppgave.py), so every
  // option is known ahead of the data and none can go stale -- which is also
  // why pruneFilterSets deliberately leaves them alone. Labels match the popup
  // and table via listingmeta.js's formatters.
  selectionChipRow(fields, {
    label: "Ferdigattest",
    labelHint: SALGSOPPGAVE_HINT,
    options: FERDIGATTEST_OPTIONS,
    selected: filters.ferdigattestSelected,
    onChange,
  });
  selectionChipRow(fields, {
    label: "Utleie",
    labelHint: SALGSOPPGAVE_HINT,
    options: UTLEIE_OPTIONS,
    selected: filters.utleieSelected,
    onChange,
  });
  selectionChipRow(fields, {
    label: "Husdyr",
    labelHint: SALGSOPPGAVE_HINT,
    options: HUSDYR_OPTIONS,
    selected: filters.husdyrSelected,
    onChange,
  });
  selectionChipRow(fields, {
    label: "Tilgjengelighet",
    options: vocabs.tilgjengelighet,
    selected: filters.tilgjengelighetSelected,
    // "" is "Til salgs" here, a real status and the most common one -- it must
    // keep its by-count position rather than being sorted to the end.
    emptyIsRealValue: true,
    onChange,
  });
  const tagColors = assignTagColors(vocabs.tags.map((o) => o.key));
  selectionChipRow(fields, {
    label: "Tags",
    options: vocabs.tags,
    selected: filters.tagSelected,
    colorFor: (key) => colorForTag(key, tagColors),
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
