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
