// Sortable/filterable table view (Phase 5 Task 8): every /api/listings row
// in one <table>, click-to-sort headers, a text filter box, inline
// kommentar/tag editing (via the shared ./annotations.js save helper -- the
// same one popup.js's map-popup editor uses), a lazy Sold toggle, and a
// "Kart" link that hands off to `/#finnkode=...` (index.html/app.js's
// existing hash-focus handling).

import { saveAnnotation } from "./annotations.js";
import { isNew, fmtDate, premiumPct, fmtPremium } from "./listingmeta.js";
import { listingExcluded, deriveVocabs, tagChipRow, openPopover } from "./filters.js";
import { assignTagColors, colorForTag } from "./tagcolors.js";
import {
  loadFilters,
  saveFilters,
  activeFilterCount,
  subscribeOtherTabs,
  resetFilters,
} from "./filterstate.js";
import {
  isColumnFilterActive,
  makeFilterButton,
  openFacilitiesPopover,
  closePopover,
} from "./tablefilters.js";

const NOK = new Intl.NumberFormat("nb-NO");
const STORAGE_KEY = "skannonser.ui.v1"; // shared with app.js -- this page
// reads/writes `sold` and `hiddenColumns` in that blob (filters live there
// too, via filterstate.js); neither page needs to know the other's full
// UI-state shape, just its own fields within the shared blob.

// Columns whose values are compared numerically (nulls always sort last,
// regardless of sort direction -- see `compareItems`). Every other column
// sorts as case-insensitive text.
const NUMERIC_COLUMNS = new Set([
  "pris",
  "pris_kvm",
  "bra_i",
  "byggeaar",
  "brj",
  "mvv",
  "mvv_uni",
  "sold_price",
  "premium",
  "soverom",
  "etasje",
  "totalpris",
  "felleskost_mnd",
  "pris_kvm_totalpris",
  "maanedskost",
]);

// key: how a column's raw value is read off an item (travel columns reach
// into item.travel; premium is derived). label: header text. sortable: false
// only for the action-only Kart column. Kart sits right after Adresse so the
// map handoff never needs a horizontal scroll.
const COLUMNS = [
  { key: "adresse", label: "Adresse", sortable: true },
  { key: "kart", label: "Kart", sortable: false },
  { key: "scraped_at", label: "Først sett", sortable: true },
  { key: "postnummer", label: "Postnummer", sortable: true },
  { key: "pris", label: "Pris", sortable: true },
  { key: "pris_kvm", label: "Pris/kvm", sortable: true },
  { key: "totalpris", label: "Totalpris", sortable: true },
  { key: "pris_kvm_totalpris", label: "Total/kvm", sortable: true },
  { key: "felleskost_mnd", label: "Felleskost", sortable: true },
  { key: "maanedskost", label: "Mnd-kost", sortable: true },
  { key: "sold_price", label: "Solgt for", sortable: true },
  { key: "sold_date", label: "Solgt dato", sortable: true },
  { key: "premium", label: "Budpremie", sortable: true },
  { key: "bra_i", label: "BRA-i", sortable: true },
  { key: "soverom", label: "Sov", sortable: true },
  { key: "etasje", label: "Etg", sortable: true },
  { key: "boligtype", label: "Boligtype", sortable: true },
  { key: "eieform", label: "Eieform", sortable: true },
  { key: "byggeaar", label: "Byggeår", sortable: true },
  { key: "energimerke", label: "Energi", sortable: true },
  { key: "brj", label: "BRJ", sortable: true },
  { key: "mvv", label: "MVV", sortable: true },
  { key: "mvv_uni", label: "UNI", sortable: true },
  { key: "tilgjengelighet", label: "Tilgjengelighet", sortable: true },
  { key: "kommentar", label: "Kommentar", sortable: true },
  { key: "tag", label: "Tag", sortable: true },
];

// Column picker (2026-07-25 spec §7): first-run default hides the noise
// columns (Pris/Felleskost are semi-redundant with Totalpris/Mnd-kost).
// Adresse and Kart are load-bearing (identity + map handoff) -- not hideable.
const DEFAULT_HIDDEN_COLUMNS = ["postnummer", "pris", "felleskost_mnd", "soverom", "etasje"];
const ALWAYS_VISIBLE_COLUMNS = new Set(["adresse", "kart"]);

function loadHiddenColumns() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const stored = raw ? JSON.parse(raw).hiddenColumns : null;
    return new Set(Array.isArray(stored) ? stored : DEFAULT_HIDDEN_COLUMNS);
  } catch (_) {
    return new Set(DEFAULT_HIDDEN_COLUMNS);
  }
}

function saveHiddenColumns() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const blob = raw ? JSON.parse(raw) : {};
    blob.hiddenColumns = [...state.hiddenColumns];
    localStorage.setItem(STORAGE_KEY, JSON.stringify(blob));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

function visibleColumns() {
  return COLUMNS.filter((c) => !state.hiddenColumns.has(c.key));
}

const state = {
  items: [], // all loaded items (eie + dnb, + sold once toggled on)
  soldLoaded: false,
  showSold: false, // tracks "Vis solgte" toggle state; sold items stay in items
  focusFinnkode: null, // deep-linked row (map popup "Tabell" handoff): exempt from filters
  sortKey: "scraped_at", // newest first: the scanner's daily question
  sortDir: "desc",
  filterText: "",
  meta: null,
  filters: null,
  vocabs: null,
  hiddenColumns: loadHiddenColumns(),
  tagColors: new Map(), // assigned for real in refreshVocabs; declared here
  // (matching app.js's state shape) so the field is self-documenting.
};

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
  // Same rule as the map (app.js vocabItems): the vocabulary describes the
  // rows the user can see. `state.items` only ever grows, so without this the
  // tag chips keep values that only closed rows carried after "Vis solgte" is
  // switched back off.
  const visible = state.showSold ? state.items : state.items.filter((it) => !it.closed);
  state.vocabs = deriveVocabs(visible);
  state.tagColors = assignTagColors(state.vocabs.tags.map((o) => o.key));
}

function fmtPris(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return NOK.format(Math.round(n));
}

function setStatus(text) {
  const node = document.getElementById("table-status");
  if (node) node.textContent = text || "";
}

function loadSoldPref() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return false;
    return !!JSON.parse(raw).sold;
  } catch (_) {
    return false;
  }
}

function saveSoldPref(sold) {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const stored = raw ? JSON.parse(raw) : {};
    stored.sold = sold;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(stored));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

// sold=truthy fetches ONLY the sold bucket (?bucket=sold) -- the actives are
// already loaded, so the old merged ?sold=1 shape just re-shipped them.
async function fetchListings(sold) {
  const resp = await fetch("/api/listings" + (sold ? "?bucket=sold" : ""));
  if (!resp.ok) throw new Error("HTTP " + resp.status);
  const data = await resp.json();
  return data.listings || [];
}

function cellValue(item, key) {
  switch (key) {
    case "brj":
    case "mvv":
    case "mvv_uni":
      return (item.travel || {})[key];
    case "premium":
      return premiumPct(item);
    case "kart":
      return null;
    default:
      return item[key];
  }
}

function isBlank(v) {
  return v === null || v === undefined || v === "";
}

// Nulls sort last no matter the direction: only a defined-vs-defined pair
// gets its comparison flipped by `dir`.
function compareItems(a, b, key, dir) {
  const av = cellValue(a, key);
  const bv = cellValue(b, key);
  const aBlank = isBlank(av);
  const bBlank = isBlank(bv);
  if (aBlank && bBlank) return 0;
  if (aBlank) return 1;
  if (bBlank) return -1;

  let cmp;
  if (NUMERIC_COLUMNS.has(key)) {
    cmp = Number(av) - Number(bv);
  } else {
    cmp = String(av).localeCompare(String(bv), "nb", { sensitivity: "base" });
  }
  return dir === "asc" ? cmp : -cmp;
}

function matchesFilter(item, text) {
  if (!text) return true;
  const needle = text.toLowerCase();
  // Includes kommentar/tag so your own notes are searchable.
  return [item.adresse, item.postnummer, item.boligtype, item.kommentar, item.tag].some(
    (v) => !isBlank(v) && String(v).toLowerCase().includes(needle)
  );
}

function visibleRows() {
  const filtered = state.items.filter((item) => {
    if (state.focusFinnkode && String(item.finnkode) === state.focusFinnkode) return true;
    if (!state.showSold && item.closed) return false;
    if (listingExcluded(item, state.filters, state.meta)) return false;
    return matchesFilter(item, state.filterText);
  });
  filtered.sort((a, b) => compareItems(a, b, state.sortKey, state.sortDir));
  return filtered;
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
}

function renderHead() {
  const row = document.getElementById("table-head-row");
  row.innerHTML = "";
  visibleColumns().forEach((col) => {
    const th = el("th", null, col.label);
    if (col.sortable) {
      th.classList.add("sortable");
      if (state.sortKey === col.key) {
        th.classList.add(state.sortDir === "asc" ? "sort-asc" : "sort-desc");
      }
      th.addEventListener("click", () => {
        if (state.sortKey === col.key) {
          state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
          state.sortKey = col.key;
          state.sortDir = "asc";
        }
        render();
      });
    }
    const filterBtn = makeFilterButton(col.key, filterCtx());
    if (filterBtn) {
      th.appendChild(filterBtn);
      if (isColumnFilterActive(col.key, filterCtx())) th.classList.add("filter-active");
    }
    row.appendChild(th);
  });
}

// Server-side normalization mirrored here (see annotations.js's
// saveAnnotation payload) so the dirty-check below compares like with like:
// "" and null and "  " must all be treated as the same (unset) value.
function normalizeAnnotationValue(v) {
  return (v || "").trim() || null;
}

// Wires blur/Enter-commit for one inline kommentar/tag <input>. `field` is
// "kommentar" or "tag"; the OTHER field's current value always comes off
// `item` (already-saved state), so a save only ever changes the one field
// the user actually edited.
function wireCellEdit(input, item, field) {
  let saving = false;
  const commit = async () => {
    if (saving) return;
    const kommentar = field === "kommentar" ? input.value : item.kommentar;
    const tag = field === "tag" ? input.value : item.tag;
    // Skip the PUT when the edited field didn't actually change from the
    // last-saved item state (e.g. tabbing/clicking through a cell without
    // typing, which still fires `blur`). WHY this matters: every PUT bumps
    // the row's updated_at even when the payload is byte-identical, and a
    // bumped updated_at is exactly the signal sheet-import protection uses
    // to treat an import-created row as "user has edited this, don't
    // overwrite it" -- so a no-op blur was silently and permanently
    // flipping that protection on for rows nobody actually touched.
    if (
      normalizeAnnotationValue(kommentar) === normalizeAnnotationValue(item.kommentar) &&
      normalizeAnnotationValue(tag) === normalizeAnnotationValue(item.tag)
    ) {
      return;
    }
    saving = true;
    input.classList.remove("saved", "error");
    try {
      const saved = await saveAnnotation(item.finnkode, kommentar, tag);
      item.kommentar = saved.kommentar;
      item.tag = saved.tag;
      input.value = saved[field] || "";
      input.classList.add("saved");
      setTimeout(() => input.classList.remove("saved"), 1500);
      // Tag vocab may have gained a new value -- refresh it and re-render so
      // the Tag column filter's option list and any active tag filter both
      // reflect it. render() rebuilds the table body (this input included),
      // which is fine: commit only fires on blur/Enter, so the user is done
      // editing by the time we get here, and the `saving` guard above (plus
      // blur already having fired) means the now-detached input can't
      // re-trigger commit.
      refreshVocabs();
      render();
    } catch (err) {
      input.classList.add("error");
    } finally {
      saving = false;
    }
  };
  input.addEventListener("blur", commit);
  input.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      input.blur(); // triggers `commit` via the blur listener above
    }
  });
}

function buildRow(item) {
  const tr = el("tr", item.sold ? "sold-row" : item.closed ? "inactive-row" : null);
  tr.dataset.finnkode = item.finnkode;

  visibleColumns().forEach((col) => {
    const td = el("td");
    switch (col.key) {
      case "adresse": {
        if (item.url) {
          const a = el("a", null, item.adresse || "(ukjent adresse)");
          a.href = item.url;
          a.target = "_blank";
          a.rel = "noopener";
          td.appendChild(a);
        } else {
          td.textContent = item.adresse || "(ukjent adresse)";
        }
        if (item.sold) td.appendChild(el("span", "sold-badge", "Solgt"));
        else if (item.closed) td.appendChild(el("span", "inactive-badge", item.tilgjengelighet));
        if (isNew(item)) td.appendChild(el("span", "ny-badge", "Ny"));
        break;
      }
      case "scraped_at": {
        td.textContent = fmtDate(item.scraped_at) || "";
        td.classList.add("num");
        break;
      }
      case "pris":
      case "pris_kvm":
      case "totalpris":
      case "pris_kvm_totalpris":
      case "felleskost_mnd":
      case "maanedskost":
      case "sold_price": {
        const formatted = fmtPris(item[col.key]);
        td.textContent = formatted || "";
        td.classList.add("num");
        break;
      }
      case "sold_date": {
        td.textContent = fmtDate(item.sold_date) || "";
        td.classList.add("num");
        break;
      }
      case "premium": {
        const pct = premiumPct(item);
        if (pct != null) {
          td.appendChild(
            el("span", pct >= 0 ? "premie-pos" : "premie-neg", fmtPremium(pct))
          );
        }
        td.classList.add("num");
        break;
      }
      case "bra_i":
      case "soverom":
      case "etasje":
      case "byggeaar":
      case "brj":
      case "mvv":
      case "mvv_uni": {
        const v = cellValue(item, col.key);
        td.textContent = isBlank(v) ? "" : String(v);
        td.classList.add("num");
        break;
      }
      case "kommentar":
      case "tag": {
        const input = el("input");
        input.type = "text";
        input.value = item[col.key] || "";
        input.className = "cell-edit";
        wireCellEdit(input, item, col.key);
        td.appendChild(input);
        if (col.key === "tag") {
          // Saved-tag accent; a save triggers render() so this repaints.
          const color = colorForTag(item.tag, state.tagColors || new Map());
          if (color) {
            td.style.boxShadow = "inset 3px 0 0 " + color;
            td.style.background = color + "14"; // ~8% alpha tint
          }
        }
        break;
      }
      case "kart": {
        if (item.lat != null && item.lng != null) {
          const a = el("a", null, "Kart");
          a.href = "/#finnkode=" + encodeURIComponent(item.finnkode);
          td.appendChild(a);
        }
        break;
      }
      default:
        td.textContent = isBlank(item[col.key]) ? "" : String(item[col.key]);
    }
    tr.appendChild(td);
  });

  return tr;
}

function render() {
  renderHead();
  const chipMount = document.getElementById("table-tag-chips");
  if (chipMount) {
    chipMount.innerHTML = "";
    tagChipRow(chipMount, {
      options: state.vocabs.tags,
      hidden: state.filters.tagHidden,
      tagColors: state.tagColors,
      onChange: onFilterChange,
    });
  }
  const body = document.getElementById("table-body");
  body.innerHTML = "";
  const rows = visibleRows();
  rows.forEach((item) => body.appendChild(buildRow(item)));
  const n = activeFilterCount(state.filters, state.meta);
  setStatus(
    rows.length + " av " + state.items.length + " annonser" +
    (n ? " · " + n + " filtre aktive" : "")
  );
  // Runs on every render (onFilterChange + cross-tab sync included) so the
  // button's active cue never drifts from the actual filter state.
  const facBtn = document.getElementById("facilities-filter-btn");
  if (facBtn) {
    const hasFacilities =
      Object.keys(state.filters.facilitiesRequired || {}).length > 0;
    facBtn.classList.toggle("active", hasFacilities);
  }
}

function wireToolbar() {
  const filterInput = document.getElementById("table-filter");
  filterInput.addEventListener("input", () => {
    state.filterText = filterInput.value.trim();
    render();
  });

  const soldToggle = document.getElementById("table-sold");
  soldToggle.checked = loadSoldPref();
  state.showSold = soldToggle.checked;
  soldToggle.addEventListener("change", async () => {
    state.showSold = soldToggle.checked;
    saveSoldPref(soldToggle.checked);
    if (soldToggle.checked && !state.soldLoaded) {
      soldToggle.disabled = true;
      setStatus("Laster solgte …");
      try {
        state.items = state.items.concat(await fetchListings(1));
        state.soldLoaded = true;
        refreshVocabs();
      } catch (err) {
        setStatus("Kunne ikke laste solgte: " + err.message);
      } finally {
        soldToggle.disabled = false;
      }
    }
    // The off path changes the vocabulary boundary just as much as the on path.
    refreshVocabs();
    render();
  });

  const facBtn = document.getElementById("facilities-filter-btn");
  if (facBtn) {
    facBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openFacilitiesPopover(facBtn, filterCtx());
    });
  }

  const colsBtn = document.getElementById("table-columns-btn");
  if (colsBtn) {
    colsBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openPopover(colsBtn, (pop) => {
        const wrap = el("div", "filter-row checkbox-group");
        wrap.appendChild(el("div", "filter-head", "Vis kolonner"));
        COLUMNS.filter((c) => !ALWAYS_VISIBLE_COLUMNS.has(c.key)).forEach((col) => {
          const row = el("label", "toggle");
          const cb = el("input");
          cb.type = "checkbox";
          cb.checked = !state.hiddenColumns.has(col.key);
          cb.addEventListener("change", () => {
            if (cb.checked) state.hiddenColumns.delete(col.key);
            else state.hiddenColumns.add(col.key);
            saveHiddenColumns();
            render();
          });
          row.appendChild(cb);
          row.appendChild(document.createTextNode(col.label));
          wrap.appendChild(row);
        });
        pop.appendChild(wrap);
      });
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
}

// Receiving end of the popup's "Tabell" deep link -- mirror of app.js's
// handleHash. The focused row bypasses filters (a deep link onto an
// empty-looking table reads as broken) and gets a flash so the eye lands.
async function handleHash() {
  const raw = decodeURIComponent(window.location.hash.replace(/^#/, ""));
  state.focusFinnkode = null;
  if (!raw) {
    render();
    return;
  }
  const finnkode = raw.startsWith("finnkode=") ? raw.slice("finnkode=".length) : raw;
  const byId = () => state.items.find((it) => String(it.finnkode) === finnkode);
  let item = byId();
  // Deep links to closed listings can arrive before the lazily-fetched
  // sold bucket on a cold load -- pull it and retry (same race app.js solves).
  if (!item && !state.soldLoaded) {
    setStatus("Laster solgte …");
    try {
      state.items = state.items.concat(await fetchListings(1));
      state.soldLoaded = true;
      refreshVocabs();
    } catch (_) {
      /* fall through; not-found reported below */
    }
    item = byId();
  }
  if (!item) {
    render();
    setStatus("Fant ikke annonse " + finnkode);
    return;
  }
  if (item.closed && !state.showSold) {
    state.showSold = true;
    const soldToggle = document.getElementById("table-sold");
    if (soldToggle) soldToggle.checked = true;
    saveSoldPref(true);
  }
  state.focusFinnkode = finnkode;
  render();
  const row = document.querySelector('tr[data-finnkode="' + finnkode + '"]');
  if (row) {
    row.scrollIntoView({ block: "center" });
    row.classList.add("row-flash");
    setTimeout(() => row.classList.remove("row-flash"), 2400);
  }
}

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
  if (window.location.hash) await handleHash();
  window.addEventListener("hashchange", handleHash);
}

init();
