// Sortable/filterable table view (Phase 5 Task 8): every /api/listings row
// in one <table>, click-to-sort headers, a text filter box, inline
// kommentar/tag editing (via the shared ./annotations.js save helper -- the
// same one popup.js's map-popup editor uses), the shared Status popover
// (filters.tilgjengelighetSelected, which lazily pulls in the closed bucket
// the first time it is asked for), and a "Kart" link that hands off to
// `/#finnkode=...` (index.html/app.js's existing hash-focus handling).

import { commitAnnotation } from "./annotations.js";
import {
  isNew, fmtDate, premiumPct, fmtPremium, travelMinutes,
  fmtJaNei, fmtOmtalt, fmtFerdigattest, fmtUtleie, fmtHusdyr, fmtAlvorlighet,
  resolveHiddenColumns, applyTilstandColumnsMigration,
  SALGSOPPGAVE_DERIVED, SALGSOPPGAVE_HINT, TILSTAND_DERIVED, TILSTAND_HINT,
  labelWithSource, TILGJENGELIGHET_OPTIONS,
} from "./listingmeta.js";
import {
  listingExcluded,
  deriveVocabs,
  selectionChipRow,
  openPopover,
  selectionExcludes,
  statusVocabComplete,
  wantsClosed,
} from "./filters.js";
import { assignTagColors, colorForTag } from "./tagcolors.js";
import { attachTagList, syncTagOptions } from "./tagoptions.js";
import {
  loadFilters,
  saveFilters,
  activeFilterCount,
  subscribeOtherTabs,
  resetFilters,
  pruneFilterSets,
  seedStatus,
} from "./filterstate.js";
import {
  isColumnFilterActive,
  makeFilterButton,
  openFacilitiesPopover,
  closePopover,
} from "./tablefilters.js";

const NOK = new Intl.NumberFormat("nb-NO");
const STORAGE_KEY = "skannonser.ui.v1"; // shared with app.js -- this page
// reads/writes `hiddenColumns` in that blob (filters, including the status
// selection, live there too via filterstate.js); neither page needs to know
// the other's full UI-state shape, just its own fields within the shared blob.

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
  "tg3_count",
  "reparasjon_est",
  "reparasjon_usikkerhet",
  // alvorlighet itself is a text enum, but cellValue() below maps it onto
  // ALVORLIGHET_ORDER's severity rank, and that rank must be compared
  // numerically -- alphabetical would sort "alvorlig" before "kosmetisk".
  "alvorlighet",
]);

// Severity rank used only for sorting (see cellValue's "alvorlighet" case);
// display always goes through fmtAlvorlighet on the raw string.
const ALVORLIGHET_ORDER = { kosmetisk: 0, mindre: 1, vesentlig: 2, alvorlig: 3 };

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
  // Salgsoppgave enrichment (migration 015). Default-hidden: eight more
  // columns is a lot of horizontal scroll to impose on everyone, and most
  // readers want one or two of these, not all of them.
  { key: "ferdigattest", label: "Ferdigattest", sortable: true },
  { key: "eiendomsskatt_kr", label: "Eiendomsskatt", sortable: true },
  { key: "verditakst", label: "Verditakst", sortable: true },
  { key: "utleie", label: "Utleie", sortable: true },
  { key: "husdyr", label: "Husdyr", sortable: true },
  { key: "heftelser", label: "Heftelser", sortable: true },
  { key: "radon_omtalt", label: "Radon", sortable: true },
  { key: "boligselgerforsikring", label: "Selgerforsikring", sortable: true },
  // Tilstand classifier (migration 016). Default-hidden like the salgsoppgave
  // columns just above, and listed in their own migration array (see
  // TILSTAND_COLUMNS) so stored column preferences that predate them hide
  // them once without re-hiding a column a reader has since chosen to show.
  { key: "tg3_count", label: "TG3", sortable: true },
  { key: "reparasjon_est", label: "Utbedring", sortable: true },
  { key: "reparasjon_usikkerhet", label: "Usikkerhet", sortable: true },
  { key: "alvorlighet", label: "Alvorlighet", sortable: true },
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
// First-run defaults. `tilgjengelighet` is empty for every active listing in
// production (0 of 770) -- it stays in the picker for anyone who wants it, but
// costs a column of horizontal scroll by default for nothing.
// Added by migration 015; listed separately so the one-time migration in
// `loadHiddenColumns` can hide them for readers whose stored preferences
// predate them, without re-hiding a column they have since chosen to show.
const SALGSOPPGAVE_COLUMNS = [
  "ferdigattest", "eiendomsskatt_kr", "verditakst", "utleie", "husdyr",
  "heftelser", "radon_omtalt", "boligselgerforsikring",
];
// Migration 016's three columns, hidden by default the same way. Kept as its
// OWN migration array with its own `tilstandColumnsDefaulted` flag below,
// rather than folded into SALGSOPPGAVE_COLUMNS: a reader who already passed
// the 015 migration has `salgsoppgaveColumnsDefaulted: true`, and
// resolveHiddenColumns checks exactly that one flag -- reusing it here would
// make it skip hiding these new columns for every reader who upgraded
// through 015 before 016 existed.
const TILSTAND_COLUMNS = [
  "tg3_count", "reparasjon_est", "reparasjon_usikkerhet", "alvorlighet",
];
const DEFAULT_HIDDEN_COLUMNS = [
  "postnummer", "pris", "felleskost_mnd", "soverom", "etasje", "tilgjengelighet",
  ...SALGSOPPGAVE_COLUMNS,
  ...TILSTAND_COLUMNS,
];
const ALWAYS_VISIBLE_COLUMNS = new Set(["adresse", "kart"]);

function loadHiddenColumns() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const stored = raw ? JSON.parse(raw) : null;
    const hidden = resolveHiddenColumns(stored, DEFAULT_HIDDEN_COLUMNS, SALGSOPPGAVE_COLUMNS);
    return applyTilstandColumnsMigration(hidden, stored, TILSTAND_COLUMNS);
  } catch (_) {
    return new Set(DEFAULT_HIDDEN_COLUMNS);
  }
}

function saveHiddenColumns() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const blob = raw ? JSON.parse(raw) : {};
    blob.hiddenColumns = [...state.hiddenColumns];
    blob.salgsoppgaveColumnsDefaulted = true;
    blob.tilstandColumnsDefaulted = true;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(blob));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

function visibleColumns() {
  return COLUMNS.filter((c) => !state.hiddenColumns.has(c.key));
}

const state = {
  items: [], // all loaded items (eie + dnb, + closed once the Status filter asks for it)
  soldLoaded: false,
  soldPromise: null, // in-flight ensureSoldBucket, so concurrent callers share one fetch
  statusError: null, // one-shot: a fetch failure message that must survive the
  // next render() (which otherwise immediately overwrites the status line
  // with the row count); render() shows it once and clears it
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
  // rows the user can see, and state.items only ever grows.
  const visible = state.items.filter(
    (it) => !selectionExcludes(state.filters.tilgjengelighetSelected, it.tilgjengelighet || "")
  );
  state.vocabs = deriveVocabs(visible);
  const vocabComplete =
    statusVocabComplete(state.filters.tilgjengelighetSelected) && state.soldLoaded;
  if (pruneFilterSets(state.filters, state.vocabs, vocabComplete)) saveFilters(state.filters);
  state.tagColors = assignTagColors(state.vocabs.tags.map((o) => o.key));
  // Same source as the colours, so a tag saved in a cell is suggestable in the
  // next one without a reload: wireCellEdit re-derives the vocab on every save.
  syncTagOptions(state.vocabs.tags.map((o) => o.key));
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

// Fetch the closed bucket exactly once, no matter who asks or why. The
// in-flight promise is memoized on state.soldPromise so concurrent callers
// (e.g. Solgt unchecked then re-checked while the first ~3500-row fetch is
// still outstanding, or a popover selection change racing a hashchange) share
// the one fetch instead of each concat-ing their own response into
// state.items and duplicating every closed row. Deliberately unconditional --
// "should this fetch happen at all" is the caller's decision (see
// ensureSoldForSelection, which gates on the status selection, and
// handleHash, which needs the bucket regardless of selection for a deep link
// to resolve) -- so both can share this one memo instead of each keeping its
// own promise and racing the other's write to state.items. Mirrors app.js's
// ensureSoldLoaded. Rejects on a failed fetch so callers that need to know --
// e.g. to roll back a selection that just asked for rows which never arrived
// -- can catch it; the memo is cleared either way so a later retry can still
// succeed. The failure message is stashed on state.statusError rather than
// just setStatus() here, because the render() that follows shortly after
// (onFilterChange, or handleHash's own render) would otherwise immediately
// overwrite it with the row count -- render() checks statusError first and
// consumes it, so the message survives that one render.
function ensureSoldBucket() {
  if (state.soldLoaded) return Promise.resolve();
  if (state.soldPromise) return state.soldPromise;
  setStatus("Laster solgte …");
  state.soldPromise = (async () => {
    try {
      state.items = state.items.concat(await fetchListings(1));
      state.soldLoaded = true;
      refreshVocabs();
    } catch (err) {
      state.statusError = "Kunne ikke laste solgte: " + err.message;
      setStatus(state.statusError);
      throw err;
    }
  })().finally(() => {
    state.soldPromise = null;
  });
  return state.soldPromise;
}

// Fetch the closed bucket if the current status selection asks for it and it
// is not already loaded. Idempotent and safe to call on every filter change;
// the fetch itself (including its in-flight memoization) lives in
// ensureSoldBucket -- this just adds the selection-driven gate on top.
function ensureSoldForSelection() {
  if (!wantsClosed(state.filters.tilgjengelighetSelected)) return Promise.resolve();
  return ensureSoldBucket();
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
      // Not a raw read: travelMinutes nulls out the pipeline's negative
      // failure codes, which puts them on the blank path below (empty cell,
      // and `compareItems` sorts them last) instead of printing "-1" and
      // sorting it ahead of every real commute.
      return travelMinutes(item, key);
    case "premium":
      return premiumPct(item);
    case "alvorlighet": {
      // Sort by severity, not alphabetically: raw values are the enum keys
      // (kosmetisk/mindre/vesentlig/alvorlig), and localeCompare on those
      // would put "alvorlig" before "kosmetisk". An unmapped/missing value
      // returns null, which compareItems' isBlank() sorts last either way.
      const rank = ALVORLIGHET_ORDER[item.alvorlighet];
      return rank === undefined ? null : rank;
    }
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
    const th = el("th", null, labelWithSource(col.key, col.label));
    // Mark the prose-derived columns: their blanks mean "the prospectus did not
    // say", not "no", and that is worth knowing before filtering on one.
    if (SALGSOPPGAVE_DERIVED.has(col.key)) {
      th.classList.add("from-salgsoppgave");
      th.title = SALGSOPPGAVE_HINT;
    } else if (TILSTAND_DERIVED.has(col.key)) {
      // Distinct from the regex-derived marker above, and deliberately louder:
      // these are a model's judgement, not a pattern match over the prose.
      th.classList.add("from-llm");
      th.title = TILSTAND_HINT;
    }
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

// Wires blur/Enter-commit for one inline kommentar/tag <input>. `field` is
// "kommentar" or "tag"; the OTHER field's current value always comes off
// `item` (already-saved state), so a save only ever changes the one field the
// user actually edited. The skip-when-unchanged guard lives in
// commitAnnotation -- it returns null when it sent nothing.
function wireCellEdit(input, item, field) {
  let saving = false;
  const commit = async () => {
    if (saving) return;
    const kommentar = field === "kommentar" ? input.value : item.kommentar;
    const tag = field === "tag" ? input.value : item.tag;
    saving = true;
    input.classList.remove("saved", "error");
    try {
      const saved = await commitAnnotation(item, { kommentar, tag });
      if (!saved) return; // nothing changed; no PUT was sent
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
    // Tint the whole column, not just its header: a reader scanning rows
    // should see at a glance which numbers a model produced.
    if (TILSTAND_DERIVED.has(col.key)) td.classList.add("from-llm-cell");
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
      case "eiendomsskatt_kr":
      case "verditakst": {
        td.textContent = fmtPris(item[col.key]) || "";
        td.classList.add("num");
        break;
      }
      case "ferdigattest": {
        td.textContent = fmtFerdigattest(item.ferdigattest) || "";
        break;
      }
      case "utleie": {
        td.textContent = fmtUtleie(item.utleie) || "";
        break;
      }
      case "husdyr": {
        td.textContent = fmtHusdyr(item.husdyr) || "";
        break;
      }
      case "heftelser":
      case "radon_omtalt": {
        // Omtalt/Ikke omtalt, NOT Ja/Nei: these detect whether the prospectus
        // mentions the topic at all. "Radon: Ja" reads as a radon problem.
        td.textContent = fmtOmtalt(item[col.key]) || "";
        break;
      }
      case "boligselgerforsikring": {
        // A true yes/no: the parser distinguishes "har tegnet" from "har ikke
        // tegnet". Via fmtJaNei, not the default branch, since `String(false)`
        // would print the literal "false".
        td.textContent = fmtJaNei(item.boligselgerforsikring) || "";
        break;
      }
      case "tg3_count": {
        td.textContent = item.tg3_count ?? "";
        td.classList.add("num");
        break;
      }
      case "reparasjon_usikkerhet": {
        // The +/- half-width of the repair-cost range. Utbedring alone is a
        // midpoint, and on a listing spanning 1.0-2.7M that reads far more
        // precise than it is; this is the spread that midpoint hides.
        const u = fmtPris(item.reparasjon_usikkerhet);
        td.textContent = u ? "\u00b1 " + u : "";
        td.classList.add("num");
        break;
      }
      case "reparasjon_est": {
        const v = fmtPris(item.reparasjon_est);
        // "~" hedges a model estimate; a plain figure means the surveyor's
        // own number (reparasjon_kilde === "takst") came through unchanged.
        td.textContent = v
          ? (item.reparasjon_kilde === "takst" ? v : "~" + v)
          : "";
        td.classList.add("num");
        break;
      }
      case "alvorlighet": {
        td.textContent = fmtAlvorlighet(item.alvorlighet) || "";
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
        if (col.key === "tag") attachTagList(input); // existing tags, as a dropdown
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
    // No label: the toolbar has no room for a heading, and these chips sit
    // beside buttons that already name themselves.
    selectionChipRow(chipMount, {
      options: state.vocabs.tags,
      selected: state.filters.tagSelected,
      colorFor: (key) => colorForTag(key, state.tagColors),
      onChange: onFilterChange,
    });
  }
  const body = document.getElementById("table-body");
  body.innerHTML = "";
  const rows = visibleRows();
  rows.forEach((item) => body.appendChild(buildRow(item)));
  if (!rows.length && state.items.length) {
    const tr = el("tr");
    const td = el("td", "empty-row", "Ingen annonser vises med gjeldende lag og filtre. ");
    td.colSpan = visibleColumns().length;
    const btn = el("button", null, "Nullstill filtre");
    btn.type = "button";
    btn.addEventListener("click", () => {
      resetFilters(state.filters, state.meta);
      saveFilters(state.filters);
      // Also restore the status selection to its unfiltered floor. The
      // message names layers/filters as a possible cause of the empty table,
      // so the button must be able to undo a status selection too -- a
      // filters-only reset leaves a user who emptied the table via the Status
      // popover clicking on nothing. resetFilters always clears
      // tilgjengelighetSelected first, so seedStatus always re-floors it to
      // [""] here -- which never wants the closed bucket -- so this path
      // skips ensureSoldForSelection, same as the toolbar reset below.
      seedStatus(state.filters);
      refreshVocabs();
      render();
    });
    td.appendChild(btn);
    tr.appendChild(td);
    body.appendChild(tr);
  }
  if (state.statusError) {
    // Show the stashed failure once, then fall back to the normal row-count
    // line on every render after this one.
    setStatus(state.statusError);
    state.statusError = null;
  } else {
    const n = activeFilterCount(state.filters, state.meta);
    setStatus(
      rows.length + " av " + state.items.length + " annonser" +
      (n ? " · " + n + " filtre aktive" : "")
    );
  }
  // Runs on every render (onFilterChange + cross-tab sync included) so the
  // button's active cue never drifts from the actual filter state.
  const facBtn = document.getElementById("facilities-filter-btn");
  if (facBtn) {
    const hasFacilities =
      Object.keys(state.filters.facilitiesRequired || {}).length > 0;
    facBtn.classList.toggle("active", hasFacilities);
  }
}

// The Status popover's body. Extracted so it can be re-invoked from inside
// its own onChange: selectionChipRow repaints its chips BEFORE calling
// onChange, so if this handler's seedStatus (or the failure rollback below)
// changes the selection, the chips just painted are already stale -- e.g.
// clicking "Til salgs" from [""] empties the array, paints all-off, and only
// then does seedStatus put "" back, leaving the chip reading "off" while it
// IS selected. Re-mounting from scratch (innerHTML + a fresh selectionChipRow
// call) re-derives the paint from the real post-seed state instead. Safe
// against recursion/double-binding: this only re-runs from an actual click,
// never synchronously from within itself, and clearing `pop` first discards
// the old chip buttons (and their listeners) rather than layering new ones
// on top. Mirrors app.js's wireStatusToggles, which solves the same problem
// by rebuilding its checkbox mount.
function renderStatusPopover(pop) {
  // Snapshot before this paint's click can mutate the array in place --
  // exactly the selection to roll back to if the click's fetch fails below.
  const before = state.filters.tilgjengelighetSelected.slice();
  pop.innerHTML = "";
  // Counts come from the derived vocabulary when the bucket is loaded, but
  // the OPTION LIST is the fixed constant -- a status absent from the loaded
  // items still needs a control, or there is nothing to click to trigger the
  // fetch that would produce it.
  const counts = new Map(state.vocabs.tilgjengelighet.map((o) => [o.key, o.count]));
  selectionChipRow(pop, {
    label: "Status",
    options: TILGJENGELIGHET_OPTIONS.map((o) => ({ ...o, count: counts.get(o.key) })),
    selected: state.filters.tilgjengelighetSelected,
    emptyIsRealValue: true,
    onChange: async () => {
      seedStatus(state.filters);
      try {
        await ensureSoldForSelection();
      } catch (_) {
        // Fetch failed. ensureSoldBucket already stashed the failure message
        // on state.statusError so it survives the render() a few lines below
        // (onFilterChange -> render() would otherwise clobber a plain
        // setStatus() call with the row count in the same microtask). Undo
        // this click: `before` still holds the row-count-accurate selection so a
        // failed "Solgt" fetch can't leave the filter claiming rows that were
        // never loaded, which would otherwise also cross to the map via
        // saveFilters. Re-seed in case `before` was itself []; app.js's
        // wireStatusToggles rolls back the same way for the same reason.
        state.filters.tilgjengelighetSelected.splice(
          0, state.filters.tilgjengelighetSelected.length, ...before
        );
        seedStatus(state.filters);
      }
      // Every status change moves the vocabulary boundary, fetch or not: an
      // uncheck, or a re-check once soldLoaded is already true, never reaches
      // ensureSoldForSelection's own refreshVocabs call, and without this the
      // toolbar's tag chips and the popover's own counts go stale.
      refreshVocabs();
      // Repaint from the (possibly reseeded, possibly rolled-back) state --
      // see the function comment above.
      renderStatusPopover(pop);
      onFilterChange();
    },
  });
}

function wireToolbar() {
  const filterInput = document.getElementById("table-filter");
  filterInput.addEventListener("input", () => {
    state.filterText = filterInput.value.trim();
    render();
  });

  const statusBtn = document.getElementById("table-status-btn");
  if (statusBtn) {
    statusBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openPopover(statusBtn, (pop) => renderStatusPopover(pop));
    });
  }

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
          // The picker names the same columns, so it carries the same marker.
          const name = document.createElement("span");
          name.textContent = labelWithSource(col.key, col.label);
          if (TILSTAND_DERIVED.has(col.key)) {
            name.classList.add("from-llm");
            name.title = TILSTAND_HINT;
          } else if (SALGSOPPGAVE_DERIVED.has(col.key)) {
            name.classList.add("from-salgsoppgave");
            name.title = SALGSOPPGAVE_HINT;
          }
          row.appendChild(name);
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
      // resetFilters restores defaultFilters, whose tilgjengelighetSelected
      // is [] -- but [] means "unfiltered" everywhere except status, where it
      // collides with the lazily-fetched closed bucket: an empty selection
      // reached here (closed rows possibly already loaded this session) would
      // show the ~4387-row table instead of the ~867-row floor a cold load
      // shows for the same stored value. Re-seed so the reset lands on the
      // floor like every other reset path. [""] is Til salgs only, so this
      // never needs the closed bucket -- same as the empty-state reset above.
      seedStatus(state.filters);
      // The reset always lands the selection back on [""] (see comment
      // above), which narrows what's visible after a session that had
      // widened it -- state.vocabs must be re-derived or the tag chips and
      // Status popover counts would keep describing the wider, pre-reset set.
      refreshVocabs();
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
  // Deep links to closed listings can arrive before the lazily-fetched sold
  // bucket on a cold load -- pull it and retry (same race app.js solves).
  // Goes through ensureSoldBucket unconditionally (NOT ensureSoldForSelection,
  // which would skip the fetch here: the selection is still [""] at this
  // point on a cold load) so this shares the one memoized fetch with the
  // Status popover instead of racing it with a second ~3500-row request.
  if (!item && !state.soldLoaded) {
    try {
      await ensureSoldBucket();
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
  const status = item.tilgjengelighet || "";
  if (selectionExcludes(state.filters.tilgjengelighetSelected, status)) {
    state.filters.tilgjengelighetSelected.push(status);
    saveFilters(state.filters);
    refreshVocabs();
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
  seedStatus(state.filters);
  try {
    await ensureSoldForSelection();
  } catch (_) {
    // setStatus already reports the failure; cold load continues with the
    // active bucket alone (refreshVocabs below still runs either way).
  }
  refreshVocabs();
  wireToolbar();
  // Live cross-tab sync: the map (or another table tab) changed the filters.
  subscribeOtherTabs(() => {
    state.filters = loadFilters(state.meta);
    // loadFilters installs a brand-new filters object (and array), so
    // whatever seeded THIS tab's floor at init does not carry over. Any
    // stored blob with an empty tilgjengelighetSelected -- a stale write
    // from before this floor existed, or a future write path that forgets
    // to seed -- would otherwise resurrect the cold-load-vs-warm-load split
    // this floor exists to prevent. Cheap and idempotent, so just reseed.
    seedStatus(state.filters);
    // The synced selection may be narrower or wider than this tab's previous
    // one; either way state.vocabs (tag chips, popover counts) must be
    // re-derived against it before render(), same as every other path that
    // changes the selection.
    refreshVocabs();
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
