// Sortable/filterable table view (Phase 5 Task 8): every /api/listings row
// in one <table>, click-to-sort headers, a text filter box, inline
// kommentar/tag editing (via the shared ./annotations.js save helper -- the
// same one popup.js's map-popup editor uses), a lazy Sold toggle, and a
// "Kart" link that hands off to `/#finnkode=...` (index.html/app.js's
// existing hash-focus handling).

import { saveAnnotation } from "./annotations.js";
import { isNew, fmtDate, premiumPct, fmtPremium } from "./listingmeta.js";

const NOK = new Intl.NumberFormat("nb-NO");
const STORAGE_KEY = "skannonser.ui.v1"; // shared with app.js -- only the
// `sold` field is read/written here so the two pages agree on that one
// toggle without either page needing to know the other's full UI-state shape.

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

const state = {
  items: [], // all loaded items (eie + dnb, + sold once toggled on)
  soldLoaded: false,
  showSold: false, // tracks "Vis solgte" toggle state; sold items stay in items
  sortKey: "scraped_at", // newest first: the scanner's daily question
  sortDir: "desc",
  filterText: "",
};

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
    // Hide sold items when showSold is false
    if (!state.showSold && item.sold) return false;
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
  COLUMNS.forEach((col) => {
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
  const tr = el("tr", item.sold ? "sold-row" : null);

  COLUMNS.forEach((col) => {
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
  const body = document.getElementById("table-body");
  body.innerHTML = "";
  const rows = visibleRows();
  rows.forEach((item) => body.appendChild(buildRow(item)));
  setStatus(rows.length + " av " + state.items.length + " annonser");
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
      } catch (err) {
        setStatus("Kunne ikke laste solgte: " + err.message);
      } finally {
        soldToggle.disabled = false;
      }
    }
    render();
  });
}

async function init() {
  setStatus("Laster …");
  wireToolbar();
  try {
    state.items = await fetchListings(0);
  } catch (err) {
    setStatus("Kunne ikke laste data: " + err.message);
    return;
  }
  const soldToggle = document.getElementById("table-sold");
  if (soldToggle.checked) {
    try {
      state.items = state.items.concat(await fetchListings(1));
      state.soldLoaded = true;
      state.showSold = true;
    } catch (_) {
      /* fall through with just the non-sold rows loaded */
    }
  }
  render();
}

init();
