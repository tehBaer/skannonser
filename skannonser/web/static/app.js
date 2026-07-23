// Orchestrates the map (Phase 5 Task 6 core + Task 7 filters/stations):
// fetch meta + listings, set up palette/layers, wire the sidebar (layer
// toggles, metric filters, per-boligtype visibility, station overlays +
// commute filter, missing-coords panel), all persisted to one localStorage
// key, draw the FINN boundary, render popups, honour a `#<finnkode>` hash.

import {
  createMap,
  addListingGroups,
  buildGroups,
  groupIdForItem,
  addBoundary,
  boligtypePalette,
  syncClusterMarkers,
  clearClusterCache,
  setSoldColorMode,
  PREMIUM_LEGEND,
  DEFAULT_UNKNOWN_TYPE_COLOR,
} from "./map.js";
import { buildPopupContent } from "./popup.js";
import { isNew, parseScrapedAt, premiumPct } from "./listingmeta.js";
import {
  defaultFilterState,
  metricDimmed,
  boligtypeHidden,
  residualOpacity,
  buildMetricFilterUI,
  buildBoligtypeFilterUI,
} from "./filters.js";
import {
  addStationLayers,
  updateStationLayers,
  wireStationNamePopup,
  distinctLines,
  visibleLineSet,
  nearestCoveringStation,
  effectiveSandvikaMinutes,
  anyLineVisibleStation,
  commuteDisabled,
  SANDVIKA_MAX,
} from "./stations.js";

/* global maplibregl */

const STORAGE_KEY = "skannonser.ui.v1";
// Last time the map was opened -- drives the "N nye siden sist" status chip.
const LAST_VISIT_KEY = "skannonser.lastVisit";

// One versioned UI-state object (merged over stored values on load). Task 6
// shipped only {eie,dnb,sold} under this key; the deep-merge below keeps those
// working while filling in the Task 7 fields.
function defaultUi(meta) {
  return {
    eie: true,
    dnb: true,
    sold: false,
    filters: defaultFilterState(meta),
    dimIntensity: 75, // % dimming for non-matching listings
    // Sold-only dimming defaults ON (50 %): with thousands of sold dots at
    // full strength the actives drown -- subdued-by-default keeps the sold
    // layer readable the moment it's toggled on. Slide to 0 for full colour.
    soldDim: 50,
    soldPremium: false, // colour sold dots by budpremie instead of boligtype
    combineSold: false, // cluster sold + active together (vs separately)
    boligtypeHidden: {},
    tagHidden: {}, // {tag: true} -> listings with that tag are hidden ("" = untagged)
    collapsed: {}, // {panelId: true} -> sidebar panel collapsed
    stations: {
      show: false,
      hideOutside: false,
      includeTransfer: false,
      sandvikaMax: SANDVIKA_MAX, // == max -> commute filter off
      lineHidden: {},
    },
  };
}

const state = {
  meta: null,
  destinations: [],
  itemsById: new Map(),
  soldLoaded: false,
  soldPromise: null, // in-flight ensureSoldLoaded, so concurrent callers share one fetch
  ui: null,
  clusterMarkers: {},
  map: null,
  layersReady: false, // set once map 'load' has added sources/layers; applyAll no-ops before
  lastVariantMode: null, // "both" | "split" -- tracks combineSold across applyAll calls
  popup: null,
  colorByType: {},
  groups: [],
  validGroupIds: new Set(),
  newSinceLast: 0,
};

function loadUi(meta) {
  const base = defaultUi(meta);
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const stored = JSON.parse(raw);
      const ui = {
        ...base,
        ...stored,
        filters: {
          ...base.filters,
          ...(stored.filters || {}),
          travelMax: { ...base.filters.travelMax, ...((stored.filters || {}).travelMax || {}) },
        },
        boligtypeHidden: { ...(stored.boligtypeHidden || {}) },
        tagHidden: { ...(stored.tagHidden || {}) },
        collapsed: { ...(stored.collapsed || {}) },
        stations: {
          ...base.stations,
          ...(stored.stations || {}),
          lineHidden: { ...((stored.stations || {}).lineHidden || {}) },
        },
      };
      // One-time nudge (2026-07-23): soldDim used to DEFAULT to 0, so every
      // pre-existing blob carries 0 without the user ever choosing it. Lift
      // those to the new 50 % default once; an explicit non-zero setting is
      // kept as-is, and after the nudge the slider is fully user-owned again.
      if (!stored.soldDimNudged) {
        ui.soldDim = Math.max(Number(ui.soldDim) || 0, 50);
        ui.soldDimNudged = true;
      }
      return ui;
    }
  } catch (_) {
    /* malformed storage -> defaults */
  }
  return base;
}

function saveUi() {
  try {
    const { _allLines, ...persist } = state.ui; // _allLines is derived at load
    localStorage.setItem(STORAGE_KEY, JSON.stringify(persist));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

function setStatus(text) {
  const node = document.getElementById("status");
  if (node) node.textContent = text || "";
}

function bucketOf(item) {
  if (item.sold) return "sold";
  if (item.source === "dnb") return "dnb";
  return "eie";
}

// Per-listing dim decision: metric filters OR commute OR hide-outside-radius.
// `ctx` carries the once-per-recompute station context.
function isDimmed(item, ctx) {
  if (metricDimmed(item, state.ui, state.meta)) return true;

  const st = state.ui.stations;
  const covering = nearestCoveringStation(item, ctx.stations, ctx.visibleLines);

  // Commute: nearest in-radius station's effective minutes must be <= threshold.
  if (ctx.commuteEnabled && covering) {
    const mins = effectiveSandvikaMinutes(covering.station, {
      visibleLines: ctx.visibleLines,
      includeTransfer: st.includeTransfer,
    });
    if (mins == null || mins > st.sandvikaMax) return true;
  }

  // Hide-outside: dim listings not within any line-visible station's radius.
  if (st.hideOutside && ctx.anyStation && !covering) return true;

  return false;
}

function tagKeyOf(item) {
  return item.tag ? String(item.tag).trim() : "";
}

function itemToFeature(item, op) {
  const properties = {
    finnkode: item.finnkode,
    source: item.source,
    sold: !!item.sold,
    boligtype: item.boligtype || "",
    op, // 1, or the dimmed residual opacity (see filters.residualOpacity)
  };
  if (tagKeyOf(item)) properties.hasTag = true; // drives the tag-ring layer
  if (item.sold) {
    const pct = premiumPct(item);
    if (pct != null) properties.premium = Math.round(pct * 10) / 10;
  }
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [item.lng, item.lat] },
    properties,
  };
}

// Bucket the visible listings into one FeatureCollection per group source
// (sold group + per-boligtype groups), so each source clusters independently.
function featureCollectionsByGroup() {
  const ctx = {
    stations: state.meta.stations || [],
    visibleLines: visibleLineSet(state.ui),
    commuteEnabled: !commuteDisabled(state.ui.stations.sandvikaMax),
    anyStation: anyLineVisibleStation(state.meta.stations || [], visibleLineSet(state.ui)),
  };
  const residual = residualOpacity(state.ui);
  // Sold listings are DETACHED from the filters + "Nedtoning": their opacity is
  // driven solely by the separate "Solgt nedtoning" slider (uniform). Active
  // listings are driven solely by the filters + "Nedtoning".
  const soldPct = Math.max(0, Math.min(100, Number(state.ui.soldDim) || 0));
  const soldOpacity = 1 - soldPct / 100;
  const byGroup = {};
  state.groups.forEach((g) => (byGroup[g.id] = []));
  state.itemsById.forEach((item) => {
    if (item.lat == null || item.lng == null) return;
    if (!state.ui[bucketOf(item)]) return; // layer toggle (eie/dnb/sold)
    if (boligtypeHidden(item, state.ui)) return; // per-type visibility (hidden)
    if (state.ui.tagHidden[tagKeyOf(item)]) return; // tag visibility (hidden)
    const gid = groupIdForItem(item, state.validGroupIds, state.ui.combineSold);
    if (!byGroup[gid]) return; // safety: no source for this group
    const op = item.sold ? soldOpacity : isDimmed(item, ctx) ? residual : 1;
    byGroup[gid].push(itemToFeature(item, op));
  });
  return byGroup;
}

// Full re-render after any filter/station change: group sources + stations.
// Safe to call before the map's layers exist (sidebar wires up first) -- it
// just no-ops until `load` has run addListingGroups.
let rafPending = false;
function applyAll() {
  if (!state.map || !state.layersReady) return;
  if (rafPending) return;
  rafPending = true;
  requestAnimationFrame(() => {
    rafPending = false;
    const byGroup = featureCollectionsByGroup();
    // Clear cached cluster markers BEFORE setData -- see clearClusterCache's
    // doc comment in map.js. Reused cluster_ids after a data change would
    // otherwise leave stale bubbles (wrong count/position) on screen.
    clearClusterCache(state.clusterMarkers);
    // Only the variants of the CURRENT clustering mode get real data; the
    // other mode's sources are already empty, so skip their setData (each one
    // costs a supercluster re-index) -- except on a mode switch, where the
    // now-unused variants must be cleared once.
    const mode = state.ui.combineSold ? "both" : "split";
    const modeChanged = state.lastVariantMode !== mode;
    state.lastVariantMode = mode;
    state.groups.forEach((g) => {
      const isBothVariant = g.hasActive && g.hasSold;
      const inMode = mode === "both" ? isBothVariant : !isBothVariant;
      if (!inMode && !modeChanged) return;
      const src = state.map.getSource(g.id);
      if (src) {
        src.setData({
          type: "FeatureCollection",
          features: inMode ? byGroup[g.id] || [] : [],
        });
      }
    });
    updateStationLayers(state.map, state.meta.stations || [], state.ui);
  });
}

function ingestItems(items) {
  items.forEach((item) => state.itemsById.set(item.finnkode, item));
}

function ensureSoldLoaded() {
  if (state.soldLoaded) return Promise.resolve();
  if (state.soldPromise) return state.soldPromise;
  state.soldPromise = (async () => {
    setStatus("Laster solgte …");
    try {
      const resp = await fetch("/api/listings?bucket=sold");
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const data = await resp.json();
      ingestItems(data.listings || []);
      state.soldLoaded = true;
      buildTagFilterUI(); // sold items may introduce tags
      updateStatus();
    } catch (err) {
      setStatus("Kunne ikke laste solgte: " + err.message);
      throw err;
    }
  })().finally(() => {
    state.soldPromise = null;
  });
  return state.soldPromise;
}

function renderSourceLegend() {
  const node = document.getElementById("source-legend");
  if (!node) return;
  node.innerHTML = "";
  // Colour = boligtype (see BOLIGTYPE above). Here we key the SHAPE (DNB square)
  // and the BORDER (active = dark, sold = white). Swatches use a neutral fill so
  // the border reads.
  [
    { label: "Aktiv (mørk kant)", border: "#111111", square: false },
    { label: "Solgt (hvit kant)", border: "#ffffff", square: false },
    { label: "DNB (kvadrat)", border: "#111111", square: true },
  ].forEach(({ label, border, square }) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const sw = document.createElement("span");
    sw.className = "legend-swatch" + (square ? " square" : "");
    sw.style.background = DEFAULT_UNKNOWN_TYPE_COLOR;
    sw.style.border = "2px solid " + border;
    row.appendChild(sw);
    row.appendChild(document.createTextNode(label));
    node.appendChild(row);
  });
}

function openPopup(finnkode, coordinates) {
  const item = state.itemsById.get(finnkode);
  if (!item) return;
  const content = buildPopupContent(item, state.destinations);
  if (!state.popup) state.popup = new maplibregl.Popup({ maxWidth: "300px" });
  state.popup
    .setLngLat(coordinates || [item.lng, item.lat])
    .setDOMContent(content)
    .addTo(state.map);
  panPopupIntoView();
}

// MapLibre popups don't auto-pan: a tall popup (thumbnail + editor) opened
// low on screen used to hang its annotation editor below the viewport. Pan
// the map just enough to expose the whole popup. Skipped mid-animation (e.g.
// the deep-link flyTo) -- a panBy would cancel the flight.
function panPopupIntoView() {
  if (!state.popup || !state.map || state.map.isMoving()) return;
  requestAnimationFrame(() => {
    const popupEl = state.popup.getElement();
    const mapEl = document.getElementById("map");
    if (!popupEl || !mapEl || state.map.isMoving()) return;
    const pr = popupEl.getBoundingClientRect();
    const mr = mapEl.getBoundingClientRect();
    const margin = 12;
    let dx = 0;
    let dy = 0;
    if (pr.bottom > mr.bottom - margin) dy = pr.bottom - (mr.bottom - margin);
    if (pr.right > mr.right - margin) dx = pr.right - (mr.right - margin);
    // Top/left last: if the popup is larger than the map, showing its start wins.
    if (pr.top < mr.top + margin) dy = pr.top - (mr.top + margin);
    if (pr.left < mr.left + margin) dx = pr.left - (mr.left + margin);
    if (dx || dy) state.map.panBy([dx, dy], { duration: 250 });
  });
}

function wireLayerToggles() {
  const map = { eie: "toggle-eie", dnb: "toggle-dnb", sold: "toggle-sold" };
  Object.entries(map).forEach(([bucket, id]) => {
    const input = document.getElementById(id);
    if (!input) return;
    input.checked = !!state.ui[bucket];
    input.addEventListener("change", async () => {
      state.ui[bucket] = input.checked;
      saveUi();
      if (bucket === "sold" && input.checked) {
        input.disabled = true;
        try {
          await ensureSoldLoaded();
        } catch (_) {
          // Fetch failed (status already says so): roll the toggle back so
          // the UI never claims a sold layer it doesn't have.
          input.checked = false;
          state.ui.sold = false;
          saveUi();
        } finally {
          input.disabled = false;
        }
      }
      applyAll();
    });
  });

  const combine = document.getElementById("toggle-combine-sold");
  if (combine) {
    combine.checked = !!state.ui.combineSold;
    combine.addEventListener("change", async () => {
      state.ui.combineSold = combine.checked;
      saveUi();
      // Combining needs the sold set loaded to be meaningful.
      if (combine.checked && state.ui.sold && !state.soldLoaded) {
        try {
          await ensureSoldLoaded();
        } catch (_) {
          /* combined clustering still works for the active set alone */
        }
      }
      applyAll();
    });
  }
}

// "Farg solgte etter budpremie": recolours the "-sold" GL layers between
// boligtype colour and the premium scale, with a mini-legend while active.
function renderPremiumLegend() {
  const node = document.getElementById("premium-legend");
  if (!node) return;
  const on = !!state.ui.soldPremium;
  node.hidden = !on;
  node.innerHTML = "";
  if (!on) return;
  PREMIUM_LEGEND.forEach(({ color, label }) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const sw = document.createElement("span");
    sw.className = "legend-swatch";
    sw.style.background = color;
    row.appendChild(sw);
    row.appendChild(document.createTextNode(label));
    node.appendChild(row);
  });
}

function wirePremiumToggle() {
  const cb = document.getElementById("toggle-sold-premium");
  if (!cb) return;
  cb.checked = !!state.ui.soldPremium;
  renderPremiumLegend();
  cb.addEventListener("change", async () => {
    state.ui.soldPremium = cb.checked;
    saveUi();
    renderPremiumLegend();
    if (cb.checked && !state.soldLoaded) {
      try {
        await ensureSoldLoaded();
      } catch (_) {
        /* colours flip anyway; dots appear when sold loads later */
      }
    }
    if (state.layersReady) {
      setSoldColorMode(state.map, state.groups, state.ui.soldPremium);
    }
    applyAll();
  });
}

// Per-tag visibility -- rebuilt whenever the tag universe can change (initial
// load, sold load, an annotation save).
function buildTagFilterUI() {
  const node = document.getElementById("tag-filter");
  if (!node) return;
  const tags = new Set();
  state.itemsById.forEach((item) => {
    const t = tagKeyOf(item);
    if (t) tags.add(t);
  });
  node.innerHTML = "";
  node.classList.remove("muted");
  if (!tags.size) {
    node.textContent = "Ingen tags ennå — sett Tag i en popup eller i tabellen.";
    node.classList.add("muted");
    return;
  }
  const rows = [
    ...[...tags].sort((a, b) => a.localeCompare(b, "nb")).map((t) => [t, t]),
    ["", "(uten tag)"],
  ];
  rows.forEach(([key, label]) => {
    const row = document.createElement("label");
    row.className = "toggle boligtype-toggle";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !state.ui.tagHidden[key];
    cb.addEventListener("change", () => {
      if (cb.checked) delete state.ui.tagHidden[key];
      else state.ui.tagHidden[key] = true;
      saveUi();
      applyAll();
    });
    row.appendChild(cb);
    row.appendChild(document.createTextNode(label));
    node.appendChild(row);
  });
}

// Collapsible sidebar panels: persist which <details> the user closed.
function wireCollapsiblePanels() {
  document.querySelectorAll("details.panel").forEach((panel) => {
    if (!panel.id) return;
    if (state.ui.collapsed[panel.id]) panel.open = false;
    panel.addEventListener("toggle", () => {
      if (panel.open) delete state.ui.collapsed[panel.id];
      else state.ui.collapsed[panel.id] = true;
      saveUi();
    });
  });
}

// Mobile: hamburger <-> off-canvas sidebar drawer (pure class toggling; the
// media query in style.css decides when the drawer layout is active).
function wireDrawer() {
  const app = document.getElementById("app");
  const btn = document.getElementById("sidebar-toggle");
  const backdrop = document.getElementById("drawer-backdrop");
  if (!app || !btn) return;
  const setOpen = (open) => {
    app.classList.toggle("drawer-open", open);
    btn.setAttribute("aria-expanded", String(open));
  };
  btn.addEventListener("click", () => setOpen(!app.classList.contains("drawer-open")));
  if (backdrop) backdrop.addEventListener("click", () => setOpen(false));
}

function updateStatus() {
  let text = state.itemsById.size + " annonser lastet";
  if (state.newSinceLast > 0) {
    text += " · " + state.newSinceLast + " nye siden sist";
  }
  setStatus(text);
}

function wireStationControls() {
  const st = state.ui.stations;
  const bindCheckbox = (id, key) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.checked = !!st[key];
    el.addEventListener("change", () => {
      st[key] = el.checked;
      saveUi();
      applyAll();
    });
  };
  bindCheckbox("toggle-stations", "show");
  bindCheckbox("toggle-hide-outside", "hideOutside");
  bindCheckbox("toggle-transfer", "includeTransfer");

  const slider = document.getElementById("sandvika-max");
  const label = document.getElementById("sandvika-val");
  if (slider) {
    slider.max = String(SANDVIKA_MAX);
    slider.value = String(st.sandvikaMax);
    const paint = () => {
      const v = Number(slider.value);
      if (label) label.textContent = v >= SANDVIKA_MAX ? "Av" : "≤ " + v + " min";
    };
    paint();
    slider.addEventListener("input", () => {
      st.sandvikaMax = Number(slider.value);
      paint();
      saveUi();
      applyAll();
    });
  }

  // Line visibility toggles.
  const container = document.getElementById("line-toggles");
  if (container) {
    container.innerHTML = "";
    container.classList.remove("muted");
    const lines = state.ui._allLines || [];
    if (!lines.length) {
      container.textContent = "Ingen linjer.";
      container.classList.add("muted");
    }
    lines.forEach((line) => {
      const row = document.createElement("label");
      row.className = "toggle line-toggle";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = !st.lineHidden[line];
      cb.addEventListener("change", () => {
        if (cb.checked) delete st.lineHidden[line];
        else st.lineHidden[line] = true;
        saveUi();
        applyAll();
      });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(line));
      container.appendChild(row);
    });
  }
}

async function loadMissingCoords() {
  const node = document.getElementById("missing-coords");
  if (!node) return;
  let rows;
  try {
    const resp = await fetch("/api/missing-coords");
    rows = (await resp.json()).rows || [];
  } catch (_) {
    node.textContent = "Kunne ikke laste.";
    return;
  }
  node.innerHTML = "";
  if (!rows.length) {
    node.textContent = "Alle synlige annonser har koordinater.";
    node.classList.add("muted");
    return;
  }
  node.classList.remove("muted");
  const summary = document.createElement("p");
  summary.className = "muted missing-summary";
  summary.textContent = rows.length + " uten koordinater";
  node.appendChild(summary);
  rows.forEach((row) => {
    const line = document.createElement("div");
    line.className = "missing-row";
    // finnkode -> Finn ad (user-navigation hyperlink, click-only).
    const link = document.createElement("a");
    link.href = "https://www.finn.no/realestate/homes/ad.html?finnkode=" +
      encodeURIComponent(row.finnkode);
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = row.finnkode;
    line.appendChild(link);
    if (row.adresse) {
      line.appendChild(document.createTextNode(" — " + row.adresse));
    }
    node.appendChild(line);
  });
}

async function handleHash() {
  const raw = decodeURIComponent(window.location.hash.replace(/^#/, ""));
  if (!raw) return;
  const finnkode = raw.startsWith("finnkode=") ? raw.slice("finnkode=".length) : raw;
  let item = state.itemsById.get(finnkode);
  // Deep links to SOLD listings (e.g. the table's Kart column) arrive before
  // the lazily-fetched sold set on a cold load. On a miss, pull the sold
  // bucket and retry -- and switch the Solgt layer on so the dot is visible.
  if (!item && !state.soldLoaded) {
    try {
      await ensureSoldLoaded();
    } catch (_) {
      return;
    }
    item = state.itemsById.get(finnkode);
  }
  if (!item || item.lat == null || item.lng == null) return;
  if (item.sold && !state.ui.sold) {
    state.ui.sold = true;
    const cb = document.getElementById("toggle-sold");
    if (cb) cb.checked = true;
    saveUi();
  }
  applyAll();
  state.map.flyTo({ center: [item.lng, item.lat], zoom: 15 });
  openPopup(finnkode, [item.lng, item.lat]);
}

// Fit the initial view to the FINN search polygon (the authoritative area)
// instead of a hardcoded center/zoom that cut off the data's edges.
function fitToPolygon(map, polygon) {
  if (!polygon || polygon.length < 3) return;
  const bounds = polygon.reduce(
    (b, p) => b.extend([p[0], p[1]]),
    new maplibregl.LngLatBounds(
      [polygon[0][0], polygon[0][1]],
      [polygon[0][0], polygon[0][1]]
    )
  );
  map.fitBounds(bounds, { padding: 40, animate: false });
}

async function init() {
  setStatus("Laster …");
  let meta, listings;
  try {
    [meta, listings] = await Promise.all([
      fetch("/api/meta").then((r) => r.json()),
      fetch("/api/listings").then((r) => r.json()),
    ]);
  } catch (err) {
    setStatus("Kunne ikke laste data: " + err.message);
    return;
  }
  state.meta = meta;
  state.destinations = meta.destinations || [];
  state.ui = loadUi(meta);
  state.ui._allLines = distinctLines(meta.stations || []);
  ingestItems(listings.listings || []);

  // "N nye siden sist": actives first seen after the previous visit.
  const prevVisit = (() => {
    try {
      return localStorage.getItem(LAST_VISIT_KEY);
    } catch (_) {
      return null;
    }
  })();
  if (prevVisit) {
    const prevTs = Date.parse(prevVisit);
    state.itemsById.forEach((item) => {
      const t = parseScrapedAt(item.scraped_at);
      if (!item.sold && t != null && t > prevTs) state.newSinceLast += 1;
    });
  }
  try {
    localStorage.setItem(LAST_VISIT_KEY, new Date().toISOString());
  } catch (_) {
    /* non-fatal */
  }

  const { colorByType } = boligtypePalette(meta.boligtyper || []);
  state.colorByType = colorByType;
  state.groups = buildGroups(meta.boligtyper || [], colorByType);
  state.validGroupIds = new Set(state.groups.map((g) => g.id));

  // Sidebar FIRST, before the map exists: the persisted UI state must show
  // immediately, not after the (possibly slow) first tile load. Control
  // handlers call applyAll(), which no-ops until the map layers are ready.
  buildBoligtypeFilterUI(
    document.getElementById("boligtype-filter"),
    meta,
    { ...colorByType, "": DEFAULT_UNKNOWN_TYPE_COLOR },
    state.ui,
    () => { saveUi(); applyAll(); }
  );
  renderSourceLegend();
  buildMetricFilterUI(
    document.getElementById("metric-filters"),
    meta,
    state.ui,
    () => { saveUi(); applyAll(); }
  );
  buildTagFilterUI();
  wireLayerToggles();
  wirePremiumToggle();
  wireStationControls();
  wireCollapsiblePanels();
  wireDrawer();
  loadMissingCoords();
  document.addEventListener("sk-annotation-saved", () => {
    buildTagFilterUI();
    applyAll(); // tag rings / tag-visibility may have changed
  });

  const map = createMap("map");
  state.map = map;

  map.on("load", () => {
    map.resize();
    addListingGroups(map, state.groups, openPopup);
    addStationLayers(map);
    wireStationNamePopup(map);
    addBoundary(map, meta.polygon || []);
    state.layersReady = true;
    if (state.ui.soldPremium) setSoldColorMode(map, state.groups, true);

    applyAll();

    map.on("render", () => syncClusterMarkers(map, state.groups, state.clusterMarkers));
    map.on("moveend", () => syncClusterMarkers(map, state.groups, state.clusterMarkers));

    if (state.ui.sold && !state.soldLoaded) {
      ensureSoldLoaded().then(applyAll).catch(() => {});
    }

    updateStatus();
    if (window.location.hash) {
      handleHash();
    } else {
      fitToPolygon(map, meta.polygon || []);
    }
    window.addEventListener("hashchange", handleHash);
  });
}

init();
