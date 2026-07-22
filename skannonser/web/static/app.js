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
  DEFAULT_UNKNOWN_TYPE_COLOR,
} from "./map.js";
import { buildPopupContent } from "./popup.js";
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
    soldDim: 0, // % extra dimming applied to sold listings only (independent slider)
    boligtypeHidden: {},
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
  ui: null,
  clusterMarkers: {},
  map: null,
  popup: null,
  colorByType: {},
  groups: [],
  validGroupIds: new Set(),
};

function loadUi(meta) {
  const base = defaultUi(meta);
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const stored = JSON.parse(raw);
      return {
        ...base,
        ...stored,
        filters: {
          ...base.filters,
          ...(stored.filters || {}),
          travelMax: { ...base.filters.travelMax, ...((stored.filters || {}).travelMax || {}) },
        },
        boligtypeHidden: { ...(stored.boligtypeHidden || {}) },
        stations: {
          ...base.stations,
          ...(stored.stations || {}),
          lineHidden: { ...((stored.stations || {}).lineHidden || {}) },
        },
      };
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

function itemToFeature(item, op) {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [item.lng, item.lat] },
    properties: {
      finnkode: item.finnkode,
      source: item.source,
      sold: !!item.sold,
      boligtype: item.boligtype || "",
      op, // 1, or the dimmed residual opacity (see filters.residualOpacity)
    },
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
  // Independent "solgt nedtoning": an extra multiplier applied only to sold
  // listings, on top of any filter dimming.
  const soldPct = Math.max(0, Math.min(100, Number(state.ui.soldDim) || 0));
  const soldOpacity = 1 - soldPct / 100;
  const byGroup = {};
  state.groups.forEach((g) => (byGroup[g.id] = []));
  state.itemsById.forEach((item) => {
    if (item.lat == null || item.lng == null) return;
    if (!state.ui[bucketOf(item)]) return; // layer toggle (eie/dnb/sold)
    if (boligtypeHidden(item, state.ui)) return; // per-type visibility (hidden)
    const gid = groupIdForItem(item, state.validGroupIds);
    if (!byGroup[gid]) return; // safety: no source for this group
    let op = isDimmed(item, ctx) ? residual : 1;
    if (item.sold) op *= soldOpacity;
    byGroup[gid].push(itemToFeature(item, op));
  });
  return byGroup;
}

// Full re-render after any filter/station change: all group sources + stations.
let rafPending = false;
function applyAll() {
  if (rafPending) return;
  rafPending = true;
  requestAnimationFrame(() => {
    rafPending = false;
    const byGroup = featureCollectionsByGroup();
    // Clear cached cluster markers BEFORE setData -- see clearClusterCache's
    // doc comment in map.js. Reused cluster_ids after a data change would
    // otherwise leave stale bubbles (wrong count/position) on screen.
    clearClusterCache(state.clusterMarkers);
    state.groups.forEach((g) => {
      const src = state.map.getSource(g.id);
      if (src) {
        src.setData({ type: "FeatureCollection", features: byGroup[g.id] || [] });
      }
    });
    updateStationLayers(state.map, state.meta.stations || [], state.ui);
  });
}

function ingestItems(items) {
  items.forEach((item) => state.itemsById.set(item.finnkode, item));
}

async function ensureSoldLoaded() {
  if (state.soldLoaded) return;
  setStatus("Laster solgte …");
  const resp = await fetch("/api/listings?sold=1");
  const data = await resp.json();
  ingestItems((data.listings || []).filter((it) => it.sold));
  state.soldLoaded = true;
  setStatus("");
}

function renderSourceLegend() {
  const node = document.getElementById("source-legend");
  if (!node) return;
  node.innerHTML = "";
  [
    ["DNB (kvadrat)", DEFAULT_UNKNOWN_TYPE_COLOR, true],
    ["Solgt", "#9aa5a0", false],
  ].forEach(([label, color, square]) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const sw = document.createElement("span");
    sw.className = "legend-swatch" + (square ? " square" : "");
    sw.style.background = color;
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
        } finally {
          input.disabled = false;
        }
      }
      applyAll();
    });
  });
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

function handleHash() {
  const raw = decodeURIComponent(window.location.hash.replace(/^#/, ""));
  if (!raw) return;
  const finnkode = raw.startsWith("finnkode=") ? raw.slice("finnkode=".length) : raw;
  const item = state.itemsById.get(finnkode);
  if (!item || item.lat == null || item.lng == null) return;
  state.map.flyTo({ center: [item.lng, item.lat], zoom: 15 });
  openPopup(finnkode, [item.lng, item.lat]);
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

  const { colorByType, expression } = boligtypePalette(meta.boligtyper || []);
  state.colorByType = colorByType;
  state.soldColorExpr = expression; // sold circles coloured by boligtype
  state.groups = buildGroups(meta.boligtyper || [], colorByType);
  state.validGroupIds = new Set(state.groups.map((g) => g.id));

  const map = createMap("map");
  state.map = map;

  map.on("load", () => {
    map.resize();
    addListingGroups(map, state.groups, state.soldColorExpr, openPopup);
    addStationLayers(map);
    wireStationNamePopup(map);
    addBoundary(map, meta.polygon || []);

    // Sidebar UI.
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
    wireLayerToggles();
    wireStationControls();
    loadMissingCoords();

    applyAll();

    map.on("render", () => syncClusterMarkers(map, state.groups, state.clusterMarkers));
    map.on("moveend", () => syncClusterMarkers(map, state.groups, state.clusterMarkers));

    if (state.ui.sold && !state.soldLoaded) {
      ensureSoldLoaded().then(applyAll);
    }

    setStatus(state.itemsById.size + " annonser lastet");
    handleHash();
    window.addEventListener("hashchange", handleHash);
  });
}

init();
