// Orchestrates the map core (Phase 5 Task 6): fetch meta + listings, set up
// the palette/layers, wire the sidebar toggles (with localStorage-persisted
// state and lazy Sold loading), draw the FINN boundary, render popups, and
// honour a `#<finnkode>` hash by flying to that listing.

import {
  createMap,
  addListingLayers,
  addBoundary,
  boligtypePalette,
  syncClusterMarkers,
  SOURCE_ID,
  DEFAULT_UNKNOWN_TYPE_COLOR,
} from "./map.js";
import { buildPopupContent } from "./popup.js";

/* global maplibregl */

const STORAGE_KEY = "skannonser.ui.v1";
const DEFAULT_TOGGLES = { eie: true, dnb: true, sold: false };

const state = {
  meta: null,
  destinations: [],
  itemsById: new Map(), // finnkode -> full listing item
  soldLoaded: false,
  toggles: loadToggles(),
  clusterMarkers: {},
  map: null,
  popup: null,
};

function loadToggles() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return { ...DEFAULT_TOGGLES, ...JSON.parse(raw) };
  } catch (_) {
    /* ignore malformed storage */
  }
  return { ...DEFAULT_TOGGLES };
}

function saveToggles() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state.toggles));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

function setStatus(text) {
  const node = document.getElementById("status");
  if (node) node.textContent = text || "";
}

// Which toggle bucket a listing belongs to.
function bucketOf(item) {
  if (item.sold) return "sold";
  if (item.source === "dnb") return "dnb";
  return "eie";
}

function itemToFeature(item) {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [item.lng, item.lat] },
    properties: {
      finnkode: item.finnkode,
      source: item.source,
      sold: !!item.sold,
      // null boligtype omitted -> match expression falls to unknown default
      boligtype: item.boligtype || "",
    },
  };
}

function currentFeatureCollection() {
  const features = [];
  state.itemsById.forEach((item) => {
    if (item.lat == null || item.lng == null) return; // skip null coords
    if (!state.toggles[bucketOf(item)]) return;
    features.push(itemToFeature(item));
  });
  return { type: "FeatureCollection", features };
}

function applyToggles() {
  const src = state.map.getSource(SOURCE_ID);
  if (src) src.setData(currentFeatureCollection());
}

function ingestItems(items) {
  items.forEach((item) => {
    state.itemsById.set(item.finnkode, item);
  });
}

async function ensureSoldLoaded() {
  if (state.soldLoaded) return;
  setStatus("Laster solgte …");
  const resp = await fetch("/api/listings?sold=1");
  const data = await resp.json();
  // Only merge the genuinely-sold rows (the payload also re-includes the
  // active Eie + DNB buckets, already loaded).
  ingestItems((data.listings || []).filter((it) => it.sold));
  state.soldLoaded = true;
  setStatus("");
}

function renderLegend(colorByType) {
  const node = document.getElementById("boligtype-legend");
  if (!node) return;
  node.innerHTML = "";
  node.classList.remove("muted");
  const types = Object.keys(colorByType);
  if (!types.length) {
    node.textContent = "Ingen boligtyper.";
    node.classList.add("muted");
  }
  types.forEach((typeName) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const sw = document.createElement("span");
    sw.className = "legend-swatch";
    sw.style.background = colorByType[typeName];
    row.appendChild(sw);
    row.appendChild(document.createTextNode(typeName));
    node.appendChild(row);
  });
  // DNB + Sold key entries.
  [
    ["DNB", "#0f4c81", true],
    ["Solgt", "#9aa5a0", false],
    ["Ukjent boligtype", DEFAULT_UNKNOWN_TYPE_COLOR, false],
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
  if (!state.popup) {
    state.popup = new maplibregl.Popup({ maxWidth: "300px" });
  }
  state.popup
    .setLngLat(coordinates || [item.lng, item.lat])
    .setDOMContent(content)
    .addTo(state.map);
}

function wireToggles() {
  const map = { eie: "toggle-eie", dnb: "toggle-dnb", sold: "toggle-sold" };
  Object.entries(map).forEach(([bucket, id]) => {
    const input = document.getElementById(id);
    if (!input) return;
    input.checked = !!state.toggles[bucket];
    input.addEventListener("change", async () => {
      state.toggles[bucket] = input.checked;
      saveToggles();
      if (bucket === "sold" && input.checked) {
        input.disabled = true;
        try {
          await ensureSoldLoaded();
        } finally {
          input.disabled = false;
        }
      }
      applyToggles();
    });
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
  ingestItems(listings.listings || []);

  const { colorByType, expression } = boligtypePalette(meta.boligtyper || []);
  renderLegend(colorByType);

  const map = createMap("map");
  state.map = map;

  map.on("load", () => {
    // The flex layout may not have settled the #map height when the Map was
    // constructed; resize once now so tile coverage matches the final size.
    map.resize();
    addListingLayers(map, expression, openPopup);
    addBoundary(map, meta.polygon || []);
    applyToggles();
    wireToggles();

    map.on("render", () => syncClusterMarkers(map, state.clusterMarkers));
    map.on("moveend", () => syncClusterMarkers(map, state.clusterMarkers));

    // Sold may be persisted-on from a previous visit: lazy-load then re-apply.
    if (state.toggles.sold && !state.soldLoaded) {
      ensureSoldLoaded().then(applyToggles);
    }

    const count = state.itemsById.size;
    setStatus(count + " annonser lastet");

    handleHash();
    window.addEventListener("hashchange", handleHash);
  });
}

init();
