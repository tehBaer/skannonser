// MapLibre map core: OSM raster base, clustered listings source, unclustered
// per-source layers, and DOM cluster markers (Phase 5 Task 6).
//
// Cluster COUNT rendering uses DOM markers rather than a GL symbol/text
// layer on purpose: GL text needs a `glyphs` font-server URL, and the only
// options are an external CDN (banned -- this app must serve zero external
// URLs beyond the OSM tiles, and must work offline) or vendoring glyph PBFs.
// The DOM-marker pattern is the documented offline-safe way to show cluster
// counts, so unclustered points are GL layers and clusters are synced DOM
// bubbles.
//
// BOLIGTYPE PALETTE: ported verbatim from apps_script/map/map.html --
//   * TYPE_COLOR_PALETTE            -> map.html lines 1046-1048
//   * FIXED_BOLIGTYPE_DEFAULT_COLORS -> map.html lines 1065-1067
//   * DEFAULT_UNKNOWN_TYPE_COLOR     -> map.html line 1042
//   * assignment order / override    -> getDefaultBoligtypeColor, map.html
//                                       lines 3863-3872; getBoligtypeColor
//                                       (unknown -> default) lines 4248-4254.
// We assign palette colours over the alphabetically-sorted boligtyper from
// /api/meta (deterministic), applying the tomannsbolig fixed override and
// the grey unknown default -- the same colour set, same rules.

/* global maplibregl */

export const SOURCE_ID = "listings";

// --- ported palette (see header) ---
export const TYPE_COLOR_PALETTE = [
  "#0f8c56", "#be3a34", "#0f4c81", "#e08a00", "#6f53b3",
  "#0c8f9c", "#8f6a1f", "#7f3567", "#2f5f40", "#8c2e2e",
];
export const FIXED_BOLIGTYPE_DEFAULT_COLORS = { tomannsbolig: "#f2d34f" };
export const DEFAULT_UNKNOWN_TYPE_COLOR = "#6f7e76";

const SOLD_COLOR = "#9aa5a0";
const DNB_COLOR = "#0f4c81";

const OSM_STYLE = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      maxzoom: 19,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [{ id: "osm-base", type: "raster", source: "osm" }],
};

// {boligtype: color} + a MapLibre 'match' expression on ['get','boligtype'].
export function boligtypePalette(boligtyper) {
  const colorByType = {};
  (boligtyper || []).forEach((typeName, idx) => {
    const key = String(typeName).toLowerCase();
    colorByType[typeName] =
      FIXED_BOLIGTYPE_DEFAULT_COLORS[key] ||
      TYPE_COLOR_PALETTE[idx % TYPE_COLOR_PALETTE.length];
  });
  const pairs = [];
  Object.keys(colorByType).forEach((typeName) => {
    pairs.push(typeName, colorByType[typeName]);
  });
  const expression =
    pairs.length > 0
      ? ["match", ["get", "boligtype"], ...pairs, DEFAULT_UNKNOWN_TYPE_COLOR]
      : DEFAULT_UNKNOWN_TYPE_COLOR;
  return { colorByType, expression };
}

export function createMap(container) {
  return new maplibregl.Map({
    container,
    style: OSM_STYLE,
    center: [10.75, 59.91], // Oslo
    zoom: 10,
    attributionControl: true,
  });
}

// A filled square icon for DNB points (no sprite sheet needed).
function ensureDnbIcon(map) {
  if (map.hasImage("dnb-square")) return;
  const size = 18;
  const cvs = document.createElement("canvas");
  cvs.width = size;
  cvs.height = size;
  const ctx = cvs.getContext("2d");
  ctx.fillStyle = DNB_COLOR;
  ctx.fillRect(0, 0, size, size);
  ctx.strokeStyle = "#ffffff";
  ctx.lineWidth = 2;
  ctx.strokeRect(1, 1, size - 2, size - 2);
  const data = ctx.getImageData(0, 0, size, size);
  map.addImage("dnb-square", { width: size, height: size, data: data.data });
}

const NOT_CLUSTER = ["!", ["has", "point_count"]];

// Adds the clustered source + unclustered GL layers. `colorExpr` is the
// boligtype match expression for Eie circles.
export function addListingLayers(map, colorExpr, onListingClick) {
  ensureDnbIcon(map);

  map.addSource(SOURCE_ID, {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
    cluster: true,
    clusterRadius: 40,
    clusterMaxZoom: 15,
  });

  // Sold (grey) at the bottom.
  map.addLayer({
    id: "unclustered-sold",
    type: "circle",
    source: SOURCE_ID,
    filter: ["all", NOT_CLUSTER, ["==", ["get", "sold"], true]],
    paint: {
      "circle-color": SOLD_COLOR,
      "circle-radius": 6,
      "circle-stroke-width": 1.5,
      "circle-stroke-color": "#ffffff",
    },
  });

  // Eie (circle coloured by boligtype).
  map.addLayer({
    id: "unclustered-eie",
    type: "circle",
    source: SOURCE_ID,
    filter: [
      "all",
      NOT_CLUSTER,
      ["==", ["get", "source"], "eie"],
      ["==", ["get", "sold"], false],
    ],
    paint: {
      "circle-color": colorExpr,
      "circle-radius": 7,
      "circle-stroke-width": 1.5,
      "circle-stroke-color": "#ffffff",
    },
  });

  // DNB (square symbol).
  map.addLayer({
    id: "unclustered-dnb",
    type: "symbol",
    source: SOURCE_ID,
    filter: ["all", NOT_CLUSTER, ["==", ["get", "source"], "dnb"]],
    layout: {
      "icon-image": "dnb-square",
      "icon-size": 1,
      "icon-allow-overlap": true,
    },
  });

  const clickLayers = ["unclustered-sold", "unclustered-eie", "unclustered-dnb"];
  clickLayers.forEach((layerId) => {
    map.on("click", layerId, (e) => {
      const f = e.features && e.features[0];
      if (f) onListingClick(f.properties.finnkode, f.geometry.coordinates);
    });
    map.on("mouseenter", layerId, () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", layerId, () => {
      map.getCanvas().style.cursor = "";
    });
  });
}

// DOM cluster markers synced to the current viewport. `cache` is a caller-held
// object mapping cluster_id -> maplibregl.Marker; call on 'render'/'moveend'.
export function syncClusterMarkers(map, cache) {
  if (!map.getSource(SOURCE_ID) || !map.isSourceLoaded(SOURCE_ID)) return;
  const features = map.querySourceFeatures(SOURCE_ID, {
    filter: ["has", "point_count"],
  });

  const seen = {};
  features.forEach((f) => {
    const id = f.properties.cluster_id;
    seen[id] = true;
    if (cache[id]) return;
    const count = f.properties.point_count;
    const size = count < 25 ? 34 : count < 100 ? 42 : 52;
    const div = document.createElement("div");
    div.className = "cluster-marker";
    div.style.width = size + "px";
    div.style.height = size + "px";
    div.textContent = f.properties.point_count_abbreviated;
    div.addEventListener("click", () => {
      map
        .getSource(SOURCE_ID)
        .getClusterExpansionZoom(id)
        .then((zoom) => {
          map.easeTo({ center: f.geometry.coordinates, zoom });
        });
    });
    cache[id] = new maplibregl.Marker({ element: div }).setLngLat(
      f.geometry.coordinates
    ).addTo(map);
  });

  Object.keys(cache).forEach((id) => {
    if (!seen[id]) {
      cache[id].remove();
      delete cache[id];
    }
  });
}

// FINN boundary polygon (meta.polygon is [lng,lat] pairs) as a line layer.
export function addBoundary(map, polygon) {
  if (!polygon || polygon.length < 3) return;
  const ring = polygon.map((p) => [p[0], p[1]]);
  if (ring.length) ring.push(ring[0]); // close the ring
  map.addSource("boundary", {
    type: "geojson",
    data: {
      type: "Feature",
      geometry: { type: "LineString", coordinates: ring },
      properties: {},
    },
  });
  map.addLayer({
    id: "boundary-line",
    type: "line",
    source: "boundary",
    paint: {
      "line-color": "#156f55",
      "line-width": 2,
      "line-dasharray": [3, 2],
    },
  });
}
