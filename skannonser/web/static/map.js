// MapLibre map core: OSM raster base, PER-GROUP clustered sources, unclustered
// GL layers, and DOM cluster markers.
//
// CLUSTER PARTITIONING: MapLibre/supercluster clusters every point in a source
// together, so to make a cluster only ever combine listings of the SAME
// boligtype we use one clustered source per boligtype (plus a separate "sold"
// group). Each group's cluster bubbles are coloured by that group's colour.
//
// Cluster COUNT rendering uses DOM markers rather than a GL symbol/text layer on
// purpose: GL text needs a `glyphs` font-server URL, and the only options are an
// external CDN (banned) or vendoring glyph PBFs. The DOM-marker pattern is the
// documented offline-safe way to show cluster counts.
//
// BOLIGTYPE PALETTE: ported verbatim from apps_script/map/map.html --
//   * TYPE_COLOR_PALETTE            -> map.html lines 1046-1048
//   * FIXED_BOLIGTYPE_DEFAULT_COLORS -> map.html lines 1065-1067
//   * DEFAULT_UNKNOWN_TYPE_COLOR     -> map.html line 1042
// We assign palette colours over the alphabetically-sorted boligtyper from
// /api/meta (deterministic), applying the tomannsbolig fixed override and the
// grey unknown default -- the same colour set, same rules.

/* global maplibregl */

// --- ported palette (see header) ---
export const TYPE_COLOR_PALETTE = [
  "#0f8c56", "#be3a34", "#0f4c81", "#e08a00", "#6f53b3",
  "#0c8f9c", "#8f6a1f", "#7f3567", "#2f5f40", "#8c2e2e",
];
export const FIXED_BOLIGTYPE_DEFAULT_COLORS = { tomannsbolig: "#f2d34f" };
export const DEFAULT_UNKNOWN_TYPE_COLOR = "#6f7e76";

const SOLD_COLOR = "#9aa5a0";

// Clustering knobs. Lower clusterMaxZoom => clusters break into individual
// points sooner as you zoom in; smaller clusterRadius => fewer points merge.
const CLUSTER_RADIUS = 28;
const CLUSTER_MAX_ZOOM = 12;

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

// --- group model ------------------------------------------------------------
// One clustered source per group: a single "sold" group (all sold together,
// grey) + one per boligtype (coloured) + an unknown/"" bucket.

export const SOLD_GROUP_ID = "listings-sold";
const TYPE_GROUP_PREFIX = "listings-type-";
const UNKNOWN_TYPE_KEY = "__unknown__";

function typeKey(type) {
  return (
    String(type || "").toLowerCase().replace(/[^a-z0-9]/gi, "") || UNKNOWN_TYPE_KEY
  );
}
function typeGroupId(type) {
  return TYPE_GROUP_PREFIX + typeKey(type);
}

export function buildGroups(boligtyper, colorByType) {
  const groups = [
    { id: SOLD_GROUP_ID, isSold: true, type: null, color: SOLD_COLOR },
  ];
  const seen = new Set();
  (boligtyper || []).concat([""]).forEach((t) => {
    const id = typeGroupId(t);
    if (seen.has(id)) return;
    seen.add(id);
    groups.push({
      id,
      isSold: false,
      type: t,
      color: (colorByType && colorByType[t]) || DEFAULT_UNKNOWN_TYPE_COLOR,
    });
  });
  return groups;
}

// Which group a listing belongs to. Sold always -> the sold group; otherwise by
// boligtype. `validIds` (optional) folds an unrecognised type into unknown.
export function groupIdForItem(item, validIds) {
  if (item.sold) return SOLD_GROUP_ID;
  const id = typeGroupId(item.boligtype || "");
  if (validIds && !validIds.has(id)) return typeGroupId("");
  return id;
}

const NOT_CLUSTER = ["!", ["has", "point_count"]];

// Per-feature opacity: app.js precomputes an `op` property (1, or the dimmed
// residual) on every listing feature; clusters have no `op` (coalesce -> 1).
const OP = ["coalesce", ["get", "op"], 1];

// One small white-bordered square canvas icon per colour (DNB points), keyed by
// colour hex, registered once.
function ensureSquareIcon(map, color) {
  const name = "dnb-sq-" + color.replace(/[^a-z0-9]/gi, "");
  if (map.hasImage(name)) return name;
  const size = 18;
  const cvs = document.createElement("canvas");
  cvs.width = size;
  cvs.height = size;
  const ctx = cvs.getContext("2d");
  ctx.fillStyle = color;
  ctx.fillRect(0, 0, size, size);
  ctx.strokeStyle = "#ffffff";
  ctx.lineWidth = 2;
  ctx.strokeRect(1, 1, size - 2, size - 2);
  const data = ctx.getImageData(0, 0, size, size);
  map.addImage(name, { width: size, height: size, data: data.data });
  return name;
}

// Adds one clustered source per group, with unclustered GL layers:
//  * sold group -> grey circle
//  * type group -> Eie circle + DNB square, both in the type's colour
// So a cluster only merges same-boligtype listings; sold stays its own group.
export function addListingGroups(map, groups, onListingClick) {
  const clickLayers = [];
  groups.forEach((g) => {
    map.addSource(g.id, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
      cluster: true,
      clusterRadius: CLUSTER_RADIUS,
      clusterMaxZoom: CLUSTER_MAX_ZOOM,
    });

    if (g.isSold) {
      map.addLayer({
        id: g.id + "-pt",
        type: "circle",
        source: g.id,
        filter: NOT_CLUSTER,
        paint: {
          "circle-color": SOLD_COLOR,
          "circle-radius": 6,
          "circle-stroke-width": 1.5,
          "circle-stroke-color": "#ffffff",
          "circle-opacity": OP,
          "circle-stroke-opacity": OP,
        },
      });
      clickLayers.push(g.id + "-pt");
    } else {
      map.addLayer({
        id: g.id + "-eie",
        type: "circle",
        source: g.id,
        filter: ["all", NOT_CLUSTER, ["==", ["get", "source"], "eie"]],
        paint: {
          "circle-color": g.color,
          "circle-radius": 7,
          "circle-stroke-width": 1.5,
          "circle-stroke-color": "#ffffff",
          "circle-opacity": OP,
          "circle-stroke-opacity": OP,
        },
      });
      map.addLayer({
        id: g.id + "-dnb",
        type: "symbol",
        source: g.id,
        filter: ["all", NOT_CLUSTER, ["==", ["get", "source"], "dnb"]],
        layout: {
          "icon-image": ensureSquareIcon(map, g.color),
          "icon-size": 1,
          "icon-allow-overlap": true,
        },
        paint: { "icon-opacity": OP },
      });
      clickLayers.push(g.id + "-eie", g.id + "-dnb");
    }
  });

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

// Continuous cluster-bubble size (px), scaling with sqrt(count) so a 200-point
// cluster reads clearly bigger than a 20-point one, clamped to a sane range.
export function clusterSize(count) {
  return Math.max(26, Math.min(60, Math.round(20 + 5.5 * Math.sqrt(count))));
}

// Removes and forgets every cached cluster DOM marker. MUST be called before
// any setData() that can change a clustered feature set (filter toggles,
// sold-visibility, boligtype visibility, ...). WHY: supercluster reuses
// cluster_id values keyed by tree position, so after setData() a given id can
// refer to a different cluster; syncClusterMarkers treats a cache hit as
// "already correct", so a stale entry would show the wrong bubble until an
// unrelated pan/zoom evicts it. Clearing forces a fresh rebuild.
export function clearClusterCache(cache) {
  Object.keys(cache).forEach((id) => {
    cache[id].remove();
    delete cache[id];
  });
}

// DOM cluster markers across ALL group sources, each coloured by its group.
// `cache` is a caller-held object mapping key -> maplibregl.Marker; call on
// 'render'/'moveend'. Keys are namespaced by group id because cluster_id is only
// unique within a single source.
export function syncClusterMarkers(map, groups, cache) {
  const seen = {};
  groups.forEach((g) => {
    const src = map.getSource(g.id);
    if (!src || !map.isSourceLoaded(g.id)) return;
    const features = map.querySourceFeatures(g.id, {
      filter: ["has", "point_count"],
    });
    features.forEach((f) => {
      const key = g.id + ":" + f.properties.cluster_id;
      seen[key] = true;
      if (cache[key]) return;
      const size = clusterSize(f.properties.point_count);
      const div = document.createElement("div");
      div.className = "cluster-marker";
      div.style.width = size + "px";
      div.style.height = size + "px";
      div.style.background = g.color;
      div.textContent = f.properties.point_count_abbreviated;
      const clusterId = f.properties.cluster_id;
      const coords = f.geometry.coordinates;
      div.addEventListener("click", () => {
        src.getClusterExpansionZoom(clusterId).then((zoom) => {
          map.easeTo({ center: coords, zoom });
        });
      });
      cache[key] = new maplibregl.Marker({ element: div })
        .setLngLat(coords)
        .addTo(map);
    });
  });

  Object.keys(cache).forEach((key) => {
    if (!seen[key]) {
      cache[key].remove();
      delete cache[key];
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
