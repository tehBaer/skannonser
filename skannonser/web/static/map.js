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
const CLUSTER_RADIUS = 22;
const CLUSTER_MAX_ZOOM = 10;

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
// One clustered source per (boligtype, active|sold) pair -- so a cluster only
// merges same-boligtype listings AND active never merges with sold, while both
// active and sold clusters are still coloured by their boligtype. Active vs sold
// is distinguished by BORDER colour (active dark, sold white).

const TYPE_GROUP_PREFIX = "listings-";
const UNKNOWN_TYPE_KEY = "__unknown__";

function typeKey(type) {
  return (
    String(type || "").toLowerCase().replace(/[^a-z0-9]/gi, "") || UNKNOWN_TYPE_KEY
  );
}
// variant: "active" | "sold" | "both"
function groupId(type, variant) {
  return TYPE_GROUP_PREFIX + typeKey(type) + "-" + variant;
}

// Three source variants per boligtype so a toggle can switch between clustering
// active/sold SEPARATELY (active + sold variants) and TOGETHER (both variant),
// without adding/removing sources at runtime -- the unused variant just stays
// empty. hasActive/hasSold say which listings a variant renders.
export function buildGroups(boligtyper, colorByType) {
  const groups = [];
  const seen = new Set();
  (boligtyper || []).concat([""]).forEach((t) => {
    const k = typeKey(t);
    if (seen.has(k)) return;
    seen.add(k);
    const color = (colorByType && colorByType[t]) || DEFAULT_UNKNOWN_TYPE_COLOR;
    groups.push({ id: groupId(t, "active"), type: t, color, hasActive: true, hasSold: false });
    groups.push({ id: groupId(t, "sold"), type: t, color, hasActive: false, hasSold: true });
    groups.push({ id: groupId(t, "both"), type: t, color, hasActive: true, hasSold: true });
  });
  return groups;
}

// Which source a listing belongs to: by boligtype, and by the current
// clustering mode. `combineSold` -> the "both" variant (active+sold together);
// otherwise the "active"/"sold" split. `validIds` folds an unrecognised type
// into the unknown bucket, preserving the variant.
export function groupIdForItem(item, validIds, combineSold) {
  const variant = combineSold ? "both" : item.sold ? "sold" : "active";
  const id = groupId(item.boligtype || "", variant);
  if (validIds && !validIds.has(id)) return groupId("", variant);
  return id;
}

const NOT_CLUSTER = ["!", ["has", "point_count"]];

// Per-feature opacity: app.js precomputes an `op` property (1, or the dimmed
// residual) on every listing feature; clusters have no `op` (coalesce -> 1).
const OP = ["coalesce", ["get", "op"], 1];

// One small bordered square canvas icon per (fill, stroke) colour pair (DNB
// points), keyed by both, registered once.
function ensureSquareIcon(map, color, strokeColor) {
  const stroke = strokeColor || "#ffffff";
  const safe = (c) => c.replace(/[^a-z0-9]/gi, "");
  const name = "dnb-sq-" + safe(color) + "-" + safe(stroke);
  if (map.hasImage(name)) return name;
  const size = 18;
  const cvs = document.createElement("canvas");
  cvs.width = size;
  cvs.height = size;
  const ctx = cvs.getContext("2d");
  ctx.fillStyle = color;
  ctx.fillRect(0, 0, size, size);
  ctx.strokeStyle = stroke;
  ctx.lineWidth = 2;
  ctx.strokeRect(1, 1, size - 2, size - 2);
  const data = ctx.getImageData(0, 0, size, size);
  map.addImage(name, { width: size, height: size, data: data.data });
  return name;
}

// Border convention: ACTIVE listings get a black border, SOLD keep a white
// border (both are coloured by boligtype).
const ACTIVE_BORDER = "#111111";
const SOLD_BORDER = "#ffffff";

const IS_SOLD = ["==", ["get", "sold"], true];
const NOT_SOLD = ["==", ["get", "sold"], false];

// Adds one clustered source per group, with unclustered GL layers. Both active
// and sold are coloured by their boligtype (g.color); active gets a dark border,
// sold a white border. Layers are gated by g.hasActive/g.hasSold so a "both"
// source renders active AND sold, while active/sold variants render just one.
export function addListingGroups(map, groups, onListingClick) {
  const clickLayers = [];
  groups.forEach((g) => {
    map.addSource(g.id, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
      cluster: true,
      clusterRadius: CLUSTER_RADIUS,
      clusterMaxZoom: CLUSTER_MAX_ZOOM,
      // Aggregate each member's per-feature opacity so a cluster bubble can be
      // faded in proportion to how many of its listings are dimmed (nedtoning).
      clusterProperties: {
        op_sum: ["+", ["get", "op"]],
      },
    });

    if (g.hasActive) {
      map.addLayer({
        id: g.id + "-eie",
        type: "circle",
        source: g.id,
        filter: ["all", NOT_CLUSTER, NOT_SOLD, ["==", ["get", "source"], "eie"]],
        paint: {
          "circle-color": g.color,
          "circle-radius": 7,
          "circle-stroke-width": 1.5,
          "circle-stroke-color": ACTIVE_BORDER, // active = dark border
          "circle-opacity": OP,
          "circle-stroke-opacity": OP,
        },
      });
      map.addLayer({
        id: g.id + "-dnb",
        type: "symbol",
        source: g.id,
        filter: ["all", NOT_CLUSTER, NOT_SOLD, ["==", ["get", "source"], "dnb"]],
        layout: {
          "icon-image": ensureSquareIcon(map, g.color, ACTIVE_BORDER),
          "icon-size": 1,
          "icon-allow-overlap": true,
        },
        paint: { "icon-opacity": OP },
      });
      clickLayers.push(g.id + "-eie", g.id + "-dnb");
    }
    if (g.hasSold) {
      map.addLayer({
        id: g.id + "-sold",
        type: "circle",
        source: g.id,
        filter: ["all", NOT_CLUSTER, IS_SOLD],
        paint: {
          "circle-color": g.color, // sold coloured by boligtype
          "circle-radius": 6,
          "circle-stroke-width": 1.5,
          "circle-stroke-color": SOLD_BORDER, // sold = white border
          "circle-opacity": OP,
          "circle-stroke-opacity": OP,
        },
      });
      clickLayers.push(g.id + "-sold");
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

export function clampOpacity(v) {
  return String(Math.max(0.15, Math.min(1, v)));
}

// Read a feature's `op` (per-feature opacity), defaulting to 1. Must treat 0 as
// a real value (a fully toned-down listing), so no truthiness shortcuts.
export function opOf(feature) {
  const raw = feature && feature.properties ? feature.properties.op : undefined;
  const n = typeof raw === "number" ? raw : Number(raw);
  return Number.isFinite(n) ? n : 1;
}

// Fade a cluster bubble by the average opacity of its members, so dimming
// (nedtoning) carries through to clusters and not just individual points.
//
// Fast path: the `op_sum` clusterProperty. Fallback: average the actual cluster
// leaves -- aggregated cluster properties are not reliably surfaced through
// `querySourceFeatures`, so we never depend on them alone.
function applyClusterOpacity(div, src, clusterId, count, opSum) {
  if (typeof opSum === "number" && count > 0) {
    div.style.opacity = clampOpacity(opSum / count);
    return;
  }
  div.style.opacity = "1";
  if (!src || typeof src.getClusterLeaves !== "function" || !count) return;
  try {
    const p = src.getClusterLeaves(clusterId, Math.min(count, 200), 0);
    if (!p || typeof p.then !== "function") return;
    p.then((leaves) => {
      if (!leaves || !leaves.length) return;
      const sum = leaves.reduce((acc, lf) => acc + opOf(lf), 0);
      div.style.opacity = clampOpacity(sum / leaves.length);
    }).catch(() => {});
  } catch (_) {
    /* leaves unavailable -> leave the bubble solid */
  }
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
      const count = f.properties.point_count;
      const size = clusterSize(count);
      // Wrapper is the maplibregl.Marker element -- MapLibre manages ITS opacity
      // per render frame (v4 behaviour), so the visible bubble is an INNER div
      // whose opacity/style we own and MapLibre never touches.
      const wrap = document.createElement("div");
      const div = document.createElement("div");
      div.className = "cluster-marker";
      div.style.width = size + "px";
      div.style.height = size + "px";
      div.style.background = g.color;
      // Sold-only bubbles get a white border, active (and mixed "both") dark.
      div.style.borderColor = g.hasSold && !g.hasActive ? SOLD_BORDER : ACTIVE_BORDER;
      div.textContent = f.properties.point_count_abbreviated;
      wrap.appendChild(div);
      const clusterId = f.properties.cluster_id;
      const coords = f.geometry.coordinates;
      // An all-dimmed cluster reads as toned-down, a fully-matching one solid.
      applyClusterOpacity(div, src, clusterId, count, f.properties.op_sum);
      wrap.addEventListener("click", () => {
        src.getClusterExpansionZoom(clusterId).then((zoom) => {
          map.easeTo({ center: coords, zoom });
        });
      });
      cache[key] = new maplibregl.Marker({ element: wrap })
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
