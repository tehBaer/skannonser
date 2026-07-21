// Station overlays + commute-to-Sandvika logic (Phase 5 Task 7).
//
// PORT PROVENANCE (apps_script/map/map.html):
//   * Line-colour mapping ...... FIXED_LINE_DEFAULT_COLORS map.html 1049-1064,
//                                PALETTE_COLORS 1069-1074, hashString 3888-3895,
//                                getDefaultLineColor/getStationLineColor 3897-3912,
//                                DEFAULT_UNKNOWN_LINE_COLOR 1045.
//   * normalizeLineId .......... map.html 3874-3878.
//   * haversineMeters .......... used by nearestStationDistanceM 2923-2936.
//   * effective station radius . getEffectiveStationRadiusM 2573-2579
//                                (default-radius fallback).
//   * commute (Sandvika) ....... parseStationSandvikaDirectMinutes 1609-1619,
//                                buildOsloToSandvikaTransferMinutes 1632-1673,
//                                parseStationSandvikaTravelMinutes 1702-1727,
//                                isSandvikaFilterDisabled 1735-1742,
//                                isStationVisibleBySandvika 1744-1753.
//   * nearest-station / radius . nearestStationDistanceM 2923-2936, dim logic
//                                renderMarkersOnly 2783-2796.
//
// SIMPLIFICATIONS vs legacy (see task-7-report.md "ported vs simplified"):
//   * The legacy client re-derived per-line direct minutes from raw
//     TO_SANDVIKA_MIN with an isotonic (PAVA) fit over distance-sorted
//     stations (map.html buildSandvikaNormalizedMinutes 1531-1593) and
//     synthesised the Oslo-S transfer leg per line (1632-1673). The rebuild
//     API already serves each station's `travel` pre-merged to the MINIMUM
//     across its lines -- `travel["Sandvika"]` (direct) and
//     `travel["Sandvika Transfer"]` (the transfer fallback leg) -- so the
//     isotonic fit and per-line transfer synthesis are done server-side/omitted.
//     We consume the two pre-computed values directly.
//   * Because `travel` is station-level (no per-line minutes in /api/meta),
//     "direct minutes only counts if THAT line is visible" is approximated at
//     station granularity: the direct value counts when the station has >=1
//     visible line; the transfer value is the fallback (gated by the
//     include-transfer toggle). effective = min of the available two.
//   * "Default station radius" slider + "Refresh radii" + station opacity /
//     stroke-weight / persistent name labels (includeStationNames) are NOT
//     ported; radius comes straight off `radius_m` with a fixed default
//     fallback, and station names show on hover/click of the ring.

/* global maplibregl */

export const STATION_SOURCE_ID = "stations";
export const STATION_CIRCLE_LAYER = "station-circles";
export const STATION_RING_LAYER = "station-ring";

// getEffectiveStationRadiusM default (map.html defaultStationRadius default 1000).
export const DEFAULT_STATION_RADIUS_M = 1000;

// station.travel destination keys (see /api/meta `_stations_meta`, and the live
// station_travel.destination values: "Sandvika", "Sandvika Transfer").
const SANDVIKA_KEY = "Sandvika";
const SANDVIKA_TRANSFER_KEY = "Sandvika Transfer";

// Commute slider max: at this value the filter is "off" (all stations pass) --
// mirrors isSandvikaFilterDisabled (value >= max), map.html 1735-1742.
export const SANDVIKA_MAX = 120;

const UNASSIGNED_LINE_LABEL = "UNASSIGNED";
export const DEFAULT_UNKNOWN_LINE_COLOR = "#6f7e76";

// --- ported line palette (map.html 1049-1074) ---
const FIXED_LINE_DEFAULT_COLORS = {
  L1: "#1f77b4", L2: "#2ca02c", R10: "#d62728", R11: "#ff7f0e", R12: "#9467bd",
  R13: "#8c564b", R14: "#17becf", R21: "#1f4e79", R22: "#2e8b57", R23: "#bc5090",
  RE11: "#e41a1c", RE20: "#377eb8", RE30: "#4daf4a", UNASSIGNED: "#6f7e76",
};
const LINE_PALETTE = [
  "#0f8c56", "#be3a34", "#0f4c81", "#e08a00", "#6f53b3",
  "#0c8f9c", "#8f6a1f", "#7f3567", "#2f5f40", "#8c2e2e",
  "#1ab3c2", "#d4553a", "#3a7ebf", "#c09830", "#a04580",
  "#4a8f60", "#c14040", "#5577aa", "#999933", "#cc6699",
];

// normalizeLineId (map.html 3874-3878): uppercase-trim, blank -> UNASSIGNED.
export function normalizeLineId(lineId) {
  const s = String(lineId == null ? "" : lineId).trim().toUpperCase();
  return s || UNASSIGNED_LINE_LABEL;
}

// hashString (map.html 3888-3895) -> stable palette index for unmapped lines.
function hashString(text) {
  let hash = 0;
  const value = String(text || "");
  for (let i = 0; i < value.length; i++) {
    hash = ((hash << 5) - hash + value.charCodeAt(i)) >>> 0;
  }
  return hash;
}

// getStationLineColor (map.html 3897-3912): fixed map first, else deterministic
// palette pick by hash. No user recolouring in the rebuild (legacy stored
// overrides in LINE_COLOR_STORAGE_KEY) -- the mapping is fixed/derived.
export function lineColor(lineId) {
  const normalized = normalizeLineId(lineId);
  if (FIXED_LINE_DEFAULT_COLORS[normalized]) return FIXED_LINE_DEFAULT_COLORS[normalized];
  const idx = hashString(normalized) % LINE_PALETTE.length;
  return LINE_PALETTE[idx] || DEFAULT_UNKNOWN_LINE_COLOR;
}

// Distinct, sorted, normalized line ids across all stations.
export function distinctLines(stations) {
  const set = new Set();
  (stations || []).forEach((s) => {
    (s.lines || []).forEach((l) => set.add(normalizeLineId(l)));
  });
  return Array.from(set).sort();
}

function stationLineIds(station) {
  const ids = (station.lines || [])
    .map(normalizeLineId)
    .filter((l) => l && l !== UNASSIGNED_LINE_LABEL);
  return ids.length ? ids : [UNASSIGNED_LINE_LABEL];
}

// --- geometry ---

export function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
}

// Geodesic circle as a closed [lng,lat] ring of `points` samples -- a REAL
// geographic radius (metres), not a screen-pixel circle. Uses the spherical
// destination-point formula so the polygon stays a true metre-radius circle at
// any latitude/zoom.
export function geodesicCircle(lng, lat, radiusM, points = 64) {
  const R = 6371000;
  const latR = (lat * Math.PI) / 180;
  const lngR = (lng * Math.PI) / 180;
  const d = radiusM / R;
  const ring = [];
  for (let i = 0; i <= points; i++) {
    const brng = (2 * Math.PI * i) / points;
    const lat2 = Math.asin(
      Math.sin(latR) * Math.cos(d) + Math.cos(latR) * Math.sin(d) * Math.cos(brng)
    );
    const lng2 =
      lngR +
      Math.atan2(
        Math.sin(brng) * Math.sin(d) * Math.cos(latR),
        Math.cos(d) - Math.sin(latR) * Math.sin(lat2)
      );
    ring.push([(lng2 * 180) / Math.PI, (lat2 * 180) / Math.PI]);
  }
  return ring;
}

// getEffectiveStationRadiusM (map.html 2573-2579).
export function effectiveStationRadiusM(station) {
  const r = Number(station && station.radius_m);
  return Number.isFinite(r) && r > 0 ? r : DEFAULT_STATION_RADIUS_M;
}

// One Polygon feature per (station, line): each line's ring in its own colour,
// filterable by line visibility -- mirrors getExpandedStations (map.html
// 3957-3992) drawing one Circle per (station, line).
export function stationCircleFeatures(stations) {
  const features = [];
  (stations || []).forEach((station) => {
    if (station.lat == null || station.lng == null) return;
    const radiusM = effectiveStationRadiusM(station);
    const ring = geodesicCircle(station.lng, station.lat, radiusM);
    stationLineIds(station).forEach((line) => {
      features.push({
        type: "Feature",
        geometry: { type: "Polygon", coordinates: [ring] },
        properties: { name: station.name || "Stasjon", line, color: lineColor(line) },
      });
    });
  });
  return { type: "FeatureCollection", features };
}

// --- commute (Sandvika) semantics ---

function finiteOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

// effective minutes-to-Sandvika for a station (see file header "SIMPLIFICATIONS").
// direct counts only when the station has >=1 visible line; transfer is the
// fallback, gated by includeTransfer. Returns min of the available values, or
// null when neither is usable.
export function effectiveSandvikaMinutes(station, { visibleLines, includeTransfer }) {
  const travel = station.travel || {};
  const hasVisibleLine = stationLineIds(station).some(
    (l) => !visibleLines || visibleLines.has(l)
  );
  const direct = hasVisibleLine ? finiteOrNull(travel[SANDVIKA_KEY]) : null;
  const transfer = includeTransfer ? finiteOrNull(travel[SANDVIKA_TRANSFER_KEY]) : null;
  const candidates = [direct, transfer].filter((v) => v != null);
  if (!candidates.length) return null;
  return Math.min(...candidates);
}

// isSandvikaFilterDisabled (map.html 1735-1742): at/above the slider max -> off.
export function commuteDisabled(sandvikaMax) {
  return Number(sandvikaMax) >= SANDVIKA_MAX;
}

// isStationVisibleBySandvika (map.html 1744-1753).
export function stationCommuteVisible(station, opts) {
  if (commuteDisabled(opts.sandvikaMax)) return true;
  const mins = effectiveSandvikaMinutes(station, opts);
  return mins != null && mins <= Number(opts.sandvikaMax);
}

// Nearest station whose radius covers `item`, restricted to stations with >=1
// visible line -- the "in-radius" set both the commute filter and the
// hide-outside toggle key off (nearestStationDistanceM + inVicinity, map.html
// 2783-2790). Returns {station, distanceM} or null.
export function nearestCoveringStation(item, stations, visibleLines) {
  let best = null;
  (stations || []).forEach((station) => {
    if (station.lat == null || station.lng == null) return;
    const hasVisibleLine = stationLineIds(station).some(
      (l) => !visibleLines || visibleLines.has(l)
    );
    if (!hasVisibleLine) return;
    const d = haversineMeters(item.lat, item.lng, station.lat, station.lng);
    if (d > effectiveStationRadiusM(station)) return; // not in radius
    if (!best || d < best.distanceM) best = { station, distanceM: d };
  });
  return best;
}

// True when at least one line-visible station exists (guards the hide-outside
// toggle, mirroring `activeStations.length > 0`, map.html 2790).
export function anyLineVisibleStation(stations, visibleLines) {
  return (stations || []).some((station) => {
    if (station.lat == null || station.lng == null) return false;
    return stationLineIds(station).some((l) => !visibleLines || visibleLines.has(l));
  });
}

// --- map layers ---

export function addStationLayers(map) {
  map.addSource(STATION_SOURCE_ID, {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
  });
  // A near-transparent fill so the whole disc is hoverable for the name popup
  // (legacy used fillOpacity 0 + separate label markers; we fold the two into a
  // faint fill + a hover popup).
  map.addLayer({
    id: STATION_CIRCLE_LAYER,
    type: "fill",
    source: STATION_SOURCE_ID,
    paint: { "fill-color": ["get", "color"], "fill-opacity": 0.05 },
  });
  map.addLayer({
    id: STATION_RING_LAYER,
    type: "line",
    source: STATION_SOURCE_ID,
    paint: { "line-color": ["get", "color"], "line-width": 2, "line-opacity": 0.85 },
  });
}

// Recompute the station source (visible lines + commute-visible stations only)
// and toggle the master "show stations" visibility.
export function updateStationLayers(map, stations, ui) {
  const src = map.getSource(STATION_SOURCE_ID);
  if (!src) return;
  const visibleLines = visibleLineSet(ui);
  const opts = {
    visibleLines,
    includeTransfer: ui.stations.includeTransfer,
    sandvikaMax: ui.stations.sandvikaMax,
  };
  const kept = (stations || []).filter((s) => {
    const lines = (s.lines || []).map(normalizeLineId);
    const anyVisible = lines.length
      ? lines.some((l) => visibleLines.has(l))
      : true;
    return anyVisible && stationCommuteVisible(s, opts);
  });
  src.setData(stationCircleFeatures(kept));

  const vis = ui.stations.show ? "visible" : "none";
  [STATION_CIRCLE_LAYER, STATION_RING_LAYER].forEach((id) => {
    if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", vis);
  });
}

// Set of visible normalized line ids given the persisted `lineHidden` map.
export function visibleLineSet(ui) {
  const hidden = (ui.stations && ui.stations.lineHidden) || {};
  const set = new Set();
  (ui._allLines || []).forEach((l) => {
    if (!hidden[l]) set.add(l);
  });
  return set;
}

// Bind the hover/click station-name popup to the circle layers.
export function wireStationNamePopup(map) {
  let popup = null;
  const show = (e) => {
    const f = e.features && e.features[0];
    if (!f) return;
    map.getCanvas().style.cursor = "pointer";
    if (!popup) popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
    const name = f.properties.name || "Stasjon";
    const line = f.properties.line && f.properties.line !== UNASSIGNED_LINE_LABEL
      ? " (" + f.properties.line + ")"
      : "";
    popup.setLngLat(e.lngLat).setHTML('<div class="sk-station-name"></div>').addTo(map);
    popup.getElement().querySelector(".sk-station-name").textContent = name + line;
  };
  const hide = () => {
    map.getCanvas().style.cursor = "";
    if (popup) { popup.remove(); popup = null; }
  };
  [STATION_CIRCLE_LAYER, STATION_RING_LAYER].forEach((id) => {
    map.on("mouseenter", id, show);
    map.on("mousemove", id, show);
    map.on("mouseleave", id, hide);
    map.on("click", id, show);
  });
}
