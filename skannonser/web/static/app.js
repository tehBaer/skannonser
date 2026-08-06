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
import { assignTagColors, colorForTag } from "./tagcolors.js";
import { buildPopupContent } from "./popup.js";
import { isNew, parseScrapedAt, premiumPct, TILGJENGELIGHET_OPTIONS } from "./listingmeta.js";
import {
  listingExcluded,
  residualOpacity,
  buildFilterPanelUI,
  buildDisplayUI,
  deriveVocabs,
  selectionChipRow,
  selectionExcludes,
  statusVocabComplete,
  wantsClosed,
} from "./filters.js";
import {
  defaultFilters,
  loadFilters,
  activeFilterEntries,
  subscribeOtherTabs,
  resetFilters,
  pruneFilterSets,
  seedStatus,
} from "./filterstate.js";
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
  lineColor,
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
    // `sold`/`inactive` used to live here as layer toggles. Status visibility
    // is now the shared filters.tilgjengelighetSelected, seeded to [""] --
    // which reproduces the old false/false default exactly. A stored
    // `sold`/`inactive` is not migrated; loadUi deletes it explicitly (its
    // `...base, ...stored` merge would otherwise let a stale value survive
    // and be re-persisted forever, since neither key is read by anything).
    filters: defaultFilters(meta),
    dimIntensity: 75, // % dimming for non-matching listings
    // Sold-only dimming defaults ON (50 %): with thousands of sold dots at
    // full strength the actives drown -- subdued-by-default keeps the sold
    // layer readable the moment it's toggled on. Slide to 0 for full colour.
    soldDim: 50,
    // Budpremie colouring is retired for now (owner, 2026-07-26): the control is
    // gone from the sidebar but setSoldColorMode/PREMIUM_* remain in map.js so it
    // can be brought back. Forced false on load so a stored `true` from before the
    // control disappeared cannot strand anyone in premium colours.
    soldPremium: false,
    combineSold: false, // cluster sold + active together (vs separately)
    collapsed: {}, // {panelId: true} -> sidebar panel collapsed
    stations: {
      show: false,
      showRadius: true,
      hideOutside: false,
      includeTransfer: false,
      sandvikaMax: SANDVIKA_MAX, // == max -> commute filter off
      // Empty == every line shows, same as the sidebar's value filters.
      lineSelected: [],
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
  tagColors: new Map(),
  // The DOM node currently inside state.popup, so it can be flushed when the
  // popup closes or its content is swapped out from under it.
  popupContent: null,
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
        filters: loadFilters(meta),
        collapsed: { ...(stored.collapsed || {}) },
        stations: {
          ...base.stations,
          ...(stored.stations || {}),
          lineSelected: [...((stored.stations || {}).lineSelected || [])],
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
      // Legacy roots migrated into filters by loadFilters -- strip them so
      // saveUi can never re-persist the old shape.
      delete ui.boligtypeHidden;
      delete ui.tagHidden;
      // Retired layer toggles (see defaultUi above): nothing reads them, but
      // the ...base, ...stored spread would otherwise let a stored value
      // survive and be re-persisted forever.
      delete ui.sold;
      delete ui.inactive;
      // Same 2026-07-26 hidden-set -> selection conversion, for the station
      // lines. The ...stored.stations spread above copies the old key straight
      // through, so drop it here or saveUi re-persists it forever.
      delete ui.stations.lineHidden;
      // Budpremie colouring is retired (owner, 2026-07-26): the ...stored spread
      // above would otherwise let a pre-existing `soldPremium: true` survive
      // forever, with no control left in the sidebar to turn it back off.
      ui.soldPremium = false;
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
  if (item.closed) return "inactive";
  if (item.source === "dnb") return "dnb";
  return "eie";
}

// Vocabularies must describe what the user can actually SEE. Deriving them from
// every loaded item strands values from a switched-off bucket in the filter UI
// forever, because the item store only ever grows -- once the sold bucket is
// fetched it stays for the session. Scope this to the LAYER toggles only:
// deriving from "passes all filters" instead would make a tag vanish the moment
// a price slider hid it, leaving no way to click it back.
function vocabItems() {
  return [...state.itemsById.values()].filter((it) => {
    const bucket = bucketOf(it);
    if (bucket === "eie" || bucket === "dnb") return state.ui[bucket];
    return !selectionExcludes(
      state.ui.filters.tilgjengelighetSelected, it.tilgjengelighet || ""
    );
  });
}

// Whether vocabItems() currently covers EVERY listing this app can hold, which
// is the only state in which deleting a stored filter value is safe (see
// pruneFilterSets). Any switched-off bucket, or a not-yet-fetched closed
// bucket, means a value can be absent from the vocabulary while still very
// much existing -- and the deletion is irreversible and shared with the table.
function vocabIsComplete() {
  return Boolean(
    state.ui.eie &&
      state.ui.dnb &&
      statusVocabComplete(state.ui.filters.tilgjengelighetSelected) &&
      state.soldLoaded
  );
}

// Per-listing dim decision: metric filters OR commute OR hide-outside-radius.
// `ctx` carries the once-per-recompute station context.
function isDimmed(item, ctx) {
  if (listingExcluded(item, state.ui.filters, state.meta)) return true;

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
  const properties = {
    finnkode: item.finnkode,
    source: item.source,
    sold: !!item.sold,
    closed: !!item.closed,
    boligtype: item.boligtype || "",
    op, // 1, or the dimmed residual opacity (see filters.residualOpacity)
  };
  // The ring itself is white-on-black now, not per-tag coloured, so the ring
  // layer needs the flag but no longer the colour. colorForTag stays the test
  // for "is this a tag we know", which is what hasTag has always meant.
  if (colorForTag(item.tag, state.tagColors || new Map())) {
    properties.hasTag = true; // drives the tag-ring layer
  }
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
  // Rebuilt every recompute: cheap (one hash per distinct tag) and always
  // in sync with the current tag set -- popup chips read this same map.
  state.tagColors = assignTagColors(
    [...state.itemsById.values()].map((i) => i.tag)
  );
  const ctx = {
    stations: state.meta.stations || [],
    visibleLines: visibleLineSet(state.ui),
    commuteEnabled: !commuteDisabled(state.ui.stations.sandvikaMax),
    anyStation: anyLineVisibleStation(state.meta.stations || [], visibleLineSet(state.ui)),
  };
  const residual = residualOpacity(state.ui);
  // Sold listings now follow the filters + "Nedtoning" like active ones do;
  // only PASSING sold dots additionally get the separate "Solgt nedtoning"
  // slider (see the op ternary below).
  const soldPct = Math.max(0, Math.min(100, Number(state.ui.soldDim) || 0));
  const soldOpacity = 1 - soldPct / 100;
  const byGroup = {};
  let shown = 0;
  state.groups.forEach((g) => (byGroup[g.id] = []));
  const hideExcluded = Number(state.ui.dimIntensity) >= 100;
  state.itemsById.forEach((item) => {
    if (item.lat == null || item.lng == null) return;
    const bucket = bucketOf(item);
    // Source layers (Finn.no / DNB) still gate on ui; status gates on the
    // shared filter. A hard `return`, not a dim: filters dim (residualOpacity)
    // and only hide at Nedtoning 100 %, so falling through to listingExcluded
    // would paint ~3500 faint sold dots on a default load. This preserves what
    // the old layer toggles for Solgt and Inaktiv/Trukket did.
    if (bucket === "eie" || bucket === "dnb") {
      if (!state.ui[bucket]) return;
    } else if (
      selectionExcludes(state.ui.filters.tilgjengelighetSelected, item.tilgjengelighet || "")
    ) {
      return;
    }
    const excluded = isDimmed(item, ctx);
    // Nedtoning at 100 % = today's hard-hide (incl. cluster counts).
    if (excluded && hideExcluded) return;
    const gid = groupIdForItem(item, state.validGroupIds, state.ui.combineSold);
    if (!byGroup[gid]) return; // safety: no source for this group
    // Sold dots follow the filters too now (approved change): excluded ->
    // filter dim; passing sold dots keep the separate "Solgt nedtoning".
    const op = excluded ? residual : item.closed ? soldOpacity : 1;
    byGroup[gid].push(itemToFeature(item, op));
    shown++;
  });
  // The only point that knows what survived layers + filters + hard-hide.
  state.shownCount = shown;
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
    // A blank map reads as a loading failure, not as a filter result.
    const emptyEl = document.getElementById("map-empty");
    if (emptyEl) emptyEl.hidden = !(state.itemsById.size > 0 && state.shownCount === 0);
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
      rebuildFilterUIs(); // sold items may add tags AND grow other vocabularies
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
  // and the BORDER (active = dark, sold/closed = hollow ring). Swatches use a
  // neutral fill so the border reads, except the hollow row which mirrors the
  // map's ringed markers.
  [
    { label: "Aktiv (mørk kant)", border: "#111111", square: false, hollow: false },
    { label: "Solgt/lukket (ring)", border: DEFAULT_UNKNOWN_TYPE_COLOR, square: false, hollow: true },
    { label: "DNB (kvadrat)", border: "#111111", square: true, hollow: false },
  ].forEach(({ label, border, square, hollow }) => {
    const row = document.createElement("div");
    row.className = "legend-row";
    const sw = document.createElement("span");
    sw.className = "legend-swatch" + (square ? " square" : "");
    // Hollow mirrors the map: closed listings are a ring, not a fill.
    sw.style.background = hollow ? "transparent" : DEFAULT_UNKNOWN_TYPE_COLOR;
    sw.style.border = (hollow ? "3px solid " : "2px solid ") + border;
    row.appendChild(sw);
    row.appendChild(document.createTextNode(label));
    node.appendChild(row);
  });

  // Inactive/withdrawn dots keep their boligtype colour + a white X overlay
  // (see ensureXIcon / the "-inactive-x" layers in map.js) rather than a
  // flat grey fill -- give them their own row with a swatch that mirrors it.
  const mutedRow = document.createElement("div");
  mutedRow.className = "legend-row";
  const mutedSw = document.createElement("span");
  mutedSw.className = "legend-swatch";
  mutedSw.style.background = DEFAULT_UNKNOWN_TYPE_COLOR;
  mutedSw.style.position = "relative";
  const xGlyph = document.createElement("span");
  xGlyph.textContent = "✕";
  xGlyph.style.position = "absolute";
  xGlyph.style.inset = "0";
  xGlyph.style.display = "flex";
  xGlyph.style.alignItems = "center";
  xGlyph.style.justifyContent = "center";
  xGlyph.style.color = "#ffffff";
  xGlyph.style.fontSize = "9px";
  xGlyph.style.lineHeight = "1";
  mutedSw.appendChild(xGlyph);
  mutedRow.appendChild(mutedSw);
  mutedRow.appendChild(document.createTextNode("Inaktiv/Trukket (X)"));
  node.appendChild(mutedRow);
}

function openPopup(finnkode, coordinates) {
  const item = state.itemsById.get(finnkode);
  if (!item) return;
  // Marker -> marker reuses the ONE Popup instance below, so setDOMContent
  // replaces the previous editor's node. Flush it BEFORE that happens, or a
  // kommentar typed and abandoned by clicking the next marker dies with the
  // node. (What does or doesn't fire `close` is covered where the handler is
  // registered, below.)
  flushPopupEditor();
  // A function, not a snapshot: a tag invented in the editor only gains a
  // colour once applyAll() has rebuilt state.tagColors.
  const content = buildPopupContent(item, state.destinations, () => state.tagColors);
  // Sections that fill in asynchronously (Solgt i nabolaget) and the tag chip
  // row both grow the popup after the pan below has measured it -- re-pan when
  // they say so.
  content.addEventListener("sk-popup-resized", () => panPopupIntoView());
  if (!state.popup) {
    // closeOnClick is OFF on purpose; closing on a map click is done by hand
    // below instead. WHY: we reuse ONE Popup instance for every listing, and
    // closeOnClick registers a map-level `click` handler bound to that
    // instance. Clicking marker B while A's popup is open dispatches a single
    // click over a SNAPSHOT of the listener list: the layer delegate runs
    // first and calls openPopup(B), whose addTo() re-adds the same instance --
    // and then the snapshot's now-stale handler still runs and removes the
    // popup we just opened. Symptom: every second marker click appeared to do
    // nothing, and the marker -> marker editor flush could never run.
    // Confirmed on the deployed build: markers 1 and 3 opened, marker 2 did not.
    state.popup = new maplibregl.Popup({ maxWidth: "300px", closeOnClick: false });
    // The close button, Escape, and the hand-rolled map-click close below all
    // route through remove(), which fires this. addTo() ALSO fires it on every
    // marker -> marker swap (addTo re-adds an already-added popup by calling
    // remove() first) -- that path is instead covered by the explicit flush
    // at the top of this function, which must run before the outgoing node
    // is replaced.
    state.popup.on("close", flushPopupEditor);
    // Restores what closeOnClick used to give us, minus the self-inflicted
    // removal: close only when the click landed on bare map. Any rendered
    // feature under the cursor -- a listing dot, its tag ring, a cluster --
    // means the click was aimed at something, so the popup stays. Raster tiles
    // yield no queryable features, so empty map reads as an empty result.
    state.map.on("click", (e) => {
      if (!state.popup || !state.popup.isOpen()) return;
      if (state.map.queryRenderedFeatures(e.point).length === 0) state.popup.remove();
    });
  }
  state.popup
    .setLngLat(coordinates || [item.lng, item.lat])
    .setDOMContent(content)
    .addTo(state.map);
  // addTo() re-adds an existing popup by calling remove() first, which fires
  // `close` -- assign after that, or the handler nulls the handle we just set.
  state.popupContent = content;
  panPopupIntoView();
}

// Commit whatever the outgoing editor was holding. Cleared first so the two
// teardown paths cannot both flush the same node; skFlush is idempotent
// anyway, since commitAnnotation skips a PUT that would change nothing.
function flushPopupEditor() {
  const content = state.popupContent;
  state.popupContent = null;
  if (content && typeof content.skFlush === "function") content.skFlush();
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

// Put the layer buckets back to their DEFAULTS, checkboxes included. Used by
// the empty-state reset: a "Nullstill filtre" that cannot undo a layer toggle
// is a dead button for the user who emptied the map by unchecking Eie and DNB.
// Only eie/dnb remain here -- status visibility moved to the shared
// filters.tilgjengelighetSelected, so the emptyReset handler resets it via
// resetFilters + seedStatus alongside this call, not through this function.
function restoreLayerToggles() {
  const defaults = { eie: true, dnb: true };
  Object.entries(defaults).forEach(([bucket, on]) => {
    state.ui[bucket] = on;
    const cb = document.getElementById("toggle-" + bucket);
    if (cb) cb.checked = on;
  });
  saveUi();
}

function wireLayerToggles() {
  const sources = { eie: "toggle-eie", dnb: "toggle-dnb" };
  Object.entries(sources).forEach(([bucket, id]) => {
    const input = document.getElementById(id);
    if (!input) return;
    input.checked = !!state.ui[bucket];
    input.addEventListener("change", () => {
      state.ui[bucket] = input.checked;
      saveUi();
      // Every bucket change moves the vocabulary boundary, in both directions.
      rebuildFilterUIs();
      applyAll();
    });
  });
  wireStatusToggles();
  wireCombineToggle();
}

// The four status checkboxes, built from TILGJENGELIGHET_OPTIONS so the markup
// and the vocabulary cannot drift. These write the SHARED filter, so the table
// sees the change through subscribeOtherTabs like any other filter edit.
function wireStatusToggles() {
  const mount = document.getElementById("status-toggles");
  if (!mount) return;
  const selected = state.ui.filters.tilgjengelighetSelected;
  mount.innerHTML = "";
  TILGJENGELIGHET_OPTIONS.forEach((opt) => {
    const label = document.createElement("label");
    label.className = "toggle";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = selected.includes(opt.key);
    input.addEventListener("change", async () => {
      const wasChecking = input.checked;
      const next = wasChecking
        ? [...selected, opt.key]
        : selected.filter((k) => k !== opt.key);
      selected.splice(0, selected.length, ...next);
      seedStatus(state.ui.filters);
      saveUi();
      if (wantsClosed(selected) && !state.soldLoaded) {
        input.disabled = true;
        try {
          await ensureSoldLoaded();
        } catch (_) {
          // Fetch failed (status line already says so): roll the selection
          // back. Only undo an actual check -- if the user was UNCHECKING,
          // `next.filter(k => k !== opt.key)` is a no-op (opt.key was already
          // absent from `next`), so unconditionally forcing a re-check would
          // falsely restore a box the user just cleared.
          if (wasChecking) {
            selected.splice(0, selected.length, ...next.filter((k) => k !== opt.key));
            seedStatus(state.ui.filters);
            saveUi();
          }
        } finally {
          input.disabled = false;
        }
      }
      // seedStatus may just have re-floored an emptied selection to [""], or
      // the rollback above may have restored a prior selection -- either way
      // this handler's own checkbox node does not reflect it yet. Rebuild the
      // mount so the DOM can never desync from state.ui.filters. Safe against
      // recursion: this replaces the DOM (innerHTML + fresh listeners) rather
      // than firing a change event, so it does not re-enter itself.
      wireStatusToggles();
      rebuildFilterUIs();
      applyAll();
    });
    label.appendChild(input);
    label.appendChild(document.createTextNode(" " + opt.label));
    mount.appendChild(label);
  });
}

function wireCombineToggle() {
  const combine = document.getElementById("toggle-combine-sold");
  if (combine) {
    combine.checked = !!state.ui.combineSold;
    combine.addEventListener("change", async () => {
      state.ui.combineSold = combine.checked;
      saveUi();
      // Combining needs the sold set loaded to be meaningful.
      if (combine.checked && wantsClosed(state.ui.filters.tilgjengelighetSelected) && !state.soldLoaded) {
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
  bindCheckbox("toggle-station-radius", "showRadius");
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

  // Lines pick like every other value filter, on the shared chip row -- which
  // brings its own "Nullstill" control, so the panel carries no bulk markup.
  // There is deliberately no "Ingen": an empty selection means *all* lines, so
  // "no lines" is unrepresentable. "Vis stasjoner" clears the stations from the
  // map, which covers the visible intent -- but note it is NOT equivalent to the
  // old "Ingen". That emptied visibleLineSet, which also silently switched off
  // the commute filter and "Ton ned utenfor radius" while both controls still
  // looked active. Losing that back door is the point; those two filters now
  // stay in force until you turn them off yourself.
  const container = document.getElementById("line-toggles");
  if (container) {
    container.innerHTML = "";
    container.classList.remove("muted");
    selectionChipRow(container, {
      label: "Linjer",
      options: (state.ui._allLines || []).map((l) => ({ key: l, label: l })),
      selected: st.lineSelected,
      colorFor: lineColor,
      onChange: () => {
        saveUi();
        applyAll();
      },
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
  // A deep link to a closed listing must switch its status on, or the dot the
  // link points at is hidden. Widening what's on screen also widens the
  // vocabulary, so the chip rows have to be rebuilt or they will describe a
  // narrower set than the map shows.
  const status = item.tilgjengelighet || "";
  const selected = state.ui.filters.tilgjengelighetSelected;
  if (selectionExcludes(selected, status)) {
    selected.push(status);
    saveUi();
    wireStatusToggles();
    rebuildFilterUIs();
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

// Collapsed/expanded state of the active-filter list (session-local, not persisted).
let activeFiltersExpanded = false;

function renderActiveFilterLine() {
  const node = document.getElementById("active-filters");
  if (!node) return;
  node.innerHTML = "";
  const entries = activeFilterEntries(state.ui.filters, state.meta);
  const head = document.createElement("button");
  head.type = "button";
  head.className = "af-head";
  head.textContent = entries.length
    ? entries.length + " filtre aktive · " + (activeFiltersExpanded ? "skjul" : "vis")
    : "Ingen aktive filtre";
  head.disabled = !entries.length;
  head.addEventListener("click", () => {
    activeFiltersExpanded = !activeFiltersExpanded;
    renderActiveFilterLine();
  });
  node.appendChild(head);
  if (!activeFiltersExpanded || !entries.length) return;
  entries.forEach((entry) => {
    const row = document.createElement("div");
    row.className = "af-row";
    const text = document.createElement("span");
    text.textContent = entry.label + ": " + entry.valueText;
    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "af-clear";
    clearBtn.setAttribute("aria-label", "Fjern filter");
    clearBtn.textContent = "×";
    clearBtn.addEventListener("click", () => {
      entry.clear(state.ui.filters);
      // tilgjengelighetSelected never reaches [] in the UI -- an empty status
      // selection is indistinguishable from "show every status", which would
      // surface every already-fetched closed listing. Re-floor it here (the
      // ["", "Solgt"] and ["Solgt"] cases can both land here since only the
      // [""]-only default is excluded from the active-filter list) and rebind
      // the checkboxes so they cannot show stale state after the clear.
      seedStatus(state.ui.filters);
      wireStatusToggles();
      rebuildFilterUIs(); // field summaries + sliders must reflect the clear
      onFilterChange();
    });
    row.appendChild(text);
    row.appendChild(clearBtn);
    node.appendChild(row);
  });
}

function onFilterChange() {
  saveUi();
  renderActiveFilterLine();
  applyAll();
}

// (Re)build every filter/display control that renders shared state -- init,
// reset, cross-tab storage event, sold-load, annotation-save.
function rebuildFilterUIs() {
  const vocabs = deriveVocabs(vocabItems());
  if (pruneFilterSets(state.ui.filters, vocabs, vocabIsComplete())) saveUi();
  buildFilterPanelUI(document.getElementById("filter-panel-body"), {
    meta: state.meta,
    vocabs,
    colorByType: { ...state.colorByType, "": DEFAULT_UNKNOWN_TYPE_COLOR },
    filters: state.ui.filters,
    collapsed: state.ui.collapsed,
    onChange: onFilterChange,
    onCollapse: saveUi,
  });
  buildDisplayUI(document.getElementById("display-sliders"), state.ui, onFilterChange);
  renderActiveFilterLine();
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
  seedStatus(state.ui.filters);
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
      if (!item.closed && t != null && t > prevTs) state.newSinceLast += 1;
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
  rebuildFilterUIs();
  renderSourceLegend();
  wireLayerToggles();
  wirePremiumToggle();
  wireStationControls();
  wireCollapsiblePanels();
  wireDrawer();
  loadMissingCoords();
  document.addEventListener("sk-annotation-saved", () => {
    // The popup repaints its chip row the instant this returns, so the colour
    // map has to be current NOW. applyAll() below also rebuilds it, but only
    // inside a requestAnimationFrame -- a frame too late for that repaint, and
    // a brand-new tag would render with no chip at all. Same inputs as
    // featureCollectionsByGroup uses, so the rAF's rebuild is a no-op repeat.
    state.tagColors = assignTagColors(
      [...state.itemsById.values()].map((i) => i.tag)
    );
    rebuildFilterUIs(); // tag vocab may have changed
    applyAll(); // tag rings / tag-visibility may have changed
  });

  const resetBtn = document.getElementById("reset-filters");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      resetFilters(state.ui.filters, state.meta);
      // resetFilters reassigns tilgjengelighetSelected to a fresh (empty)
      // array, which would otherwise show every closed listing that happens
      // to already be loaded -- the exact state the [""] floor exists to
      // prevent. Rewire the status checkboxes too: their change handlers
      // closed over the array reference that reset just orphaned.
      seedStatus(state.ui.filters);
      wireStatusToggles();
      rebuildFilterUIs();
      onFilterChange();
    });
  }
  const emptyReset = document.getElementById("map-empty-reset");
  if (emptyReset) {
    emptyReset.addEventListener("click", () => {
      // Unlike the sidebar reset button above, this one ALSO switches the
      // layer buckets back on: its message blames "lag og filtre", and the
      // commonest way to empty the map is unchecking Eie and DNB, where a
      // filters-only reset visibly does nothing at all.
      restoreLayerToggles();
      // resetFilters needs state.meta (defaultFilters reads meta.destinations)
      // and onFilterChange keeps the "active filters" line in sync, not just
      // the map data.
      resetFilters(state.ui.filters, state.meta);
      seedStatus(state.ui.filters);
      wireStatusToggles();
      rebuildFilterUIs();
      onFilterChange();
    });
  }
  // Live cross-tab sync: another tab (e.g. the table) changed the filters.
  subscribeOtherTabs(() => {
    state.ui.filters = loadFilters(state.meta);
    // loadFilters returns a whole new filters object, so the status
    // checkboxes' handlers -- closed over the old tilgjengelighetSelected
    // array -- must be rebound or they would mutate a detached array.
    seedStatus(state.ui.filters);
    wireStatusToggles();
    rebuildFilterUIs();
    applyAll();
    // The synced selection may now want the closed bucket even though this
    // tab never fetched it -- the write that enabled it happened in the
    // other tab, paired with that tab's own fetch. Mirror the same guarded
    // call init() makes after map load. ensureSoldLoaded memoizes on
    // state.soldPromise, so this cannot double-fetch alongside a same-tab
    // fetch already in flight.
    if (wantsClosed(state.ui.filters.tilgjengelighetSelected) && !state.soldLoaded) {
      ensureSoldLoaded().then(applyAll).catch(() => {});
    }
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
    // No initial setSoldColorMode(..., true) call here: budpremie colouring is
    // retired (Task 7B) and loadUi() forces soldPremium false on every load, so
    // this branch could never fire -- removed rather than left as dead code.
    applyAll();

    map.on("render", () => syncClusterMarkers(map, state.groups, state.clusterMarkers));
    map.on("moveend", () => syncClusterMarkers(map, state.groups, state.clusterMarkers));

    if (wantsClosed(state.ui.filters.tilgjengelighetSelected) && !state.soldLoaded) {
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
