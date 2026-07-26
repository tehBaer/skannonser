# UI polish round 3 — vocabulary correctness, marker encoding, stations panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the vocabulary-pollution bug found in the 2026-07-25 UX review, make active listings visually dominant over closed ones, and reclaim a quarter of the map sidebar from the stations panel.

**Architecture:** All work is in `skannonser/web/static/` — no backend, no API, no migrations. Three of the four phases are independent and can stop cleanly at a phase boundary. Phase 1 fixes a state bug in the shared filter pipeline (`filters.js` / `app.js` / `table.js` / `filterstate.js`). Phase 2 changes MapLibre paint and layer-add order in `map.js`. Phase 3 adds a station point layer and re-renders the line toggles. Phase 4 is sidebar and table polish.

**Tech Stack:** Plain ES modules, no build step. MapLibre GL. Tests are `node --test` over `tests/web/*.test.mjs`. Python backend untouched.

## Global Constraints

- **Zero backend changes.** No edits under `skannonser/` outside `skannonser/web/static/`. No API shape change, no migrations.
- No external CDNs, no new dependencies, no build step — plain ES modules only.
- UI copy is Norwegian (bokmål), matching existing labels ("Vis stasjoner", "Nullstill filtre", "Kolonner", "Fant ikke annonse …").
- Shared filter state rules: new filter keys get defaults in `defaultFilters`, entries in `activeFilterEntries`, predicates in `listingExcluded`, and UI in `COLUMN_FILTERS` — never a partial subset.
- localStorage stays inside the one `skannonser.ui.v1` blob (read-modify-write, never overwrite the whole blob).
- Comment style: comments state constraints/WHY, not what the next line does (match the files' existing voice).
- **Test command is `node --test tests/web/*.test.mjs`** — the directory form `node --test tests/web/` is broken on node v25.
- The Python suite must stay green: `.venv/bin/pytest -q` → 656 passed (no changes expected; run it in the final task).

## Explicit non-goals

- **Keyboard access to map markers.** The dots are canvas-drawn, so there is no tab path to a listing popup. Giving the map a keyboard affordance is a real piece of work, and the table already exposes the same data with full keyboard support. Recorded as a deliberate choice, not an oversight — do not add a partial version of it here.
- **Dynamic column visibility tied to "Vis solgte".** See Task 12's note: it would fight the user's explicit picker choices.
- **Zoom-interpolated marker radii.** Task 6 makes the radii derivable from one constant, which is the prerequisite. Whether they should also scale with zoom is a separate judgement best made after looking at the new flat sizes.

## Dev-server setup

Reuse the round-2 scratch database and launch entry. It is already migrated and seeded.

```bash
SCRATCH=/private/tmp/claude-501/-Users-tehbaer-kode-skannonser/f9854acb-68d2-44c1-a28d-6fcf9b3c1377/scratchpad
ls "$SCRATCH/ui-polish.db"   # if missing, recreate per the round-2 plan's setup section
```

`.claude/launch.json` already has `skannonser-web-review` pointing at that DB on 127.0.0.1:8377. Start it with `preview_start {name: "skannonser-web-review"}` — **never via Bash**.

Seeded tags on active listings: `461666906` "maybe", `465492220` "definitivt", `282785835` "hard no".

**Two harness quirks that will cost you time if you don't know them:**

1. **The browser pane caches ES modules aggressively.** After editing any file under `web/static/`, a plain reload keeps serving the old module. Fix: from page context run `fetch(url, {cache: "reload"})` for each changed module, then `location.reload()`. Restarting the server does not help.
2. **rAF is throttled when the browser pane is not frontmost**, and MapLibre's render loop is rAF-driven. Allow several seconds after reload, and prefer `javascript_tool` DOM/state inspection over screenshots where possible. Static assets are served at the root (`/map.js`), not under `/static/`.

---

## Phase 1 — Vocabulary correctness (the bug)

### Task 1: Make `filters.js` importable under node, and pin `deriveVocabs`

**Files:**
- Modify: `skannonser/web/static/filters.js` (the two top-level `document.addEventListener` calls, currently at lines 366 and 372)
- Create: `tests/web/vocabs.test.mjs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `filters.js` becomes importable in node, which every later vocabulary test depends on. No export changes.

**Why this task exists:** `filters.js` currently throws `ReferenceError: document is not defined` on import because two popover-dismiss listeners run at module scope. That makes `deriveVocabs` and `listingExcluded` — the two functions Phase 1 changes — untestable. Fixing it first gives the rest of the phase a real test harness.

- [ ] **Step 1: Write the failing test**

Create `tests/web/vocabs.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { deriveVocabs } from "../../skannonser/web/static/filters.js";

test("deriveVocabs counts only the items it is handed", () => {
  const v = deriveVocabs([
    { tag: "maybe", tilgjengelighet: null, postnummer: "0170", nabolag: "Sentrum" },
    { tag: null, tilgjengelighet: null, postnummer: "0170", nabolag: null },
  ]);
  const tags = Object.fromEntries(v.tags.map((o) => [o.key, o.count]));
  assert.equal(tags["maybe"], 1);
  assert.equal(tags[""], 1, "untagged items land in the \"\" bucket");
  assert.equal(v.postnummer.find((o) => o.key === "0170").count, 2);
});

test("a value carried only by an omitted item does not appear", () => {
  const v = deriveVocabs([{ tag: "maybe" }]);
  assert.ok(!v.tags.some((o) => o.key === "kun-solgt"));
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/vocabs.test.mjs`
Expected: FAIL with `ReferenceError: document is not defined`.

- [ ] **Step 3: Guard the module-scope listeners**

In `filters.js`, replace:

```js
// One document-level dismiss wiring (module init).
document.addEventListener("click", (ev) => {
  if (!popoverEl) return;
  if (popoverEl.contains(ev.target)) return;
  if (popoverAnchor && popoverAnchor.contains(ev.target)) return;
  closePopover();
});
document.addEventListener("keydown", (ev) => {
  if (ev.key === "Escape") closePopover();
});
```

with:

```js
// One document-level dismiss wiring (module init). Guarded because this module
// holds the shared filter predicate and vocabulary derivation, which are unit
// tested under node -- where there is no document.
if (typeof document !== "undefined") {
  document.addEventListener("click", (ev) => {
    if (!popoverEl) return;
    if (popoverEl.contains(ev.target)) return;
    if (popoverAnchor && popoverAnchor.contains(ev.target)) return;
    closePopover();
  });
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") closePopover();
  });
}
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 7 tests total (5 pre-existing tagcolors + 2 new).

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/filters.js tests/web/vocabs.test.mjs
git commit -m "test(web): make filters.js node-importable and pin deriveVocabs"
```

---

### Task 2: Map vocabularies follow the layer toggles

**Files:**
- Modify: `skannonser/web/static/app.js` (new `vocabItems()` helper beside `bucketOf`; `rebuildFilterUIs`; the change handler inside `wireLayerToggles`)

**Interfaces:**
- Consumes: existing `bucketOf(item)` (returns `"sold" | "inactive" | "dnb" | "eie"`, matching the `state.ui` bucket keys), `deriveVocabs` from filters.js.
- Produces: `vocabItems()` — the item subset every vocabulary on the map page is derived from. Task 4 calls `deriveVocabs(vocabItems())` again when pruning.

- [ ] **Step 1: Add the helper**

In `app.js`, directly below the `bucketOf` function, add:

```js
// Vocabularies must describe what the user can actually SEE. Deriving them from
// every loaded item strands values from a switched-off bucket in the filter UI
// forever, because the item store only ever grows -- once the sold bucket is
// fetched it stays for the session. Scope this to the LAYER toggles only:
// deriving from "passes all filters" instead would make a tag vanish the moment
// a price slider hid it, leaving no way to click it back.
function vocabItems() {
  return [...state.itemsById.values()].filter((it) => state.ui[bucketOf(it)]);
}
```

- [ ] **Step 2: Point `rebuildFilterUIs` at it**

In `rebuildFilterUIs`, replace:

```js
    vocabs: deriveVocabs([...state.itemsById.values()]),
```

with:

```js
    vocabs: deriveVocabs(vocabItems()),
```

- [ ] **Step 3: Rebuild the filter UI when a layer is switched OFF too**

In `wireLayerToggles`, the change handler currently ends:

```js
      applyAll();
    });
```

Replace with:

```js
      // Every bucket change moves the vocabulary boundary, in both directions.
      // The enable path already rebuilds via ensureSoldLoaded, but eie/dnb and
      // every disable path did not, which is how switched-off values got stuck.
      rebuildFilterUIs();
      applyAll();
    });
```

- [ ] **Step 4: Verify in the browser**

Start the preview server. Then, in page context:

```js
fetch("/api/annotations/466450402", { method: "PUT", headers: {"Content-Type":"application/json"},
  body: JSON.stringify({ kommentar: null, tag: "kun-solgt" }) })
```

Reload with Solgt off. The chip row must show only `(uten tag)`, `definitivt`, `hard no`, `maybe`. Switch Solgt on → `kun-solgt` appears and `(uten tag)` grows. Switch Solgt off → `kun-solgt` is **gone** and `(uten tag)` returns to its active-only count. Repeat with the DNB toggle and confirm no console errors.

Clean up afterwards: `PUT` the same finnkode back with `{ kommentar: null, tag: null }`.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/app.js
git commit -m "fix(map): derive filter vocabularies from the enabled layers only"
```

---

### Task 3: Table vocabularies follow "Vis solgte"

**Files:**
- Modify: `skannonser/web/static/table.js` (`refreshVocabs`; the `soldToggle` change handler in `wireToolbar`)

**Interfaces:**
- Consumes: existing `state.showSold`, `state.items`, `deriveVocabs`, `assignTagColors`.
- Produces: nothing new exported.

- [ ] **Step 1: Scope `refreshVocabs` to the visible bucket**

Replace:

```js
function refreshVocabs() {
  state.vocabs = deriveVocabs(state.items);
  state.tagColors = assignTagColors(state.vocabs.tags.map((o) => o.key));
}
```

with:

```js
function refreshVocabs() {
  // Same rule as the map (app.js vocabItems): the vocabulary describes the
  // rows the user can see. `state.items` only ever grows, so without this the
  // tag chips keep values that only closed rows carried after "Vis solgte" is
  // switched back off.
  const visible = state.showSold ? state.items : state.items.filter((it) => !it.closed);
  state.vocabs = deriveVocabs(visible);
  state.tagColors = assignTagColors(state.vocabs.tags.map((o) => o.key));
}
```

- [ ] **Step 2: Refresh on the OFF path too**

In the `soldToggle` change handler, replace:

```js
    }
    render();
  });
```

with:

```js
    }
    // The off path changes the vocabulary boundary just as much as the on path.
    refreshVocabs();
    render();
  });
```

- [ ] **Step 3: Verify in the browser**

On `/table`, seed the same `kun-solgt` tag as in Task 2. With "Vis solgte/inaktive" on, the chip appears in the toolbar; switching it off removes the chip and restores the `(uten tag)` count. The Tag column popover's option list must follow the same boundary. No console errors.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/table.js
git commit -m "fix(table): derive filter vocabularies from the visible rows only"
```

---

### Task 4: Prune filter entries whose value no longer exists

**Files:**
- Modify: `skannonser/web/static/filterstate.js` (new `pruneFilterSets` export)
- Modify: `skannonser/web/static/app.js` (`rebuildFilterUIs`)
- Modify: `skannonser/web/static/table.js` (`refreshVocabs`)
- Test: `tests/web/prunefilters.test.mjs` (create)

**Interfaces:**
- Consumes: the `vocabs` object shape from `deriveVocabs` — `{ postnummer, nabolag, tilgjengelighet, tags }`, each an array of `{ key, label, count }`.
- Produces: `pruneFilterSets(filters, vocabs) -> boolean` (true when it deleted something, so callers can persist).

**Why:** hiding a chip writes `tagHidden[key] = true`. When that key later leaves the vocabulary, the entry filters nothing but still counts toward "N filtre aktive" forever, and it persists to localStorage. Only the four `deriveVocabs`-derived sets are pruned — `boligtypeHidden`, `eieformHidden` and `energiHidden` come from the server's static `meta` vocabulary and must be left alone.

- [ ] **Step 1: Write the failing test**

Create `tests/web/prunefilters.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { pruneFilterSets } from "../../skannonser/web/static/filterstate.js";

const vocabs = {
  tags: [{ key: "" }, { key: "maybe" }],
  tilgjengelighet: [{ key: "" }],
  postnummer: [{ key: "0170" }],
  nabolag: [{ key: "Sentrum" }],
};

test("drops hidden keys that left the vocabulary", () => {
  const filters = { tagHidden: { maybe: true, "kun-solgt": true } };
  assert.equal(pruneFilterSets(filters, vocabs), true);
  assert.deepEqual(filters.tagHidden, { maybe: true });
});

test("drops selected values that left the vocabulary", () => {
  const filters = { postnummerSelected: ["0170", "9999"], nabolagSelected: ["Sentrum"] };
  assert.equal(pruneFilterSets(filters, vocabs), true);
  assert.deepEqual(filters.postnummerSelected, ["0170"]);
  assert.deepEqual(filters.nabolagSelected, ["Sentrum"]);
});

test("leaves meta-derived sets alone and reports no change", () => {
  const filters = { boligtypeHidden: { Leilighet: true }, eieformHidden: { Selveier: true } };
  assert.equal(pruneFilterSets(filters, vocabs), false);
  assert.deepEqual(filters.boligtypeHidden, { Leilighet: true });
  assert.deepEqual(filters.eieformHidden, { Selveier: true });
});

test("is a no-op when nothing is stale", () => {
  const filters = { tagHidden: { maybe: true }, postnummerSelected: ["0170"] };
  assert.equal(pruneFilterSets(filters, vocabs), false);
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/prunefilters.test.mjs`
Expected: FAIL — `pruneFilterSets is not a function`.

- [ ] **Step 3: Implement it**

Add to `filterstate.js`, below `resetFilters`:

```js
// Drop hidden/selected entries whose value no longer exists in the current
// vocabulary. Without this, hiding a chip for a value that later leaves the
// vocabulary (a tag only closed listings carried, say) leaves an entry that
// filters nothing but keeps counting toward "N filtre aktive" forever -- and
// persists to localStorage. Returns true when something was removed so the
// caller can save. Only the deriveVocabs-backed sets are pruned; boligtype,
// eieform and energimerke come from the server's static meta vocabulary.
export function pruneFilterSets(filters, vocabs) {
  if (!filters || !vocabs) return false;
  let changed = false;
  const keysOf = (list) => new Set((list || []).map((o) => o.key));

  const pruneHidden = (setKey, allowed) => {
    const set = filters[setKey];
    if (!set) return;
    Object.keys(set).forEach((k) => {
      if (!allowed.has(k)) {
        delete set[k];
        changed = true;
      }
    });
  };
  const pruneSelected = (arrKey, allowed) => {
    const arr = filters[arrKey];
    if (!Array.isArray(arr)) return;
    for (let i = arr.length - 1; i >= 0; i--) {
      if (!allowed.has(arr[i])) {
        arr.splice(i, 1);
        changed = true;
      }
    }
  };

  pruneHidden("tagHidden", keysOf(vocabs.tags));
  pruneHidden("tilgjengelighetHidden", keysOf(vocabs.tilgjengelighet));
  pruneSelected("postnummerSelected", keysOf(vocabs.postnummer));
  pruneSelected("nabolagSelected", keysOf(vocabs.nabolag));
  return changed;
}
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 11 tests.

- [ ] **Step 5: Call it from the map**

In `app.js`, add `pruneFilterSets` to the existing `./filterstate.js` import list. Then in `rebuildFilterUIs`, replace:

```js
function rebuildFilterUIs() {
  buildFilterPanelUI(document.getElementById("filter-panel-body"), {
    meta: state.meta,
    vocabs: deriveVocabs(vocabItems()),
```

with:

```js
function rebuildFilterUIs() {
  const vocabs = deriveVocabs(vocabItems());
  if (pruneFilterSets(state.ui.filters, vocabs)) saveUi();
  buildFilterPanelUI(document.getElementById("filter-panel-body"), {
    meta: state.meta,
    vocabs,
```

- [ ] **Step 6: Call it from the table**

In `table.js`, add `pruneFilterSets` to the existing `./filterstate.js` import list. Then in `refreshVocabs`, replace:

```js
  state.vocabs = deriveVocabs(visible);
```

with:

```js
  state.vocabs = deriveVocabs(visible);
  if (pruneFilterSets(state.filters, state.vocabs)) saveFilters(state.filters);
```

(`saveFilters` is already imported in `table.js`; if it is not, add it to the same import list.)

- [ ] **Step 7: Verify in the browser**

Seed `kun-solgt` again. Switch Solgt on, click the `kun-solgt` chip to hide it, then switch Solgt off. The active-filter summary must read "Ingen aktive filtre", and `JSON.parse(localStorage.getItem("skannonser.ui.v1")).filters.tagHidden` must no longer contain `kun-solgt`. Confirm a still-valid hidden tag (hide `maybe` with Solgt off) survives a reload untouched.

- [ ] **Step 8: Commit**

```bash
git add skannonser/web/static/filterstate.js skannonser/web/static/app.js skannonser/web/static/table.js tests/web/prunefilters.test.mjs
git commit -m "fix(filters): prune hidden/selected entries whose value left the vocabulary"
```

---

## Phase 2 — Map marker encoding

### Task 5: Draw active listings on top of closed ones

**Files:**
- Modify: `skannonser/web/static/map.js` (`addListingGroups`, currently one `groups.forEach` adding all layer kinds per group)
- Test: `tests/web/maplayers.test.mjs` (create)

**Interfaces:**
- Consumes: existing `buildGroups(boligtyper, colorByType)` output shape — objects with `{ id, type, color, hasActive, hasSold }`.
- Produces: no signature change to `addListingGroups(map, groups, onListingClick)`. Layer ids are unchanged; only their **add order** changes.

**Why:** layers are added per group in one pass, so within a group the `-sold` layer lands after `-eie`/`-dnb`, and across groups a later boligtype covers every earlier one. Production has 3 442 closed listings against 770 active, so the buyable homes currently sit at the bottom of the pile.

- [ ] **Step 1: Write the failing test**

Create `tests/web/maplayers.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { addListingGroups, buildGroups } from "../../skannonser/web/static/map.js";

// Minimal stand-in for a MapLibre map: records the order layers are added.
function fakeMap() {
  const added = [];
  return {
    added,
    addSource() {},
    addLayer(spec) { added.push(spec.id); },
    getLayer() { return null; },
    hasImage() { return true; },
    addImage() {},
    on() {},
    getSource() { return null; },
  };
}

test("every active dot layer is added after every closed dot layer", () => {
  const groups = buildGroups(["Enebolig", "Leilighet"], { Enebolig: "#111", Leilighet: "#222" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});

  const lastClosed = Math.max(...map.added.flatMap((id, i) => (id.endsWith("-sold") ? [i] : [])));
  const firstActive = Math.min(
    ...map.added.flatMap((id, i) => (id.endsWith("-eie") || id.endsWith("-dnb") ? [i] : []))
  );
  assert.ok(lastClosed < firstActive,
    `closed layers must precede active ones; got last closed ${lastClosed}, first active ${firstActive}`);
});

test("tag rings sit beneath every dot layer", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const lastRing = Math.max(...map.added.flatMap((id, i) => (id.endsWith("-tagring") ? [i] : [])));
  const firstDot = Math.min(
    ...map.added.flatMap((id, i) => (/-(eie|dnb|sold)$/.test(id) ? [i] : []))
  );
  assert.ok(lastRing < firstDot, "rings must be added before dots so they read as haloes");
});

test("the inactive X is added after the closed dot it marks", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const x = map.added.findIndex((id) => id.endsWith("-inactive-x"));
  const sold = map.added.findIndex((id) => id.endsWith("-sold"));
  assert.ok(sold < x && x !== -1, "X must draw over its dot");
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/maplayers.test.mjs`
Expected: FAIL on the first test — closed layers currently come after active ones within a group.

- [ ] **Step 3: Restructure `addListingGroups` into ordered passes**

Replace the single `groups.forEach` body that adds clusters, tag rings and dot layers with five sequential passes over the same `groups` array. Keep every layer's `source`, `filter` and `paint` exactly as it is today — only the iteration structure changes:

```js
export function addListingGroups(map, groups, onListingClick) {
  const clickLayers = [];

  // Layers are added in five passes rather than one per group, because add
  // order IS z-order in MapLibre. Per-group ordering put every later boligtype
  // over every earlier one, and closed dots over active ones -- with 4.5x more
  // closed than active listings in production, that buried exactly the dots
  // that matter. Passes: sources -> clusters -> rings -> closed -> active.

  groups.forEach((g) => {
    map.addSource(g.id, { /* ...unchanged source config, including clusterProperties... */ });
  });

  groups.forEach((g) => { /* ...unchanged -cluster layer... */ });

  groups.forEach((g) => { /* ...unchanged -tagring layer... */ });

  groups.forEach((g) => {
    if (!g.hasSold) return;
    /* ...unchanged -sold layer... */
    /* ...unchanged -inactive-x layer... */
    clickLayers.push(g.id + "-sold");
  });

  groups.forEach((g) => {
    if (!g.hasActive) return;
    /* ...unchanged -eie layer... */
    /* ...unchanged -dnb layer... */
    clickLayers.push(g.id + "-eie");
    clickLayers.push(g.id + "-dnb");
  });

  // UNCHANGED: the existing trailing block that wires click / mouseenter /
  // mouseleave over clickLayers stays exactly as it is, at the end of the
  // function. addListingGroups returns nothing -- do not add a return.
  clickLayers.forEach((layerId) => {
    /* ...unchanged map.on("click" | "mouseenter" | "mouseleave") wiring... */
  });
}
```

This is a **pure move**: cut each existing layer spec and paste it into the matching pass. Do not edit any `paint`, `layout`, `filter`, `source` or `id` value while moving — Task 6 and Task 7 change paint, and keeping this task move-only is what makes those diffs readable. The `clickLayers` pushes travel with their layers, so the final `clickLayers` array ends up in a different order than before; that is harmless, since it only drives event wiring.

Note the `-dnb` layer currently sits inside the same `if (g.hasActive)` branch as `-eie` — keep them together in the active pass.

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 14 tests.

- [ ] **Step 5: Verify in the browser**

Enable all four layers with "Solgt nedtoning" at 0 (fully opaque) and zoom into central Oslo. Active dots must now sit visibly on top where they overlap closed ones. Clicking a dot still opens the right popup — check both an active and a sold dot, and confirm the click does not double-fire (the popup opens once).

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/map.js tests/web/maplayers.test.mjs
git commit -m "fix(map): draw active listings above closed ones"
```

---

### Task 6: One marker-geometry constant

**Files:**
- Modify: `skannonser/web/static/map.js` (new size constants near the top of the marker section; `ensureSquareIcon`; `ensureXIcon`; the `-tagring`, `-eie`, `-dnb`, `-sold`, `-inactive-x` layer specs)
- Test: `tests/web/markersize.test.mjs` (create)

**Interfaces:**
- Consumes: nothing new.
- Produces: exported `DOT_R` (number) so tests and future tasks can reason about marker geometry from one place.

**Why:** the dot radius, the closed radius, the tag-ring radius, the DNB square raster and the X raster are five independent hardcoded numbers. The square and X are canvas images that do **not** follow `circle-radius`, and the ring radius (12) was hand-tuned against a dot radius of 7. Changing one without the others breaks the visual relationship.

- [ ] **Step 1: Write the failing test**

Create `tests/web/markersize.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { DOT_R } from "../../skannonser/web/static/map.js";

test("the dot radius is exported as a single source of truth", () => {
  assert.equal(typeof DOT_R, "number");
  assert.ok(DOT_R >= 8, "round 3 enlarges the active dot from its old radius of 7");
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/markersize.test.mjs`
Expected: FAIL — `DOT_R` is undefined.

- [ ] **Step 3: Add the constants**

In `map.js`, replace the `ACTIVE_BORDER` / `SOLD_BORDER` block's neighbourhood by adding, directly above it:

```js
// One source of truth for marker geometry. The DNB square and the inactive X
// are canvas rasters that do NOT follow circle-radius, and the tag ring is its
// own circle layer -- so all of them must be derived from one number or they
// drift apart (the ring's old 12 was hand-tuned against a dot radius of 7).
// Raster icons are cached by name, so these must stay module constants: making
// them dynamic without putting the size in the cache key would serve stale art.
export const DOT_R = 9;
const CLOSED_R = DOT_R - 1.5;
const RING_R = DOT_R + 6;
const SQUARE_PX = Math.round(DOT_R * 2.55);
const X_PX = Math.round(DOT_R * 2);
```

- [ ] **Step 4: Derive the raster sizes**

In `ensureSquareIcon`, replace `const size = 18;` with:

```js
  const size = SQUARE_PX;
```

In `ensureXIcon`, replace `const size = 16;` with:

```js
  const size = X_PX;
```

- [ ] **Step 5: Derive the layer radii**

In the `-tagring` layer spec, replace `"circle-radius": 12,` with `"circle-radius": RING_R,`.

In the `-eie` layer spec, replace `"circle-radius": 7,` with `"circle-radius": DOT_R,`.

In the `-sold` layer spec, replace `"circle-radius": 6,` with `"circle-radius": CLOSED_R,`.

- [ ] **Step 6: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 15 tests.

- [ ] **Step 7: Verify in the browser**

Zoom into a dense area. Dots are visibly larger than before and read above the OSM basemap clutter. The DNB squares must still look the same visual weight as the round dots beside them, the tag ring must still read as a halo clearing its dot, and the X must still sit inside its dot without overflowing. Check at zoom levels 11, 13 and 15.

- [ ] **Step 8: Commit**

```bash
git add skannonser/web/static/map.js tests/web/markersize.test.mjs
git commit -m "feat(map): derive all marker geometry from one DOT_R constant"
```

---

### Task 7: Closed listings become hollow

**Files:**
- Modify: `skannonser/web/static/map.js` (the `-sold` layer paint; `setSoldColorMode`; remove the now-unused `SOLD_BORDER`)
- Test: `tests/web/maplayers.test.mjs` (extend)

**Interfaces:**
- Consumes: `CLOSED_R` and `DOT_R` from Task 6; existing `PREMIUM_COLOR` expression and `OP`.
- Produces: no signature change. `setSoldColorMode(map, groups, premiumOn)` keeps its signature but now writes `circle-stroke-color` instead of `circle-color`.

**Why:** an active and a closed dot of the same boligtype measure 2.94:1 contrast at identical hue — fill, stroke width and (nearly) radius are the same, so "closed" reads as a faded version of the same thing rather than a different category. A dimmed orange closed dot also lands on the colour of an *active* tomannsbolig. Filled-versus-hollow is a shape difference, so it survives the "Solgt nedtoning" slider and cannot collide with the DNB square or the inactive X.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/maplayers.test.mjs`:

```js
import { setSoldColorMode } from "../../skannonser/web/static/map.js";

test("closed dots are hollow: no fill, a thick boligtype-coloured ring", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const specs = [];
  const map = fakeMap();
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  const sold = specs.find((s) => s.id.endsWith("-sold"));
  assert.equal(sold.paint["circle-opacity"], 0, "no fill");
  assert.equal(sold.paint["circle-stroke-color"], "#0f4c81", "ring carries the boligtype colour");
  assert.ok(sold.paint["circle-stroke-width"] >= 3, "ring must be thick enough to read");
});

test("budpremie mode recolours the ring, not the fill, and spares inactive dots", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const writes = [];
  const map = fakeMap();
  map.getLayer = () => ({});
  map.setPaintProperty = (id, prop, value) => writes.push({ id, prop, value });

  setSoldColorMode(map, groups, true);
  assert.ok(writes.length > 0, "at least one closed layer is recoloured");
  assert.ok(writes.every((w) => w.prop === "circle-stroke-color"),
    "hollow dots carry their colour on the stroke");
  assert.ok(JSON.stringify(writes[0].value).includes("#0f4c81"),
    "inactive dots keep the boligtype colour even in budpremie mode");

  writes.length = 0;
  setSoldColorMode(map, groups, false);
  assert.deepEqual(writes.map((w) => w.value), ["#0f4c81"]);
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/maplayers.test.mjs`
Expected: FAIL — the sold layer still has a fill and `setSoldColorMode` writes `circle-color`.

- [ ] **Step 3: Make the closed layer hollow**

Replace the `-sold` layer's `paint` block with:

```js
        paint: {
          // Hollow on purpose. A lighter tint of the same hue measured 2.94:1
          // against the active dot and read as "same thing, faded" -- a dimmed
          // orange even landed on the colour of an ACTIVE tomannsbolig. Filled
          // vs hollow is a shape difference, so it survives the nedtoning
          // slider and cannot be confused with another boligtype.
          "circle-color": "rgba(0,0,0,0)",
          "circle-opacity": 0,
          "circle-radius": CLOSED_R,
          "circle-stroke-width": 3,
          "circle-stroke-color": g.color,
          "circle-stroke-opacity": OP,
        },
```

- [ ] **Step 4: Move budpremie mode onto the stroke**

Replace `setSoldColorMode` with:

```js
// Flip every closed layer between boligtype colour and the budpremie scale.
// Closed dots are hollow, so the colour lives on the stroke. The premium scale
// applies only to genuine sales; inactive/trukket dots never had a premium and
// keep their boligtype colour in both modes.
export function setSoldColorMode(map, groups, premiumOn) {
  groups.forEach((g) => {
    if (!g.hasSold) return;
    const layerId = g.id + "-sold";
    if (!map.getLayer(layerId)) return;
    map.setPaintProperty(
      layerId,
      "circle-stroke-color",
      premiumOn ? ["case", ["==", ["get", "sold"], true], PREMIUM_COLOR, g.color] : g.color
    );
  });
}
```

- [ ] **Step 5: Delete the dead constant**

`SOLD_BORDER` is now unreferenced. Remove its declaration and update the `ACTIVE_BORDER` comment to describe the surviving convention:

```js
// Border convention: ACTIVE listings are solid boligtype-coloured dots with a
// dark border. CLOSED listings are hollow rings in the same colour (see the
// -sold layer paint).
const ACTIVE_BORDER = "#111111";
```

Confirm nothing else referenced it: `grep -rn "SOLD_BORDER" skannonser/web/static/` must return nothing.

- [ ] **Step 6: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 17 tests.

- [ ] **Step 7: Verify in the browser**

With all layers on and "Solgt nedtoning" at 0, closed listings render as rings and active ones as solid dots — distinguishable at a glance without reading colour. Drag the nedtoning slider: the rings fade but stay rings. Toggle "Farg solgte etter budpremie": sold rings take the premium scale while inactive rings keep their boligtype colour and their X. Confirm the sidebar legend row "Solgt (hvit kant)" is now wrong — it is fixed in Task 11, note it and move on.

- [ ] **Step 8: Commit**

```bash
git add skannonser/web/static/map.js tests/web/maplayers.test.mjs
git commit -m "feat(map): closed listings render hollow instead of tinted"
```

---

## Phase 3 — Stations panel

### Task 8: Line toggles become a wrapping pill row

**Files:**
- Modify: `skannonser/web/static/app.js` (the `line-toggles` rendering block inside `wireStationControls`)
- Modify: `skannonser/web/static/style.css` (append; the existing `.boligtype-toggle, .line-toggle` rule)
- Modify: `skannonser/web/static/index.html` (the `Linjer` subhead area)

**Interfaces:**
- Consumes: `lineColor(lineId)` exported from `stations.js`; existing `state.ui.stations.lineHidden`, `state.ui._allLines`, `saveUi`, `applyAll`.
- Produces: no new exports.

**Why:** thirteen checkbox rows occupy 423px — 13% of the 3 221px sidebar — to display two-to-four character labels. As a wrapping pill row they fit in about three rows.

- [ ] **Step 1: Add the "alle / ingen" control to the markup**

In `index.html`, replace:

```html
        <h3 class="subhead">Linjer</h3>
```

with:

```html
        <div class="subhead-row">
          <h3 class="subhead">Linjer</h3>
          <span class="line-bulk">
            <button type="button" id="lines-all" class="linkish">Alle</button>
            <button type="button" id="lines-none" class="linkish">Ingen</button>
          </span>
        </div>
```

- [ ] **Step 2: Render pills instead of checkbox rows**

In `app.js`, replace the `lines.forEach((line) => { ... })` body that builds `label.toggle.line-toggle` rows with:

```js
    lines.forEach((line) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "line-chip" + (st.lineHidden[line] ? " off" : "");
      btn.style.setProperty("--line-color", lineColor(line));
      btn.textContent = line;
      btn.setAttribute("aria-pressed", String(!st.lineHidden[line]));
      btn.addEventListener("click", () => {
        if (st.lineHidden[line]) delete st.lineHidden[line];
        else st.lineHidden[line] = true;
        btn.classList.toggle("off", Boolean(st.lineHidden[line]));
        btn.setAttribute("aria-pressed", String(!st.lineHidden[line]));
        saveUi();
        applyAll();
      });
      container.appendChild(btn);
    });
```

Add `lineColor` to the existing `./stations.js` import list in `app.js`.

- [ ] **Step 3: Wire the bulk buttons**

Directly after the `lines.forEach` block, add:

```js
    // Isolating one line meant twelve clicks before this existed.
    const setAll = (hidden) => {
      lines.forEach((line) => {
        if (hidden) st.lineHidden[line] = true;
        else delete st.lineHidden[line];
      });
      // Repaint the chips in place. Calling wireStationControls() again would
      // stack a second change listener on every checkbox that bindCheckbox
      // touches, so each later click would fire its handler twice.
      container.querySelectorAll(".line-chip").forEach((chip) => {
        chip.classList.toggle("off", hidden);
        chip.setAttribute("aria-pressed", String(!hidden));
      });
      saveUi();
      applyAll();
    };
    const allBtn = document.getElementById("lines-all");
    const noneBtn = document.getElementById("lines-none");
    if (allBtn) allBtn.onclick = () => setAll(false);
    if (noneBtn) noneBtn.onclick = () => setAll(true);
```

Two deliberate choices here. `onclick` rather than `addEventListener`, because `wireStationControls` runs again on later data loads and assignment overwrites instead of stacking. And the chips are repainted in place rather than by re-rendering, for the reason in the comment — this is the same listener-stacking trap, and it is easy to reintroduce.

- [ ] **Step 4: Style the pills**

Append to `style.css`:

```css
/* --- line chips (2026-07-25 round 3): thirteen checkbox rows cost 423px, an
   eighth of the sidebar, to show labels of two to four characters. --- */
#line-toggles { display: flex; flex-wrap: wrap; gap: 6px; }
.line-chip {
  border: 1px solid var(--line-color);
  background: var(--line-color);
  color: #fff;
  border-radius: 12px;
  padding: 2px 10px;
  font: inherit;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}
.line-chip.off { background: transparent; color: var(--line-color); opacity: 0.55; }
.subhead-row { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
.line-bulk { display: flex; gap: 8px; }
.linkish {
  background: none; border: none; padding: 0;
  font: inherit; font-size: 12px; color: var(--accent, #156f55);
  cursor: pointer; text-decoration: underline;
}
```

Then remove `.line-toggle` from the existing rule so it reads:

```css
.boligtype-toggle { font-size: 13px; }
```

- [ ] **Step 5: Verify in the browser**

The Linjer section is a wrapping row of coloured pills. Measure it:

```js
Math.round(document.getElementById("line-toggles").getBoundingClientRect().height)
```

Expected: well under 150px (was 423px). Clicking a pill fades it and removes that line's stations from the map. "Alle" and "Ingen" work and persist across reload. No console errors.

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/app.js skannonser/web/static/index.html skannonser/web/static/style.css
git commit -m "feat(stations): line toggles become a wrapping pill row with alle/ingen"
```

---

### Task 9: A real station point layer, and a separate radius toggle

**Files:**
- Modify: `skannonser/web/static/stations.js` (new source/layer ids; `stationPointFeatures`; `addStationLayers`; `updateStationLayers`)
- Modify: `skannonser/web/static/app.js` (station UI state default; `wireStationControls`)
- Modify: `skannonser/web/static/index.html` (split the one checkbox into two)
- Test: `tests/web/stations.test.mjs` (create)

**Interfaces:**
- Consumes: existing `stationLineIds(station)`, `lineColor(lineId)`, `effectiveStationRadiusM`, `geodesicCircle`.
- Produces: `STATION_POINT_SOURCE_ID`, `STATION_POINT_LAYER`, `stationPointFeatures(stations) -> FeatureCollection` of Points. `ui.stations.showRadius` (boolean, default `true`) joins `ui.stations.show`.

**Why:** both existing station layers draw the *same* radius polygon — one as a 5%-opacity fill for hover targeting, one as its outline. There is no marker for the station itself; its position is only implied by the circle's centre. "Show stations without radii" therefore needs a new layer, not a split toggle.

- [ ] **Step 1: Write the failing test**

Create `tests/web/stations.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { stationPointFeatures } from "../../skannonser/web/static/stations.js";

const stations = [
  { name: "Sandvika", lat: 59.89, lng: 10.52, lines: ["L1", "R11"] },
  { name: "Asker", lat: 59.83, lng: 10.43, lines: ["L1"] },
  { name: "Uten koord", lat: null, lng: null, lines: ["L1"] },
];

test("one point per station, regardless of how many lines it serves", () => {
  const fc = stationPointFeatures(stations);
  assert.equal(fc.features.length, 2, "the coordinate-less station is skipped");
  assert.equal(fc.features.filter((f) => f.properties.name === "Sandvika").length, 1);
});

test("points carry the station name and a colour", () => {
  const fc = stationPointFeatures(stations);
  const sandvika = fc.features.find((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.geometry.type, "Point");
  assert.deepEqual(sandvika.geometry.coordinates, [10.52, 59.89]);
  assert.match(sandvika.properties.color, /^#/);
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/stations.test.mjs`
Expected: FAIL — `stationPointFeatures is not a function`.

- [ ] **Step 3: Add the ids and the feature builder**

In `stations.js`, beside the existing `STATION_SOURCE_ID` / `STATION_CIRCLE_LAYER` / `STATION_RING_LAYER` exports, add:

```js
export const STATION_POINT_SOURCE_ID = "station-points";
export const STATION_POINT_LAYER = "station-point-dots";
```

Then, directly below `stationCircleFeatures`, add:

```js
// The station itself, as a point. Until round 3 the only station geometry was
// the radius polygon, so a station's position was merely implied by the centre
// of its circle -- which is why "show stations without radii" was impossible.
// One feature per station (not per line): the radius circles are line-filtered
// upstream in updateStationLayers, so nothing here needs per-line duplicates.
export function stationPointFeatures(stations) {
  const features = [];
  (stations || []).forEach((station) => {
    if (station.lat == null || station.lng == null) return;
    const lines = stationLineIds(station);
    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: [station.lng, station.lat] },
      properties: {
        name: station.name || "Stasjon",
        color: lineColor(lines[0] || ""),
      },
    });
  });
  return { type: "FeatureCollection", features };
}
```

- [ ] **Step 4: Add the layer**

In `addStationLayers`, after the existing `STATION_RING_LAYER` `addLayer` call, add:

```js
  map.addSource(STATION_POINT_SOURCE_ID, {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
  });
  // Added last so station dots sit above their own radius rings.
  map.addLayer({
    id: STATION_POINT_LAYER,
    type: "circle",
    source: STATION_POINT_SOURCE_ID,
    paint: {
      "circle-radius": 4,
      "circle-color": ["get", "color"],
      "circle-stroke-width": 1.5,
      "circle-stroke-color": "#ffffff",
    },
  });
```

- [ ] **Step 5: Drive both layers from their own flags**

In `updateStationLayers`, replace:

```js
  const vis = ui.stations.show ? "visible" : "none";
  [STATION_CIRCLE_LAYER, STATION_RING_LAYER].forEach((id) => {
    if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", vis);
  });
```

with:

```js
  const pointSrc = map.getSource(STATION_POINT_SOURCE_ID);
  if (pointSrc) pointSrc.setData(stationPointFeatures(kept));

  // The radius is a detail OF the stations, so it can only show when they do.
  const showStations = !!ui.stations.show;
  const showRadius = showStations && ui.stations.showRadius !== false;
  const setVis = (id, on) => {
    if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", on ? "visible" : "none");
  };
  setVis(STATION_POINT_LAYER, showStations);
  setVis(STATION_CIRCLE_LAYER, showRadius);
  setVis(STATION_RING_LAYER, showRadius);
```

- [ ] **Step 6: Split the checkbox**

In `index.html`, replace:

```html
        <label class="toggle"><input type="checkbox" id="toggle-stations"> Vis stasjoner og radius</label>
```

with:

```html
        <label class="toggle"><input type="checkbox" id="toggle-stations"> Vis stasjoner</label>
        <label class="toggle"><input type="checkbox" id="toggle-station-radius"> Vis radius</label>
```

- [ ] **Step 7: Wire the new flag**

In `app.js`, add `showRadius: true,` to the `stations` block of the default UI state object (beside the existing `show` key). Then in `wireStationControls`, directly after the existing `bindCheckbox("toggle-stations", "show");`, add:

```js
  bindCheckbox("toggle-station-radius", "showRadius");
```

- [ ] **Step 8: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 19 tests.

- [ ] **Step 9: Verify in the browser**

Switch "Vis stasjoner" on with "Vis radius" off: coloured station dots appear with no circles. Switch "Vis radius" on: the circles return around those same dots. Switch "Vis stasjoner" off: both disappear, and "Vis radius" has no effect while it is off. Both settings survive a reload. Station name popups still work. No console errors.

- [ ] **Step 10: Commit**

```bash
git add skannonser/web/static/stations.js skannonser/web/static/app.js skannonser/web/static/index.html tests/web/stations.test.mjs
git commit -m "feat(stations): real station point layer with an independent radius toggle"
```

---

### Task 10: One radius polygon per station

**Files:**
- Modify: `skannonser/web/static/stations.js` (`stationCircleFeatures`)
- Test: `tests/web/stations.test.mjs` (extend)

**Interfaces:**
- Consumes: `stationLineIds`, `effectiveStationRadiusM`, `geodesicCircle`, `lineColor` — all unchanged.
- Produces: `stationCircleFeatures` keeps its signature; each feature's `properties` gains `lines` (comma-joined) and drops the single `line` key.

**Why:** the function emits one polygon per line a station serves, so production draws 214 polygons for 138 stations. The 28 multi-line stations get identical circles stacked on each other, darkening their edges. Line filtering happens upstream on the station list, so the duplicates carry no information.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/stations.test.mjs`:

```js
import { stationCircleFeatures } from "../../skannonser/web/static/stations.js";

test("a multi-line station produces exactly one radius polygon", () => {
  const fc = stationCircleFeatures(stations);
  assert.equal(fc.features.length, 2, "two stations have coordinates");
  const sandvika = fc.features.filter((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.length, 1, "L1 and R11 must not stack identical circles");
  assert.equal(sandvika[0].geometry.type, "Polygon");
  assert.ok(sandvika[0].properties.lines.includes("L1"));
  assert.ok(sandvika[0].properties.lines.includes("R11"));
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/stations.test.mjs`
Expected: FAIL — Sandvika currently yields 2 polygons, so `features.length` is 3.

- [ ] **Step 3: Emit one polygon per station**

Replace the body of `stationCircleFeatures` with:

```js
export function stationCircleFeatures(stations) {
  const features = [];
  (stations || []).forEach((station) => {
    if (station.lat == null || station.lng == null) return;
    const lines = stationLineIds(station);
    const radiusM = effectiveStationRadiusM(station);
    const ring = geodesicCircle(station.lng, station.lat, radiusM);
    // ONE circle per station. Emitting one per line stacked identical polygons
    // on the 28 multi-line stations, darkening their edges and costing geometry
    // for no information -- line filtering already happens on the station list
    // in updateStationLayers, before this is called.
    features.push({
      type: "Feature",
      geometry: { type: "Polygon", coordinates: [ring] },
      properties: {
        name: station.name || "Stasjon",
        lines: lines.join(","),
        color: lineColor(lines[0] || ""),
      },
    });
  });
  return { type: "FeatureCollection", features };
}
```

- [ ] **Step 4: Check for readers of the old `line` property**

Run: `grep -rn '"line"\|\.line\b\|get.*\bline\b' skannonser/web/static/stations.js skannonser/web/static/app.js`

Any paint expression or popup handler reading a feature's `line` property must be updated to `lines`. If none exists, note that in your report.

- [ ] **Step 5: Run the tests and make sure they pass**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 20 tests.

- [ ] **Step 6: Verify in the browser**

With stations and radius both on, circle edges are now uniform — no station has a visibly darker ring than its neighbours. Line filtering still removes the right circles. Hovering a circle still shows the station name.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/stations.js tests/web/stations.test.mjs
git commit -m "perf(stations): one radius polygon per station instead of one per line"
```

---

## Phase 4 — Sidebar and table polish

### Task 11: Label the tag chips, demote "(uten tag)", fix the legend

**Files:**
- Modify: `skannonser/web/static/filters.js` (`tagChipRow`; the `buildFilterPanelUI` call site that mounts it)
- Modify: `skannonser/web/static/app.js` (`renderSourceLegend` — the "Solgt (hvit kant)" row)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: existing `tagChipRow(parent, { options, hidden, tagColors, onChange })`.
- Produces: `tagChipRow` gains an optional `label` option. Callers that omit it are unchanged, so `table.js` needs no edit.

**Why:** Boligtype, Eieform, Energimerking and Tilgjengelighet are all labelled boxes with an "Alle" summary; the chips float beneath them unlabelled. Separately, "(uten tag)" is the widest and heaviest chip while representing the listings the user has *not* assessed. And Task 7 made the legend row "Solgt (hvit kant)" false.

- [ ] **Step 1: Give `tagChipRow` an optional heading and demote the empty bucket**

In `filters.js`, replace the `tagChipRow` signature and body opening:

```js
export function tagChipRow(parent, { options, hidden, tagColors, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "tag-chip-row";
  options.forEach((opt) => {
```

with:

```js
export function tagChipRow(parent, { options, hidden, tagColors, onChange, label }) {
  if (label) {
    const head = document.createElement("div");
    head.className = "filter-head chip-row-head";
    head.textContent = label;
    parent.appendChild(head);
  }
  const wrap = document.createElement("div");
  wrap.className = "tag-chip-row";
  // The "" bucket is the listings NOT yet assessed -- the least interesting
  // group, and the widest chip. Sort it last so the tags the user actually
  // applied lead the row.
  const ordered = [...options].sort((a, b) => (a.key === "" ? 1 : b.key === "" ? -1 : 0));
  ordered.forEach((opt) => {
```

In the same function, where the chip's class is assigned, add the untagged modifier:

```js
    btn.className = "tag-chip" + (hidden[opt.key] ? " off" : "") + (opt.key === "" ? " untagged" : "");
```

- [ ] **Step 2: Pass the label from the map sidebar**

In `buildFilterPanelUI`, add `label: "Tags",` to the `tagChipRow({ ... })` call so it reads:

```js
  tagChipRow(fields, {
    label: "Tags",
    options: vocabs.tags,
    hidden: filters.tagHidden,
    tagColors: assignTagColors(vocabs.tags.map((o) => o.key)),
    onChange,
  });
```

- [ ] **Step 3: Style the heading and the demoted chip**

Append to `style.css`:

```css
.chip-row-head { margin-top: 10px; }
/* The untagged bucket is the default state, not a choice -- give it less
   weight than the tags the user actually applied. */
.tag-chip.untagged { font-weight: 500; opacity: 0.75; }
```

- [ ] **Step 4: Fix the now-false legend row**

In `app.js`'s `renderSourceLegend`, the first three legend rows are built from a literal array of `{ label, border, square }` objects. Task 7 made "Solgt (hvit kant)" false — closed listings are now hollow rings. Add a `hollow` flag to that array and honour it in the builder.

Replace:

```js
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
```

with:

```js
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
```

Also update the comment two lines above that array, which currently says the border distinguishes "active = dark, sold = white".

- [ ] **Step 5: Verify in the browser**

The map sidebar shows a "Tags" heading above the chips, matching the other four filters. "(uten tag)" sits last and reads lighter. The legend's closed-listing row shows a ring and says "Solgt/lukket (ring)". The table toolbar chips are unchanged (no label there — the toolbar has no room and the chips sit beside labelled buttons). No console errors on either page.

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/filters.js skannonser/web/static/app.js skannonser/web/static/style.css
git commit -m "feat(web): label the tag chips, demote the untagged bucket, fix the closed-listing legend"
```

---

### Task 12: Table defaults and a readable closed-row marker

**Files:**
- Modify: `skannonser/web/static/table.js` (`DEFAULT_HIDDEN_COLUMNS`)
- Modify: `skannonser/web/static/style.css` (the `.sold-badge, .inactive-badge` rule)

**Interfaces:**
- Consumes: existing `DEFAULT_HIDDEN_COLUMNS`, `loadHiddenColumns`.
- Produces: no new exports.

**Why:** `tilgjengelighet` is empty for all 770 active listings in production yet ships visible. And a closed row is marked only by a barely-perceptible background tint plus a small grey badge pressed against the address with no spacing — the same weak-encoding problem Task 7 fixed on the map.

Note on scope: the sold-only columns (`sold_price`, `sold_date`, `premium`) are deliberately **not** added to the hidden defaults. They become useful the moment "Vis solgte" is enabled, and making column visibility react to that toggle would fight the user's own explicit choices in the column picker.

- [ ] **Step 1: Hide the always-empty column by default**

Replace:

```js
const DEFAULT_HIDDEN_COLUMNS = ["postnummer", "pris", "felleskost_mnd", "soverom", "etasje"];
```

with:

```js
// First-run defaults. `tilgjengelighet` is empty for every active listing in
// production (0 of 770) -- it stays in the picker for anyone who wants it, but
// costs a column of horizontal scroll by default for nothing.
const DEFAULT_HIDDEN_COLUMNS = [
  "postnummer", "pris", "felleskost_mnd", "soverom", "etasje", "tilgjengelighet",
];
```

- [ ] **Step 2: Give the closed-row badge real weight**

Replace the existing `.sold-badge, .inactive-badge` rule in `style.css` with:

```css
/* A closed row needs to read as a different category, not a slightly greyer
   one -- same reasoning as the hollow map dots. */
.sold-badge, .inactive-badge {
  display: inline-block;
  margin-left: 8px;
  padding: 1px 7px;
  border-radius: 9px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.02em;
  border: 1px solid currentColor;
  background: transparent;
}
.sold-badge { color: #6b5a1f; }
.inactive-badge { color: #7a4a45; }
```

Keep any other declarations the original rule had that are not overridden here.

- [ ] **Step 3: Verify in the browser**

Clear `hiddenColumns` from the stored blob and reload `/table`: Tilgjengelighet is hidden and still listed in the "Kolonner" picker, where re-enabling it works. With "Vis solgte/inaktive" on, closed rows carry a clearly legible outlined badge separated from the address. Confirm the table's horizontal scroll width shrank:

```js
document.querySelector("table").scrollWidth
```

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/table.js skannonser/web/static/style.css
git commit -m "feat(table): hide the always-empty column by default, make closed rows legible"
```

---

### Task 13: Say something when the filters hide everything

**Files:**
- Modify: `skannonser/web/static/app.js` (`updateStatus` or the equivalent that writes the map's count line)
- Modify: `skannonser/web/static/table.js` (`render`, where the "N av M annonser" line is written)
- Modify: `skannonser/web/static/index.html` (an empty-state container over the map)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: existing `resetFilters` from filterstate.js (already imported on both pages), the existing visible/total counts.
- Produces: no new exports.

**Why:** at 100% nedtoning with a tight filter the map goes blank with no explanation. Note the map is *worse* off than the table here: the table at least writes "N av M annonser", while the map's status line only ever says "N annonser lastet" — it has no visible-count concept at all, so this task has to introduce one.

**Message wording:** the map can be empty because every layer is off *or* because filters exclude everything, and the reset button only helps the second case. Use one honest message that covers both rather than two code paths: `"Ingen annonser vises med gjeldende lag og filtre."`

- [ ] **Step 1: Add the map's empty-state element**

In `index.html`, directly after `<div id="map"></div>`, add:

```html
    <div id="map-empty" class="map-empty" hidden>
      <p>Ingen annonser vises med gjeldende lag og filtre.</p>
      <button type="button" id="map-empty-reset">Nullstill filtre</button>
    </div>
```

- [ ] **Step 2: Style it**

Append to `style.css`:

```css
/* Shown only when filters hide every listing -- a blank map otherwise looks
   like a loading failure. */
.map-empty {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  background: rgba(255, 255, 255, 0.94);
  border: 1px solid #d9dedb;
  border-radius: 6px;
  padding: 14px 18px;
  text-align: center;
  z-index: 3;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.12);
}
.map-empty p { margin: 0 0 8px; font-size: 14px; }
```

- [ ] **Step 3: Count what actually reached the map, then toggle the message**

`featureCollectionsByGroup` is the only place that knows how many listings survived the layer toggles, the filters and the hard-hide at 100% nedtoning. Record the tally there.

In `app.js`, inside `featureCollectionsByGroup`, replace the final push and return:

```js
    const op = excluded ? residual : item.closed ? soldOpacity : 1;
    byGroup[gid].push(itemToFeature(item, op));
  });
  return byGroup;
}
```

with:

```js
    const op = excluded ? residual : item.closed ? soldOpacity : 1;
    byGroup[gid].push(itemToFeature(item, op));
    shown++;
  });
  // The only point that knows what survived layers + filters + hard-hide.
  state.shownCount = shown;
  return byGroup;
}
```

and declare the counter beside `const byGroup = {};`:

```js
  let shown = 0;
```

Then in `applyAll`, inside the `requestAnimationFrame` callback and after `const byGroup = featureCollectionsByGroup();`, add:

```js
    // A blank map reads as a loading failure, not as a filter result.
    const emptyEl = document.getElementById("map-empty");
    if (emptyEl) emptyEl.hidden = !(state.itemsById.size > 0 && state.shownCount === 0);
```

Wire the button once, beside the other one-time wiring in `init`:

```js
  const emptyReset = document.getElementById("map-empty-reset");
  if (emptyReset) {
    emptyReset.addEventListener("click", () => {
      resetFilters(state.ui.filters);
      saveUi();
      rebuildFilterUIs();
      applyAll();
    });
  }
```

- [ ] **Step 4: Do the same for the table**

The table already writes `rows.length + " av " + state.items.length + " annonser"` (around line 472), so it only needs the in-body message. In `render`, after the body rows are appended and using the same `rows` array that count line uses, add a single full-width row when nothing matched:

```js
  if (!rows.length && state.items.length) {
    const tr = el("tr");
    const td = el("td", "empty-row", "Ingen annonser vises med gjeldende lag og filtre. ");
    td.colSpan = visibleColumns().length;
    const btn = el("button", null, "Nullstill filtre");
    btn.type = "button";
    btn.addEventListener("click", () => {
      resetFilters(state.filters);
      saveFilters(state.filters);
      refreshVocabs();
      render();
    });
    td.appendChild(btn);
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
```

Adapt `rows` / `tbody` to the names `render` already uses.

- [ ] **Step 5: Verify in the browser**

On the map, drag "Maks pris" to its minimum with "Filtret nedtoning" at 100% so every dot is hard-hidden: the message appears, and "Nullstill filtre" restores everything. Switch all four layer checkboxes off: the same message appears (which is why it names both layers and filters). On the table, filter the text box to nonsense: the in-body message appears with a working reset.

Confirm the message does **not** appear during the initial load, while `state.itemsById` is still empty — reload with a hard filter already stored and watch that it only appears once data has arrived.

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/app.js skannonser/web/static/table.js skannonser/web/static/index.html skannonser/web/static/style.css
git commit -m "feat(web): explain an empty result instead of showing a blank map"
```

---

### Task 14: Full verification pass

**Files:** none created — this is the acceptance run.

- [ ] **Step 1: Both suites**

Run: `node --test tests/web/*.test.mjs` → expect `# fail 0`, 20 tests.
Run: `.venv/bin/pytest -q` → expect `656 passed`. Any failure here means a task strayed outside `web/static/` — stop and fix.

- [ ] **Step 2: Acceptance walk-through**

On the seeded dev server, confirm end to end:

1. Tag a closed listing, toggle Solgt on then off — the chip appears and then **disappears**, counts return to their active-only values, and a hidden orphan does not linger in "N filtre aktive". Repeat on the table.
2. Active dots draw above closed ones; closed dots are hollow rings; the X still marks inactive ones; budpremie mode recolours sold rings only.
3. Markers are visibly larger, and the DNB square, tag ring and X still match their dots at zoom 11, 13 and 15.
4. Linjer is a pill row under 150px with working "Alle"/"Ingen"; stations show without radii; radius follows stations.
5. Chips carry a "Tags" heading with "(uten tag)" last; the legend says "Solgt/lukket (ring)".
6. Tilgjengelighet is hidden by default and restorable; closed rows carry a legible badge.
7. Filtering everything out produces the empty-state message on both pages.

- [ ] **Step 3: Console and mobile sweep**

`read_console_messages` clean on both pages after the walk-through. At 375px: the line pills and tag chips wrap without horizontal overflow, the drawer still opens, and the empty-state box fits the viewport.

- [ ] **Step 4: Update the round-2 spec status note**

In `docs/superpowers/specs/2026-07-25-ui-polish-round-2-design.md`, append to the status line area:

```markdown
**Follow-up:** the 2026-07-25 UX review found the tag chips introduced here inherit a
pre-existing vocabulary-pollution bug; fixed in round 3 (see
`docs/superpowers/plans/2026-07-25-ui-polish-round-3.md`).
```

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-07-25-ui-polish-round-2-design.md
git commit -m "docs: cross-reference the round 3 follow-up from the round 2 spec"
```

---

## Plan amendment (owner feedback after Phase 2, 2026-07-26)

Owner reviewed the Phase 2 markers in their own browser. Sizes approved. Two changes requested,
inserted here as Tasks 7A and 7B and to be done before Phase 3.

### Task 7A: Make the tag colour read on the bubble again

**Files:**
- Modify: `skannonser/web/static/map.js` (the `RING_R` constant; the `-tagring` pass in `addListingGroups`)
- Test: `tests/web/maplayers.test.mjs` (extend)

**Interfaces:**
- Consumes: `DOT_R` from Task 6.
- Produces: no export change. `RING_R` stays private.

**Why:** the owner reports tagged listings no longer read as tagged. Two changes compounded.
Task 6 moved the ring from radius 12 to 15 while the dot went 7 → 9, so the gap between dot edge
and ring grew from 3.5px to 4.5px and the ring's outer edge went from 15px to 18px — it reads as a
large loose circle near the dot rather than an outline on it. Task 5 then moved every ring into a
single pass **beneath every dot on the map**; previously a group's ring drew above earlier groups'
dots. In dense areas a 36px-wide ring is now substantially covered by neighbouring dots.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/maplayers.test.mjs`:

```js
test("the tag ring hugs its dot and draws above every dot layer", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const specs = [];
  const map = fakeMap();
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  const ring = specs.find((s) => s.id.endsWith("-tagring"));
  const dot = specs.find((s) => s.id.endsWith("-eie"));
  const dotOuter = dot.paint["circle-radius"] + dot.paint["circle-stroke-width"];
  const gap = ring.paint["circle-radius"] - dotOuter;
  assert.ok(gap >= 0 && gap <= 2.5,
    `ring should hug the dot, got a ${gap}px gap`);

  const lastDot = Math.max(...map.added.flatMap((id, i) => (/-(eie|dnb|sold)$/.test(id) ? [i] : [])));
  const firstRing = Math.min(...map.added.flatMap((id, i) => (id.endsWith("-tagring") ? [i] : [])));
  assert.ok(firstRing > lastDot,
    "rings must draw above dots so a neighbouring dot cannot cover them");
});
```

Note this REPLACES the intent of the earlier "tag rings sit beneath every dot layer" test added in
Task 5 — delete that test, since the two now contradict and this one is the owner's decision.

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/maplayers.test.mjs`
Expected: FAIL on the gap assertion (4.5px) and on the ordering assertion.

- [ ] **Step 3: Tighten the ring**

Change the `RING_R` constant to hug the dot:

```js
const RING_R = DOT_R + 2; // ring band sits just outside the dot's 1.5px border
```

- [ ] **Step 4: Draw rings above the dots**

Move the whole `groups.forEach` pass that adds the `-tagring` layer so it runs AFTER the closed
pass and the active pass, i.e. make it the last layer pass in `addListingGroups`, immediately
before the trailing click-wiring block. Do not change the layer's `paint`, `filter` or `id`.

Update the layer's leading comment: the ring is a halo drawn ABOVE the dots so a neighbouring
dot cannot cover it, and it can safely sit on top because its fill is transparent.

- [ ] **Step 5: Run the tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`. Count drops by one (deleted test) and rises by one (new test).

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/map.js tests/web/maplayers.test.mjs
git commit -m "fix(map): tag ring hugs its dot and draws above neighbouring dots"
```

---

### Task 7B: Retire the budpremie colouring control

**Files:**
- Modify: `skannonser/web/static/index.html` (the `toggle-sold-premium` label; the `premium-legend` div)
- Modify: `skannonser/web/static/app.js` (`soldPremium` default and its wiring)

**Interfaces:**
- Consumes: existing `setSoldColorMode`, `PREMIUM_LEGEND`.
- Produces: nothing new. `setSoldColorMode`, `PREMIUM_COLOR` and `PREMIUM_LEGEND` stay in `map.js`
  unused-but-intact.

**Why:** the owner finds the budpremie colouring confusing and does not want it for now, but may
want it later. So this hides the control rather than deleting the mechanism.

**Critical:** a user with `soldPremium: true` already persisted in their `skannonser.ui.v1` blob
must not be stranded in premium colouring with no control to leave it. Force the flag off.

- [ ] **Step 1: Remove the control from the markup**

In `index.html`, delete the `toggle-sold-premium` label line and the `premium-legend` div that
follows it.

- [ ] **Step 2: Force the flag off and stop applying the mode**

In `app.js`, change the state default to make the retirement explicit:

```js
    // Budpremie colouring is retired for now (owner, 2026-07-26): the control is
    // gone from the sidebar but setSoldColorMode/PREMIUM_* remain in map.js so it
    // can be brought back. Forced false on load so a stored `true` from before the
    // control disappeared cannot strand anyone in premium colours.
    soldPremium: false,
```

Then, wherever the UI state is loaded, force it off after the merge with stored values, so a
persisted `true` cannot survive. Find the load path and set `ui.soldPremium = false` there.

Make `wirePremiumToggle` (or whatever wires the checkbox) tolerate the missing element and do
nothing — it should already guard with a null check; verify it does and leave it otherwise intact.

Remove the call that applies the mode on map load (`if (state.ui.soldPremium) setSoldColorMode(...)`)
since the flag is now always false — or leave it, and say which you chose and why.

- [ ] **Step 3: Verify no dead reference breaks**

Run: `grep -rn "toggle-sold-premium\|premium-legend" skannonser/web/static/`
Every remaining hit must be a null-guarded lookup, not an unconditional dereference.

- [ ] **Step 4: Run the tests**

Run: `node --test tests/web/*.test.mjs` — expected `# fail 0`. `PREMIUM_LEGEND` and
`setSoldColorMode` keep their tests; they are still exported and still work.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/index.html skannonser/web/static/app.js
git commit -m "feat(map): retire the budpremie colouring control for now"
```

---

### Task 7C: Cluster halo showing how much of a cluster is reviewed

**Files:**
- Modify: `skannonser/web/static/map.js` (`clusterProperties` on each source; a new cluster-halo layer in the final tag pass)
- Test: `tests/web/maplayers.test.mjs` (extend)

**Interfaces:**
- Consumes: the per-feature `hasTag` boolean app.js already stamps (the `-tagring` layer filters on it); the existing `clusterProperties` mechanism, which already aggregates `op_sum`.
- Produces: a `tag_sum` cluster property and a `{g.id}-cluster-tagring` layer. No export change.

**Why:** a tag marks a listing the owner has reviewed, but the ring disappears when dots collapse into a cluster, so at overview zoom there is no sense of which areas have been worked through. A binary "contains ≥1 tagged listing" indicator was rejected: a cluster of 50 with one tag would look identical to a cluster of 3 fully reviewed. Scaling the halo with the reviewed FRACTION answers the question actually asked at that zoom — how much of this area have I been through — and costs the same single layer.

Note the halo uses ONE fixed colour, not a tag colour: a cluster mixes tags, so no single tag's colour applies. Colour is deliberately secondary here — the owner's stated priority is that the marker reads as "reviewed" at all.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/maplayers.test.mjs`:

```js
test("clusters carry a tagged-count property and a proportional halo", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const specs = [];
  const sources = [];
  const map = fakeMap();
  map.addSource = (id, cfg) => sources.push({ id, cfg });
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  assert.ok(sources.every((s) => s.cfg.clusterProperties && s.cfg.clusterProperties.tag_sum),
    "every clustered source must aggregate a tagged count");

  const halo = specs.find((s) => s.id.endsWith("-cluster-tagring"));
  assert.ok(halo, "a cluster halo layer must exist");
  assert.equal(halo.paint["circle-opacity"], 0, "halo is a ring, not a disc");
  assert.ok(JSON.stringify(halo.filter).includes("point_count"),
    "halo applies to clusters only");
  assert.ok(JSON.stringify(halo.paint["circle-stroke-opacity"]).includes("tag_sum"),
    "halo strength must derive from the tagged fraction, not be a constant");
});

test("the cluster halo draws above the cluster bubble", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const bubble = Math.max(...map.added.flatMap((id, i) => (id.endsWith("-cluster") ? [i] : [])));
  const halo = Math.min(...map.added.flatMap((id, i) => (id.endsWith("-cluster-tagring") ? [i] : [])));
  assert.ok(halo > bubble, "halo must not be hidden behind its own bubble");
});
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node --test tests/web/maplayers.test.mjs`
Expected: FAIL — no `tag_sum`, no `-cluster-tagring` layer.

- [ ] **Step 3: Aggregate the tagged count**

In the source config, extend `clusterProperties`:

```js
      clusterProperties: {
        op_sum: ["+", ["get", "op"]],
        // How many members carry a tag. With point_count this gives the
        // reviewed FRACTION, which drives the halo's strength below.
        tag_sum: ["+", ["case", ["==", ["get", "hasTag"], true], 1, 0]],
      },
```

- [ ] **Step 4: Add the halo layer**

Add this inside the SAME final pass that adds the `-tagring` layer (the last layer pass, so tag indicators are never covered), directly after the `-tagring` `addLayer` call:

```js
    // Cluster-level "how much of this have I reviewed" halo. One fixed colour,
    // not a tag colour: a cluster mixes tags, so no single tag's colour applies.
    // Strength scales with the reviewed fraction -- a barely-touched cluster
    // shows a faint ring, a fully-reviewed one a strong one -- because a binary
    // "contains a tag" mark would make 1-of-50 look like 3-of-3. Multiplied by
    // the same cluster opacity as the bubble so nedtoning still fades it.
    map.addLayer({
      id: g.id + "-cluster-tagring",
      type: "circle",
      source: g.id,
      filter: ["all", ["has", "point_count"], [">", ["get", "tag_sum"], 0]],
      paint: {
        "circle-radius": [
          "interpolate", ["linear"], ["get", "point_count"],
          2, 18, 25, 23, 100, 29, 500, 34,
        ],
        "circle-color": "rgba(0,0,0,0)",
        "circle-opacity": 0,
        "circle-stroke-width": 2.5,
        "circle-stroke-color": TAG_CLUSTER_HALO,
        "circle-stroke-opacity": [
          "*",
          clusterOpacity,
          [
            "interpolate", ["linear"],
            ["/", ["get", "tag_sum"], ["get", "point_count"]],
            0, 0.3,
            1, 1,
          ],
        ],
      },
    });
```

The radius stops are the cluster bubble's own stops (`2,14 25,19 100,25 500,30`) plus 4, so the halo clears the bubble at every size.

`clusterOpacity` is the expression already computed in the cluster pass. It is scoped to that pass — hoist it, or recompute the identical expression in this pass and say which you did.

- [ ] **Step 5: Define the halo colour**

Beside `ACTIVE_BORDER`, add:

```js
// Cluster review-halo colour. Deliberately one fixed colour rather than a tag
// colour (clusters mix tags), and taken from the TAG palette family rather than
// the boligtype palette so it reads as annotation, not data.
const TAG_CLUSTER_HALO = "#c2185b";
```

- [ ] **Step 6: Run the tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 21 tests.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/map.js tests/web/maplayers.test.mjs
git commit -m "feat(map): cluster halo scaled to how much of the cluster is reviewed"
```

---

### Task 7D: Cluster review indicator becomes a progress arc, not a full ring

**Files:**
- Modify: `skannonser/web/static/map.js` (delete the `-cluster-tagring` GL layer and `TAG_CLUSTER_HALO`; extend `syncClusterMarkers`)
- Modify: `skannonser/web/static/style.css` (append)
- Test: `tests/web/maplayers.test.mjs` (replace the two Task 7C halo tests)

**Interfaces:**
- Consumes: the `tag_sum` cluster property from Task 7C (KEEP it — the DOM marker needs it), `clusterSize(count)`.
- Produces: a `--reviewed` CSS custom property and a `data-reviewed` attribute on cluster marker elements.

**Why:** owner feedback on the Task 7C halo. A full ring whose *thickness* encodes proportion is hard to read as a proportion — a progress arc is directly legible ("this much of the circle is filled in" = "this much reviewed"). A GL `circle` layer cannot draw an arc, but the cluster count marker is already a DOM element sitting exactly over the bubble, and CSS `conic-gradient` draws arcs trivially. Owner also rejected the crimson as too aggressive and asked for white or another neutral.

- [ ] **Step 1: Replace the two halo tests**

In `tests/web/maplayers.test.mjs`, DELETE the two Task 7C tests ("clusters carry a tagged-count property and a proportional halo" and "the cluster halo draws above the cluster bubble") and add:

```js
test("the tagged-count cluster property survives (the DOM arc needs it)", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const sources = [];
  const map = fakeMap();
  map.addSource = (id, cfg) => sources.push({ id, cfg });
  addListingGroups(map, groups, () => {});
  assert.ok(sources.every((s) => s.cfg.clusterProperties && s.cfg.clusterProperties.tag_sum),
    "every clustered source must still aggregate a tagged count");
});

test("no GL cluster-halo layer remains — the arc is drawn in the DOM", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  assert.ok(!map.added.some((id) => id.endsWith("-cluster-tagring")),
    "the GL halo layer must be gone");
});
```

- [ ] **Step 2: Run them to confirm the second fails**

Run: `node --test tests/web/maplayers.test.mjs`
Expected: FAIL on "no GL cluster-halo layer remains" — the layer is still there.

- [ ] **Step 3: Delete the GL halo**

Remove the whole `map.addLayer({ id: g.id + "-cluster-tagring", ... })` block from the final tag pass, and delete the `TAG_CLUSTER_HALO` constant. Leave the `-tagring` per-dot layer and the `tag_sum` aggregation exactly as they are.

Confirm: `grep -rn "TAG_CLUSTER_HALO\|cluster-tagring" skannonser/web/static/` returns nothing.

- [ ] **Step 4: Feed the fraction to the DOM marker**

In `syncClusterMarkers`, after `div.textContent = f.properties.point_count_abbreviated;` add:

```js
      // Reviewed-progress arc (CSS draws it; see style.css). A GL circle layer
      // can only draw a full ring, and thickness reads poorly as a proportion --
      // an arc is directly legible. Only set when something is tagged, so
      // untouched clusters carry no decoration at all.
      const reviewed = Number(f.properties.tag_sum) || 0;
      if (reviewed > 0 && count > 0) {
        div.dataset.reviewed = "";
        div.style.setProperty("--reviewed", String(Math.min(1, reviewed / count)));
      }
```

- [ ] **Step 5: Draw the arc**

Append to `style.css`:

```css
/* Cluster review-progress arc (2026-07-26). The count marker sits exactly over
   the GL bubble, so a pseudo-element just outside it reads as that bubble's
   own progress. White with a soft dark shadow so it holds up on both the pale
   and the dark parts of the OSM basemap without competing with the boligtype
   colours the way the earlier crimson ring did. */
.cluster-marker.cluster-count { position: relative; }
.cluster-marker.cluster-count[data-reviewed]::after {
  content: "";
  position: absolute;
  inset: -5px;
  border-radius: 50%;
  /* from -90deg so the arc starts at 12 o'clock, like a progress dial. */
  background: conic-gradient(from -90deg, #ffffff calc(var(--reviewed, 0) * 360deg), rgba(0, 0, 0, 0) 0);
  -webkit-mask: radial-gradient(farthest-side, rgba(0, 0, 0, 0) calc(100% - 3px), #000 calc(100% - 3px));
  mask: radial-gradient(farthest-side, rgba(0, 0, 0, 0) calc(100% - 3px), #000 calc(100% - 3px));
  filter: drop-shadow(0 0 1px rgba(0, 0, 0, 0.55));
  pointer-events: none;
}
```

- [ ] **Step 6: Run the tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 21 tests.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/map.js skannonser/web/static/style.css tests/web/maplayers.test.mjs
git commit -m "feat(map): cluster review indicator becomes a white progress arc"
```
