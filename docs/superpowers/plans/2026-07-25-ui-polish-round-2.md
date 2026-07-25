# UI Polish Round 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make user tags first-class visual objects (colors, rings, chips, quick-filtering), restore boligtype color to inactive map dots with an X overlay, turn popup links into buttons, fill column-filter gaps, add a column picker, and add the map→table deep link.

**Architecture:** Frontend-only — every change lives in `skannonser/web/static/` (+ one HTML file, one CSS file, docs). A new pure module `tagcolors.js` is the single source of tag→color truth, consumed by app.js (map features), filters.js (chips), popup.js and table.js. All new filters ride the existing shared-state pipeline (filterstate.js defaults/entries → filters.js `listingExcluded` → tablefilters.js popovers) so map and table always agree.

**Tech Stack:** Vanilla ES modules (no build step), MapLibre GL, FastAPI static serving (untouched). Tests: `node --test` for the pure module; browser verification via the in-app preview for everything DOM/GL.

**Spec:** `docs/superpowers/specs/2026-07-25-ui-polish-round-2-design.md`

## Global Constraints

- **Zero backend changes.** No edits under `skannonser/` outside `skannonser/web/static/`. No API shape change, no migrations.
- No external CDNs or new dependencies; no build step — plain ES modules only.
- UI copy is Norwegian (bokmål), matching existing labels ("Maks pris/kvm", "Kolonner", "Vis kolonner", "Fant ikke annonse …").
- Shared filter state rules: new filter keys get defaults in `defaultFilters`, entries in `activeFilterEntries`, predicates in `listingExcluded`, and UI in `COLUMN_FILTERS` — never a partial subset.
- localStorage stays inside the one `skannonser.ui.v1` blob (read-modify-write, never overwrite the whole blob).
- Comment style: comments state constraints/WHY, not what the next line does (match the files' existing voice).
- The Python suite must stay green: `.venv/bin/pytest -q` (no changes expected; run it in the final task).

## Dev-server setup (used from Task 2 on)

The local `main/database/properties.db` is only migrated through 005 (live data is on the server). Browser verification uses a scratch copy, migrated and seeded:

```bash
SCRATCH=/private/tmp/claude-501/-Users-tehbaer-kode-skannonser/f9854acb-68d2-44c1-a28d-6fcf9b3c1377/scratchpad
cp main/database/properties.db "$SCRATCH/ui-polish.db"
SKANNONSER_DB_PATH="$SCRATCH/ui-polish.db" .venv/bin/skannonser db migrate
```

Expected: `Applied: 006_sold_prices, 007_sold_sweep_state, 008_postnummer_pad, 009_sold_attempts, 010_listing_details, 011_neighbour_sold` (details/facilities tables will be empty — fine; tag/ring/chip/X/column features don't need them, and null enrichment cells are the normal degraded case).

Seed three tags on active listings with coordinates:

```bash
sqlite3 "$SCRATCH/ui-polish.db" "
WITH cand AS (
  SELECT e.finnkode, ROW_NUMBER() OVER (ORDER BY e.finnkode) AS rn
  FROM eiendom e JOIN eiendom_processed p ON p.finnkode = e.finnkode
  WHERE e.active = 1 AND p.lat IS NOT NULL LIMIT 3
)
INSERT OR REPLACE INTO annotations (finnkode, tag, updated_at)
SELECT finnkode,
       CASE rn WHEN 1 THEN 'maybe' WHEN 2 THEN 'definitivt' ELSE 'hard no' END,
       datetime('now')
FROM cand;"
```

Update `.claude/launch.json`'s existing `skannonser-web-review` entry so `--db` points at `$SCRATCH/ui-polish.db` (keep host/port 127.0.0.1:8377), then start it with the preview tool (`preview_start {name: "skannonser-web-review"}`) — never via Bash.

---

### Task 1: `tagcolors.js` — deterministic tag→color assignment

**Files:**
- Create: `skannonser/web/static/tagcolors.js`
- Test: `tests/web/tagcolors.test.mjs` (new directory)

**Interfaces:**
- Produces: `TAG_PALETTE: string[]` (10 hex colors), `normalizeTag(tag) -> string` ("" for empty), `assignTagColors(tagKeys: iterable) -> Map<string,string>` (normalized tag → color), `colorForTag(tag, colors: Map) -> string|null`. Every later task consumes these exact names.

- [ ] **Step 1: Write the failing test**

```js
// tests/web/tagcolors.test.mjs
// Node's built-in runner (node --test) -- the static frontend's only pure,
// DOM-free module, so it gets the one real unit test cycle.
import test from "node:test";
import assert from "node:assert/strict";
import {
  assignTagColors,
  colorForTag,
  TAG_PALETTE,
} from "../../skannonser/web/static/tagcolors.js";

test("deterministic and normalization-insensitive", () => {
  const a = assignTagColors(["maybe", "definitivt", "hard no"]);
  const b = assignTagColors(["  Hard No ", "definitivt", "maybe", "maybe", "", null]);
  assert.equal(colorForTag("maybe", a), colorForTag(" MAYBE ", b));
  assert.equal(colorForTag("hard no", a), colorForTag("hard no", b));
});

test("all colors come from the palette", () => {
  const colors = assignTagColors(["maybe", "definitivt", "hard no"]);
  for (const c of colors.values()) assert.ok(TAG_PALETTE.includes(c));
});

test("distinct tags get distinct colors up to palette size", () => {
  const tags = ["maybe", "definitivt", "hard no", "ja", "nei", "kanskje", "se på", "bud", "x", "y"];
  const colors = assignTagColors(tags);
  assert.equal(new Set(colors.values()).size, tags.length);
});

test("empty/unknown tags have no color", () => {
  const colors = assignTagColors(["maybe"]);
  assert.equal(colorForTag("", colors), null);
  assert.equal(colorForTag(null, colors), null);
  assert.equal(colorForTag("never-seen", colors), null);
});

test("a tag keeps its color when a later-sorting tag arrives", () => {
  const before = assignTagColors(["maybe"]);
  const after = assignTagColors(["maybe", "zzz-nyeste"]);
  assert.equal(colorForTag("maybe", before), colorForTag("maybe", after));
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/web/*.test.mjs`
Expected: FAIL — `Cannot find module .../skannonser/web/static/tagcolors.js`

- [ ] **Step 3: Write the implementation**

```js
// skannonser/web/static/tagcolors.js
// Deterministic tag -> color assignment (2026-07-25 UI polish round 2, §1).
// A tag hashes (djb2) into a fixed palette; collisions probe to the next
// free slot with tags processed in SORTED order, so the assignment is a
// pure function of the current tag SET: distinct colors are guaranteed
// while <= palette-size tags exist, and a tag keeps its hash slot unless
// an earlier-sorted tag claims it. (Pure per-tag hashing was rejected:
// "maybe" and "definitivt" collide under djb2 mod 10.)

// Hue-offset from map.js's TYPE_COLOR_PALETTE (boligtype dots) so a tag
// ring is never confusable with its own dot color; all carry white text.
export const TAG_PALETTE = [
  "#c2185b", "#7b1fa2", "#303f9f", "#0277bd", "#00695c",
  "#558b2f", "#ff8f00", "#d84315", "#5d4037", "#455a64",
];

export function normalizeTag(tag) {
  return tag ? String(tag).trim().toLowerCase() : "";
}

function djb2(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h << 5) + h + str.charCodeAt(i)) >>> 0;
  return h;
}

// tagKeys: any iterable of raw tag values (dupes/empties fine).
export function assignTagColors(tagKeys) {
  const keys = [...new Set([...tagKeys].map(normalizeTag).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "nb"));
  const taken = new Set();
  const colors = new Map();
  keys.forEach((key) => {
    let idx = djb2(key) % TAG_PALETTE.length;
    // Probing only makes sense while free slots exist; past palette size,
    // collisions are unavoidable and plain hashing is the stable choice.
    if (keys.length <= TAG_PALETTE.length) {
      while (taken.has(idx)) idx = (idx + 1) % TAG_PALETTE.length;
    }
    taken.add(idx);
    colors.set(key, TAG_PALETTE[idx]);
  });
  return colors;
}

export function colorForTag(tag, colors) {
  return colors.get(normalizeTag(tag)) || null;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/web/*.test.mjs`
Expected: `# pass 5`, `# fail 0`

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/tagcolors.js tests/web/tagcolors.test.mjs
git commit -m "feat(web): deterministic tag->color module with collision probing"
```

---

### Task 2: Map tag rings take the tag's color

**Files:**
- Modify: `skannonser/web/static/app.js` (imports ~line 20; `itemToFeature` lines 186-205; `featureCollectionsByGroup` lines 209-239)
- Modify: `skannonser/web/static/map.js` (delete `TAG_RING_COLOR` line 185; `-tagring` layer paint lines 272-284)

**Interfaces:**
- Consumes: `assignTagColors`, `colorForTag` from Task 1.
- Produces: `state.tagColors: Map` on the app.js state object, refreshed at the top of every `featureCollectionsByGroup()` run — Task 5 (popup chip) reads it. GeoJSON features carry `tagColor: "#rrggbb"` whenever they carry `hasTag: true`.

- [ ] **Step 1: Set up the dev server** — follow "Dev-server setup" at the top of this plan (scratch DB copy, migrate, seed 3 tags, point launch.json at it, `preview_start`). Load `http://127.0.0.1:8377/` and confirm the map renders with today's purple rings on the three seeded listings.

- [ ] **Step 2: Stamp `tagColor` on features (app.js)**

Add to the imports block:

```js
import { assignTagColors, colorForTag } from "./tagcolors.js";
```

In `itemToFeature` replace

```js
  if (tagKeyOf(item)) properties.hasTag = true; // drives the tag-ring layer
```

with

```js
  const tagColor = colorForTag(item.tag, state.tagColors || new Map());
  if (tagColor) {
    properties.hasTag = true; // drives the tag-ring layer
    properties.tagColor = tagColor; // the ring's stroke color
  }
```

Then delete the now-unused `tagKeyOf` function (lines 182-184 — that replaced line was its only call site). `colorForTag` is non-null exactly when the trimmed tag is non-empty, since `state.tagColors` is built from the full item set below.

At the top of `featureCollectionsByGroup()` (before `const ctx = {`):

```js
  // Rebuilt every recompute: cheap (one hash per distinct tag) and always
  // in sync with the current tag set -- popup chips read this same map.
  state.tagColors = assignTagColors(
    [...state.itemsById.values()].map((i) => i.tag)
  );
```

- [ ] **Step 3: Ring reads the feature's color (map.js)**

Delete:

```js
// Ring drawn beneath any listing that carries a tag -- the "this one is
// annotated" marker, independent of boligtype colour.
const TAG_RING_COLOR = "#7c3aed";
```

In the `-tagring` layer, replace `"circle-stroke-color": TAG_RING_COLOR,` with `"circle-stroke-color": ["get", "tagColor"],` and update the layer's comment to say the ring carries the TAG's color (features matching the `hasTag` filter always carry `tagColor`).

- [ ] **Step 4: Verify in browser** — reload the preview. The three seeded listings' rings are three different colors (expected from the palette: "definitivt", "hard no", "maybe" all distinct), no purple remains, and `read_console_messages` shows no errors. Set a 4th tag via a popup editor and confirm a ring appears immediately (the `sk-annotation-saved` → `applyAll()` path recomputes `state.tagColors`).

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/app.js skannonser/web/static/map.js
git commit -m "feat(map): tag rings colored by tag via tagcolors module"
```

---

### Task 3: Inactive dots keep boligtype color, X overlay

**Files:**
- Modify: `skannonser/web/static/map.js` (delete `INACTIVE_COLOR` + `closedColorExpr` lines 173-178; `setSoldColorMode` lines 209-220; `addListingGroups` sold-branch lines 315-331; new `ensureXIcon` beside `ensureSquareIcon`)

**Interfaces:**
- Consumes: existing `NOT_CLUSTER`, `IS_CLOSED`, `OP` expressions, `ensureSquareIcon` pattern.
- Produces: per-group layer id `g.id + "-inactive-x"`. No exports change.

- [ ] **Step 1: Drop the grey branch**

Delete lines 173-178 (`INACTIVE_COLOR` and `closedColorExpr` with their comments). In the `-sold` layer paint, replace

```js
        "circle-color": closedColorExpr(g.color), // sold: boligtype colour; inactive/trukket: grey
```

with

```js
        "circle-color": g.color, // sold AND inactive: boligtype colour (inactive adds an X on top)
```

In `setSoldColorMode`, replace `premiumOn ? PREMIUM_COLOR : closedColorExpr(g.color)` with `premiumOn ? PREMIUM_COLOR : g.color`.

- [ ] **Step 2: Add the X icon + layer**

Below `ensureSquareIcon`, add:

```js
// One shared X icon for closed-without-a-sale dots (Inaktiv/Trukket): white
// stroke over a dark outline so it reads on ANY boligtype color. Color-
// neutral, so a single registered image serves every group.
function ensureXIcon(map) {
  const name = "inactive-x";
  if (map.hasImage(name)) return name;
  const size = 16;
  const cvs = document.createElement("canvas");
  cvs.width = size;
  cvs.height = size;
  const ctx = cvs.getContext("2d");
  ctx.lineCap = "round";
  const drawX = () => {
    ctx.beginPath();
    ctx.moveTo(3, 3);
    ctx.lineTo(size - 3, size - 3);
    ctx.moveTo(size - 3, 3);
    ctx.lineTo(3, size - 3);
    ctx.stroke();
  };
  ctx.strokeStyle = "rgba(0,0,0,0.85)";
  ctx.lineWidth = 5;
  drawX();
  ctx.strokeStyle = "#ffffff";
  ctx.lineWidth = 2.5;
  drawX();
  const data = ctx.getImageData(0, 0, size, size);
  map.addImage(name, { width: size, height: size, data: data.data });
  return name;
}
```

In `addListingGroups`, inside `if (g.hasSold) {` directly after the `-sold` `map.addLayer({...})` call (before `clickLayers.push(g.id + "-sold");`), add:

```js
      map.addLayer({
        id: g.id + "-inactive-x",
        type: "symbol",
        source: g.id,
        filter: ["all", NOT_CLUSTER, IS_CLOSED, ["!=", ["get", "sold"], true]],
        layout: {
          "icon-image": ensureXIcon(map),
          "icon-size": 0.75,
          "icon-allow-overlap": true,
        },
        paint: { "icon-opacity": OP },
      });
```

(Not added to `clickLayers` — the `-sold` circle beneath it already handles clicks; listing it twice would double-fire the handler.)

- [ ] **Step 3: Verify in browser** — reload; enable the Inaktiv layer toggle. Inactive/Trukket dots now show boligtype colors with a crisp X; genuine Solgt dots are unchanged (no X, white border). Toggle "Farg solgte etter budpremie" on and off — sold dots swap palettes, X dots keep their boligtype color and X. Drag "Solgt nedtoning" — X fades with its dot. No console errors.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/map.js
git commit -m "feat(map): inactive dots keep boligtype color with X overlay"
```

---

### Task 4: Colored tag quick-chips (map sidebar + table toolbar)

**Files:**
- Modify: `skannonser/web/static/filters.js` (new `tagChipRow` export; `buildFilterPanelUI` Tags block lines 493-499)
- Modify: `skannonser/web/static/table.js` (imports; `refreshVocabs` lines 110-112; `render` lines 406-425)
- Modify: `skannonser/web/static/table.html` (toolbar lines 19-27)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: `assignTagColors`, `colorForTag` (Task 1); existing `vocabs.tags` (`[{key,label,count}]`, `""` key = "(uten tag)"), `filters.tagHidden`.
- Produces: `tagChipRow(parent, { options, hidden, tagColors, onChange })` exported from filters.js; `state.tagColors: Map` on table.js's state (refreshed in `refreshVocabs`) — Task 5's table cells read it.

- [ ] **Step 1: The chip component (filters.js)**

Add to filters.js imports:

```js
import { assignTagColors, colorForTag } from "./tagcolors.js";
```

Add above `buildFilterPanelUI`:

```js
// Colored tag quick-chips over the shared tagHidden set (2026-07-25 spec
// §3). Same storage semantics as the checkbox group: chip visible = key
// absent from `hidden`. options = vocabs.tags; the "" bucket renders as its
// "(uten tag)" label with a neutral color.
export function tagChipRow(parent, { options, hidden, tagColors, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "tag-chip-row";
  options.forEach((opt) => {
    const color = colorForTag(opt.key, tagColors) || "#6f7e76";
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "tag-chip" + (hidden[opt.key] ? " off" : "");
    btn.style.setProperty("--tag-color", color);
    btn.textContent = opt.count != null ? `${opt.label} (${opt.count})` : opt.label;
    btn.addEventListener("click", () => {
      if (hidden[opt.key]) delete hidden[opt.key];
      else hidden[opt.key] = true;
      btn.classList.toggle("off", Boolean(hidden[opt.key]));
      onChange();
    });
    wrap.appendChild(btn);
  });
  parent.appendChild(wrap);
  return wrap;
}
```

In `buildFilterPanelUI`, replace the Tags `selectField` call (lines 493-499):

```js
  selectField(fields, {
    label: "Tags",
    options: vocabs.tags,
    hidden: filters.tagHidden,
    searchable: true,
    onChange,
  });
```

with

```js
  // Tags render as always-visible colored chips, not a select-field: at tag
  // cardinality the chips ARE the better summary, and they double as the
  // one-click filter (2026-07-25 spec §3).
  tagChipRow(fields, {
    options: vocabs.tags,
    hidden: filters.tagHidden,
    tagColors: assignTagColors(vocabs.tags.map((o) => o.key)),
    onChange,
  });
```

- [ ] **Step 2: Chip CSS (style.css, append)**

```css
/* --- tag quick-chips (2026-07-25 UI polish round 2) --- */
.tag-chip-row { display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0; }
.tag-chip {
  border: 1px solid var(--tag-color);
  background: var(--tag-color);
  color: #fff;
  border-radius: 12px;
  padding: 2px 10px;
  font: inherit;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}
.tag-chip.off { background: transparent; color: var(--tag-color); opacity: 0.55; }
```

- [ ] **Step 3: Table toolbar mount**

table.html — insert between the Fasiliteter button and the include-unknown toggle:

```html
      <div id="table-tag-chips"></div>
```

table.js — add to imports: `tagChipRow` from `"./filters.js"` (extend the existing `listingExcluded, deriveVocabs` import) and a new line `import { assignTagColors } from "./tagcolors.js";`

Extend `refreshVocabs`:

```js
function refreshVocabs() {
  state.vocabs = deriveVocabs(state.items);
  state.tagColors = assignTagColors(state.vocabs.tags.map((o) => o.key));
}
```

In `render()`, after `renderHead();` add:

```js
  const chipMount = document.getElementById("table-tag-chips");
  if (chipMount) {
    chipMount.innerHTML = "";
    tagChipRow(chipMount, {
      options: state.vocabs.tags,
      hidden: state.filters.tagHidden,
      tagColors: state.tagColors,
      onChange: onFilterChange,
    });
  }
```

- [ ] **Step 4: Verify in browser** — map sidebar Filtre panel shows chips ("(uten tag)", "definitivt", "hard no", "maybe" with counts) instead of the Tags select-field; clicking "maybe" fades the chip and dims/hides that listing (per nedtoning); table toolbar shows the same chips, chip state syncs map↔table across tabs; the table Tag column popover still works. No console errors.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/filters.js skannonser/web/static/table.js skannonser/web/static/table.html skannonser/web/static/style.css
git commit -m "feat(web): colored tag quick-chips replace Tags select-field, mounted on both pages"
```

---

### Task 5: Tag color in table cells + popup chip

**Files:**
- Modify: `skannonser/web/static/table.js` (`buildRow` kommentar/tag case lines 379-388)
- Modify: `skannonser/web/static/popup.js` (imports; `buildPopupContent` signature + address block lines 51-81)
- Modify: `skannonser/web/static/app.js` (`openPopup` call site line 344)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: `state.tagColors` (table.js, Task 4; app.js, Task 2), `colorForTag` (Task 1).
- Produces: `buildPopupContent(item, destinations, tagColors)` — third parameter, a Map (callers may pass `new Map()` when unavailable).

- [ ] **Step 1: Table tag cell accent**

In table.js, extend the Task-4 tagcolors import to `import { assignTagColors, colorForTag } from "./tagcolors.js";`, then in `buildRow` replace the shared case:

```js
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
```

with

```js
      case "kommentar":
      case "tag": {
        const input = el("input");
        input.type = "text";
        input.value = item[col.key] || "";
        input.className = "cell-edit";
        wireCellEdit(input, item, col.key);
        td.appendChild(input);
        if (col.key === "tag") {
          // Saved-tag accent; a save triggers render() so this repaints.
          const color = colorForTag(item.tag, state.tagColors || new Map());
          if (color) {
            td.style.boxShadow = "inset 3px 0 0 " + color;
            td.style.background = color + "14"; // ~8% alpha tint
          }
        }
        break;
      }
```

- [ ] **Step 2: Popup chip**

popup.js — add to imports:

```js
import { colorForTag } from "./tagcolors.js";
```

Change the signature to `export function buildPopupContent(item, destinations, tagColors)` and, directly after the `if (isNew(item)) addr.appendChild(el("span", "ny-badge", "Ny"));` line, add:

```js
  const tagColor = colorForTag(item.tag, tagColors || new Map());
  if (tagColor) {
    const chip = el("span", "tag-chip-mini", String(item.tag).trim());
    chip.style.background = tagColor;
    addr.appendChild(chip);
  }
```

app.js line 344 — pass the map's color assignment:

```js
  const content = buildPopupContent(item, state.destinations, state.tagColors);
```

- [ ] **Step 3: Chip CSS (style.css, append)**

```css
.sk-popup .tag-chip-mini {
  display: inline-block;
  font-size: 11px;
  font-weight: 600;
  padding: 1px 8px;
  border-radius: 10px;
  color: #fff;
  margin-left: 6px;
}
```

- [ ] **Step 4: Verify in browser** — table: the three tagged rows' Tag cells show a colored left bar + tint matching their map ring; editing a tag then blurring repaints the accent. Map: opening a tagged listing's popup shows the colored chip next to the address; the popup editor still saves. No console errors.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/table.js skannonser/web/static/popup.js skannonser/web/static/app.js skannonser/web/static/style.css
git commit -m "feat(web): tag color accents in table cells and popup chip"
```

---

### Task 6: Popup buttons (Finn / Google Maps / Tabell)

**Files:**
- Modify: `skannonser/web/static/popup.js` (links block lines 143-158)
- Modify: `skannonser/web/static/style.css` (`.sk-popup .links` rules lines 252-254)

**Interfaces:**
- Consumes: nothing new.
- Produces: the popup's "Tabell" link (`/table#finnkode=<id>`) whose receiving end Task 8 implements. Works for DNB rows too — the API ships a `finnkode` field for both sources.

- [ ] **Step 1: Add the Tabell link (popup.js)**

After the Google Maps `links.appendChild(gmap);` block, add:

```js
  const tbl = el("a", null, "Tabell");
  // Same-tab on purpose: Kart -> Tabell is in-app navigation, unlike the
  // external Finn/Maps links.
  tbl.href = "/table#finnkode=" + encodeURIComponent(item.finnkode);
  links.appendChild(tbl);
```

- [ ] **Step 2: Button styling (style.css)**

Replace

```css
.sk-popup .links a { color: var(--accent); text-decoration: none; }
.sk-popup .links a:hover { text-decoration: underline; }
```

with

```css
.sk-popup .links a {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 6px;
  background: var(--accent);
  color: #fff;
  text-decoration: none;
  font-weight: 600;
  font-size: 12px;
}
.sk-popup .links a:hover { background: #0f5a44; }
```

- [ ] **Step 3: Verify in browser** — popups show three green buttons (Finn, Google Maps, Tabell). Finn/Maps still open new tabs; Tabell navigates to `/table#finnkode=...` in the same tab (the table won't scroll to the row yet — that's Task 8; the URL carrying the hash is this task's proof). Screenshot the popup for the record.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/popup.js skannonser/web/static/style.css
git commit -m "feat(popup): button-styled links + Tabell deep-link handoff"
```

---

### Task 7: Missing column filters — Pris/kvm, Solgt for, Budpremie

**Files:**
- Modify: `skannonser/web/static/filterstate.js` (bounds lines 11-18; `defaultFilters` lines 24-53; `activeFilterEntries` lines 151-158)
- Modify: `skannonser/web/static/filters.js` (imports lines 13-23; `listingExcluded` after line 82)
- Modify: `skannonser/web/static/tablefilters.js` (imports lines 7-17; `COLUMN_FILTERS` lines 27-46; `buildBody` slider-max case lines 83-98)

**Interfaces:**
- Consumes: existing `overMax` helper, `premiumPct(item)` from listingmeta.js (returns percent number or null; listingmeta.js imports nothing, so no cycle).
- Produces: filter state keys `prisKvmMax`, `soldPriceMax`, `premiumMax`; exported bounds `PRIS_KVM_MAX = 150_000`, `SOLD_PRICE_MAX = 10_000_000`, `PREMIUM_MAX = 30`; optional `fmtFn` on slider-max descriptors.

- [ ] **Step 1: State + entries (filterstate.js)**

Add to the bounds block:

```js
export const PRIS_KVM_MAX = 150_000;
export const SOLD_PRICE_MAX = 10_000_000;
export const PREMIUM_MAX = 30; // percent over prisantydning
```

In `defaultFilters`, after `maanedskostMax: MAANEDSKOST_MAX,`:

```js
    prisKvmMax: PRIS_KVM_MAX,
    soldPriceMax: SOLD_PRICE_MAX,
    premiumMax: PREMIUM_MAX,
```

(Existing stored blobs lack these keys; `loadFilters`' `{...base, ...stored}` merge fills them — no migration needed.)

In `activeFilterEntries`, after the `totalKvmMax` line:

```js
  maxSlider("prisKvmMax", "Maks pris/kvm", PRIS_KVM_MAX, kr);
  maxSlider("soldPriceMax", "Maks solgt-pris", SOLD_PRICE_MAX, kr);
  maxSlider("premiumMax", "Maks budpremie", PREMIUM_MAX, (v) => "≤ +" + v + " %");
```

- [ ] **Step 2: Predicate (filters.js)**

Extend the filterstate import list with `PRIS_KVM_MAX, SOLD_PRICE_MAX, PREMIUM_MAX` and add a new import line:

```js
import { premiumPct } from "./listingmeta.js";
```

In `listingExcluded`, after the `maanedskost` line (82):

```js
  if (overMax(item.pris_kvm, f.prisKvmMax, PRIS_KVM_MAX)) return true;
  // Sold-outcome filters apply ONLY to sold items -- actives structurally
  // lack these fields, and must never be swept out by includeUnknown=false.
  if (item.sold) {
    if (overMax(item.sold_price, f.soldPriceMax, SOLD_PRICE_MAX)) return true;
    if ((f.premiumMax ?? PREMIUM_MAX) < PREMIUM_MAX) {
      const pct = premiumPct(item);
      if (pct == null) {
        if (unknownFails) return true;
      } else if (pct > f.premiumMax) {
        return true;
      }
    }
  }
```

- [ ] **Step 3: Column popovers (tablefilters.js)**

Extend the filterstate import list with `PRIS_KVM_MAX, SOLD_PRICE_MAX, PREMIUM_MAX`. Add to `COLUMN_FILTERS` after the `pris` entry:

```js
  pris_kvm: { kind: "slider-max", stateKey: "prisKvmMax", bound: () => PRIS_KVM_MAX, step: 1000, fmt: "kr" },
  sold_price: { kind: "slider-max", stateKey: "soldPriceMax", bound: () => SOLD_PRICE_MAX, step: 100000, fmt: "kr" },
  premium: {
    kind: "slider-max", stateKey: "premiumMax", bound: () => PREMIUM_MAX, step: 1,
    fmtFn: (bound) => (v) => (v >= bound ? "Av" : "≤ +" + v + " %"),
  },
```

In `buildBody`'s `"slider-max"` case, change `fmt: fmtKr(bound),` to:

```js
        fmt: desc.fmtFn ? desc.fmtFn(bound) : fmtKr(bound),
```

- [ ] **Step 4: Verify in browser** — table: Pris/kvm header now has a funnel; dragging it below some rows' values hides them AND (switch tabs) dims them on the map; the sidebar's active-filter line shows "Maks pris/kvm" with a working clear ×. With "Vis solgte" on, Solgt for and Budpremie funnels filter sold rows without touching actives; "Nullstill filtre" clears all three. Set "Inkluder ukjent verdi" off with only a premium filter active — active listings must remain visible.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/filterstate.js skannonser/web/static/filters.js skannonser/web/static/tablefilters.js
git commit -m "feat(filters): pris/kvm, solgt-pris and budpremie filters across map+table"
```

---

### Task 8: Column picker with trimmed defaults

**Files:**
- Modify: `skannonser/web/static/table.html` (toolbar lines 19-27)
- Modify: `skannonser/web/static/table.js` (imports; constants near `COLUMNS`; `renderHead` line 224; `buildRow` line 317; `wireToolbar`)

**Interfaces:**
- Consumes: `openPopover` from filters.js (add to table.js's imports — it is already exported there).
- Produces: `state.hiddenColumns: Set<string>`, `visibleColumns() -> array` (Task 9's row rendering inherits it transparently); `hiddenColumns: string[]` field inside the `skannonser.ui.v1` localStorage blob.

- [ ] **Step 1: Toolbar button (table.html)** — after the Fasiliteter button line, add:

```html
      <button type="button" id="table-columns-btn" class="toolbar-filter-btn">Kolonner</button>
```

- [ ] **Step 2: State + persistence (table.js)**

Add `openPopover` to the filters.js import (alongside `listingExcluded, deriveVocabs, tagChipRow`). Below the `COLUMNS` array add:

```js
// Column picker (2026-07-25 spec §7): first-run default hides the noise
// columns (Pris/Felleskost are semi-redundant with Totalpris/Mnd-kost).
// Adresse and Kart are load-bearing (identity + map handoff) -- not hideable.
const DEFAULT_HIDDEN_COLUMNS = ["postnummer", "pris", "felleskost_mnd", "soverom", "etasje"];
const ALWAYS_VISIBLE_COLUMNS = new Set(["adresse", "kart"]);

function loadHiddenColumns() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const stored = raw ? JSON.parse(raw).hiddenColumns : null;
    return new Set(Array.isArray(stored) ? stored : DEFAULT_HIDDEN_COLUMNS);
  } catch (_) {
    return new Set(DEFAULT_HIDDEN_COLUMNS);
  }
}

function saveHiddenColumns() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const blob = raw ? JSON.parse(raw) : {};
    blob.hiddenColumns = [...state.hiddenColumns];
    localStorage.setItem(STORAGE_KEY, JSON.stringify(blob));
  } catch (_) {
    /* storage may be unavailable; non-fatal */
  }
}

function visibleColumns() {
  return COLUMNS.filter((c) => !state.hiddenColumns.has(c.key));
}
```

Add `hiddenColumns: loadHiddenColumns(),` to the `state` object literal.

- [ ] **Step 3: Render only visible columns** — in `renderHead` change `COLUMNS.forEach((col) => {` to `visibleColumns().forEach((col) => {`; in `buildRow` change `COLUMNS.forEach((col) => {` to `visibleColumns().forEach((col) => {`.

- [ ] **Step 4: The picker popover** — in `wireToolbar`, after the facilities-button block:

```js
  const colsBtn = document.getElementById("table-columns-btn");
  if (colsBtn) {
    colsBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openPopover(colsBtn, (pop) => {
        const wrap = el("div", "filter-row checkbox-group");
        wrap.appendChild(el("div", "filter-head", "Vis kolonner"));
        COLUMNS.filter((c) => !ALWAYS_VISIBLE_COLUMNS.has(c.key)).forEach((col) => {
          const row = el("label", "toggle");
          const cb = el("input");
          cb.type = "checkbox";
          cb.checked = !state.hiddenColumns.has(col.key);
          cb.addEventListener("change", () => {
            if (cb.checked) state.hiddenColumns.delete(col.key);
            else state.hiddenColumns.add(col.key);
            saveHiddenColumns();
            render();
          });
          row.appendChild(cb);
          row.appendChild(document.createTextNode(col.label));
          wrap.appendChild(row);
        });
        pop.appendChild(wrap);
      });
    });
  }
```

(A hidden column's filter needs no special handling: `activeFilterCount` and `listingExcluded` are state-based, so the toolbar's "N filtre aktive" keeps counting it — the spec's no-invisible-filtering rule.)

- [ ] **Step 5: Verify in browser** — clear the site's localStorage, reload `/table`: Postnummer, Pris, Felleskost, Sov, Etg are gone; "Kolonner" popover lists all columns except Adresse/Kart with correct checkmarks; re-enabling Pris restores it in the right position; the choice survives reload; with a Pris/kvm filter active and the column then hidden, the status line still reports the active filter.

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/table.html skannonser/web/static/table.js
git commit -m "feat(table): column picker with trimmed defaults, persisted in ui blob"
```

---

### Task 9: Table receives the deep link

**Files:**
- Modify: `skannonser/web/static/table.js` (state; `visibleRows` lines 204-212; `buildRow` line 315; new `handleHash`; `init` lines 484-525)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:**
- Consumes: the `/table#finnkode=<id>` links from Task 6; existing `fetchListings(1)`, `saveSoldPref`, `refreshVocabs`.
- Produces: `state.focusFinnkode: string|null` — the one-row filter exemption.

- [ ] **Step 1: Focus state + exemption**

Add `focusFinnkode: null,` to the `state` object literal. In `visibleRows`, make the deep-linked row immune to filters (spec §8 — a deep link must never land on an empty-looking table):

```js
  const filtered = state.items.filter((item) => {
    if (state.focusFinnkode && String(item.finnkode) === state.focusFinnkode) return true;
    if (!state.showSold && item.closed) return false;
    if (listingExcluded(item, state.filters, state.meta)) return false;
    return matchesFilter(item, state.filterText);
  });
```

In `buildRow`, after the `const tr = el("tr", ...)` line, add:

```js
  tr.dataset.finnkode = item.finnkode;
```

- [ ] **Step 2: The hash handler**

Add above `init()`:

```js
// Receiving end of the popup's "Tabell" deep link -- mirror of app.js's
// handleHash. The focused row bypasses filters (a deep link onto an
// empty-looking table reads as broken) and gets a flash so the eye lands.
async function handleHash() {
  const raw = decodeURIComponent(window.location.hash.replace(/^#/, ""));
  state.focusFinnkode = null;
  if (!raw) {
    render();
    return;
  }
  const finnkode = raw.startsWith("finnkode=") ? raw.slice("finnkode=".length) : raw;
  const byId = () => state.items.find((it) => String(it.finnkode) === finnkode);
  let item = byId();
  // Deep links to closed listings can arrive before the lazily-fetched
  // sold bucket on a cold load -- pull it and retry (same race app.js solves).
  if (!item && !state.soldLoaded) {
    setStatus("Laster solgte …");
    try {
      state.items = state.items.concat(await fetchListings(1));
      state.soldLoaded = true;
      refreshVocabs();
    } catch (_) {
      /* fall through; not-found reported below */
    }
    item = byId();
  }
  if (!item) {
    render();
    setStatus("Fant ikke annonse " + finnkode);
    return;
  }
  if (item.closed && !state.showSold) {
    state.showSold = true;
    const soldToggle = document.getElementById("table-sold");
    if (soldToggle) soldToggle.checked = true;
    saveSoldPref(true);
  }
  state.focusFinnkode = finnkode;
  render();
  const row = document.querySelector('tr[data-finnkode="' + finnkode + '"]');
  if (row) {
    row.scrollIntoView({ block: "center" });
    row.classList.add("row-flash");
    setTimeout(() => row.classList.remove("row-flash"), 2400);
  }
}
```

In `init()`, replace the final `render();` line with:

```js
  render();
  if (window.location.hash) await handleHash();
  window.addEventListener("hashchange", handleHash);
```

- [ ] **Step 3: Flash CSS (style.css, append)**

```css
/* Deep-linked row (map popup "Tabell" handoff): one attention flash. */
@keyframes row-flash {
  0% { background: #ffe9a8; }
  100% { background: transparent; }
}
#listings-table tr.row-flash td { animation: row-flash 2.4s ease-out; }
```

- [ ] **Step 4: Verify in browser** — (a) map popup → Tabell: lands on `/table#finnkode=...`, row centered + flashed; (b) sold listing from a cold table load (clear sessionStorage/reload first): sold bucket loads, Vis-solgte flips on, row appears; (c) set a Maks-pris filter that excludes the target, follow the link: the row still shows while other excluded rows stay hidden; clearing the hash (navigate to `/table#`) drops the exemption; (d) `/table#finnkode=999999999`: status shows "Fant ikke annonse 999999999"; (e) the table→map "Kart" link still works in the other direction.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/table.js skannonser/web/static/style.css
git commit -m "feat(table): receive map deep links with filter-exempt focused row"
```

---

### Task 10: Full verification pass

**Files:** none created — this is the spec's acceptance run plus regression checks.

- [ ] **Step 1: Unit + Python suites**

Run: `node --test tests/web/*.test.mjs` → `# fail 0`.
Run: `.venv/bin/pytest -q` → all pass (backend untouched; any failure here means a task strayed out of scope — stop and fix).

- [ ] **Step 2: Spec acceptance walk-through** (dev server from Task 2's setup) — run the spec's Testing list end-to-end:

1. Three tagged listings → three distinct stable ring colors; chips on both pages; chip toggling syncs cross-tab live.
2. Inactive X dots keep boligtype color; nedtoning fades them; budpremie mode unaffected.
3. Pris/kvm filter works from the column funnel and dims dots on the map; column picker hides/shows, survives reload, defaults correct with cleared localStorage.
4. Deep link works cold (sold listing) and against an excluding filter; unknown finnkode reports cleanly.
5. Popup shows button-styled Finn / Google Maps / Tabell.

- [ ] **Step 3: Console + mobile sweep** — `read_console_messages` clean on both pages after the walk-through; resize to mobile (375px): chip rows wrap sanely in the drawer and toolbar, popup buttons fit the 86vw popup.

- [ ] **Step 4: Update the spec status line** — change `**Status:** Approved (design), not yet implemented` to `**Status:** Implemented 2026-07-25` in `docs/superpowers/specs/2026-07-25-ui-polish-round-2-design.md`.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-07-25-ui-polish-round-2-design.md
git commit -m "docs: mark UI polish round 2 spec implemented"
```
