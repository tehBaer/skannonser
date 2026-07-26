# Filter selection semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert every value-list filter from "which values are hidden" to "which values do I want to see", with one chip component serving all seven.

**Architecture:** Six `*Hidden` keys become `*Selected` arrays, joining the two that already work that way. One shared selection helper replaces the hidden-set helper in the predicate both pages use. The popover select-field is retired from the sidebar in favour of chip rows; the table's column-header funnels keep their popovers but flip their semantics. Old filter state is dropped on load, not migrated.

**Tech Stack:** Plain ES modules, no build step. MapLibre GL. Tests are `node --test tests/web/*.test.mjs`. Python backend untouched.

**Spec:** `docs/superpowers/specs/2026-07-26-filter-selection-semantics-design.md`

## Global Constraints

- **Zero backend changes.** No edits under `skannonser/` outside `skannonser/web/static/`. No API shape change, no migrations.
- No external CDNs, no new dependencies, no build step — plain ES modules only.
- UI copy is Norwegian (bokmål): "Alle", "Tøm", and existing labels ("Boligtype", "Eieform", "Energimerking", "Tilgjengelighet", "Tags", "Linjer").
- localStorage stays inside the one `skannonser.ui.v1` blob (read-modify-write, never overwrite the whole blob).
- Comment style: comments state constraints/WHY, not what the next line does.
- **Test command is `node --test tests/web/*.test.mjs`** — the directory form is broken on node v25.
- The Python suite must stay green: `.venv/bin/pytest -q` → 659 passed.

## Explicit non-goals

- Sidebar information architecture (segmentation, collapse defaults, transparency controls, quieter legend). Deferred by the owner until the chips are real. This change makes the sidebar taller; that is expected.
- Tag autocomplete, moving comments/tags left in the table, nudging co-located dots, and the "inkluder ukjent verdi" question. All separately scoped.
- `facilitiesRequired` keeps required-AND semantics and is not touched.

## Dev-server setup

Reuse the existing scratch database and launch entry — already migrated and seeded.

```bash
ls /private/tmp/claude-501/-Users-tehbaer-kode-skannonser/f9854acb-68d2-44c1-a28d-6fcf9b3c1377/scratchpad/ui-polish.db
```

`.claude/launch.json` has `skannonser-web-review` on 127.0.0.1:8377. Start with `preview_start {name: "skannonser-web-review"}` — **never via Bash**.

**Two harness quirks that will cost you time:**

1. **The browser pane caches aggressively.** After editing a file under `web/static/`, refresh each changed module with `fetch(url, {cache: "reload"})` from page context, then reload. For the HTML document itself, refresh `"/"` — refreshing `"/index.html"` does not refresh the page served at `/`.
2. **The map canvas does not render** in this environment: MapLibre's `load` never fires, so the page stays at "Laster …". The **sidebar and the table page render fine** and must be verified live. Anything drawn on the canvas is verified by tests and code reading only.

Seeded tags: `461666906` "maybe", `465492220` "definitivt", `282785835` "hard no". Note the scratch DB has **no** eieform or energimerke values — production has 3 and 7 — so those two chip rows will legitimately be empty and are the case for the empty-list message.

---

### Task 1: Selection semantics in the predicate and state

**Files:**
- Modify: `skannonser/web/static/filters.js` (`hiddenSetExcludes` → a selection helper; the six call sites in `listingExcluded`)
- Modify: `skannonser/web/static/filterstate.js` (`defaultFilters`; `activeFilterEntries`'s `hiddenSet` helper; a migration in the load path)
- Test: `tests/web/selection.test.mjs` (create)

**Interfaces:**
- Consumes: the existing `selectedSetExcludes(selected, raw, unknownFails)` in filters.js, which stays as-is for postnummer/nabolag/eieform/energimerking.
- Produces: `selectionExcludes(selected, value)` exported from filters.js — the explicit-value variant. Filter keys `boligtypeSelected`, `eieformSelected`, `energiSelected`, `tilgjengelighetSelected`, `tagSelected` (all arrays). Tasks 2-5 consume these names.

**Why two helpers.** `selectedSetExcludes` treats a null/empty value as *unknown*, governed by `includeUnknown` — right for postnummer, nabolag, eieform and energimerking, where a missing value means "we don't know". But boligtype, tag and tilgjengelighet render `""` as an explicit, selectable bucket ("Ukjent boligtype", "(uten tag)", "Ingen status"), so for those `""` is a value like any other and must not be routed through `includeUnknown`.

- [ ] **Step 1: Write the failing test**

Create `tests/web/selection.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { selectionExcludes, listingExcluded } from "../../skannonser/web/static/filters.js";
import { defaultFilters, activeFilterEntries } from "../../skannonser/web/static/filterstate.js";

test("an empty selection excludes nothing", () => {
  assert.equal(selectionExcludes([], "Leilighet"), false);
  assert.equal(selectionExcludes(undefined, "Leilighet"), false);
});

test("a non-empty selection excludes everything not in it", () => {
  assert.equal(selectionExcludes(["Leilighet"], "Leilighet"), false);
  assert.equal(selectionExcludes(["Leilighet"], "Enebolig"), true);
});

test('"" is a real selectable value, not "unknown"', () => {
  assert.equal(selectionExcludes([""], ""), false, "selecting the empty bucket keeps it");
  assert.equal(selectionExcludes([""], "maybe"), true);
  assert.equal(selectionExcludes(["maybe"], ""), true, "untagged is excluded when a tag is selected");
});

test("the predicate routes the explicit-value filters through selection", () => {
  const base = defaultFilters({ destinations: [] });
  const meta = {};
  const mk = (over) => ({ ...base, ...over });

  const leilighet = { boligtype: "Leilighet", tag: "maybe" };
  const enebolig = { boligtype: "Enebolig", tag: null };

  assert.equal(listingExcluded(leilighet, mk({}), meta), false, "no selection = everything passes");
  assert.equal(listingExcluded(enebolig, mk({ boligtypeSelected: ["Leilighet"] }), meta), true);
  assert.equal(listingExcluded(leilighet, mk({ boligtypeSelected: ["Leilighet"] }), meta), false);
  assert.equal(listingExcluded(enebolig, mk({ tagSelected: [""] }), meta), false, "untagged selected");
  assert.equal(listingExcluded(leilighet, mk({ tagSelected: [""] }), meta), true);
});

test("a non-empty selection counts as an active filter and clears back to empty", () => {
  const base = defaultFilters({ destinations: [] });
  base.tagSelected = ["maybe", "hard no"];
  const entries = activeFilterEntries(base, {});
  const tag = entries.find((e) => e.key === "tagSelected");
  assert.ok(tag, "a selection must appear in the active-filter list");
  assert.match(tag.valueText, /2/);
  tag.clear(base);
  assert.deepEqual(base.tagSelected, []);
});

test("defaultFilters ships the six selections empty and no *Hidden keys", () => {
  const f = defaultFilters({ destinations: [] });
  ["boligtypeSelected", "eieformSelected", "energiSelected",
   "tilgjengelighetSelected", "tagSelected"].forEach((k) => {
    assert.deepEqual(f[k], [], k + " starts empty");
  });
  Object.keys(f).forEach((k) => {
    assert.ok(!/Hidden$/.test(k), "no hidden-set key survives: " + k);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `node --test tests/web/selection.test.mjs`
Expected: FAIL — `selectionExcludes` is not exported.

- [ ] **Step 3: Add the selection helper**

In `filters.js`, beside `selectedSetExcludes`, add:

```js
// Selection over values the UI renders EXPLICITLY, including the "" bucket
// ("Ukjent boligtype" / "(uten tag)" / "Ingen status"). Distinct from
// selectedSetExcludes, which treats a missing value as *unknown* and defers to
// `includeUnknown`: here "" is a value the user can pick like any other, so
// routing it through the unknown policy would make the empty bucket
// unselectable. Empty selection = filter off.
export function selectionExcludes(selected, value) {
  if (!selected || !selected.length) return false;
  return !selected.includes(value);
}
```

- [ ] **Step 4: Convert the six predicate call sites**

In `listingExcluded`, replace:

```js
  // Hidden sets with explicit "" buckets.
  if (hiddenSetExcludes(f.boligtypeHidden, item.boligtype || "")) return true;
  if (hiddenSetExcludes(f.tagHidden, item.tag ? String(item.tag).trim() : "")) return true;
  if (hiddenSetExcludes(f.tilgjengelighetHidden, item.tilgjengelighet || "")) return true;
```

with:

```js
  // Selections over explicitly-rendered values, "" bucket included.
  if (selectionExcludes(f.boligtypeSelected, item.boligtype || "")) return true;
  if (selectionExcludes(f.tagSelected, item.tag ? String(item.tag).trim() : "")) return true;
  if (selectionExcludes(f.tilgjengelighetSelected, item.tilgjengelighet || "")) return true;
```

Then replace the two `Object.keys(...).length` blocks for `energiHidden` and `eieformHidden` with the existing unknown-aware helper, since a missing energy grade or ownership form genuinely means "we don't know":

```js
  if (selectedSetExcludes(f.energiSelected, item.energimerke, unknownFails)) return true;
  if (selectedSetExcludes(f.eieformSelected, item.eieform, unknownFails)) return true;
```

Delete `hiddenSetExcludes` once nothing calls it. Confirm with `grep -rn "hiddenSetExcludes" skannonser/web/static/`.

- [ ] **Step 5: Convert the defaults**

In `filterstate.js`'s `defaultFilters`, replace the five `*Hidden: {}` entries with:

```js
    boligtypeSelected: [],
    eieformSelected: [],
    energiSelected: [],
    tilgjengelighetSelected: [],
    tagSelected: [],
```

- [ ] **Step 6: Convert the active-filter entries**

In `activeFilterEntries`, delete the `hiddenSet` helper and its five calls, and add the five keys to the existing `selectedSet` helper instead:

```js
  selectedSet("boligtypeSelected", "Boligtype");
  selectedSet("eieformSelected", "Eieform");
  selectedSet("energiSelected", "Energimerking");
  selectedSet("tilgjengelighetSelected", "Tilgjengelighet");
  selectedSet("tagSelected", "Tag");
  selectedSet("postnummerSelected", "Postnummer");
  selectedSet("nabolagSelected", "Nabolag");
```

- [ ] **Step 7: Drop the old keys on load**

`loadFilters` merges stored values over the defaults. Add the removal immediately after that merge, so a stale key can never be re-persisted:

```js
  // The 2026-07-26 conversion from hidden-sets to selections. Inverting a
  // hidden set needs the COMPLETE value list to select everything else, and for
  // tags and tilgjengelighet that list is not known until listings load --
  // acting on a partial vocabulary is exactly what silently destroyed saved
  // filters before. So the six converted filters reset once; nothing else in
  // the blob is touched.
  ["boligtypeHidden", "eieformHidden", "energiHidden",
   "tilgjengelighetHidden", "tagHidden"].forEach((k) => delete merged[k]);
```

Adapt `merged` to whatever the function already calls the merged object.

- [ ] **Step 8: Run the tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`. The count rises by 6 from the current 35, to 41. Existing tests that reference `*Hidden` keys must be updated to the new names rather than deleted — they are pinning real behaviour.

- [ ] **Step 9: Commit**

```bash
git add skannonser/web/static/filters.js skannonser/web/static/filterstate.js tests/web/selection.test.mjs
git commit -m "feat(filters): value filters select rather than hide"
```

---

### Task 2: The selection chip row component

**Files:**
- Modify: `skannonser/web/static/filters.js` (generalise `tagChipRow` into `selectionChipRow`)
- Modify: `skannonser/web/static/style.css` (append)
- Test: `tests/web/chiprow.test.mjs` (create)

**Interfaces:**
- Consumes: `selectionExcludes` semantics from Task 1 (empty = all).
- Produces: `selectionChipRow(parent, { label, options, selected, colorFor, onChange })` exported from filters.js.
  - `options`: `[{ key, label, count }]`
  - `selected`: the array being mutated in place
  - `colorFor`: optional `(key) => color|null`; when it returns null the chip uses a neutral style
  - Returns the wrapper element.

**On "Ingen" — a design correction made while writing this plan.** The spec said lines keep "Alle / Ingen". Under selection semantics that is not representable: an empty selection means *everything shows*, so there is no array value that means "show no lines". Worse, it is redundant — "Vis stasjoner" already hides every station with one click, which is exactly what "Ingen" did.

So every row gets the same two controls, and hiding all stations stays where it already works. If the owner wants a real "show nothing" state for lines, that is a different feature (a visibility flag, not a selection) and belongs in its own change.

**Why one component.** Seven controls, one behaviour. `tagChipRow` already does most of this over a hidden-set; this generalises it to a selection and adds the label, bulk controls and empty-list message the spec requires.

- [ ] **Step 1: Write the failing test**

Create `tests/web/chiprow.test.mjs`. The component is DOM code, so the test covers the pure selection logic it applies, exported for the purpose:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { applyChipClick } from "../../skannonser/web/static/filters.js";

test("the first click isolates", () => {
  assert.deepEqual(applyChipClick([], "maybe", ["", "maybe", "hard no"]), ["maybe"]);
});

test("a further click adds", () => {
  assert.deepEqual(applyChipClick(["maybe"], "hard no", ["", "maybe", "hard no"]),
    ["maybe", "hard no"]);
});

test("clicking a selected chip removes it", () => {
  assert.deepEqual(applyChipClick(["maybe", "hard no"], "maybe", ["", "maybe", "hard no"]),
    ["hard no"]);
});

test("removing the last selection returns to everything", () => {
  assert.deepEqual(applyChipClick(["maybe"], "maybe", ["", "maybe", "hard no"]), []);
});

test("the empty bucket behaves like any other value", () => {
  assert.deepEqual(applyChipClick([], "", ["", "maybe"]), [""]);
  assert.deepEqual(applyChipClick([""], "", ["", "maybe"]), []);
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `node --test tests/web/chiprow.test.mjs`
Expected: FAIL — `applyChipClick` is not exported.

- [ ] **Step 3: Add the click rule**

In `filters.js`, above the chip row:

```js
// The one interaction rule: a chip toggles, EXCEPT that the first selection
// isolates. Returns the new selection; never mutates its input. `allKeys` is
// unused by the rule itself but pins the caller's vocabulary at click time so
// a future "select all" can share this function.
export function applyChipClick(selected, key, allKeys) {
  const current = selected || [];
  if (!current.length) return [key];
  if (current.includes(key)) return current.filter((k) => k !== key);
  return current.concat([key]);
}
```

- [ ] **Step 4: Generalise the chip row**

Replace `tagChipRow` with `selectionChipRow`. Keep the existing chip markup and `--tag-color` custom property so the current CSS keeps working; add the heading, the bulk row, and the empty-list message:

```js
// One selection control for every value list: tags, boligtype, eieform,
// energimerking, tilgjengelighet, and the station lines. Selected chips are
// FILLED and unselected ones outlined, so state reads without relying on the
// per-value colour -- tags and lines carry their own colours and cannot also
// use colour to mean "on".
export function selectionChipRow(parent, { label, options, selected, colorFor, onChange }) {
  const wrap = document.createElement("div");
  wrap.className = "chip-row-block";

  const head = document.createElement("div");
  head.className = "filter-head chip-row-head";
  const name = document.createElement("span");
  name.textContent = label;
  head.appendChild(name);

  const bulkWrap = document.createElement("span");
  bulkWrap.className = "chip-bulk";
  const mkBulk = (text, fn) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "linkish";
    b.textContent = text;
    b.addEventListener("click", () => {
      fn();
      onChange();
    });
    bulkWrap.appendChild(b);
  };
  // Both controls reach the same resting state -- an empty selection shows
  // everything -- but they read differently to a user mid-filter, so both are
  // offered. "Alle" is the answer to "show me everything again"; "Tøm" is the
  // answer to "undo my picks".
  mkBulk("Alle", () => selected.splice(0, selected.length));
  mkBulk("Tøm", () => selected.splice(0, selected.length));
  head.appendChild(bulkWrap);
  wrap.appendChild(head);

  if (!options.length) {
    const empty = document.createElement("div");
    empty.className = "chip-row-empty muted";
    empty.textContent = "Ingen verdier";
    wrap.appendChild(empty);
    parent.appendChild(wrap);
    return wrap;
  }

  const row = document.createElement("div");
  row.className = "tag-chip-row";
  const allKeys = options.map((o) => o.key);
  options.forEach((opt) => {
    const btn = document.createElement("button");
    btn.type = "button";
    const color = colorFor ? colorFor(opt.key) : null;
    btn.style.setProperty("--tag-color", color || "#6f7e76");
    const paint = () => {
      const on = selected.includes(opt.key);
      btn.className = "tag-chip" + (on ? "" : " off") + (opt.key === "" ? " untagged" : "");
      btn.setAttribute("aria-pressed", String(on));
    };
    btn.textContent = opt.count != null ? `${opt.label} (${opt.count})` : opt.label;
    btn.addEventListener("click", () => {
      const next = applyChipClick(selected, opt.key, allKeys);
      selected.splice(0, selected.length, ...next);
      [...row.querySelectorAll(".tag-chip")].forEach((el, i) => {
        const on = selected.includes(allKeys[i]);
        el.classList.toggle("off", !on);
        el.setAttribute("aria-pressed", String(on));
      });
      onChange();
    });
    paint();
    row.appendChild(btn);
  });
  wrap.appendChild(row);
  parent.appendChild(wrap);
  return wrap;
}
```

- [ ] **Step 5: Style the additions**

Append to `style.css`:

```css
/* Selection chip rows (2026-07-26). Selected chips are filled, unselected
   outlined -- state must not depend on the per-value colour, since tags and
   lines already use colour to mean *which* value. */
.chip-row-block { margin: 10px 0; }
.chip-bulk { display: flex; gap: 8px; }
.chip-row-empty { font-size: 12px; padding: 2px 0; }
```

- [ ] **Step 6: Run the tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`, 46 tests.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/filters.js skannonser/web/static/style.css tests/web/chiprow.test.mjs
git commit -m "feat(filters): one selection chip row for every value list"
```

---

### Task 3: Map sidebar uses chip rows for all six

**Files:**
- Modify: `skannonser/web/static/filters.js` (`buildFilterPanelUI`; delete `selectField` and its popover helpers once unused)

**Interfaces:**
- Consumes: `selectionChipRow` (Task 2), the `*Selected` keys (Task 1), `assignTagColors`/`colorForTag` from tagcolors.js, the existing `vocabs` and `meta` shapes.
- Produces: no new exports. `selectField` is removed.

- [ ] **Step 1: Replace the four select-fields and the tag chip row**

In `buildFilterPanelUI`, replace the `selectField` calls for Boligtype, Eieform, Energimerking and Tilgjengelighet, and the existing `tagChipRow` call, with six `selectionChipRow` calls. Boligtype keeps its per-value swatch colour from `colorByType`; tags keep theirs from `assignTagColors`; the other four pass no `colorFor`.

Each needs its options in the shape `[{ key, label, count }]`. Boligtype, eieform and energimerking come from `meta` (`boligtyper`, `eieformer`, `energimerker`) and have no counts — map them to `{ key: v, label: v }`. Boligtype additionally keeps its explicit `""` entry labelled "Ukjent boligtype", as the select-field had. Tilgjengelighet and tags come from `vocabs` and already carry counts and labels.

- [ ] **Step 2: Delete the dead component**

Remove `selectField`. If `openPopover`/`closePopover` and the module-level dismiss listeners are now unused by this file, check before deleting: `tablefilters.js` uses the popover for column headers, and `table.js` uses it for the column picker. Run `grep -rn "openPopover\|closePopover\|selectField" skannonser/web/static/` and delete only what is genuinely unreferenced.

- [ ] **Step 3: Verify in the browser**

Start the preview server. In the map sidebar, all six filters render as chip rows with headings and "Alle"/"Tøm". Click a boligtype chip: only that type stays visible in the count line and the others' chips go outlined. Click a second: both. Click one of them again: it drops out. Click the last: everything returns. "Tøm" clears. Eieform and Energimerking show "Ingen verdier" (the scratch database has none). Zero console errors.

- [ ] **Step 4: Commit**

```bash
git add skannonser/web/static/filters.js
git commit -m "feat(map): every sidebar value filter is a selection chip row"
```

---

### Task 4: Station lines select rather than hide

**Files:**
- Modify: `skannonser/web/static/stations.js` (`visibleLineSet`; the `lineHidden` doc comment)
- Modify: `skannonser/web/static/app.js` (default UI state; the line-pill rendering in `wireStationControls`)

**Interfaces:**
- Consumes: `selectionChipRow` (Task 2), `lineColor` from stations.js.
- Produces: `ui.stations.lineSelected` (array) replacing `ui.stations.lineHidden` (object).

- [ ] **Step 1: Convert the visibility set**

In `stations.js`, replace `visibleLineSet`:

```js
// Which lines are visible: an EMPTY selection means all of them, matching every
// other value filter. Previously an inverted hidden-map.
export function visibleLineSet(ui) {
  const all = (ui._allLines || []);
  const selected = (ui.stations && ui.stations.lineSelected) || [];
  return new Set(selected.length ? selected : all);
}
```

- [ ] **Step 2: Convert the default and drop the old key**

In `app.js`'s default UI state, replace `lineHidden: {}` with `lineSelected: []`. In the UI load path, delete `stations.lineHidden` from the merged object, with a comment pointing at the same 2026-07-26 conversion as Task 1's filter migration.

- [ ] **Step 3: Render the lines with the shared component**

Replace the hand-rolled `.line-chip` loop in `wireStationControls` with a `selectionChipRow` call: label "Linjer", options `state.ui._allLines.map((l) => ({ key: l, label: l }))`, `selected: st.lineSelected`, `colorFor: lineColor`, and an `onChange` that saves and calls `applyAll`.

Delete the now-unused `.line-chip` CSS and the bespoke `#lines-all`/`#lines-none` markup and handlers from `index.html` and `app.js`, since the component provides its own bulk controls. Confirm nothing else references those ids.

- [ ] **Step 4: Verify in the browser**

The Linjer row still wraps to about three rows and behaves exactly as the other chip rows do. Selecting one line leaves only that line's stations on the map; selecting a second adds it; clearing returns all. Confirm that unchecking "Vis stasjoner" still hides every station, since that is now the only route to a station-free map.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/stations.js skannonser/web/static/app.js skannonser/web/static/index.html skannonser/web/static/style.css
git commit -m "feat(stations): line filter selects rather than hides"
```

---

### Task 5: Table column filters follow the same semantics

**Files:**
- Modify: `skannonser/web/static/tablefilters.js` (`COLUMN_FILTERS` entries; the `"set"` kind's body)
- Modify: `skannonser/web/static/table.js` (the tag chip row call site)

**Interfaces:**
- Consumes: the `*Selected` keys (Task 1), `selectionChipRow` (Task 2).
- Produces: no new exports. The `COLUMN_FILTERS` kind `"set"` is renamed `"selection"`.

- [ ] **Step 1: Point the five column filters at the new keys**

In `COLUMN_FILTERS`, change the five `stateKey` values to `boligtypeSelected`, `eieformSelected`, `energiSelected`, `tilgjengelighetSelected`, `tagSelected`, and rename `kind: "set"` to `kind: "selection"` on each.

- [ ] **Step 2: Convert the popover body**

In the builder for that kind, the checkbox list currently means "checked = visible" over a hidden-set. Change it so checked means *selected*, writing into the array: checking adds the value, unchecking removes it, and an empty array means the filter is off. Keep the column's active-state indicator working — a column is active exactly when its selection is non-empty.

Note this popover keeps the plain checkbox list rather than chips: it lives in a column header where the funnel pattern is right, and the spec deliberately scopes chips to the sidebar.

- [ ] **Step 3: Update the table's tag chips**

`table.js` calls the old `tagChipRow`. Point it at `selectionChipRow` with `selected: state.filters.tagSelected`, `colorFor` from `state.tagColors`, and no `label` heading — the toolbar has no room for one, and the chips sit beside already-labelled buttons.

- [ ] **Step 4: Verify in the browser**

On `/table`: the toolbar chips isolate and add as on the map. A column funnel opens with checkboxes; checking one narrows the rows to that value, checking a second widens to both, unchecking all restores everything. The header shows the column as filtered exactly while a selection exists. A filter set on the map is still in effect when you switch to the table, and vice versa. Zero console errors.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/tablefilters.js skannonser/web/static/table.js
git commit -m "feat(table): column filters and toolbar chips select rather than hide"
```

---

### Task 6: Verification pass

**Files:** none created.

- [ ] **Step 1: Both suites**

Run: `node --test tests/web/*.test.mjs` → `# fail 0`.
Run: `.venv/bin/pytest -q` → `659 passed`. A failure here means a task strayed outside `web/static/` — stop and fix.

- [ ] **Step 2: Migration check**

In the browser, seed an old-shape blob and reload:

```js
const b = JSON.parse(localStorage.getItem("skannonser.ui.v1"));
b.filters.tagHidden = { maybe: true };
b.filters.boligtypeHidden = { Leilighet: true };
b.stations.lineHidden = { L1: true };
localStorage.setItem("skannonser.ui.v1", JSON.stringify(b));
location.reload();
```

After reload: no `*Hidden` key survives anywhere in the stored blob, the six selections are empty, everything shows, and unrelated settings (column choices, sliders, layer toggles, dimming, sold preference) are unchanged. Reload a second time and confirm the keys have not come back.

- [ ] **Step 3: Acceptance walk-through**

Isolate-then-add on tags, boligtype and lines; "Alle"/"Tøm"; the `""` bucket selectable on tags, boligtype and tilgjengelighet; empty-list message on eieform and energimerking; cross-page consistency; the active-filter summary counting selections and its × clearing them.

- [ ] **Step 4: Console and mobile sweep**

`read_console_messages` clean on both pages. At 375px: six chip rows plus lines wrap without horizontal overflow and the drawer still opens. Record the sidebar's new height — it will be taller, and that number is the input to the deferred layout work.

- [ ] **Step 5: Update the spec status**

In `docs/superpowers/specs/2026-07-26-filter-selection-semantics-design.md`, change `**Status:** Approved (design), not yet implemented` to `**Status:** Implemented 2026-07-26`.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-07-26-filter-selection-semantics-design.md
git commit -m "docs: mark the filter selection spec implemented"
```
