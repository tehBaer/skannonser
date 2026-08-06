# Table Toolbar Redesign + Unified Status Filter — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the three overlapping status-visibility mechanisms into one shared filter, then rebuild the table toolbar around popovers with a real visual hierarchy.

**Architecture:** `filters.tilgjengelighetSelected` becomes the single source of truth for status visibility on both pages. The map's `ui.sold`/`ui.inactive` layer toggles and the table's `state.showSold` are deleted; both pages read and write the shared selection, and the lazy closed-bucket fetch derives from it. The table's inline tag chips and sold checkbox move into popover buttons with three visual tiers.

**Tech Stack:** Vanilla ES modules (no build step), `node --test` for JS, pytest for Python, FastAPI backend (untouched by this plan).

## Coordinates with: `2026-08-06-radon-classifier.md`

Both plans are in flight and both touch two files. They were kept separate
because radon is 7/8 backend (migration 017, LLM cache versioning, prompt and
schema) while this plan changes no Python at all — merging them would cost the
"pytest stays at 858" invariant that catches an accidental backend edit here.

There are **no overlapping hunks**; the two can be developed in parallel and
merged in either order.

| File | Radon touches | This plan touches |
|---|---|---|
| `listingmeta.js` | radon formatters (~260), `TILSTAND_DERIVED` (~318) | `premiumPct` (~106), new `TILGJENGELIGHET_OPTIONS` after `HUSDYR_OPTIONS` (~226) |
| `table.js` | `COLUMNS` (~106-115), `TILSTAND_COLUMNS` (~145), cell switch (~535) | `state` (~185), `render` (~637), `wireToolbar` (~675), `visibleRows` (~355) |

One soft ordering note: radon's Task 7 adds a `radon_status` column and
relabels the existing one to "Radon nevnt". This plan's Task 5 rebuilds the
toolbar around the **Kolonner** picker, which reads `COLUMNS` dynamically — so
a column added on either side appears automatically. No coordination needed
beyond a rerun of `node --test tests/web/*.test.mjs` after the second merge.

## Global Constraints

- **Work in this worktree.** `/Users/tehbaer/kode/skannonser/.claude/worktrees/table-toolbar-redesign`, branch `worktree-table-toolbar-redesign`. Do NOT `cd` to the main clone.
- **JS tests:** `node --test tests/web/*.test.mjs` — the bare directory form (`node --test tests/web/`) fails. Baseline: **183 passing**.
- **Python tests:** `PYTHONPATH=. ./.venv/bin/pytest` — bare `pytest` in a worktree silently tests the MAIN clone. Baseline: **858 passing**. No task in this plan changes Python; 858 must hold throughout.
- **No build step.** The `static/` files are served as-is. No transpilation, no bundler, no new dependencies.
- **Norwegian UI copy.** All user-visible strings are nb-NO. Existing labels are reused verbatim where they exist.
- **Comments explain WHY, not WHAT.** This codebase's comments document rationale and traps. Match that register; do not narrate the code.
- **Never use bare `git stash`.** The stash stack is shared across worktrees.

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `skannonser/web/static/listingmeta.js` | `premiumPct` guard; new `TILGJENGELIGHET_OPTIONS` constant | 1, 2 |
| `skannonser/web/static/filters.js` | `wantsClosed`; `selectionChipRow` single-bulk + mandatory label; remove sidebar Tilgjengelighet chip row | 2, 3, 5 |
| `skannonser/web/static/filterstate.js` | `seedStatus` floor helper | 2 |
| `skannonser/web/static/index.html` | Map sidebar: two layer checkboxes → four status checkboxes | 3 |
| `skannonser/web/static/app.js` | Drop `ui.sold`/`ui.inactive`; status gating in render, vocab, deep-link, init | 3 |
| `skannonser/web/static/table.html` | Toolbar markup: status button, tag button, switch, tiers | 4, 5 |
| `skannonser/web/static/table.js` | Drop `showSold`; status popover; wire the counter | 4, 5, 6 |
| `skannonser/web/static/tablerows.js` | **Create.** `matchesFilter` + `partitionRows`, importable without side effects | 6 |
| `skannonser/web/static/tablefilters.js` | `statusBadges`; toolbar button builder | 5 |
| `skannonser/web/static/style.css` | Button tiers, switch, popover chrome | 5 |
| `tests/web/premium.test.mjs` | **Create.** premiumPct null-guard regression | 1 |
| `tests/web/status.test.mjs` | **Create.** `wantsClosed`, `seedStatus`, options constant | 2 |
| `tests/web/toolbar.test.mjs` | **Create.** `statusBadges`, `partitionRows` | 5, 6 |
| `tests/web/chiprow.test.mjs` | Extend: single bulk control | 5 |
| `tests/web/vocabs.test.mjs` | Extend: `statusVocabComplete` | 3 |

---

## Task 1: premiumPct null guard

Independent of everything else in this plan. Lands first so it can ship even if the rest slips.

**Files:**
- Modify: `skannonser/web/static/listingmeta.js:106-113`
- Test: `tests/web/premium.test.mjs` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `premiumPct(item)` returns `null` (not `-100`) when `sold_price` is null/undefined/`""`. Signature unchanged.

**Background:** The API ships `sold_price: null` for closed listings whose sale is not yet tinglyst (~100 days after the bidding round). `Number(null) === 0` and `Number.isFinite(0) === true`, so the existing guard passes and the formula computes `(0 / asking - 1) * 100 === -100`. 13 live listings render "−100 %". The same file's `coords()` already documents this exact trap.

- [ ] **Step 1: Write the failing test**

Create `tests/web/premium.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { premiumPct } from "../../skannonser/web/static/listingmeta.js";

// The bug: Number(null) === 0 passes Number.isFinite, so a closed listing
// whose sale is not yet tinglyst computed as a sale for 0 kr -> -100 %.
test("a null sold_price is unknown, not a sale for zero", () => {
  assert.equal(premiumPct({ sold_price: null, price_suggestion: 4990000 }), null);
});

test("an undefined sold_price is unknown", () => {
  assert.equal(premiumPct({ price_suggestion: 4990000 }), null);
});

test("an empty-string sold_price is unknown", () => {
  assert.equal(premiumPct({ sold_price: "", price_suggestion: 4990000 }), null);
});

test("a null price_suggestion is unknown", () => {
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: null }), null);
});

// A genuine 0 kr sale is not in the data (no sold_prices row has one), but if
// FINN ever ships one it must read as -100 %, not as missing. The guard keys
// on blankness, not on the numeric value.
test("an explicit zero sold_price is a real -100 %", () => {
  assert.equal(premiumPct({ sold_price: 0, price_suggestion: 4990000 }), -100);
});

test("a normal sale still computes", () => {
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: 5000000 }), 4);
});

test("a below-asking sale still computes", () => {
  assert.equal(premiumPct({ sold_price: 4750000, price_suggestion: 5000000 }), -5);
});

test("a zero or negative asking price is unknown", () => {
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: 0 }), null);
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: -1 }), null);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/web/premium.test.mjs`

Expected: FAIL — "a null sold_price is unknown" reports `-100 !== null`. The last four tests pass already.

- [ ] **Step 3: Write minimal implementation**

Replace `skannonser/web/static/listingmeta.js:104-113` with:

```js
// Percent over/under prisantydning for a sold item, or null when either the
// tinglyst price or the asking price is missing.
//
// Blankness is rejected BEFORE Number(), not after: Number(null) is 0 and
// Number("") is 0, both of which satisfy Number.isFinite, so a closed listing
// whose sale is not yet tinglyst used to compute as a sale for 0 kr and render
// "-100 %". Same trap `coords()` documents above. An explicit numeric 0 is
// still a real (if implausible) sale and keeps computing.
export function premiumPct(item) {
  const rawSold = item.sold_price;
  const rawAsking = item.price_suggestion;
  if (rawSold === null || rawSold === undefined || rawSold === "") return null;
  if (rawAsking === null || rawAsking === undefined || rawAsking === "") return null;
  const soldPrice = Number(rawSold);
  const asking = Number(rawAsking);
  if (!Number.isFinite(soldPrice) || !Number.isFinite(asking) || asking <= 0) {
    return null;
  }
  return (soldPrice / asking - 1) * 100;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --test tests/web/premium.test.mjs`
Expected: PASS, 8 tests.

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, **191 tests** (183 baseline + 8); Task 1's follow-up fix added a 9th, ending at **192**.

- [ ] **Step 5: Commit**

```bash
git add tests/web/premium.test.mjs skannonser/web/static/listingmeta.js
git commit -m "$(cat <<'EOF'
fix(web): premiumPct read a null sold_price as a 0 kr sale

Number(null) is 0 and satisfies Number.isFinite, so the guard passed and
the formula returned -100 % for any closed listing whose sale is not yet
tinglyst. 13 live listings rendered "-100 %" in the Budpremie column, sorted
to the top as apparent bargains, coloured deep green on the premium scale,
and slipped through the "Maks budpremie" slider because -100 is not null and
so never hit the unknown-value branch.

Reject blankness before coercing, the same way coords() in this file already
does for lat/lng. An explicit numeric 0 still computes.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Status vocabulary and pure helpers

Foundation for Tasks 3–6. No behaviour change on its own.

**Files:**
- Modify: `skannonser/web/static/listingmeta.js` (append `TILGJENGELIGHET_OPTIONS`)
- Modify: `skannonser/web/static/filters.js` (append `wantsClosed`)
- Modify: `skannonser/web/static/filterstate.js` (append `seedStatus`)
- Test: `tests/web/status.test.mjs` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `TILGJENGELIGHET_OPTIONS: Array<{key: string, label: string}>` — exactly `[{key:"",label:"Til salgs"}, {key:"Solgt",label:"Solgt"}, {key:"Inaktiv",label:"Inaktiv"}, {key:"Trukket",label:"Trukket"}]`, from `listingmeta.js`.
  - `wantsClosed(selected: string[]): boolean` from `filters.js`.
  - `seedStatus(filters: object): object` from `filterstate.js` — mutates and returns `filters`.

**Why a fixed constant and not `deriveVocabs`:** `deriveVocabs` counts the statuses present in *loaded* items. On a cold load only actives are loaded, so `Solgt` would be absent from the vocabulary — leaving no chip to click to trigger the fetch that would produce it. The control needs a vocabulary known ahead of the data, exactly like `FERDIGATTEST_OPTIONS`. Counts still come from `deriveVocabs` when available (Task 4).

- [ ] **Step 1: Write the failing test**

Create `tests/web/status.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { TILGJENGELIGHET_OPTIONS } from "../../skannonser/web/static/listingmeta.js";
import { wantsClosed } from "../../skannonser/web/static/filters.js";
import { seedStatus } from "../../skannonser/web/static/filterstate.js";

test("the status vocabulary is fixed, not derived", () => {
  assert.deepEqual(TILGJENGELIGHET_OPTIONS, [
    { key: "", label: "Til salgs" },
    { key: "Solgt", label: "Solgt" },
    { key: "Inaktiv", label: "Inaktiv" },
    { key: "Trukket", label: "Trukket" },
  ]);
});

// The closed bucket is a separate lazy fetch. Selecting any non-"" status is
// what asks for it; "" (Til salgs) never does.
test("wantsClosed is false for an empty selection", () => {
  assert.equal(wantsClosed([]), false);
});

test("wantsClosed is false for Til salgs alone", () => {
  assert.equal(wantsClosed([""]), false);
});

test("wantsClosed is true for any closed status", () => {
  assert.equal(wantsClosed(["Solgt"]), true);
  assert.equal(wantsClosed(["Inaktiv"]), true);
  assert.equal(wantsClosed(["Trukket"]), true);
});

test("wantsClosed is true when a closed status rides along with Til salgs", () => {
  assert.equal(wantsClosed(["", "Inaktiv"]), true);
});

test("wantsClosed tolerates a missing selection", () => {
  assert.equal(wantsClosed(undefined), false);
  assert.equal(wantsClosed(null), false);
});

// The floor: an empty selection means "unfiltered", which combined with the
// lazy fetch would make a cold load and a post-reset load disagree.
test("seedStatus turns an empty selection into Til salgs", () => {
  const f = { tilgjengelighetSelected: [] };
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

test("seedStatus leaves a real selection alone", () => {
  const f = { tilgjengelighetSelected: ["Solgt"] };
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, ["Solgt"]);
});

test("seedStatus is idempotent", () => {
  const f = { tilgjengelighetSelected: [] };
  seedStatus(f);
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

test("seedStatus creates the array when the key is absent", () => {
  const f = {};
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

test("seedStatus mutates in place and returns the same object", () => {
  const f = { tilgjengelighetSelected: [] };
  assert.equal(seedStatus(f), f);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/web/status.test.mjs`
Expected: FAIL — `SyntaxError: The requested module ... does not provide an export named 'TILGJENGELIGHET_OPTIONS'`.

- [ ] **Step 3: Add the vocabulary constant**

Append to `skannonser/web/static/listingmeta.js`, immediately after the `HUSDYR_OPTIONS` export:

```js
// ---------------------------------------------------------------------------
// Status (tilgjengelighet) vocabulary
//
// FIXED, not derived. `deriveVocabs` counts the statuses present in loaded
// items, and on a cold load only actives are loaded -- so "Solgt" would be
// missing from the vocabulary, leaving no control to click to trigger the very
// fetch that would produce it. Same reasoning as FERDIGATTEST_OPTIONS above:
// the value list is known ahead of the data.
//
// "" is "Til salgs", a real status and the most common one -- the backend only
// fills tilgjengelighet for CLOSED listings. It leads the list rather than
// being sorted to the end like other "" buckets (see selectionChipRow's
// emptyIsRealValue).
export const TILGJENGELIGHET_OPTIONS = [
  { key: "", label: "Til salgs" },
  { key: "Solgt", label: "Solgt" },
  { key: "Inaktiv", label: "Inaktiv" },
  { key: "Trukket", label: "Trukket" },
];
```

- [ ] **Step 4: Add `wantsClosed`**

Append to `skannonser/web/static/filters.js`, immediately after the `selectionExcludes` export:

```js
// Does this status selection require the lazily-fetched closed bucket?
//
// The closed listings (~3500) are a separate /api/listings?bucket=sold fetch,
// deliberately not loaded on a cold start. Any selected status other than ""
// (Til salgs) is a request for them. An empty selection does NOT pull them:
// both pages seed the selection to [""] (see seedStatus), so empty only ever
// occurs transiently before that floor is applied.
export function wantsClosed(selected) {
  return Boolean(selected && selected.some((k) => k !== ""));
}
```

- [ ] **Step 5: Add `seedStatus`**

Append to `skannonser/web/static/filterstate.js`, immediately after the `resetFilters` export:

```js
// The status floor. An empty selection means "unfiltered" everywhere else in
// this module, but for status that collides with the lazy closed-bucket fetch:
// a cold load with an empty selection shows only actives (the closed rows were
// never fetched), while an empty selection reached by selecting Solgt and then
// pressing Nullstill shows everything. Same stored value, two different views.
//
// So the pages never hold an empty status selection. Applied on load and again
// after resetFilters, which keeps ONE definition of "default" in defaultFilters
// and layers this floor on top rather than forking it.
export function seedStatus(filters) {
  if (!filters.tilgjengelighetSelected || !filters.tilgjengelighetSelected.length) {
    filters.tilgjengelighetSelected = [""];
  }
  return filters;
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `node --test tests/web/status.test.mjs`
Expected: PASS, 11 tests.

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, **203 tests** (192 + 11).

- [ ] **Step 7: Commit**

```bash
git add tests/web/status.test.mjs skannonser/web/static/listingmeta.js \
        skannonser/web/static/filters.js skannonser/web/static/filterstate.js
git commit -m "$(cat <<'EOF'
feat(web): status vocabulary and selection helpers

Groundwork for unifying the three status-visibility mechanisms. No behaviour
change yet -- nothing calls these.

TILGJENGELIGHET_OPTIONS is fixed rather than derived because deriveVocabs
counts loaded items, and a cold load holds only actives: "Solgt" would be
absent from the vocabulary, leaving no control to click to trigger the fetch
that would produce it.

seedStatus is the floor that stops an empty status selection from meaning two
different things depending on whether the closed bucket happens to be loaded.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Map — status filter replaces the layer toggles

**Files:**
- Modify: `skannonser/web/static/index.html:25-26`
- Modify: `skannonser/web/static/app.js` — `defaultUi` (~line 66), `vocabItems` (~187), `vocabIsComplete` (~196), render gate (~277), `wireLayerToggles` (~515), `combine` handler (~559), `handleHash` (~769), init sold path (~982)
- Modify: `skannonser/web/static/filters.js:632-640` (remove the sidebar Tilgjengelighet chip row)
- Test: `tests/web/vocabs.test.mjs` (extend)

**Interfaces:**
- Consumes: `TILGJENGELIGHET_OPTIONS`, `wantsClosed`, `seedStatus` (Task 2).
- Produces: `statusVocabComplete(selected): boolean` from `filters.js` — true only when all four statuses are selected.

**Behaviour change to be explicit about:** status becomes a **hard hide** on the map, not a dim. Today `ui.sold`/`ui.inactive` hide closed markers outright while the Tilgjengelighet chip row only dims them. After this task the single status selection hides. That is what makes the control read as a layer, and it preserves the behaviour of the toggle it replaces. The trade is that the old chip row's dim-only behaviour is gone — accepted, since two controls for one fact was the bug.

**Why the render gate cannot just fall through to `listingExcluded`:** filters *dim* (`isDimmed` → `residualOpacity`), they do not hide unless Nedtoning is 100 %. Falling through would render ~3500 dimmed sold dots on a default load. Status needs its own `return` in the render loop.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/vocabs.test.mjs`:

```js
import { statusVocabComplete } from "../../skannonser/web/static/filters.js";

// vocabIsComplete gates pruneFilterSets, which DELETES stored filter values
// shared with the table. It must be false whenever a status is switched off,
// because a value can be absent from the vocabulary while very much existing.
test("statusVocabComplete needs every status selected", () => {
  assert.equal(statusVocabComplete(["", "Solgt", "Inaktiv", "Trukket"]), true);
});

test("statusVocabComplete is false when any status is missing", () => {
  assert.equal(statusVocabComplete([""]), false);
  assert.equal(statusVocabComplete(["", "Solgt", "Inaktiv"]), false);
  assert.equal(statusVocabComplete(["Solgt", "Inaktiv", "Trukket"]), false);
});

// An empty selection means "unfiltered" -> every status is visible -> the
// vocabulary IS complete. seedStatus normally prevents this state, but the
// predicate must be correct on its own rather than relying on that.
test("statusVocabComplete is true for an empty selection", () => {
  assert.equal(statusVocabComplete([]), true);
});

test("statusVocabComplete ignores order and duplicates", () => {
  assert.equal(statusVocabComplete(["Trukket", "", "Solgt", "Solgt", "Inaktiv"]), true);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/web/vocabs.test.mjs`
Expected: FAIL — no export named `statusVocabComplete`.

- [ ] **Step 3: Add `statusVocabComplete`**

Append to `skannonser/web/static/filters.js`, immediately after `wantsClosed`:

```js
// Does this status selection cover every status the app can hold?
//
// Feeds vocabIsComplete, which gates pruneFilterSets -- and that deletion is
// irreversible and shared with the table. An empty selection is "unfiltered",
// so it covers everything; otherwise every key in TILGJENGELIGHET_OPTIONS must
// be present.
export function statusVocabComplete(selected) {
  if (!selected || !selected.length) return true;
  const have = new Set(selected);
  return TILGJENGELIGHET_OPTIONS.every((o) => have.has(o.key));
}
```

Add `TILGJENGELIGHET_OPTIONS` to the existing `./listingmeta.js` import block at the top of `filters.js`.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test tests/web/vocabs.test.mjs`
Expected: PASS, 4 new tests.

- [ ] **Step 5: Replace the sidebar layer checkboxes**

In `skannonser/web/static/index.html`, replace lines 25-26:

```html
        <label class="toggle"><input type="checkbox" id="toggle-sold"> Solgt</label>
        <label class="toggle"><input type="checkbox" id="toggle-inactive"> Inaktiv/Trukket</label>
```

with:

```html
        <div id="status-toggles" class="status-toggles"></div>
```

The four checkboxes are built in JS from `TILGJENGELIGHET_OPTIONS` so the markup and the vocabulary cannot drift.

- [ ] **Step 6: Drop the ui buckets from `defaultUi`**

In `skannonser/web/static/app.js`, in `defaultUi`, delete these two lines:

```js
    sold: false,
    inactive: false,
```

Add a comment where they were:

```js
    // `sold`/`inactive` used to live here as layer toggles. Status visibility
    // is now the shared filters.tilgjengelighetSelected, seeded to [""] --
    // which reproduces the old false/false default exactly. Stale stored keys
    // are ignored rather than migrated; the deep-merge in loadUi drops any key
    // not present in this object.
```

- [ ] **Step 7: Gate rendering on status**

In `app.js`'s `applyAll` render loop, replace:

```js
    if (!state.ui[bucketOf(item)]) return; // layer toggle (eie/dnb/sold)
```

with:

```js
    const bucket = bucketOf(item);
    // Source layers (Finn.no / DNB) still gate on ui; status gates on the
    // shared filter. A hard `return`, not a dim: filters dim (residualOpacity)
    // and only hide at Nedtoning 100 %, so falling through to listingExcluded
    // would paint ~3500 faint sold dots on a default load. This preserves what
    // the ui.sold/ui.inactive toggles did.
    if (bucket === "eie" || bucket === "dnb") {
      if (!state.ui[bucket]) return;
    } else if (
      selectionExcludes(state.filters.tilgjengelighetSelected, item.tilgjengelighet || "")
    ) {
      return;
    }
```

Add `selectionExcludes` and `statusVocabComplete` to the existing `./filters.js` import block in `app.js`.

`bucket` is now bound in the loop body; the existing `groupIdForItem` call further down does not re-derive it, so nothing else in the loop needs changing.

- [ ] **Step 8: Update `vocabItems` and `vocabIsComplete`**

Replace `vocabItems`:

```js
function vocabItems() {
  return [...state.itemsById.values()].filter((it) => {
    const bucket = bucketOf(it);
    if (bucket === "eie" || bucket === "dnb") return state.ui[bucket];
    return !selectionExcludes(
      state.filters.tilgjengelighetSelected, it.tilgjengelighet || ""
    );
  });
}
```

Replace `vocabIsComplete`:

```js
function vocabIsComplete() {
  return Boolean(
    state.ui.eie &&
      state.ui.dnb &&
      statusVocabComplete(state.filters.tilgjengelighetSelected) &&
      state.soldLoaded
  );
}
```

- [ ] **Step 9: Rewrite the toggle wiring**

Replace `wireLayerToggles`'s `map` object and loop with source-only wiring, then add the status list builder:

```js
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
      const next = input.checked
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
          // back so the UI never claims a layer it does not have.
          selected.splice(0, selected.length, ...next.filter((k) => k !== opt.key));
          seedStatus(state.ui.filters);
          saveUi();
          input.checked = false;
        } finally {
          input.disabled = false;
        }
      }
      rebuildFilterUIs();
      applyAll();
    });
    label.appendChild(input);
    label.appendChild(document.createTextNode(" " + opt.label));
    mount.appendChild(label);
  });
}
```

Extract the existing `combine` block verbatim into `wireCombineToggle()`, changing only its guard:

```js
      if (combine.checked && wantsClosed(state.ui.filters.tilgjengelighetSelected) && !state.soldLoaded) {
```

Add `TILGJENGELIGHET_OPTIONS` to the `./listingmeta.js` import and `wantsClosed`/`seedStatus` to the `./filterstate.js` and `./filters.js` imports in `app.js`. Note `saveUi()` is the correct persist call here, NOT `saveFilters` -- app.js keeps the filters nested inside `state.ui`, and `saveUi` writes the whole blob (including `.filters`) to the same localStorage key `saveFilters` writes.

- [ ] **Step 10: Update the deep-link path**

In `handleHash`, replace the layer-switching block:

```js
  const bucket = bucketOf(item);
  if ((bucket === "sold" || bucket === "inactive") && !state.ui[bucket]) {
    state.ui[bucket] = true;
    const cb = document.getElementById(bucket === "sold" ? "toggle-sold" : "toggle-inactive");
    if (cb) cb.checked = true;
    saveUi();
    rebuildFilterUIs();
  }
```

with:

```js
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
```

- [ ] **Step 11: Update the init fetch trigger**

Replace:

```js
    if ((state.ui.sold || state.ui.inactive) && !state.soldLoaded) {
```

with:

```js
    if (wantsClosed(state.ui.filters.tilgjengelighetSelected) && !state.soldLoaded) {
```

Then, in `loadUi` (or immediately after it is called in init), apply the floor:

```js
  seedStatus(state.ui.filters);
```

- [ ] **Step 12: Remove the redundant sidebar chip row**

In `skannonser/web/static/filters.js`, delete the `selectionChipRow` call for `"Tilgjengelighet"` (lines 632-640) — the four checkboxes in the Lag panel are now the same value under a better name.

- [ ] **Step 13: Run all JS tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, **207 tests** (203 + 4).

- [ ] **Step 14: Verify Python is untouched**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q`
Expected: **858 passed**.

- [ ] **Step 15: Commit**

```bash
git add skannonser/web/static/index.html skannonser/web/static/app.js \
        skannonser/web/static/filters.js tests/web/vocabs.test.mjs
git commit -m "$(cat <<'EOF'
refactor(web): one status filter, replacing the map's layer toggles

ui.sold/ui.inactive are gone. The map's Lag panel now carries four status
checkboxes -- Til salgs, Solgt, Inaktiv, Trukket -- bound to the shared
filters.tilgjengelighetSelected, and the redundant Tilgjengelighet chip row
in the filter panel is removed. bucketOf's buckets already mapped exactly
onto the derived statuses (api.py sets item.sold from derived == "Solgt"),
so this re-presents one fact rather than adding a feature.

Status is a hard hide in the render loop, not a dim. Filters dim and only
hide at Nedtoning 100 %, so falling through to listingExcluded would paint
~3500 faint sold dots on a default load.

vocabIsComplete now requires all four statuses. It gates pruneFilterSets,
whose deletion is irreversible and shared with the table, so statusVocabComplete
carries its own tests.

Stale ui.sold/ui.inactive keys are ignored, not migrated: their false/false
default is exactly what the seeded [""] selection reproduces.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Table — status popover replaces `showSold`

**Files:**
- Modify: `skannonser/web/static/table.html:22` (remove the sold checkbox, add the Status button)
- Modify: `skannonser/web/static/table.js` — `state.showSold` (~185), `refreshVocabs` (~212), `loadSoldPref`/`saveSoldPref` (~244-262), `enableSold` (~268), `visibleRows` (~355), `wireToolbar` (~675), empty-state reset (~652), `handleHash` (~800), `init` (~837)
- Test: covered by Task 6's `partitionRows` tests; this task is verified manually and by the existing suite staying green.

**Interfaces:**
- Consumes: `TILGJENGELIGHET_OPTIONS`, `wantsClosed`, `seedStatus` (Task 2).
- Produces: `state.filters.tilgjengelighetSelected` as the table's only status control. `state.showSold`, `loadSoldPref` and `saveSoldPref` no longer exist.

- [ ] **Step 1: Replace the checkbox with a button**

In `skannonser/web/static/table.html`, replace line 22:

```html
      <label class="toggle"><input type="checkbox" id="table-sold"> Vis solgte/inaktive</label>
```

with:

```html
      <button type="button" id="table-status-btn" class="toolbar-filter-btn">Status</button>
```

- [ ] **Step 2: Delete the sold preference plumbing**

In `table.js`, delete `loadSoldPref` and `saveSoldPref` entirely, and remove `showSold: false` from `state`. Update the `STORAGE_KEY` comment, which claims the page reads `sold`:

```js
const STORAGE_KEY = "skannonser.ui.v1"; // shared with app.js -- this page
// reads/writes `hiddenColumns` in that blob (filters, including the status
// selection, live there too via filterstate.js); neither page needs to know
// the other's full UI-state shape, just its own fields within the shared blob.
```

- [ ] **Step 3: Derive closed-bucket loading from the selection**

Replace `enableSold` with:

```js
// Fetch the closed bucket if the current status selection asks for it and it
// is not already loaded. Idempotent and safe to call on every filter change.
async function ensureSoldForSelection() {
  if (state.soldLoaded) return;
  if (!wantsClosed(state.filters.tilgjengelighetSelected)) return;
  setStatus("Laster solgte …");
  try {
    state.items = state.items.concat(await fetchListings(1));
    state.soldLoaded = true;
  } catch (err) {
    setStatus("Kunne ikke laste solgte: " + err.message);
    return;
  }
  refreshVocabs();
}
```

- [ ] **Step 4: Replace every `showSold` read**

`refreshVocabs`:

```js
function refreshVocabs() {
  // Same rule as the map (app.js vocabItems): the vocabulary describes the
  // rows the user can see, and state.items only ever grows.
  const visible = state.items.filter(
    (it) => !selectionExcludes(state.filters.tilgjengelighetSelected, it.tilgjengelighet || "")
  );
  state.vocabs = deriveVocabs(visible);
  const vocabComplete =
    statusVocabComplete(state.filters.tilgjengelighetSelected) && state.soldLoaded;
  if (pruneFilterSets(state.filters, state.vocabs, vocabComplete)) saveFilters(state.filters);
  state.tagColors = assignTagColors(state.vocabs.tags.map((o) => o.key));
  syncTagOptions(state.vocabs.tags.map((o) => o.key));
}
```

`visibleRows` — delete the line `if (!state.showSold && item.closed) return false;`. Status is now applied by `listingExcluded` on the very next line. (Task 6 restructures this function.)

Add `selectionExcludes` and `statusVocabComplete` to the `./filters.js` import; add `wantsClosed`, `seedStatus` and `TILGJENGELIGHET_OPTIONS` to their import blocks.

- [ ] **Step 5: Wire the Status button**

In `wireToolbar`, replace the whole `soldToggle` block with:

```js
  const statusBtn = document.getElementById("table-status-btn");
  if (statusBtn) {
    statusBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openPopover(statusBtn, (pop) => {
        // Counts come from the derived vocabulary when the bucket is loaded,
        // but the OPTION LIST is the fixed constant -- a status absent from
        // the loaded items still needs a control, or there is nothing to click
        // to trigger the fetch that would produce it.
        const counts = new Map(state.vocabs.tilgjengelighet.map((o) => [o.key, o.count]));
        selectionChipRow(pop, {
          label: "Status",
          options: TILGJENGELIGHET_OPTIONS.map((o) => ({ ...o, count: counts.get(o.key) })),
          selected: state.filters.tilgjengelighetSelected,
          emptyIsRealValue: true,
          onChange: async () => {
            seedStatus(state.filters);
            await ensureSoldForSelection();
            onFilterChange();
          },
        });
      });
    });
  }
```

- [ ] **Step 6: Update the empty-state reset**

In the empty-table `Nullstill filtre` handler, replace `await enableSold();` with:

```js
      seedStatus(state.filters);
      await ensureSoldForSelection();
```

- [ ] **Step 7: Update the deep-link and init paths**

In `handleHash`, replace the `showSold` promotion block with:

```js
  const status = item.tilgjengelighet || "";
  if (selectionExcludes(state.filters.tilgjengelighetSelected, status)) {
    state.filters.tilgjengelighetSelected.push(status);
    saveFilters(state.filters);
    refreshVocabs();
  }
```

In `init`, replace the `soldToggle.checked` block with:

```js
  seedStatus(state.filters);
  await ensureSoldForSelection();
```

placed immediately after `state.filters = loadFilters(meta);` and before `refreshVocabs()`.

- [ ] **Step 8: Run all JS tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, **207 tests** (unchanged — this task adds no tests).

- [ ] **Step 9: Commit**

```bash
git add skannonser/web/static/table.html skannonser/web/static/table.js
git commit -m "$(cat <<'EOF'
refactor(web): table status filter replaces the sold checkbox

state.showSold and its `sold` localStorage key are gone; the table now reads
and writes the same filters.tilgjengelighetSelected the map does, so a status
set on either page is visible and adjustable on the other. Before this, the
table applied that filter while offering no control for it -- a status set on
the map could empty the table with nothing on screen explaining why.

The Status popover's option list is the fixed TILGJENGELIGHET_OPTIONS with
counts grafted on from the derived vocabulary, not the derived vocabulary
itself: on a cold load "Solgt" has no loaded items and so no derived entry,
and without a control for it there is nothing to click to trigger the fetch.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Toolbar chrome — popovers, tiers, switch

**Files:**
- Modify: `skannonser/web/static/table.html:19-29`
- Modify: `skannonser/web/static/table.js` (~623, tag chip mount; `wireToolbar` ~675)
- Modify: `skannonser/web/static/tablefilters.js` (add `statusBadges`)
- Modify: `skannonser/web/static/filters.js` (`selectionChipRow`: one bulk control, mandatory label)
- Modify: `skannonser/web/static/style.css`
- Test: `tests/web/toolbar.test.mjs` (create), `tests/web/chiprow.test.mjs` (extend)

**Interfaces:**
- Consumes: `activeFilterEntries` (existing, `filterstate.js`).
- Produces: `statusBadges(entries: Array<{key: string}>): {status: number, tag: number, facilities: number}` from `tablefilters.js`.

- [ ] **Step 1: Write the failing tests**

Create `tests/web/toolbar.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { statusBadges } from "../../skannonser/web/static/tablefilters.js";

// Badges derive from activeFilterEntries rather than a parallel count, so a
// button's badge and the toolbar's "N filtre aktive" cannot disagree.
test("no entries means no badges", () => {
  assert.deepEqual(statusBadges([]), { status: 0, tag: 0, facilities: 0 });
});

test("each button counts only its own entry key", () => {
  const entries = [
    { key: "tilgjengelighetSelected" },
    { key: "tagSelected" },
    { key: "facilitiesRequired" },
  ];
  assert.deepEqual(statusBadges(entries), { status: 1, tag: 1, facilities: 1 });
});

test("unrelated entries do not raise any badge", () => {
  const entries = [{ key: "priceMax" }, { key: "braIMin" }, { key: "travelMax.brj" }];
  assert.deepEqual(statusBadges(entries), { status: 0, tag: 0, facilities: 0 });
});

test("a missing entries list is tolerated", () => {
  assert.deepEqual(statusBadges(undefined), { status: 0, tag: 0, facilities: 0 });
});
```

Append to `tests/web/chiprow.test.mjs`:

```js
import { selectionChipRow } from "../../skannonser/web/static/filters.js";

// A minimal DOM stub -- these tests run under plain node, no jsdom. Only the
// handful of methods selectionChipRow touches are implemented.
function stubDocument() {
  const make = (tag) => ({
    tagName: tag.toUpperCase(),
    className: "",
    textContent: "",
    children: [],
    style: { setProperty() {} },
    classList: { add() {} },
    appendChild(c) { this.children.push(c); return c; },
    addEventListener() {},
    setAttribute() {},
  });
  return { createElement: make };
}

function bulkButtons(node) {
  const found = [];
  const walk = (n) => {
    if (n.className === "linkish") found.push(n);
    (n.children || []).forEach(walk);
  };
  walk(node);
  return found;
}

test("an unfiltered row offers no bulk control", (t) => {
  const prev = globalThis.document;
  globalThis.document = stubDocument();
  t.after(() => { globalThis.document = prev; });
  const parent = globalThis.document.createElement("div");
  selectionChipRow(parent, {
    label: "Status",
    options: [{ key: "", label: "Til salgs" }, { key: "Solgt", label: "Solgt" }],
    selected: [],
    onChange() {},
  });
  assert.equal(bulkButtons(parent).length, 0);
});

// Two identical "Alle"/"Tøm" buttons used to render here, with byte-identical
// handlers -- both spliced the selection empty.
test("a filtered row offers exactly one bulk control", (t) => {
  const prev = globalThis.document;
  globalThis.document = stubDocument();
  t.after(() => { globalThis.document = prev; });
  const parent = globalThis.document.createElement("div");
  selectionChipRow(parent, {
    label: "Status",
    options: [{ key: "", label: "Til salgs" }, { key: "Solgt", label: "Solgt" }],
    selected: ["Solgt"],
    onChange() {},
  });
  const bulk = bulkButtons(parent);
  assert.equal(bulk.length, 1);
  assert.equal(bulk[0].textContent, "Nullstill");
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `node --test tests/web/toolbar.test.mjs tests/web/chiprow.test.mjs`
Expected: FAIL — no export named `statusBadges`; chiprow reports 2 bulk buttons where 1 is expected and 2 where 0 is expected.

- [ ] **Step 3: Add `statusBadges`**

Append to `skannonser/web/static/tablefilters.js`:

```js
// Per-button badge counts, derived from activeFilterEntries rather than
// recounted from the filters object -- a second count is a second thing to
// keep in sync with "N filtre aktive", and it would drift.
const BADGE_KEYS = {
  status: "tilgjengelighetSelected",
  tag: "tagSelected",
  facilities: "facilitiesRequired",
};

export function statusBadges(entries) {
  const list = entries || [];
  const out = { status: 0, tag: 0, facilities: 0 };
  Object.entries(BADGE_KEYS).forEach(([button, key]) => {
    out[button] = list.filter((e) => e.key === key).length;
  });
  return out;
}
```

- [ ] **Step 4: Collapse the bulk controls**

In `skannonser/web/static/filters.js`'s `selectionChipRow`, replace the `mkBulk` block (lines 481-487) with:

```js
  // One control, not two. "Alle" and "Tøm" shipped byte-identical handlers --
  // both spliced the selection empty -- on the theory that they read
  // differently mid-filter. They do not: an empty selection IS the unfiltered
  // state, so clearing and showing-everything are one action. Rendered only
  // when there is something to clear.
  if (selected.length) {
    mkBulk("Nullstill", () => selected.splice(0, selected.length));
  }
  head.appendChild(bulkWrap);
```

Make `label` mandatory — every call site now passes one, since the popover has room for a heading. Replace the `if (label) { ... }` block with an unconditional version and drop the comment about the toolbar having no room.

- [ ] **Step 5: Run tests to verify they pass**

Run: `node --test tests/web/toolbar.test.mjs tests/web/chiprow.test.mjs`
Expected: PASS.

- [ ] **Step 6: Restructure the toolbar markup**

Replace `skannonser/web/static/table.html` lines 19-29 with:

```html
    <div class="table-toolbar">
      <input type="text" id="table-filter" class="table-filter-input"
             placeholder="Filtrer adresse, postnummer, boligtype …">

      <div class="toolbar-group toolbar-filters">
        <button type="button" id="table-status-btn" class="toolbar-btn">Status</button>
        <button type="button" id="table-tags-btn" class="toolbar-btn">Tagger</button>
        <button type="button" id="facilities-filter-btn" class="toolbar-btn">Fasiliteter</button>
        <label class="switch">
          <input type="checkbox" id="table-include-unknown">
          <span class="switch-track" aria-hidden="true"></span>
          <span class="switch-label">Ukjent verdi</span>
        </label>
      </div>

      <div class="toolbar-group toolbar-view">
        <button type="button" id="table-columns-btn" class="toolbar-btn toolbar-btn-view">Kolonner</button>
        <button type="button" id="table-reset-filters" class="toolbar-reset">Nullstill filtre</button>
      </div>

      <span id="table-status" class="muted"></span>
    </div>
```

The `#table-tag-chips` mount is gone — tags now live in the `Tagger` popover.

- [ ] **Step 7: Move the tag chips into a popover**

In `table.js`'s `render`, delete the `chipMount` block entirely. In `wireToolbar`, add:

```js
  const tagsBtn = document.getElementById("table-tags-btn");
  if (tagsBtn) {
    tagsBtn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      openPopover(tagsBtn, (pop) => {
        selectionChipRow(pop, {
          label: "Tagger",
          options: state.vocabs.tags,
          selected: state.filters.tagSelected,
          colorFor: (key) => colorForTag(key, state.tagColors),
          onChange: onFilterChange,
        });
      });
    });
  }
```

- [ ] **Step 8: Paint the badges**

In `render`, replace the trailing `facBtn` block with:

```js
  // Runs on every render (onFilterChange + cross-tab sync included) so the
  // buttons' active cues never drift from the actual filter state.
  const badges = statusBadges(activeFilterEntries(state.filters, state.meta));
  const paintBtn = (id, count) => {
    const btn = document.getElementById(id);
    if (!btn) return;
    btn.classList.toggle("active", count > 0);
    btn.dataset.badge = count > 0 ? String(count) : "";
  };
  paintBtn("table-status-btn", badges.status);
  paintBtn("table-tags-btn", badges.tag);
  paintBtn("facilities-filter-btn", badges.facilities);

  const resetBtn = document.getElementById("table-reset-filters");
  if (resetBtn) resetBtn.disabled = n === 0;
```

Add `statusBadges` to the `./tablefilters.js` import and `activeFilterEntries` to the `./filterstate.js` import in `table.js`.

- [ ] **Step 9: Style the three tiers and the switch**

Replace `.toolbar-filter-btn`'s two rules in `style.css` (lines 463-464) and add:

```css
/* --- table toolbar tiers (2026-08-06 redesign) ---------------------------
   Three tiers, because the old toolbar gave Fasiliteter, Kolonner and
   Nullstill one shared two-property rule and they read as the same control:
   filter buttons (pill, bordered, badge), the view control (same shape,
   muted, right-aligned) and reset (borderless text). */
.table-toolbar { align-items: flex-start; }
.toolbar-group { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.toolbar-view { margin-left: auto; }

.toolbar-btn {
  font: inherit;
  font-size: 13px;
  padding: 5px 12px;
  border: 1px solid var(--border);
  border-radius: 16px;
  background: var(--panel);
  color: var(--text);
  cursor: pointer;
}
.toolbar-btn::after { content: " ▾"; opacity: 0.5; }
.toolbar-btn:hover { border-color: var(--accent); }
.toolbar-btn.active {
  border-color: var(--accent);
  background: #e8f3ee;
  color: var(--accent);
  font-weight: 600;
}
/* The badge rides in a data attribute so painting is one assignment and an
   empty string removes it -- no separate span to create and tear down. */
.toolbar-btn.active[data-badge]:not([data-badge=""])::before {
  content: attr(data-badge);
  display: inline-block;
  min-width: 16px;
  margin-right: 6px;
  padding: 0 4px;
  border-radius: 8px;
  background: var(--accent);
  color: #fff;
  font-size: 11px;
  text-align: center;
}
.toolbar-btn-view { color: var(--muted); }
.toolbar-btn-view::after { content: " ▾"; opacity: 0.35; }

.toolbar-reset {
  font: inherit;
  font-size: 13px;
  padding: 5px 4px;
  border: 0;
  background: none;
  color: var(--accent);
  cursor: pointer;
  text-decoration: underline;
}
.toolbar-reset:disabled {
  color: var(--muted);
  cursor: default;
  text-decoration: none;
  opacity: 0.6;
}

/* Switch: a checkbox in behaviour and semantics (same id, same `checked`,
   same includeUnknown wiring) -- only the paint differs. The native input
   stays in the DOM and keeps focus/keyboard handling. */
.switch { display: inline-flex; align-items: center; gap: 8px; cursor: pointer; font-size: 13px; }
.switch input { position: absolute; opacity: 0; width: 0; height: 0; }
.switch-track {
  width: 32px; height: 18px; border-radius: 9px;
  background: var(--border); position: relative; transition: background 0.15s;
}
.switch-track::after {
  content: ""; position: absolute; top: 2px; left: 2px;
  width: 14px; height: 14px; border-radius: 50%;
  background: #fff; transition: transform 0.15s;
}
.switch input:checked + .switch-track { background: var(--accent); }
.switch input:checked + .switch-track::after { transform: translateX(14px); }
.switch input:focus-visible + .switch-track { outline: 2px solid var(--accent); outline-offset: 2px; }
```

- [ ] **Step 10: Run all JS tests**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, **213 tests** (207 + 4 toolbar + 2 chiprow).

- [ ] **Step 11: Commit**

```bash
git add skannonser/web/static/table.html skannonser/web/static/table.js \
        skannonser/web/static/tablefilters.js skannonser/web/static/filters.js \
        skannonser/web/static/style.css tests/web/toolbar.test.mjs tests/web/chiprow.test.mjs
git commit -m "$(cat <<'EOF'
feat(web): table toolbar popovers and visual tiers

Twelve tag chips no longer render permanently; they move into a Tagger
popover beside Status and Fasiliteter. The three buttons that shared one
two-property CSS rule now read as three tiers: filter pills with count
badges, a muted right-aligned Kolonner, and a borderless Nullstill that
disables itself when nothing is filtered.

selectionChipRow's "Alle" and "Tøm" collapse into one "Nullstill". They
shipped byte-identical handlers -- both spliced the selection empty -- on the
theory that they read differently mid-filter. An empty selection IS the
unfiltered state, so there was only ever one action.

"Inkluder ukjent verdi" keeps its checkbox semantics and wiring; only the
paint changes.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Counter denominator

**Files:**
- Create: `skannonser/web/static/tablerows.js`
- Modify: `skannonser/web/static/table.js` — delete `matchesFilter` (~346) and `visibleRows` (~355), update `render` (~637, ~661)
- Test: `tests/web/toolbar.test.mjs` (extend)

**Interfaces:**
- Consumes: `selectionExcludes`, `listingExcluded`.
- Produces, from the new `tablerows.js`:
  - `matchesFilter(item, text): boolean` — moved verbatim from `table.js`.
  - `partitionRows(items, filters, meta, {text, focusFinnkode}): {rows: object[], universe: number}`.

**Why a new module:** `table.js` calls `init()` at module scope (its last line). Importing anything from it in a `node --test` file would execute the whole app against a nonexistent DOM. The two pure functions move to a side-effect-free module so they can be tested at all; `table.js` imports them back.

**The bug:** `state.items` only ever grows. Once the closed bucket is fetched it stays, so with the status filter back on Til salgs the denominator still counted all 4387 rows.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/toolbar.test.mjs`:

```js
import { partitionRows } from "../../skannonser/web/static/tablerows.js";

const META = { boligtyper: [], eieformer: [], energimerker: [], destinations: [] };
const BASE = { includeUnknown: true, tilgjengelighetSelected: [""], tagSelected: [],
  boligtypeSelected: [], eieformSelected: [], energiSelected: [], postnummerSelected: [],
  nabolagSelected: [], ferdigattestSelected: [], utleieSelected: [], husdyrSelected: [],
  alvorlighetSelected: [], facilitiesRequired: {}, travelMax: {} };

const ITEMS = [
  { finnkode: "1", adresse: "Aveien 1", tilgjengelighet: null, closed: false },
  { finnkode: "2", adresse: "Bveien 2", tilgjengelighet: null, closed: false },
  { finnkode: "3", adresse: "Cveien 3", tilgjengelighet: "Solgt", closed: true, sold: true },
  { finnkode: "4", adresse: "Dveien 4", tilgjengelighet: "Inaktiv", closed: true },
];

test("the denominator counts only rows whose status is selected", () => {
  const { rows, universe } = partitionRows(ITEMS, { ...BASE }, META, {});
  assert.equal(universe, 2);
  assert.equal(rows.length, 2);
});

test("selecting a closed status widens the denominator", () => {
  const filters = { ...BASE, tilgjengelighetSelected: ["", "Solgt"] };
  const { universe } = partitionRows(ITEMS, filters, META, {});
  assert.equal(universe, 3);
});

// The whole point: a text search narrows the numerator, never the denominator.
test("a text filter narrows rows but not the universe", () => {
  const { rows, universe } = partitionRows(ITEMS, { ...BASE }, META, { text: "Aveien" });
  assert.equal(rows.length, 1);
  assert.equal(universe, 2);
});

test("a deep-linked row survives a status it does not match, in both counts", () => {
  const { rows, universe } = partitionRows(ITEMS, { ...BASE }, META, { focusFinnkode: "3" });
  assert.equal(universe, 3);
  assert.ok(rows.some((r) => r.finnkode === "3"));
});

test("an empty status selection counts every loaded row", () => {
  const filters = { ...BASE, tilgjengelighetSelected: [] };
  const { universe } = partitionRows(ITEMS, filters, META, {});
  assert.equal(universe, 4);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/web/toolbar.test.mjs`
Expected: FAIL — `ERR_MODULE_NOT_FOUND` for `tablerows.js`.

- [ ] **Step 3: Create `tablerows.js`**

Create `skannonser/web/static/tablerows.js`. `matchesFilter` moves here verbatim from `table.js`; `partitionRows` is new.

```js
// Row selection for the table page. Split out of table.js because that module
// calls init() at import time -- importing a predicate from it would boot the
// whole app -- and these two functions are pure, so they are worth testing
// directly.

import { selectionExcludes, listingExcluded } from "./filters.js";

// Exported: table.js's compareItems and cell rendering use it too, and one
// definition beats two identical four-line copies.
export function isBlank(v) {
  return v === null || v === undefined || v === "";
}

export function matchesFilter(item, text) {
  if (!text) return true;
  const needle = text.toLowerCase();
  // Includes kommentar/tag so your own notes are searchable.
  return [item.adresse, item.postnummer, item.boligtype, item.kommentar, item.tag].some(
    (v) => !isBlank(v) && String(v).toLowerCase().includes(needle)
  );
}

// Two passes, because the row counter's two numbers answer different
// questions. The universe is "rows whose status I asked to see" -- the
// denominator; rows is "rows passing everything" -- the numerator. Splitting
// them fixes a counter that read "867 av 4387" after the closed bucket had
// been fetched and then filtered back out: state.items only ever grows, so
// using its length as the denominator counted rows that were not on screen.
//
// listingExcluded still applies the status filter internally -- it is the
// shared predicate and the map depends on that. The double application is
// idempotent and deliberately not optimised away, so the two pages cannot
// drift apart.
export function partitionRows(items, filters, meta, { text, focusFinnkode } = {}) {
  const focused = (item) => focusFinnkode && String(item.finnkode) === focusFinnkode;
  const universe = items.filter(
    (item) =>
      focused(item) ||
      !selectionExcludes(filters.tilgjengelighetSelected, item.tilgjengelighet || "")
  );
  const rows = universe.filter(
    (item) =>
      focused(item) ||
      (!listingExcluded(item, filters, meta) && matchesFilter(item, text))
  );
  return { rows, universe: universe.length };
}
```

Then in `table.js`: delete `matchesFilter`, delete `visibleRows`, and **delete the local `isBlank`** — import it from `tablerows.js` instead. `compareItems` and two cell-render branches still use it (lines 331, 332, 581, 613); they now use the import, so the predicate has exactly one definition. Add:

```js
import { isBlank, matchesFilter, partitionRows } from "./tablerows.js";
```

- [ ] **Step 4: Use it in `render`**

Replace `const rows = visibleRows();` with:

```js
  const { rows, universe } = partitionRows(state.items, state.filters, state.meta, {
    text: state.filterText,
    focusFinnkode: state.focusFinnkode,
  });
  rows.sort((a, b) => compareItems(a, b, state.sortKey, state.sortDir));
```

Replace the status line:

```js
  setStatus(
    rows.length + " av " + universe + " annonser" +
    (n ? " · " + n + " filtre aktive" : "")
  );
```

The empty-state guard changes from `if (!rows.length && state.items.length)` to `if (!rows.length && universe)` — with every status deselected the table is empty by request, and offering "Nullstill filtre" there is right, but the message should not claim rows exist when the universe is zero. Keep `state.items.length` as an additional guard so a still-loading table shows nothing rather than the empty-state row:

```js
  if (!rows.length && state.items.length) {
```

(unchanged — `state.items.length` remains the correct "have we loaded anything at all" test.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `node --test tests/web/toolbar.test.mjs`
Expected: PASS, 9 tests.

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, **218 tests** (213 + 5).

- [ ] **Step 6: Verify Python is still untouched**

Run: `PYTHONPATH=. ./.venv/bin/pytest -q`
Expected: **858 passed**.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/table.js tests/web/toolbar.test.mjs
git commit -m "$(cat <<'EOF'
fix(web): row counter denominator ignored the status filter

state.items only ever grows -- the closed bucket is concatenated on fetch and
never removed -- so once it had been loaded the denominator counted all 4387
rows even with the status filter back on Til salgs. Toggling sold on and then
off left "867 av 4387": 3520 hidden rows still in the total.

partitionRows splits the two questions the counter answers. The universe is
the rows whose status was asked for (the denominator); rows is what passes
every filter (the numerator). A deep-linked row escapes both passes so it is
never counted out of its own denominator.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Final verification

- [ ] **Run the full suites**

```bash
node --test tests/web/*.test.mjs && PYTHONPATH=. ./.venv/bin/pytest -q
```

Expected: **218 node, 858 pytest**.

- [ ] **Serve and verify by hand**

The browser pane is unreliable here in two documented ways: MapLibre GL never initialises in it, and `preview_start` reads the **main clone's** `launch.json` — so it may serve the main clone's files, not this worktree's. Before believing any UI observation, confirm which file is being served:

```bash
curl -s http://localhost:8000/table.html | grep -c "table-status-btn"
```

Expect `1`. A `0` means the main clone is being served and the whole verification is meaningless.

- [ ] **Manual checklist**

| Check | Expected |
|---|---|
| Cold load, table | "N av N annonser", Status badge ①, only actives |
| Select Solgt | closed bucket fetches once; both counter numbers grow |
| Deselect Solgt | both numbers shrink back; no second fetch |
| Nullstill filtre | returns to Til salgs; button then disabled |
| Map, cold load | only active dots; Til salgs checked, other three unchecked |
| Map, tick Solgt | sold dots appear; table (other tab) follows via cross-tab sync |
| Map deep link to a sold listing | its status switches on; dot visible |
| Ukjent verdi switch | drives `includeUnknown`; keyboard-focusable |
| Budpremie column | the 13 null-sold-price rows are blank, not "−100 %" |
| Returning reader with stale `ui.sold: true` | sees Til salgs only; no crash |

---

## Self-review notes

**Spec coverage.** §1 unified status → Tasks 2, 3, 4. §2 seeded default → Task 2 (helper), Tasks 3 and 4 (application). §3 toolbar layout → Task 5. §4 chips into popovers → Task 5. §5 counter → Task 6. §6 badges → Task 5. The −100 % fix → Task 1.

**Deviation from the spec's staging.** The spec proposed four commits; this plan uses six, splitting the status unification into a pure-helper task (2) and per-page application (3, 4). The map task is the risky one and is easier to review and revert on its own.

**Known gap.** Task 4 ships no new tests of its own — its logic is table wiring, and the pure part is covered by Task 6's `partitionRows` tests. Its verification is the existing suite staying green plus the manual checklist. Flagged rather than papered over with a DOM-stub test that would assert little.
