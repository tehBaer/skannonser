# Coloured Tag Picker + Auto-Save Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the map popup's unstyleable tag datalist with a coloured chip row that saves on click, and drop the Lagre button in favour of auto-save.

**Architecture:** Frontend only — no Python, no API, no migration. A new pure-ish module `tagpicker.js` owns the chip row; `annotations.js` gains the shared save/dirty-check helper that `table.js` currently keeps to itself; `popup.js`'s editor is rewritten around both; `app.js` wires teardown flushing and hands the popup a live colour accessor. `tagcolors.js` grows its palette array.

**Tech Stack:** Plain ES modules, no build step. MapLibre GL. Tests are `node --test tests/web/*.test.mjs` plus pytest for the static-serving guarantees.

## Global Constraints

- **Test command is `node --test tests/web/*.test.mjs`** — the directory form is broken on node v25 (this machine runs v25.8.0).
- Python tests: `pytest tests/rebuild/test_web_static.py -q`.
- No build step. Every static file is served as authored; imports are relative and end in `.js`.
- **No external resources.** `tests/rebuild/test_web_static.py` fails the build if any authored static file references a third-party host in a resource position (`<script src>`, `<link href>`, `@import`, `url(...)`).
- Norwegian (Bokmål) for all user-visible copy, matching the existing popup ("Kommentar", "Tag", "Lagre").
- Branch is `master`. Commit after every task.
- `main/database/properties.db` and the dev-server ports are shared across worktrees — see CLAUDE.md. Use port 8011 for manual checks.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `skannonser/web/static/tagcolors.js` | tag → colour truth | Modify: palette 10 → 14 entries |
| `skannonser/web/static/annotations.js` | the single PUT contract + dirty-check | Modify: add `normalizeAnnotationValue`, `annotationChanged`, `commitAnnotation` |
| `skannonser/web/static/tagpicker.js` | the popup's coloured chip row | **Create** |
| `skannonser/web/static/popup.js` | popup DOM + auto-saving editor | Modify: `buildEditor` rewritten, `buildPopupContent` signature |
| `skannonser/web/static/table.js` | table view | Modify: `wireCellEdit` calls the shared helper |
| `skannonser/web/static/app.js` | map controller | Modify: flush wiring, colour accessor, drop dead datalist call |
| `skannonser/web/static/style.css` | styling | Modify: chip sizing, input flash, remove button rules |
| `tests/web/tagcolors.test.mjs` | palette guarantees | Modify: extend |
| `tests/web/annotations.test.mjs` | save/dirty-check | **Create** |
| `tests/web/tagpicker.test.mjs` | chip row behaviour | **Create** |
| `tests/rebuild/test_web_static.py` | serving + no-CDN | Modify: one list entry |

---

### Task 1: Widen the tag palette to 14

The live vocabulary is 11 tags against a 10-colour palette. Past palette size `assignTagColors` disables collision probing, so 11 tags currently resolve to **7** distinct colours. Growing the array to 14 re-engages probing. No logic changes.

The four additions were chosen as the Material entries ≥16 ΔE (CIE Lab) from every existing entry and from the boligtype dot palette, which keeps all ten familiar colours and yields a closest pair of ΔE 17.2.

**Files:**
- Modify: `skannonser/web/static/tagcolors.js:13-16`
- Test: `tests/web/tagcolors.test.mjs`

**Interfaces:**
- Consumes: nothing.
- Produces: `TAG_PALETTE` with 14 entries. Every other task treats this as opaque.

- [ ] **Step 1: Write the failing tests**

Append to `tests/web/tagcolors.test.mjs`:

```js
// The live vocabulary as of 2026-07-29. Kept verbatim because the collision
// this palette size exists to prevent was found against these exact strings.
const LIVE_TAGS = [
  "nei", "joda", "nja", "tja", "nice", "wow", "fin",
  "veldig fin", "too far", "fin, men nær veg", "fake?",
];

// Minimal CIE-Lab ΔE. Present so the palette cannot silently regain a
// near-duplicate: the whole point of 14 entries is that a chip row of the
// live tags reads as 11 different colours.
function lab(hex) {
  const [r, g, b] = [1, 3, 5]
    .map((i) => parseInt(hex.slice(i, i + 2), 16) / 255)
    .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  const f = (t) => (t > 0.008856 ? Math.cbrt(t) : 7.787 * t + 16 / 116);
  const x = f((0.4124 * r + 0.3576 * g + 0.1805 * b) / 0.95047);
  const y = f(0.2126 * r + 0.7152 * g + 0.0722 * b);
  const z = f((0.0193 * r + 0.1192 * g + 0.9505 * b) / 1.08883);
  return [116 * y - 16, 500 * (x - y), 200 * (y - z)];
}

function deltaE(a, b) {
  const [A, B] = [lab(a), lab(b)];
  return Math.hypot(A[0] - B[0], A[1] - B[1], A[2] - B[2]);
}

test("the palette holds 14 distinct colors", () => {
  assert.equal(TAG_PALETTE.length, 14);
  assert.equal(new Set(TAG_PALETTE).size, 14, "no duplicate hex values");
});

test("no two palette colors are near-duplicates", () => {
  for (let i = 0; i < TAG_PALETTE.length; i++) {
    for (let j = i + 1; j < TAG_PALETTE.length; j++) {
      const d = deltaE(TAG_PALETTE[i], TAG_PALETTE[j]);
      assert.ok(
        d >= 15,
        `${TAG_PALETTE[i]} and ${TAG_PALETTE[j]} are only ΔE ${d.toFixed(1)} apart`
      );
    }
  }
});

test("the live vocabulary resolves to one color per tag", () => {
  const colors = assignTagColors(LIVE_TAGS);
  assert.equal(colors.size, LIVE_TAGS.length);
  assert.equal(
    new Set(colors.values()).size,
    LIVE_TAGS.length,
    "11 tags must not share colors -- this is the bug the widening fixes"
  );
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test tests/web/tagcolors.test.mjs`
Expected: FAIL — `the palette holds 14 distinct colors` reports `Expected values to be strictly equal: 10 !== 14`, and `the live vocabulary resolves to one color per tag` reports `7 !== 11`.

- [ ] **Step 3: Widen the palette**

In `skannonser/web/static/tagcolors.js`, replace lines 10-16 (the comment and the `TAG_PALETTE` array) with:

```js
// Chosen to be visually distinct from map.js's TYPE_COLOR_PALETTE (boligtype
// dots) so a tag chip is never confusable with a boligtype chip in the filter
// panel; all carry white text.
//
// Fourteen rather than ten because the live vocabulary reached 11 tags, and
// past palette size assignTagColors stops probing -- which collapsed 11 tags
// onto 7 colours. The last four entries were picked as the Material colours
// at least 16 ΔE (CIE Lab) from every entry above them AND from every
// boligtype colour, so the closest pair in this array sits at ΔE 17.2. Adding
// more is not free: see the design doc for why 18 was measured and rejected.
export const TAG_PALETTE = [
  "#c2185b", "#7b1fa2", "#303f9f", "#0277bd", "#00695c",
  "#558b2f", "#ff8f00", "#d84315", "#5d4037", "#455a64",
  "#1b5e20", "#311b92", "#263238", "#1565c0",
];
```

- [ ] **Step 4: Run the whole JS suite**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`. The pre-existing `distinct tags get distinct colors up to palette size` test uses 10 tags and still passes against a 14-slot palette.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/tagcolors.js tests/web/tagcolors.test.mjs
git commit -m "feat(tagcolors): widen the palette to 14 so 11 live tags stay distinct"
```

---

### Task 2: Move the save + dirty-check into `annotations.js`

`table.js` owns a dirty-check whose comment records a real, previously-shipped bug: a no-op PUT bumps `updated_at`, and a bumped `updated_at` is exactly what sheet-import protection reads as "the user edited this row, do not overwrite it". A blur that changed nothing was silently and permanently flipping that protection on for untouched rows.

Task 4 adds a *second* auto-saving editor. If it reimplements the guard, that bug comes back. This task makes the guard shared and unavoidable.

**Files:**
- Modify: `skannonser/web/static/annotations.js` (append)
- Modify: `skannonser/web/static/table.js:336-390`
- Test: `tests/web/annotations.test.mjs` (create)

**Interfaces:**
- Consumes: the existing `saveAnnotation(finnkode, kommentar, tag)`.
- Produces, for Tasks 3-5:
  - `normalizeAnnotationValue(v) -> string|null` — `""`, `"  "`, `null`, `undefined` all become `null`.
  - `annotationChanged(item, kommentar, tag) -> boolean`
  - `commitAnnotation(item, { kommentar, tag }) -> Promise<{finnkode,kommentar,tag}|null>` — returns `null` when the PUT was skipped, otherwise the server-normalized object. Mutates `item.kommentar` / `item.tag` on success only.

- [ ] **Step 1: Write the failing test**

Create `tests/web/annotations.test.mjs`:

```js
// tests/web/annotations.test.mjs
// The save contract shared by the table's inline cells and the map popup's
// editor. The skip-when-unchanged behaviour is the load-bearing part: see the
// updated_at note in annotations.js.
import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeAnnotationValue,
  annotationChanged,
  commitAnnotation,
} from "../../skannonser/web/static/annotations.js";

// Records every PUT so a test can assert that none happened.
function stubFetch(response) {
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    calls.push({ url, body: JSON.parse(opts.body) });
    return { ok: true, json: async () => response };
  };
  return calls;
}

test("every blank form normalizes to the same value", () => {
  assert.equal(normalizeAnnotationValue(""), null);
  assert.equal(normalizeAnnotationValue("   "), null);
  assert.equal(normalizeAnnotationValue(null), null);
  assert.equal(normalizeAnnotationValue(undefined), null);
  assert.equal(normalizeAnnotationValue("  fin  "), "fin");
});

test("a blur that retyped the same value is not a change", () => {
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  assert.equal(annotationChanged(item, "", "fin"), false);
  assert.equal(annotationChanged(item, "   ", "  fin  "), false);
  assert.equal(annotationChanged(item, "ny", "fin"), true);
  assert.equal(annotationChanged(item, "", ""), true); // clearing the tag
});

test("commitAnnotation issues no PUT when nothing changed", async () => {
  const calls = stubFetch({ finnkode: "1", kommentar: null, tag: "fin" });
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  const saved = await commitAnnotation(item, { kommentar: "  ", tag: "fin" });
  assert.equal(saved, null);
  assert.equal(calls.length, 0, "a no-op PUT would bump updated_at");
});

test("commitAnnotation saves and mirrors the server's values into the item", async () => {
  const calls = stubFetch({ finnkode: "1", kommentar: "ny", tag: "wow" });
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  const saved = await commitAnnotation(item, { kommentar: "  ny  ", tag: "wow" });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].body, { kommentar: "ny", tag: "wow" });
  assert.equal(saved.tag, "wow");
  assert.equal(item.kommentar, "ny");
  assert.equal(item.tag, "wow");
});

test("a failed PUT leaves the item untouched so the next blur retries", async () => {
  globalThis.fetch = async () => ({ ok: false, status: 500 });
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  await assert.rejects(() => commitAnnotation(item, { kommentar: "ny", tag: "fin" }));
  assert.equal(item.kommentar, null, "item must stay dirty");
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test tests/web/annotations.test.mjs`
Expected: FAIL — `SyntaxError: The requested module ... does not provide an export named 'normalizeAnnotationValue'`.

- [ ] **Step 3: Add the helpers to `annotations.js`**

Append to `skannonser/web/static/annotations.js`:

```js
// Server-side normalization mirrored here (see saveAnnotation's payload above)
// so the dirty-check compares like with like: "", null and "  " are all the
// same (unset) value.
export function normalizeAnnotationValue(v) {
  return (v || "").trim() || null;
}

export function annotationChanged(item, kommentar, tag) {
  return (
    normalizeAnnotationValue(kommentar) !== normalizeAnnotationValue(item.kommentar) ||
    normalizeAnnotationValue(tag) !== normalizeAnnotationValue(item.tag)
  );
}

// The one way to save an annotation. Returns the server-normalized object, or
// null when the values matched what `item` already held and no PUT was sent.
//
// WHY the skip matters: every PUT bumps the row's updated_at even when the
// payload is byte-identical, and a bumped updated_at is exactly the signal
// sheet-import protection uses to treat an import-created row as "the user has
// edited this, don't overwrite it". A no-op blur was silently and permanently
// flipping that protection on for rows nobody actually touched. Both callers
// (table cells, popup editor) fire on blur, so both would hit it.
//
// `item` is mutated only on success: a failed PUT must leave it dirty so the
// next blur retries.
export async function commitAnnotation(item, { kommentar, tag }) {
  if (!annotationChanged(item, kommentar, tag)) return null;
  const saved = await saveAnnotation(item.finnkode, kommentar, tag);
  item.kommentar = saved.kommentar;
  item.tag = saved.tag;
  return saved;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node --test tests/web/annotations.test.mjs`
Expected: PASS, `# fail 0`, 5 tests.

- [ ] **Step 5: Rewire `table.js` onto the shared helper**

In `skannonser/web/static/table.js`, change the import on line 8 from:

```js
import { saveAnnotation } from "./annotations.js";
```

to:

```js
import { commitAnnotation } from "./annotations.js";
```

Then delete lines 336-341 entirely (the local `normalizeAnnotationValue` and its comment — it now lives in `annotations.js`), and replace the whole `wireCellEdit` function (lines 343-398) with:

```js
// Wires blur/Enter-commit for one inline kommentar/tag <input>. `field` is
// "kommentar" or "tag"; the OTHER field's current value always comes off
// `item` (already-saved state), so a save only ever changes the one field the
// user actually edited. The skip-when-unchanged guard lives in
// commitAnnotation -- it returns null when it sent nothing.
function wireCellEdit(input, item, field) {
  let saving = false;
  const commit = async () => {
    if (saving) return;
    const kommentar = field === "kommentar" ? input.value : item.kommentar;
    const tag = field === "tag" ? input.value : item.tag;
    saving = true;
    input.classList.remove("saved", "error");
    try {
      const saved = await commitAnnotation(item, { kommentar, tag });
      if (!saved) return; // nothing changed; no PUT was sent
      input.value = saved[field] || "";
      input.classList.add("saved");
      setTimeout(() => input.classList.remove("saved"), 1500);
      // Tag vocab may have gained a new value -- refresh it and re-render so
      // the Tag column filter's option list and any active tag filter both
      // reflect it. render() rebuilds the table body (this input included),
      // which is fine: commit only fires on blur/Enter, so the user is done
      // editing by the time we get here, and the `saving` guard above (plus
      // blur already having fired) means the now-detached input can't
      // re-trigger commit.
      refreshVocabs();
      render();
    } catch (err) {
      input.classList.add("error");
    } finally {
      saving = false;
    }
  };
  input.addEventListener("blur", commit);
  input.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      input.blur(); // triggers `commit` via the blur listener above
    }
  });
}
```

Behaviour is unchanged but for one triviality: a no-op blur now clears a lingering `.saved` outline slightly earlier, because the class reset moved above the dirty-check. Nothing depends on it.

- [ ] **Step 6: Run the whole JS suite**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`.

- [ ] **Step 7: Commit**

```bash
git add skannonser/web/static/annotations.js skannonser/web/static/table.js tests/web/annotations.test.mjs
git commit -m "refactor(annotations): share the save + no-op-PUT guard with the popup"
```

---

### Task 3: `tagpicker.js` — the coloured chip row

A native `<datalist>` cannot be styled, so the popup gets a chip row instead. It cannot be an absolutely-positioned dropdown: `.maplibregl-popup-content` sets `overflow: hidden` to clip the thumbnail into the popup's rounded corners, and would clip the panel too.

**Files:**
- Create: `skannonser/web/static/tagpicker.js`
- Test: `tests/web/tagpicker.test.mjs` (create)

**Interfaces:**
- Consumes: `normalizeTag` from `./tagcolors.js`, `tagOptionValues` from `./tagoptions.js` (reused so the chip order matches the table dropdown's order exactly).
- Produces, for Task 4:
  - `nextTagValue(current, clicked) -> string` — normalized `clicked`, or `""` when it equals `current`.
  - `buildTagPicker({ current, vocabulary, colorFor, onPick, doc }) -> { node, repaint, chipCount, pendingNewTag }`
    - `node` — the DOM node to append.
    - `repaint(currentTag, vocabulary)` — re-renders chips and selection.
    - `chipCount() -> number` — how many chips are rendered (Task 4 compares before/after to decide whether the popup needs re-panning).
    - `pendingNewTag() -> string` — the trimmed, uncommitted `+ ny tag` value, `""` when empty.
  - `onPick(value)` is called with the already-resolved new tag value; the caller saves it.

- [ ] **Step 1: Write the failing test**

Create `tests/web/tagpicker.test.mjs`:

```js
// tests/web/tagpicker.test.mjs
// The popup's coloured chip row. Same no-jsdom approach as
// tagoptions.test.mjs: a stand-in for exactly the document surface the module
// touches, so the DOM builder is testable without a browser.
import test from "node:test";
import assert from "node:assert/strict";
import { nextTagValue, buildTagPicker } from "../../skannonser/web/static/tagpicker.js";

function fakeDoc() {
  return {
    createElement(tagName) {
      const node = {
        tagName,
        className: "",
        type: "",
        value: "",
        placeholder: "",
        children: [],
        attrs: {},
        styleProps: {},
        listeners: {},
        appendChild(c) {
          this.children.push(c);
          return c;
        },
        addEventListener(ev, fn) {
          (this.listeners[ev] = this.listeners[ev] || []).push(fn);
        },
        setAttribute(k, v) {
          this.attrs[k] = v;
        },
        // Lets a test drive a handler the module registered.
        fire(ev, arg) {
          (this.listeners[ev] || []).forEach((fn) => fn(arg));
        },
        // "" is the clear-the-children idiom, same as tagoptions.test.mjs.
        set textContent(v) {
          if (v === "") this.children.length = 0;
          this._text = v;
        },
        get textContent() {
          return this._text || "";
        },
      };
      node.style = { setProperty: (k, v) => (node.styleProps[k] = v) };
      return node;
    },
  };
}

const VOCAB = ["wow", "fin", "nei"];
const COLORS = { wow: "#111111", fin: "#222222", nei: "#333333" };
const colorFor = (t) => COLORS[t] || null;

// node > [chip row, new-tag input]; chips are the row's children.
const chipsOf = (picker) => picker.node.children[0].children;

test("clicking an unselected tag selects it", () => {
  assert.equal(nextTagValue("fin", "wow"), "wow");
  assert.equal(nextTagValue("", "wow"), "wow");
  assert.equal(nextTagValue(null, "wow"), "wow");
});

test("clicking the selected tag clears it", () => {
  assert.equal(nextTagValue("wow", "wow"), "");
  assert.equal(nextTagValue("  WOW  ", "wow"), "", "normalization-insensitive");
});

test("chips render in the same sorted order the table dropdown uses", () => {
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick() {},
    doc: fakeDoc(),
  });
  assert.deepEqual(
    chipsOf(picker).map((c) => c.textContent),
    ["fin", "nei", "wow"]
  );
  assert.equal(picker.chipCount(), 3);
});

test("only the selected chip is filled, and every chip carries its colour", () => {
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick() {},
    doc: fakeDoc(),
  });
  const chips = chipsOf(picker);
  const [fin, nei, wow] = chips;
  assert.equal(fin.className, "tag-chip");
  assert.equal(nei.className, "tag-chip off");
  assert.equal(wow.className, "tag-chip off");
  assert.equal(fin.styleProps["--tag-color"], "#222222");
  assert.equal(wow.styleProps["--tag-color"], "#111111");
  assert.equal(fin.attrs["aria-pressed"], "true");
  assert.equal(nei.attrs["aria-pressed"], "false");
});

test("clicking a chip reports the resolved value, not the raw tag", () => {
  const picked = [];
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick: (v) => picked.push(v),
    doc: fakeDoc(),
  });
  const [fin, , wow] = chipsOf(picker);
  wow.fire("click");
  assert.deepEqual(picked, ["wow"]);
  fin.fire("click");
  assert.deepEqual(picked, ["wow", ""], "re-clicking the selected chip clears");
});

test("repaint moves the selection and picks up a new tag", () => {
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick() {},
    doc: fakeDoc(),
  });
  picker.repaint("nei", [...VOCAB, "nytt"]);
  const chips = chipsOf(picker);
  assert.deepEqual(chips.map((c) => c.textContent), ["fin", "nei", "nytt", "wow"]);
  assert.equal(picker.chipCount(), 4);
  assert.equal(chips[1].className, "tag-chip");
  assert.equal(chips[0].className, "tag-chip off");
});

test("the new-tag field commits on Enter and then empties", () => {
  const picked = [];
  const picker = buildTagPicker({
    current: "",
    vocabulary: VOCAB,
    colorFor,
    onPick: (v) => picked.push(v),
    doc: fakeDoc(),
  });
  const input = picker.node.children[1];
  input.value = "  Helt Ny  ";
  assert.equal(picker.pendingNewTag(), "helt ny", "readable before it commits");
  input.fire("keydown", { key: "Enter", preventDefault() {} });
  assert.deepEqual(picked, ["helt ny"]);
  assert.equal(input.value, "");
  assert.equal(picker.pendingNewTag(), "");
});

test("the new-tag field ignores an empty commit", () => {
  const picked = [];
  const picker = buildTagPicker({
    current: "",
    vocabulary: VOCAB,
    colorFor,
    onPick: (v) => picked.push(v),
    doc: fakeDoc(),
  });
  const input = picker.node.children[1];
  input.value = "   ";
  input.fire("blur");
  assert.deepEqual(picked, [], "whitespace is not a tag");
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test tests/web/tagpicker.test.mjs`
Expected: FAIL — `Cannot find module .../skannonser/web/static/tagpicker.js`.

- [ ] **Step 3: Write `tagpicker.js`**

Create `skannonser/web/static/tagpicker.js`:

```js
// skannonser/web/static/tagpicker.js
// The map popup's tag control: one coloured chip per known tag, click to set,
// click the selected one again to clear, plus a field for minting a new tag.
//
// WHY not a dropdown. The tag input used to be a native <input list> pointed
// at tagoptions.js's shared <datalist>, which is still what the table's Tag
// cells use. A datalist costs no focus/keyboard/positioning code and survives
// inside a MapLibre popup -- but its options cannot be styled by any browser,
// so it can never show the tag colours. A custom dropdown PANEL is not an
// option either: .maplibregl-popup-content sets overflow:hidden (it clips the
// thumbnail into the popup's rounded corners) and would clip an absolutely
// positioned list. Anything that opens has to open in flow and grow the popup
// -- at which point it is this chip row with extra steps.
//
// This module builds DOM but holds no state of its own beyond the last painted
// values: the caller owns the tag and calls repaint() after it saves.
import { normalizeTag } from "./tagcolors.js";
import { tagOptionValues } from "./tagoptions.js";

// What the tag becomes when `clicked` is clicked while `current` is set.
// Pure, and the only place the click-again-clears rule lives.
export function nextTagValue(current, clicked) {
  const next = normalizeTag(clicked);
  return normalizeTag(current) === next ? "" : next;
}

export function buildTagPicker({ current, vocabulary, colorFor, onPick, doc }) {
  const d = doc || document;
  const node = d.createElement("div");
  node.className = "sk-tagpicker";

  const row = d.createElement("div");
  row.className = "tag-chip-row";
  node.appendChild(row);

  const newTag = d.createElement("input");
  newTag.type = "text";
  newTag.className = "sk-newtag";
  newTag.placeholder = "+ ny tag …";
  node.appendChild(newTag);

  let selected = normalizeTag(current);
  let chips = [];

  function paint(tags) {
    row.textContent = ""; // clear-the-children idiom, as in tagoptions.js
    chips = tagOptionValues(tags).map((tag) => {
      const chip = d.createElement("button");
      chip.type = "button";
      chip.textContent = tag;
      // Filled when selected, outlined otherwise -- the same reading the
      // filter panel's chips already have, so the colour never has to double
      // as the on/off signal.
      const on = tag === selected;
      chip.className = "tag-chip" + (on ? "" : " off");
      chip.setAttribute("aria-pressed", String(on));
      chip.style.setProperty("--tag-color", colorFor(tag) || "#6f7e76");
      chip.addEventListener("click", () => onPick(nextTagValue(selected, tag)));
      row.appendChild(chip);
      return chip;
    });
  }

  const commitNew = () => {
    const value = normalizeTag(newTag.value);
    if (!value) return; // whitespace is not a tag
    newTag.value = "";
    onPick(value);
  };

  newTag.addEventListener("blur", commitNew);
  newTag.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      commitNew();
    }
  });

  paint(vocabulary);

  return {
    node,
    repaint(currentTag, tags) {
      selected = normalizeTag(currentTag);
      paint(tags);
    },
    chipCount: () => chips.length,
    // The uncommitted contents of the new-tag field, for the caller's
    // flush-on-close path: the popup can be torn down before blur fires.
    pendingNewTag: () => normalizeTag(newTag.value),
  };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node --test tests/web/tagpicker.test.mjs`
Expected: PASS, `# fail 0`, 8 tests.

- [ ] **Step 5: Run the whole JS suite**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`.

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/tagpicker.js tests/web/tagpicker.test.mjs
git commit -m "feat(tagpicker): coloured tag chip row for the map popup"
```

---

### Task 4: Rewrite the popup editor around the picker and auto-save

**Files:**
- Modify: `skannonser/web/static/popup.js` — imports (lines 10-13), `buildPopupContent` (lines 126-254), `buildEditor` (lines 256-311)

**Interfaces:**
- Consumes: `commitAnnotation` (Task 2), `buildTagPicker` (Task 3).
- Produces, for Task 5:
  - `buildPopupContent(item, destinations, getTagColors)` — the third parameter is now a **function** returning the live `Map`, not a `Map`.
  - The returned root node carries `root.skFlush() -> Promise<void>`, which commits any uncommitted kommentar and pending new tag. Safe to call more than once: the dirty-check makes the second call a no-op.

- [ ] **Step 1: Update the imports**

In `skannonser/web/static/popup.js`, replace lines 10-13:

```js
import { saveAnnotation } from "./annotations.js";
import { isNew, fmtDate, premiumPct, fmtPremium, travelMinutes } from "./listingmeta.js";
import { colorForTag } from "./tagcolors.js";
import { attachTagList } from "./tagoptions.js";
```

with:

```js
import { commitAnnotation } from "./annotations.js";
import { isNew, fmtDate, premiumPct, fmtPremium, travelMinutes } from "./listingmeta.js";
import { colorForTag } from "./tagcolors.js";
import { buildTagPicker } from "./tagpicker.js";
```

- [ ] **Step 2: Update the header comment**

Replace lines 1-8 of `skannonser/web/static/popup.js`:

```js
// Popup DOM builder + inline kommentar/tag editor.
//
// buildPopupContent(item, destinations, getTagColors) returns a DOM node for
// MapLibre's Popup.setDOMContent(). The node carries a self-contained
// annotation editor with no save button: the kommentar commits on blur/Enter,
// a tag commits the moment its chip is clicked, and app.js calls the node's
// skFlush() when the popup is torn down (see openPopup for why blur alone is
// not enough). Saves go through annotations.js's commitAnnotation -- the same
// helper table.js's inline cells use -- and mutate `item` (the shared
// per-listing object) in place so a re-open reflects the saved values.
//
// getTagColors is a FUNCTION, not a Map: app.js rebuilds state.tagColors on
// every recompute, and a tag invented in this editor has no colour until that
// rebuild has run.
```

- [ ] **Step 3: Change `buildPopupContent`'s signature and make the header chip repaintable**

Replace lines 123-127 (the comment above `buildPopupContent`, its signature, and the `root` line):

```js
// destinations: [{key,label}] from /api/meta (for the travel-minute rows).
// getTagColors: () => Map from tagcolors.js's assignTagColors, kept in sync
// with the table/map's palette so the popup chip matches the cell accent.
export function buildPopupContent(item, destinations, getTagColors) {
  const colors = () => (getTagColors && getTagColors()) || new Map();
  const root = el("div", "sk-popup");
```

Then replace lines 157-162 (the mini-chip block):

```js
  // Rebuilt rather than built once: the editor below can change the tag while
  // the popup is open, and a header chip still showing the old tag reads as a
  // bug. Declared here so buildEditor's save can call it.
  let miniChip = null;
  function refreshMiniChip() {
    if (miniChip) {
      miniChip.remove();
      miniChip = null;
    }
    const tagColor = colorForTag(item.tag, colors());
    if (!tagColor) return;
    miniChip = el("span", "tag-chip-mini", String(item.tag).trim());
    miniChip.style.background = tagColor;
    addr.appendChild(miniChip);
  }
  refreshMiniChip();
```

- [ ] **Step 4: Mount the new editor**

Replace lines 250-255 (the tail of `buildPopupContent`, closing brace included):

```js
  root.appendChild(body);
  const nabolagSection = buildNabolagSection(item);
  if (nabolagSection) root.appendChild(nabolagSection);
  const editor = buildEditor(item, colors, refreshMiniChip);
  root.appendChild(editor);
  // app.js calls this on both teardown paths -- see openPopup.
  root.skFlush = editor.skFlush;
  return root;
}
```

- [ ] **Step 5: Replace `buildEditor` entirely**

Replace the whole `buildEditor` function (lines 256-311) with:

```js
// `colors` is () => Map; `onSaved` repaints the header chip.
function buildEditor(item, colors, onSaved) {
  const editor = el("div", "sk-editor");

  editor.appendChild(el("label", null, "Kommentar"));
  const komInput = el("input");
  komInput.type = "text";
  komInput.value = item.kommentar || "";
  editor.appendChild(komInput);

  editor.appendChild(el("label", null, "Tag"));
  const picker = buildTagPicker({
    current: item.tag,
    vocabulary: [...colors().keys()],
    colorFor: (tag) => colorForTag(tag, colors()),
    onPick: (value) => save({ tag: value }, null),
  });
  editor.appendChild(picker.node);

  // Saves are SERIALIZED, not guarded by an "is one in flight?" boolean.
  // Clicking a chip blurs the kommentar field first, so a chip click routinely
  // arrives while the kommentar's own PUT is still in flight -- and a guard
  // that drops the second call would silently lose the tag the user just
  // clicked. Chaining runs them in order instead.
  let chain = Promise.resolve();

  function save(patch, control) {
    chain = chain.then(() => runSave(patch, control));
    return chain;
  }

  // The kommentar ALWAYS travels with whatever the field currently shows.
  // Both controls are visible and auto-saving, so the visible state is the
  // intent; taking it off `item` instead would let a chip click overwrite
  // text the user had typed but not yet blurred. Only the tag comes from
  // `patch`, and only when the caller set one.
  //
  // `control` is the input to flash, or null when there is nothing to flash
  // (a chip click, or a flush on a popup that is already gone).
  async function runSave(patch, control) {
    if (control) control.classList.remove("saved", "error");
    try {
      const saved = await commitAnnotation(item, {
        kommentar: komInput.value,
        tag: "tag" in patch ? patch.tag : item.tag,
      });
      if (!saved) return; // nothing changed; no PUT was sent
      komInput.value = saved.kommentar || "";
      if (control) {
        control.classList.add("saved");
        setTimeout(() => control.classList.remove("saved"), 1500);
      }
      // Order matters: app.js rebuilds state.tagColors inside this handler, so
      // a brand-new tag has no colour until it has run. Repainting first would
      // paint the new chip grey.
      document.dispatchEvent(
        new CustomEvent("sk-annotation-saved", { detail: { finnkode: item.finnkode } })
      );
      const before = picker.chipCount();
      picker.repaint(item.tag, [...colors().keys()]);
      onSaved();
      // A new chip can wrap the row onto another line; the popup grew, so ask
      // for the same re-pan the async nabolag section uses.
      if (picker.chipCount() !== before) {
        editor.dispatchEvent(new CustomEvent("sk-popup-resized", { bubbles: true }));
      }
    } catch (err) {
      if (control) control.classList.add("error");
      // The flush path has no control to mark and no popup left to show it in.
      else console.warn("skannonser: lagring av notat feilet", err);
    }
  }

  komInput.addEventListener("blur", () => save({}, komInput));
  komInput.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      komInput.blur(); // triggers the blur listener above
    }
  });

  // Last chance to save before the DOM goes away. Browsers do not reliably
  // fire blur on a focused element that is removed from the document, so
  // neither listener above can be trusted to have run.
  editor.skFlush = () => {
    const pending = picker.pendingNewTag();
    return save(pending ? { tag: pending } : {}, null);
  };

  return editor;
}
```

- [ ] **Step 6: Verify nothing still references the removed pieces**

Run: `grep -n "saveAnnotation\|attachTagList\|Lagre" skannonser/web/static/popup.js`
Expected: no output.

- [ ] **Step 7: Run the whole JS suite**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`. (No test imports `popup.js` — it is a DOM builder. Task 6 verifies it in a browser.)

- [ ] **Step 8: Commit**

```bash
git add skannonser/web/static/popup.js
git commit -m "feat(popup): coloured tag chips and auto-save, no Lagre button"
```

---

### Task 5: Wire teardown flushing and live colours in `app.js`

Two teardown paths exist and only one fires an event. MapLibre's own close — the X, a click on the map, Escape — calls `popup.remove()` and fires `close`. But clicking **another marker** calls `openPopup`, which reuses the single `state.popup` instance and calls `setDOMContent`; the old DOM is discarded with no event at all. Without this task, a comment typed and then abandoned by clicking the next marker is lost, which is the one thing the Lagre button used to guarantee.

**Files:**
- Modify: `skannonser/web/static/app.js` — line 21 (import), line 107 area (state), lines 410-423 (`openPopup`), lines 798-804 (`rebuildFilterUIs`)

**Interfaces:**
- Consumes: `root.skFlush()` and the three-arg `buildPopupContent` (Task 4).
- Produces: nothing downstream.

- [ ] **Step 1: Drop the now-dead datalist wiring**

The map page's only datalist consumer was the popup's tag input, which Task 4 removed. `table.js` keeps its own `syncTagOptions` call, so `tagoptions.js` stays.

Delete line 21 of `skannonser/web/static/app.js`:

```js
import { syncTagOptions } from "./tagoptions.js";
```

Then delete lines 800-804 — the four-line comment beginning "The popup's tag input suggests these" together with the `syncTagOptions(...)` call beneath it. `rebuildFilterUIs` should open:

```js
function rebuildFilterUIs() {
  const vocabs = deriveVocabs(vocabItems());
  if (pruneFilterSets(state.ui.filters, vocabs, vocabIsComplete())) saveUi();
```

- [ ] **Step 2: Add the content handle to state**

In `skannonser/web/static/app.js`, immediately after the `tagColors: new Map(),` line (around line 107), add:

```js
  // The DOM node currently inside state.popup, so it can be flushed when the
  // popup closes or its content is swapped out from under it.
  popupContent: null,
```

- [ ] **Step 3: Replace `openPopup` and add the flush**

Replace lines 410-423 of `skannonser/web/static/app.js`:

```js
function openPopup(finnkode, coordinates) {
  const item = state.itemsById.get(finnkode);
  if (!item) return;
  // Marker -> marker reuses the ONE Popup instance below, so setDOMContent
  // silently discards the previous editor without firing `close`. Flush it
  // first, or a kommentar typed and abandoned by clicking the next marker
  // dies with the node.
  flushPopupEditor();
  // A function, not a snapshot: a tag invented in the editor only gains a
  // colour once applyAll() has rebuilt state.tagColors.
  const content = buildPopupContent(item, state.destinations, () => state.tagColors);
  // Sections that fill in asynchronously (Solgt i nabolaget) and the tag chip
  // row both grow the popup after the pan below has measured it -- re-pan when
  // they say so.
  content.addEventListener("sk-popup-resized", () => panPopupIntoView());
  if (!state.popup) {
    state.popup = new maplibregl.Popup({ maxWidth: "300px" });
    // The close button, a click on the map and Escape all route through
    // remove(), which fires this. The marker -> marker path does NOT.
    state.popup.on("close", flushPopupEditor);
  }
  state.popupContent = content;
  state.popup
    .setLngLat(coordinates || [item.lng, item.lat])
    .setDOMContent(content)
    .addTo(state.map);
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
```

- [ ] **Step 4: Verify the dead import is gone and the accessor is in place**

Run: `grep -n "syncTagOptions\|buildPopupContent\|skFlush" skannonser/web/static/app.js`
Expected: exactly three lines — the `buildPopupContent` import, the `buildPopupContent(item, state.destinations, () => state.tagColors)` call, and the `skFlush` call inside `flushPopupEditor`. No `syncTagOptions`.

- [ ] **Step 5: Run the whole JS suite**

Run: `node --test tests/web/*.test.mjs`
Expected: PASS, `# fail 0`.

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/app.js
git commit -m "feat(map): flush the popup editor on close and on marker swap"
```

---

### Task 6: Styling, serving test, and browser verification

**Files:**
- Modify: `skannonser/web/static/style.css:292-317` and the tag-chip block at 523-536
- Modify: `tests/rebuild/test_web_static.py:136`

**Interfaces:**
- Consumes: the class names Tasks 3-4 emit — `.sk-tagpicker`, `.sk-newtag`, `.tag-chip` inside `.sk-editor`, and `.saved` / `.error` on `.sk-editor input`.
- Produces: nothing.

- [ ] **Step 1: Add `tagpicker.js` to the served-modules test**

In `tests/rebuild/test_web_static.py`, replace line 136:

```python
    for name in ("app.js", "map.js", "popup.js", "filters.js", "stations.js", "tagpicker.js"):
```

- [ ] **Step 2: Run the Python static tests**

Run: `pytest tests/rebuild/test_web_static.py -q`
Expected: PASS. The no-CDN check globs `STATIC_DIR/*.js` and already covers the new module.

- [ ] **Step 3: Restyle the editor**

In `skannonser/web/static/style.css`, replace the `.sk-editor` block at lines 292-317 with:

```css
.sk-editor {
  border-top: 1px solid var(--border);
  padding: 10px 14px 12px;
  background: #fbfdfb;
}
.sk-editor label { display: block; font-size: 11px; color: var(--muted); margin: 4px 0 2px; }
.sk-editor input {
  width: 100%;
  padding: 5px 7px;
  border: 1px solid var(--border);
  border-radius: 5px;
  font: inherit;
}
/* Auto-save feedback. There is no Lagre button and no status line any more:
   a saved field flashes its border for 1.5s, a failed one keeps a red border
   until the next attempt. Same vocabulary as the table's .cell-edit. */
.sk-editor input.saved { border-color: var(--accent); }
.sk-editor input.error { border-color: #a2392e; }

/* Tag picker. The chips are the filter panel's .tag-chip component, sized
   down: the popup is 280px, against a sidebar that is not. */
.sk-editor .tag-chip-row { margin: 2px 0 0; gap: 4px; }
.sk-editor .tag-chip { font-size: 11px; padding: 1px 8px; border-radius: 10px; }
.sk-newtag { margin-top: 6px; }
```

This drops `.sk-editor .row`, `.sk-editor button`, `.sk-editor button:disabled` and the two span-based `.saved` / `.error` rules — all of which belonged to the deleted Lagre row.

- [ ] **Step 4: Confirm no orphaned selectors remain**

Run: `grep -n "sk-editor" skannonser/web/static/style.css`
Expected: only the rules written in Step 3. No `button`, no `.row`.

Run: `grep -rn "sk-editor .row\|sk-editor button" skannonser/web/static/`
Expected: no output.

- [ ] **Step 5: Serve the app and verify in a browser**

```bash
skannonser web --port 8011
```

Open `http://localhost:8011/` and click a marker that has coordinates. Check each of these:

1. The Tag section shows one coloured chip per existing tag; the listing's own tag is filled, the rest outlined.
2. Clicking an outlined chip fills it, the header mini-chip updates to match, and no reload is needed. Re-opening the popup shows the new tag.
3. Clicking the filled chip clears the tag: no chip is filled, and the header mini-chip disappears.
4. Typing in `+ ny tag …` and pressing Enter mints the tag, adds a coloured chip, and leaves it selected. The sidebar's tag filter gains the new value.
5. Typing a Kommentar and pressing Tab flashes the field's border green.
6. **Typing a Kommentar and then clicking a chip directly, without leaving the field first, keeps BOTH.** Re-open the marker: the comment and the new tag are each there. This is the interaction the serialized save chain in Task 4 exists for — clicking a chip blurs the field, so two saves race. A regression here loses either the comment or the tag, silently.
7. Typing a Kommentar and then **clicking the map** (closing the popup) saves it — re-open the marker and the text is there.
8. Typing a Kommentar and then clicking a **different marker** saves it — this is the path that fires no `close` event.
9. The browser console is free of errors throughout.

- [ ] **Step 6: Confirm the table view still saves**

Open `http://localhost:8011/table`, edit a Kommentar cell and a Tag cell, and confirm each flashes and persists after a reload. Task 2 rewired this path; nothing about it should have changed.

- [ ] **Step 7: Run the full test suite**

Run: `node --test tests/web/*.test.mjs && pytest tests/rebuild -q`
Expected: both PASS.

- [ ] **Step 8: Commit**

```bash
git add skannonser/web/static/style.css tests/rebuild/test_web_static.py
git commit -m "style(popup): size the tag chips for the popup, drop the Lagre button rules"
```

---

## Deploying

Frontend-only: the server needs a pull, but **no container restart** — the static files are served from disk. Restarting while the Spotify-style circuit breakers are open is a separate concern that does not apply here, but a needless restart is still a needless risk.

```bash
ssh mbp2016@100.77.139.22 'cd ~/kode/skannonser && git pull'
```

`index.html` is auth-gated, so verify the deploy by fetching an un-gated asset rather than the page:

```bash
curl -s https://<host>/tagpicker.js | head -5
```

Expected: the module header comment, not a login redirect.

## Out of scope

Named here so a reviewer does not flag them as omissions:

- The table's Tag cells keep the plain datalist. `tagoptions.js` stays for them.
- The three pre-existing palette entries that fail white-text contrast (`#ff8f00` at 2.29:1, `#558b2f` at 4.10:1, `#d84315` at 4.44:1). Tracked separately; fixing them reshuffles colours the user already recognises.
- Keyboard navigation beyond native button tabbing.
- Multi-select tags — `annotations.tag` is a single `TEXT` column.
- Collapsing the chip row behind the current tag if the ~3 lines of added popup height prove annoying. Decide after seeing it live.
