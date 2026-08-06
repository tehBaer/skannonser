import { test } from "node:test";
import assert from "node:assert/strict";
import { applyChipClick, selectionChipRow } from "../../skannonser/web/static/filters.js";

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

// Guarded explicitly because every other test here passes against a mutating
// implementation: none of them retains a reference to the input and re-checks
// it. The caller splices the returned array into the live selection, so an
// implementation that returned its own input would splice an array into itself.
test("applyChipClick never mutates its input", () => {
  const input = ["maybe", "hard no"];
  applyChipClick(input, "maybe", ["", "maybe", "hard no"]);
  assert.deepEqual(input, ["maybe", "hard no"], "the caller's array is untouched");
  const empty = [];
  applyChipClick(empty, "maybe", ["", "maybe"]);
  assert.deepEqual(empty, []);
});

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
    // Only "click" is stored, and only the last handler wins -- the row never
    // registers more than one per element -- so a test can simulate a click
    // via `_click()` without pulling in a real event system.
    addEventListener(type, fn) { if (type === "click") this._click = fn; },
    setAttribute() {},
    hidden: false,
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

// The control is always created now (the row is never remounted, so its
// visibility can't be decided once at construction time) -- what varies is
// whether `repaint` hides it. See the mount-time regression this guards
// against: a row mounting empty never gained the button before, even after
// a later selection.
test("an unfiltered row mounts with the bulk control hidden", (t) => {
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
  const bulk = bulkButtons(parent);
  assert.equal(bulk.length, 1);
  assert.equal(bulk[0].hidden, true);
});

// Two identical "Alle"/"Tøm" buttons used to render here, with byte-identical
// handlers -- both spliced the selection empty.
test("a filtered row shows exactly one visible bulk control", (t) => {
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
  assert.equal(bulk[0].hidden, false);
});

// Clicking the control mid-session (selection grew after mount, no remount
// happened) must both empty the selection and re-hide the button -- the
// exact path finding 1 broke.
test("clicking the bulk control empties the selection and hides itself", (t) => {
  const prev = globalThis.document;
  globalThis.document = stubDocument();
  t.after(() => { globalThis.document = prev; });
  const parent = globalThis.document.createElement("div");
  const selected = ["Solgt"];
  let changed = 0;
  selectionChipRow(parent, {
    label: "Status",
    options: [{ key: "", label: "Til salgs" }, { key: "Solgt", label: "Solgt" }],
    selected,
    onChange() { changed++; },
  });
  const bulk = bulkButtons(parent)[0];
  assert.equal(bulk.hidden, false);
  bulk._click();
  assert.deepEqual(selected, []);
  assert.equal(bulk.hidden, true);
  assert.equal(changed, 1);
});
