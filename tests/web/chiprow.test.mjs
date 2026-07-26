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
