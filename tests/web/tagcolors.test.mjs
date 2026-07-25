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
