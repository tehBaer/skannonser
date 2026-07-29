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
