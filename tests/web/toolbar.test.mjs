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

// activeFilterEntries emits at most one entry per key, so before `count`
// existed a badge could only ever read 0 or 1 -- selecting nine tags still
// painted "1", which reads as a count and was wrong.
test("a multi-value selection sums count, not entries", () => {
  const entries = [
    { key: "tagSelected", count: 9 },
    { key: "tilgjengelighetSelected", count: 3 },
    { key: "facilitiesRequired", count: 4 },
  ];
  assert.deepEqual(statusBadges(entries), { status: 3, tag: 9, facilities: 4 });
});
