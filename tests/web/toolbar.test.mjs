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
