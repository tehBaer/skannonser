import { test } from "node:test";
import assert from "node:assert/strict";
import { statusBadges } from "../../skannonser/web/static/tablefilters.js";
import { partitionRows } from "../../skannonser/web/static/tablerows.js";

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
