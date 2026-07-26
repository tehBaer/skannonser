// tests/web/travelsentinel.test.mjs
// The pipeline stores negative failure codes in the travel columns
// (skannonser/enrich/sentinels.py: -1 no routes, -2 unrealistic, -3 API
// error). They are NOT minutes, and a raw -1 is a perfectly finite number --
// so before this was guarded it passed every "maks reisetid" slider and sorted
// as the fastest commute in the table.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  isTravelSentinel,
  travelMinutes,
  TRAVEL_UNREACHABLE,
} from "../../skannonser/web/static/listingmeta.js";
import { listingExcluded } from "../../skannonser/web/static/filters.js";
import { defaultFilters, TRAVEL_MAX } from "../../skannonser/web/static/filterstate.js";

const META = { destinations: [{ key: "brj" }, { key: "mvv" }] };

function filtersWith(brjMax) {
  const f = defaultFilters(META);
  f.travelMax.brj = brjMax;
  return f;
}

// --- isTravelSentinel -------------------------------------------------------

test("every negative failure code is a sentinel", () => {
  for (const v of [-1, -2, -3]) {
    assert.equal(isTravelSentinel(v), true, `${v} should be a sentinel`);
  }
});

test("real minutes, zero and unknowns are not sentinels", () => {
  for (const v of [0, 1, 42, 120, 999, null, undefined, "", "abc", NaN]) {
    assert.equal(isTravelSentinel(v), false, `${String(v)} should not be a sentinel`);
  }
});

test("a sentinel arriving as a JSON string still counts", () => {
  assert.equal(isTravelSentinel("-1"), true);
});

// --- travelMinutes (table display + sort) -----------------------------------

test("travelMinutes reports a sentinel as unknown, not as a number", () => {
  assert.equal(travelMinutes({ travel: { brj: -1 } }, "brj"), null);
  assert.equal(travelMinutes({ travel: { brj: -2 } }, "brj"), null);
});

test("travelMinutes passes real minutes through as a number", () => {
  assert.equal(travelMinutes({ travel: { brj: 48 } }, "brj"), 48);
  assert.equal(travelMinutes({ travel: { brj: "48" } }, "brj"), 48);
  assert.equal(travelMinutes({ travel: { brj: 0 } }, "brj"), 0);
});

test("travelMinutes tolerates a missing travel object or key", () => {
  assert.equal(travelMinutes({}, "brj"), null);
  assert.equal(travelMinutes({ travel: {} }, "brj"), null);
  assert.equal(travelMinutes({ travel: { brj: null } }, "brj"), null);
});

// --- listingExcluded (the filter) -------------------------------------------

test("an active slider excludes a sentinel commute", () => {
  const filters = filtersWith(30);
  for (const code of [-1, -2, -3]) {
    const item = { travel: { brj: code } };
    assert.equal(
      listingExcluded(item, filters, {}),
      true,
      `${code} should fail a "<= 30 min" filter`
    );
  }
});

test("a sentinel on ANY destination excludes, even when the others are fine", () => {
  // Torghagen in the live DB: 48 min to Sandvika, 68 to Lambertseter, -1 to
  // Gaustadalleen.
  const item = { travel: { brj: 48, mvv: -1 } };
  const filters = defaultFilters(META);
  filters.travelMax.mvv = 90;
  assert.equal(listingExcluded(item, filters, {}), true);
});

test("a slider parked at its bound is off and lets a sentinel through", () => {
  const item = { travel: { brj: -1 } };
  assert.equal(listingExcluded(item, filtersWith(TRAVEL_MAX), {}), false);
});

test("a genuinely missing commute still never excludes (legacy rule)", () => {
  const filters = filtersWith(30);
  assert.equal(listingExcluded({ travel: { brj: null } }, filters, {}), false);
  assert.equal(listingExcluded({ travel: {} }, filters, {}), false);
  assert.equal(listingExcluded({}, filters, {}), false);
});

test("includeUnknown=false does not change the missing-commute rule", () => {
  const filters = filtersWith(30);
  filters.includeUnknown = false;
  assert.equal(listingExcluded({ travel: { brj: null } }, filters, {}), false);
});

test("real minutes still pass or fail on their own merit", () => {
  const filters = filtersWith(30);
  assert.equal(listingExcluded({ travel: { brj: 29 } }, filters, {}), false);
  assert.equal(listingExcluded({ travel: { brj: 30 } }, filters, {}), false);
  assert.equal(listingExcluded({ travel: { brj: 31 } }, filters, {}), true);
});

// --- the constant -----------------------------------------------------------

test("TRAVEL_UNREACHABLE sits above the slider bound", () => {
  // The substitution only works because no reachable slider position can be
  // >= it; if TRAVEL_MAX ever grows past 999 this silently stops excluding.
  assert.ok(TRAVEL_UNREACHABLE > TRAVEL_MAX);
});
