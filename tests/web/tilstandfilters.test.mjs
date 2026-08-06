// tests/web/tilstandfilters.test.mjs
// Filters for the tilstand classifier (migration 016): an `alvorlighet` chip
// selection and a `reparasjon_est` max-slider.
//
// alvorlighet follows ENERGIMERKING's routing (selection.test.mjs), not
// eieform's: a null alvorlighet means the listing was never classified (no
// tilstandsrapport read), so it must be its own explicit "Ukjent" chip rather
// than deferring to includeUnknown -- otherwise picking "Alvorlig" would also
// return every unclassified listing.
//
// reparasjon_est follows the existing money sliders (pris/totalpris): it
// routes through the shared overMax() helper in filters.js, which defers a
// missing value to `includeUnknown` (on by default, so null passes) rather
// than always passing regardless of that policy.

import { test } from "node:test";
import assert from "node:assert/strict";
import { listingExcluded, selectionExcludes } from "../../skannonser/web/static/filters.js";
import { defaultFilters, REPARASJON_MAX } from "../../skannonser/web/static/filterstate.js";
import { fmtAlvorlighet } from "../../skannonser/web/static/listingmeta.js";

const META = { destinations: [] };

// A listing that passes every other filter, so an exclusion can only come
// from the field under test.
function listing(extra) {
  return {
    finnkode: "1",
    pris: 3_000_000,
    bra_i: 80,
    lat: 59.9,
    lng: 10.7,
    ...extra,
  };
}

function filtersWith(over) {
  return { ...defaultFilters(META), ...over };
}

// --- alvorlighet chip ---------------------------------------------------

test("defaultFilters ships alvorlighetSelected empty and reparasjonMax at its ceiling", () => {
  const f = defaultFilters(META);
  assert.deepEqual(f.alvorlighetSelected, []);
  assert.equal(f.reparasjonMax, REPARASJON_MAX);
});

test("alvorlighet: an empty selection filters nothing", () => {
  const f = filtersWith({});
  assert.equal(listingExcluded(listing({ alvorlighet: "alvorlig" }), f, META), false);
  assert.equal(listingExcluded(listing({ alvorlighet: null }), f, META), false);
});

test("alvorlighet: selecting a value admits it and excludes the others", () => {
  const f = filtersWith({ alvorlighetSelected: ["alvorlig"] });
  assert.equal(listingExcluded(listing({ alvorlighet: "alvorlig" }), f, META), false);
  assert.equal(listingExcluded(listing({ alvorlighet: "kosmetisk" }), f, META), true);
});

test("alvorlighet: an unclassified listing does NOT ride along with a real value", () => {
  const f = filtersWith({ alvorlighetSelected: ["alvorlig"] });
  assert.equal(f.includeUnknown, true, "precondition: unknowns are included by default");
  assert.equal(
    listingExcluded(listing({ alvorlighet: null }), f, META),
    true,
    'picking "alvorlig" must not also return every unclassified listing'
  );
  // includeUnknown must not rescue it either -- the bucket is explicit.
  assert.equal(
    listingExcluded(listing({ alvorlighet: null }), filtersWith({ alvorlighetSelected: ["alvorlig"], includeUnknown: true }), META),
    true
  );
});

test('alvorlighet: "Ukjent" ("") is selectable in its own right', () => {
  const f = filtersWith({ alvorlighetSelected: [""] });
  assert.equal(listingExcluded(listing({ alvorlighet: null }), f, META), false);
  assert.equal(listingExcluded(listing({ alvorlighet: undefined }), f, META), false);
  assert.equal(listingExcluded(listing({}), f, META), false, "field entirely absent counts as Ukjent too");
  assert.equal(listingExcluded(listing({ alvorlighet: "alvorlig" }), f, META), true);
});

test("alvorlighet: Ukjent stays selectable with includeUnknown off", () => {
  const f = filtersWith({ alvorlighetSelected: [""], includeUnknown: false });
  assert.equal(listingExcluded(listing({ alvorlighet: null }), f, META), false);
});

test("alvorlighet: a value and Ukjent can be selected together", () => {
  const f = filtersWith({ alvorlighetSelected: ["alvorlig", ""] });
  assert.equal(listingExcluded(listing({ alvorlighet: "alvorlig" }), f, META), false);
  assert.equal(listingExcluded(listing({ alvorlighet: null }), f, META), false);
  assert.equal(listingExcluded(listing({ alvorlighet: "kosmetisk" }), f, META), true);
});

test("alvorlighet: selectionExcludes is the routing (same primitive as energimerke/tag)", () => {
  assert.equal(selectionExcludes(["alvorlig"], "alvorlig"), false);
  assert.equal(selectionExcludes(["alvorlig"], ""), true);
  assert.equal(selectionExcludes([""], ""), false);
});

// --- reparasjon_est max-slider -------------------------------------------

test("reparasjon_est: at the ceiling the slider is off", () => {
  const f = filtersWith({});
  assert.equal(f.reparasjonMax, REPARASJON_MAX);
  assert.equal(listingExcluded(listing({ reparasjon_est: 50_000_000 }), f, META), false);
});

test("reparasjon_est: above the slider value is excluded", () => {
  const f = filtersWith({ reparasjonMax: 500_000 });
  assert.equal(listingExcluded(listing({ reparasjon_est: 400_000 }), f, META), false);
  assert.equal(listingExcluded(listing({ reparasjon_est: 500_000 }), f, META), false, "at the bound passes");
  assert.equal(listingExcluded(listing({ reparasjon_est: 500_001 }), f, META), true);
});

test("reparasjon_est: null passes while includeUnknown is on (the default)", () => {
  const f = filtersWith({ reparasjonMax: 500_000 });
  assert.equal(f.includeUnknown, true);
  assert.equal(listingExcluded(listing({ reparasjon_est: null }), f, META), false);
  assert.equal(listingExcluded(listing({}), f, META), false, "field entirely absent (DNB items) also passes");
});

test("reparasjon_est: null follows the SAME unknown policy as pris/totalpris, not a hardcoded pass", () => {
  // filters.js's overMax() helper -- shared by every money slider -- defers a
  // missing value to includeUnknown. reparasjon_est reuses that helper rather
  // than a bespoke "null always passes" rule, so switching includeUnknown off
  // must exclude it exactly like an unknown totalpris would.
  const f = filtersWith({ reparasjonMax: 500_000, includeUnknown: false });
  assert.equal(listingExcluded(listing({ reparasjon_est: null }), f, META), true);
  // Sanity: totalprisMax behaves identically for a missing totalpris, so this
  // is not a one-off rule invented for reparasjon_est.
  const g = filtersWith({ totalprisMax: 5_000_000, includeUnknown: false });
  assert.equal(listingExcluded(listing({ totalpris: null }), g, META), true);
});

// --- table wiring: same shared state, same option/label source -----------

import { COLUMN_FILTERS, isColumnFilterActive } from "../../skannonser/web/static/tablefilters.js";

test("alvorlighet column filter is wired to alvorlighetSelected and formats via fmtAlvorlighet", () => {
  const desc = COLUMN_FILTERS.alvorlighet;
  assert.ok(desc, "no COLUMN_FILTERS entry -- the column would get no filter button");
  assert.equal(desc.stateKey, "alvorlighetSelected");
  assert.equal(desc.vocab, "meta:alvorligheter");
  assert.equal(desc.unknownBucket, "Ukjent");
  assert.equal(desc.labelFn, fmtAlvorlighet, "chip text must match the cell/popup formatter");
});

test("reparasjon_est column filter is a slider-max bound at REPARASJON_MAX", () => {
  const desc = COLUMN_FILTERS.reparasjon_est;
  assert.ok(desc);
  assert.equal(desc.stateKey, "reparasjonMax");
  assert.equal(desc.kind, "slider-max");
  assert.equal(desc.bound(), REPARASJON_MAX);
});

test("both tilstand columns read as filtered exactly while something is set", () => {
  const ctx = { filters: defaultFilters(META), meta: { alvorligheter: [] }, vocabs: {} };
  assert.equal(isColumnFilterActive("alvorlighet", ctx), false);
  assert.equal(isColumnFilterActive("reparasjon_est", ctx), false);
  ctx.filters.alvorlighetSelected = ["alvorlig"];
  ctx.filters.reparasjonMax = 500_000;
  assert.equal(isColumnFilterActive("alvorlighet", ctx), true);
  assert.equal(isColumnFilterActive("reparasjon_est", ctx), true);
});
