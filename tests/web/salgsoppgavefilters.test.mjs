// tests/web/salgsoppgavefilters.test.mjs
// Chip filters for the three salgsoppgave enums (ferdigattest / utleie /
// husdyr).
//
// These deliberately follow ENERGIMERKING's routing, not eieform's. The two
// differ in how a missing value is treated, and the difference is not cosmetic
// here: `null` means the listing's prospectus was never parsed, and that is
// ~36 % of live listings (314 of 868 have no ferdigattest value). Under
// eieform's routing a missing value defers to `includeUnknown`, which is on by
// default -- so picking "Ja" would return every unparsed listing too, and "Ja"
// would be mostly not-Ja. filters.js's own comment records that exact bug
// being fixed for energimerking on 2026-07-27; these tests stop it being
// reintroduced here.

import { test } from "node:test";
import assert from "node:assert/strict";
import { listingExcluded } from "../../skannonser/web/static/filters.js";
import { defaultFilters } from "../../skannonser/web/static/filterstate.js";

const META = { destinations: [] };

// A listing that passes every other filter, so an exclusion can only come from
// the field under test.
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

function filtersWith(key, selected) {
  const f = defaultFilters(META);
  f[key] = selected;
  return f;
}

const CASES = [
  {
    field: "ferdigattest",
    key: "ferdigattestSelected",
    value: "ferdigattest",
    other: "ingen",
  },
  { field: "utleie", key: "utleieSelected", value: "tillatt", other: "ikke_tillatt" },
  { field: "husdyr", key: "husdyrSelected", value: "tillatt", other: "ikke_tillatt" },
];

for (const { field, key, value, other } of CASES) {
  test(`${field}: an empty selection filters nothing`, () => {
    const f = filtersWith(key, []);
    assert.equal(listingExcluded(listing({ [field]: value }), f, META), false);
    assert.equal(listingExcluded(listing({ [field]: null }), f, META), false);
  });

  test(`${field}: selecting a value admits it and excludes the others`, () => {
    const f = filtersWith(key, [value]);
    assert.equal(listingExcluded(listing({ [field]: value }), f, META), false);
    assert.equal(listingExcluded(listing({ [field]: other }), f, META), true);
  });

  test(`${field}: an unparsed listing does NOT ride along with a real value`, () => {
    // The energimerking bug, restated for this field: includeUnknown is on by
    // default, and if the value were routed through it, this would pass.
    const f = filtersWith(key, [value]);
    assert.equal(f.includeUnknown, true, "precondition: unknowns are included by default");
    assert.equal(
      listingExcluded(listing({ [field]: null }), f, META),
      true,
      `picking "${value}" must not also return every unparsed listing`
    );
  });

  test(`${field}: "Ukjent" is selectable in its own right`, () => {
    const f = filtersWith(key, [""]);
    assert.equal(listingExcluded(listing({ [field]: null }), f, META), false);
    assert.equal(listingExcluded(listing({ [field]: undefined }), f, META), false);
    assert.equal(listingExcluded(listing({ [field]: value }), f, META), true);
  });

  test(`${field}: a value and Ukjent can be selected together`, () => {
    const f = filtersWith(key, [value, ""]);
    assert.equal(listingExcluded(listing({ [field]: value }), f, META), false);
    assert.equal(listingExcluded(listing({ [field]: null }), f, META), false);
    assert.equal(listingExcluded(listing({ [field]: other }), f, META), true);
  });
}

test("the three filters compose: each narrows independently", () => {
  const f = defaultFilters(META);
  f.ferdigattestSelected = ["ferdigattest"];
  f.utleieSelected = ["tillatt"];

  const passes = listing({ ferdigattest: "ferdigattest", utleie: "tillatt" });
  assert.equal(listingExcluded(passes, f, META), false);

  // Right ferdigattest, wrong utleie -> still excluded.
  const wrongUtleie = listing({ ferdigattest: "ferdigattest", utleie: "ikke_tillatt" });
  assert.equal(listingExcluded(wrongUtleie, f, META), true);
});

test("defaultFilters ships the three sets, so a fresh profile has them off", () => {
  const f = defaultFilters(META);
  for (const { key } of CASES) {
    assert.deepEqual(f[key], [], `${key} must default to an empty selection`);
  }
});

// --- one source of truth for the labels --------------------------------------
// The same three vocabularies are rendered in three places: the popup row, the
// map filter chip and the table column popover. They are built from one object
// so a label cannot drift between them -- these pin that, and pin that every
// list carries the Ukjent bucket the filter routing depends on.

import {
  FERDIGATTEST_OPTIONS,
  UTLEIE_OPTIONS,
  HUSDYR_OPTIONS,
  fmtFerdigattest,
  fmtUtleie,
  fmtHusdyr,
} from "../../skannonser/web/static/listingmeta.js";
import { COLUMN_FILTERS, isColumnFilterActive } from "../../skannonser/web/static/tablefilters.js";

const OPTION_SETS = [
  { name: "ferdigattest", options: FERDIGATTEST_OPTIONS, fmt: fmtFerdigattest },
  { name: "utleie", options: UTLEIE_OPTIONS, fmt: fmtUtleie },
  { name: "husdyr", options: HUSDYR_OPTIONS, fmt: fmtHusdyr },
];

for (const { name, options, fmt } of OPTION_SETS) {
  test(`${name}: every option label matches what the cell/popup would render`, () => {
    for (const { key, label } of options) {
      if (key === "") continue; // the Ukjent bucket has no formatter counterpart
      assert.equal(
        fmt(key),
        label,
        `chip says "${label}" but the popup would say "${fmt(key)}"`
      );
    }
  });

  test(`${name}: the option list ends with the Ukjent bucket`, () => {
    const last = options[options.length - 1];
    assert.equal(last.key, "", "the unknown bucket must be selectable");
    assert.equal(last.label, "Ukjent");
  });

  test(`${name}: the table column is wired to the same state key`, () => {
    const desc = COLUMN_FILTERS[name];
    assert.ok(desc, `no COLUMN_FILTERS entry -- the column would get no filter button`);
    assert.equal(desc.stateKey, `${name}Selected`);
    assert.equal(desc.options, options, "must reuse the shared list, not a copy");
  });

  test(`${name}: the column reads as filtered exactly while something is picked`, () => {
    const ctx = { filters: defaultFilters(META), meta: {}, vocabs: {} };
    assert.equal(isColumnFilterActive(name, ctx), false);
    ctx.filters[`${name}Selected`] = [options[0].key];
    assert.equal(isColumnFilterActive(name, ctx), true);
  });
}
