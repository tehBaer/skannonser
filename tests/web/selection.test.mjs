import { test } from "node:test";
import assert from "node:assert/strict";
import { selectionExcludes, listingExcluded } from "../../skannonser/web/static/filters.js";
import { defaultFilters, activeFilterEntries } from "../../skannonser/web/static/filterstate.js";

test("an empty selection excludes nothing", () => {
  assert.equal(selectionExcludes([], "Leilighet"), false);
  assert.equal(selectionExcludes(undefined, "Leilighet"), false);
});

test("a non-empty selection excludes everything not in it", () => {
  assert.equal(selectionExcludes(["Leilighet"], "Leilighet"), false);
  assert.equal(selectionExcludes(["Leilighet"], "Enebolig"), true);
});

test('"" is a real selectable value, not "unknown"', () => {
  assert.equal(selectionExcludes([""], ""), false, "selecting the empty bucket keeps it");
  assert.equal(selectionExcludes([""], "maybe"), true);
  assert.equal(selectionExcludes(["maybe"], ""), true, "untagged is excluded when a tag is selected");
});

test("the predicate routes the explicit-value filters through selection", () => {
  const base = defaultFilters({ destinations: [] });
  const meta = {};
  const mk = (over) => ({ ...base, ...over });

  const leilighet = { boligtype: "Leilighet", tag: "maybe" };
  const enebolig = { boligtype: "Enebolig", tag: null };

  assert.equal(listingExcluded(leilighet, mk({}), meta), false, "no selection = everything passes");
  assert.equal(listingExcluded(enebolig, mk({ boligtypeSelected: ["Leilighet"] }), meta), true);
  assert.equal(listingExcluded(leilighet, mk({ boligtypeSelected: ["Leilighet"] }), meta), false);
  assert.equal(listingExcluded(enebolig, mk({ tagSelected: [""] }), meta), false, "untagged selected");
  assert.equal(listingExcluded(leilighet, mk({ tagSelected: [""] }), meta), true);

  // The one case that actually distinguishes the two helpers: with
  // includeUnknown OFF, an untagged listing must still pass when "" is the
  // selected value. Routing tags through the unknown-aware helper instead
  // would return unknownFails here and make the empty bucket unselectable --
  // the exact failure the split exists to prevent.
  assert.equal(
    listingExcluded(enebolig, mk({ tagSelected: [""], includeUnknown: false }), meta),
    false,
    "the empty bucket stays selectable when unknowns are excluded"
  );
});

test("Energimerking has an explicit Ukjent bucket, not an unknown-policy hole", () => {
  const base = defaultFilters({ destinations: [] });
  const meta = {};
  const mk = (over) => ({ ...base, ...over });

  const gradeA = { energimerke: "A" };
  const gradeC = { energimerke: "C" };
  const ungraded = { energimerke: null };

  assert.equal(listingExcluded(ungraded, mk({}), meta), false, "no selection = everything passes");

  // The reported bug: picking A returned ungraded listings too, because a null
  // grade was "unknown" and includeUnknown (default true) waved it through.
  // ~70 % of listings were ungraded, so "A" was mostly not-A.
  assert.equal(listingExcluded(gradeA, mk({ energiSelected: ["A"] }), meta), false);
  assert.equal(listingExcluded(gradeC, mk({ energiSelected: ["A"] }), meta), true);
  assert.equal(
    listingExcluded(ungraded, mk({ energiSelected: ["A"] }), meta),
    true,
    "an ungraded listing must NOT survive a grade selection"
  );
  // ...and includeUnknown must not rescue it either: the bucket is explicit now.
  assert.equal(
    listingExcluded(ungraded, mk({ energiSelected: ["A"], includeUnknown: true }), meta),
    true,
    "includeUnknown no longer governs Energimerking"
  );

  // Selecting Ukjent isolates exactly the ungraded ones, and stays selectable
  // with unknowns switched off (same rule the tag "" bucket relies on).
  assert.equal(listingExcluded(ungraded, mk({ energiSelected: [""] }), meta), false);
  assert.equal(listingExcluded(gradeA, mk({ energiSelected: [""] }), meta), true);
  assert.equal(
    listingExcluded(ungraded, mk({ energiSelected: [""], includeUnknown: false }), meta),
    false,
    "the Ukjent bucket stays selectable when unknowns are excluded"
  );

  // A grade and Ukjent together = union of both.
  assert.equal(listingExcluded(ungraded, mk({ energiSelected: ["A", ""] }), meta), false);
  assert.equal(listingExcluded(gradeA, mk({ energiSelected: ["A", ""] }), meta), false);
  assert.equal(listingExcluded(gradeC, mk({ energiSelected: ["A", ""] }), meta), true);

  // An absent field is the same case as an explicit null -- the API omits
  // energimerke entirely on DNB items, which have no grade at all.
  assert.equal(listingExcluded({}, mk({ energiSelected: ["A"] }), meta), true);
  assert.equal(listingExcluded({}, mk({ energiSelected: [""] }), meta), false);
});

test("a non-empty selection counts as an active filter and clears back to empty", () => {
  const base = defaultFilters({ destinations: [] });
  base.tagSelected = ["maybe", "hard no"];
  const entries = activeFilterEntries(base, {});
  const tag = entries.find((e) => e.key === "tagSelected");
  assert.ok(tag, "a selection must appear in the active-filter list");
  assert.match(tag.valueText, /2/);
  tag.clear(base);
  assert.deepEqual(base.tagSelected, []);
});

test("defaultFilters ships the five selections empty and no *Hidden keys", () => {
  const f = defaultFilters({ destinations: [] });
  ["boligtypeSelected", "eieformSelected", "energiSelected",
   "tilgjengelighetSelected", "tagSelected"].forEach((k) => {
    assert.deepEqual(f[k], [], k + " starts empty");
  });
  Object.keys(f).forEach((k) => {
    assert.ok(!/Hidden$/.test(k), "no hidden-set key survives: " + k);
  });
});
