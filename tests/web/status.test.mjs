import { test } from "node:test";
import assert from "node:assert/strict";
import { TILGJENGELIGHET_OPTIONS } from "../../skannonser/web/static/listingmeta.js";
import { wantsClosed } from "../../skannonser/web/static/filters.js";
import { seedStatus, activeFilterEntries } from "../../skannonser/web/static/filterstate.js";

test("the status vocabulary is fixed, not derived", () => {
  assert.deepEqual(TILGJENGELIGHET_OPTIONS, [
    { key: "", label: "Til salgs" },
    { key: "Solgt", label: "Solgt" },
    { key: "Inaktiv", label: "Inaktiv" },
    { key: "Trukket", label: "Trukket" },
  ]);
});

// The closed bucket is a separate lazy fetch. Selecting any non-"" status is
// what asks for it; "" (Til salgs) never does.
test("wantsClosed is false for an empty selection", () => {
  assert.equal(wantsClosed([]), false);
});

test("wantsClosed is false for Til salgs alone", () => {
  assert.equal(wantsClosed([""]), false);
});

test("wantsClosed is true for any closed status", () => {
  assert.equal(wantsClosed(["Solgt"]), true);
  assert.equal(wantsClosed(["Inaktiv"]), true);
  assert.equal(wantsClosed(["Trukket"]), true);
});

test("wantsClosed is true when a closed status rides along with Til salgs", () => {
  assert.equal(wantsClosed(["", "Inaktiv"]), true);
});

test("wantsClosed tolerates a missing selection", () => {
  assert.equal(wantsClosed(undefined), false);
  assert.equal(wantsClosed(null), false);
});

// The floor: an empty selection means "unfiltered", which combined with the
// lazy fetch would make a cold load and a post-reset load disagree.
test("seedStatus turns an empty selection into Til salgs", () => {
  const f = { tilgjengelighetSelected: [] };
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

test("seedStatus leaves a real selection alone", () => {
  const f = { tilgjengelighetSelected: ["Solgt"] };
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, ["Solgt"]);
});

test("seedStatus is idempotent", () => {
  const f = { tilgjengelighetSelected: [] };
  seedStatus(f);
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

test("seedStatus creates the array when the key is absent", () => {
  const f = {};
  seedStatus(f);
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

test("seedStatus mutates in place and returns the same object", () => {
  const f = { tilgjengelighetSelected: [] };
  const arr = f.tilgjengelighetSelected;
  assert.equal(seedStatus(f), f);
  // The regression this guards against: a reassigning seedStatus still
  // returns the same FILTERS object (this assertion alone used to pass
  // against the buggy version) while silently replacing the ARRAY, orphaning
  // every checkbox handler closed over the old reference. Only checking array
  // identity catches that.
  assert.equal(f.tilgjengelighetSelected, arr, "must mutate the existing array, not replace it");
});

test("seedStatus mutates the array in place even when creating it from scratch", () => {
  const f = {};
  seedStatus(f);
  const arr = f.tilgjengelighetSelected;
  f.tilgjengelighetSelected.length = 0; // simulate a handler emptying it
  seedStatus(f);
  assert.equal(f.tilgjengelighetSelected, arr, "re-seeding must not replace the array");
  assert.deepEqual(f.tilgjengelighetSelected, [""]);
});

// Finding 4: the [""] floor must not count as an active filter, or
// activeFilterCount can never reach 0 and "N filtre aktive" is permanently
// stuck at >= 1.
test("activeFilterEntries omits tilgjengelighetSelected for the seeded default [\"\"]", () => {
  const filters = { tilgjengelighetSelected: [""] };
  const entries = activeFilterEntries(filters, {});
  assert.equal(entries.find((e) => e.key === "tilgjengelighetSelected"), undefined);
});

test("activeFilterEntries still reports a real status selection", () => {
  for (const sel of [["Solgt"], ["", "Solgt"]]) {
    const filters = { tilgjengelighetSelected: sel };
    const entries = activeFilterEntries(filters, {});
    const entry = entries.find((e) => e.key === "tilgjengelighetSelected");
    assert.ok(entry, "selection " + JSON.stringify(sel) + " must be reported active");
    assert.match(entry.valueText, /\d+ valgt/);
  }
});

test("activeFilterEntries reports no status entry for an empty selection", () => {
  // [] is not the seeded floor, but selectedSet-style helpers only emit for
  // a non-empty array in the first place, so an empty selection is a no-op
  // here regardless -- confirmed by reading the shared selectedSet() helper
  // in filterstate.js rather than assumed.
  const filters = { tilgjengelighetSelected: [] };
  const entries = activeFilterEntries(filters, {});
  assert.equal(entries.find((e) => e.key === "tilgjengelighetSelected"), undefined);
});
