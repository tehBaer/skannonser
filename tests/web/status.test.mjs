import { test } from "node:test";
import assert from "node:assert/strict";
import { TILGJENGELIGHET_OPTIONS } from "../../skannonser/web/static/listingmeta.js";
import { wantsClosed } from "../../skannonser/web/static/filters.js";
import { seedStatus } from "../../skannonser/web/static/filterstate.js";

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
  assert.equal(seedStatus(f), f);
});
