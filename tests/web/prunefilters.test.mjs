import { test } from "node:test";
import assert from "node:assert/strict";
import { pruneFilterSets } from "../../skannonser/web/static/filterstate.js";

const vocabs = {
  tags: [{ key: "" }, { key: "maybe" }],
  tilgjengelighet: [{ key: "" }],
  postnummer: [{ key: "0170" }],
  nabolag: [{ key: "Sentrum" }],
};

// The third argument asserts the vocabulary covers every listing the page can
// hold; without it a prune is speculative and must not touch anything.
const COMPLETE = true;

test("drops selected tags that left the vocabulary", () => {
  const filters = { tagSelected: ["maybe", "kun-solgt"] };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), true);
  assert.deepEqual(filters.tagSelected, ["maybe"]);
});

test("drops selected values that left the vocabulary", () => {
  const filters = { postnummerSelected: ["0170", "9999"], nabolagSelected: ["Sentrum"] };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), true);
  assert.deepEqual(filters.postnummerSelected, ["0170"]);
  assert.deepEqual(filters.nabolagSelected, ["Sentrum"]);
});

test("leaves meta-derived sets alone and reports no change", () => {
  const filters = { boligtypeSelected: ["Leilighet"], eieformSelected: ["Selveier"] };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), false);
  assert.deepEqual(filters.boligtypeSelected, ["Leilighet"]);
  assert.deepEqual(filters.eieformSelected, ["Selveier"]);
});

test("is a no-op when nothing is stale", () => {
  const filters = { tagSelected: ["maybe"], postnummerSelected: ["0170"] };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), false);
});

// F3: the load path builds the filter UI from a knowingly partial vocabulary
// (map: before the closed bucket is fetched; table: before the "Vis solgte"
// pref is read). A prune there used to delete -- and immediately persist --
// every value that only closed listings carry.
test("an incomplete vocabulary never deletes a stored value", () => {
  const filters = {
    tagSelected: ["maybe", "kun-solgt"],
    tilgjengelighetSelected: ["Heis"],
    postnummerSelected: ["0170", "9999"],
    nabolagSelected: ["Sentrum", "Kun i solgte"],
  };
  assert.equal(
    pruneFilterSets(filters, vocabs, false),
    false,
    "must report no change so the caller does not persist"
  );
  assert.deepEqual(filters.tagSelected, ["maybe", "kun-solgt"]);
  assert.deepEqual(filters.tilgjengelighetSelected, ["Heis"]);
  assert.deepEqual(filters.postnummerSelected, ["0170", "9999"]);
  assert.deepEqual(filters.nabolagSelected, ["Sentrum", "Kun i solgte"]);
});

// Two clicks used to be enough to lose everything: switch every layer off and
// the vocabulary is empty, so the prune wiped the lot.
test("an empty vocabulary cannot wipe the selection", () => {
  const empty = { tags: [], tilgjengelighet: [], postnummer: [], nabolag: [] };
  const filters = { tagSelected: ["maybe"], postnummerSelected: ["0170"] };
  assert.equal(pruneFilterSets(filters, empty, false), false);
  assert.deepEqual(filters.tagSelected, ["maybe"]);
  assert.deepEqual(filters.postnummerSelected, ["0170"]);
});

// Defaulting to "safe" matters: a caller that forgets the flag must lose
// nothing rather than silently delete.
test("omitting the completeness flag defaults to not deleting", () => {
  const filters = { tagSelected: ["kun-solgt"] };
  assert.equal(pruneFilterSets(filters, vocabs), false);
  assert.deepEqual(filters.tagSelected, ["kun-solgt"]);
});

// Selections invert the stakes of pruning. Under the old hidden-sets, dropping
// the last stale key changed nothing visible -- the filter was already off. A
// selection holding only vocabulary-absent values matches ZERO listings, so
// pruning it flips the map from empty to full. That is the better of the two
// states (a filter for a value nobody has is not worth an empty map), but it is
// a visible jump and must not be "fixed" into something quieter by accident.
test("clearing an all-stale selection turns the filter off, not on", () => {
  const filters = { tagSelected: ["gone", "also-gone"] };
  assert.equal(pruneFilterSets(filters, vocabs, true), true);
  assert.deepEqual(filters.tagSelected, [],
    "an empty selection means everything shows -- the filter is off, not matching nothing");
});

test("a partially-stale selection keeps its surviving values", () => {
  const filters = { tagSelected: ["maybe", "gone"] };
  assert.equal(pruneFilterSets(filters, vocabs, true), true);
  assert.deepEqual(filters.tagSelected, ["maybe"]);
});
