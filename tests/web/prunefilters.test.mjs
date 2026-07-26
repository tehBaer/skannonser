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

test("drops hidden keys that left the vocabulary", () => {
  const filters = { tagHidden: { maybe: true, "kun-solgt": true } };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), true);
  assert.deepEqual(filters.tagHidden, { maybe: true });
});

test("drops selected values that left the vocabulary", () => {
  const filters = { postnummerSelected: ["0170", "9999"], nabolagSelected: ["Sentrum"] };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), true);
  assert.deepEqual(filters.postnummerSelected, ["0170"]);
  assert.deepEqual(filters.nabolagSelected, ["Sentrum"]);
});

test("leaves meta-derived sets alone and reports no change", () => {
  const filters = { boligtypeHidden: { Leilighet: true }, eieformHidden: { Selveier: true } };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), false);
  assert.deepEqual(filters.boligtypeHidden, { Leilighet: true });
  assert.deepEqual(filters.eieformHidden, { Selveier: true });
});

test("is a no-op when nothing is stale", () => {
  const filters = { tagHidden: { maybe: true }, postnummerSelected: ["0170"] };
  assert.equal(pruneFilterSets(filters, vocabs, COMPLETE), false);
});

// F3: the load path builds the filter UI from a knowingly partial vocabulary
// (map: before the closed bucket is fetched; table: before the "Vis solgte"
// pref is read). A prune there used to delete -- and immediately persist --
// every value that only closed listings carry.
test("an incomplete vocabulary never deletes a stored value", () => {
  const filters = {
    tagHidden: { maybe: true, "kun-solgt": true },
    tilgjengelighetHidden: { Heis: true },
    postnummerSelected: ["0170", "9999"],
    nabolagSelected: ["Sentrum", "Kun i solgte"],
  };
  assert.equal(
    pruneFilterSets(filters, vocabs, false),
    false,
    "must report no change so the caller does not persist"
  );
  assert.deepEqual(filters.tagHidden, { maybe: true, "kun-solgt": true });
  assert.deepEqual(filters.tilgjengelighetHidden, { Heis: true });
  assert.deepEqual(filters.postnummerSelected, ["0170", "9999"]);
  assert.deepEqual(filters.nabolagSelected, ["Sentrum", "Kun i solgte"]);
});

// Two clicks used to be enough to lose everything: switch every layer off and
// the vocabulary is empty, so the prune wiped the lot.
test("an empty vocabulary cannot wipe the selection", () => {
  const empty = { tags: [], tilgjengelighet: [], postnummer: [], nabolag: [] };
  const filters = { tagHidden: { maybe: true }, postnummerSelected: ["0170"] };
  assert.equal(pruneFilterSets(filters, empty, false), false);
  assert.deepEqual(filters.tagHidden, { maybe: true });
  assert.deepEqual(filters.postnummerSelected, ["0170"]);
});

// Defaulting to "safe" matters: a caller that forgets the flag must lose
// nothing rather than silently delete.
test("omitting the completeness flag defaults to not deleting", () => {
  const filters = { tagHidden: { "kun-solgt": true } };
  assert.equal(pruneFilterSets(filters, vocabs), false);
  assert.deepEqual(filters.tagHidden, { "kun-solgt": true });
});
