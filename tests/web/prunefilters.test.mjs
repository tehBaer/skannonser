import { test } from "node:test";
import assert from "node:assert/strict";
import { pruneFilterSets } from "../../skannonser/web/static/filterstate.js";

const vocabs = {
  tags: [{ key: "" }, { key: "maybe" }],
  tilgjengelighet: [{ key: "" }],
  postnummer: [{ key: "0170" }],
  nabolag: [{ key: "Sentrum" }],
};

test("drops hidden keys that left the vocabulary", () => {
  const filters = { tagHidden: { maybe: true, "kun-solgt": true } };
  assert.equal(pruneFilterSets(filters, vocabs), true);
  assert.deepEqual(filters.tagHidden, { maybe: true });
});

test("drops selected values that left the vocabulary", () => {
  const filters = { postnummerSelected: ["0170", "9999"], nabolagSelected: ["Sentrum"] };
  assert.equal(pruneFilterSets(filters, vocabs), true);
  assert.deepEqual(filters.postnummerSelected, ["0170"]);
  assert.deepEqual(filters.nabolagSelected, ["Sentrum"]);
});

test("leaves meta-derived sets alone and reports no change", () => {
  const filters = { boligtypeHidden: { Leilighet: true }, eieformHidden: { Selveier: true } };
  assert.equal(pruneFilterSets(filters, vocabs), false);
  assert.deepEqual(filters.boligtypeHidden, { Leilighet: true });
  assert.deepEqual(filters.eieformHidden, { Selveier: true });
});

test("is a no-op when nothing is stale", () => {
  const filters = { tagHidden: { maybe: true }, postnummerSelected: ["0170"] };
  assert.equal(pruneFilterSets(filters, vocabs), false);
});
