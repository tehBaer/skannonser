import { test } from "node:test";
import assert from "node:assert/strict";
import { deriveVocabs } from "../../skannonser/web/static/filters.js";

test("deriveVocabs counts only the items it is handed", () => {
  const v = deriveVocabs([
    { tag: "maybe", tilgjengelighet: null, postnummer: "0170", nabolag: "Sentrum" },
    { tag: null, tilgjengelighet: null, postnummer: "0170", nabolag: null },
  ]);
  const tags = Object.fromEntries(v.tags.map((o) => [o.key, o.count]));
  assert.equal(tags["maybe"], 1);
  assert.equal(tags[""], 1, "untagged items land in the \"\" bucket");
  assert.equal(v.postnummer.find((o) => o.key === "0170").count, 2);
});

test("a value carried only by an omitted item does not appear", () => {
  const v = deriveVocabs([{ tag: "maybe" }]);
  assert.ok(!v.tags.some((o) => o.key === "kun-solgt"));
});
