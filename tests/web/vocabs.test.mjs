import { test } from "node:test";
import assert from "node:assert/strict";
import { deriveVocabs, statusVocabComplete } from "../../skannonser/web/static/filters.js";

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

// vocabIsComplete gates pruneFilterSets, which DELETES stored filter values
// shared with the table. It must be false whenever a status is switched off,
// because a value can be absent from the vocabulary while very much existing.
test("statusVocabComplete needs every status selected", () => {
  assert.equal(statusVocabComplete(["", "Solgt", "Inaktiv", "Trukket"]), true);
});

test("statusVocabComplete is false when any status is missing", () => {
  assert.equal(statusVocabComplete([""]), false);
  assert.equal(statusVocabComplete(["", "Solgt", "Inaktiv"]), false);
  assert.equal(statusVocabComplete(["Solgt", "Inaktiv", "Trukket"]), false);
});

// An empty selection means "unfiltered" -> every status is visible -> the
// vocabulary IS complete. seedStatus normally prevents this state, but the
// predicate must be correct on its own rather than relying on that.
test("statusVocabComplete is true for an empty selection", () => {
  assert.equal(statusVocabComplete([]), true);
});

test("statusVocabComplete ignores order and duplicates", () => {
  assert.equal(statusVocabComplete(["Trukket", "", "Solgt", "Solgt", "Inaktiv"]), true);
});
