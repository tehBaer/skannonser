// tests/web/annotations.test.mjs
// The save contract shared by the table's inline cells and the map popup's
// editor. The skip-when-unchanged behaviour is the load-bearing part: see the
// updated_at note in annotations.js.
import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeAnnotationValue,
  annotationChanged,
  commitAnnotation,
} from "../../skannonser/web/static/annotations.js";

// Records every PUT so a test can assert that none happened.
function stubFetch(response) {
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    calls.push({ url, body: JSON.parse(opts.body) });
    return { ok: true, json: async () => response };
  };
  return calls;
}

test("every blank form normalizes to the same value", () => {
  assert.equal(normalizeAnnotationValue(""), null);
  assert.equal(normalizeAnnotationValue("   "), null);
  assert.equal(normalizeAnnotationValue(null), null);
  assert.equal(normalizeAnnotationValue(undefined), null);
  assert.equal(normalizeAnnotationValue("  fin  "), "fin");
});

test("a blur that retyped the same value is not a change", () => {
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  assert.equal(annotationChanged(item, "", "fin"), false);
  assert.equal(annotationChanged(item, "   ", "  fin  "), false);
  assert.equal(annotationChanged(item, "ny", "fin"), true);
  assert.equal(annotationChanged(item, "", ""), true); // clearing the tag
});

test("commitAnnotation issues no PUT when nothing changed", async () => {
  const calls = stubFetch({ finnkode: "1", kommentar: null, tag: "fin" });
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  const saved = await commitAnnotation(item, { kommentar: "  ", tag: "fin" });
  assert.equal(saved, null);
  assert.equal(calls.length, 0, "a no-op PUT would bump updated_at");
});

test("commitAnnotation saves and mirrors the server's values into the item", async () => {
  const calls = stubFetch({ finnkode: "1", kommentar: "ny", tag: "wow" });
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  const saved = await commitAnnotation(item, { kommentar: "  ny  ", tag: "wow" });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].body, { kommentar: "ny", tag: "wow" });
  assert.equal(saved.tag, "wow");
  assert.equal(item.kommentar, "ny");
  assert.equal(item.tag, "wow");
});

test("a failed PUT leaves the item untouched so the next blur retries", async () => {
  globalThis.fetch = async () => ({ ok: false, status: 500 });
  const item = { finnkode: "1", kommentar: null, tag: "fin" };
  await assert.rejects(() => commitAnnotation(item, { kommentar: "ny", tag: "fin" }));
  assert.equal(item.kommentar, null, "item must stay dirty");
});
