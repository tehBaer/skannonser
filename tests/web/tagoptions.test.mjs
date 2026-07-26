import { test } from "node:test";
import assert from "node:assert/strict";
import {
  TAG_LIST_ID,
  tagOptionValues,
  syncTagOptions,
  attachTagList,
} from "../../skannonser/web/static/tagoptions.js";

// Minimal stand-in for the bits of `document` syncTagOptions touches. Same
// spirit as maplayers.test.mjs's fakeMap: no jsdom, just the surface used.
function fakeDoc() {
  const body = {
    children: [],
    appendChild(child) { this.children.push(child); return child; },
  };
  return {
    body,
    created: 0,
    createElement(tagName) {
      this.created += 1;
      return {
        tagName,
        id: "",
        value: "",
        children: [],
        appendChild(child) { this.children.push(child); return child; },
        // The only writes syncTagOptions makes; "" is its clear-the-list idiom.
        set textContent(v) { if (v === "") this.children.length = 0; },
        get textContent() { return ""; },
      };
    },
    getElementById(id) { return body.children.find((c) => c.id === id) || null; },
  };
}

function fakeInput() {
  return { attrs: {}, setAttribute(k, v) { this.attrs[k] = v; } };
}

test("option values are normalized, deduped and sorted like the tag colours", () => {
  assert.deepEqual(
    tagOptionValues(["Kanskje", "nei", "kanskje", "  NEI  ", "interessant"]),
    ["interessant", "kanskje", "nei"]
  );
});

test("empty tags never become options", () => {
  // Most listings carry no tag at all, and deriveVocabs keeps a bucket for
  // them -- an empty option would be an invisible row in the dropdown.
  assert.deepEqual(tagOptionValues(["", null, undefined, "   ", "ja"]), ["ja"]);
});

test("syncTagOptions builds one datalist and fills it", () => {
  const doc = fakeDoc();
  const values = syncTagOptions(["nei", "kanskje"], doc);

  const list = doc.getElementById(TAG_LIST_ID);
  assert.ok(list, "a datalist must be appended to the body");
  assert.equal(list.tagName, "datalist");
  assert.deepEqual(values, ["kanskje", "nei"]);
  assert.deepEqual(list.children.map((o) => o.value), ["kanskje", "nei"]);
});

test("a later sync replaces the options instead of appending to them", () => {
  // Saving an annotation re-derives the vocabulary and syncs again; without a
  // clear, every save would leave the previous list stacked underneath.
  const doc = fakeDoc();
  syncTagOptions(["nei"], doc);
  syncTagOptions(["nei", "kanskje"], doc);

  const lists = doc.body.children.filter((c) => c.id === TAG_LIST_ID);
  assert.equal(lists.length, 1, "the datalist is created once, not per sync");
  assert.deepEqual(lists[0].children.map((o) => o.value), ["kanskje", "nei"]);
});

test("a tag dropped from the data drops out of the dropdown", () => {
  const doc = fakeDoc();
  syncTagOptions(["nei", "kanskje"], doc);
  syncTagOptions(["kanskje"], doc);
  assert.deepEqual(
    doc.getElementById(TAG_LIST_ID).children.map((o) => o.value),
    ["kanskje"]
  );
});

test("attachTagList points an input at the list syncTagOptions builds", () => {
  // The two halves only meet through this id -- renaming one side silently
  // leaves an input pointing at a datalist that does not exist.
  const doc = fakeDoc();
  syncTagOptions(["ja"], doc);
  const input = fakeInput();
  attachTagList(input);
  assert.equal(input.attrs.list, doc.getElementById(TAG_LIST_ID).id);
});
