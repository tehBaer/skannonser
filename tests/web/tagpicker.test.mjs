// tests/web/tagpicker.test.mjs
// The popup's coloured chip row. Same no-jsdom approach as
// tagoptions.test.mjs: a stand-in for exactly the document surface the module
// touches, so the DOM builder is testable without a browser.
import test from "node:test";
import assert from "node:assert/strict";
import { nextTagValue, buildTagPicker } from "../../skannonser/web/static/tagpicker.js";

function fakeDoc() {
  return {
    createElement(tagName) {
      const node = {
        tagName,
        className: "",
        type: "",
        value: "",
        placeholder: "",
        children: [],
        attrs: {},
        styleProps: {},
        listeners: {},
        appendChild(c) {
          this.children.push(c);
          return c;
        },
        addEventListener(ev, fn) {
          (this.listeners[ev] = this.listeners[ev] || []).push(fn);
        },
        setAttribute(k, v) {
          this.attrs[k] = v;
        },
        // Lets a test drive a handler the module registered.
        fire(ev, arg) {
          (this.listeners[ev] || []).forEach((fn) => fn(arg));
        },
        // "" is the clear-the-children idiom, same as tagoptions.test.mjs.
        set textContent(v) {
          if (v === "") this.children.length = 0;
          this._text = v;
        },
        get textContent() {
          return this._text || "";
        },
      };
      node.style = { setProperty: (k, v) => (node.styleProps[k] = v) };
      return node;
    },
  };
}

const VOCAB = ["wow", "fin", "nei"];
const COLORS = { wow: "#111111", fin: "#222222", nei: "#333333" };
const colorFor = (t) => COLORS[t] || null;

// node > [chip row, new-tag input]; chips are the row's children.
const chipsOf = (picker) => picker.node.children[0].children;

test("clicking an unselected tag selects it", () => {
  assert.equal(nextTagValue("fin", "wow"), "wow");
  assert.equal(nextTagValue("", "wow"), "wow");
  assert.equal(nextTagValue(null, "wow"), "wow");
});

test("clicking the selected tag clears it", () => {
  assert.equal(nextTagValue("wow", "wow"), "");
  assert.equal(nextTagValue("  WOW  ", "wow"), "", "normalization-insensitive");
});

test("chips render in the same sorted order the table dropdown uses", () => {
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick() {},
    doc: fakeDoc(),
  });
  assert.deepEqual(
    chipsOf(picker).map((c) => c.textContent),
    ["fin", "nei", "wow"]
  );
  assert.equal(picker.chipCount(), 3);
});

test("only the selected chip is filled, and every chip carries its colour", () => {
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick() {},
    doc: fakeDoc(),
  });
  const chips = chipsOf(picker);
  const [fin, nei, wow] = chips;
  assert.equal(fin.className, "tag-chip");
  assert.equal(nei.className, "tag-chip off");
  assert.equal(wow.className, "tag-chip off");
  assert.equal(fin.styleProps["--tag-color"], "#222222");
  assert.equal(wow.styleProps["--tag-color"], "#111111");
  assert.equal(fin.attrs["aria-pressed"], "true");
  assert.equal(nei.attrs["aria-pressed"], "false");
});

test("clicking a chip reports the resolved value, not the raw tag", () => {
  const picked = [];
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick: (v) => picked.push(v),
    doc: fakeDoc(),
  });
  const [fin, , wow] = chipsOf(picker);
  wow.fire("click");
  assert.deepEqual(picked, ["wow"]);
  fin.fire("click");
  assert.deepEqual(picked, ["wow", ""], "re-clicking the selected chip clears");
});

test("repaint moves the selection and picks up a new tag", () => {
  const picker = buildTagPicker({
    current: "fin",
    vocabulary: VOCAB,
    colorFor,
    onPick() {},
    doc: fakeDoc(),
  });
  picker.repaint("nei", [...VOCAB, "nytt"]);
  const chips = chipsOf(picker);
  assert.deepEqual(chips.map((c) => c.textContent), ["fin", "nei", "nytt", "wow"]);
  assert.equal(picker.chipCount(), 4);
  assert.equal(chips[1].className, "tag-chip");
  assert.equal(chips[0].className, "tag-chip off");
});

test("the new-tag field commits on Enter and then empties", () => {
  const picked = [];
  const picker = buildTagPicker({
    current: "",
    vocabulary: VOCAB,
    colorFor,
    onPick: (v) => picked.push(v),
    doc: fakeDoc(),
  });
  const input = picker.node.children[1];
  input.value = "  Helt Ny  ";
  assert.equal(picker.pendingNewTag(), "helt ny", "readable before it commits");
  input.fire("keydown", { key: "Enter", preventDefault() {} });
  assert.deepEqual(picked, ["helt ny"]);
  assert.equal(input.value, "");
  assert.equal(picker.pendingNewTag(), "");
});

test("the new-tag field ignores an empty commit", () => {
  const picked = [];
  const picker = buildTagPicker({
    current: "",
    vocabulary: VOCAB,
    colorFor,
    onPick: (v) => picked.push(v),
    doc: fakeDoc(),
  });
  const input = picker.node.children[1];
  input.value = "   ";
  input.fire("blur");
  assert.deepEqual(picked, [], "whitespace is not a tag");
});
