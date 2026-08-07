// tests/web/popoverdismiss.test.mjs
// The shared popover singleton's click-away dismissal. Same no-jsdom approach
// as tagpicker.test.mjs, but this stub also has to model EVENT DISPATCH,
// because the bug this file pins lives in dispatch order rather than in any
// one function: the path is snapshotted when dispatch starts, so `document`
// still receives a click whose target a handler removed from the DOM in the
// meantime -- and a dismissal that asks "is ev.target inside the popover?"
// then answers "no" about a node that was inside it when the user clicked.
//
// filters.js registers its dismiss wiring at module init, guarded on
// `typeof document`, so the stub has to be installed BEFORE the import. Hence
// the dynamic import below.
import test from "node:test";
import assert from "node:assert/strict";

function stubDom() {
  const doc = { listeners: [], body: null };

  const make = (tagName) => {
    const node = {
      tagName,
      className: "",
      textContent: "",
      hidden: false,
      children: [],
      parentNode: null,
      listeners: [],
      style: { setProperty() {} },
      classList: { add() {}, contains: () => false },
      setAttribute() {},
      appendChild(c) {
        c.parentNode = this;
        this.children.push(c);
        return c;
      },
      remove() {
        if (!this.parentNode) return;
        const i = this.parentNode.children.indexOf(this);
        if (i >= 0) this.parentNode.children.splice(i, 1);
        this.parentNode = null;
      },
      // The one thing the dismissal actually asks a node. A detached subtree
      // is unreachable from here, which is exactly the browser's answer too.
      contains(other) {
        if (other === this) return true;
        return this.children.some((c) => c.contains(other));
      },
      addEventListener(type, fn, capture) {
        this.listeners.push({ type, fn, capture: Boolean(capture) });
      },
      // `pop.innerHTML = ""` is how the Status popover rebuilds its body.
      // Detaching the children is the part that matters: the node the user
      // clicked is no longer inside the popover afterwards.
      set innerHTML(v) {
        if (v !== "") throw new Error("stub only models innerHTML = ''");
        this.children.forEach((c) => (c.parentNode = null));
        this.children.length = 0;
      },
      get innerHTML() {
        return "";
      },
      getBoundingClientRect: () => ({ top: 0, left: 0, bottom: 20, right: 40 }),
    };
    return node;
  };

  doc.createElement = make;
  doc.addEventListener = (type, fn, capture) =>
    doc.listeners.push({ type, fn, capture: Boolean(capture) });
  doc.contains = (other) => doc.body.contains(other);
  doc.body = make("body");
  return { doc, make };
}

// A click, dispatched the way the DOM dispatches one: the propagation path is
// computed ONCE, up front, then a capture pass root->target and a bubble pass
// target->root. Nodes stay on the path even if a handler detaches them.
// `await null` between handlers stands in for the microtask checkpoint the
// browser runs there -- that is what lets an `async` handler's continuation
// (the Status popover's rebuild, which runs after `await
// ensureSoldForSelection()`) land between the chip's handler and document's.
async function dispatchClick(doc, target) {
  const path = [];
  for (let n = target; n; n = n.parentNode) path.push(n);
  path.push(doc);
  const ev = { type: "click", target };
  const run = (node, capture) => {
    (node.listeners || [])
      .filter((l) => l.type === "click" && l.capture === capture)
      .forEach((l) => l.fn(ev));
  };
  for (let i = path.length - 1; i >= 0; i--) {
    run(path[i], true);
    for (let k = 0; k < 5; k++) await null;
  }
  for (let i = 0; i < path.length; i++) {
    run(path[i], false);
    for (let k = 0; k < 5; k++) await null;
  }
}

// ONE stub document for the file, installed before the one import that
// registers the dismiss wiring against it. Per-test stubs would not work: the
// module is evaluated once, so its listeners stay bound to whichever document
// was global at import time -- a second stub would silently have no dismissal
// at all, and every test would "pass".
const { doc, make } = stubDom();
globalThis.document = doc;
globalThis.window = { innerWidth: 1280 };
const mod = await import("../../skannonser/web/static/filters.js");

async function withDom(t, fn) {
  t.after(() => {
    mod.closePopover();
    doc.body.children.forEach((c) => (c.parentNode = null));
    doc.body.children.length = 0;
  });
  return fn(mod, doc, make);
}

const isOpen = (doc) => doc.body.children.some((c) => c.className === "th-popover");

// The bug, in the shape the user hit it: click a status chip, watch the
// popover vanish, reopen it, click the next status, watch it vanish again.
test("a popover that rebuilds its own body in a click handler stays open", (t) =>
  withDom(t, async (mod, doc, make) => {
    const anchor = make("button");
    doc.body.appendChild(anchor);
    // Mirrors renderStatusPopover: the body is re-mounted from scratch inside
    // its own change handler, so the chip that was clicked is gone by the time
    // the click reaches document.
    const build = (pop) => {
      const chip = doc.createElement("button");
      chip.className = "tag-chip";
      chip.addEventListener("click", async () => {
        await Promise.resolve(); // the real handler awaits ensureSoldForSelection()
        pop.innerHTML = "";
        build(pop);
      });
      pop.appendChild(chip);
    };
    mod.openPopover(anchor, build);
    assert.ok(isOpen(doc), "precondition: the popover is open");

    const chip = doc.body.children.find((c) => c.className === "th-popover").children[0];
    await dispatchClick(doc, chip);

    assert.ok(isOpen(doc), "clicking a chip must not dismiss the popover it lives in");
  }));

// Same failure with a handler that re-renders synchronously -- the await is
// what makes it happen for Status, but it is not what makes it wrong.
test("a synchronous rebuild does not dismiss the popover either", (t) =>
  withDom(t, async (mod, doc, make) => {
    const anchor = make("button");
    doc.body.appendChild(anchor);
    const build = (pop) => {
      const chip = doc.createElement("button");
      chip.className = "tag-chip";
      chip.addEventListener("click", () => {
        pop.innerHTML = "";
        build(pop);
      });
      pop.appendChild(chip);
    };
    mod.openPopover(anchor, build);
    const chip = doc.body.children.find((c) => c.className === "th-popover").children[0];
    await dispatchClick(doc, chip);
    assert.ok(isOpen(doc));
  }));

// The other half of the contract: click-away must still work, or the fix is
// just "never dismiss".
test("a click outside still dismisses", (t) =>
  withDom(t, async (mod, doc, make) => {
    const anchor = make("button");
    const elsewhere = make("div");
    doc.body.appendChild(anchor);
    doc.body.appendChild(elsewhere);
    mod.openPopover(anchor, (pop) => pop.appendChild(doc.createElement("span")));
    assert.ok(isOpen(doc));
    await dispatchClick(doc, elsewhere);
    assert.equal(isOpen(doc), false, "an outside click closes the popover");
  }));

// Clicking the anchor is the toggle path: the dismissal must leave it alone so
// the anchor's own handler can decide (openPopover closes when re-anchored).
test("a click on the anchor is left to the anchor's own toggle", (t) =>
  withDom(t, async (mod, doc, make) => {
    const anchor = make("button");
    doc.body.appendChild(anchor);
    mod.openPopover(anchor, (pop) => pop.appendChild(doc.createElement("span")));
    await dispatchClick(doc, anchor);
    assert.ok(isOpen(doc), "the dismissal must not pre-empt the anchor's toggle");
    mod.openPopover(anchor, (pop) => pop.appendChild(doc.createElement("span")));
    assert.equal(isOpen(doc), false, "re-anchoring toggles it closed");
  }));
