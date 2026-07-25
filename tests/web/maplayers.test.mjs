import { test } from "node:test";
import assert from "node:assert/strict";
import { addListingGroups, buildGroups } from "../../skannonser/web/static/map.js";

// Minimal stand-in for a MapLibre map: records the order layers are added.
function fakeMap() {
  const added = [];
  return {
    added,
    addSource() {},
    addLayer(spec) { added.push(spec.id); },
    getLayer() { return null; },
    hasImage() { return true; },
    addImage() {},
    on() {},
    getSource() { return null; },
  };
}

test("every active dot layer is added after every closed dot layer", () => {
  const groups = buildGroups(["Enebolig", "Leilighet"], { Enebolig: "#111", Leilighet: "#222" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});

  const lastClosed = Math.max(...map.added.flatMap((id, i) => (id.endsWith("-sold") ? [i] : [])));
  const firstActive = Math.min(
    ...map.added.flatMap((id, i) => (id.endsWith("-eie") || id.endsWith("-dnb") ? [i] : []))
  );
  assert.ok(lastClosed < firstActive,
    `closed layers must precede active ones; got last closed ${lastClosed}, first active ${firstActive}`);
});

test("tag rings sit beneath every dot layer", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const lastRing = Math.max(...map.added.flatMap((id, i) => (id.endsWith("-tagring") ? [i] : [])));
  const firstDot = Math.min(
    ...map.added.flatMap((id, i) => (/-(eie|dnb|sold)$/.test(id) ? [i] : []))
  );
  assert.ok(lastRing < firstDot, "rings must be added before dots so they read as haloes");
});

test("the inactive X is added after the closed dot it marks", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const x = map.added.findIndex((id) => id.endsWith("-inactive-x"));
  const sold = map.added.findIndex((id) => id.endsWith("-sold"));
  assert.ok(sold < x && x !== -1, "X must draw over its dot");
});
