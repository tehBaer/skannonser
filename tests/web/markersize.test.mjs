import { test } from "node:test";
import assert from "node:assert/strict";
import { DOT_R, SQUARE_PX, addListingGroups, buildGroups } from "../../skannonser/web/static/map.js";

test("the dot radius is exported as a single source of truth", () => {
  assert.equal(typeof DOT_R, "number");
  assert.ok(DOT_R >= 8, "round 3 enlarges the active dot from its old radius of 7");
});

// Minimal stand-in for a MapLibre map: records the specs layers are added with.
function fakeMap() {
  const specs = [];
  return {
    specs,
    addSource() {},
    addLayer(spec) { specs.push(spec); },
    getLayer() { return null; },
    hasImage() { return true; },
    addImage() {},
    on() {},
    getSource() { return null; },
  };
}

test("marker geometry keeps its derived relationships: closed < DOT_R < ring, square > dot diameter", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});

  const eie = map.specs.find((s) => s.id.endsWith("-eie"));
  const sold = map.specs.find((s) => s.id.endsWith("-sold"));
  const ring = map.specs.find((s) => s.id.endsWith("-tagring"));

  assert.equal(eie.paint["circle-radius"], DOT_R);
  assert.ok(sold.paint["circle-radius"] < DOT_R, "closed dot must stay smaller than the active dot");
  assert.ok(ring.paint["circle-radius"] > DOT_R, "tag ring must stay larger than the dot so it reads as a halo");
  assert.ok(SQUARE_PX > DOT_R * 2, "the DNB square raster must stay larger than the dot's diameter");
});
