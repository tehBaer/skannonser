import { test } from "node:test";
import assert from "node:assert/strict";
import {
  DOT_R,
  SQUARE_PX,
  CLUSTER_RADIUS_STOPS,
  clusterBubbleRadius,
  clusterSize,
  addListingGroups,
  buildGroups,
} from "../../skannonser/web/static/map.js";

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

test("the closed ring has the active dot's footprint and a band you can see", () => {
  // "Sold looks dimmed and smaller" (owner, 2026-07-26) was neither opacity nor
  // size: at 0 % sold-dim the features carry op=1, and the ring already ended
  // on the active dot's outer edge. It was the 3px band -- a hollow hairline
  // reads faint next to a filled disc. Both halves of that are pinned here.
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});

  const eie = map.specs.find((s) => s.id.endsWith("-eie"));
  const sold = map.specs.find((s) => s.id.endsWith("-sold"));
  // GL strokes are drawn OUTSIDE circle-radius, so this sum is the outer edge.
  const outer = (s) => s.paint["circle-radius"] + s.paint["circle-stroke-width"];
  assert.equal(outer(sold), outer(eie),
    `closed and active must occupy the same circle -- filled vs hollow is the only difference; got ${outer(sold)} vs ${outer(eie)}`);
  assert.ok(sold.paint["circle-stroke-width"] >= 4,
    `the ring band must stay thick enough to read, got ${sold.paint["circle-stroke-width"]}px`);
});

// The DOM count marker's progress arc is sized from the GL bubble, so the
// number clusterBubbleRadius() returns and the expression the GL layer is
// built with MUST come from the same stops. If someone edits the layer's
// radius inline again, this fails.
test("the GL cluster radius expression is built from CLUSTER_RADIUS_STOPS", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const cluster = map.specs.find((s) => s.id.endsWith("-cluster"));
  assert.deepEqual(cluster.paint["circle-radius"], [
    "interpolate", ["linear"], ["get", "point_count"], ...CLUSTER_RADIUS_STOPS,
  ]);
});

test("clusterBubbleRadius evaluates the stops the way MapLibre's interpolate does", () => {
  // exactly on each stop
  for (let i = 0; i < CLUSTER_RADIUS_STOPS.length; i += 2) {
    assert.equal(clusterBubbleRadius(CLUSTER_RADIUS_STOPS[i]), CLUSTER_RADIUS_STOPS[i + 1]);
  }
  // clamped outside the first/last stop, not extrapolated
  assert.equal(clusterBubbleRadius(1), 14);
  assert.equal(clusterBubbleRadius(5000), 30);
  // linear in between: halfway from (25,19) to (100,25) in x is halfway in y
  assert.ok(Math.abs(clusterBubbleRadius(62.5) - 22) < 1e-9);
});

// The regression that made the arc float clear of its bubble: the arc was
// sized off clusterSize()'s box, which tracks a different curve.
test("the marker box and the bubble are different sizes, so the arc must use the bubble", () => {
  const boxD = clusterSize(50);
  const bubbleD = 2 * clusterBubbleRadius(50);
  assert.ok(boxD - bubbleD > 8,
    `box ${boxD}px vs bubble ${bubbleD}px -- an arc sized off the box misses the bubble`);
});
