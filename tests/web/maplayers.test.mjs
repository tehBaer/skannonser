import { test } from "node:test";
import assert from "node:assert/strict";
import { addListingGroups, buildGroups, setSoldColorMode } from "../../skannonser/web/static/map.js";

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

test("the inactive X is added after the closed dot it marks", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#111" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const x = map.added.findIndex((id) => id.endsWith("-inactive-x"));
  const sold = map.added.findIndex((id) => id.endsWith("-sold"));
  assert.ok(sold < x && x !== -1, "X must draw over its dot");
});

test("closed dots are hollow: no fill, a thick boligtype-coloured ring", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const specs = [];
  const map = fakeMap();
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  const sold = specs.find((s) => s.id.endsWith("-sold"));
  assert.equal(sold.paint["circle-opacity"], 0, "no fill");
  assert.equal(sold.paint["circle-stroke-color"], "#0f4c81", "ring carries the boligtype colour");
  assert.ok(sold.paint["circle-stroke-width"] >= 3, "ring must be thick enough to read");
});

test("budpremie mode recolours the ring, not the fill, and spares inactive dots", () => {
  // buildGroups always appends an "__unknown__" fallback type and splits each
  // type into active/sold/both variants (see buildGroups' own doc comment) --
  // neither is what this test is about, so narrow to the one Enebolig group
  // that actually carries hasSold, to isolate setSoldColorMode's per-group
  // stroke-colour behaviour from that unrelated fan-out.
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" }).filter(
    (g) => g.type === "Enebolig" && g.hasSold && !g.hasActive
  );
  const writes = [];
  const map = fakeMap();
  map.getLayer = () => ({});
  map.setPaintProperty = (id, prop, value) => writes.push({ id, prop, value });

  setSoldColorMode(map, groups, true);
  assert.ok(writes.length > 0, "at least one closed layer is recoloured");
  assert.ok(writes.every((w) => w.prop === "circle-stroke-color"),
    "hollow dots carry their colour on the stroke");
  assert.ok(JSON.stringify(writes[0].value).includes("#0f4c81"),
    "inactive dots keep the boligtype colour even in budpremie mode");

  writes.length = 0;
  setSoldColorMode(map, groups, false);
  assert.deepEqual(writes.map((w) => w.value), ["#0f4c81"]);
});

test("closed-only cluster bubbles are hollow, mixed clusters stay filled", () => {
  // buildGroups fans each boligtype into active/sold/both variants -- pick
  // the sold-only Enebolig group (hasSold && !hasActive) for the hollow
  // assertion, and the "both" variant (hasActive && hasSold) for the filled
  // one, since that's the only variant genuinely containing active listings.
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const soldOnly = groups.find((g) => g.type === "Enebolig" && g.hasSold && !g.hasActive);
  const both = groups.find((g) => g.type === "Enebolig" && g.hasActive && g.hasSold);
  assert.ok(soldOnly && both, "buildGroups must produce both a sold-only and a both variant");

  const specs = [];
  const map = fakeMap();
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  const soldCluster = specs.find((s) => s.id === soldOnly.id + "-cluster");
  const bothCluster = specs.find((s) => s.id === both.id + "-cluster");

  assert.equal(soldCluster.paint["circle-opacity"], 0, "closed-only cluster has no fill");
  assert.equal(soldCluster.paint["circle-stroke-color"], "#0f4c81",
    "closed-only cluster ring carries the boligtype colour");
  assert.ok(soldCluster.paint["circle-stroke-width"] > 2,
    "closed-only cluster ring must be thicker than the default cluster border");

  assert.notEqual(bothCluster.paint["circle-opacity"], 0, "mixed cluster keeps its fill");
  assert.equal(bothCluster.paint["circle-stroke-color"], "#111111",
    "mixed cluster keeps the dark border, not the hollow ring");
});

test("the tag ring hugs its dot and draws above every dot layer", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const specs = [];
  const map = fakeMap();
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  const ring = specs.find((s) => s.id.endsWith("-tagring"));
  const dot = specs.find((s) => s.id.endsWith("-eie"));
  const dotOuter = dot.paint["circle-radius"] + dot.paint["circle-stroke-width"];
  const gap = ring.paint["circle-radius"] - dotOuter;
  assert.ok(gap >= 0 && gap <= 2.5,
    `ring should hug the dot, got a ${gap}px gap`);

  const lastDot = Math.max(...map.added.flatMap((id, i) => (/-(eie|dnb|sold)$/.test(id) ? [i] : [])));
  const firstRing = Math.min(...map.added.flatMap((id, i) => (id.endsWith("-tagring") ? [i] : [])));
  assert.ok(firstRing > lastDot,
    "rings must draw above dots so a neighbouring dot cannot cover them");
});

test("clusters carry a tagged-count property and a proportional halo", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const specs = [];
  const sources = [];
  const map = fakeMap();
  map.addSource = (id, cfg) => sources.push({ id, cfg });
  map.addLayer = (spec) => { map.added.push(spec.id); specs.push(spec); };
  addListingGroups(map, groups, () => {});

  assert.ok(sources.every((s) => s.cfg.clusterProperties && s.cfg.clusterProperties.tag_sum),
    "every clustered source must aggregate a tagged count");

  const halo = specs.find((s) => s.id.endsWith("-cluster-tagring"));
  assert.ok(halo, "a cluster halo layer must exist");
  assert.equal(halo.paint["circle-opacity"], 0, "halo is a ring, not a disc");
  assert.ok(JSON.stringify(halo.filter).includes("point_count"),
    "halo applies to clusters only");
  assert.ok(JSON.stringify(halo.paint["circle-stroke-opacity"]).includes("tag_sum"),
    "halo strength must derive from the tagged fraction, not be a constant");
});

test("the cluster halo draws above the cluster bubble", () => {
  const groups = buildGroups(["Enebolig"], { Enebolig: "#0f4c81" });
  const map = fakeMap();
  addListingGroups(map, groups, () => {});
  const bubble = Math.max(...map.added.flatMap((id, i) => (id.endsWith("-cluster") ? [i] : [])));
  const halo = Math.min(...map.added.flatMap((id, i) => (id.endsWith("-cluster-tagring") ? [i] : [])));
  assert.ok(halo > bubble, "halo must not be hidden behind its own bubble");
});
