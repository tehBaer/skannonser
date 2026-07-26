import { test } from "node:test";
import assert from "node:assert/strict";
import { stationPointFeatures } from "../../skannonser/web/static/stations.js";

const stations = [
  { name: "Sandvika", lat: 59.89, lng: 10.52, lines: ["L1", "R11"] },
  { name: "Asker", lat: 59.83, lng: 10.43, lines: ["L1"] },
  { name: "Uten koord", lat: null, lng: null, lines: ["L1"] },
];

test("one point per station, regardless of how many lines it serves", () => {
  const fc = stationPointFeatures(stations);
  assert.equal(fc.features.length, 2, "the coordinate-less station is skipped");
  assert.equal(fc.features.filter((f) => f.properties.name === "Sandvika").length, 1);
});

test("points carry the station name and a colour", () => {
  const fc = stationPointFeatures(stations);
  const sandvika = fc.features.find((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.geometry.type, "Point");
  assert.deepEqual(sandvika.geometry.coordinates, [10.52, 59.89]);
  assert.match(sandvika.properties.color, /^#/);
});
