import { test } from "node:test";
import assert from "node:assert/strict";
import {
  stationPointFeatures,
  stationCircleFeatures,
  lineColor,
  visibleLineSet,
} from "../../skannonser/web/static/stations.js";

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

test("a multi-line station produces exactly one radius polygon", () => {
  const fc = stationCircleFeatures(stations);
  assert.equal(fc.features.length, 2, "two stations have coordinates");
  const sandvika = fc.features.filter((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.length, 1, "L1 and R11 must not stack identical circles");
  assert.equal(sandvika[0].geometry.type, "Polygon");
  assert.ok(sandvika[0].properties.lines.includes("L1"));
  assert.ok(sandvika[0].properties.lines.includes("R11"));
});

// Point hovers must read identically to radius hovers (both derive their line
// suffix from the same `lines` property) -- see wireStationNamePopup.
test("points also carry the comma-joined lines property, same as radius polygons", () => {
  const fc = stationPointFeatures(stations);
  const sandvika = fc.features.find((f) => f.properties.name === "Sandvika");
  assert.ok(sandvika.properties.lines.includes("L1"));
  assert.ok(sandvika.properties.lines.includes("R11"));
});

// A station takes the colour of the first line the user can still SEE. Hiding
// L1 and keeping R11 must not leave Sandvika painted in L1's colour -- the
// line chips carry these same colours, so it named a switched-off line.
test("stations are coloured by their first VISIBLE line, not simply their first", () => {
  const l1Only = stationPointFeatures(stations, new Set(["L1"]));
  const r11Only = stationPointFeatures(stations, new Set(["R11"]));
  const sandvikaL1 = l1Only.features.find((f) => f.properties.name === "Sandvika");
  const sandvikaR11 = r11Only.features.find((f) => f.properties.name === "Sandvika");
  assert.equal(sandvikaL1.properties.color, lineColor("L1"));
  assert.equal(sandvikaR11.properties.color, lineColor("R11"));
  assert.notEqual(sandvikaL1.properties.color, sandvikaR11.properties.color);
});

test("radius polygons follow the same visible-line colour rule as the points", () => {
  const fc = stationCircleFeatures(stations, new Set(["R11"]));
  const sandvika = fc.features.find((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.properties.color, lineColor("R11"));
});

test("with no visible line left, the colour falls back to the first line", () => {
  // Reachable in passing: updateStationLayers drops such stations from the
  // source, but a station whose only lines are hidden must not throw or lose
  // its colour on the way out.
  const fc = stationPointFeatures(stations, new Set(["R99"]));
  const sandvika = fc.features.find((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.properties.color, lineColor("L1"));
});

test("omitting visibleLines keeps the previous first-line behaviour", () => {
  const fc = stationPointFeatures(stations);
  const sandvika = fc.features.find((f) => f.properties.name === "Sandvika");
  assert.equal(sandvika.properties.color, lineColor("L1"));
});

// visibleLineSet is now SELECTION-shaped like every other value filter
// (2026-07-26): an empty selection means all, never none. The inverted
// lineHidden map is gone.
test("an empty line selection means every line is visible", () => {
  const ui = { _allLines: ["L1", "R11", "L13"], stations: { lineSelected: [] } };
  assert.deepEqual([...visibleLineSet(ui)].sort(), ["L1", "L13", "R11"]);
});

test("a non-empty selection means only those lines", () => {
  const ui = { _allLines: ["L1", "R11", "L13"], stations: { lineSelected: ["R11"] } };
  assert.deepEqual([...visibleLineSet(ui)], ["R11"]);
});

test("adding a second line widens the selection rather than replacing it", () => {
  const ui = { _allLines: ["L1", "R11", "L13"], stations: { lineSelected: ["R11", "L13"] } };
  assert.deepEqual([...visibleLineSet(ui)].sort(), ["L13", "R11"]);
});

// A line can disappear from the timetable while the user's pick survives in
// localStorage; that must narrow the map, not throw on the way to drawing it.
test("an unknown line id in the selection is harmless", () => {
  const ui = { _allLines: ["L1", "R11"], stations: { lineSelected: ["R99"] } };
  const set = visibleLineSet(ui);
  assert.deepEqual([...set], ["R99"]);
  assert.equal(set.has("L1"), false);
});

test("a missing stations or _allLines key yields an empty set, not a throw", () => {
  assert.equal(visibleLineSet({}).size, 0);
  assert.equal(visibleLineSet({ _allLines: [] }).size, 0);
});
