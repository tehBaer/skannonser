// tests/web/geolinks.test.mjs
// The popup's external map links are built from the geocode, not the address:
// `adresse` is street-only ("Vinterkroken 75" -- no city, no postcode), so an
// address-search URL would be ambiguous nationwide. ~460 of 6141 listings have
// no geocode at all, and those must yield NO link rather than a URL pointing at
// "null,null" -- which is what an unguarded template literal produces, since
// null stringifies happily.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mapsUrl, earthUrl } from "../../skannonser/web/static/listingmeta.js";

const OSLO = { lat: 59.9127, lng: 10.7461 };

// --- mapsUrl ----------------------------------------------------------------

test("mapsUrl points Google Maps at the geocoded coordinates", () => {
  assert.equal(mapsUrl(OSLO), "https://www.google.com/maps?q=59.9127,10.7461");
});

// --- earthUrl ---------------------------------------------------------------

test("earthUrl frames the coordinates as a tilted 3D view", () => {
  // The camera suffix is Earth Web's own syntax: 0a ground altitude, 300d
  // range from the target, 35y field of view, 0h north-up heading, 60t tilt
  // off vertical, 0r roll. 300d/60t is the "what does this place look like"
  // framing the design settled on -- close enough to read the building, tilted
  // enough to show terrain the flat Maps link cannot.
  assert.equal(
    earthUrl(OSLO),
    "https://earth.google.com/web/@59.9127,10.7461,0a,300d,35y,0h,60t,0r"
  );
});

// --- the missing-geocode guard, which is the whole point ---------------------

for (const [name, builder] of [
  ["mapsUrl", mapsUrl],
  ["earthUrl", earthUrl],
]) {
  test(`${name} returns null when the listing was never geocoded`, () => {
    assert.equal(builder({ lat: null, lng: null }), null);
    assert.equal(builder({}), null);
  });

  test(`${name} returns null when only one coordinate survived`, () => {
    assert.equal(builder({ lat: 59.9127, lng: null }), null);
    assert.equal(builder({ lat: null, lng: 10.7461 }), null);
  });

  test(`${name} rejects coordinates that are not numbers`, () => {
    // geocode_failed rows and hand-edited overrides have produced empty
    // strings here; Number("") is 0, which would silently plot the Gulf of
    // Guinea instead of dropping the link.
    assert.equal(builder({ lat: "", lng: "" }), null);
    assert.equal(builder({ lat: "ukjent", lng: "ukjent" }), null);
    assert.equal(builder({ lat: NaN, lng: 10.7461 }), null);
  });

  test(`${name} accepts a coordinate of exactly zero`, () => {
    // Not reachable for Norwegian listings, but 0 is a real number and the
    // guard must key on "is it numeric", not on truthiness.
    assert.ok(builder({ lat: 0, lng: 0 }));
  });

  test(`${name} accepts numeric strings, as JSON round-trips can produce`, () => {
    assert.equal(builder({ lat: "59.9127", lng: "10.7461" }), builder(OSLO));
  });
}
