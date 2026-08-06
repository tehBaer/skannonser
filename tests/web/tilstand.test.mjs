// tests/web/tilstand.test.mjs
// Display formatting for the tilstand classifier (migration 016). Same two
// guards as salgsoppgave.test.mjs: booleans/nulls must not leak as JS
// literals, and enum keys must never reach a Norwegian reader verbatim --
// but an UNMAPPED key passes through as-is (parser grew a value the UI
// hasn't caught up with; ugly beats silent).
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  fmtAlvorlighet,
  fmtBygningsdel,
  fmtKostnadBand,
  BYGNINGSDEL_LABELS,
} from "../../skannonser/web/static/listingmeta.js";

test("fmtAlvorlighet maps the four tiers and passes null through", () => {
  assert.equal(fmtAlvorlighet("alvorlig"), "Alvorlig");
  assert.equal(fmtAlvorlighet("kosmetisk"), "Kosmetisk");
  assert.equal(fmtAlvorlighet(null), null);
  assert.equal(fmtAlvorlighet("ukjent_verdi"), "ukjent_verdi"); // unmapped passes through
});

test("fmtBygningsdel covers the whole enum", () => {
  assert.equal(Object.keys(BYGNINGSDEL_LABELS).length, 18);
  assert.equal(fmtBygningsdel("vatrom"), "Våtrom");
  assert.equal(fmtBygningsdel("vinduer_dorer"), "Vinduer/dører");
  assert.equal(fmtBygningsdel(null), null);
});

test("fmtKostnadBand renders bands with provenance", () => {
  // takst: the surveyor said it -- no hedge marker
  assert.equal(fmtKostnadBand(200000, 500000, "takst"), "200 000 – 500 000 kr");
  // estimat/blandet: model judgment -- hedged with ~
  assert.equal(fmtKostnadBand(200000, 500000, "estimat"), "~200 000 – 500 000 kr");
  assert.equal(fmtKostnadBand(200000, 500000, "blandet"), "~200 000 – 500 000 kr");
  assert.equal(fmtKostnadBand(0, 10000, "takst"), "under 10 000 kr");
  assert.equal(fmtKostnadBand(500000, 1000000, "estimat"), "~over 500 000 kr");
  assert.equal(fmtKostnadBand(null, null, null), null);
  assert.equal(fmtKostnadBand(1000000, 1000000, "takst"), "over 1 000 000 kr");
});
