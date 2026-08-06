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

test("fmtKostnadBand treats the full (0, 1M+) span as unknown, not 'under 1M'", () => {
  // lav===0 alone would hit the "under" branch and hoy===1000000 alone would
  // hit the "over" branch; together the band spans the entire grid, which is
  // not a real signal, so it must render as no band at all.
  assert.equal(fmtKostnadBand(0, 1000000, "estimat"), null);
  assert.equal(fmtKostnadBand(0, 1000000, "takst"), null);
});

// --- applyTilstandColumnsMigration (table.js's second, independent -----
// hidden-columns migration for migration 016's three columns) --------------
// table.js's loadHiddenColumns composes this with resolveHiddenColumns:
//   resolveHiddenColumns(stored, DEFAULTS, SALGSOPPGAVE_NEW)
//   -> applyTilstandColumnsMigration(hidden, stored, TILSTAND_NEW)
// A reader can have `salgsoppgaveColumnsDefaulted: true` from BEFORE the
// tilstand columns existed, so the two flags must be independent -- this
// mirrors that composition with small stand-in column lists.
import { resolveHiddenColumns, applyTilstandColumnsMigration } from "../../skannonser/web/static/listingmeta.js";

const S_DEFAULTS = ["pris", "ferdigattest", "utleie", "tg3_count", "alvorlighet"];
const S_NEW = ["ferdigattest", "utleie"];
const T_NEW = ["tg3_count", "alvorlighet"];

function loadHidden(stored) {
  const hidden = resolveHiddenColumns(stored, S_DEFAULTS, S_NEW);
  return applyTilstandColumnsMigration(hidden, stored, T_NEW);
}

test("fresh reader (no stored prefs) gets the full defaults -- both migrations included", () => {
  assert.deepEqual([...loadHidden(null)].sort(), [...S_DEFAULTS].sort());
});

test("pre-015 reader (no flags at all) gets both the salgsoppgave and tilstand columns hidden", () => {
  const stored = { hiddenColumns: ["pris"] };
  assert.deepEqual(
    [...loadHidden(stored)].sort(),
    ["alvorlighet", "ferdigattest", "pris", "tg3_count", "utleie"]
  );
});

test("post-015-pre-016 reader (salgsoppgaveColumnsDefaulted set) only gets the tilstand columns added", () => {
  const stored = { hiddenColumns: ["pris", "ferdigattest", "utleie"], salgsoppgaveColumnsDefaulted: true };
  assert.deepEqual(
    [...loadHidden(stored)].sort(),
    ["alvorlighet", "ferdigattest", "pris", "tg3_count", "utleie"]
  );
});

test("reader who already passed 016 and manually unhid alvorlighet keeps it visible", () => {
  const stored = {
    hiddenColumns: ["pris", "ferdigattest", "utleie", "tg3_count"],
    salgsoppgaveColumnsDefaulted: true,
    tilstandColumnsDefaulted: true,
  };
  assert.deepEqual(
    [...loadHidden(stored)].sort(),
    ["ferdigattest", "pris", "tg3_count", "utleie"],
    "alvorlighet must stay out of the hidden set -- the reader chose to show it"
  );
});
