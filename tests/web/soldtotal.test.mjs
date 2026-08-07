import { test } from "node:test";
import assert from "node:assert/strict";
import {
  soldTotalpris,
  soldPrisKvmTotalpris,
  COMPUTED_COLUMNS,
  COMPUTED_SUFFIX,
  COMPUTED_HINT,
  SALGSOPPGAVE_DERIVED,
  labelWithSource,
} from "../../skannonser/web/static/listingmeta.js";

// --- soldTotalpris ----------------------------------------------------------
// The sold-side counterpart of `totalpris`: what the buyer actually committed
// to, so the "Solgt" columns can be read against "Totalpris" on the same basis
// rather than looking like a discount the size of the omkostninger+fellesgjeld
// wedge. Real numbers throughout: Haugerudveien 84, finnkode 460216378.

test("sold price plus omkostninger and fellesgjeld", () => {
  assert.equal(
    soldTotalpris({ sold_price: 4600000, omkostninger: 9496, fellesgjeld: 349970 }),
    4959466
  );
});

// Same blankness trap premiumPct documents: Number(null) is 0 and passes
// Number.isFinite, so a closed listing with no tinglyst price yet would
// otherwise render as a sale for exactly the omkostninger.
test("a blank sold_price is unknown, not a sale for the omkostninger", () => {
  for (const blank of [null, undefined, ""]) {
    assert.equal(
      soldTotalpris({ sold_price: blank, omkostninger: 9496, fellesgjeld: 349970 }),
      null
    );
  }
});

// The other direction: a MISSING fellesgjeld means zero, not unknown. Selveier
// ads omit the Fellesgjeld line entirely -- 2 867 of 6 258 listing_details rows
// have it NULL, and coalescing those to 0 reconciles
// `totalpris - omkostninger - fellesgjeld` against FINN's own priceSuggestion
// on 82 of the 85 sold rows where it is NULL. Treating it as unknown would
// blank the column for nearly every selveier.
test("a missing fellesgjeld counts as zero", () => {
  assert.equal(soldTotalpris({ sold_price: 5000000, omkostninger: 125000 }), 5125000);
  assert.equal(
    soldTotalpris({ sold_price: 5000000, omkostninger: 125000, fellesgjeld: null }),
    5125000
  );
});

test("a missing omkostninger counts as zero", () => {
  assert.equal(soldTotalpris({ sold_price: 5000000, fellesgjeld: 200000 }), 5200000);
});

test("both addends missing leaves the sold price untouched", () => {
  assert.equal(soldTotalpris({ sold_price: 5000000 }), 5000000);
});

// A non-numeric addend is a parser fault, not a signal; it must not poison the
// sum into NaN and it must not blank a column the sold price alone can carry.
test("a non-numeric addend counts as zero rather than poisoning the sum", () => {
  assert.equal(soldTotalpris({ sold_price: 5000000, omkostninger: "n/a" }), 5000000);
});

// Mirrors premium.test.mjs: the guard keys on blankness, not on the value.
test("an explicit zero sold_price is a real sale", () => {
  assert.equal(soldTotalpris({ sold_price: 0, omkostninger: 9496 }), 9496);
});

test("string numbers coerce, matching the rest of the payload handling", () => {
  assert.equal(
    soldTotalpris({ sold_price: "4600000", omkostninger: "9496", fellesgjeld: "349970" }),
    4959466
  );
});

// --- soldPrisKvmTotalpris ---------------------------------------------------
// The sold analogue of Total/kvm (api.py's _pris_kvm_totalpris), and the
// number that actually compares two flats. Same rounding, same both-positive
// precondition.

test("sold total per BRA-i, rounded", () => {
  // 4 959 466 / 83 = 59 752,6 -> 59 753, matching the Total/kvm column's rule.
  assert.equal(
    soldPrisKvmTotalpris({
      sold_price: 4600000, omkostninger: 9496, fellesgjeld: 349970, bra_i: 83,
    }),
    59753
  );
});

test("no sold total means no sold total per kvm", () => {
  assert.equal(soldPrisKvmTotalpris({ sold_price: null, bra_i: 83 }), null);
});

test("a missing or zero BRA-i is unknown, never a division by zero", () => {
  for (const bra of [null, undefined, "", 0, -5]) {
    assert.equal(soldPrisKvmTotalpris({ sold_price: 4600000, bra_i: bra }), null);
  }
});

// --- the (b) marker ---------------------------------------------------------
// Third provenance class, alongside SALGSOPPGAVE_DERIVED's "(s)" (regex over
// prospectus prose) and TILSTAND_DERIVED's colour (model judgement): a value
// this UI computed from other fields rather than one FINN published.

test("COMPUTED_COLUMNS covers every arithmetic column", () => {
  for (const key of [
    "pris_kvm", "pris_kvm_totalpris", "maanedskost", "premium",
    "sold_totalpris", "sold_pris_kvm_totalpris",
  ]) {
    assert.ok(COMPUTED_COLUMNS.has(key), `${key} should be marked as computed`);
  }
});

test("COMPUTED_COLUMNS excludes columns read verbatim off the listing", () => {
  // These are single figures FINN published -- totalpris and omkostninger come
  // straight out of the pricing <dl>, sold_price straight off the sold card.
  // Marking them would spend the marker's credibility on nothing.
  for (const key of ["pris", "totalpris", "felleskost_mnd", "sold_price", "bra_i"]) {
    assert.ok(!COMPUTED_COLUMNS.has(key), `${key} is not computed`);
  }
});

// Both markers are suffixes on the same label; a key in both sets would render
// "Foo (s) (b)". The sets must stay disjoint, and the LLM-tinted columns are
// deliberately not double-marked either (their colour is the louder signal).
test("the provenance marker sets are disjoint", () => {
  for (const key of COMPUTED_COLUMNS) {
    assert.ok(!SALGSOPPGAVE_DERIVED.has(key), `${key} is marked twice`);
  }
});

test("computed columns get the marker", () => {
  assert.equal(labelWithSource("premium", "Budpremie"), "Budpremie (b)");
  assert.equal(labelWithSource("sold_totalpris", "Solgt totalt"), "Solgt totalt (b)");
  assert.equal(labelWithSource("pris_kvm", "Pris/kvm"), "Pris/kvm (b)");
});

test("unmarked columns keep their label untouched", () => {
  assert.equal(labelWithSource("totalpris", "Totalpris"), "Totalpris");
  assert.equal(labelWithSource("sold_price", "Solgt for"), "Solgt for");
});

test("the salgsoppgave marker still wins for its own columns", () => {
  assert.equal(labelWithSource("ferdigattest", "Ferdigattest"), "Ferdigattest (s)");
});

test("the suffix and hint agree on the glyph", () => {
  assert.ok(COMPUTED_HINT.includes(COMPUTED_SUFFIX.trim()));
});
