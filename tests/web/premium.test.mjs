import { test } from "node:test";
import assert from "node:assert/strict";
import { premiumPct } from "../../skannonser/web/static/listingmeta.js";

// The bug: Number(null) === 0 passes Number.isFinite, so a closed listing
// whose sale is not yet tinglyst computed as a sale for 0 kr -> -100 %.
test("a null sold_price is unknown, not a sale for zero", () => {
  assert.equal(premiumPct({ sold_price: null, price_suggestion: 4990000 }), null);
});

test("an undefined sold_price is unknown", () => {
  assert.equal(premiumPct({ price_suggestion: 4990000 }), null);
});

test("an empty-string sold_price is unknown", () => {
  assert.equal(premiumPct({ sold_price: "", price_suggestion: 4990000 }), null);
});

test("a null price_suggestion is unknown", () => {
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: null }), null);
});

// A genuine 0 kr sale is not in the data (no sold_prices row has one), but if
// FINN ever ships one it must read as -100 %, not as missing. The guard keys
// on blankness, not on the numeric value.
test("an explicit zero sold_price is a real -100 %", () => {
  assert.equal(premiumPct({ sold_price: 0, price_suggestion: 4990000 }), -100);
});

// premiumPct is no longer rounded (see the fractional-percentage test below),
// so these compare with tolerance: (soldPrice / asking - 1) * 100 lands a
// hair off 4 / -5 in IEEE-754 (e.g. 4.0000000000000036).
test("a normal sale still computes", () => {
  const pct = premiumPct({ sold_price: 5200000, price_suggestion: 5000000 });
  assert.ok(Math.abs(pct - 4) < 1e-9, `expected ~4, got ${pct}`);
});

test("a below-asking sale still computes", () => {
  const pct = premiumPct({ sold_price: 4750000, price_suggestion: 5000000 });
  assert.ok(Math.abs(pct - -5) < 1e-9, `expected ~-5, got ${pct}`);
});

test("a zero or negative asking price is unknown", () => {
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: 0 }), null);
  assert.equal(premiumPct({ sold_price: 5200000, price_suggestion: -1 }), null);
});

// premiumPct must return a fractional percentage, not a rounded integer.
// fmtPremium formats with toLocaleString(..., { maximumFractionDigits: 1 })
// specifically to show one decimal place (e.g. "+7,2 %"), and app.js's map
// feature does its own Math.round(pct * 10) / 10 to get one decimal of
// precision -- both are no-ops if this function already rounded to a whole
// number.
test("premiumPct returns a fractional percentage, not a rounded integer", () => {
  const pct = premiumPct({ sold_price: 5360000, price_suggestion: 5000000 });
  assert.ok(!Number.isInteger(pct), `expected a fractional value, got ${pct}`);
  assert.ok(Math.abs(pct - 7.2) < 1e-9, `expected ~7.2, got ${pct}`);
});
