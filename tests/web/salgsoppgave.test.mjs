// tests/web/salgsoppgave.test.mjs
// Display formatting for the salgsoppgave fields (migration 015).
//
// Two things these guard. First, `addRow` in popup.js only skips
// null/undefined/"" -- a raw boolean `false` renders as the literal string
// "false", so booleans MUST go through a formatter. Second, the DB stores
// enum keys (`ikke_tillatt`, `krever_godkjenning`); showing those verbatim to
// a Norwegian reader is not acceptable, but silently dropping a value we don't
// recognise is worse -- an unmapped key means the parser grew a value the UI
// hasn't caught up with, and hiding it would make that invisible.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  fmtJaNei,
  fmtFerdigattest,
  fmtUtleie,
  fmtHusdyr,
} from "../../skannonser/web/static/listingmeta.js";

// --- fmtJaNei ---------------------------------------------------------------

test("fmtJaNei renders booleans in Norwegian, not as JS literals", () => {
  assert.equal(fmtJaNei(true), "Ja");
  assert.equal(fmtJaNei(false), "Nei");
});

test("fmtJaNei passes null through so addRow skips the row", () => {
  // null means "no salgsoppgave text existed" -- distinct from False, which
  // means "the text was read and the topic wasn't mentioned". Showing "Nei"
  // for an unparsed listing would assert something we never checked.
  assert.equal(fmtJaNei(null), null);
  assert.equal(fmtJaNei(undefined), null);
});

// --- fmtFerdigattest --------------------------------------------------------

test("fmtFerdigattest distinguishes the three states a buyer cares about", () => {
  assert.equal(fmtFerdigattest("ferdigattest"), "Ja");
  assert.equal(fmtFerdigattest("midlertidig"), "Midlertidig brukstillatelse");
  assert.equal(fmtFerdigattest("ingen"), "Nei");
});

test("fmtFerdigattest returns null when absent", () => {
  assert.equal(fmtFerdigattest(null), null);
  assert.equal(fmtFerdigattest(undefined), null);
});

test("fmtFerdigattest surfaces an unmapped value rather than hiding it", () => {
  assert.equal(fmtFerdigattest("noe_nytt"), "noe_nytt");
});

// --- fmtUtleie --------------------------------------------------------------

test("fmtUtleie maps every enum value the parser can emit", () => {
  assert.equal(fmtUtleie("tillatt"), "Tillatt");
  assert.equal(fmtUtleie("ikke_tillatt"), "Ikke tillatt");
  assert.equal(fmtUtleie("egen_enhet"), "Egen utleiedel");
});

test("fmtUtleie returns null when absent", () => {
  assert.equal(fmtUtleie(null), null);
});

// --- fmtHusdyr --------------------------------------------------------------

test("fmtHusdyr maps every enum value the parser can emit", () => {
  assert.equal(fmtHusdyr("tillatt"), "Tillatt");
  assert.equal(fmtHusdyr("krever_godkjenning"), "Krever godkjenning");
  assert.equal(fmtHusdyr("ikke_tillatt"), "Ikke tillatt");
});

test("fmtHusdyr returns null when absent", () => {
  assert.equal(fmtHusdyr(null), null);
});

// --- no underscores reach the reader ----------------------------------------

test("no mapped value leaks a raw enum key", () => {
  const mapped = [
    fmtFerdigattest("ferdigattest"),
    fmtFerdigattest("midlertidig"),
    fmtFerdigattest("ingen"),
    fmtUtleie("tillatt"),
    fmtUtleie("ikke_tillatt"),
    fmtUtleie("egen_enhet"),
    fmtHusdyr("tillatt"),
    fmtHusdyr("krever_godkjenning"),
    fmtHusdyr("ikke_tillatt"),
  ];
  for (const value of mapped) {
    assert.ok(!value.includes("_"), `"${value}" still looks like an enum key`);
  }
});

// --- resolveHiddenColumns ---------------------------------------------------
// Migration 015 adds eight columns. A reader with stored preferences written
// before it simply has no opinion about them -- and "no opinion" must mean
// hidden, not visible, or their table silently grows eight columns of
// horizontal scroll. But the fix must apply exactly once: a reader who then
// chooses to show one must keep it shown.

import { resolveHiddenColumns } from "../../skannonser/web/static/listingmeta.js";

const DEFAULTS = ["pris", "etasje"];
const NEW = ["ferdigattest", "utleie"];

test("no stored preferences falls back to the defaults", () => {
  assert.deepEqual([...resolveHiddenColumns(null, DEFAULTS, NEW)].sort(), ["etasje", "pris"]);
  assert.deepEqual([...resolveHiddenColumns({}, DEFAULTS, NEW)].sort(), ["etasje", "pris"]);
});

test("preferences predating the migration get the new columns hidden", () => {
  const stored = { hiddenColumns: ["pris"] };
  assert.deepEqual(
    [...resolveHiddenColumns(stored, DEFAULTS, NEW)].sort(),
    ["ferdigattest", "pris", "utleie"]
  );
});

test("once defaulted, a column the reader unhid stays unhidden", () => {
  // They have seen the new columns and deliberately shown `ferdigattest`.
  const stored = { hiddenColumns: ["pris", "utleie"], salgsoppgaveColumnsDefaulted: true };
  assert.deepEqual(
    [...resolveHiddenColumns(stored, DEFAULTS, NEW)].sort(),
    ["pris", "utleie"],
    "re-hiding a deliberately shown column would make the picker feel broken"
  );
});

test("an explicitly empty stored set is respected, not treated as absent", () => {
  const stored = { hiddenColumns: [], salgsoppgaveColumnsDefaulted: true };
  assert.deepEqual([...resolveHiddenColumns(stored, DEFAULTS, NEW)], []);
});

test("the returned set is independent of the inputs", () => {
  const stored = { hiddenColumns: ["pris"] };
  const out = resolveHiddenColumns(stored, DEFAULTS, NEW);
  out.add("mutated");
  assert.deepEqual(stored.hiddenColumns, ["pris"]);
  assert.ok(!DEFAULTS.includes("mutated"));
});

// --- "mentioned" is not "has" -----------------------------------------------
// `heftelser` and `radon_omtalt` are mention-detectors: the parser looks for
// the words `servitutt|heftelse` and `radon` anywhere in the prospectus. So
// True means "the prospectus discusses this", NOT "this property has it".
// Rendering them as Ja/Nei was wrong and alarming -- "Radon: Ja" reads as a
// radon problem, when half of all listings say something about radon purely as
// boilerplate. Omtalt/Ikke omtalt says what we actually know.
//
// boligselgerforsikring is deliberately NOT in this group: the parser
// distinguishes "har tegnet" from "har ikke tegnet", so Ja/Nei is a true
// statement about the seller.

import { fmtOmtalt, SALGSOPPGAVE_DERIVED } from "../../skannonser/web/static/listingmeta.js";

test("fmtOmtalt says what was observed, not what is true of the property", () => {
  assert.equal(fmtOmtalt(true), "Omtalt");
  assert.equal(fmtOmtalt(false), "Ikke omtalt");
});

test("fmtOmtalt never renders Ja or Nei", () => {
  for (const v of [true, false]) {
    assert.ok(!["Ja", "Nei"].includes(fmtOmtalt(v)));
  }
});

test("fmtOmtalt passes null through (no prospectus text at all)", () => {
  assert.equal(fmtOmtalt(null), null);
  assert.equal(fmtOmtalt(undefined), null);
});

test("SALGSOPPGAVE_DERIVED marks the prose-extracted fields", () => {
  for (const key of [
    "ferdigattest", "utleie", "husdyr", "heftelser",
    "radon_omtalt", "boligselgerforsikring", "eiendomsskatt_kr",
  ]) {
    assert.ok(SALGSOPPGAVE_DERIVED.has(key), `${key} should be marked as derived`);
  }
});

test("SALGSOPPGAVE_DERIVED excludes fields read from structured markup", () => {
  // verditakst comes from the pricing <dl>, exactly like totalpris and
  // omkostninger, which carry no marker. Marking it would imply a softness it
  // does not have.
  assert.ok(!SALGSOPPGAVE_DERIVED.has("verditakst"));
  assert.ok(!SALGSOPPGAVE_DERIVED.has("totalpris"));
  assert.ok(!SALGSOPPGAVE_DERIVED.has("energimerke"));
});

// --- the (s) marker ---------------------------------------------------------
// Applied from SALGSOPPGAVE_DERIVED at render time, not baked into each
// label, so a field added to the set is marked everywhere at once and the
// marker can never disagree with the set.

import { labelWithSource, SALGSOPPGAVE_SUFFIX, SALGSOPPGAVE_HINT }
  from "../../skannonser/web/static/listingmeta.js";

test("derived fields get the marker", () => {
  assert.equal(labelWithSource("ferdigattest", "Ferdigattest"), "Ferdigattest (s)");
  assert.equal(labelWithSource("radon_omtalt", "Radon"), "Radon (s)");
  assert.equal(labelWithSource("eiendomsskatt_kr", "Eiendomsskatt"), "Eiendomsskatt (s)");
});

test("structured-source fields do NOT get the marker", () => {
  // Marking these would imply a softness they do not have.
  assert.equal(labelWithSource("verditakst", "Verditakst"), "Verditakst");
  assert.equal(labelWithSource("totalpris", "Totalpris"), "Totalpris");
  assert.equal(labelWithSource("byggeaar", "Byggeår"), "Byggeår");
});

test("every derived field is marked, with no hand-maintained second list", () => {
  for (const key of SALGSOPPGAVE_DERIVED) {
    assert.ok(
      labelWithSource(key, "X").endsWith(SALGSOPPGAVE_SUFFIX),
      `${key} is in the derived set but renders unmarked`
    );
  }
});

test("the tooltip explains what (s) means", () => {
  assert.ok(
    SALGSOPPGAVE_HINT.includes(SALGSOPPGAVE_SUFFIX.trim()),
    "a marker nobody can decode is just noise"
  );
});
