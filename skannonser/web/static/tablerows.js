// Row selection for the table page. Split out of table.js because that module
// calls init() at import time -- importing a predicate from it would boot the
// whole app -- and these two functions are pure, so they are worth testing
// directly.

import { selectionExcludes, listingExcluded } from "./filters.js";

// Exported: table.js's compareItems and cell rendering use it too, and one
// definition beats two identical four-line copies.
export function isBlank(v) {
  return v === null || v === undefined || v === "";
}

export function matchesFilter(item, text) {
  if (!text) return true;
  const needle = text.toLowerCase();
  // Includes kommentar/tag so your own notes are searchable.
  return [item.adresse, item.postnummer, item.boligtype, item.kommentar, item.tag].some(
    (v) => !isBlank(v) && String(v).toLowerCase().includes(needle)
  );
}

// Two passes, because the row counter's two numbers answer different
// questions. The universe is "rows whose status I asked to see" -- the
// denominator; rows is "rows passing everything" -- the numerator. Splitting
// them fixes a counter that read "867 av 4387" after the closed bucket had
// been fetched and then filtered back out: state.items only ever grows, so
// using its length as the denominator counted rows that were not on screen.
//
// listingExcluded still applies the status filter internally -- it is the
// shared predicate and the map depends on that. The double application is
// idempotent and deliberately not optimised away, so the two pages cannot
// drift apart.
export function partitionRows(items, filters, meta, { text, focusFinnkode } = {}) {
  const focused = (item) => focusFinnkode && String(item.finnkode) === focusFinnkode;
  const universe = items.filter(
    (item) =>
      focused(item) ||
      !selectionExcludes(filters.tilgjengelighetSelected, item.tilgjengelighet || "")
  );
  const rows = universe.filter(
    (item) =>
      focused(item) ||
      (!listingExcluded(item, filters, meta) && matchesFilter(item, text))
  );
  return { rows, universe: universe.length };
}
