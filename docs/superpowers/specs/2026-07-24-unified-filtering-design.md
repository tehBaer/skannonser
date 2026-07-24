# Unified map/table filtering — design

**Date:** 2026-07-24
**Status:** Approved (design), not yet implemented

## Goal

One shared filter state driving both surfaces: Notion-style column-header
filter popovers on the table, the existing slider/checkbox sidebar on the map,
both applying the identical predicate to the identical listing set. Setting a
filter anywhere filters everywhere — including live across two open tabs.

Continuous quantities (price, travel time, areas, monthly costs) stay
**sliders**; discrete vocabularies (boligtype, eieform, energimerke, …) are
**checkbox sets**; large vocabularies (postnummer, nabolag) are **searchable
multi-selects**.

## Approach (decision record)

Three options considered:

1. **Shared filter-state module + two renderings** — **chosen.** A new
   `filterstate.js` owns one localStorage-backed filters object; one shared
   predicate in `filters.js`; the map sidebar and the new table header
   popovers are two UIs over the same state. `storage` events give free
   cross-tab live sync. Zero backend changes — both pages already hold every
   listing client-side.
2. **Merge map + table into one page** — rejected: much larger rebuild of two
   working pages; the user scoped this to filtering.
3. **Server-side filtering via query params** — rejected: the map needs
   excluded items delivered anyway (to dim them), and ~4k rows filter
   instantly client-side.

**Approved behavior changes** (user sign-off 2026-07-24):

- Unchecking a boligtype or tag on the map now **dims** (like metric filters)
  instead of hiding; "Nedtoning" at 100 % restores full hiding.
- Eieform goes from single-select dropdown to multi-select checkboxes.
- (Folded in, see §3:) the legacy pris/BRA-i filters adopt the same
  null-handling as the details filters (`numOrNull` + "inkluder ukjent"),
  which also fixes the pre-existing dead missing-value branches in
  `metricDimmed` — superseding the standalone fix task spawned 2026-07-23.

## 1. Column → filter mapping

| Filter style | Columns / dimensions |
|---|---|
| Slider (max) | Pris, Totalpris, Total/kvm, Felleskost, Mnd-kost, BRJ, MVV, UNI |
| Slider (min) | BRA-i, Soverom, Byggeår *(new)* |
| Checkbox set (small vocab, "hidden set") | Boligtype, Eieform, Energimerke, Tag, Tilgjengelighet *(new)* |
| Searchable multi-select ("selected set") | Postnummer *(new)*, Nabolag *(new)* |
| Special | Fasiliteter ("must have" checkbox list): toolbar popover on the table, existing sidebar panel on the map |
| Sort/display only | Adresse, Kart, Først sett, Solgt-fields, Budpremie, Etasje, Kommentar |
| Page-local (not synced) | The table's free-text search box |

Slider ceilings/floors (slider AT the bound = filter off, existing idiom):
existing consts unchanged; new: `BYGGEAAR_FLOOR = 1900` (min-slider at floor =
off, range 1900–2030, step 1), `TOTAL_KVM_MAX = 120_000` (step 1000),
`MAANEDSKOST_MAX = 20_000` (step 250).

## 2. Shared state — `skannonser/web/static/filterstate.js` (new)

Owns the canonical `filters` object inside the existing `skannonser.ui.v1`
localStorage blob. Both pages import it; neither touches the blob's filter
keys directly anymore.

```js
filters = {
  // sliders (existing keys unchanged)
  priceMax, braIMin, travelMax: {brj, mvv, mvv_uni},
  totalprisMax, felleskostMax, soveromMin,
  // sliders (new)
  byggeaarMin, totalKvmMax, maanedskostMax,
  // hidden sets: {} = nothing hidden = off; key present ⇒ value excluded.
  // Default-visible semantics — matches today's sidebar toggles.
  boligtypeHidden, tagHidden, energiHidden,        // exist (first two move in from ui root)
  eieformHidden, tilgjengelighetHidden,            // new (eieformHidden replaces `eieform` string)
  // selected sets: [] = off; non-empty ⇒ ONLY these values pass.
  // Notion-style — right shape for 100+-value vocabularies.
  postnummerSelected, nabolagSelected,             // new
  // special
  facilitiesRequired,                              // exists
  includeUnknown,                                  // exists, global
}
```

API: `defaultFilters(meta)`, `loadFilters(meta)` (merge stored-over-default,
run migrations), `saveFilters(filters)`, `subscribe(onChange)` (wraps the
`storage` event → fires when ANOTHER tab writes; same-tab callers re-render
directly after saving), `activeFilterCount(filters, meta)`,
`resetFilters()` (back to defaults, preserving `includeUnknown`).

**Migration of existing blobs** (silent, in `loadFilters`): `ui.boligtypeHidden`
/ `ui.tagHidden` move under `filters.*` (map-side readers updated); a stored
non-empty single-select `filters.eieform` (e.g. `"Andel"`) migrates to
`eieformHidden = {v: true for every v in meta.eieformer except that value}`;
empty string migrates to `{}` (off). Legacy keys are deleted after migration.

Map-only presentation state stays where it is (`dimIntensity`, `soldDim`,
`collapsed`, `stations`, `combineSold`, …) — it is not filtering.

## 3. One predicate — `listingExcluded(item, filters, meta)` in filters.js

Replaces the `metricDimmed` + `boligtypeHidden` + tag-check split. Pure
function, no DOM. Rules:

- Sliders: numeric compare via `numOrNull`; `null`/absent = **unknown** →
  passes while `includeUnknown` is true, fails while false. This now applies
  to pris and BRA-i too (legacy special-casing of DNB vs Eie missing values
  is dropped — the includeUnknown toggle is the single policy).
- Hidden sets: item's value in the set → excluded. Null value maps to the
  vocabulary's explicit unknown bucket (`""`): boligtype's "Ukjent boligtype"
  bucket keeps working; tilgjengelighet gets an "Ingen status" bucket (null =
  a normal open listing — bucketed, not includeUnknown-governed).
- Selected sets: empty → pass; non-empty → item's value must be in the set;
  null value follows `includeUnknown`.
- Facilities: unchanged AND-semantics; missing/empty list = unknown.
- Tag: annotation tag, same bucket logic as today ("" = untagged bucket).

Consumers:

- **Map (app.js):** excluded → dimmed at `residualOpacity(ui)`; when
  `dimIntensity === 100`, excluded items are fully removed from the cluster
  sources (today's hide behavior, now opt-in via the slider). Tag rings and
  sold/premium colouring unchanged for items that pass.
- **Table (table.js):** excluded → row not rendered. Status line becomes
  "X av Y annonser · N filtre aktive".

## 4. Table header popovers — `skannonser/web/static/tablefilters.js` (new)

- Each filterable `<th>` gets a funnel button beside the label. Label click =
  sort (unchanged); funnel click = popover anchored to the header, one open
  at a time, closed on outside-click/Escape. Active filter ⇒ filled funnel +
  tinted header cell.
- Popover contents by kind, driven by a `COLUMN_FILTERS` descriptor map
  (column key → {kind: "slider-max" | "slider-min" | "set" | "search-set",
  stateKey, bounds/step, vocabulary source}):
  - sliders reuse `rangeRow` from filters.js;
  - "set" renders the checkbox group component;
  - "search-set" renders a text box filtering a checkbox list (values +
    counts), with "velg alle synlige / tøm" actions.
- Toolbar additions: Fasiliteter popover button, "Inkluder ukjent" toggle,
  "Nullstill filtre" button.
- The three UI components (checkbox group, searchable multi-select, range
  row) live in filters.js and are shared verbatim with the sidebar — one
  implementation, two mounts.
- Mobile: popovers clamp to the viewport (position: fixed fallback under
  ~480 px).

## 5. Map sidebar (app.js + filters.js builders)

- Existing panels keep their look, rewired to `filterstate`.
- Metric panel gains the three new sliders (Byggeår min, Total/kvm max,
  Mnd-kost max).
- New collapsible "Flere filtre" panel: Tilgjengelighet checkbox group,
  Postnummer + Nabolag searchable multi-selects (same shared components).
- Both surfaces show an active-filter line: "N filtre aktive · nullstill" —
  essential now that filters can be set from the other page.

## 6. Vocabularies — client-side derivation

Postnummer, nabolag, and tilgjengelighet vocabularies (values + counts) are
derived from the already-loaded listing set on each page — **no `/api/meta`
change**. Existing meta vocabularies (boligtyper, energimerker, eieformer,
facilities) stay as-is. Consequence: sold-only values join the vocabulary
lists when the sold bucket is lazily loaded; acceptable, since filtering sold
rows requires them loaded anyway.

## 7. Live sync

`filterstate.subscribe` listens for the `storage` event: a save in one tab
re-renders filters + listings in the other (map re-clusters, table re-renders,
sidebar/popover controls re-read state). Same-tab flows keep the current
direct `onChange` path. No polling, no backend.

## 8. Testing & verification

- Repo pattern unchanged: no JS test harness. `node --check` on every touched
  file; pytest suite (616) guards the untouched API/export surface.
- Browser verification checklist: each popover kind (slider/set/search-set)
  filters the table AND dims the map identically; nedtoning 100 % hides incl.
  cluster counts; boligtype/tag now dim (approved change); eieform
  multi-select (approved change); pris/BRA-i respect "inkluder ukjent"
  (approved change; supersedes the standalone dead-branch fix task);
  cross-tab: two windows, filter in one, other updates without reload;
  legacy localStorage blob migrates (no lost settings, old keys gone);
  mobile drawer + popover clamping; reset button clears everything but
  includeUnknown.

## Out of scope

- Any layout merge of the two pages; saved filter presets; URL-shareable
  filter links; server-side filtering; sheet export changes (unaffected by
  design — no API/SQL changes at all).
