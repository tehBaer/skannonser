# Map sidebar overhaul + Kart/Tabell tabs — design

**Date:** 2026-07-24
**Status:** Approved (design), not yet implemented

## Goal

Make the map sidebar short and sensibly grouped, connect the two pages with a
shared Kart | Tabell tab header, compress the checkbox forests into
Notion-style select-fields, separate display settings (nedtoning) from
filters, and drop the three list-heavy sections (Må ha fasiliteter,
Postnummer, Nabolag) from the sidebar — they remain fully usable from the
table's header popovers over the same shared state.

Prerequisite context: the 2026-07-24 unified-filtering feature (shared
`filterstate.js` state, `listingExcluded` predicate, table header popovers in
`tablefilters.js`). **This overhaul is purely presentational — zero changes
to the filter state schema, the predicate, migrations, or the backend.**

## Approach (decision record)

- **Tabs:** shared tab header on both pages, normal navigation between them
  (user choice over a single-page merge). State is already live-synced via
  localStorage; assets are cached, so switching is effectively instant.
- **Sidebar:** regroup + compress in place (Approach A) — rejected the more
  radical "Notion filter-list" (only-active-filters + add-menu) because the
  user wants the filter sliders to stay directly visible.
- **Select-fields keep hidden-set storage** — the compact field is a new
  rendering over the existing `filters.*Hidden` objects; no state migration.
- **Expandable active-line approved** — needed because fasiliteter/
  postnummer/nabolag filters can now only be *edited* from the table, so the
  map must still show and clear them.

## 1. Shared tab header (both pages)

- A small header component rendered identically on `index.html` and
  `table.html`: brand + two tab links `Kart` / `Tabell`, the current page's
  tab styled active. Replaces the table's "← Kart" nav link and the map
  sidebar's "Tabell" link panel (`#table-link-panel` is deleted).
- Plain `<a href="/">` / `<a href="/table">` — no JS routing. Styled as tabs
  (underline/pill on the active one), consistent on mobile.
- On the map page the header sits at the top of the sidebar (above "Lag");
  on the table page it replaces the current `.table-header` content.

## 2. New sidebar structure (index.html + app.js wiring)

Top-to-bottom:

| Panel (`<details class="panel">`) | Contents |
|---|---|
| *(tab header, not a panel)* | Kart \| Tabell |
| **Lag** | Eie / DNB / Solgt layer toggles + the kant/shape legend rows (Aktiv/Solgt/DNB). "Klyng solgte + aktive" and "Farg solgte etter budpremie" MOVE OUT to Visning. |
| **Filtre** | 1. Active-filter line (expandable, see §5) + "Nullstill filtre" button. 2. Five select-fields (§4): Boligtype, Eieform, Energimerking, Tilgjengelighet, Tags. 3. Sliders in three nested collapsible sub-groups (`<details class="subgroup">` with ids, collapsed state persisted in the existing `ui.collapsed` map): **Pris og kostnad** (Maks pris, Maks totalpris, Maks felleskost/mnd, Maks mnd-kost, Maks total/kvm), **Bolig** (Min BRA-i, Min soverom, Min byggeår), **Reisetid** (Maks BRJ/MVV/UNI). Sub-groups default OPEN the first time (no stored state) — the panel is short enough once the checkbox forests are gone; users collapse what they don't use. 4. "Inkluder ukjent verdi" toggle at the bottom. |
| **Visning** *(new)* | Filtret nedtoning, Solgt nedtoning (sliders — moved from the filter panel; they are display settings, not filters), Klyng solgte + aktive, Farg solgte etter budpremie + the premium legend. |
| **Stasjoner** | Unchanged. |
| **Mangler koordinater** | Unchanged content, but `open` attribute removed — collapsed by default (diagnostics). |

Removed from the sidebar entirely: **Må ha fasiliteter, Postnummer, Nabolag**
(the `#more-filters-panel` is deleted; `buildMoreFiltersUI` is retired or
reduced — see §7). These filters remain: editable in the table's popovers,
counted in the active line, listed/clearable in the expanded active-line
(§5), and cleared by Nullstill.

The old separate **Boligtype** and **Tags** panels disappear — both become
select-fields inside Filtre. The boligtype colour swatches move into the
field's dropdown rows (and the closed field's chips carry a small swatch dot).

## 3. Popover primitives become shared

`tablefilters.js`'s popover machinery (`openPopover`, `closePopover`,
`placePopover`, the document-level outside-click/Escape dismiss) moves to the
shared component home (`filters.js`), exported; `tablefilters.js` imports it
from there. Behavior unchanged. This lets the sidebar select-fields open the
exact same popovers the table headers use.

## 4. The select-field component (new, in filters.js)

`selectField(parent, { label, options, hidden, swatches?, searchable?, onChange })`

- **Closed state:** one row, `label` left, value summary right:
  - nothing hidden → "Alle" (muted);
  - otherwise chips of the VISIBLE values when ≤ 3 remain visible, else
    "N av M". Chips show the value text (+ swatch dot for boligtype).
- **Click** opens a popover (shared machinery, §3) anchored to the field:
  the existing `checkboxGroup` semantics (checked = visible, unchecking
  writes into the hidden-set), swatches for boligtype, plus a search box
  (reusing the `searchableMultiSelect` search idiom) when `searchable`
  (Tags, since the tag vocabulary is unbounded).
- The field re-renders its closed summary after every change (popover stays
  open while toggling, like the table's popovers).
- Storage semantics untouched: same `boligtypeHidden`/`eieformHidden`/
  `energiHidden`/`tilgjengelighetHidden`/`tagHidden` objects.
- Vocabularies: boligtype/eieform/energimerke from `meta`, tilgjengelighet/
  tags from `deriveVocabs(items)` — exactly the sources the table popovers
  use (`COLUMN_FILTERS` vocab mapping stays the single source of truth where
  practical).

## 5. Expandable active-filter line (both pages' benefit, map-side UI)

- Collapsed (default): "N filtre aktive · vis" (or "Ingen aktive filtre").
- Expanded: one row per ACTIVE filter — label + short value + a "×" that
  clears just that filter. Examples: "Maks felleskost: 5 000 kr ×",
  "Boligtype: 4 av 6 ×", "Fasiliteter: 2 krav ×", "Postnummer: 3 valgt ×".
- Clearing writes the filter back to its default (slider → bound, set → {},
  selected → []) through the normal onFilterChange path (saves + re-renders
  + syncs cross-tab).
- Implemented as a new shared helper `activeFilterEntries(filters, meta) ->
  [{key, label, valueText, clear()}]` in `filterstate.js` (it already owns
  `activeFilterCount`; the count becomes `entries.length` to keep the two
  from drifting).
- The table's status line keeps its current "X av Y · N filtre aktive" text
  (the table already shows its filters in the headers); the expandable list
  is sidebar-only for now.

## 6. Files touched

- `index.html` — tab header, panel restructure, removed panels.
- `table.html` — tab header replaces `.table-header` content.
- `style.css` — tab header styles, select-field (closed row + chips),
  subgroup details styling, active-line list rows.
- `filters.js` — gains popover primitives (from tablefilters.js),
  `selectField`, slider sub-group builder; `buildMetricFilterUI` reshaped
  into the new Filtre-panel builder; `buildBoligtypeFilterUI` and
  `buildMoreFiltersUI` retired (their state keys live on in the fields /
  table popovers).
- `tablefilters.js` — imports popover primitives instead of owning them;
  otherwise unchanged.
- `filterstate.js` — `activeFilterEntries` added; `activeFilterCount`
  reimplemented on top of it. No schema change.
- `app.js` — new panel wiring (Visning sliders + moved toggles), tag/
  boligtype/tilgjengelighet select-field mounting, expandable active line,
  removal of retired builders. `rebuildFilterUIs` stays the single rebuild
  path (init / reset / cross-tab / sold-load).
- `table.js` — only the header swap (tab header) — filters untouched.

## 7. Behavior invariants (unchanged, verified after)

- Filter state schema, `listingExcluded`, migration, cross-tab sync: NO
  changes. A user's existing filters render correctly in the new UI.
- The table page's popovers, toolbar (Fasiliteter / Inkluder ukjent /
  Nullstill) and columns: unchanged apart from the header.
- Mobile drawer behavior unchanged; select-field popovers use the same
  ≤480 px viewport clamp as table popovers.
- Sold/premium colouring, stations, klyng — same logic, new panel homes.

## 8. Verification

No JS harness (repo pattern): `node --check` per touched file + controller
browser verification: tab header on both pages (active state correct);
sidebar renders the new structure ≈ 1–1.5 screens; each select-field opens,
edits, and summarizes correctly (incl. boligtype swatches and tag search);
sliders still filter; Visning sliders dim without affecting the active-filter
count; active-line expands, lists table-set filters (set a fasiliteter filter
in the table, clear it from the map's active line ×), Nullstill still clears
everything; cross-tab sync still live; existing-blob load shows correct
field summaries; mobile drawer + popover clamp; zero console errors.

## Out of scope

- Single-page merge, URL routing, split view.
- Any filterstate schema or predicate change; any backend change.
- Table-side layout changes beyond the header.
