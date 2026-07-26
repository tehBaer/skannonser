# Filter selection semantics — design

**Date:** 2026-07-26
**Status:** Approved (design), not yet implemented

## Goal

Make every value-list filter in the web UI answer one question — *which values do I want to see?* — with one interaction and one component. Today they answer the opposite question ("which values do I want hidden?"), in three different storage shapes, through two different controls.

## The problem

Three storage models coexist:

| Model | Filters | Shape |
|---|---|---|
| Hidden-set | boligtype, eieform, energimerking, tilgjengelighet, tag, lines | object, `{value: true}` = hidden |
| Selected-set | postnummer, nabolag | array, empty = filter off |
| Required-AND | facilities | object, all must be present |

Two controls: colored chip rows (tags, lines) and popover select-fields (boligtype, eieform, energimerking, tilgjengelighet).

The result is that two visually similar controls behave in opposite directions, and the same conceptual question is stored three ways.

## Decisions

Every decision below was made explicitly during brainstorming.

1. **Convert all six hidden-set filters to selection.** Not just the three originally named — leaving eieform, energimerking and tilgjengelighet on hidden semantics while their siblings select would mean two similar controls behaving in opposite ways, which is worse than today's inconsistency.
2. **Chips for all seven** (the six plus lines). Every value list is small enough: boligtype 6, eieform 3, energimerking 7, tilgjengelighet 3, tags a handful, lines 13. The select-field popover is retired.
3. **Discard existing filter state** for the converted filters on first load rather than migrating it.
4. **Lines keep "Alle / Ingen"**; the other six get **"Alle"** and **"Tøm"**.
5. Selection remains **shared between the map and table**, as filters are today.

## Interaction model

One rule plus one special case:

- **Empty selection shows everything.** This is the resting state and where "Tøm" returns you.
- **A chip toggles**: clicking an unselected chip selects it, clicking a selected chip deselects it.
- **Special case — the first selection clears the rest.** Clicking a chip while nothing is selected isolates that value.
- Removing the last selected chip returns to empty, so everything shows again. The same chip gets you in and out of an isolation.

Known limitation, accepted: with a selection active there is no single click that jumps to isolating a *different* value — you clear, then click. A modifier key was rejected as undiscoverable.

**Why this shape.** It turns filtering into a work queue. Isolate "(uten tag)" and each listing leaves the map as you tag it, because tagging changes whether it matches. That is the behaviour the owner asked for and the reason the migration below is worth its cost.

## Data model

Six `*Hidden` keys are replaced by `*Selected` arrays:

| Old | New |
|---|---|
| `boligtypeHidden` | `boligtypeSelected` |
| `eieformHidden` | `eieformSelected` |
| `energiHidden` | `energiSelected` |
| `tilgjengelighetHidden` | `tilgjengelighetSelected` |
| `tagHidden` | `tagSelected` |
| `stations.lineHidden` | `stations.lineSelected` |

All are arrays; empty means the filter is off. This is the shape `postnummerSelected` and `nabolagSelected` already use, so the codebase drops from three filter models to two — selection, plus `facilitiesRequired`, which keeps its required-AND semantics and is out of scope.

The shared predicate `listingExcluded` gets one selection helper that replaces the current hidden-set helper, and every converted filter routes through it. `activeFilterEntries` and `activeFilterCount` follow the same change, so a selection counts as an active filter exactly when it is non-empty.

### Empty values in the value lists

`""` remains a real, selectable value, rendered with its existing label — "(uten tag)", "Ingen status", "Ukjent boligtype". Selecting it isolates listings with no value for that field, which is the primary triage case for tags.

## Migration

**Old keys are dropped and the new ones start empty.** The six converted filters reset to "everything shows" once, on first load after the upgrade. Everything else in the stored blob — column choices, sliders, layer toggles, dimming, sold preference, facilities — is untouched.

**Why not invert.** Turning "hide maybe" into a selection requires the complete list of values in order to select everything else. For tags and tilgjengelighet that list is not known until listings load, and acting on a partial vocabulary is exactly the bug that silently destroyed saved filters earlier in this project. A partial migration (inverting only the three server-provided lists) was rejected: it is more code to half-rescue settings the owner is about to redo, and it would make two of the six behave differently on upgrade.

The removal must be unconditional on load, in the same place the blob is normalised, so a stale key cannot be re-persisted later.

## Components

One chip-row component serves all seven filters. It takes a value list, the selection array, an optional per-value colour, and a change callback. It renders:

- a heading naming the filter,
- one chip per value, showing the value's label and count,
- a bulk control: "Alle" and "Tøm" for six of them, "Alle" and "Ingen" for lines.

Selected and unselected chips must be distinguishable without relying on colour alone, since tags and lines carry their own colours: the existing filled-versus-outline treatment carries this.

The popover select-field is removed once nothing calls it.

### The table's column-header filters

Five of the six converted filters — boligtype, eieform, energimerke, tilgjengelighet and tag — are also reachable from the table's column headers, declared in `COLUMN_FILTERS` with `kind: "set"` and pointing at the same state keys. They are part of this conversion, not a follow-up: they read the storage being changed, so leaving them would break them outright.

Their popovers keep the funnel-in-a-column-header pattern, which is the right shape there and unrelated to the sidebar's chip decision. What changes is the semantics inside: checking a value selects it rather than un-hiding it, and the `"set"` kind is renamed to say what it now means. A column whose filter has a non-empty selection must still show as active in the header, exactly as today.

**Empty value list.** A filter whose value list is empty currently opens a blank popover with no explanation — a real bug, made more visible by this change, since an empty chip row is an unexplained gap. Each chip row shows a short muted message instead when it has no values. This matters in practice: eieform and energimerking are empty in the test database, though populated in production.

## Testing

The predicate and state changes are pure functions and are unit tested: selection semantics for each converted filter, empty-selection-means-all, the `""` value participating, the migration dropping old keys without touching unrelated ones, and `activeFilterCount` counting a non-empty selection.

The chip component and the sidebar wiring are DOM code, verified in the browser: the isolate-then-add sequence, "Alle"/"Tøm"/"Ingen", cross-page consistency, and the empty-list message.

**Environment note.** The map canvas does not render in the agent's browser, so anything drawn on the canvas is verified by tests and code reading only. The sidebar and the table page do render and are verified live. Filter *effects* on map markers need a human.

## Out of scope

Recorded so they are not lost, but deliberately excluded:

- **Sidebar information architecture** — segmentation, collapsed-by-default panels, a dedicated transparency control area, a quieter legend. Deferred on purpose: the owner wanted to decide the layout after living with the new chips. Note this change makes the sidebar taller, since four one-line fields become four wrapping chip rows; making chip selection a popup to reclaim that space is the owner's stated follow-up.
- **Tag autocomplete** when annotating a listing.
- **Comments and tags reachable in the table** without scrolling right — currently the last two columns.
- **Nudging co-located map dots apart** so two listings at one address are both clickable; approved separately, ~8 metres.
- **"Inkluder ukjent verdi"** — a single global switch governing every numeric filter at once. Assessed as not earning its place (energimerke is 29% populated in production, monthly cost 68%), but removing or replacing it is its own decision.
- **`facilitiesRequired`** keeps required-AND semantics.
