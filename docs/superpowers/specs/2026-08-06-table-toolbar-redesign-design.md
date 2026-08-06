# Table toolbar redesign + unified status filter — design

Date: 2026-08-06
Status: approved

## Problem

The table page's toolbar is one flat wrapping row holding a text input, two
checkboxes, three visually identical buttons and twelve always-visible tag
chips. Four defects, all confirmed against the live deployment:

1. **"Alle" and "Tøm" are the same button.** `filters.js:485-486` gives both
   the identical handler, `selected.splice(0, selected.length)`. The comment
   above them asserts they "read differently to a user mid-filter"; they do
   not, because an empty selection *is* the unfiltered state.

2. **The counter's denominator ignores the sold toggle.** `table.js:649`
   reads `rows.length + " av " + state.items.length`. `state.items` only ever
   grows — `enableSold()` concatenates the closed bucket and unchecking the
   box never removes it. Toggling "Vis solgte/inaktive" on and then off
   leaves "867 av 4387": 3520 hidden rows still counted.

3. **Sold and inactive are indistinguishable on the table.** The API derives
   three closed statuses (`api.py:482`): Solgt, Inaktiv, Trukket. Live counts
   are 2959 / 561 / 0. One checkbox collapses all three.

4. **No visual hierarchy.** Fasiliteter, Kolonner and Nullstill filtre share
   `.toolbar-filter-btn`, whose entire rule is
   `{ font-size: 13px; cursor: pointer; }`. Twelve tag chips render
   permanently with counts, dominating the row.

Investigating (3) surfaced the root cause: **three overlapping mechanisms
control status visibility**, and no single one of them is authoritative.

| Mechanism | Scope | Storage | Granularity |
|---|---|---|---|
| `state.showSold` | table only | `skannonser.ui.v1` → `sold` | all-closed / none |
| `ui.sold`, `ui.inactive` | map only | `skannonser.ui.v1` → `ui` | Solgt / {Inaktiv,Trukket} |
| `tilgjengelighetSelected` | **both**, applied via `listingExcluded` | shared filter blob | all four statuses |

The table applies `tilgjengelighetSelected` but offers no control for it, so a
status filter set on the map can empty the table with nothing on screen
explaining why. This spec collapses all three into one.

A fifth issue lives in the same files but is **out of scope**, landing as its
own commit ahead of this work: `premiumPct` returns −100 % for closed
listings whose `sold_price` is null, because `Number(null) === 0` passes the
`Number.isFinite` guard (`listingmeta.js:106`). 13 listings show it today.

## Key insight

The map's layer buckets already map exactly onto the derived statuses.
`bucketOf` (`app.js:174`) returns `sold` when `item.sold`, which `api.py:351`
sets as `derived == "Solgt"`; it returns `inactive` for closed-but-not-sold,
i.e. `{Inaktiv, Trukket}`. The two map checkboxes and the four-value chip row
are therefore two encodings of one fact. Unifying them is a re-presentation,
not a new feature.

## Non-goals

- **Filter semantics do not change.** `applyChipClick`'s isolate-on-first-click
  rule and the "empty selection means unfiltered" convention both survive in
  `selectionExcludes`, so `filterstate.js` persistence and cross-tab sync are
  unaffected. The seeded default in §2 is a floor applied by the *pages* — it
  never stores an empty status selection — not a change to what empty means.
- **`ui.eie` / `ui.dnb` are untouched.** Those are genuine *source* layers
  (Finn.no vs DNB), orthogonal to status.
- **`bucketOf` survives.** It still drives marker grouping, colour, sold-dimming
  and clustering. Only *visibility gating* moves off it.
- No new filter dimensions. Every filter offered after this change exists today.
- The map's sidebar chip rows for the other five filters stay always-visible.
  It has a 280 px column with room; the table does not. The two pages will
  deliberately differ in filter presentation.

## Design

### 1. One status value

`filters.tilgjengelighetSelected` becomes the single source of truth for
status visibility across both pages. Its vocabulary is the four derived
values: `""` (Til salgs), `"Solgt"`, `"Inaktiv"`, `"Trukket"`.

**Map.** The two layer checkboxes `#toggle-sold` and `#toggle-inactive` are
replaced by a four-item **checkbox** list — Til salgs, Solgt, Inaktiv,
Trukket — bound to `tilgjengelighetSelected`, keeping the sidebar's existing
`.toggle` styling so it still reads as a layer control rather than a filter
chip row. The
now-redundant standalone Tilgjengelighet chip row (`filters.js:634`) is
removed — it was the same value under a second name.

`ui.sold` and `ui.inactive` are dropped from `defaultUi`. Three call sites
move to the status filter:

- `vocabItems()` (`app.js:188`) — gate on status for closed items, on
  `ui[bucketOf(it)]` only for `eie`/`dnb`.
- `vocabIsComplete()` (`app.js:196`) — becomes `ui.eie && ui.dnb &&`
  all four statuses selected `&& state.soldLoaded`.
- The render gate at `app.js:277`.

Stale `ui.sold`/`ui.inactive` keys in a returning reader's localStorage are
ignored, not migrated. Their default was `false`/`false`, which is exactly
what the seeded `[""]` default (below) produces, so nobody's view changes on
upgrade.

**Table.** `state.showSold` is deleted. `Status ▾` opens a popover rendering
`selectionChipRow` over `state.vocabs.tilgjengelighet` with
`emptyIsRealValue: true`. Same component, same stored value, chips instead of
checkboxes.

**Lazy fetch**, on both pages, derives from the selection:

```js
// A closed status is any selected value that is not "" (Til salgs).
const wantsClosed = (selected) => selected.some((k) => k !== "");
```

Selecting any closed status triggers the existing `ensureSoldLoaded()` /
`enableSold()` path, which fetches the closed bucket once and caches it in
`state.soldLoaded`. The existing rollback-on-failure behaviour in
`wireLayerToggles` is preserved: a failed fetch reverts the selection so the
UI never claims a layer it does not have.

### 2. The empty-selection problem, and the seeded default

An empty selection means *unfiltered* — every status. Combined with the lazy
fetch, the same stored value yields two different tables: a cold load shows
867 rows because the closed bucket was never fetched, while an empty selection
reached by ticking Solgt then pressing Nullstill shows 4387.

Resolution: **the status selection is never empty.**

- On first render, if `tilgjengelighetSelected` is empty, it is seeded to
  `[""]` (Til salgs) and persisted.
- Nullstill calls the shared `resetFilters` and then re-seeds, so
  `filterstate.js` keeps one definition of "default" and the seed is applied
  as a floor on top.

Cold load and post-reset agree, and the closed bucket is fetched only when a
closed status is explicitly chosen. Because the map now writes into the same
value, seeding is safe there too: ticking Solgt on the map adds `"Solgt"` to
the selection rather than being overruled by it.

### 3. Toolbar layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│ [🔍 Filtrer adresse, postnummer …]                                        │
│                                                                           │
│  Status ①  Tagger ②  Fasiliteter  (—•) Ukjent verdi  │  Kolonner  Nullstill│
│  ▔▔▔▔▔▔▔▔  ▔▔▔▔▔▔▔▔                                  │                     │
│                                                                           │
│  40 av 867 annonser · 3 filtre aktive                                     │
└──────────────────────────────────────────────────────────────────────────┘
```

Three tiers replace the current one:

**Filter buttons** — Status, Tagger, Fasiliteter. Pill-shaped, bordered, with
a `▾` affordance and a count badge when active. Active state is an accent
border plus tinted background, replacing today's colour-and-weight-only cue.

**View control** — Kolonner. Same pill geometry but muted, pushed right behind
a `margin-left: auto` separator. It changes what is displayed, not which rows
exist.

**Reset** — Nullstill filtre. Borderless text button in `--muted`, turning
accent when a filter is active, and `disabled` when
`activeFilterCount(state.filters, state.meta) === 0`.

**Ukjent verdi** stays a checkbox — same `id`, same `checked` semantics, same
`state.filters.includeUnknown` wiring — restyled as a switch via a wrapper
class and CSS only. No JS behaviour moves.

### 4. Chips move into popovers

`selectionChipRow` keeps its chips, colours and isolate-on-first-click rule.
Only the container changes.

- The two identical bulk buttons collapse into one `Nullstill` link in the
  popover header, rendered only when that row has a non-empty selection.
- The `label`-less variant — which exists solely because the toolbar row had
  no space for a heading — is retired. Every call site passes a `label`, so
  the `if (label)` branch goes away.

`Tagger ▾` mounts a `selectionChipRow` over `state.vocabs.tags` bound to
`filters.tagSelected` — what the toolbar renders inline today.

### 5. Counter semantics

`visibleRows()` splits into two passes so the denominator tracks status:

```js
function visibleSets() {
  const focused = (item) =>
    state.focusFinnkode && String(item.finnkode) === state.focusFinnkode;
  const universe = state.items.filter(
    (item) =>
      focused(item) ||
      !selectionExcludes(state.filters.tilgjengelighetSelected, item.tilgjengelighet || "")
  );
  const rows = universe.filter(
    (item) =>
      focused(item) ||
      (!listingExcluded(item, state.filters, state.meta) &&
        matchesFilter(item, state.filterText))
  );
  rows.sort((a, b) => compareItems(a, b, state.sortKey, state.sortDir));
  return { rows, universe: universe.length };
}
```

Status is the universe; everything else selects from it. The status line reads
`"40 av 867 annonser · 3 filtre aktive"`. Unticking Solgt drops 2959 rows out
of both numbers.

`listingExcluded` still applies the status filter internally — it is the shared
predicate and the map depends on that. The double application is idempotent
and deliberately not optimised away, so the two pages cannot drift.

The deep-link escape (`state.focusFinnkode`) applies to both passes, so a
focused row is never counted out of its own denominator.

### 6. Per-button badges

Badges derive from the existing `activeFilterEntries(filters, meta)` rather
than a parallel count, so a button's badge and "N filtre aktive" cannot disagree:

| Button      | Entry key              |
|-------------|------------------------|
| Status      | `tilgjengelighetSelected` |
| Tagger      | `tagSelected`          |
| Fasiliteter | `facilitiesRequired`   |

A button with no matching entry renders no badge and no active styling.

## Components and boundaries

| Unit | Responsibility | Depends on |
|------|----------------|------------|
| `wantsClosed(selected)` (new, `filters.js`) | Does this selection need the closed bucket? | — |
| `seedStatus(filters)` (new, `filterstate.js`) | Apply the `[""]` floor; used on load and after reset | — |
| `visibleSets()` (replaces `visibleRows`, `table.js`) | Two-pass filtering; rows + universe size | `selectionExcludes`, `listingExcluded`, `matchesFilter` |
| `statusBadges(entries)` (new, `tablefilters.js`) | Map `activeFilterEntries` to per-button counts | `activeFilterEntries` |
| `toolbarButtons` (new, `tablefilters.js`) | Build pills, wire popovers, paint badges | `openPopover`, `selectionChipRow`, `statusBadges` |

`wantsClosed`, `seedStatus`, `visibleSets` and `statusBadges` are pure and get
direct unit tests. The button builder is DOM-bound; it is covered through
`statusBadges` rather than by driving a headless DOM.

## Testing

Baselines on this worktree: **856 pytest, 183 node**.

New `tests/web/status.test.mjs`:

- `wantsClosed([])` false; `[""]` false; `["Solgt"]` true; `["", "Inaktiv"]` true.
- `seedStatus` turns `[]` into `[""]`, leaves `["Solgt"]` alone, and is idempotent.
- `resetFilters` followed by `seedStatus` yields `[""]`, matching cold load.

New `tests/web/toolbar.test.mjs`:

- `visibleSets` universe excludes rows whose status is deselected.
- `visibleSets` universe is unaffected by `filterText` and by non-status filters.
- A focused finnkode appears in both `rows` and `universe` even when its status
  is deselected.
- `statusBadges` maps entries to the right buttons; no badge when unfiltered.

Extend `tests/web/chiprow.test.mjs`:

- `selectionChipRow` renders exactly one bulk control, and only when the
  selection is non-empty.

Extend `tests/web/vocabs.test.mjs`:

- `vocabIsComplete` is false unless all four statuses are selected.

No Python changes, so pytest stays at 856. The −100 % fix commit carries its
own regression test and is counted separately.

## Staging

The work splits into four commits, each independently green and revertable:

1. **`fix(web): premiumPct null guard`** — the −100 % bug. Independent of
   everything below; lands first so it can ship even if the rest slips.
2. **`refactor(web): unify status filter`** — §1 and §2. Both pages, no visual
   redesign. The riskiest commit; verified before any chrome changes.
3. **`feat(web): table toolbar popovers`** — §3, §4, §6. Chips into popovers,
   button tiers, the switch.
4. **`fix(web): counter denominator`** — §5. Small and self-contained, but
   depends on the status filter from (2).

## Risks

- **Map regression is the main risk.** `ui.sold`/`ui.inactive` have four call
  sites (`vocabItems`, `vocabIsComplete`, the render gate, `wireLayerToggles`'s
  combine handler). Missing one leaves closed markers rendering while the
  status filter says they are hidden. The `vocabIsComplete` test above is the
  guard: it is the gate on irreversible, shared filter-value deletion via
  `pruneFilterSets`, so getting it wrong destroys stored filters.
- **Browser-pane verification is unreliable here.** MapLibre GL does not
  initialise in the pane, and `preview_start` reads the *main clone's*
  `launch.json`. The map half must be verified by curl-checking the served file
  is the worktree's, then driving the modules directly — or by hand.

## Verification

`node --test tests/web/*.test.mjs` and `PYTHONPATH=. ./.venv/bin/pytest`.

Manual: ticking Solgt fetches the closed bucket once and shows sold markers on
the map and sold rows in the table; the counter's denominator moves with the
status selection; Nullstill returns both pages to Til salgs; Nullstill is
disabled when no filter is active; the switch reflects and drives
`includeUnknown`; a returning reader with stale `ui.sold: true` sees Til salgs.
