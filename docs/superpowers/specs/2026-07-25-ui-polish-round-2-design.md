# UI polish round 2 — tags, inactive markers, table ergonomics — design

**Date:** 2026-07-25
**Status:** Approved (design), not yet implemented

## Goal

Eight small, related web-UI improvements: make user tags first-class visual
objects (deterministic colors, colored map rings, chips, one-click chip
filtering), restore boligtype color to inactive dots (X overlay instead of
grey), turn popup links into buttons, fill the column-filter gaps
(Pris/kvm and friends), add a column picker with trimmed defaults, and add
the missing map→table deep link (mirror of the existing table→map "Kart"
handoff).

**Zero backend changes.** No API shape change, no migrations, no new
endpoints. Everything is `skannonser/web/static/*` only.

## Approach (decision record)

- **Tag colors: auto-assigned, hash-based** (user choice over semantic
  hardcoding and an editable mapping UI). A tag's color is a pure function
  of its normalized name — stable forever, zero config, new tags just work.
- **Tag filtering: colored quick-chips over the existing state** (user
  choice). No new filter state — chips are a second rendering of
  `filters.tagHidden`, so chips, the sidebar select-field, the table column
  popover, and cross-tab sync stay consistent by construction.
- **Column redundancy handled by defaults, not deletion** (user choice).
  Pris and Felleskost are semi-redundant with Totalpris and Mnd-kost
  (Totalpris = pris + omkostninger + fellesgjeld; Mnd-kost = felleskost +
  kommunale avg/12), but they stay in the API and the column list —
  default-hidden via the new column picker.
- **Inactive dots keep boligtype color, X drawn on top** (user request).
  The grey `INACTIVE_COLOR` branch goes away.

## 1. Tag colors — shared foundation (`tagcolors.js`, new)

- `assignTagColors(tagKeys) -> Map<normalizedTag, color>`: normalize
  (trim, lowercase), hash (djb2) into a fixed 10-color palette, resolving
  collisions by probing to the next free slot with tags processed in
  sorted order. A pure function of the current tag SET: distinct colors
  are guaranteed while ≤ 10 tags exist, and a tag keeps its hash slot
  unless an earlier-sorted tag claims it. (Amended from pure per-tag
  hashing: the user's actual tags "maybe" and "definitivt" collide under
  djb2 mod 10.) `colorForTag(tag, colors)` is the lookup; empty/null tag
  → `null` (no color).
- Palette is hue-offset from `TYPE_COLOR_PALETTE` in map.js so a tag ring
  is never confusable with its own boligtype dot color. Colors must keep
  white text readable (chips) and stand out on the OSM basemap (rings).
- Consumed by map ring (§2), chips (§3), table tag cell (§4), popup chip
  (§4).

## 2. Map: tag ring takes the tag's color

- app.js already stamps `hasTag` on each GeoJSON feature; it additionally
  stamps `tagColor` (from `colorForTag`).
- The `-tagring` layer (map.js) changes `circle-stroke-color` from the
  fixed purple `TAG_RING_COLOR` to `["get", "tagColor"]`. `TAG_RING_COLOR`
  is deleted.
- Ring geometry/opacity unchanged.

## 3. Colored quick-chips for tag filtering

- A chip row: one chip per tag in `vocabs.tags` (which already includes the
  `""` → "(uten tag)" bucket), colored via `colorForTag` ("(uten tag)"
  gets neutral grey). Click toggles that tag in `filters.tagHidden` and
  calls the normal onChange path (persist + render).
- Visible state: chip solid (colored bg, white text); hidden state: faded
  outline (transparent bg, colored border/text, reduced opacity).
- Mounts: (a) map sidebar — inside the Filtre panel, REPLACING the
  existing Tags select-field (chips carry the same state and are strictly
  more usable at this cardinality); (b) table — in the toolbar row. Both
  render from the same component in filters.js. The table's Tag column
  popover keeps its checkbox group.
- Chip list refreshes on `sk-annotation-saved` / vocab refresh, same as
  the existing tag vocab consumers.

## 4. Tag rendering in table + popup

- **Table Tag cell:** keeps the inline `<input>` (editing unchanged); the
  cell gets a colored accent — background tint + left border in the tag's
  color — applied/updated whenever the input's saved value changes.
- **Popup:** a colored tag chip next to the address line (only when a tag
  is set). The editor's Tag input is unchanged.

## 5. Inactive dots: boligtype color + X overlay

- map.js: `closedColorExpr` loses its grey branch — the `-sold` layer's
  normal-mode `circle-color` becomes plain `g.color` for sold AND
  inactive. `INACTIVE_COLOR` is deleted. Budpremie mode (`PREMIUM_COLOR`)
  is untouched.
- New per-group symbol layer `-inactive-x` above the `-sold` circle layer,
  filtered to closed-and-not-sold features: a small canvas-generated X
  icon (white stroke with dark outline for contrast on any dot color),
  built with the same register-once pattern as `ensureSquareIcon`. One
  shared icon (not per-color) since the X itself is color-neutral.
- `icon-opacity` follows the existing `OP` expression so nedtoning still
  fades the whole marker.
- The popup/table "Inaktiv"/"Trukket" badges are unchanged.

## 6. Popup Finn / Google Maps buttons (+ Tabell, §8)

- CSS only: `.sk-popup .links a` restyled as buttons — padding, border-
  radius, accent background, white text, hover/active states. All three
  links (Finn, Google Maps, Tabell) share the style.

## 7. Missing column filters + column picker

**New `COLUMN_FILTERS` entries (tablefilters.js):**

| column | kind | notes |
|---|---|---|
| `pris_kvm` | slider-max | step 1000; the gap the user hit |
| `sold_price` | slider-max | same bound family as pris |
| `premium` | slider-max | "maks budpremie %", step 1 |

Any needed new `*Max` state keys + bounds follow the existing
filterstate.js pattern (defaults = "Av", participate in
`activeFilterCount` / `resetFilters` / `listingExcluded`). Note
`listingExcluded` lives in filters.js and must learn the new predicates so
the MAP obeys them too — shared-state rule, same as every other filter.
Date filters (Først sett / Solgt dato) are out of scope; sorting covers
them.

**Column picker (table.js):**

- A "Kolonner" toolbar button opening the standard popover with one
  checkbox per `COLUMNS` entry (Adresse and Kart always on, not listed).
- Hidden set persisted in localStorage under the existing
  `skannonser.ui.v1` object (new `hiddenColumns` field, table page only).
- Default hidden (first run / no stored value): **Postnummer, Pris,
  Felleskost, Sov, Etg**.
- `renderHead` and `buildRow` skip hidden columns. A hidden column's
  filter stays active if set — it still counts in `activeFilterCount`
  and the status line, so filters can never invisibly hide rows.

## 8. Map → table deep link

- Popup links row gains **Tabell** → `/table#finnkode=<finnkode>`
  (button-styled per §6). Works for DNB rows too — the API ships a
  `finnkode` field for both sources.
- table.js gains `handleHash()` (init + `hashchange`), mirroring
  app.js's map-side handler:
  - Hash format `#finnkode=<id>` (also accept a bare id, like app.js).
  - If the target is `closed` and the sold bucket isn't loaded, fetch it
    first and flip the Vis-solgte toggle on (same cold-load race app.js
    solves).
  - The target row renders **even when active filters or the text filter
    would exclude it** (a one-row exemption in `visibleRows`, cleared when
    the hash changes/clears), in normal sort position; scrolled into view
    with a brief highlight flash (CSS animation on the row).
  - Unknown finnkode → status line says "Fant ikke annonse <id>".

## Testing & verification

No JS test harness exists for the static frontend; verification is manual
via the dev server + in-app browser, driven by the implementer:

1. Tags: set tags "maybe"/"definitivt"/"hard no" on three listings — rings
   take three distinct stable colors; chips appear on map + table; chip
   click hides/shows and syncs to the other page (cross-tab).
2. Inactive: X-marked dots keep boligtype color; nedtoning fades them;
   budpremie mode unaffected.
3. Table: Pris/kvm filter slider works and the map obeys it; column
   picker hides/shows and survives reload; defaults correct on a cleared
   localStorage.
4. Deep link: popup → Tabell lands scrolled + highlighted, including on a
   sold listing from a cold table load and on a listing excluded by an
   active filter.
5. Python API untouched — `pytest` suite must stay green (no changes
   expected).
