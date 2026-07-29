# Coloured tag picker + auto-save in the map popup — design

**Date:** 2026-07-29
**Status:** Designed

## Goal

Make the map popup's tag control show each tag in its own colour, and save
without a button.

## The problem

The popup's tag field is a plain `<input list="sk-tag-options">` pointed at a
shared `<datalist>` ([`tagoptions.js`](../../../skannonser/web/static/tagoptions.js)).
That was a deliberate choice — a native datalist costs no focus, keyboard or
positioning code, and it works inside a MapLibre popup, which is a floating
element a hand-built dropdown would have to fight.

The cost of that choice is that **`<datalist>` options cannot be styled at
all**. No browser exposes them to CSS. So the one thing this change asks for —
seeing the tag colours while picking — is precisely the thing the current
control cannot do. The datalist has to go from the popup.

Saving is a second, independent irritation: the editor holds an explicit
"Lagre" button that PUTs both `kommentar` and `tag` together, while the table's
inline cells have auto-saved on blur/Enter since Phase 5. The two views
disagree about how editing works.

## Scope

Map popup only. The table's Tag column keeps its datalist and its current
appearance; the two tag inputs deliberately diverge in look. Revisiting the
table is a separate piece of work — a table cell is one row high and cannot
host a chip row without a different design.

## The picker

A **chip row**, not a dropdown. Every existing tag renders as a chip in its own
palette colour; the selected one is filled, the rest outlined. Clicking a chip
sets that tag, clicking the selected chip again clears it. A `+ ny tag` text
field below the row mints tags that do not exist yet.

Three things recommend this over rebuilding the dropdown as a custom listbox:

- `.maplibregl-popup-content` sets `overflow: hidden`
  ([`style.css:219`](../../../skannonser/web/static/style.css:219)) to clip the
  thumbnail into the popup's rounded corners. An absolutely-positioned dropdown
  panel would be **clipped by it**. Anything that opens has to open *in flow*,
  growing the popup — at which point it is a chip row with extra steps.
- The `.tag-chip` component already exists, already takes a `--tag-color`, and
  already renders exactly this filled/outlined pair in the filter panel
  ([`filters.js:485`](../../../skannonser/web/static/filters.js:485)).
- With auto-save, one click sets a tag. No typing, no confirm.

The cost is honest: roughly three extra lines of popup height, always, on every
popup. With 11 tags in the live vocabulary that is acceptable; if the
vocabulary grew past ~25 the chip row would need to become scrollable or
collapse behind a toggle.

## Colours: why the palette grows to 14, and not to 18

`assignTagColors` hashes each tag into `TAG_PALETTE` and probes to the next
free slot on collision — but only while the vocabulary fits the palette
([`tagcolors.js:38`](../../../skannonser/web/static/tagcolors.js:38)). Past that
point probing is disabled and plain hashing is the stable choice.

The live vocabulary is **11 tags against a 10-colour palette**, so probing is
off and the assignment currently yields **7 distinct colours for 11 tags**:

| collision | tags |
|---|---|
| `#c2185b` | `fin`, `wow` |
| `#303f9f` | `nja`, `too far` |
| `#ff8f00` | `tja`, `fin, men nær veg` |
| `#455a64` | `fake?`, `veldig fin` |

A coloured chip row with four indistinguishable pairs would not deliver the
feature, so the palette has to grow past 11 regardless.

**18 was investigated and rejected on measurement.** Three constraints bound
the available colour space: white text needs ≥4.5:1 contrast (which rules out
the entire yellow/amber band), tag colours should stay clear of the 12
boligtype colours, and the entries must stay visually apart. Searching the
Material 700/800/900 set under those constraints:

| palette size | worst pair in palette |
|---|---|
| 10 (today) | ΔE 25.5 |
| 14 built from scratch | ΔE 13.3 |
| 16 | ΔE 9.6 |
| 18 | ΔE 8.2 |

Rebuilding from scratch also discards all ten familiar colours for a worse
result. **Extending the existing ten** does far better: only colours ≥16 ΔE
from every current entry are eligible, and the four best are

```
#1b5e20   #311b92   #263238   #1565c0
```

giving a 14-colour palette whose closest pair is **ΔE 17.2** (`#0277bd` vs
`#1565c0`), with no pair under 15. Against the live vocabulary that is
**11 / 11 tags distinct, worst real pair ΔE 17.3** — up from 7/11 and ΔE 0.0.
Three spare slots remain before the probing guarantee lapses again.

Only the array literal changes. No logic in `tagcolors.js` moves.

### Known, pre-existing, not fixed here

Three current palette entries fail white-text contrast: `#558b2f` (4.10:1),
`#d84315` (4.44:1) and `#ff8f00` (**2.29:1**, well under AA). They are
untouched by this change — fixing them would alter colours the user already
recognises, for a reason unrelated to this work. Tracked separately.

## Components

**`tagcolors.js`** — `TAG_PALETTE` grows from 10 to 14 entries. Nothing else.

**`tagpicker.js`** *(new)* — one module, one concern, matching how
`tagcolors.js` / `tagoptions.js` / `listingmeta.js` are each scoped:

- `nextTagValue(current, clicked)` — pure. Returns `clicked`, or `""` when it
  equals `current` under `normalizeTag`. This is the click-again-clears rule,
  and it is the piece worth unit-testing.
- `buildTagPicker({ current, vocabulary, colorFor, onPick })` → DOM node
  carrying the chip row and the `+ ny tag` field.
- A `repaint(currentTag)` handle so a save can re-render selection and pick up
  a newly minted tag's colour.

It lives outside `popup.js` because that file is already 311 lines and would
land near 430 with the picker inlined.

**`popup.js`** — `buildEditor` loses the Lagre button, the feedback span and
the `attachTagList` import. Kommentar keeps its input and gains blur/Enter
commit. The root node exposes `skFlush()` for teardown.

**`annotations.js`** — absorbs `normalizeAnnotationValue` and the
skip-if-unchanged guard currently living in `table.js`. This is not tidying.
The comment at [`table.js:355`](../../../skannonser/web/static/table.js:355)
records that a no-op PUT bumps `updated_at`, and a bumped `updated_at` is the
exact signal sheet-import protection reads as "the user edited this row, do not
overwrite it" — so a blur that changed nothing was silently and permanently
flipping that protection on for untouched rows. A second auto-saving editor
that reimplements the guard would eventually reintroduce that bug. One shared
`commitAnnotation(item, { kommentar, tag })` makes it impossible.

`table.js` is edited only to call the moved helper; its behaviour is unchanged.

**`app.js`** — flush wiring, plus a live colour accessor.

**`style.css`** — chip sizing for the 280px popup, the `.saved` flash, and
removal of the now-unused `.sk-editor button` rules.

## Saving

Commit on **blur, Enter, and popup teardown**, matching the table and closing
the gap the Lagre button used to cover.

Teardown needs both paths covered, and only one of them fires an event:

1. MapLibre's own close — the X, a click on the map, Escape — calls
   `popup.remove()` internally and fires `close`. Catchable.
2. Clicking **another marker** calls `openPopup`, which reuses the single
   `state.popup` instance and calls `setDOMContent`
   ([`app.js:417`](../../../skannonser/web/static/app.js:417)). The old DOM is
   discarded with **no event at all**.

Relying on `blur` alone does not save either case: browsers do not reliably
fire `blur` on an element removed from the document while focused. So
`openPopup` flushes the outgoing content before swapping, and `close` is wired
once at popup creation.

### Live colours for a brand-new tag

`buildPopupContent`'s third argument changes from a `Map` snapshot to a
`() => state.tagColors` accessor. `state.tagColors` is rebuilt inside
`featureCollectionsByGroup` ([`app.js:254`](../../../skannonser/web/static/app.js:254)),
which `applyAll()` drives — so a tag invented in the popup only has a colour
*after* the `sk-annotation-saved` handler runs. Since `dispatchEvent` is
synchronous, the order is fixed: **dispatch, then repaint**. A repaint before
the dispatch would paint the new chip with no colour.

A repaint that adds a chip can wrap the row onto a new line, so it re-fires the
existing `sk-popup-resized` event and the established re-pan handles it.

## Data flow

```
click chip → nextTagValue → commitAnnotation → PUT
   → mutate item → dispatch sk-annotation-saved
   → app.js applyAll() rebuilds state.tagColors
   → picker repaint() off the accessor
   → row grew? dispatch sk-popup-resized → panPopupIntoView
```

Kommentar and `+ ny tag` follow the same path from blur/Enter/flush instead of
a click.

## Error handling

A failed PUT marks the control `.error`, leaves the typed value in the field,
and does **not** mutate `item` — so the value still reads dirty and the next
blur retries it. No alert, no modal.

Flush-on-close failures are silent apart from a `console.warn`. The popup is
already gone, there is nowhere to show anything, and the failure mode requires
the server to be down — at which point the map is already broken. This matches
how the "Solgt i nabolaget" fetch already fails.

## Testing

`node --test tests/web/*.test.mjs` — the directory form is broken on node v25.

- **`tests/web/tagpicker.test.mjs`** *(new)*: `nextTagValue` across pick,
  re-click-clears, and case/whitespace normalisation; then `buildTagPicker`
  against the hand-rolled `fakeDoc` stub that `tagoptions.test.mjs` established
  — chips in sorted vocabulary order, the selected chip alone lacking `.off`,
  and a click calling `onPick` with the right value.
- **`tests/web/tagcolors.test.mjs`** *(extend)*: 14 unique entries; the live
  11-tag vocabulary resolving to 11 distinct colours; no entry within ΔE 15 of
  another.
- **`tests/rebuild/test_web_static.py`**: the no-CDN guarantee globs
  `STATIC_DIR/*.js` and picks the new module up automatically. Add
  `tagpicker.js` to the explicit serving list at line 136.
- **Manual**, on the real map: click a chip; type a comment and click the map
  mid-word; type a comment and click a *different marker* mid-word; invent a
  tag and confirm the chip appears coloured without a reload.

## Not doing

- The table's Tag cells. Out of scope by decision.
- Keyboard navigation for the chip row beyond native button tabbing.
- Fixing the three pre-existing low-contrast palette entries.
- Multi-select tags. `annotations.tag` is a single `TEXT` column.
