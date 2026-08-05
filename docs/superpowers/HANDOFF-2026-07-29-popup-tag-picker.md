# Handoff — coloured tag picker + auto-save in the map popup

**Status:** merged, deployed, and verified. Server is on `5fc460c` (container healthy). The closeOnClick bug found during verification is fixed — see §3.
**Spec:** `docs/superpowers/specs/2026-07-29-popup-tag-picker-design.md`
**Plan:** `docs/superpowers/plans/2026-07-29-popup-tag-picker.md`

## What shipped

The map popup's tag control was a native `<input list>` on a shared `<datalist>`. No
browser can style datalist options, so it could never show the tag colours. It is now a
row of coloured chips: click to set, click the selected one to clear, `+ ny tag` to mint.
The **Lagre button is gone** — kommentar saves on blur/Enter, a tag saves on chip click,
and the editor is flushed when the popup is torn down.

`TAG_PALETTE` grew 10 → 14. The live vocabulary had reached 11 tags, and `assignTagColors`
stops collision-probing once the vocabulary exceeds the palette, which had collapsed
**11 tags onto 7 colours** (`fin`=`wow`, `nja`=`too far`, `tja`=`fin, men nær veg`,
`fake?`=`veldig fin`). All 11 are now distinct.

The table view is unchanged by design — its Tag cells keep the datalist.

## 1. Deploy

No migrations on this branch, so no `db migrate` step.

```bash
ssh mbp2016@100.77.139.22 'cd ~/kode/skannonser && git pull && docker compose up -d --build'
```

**`--build` is load-bearing.** Application code and `web/static/` are baked into the image,
not bind-mounted (README line ~340), so `docker compose restart` deploys nothing. If you
have seen "frontend needs no restart" for another project of mine, it does not apply here.

`index.html` is auth-gated, so verify against an un-gated asset rather than the page:

```bash
curl -s https://<host>/tagpicker.js | head -5
```

Expect the module's header comment, not a login redirect.

## 2. Expect a one-time colour reshuffle — this is not a bug

`assignTagColors` hashes each tag into `TAG_PALETTE` by array position, so changing the
array's length changes which colour most tags land on. **Every existing tag will look
different** in the sidebar filter chips, the table's Tag cell accents, and the popup.
This was a deliberate, approved trade: it is the cost of getting 11 distinct colours.
It happens once, on deploy.

## 3. The closeOnClick bug — found, fixed, verified (`5fc460c`)

**Resolved. Recorded here because the mechanism is worth not rediscovering.**

The three-marker check reproduced it: markers 1 and 3 opened a popup, marker 2 did not.

`state.popup` is a **single reused Popup instance**, and `closeOnClick` (defaulting to
true) registered a map-level `click` handler bound to that instance. A marker click is
dispatched over a *snapshot* of the listener list: the layer delegate runs first and calls
`openPopup(B)`, whose `addTo()` re-adds the same instance — and then the snapshot's
now-stale handler still runs and removes the popup that was just opened. Marker 3 works
because the handler was unregistered when the popup was removed.

Pre-existing, but it also meant the marker→marker editor flush added in `81a14b8` — the
thing replacing the Lagre button's safety net — could never actually run.

Fix: `closeOnClick: false`, plus closing on a map click by hand, but only when
`queryRenderedFeatures` finds nothing under the cursor, so a click aimed at a dot, a tag
ring or a cluster leaves the popup alone. Verified on the deployed build: all three
markers open, and bare map still closes.

One deliberate behaviour change: **clicking a cluster now leaves the popup open**, where
`closeOnClick` used to close it.

A two-marker test passes even when this bug is present. It takes three.

## 4. What was verified, and what was not

Verified in a browser against a seeded DB: chips render 11 distinct colours with only the
listing's own filled; click-to-set; click-to-clear; minting a new tag yields a chip with a
real colour rather than the grey fallback; kommentar blur flashes and persists; a
typed-but-unblurred kommentar **plus** a chip click both persist from that single click;
`skFlush` saves unblurred text and is idempotent.

Verified by hand on the deployed build: the three-marker sequence in §3, and bare-map
click still closing the popup.

**Still resting on code reading alone:**

- MapLibre close via the X and Escape
- the `sk-popup-resized` re-pan when a new chip wraps the row

`popup.js` and `app.js` have **no automated coverage** — no test imports them — so manual
passes are the only gate on those two.

**Note for whoever automates this.** Claude's sandboxed browser cannot run these checks:
MapLibre GL never initialises in it. A minimal map built with an *empty* style — no
sources, no layers, zero network — never fires `load` and emits no error; it just hangs.
Two plausible-sounding explanations were tried and are both **wrong**: tiles are not
blocked (`fetch('https://tile.openstreetmap.org/...')` succeeds from the page), and it is
not ES-module caching (a fresh origin behaves identically). Do not spend time re-deriving
this — either test on a real browser, or drive the modules directly, which does work:
`import('/popup.js')`, build a popup node against a live `/api/listings` row, and interact
with it. That path verified the whole editor end to end, including real PUTs.

Tests on merged master: JS **104/104**, pytest **792 passed, 0 failed**.

## 5. Deferred findings, in the order I'd do them

Each is small and independent. None blocks the deploy.

1. **`annotations.js` — `saveAnnotation` is now a dead export.** `commitAnnotation` is its
   only caller. Drop the `export`.
2. **`popup.js` — `<label>Tag</label>` labels nothing** now the input is gone. It is a bare
   label above a group of buttons. Wrap the chip row in `role="group"` +
   `aria-labelledby`.
3. **The two views derive different colour maps.** `table.js` builds them from
   `state.vocabs.tags` (visible rows only); `app.js`/the popup builds them from all of
   `state.itemsById`. When the sets differ — e.g. Solgt hidden in one view — probing lands
   the same tag on a different hex, so a tag's table accent can disagree with its popup
   chip. Pre-existing, but far more visible now that the popup shows the whole palette at
   once.
4. **`app.js` duplicates the `state.tagColors` rebuild** verbatim in two places
   (`featureCollectionsByGroup` and the `sk-annotation-saved` handler), with a comment as
   the only guard against drift. Extract a `rebuildTagColors()` helper.
5. **`tagpicker.js` imports `tagOptionValues` from `tagoptions.js`** — the datalist module
   it replaces. Works, but couples the new component to the superseded one; that sort
   belongs in `tagcolors.js`.
6. **No `pagehide`/`beforeunload` flush.** Text typed and never blurred is lost on tab
   close or Back. All in-page paths are covered (any click blurs first). One line if it
   ever bites.
7. **Missing unit tests** in `tagpicker.test.mjs`: the `colorFor`-returns-null fallback
   (reachable in production) and the empty-vocabulary case.

## 6. Known merge collision, unrelated to deploying

`worktree-travel-sentinel-filter` has an unmerged 2-line change to `popup.js`'s import
block. Task 4 rewrote that block. Whenever that branch merges, expect a small mechanical
conflict there — nothing subtle.

## 7. Cleanup when you're done reading

The worktree still exists and holds the full execution record — progress ledger, six task
briefs, and every implementer and reviewer report — under
`.claude/worktrees/ui+popup-tag-picker/.superpowers/sdd/`. That directory is gitignored
scratch and `worktree remove` will take it with it.

```bash
git -C ~/kode/skannonser worktree remove .claude/worktrees/ui+popup-tag-picker
```

## Deliberately out of scope

The table's Tag cells (still a datalist); the three pre-existing palette entries that fail
white-text contrast (`#ff8f00` at 2.29:1, `#558b2f` at 4.10:1, `#d84315` at 4.44:1 —
tracked separately); keyboard navigation beyond native button tabbing; multi-select tags
(`annotations.tag` is a single `TEXT` column).
