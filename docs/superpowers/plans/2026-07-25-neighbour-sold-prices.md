# Neighbour Sold Prices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store every sold-price card the sweep already receives — including untracked neighbour sales — and surface "Solgt i nabolaget" in the map popup, at zero additional FINN requests.

**Architecture:** `parse_sold_card` widens to five more card fields; migration 011 adds them plus `discovered_near_finnkode` to `sold_prices` (same table — a neighbour card is the same entity as a tracked one; "tracked" is derived via `EXISTS` against `eiendom`, never stored). `run_sold_sweep`'s `collect()` stops discarding unknown finnkodes and anchors each neighbour to the tracked listing whose ~120 m query box surfaced it. A new `/api/listings/{finnkode}/nabolag` endpoint serves those anchored sales; the popup fetches it lazily.

**Tech Stack:** Python/SQLite/FastAPI backend, plain-JS popup, pytest.

**Spec:** `docs/superpowers/specs/2026-07-25-neighbour-sold-prices-design.md`

## Global Constraints

- **Zero additional FINN requests — a hard invariant, pinned by test.** Target selection, tier ordering, the inaktiv reserve, the attempts ceiling, budget caps, adaptive shrink, and throttle/suspend behavior are all untouched. Neighbours never consume budget, never mark a target matched, never appear in the attempts ledger.
- Same table (`sold_prices`), no `tracked` flag — derive with `EXISTS (SELECT 1 FROM eiendom …)`.
- Never fetch original asking prices for neighbours (politeness). The card's `priceSuggestion` is the asking price *at sale time* (possibly reduced); label it as final asking in comments/docs. Tracked listings' first-seen asking (`eiendom.pris`) is a different concept — do not conflate.
- `discovered_near_finnkode` is FILL-ONLY (first discovery anchor wins). The five card-fact columns are set-as-given. Existing fill-only semantics on `sold_price`/`cadastral_sold_date` must not regress.
- A target's own card never gets an anchor; anchors apply only to finnkodes not in the sweep's `known` set.
- Norwegian UI copy exact: section header `Solgt i nabolaget`, empty text `ingen registrerte nabolagssalg ennå`.
- Baseline suite: 641 passed. Run `.venv/bin/pytest tests/rebuild -q` at the end of every task and report the exact count.
- Frontend files live under `skannonser/web/static/` — never create a root-level `web/` dir. `node --check` every touched JS file.

---

### Task 1: Parser — five more card fields

**Files:**
- Modify: `skannonser/enrich/sold.py` (`parse_sold_card`, lines ~46-60)
- Test: `tests/rebuild/test_sold.py` (extend `_CARD` + assertions)

**Interfaces:**
- Produces: `parse_sold_card(doc) -> dict | None` records additionally carry `size`, `property_type`, `bedrooms`, `collective_debt`, `ownership_type` (all `None` when absent).

- [ ] **Step 1: Write the failing tests.** In `tests/rebuild/test_sold.py`, extend the module-level `_CARD` fixture (it already carries `propertyType` and `size` from the live shape) with the remaining keys, and add assertions:

```python
# _CARD gains (real endpoint keys, see the finn-sold-price-endpoint reference):
    "bedrooms": 3,
    "collectiveDebt": 120000,
    "ownershipType": "FREEHOLD",
```

```python
def test_parse_sold_card_captures_card_facts():
    rec = parse_sold_card(_CARD)
    assert rec["size"] == 150
    assert rec["property_type"] == "DETACHED"
    assert rec["bedrooms"] == 3
    assert rec["collective_debt"] == 120000
    assert rec["ownership_type"] == "FREEHOLD"


def test_parse_sold_card_card_facts_default_none():
    rec = parse_sold_card({"adId": 1})
    for key in ("size", "property_type", "bedrooms", "collective_debt", "ownership_type"):
        assert rec[key] is None
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q -k card_facts`
Expected: FAIL with `KeyError: 'size'`.

- [ ] **Step 3: Implement.** In `parse_sold_card`, extend the returned dict:

```python
        "size": doc.get("size"),
        "property_type": doc.get("propertyType"),
        "bedrooms": doc.get("bedrooms"),
        "collective_debt": doc.get("collectiveDebt"),
        "ownership_type": doc.get("ownershipType"),
```

(Skipped as marginal, per spec: `realtorOfficeName`, `salesCostSum`.)

- [ ] **Step 4: Run tests, then the full suite**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all pass (extra record keys are invisible to the repo until Task 2 — its upsert reads columns by name).

- [ ] **Step 5: Commit**

```bash
git add skannonser/enrich/sold.py tests/rebuild/test_sold.py
git commit -m "feat(sold): parse size/type/bedrooms/debt/ownership off the card"
```

---

### Task 2: Migration 011 + repository columns

**Files:**
- Create: `skannonser/store/migrations/011_neighbour_sold.sql`
- Modify: `skannonser/store/repositories/sold.py` (the `_FILL_ONLY`/`_SET` tuples, lines 12-14)
- Test: `tests/rebuild/test_sold.py` (repo round-trips), `tests/rebuild/test_migrations.py` (register 011 + new columns)

**Interfaces:**
- Produces: `sold_prices` columns `size INTEGER`, `property_type TEXT`, `bedrooms INTEGER`, `collective_debt INTEGER`, `ownership_type TEXT`, `discovered_near_finnkode TEXT`; `SoldPricesRepo.upsert` persists them (facts set-as-given, anchor fill-only).

- [ ] **Step 1: Write the migration**

```sql
-- 011_neighbour_sold.sql
-- Neighbour sold prices (2026-07-25 spec): the sweep now keeps EVERY card a
-- response carries, not just tracked targets -- sold data is the one dataset
-- that's expensive to re-acquire, and neighbouring sales are the best signal
-- for how a neighbourhood is priced. SAME table on purpose: a neighbour card
-- is the same entity as a tracked one (same endpoint, same fields, keyed by
-- finnkode); only its relationship to `eiendom` differs, and that is derived
-- via EXISTS, never stored (a flag could drift, a join can't). Every existing
-- consumer joins FROM eiendom, so untracked rows are invisible to the sold
-- bucket / promotion / coverage / budpremie by construction.
-- price_suggestion for neighbours is the asking price AT SALE TIME (possibly
-- reduced) -- tracked listings' first-seen asking lives in eiendom.pris.
ALTER TABLE sold_prices ADD COLUMN size INTEGER;
ALTER TABLE sold_prices ADD COLUMN property_type TEXT;
ALTER TABLE sold_prices ADD COLUMN bedrooms INTEGER;
ALTER TABLE sold_prices ADD COLUMN collective_debt INTEGER;
ALTER TABLE sold_prices ADD COLUMN ownership_type TEXT;
-- The tracked listing whose ~120 m query box surfaced this card ("sales near
-- X" lookup, no geocoding spend). NULL for --bbox probes and targets' own
-- cards. Fill-only: the FIRST discovery anchor wins.
ALTER TABLE sold_prices ADD COLUMN discovered_near_finnkode TEXT;
```

- [ ] **Step 2: Write the failing tests**

```python
def test_repo_persists_card_facts_and_anchor(conn):  # use the file's existing repo/conn fixture
    repo = SoldPricesRepo(conn)
    repo.upsert([{
        "finnkode": "900", "sold_price": 5000000, "sold_date": "2026-03-01",
        "price_suggestion": 4800000, "address": "Naboveien 1",
        "size": 90, "property_type": "FLAT", "bedrooms": 2,
        "collective_debt": 250000, "ownership_type": "PART_OWNERSHIP",
        "discovered_near_finnkode": "111",
    }])
    row = conn.execute("SELECT * FROM sold_prices WHERE finnkode='900'").fetchone()
    assert row["size"] == 90
    assert row["property_type"] == "FLAT"
    assert row["bedrooms"] == 2
    assert row["collective_debt"] == 250000
    assert row["ownership_type"] == "PART_OWNERSHIP"
    assert row["discovered_near_finnkode"] == "111"


def test_anchor_is_fill_only_but_facts_update(conn):
    repo = SoldPricesRepo(conn)
    repo.upsert([{"finnkode": "900", "discovered_near_finnkode": "111", "size": 90}])
    # Re-seen near a DIFFERENT target with a corrected size:
    repo.upsert([{"finnkode": "900", "discovered_near_finnkode": "222", "size": 92}])
    row = conn.execute(
        "SELECT discovered_near_finnkode, size FROM sold_prices WHERE finnkode='900'"
    ).fetchone()
    assert row["discovered_near_finnkode"] == "111"  # first anchor wins
    assert row["size"] == 92                          # facts are set-as-given
```

- [ ] **Step 3: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q -k "card_facts_and_anchor or fill_only_but_facts"`
Expected: FAIL (`no such column: size`).

- [ ] **Step 4: Implement.** In `skannonser/store/repositories/sold.py` change only the tuples (the f-string SQL derives everything from them):

```python
_FILL_ONLY = ("sold_price", "cadastral_sold_date", "discovered_near_finnkode")
_SET = (
    "sold_date",
    "price_suggestion",
    "address",
    "size",
    "property_type",
    "bedrooms",
    "collective_debt",
    "ownership_type",
)
```

Update the module docstring's fill-only sentence to mention the anchor. Register `011_neighbour_sold` in `tests/rebuild/test_migrations.py`'s `ALL_MIGRATIONS` (and its expected-columns assertions if that file pins `sold_prices` columns — read it and follow its conventions).

- [ ] **Step 5: Run tests + full suite**

Run: `.venv/bin/pytest tests/rebuild -q`
Expected: all pass (existing fill-only tests prove no regression on `sold_price`).

- [ ] **Step 6: Commit**

```bash
git add skannonser/store/migrations/011_neighbour_sold.sql skannonser/store/repositories/sold.py tests/rebuild/test_sold.py tests/rebuild/test_migrations.py
git commit -m "feat(sold): migration 011 -- card facts + discovered_near anchor on sold_prices"
```

---

### Task 3: Sweep keeps every card

**Files:**
- Modify: `skannonser/enrich/sold.py` (`collect()` closure + its call site + stats in `run_sold_sweep`, lines ~430-515)
- Test: `tests/rebuild/test_sold.py`

**Interfaces:**
- Produces: `run_sold_sweep`'s return dict gains `"neighbours_stored": int`. `matched`/`stored`/`tiles_queried`/attempts semantics unchanged. `run_sold_backlog` passes the new counter through untouched (it spreads `**stats` already).

- [ ] **Step 1: Write the failing tests.** Follow the file's existing sweep-test conventions (fake `fetch` returning canned `{"docs": [...]}` per bbox, seeded conn):

```python
def test_sweep_stores_neighbour_cards_with_anchor(conn):
    # One target ("111", tracked+seeded with coords); its box returns the
    # target's own card plus two neighbour cards we don't track.
    ...  # seed eiendom/eiendom_processed for "111" via the file's helpers
    docs = [
        {"adId": 111, "cadastralSoldPrice": 5000000},
        {"adId": 777, "cadastralSoldPrice": 4200000, "size": 80, "address": "Naboveien 7"},
        {"adId": 888, "cadastralSoldPrice": 6100000},
    ]
    calls = []
    def fetch(url, **kw):
        calls.append(url)
        return FakeResp({"docs": docs})
    stats = run_sold_sweep(conn, fetch=fetch,
                           targets=[{"finnkode": "111", "lat": 59.9, "lng": 10.7,
                                     "status": "solgt", "attempts": 0}])
    assert len(calls) == 1                      # ZERO extra requests: one box, period
    assert stats["matched"] == 1                # only the tracked target
    assert stats["neighbours_stored"] == 2
    own = conn.execute("SELECT discovered_near_finnkode FROM sold_prices WHERE finnkode='111'").fetchone()
    assert own["discovered_near_finnkode"] is None          # never self-anchored
    nb = conn.execute("SELECT discovered_near_finnkode, size FROM sold_prices WHERE finnkode='777'").fetchone()
    assert nb["discovered_near_finnkode"] == "111"
    assert nb["size"] == 80
    # neighbours never enter the attempts ledger
    assert conn.execute("SELECT COUNT(*) FROM sold_price_attempts WHERE finnkode IN ('777','888')").fetchone()[0] == 0


def test_neighbour_card_never_marks_a_target_matched(conn):
    # Target "222" is known but its card is NOT in the response; a neighbour
    # card is. The target must still count as unmatched (attempted, not matched).
    ...
    assert stats["matched"] == 0
    assert stats["neighbours_stored"] == 1


def test_neighbour_seen_in_two_boxes_keeps_first_anchor(conn):
    # Two targets, two boxes; the same neighbour card (adId 777) appears in
    # both responses. One stored row, anchored to the FIRST box's target.
    ...
    assert conn.execute("SELECT COUNT(*) FROM sold_prices WHERE finnkode='777'").fetchone()[0] == 1
    assert row["discovered_near_finnkode"] == first_target_finnkode
```

(The `...` arrange lines use the file's existing seeding helpers — copy the neighbouring sweep tests' concrete setup; every assert above is the contract.)

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q -k neighbour`
Expected: FAIL (`KeyError: 'neighbours_stored'`).

- [ ] **Step 3: Implement.** In `run_sold_sweep`, add `neighbour_records: dict[str, dict] = {}` next to `records`, and replace the `collect` closure and its call site:

```python
    def collect(docs, near_finnkode):
        for doc in docs:
            rec = parse_sold_card(doc)
            if rec is None:
                continue
            fk = rec["finnkode"]
            if fk in known:
                if fk in matched:
                    continue
                matched.add(fk)
                records.append(rec)
                continue
            # Neighbour sale we don't track: keep it -- we already paid the
            # request for this response (2026-07-25 spec, zero-extra-requests
            # invariant). Anchor it to the tracked listing whose box surfaced
            # it ("sales near X"); setdefault keeps the first in-run anchor
            # and the repo's fill-only column keeps the first across runs.
            # Neighbours never touch matched/attempts/stats for targets.
            rec["discovered_near_finnkode"] = near_finnkode
            neighbour_records.setdefault(fk, rec)
```

Call site inside `run_phase` becomes `collect(docs, t["finnkode"])`. After the existing `stats = SoldPricesRepo(conn).upsert(records)` line:

```python
    neighbour_stats = SoldPricesRepo(conn).upsert(list(neighbour_records.values()))
```

and the return dict gains:

```python
        "neighbours_stored": neighbour_stats["inserted"] + neighbour_stats["updated"],
```

(Note the target's own card is in `known`, so it takes the first branch and never receives an anchor — the self-anchor exclusion falls out of the branch order. `run_sold_enrich`'s `--bbox` path is untouched: `--all` already stores neighbours there, with `discovered_near_finnkode` absent → NULL.)

- [ ] **Step 4: Run the sold tests + full suite**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all pass — including every pre-existing sweep test unmodified (they prove `matched`/`stored`/attempts/budget semantics unchanged; the reserve/ceiling tests prove request counts unchanged).

- [ ] **Step 5: Commit**

```bash
git add skannonser/enrich/sold.py tests/rebuild/test_sold.py
git commit -m "feat(sold): keep every card -- neighbours stored with discovered_near anchor"
```

---

### Task 4: API — `/api/listings/{finnkode}/nabolag`

**Files:**
- Modify: `skannonser/web/api.py` (new route next to `get_listing_detail`)
- Test: `tests/rebuild/test_web_api.py`

**Interfaces:**
- Produces: `GET /api/listings/{finnkode}/nabolag` → `{"sales": [{finnkode, address, sold_price, sold_date, price_suggestion, size, property_type, bedrooms, price_per_m2, tracked}]}`, newest `sold_date` first, ≤ 15 rows, `[]` for any id with no anchored sales (no 404).

- [ ] **Step 1: Write the failing tests** (use the file's `client`/`db_path` fixtures and raw-SQL seeding conventions):

```python
def test_nabolag_lists_anchored_sales(client, db_path):
    # Seed sold_prices rows: two anchored to '111' (one tracked in eiendom,
    # one not), one anchored elsewhere, one with no anchor.
    ...
    data = client.get("/api/listings/111/nabolag").json()
    assert [s["finnkode"] for s in data["sales"]] == ["902", "901"]  # newest sold_date first
    by_fk = {s["finnkode"]: s for s in data["sales"]}
    assert by_fk["901"]["tracked"] is True    # 901 seeded in eiendom
    assert by_fk["902"]["tracked"] is False
    assert by_fk["901"]["price_per_m2"] == 50000   # 5_000_000 / 100
    assert by_fk["902"]["price_per_m2"] is None    # size NULL -> NULL, no crash


def test_nabolag_unknown_id_is_empty_not_404(client, db_path):
    resp = client.get("/api/listings/999999/nabolag")
    assert resp.status_code == 200
    assert resp.json() == {"sales": []}


def test_nabolag_caps_at_15(client, db_path):
    # Seed 20 anchored rows -> 15 returned.
    ...
    assert len(client.get("/api/listings/111/nabolag").json()["sales"]) == 15
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_web_api.py -q -k nabolag`
Expected: FAIL with 404 (route missing).

- [ ] **Step 3: Implement** (place directly after `get_listing_detail`; reuse the module's existing `_validate_finnkode` and `ro_conn`):

```python
@router.get("/listings/{finnkode}/nabolag")
def get_nabolag(
    finnkode: str, conn: sqlite3.Connection = Depends(ro_conn)
) -> dict:
    """Sold sales discovered in this listing's sweep boxes (~120 m) --
    incl. sales we never tracked (2026-07-25 neighbour-sold-prices spec).
    `tracked` is derived via EXISTS, never stored. `price_suggestion` is the
    asking price AT SALE TIME (possibly reduced) -- not first asking. Empty
    list (not 404) for ids without anchored sales: absence of neighbours is
    a normal state, not an error."""
    _validate_finnkode(finnkode)
    rows = conn.execute(
        """
        SELECT s.finnkode, s.address, s.sold_price, s.sold_date,
               s.price_suggestion, s.size, s.property_type, s.bedrooms,
               EXISTS(SELECT 1 FROM eiendom e WHERE e.finnkode = s.finnkode)
                   AS tracked
        FROM sold_prices s
        WHERE s.discovered_near_finnkode = ?
        ORDER BY COALESCE(s.sold_date, '') DESC, s.finnkode
        LIMIT 15
        """,
        (finnkode,),
    ).fetchall()
    sales = []
    for r in rows:
        price_per_m2 = None
        if r["sold_price"] and r["size"]:
            price_per_m2 = round(r["sold_price"] / r["size"])
        sales.append(
            {
                "finnkode": r["finnkode"],
                "address": r["address"],
                "sold_price": r["sold_price"],
                "sold_date": r["sold_date"],
                "price_suggestion": r["price_suggestion"],
                "size": r["size"],
                "property_type": r["property_type"],
                "bedrooms": r["bedrooms"],
                "price_per_m2": price_per_m2,
                "tracked": bool(r["tracked"]),
            }
        )
    return {"sales": sales}
```

- [ ] **Step 4: Run tests + full suite**

Run: `.venv/bin/pytest tests/rebuild/test_web_api.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/api.py tests/rebuild/test_web_api.py
git commit -m "feat(web-api): /api/listings/{finnkode}/nabolag -- anchored neighbour sales"
```

---

### Task 5: Popup — lazy "Solgt i nabolaget" section

**Files:**
- Modify: `skannonser/web/static/popup.js` (`buildPopupContent` + one new builder)
- Modify: `skannonser/web/static/style.css` (append)

**Interfaces:** consumes Task 4's endpoint. No other JS files change.

- [ ] **Step 1: Add the section builder** (above `buildPopupContent`):

```js
// "Solgt i nabolaget": sales the sold-price sweep discovered in this
// listing's ~120 m query boxes -- incl. sales we never tracked. Lazy: the
// fetch fires when the popup is built and fills in when it lands. Data
// accumulates from sweep responses only (no backfill exists), so this reads
// "ingen … ennå" for most listings at first.
function buildNabolagSection(item) {
  const wrap = el("div", "sk-nabolag");
  if (item.source === "dnb") return wrap; // DNB ids never anchor sweep boxes
  fetch("/api/listings/" + encodeURIComponent(item.finnkode) + "/nabolag")
    .then((resp) => (resp.ok ? resp.json() : { sales: [] }))
    .then(({ sales }) => {
      const head = el("p", "sk-nabolag-head", "Solgt i nabolaget" + (sales.length ? " (" + sales.length + ")" : ""));
      wrap.appendChild(head);
      if (!sales.length) {
        wrap.appendChild(el("p", "muted sk-nabolag-empty", "ingen registrerte nabolagssalg ennå"));
        return;
      }
      sales.slice(0, 5).forEach((s) => {
        const row = el("div", "sk-nabolag-row");
        if (s.tracked) {
          const a = el("a", null, s.address || s.finnkode);
          a.href = "/#finnkode=" + encodeURIComponent(s.finnkode);
          row.appendChild(a);
        } else {
          row.appendChild(el("span", null, s.address || "(ukjent adresse)"));
        }
        const parts = [];
        if (s.sold_price) parts.push(NOK.format(s.sold_price) + " kr");
        if (s.price_per_m2) parts.push(NOK.format(s.price_per_m2) + "/m²");
        const date = fmtDate(s.sold_date);
        if (date) parts.push(date);
        row.appendChild(el("span", "muted", parts.join(" · ")));
        wrap.appendChild(row);
      });
    })
    .catch(() => {
      /* popup stays useful without the section; no error noise */
    });
  return wrap;
}
```

- [ ] **Step 2: Mount it.** In `buildPopupContent`, directly after `root.appendChild(body);` and before `root.appendChild(buildEditor(item));`:

```js
  root.appendChild(buildNabolagSection(item));
```

- [ ] **Step 3: Styles** (append to `skannonser/web/static/style.css`):

```css
/* --- Solgt i nabolaget (2026-07-25) --- */
.sk-nabolag { padding: 6px 10px 2px; border-top: 1px solid rgba(0, 0, 0, 0.08); }
.sk-nabolag-head { margin: 0 0 4px; font-size: 12px; font-weight: 700; }
.sk-nabolag-empty { margin: 0 0 4px; font-size: 12px; }
.sk-nabolag-row { display: flex; justify-content: space-between; gap: 8px; font-size: 12px; margin: 2px 0; }
.sk-nabolag-row .muted { white-space: nowrap; }
```

- [ ] **Step 4: Check + commit**

Run: `node --check skannonser/web/static/popup.js`
Expected: exit 0. (Controller drives `buildPopupContent` live in Task 6.)

```bash
git add skannonser/web/static/popup.js skannonser/web/static/style.css
git commit -m "feat(web): lazy 'Solgt i nabolaget' section in the map popup"
```

---

### Task 6: Verification + README (controller-led)

**Files:**
- Modify: `README.md` (sold-price enrichment paragraph)

- [ ] **Step 1:** `.venv/bin/pytest tests/rebuild -q` → all green (report count).
- [ ] **Step 2:** Controller browser verification on the smoke DB: seed 2-3 `sold_prices` rows anchored to a visible listing (one tracked, one not, one size-less); drive `buildPopupContent` for that listing — section renders header + rows (kr, /m², date; tracked row links via `#finnkode=`); a listing without anchors shows "ingen registrerte nabolagssalg ennå"; a DNB popup shows no section; endpoint returns the seeded shape; zero console errors.
- [ ] **Step 3:** README — in the sold-price enrichment paragraph: the sweep now stores EVERY card a response carries (same `sold_prices` table; `discovered_near_finnkode` anchors neighbour sales to the tracked listing whose box surfaced them; zero extra requests), and the popup's "Solgt i nabolaget" section; note the final-asking caveat for neighbour `price_suggestion`. Commit as `docs: neighbour sold prices -- README`.

---

## Self-review notes (already applied)

- Spec §1→Task 1, §2→Task 2, §3 (repo semantics)→Task 2, §4→Task 3, §5→Tasks 4-5, §6 tests distributed per task; "zero extra requests" pinned in Task 3 Step 1 (`len(calls) == 1`).
- Type consistency: record keys (`size`…`discovered_near_finnkode`) identical across parser (T1), repo tuples (T2), sweep (T3), endpoint SELECT (T4); `neighbours_stored` produced in T3, spread through `run_sold_backlog`'s existing `**stats`.
- Tasks 3/4 use `...` arrange blocks by the repo's established convention (asserts are the contract; implementers copy the neighbouring tests' concrete seeding helpers).
- `run_sold_enrich`/`--bbox` deliberately untouched (spec §4); sheet export untouched (join-guarded, spec out-of-scope).
