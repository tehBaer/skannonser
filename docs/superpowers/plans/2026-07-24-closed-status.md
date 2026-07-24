# Closed-Listing Status (Solgt / Inaktiv / Trukket) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Derive the true status of closed listings (Solgt / Inaktiv / Trukket) instead of labelling everything "Solgt", widen the sold-price sweep to in-grace Inaktiv listings (solgt-first priority), and split the UI accordingly (badges, muted styling, an Inaktiv/Trukket layer toggle, Eie→Finn.no rename).

**Architecture:** A pure `_derived_status` helper in the web API maps (raw status, sold_price, age-since-closed) → display status; stored data is never mutated. The sweep's target query widens to two tiers. The frontend splits the closed bucket client-side: `sold` (derived-genuine) vs `closed && !sold`, with a fourth layer toggle sharing the existing lazy fetch. Grace period `trukket_grace_days = 180` lives in `config/domain.toml`.

**Tech Stack:** Python (pydantic config, sqlite, FastAPI), plain-JS frontend, pytest.

**Spec:** `docs/superpowers/specs/2026-07-24-closed-status-design.md`

## Global Constraints

- **No stored-status mutation, no migrations.** `eiendom.tilgjengelighet` is read-only truth; promotion is display-only via `sold_prices.sold_price`.
- Derivation rule (spec §1, exact): raw `solgt` → **Solgt** (any price, any age); raw `inaktiv` + price → **Solgt**; raw `inaktiv` + no price + age < G → **Inaktiv**; raw `inaktiv` + no price + age ≥ G → **Trukket**. Age proxy: `eiendom.updated_at`. Open listings → derived `None` (raw status shown).
- G default **180**, config key `[sold] trukket_grace_days` in `config/domain.toml`, pydantic default 180 so old toml files stay valid.
- Sweep priority is a **strict two-tier order**: every raw-`solgt` target before any `inaktiv` target; existing fewest-attempts-then-density ordering preserved *within* each tier. Aged-out (≥ G) inaktiv rows are excluded from the target set entirely.
- Sheet export byte-identical (existing guard test must stay green); notifications/ingest/DNB untouched.
- API item contract: `sold` = derived Solgt only; new `closed: bool` for all three derived states; `tilgjengelighet` carries the derived label for closed items, raw for open; detail endpoint still exposes raw via the uppercase `Tilgjengelighet` raw-column spread.
- UI copy exact: "Finn.no" (Lag toggle), "Finn" (badge), "Inaktiv/Trukket" (new toggle label), "Vis solgte/inaktive" (table toggle), badges "Solgt"/"Inaktiv"/"Trukket". Muted dot colour `#9aa39c`.
- "Solgt nedtoning" governs the WHOLE closed set's opacity; budpremie colouring applies only to derived-Solgt (inactive items fall into the existing "Ingen tinglyst pris ennå" grey in premium mode).
- Python gates: `.venv/bin/pytest tests/rebuild -q` green after every backend task. JS gates: `node --check` per touched file; browser verification is the controller's (implementers never start servers).
- Real static dir is `skannonser/web/static/` — never create a root-level `web/`.

---

### Task 1: Config — `[sold] trukket_grace_days`

**Files:**
- Modify: `skannonser/config/domain.py` (new `Sold` model + field on `DomainConfig`)
- Modify: `config/domain.toml` (new section)
- Test: `tests/rebuild/test_domain_config.py` (find the existing domain-config test file with `grep -rl "load_domain" tests/rebuild/` and add there; if the file is named differently, use that file)

**Interfaces:**
- Produces: `load_domain().sold.trukket_grace_days -> int` (default 180).

- [ ] **Step 1: Write the failing tests** (append to the existing domain-config test file, following its fixture style):

```python
def test_sold_grace_days_default():
    # A config WITHOUT a [sold] section still loads, with the 180 default.
    # Use the existing minimal-toml fixture/helper in this file; do not add
    # [sold] to it.
    cfg = load_domain(path_to_minimal_toml_without_sold_section)
    assert cfg.sold.trukket_grace_days == 180


def test_sold_grace_days_from_toml(tmp_path):
    # Copy the minimal toml, append a [sold] section, assert override.
    body = minimal_toml_text + "\n[sold]\ntrukket_grace_days = 90\n"
    p = tmp_path / "domain.toml"
    p.write_text(body)
    assert load_domain(p).sold.trukket_grace_days == 90
```

(Adapt the two placeholders-in-intent — `path_to_minimal_toml_without_sold_section` / `minimal_toml_text` — to the file's real existing fixture; the asserts are the contract.)

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild -q -k "sold_grace"`
Expected: FAIL — `AttributeError: 'DomainConfig' object has no attribute 'sold'`

- [ ] **Step 3: Implement.** In `skannonser/config/domain.py`, after the `Crawl` class:

```python
class Sold(BaseModel):
    # Grace period for closed-listing status derivation + the sold-price
    # sweep's inaktiv tier (2026-07-24 closed-status spec): an Inaktiv
    # listing with no tinglyst price is "pending" (still swept, labelled
    # Inaktiv) until this many days after it closed, then derived "Trukket"
    # and dropped from the sweep's target set.
    trukket_grace_days: int = 180
```

and on `DomainConfig`, next to `crawl`: `sold: Sold = Sold()`.

In `config/domain.toml`, append:

```toml
[sold]
# Days after closing before a price-less Inaktiv listing is considered
# Trukket (withdrawn) rather than pending -- see the 2026-07-24 spec.
trukket_grace_days = 180
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/pytest tests/rebuild -q -k "sold_grace"` then `.venv/bin/pytest tests/rebuild -q`
Expected: new tests pass; full suite green.

- [ ] **Step 5: Commit**

```bash
git add skannonser/config/domain.py config/domain.toml tests/rebuild/
git commit -m "feat(config): [sold] trukket_grace_days = 180"
```

---

### Task 2: Sweep — inaktiv tier, strict solgt-first priority, coverage line

**Files:**
- Modify: `skannonser/enrich/sold.py` (`select_sold_targets`, `run_sold_sweep` ordering, new `inaktiv_pending`, `run_sold_backlog` plumbing)
- Modify: `skannonser/commands/run_cmd.py` (`enrich-sold`: pass grace from domain config; `--status` prints the inaktiv line)
- Test: `tests/rebuild/test_sold.py`

**Interfaces:**
- Consumes: `load_domain().sold.trukket_grace_days` (Task 1).
- Produces: `select_sold_targets(conn, min_age_days=None, grace_days=180)` — each target dict gains `"status": "solgt" | "inaktiv"`; `run_sold_sweep` unchanged signature but strict-tier ordering; `inaktiv_pending(conn, grace_days=180) -> {"pending", "priced"}`; `run_sold_backlog(..., grace_days=180)`.

- [ ] **Step 1: Write the failing tests.** Append to `tests/rebuild/test_sold.py`, reusing its existing seeding helpers (it seeds `eiendom` + `eiendom_processed` rows and fake fetches; read the file and follow its idioms — the asserts below are the contract):

```python
def test_targets_include_inaktiv_within_grace(conn):
    # solgt row, inaktiv row closed 10 days ago, inaktiv row closed 200 days ago
    seed_closed(conn, "111", status="Solgt", updated_days_ago=10)
    seed_closed(conn, "222", status="Inaktiv", updated_days_ago=10)
    seed_closed(conn, "333", status="Inaktiv", updated_days_ago=200)
    targets = select_sold_targets(conn, grace_days=180)
    by_fk = {t["finnkode"]: t for t in targets}
    assert set(by_fk) == {"111", "222"}          # aged-out inaktiv excluded
    assert by_fk["111"]["status"] == "solgt"
    assert by_fk["222"]["status"] == "inaktiv"


def test_targets_inaktiv_with_price_excluded(conn):
    seed_closed(conn, "222", status="Inaktiv", updated_days_ago=10)
    seed_sold_price(conn, "222", sold_price=5000000)
    assert select_sold_targets(conn, grace_days=180) == []


def test_sweep_orders_solgt_tier_first(conn):
    # Two inaktiv targets DENSER than the solgt target; with a 1-request
    # budget the solgt target must still be attempted first.
    # (Seed coordinates so the inaktiv pair are neighbours, the solgt target
    # is isolated; give the solgt target MORE prior attempts than the
    # inaktiv ones -- tier must beat both density and attempts.)
    ...
    stats = run_sold_sweep(conn, fetch=fake_fetch, targets=targets,
                           max_requests=1, order_by_density=True)
    assert attempted_finnkodes(conn) == ["<the solgt finnkode>"]


def test_sweep_stores_price_for_inaktiv_target(conn):
    # An inaktiv in-grace target whose bbox returns a card with a price:
    # the price must be stored (this is the whole point of the widening).
    ...
    row = conn.execute("SELECT sold_price FROM sold_prices WHERE finnkode='222'").fetchone()
    assert row["sold_price"] == 4500000


def test_inaktiv_pending_counts(conn):
    seed_closed(conn, "222", status="Inaktiv", updated_days_ago=10)   # pending
    seed_closed(conn, "333", status="Inaktiv", updated_days_ago=200)  # aged out
    seed_closed(conn, "444", status="Inaktiv", updated_days_ago=10)
    seed_sold_price(conn, "444", sold_price=1)                        # priced
    out = inaktiv_pending(conn, grace_days=180)
    assert out == {"pending": 1, "priced": 1}
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q`
Expected: new tests FAIL (`status` KeyError / `inaktiv_pending` NameError); existing tests still pass.

- [ ] **Step 3: Implement in `sold.py`.**

`select_sold_targets` — new signature and WHERE (keep the docstring's spirit, document the tier field):

```python
def select_sold_targets(conn, min_age_days: Optional[int] = None, grace_days: int = 180) -> list[dict]:
    age_clause = ""
    params: list = [f"-{int(grace_days)} days"]
    if min_age_days is not None:
        age_clause = "AND e.updated_at < datetime('now', ?)"
        params.append(f"-{int(min_age_days)} days")
    rows = conn.execute(
        f"""
        SELECT e.finnkode AS finnkode, p.lat AS lat, p.lng AS lng,
               LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) AS status,
               COALESCE(a.attempts, 0) AS attempts
        FROM eiendom e
        JOIN eiendom_processed p ON e.finnkode = p.finnkode
        LEFT JOIN sold_prices s ON e.finnkode = s.finnkode
        LEFT JOIN sold_price_attempts a ON e.finnkode = a.finnkode
        WHERE (
            LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'solgt'
            OR (
              LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'inaktiv'
              AND e.updated_at >= datetime('now', ?)
            )
          )
          AND p.lat IS NOT NULL AND p.lng IS NOT NULL
          AND (s.finnkode IS NULL OR s.sold_price IS NULL)
          {age_clause}
        """,
        params,
    )
    return [
        {"finnkode": str(r["finnkode"]), "lat": r["lat"], "lng": r["lng"],
         "status": r["status"], "attempts": r["attempts"]}
        for r in rows
    ]
```

`run_sold_sweep` ordering (replace the current `order = targets` / `if order_by_density` block):

```python
    # Strict two-tier priority (2026-07-24 spec): every raw-Solgt target
    # before any Inaktiv target -- Solgt listings are far likelier to have a
    # tinglyst price, so a tight budget goes to them first. Within a tier
    # the existing fewest-attempts-then-density ordering applies.
    tier = lambda t: 0 if t.get("status", "solgt") == "solgt" else 1  # noqa: E731
    if order_by_density:
        order = sorted(
            targets,
            key=lambda t: (
                tier(t),
                t.get("attempts", 0),
                -len(_targets_in_bbox(targets, target_bbox(t, pad_lon, pad_lat))),
            ),
        )
    else:
        order = sorted(targets, key=tier)  # stable sort keeps given order within tiers
```

New `inaktiv_pending` (next to `sold_coverage`):

```python
def inaktiv_pending(conn, grace_days: int = 180) -> dict:
    """The inaktiv sweep tier at a glance: how many in-grace Inaktiv listings
    still await a price (``pending``) and how many Inaktiv listings have been
    priced -- i.e. promoted to derived-Solgt (``priced``)."""
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN s.sold_price IS NULL
                    AND e.updated_at >= datetime('now', ?) THEN 1 ELSE 0 END) AS pending,
          SUM(CASE WHEN s.sold_price IS NOT NULL THEN 1 ELSE 0 END) AS priced
        FROM eiendom e
        JOIN eiendom_processed p ON e.finnkode = p.finnkode
        LEFT JOIN sold_prices s ON e.finnkode = s.finnkode
        WHERE LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'inaktiv'
          AND p.lat IS NOT NULL AND p.lng IS NOT NULL
        """,
        (f"-{int(grace_days)} days",),
    ).fetchone()
    return {"pending": row["pending"] or 0, "priced": row["priced"] or 0}
```

`run_sold_backlog`: add parameter `grace_days: int = 180`, pass it to `select_sold_targets(conn, min_age_days=min_age_days, grace_days=grace_days)`.

- [ ] **Step 4: CLI plumbing in `run_cmd.py`.** In the `enrich-sold` command: load the domain config the same way other commands in the file do (`load_domain()` is already imported there or import it), and:
  - pass `grace_days=domain.sold.trukket_grace_days` to `run_sold_backlog(...)`;
  - in the `--status` branch, after the existing coverage line, add:

```python
        pend = inaktiv_pending(conn, domain.sold.trukket_grace_days)
        typer.echo(
            f"  inaktiv tier: {pend['pending']} pending (in grace), "
            f"{pend['priced']} priced (promoted to Solgt)"
        )
```

(add `inaktiv_pending` to the `from skannonser.enrich.sold import (...)` block).

- [ ] **Step 5: Run tests + full suite**

Run: `.venv/bin/pytest tests/rebuild/test_sold.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all green (update any existing `select_sold_targets` test that asserts on returned dict keys — the new `status` key is additive).

- [ ] **Step 6: Commit**

```bash
git add skannonser/enrich/sold.py skannonser/commands/run_cmd.py tests/rebuild/test_sold.py
git commit -m "feat(sold): inaktiv sweep tier with strict solgt-first priority + pending coverage"
```

---

### Task 3: API — `UPDATED_AT` column, `_derived_status`, item contract

**Files:**
- Modify: `skannonser/publish/rows.py` (`_EIE_SELECT_TAIL`)
- Modify: `skannonser/web/api.py` (`_derived_status` helper, `_eie_item` rename `sold=` → `closed=` + new fields, all four call sites)
- Test: `tests/rebuild/test_web_api.py`, `tests/rebuild/test_export.py`

**Interfaces:**
- Consumes: `domain.sold.trukket_grace_days` (Task 1).
- Produces: every eie-shaped record carries `"UPDATED_AT"`; API items carry `sold` (derived-genuine), `closed` (bool), derived `tilgjengelighet` for closed items; `_derived_status(rec, grace_days) -> str | None` (returns "Solgt"/"Inaktiv"/"Trukket"/None).

- [ ] **Step 1: Write the failing tests.** Append to `test_web_api.py` (its raw-SQL seeding helpers; set `updated_at` explicitly with `datetime('now', '-N days')` when inserting):

```python
def test_closed_raw_solgt_is_solgt_even_without_price(client, db_path):
    # active=0, tilgjengelighet='Solgt', no sold_prices row, closed 300 days ago
    item = ...  # from GET /api/listings?bucket=sold
    assert item["sold"] is True
    assert item["closed"] is True
    assert item["tilgjengelighet"] == "Solgt"


def test_closed_inaktiv_with_price_promoted_to_solgt(client, db_path):
    # active=0, 'Inaktiv', sold_prices.sold_price set
    assert item["sold"] is True
    assert item["tilgjengelighet"] == "Solgt"
    assert item["sold_price"] == 4500000


def test_closed_inaktiv_young_is_inaktiv(client, db_path):
    # 'Inaktiv', no price, updated_at 10 days ago
    assert item["sold"] is False
    assert item["closed"] is True
    assert item["tilgjengelighet"] == "Inaktiv"


def test_closed_inaktiv_aged_is_trukket(client, db_path):
    # 'Inaktiv', no price, updated_at 200 days ago (grace 180)
    assert item["sold"] is False
    assert item["tilgjengelighet"] == "Trukket"


def test_open_listing_keeps_raw_status_and_not_closed(client, db_path):
    # active=1, tilgjengelighet='Kommer for salg'
    item = ...  # from GET /api/listings (active set)
    assert item["closed"] is False
    assert item["sold"] is False
    assert item["tilgjengelighet"] == "Kommer for salg"


def test_detail_exposes_raw_and_derived(client, db_path):
    # aged price-less Inaktiv listing:
    data = client.get("/api/listings/111").json()
    assert data["tilgjengelighet"] == "Trukket"   # derived (item spread)
    assert data["Tilgjengelighet"] == "Inaktiv"   # raw (column spread)
    assert data["closed"] is True
```

Append to `test_export.py` a guard that the new SELECT column doesn't leak into the sheet payload:

```python
def test_eie_sheet_payload_unchanged_by_updated_at_column(...):
    # Same pattern as test_eie_sheet_payload_unchanged_by_details: capture
    # eie_rows() before/after is not applicable here (the column is
    # unconditional) -- instead assert no header named "UPDATED_AT" is in
    # EIE_HEADER and eie_rows()[0] keys == EIE_HEADER (payload shape pinned).
```

(Write it as a real test against the file's existing helpers.)

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_web_api.py -q -k "closed or derived or raw_status"`
Expected: FAIL (missing `closed` key etc.).

- [ ] **Step 3: Implement.** In `rows.py`, `_EIE_SELECT_TAIL` gains (after the `"SCRAPED_AT"` line, before the `ld.` block):

```python
    e.updated_at AS "UPDATED_AT",
```

In `api.py`, add near `_sold_from_hidden`:

```python
def _age_days(updated_at) -> float | None:
    """Days since ``eiendom.updated_at`` ('YYYY-MM-DD HH:MM:SS', UTC).
    None when missing/unparsable -- callers treat that as 'young' (keep the
    listing pending rather than prematurely stamping Trukket)."""
    if not updated_at:
        return None
    try:
        dt = datetime.strptime(str(updated_at)[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return (datetime.utcnow() - dt).total_seconds() / 86400.0


def _derived_status(rec: dict, grace_days: int) -> str | None:
    """Closed-listing display status (2026-07-24 closed-status spec §1):
    raw Solgt -> "Solgt"; raw Inaktiv + tinglyst price -> "Solgt" (promoted);
    price-less Inaktiv -> "Inaktiv" inside the grace window, "Trukket" after.
    None for open listings (raw status is shown instead). Stored status is
    never mutated -- this is display-only derivation."""
    if rec.get("_active"):
        return None
    raw = str(rec.get("Tilgjengelighet") or "").strip().lower()
    if raw == "solgt":
        return "Solgt"
    if raw != "inaktiv":
        return None
    if rec.get("SOLD_PRICE") is not None:
        return "Solgt"
    age = _age_days(rec.get("UPDATED_AT"))
    if age is None or age < grace_days:
        return "Inaktiv"
    return "Trukket"
```

(add `from datetime import datetime` to api.py's imports if absent).

`_eie_item`: rename the keyword-only `sold` parameter to `closed`; body top becomes:

```python
    derived = _derived_status(rec, domain.sold.trukket_grace_days) if closed else None
    sold = derived == "Solgt"
```

and in the item dict: `"tilgjengelighet": derived if derived is not None else rec.get("Tilgjengelighet"),` plus `"sold": sold, "closed": derived is not None,`. The sold-outcome block changes its guard from `if sold:` to `if closed:` (whole closed bucket ships the keys, as the spec pins) — keep the keys/comment otherwise.

Call sites: `bucket=sold` branch and the `sold=1` merged branch pass `closed=True`; the actives comprehension passes `closed=False`; the detail endpoint passes `closed=_sold_from_hidden(rec)`.

- [ ] **Step 4: Run tests + full suite**

Run: `.venv/bin/pytest tests/rebuild/test_web_api.py tests/rebuild/test_export.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all green. Existing tests asserting `item["sold"] is True` for closed seeds with raw 'Inaktiv' will need their expectation updated to the derived contract — update them to assert the new semantics, do not weaken them.

- [ ] **Step 5: Commit**

```bash
git add skannonser/publish/rows.py skannonser/web/api.py tests/rebuild/test_web_api.py tests/rebuild/test_export.py
git commit -m "feat(web-api): derived Solgt/Inaktiv/Trukket status + closed flag (display-only promotion)"
```

---

### Task 4: Map — layer split, muted paint, toggles, rename

**Files:**
- Modify: `skannonser/web/static/map.js` (closed-based filters/variant, muted colour case, `setSoldColorMode` off-branch)
- Modify: `skannonser/web/static/app.js` (ui key, bucketOf, feature props, toggles, hash, newSinceLast)
- Modify: `skannonser/web/static/index.html` (Lag panel)
- Modify: `skannonser/web/static/style.css` (badge class, appended)

**Interfaces:**
- Consumes: API items with `sold`/`closed`/derived `tilgjengelighet` (Task 3).
- Produces: four layer toggles; features carry `sold` AND `closed` properties; `groupIdForItem` variant-selects on `item.closed`.

- [ ] **Step 1: index.html Lag panel** — replace the three toggle lines with:

```html
        <label class="toggle"><input type="checkbox" id="toggle-eie" checked> Finn.no</label>
        <label class="toggle"><input type="checkbox" id="toggle-dnb" checked> DNB</label>
        <label class="toggle"><input type="checkbox" id="toggle-sold"> Solgt</label>
        <label class="toggle"><input type="checkbox" id="toggle-inactive"> Inaktiv/Trukket</label>
```

- [ ] **Step 2: app.js.**
  - `defaultUi`: after `sold: false,` add `inactive: false,`.
  - `bucketOf`:

```js
function bucketOf(item) {
  if (item.sold) return "sold";
  if (item.closed) return "inactive";
  if (item.source === "dnb") return "dnb";
  return "eie";
}
```

  - `itemToFeature` properties: keep `sold: !!item.sold` and add `closed: !!item.closed` right after it.
  - `featureCollectionsByGroup`: the opacity line becomes `const op = excluded ? residual : item.closed ? soldOpacity : 1;` (whole closed set rides "Solgt nedtoning").
  - `wireLayerToggles`: the id map becomes `{ eie: "toggle-eie", dnb: "toggle-dnb", sold: "toggle-sold", inactive: "toggle-inactive" }`, and the lazy-load condition becomes `if ((bucket === "sold" || bucket === "inactive") && input.checked)` (same `ensureSoldLoaded()` body, incl. the rollback path resetting `state.ui[bucket]`).
  - `groupIdForItem` call sites pass items as today (the variant logic moves into map.js, Step 3).
  - `handleHash`: replace the sold-flip block with:

```js
  const bucket = bucketOf(item);
  if ((bucket === "sold" || bucket === "inactive") && !state.ui[bucket]) {
    state.ui[bucket] = true;
    const cb = document.getElementById(bucket === "sold" ? "toggle-sold" : "toggle-inactive");
    if (cb) cb.checked = true;
    saveUi();
  }
```

  - `newSinceLast` counting: `if (!item.sold && …)` → `if (!item.closed && …)` (closed listings are never "nye").

- [ ] **Step 3: map.js.**
  - `groupIdForItem` (line ~131): variant selection switches from `item.sold` to `item.closed`: `const variant = combineSold ? "both" : item.closed ? "sold" : "active";` (internal variant/source names stay "sold" — data-plumbing rename is not worth the churn; add a one-line comment saying the "sold" variant now means "closed").
  - Filters: `IS_SOLD` / `NOT_SOLD` become closed-based:

```js
const IS_CLOSED = ["==", ["get", "closed"], true];
const NOT_CLOSED = ["==", ["get", "closed"], false];
```

  Replace every `IS_SOLD`/`NOT_SOLD` usage with `IS_CLOSED`/`NOT_CLOSED` (the `-eie`/`-dnb` layers gate on NOT_CLOSED, the `-sold` layer on IS_CLOSED).
  - Muted colour: add near `SOLD_BORDER`:

```js
// Closed-without-a-sale (derived Inaktiv/Trukket) dots: one neutral grey,
// not boligtype-coloured -- visually quiet, distinct from genuine Solgt.
const INACTIVE_COLOR = "#9aa39c";
// Normal-mode colour for a closed layer: genuine sold keeps the group's
// boligtype colour, inactive/trukket goes grey.
const closedColorExpr = (color) => ["case", ["==", ["get", "sold"], true], color, INACTIVE_COLOR];
```

  In `addListingGroups`, the `-sold` layer's `"circle-color": g.color` becomes `"circle-color": closedColorExpr(g.color)`. In `setSoldColorMode`, the non-premium branch's `g.color` likewise becomes `closedColorExpr(g.color)` (the premium branch is untouched — priceless items already fall to the "Ingen tinglyst pris ennå" grey).

- [ ] **Step 4: style.css** (append):

```css
/* --- closed-status (2026-07-24): muted badge for Inaktiv/Trukket --- */
.source-tag.inactive, .inactive-badge {
  background: #9aa39c;
  color: #fff;
}
.inactive-row td { color: var(--muted, #8a938c); }
```

- [ ] **Step 5: Gates**

Run: `node --check skannonser/web/static/app.js && node --check skannonser/web/static/map.js`
Expected: exit 0 both. `grep -n "IS_SOLD\|NOT_SOLD" skannonser/web/static/map.js` → no matches.

- [ ] **Step 6: Commit** (controller browser-verifies after this task)

```bash
git add skannonser/web/static/map.js skannonser/web/static/app.js skannonser/web/static/index.html skannonser/web/static/style.css
git commit -m "feat(web): Inaktiv/Trukket map layer -- muted dots, fourth toggle, Finn.no rename"
```

---

### Task 5: Popup + table — derived badges, muted rows, toggle label

**Files:**
- Modify: `skannonser/web/static/popup.js` (source-tag + price-block gate)
- Modify: `skannonser/web/static/table.js` (row class, badge, sold-toggle gate)
- Modify: `skannonser/web/static/table.html` (toggle label)

**Interfaces:** consumes API items with `sold`/`closed`/derived `tilgjengelighet` (Task 3) and the `.inactive-badge`/`.source-tag.inactive`/`.inactive-row` CSS (Task 4).

- [ ] **Step 1: popup.js.** Replace the source-tag lines (currently `item.sold ? " sold" : …` / `item.sold ? "Solgt" : …`):

```js
  const tagClass =
    "source-tag" +
    (item.sold ? " sold" : item.closed ? " inactive" : item.source === "dnb" ? " dnb" : "");
  const tag = el("span", tagClass);
  tag.textContent = item.sold
    ? "Solgt"
    : item.closed
      ? item.tilgjengelighet // derived "Inaktiv" / "Trukket"
      : item.source === "dnb"
        ? "DNB"
        : "Finn";
  addr.appendChild(tag);
```

(The existing `if (item.sold)` price block needs NO change — `item.sold` is now derived-genuine, so Inaktiv/Trukket items automatically skip the sold-price rows, including "ingen tinglyst pris ennå", while raw-Solgt-without-price still shows it.)

- [ ] **Step 2: table.js.**
  - Row class in `buildRow`: `el("tr", item.sold ? "sold-row" : null)` → `el("tr", item.sold ? "sold-row" : item.closed ? "inactive-row" : null)`.
  - The adresse-cell badge: `if (item.sold) td.appendChild(el("span", "sold-badge", "Solgt"));` →

```js
        if (item.sold) td.appendChild(el("span", "sold-badge", "Solgt"));
        else if (item.closed) td.appendChild(el("span", "inactive-badge", item.tilgjengelighet));
```

  - The sold-visibility gate in `visibleRows`: `if (!state.showSold && item.sold) return false;` → `if (!state.showSold && item.closed) return false;`.

- [ ] **Step 3: table.html.** The toggle label `Vis solgte` → `Vis solgte/inaktive`.

- [ ] **Step 4: Gates**

Run: `node --check skannonser/web/static/popup.js && node --check skannonser/web/static/table.js`
Expected: exit 0 both.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/popup.js skannonser/web/static/table.js skannonser/web/static/table.html
git commit -m "feat(web): derived Solgt/Inaktiv/Trukket badges + muted rows; Finn badge rename"
```

---

### Task 6: Verification + README (controller-led)

**Files:**
- Modify: `README.md` (sold-price enrichment paragraph + web bullet)

- [ ] **Step 1:** `.venv/bin/pytest tests/rebuild -q` → all green.
- [ ] **Step 2:** Controller browser verification (details-populated dev DB + a seeded closed set): four Lag toggles with "Finn.no" rename; Solgt layer shows only derived-Solgt (boligtype colours, budpremie mode intact); Inaktiv/Trukket layer grey; popup badges "Inaktiv"/"Trukket"/"Solgt"/"Finn"/"DNB" correct incl. promoted-inaktiv showing Solgt + price rows; table rows muted with derived badges; Tilgjengelighet filter options show derived labels; deep link to an inaktiv listing flips the new toggle; "Vis solgte/inaktive" shows all closed; zero console errors.
- [ ] **Step 3:** README — in the sold-price enrichment paragraph: targets now cover Solgt + in-grace (≤ `trukket_grace_days`, default 180) Inaktiv listings with strict Solgt-first priority; in the web bullet: derived Solgt/Inaktiv/Trukket statuses (display-only promotion by tinglyst price), the four layer toggles incl. the Finn.no rename. Commit as `docs: closed-status derivation + sweep widening -- README`.

---

## Self-review notes (already applied)

- Spec §1→Task 3 (`_derived_status`, exact table incl. raw-solgt-stays-Solgt and the unparsable-age→Inaktiv safety); §2→Task 2 (WHERE widening, strict tier via leading sort key, `inaktiv_pending`, backlog/CLI plumbing); §3→Task 3 (UPDATED_AT in shared TAIL + sheet-shape guard test, item contract, detail raw-vs-derived); §4→Tasks 4–5 (toggles/rename/paint/badges/labels/gates); §6 tests distributed per task.
- Type consistency: `closed=` keyword across all four `_eie_item` call sites; `grace_days` param name uniform across sold.py functions; feature property names `sold`/`closed` identical in app.js (producer) and map.js (consumer expressions).
- Deliberate simplification kept visible: map.js keeps internal "sold" variant/source/layer NAMES (only their gating switches to `closed`) — renaming the id scheme would churn the cluster-cache and combine-mode logic for zero user-visible gain.
- Tasks 2/3 test code uses the existing seeding helpers by intent (`...` arrange blocks with complete asserts) — same convention as previous plans in this repo; implementers copy the neighbouring tests' concrete helpers.
