# Tilstand Priority Ordering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Order the tilstand classifier's corpus walk so a `--limit` run spends on active listings that match the commute and size criteria, instead of on whatever was scraped earliest.

**Architecture:** One new method, `TilstandRepo.candidate_finnkodes()`, returns the finnkode list ordered by status tier, then match band, then finnkode. Both walks in `tilstand_backfill.py` call it in place of their raw `SELECT finnkode FROM eiendom`. No migration, no schema change, no new dependency — the ordering is derived at query time from `eiendom` and `eiendom_processed`.

**Tech Stack:** Python 3, SQLite (`sqlite3` stdlib), pytest. Existing repo/enrich layering: SQL lives in `skannonser/store/repositories/`, the `enrich` modules stay SQL-free.

**Spec:** `docs/superpowers/specs/2026-08-06-tilstand-priority-ordering-design.md`

## Global Constraints

- **Work in a worktree.** Per `CLAUDE.md`, enter one and run `./ops/setup-worktree.sh --with-db` first (the `--with-db` flag is needed — Task 1 Step 5 and Task 3 Step 3 read the live DB snapshot, and a worktree has no `main/database/properties.db` without it). Baseline verified 2026-08-06 is **858 passed**; anything less was already broken. `CLAUDE.md` still says 659 — it is stale, trust the number you measure.
- **Run pytest with `PYTHONPATH=.`** — a bare `pytest` in a worktree imports the *main clone's* `skannonser` package, so a green run proves nothing about your changes.
- **No filtering.** Every ad stays eligible. `candidate_finnkodes()` must return all 5863 rows of `eiendom`, only reordered. A row missing its `eiendom_processed` join partner must still appear.
- **Thresholds are constants in this revision:** 80 (m² BRA) and 70 (minutes). Not configurable.
- **Travel sentinels are negative:** `-1` no routes, `-2` unrealistic, `-3` API error (`skannonser/enrich/sentinels.py`). They must never satisfy the `<= 70` test.
- **Do not modify** `skannonser/enrich/tilstand_validate.py` — its walk is the calibration harness and stays as-is.

---

### Task 1: Ordered candidate query on TilstandRepo

**Files:**
- Modify: `skannonser/store/repositories/tilstand.py` (add module-level SQL constants after `_ROLLUP_COLS` at line 21; add method to `TilstandRepo` after `wipe()` at line 76)
- Test: `tests/rebuild/test_tilstand_repo.py` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `TilstandRepo.candidate_finnkodes(self) -> list[str]` — every finnkode in `eiendom`, ordered best-first. Task 2 depends on this exact name and return type.

- [ ] **Step 1: Write the failing tests**

Append to `tests/rebuild/test_tilstand_repo.py`. Note the existing `_db()` helper inserts finnkode `'42'`, so these tests build their own rows with a local helper.

```python
def _rank_db(tmp_path):
    """A DB with one eiendom row per (tier, band) case we care about."""
    conn = connection.connect(tmp_path / "rank.db")
    migrations.migrate(conn)
    return conn


def _ad(conn, finnkode, *, active=1, tilg=None, area=None, brj=None, mvv=None, donor=None):
    conn.execute(
        "INSERT INTO eiendom (finnkode, active, tilgjengelighet, info_usable_area) "
        "VALUES (?, ?, ?, ?)",
        (finnkode, active, tilg, area),
    )
    conn.execute(
        "INSERT INTO eiendom_processed "
        "(finnkode, pendl_rush_brj, pendl_rush_mvv, travel_copy_from_finnkode) "
        "VALUES (?, ?, ?, ?)",
        (finnkode, brj, mvv, donor),
    )
    conn.commit()


def test_candidate_order_puts_active_before_inactive_before_sold(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "sold", active=0, tilg="Solgt", area=100, brj=30, mvv=30)
    _ad(conn, "inactive", active=0, tilg="Inaktiv", area=100, brj=30, mvv=30)
    _ad(conn, "active", active=1, tilg=None, area=100, brj=30, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["active", "inactive", "sold"]


def test_active_flag_loses_to_inaktiv_tilgjengelighet(tmp_path):
    # 77 production rows carry active=1 AND tilgjengelighet='Inaktiv'.
    # publish/rows.py:205 resolves those to NOT active; so must we.
    conn = _rank_db(tmp_path)
    _ad(conn, "conflicted", active=1, tilg="Inaktiv", area=100, brj=30, mvv=30)
    _ad(conn, "clean", active=1, tilg=None, area=100, brj=30, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["clean", "conflicted"]


def test_band_orders_match_then_unknown_then_miss(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "miss_far", area=100, brj=30, mvv=99)
    _ad(conn, "miss_small", area=60, brj=30, mvv=30)
    _ad(conn, "unknown", area=100, brj=None, mvv=30)
    _ad(conn, "match", area=100, brj=30, mvv=30)
    order = TilstandRepo(conn).candidate_finnkodes()
    assert order[0] == "match"
    assert order[1] == "unknown"
    assert set(order[2:]) == {"miss_far", "miss_small"}


def test_both_commutes_must_qualify(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "one_only", area=100, brj=30, mvv=71)
    _ad(conn, "both", area=100, brj=70, mvv=70)
    assert TilstandRepo(conn).candidate_finnkodes() == ["both", "one_only"]


def test_travel_sentinel_is_unknown_not_a_great_commute(tmp_path):
    # -1 is numerically under 70 but means "no routes" (enrich/sentinels.py).
    conn = _rank_db(tmp_path)
    _ad(conn, "sentinel", area=100, brj=-1, mvv=30)
    _ad(conn, "real", area=100, brj=30, mvv=30)
    _ad(conn, "miss", area=100, brj=90, mvv=30)
    assert TilstandRepo(conn).candidate_finnkodes() == ["real", "sentinel", "miss"]


def test_donor_travel_times_decide_the_band(tmp_path):
    conn = _rank_db(tmp_path)
    _ad(conn, "donor", area=100, brj=30, mvv=30)
    _ad(conn, "borrower", area=100, brj=None, mvv=None, donor="donor")
    _ad(conn, "faraway", area=100, brj=90, mvv=90)
    order = TilstandRepo(conn).candidate_finnkodes()
    assert order.index("borrower") < order.index("faraway")


def test_every_ad_is_returned_even_without_a_processed_row(tmp_path):
    conn = _rank_db(tmp_path)
    conn.execute("INSERT INTO eiendom (finnkode, active) VALUES ('orphan', 1)")
    conn.commit()
    _ad(conn, "normal", area=100, brj=30, mvv=30)
    assert set(TilstandRepo(conn).candidate_finnkodes()) == {"orphan", "normal"}


def test_order_is_total_and_stable_on_finnkode(tmp_path):
    conn = _rank_db(tmp_path)
    for finnkode in ("300", "100", "200"):
        _ad(conn, finnkode, area=100, brj=30, mvv=30)
    repo = TilstandRepo(conn)
    assert repo.candidate_finnkodes() == ["100", "200", "300"]
    assert repo.candidate_finnkodes() == repo.candidate_finnkodes()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_tilstand_repo.py -v
```

Expected: the eight new tests FAIL with `AttributeError: 'TilstandRepo' object has no attribute 'candidate_finnkodes'`. The three pre-existing tests still PASS.

- [ ] **Step 3: Write the implementation**

Insert after `_ROLLUP_COLS` (line 21) in `skannonser/store/repositories/tilstand.py`:

```python
# --- Classification priority order (2026-08-06 spec) -----------------------
# `--limit` is the spend control and it cuts the walk wherever it lands, so
# the order decides what a bounded run pays for. Status is the outer key;
# commute+size fit only breaks ties inside a tier.

# Active exactly as publish/rows.py:205 defines it -- 77 production rows have
# active=1 AND tilgjengelighet='Inaktiv', and that rule resolves them to
# inactive. One definition of "active", not two.
_STATUS_TIER = """
    CASE
        WHEN LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'solgt' THEN 2
        WHEN e.active = 1
             AND LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) NOT IN ('solgt', 'inaktiv')
        THEN 0
        ELSE 1
    END
"""

# Donor-resolved travel, mirroring _DONOR_TRAVEL_SQL in publish/rows.py: a
# listing that borrows a donor's times is ranked on the borrowed values, the
# same ones the web UI shows for it.
def _donor_travel(dest: str) -> str:
    return f"""
    CASE
        WHEN ep.travel_copy_from_finnkode IS NOT NULL
             AND TRIM(ep.travel_copy_from_finnkode) != ''
             AND ep_src.pendl_rush_{dest} IS NOT NULL
        THEN ep_src.pendl_rush_{dest}
        ELSE ep.pendl_rush_{dest}
    END
    """


# `areal` is NULL on 5776 of 5863 rows; info_usable_area (BRA) is the real
# source at 5827. The others are fallbacks, not alternatives.
_AREA = "COALESCE(e.info_usable_area, e.info_primary_area, e.areal)"

# BETWEEN 0 AND 70 rather than <= 70: travel sentinels are negative (-1 no
# routes, -2 unrealistic, -3 API error; see enrich/sentinels.py) and every one
# of them would read as an excellent commute under a bare <= test.
_CANDIDATE_SQL = f"""
    SELECT e.finnkode
    FROM eiendom e
    LEFT JOIN eiendom_processed ep ON ep.finnkode = e.finnkode
    LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
    ORDER BY
        {_STATUS_TIER},
        CASE
            WHEN ({_AREA} IS NOT NULL AND {_AREA} < 80)
                 OR ({_donor_travel('brj')}) > 70
                 OR ({_donor_travel('mvv')}) > 70
            THEN 2
            WHEN {_AREA} >= 80
                 AND ({_donor_travel('brj')}) BETWEEN 0 AND 70
                 AND ({_donor_travel('mvv')}) BETWEEN 0 AND 70
            THEN 0
            ELSE 1
        END,
        e.finnkode
"""
```

Then append this method to `TilstandRepo`, after `wipe()`:

```python
    def candidate_finnkodes(self) -> list[str]:
        """Every ad in `eiendom`, ordered for classification: active before
        inactive before sold, and inside each tier the ads matching >= 80 m2
        BRA with both rush commutes <= 70 min first, then the ads we cannot
        rate, then the ones we know miss.

        Nothing is filtered -- a bounded run just spends on the good end.
        """
        return [str(r[0]) for r in self.conn.execute(_CANDIDATE_SQL)]
```

Note on the miss test: `(...) > 70` is NULL when the travel value is NULL, so a
missing commute never counts as a miss — it falls through to band 1. That is
the intended three-way split, not an oversight.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_tilstand_repo.py -v
```

Expected: all 11 tests PASS.

- [ ] **Step 5: Sanity-check the order against the real corpus**

```bash
sqlite3 main/database/properties.db "SELECT COUNT(*) FROM eiendom"
```

Expected: `5863`. Then confirm the query returns every row and starts in the active tier:

```bash
PYTHONPATH=. ./.venv/bin/python -c "
from skannonser.store import connection
from skannonser.store.repositories.tilstand import TilstandRepo
conn = connection.connect('main/database/properties.db')
ks = TilstandRepo(conn).candidate_finnkodes()
print(len(ks), len(set(ks)))
rows = {str(r[0]): (r[1], r[2]) for r in conn.execute(
    'SELECT finnkode, active, tilgjengelighet FROM eiendom')}
print([rows[k] for k in ks[:3]])
"
```

Expected: `5863 5863` (all rows, no duplicates from the double join), and three
rows that are all `(1, None)` — active listings.

- [ ] **Step 6: Commit**

```bash
git add skannonser/store/repositories/tilstand.py tests/rebuild/test_tilstand_repo.py
git commit -m "feat(store): ordered candidate walk for the tilstand classifier

Active before inactive before sold; inside each tier, >=80 m2 BRA with
both rush commutes <=70 min first, then unrated, then known misses.
Sentinel travel values (-1/-2/-3) rank as unrated, not as fast commutes."
```

---

### Task 2: Both classifier walks consume the ordered list

**Files:**
- Modify: `skannonser/enrich/tilstand_backfill.py:37` (sync driver) and `:100` (`_pending_inputs`)
- Test: `tests/rebuild/test_classify_tilstand.py` (append)

**Interfaces:**
- Consumes: `TilstandRepo.candidate_finnkodes() -> list[str]` from Task 1.
- Produces: no new public interface. `classify_tilstand` and `classify_tilstand_batch` keep their current signatures and return-dict keys exactly.

- [ ] **Step 1: Write the failing tests**

Append to `tests/rebuild/test_classify_tilstand.py`. The existing `_env()` helper
inserts bare `eiendom` rows, so add a helper that gives them rankable attributes.

```python
def _rankable(conn, finnkode, *, active=1, tilg=None, area=None, brj=None, mvv=None):
    conn.execute(
        "UPDATE eiendom SET active = ?, tilgjengelighet = ?, info_usable_area = ? "
        "WHERE finnkode = ?",
        (active, tilg, area, finnkode),
    )
    conn.execute(
        "INSERT INTO eiendom_processed (finnkode, pendl_rush_brj, pendl_rush_mvv) "
        "VALUES (?, ?, ?)",
        (finnkode, brj, mvv),
    )
    conn.commit()


def test_limit_spends_on_the_highest_priority_ads(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60, "2": "TG3 b " * 60, "3": "TG3 c " * 60})
    # Insertion order is 1, 2, 3 -- priority order is 3, 2, 1.
    _rankable(conn, "1", active=0, tilg="Solgt", area=100, brj=30, mvv=30)
    _rankable(conn, "2", active=0, tilg="Inaktiv", area=100, brj=30, mvv=30)
    _rankable(conn, "3", active=1, area=100, brj=30, mvv=30)
    result = classify_tilstand(
        conn, tmp_path, limit=2, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT
    )
    assert result["called"] == 2 and result["limit_skipped"] == 1
    classified = {
        str(r[0]) for r in conn.execute("SELECT finnkode FROM listing_tilstand")
    }
    assert classified == {"3", "2"}


def test_batch_pending_follows_the_same_priority_order(tmp_path):
    from skannonser.enrich.tilstand_backfill import _pending_inputs

    conn = _env(tmp_path, {"1": "TG3 a " * 60, "2": "TG3 b " * 60, "3": "TG3 c " * 60})
    _rankable(conn, "1", active=0, tilg="Solgt", area=100, brj=30, mvv=30)
    _rankable(conn, "2", active=1, area=60, brj=30, mvv=30)     # active, too small
    _rankable(conn, "3", active=1, area=100, brj=30, mvv=30)    # active, match
    pending = _pending_inputs(conn, tmp_path, FAKE_INPUT, 2)
    assert len(pending) == 2
    texts = list(pending.values())
    assert texts[0].startswith("TG3 c") and texts[1].startswith("TG3 b")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_classify_tilstand.py -v
```

Expected: both new tests FAIL — `test_limit_spends_on_the_highest_priority_ads`
asserts `classified == {"3", "2"}` but gets `{"1", "2"}` (insertion order), and
the batch test gets `TG3 a` first.

- [ ] **Step 3: Wire the sync driver**

In `skannonser/enrich/tilstand_backfill.py`, replace line 37:

```python
    finnkodes = [str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")]
```

with:

```python
    finnkodes = repo.candidate_finnkodes()
```

`repo` is already constructed on line 33, above the `wipe` branch, so no new
import and no reordering is needed.

- [ ] **Step 4: Wire the batch pending walk**

In the same file, `_pending_inputs` has no `repo`. Replace line 100:

```python
    for (finnkode,) in conn.execute("SELECT finnkode FROM eiendom"):
```

with:

```python
    for finnkode in TilstandRepo(conn).candidate_finnkodes():
```

`TilstandRepo` is already imported at line 18. Also update the function's
docstring to record why the order matters here — the `limit` break makes it
load-bearing:

```python
    """sha -> input text for every ad whose input is not yet cached.
    Dedup by sha is automatic (dict key); `limit` bounds the request count.
    Walks in classification priority order, so a bounded batch buys the
    highest-priority ads -- the same ones the sync driver would pick."""
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
PYTHONPATH=. ./.venv/bin/pytest tests/rebuild/test_classify_tilstand.py -v
```

Expected: all tests PASS, including the pre-existing
`test_limit_bounds_api_calls_not_cache_replays` (its three ads have no
`eiendom_processed` rows and no status, so they all land in the same tier and
band and tie-break on finnkode `1, 2, 3` — the order it already assumed).

- [ ] **Step 6: Run the full suite**

```bash
PYTHONPATH=. ./.venv/bin/pytest
```

Expected: **868 passed** (858 baseline + 8 from Task 1 + 2 from Task 2). Any
failure outside `test_tilstand_repo.py` / `test_classify_tilstand.py` means
something else regressed — investigate before committing.

- [ ] **Step 7: Commit**

```bash
git add skannonser/enrich/tilstand_backfill.py tests/rebuild/test_classify_tilstand.py
git commit -m "feat(enrich): classifier walks in priority order

Both the sync driver and the batch pending walk now consume
TilstandRepo.candidate_finnkodes(), so --limit spends on active,
well-located, large-enough ads instead of on insertion order."
```

---

### Task 3: Document the ordering in the runbook

**Files:**
- Modify: `docs/tilstand-runbook.md`

**Interfaces:**
- Consumes: the behaviour built in Tasks 1-2.
- Produces: nothing code depends on.

- [ ] **Step 1: Read the runbook and find the section describing staged `--limit` runs**

```bash
grep -n "limit" docs/tilstand-runbook.md
```

- [ ] **Step 2: Add a section explaining what `--limit` now buys**

Insert near the staged-run guidance, matching the file's existing heading level
and tone:

```markdown
## What `--limit` buys you

The walk is ordered, so a bounded run is a *prefix of the priority list*, not a
sample. Order is status tier first — active, then inactive, then sold — and
inside each tier: ads with at least 80 m² BRA and both rush commutes (BRJ and
MVV) at 70 minutes or under, then ads we cannot rate because travel or area is
missing, then ads we know miss.

Against the corpus as of 2026-08-06 that means the first 381 classifications go
to active listings matching both criteria, and the 3434 sold ads sort last.

Nothing is excluded — `--all` still covers every ad, and cached responses replay
free in any order. Re-running with a larger `--limit` simply extends the prefix.
```

- [ ] **Step 3: Verify the counts you just wrote are still true**

```bash
PYTHONPATH=. ./.venv/bin/python -c "
from skannonser.store import connection
from skannonser.store.repositories.tilstand import TilstandRepo
conn = connection.connect('main/database/properties.db')
ks = TilstandRepo(conn).candidate_finnkodes()
tier = {str(r[0]): (r[1], r[2]) for r in conn.execute(
    'SELECT finnkode, active, tilgjengelighet FROM eiendom')}
head = [k for k in ks if tier[k][0] == 1 and not tier[k][1]]
print('active total', len(head), 'corpus', len(ks))
"
```

Expected: `active total 1065 corpus 5863`. If these have drifted (the DB is
live and the crawler keeps running), update the numbers in the doc to match
what you observe and note the date you checked.

- [ ] **Step 4: Commit**

```bash
git add docs/tilstand-runbook.md
git commit -m "docs(runbook): what --limit buys under priority ordering"
```

---

## Done when

- `PYTHONPATH=. ./.venv/bin/pytest` reports 868 passed.
- `TilstandRepo.candidate_finnkodes()` returns all 5863 finnkodes with no
  duplicates, starting with active ads.
- Neither `tilstand_backfill.py` nor `tilstand_validate.py` contains a bare
  `SELECT finnkode FROM eiendom` for the classification walk — verify with
  `grep -n "FROM eiendom" skannonser/enrich/tilstand*.py`, which should show
  only the `tilstand_validate.py:94` line (deliberately unchanged).
