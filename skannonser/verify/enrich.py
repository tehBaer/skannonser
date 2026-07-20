"""Golden-master harness: compare the rebuilt enrich pipeline (Task 7/8's
``skannonser.enrich.travel``/``skannonser.enrich.donor``) against LEGACY
``main/post_process.py``, over a caller-supplied COPY of the property DB.

Three comparisons, each documented in detail on its own function below:

  1. ``_compare_estimate`` -- per-destination API-attempt estimate parity
     (``estimate()`` vs. legacy's ``_preview_api_calls``/
     ``_simulate_in_run_api_calls`` closures).
  2. ``_compare_donor_prepass`` -- donor pre-pass parity
     (``assign_donors_prepass`` vs. legacy's inline pre-pass,
     ``post_process.py:534-587``).
  3. ``_compare_sheet_values`` -- read-time donor-resolved sheet-value parity
     (``ProcessedRepo.sheet_travel_values`` vs. legacy
     ``PropertyDatabase.get_eiendom_for_sheets()``).

Both legacy comparisons (1 and 2) drive the REAL legacy function
(``main.post_process.post_process_eiendom``) rather than a reimplementation,
because ``_preview_api_calls``/``_simulate_in_run_api_calls`` and the
pre-pass are nested closures with no standalone entry point -- the only way
to exercise the actual legacy code is to call the outer function. Both call
sites are structurally guaranteed to make zero network/API calls; see each
function's docstring for the exact line-level evidence. No API key is ever
read or passed.

Legacy (``main.post_process``, ``main.database.db.PropertyDatabase``) is
imported lazily inside ``verify_enrich`` -- mirrors ``skannonser/verify/parse.py``.
"""
import io
import re
import sys
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from skannonser.config.domain import load_domain
from skannonser.enrich.travel import _prepare, estimate
from skannonser.store import connection as skconn
from skannonser.store.repositories.processed import ProcessedRepo

_REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class EstimateDiff:
    domain_target: str
    destination: str
    field: str
    legacy_value: object
    new_value: object


@dataclass(frozen=True)
class DonorDiff:
    finnkode: str
    legacy_donor: object
    new_donor: object


@dataclass(frozen=True)
class SheetValueDiff:
    finnkode: str
    field: str
    legacy_value: object
    new_value: object


@dataclass
class VerifyEnrichResult:
    estimate_diffs: list = field(default_factory=list)
    donor_diffs: list = field(default_factory=list)
    sheet_value_diffs: list = field(default_factory=list)


def _import_legacy():
    """Import legacy's post-process module + DB accessor, adding the repo
    root to `sys.path` first if needed (installed console script case) --
    same pattern as `verify/parse.py`'s `_import_legacy`."""
    try:
        from main import post_process as legacy_pp
        from main.database.db import PropertyDatabase
    except ModuleNotFoundError:
        root = str(_REPO_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)
        from main import post_process as legacy_pp
        from main.database.db import PropertyDatabase
    return legacy_pp, PropertyDatabase


# ---------------------------------------------------------------------------
# Shared: assemble the pass-2 "live_data"-equivalent frame
# ---------------------------------------------------------------------------


def _build_df_base(conn) -> pd.DataFrame:
    """Build the frame `post_process_eiendom` expects as its `df` argument,
    BEFORE its own DB commute-data merge (`post_process.py:322-351`) fills in
    LAT/LNG/travel columns.

    In real production (`main/runners/run_eiendom_db.py:216-232`) this is
    `live_data`: `pd.read_csv('data/eiendom/A_live.csv')`, the raw output of
    the current FINN scrape -- i.e. exactly the currently-active listings.
    We have no live scrape here (this harness runs against a DB snapshot
    only), so we substitute the DB's own active `eiendom` rows, which is
    what a real `live_data` resolves to for currently-active listings. Only
    the columns legacy actually reads before its merge are included:
    `Finnkode` (everywhere), `Adresse` (unconditionally `.str.title()`'d at
    line 423 -- must be present), `Postnummer`/`Pris` (used for eligible-mask
    price filtering, line 590-591, and API-call addressing, never reached
    here). Deliberately NOT included: LAT/LNG/travel columns/donor link --
    those must come from legacy's own merge (322-351), not from us, so that
    merge code path is genuinely exercised rather than bypassed.
    """
    rows = conn.execute(
        "SELECT finnkode AS Finnkode, adresse AS Adresse, postnummer AS Postnummer, "
        "pris AS Pris FROM eiendom WHERE active = 1 ORDER BY finnkode"
    ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# 1. Estimate parity (post_process.py:637-777)
# ---------------------------------------------------------------------------

_PREVIEW_RE = re.compile(
    r"^\[PREVIEW\] (?P<label>MVV UNI RUSH|BRJ|MVV) "
    r"(?P<kind>max API attempts now|simulated in-run API attempts): (?P<value>\d+)"
)
_LABEL_TO_KEY = {"BRJ": "brj", "MVV": "mvv", "MVV UNI RUSH": "mvv_uni"}
_KIND_TO_FIELD = {
    "max API attempts now": "max_attempts",
    "simulated in-run API attempts": "simulated_attempts",
}
_ZERO_COUNTS = {"max_attempts": 0, "simulated_attempts": 0}


def _parse_preview_output(text: str) -> dict:
    """Parse legacy's `[PREVIEW] ...` print lines (post_process.py:730-775)
    into `{"brj"|"mvv"|"mvv_uni": {"max_attempts": int, "simulated_attempts": int}}`.

    A destination that never gets printed (because legacy's `run_brj`/
    `run_mvv`/`run_mvv_uni` was False for the target, OR because the
    zero-missing shortcut at line 1215-1218 fired) is reported as
    max=simulated=0 -- both are trivially correct in that case: "not run for
    this target" needs no comparison (the caller only compares destinations
    that ARE part of the target), and "zero missing" means the real
    attempts count is genuinely 0.
    """
    result = {key: dict(_ZERO_COUNTS) for key in ("brj", "mvv", "mvv_uni")}
    for line in text.splitlines():
        m = _PREVIEW_RE.match(line)
        if not m:
            continue
        key = _LABEL_TO_KEY[m.group("label")]
        field_name = _KIND_TO_FIELD[m.group("kind")]
        result[key][field_name] = int(m.group("value"))
    return result


def _run_legacy_estimate(legacy_pp, db, df_base: pd.DataFrame, target: str) -> dict:
    """Drive the REAL legacy preview closures for `target` ('all' or
    'mvv_uni') by calling `post_process_eiendom` with
    `calculate_google_directions=True`, and parse their printed output.

    Zero API calls, by construction: `PublicTransitCommuteTime` is only
    imported/constructed inside the `if proceed:` branch
    (post_process.py:781-785), reached only when `confirm_with_rate_limit`
    (line 779) returns `proceed=True`. We force it to return `False` two
    ways, belt-and-braces: (a) patch `builtins.input` to always answer
    "no", which is `confirm_with_rate_limit`'s explicit decline branch
    (line 198-199 -> `(False, 60.0)`); (b) clear `TRAVEL_AUTO_CONFIRM` from
    the environment for the duration of the call, since a `1`/`true` value
    there makes `confirm_with_rate_limit` return `True` WITHOUT ever calling
    `input()` (line 180-190) -- input-mocking alone would not stop that path.
    With `proceed=False`, execution falls into the `else: print("Skipped
    location features calculation")` branch (1212-1213) and returns; the
    `[PREVIEW]` prints we parse (726-775) all happen BEFORE line 779, so
    they are computed and printed unaffected by any of this.
    """
    donor_seed_df = db.get_travel_donor_seed()
    buf = io.StringIO()
    with patch.dict("os.environ", {"TRAVEL_AUTO_CONFIRM": ""}):
        with patch("builtins.input", return_value="no"):
            with redirect_stdout(buf):
                legacy_pp.post_process_eiendom(
                    df_base.copy(deep=True),
                    "verify-enrich",
                    db=db,
                    calculate_google_directions=True,
                    travel_targets=target,
                    donor_seed_df=donor_seed_df,
                    skip_db_merge=False,
                )
    return _parse_preview_output(buf.getvalue())


def _compare_estimate(legacy_pp, PropertyDatabase, db_path: Path, conn) -> list[EstimateDiff]:
    domain = load_domain()
    db = PropertyDatabase(str(db_path))
    df_base = _build_df_base(conn)

    diffs: list[EstimateDiff] = []
    for target, dest_keys in (("all", ("brj", "mvv")), ("mvv_uni", ("mvv_uni",))):
        legacy_result = _run_legacy_estimate(legacy_pp, db, df_base, target)
        new_result = estimate(conn, domain, targets=target)["per_destination"]
        for key in dest_keys:
            legacy_counts = legacy_result.get(key, dict(_ZERO_COUNTS))
            new_counts = new_result.get(key, dict(_ZERO_COUNTS))
            for field_name in ("max_attempts", "simulated_attempts"):
                lv = legacy_counts[field_name]
                nv = new_counts[field_name]
                if lv != nv:
                    diffs.append(EstimateDiff(target, key, field_name, lv, nv))
    return diffs


# ---------------------------------------------------------------------------
# 2. Donor pre-pass parity (post_process.py:534-587)
# ---------------------------------------------------------------------------


def _compare_donor_prepass(legacy_pp, PropertyDatabase, db_path: Path, conn) -> list[DonorDiff]:
    """Legacy's inline donor pre-pass vs. `assign_donors_prepass`, on the
    same active-row set.

    Driven via `post_process_eiendom(calculate_google_directions=False)`:
    reading `post_process.py` top to bottom, the `if not
    calculate_google_directions: ... return df` early return is at line 593,
    STRICTLY AFTER the pre-pass block (534-587) -- there is no other return
    or branch between them. So this call runs the real pre-pass and returns
    right after, before anything API-related is even imported:
    `PublicTransitCommuteTime` (785) and `confirm_with_rate_limit` (779) are
    both further down, past the line-593 return, and are never reached.

    New side: `skannonser.enrich.travel._prepare` -- the same prep step
    `estimate`/`run_enrich` call internally (it builds rows the identical
    way, then calls `assign_donors_prepass`) -- reused here rather than
    re-deriving row-building logic a second time in this harness, so the
    golden master stays pinned to the actual production code path.
    """
    db = PropertyDatabase(str(db_path))
    donor_seed_df = db.get_travel_donor_seed()
    df_base = _build_df_base(conn)

    with patch.dict("os.environ", {"TRAVEL_AUTO_CONFIRM": ""}):
        with redirect_stdout(io.StringIO()):
            legacy_df = legacy_pp.post_process_eiendom(
                df_base.copy(deep=True),
                "verify-enrich",
                db=db,
                calculate_google_directions=False,
                donor_seed_df=donor_seed_df,
                skip_db_merge=False,
            )

    legacy_links: dict[str, object] = {}
    for _, row in legacy_df.iterrows():
        fk = str(row.get("Finnkode", "")).strip()
        if not fk:
            continue
        link = row.get("TRAVEL_COPY_FROM_FINNKODE")
        legacy_links[fk] = None if pd.isna(link) else (str(link).strip() or None)

    domain = load_domain()
    prep = _prepare(conn, domain, domain.destinations)
    new_links = {row["finnkode"]: (row.get("donor_link") or None) for row in prep.rows}

    diffs: list[DonorDiff] = []
    for fk in sorted(set(legacy_links) | set(new_links)):
        lv = legacy_links.get(fk)
        nv = new_links.get(fk)
        if lv != nv:
            diffs.append(DonorDiff(fk, lv, nv))
    return diffs


# ---------------------------------------------------------------------------
# 3. Sheet-value parity (db.py:get_eiendom_for_sheets vs. sheet_travel_values)
# ---------------------------------------------------------------------------


def _compare_sheet_values(PropertyDatabase, db_path: Path, conn) -> list[SheetValueDiff]:
    """For every finnkode legacy's `get_eiendom_for_sheets()` returns (its
    row set, not ours -- it isn't filtered to `active` -- see the module
    query in `db.py:788-880`), compare the three read-time donor-resolved
    travel columns against `ProcessedRepo.sheet_travel_values`.
    """
    db = PropertyDatabase(str(db_path))
    legacy_df = db.get_eiendom_for_sheets()
    repo = ProcessedRepo(conn)

    fields = ("PENDL RUSH BRJ", "PENDL RUSH MVV", "MVV UNI RUSH")
    diffs: list[SheetValueDiff] = []
    for _, row in legacy_df.iterrows():
        fk = str(row.get("Finnkode", "")).strip()
        if not fk:
            continue
        new_values = repo.sheet_travel_values(fk)
        for field_name in fields:
            raw = row.get(field_name)
            lv = None if pd.isna(raw) else int(raw)
            nv = new_values.get(field_name)
            if lv != nv:
                diffs.append(SheetValueDiff(fk, field_name, lv, nv))
    return diffs


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def verify_enrich(db_path: Path) -> VerifyEnrichResult:
    """Run all three golden-master comparisons against `db_path` (a COPY the
    caller made -- this function only ever reads/executes against whatever
    path it is given; making the copy is the caller's responsibility, see
    `skannonser.commands.verify_cmd`). Zero API calls; no key is read.
    """
    legacy_pp, PropertyDatabase = _import_legacy()
    conn = skconn.connect(db_path)
    try:
        return VerifyEnrichResult(
            estimate_diffs=_compare_estimate(legacy_pp, PropertyDatabase, db_path, conn),
            donor_diffs=_compare_donor_prepass(legacy_pp, PropertyDatabase, db_path, conn),
            sheet_value_diffs=_compare_sheet_values(PropertyDatabase, db_path, conn),
        )
    finally:
        conn.close()
