"""Enrich orchestrator: post-process derivations + the ONE parameterized
destination loop that replaces legacy ``post_process``'s three copy-pasted
travel loops (BRJ / MVV / MVV-UNI).

Ports ``main/post_process.py:post_process_eiendom`` (260-1229). The three
near-identical destination loops collapse into ``_run_destination``,
parameterized by a :class:`skannonser.config.domain.Destination`
(``df_column``/``db_column``/``address``/``exclusive``). The pure donor logic
lives in :mod:`skannonser.enrich.donor`; this module wires it to the DB
(``eiendom`` + ``eiendom_processed``) and the Routes API
(:class:`skannonser.enrich.travel_api.TransitCommute`).

Design notes (legacy line refs are to ``post_process.py``):

  * Derivations (397-423): ``compute_pris_kvm`` (area fallback
    primary->usable_i->usable, ``round(price/area)``) and ``title_address``
    (pandas ``.str.title()`` == per-string ``str.title()``) run for EVERY
    active ``eiendom`` row on every target, writing back
    ``eiendom.adresse``/``eiendom.pris_kvm`` via ``ListingsRepo.update_derived``.
  * Cache required-sets (463-466): ``brj``=[BRJ], ``mvv``=[MVV],
    ``mvv_uni``=[MVV-UNI], ``all``=all three (legacy ``transit_travel_columns``).
  * Per-destination assignment cache (RUN): brj->brj cache (815-816); mvv->all
    cache when both brj+mvv run, else mvv cache (987-988); mvv_uni->mvv_uni
    cache (1094-1095). ``add_row_as_donor_if_complete`` runs every row against
    the target cache AND the all cache (938-939/1060-1061/1191-1192).
  * mvv_uni is EXCLUSIVE (293-299): only runs when ``targets=="mvv_uni"``; its
    rows are pre-sorted donors-first (1099-1104, stable, links last) and it
    RESOLVES + WRITES the donor chain value (1146-1154) instead of merely
    skipping like brj/mvv do. Its value lookup treats sentinels as resolvable
    (485) so known failures aren't retried.
  * ``BudgetExceeded`` from ``.minutes()`` propagates BEFORE any write for that
    row; the loop halts and stats carry ``budget_exhausted=True`` (already-
    written rows persist -- every write commits immediately).
"""

import sqlite3
from typing import Optional

import requests

from skannonser.config.domain import Destination, DomainConfig
from skannonser.enrich.donor import (
    _clean,
    _is_valid_travel_value,
    add_row_as_donor_if_complete,
    assign_donors_prepass,
    build_donor_cache,
    maybe_assign_donor,
    resolve_mvv_uni_donor_value,
)
from skannonser.enrich.sentinels import is_travel_sentinel
from skannonser.enrich.travel_api import TransitCommute
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.store.repositories.listings import ListingsRepo
from skannonser.store.repositories.processed import ProcessedRepo

VALID_TARGETS = frozenset({"all", "brj", "mvv", "mvv_uni"})


# ---------------------------------------------------------------------------
# Pure derivations (post_process.py:397-423)
# ---------------------------------------------------------------------------


def _to_number(value) -> Optional[float]:
    """Pandas ``to_numeric(errors='coerce')`` for a scalar DB value.

    Numbers pass through; numeric strings parse; ``None`` / NaN / unparseable
    strings become ``None``.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def compute_pris_kvm(pris, primary_area, usable_i_area, usable_area) -> Optional[int]:
    """Price per square metre from the best available area source.

    Port of ``post_process.py:397-420``. Area fallback chain is
    primary -> usable_i -> usable (legacy ``fillna`` chain, element-wise on
    the numeric-coerced columns). Requires a parseable price AND a parseable
    area > 0; returns ``round(price/area)`` as an ``int`` (round-half-to-even,
    matching numpy/pandas), else ``None``.
    """
    area = _to_number(primary_area)
    if area is None:
        area = _to_number(usable_i_area)
    if area is None:
        area = _to_number(usable_area)

    price = _to_number(pris)
    if price is None or area is None or area <= 0:
        return None
    return int(round(price / area))


def title_address(adresse) -> Optional[str]:
    """Title-case a street address, mirroring pandas ``Series.str.title()``.

    Port of ``post_process.py:423`` (``df['Adresse'] = df['Adresse'].str.title()``).
    pandas ``.str.title()`` applies Python's per-string ``str.title()``, which
    starts a new word after any non-alphabetic character -- so a letter
    directly after a digit is capitalized (``"2a" -> "2A"``). ``None``/NaN
    passes through unchanged (as ``None``).
    """
    if adresse is None:
        return None
    if isinstance(adresse, float) and adresse != adresse:  # NaN
        return None
    return str(adresse).title()


# ---------------------------------------------------------------------------
# Shared setup (rows, caches, mvv_uni lookups, pre-pass) for run + estimate
# ---------------------------------------------------------------------------


class _Prep:
    """Everything the destination loop / estimate needs, built once."""

    __slots__ = ("rows", "caches", "links", "values", "selected", "run_keys", "all_df")

    def __init__(self, rows, caches, links, values, selected, all_df):
        self.rows = rows
        self.caches = caches
        self.links = links
        self.values = values
        self.selected = selected
        self.run_keys = {d.key for d in selected}
        self.all_df = all_df


def _select_destinations(domain: DomainConfig, targets: str) -> list[Destination]:
    """Resolve ``targets`` to the ordered destinations to process.

    ``all`` -> every non-exclusive destination (brj, mvv); a named target ->
    just that one (this is how mvv_uni's exclusivity, post_process.py:293-299,
    is honored: it is exclusive and thus never part of ``all``).
    """
    value = str(targets or "all").strip().lower()
    if value not in VALID_TARGETS:
        raise ValueError(
            f"invalid targets {targets!r}; expected one of {sorted(VALID_TARGETS)}"
        )
    by_key = {d.key: d for d in domain.destinations}
    if value == "all":
        return [d for d in domain.destinations if not d.exclusive]
    return [by_key[value]]


def _seed_to_row(seed: dict, all_df: list[str]) -> dict:
    """Convert a ``ProcessedRepo.donor_seed`` dict to a donor-module row dict."""
    return {
        "finnkode": _clean(seed.get("Finnkode")),
        "lat": _to_number(seed.get("LAT")),
        "lng": _to_number(seed.get("LNG")),
        "values": {df: seed.get(df) for df in all_df},
        "donor_link": seed.get("TRAVEL_COPY_FROM_FINNKODE"),
    }


def _build_rows(conn: sqlite3.Connection, col_map: dict[str, str]) -> list[dict]:
    """Active ``eiendom`` joined to ``eiendom_processed`` as donor-module rows.

    ``col_map`` maps db_column -> df_column for every destination, so
    ``values`` carries all three travel columns keyed the legacy way.
    """
    sql = """
        SELECT e.finnkode, e.adresse, e.postnummer,
               ep.lat AS lat, ep.lng AS lng,
               ep.pendl_rush_brj, ep.pendl_rush_mvv, ep.pendl_rush_mvv_uni_rush,
               ep.pendl_morn_cntr, ep.bil_morn_cntr, ep.pendl_dag_cntr, ep.bil_dag_cntr,
               ep.travel_copy_from_finnkode
        FROM eiendom e
        LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
        WHERE e.active = 1
        ORDER BY e.finnkode
    """
    rows = []
    for r in conn.execute(sql).fetchall():
        rows.append(
            {
                "finnkode": str(r["finnkode"]),
                "adresse": r["adresse"],
                "postnummer": r["postnummer"],
                "lat": _to_number(r["lat"]),
                "lng": _to_number(r["lng"]),
                "values": {col_map[db]: r[db] for db in col_map},
                "donor_link": r["travel_copy_from_finnkode"],
                # Read-and-passthrough for the four CNTR columns: ProcessedRepo
                # .upsert writes these UNCONDITIONALLY (unlike the fill-only
                # travel columns), so every upsert call in this module must
                # carry the existing values back or they get clobbered to NULL.
                "cntr": {
                    "pendl_morn_cntr": r["pendl_morn_cntr"],
                    "bil_morn_cntr": r["bil_morn_cntr"],
                    "pendl_dag_cntr": r["pendl_dag_cntr"],
                    "bil_dag_cntr": r["bil_dag_cntr"],
                },
                # Snapshot the link AS STORED, before the pre-pass mutates it.
                # A link that differs from this at loop time was assigned this
                # run (pre-pass or in-loop) and must be persisted -- legacy
                # relied on a final bulk df write for that; we persist per-row.
                "_stored_link": _clean(r["travel_copy_from_finnkode"]),
            }
        )
    return rows


def _seed_mvv_uni_lookup(source_rows, links, values, uni_df, max_min):
    """Port of ``_seed_mvv_uni_lookup`` (post_process.py:473-486).

    Records every stored donor link, and every MVV-UNI value that is valid OR
    a sentinel (line 485 -- sentinels count so a known failure isn't retried).
    """
    for row in source_rows:
        fk = _clean(row.get("finnkode"))
        if not fk:
            continue
        donor_fk = _clean(row.get("donor_link"))
        if donor_fk:
            links[fk] = donor_fk
        v = row.get("values", {}).get(uni_df)
        if _is_valid_travel_value(v, max_min) or is_travel_sentinel(v):
            values[fk] = int(v)


def _prepare(conn: sqlite3.Connection, domain: DomainConfig, selected: list[Destination]) -> _Prep:
    max_min = float(domain.travel.max_travel_minutes)
    reuse = float(domain.travel.reuse_within_meters)

    by_key = {d.key: d for d in domain.destinations}
    brj_df = by_key["brj"].df_column
    mvv_df = by_key["mvv"].df_column
    uni_df = by_key["mvv_uni"].df_column
    all_df = [brj_df, mvv_df, uni_df]  # legacy transit_travel_columns order
    col_map = {d.db_column: d.df_column for d in domain.destinations}

    rows = _build_rows(conn, col_map)

    caches = {
        "brj": build_donor_cache(rows, [brj_df], max_min),
        "mvv": build_donor_cache(rows, [mvv_df], max_min),
        "mvv_uni": build_donor_cache(rows, [uni_df], max_min),
        "all": build_donor_cache(rows, all_df, max_min),
    }

    # Merge cross-source donor seed (post_process.py:506-532): append donors
    # not already present per each cache's required-set.
    seed_rows = [_seed_to_row(s, all_df) for s in ProcessedRepo(conn).donor_seed()]
    required = {"brj": [brj_df], "mvv": [mvv_df], "mvv_uni": [uni_df], "all": all_df}
    for key, cache in caches.items():
        existing = {f for _, _, f in cache}
        for item in build_donor_cache(seed_rows, required[key], max_min):
            if item[2] in existing:
                continue
            cache.append(item)
            existing.add(item[2])

    # mvv_uni value/link lookups: seed first, then active rows (488-489 order).
    links: dict[str, str] = {}
    values: dict[str, int] = {}
    _seed_mvv_uni_lookup(seed_rows, links, values, uni_df, max_min)
    _seed_mvv_uni_lookup(rows, links, values, uni_df, max_min)

    # Pre-pass donor assignment (534-587): mutates rows + evicts acceptors.
    assign_donors_prepass(rows, caches, reuse)

    return _Prep(rows, caches, links, values, selected, all_df)


# ---------------------------------------------------------------------------
# run_enrich
# ---------------------------------------------------------------------------


def _assignment_cache(dest: Destination, prep: _Prep):
    """RUN-time donor cache for a destination (post_process.py:815/987/1094)."""
    if dest.key == "mvv" and "brj" in prep.run_keys and "mvv" in prep.run_keys:
        return prep.caches["all"]
    return prep.caches[dest.key]


def _apply_api_result(minutes, row, df_col, max_min, stats) -> tuple[Optional[int], bool]:
    """Store a Routes result on the row. Returns (value_written, is_valid).

    Valid value or sentinel is stored (sentinels counted); a rejected/None
    result writes nothing (post_process.py:913-931).
    """
    if minutes is not None and _is_valid_travel_value(minutes, max_min):
        v = int(minutes)
        row["values"][df_col] = v
        return v, True
    if is_travel_sentinel(minutes):
        v = int(minutes)
        row["values"][df_col] = v
        stats["sentinels_written"] += 1
        return v, False
    return None, False


def _run_destination(dest, prep, processed, gateway, api_key, post, force_api, max_min, reuse, stats):
    df_col = dest.df_column
    db_col = dest.db_column
    is_uni = dest.exclusive

    commute = TransitCommute(dest.address, gateway, api_key, post=post, max_minutes=int(max_min))
    assign_cache = _assignment_cache(dest, prep)
    add_caches = {dest.key: prep.caches[dest.key], "all": prep.caches["all"]}
    add_required = {dest.key: [df_col], "all": prep.all_df}

    rows = prep.rows
    if is_uni:
        # Donors-first: rows without a link first (they seed the value lookup),
        # rows with a link last (post_process.py:1099-1104, stable sort).
        rows = sorted(rows, key=lambda r: 1 if _clean(r.get("donor_link")) else 0)

    for row in rows:
        stored_link = row.get("_stored_link", "")
        donor = maybe_assign_donor(row, assign_cache, reuse)
        # "Newly assigned" = differs from the link already in the DB, so a
        # pre-pass-assigned link (absent from the DB) is persisted, while an
        # unchanged pre-existing link is not re-written.
        newly_assigned = bool(donor) and donor != stored_link

        # Candidacy = missing value only. Legacy's transit API is
        # address-based (no coords check) -- coords matter only for donor
        # assignment (maybe_assign_donor / the pre-pass), not here.
        is_candidate = row["values"].get(df_col) is None
        row_changed = False
        value_written: Optional[int] = None

        if is_uni:
            donor_value = (
                resolve_mvv_uni_donor_value(donor, prep.links, prep.values) if donor else None
            )
            can_use = donor_value is not None and not force_api
            # Persist a newly-assigned link regardless of whether the chain
            # value resolves -- legacy's final bulk write persisted the link
            # unconditionally; only the *value* decision depends on can_use.
            if newly_assigned:
                row["donor_link"] = donor
                row_changed = True
            if is_candidate:
                if can_use:
                    value_written = int(donor_value)
                    row["values"][df_col] = value_written
                    stats["mvv_uni_donor_written"] += 1
                    row_changed = True
                else:
                    minutes = commute.minutes(row["adresse"], row["postnummer"])
                    stats["api_calls"] += 1
                    value_written, _valid = _apply_api_result(minutes, row, df_col, max_min, stats)
                    if value_written is not None:
                        row_changed = True
                        prep.values[_clean(row["finnkode"])] = value_written
        else:
            if newly_assigned:
                row["donor_link"] = donor
                row_changed = True
            if is_candidate:
                if donor and not force_api:
                    stats["donor_skipped"] += 1
                else:
                    minutes = commute.minutes(row["adresse"], row["postnummer"])
                    stats["api_calls"] += 1
                    value_written, _valid = _apply_api_result(minutes, row, df_col, max_min, stats)
                    if value_written is not None:
                        row_changed = True

        if row_changed:
            processed.upsert(
                row["finnkode"],
                row["adresse"],
                row["postnummer"],
                travel={db_col: value_written},
                cntr=row["cntr"],
                travel_copy_from_finnkode=(row["donor_link"] or None),
            )

        # Newly-complete rows become donors for later rows in this same run.
        add_row_as_donor_if_complete(row, add_caches, add_required, max_min)


def run_enrich(
    conn: sqlite3.Connection,
    domain: DomainConfig,
    gateway: Gateway,
    api_key: str,
    targets: str = "all",
    post=requests.post,
    force_api: bool = False,
) -> dict:
    """Derive ``pris_kvm``/titled address for every active listing, then fill
    missing travel times for the selected destination(s).

    Returns stats; ``budget_exhausted=True`` means the Routes monthly budget
    ran out mid-loop (rows already written stay, remaining stay NaN for the
    next window). Raises ``ValueError`` for an unknown ``targets``.
    """
    selected = _select_destinations(domain, targets)
    stats = {
        "derived": 0,
        "api_calls": 0,
        "donor_skipped": 0,
        "mvv_uni_donor_written": 0,
        "sentinels_written": 0,
        "budget_exhausted": False,
    }

    # 1. Derivations for ALL active rows (closes STATUS deliverables 1+4).
    listings = ListingsRepo(conn)
    active = conn.execute(
        "SELECT finnkode, adresse, pris, info_primary_area, "
        "info_usable_i_area, info_usable_area FROM eiendom WHERE active = 1"
    ).fetchall()
    for a in active:
        titled = title_address(a["adresse"])
        pris_kvm = compute_pris_kvm(
            a["pris"], a["info_primary_area"], a["info_usable_i_area"], a["info_usable_area"]
        )
        listings.update_derived(a["finnkode"], titled, pris_kvm)
        stats["derived"] += 1

    # 2-4. Build rows/caches/lookups, pre-pass (reads titled adresse above).
    prep = _prepare(conn, domain, selected)
    processed = ProcessedRepo(conn)
    max_min = float(domain.travel.max_travel_minutes)
    reuse = float(domain.travel.reuse_within_meters)

    try:
        for dest in selected:
            _run_destination(
                dest, prep, processed, gateway, api_key, post, force_api, max_min, reuse, stats
            )
    except BudgetExceeded:
        stats["budget_exhausted"] = True

    return stats


# ---------------------------------------------------------------------------
# estimate (post_process.py:637-777) -- NO API calls
# ---------------------------------------------------------------------------


def _estimate_plain(df_col, cache, rows, reuse) -> tuple[int, int]:
    """(max_attempts, simulated_attempts) for a brj/mvv destination.

    ``max`` uses the fixed seed cache (post_process.py:637-670); ``simulated``
    grows a cache copy as each attempt optimistically seeds a donor
    (672-721). Candidacy is ``value is None`` only, matching the legacy
    preview (643) -- no coords check here.
    """
    max_attempts = 0
    for row in rows:
        if row["values"].get(df_col) is not None:
            continue
        if maybe_assign_donor(row, cache, reuse):
            continue
        max_attempts += 1

    sim_cache = list(cache)
    sim_attempts = 0
    for row in rows:
        if row["values"].get(df_col) is not None:
            continue
        if maybe_assign_donor(row, sim_cache, reuse):
            continue
        sim_attempts += 1
        fk = _clean(row.get("finnkode"))
        lat, lng = row.get("lat"), row.get("lng")
        if fk and lat is not None and lng is not None and not any(c[2] == fk for c in sim_cache):
            sim_cache.append((lat, lng, fk))
    return max_attempts, sim_attempts


def _estimate_uni(df_col, cache, rows, links, values, reuse) -> tuple[int, int]:
    """(max, simulated) for mvv_uni: reuse counts only when the donor chain
    value actually resolves (post_process.py:739-767); simulated == max (770-771).
    """
    attempts = 0
    for row in rows:
        if row["values"].get(df_col) is not None:
            continue
        donor = maybe_assign_donor(row, cache, reuse)
        donor_val = resolve_mvv_uni_donor_value(donor, links, values) if donor else None
        if donor_val is not None:
            continue
        attempts += 1
    return attempts, attempts


def estimate(conn: sqlite3.Connection, domain: DomainConfig, targets: str = "all") -> dict:
    """Predict Routes API attempts for a run, without calling any API.

    Per destination: ``max_attempts`` (fixed seed-donor reuse) and
    ``simulated_attempts`` (optimistic in-run donor growth). Uses the
    per-target donor caches (post_process.py:726/733/758), NOT the all-cache.
    """
    selected = _select_destinations(domain, targets)
    prep = _prepare(conn, domain, selected)
    reuse = float(domain.travel.reuse_within_meters)

    per_destination: dict[str, dict] = {}
    total_max = 0
    total_sim = 0
    for dest in selected:
        cache = prep.caches[dest.key]  # per-target cache (legacy preview)
        if dest.exclusive:
            mx, sim = _estimate_uni(
                dest.df_column, cache, prep.rows, prep.links, prep.values, reuse
            )
        else:
            mx, sim = _estimate_plain(dest.df_column, cache, prep.rows, reuse)
        per_destination[dest.key] = {"max_attempts": mx, "simulated_attempts": sim}
        total_max += mx
        total_sim += sim

    return {
        "per_destination": per_destination,
        "totals": {"max_attempts": total_max, "simulated_attempts": total_sim},
    }
