"""FINN sold-price enrichment (DORMANT feature -- not wired into nightly.py).

Fetches the actual tinglyst sale price for listings from the FINN sold map's
undocumented card endpoint and stores it in ``sold_prices``, keyed by finnkode.

Endpoint (captured live 2026-07)::

    GET https://www.finn.no/map/podium-resource/content/api/soldpropertiescard
        ?bbox=<minLon,minLat,maxLon,maxLat>
    -> {"docs": [{"adId": <finnkode>, "cadastralSoldPrice": <kr>,
                  "cadastralSoldDate": <iso>, "soldDate": <iso>,
                  "priceSuggestion": <kr>, "address": <str>, ...}, ...]}

Capped at ~15 nearest cards per bbox. A card only appears once the sale is
tinglyst (~100 days after the bidding round), so a listing that just went
Solgt has no card yet -- it fills in on a later sweep.

**Status / caveat.** This targets a path (`/map/`) that FINN's robots.txt
disallows and whose ToS prohibits automated harvesting. It exists as a free
fallback pending a licensed feed (Ambita omsetningsdata). It is intentionally
NOT called by `nightly.py`; activation is a deliberate, separate step. Run it
via `skannonser enrich-sold --bbox ...` on the polite-access infra
(browser UA + jittered delay), and if FINN ever throttles, stop -- do not
retry harder.
"""

from typing import Callable, Optional

from skannonser.http import browser_get

SOLD_CARD_URL = "https://www.finn.no/map/podium-resource/content/api/soldpropertiescard"

# bbox = (min_lon, min_lat, max_lon, max_lat)
Bbox = tuple[float, float, float, float]

# HTTP statuses we read as "FINN is pushing back" -> suspend, do not retry.
_THROTTLE_STATUSES = frozenset({429, 403, 503})


class Throttled(Exception):
    """Raised when FINN signals rate-limiting/blocking (a throttle status, or
    a 200 whose body isn't our JSON -- e.g. a challenge page). Callers must
    STOP and suspend rather than retry."""


def parse_sold_card(doc: dict) -> Optional[dict]:
    """Normalize one endpoint doc into a ``sold_prices`` record, or ``None``
    if it carries no ``adId`` (nothing to key on)."""
    ad_id = doc.get("adId")
    if ad_id is None:
        return None
    return {
        "finnkode": str(ad_id),
        "sold_price": doc.get("cadastralSoldPrice"),
        "sold_date": doc.get("soldDate"),
        "cadastral_sold_date": doc.get("cadastralSoldDate"),
        "price_suggestion": doc.get("priceSuggestion"),
        "address": doc.get("address"),
        "size": doc.get("size"),
        "property_type": doc.get("propertyType"),
        "bedrooms": doc.get("bedrooms"),
        "collective_debt": doc.get("collectiveDebt"),
        "ownership_type": doc.get("ownershipType"),
    }


def fetch_sold_cards(bbox: Bbox, fetch=browser_get) -> list[dict]:
    """Fetch the sold-property cards inside ``bbox``. Returns the ``docs``
    list, or ``[]`` on a non-200 response or malformed body."""
    param = ",".join(str(v) for v in bbox)
    resp = fetch(SOLD_CARD_URL, params={"bbox": param})
    status = getattr(resp, "status_code", None)
    if status in _THROTTLE_STATUSES:
        raise Throttled(f"FINN returned {status} for bbox {param}")
    if status != 200:
        return []  # a dud tile (404/500 etc.), not a throttle signal
    try:
        payload = resp.json()
    except ValueError as exc:
        # The endpoint always returns JSON; HTML/other means a block or
        # challenge page -- treat as throttling, not an empty tile.
        raise Throttled(f"non-JSON body for bbox {param}") from exc
    docs = payload.get("docs") if isinstance(payload, dict) else None
    return docs or []


def _known_finnkodes(conn) -> set[str]:
    return {
        str(row["finnkode"])
        for row in conn.execute("SELECT finnkode FROM eiendom")
    }


def record_attempts(conn, finnkodes) -> None:
    """Charge one attempt to each of ``finnkodes`` (see migration 009).

    Called once per target the sweep actually centers a box on -- NOT once per
    request (a capped-and-missed box costs two requests for one target) and NOT
    for targets matched incidentally by a neighbour's box, which cost nothing.
    """
    conn.executemany(
        """
        INSERT INTO sold_price_attempts (finnkode, attempts, last_attempted_at)
        VALUES (?, 1, datetime('now'))
        ON CONFLICT(finnkode) DO UPDATE SET
            attempts = attempts + 1,
            last_attempted_at = datetime('now')
        """,
        [(str(fk),) for fk in finnkodes],
    )
    conn.commit()


def select_sold_targets(
    conn,
    min_age_days: Optional[int] = None,
    grace_days: int = 180,
    max_attempts: int = 5,
) -> list[dict]:
    """Listings that need a sold price. Two tiers (2026-07-24 closed-status
    spec):

    - ``status: "solgt"`` -- raw-Solgt listings, unconditionally (as before).
    - ``status: "inaktiv"`` -- Inaktiv listings still within ``grace_days`` of
      closing. A sale that FINN never re-labels Solgt can still turn up
      tinglyst; once a listing ages out of grace it is presumed Trukket and
      drops out of the sweep for good.

    Both tiers require coordinates and no non-null ``sold_price`` stored yet.
    Returns ``[{finnkode, lat, lng, status, attempts}]``, where ``attempts`` is
    how many times the sweep has already spent a request on that target (0 if
    never) and ``status`` records which tier it belongs to (see
    :func:`run_sold_sweep`'s strict Solgt-first ordering).

    The ``sold_price IS NULL`` clause keeps a listing in the target set across
    sweeps until its price is actually tinglyst (~100-day lag). When
    ``min_age_days`` is given, only listings closed at least that long ago are
    returned (proxy: ``eiendom.updated_at`` -- Solgt/Inaktiv rows aren't
    re-touched by the stale-open refresh, so it tracks the close date).
    Focusing on aged listings avoids spending requests on recent closes that
    have no price yet.

    ``max_attempts`` is a per-target attempts ceiling: once a target's
    recorded ``sold_price_attempts.attempts`` reaches it, the target drops out
    of selection for good (see :func:`given_up_targets`). This DELIBERATELY
    REVERSES migration 009's "they are never dropped" stance -- tinglysing lag
    is still real (a miss today may hit later), which is why the default is
    generous, not why targets should cycle forever. Because
    :func:`run_sold_sweep` orders fewest-attempts-first, a target only reaches
    the ceiling after the whole eligible backlog has been combed that many
    times, so the sweep provably goes quiet instead of grinding on
    never-tinglyst listings. Replaces the old 80% coverage gate."""
    age_clause = ""
    params: list = [f"-{int(grace_days)} days"]
    if min_age_days is not None:
        age_clause = "AND e.updated_at < datetime('now', ?)"
        params.append(f"-{int(min_age_days)} days")
    params.append(int(max_attempts))
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
          AND COALESCE(a.attempts, 0) < ?
        """,
        params,
    )
    return [
        {
            "finnkode": str(r["finnkode"]),
            "lat": r["lat"],
            "lng": r["lng"],
            "status": r["status"],
            "attempts": r["attempts"],
        }
        for r in rows
    ]


def sold_coverage(conn, min_age_days: int = 100) -> dict:
    """Coverage of aged sold listings: of the Solgt-with-coords listings marked
    sold at least ``min_age_days`` ago, how many now have a stored price.

    Returns ``{"priced", "total", "fraction"}`` (fraction 0.0 when total 0)."""
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN s.sold_price IS NOT NULL THEN 1 ELSE 0 END) AS priced
        FROM eiendom e
        JOIN eiendom_processed p ON e.finnkode = p.finnkode
        LEFT JOIN sold_prices s ON e.finnkode = s.finnkode
        WHERE LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'solgt'
          AND p.lat IS NOT NULL AND p.lng IS NOT NULL
          AND e.updated_at < datetime('now', ?)
        """,
        (f"-{int(min_age_days)} days",),
    ).fetchone()
    total = row["total"] or 0
    priced = row["priced"] or 0
    return {
        "priced": priced,
        "total": total,
        "fraction": (priced / total) if total else 0.0,
    }


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


def given_up_targets(conn, max_attempts: int = 5) -> int:
    """Targets permanently abandoned by the attempts ceiling: still price-less
    but attempted at least ``max_attempts`` times, so no longer selectable.

    Mirrors :func:`select_sold_targets`'s eligibility (either raw status,
    coordinate-bearing, no stored price) but with the attempts comparison
    flipped -- ``>= max_attempts`` instead of ``< max_attempts`` -- so this and
    the selector partition the aged/coordinated/price-less backlog into
    "still selectable" and "given up"."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM eiendom e
        JOIN eiendom_processed p ON e.finnkode = p.finnkode
        LEFT JOIN sold_prices s ON e.finnkode = s.finnkode
        JOIN sold_price_attempts a ON e.finnkode = a.finnkode
        WHERE LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) IN ('solgt', 'inaktiv')
          AND p.lat IS NOT NULL AND p.lng IS NOT NULL
          AND (s.finnkode IS NULL OR s.sold_price IS NULL)
          AND a.attempts >= ?
        """,
        (int(max_attempts),),
    ).fetchone()
    return row["n"] or 0


def sold_progress(conn, since_hours: int = 24, min_age_days: int = 100) -> dict:
    """Progress snapshot for the daily digest: how many prices landed in the
    last ``since_hours``, whether the sweep is suspended, and overall coverage
    of aged sold listings. Returns ``{"new_priced", "suspended", "coverage"}``."""
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM sold_prices "
        "WHERE sold_price IS NOT NULL AND updated_at >= datetime('now', ?)",
        (f"-{int(since_hours)} hours",),
    ).fetchone()
    return {
        "new_priced": (row["n"] if row else 0) or 0,
        "suspended": is_suspended(conn),
        "coverage": sold_coverage(conn, min_age_days),
    }


def is_suspended(conn) -> bool:
    row = conn.execute(
        "SELECT suspended_at FROM sold_sweep_state WHERE id = 1"
    ).fetchone()
    return bool(row and row["suspended_at"])


def suspend(conn, reason: str) -> None:
    conn.execute(
        "UPDATE sold_sweep_state SET suspended_at = datetime('now'), "
        "suspend_reason = ? WHERE id = 1",
        (reason,),
    )
    conn.commit()


def resume(conn) -> None:
    conn.execute(
        "UPDATE sold_sweep_state SET suspended_at = NULL, suspend_reason = NULL "
        "WHERE id = 1"
    )
    conn.commit()


# Half-size of the per-target query box (degrees). ~0.0008 lon / ~0.0005 lat is
# a ~120 m box at Oslo's latitude. Chosen empirically: the endpoint returns the
# 15 sold cards NEAREST the box, and our geocode differs slightly from FINN's
# card position, so a wide box lets ~15 other sales crowd the target out of the
# cap in dense blocks. A ~120 m box reliably keeps the target in-frame (verified
# against the densest target clusters), at the cost of catching fewer neighbours
# per request. A capped-and-missed box triggers one adaptive shrink (below).
_PAD_LON = 0.0008
_PAD_LAT = 0.0005

# The endpoint's per-bbox result cap. A response at the cap may have hidden the
# target behind nearer sales -> worth one tighter retry; a short response hasn't.
_RESULT_CAP = 15


def target_bbox(t: dict, pad_lon: float = _PAD_LON, pad_lat: float = _PAD_LAT) -> Bbox:
    """A tight bbox centered on one target listing's coordinates."""
    return (
        t["lng"] - pad_lon,
        t["lat"] - pad_lat,
        t["lng"] + pad_lon,
        t["lat"] + pad_lat,
    )


def _targets_in_bbox(targets: list[dict], bbox: Bbox) -> list[dict]:
    lon0, lat0, lon1, lat1 = bbox
    return [
        t for t in targets if lon0 <= t["lng"] <= lon1 and lat0 <= t["lat"] <= lat1
    ]


def run_sold_sweep(
    conn,
    fetch=browser_get,
    delay: Optional[Callable[[], None]] = None,
    targets: Optional[list[dict]] = None,
    max_requests: Optional[int] = None,
    order_by_density: bool = False,
    pad_lon: float = _PAD_LON,
    pad_lat: float = _PAD_LAT,
    inaktiv_reserve: int = 2,
) -> dict:
    """Query a tight box centered on each target listing, storing prices as
    they're found.

    Centering the box on the target guarantees the target is the nearest point
    to the box center, so it survives the endpoint's ~15-card cap even in dense
    areas -- no subdivision needed. A box also catches neighbouring targets, so
    once a listing is matched by an earlier box it is skipped (one request often
    covers a whole cluster).

    ``targets`` defaults to :func:`select_sold_targets`. ``delay`` (if given)
    paces between fetches. ``max_requests`` hard-caps requests this run;
    leftover targets wait for the next run.

    Every target is first split into a strict Solgt-first tier (2026-07-24
    spec): ALL raw-Solgt targets sort ahead of ALL Inaktiv targets, regardless
    of density or attempts -- Solgt listings are far likelier to have a
    tinglyst price, so a tight budget goes to them first. ``order_by_density``
    then sorts WITHIN each tier by FEWEST PRIOR ATTEMPTS first, then by most
    neighbours -- so a tight budget still buys the most matches per request
    (density) but can never be monopolised by targets that keep missing. Without
    the attempt sub-order, a permanently-ungettable target at the top of the
    density ranking would absorb the budget on every single run forever, since a
    target stays selectable until its price actually lands (see migration 009).

    **Inaktiv reserve (2026-07-25 follow-up).** Strict Solgt-first ordering
    above starves the Inaktiv tier completely: measured against the live DB
    (1022 eligible Solgt targets vs 178 Inaktiv), the first Inaktiv target
    sits at position ~1023, so at any realistic budget (4-17 requests/run)
    zero Inaktiv targets are ever reached -- and each Inaktiv listing is only
    eligible for the 80-day window between the 100-day age floor and the
    180-day Trukket grace cutoff, so it ages out having never been checked
    once. ``inaktiv_reserve`` (``V``) fixes this: Solgt may spend at most
    ``solgt_cap = min(max(1, max_requests - V), max_requests)`` requests --
    never fully starved, even at ``max_requests == 1``, while the outer
    ``min`` keeps the floor from exceeding a zero/negative budget (a
    ``--requests 0`` run must make no requests at all) -- and Inaktiv then
    gets everything left of
    the overall ``max_requests``, not just ``V`` (if Solgt finishes under its
    cap because it ran out of targets, Inaktiv gets the whole remainder). If
    there are no eligible Inaktiv targets at all, Solgt gets the full budget
    -- the reserve is never wasted on an empty tier. Hitting the Solgt
    sub-cap stops spending on Solgt WITHOUT ending the sweep or setting
    ``budget_exhausted`` (that stays reserved for the OVERALL ``max_requests``
    being hit) -- it just moves on to Inaktiv targets. A target skipped
    because its tier's budget ran out costs no request and is not charged an
    attempt.

    Every target the sweep actually centers a box on is charged one attempt
    via :func:`record_attempts`. Lets :class:`Throttled` propagate. Returns
    ``{"targets", "tiles_queried", "cards_seen", "matched", "stored",
    "neighbours_stored", "budget_exhausted"}`` -- ``neighbours_stored`` counts
    non-target cards kept from the same responses, anchored via
    ``discovered_near_finnkode`` to the tracked listing whose box surfaced
    them (2026-07-25 spec: zero extra requests, we already paid for the
    response).
    """
    from skannonser.store.repositories.sold import SoldPricesRepo

    if targets is None:
        targets = select_sold_targets(conn)
    known = {t["finnkode"] for t in targets}

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

    solgt_targets = [t for t in order if tier(t) == 0]
    inaktiv_targets = [t for t in order if tier(t) == 1]
    has_inaktiv = bool(inaktiv_targets)

    matched: set[str] = set()
    records: list[dict] = []
    neighbour_records: dict[str, dict] = {}
    attempted: list[str] = []
    tiles_queried = cards_seen = 0
    first = True

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

    def run_phase(phase_targets: list[dict], cap: Optional[int]) -> tuple[int, bool]:
        """Spend up to ``cap`` requests (``None`` = unlimited) on
        ``phase_targets``. Returns ``(requests_used, phase_exhausted)`` --
        ``phase_exhausted`` is True iff the phase stopped because ``cap`` was
        hit while targets in this phase still awaited a box (a target skipped
        this way costs no request and is never appended to ``attempted``)."""
        nonlocal tiles_queried, cards_seen, first
        used = 0
        exhausted = False
        for t in phase_targets:
            if t["finnkode"] in matched:
                continue  # already caught by a neighbour's box -- no request needed
            # Attempt the target's box, then once at half size if it came back
            # capped with the target still missing (nearer sales likely hid it).
            for scale in (1.0, 0.5):
                if cap is not None and used >= cap:
                    exhausted = True
                    break
                if delay is not None and not first:
                    delay()
                first = False

                if scale == 1.0:
                    attempted.append(t["finnkode"])  # one charge per target, not per request
                docs = fetch_sold_cards(
                    target_bbox(t, pad_lon * scale, pad_lat * scale), fetch=fetch
                )
                tiles_queried += 1
                used += 1
                cards_seen += len(docs)
                collect(docs, t["finnkode"])

                if t["finnkode"] in matched or len(docs) < _RESULT_CAP:
                    break  # found it, or the box wasn't crowded so tightening won't help
            if exhausted:
                break
        return used, exhausted

    # Solgt gets the reserved-off cap (or the full budget if Inaktiv is empty
    # -- never waste the reserve on an empty tier); max(1, ...) guarantees
    # Solgt is never fully starved even when max_requests <= inaktiv_reserve,
    # but min(..., max_requests) ensures the sub-cap never exceeds the overall budget.
    if max_requests is None:
        solgt_cap = None
    elif not has_inaktiv:
        solgt_cap = max_requests
    else:
        solgt_cap = min(max(1, max_requests - inaktiv_reserve), max_requests)

    solgt_used, solgt_exhausted = run_phase(solgt_targets, solgt_cap)

    # Inaktiv gets whatever's left of the OVERALL budget -- if Solgt finished
    # under its cap (matched out or ran out of targets), that's everything,
    # not just inaktiv_reserve ("leftover flows downhill").
    inaktiv_cap = None if max_requests is None else max(0, max_requests - solgt_used)
    inaktiv_used, inaktiv_exhausted = run_phase(inaktiv_targets, inaktiv_cap)

    # budget_exhausted means the OVERALL max_requests was hit with work still
    # pending -- NOT the Solgt sub-cap (that's an internal reallocation, not a
    # sweep-ending event). The one case where hitting the Solgt cap IS hitting
    # the overall cap is when there's no Inaktiv tier to reserve against, so
    # solgt_cap == max_requests exactly.
    budget_exhausted = inaktiv_exhausted or (solgt_exhausted and not has_inaktiv)

    if attempted:
        record_attempts(conn, attempted)
    stats = SoldPricesRepo(conn).upsert(records)
    neighbour_stats = SoldPricesRepo(conn).upsert(list(neighbour_records.values()))
    return {
        "targets": len(targets),
        "tiles_queried": tiles_queried,
        "cards_seen": cards_seen,
        "matched": len(matched),
        "stored": stats["inserted"] + stats["updated"],
        "neighbours_stored": neighbour_stats["inserted"] + neighbour_stats["updated"],
        "budget_exhausted": budget_exhausted,
    }


def run_sold_backlog(
    conn,
    fetch=browser_get,
    notify: Optional[Callable[[str], None]] = None,
    max_requests: int = 4,
    min_age_days: int = 100,
    delay: Optional[Callable[[], None]] = None,
    force: bool = False,
    grace_days: int = 180,
    max_attempts: int = 5,
    inaktiv_reserve: int = 2,
) -> dict:
    """One careful, budgeted backlog pass -- the scheduled entry point.

    Order of guards:
      1. If suspended (a prior throttle), do nothing.
      2. Otherwise sweep the densest cells first, capped at ``max_requests``.

    The old guard 2 -- an early return once aged-listing coverage reached 80%
    -- is DELETED (2026-07-24 follow-up spec). It counted raw-'solgt' rows
    only, so it starved the inaktiv tier once solgt coverage crossed 80%, and
    it was never a real termination guarantee: a backlog with a large
    never-tinglyst share (borettslag share sales, fall-throughs, ads marked
    Solgt that never register) could sit below 80% forever, grinding the
    sweep on it indefinitely. A per-target attempts ceiling
    (``max_attempts``, threaded into :func:`select_sold_targets`) replaces it:
    ordering there is fewest-attempts-first, so a target only reaches the
    ceiling once the whole eligible backlog has been combed that many times --
    the sweep provably goes quiet, and it self-scales with backlog size
    instead of needing to track every future tier. ``sold_coverage`` survives
    as a reporting metric (see the returned stats and :func:`sold_progress`);
    only its use as a gate is gone.

    ``force`` no longer bypasses anything (the thing it used to bypass, the
    coverage gate, is deleted) -- kept as a parameter for CLI/caller
    compatibility rather than silently changing the CLI surface.

    ``grace_days`` is passed through to :func:`select_sold_targets` -- it
    bounds how long a closed-but-not-Solgt (Inaktiv) listing stays in the
    sweep's target set (see `load_domain().sold.trukket_grace_days`).
    ``max_attempts`` is likewise passed through -- see
    `load_domain().sold.max_attempts` and :func:`given_up_targets`.
    ``inaktiv_reserve`` is passed through to :func:`run_sold_sweep` -- see its
    docstring and `load_domain().sold.inaktiv_reserve_requests` for why the
    strict Solgt-first ordering above needs a reserved floor for Inaktiv.

    On :class:`Throttled`, the run suspends the sweep (persisted) and calls
    ``notify`` -- so pushback is recognized immediately and no further requests
    go out until a human resumes. ``notify`` is a ``str -> None`` sink."""
    conn.execute(
        "UPDATE sold_sweep_state SET last_run_at = datetime('now') WHERE id = 1"
    )
    conn.commit()

    if is_suspended(conn):
        return {"suspended": True, "reason": "already suspended", "swept": 0}

    targets = select_sold_targets(
        conn, min_age_days=min_age_days, grace_days=grace_days, max_attempts=max_attempts
    )
    try:
        stats = run_sold_sweep(
            conn,
            fetch=fetch,
            delay=delay,
            targets=targets,
            max_requests=max_requests,
            order_by_density=True,
            inaktiv_reserve=inaktiv_reserve,
        )
    except Throttled as exc:
        suspend(conn, str(exc))
        if notify is not None:
            notify(f"FINN sold sweep throttled — suspended. {exc}")
        return {"suspended": True, "throttled": True, "reason": str(exc)}

    return {
        "suspended": False,
        "coverage": sold_coverage(conn, min_age_days),
        **stats,
    }


def run_sold_enrich(
    conn,
    bboxes: list[Bbox],
    fetch=browser_get,
    delay: Optional[Callable[[], None]] = None,
    restrict: bool = True,
) -> dict:
    """Sweep ``bboxes``, storing sold prices for listings we track.

    When ``restrict`` (default), only cards whose finnkode is in ``eiendom``
    are stored -- we keep prices for our own listings, not the whole
    neighbourhood. ``delay`` (if given) paces between tiles.

    Returns ``{"tiles", "cards_seen", "matched", "stored"}``.
    """
    from skannonser.store.repositories.sold import SoldPricesRepo

    known = _known_finnkodes(conn) if restrict else None
    repo = SoldPricesRepo(conn)

    cards_seen = 0
    records: list[dict] = []
    for i, bbox in enumerate(bboxes):
        for doc in fetch_sold_cards(bbox, fetch=fetch):
            cards_seen += 1
            rec = parse_sold_card(doc)
            if rec is None:
                continue
            if known is not None and rec["finnkode"] not in known:
                continue
            records.append(rec)
        if delay is not None and i < len(bboxes) - 1:
            delay()

    stats = repo.upsert(records)
    return {
        "tiles": len(bboxes),
        "cards_seen": cards_seen,
        "matched": len(records),
        "stored": stats["inserted"] + stats["updated"],
    }
