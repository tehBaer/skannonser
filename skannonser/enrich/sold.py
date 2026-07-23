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


def select_sold_targets(conn, min_age_days: Optional[int] = None) -> list[dict]:
    """Listings that need a sold price: status Solgt, with coordinates, and no
    non-null ``sold_price`` stored yet. Returns
    ``[{finnkode, lat, lng, attempts}]``, where ``attempts`` is how many times
    the sweep has already spent a request on that target (0 if never).

    The ``sold_price IS NULL`` clause keeps a listing in the target set across
    sweeps until its price is actually tinglyst (~100-day lag). When
    ``min_age_days`` is given, only listings marked Solgt at least that long ago
    are returned (proxy: ``eiendom.updated_at`` -- Solgt rows aren't re-touched
    by the stale-open refresh, so it tracks the sold date). Focusing on aged
    listings avoids spending requests on recent sales that have no price yet."""
    age_clause = ""
    params: tuple = ()
    if min_age_days is not None:
        age_clause = "AND e.updated_at < datetime('now', ?)"
        params = (f"-{int(min_age_days)} days",)
    rows = conn.execute(
        f"""
        SELECT e.finnkode AS finnkode, p.lat AS lat, p.lng AS lng,
               COALESCE(a.attempts, 0) AS attempts
        FROM eiendom e
        JOIN eiendom_processed p ON e.finnkode = p.finnkode
        LEFT JOIN sold_prices s ON e.finnkode = s.finnkode
        LEFT JOIN sold_price_attempts a ON e.finnkode = a.finnkode
        WHERE LOWER(TRIM(COALESCE(e.tilgjengelighet, ''))) = 'solgt'
          AND p.lat IS NOT NULL AND p.lng IS NOT NULL
          AND (s.finnkode IS NULL OR s.sold_price IS NULL)
          {age_clause}
        """,
        params,
    )
    return [
        {
            "finnkode": str(r["finnkode"]),
            "lat": r["lat"],
            "lng": r["lng"],
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

    ``order_by_density`` sorts by FEWEST PRIOR ATTEMPTS first, then by most
    neighbours -- so a tight budget still buys the most matches per request
    (density) but can never be monopolised by targets that keep missing. Without
    the attempt tier, a permanently-ungettable target at the top of the density
    ranking would absorb the budget on every single run forever, since a target
    stays selectable until its price actually lands (see migration 009).

    Every target the sweep centers a box on is charged one attempt via
    :func:`record_attempts`. Lets :class:`Throttled` propagate. Returns
    ``{"targets", "tiles_queried", "cards_seen", "matched", "stored",
    "budget_exhausted"}``.
    """
    from skannonser.store.repositories.sold import SoldPricesRepo

    if targets is None:
        targets = select_sold_targets(conn)
    known = {t["finnkode"] for t in targets}

    order = targets
    if order_by_density:
        order = sorted(
            targets,
            key=lambda t: (
                t.get("attempts", 0),
                -len(_targets_in_bbox(targets, target_bbox(t, pad_lon, pad_lat))),
            ),
        )

    matched: set[str] = set()
    records: list[dict] = []
    attempted: list[str] = []
    tiles_queried = cards_seen = 0
    budget_exhausted = False
    first = True

    def collect(docs):
        for doc in docs:
            rec = parse_sold_card(doc)
            if rec is None or rec["finnkode"] not in known or rec["finnkode"] in matched:
                continue
            matched.add(rec["finnkode"])
            records.append(rec)

    for t in order:
        if t["finnkode"] in matched:
            continue  # already caught by a neighbour's box -- no request needed
        # Attempt the target's box, then once at half size if it came back capped
        # with the target still missing (nearer sales likely hid it).
        for scale in (1.0, 0.5):
            if max_requests is not None and tiles_queried >= max_requests:
                budget_exhausted = True
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
            cards_seen += len(docs)
            collect(docs)

            if t["finnkode"] in matched or len(docs) < _RESULT_CAP:
                break  # found it, or the box wasn't crowded so tightening won't help
        if budget_exhausted:
            break

    if attempted:
        record_attempts(conn, attempted)
    stats = SoldPricesRepo(conn).upsert(records)
    return {
        "targets": len(targets),
        "tiles_queried": tiles_queried,
        "cards_seen": cards_seen,
        "matched": len(matched),
        "stored": stats["inserted"] + stats["updated"],
        "budget_exhausted": budget_exhausted,
    }


def run_sold_backlog(
    conn,
    fetch=browser_get,
    notify: Optional[Callable[[str], None]] = None,
    max_requests: int = 4,
    min_age_days: int = 100,
    coverage_target: float = 0.80,
    delay: Optional[Callable[[], None]] = None,
    force: bool = False,
) -> dict:
    """One careful, budgeted backlog pass -- the scheduled entry point.

    Order of guards:
      1. If suspended (a prior throttle), do nothing.
      2. If aged-listing coverage already >= ``coverage_target`` (and not
         ``force``), do nothing -- we don't chase 100%.
      3. Otherwise sweep the densest cells first, capped at ``max_requests``.

    On :class:`Throttled`, the run suspends the sweep (persisted) and calls
    ``notify`` -- so pushback is recognized immediately and no further requests
    go out until a human resumes. ``notify`` is a ``str -> None`` sink."""
    conn.execute(
        "UPDATE sold_sweep_state SET last_run_at = datetime('now') WHERE id = 1"
    )
    conn.commit()

    if is_suspended(conn):
        return {"suspended": True, "reason": "already suspended", "swept": 0}

    coverage = sold_coverage(conn, min_age_days)
    if not force and coverage["total"] > 0 and coverage["fraction"] >= coverage_target:
        return {"suspended": False, "target_reached": True, "coverage": coverage, "swept": 0}

    targets = select_sold_targets(conn, min_age_days=min_age_days)
    try:
        stats = run_sold_sweep(
            conn,
            fetch=fetch,
            delay=delay,
            targets=targets,
            max_requests=max_requests,
            order_by_density=True,
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
