import tomllib
from pathlib import Path

from pydantic import BaseModel, field_validator, model_validator

DEFAULT_DOMAIN_PATH = Path("config/domain.toml")


class Filters(BaseModel):
    sheets_max_price: int
    url_max_price: int
    min_bra_i: int
    include_unlisted: bool


class CoordBounds(BaseModel):
    lat_min: float
    lat_max: float
    lng_min: float
    lng_max: float


class Travel(BaseModel):
    reuse_within_meters: int
    max_travel_minutes: int


class Budget(BaseModel):
    routes_monthly_cap: int
    geocode_monthly_cap: int
    warn_pcts: list[int]
    routes_rpm: int = 60
    geocode_rpm: int = 60


class Destination(BaseModel):
    key: str
    label: str
    address: str
    df_column: str
    db_column: str
    exclusive: bool = False


class Dnb(BaseModel):
    region_guids: list[str]
    max_pages: int


class Crawl(BaseModel):
    """Inter-request pacing for the FINN crawl/refresh, in seconds.

    Each pair is a (min, max) range a uniform jittered delay is drawn from
    (see `skannonser.http.jittered_delay`). Defaults are deliberately slow --
    the scanner fetches only tens of pages/ads per run, so generous, human-
    shaped gaps cost little wall-clock while keeping the footprint gentle.

    - `page_delay_*`  : between FINN result-page fetches during the crawl.
    - `fetch_delay_*` : before each ad-page network fetch (cache misses only).
    - `listing_delay_*`: between listings in the stale-open status refresh.
    """

    page_delay_min_s: float = 2.0
    page_delay_max_s: float = 8.0
    fetch_delay_min_s: float = 1.0
    fetch_delay_max_s: float = 5.0
    listing_delay_min_s: float = 1.0
    listing_delay_max_s: float = 5.0

    @model_validator(mode="after")
    def _ranges_ordered(self) -> "Crawl":
        for lo, hi, name in (
            (self.page_delay_min_s, self.page_delay_max_s, "page_delay"),
            (self.fetch_delay_min_s, self.fetch_delay_max_s, "fetch_delay"),
            (self.listing_delay_min_s, self.listing_delay_max_s, "listing_delay"),
        ):
            if lo < 0 or hi < lo:
                raise ValueError(f"invalid {name} range: min={lo}, max={hi}")
        return self


class DomainConfig(BaseModel):
    filters: Filters
    coords: CoordBounds
    travel: Travel
    destinations: list[Destination]
    polygon_points: list[tuple[float, float]]  # (lng, lat), legacy order
    budget: Budget
    dnb: Dnb
    crawl: Crawl = Crawl()

    @field_validator("polygon_points")
    @classmethod
    def _polygon_min_size(cls, v: list[tuple[float, float]]) -> list[tuple[float, float]]:
        if len(v) < 3:
            raise ValueError("polygon needs at least 3 points")
        return v

    @model_validator(mode="after")
    def _polygon_within_bounds(self) -> "DomainConfig":
        for lng, lat in self.polygon_points:
            if not (self.coords.lng_min <= lng <= self.coords.lng_max):
                raise ValueError(f"polygon lng {lng} outside coord bounds")
            if not (self.coords.lat_min <= lat <= self.coords.lat_max):
                raise ValueError(f"polygon lat {lat} outside coord bounds")
        return self


def load_domain(path: Path | None = None) -> DomainConfig:
    with open(path or DEFAULT_DOMAIN_PATH, "rb") as f:
        raw = tomllib.load(f)
    raw["polygon_points"] = raw.pop("polygon", {}).get("points", [])
    return DomainConfig(**raw)
