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


class Destination(BaseModel):
    key: str
    label: str
    address: str


class Dnb(BaseModel):
    region_guids: list[str]
    max_pages: int


class DomainConfig(BaseModel):
    filters: Filters
    coords: CoordBounds
    travel: Travel
    destinations: list[Destination]
    polygon_points: list[tuple[float, float]]  # (lng, lat), legacy order
    dnb: Dnb

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
