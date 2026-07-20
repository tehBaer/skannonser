from typing import Protocol

from pydantic import BaseModel, ConfigDict, create_model

# Field type shared by every optional legacy column: the legacy extractor's
# values are strings, ints, floats, or missing (None).
_OptionalValue = str | int | float | None

# The legacy dict returned by
# main/extractors/extraction_eiendom.py:extract_eiendom_data (lines 30-49).
# Field names are the *exact* dict keys, Norwegian characters and all — this
# is the source-of-truth contract other code (and the AST-based test) checks
# against. Some keys (spaces, commas, slashes, parentheses) are not valid
# Python identifiers, so the model is assembled with pydantic's
# ``create_model`` rather than a normal class body.
_LEGACY_FIELDS: dict[str, tuple[type, object]] = {
    "Finnkode": (str, ...),
    "URL": (str, ...),
    "Tilgjengelighet": (_OptionalValue, None),
    "Adresse": (_OptionalValue, None),
    "Postnummer": (_OptionalValue, None),
    "Pris": (_OptionalValue, None),
    "IMAGE_URL": (_OptionalValue, None),
    "Primærrom": (_OptionalValue, None),
    "Internt bruksareal (BRA-i)": (_OptionalValue, None),
    "Bruksareal": (_OptionalValue, None),
    "Eksternt bruksareal (BRA-e)": (_OptionalValue, None),
    "Innglasset balkong (BRA-b)": (_OptionalValue, None),
    "Balkong/Terrasse (TBA)": (_OptionalValue, None),
    "Tomteareal": (_OptionalValue, None),
    "Eierskap, tomt": (_OptionalValue, None),
    "Boligtype": (_OptionalValue, None),
    "Bruttoareal": (_OptionalValue, None),
    "Byggeår": (_OptionalValue, None),
}


class _NormalizedListingBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def to_row(self) -> dict:
        """Return the record as a plain dict, keyed exactly like the legacy
        extractor's output (for DataFrame/repository consumption)."""
        return self.model_dump()


NormalizedListing = create_model(
    "NormalizedListing",
    __base__=_NormalizedListingBase,
    __doc__=(
        "A listing record normalized to the legacy extractor's field "
        "contract: field names are EXACTLY the dict keys produced by "
        "main/extractors/extraction_eiendom.py:extract_eiendom_data "
        "(Norwegian names kept verbatim, including case). All fields are "
        "optional except `Finnkode` and `URL`, which the legacy extractor "
        "always populates."
    ),
    **_LEGACY_FIELDS,
)


class Source(Protocol):
    name: str

    def crawl(self) -> list[str]:
        """Return listing URLs to parse."""
        ...

    def parse(self, url: str) -> NormalizedListing | None:
        """Fetch and normalize a single listing URL."""
        ...
