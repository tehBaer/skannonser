from skannonser.ingest.base import NormalizedListing

# The complete record-key set the legacy `extract_eiendom_data()` assembled,
# harvested by AST-parsing main/extractors/extraction_eiendom.py and frozen from
# legacy at deletion, 2026-07-22 (legacy source is gone; this is the golden
# contract the NormalizedListing model must keep covering).
LEGACY_EXTRACTOR_KEYS = {
    "Adresse",
    "Balkong/Terrasse (TBA)",
    "Boligtype",
    "Bruksareal",
    "Bruttoareal",
    "Byggeår",
    "Eierskap, tomt",
    "Eksternt bruksareal (BRA-e)",
    "Finnkode",
    "IMAGE_URL",
    "Innglasset balkong (BRA-b)",
    "Internt bruksareal (BRA-i)",
    "Postnummer",
    "Primærrom",
    "Pris",
    "Tilgjengelighet",
    "Tomteareal",
    "URL",
}


def test_fields_match_legacy_extractor_keys():
    model_fields = set(NormalizedListing.model_fields)
    missing = LEGACY_EXTRACTOR_KEYS - model_fields
    assert not missing, f"model missing legacy fields: {missing}"


def test_roundtrip_to_row():
    listing = NormalizedListing(Finnkode="123", URL="https://finn.no/x?finnkode=123")
    row = listing.to_row()
    assert row["Finnkode"] == "123"
