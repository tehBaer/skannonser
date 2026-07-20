from skannonser.ingest.base import NormalizedListing


def test_fields_match_legacy_extractor_keys():
    import ast
    from pathlib import Path

    src = Path("main/extractors/extraction_eiendom.py").read_text()
    # Collect every string key assigned into the result dict of extract_eiendom_data.
    tree = ast.parse(src)
    fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "extract_eiendom_data"
    )
    # Subscript assignments of the form result['Key'] = ... would also define
    # record keys, but this function builds its record as a single dict
    # literal, so subscripts here (e.g. url.split(...)[1]) are unrelated
    # indexing operations, not record keys. We still harvest string-constant
    # subscripts for robustness, but only ast.Dict literal keys are expected
    # to contribute in this function.
    keys = {
        n.slice.value
        for n in ast.walk(fn)
        if isinstance(n, ast.Subscript)
        and isinstance(n.slice, ast.Constant)
        and isinstance(n.slice.value, str)
    }
    dict_keys = {
        k.value
        for n in ast.walk(fn)
        if isinstance(n, ast.Dict)
        for k in n.keys
        if isinstance(k, ast.Constant)
    }
    legacy_keys = keys | dict_keys
    model_fields = set(NormalizedListing.model_fields)
    missing = legacy_keys - model_fields
    assert not missing, f"model missing legacy fields: {missing}"


def test_roundtrip_to_row():
    listing = NormalizedListing(Finnkode="123", URL="https://finn.no/x?finnkode=123")
    row = listing.to_row()
    assert row["Finnkode"] == "123"
