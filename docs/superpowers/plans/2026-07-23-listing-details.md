# Listing Details Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse ~21 additional fields (rooms/eieform/facilities/nabolag, totalpris/fellesgjeld/felleskost money picture, energimerking + matrikkel) from already-cached FINN ad HTML into two new side tables, and surface them as filterable columns in the web app.

**Architecture:** A new parser `parse_details` sits beside the frozen legacy `parse_ad` and feeds a new `ListingDetails` pydantic model into `listing_details` (1:1 on finnkode) + `listing_facilities` (1:N) via a new `DetailsRepo`. Ingest and refresh call it on HTML they already fetch; a new `tools backfill-details` command re-parses the on-disk cache. The web API joins the new table into the existing Eie SELECT fragments and computes two derived fields at query time; the map filter panel and table grow matching controls/columns.

**Tech Stack:** Python 3.11+, pydantic, BeautifulSoup, SQLite, FastAPI, Typer, plain-JS frontend (no build step), pytest.

**Spec:** `docs/superpowers/specs/2026-07-23-listing-details-design.md`

## Global Constraints

- The `NormalizedListing` model in `skannonser/ingest/base.py` is a FROZEN legacy contract (`extra="forbid"`, AST-pinned by test). Never add fields to it.
- `listing_details`/`listing_facilities` are a disposable derived cache: full-row REPLACE semantics, rebuildable via `backfill-details --wipe`. No fill-only/partial-update logic.
- Every parsed field is optional; a per-field parse failure yields `None`, never an exception. `parse_details` must not raise on arbitrary HTML (worst case: an all-NULL row).
- A details failure must never fail a listing upsert or a refresh loop iteration.
- Derived fields (`pris_kvm_totalpris`, `maanedskost`) are computed in the API layer, never stored.
- Zero new FINN traffic: backfill reads `data/eiendom/html_extracted/` only.
- Sheet (Eie/Sold tab) output must be byte-identical before/after: new SQL columns may appear in shared fragments, but `EIE_HEADER`-driven payload assembly in `export.py` must not change.
- Test command: `.venv/bin/pytest tests/rebuild -q` (or `pytest tests/rebuild -q` in an activated venv). Full suite must stay green after every task.
- Frontend is plain JS with no test harness; verify via the dev server + browser tools, then commit.

---

### Task 1: `ListingDetails` model + GAM targeting fields (bedrooms/rooms/floor)

**Files:**
- Create: `skannonser/ingest/finn/parse_details.py`
- Test: `tests/rebuild/test_parse_details.py`

**Interfaces:**
- Consumes: nothing new (BeautifulSoup, pydantic).
- Produces: `ListingDetails` (pydantic model, snake_case; every field below except `finnkode` optional/defaulted) and `parse_details(html: str, finnkode: str) -> ListingDetails`. Internal helpers later tasks extend: `_gam_targeting(soup) -> dict[str, list]`, `_first_int(targeting, key) -> int | None`.

- [ ] **Step 1: Write the failing test**

```python
# tests/rebuild/test_parse_details.py
"""Unit tests for skannonser.ingest.finn.parse_details -- the group-A/B/C
field parser that sits BESIDE the frozen legacy parse_ad (see the
2026-07-23 listing-details design spec)."""
import json

from skannonser.ingest.finn.parse_details import ListingDetails, parse_details


def _gam_html(targeting: list[dict]) -> str:
    state = {"config": {"adServer": {"gam": {"targeting": targeting}}}}
    return (
        "<html><head><script type=\"application/json\" "
        "id=\"advertising-initial-state\">"
        + json.dumps(state)
        + "</script></head><body></body></html>"
    )


def test_gam_int_fields():
    html = _gam_html(
        [
            {"key": "bedrooms", "value": ["2"]},
            {"key": "rooms", "value": ["3"]},
            {"key": "floor", "value": ["5"]},
        ]
    )
    d = parse_details(html, "123")
    assert d.finnkode == "123"
    assert d.bedrooms == 2
    assert d.rooms == 3
    assert d.floor == 5


def test_missing_gam_script_yields_all_none():
    d = parse_details("<html><body><p>hei</p></body></html>", "123")
    assert d.bedrooms is None and d.rooms is None and d.floor is None


def test_malformed_gam_json_yields_none_without_raising():
    html = (
        "<html><script type=\"application/json\" "
        "id=\"advertising-initial-state\">{not json</script></html>"
    )
    d = parse_details(html, "123")
    assert d.bedrooms is None


def test_non_numeric_gam_value_yields_none():
    html = _gam_html([{"key": "bedrooms", "value": ["mange"]}])
    assert parse_details(html, "123").bedrooms is None


def test_garbage_html_never_raises():
    d = parse_details("<<<>>>\x00????", "123")
    assert isinstance(d, ListingDetails)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'skannonser.ingest.finn.parse_details'`

- [ ] **Step 3: Write the implementation**

```python
# skannonser/ingest/finn/parse_details.py
"""FINN ad HTML -> ListingDetails: the group-A/B/C enrichment fields
(2026-07-23 listing-details design spec).

Deliberately SEPARATE from `parse.py`/`NormalizedListing`: that model is a
frozen legacy contract (AST-pinned by test). This module owns everything
new. Every field is optional and every extractor is null-tolerant -- a
parse failure on any field yields None for that field, and `parse_details`
itself never raises on arbitrary HTML (worst case: an all-NULL row).
"""
import json
import re

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field


class ListingDetails(BaseModel):
    finnkode: str
    # Group A -- rooms / location (GAM targeting JSON)
    bedrooms: int | None = None
    rooms: int | None = None
    floor: int | None = None
    eieform: str | None = None
    nabolag: str | None = None
    # Group B -- money (pricing-details <dl>)
    totalpris: int | None = None
    omkostninger: int | None = None
    fellesgjeld: int | None = None
    felleskost_mnd: int | None = None
    fellesformue: int | None = None
    formuesverdi: int | None = None
    kommunale_avg_aar: int | None = None
    # Group C -- condition / identity
    energimerke: str | None = None
    energifarge: str | None = None
    kommunenr: str | None = None
    gardsnr: str | None = None
    bruksnr: str | None = None
    seksjonsnr: str | None = None
    borettslag_navn: str | None = None
    borettslag_orgnr: str | None = None
    borettslag_andelsnr: str | None = None
    facilities: list[str] = Field(default_factory=list)


def _gam_targeting(soup) -> dict[str, list]:
    """The GAM ad-targeting key/value pairs from the
    `advertising-initial-state` JSON blob -- typed data FINN itself ships on
    every ad page. `{}` on any structural surprise."""
    script = soup.find("script", {"id": "advertising-initial-state"})
    if script is None or not script.string:
        return {}
    try:
        data = json.loads(script.string)
        targeting = data["config"]["adServer"]["gam"]["targeting"]
        return {
            t["key"]: t["value"]
            for t in targeting
            if isinstance(t, dict) and "key" in t and isinstance(t.get("value"), list)
        }
    except (json.JSONDecodeError, KeyError, TypeError):
        return {}


def _first_int(targeting: dict, key: str) -> int | None:
    values = targeting.get(key) or []
    try:
        return int(str(values[0]))
    except (IndexError, ValueError, TypeError):
        return None


def parse_details(html: str, finnkode: str) -> ListingDetails:
    soup = BeautifulSoup(html, "html.parser")
    targeting = _gam_targeting(soup)
    return ListingDetails(
        finnkode=finnkode,
        bedrooms=_first_int(targeting, "bedrooms"),
        rooms=_first_int(targeting, "rooms"),
        floor=_first_int(targeting, "floor"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add skannonser/ingest/finn/parse_details.py tests/rebuild/test_parse_details.py
git commit -m "feat(details): ListingDetails model + GAM targeting parser (bedrooms/rooms/floor)"
```

---

### Task 2: Money fields from the `pricing-details` definition list

**Files:**
- Modify: `skannonser/ingest/finn/parse_details.py`
- Test: `tests/rebuild/test_parse_details.py`

**Interfaces:**
- Consumes: Task 1's module.
- Produces: `_parse_kr(text) -> int | None`, `_pricing_details(soup) -> dict` (keys are `ListingDetails` money field names); `parse_details` now fills the seven money fields.

- [ ] **Step 1: Write the failing tests** (append to `tests/rebuild/test_parse_details.py`)

```python
def _pricing_html(pairs: list[tuple[str, str]]) -> str:
    dl = "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in pairs)
    return f'<html><body><div data-testid="pricing-details"><dl>{dl}</dl></div></body></html>'


def test_money_fields_parse():
    html = _pricing_html(
        [
            ("Totalpris", "4\xa0944\xa0646 kr"),
            ("Omkostninger", "9\xa0646 kr"),
            ("Fellesgjeld", "1\xa0945\xa0000 kr"),
            ("Felleskost/mnd.", "13\xa0813 kr"),
            ("Fellesformue", "20\xa0178 kr"),
            ("Formuesverdi", "1\xa0139\xa0380 kr"),
        ]
    )
    d = parse_details(html, "123")
    assert d.totalpris == 4944646
    assert d.omkostninger == 9646
    assert d.fellesgjeld == 1945000
    assert d.felleskost_mnd == 13813
    assert d.fellesformue == 20178
    assert d.formuesverdi == 1139380


def test_kommunale_avg_per_aar_suffix():
    html = _pricing_html([("Kommunale avg.", "15\xa0088 kr per år")])
    assert parse_details(html, "123").kommunale_avg_aar == 15088


def test_zero_kr_parses_as_zero():
    html = _pricing_html([("Fellesgjeld", "0 kr")])
    assert parse_details(html, "123").fellesgjeld == 0


def test_unknown_dt_label_ignored():
    html = _pricing_html([("Prisantydning", "2\xa0990\xa0000 kr")])
    d = parse_details(html, "123")
    assert d.totalpris is None


def test_missing_pricing_section_all_money_none():
    d = parse_details("<html><body></body></html>", "123")
    assert d.totalpris is None and d.felleskost_mnd is None
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 5 failed (the new ones), 5 passed

- [ ] **Step 3: Implement** (add to `parse_details.py`, above `parse_details`)

```python
# dt label -> ListingDetails field, exactly as they appear in the
# pricing-details <dl> (verified against the 12 golden fixtures).
_PRICING_LABELS = {
    "Totalpris": "totalpris",
    "Omkostninger": "omkostninger",
    "Fellesgjeld": "fellesgjeld",
    "Felleskost/mnd.": "felleskost_mnd",
    "Fellesformue": "fellesformue",
    "Formuesverdi": "formuesverdi",
    "Kommunale avg.": "kommunale_avg_aar",
}


def _parse_kr(text: str | None) -> int | None:
    """'1\xa0945\xa0000 kr' -> 1945000. Tolerates a trailing 'per år'
    (kommunale avg.). None when no kr-amount is found."""
    match = re.search(r"([\d\xa0\s]+)\s*kr", text or "")
    if not match:
        return None
    digits = match.group(1).replace("\xa0", "").replace(" ", "")
    try:
        return int(digits)
    except ValueError:
        return None


def _pricing_details(soup) -> dict:
    out: dict = {}
    section = soup.find(attrs={"data-testid": "pricing-details"})
    if section is None:
        return out
    for dt in section.find_all("dt"):
        field = _PRICING_LABELS.get(dt.get_text(strip=True))
        if field is None:
            continue
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        value = _parse_kr(dd.get_text())
        if value is not None:
            out[field] = value
    return out
```

and extend the `parse_details` return:

```python
def parse_details(html: str, finnkode: str) -> ListingDetails:
    soup = BeautifulSoup(html, "html.parser")
    targeting = _gam_targeting(soup)
    return ListingDetails(
        finnkode=finnkode,
        bedrooms=_first_int(targeting, "bedrooms"),
        rooms=_first_int(targeting, "rooms"),
        floor=_first_int(targeting, "floor"),
        **_pricing_details(soup),
    )
```

- [ ] **Step 4: Run tests to verify all pass**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add skannonser/ingest/finn/parse_details.py tests/rebuild/test_parse_details.py
git commit -m "feat(details): money fields from pricing-details dl (totalpris..kommunale avg)"
```

---

### Task 3: Eieform, nabolag, energimerking

**Files:**
- Modify: `skannonser/ingest/finn/parse_details.py`
- Test: `tests/rebuild/test_parse_details.py`

**Interfaces:**
- Consumes: Tasks 1-2.
- Produces: `_eieform(soup, targeting) -> str | None`, `_nabolag(soup) -> str | None`, `_energy(soup) -> tuple[str | None, str | None]`; `parse_details` fills `eieform`, `nabolag`, `energimerke`, `energifarge`.

- [ ] **Step 1: Write the failing tests** (append)

```python
def test_eieform_from_dom_dd():
    html = (
        '<html><body><div data-testid="info-ownership-type">'
        "<dt>Eieform</dt><dd>Andel</dd></div></body></html>"
    )
    assert parse_details(html, "123").eieform == "Andel"


def test_eieform_fallback_maps_gam_enum():
    html = _gam_html([{"key": "ownership_type", "value": ["FREEHOLD"]}])
    assert parse_details(html, "123").eieform == "Eier (selveier)"


def test_eieform_fallback_unknown_enum_kept_raw():
    html = _gam_html([{"key": "ownership_type", "value": ["MYSTERY"]}])
    assert parse_details(html, "123").eieform == "MYSTERY"


def test_nabolag():
    html = '<html><body><span data-testid="local-area-name">Bragernes sentrum</span></body></html>'
    assert parse_details(html, "123").nabolag == "Bragernes sentrum"


def test_energy_splits_letter_and_colour():
    html = (
        '<html><body><div data-testid="energy-label">'
        "Energimerking A - Mørkegrønn</div></body></html>"
    )
    d = parse_details(html, "123")
    assert d.energimerke == "A"
    assert d.energifarge == "Mørkegrønn"


def test_energy_bare_heading_is_none():
    # A real fixture case: the section exists but carries no grade.
    html = '<html><body><div data-testid="energy-label">Energimerking</div></body></html>'
    d = parse_details(html, "123")
    assert d.energimerke is None and d.energifarge is None
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 6 failed, 10 passed

- [ ] **Step 3: Implement** (add helpers, extend `parse_details`)

```python
# GAM ownership_type enum -> Norwegian display value, used only when the
# key-info <dd> is absent. An unknown enum is stored raw rather than lost.
_OWNERSHIP_ENUM = {
    "FREEHOLD": "Eier (selveier)",
    "PART_OWNERSHIP": "Andel",
    "STOCK": "Aksje",
}


def _eieform(soup, targeting: dict) -> str | None:
    element = soup.find(attrs={"data-testid": "info-ownership-type"})
    if element is not None:
        dd = element.find("dd")
        if dd is not None:
            text = dd.get_text(strip=True)
            if text:
                return text
    values = targeting.get("ownership_type") or []
    if values:
        raw = str(values[0])
        return _OWNERSHIP_ENUM.get(raw, raw)
    return None


def _nabolag(soup) -> str | None:
    element = soup.find(attrs={"data-testid": "local-area-name"})
    if element is None:
        return None
    return element.get_text(strip=True) or None


def _energy(soup) -> tuple[str | None, str | None]:
    """'Energimerking A - Mørkegrønn' -> ('A', 'Mørkegrønn'). A bare
    'Energimerking' heading (grade missing on the ad) -> (None, None)."""
    element = soup.find(attrs={"data-testid": "energy-label"})
    if element is None:
        return None, None
    text = element.get_text(" ", strip=True)
    text = re.sub(r"^Energimerking\s*", "", text).strip()
    if not text:
        return None, None
    if " - " in text:
        letter, colour = text.split(" - ", 1)
        return letter.strip() or None, colour.strip() or None
    return text, None
```

In `parse_details`, before the return: `energimerke, energifarge = _energy(soup)`; add to the constructor call:

```python
        eieform=_eieform(soup, targeting),
        nabolag=_nabolag(soup),
        energimerke=energimerke,
        energifarge=energifarge,
```

- [ ] **Step 4: Run tests to verify all pass**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 16 passed

- [ ] **Step 5: Commit**

```bash
git add skannonser/ingest/finn/parse_details.py tests/rebuild/test_parse_details.py
git commit -m "feat(details): eieform (DOM + enum fallback), nabolag, energimerke/-farge"
```

---

### Task 4: Facilities + matrikkel (cadastre)

**Files:**
- Modify: `skannonser/ingest/finn/parse_details.py`
- Test: `tests/rebuild/test_parse_details.py`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: `_facilities(soup) -> list[str]`, `_cadastre(soup) -> dict`; `parse_details` fills `facilities` and the seven matrikkel fields. Parser is now feature-complete.

- [ ] **Step 1: Write the failing tests** (append)

```python
def test_facilities_list():
    html = (
        '<html><body><section data-testid="object-facilities"><h2>Fasiliteter</h2>'
        '<div class="grid">'
        '<div class="py-4 break-words">Heis</div>'
        '<div class="py-4 break-words">Garasje/P-plass</div>'
        '<div class="py-4 break-words">Heis</div>'  # dupe must collapse
        "</div></section></body></html>"
    )
    assert parse_details(html, "123").facilities == ["Heis", "Garasje/P-plass"]


def test_no_facilities_section_empty_list():
    assert parse_details("<html></html>", "123").facilities == []


def test_cadastre_fields():
    html = (
        '<html><body><section data-testid="cadastre-info"><h2>Matrikkel</h2><div>'
        "<div>Kommunenr : 3301</div>"
        "<div>Gårdsnr : 114</div>"
        "<div>Bruksnr : 314</div>"
        "<div>Seksjonsnr : 23</div>"
        "<div>Borettslag-navn : GALLERIET BORETTSLAG</div>"
        "<div>Borettslag-orgnummer : 921554192</div>"
        "<div>Borettslag-andelsnummer : 23</div>"
        "</div></section></body></html>"
    )
    d = parse_details(html, "123")
    assert d.kommunenr == "3301"
    assert d.gardsnr == "114"
    assert d.bruksnr == "314"
    assert d.seksjonsnr == "23"
    assert d.borettslag_navn == "GALLERIET BORETTSLAG"
    assert d.borettslag_orgnr == "921554192"
    assert d.borettslag_andelsnr == "23"


def test_cadastre_container_div_not_misparsed():
    # The wrapper <div> holding all rows must not swallow every value into
    # one label -- only LEAF divs (no div children) are parsed.
    html = (
        '<html><body><section data-testid="cadastre-info"><div>'
        "<div>Kommunenr : 3301</div><div>Gårdssnr-ukjent-label : 999</div>"
        "</div></section></body></html>"
    )
    d = parse_details(html, "123")
    assert d.kommunenr == "3301"
    assert d.gardsnr is None
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 4 failed, 16 passed

- [ ] **Step 3: Implement**

```python
def _facilities(soup) -> list[str]:
    """The Fasiliteter grid: leaf <div>s inside the section, deduped,
    document order preserved (a bounded controlled vocabulary -- 26 distinct
    values across the 12 fixtures)."""
    section = soup.find(attrs={"data-testid": "object-facilities"})
    if section is None:
        return []
    out: list[str] = []
    for div in section.find_all("div"):
        if div.find("div") is not None:  # container, not a facility cell
            continue
        text = div.get_text(strip=True)
        if text and text not in out:
            out.append(text)
    return out


# cadastre-info row label -> ListingDetails field. Values stay TEXT --
# matrikkel numbers are identity keys, not quantities.
_CADASTRE_LABELS = {
    "Kommunenr": "kommunenr",
    "Gårdsnr": "gardsnr",
    "Bruksnr": "bruksnr",
    "Seksjonsnr": "seksjonsnr",
    "Borettslag-navn": "borettslag_navn",
    "Borettslag-orgnummer": "borettslag_orgnr",
    "Borettslag-andelsnummer": "borettslag_andelsnr",
}


def _cadastre(soup) -> dict:
    out: dict = {}
    section = soup.find(attrs={"data-testid": "cadastre-info"})
    if section is None:
        return out
    for div in section.find_all("div"):
        if div.find("div") is not None:  # only leaf rows carry one label:value
            continue
        match = re.match(r"([^:]+?)\s*:\s*(\S.*)$", div.get_text(" ", strip=True))
        if not match:
            continue
        field = _CADASTRE_LABELS.get(match.group(1).strip())
        if field and field not in out:
            out[field] = match.group(2).strip()
    return out
```

Extend the `parse_details` constructor call:

```python
        facilities=_facilities(soup),
        **_cadastre(soup),
```

(Note: `**_pricing_details(soup)` and `**_cadastre(soup)` populate disjoint field-name sets, so double-kwarg collisions are impossible.)

- [ ] **Step 4: Run tests to verify all pass**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 20 passed

- [ ] **Step 5: Commit**

```bash
git add skannonser/ingest/finn/parse_details.py tests/rebuild/test_parse_details.py
git commit -m "feat(details): facilities list + matrikkel/borettslag fields; parser complete"
```

---

### Task 5: Golden fixture expectations for the 12 real ads

**Files:**
- Create: `tests/rebuild/fixtures/finn/<finnkode>.details.expected.json` × 12
- Test: `tests/rebuild/test_parse_details.py` (one parametrized golden test appended)

**Interfaces:**
- Consumes: `parse_details` (complete after Task 4).
- Produces: the pinned golden corpus — the regression net for all later refactors.

- [ ] **Step 1: Write the golden test first** (append to `tests/rebuild/test_parse_details.py`)

```python
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "finn"
DETAIL_CASES = sorted(FIXTURES.glob("*.details.expected.json"))


def test_golden_corpus_exists():
    # 12 ad fixtures ship with the repo; each must have a pinned details file.
    assert len(DETAIL_CASES) == 12


@pytest.mark.parametrize(
    "expected_path", DETAIL_CASES, ids=lambda p: p.stem.split(".")[0]
)
def test_parse_details_matches_fixture(expected_path):
    finnkode = expected_path.stem.split(".")[0]
    html = (FIXTURES / f"{finnkode}.html").read_text(encoding="utf-8", errors="replace")
    expected = json.loads(expected_path.read_text())
    got = parse_details(html, finnkode).model_dump()
    assert got == expected
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py::test_golden_corpus_exists -q`
Expected: FAIL — `assert 0 == 12`

- [ ] **Step 3: Generate the expected files**

```bash
.venv/bin/python - <<'EOF'
import json
from pathlib import Path
from skannonser.ingest.finn.parse_details import parse_details

fixtures = Path("tests/rebuild/fixtures/finn")
for html_path in sorted(fixtures.glob("*.html")):
    if "result_page" in html_path.name:
        continue
    finnkode = html_path.stem
    html = html_path.read_text(encoding="utf-8", errors="replace")
    out = parse_details(html, finnkode).model_dump()
    dest = fixtures / f"{finnkode}.details.expected.json"
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(dest.name)
EOF
```

Expected: 12 filenames printed.

- [ ] **Step 4: Spot-check two generated files against their HTML**

Open `tests/rebuild/fixtures/finn/448347467.details.expected.json` and verify against the known values for that ad: `totalpris` 4944646, `fellesgjeld` 1945000, `felleskost_mnd` 13813, `bedrooms` 2, `rooms` 3, `floor` 3, `eieform` "Andel", `energimerke` "A", `energifarge` "Mørkegrønn", `nabolag` "Bragernes sentrum", `borettslag_navn` "GALLERIET BORETTSLAG", `kommunenr` "3301", facilities includes "Heis" and "Balansert ventilasjon". Then eyeball one more (e.g. `460112195` — its energy label is the bare-heading case, so `energimerke` must be `null`). If anything is wrong, fix the PARSER (Tasks 1-4), regenerate, re-check — never hand-edit an expected file.

- [ ] **Step 5: Run the full parse_details test file**

Run: `.venv/bin/pytest tests/rebuild/test_parse_details.py -q`
Expected: 33 passed (20 unit + 13 golden)

- [ ] **Step 6: Commit**

```bash
git add tests/rebuild/fixtures/finn/*.details.expected.json tests/rebuild/test_parse_details.py
git commit -m "test(details): pin golden details corpus for the 12 fixture ads"
```

---

### Task 6: Migration 010 + `DetailsRepo`

**Files:**
- Create: `skannonser/store/migrations/010_listing_details.sql`
- Create: `skannonser/store/repositories/details.py`
- Test: `tests/rebuild/test_details_repo.py`

**Interfaces:**
- Consumes: `ListingDetails` (Task 1).
- Produces: `DetailsRepo(conn)` with `upsert_details(items: list[ListingDetails]) -> dict` (returns `{"upserted": n}`; writes scalar row + replaces facilities, one transaction), `replace_facilities(finnkode: str, facilities: list[str]) -> None` (no transaction of its own — callers wrap), `wipe() -> None`, `coverage() -> dict` (`{"eiendom_rows", "details_rows", "with_totalpris", "with_felleskost", "facilities_rows"}`).

- [ ] **Step 1: Write the migration**

```sql
-- 010_listing_details.sql
-- Listing-details enrichment (2026-07-23 design spec): group-A/B/C fields
-- parsed from cached FINN ad HTML by skannonser/ingest/finn/parse_details.py.
-- Both tables are a DERIVED, DISPOSABLE cache -- fully rebuildable from
-- data/eiendom/html_extracted/ via `skannonser tools backfill-details --wipe`.
-- Full-row REPLACE semantics, no fill-only columns.
CREATE TABLE IF NOT EXISTS listing_details (
    finnkode TEXT PRIMARY KEY REFERENCES eiendom(finnkode),
    bedrooms INTEGER, rooms INTEGER, floor INTEGER,
    eieform TEXT, nabolag TEXT,
    totalpris INTEGER, omkostninger INTEGER, fellesgjeld INTEGER,
    felleskost_mnd INTEGER, fellesformue INTEGER, formuesverdi INTEGER,
    kommunale_avg_aar INTEGER,
    energimerke TEXT, energifarge TEXT,
    kommunenr TEXT, gardsnr TEXT, bruksnr TEXT, seksjonsnr TEXT,
    borettslag_navn TEXT, borettslag_orgnr TEXT, borettslag_andelsnr TEXT,
    parsed_at TEXT
);
CREATE TABLE IF NOT EXISTS listing_facilities (
    finnkode TEXT NOT NULL REFERENCES eiendom(finnkode),
    facility TEXT NOT NULL,
    UNIQUE (finnkode, facility)
);
```

- [ ] **Step 2: Write the failing repo tests**

```python
# tests/rebuild/test_details_repo.py
"""DetailsRepo: full-row REPLACE semantics for the derived listing_details /
listing_facilities cache (2026-07-23 design spec)."""
import pytest

from skannonser.ingest.finn.parse_details import ListingDetails
from skannonser.store import connection, migrations
from skannonser.store.repositories.details import DetailsRepo


@pytest.fixture()
def repo(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    return DetailsRepo(conn)


def _details(finnkode="111", **kw) -> ListingDetails:
    return ListingDetails(finnkode=finnkode, **kw)


def test_migration_created_tables(repo):
    names = {
        r[0]
        for r in repo.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {"listing_details", "listing_facilities"} <= names


def test_upsert_inserts_scalar_row_and_facilities(repo):
    d = _details(totalpris=4944646, felleskost_mnd=13813, facilities=["Heis", "Peis/Ildsted"])
    assert repo.upsert_details([d]) == {"upserted": 1}
    row = repo.conn.execute(
        "SELECT totalpris, felleskost_mnd, parsed_at FROM listing_details WHERE finnkode='111'"
    ).fetchone()
    assert row["totalpris"] == 4944646
    assert row["parsed_at"]  # stamped
    facs = [
        r["facility"]
        for r in repo.conn.execute(
            "SELECT facility FROM listing_facilities WHERE finnkode='111' ORDER BY facility"
        )
    ]
    assert facs == ["Heis", "Peis/Ildsted"]


def test_upsert_is_full_row_replace(repo):
    repo.upsert_details([_details(totalpris=100, fellesgjeld=50, facilities=["Heis"])])
    # Re-parse now lacks fellesgjeld and has different facilities: the old
    # values must be GONE (derived cache -- no fill-only), not preserved.
    repo.upsert_details([_details(totalpris=200, facilities=["Garasje/P-plass"])])
    row = repo.conn.execute(
        "SELECT totalpris, fellesgjeld FROM listing_details WHERE finnkode='111'"
    ).fetchone()
    assert row["totalpris"] == 200
    assert row["fellesgjeld"] is None
    facs = [
        r["facility"]
        for r in repo.conn.execute(
            "SELECT facility FROM listing_facilities WHERE finnkode='111'"
        )
    ]
    assert facs == ["Garasje/P-plass"]


def test_upsert_empty_list_is_noop(repo):
    assert repo.upsert_details([]) == {"upserted": 0}


def test_wipe(repo):
    repo.upsert_details([_details(facilities=["Heis"])])
    repo.wipe()
    assert repo.conn.execute("SELECT COUNT(*) FROM listing_details").fetchone()[0] == 0
    assert repo.conn.execute("SELECT COUNT(*) FROM listing_facilities").fetchone()[0] == 0


def test_coverage(repo):
    repo.conn.execute(
        "INSERT INTO eiendom (finnkode, url) VALUES ('111', 'u1'), ('222', 'u2')"
    )
    repo.conn.commit()
    repo.upsert_details(
        [_details("111", totalpris=100, felleskost_mnd=10, facilities=["Heis"])]
    )
    cov = repo.coverage()
    assert cov == {
        "eiendom_rows": 2,
        "details_rows": 1,
        "with_totalpris": 1,
        "with_felleskost": 1,
        "facilities_rows": 1,
    }
```

- [ ] **Step 3: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_details_repo.py -q`
Expected: FAIL with `ModuleNotFoundError` (repo module missing)

- [ ] **Step 4: Implement the repo**

```python
# skannonser/store/repositories/details.py
"""``listing_details``/``listing_facilities`` repository (migration 010).

Full-row REPLACE semantics throughout -- these tables are a DERIVED cache of
`parse_details` output over cached ad HTML, never hand-curated data. The
rebuild path for any parser change is `tools backfill-details --wipe`, so
unlike ProcessedRepo/SoldPricesRepo there is deliberately NO fill-only or
partial-update logic here.
"""
import sqlite3

from skannonser.ingest.finn.parse_details import ListingDetails

_SCALAR_COLS = (
    "bedrooms", "rooms", "floor", "eieform", "nabolag",
    "totalpris", "omkostninger", "fellesgjeld", "felleskost_mnd",
    "fellesformue", "formuesverdi", "kommunale_avg_aar",
    "energimerke", "energifarge",
    "kommunenr", "gardsnr", "bruksnr", "seksjonsnr",
    "borettslag_navn", "borettslag_orgnr", "borettslag_andelsnr",
)


class DetailsRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_details(self, items: list[ListingDetails]) -> dict:
        """REPLACE each item's scalar row (parsed_at stamped now) and its
        facilities set, all in one transaction. Returns {"upserted": n}."""
        if not items:
            return {"upserted": 0}
        cols = ("finnkode",) + _SCALAR_COLS + ("parsed_at",)
        placeholders = ", ".join("?" * (len(cols) - 1))
        sql = (
            f"INSERT OR REPLACE INTO listing_details ({', '.join(cols)}) "
            f"VALUES ({placeholders}, datetime('now'))"
        )
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            for item in items:
                data = item.model_dump()
                conn.execute(
                    sql, [item.finnkode] + [data[c] for c in _SCALAR_COLS]
                )
                self.replace_facilities(item.finnkode, item.facilities)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"upserted": len(items)}

    def replace_facilities(self, finnkode: str, facilities: list[str]) -> None:
        """Delete + insert this finnkode's facility rows. No transaction of
        its own -- `upsert_details` (the only production caller) wraps it."""
        self.conn.execute(
            "DELETE FROM listing_facilities WHERE finnkode = ?", (finnkode,)
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO listing_facilities (finnkode, facility) VALUES (?, ?)",
            [(finnkode, f) for f in facilities],
        )

    def wipe(self) -> None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM listing_facilities")
            conn.execute("DELETE FROM listing_details")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def coverage(self) -> dict:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "eiendom_rows": one("SELECT COUNT(*) FROM eiendom"),
            "details_rows": one("SELECT COUNT(*) FROM listing_details"),
            "with_totalpris": one(
                "SELECT COUNT(*) FROM listing_details WHERE totalpris IS NOT NULL"
            ),
            "with_felleskost": one(
                "SELECT COUNT(*) FROM listing_details WHERE felleskost_mnd IS NOT NULL"
            ),
            "facilities_rows": one("SELECT COUNT(*) FROM listing_facilities"),
        }
```

- [ ] **Step 5: Run tests**

Run: `.venv/bin/pytest tests/rebuild/test_details_repo.py -q`
Expected: 6 passed

- [ ] **Step 6: Run the packaging test + full suite** (proves the new `.sql` ships in the wheel and nothing else broke)

Run: `.venv/bin/pytest tests/rebuild -q`
Expected: all pass (557 + new)

- [ ] **Step 7: Commit**

```bash
git add skannonser/store/migrations/010_listing_details.sql skannonser/store/repositories/details.py tests/rebuild/test_details_repo.py
git commit -m "feat(details): migration 010 + DetailsRepo (full-row-replace derived cache)"
```

---

### Task 7: Ingest + refresh integration

**Files:**
- Modify: `skannonser/pipeline.py` (`run_finn_ingest`, lines ~110-137)
- Modify: `skannonser/ingest/finn/refresh.py` (`refresh_listings` loop, lines ~129-152)
- Test: `tests/rebuild/test_pipeline.py`, `tests/rebuild/test_refresh.py` (append)

**Interfaces:**
- Consumes: `parse_details` (Task 4), `DetailsRepo` (Task 6).
- Produces: `run_finn_ingest`'s return dict gains `"details_upserted": int`. `refresh_listings` return dict is unchanged.

- [ ] **Step 1: Write the failing tests.** Append to `tests/rebuild/test_pipeline.py` (reuse that file's existing fixture/fake-fetch helpers for driving `run_finn_ingest` with `skip_crawl_urls` + cached fixture HTML — follow the pattern of the existing offline end-to-end test in that file):

```python
def test_finn_ingest_writes_listing_details(tmp_path, ...):  # match existing fixture args
    # Arrange exactly like the existing offline ingest test (fixture HTML
    # copied under tmp_path/html_extracted, skip_crawl_urls pointing at it),
    # using fixture ad 448347467.
    stats = run_finn_ingest(...)
    assert stats["details_upserted"] == stats["parsed"]
    row = conn.execute(
        "SELECT totalpris, felleskost_mnd FROM listing_details WHERE finnkode = '448347467'"
    ).fetchone()
    assert row["totalpris"] == 4944646
    assert row["felleskost_mnd"] == 13813
    facs = {
        r["facility"]
        for r in conn.execute(
            "SELECT facility FROM listing_facilities WHERE finnkode = '448347467'"
        )
    }
    assert "Heis" in facs


def test_finn_ingest_details_failure_never_fails_listing_upsert(monkeypatch, ...):
    # Force parse_details to blow up; the listing itself must still land.
    from skannonser.ingest.finn import parse_details as pd_mod
    monkeypatch.setattr(
        pd_mod, "parse_details", lambda *a, **k: (_ for _ in ()).throw(RuntimeError)
    )
    stats = run_finn_ingest(...)
    assert stats["parsed"] >= 1        # listing parse unaffected
    assert stats["details_upserted"] == 0
    assert conn.execute("SELECT COUNT(*) FROM eiendom").fetchone()[0] >= 1
```

(The `...` are the existing test file's own driving conventions — copy the arrange block of the neighbouring offline test verbatim. NOTE for the monkeypatch test: `pipeline.py` must import the module and call `parse_details_mod.parse_details(...)` — not `from ... import parse_details` — for monkeypatching to bite; the implementation below does exactly that.)

Append to `tests/rebuild/test_refresh.py`, following its existing fixture pattern:

```python
def test_refresh_reparses_details(...):
    # Existing refresh-test arrange (seed a row, fake fetch returning fixture
    # HTML with a details-bearing page), then:
    refresh_listings(conn, domain, project_dir, mode="all", fetch=fake_fetch)
    row = conn.execute(
        "SELECT totalpris FROM listing_details WHERE finnkode = ?", (fk,)
    ).fetchone()
    assert row is not None and row["totalpris"] is not None
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_pipeline.py tests/rebuild/test_refresh.py -q`
Expected: new tests FAIL (`details_upserted` key missing / no `listing_details` rows)

- [ ] **Step 3: Implement in `pipeline.py`.** Add imports:

```python
from skannonser.ingest.finn import parse_details as finn_parse_details
from skannonser.store.repositories.details import DetailsRepo
```

In `run_finn_ingest`, extend the fetch/parse loop and upsert block:

```python
    crawled = len(pairs)
    parsed = 0
    failed = 0
    listings = []
    details = []
    for finnkode, url in pairs:
        try:
            html = html_cache.load_or_fetch(
                url, project_dir, finnkode, fetch=fetch, fetch_delay=fetch_delay
            )
            listings.append(finn_parse.parse_ad(html, finnkode, url))
            parsed += 1
        except Exception:
            failed += 1
            continue
        # Details are best-effort enrichment: a failure here (parser bug,
        # markup drift) must never fail the listing itself.
        try:
            details.append(finn_parse_details.parse_details(html, finnkode))
        except Exception:
            pass

    repo = ListingsRepo(conn)
    upsert_stats = repo.upsert(listings)

    details_upserted = 0
    try:
        details_upserted = DetailsRepo(conn).upsert_details(details)["upserted"]
    except Exception:
        pass  # derived cache only -- never blocks ingest
```

and add to the return dict: `"details_upserted": details_upserted,`.

- [ ] **Step 4: Implement in `refresh.py`.** Add the same two imports; in `refresh_listings`, add `details_repo = DetailsRepo(conn)` next to `repo = ListingsRepo(conn)`, and inside the loop's `else:` branch (after `repo.update_status(...)`):

```python
            # Re-parse details off the fresh HTML too -- felleskost/totalpris
            # changes ride along with the status refresh for free. Best-effort.
            try:
                details_repo.upsert_details(
                    [finn_parse_details.parse_details(html, finnkode)]
                )
            except Exception:
                pass
```

- [ ] **Step 5: Run the two test files, then the full suite**

Run: `.venv/bin/pytest tests/rebuild/test_pipeline.py tests/rebuild/test_refresh.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add skannonser/pipeline.py skannonser/ingest/finn/refresh.py tests/rebuild/test_pipeline.py tests/rebuild/test_refresh.py
git commit -m "feat(details): capture details during finn ingest + refresh (best-effort)"
```

---

### Task 8: Backfill command — `skannonser tools backfill-details`

**Files:**
- Create: `skannonser/ingest/finn/backfill.py`
- Modify: `skannonser/commands/tools_cmd.py`
- Test: `tests/rebuild/test_backfill_details.py`

**Interfaces:**
- Consumes: `parse_details`, `DetailsRepo`.
- Produces: `backfill_details(conn, project_dir: Path, wipe: bool = False) -> dict` returning `{"eiendom_rows", "parsed", "missing_html", "upserted"}`; CLI `skannonser tools backfill-details [--db PATH] [--project-dir data/eiendom] [--wipe] [--status]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/rebuild/test_backfill_details.py
"""tools backfill-details: local re-parse of cached ad HTML into
listing_details. Purely offline -- the whole point is zero FINN traffic."""
import shutil
from pathlib import Path

import pytest

from skannonser.ingest.finn.backfill import backfill_details
from skannonser.store import connection, migrations

FIXTURES = Path(__file__).parent / "fixtures" / "finn"


@pytest.fixture()
def conn(tmp_path):
    c = connection.connect(tmp_path / "t.db")
    migrations.migrate(c)
    return c


def _seed(conn, finnkode):
    conn.execute(
        "INSERT INTO eiendom (finnkode, url) VALUES (?, ?)", (finnkode, "u")
    )
    conn.commit()


def test_backfill_parses_cached_html(conn, tmp_path):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    shutil.copy(FIXTURES / "448347467.html", project / "html_extracted" / "448347467.html")
    _seed(conn, "448347467")
    _seed(conn, "999999999")  # no cached HTML for this one

    stats = backfill_details(conn, project)
    assert stats == {
        "eiendom_rows": 2,
        "parsed": 1,
        "missing_html": 1,
        "upserted": 1,
    }
    row = conn.execute(
        "SELECT totalpris FROM listing_details WHERE finnkode = '448347467'"
    ).fetchone()
    assert row["totalpris"] == 4944646


def test_backfill_is_idempotent(conn, tmp_path):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    shutil.copy(FIXTURES / "448347467.html", project / "html_extracted" / "448347467.html")
    _seed(conn, "448347467")
    backfill_details(conn, project)
    backfill_details(conn, project)
    assert conn.execute("SELECT COUNT(*) FROM listing_details").fetchone()[0] == 1
    # facilities not duplicated either
    n = conn.execute(
        "SELECT COUNT(*) FROM listing_facilities WHERE finnkode='448347467' AND facility='Heis'"
    ).fetchone()[0]
    assert n == 1


def test_backfill_wipe_rebuilds(conn, tmp_path):
    project = tmp_path / "eiendom"
    (project / "html_extracted").mkdir(parents=True)
    shutil.copy(FIXTURES / "448347467.html", project / "html_extracted" / "448347467.html")
    _seed(conn, "448347467")
    backfill_details(conn, project)
    # Stale row for a finnkode whose HTML no longer exists must vanish on --wipe.
    conn.execute("INSERT INTO listing_details (finnkode) VALUES ('42')")
    conn.commit()
    backfill_details(conn, project, wipe=True)
    finnkodes = {
        r[0] for r in conn.execute("SELECT finnkode FROM listing_details")
    }
    assert finnkodes == {"448347467"}
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/pytest tests/rebuild/test_backfill_details.py -q`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `backfill.py`**

```python
# skannonser/ingest/finn/backfill.py
"""Local re-parse of cached ad HTML into listing_details/listing_facilities.

The recovery/bootstrap path for the details cache (2026-07-23 design spec):
iterate every `eiendom` finnkode, read
`{project_dir}/html_extracted/{finnkode}.html` where present, `parse_details`
it, upsert. Purely offline -- reads only the on-disk cache, never FINN.
"""
import sqlite3
from pathlib import Path

from skannonser.ingest.finn.parse_details import parse_details
from skannonser.store.repositories.details import DetailsRepo

_BATCH_SIZE = 200


def backfill_details(
    conn: sqlite3.Connection, project_dir: Path, wipe: bool = False
) -> dict:
    repo = DetailsRepo(conn)
    if wipe:
        repo.wipe()

    finnkodes = [
        str(r[0]) for r in conn.execute("SELECT finnkode FROM eiendom")
    ]
    parsed = missing = upserted = 0
    batch = []
    for finnkode in finnkodes:
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            missing += 1
            continue
        html = path.read_text(encoding="utf-8", errors="replace")
        batch.append(parse_details(html, finnkode))
        parsed += 1
        if len(batch) >= _BATCH_SIZE:
            upserted += repo.upsert_details(batch)["upserted"]
            batch = []
    if batch:
        upserted += repo.upsert_details(batch)["upserted"]

    return {
        "eiendom_rows": len(finnkodes),
        "parsed": parsed,
        "missing_html": missing,
        "upserted": upserted,
    }
```

- [ ] **Step 4: Wire the CLI command** (append to `skannonser/commands/tools_cmd.py`)

```python
@app.command(name="backfill-details")
def backfill_details_cmd(
    db: Path | None = typer.Option(None, "--db", help="Override the DB path for this run"),
    project_dir: Path = typer.Option(
        Path("data/eiendom"), "--project-dir", help="FINN cache root (html_extracted/ lives here)"
    ),
    wipe: bool = typer.Option(False, "--wipe", help="Clear both details tables first, then rebuild"),
    status: bool = typer.Option(False, "--status", help="Print coverage only; parse nothing"),
) -> None:
    """(Re)build the listing_details/listing_facilities derived cache from
    already-downloaded ad HTML. Purely local -- zero FINN traffic. Safe to
    re-run any time; use --wipe after a parser change."""
    from skannonser.ingest.finn.backfill import backfill_details
    from skannonser.store.repositories.details import DetailsRepo

    db_path = db if db is not None else get_secrets().db_path
    if not db_path.exists():
        typer.echo(f"Error: database not found at {db_path}", err=True)
        raise typer.Exit(code=1)
    conn = connection.connect(db_path)
    if migrations.pending(conn):
        typer.echo("Error: pending migrations - run 'skannonser db migrate' first", err=True)
        raise typer.Exit(code=1)

    if status:
        typer.echo(f"backfill-details coverage: {DetailsRepo(conn).coverage()}")
        return

    result = backfill_details(conn, project_dir, wipe=wipe)
    typer.echo(f"backfill-details: {result}")
```

- [ ] **Step 5: Run tests + full suite**

Run: `.venv/bin/pytest tests/rebuild/test_backfill_details.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add skannonser/ingest/finn/backfill.py skannonser/commands/tools_cmd.py tests/rebuild/test_backfill_details.py
git commit -m "feat(details): tools backfill-details -- offline rebuild from cached HTML"
```

---

### Task 9: Web API — join, item fields, facilities, derived values, meta vocabularies

**Files:**
- Modify: `skannonser/publish/rows.py` (`_EIE_SELECT_TAIL`, `_EIE_JOINS`)
- Modify: `skannonser/web/api.py` (`_eie_item`, `get_listings`, `get_listing_detail`, `get_meta`)
- Test: `tests/rebuild/test_web_api.py`, `tests/rebuild/test_export.py` (append)

**Interfaces:**
- Consumes: migration 010 tables.
- Produces: `/api/listings` eie/sold items gain flat keys `soverom, rom, etasje, eieform, nabolag, energimerke, energifarge, totalpris, omkostninger, fellesgjeld, felleskost_mnd, fellesformue, formuesverdi, kommunale_avg_aar, facilities (list), pris_kvm_totalpris, maanedskost`; `/api/listings/{finnkode}` additionally exposes the raw matrikkel columns (`KOMMUNENR` … `BORETTSLAG_ANDELSNR`, via its existing raw-column spread); `/api/meta` gains `facilities: [{name, count}]`, `energimerker: [str]`, `eieformer: [str]`. Internal: `_facilities_by_finnkode(conn) -> dict[str, list[str]]`, `_pris_kvm_totalpris(rec)`, `_maanedskost(rec)` in `api.py`.

**Key mechanism:** every eie-shaped query (`_EIE_SQL` in rows.py, `_SOLD_API_SQL` and `_eie_full_row` in api.py) is assembled from the shared `_EIE_SELECT_HEAD/_EIE_SELECT_TAIL/_EIE_JOINS` fragments — so extending TAIL + JOINS lands the new columns in all three automatically. The sheet export (`export.eie_rows`) iterates `EIE_HEADER` names over these same records, so extra record keys are invisible to it — but Step 1 pins that with a test anyway.

- [ ] **Step 1: Write the failing tests.** Append to `tests/rebuild/test_web_api.py`, using its existing seed helpers (raw-SQL eiendom/processed inserts) plus new details seeding:

```python
def _seed_details(conn_or_path, finnkode, **cols):
    # INSERT INTO listing_details (finnkode, <cols>) VALUES (...)
    ...


def test_listings_carry_details_fields(client, db_path):
    # seed an active eiendom row (existing helper) + details:
    # totalpris=5000000, felleskost_mnd=4000, kommunale_avg_aar=12000,
    # bedrooms=2, eieform='Andel', energimerke='C', bra_i=100
    # + listing_facilities rows ('Heis', 'Garasje/P-plass')
    item = ...  # the one eie item from GET /api/listings
    assert item["soverom"] == 2
    assert item["eieform"] == "Andel"
    assert item["energimerke"] == "C"
    assert item["totalpris"] == 5000000
    assert item["felleskost_mnd"] == 4000
    assert item["facilities"] == ["Garasje/P-plass", "Heis"]
    assert item["pris_kvm_totalpris"] == 50000       # 5_000_000 / 100
    assert item["maanedskost"] == 5000               # 4000 + 12000/12


def test_details_absent_rows_get_nulls_and_empty_facilities(client, db_path):
    # active eiendom row, NO listing_details row
    item = ...
    assert item["totalpris"] is None
    assert item["facilities"] == []
    assert item["pris_kvm_totalpris"] is None
    assert item["maanedskost"] is None


def test_maanedskost_null_kommunale_avg_contributes_zero(client, db_path):
    # felleskost_mnd=4000, kommunale_avg_aar=NULL -> 4000
    assert item["maanedskost"] == 4000


def test_sold_bucket_carries_details(client, db_path):
    # sold-visibility row (active=0, tilgjengelighet='Solgt') + details row
    # GET /api/listings?bucket=sold -> item["totalpris"] present
    ...


def test_detail_endpoint_exposes_matrikkel(client, db_path):
    # details row with kommunenr='3301', borettslag_navn='X'
    data = client.get("/api/listings/111").json()
    assert data["KOMMUNENR"] == "3301"
    assert data["BORETTSLAG_NAVN"] == "X"


def test_meta_vocabularies(client, db_path):
    # facilities across two listings: Heis (2x), Peis/Ildsted (1x);
    # energimerke 'C' and 'A'; eieform 'Andel'
    meta = client.get("/api/meta").json()
    assert meta["facilities"][0] == {"name": "Heis", "count": 2}
    assert meta["energimerker"] == ["A", "C"]
    assert meta["eieformer"] == ["Andel"]
```

Append to `tests/rebuild/test_export.py`:

```python
def test_eie_sheet_payload_unchanged_by_details(db_path_or_conn):
    # Seed one sheet-visible row; capture eie_rows() payload; insert a full
    # listing_details row for it; capture again. MUST be identical -- the
    # sheet header contract is frozen.
    before = eie_rows(conn)
    _seed_details(conn, finnkode, totalpris=5000000, ...)
    after = eie_rows(conn)
    assert before == after
```

(Write these as real tests following the neighbouring tests' concrete seeding style — the `...` above are the file's own established helpers, not placeholders to leave in.)

- [ ] **Step 2: Run to verify the new tests fail**

Run: `.venv/bin/pytest tests/rebuild/test_web_api.py tests/rebuild/test_export.py -q`
Expected: new tests FAIL (missing keys / missing columns)

- [ ] **Step 3: Extend the SQL fragments in `rows.py`.** Append to `_EIE_SELECT_TAIL` (after the `"SCRAPED_AT"` line, comma-separated):

```python
_EIE_SELECT_TAIL = """
    ep.travel_copy_from_finnkode AS "TRAVEL_COPY_FROM_FINNKODE",
    ep.google_maps_url AS "GOOGLE_MAPS_URL",
    e.scraped_at AS "SCRAPED_AT",
    ld.bedrooms AS "SOVEROM",
    ld.rooms AS "ROM",
    ld.floor AS "ETASJE",
    ld.eieform AS "EIEFORM",
    ld.nabolag AS "NABOLAG",
    ld.totalpris AS "TOTALPRIS",
    ld.omkostninger AS "OMKOSTNINGER",
    ld.fellesgjeld AS "FELLESGJELD",
    ld.felleskost_mnd AS "FELLESKOST_MND",
    ld.fellesformue AS "FELLESFORMUE",
    ld.formuesverdi AS "FORMUESVERDI",
    ld.kommunale_avg_aar AS "KOMMUNALE_AVG_AAR",
    ld.energimerke AS "ENERGIMERKE",
    ld.energifarge AS "ENERGIFARGE",
    ld.kommunenr AS "KOMMUNENR",
    ld.gardsnr AS "GARDSNR",
    ld.bruksnr AS "BRUKSNR",
    ld.seksjonsnr AS "SEKSJONSNR",
    ld.borettslag_navn AS "BORETTSLAG_NAVN",
    ld.borettslag_orgnr AS "BORETTSLAG_ORGNR",
    ld.borettslag_andelsnr AS "BORETTSLAG_ANDELSNR"
"""
```

and extend `_EIE_JOINS`:

```python
_EIE_JOINS = """
    FROM eiendom e
    LEFT JOIN eiendom_processed ep ON e.finnkode = ep.finnkode
    LEFT JOIN eiendom_processed ep_src ON ep_src.finnkode = ep.travel_copy_from_finnkode
    LEFT JOIN listing_details ld ON ld.finnkode = e.finnkode
"""
```

Update the module docstring's field-list note and `listing_rows`'s docstring to mention the `ld` join (one sentence each).

- [ ] **Step 4: Extend `api.py`.** Add near `_sold_records`:

```python
def _facilities_by_finnkode(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """Every listing's facility strings in one query, alphabetical -- grouped
    in Python rather than GROUP_CONCAT to avoid delimiter games."""
    out: dict[str, list[str]] = {}
    for row in conn.execute(
        "SELECT finnkode, facility FROM listing_facilities ORDER BY finnkode, facility"
    ):
        out.setdefault(str(row["finnkode"]), []).append(row["facility"])
    return out


def _pris_kvm_totalpris(rec: dict) -> int | None:
    """totalpris / BRA-i, rounded. Derived at query time, never stored
    (design spec: stored copies go stale silently). None unless both inputs
    are present and positive."""
    try:
        totalpris = float(rec.get("TOTALPRIS"))
        bra_i = float(rec.get("Internt bruksareal (BRA-i)"))
    except (TypeError, ValueError):
        return None
    if totalpris <= 0 or bra_i <= 0:
        return None
    return round(totalpris / bra_i)


def _maanedskost(rec: dict) -> int | None:
    """felleskost/mnd + kommunale avg/12. None when felleskost is unknown;
    a missing kommunale-avg term contributes 0 (spec's NULL rule)."""
    try:
        felleskost = int(rec.get("FELLESKOST_MND"))
    except (TypeError, ValueError):
        return None
    try:
        kommunale_mnd = round(int(rec.get("KOMMUNALE_AVG_AAR")) / 12)
    except (TypeError, ValueError):
        kommunale_mnd = 0
    return felleskost + kommunale_mnd
```

Extend `_eie_item`'s signature with `facilities: list[str] | None = None` (keyword-only, after `thumbs_dir`) and add to the `item` dict (before the `if sold:` block):

```python
        # Listing-details enrichment (migration 010; None/[] when unparsed).
        "soverom": rec.get("SOVEROM"),
        "rom": rec.get("ROM"),
        "etasje": rec.get("ETASJE"),
        "eieform": rec.get("EIEFORM"),
        "nabolag": rec.get("NABOLAG"),
        "energimerke": rec.get("ENERGIMERKE"),
        "energifarge": rec.get("ENERGIFARGE"),
        "totalpris": rec.get("TOTALPRIS"),
        "omkostninger": rec.get("OMKOSTNINGER"),
        "fellesgjeld": rec.get("FELLESGJELD"),
        "felleskost_mnd": rec.get("FELLESKOST_MND"),
        "fellesformue": rec.get("FELLESFORMUE"),
        "formuesverdi": rec.get("FORMUESVERDI"),
        "kommunale_avg_aar": rec.get("KOMMUNALE_AVG_AAR"),
        "facilities": facilities or [],
        "pris_kvm_totalpris": _pris_kvm_totalpris(rec),
        "maanedskost": _maanedskost(rec),
```

In `get_listings`: fetch `facs = _facilities_by_finnkode(conn)` once (both in the `bucket=sold` branch and the main path) and pass `facilities=facs.get(rec.get("_finnkode"))` to every `_eie_item` call. In `get_listing_detail`'s eie branch, fetch that listing's facilities with a targeted query instead of the full map:

```python
    fac_rows = conn.execute(
        "SELECT facility FROM listing_facilities WHERE finnkode = ? ORDER BY facility",
        (finnkode,),
    ).fetchall()
    item = _eie_item(
        rec, domain, sold=_sold_from_hidden(rec), thumbs_dir=thumbs_dir,
        facilities=[r["facility"] for r in fac_rows],
    )
```

(The raw matrikkel columns flow into the detail response automatically via its existing `raw = {k: v for k, v in rec.items() ...}` spread — no further change.)

In `get_meta`, add to the returned dict:

```python
        "facilities": [
            {"name": row["facility"], "count": row["n"]}
            for row in conn.execute(
                "SELECT facility, COUNT(*) AS n FROM listing_facilities "
                "GROUP BY facility ORDER BY n DESC, facility"
            )
        ],
        "energimerker": [
            row["energimerke"]
            for row in conn.execute(
                "SELECT DISTINCT energimerke FROM listing_details "
                "WHERE energimerke IS NOT NULL ORDER BY energimerke"
            )
        ],
        "eieformer": [
            row["eieform"]
            for row in conn.execute(
                "SELECT DISTINCT eieform FROM listing_details "
                "WHERE eieform IS NOT NULL ORDER BY eieform"
            )
        ],
```

- [ ] **Step 5: Run the API/export tests, then the full suite**

Run: `.venv/bin/pytest tests/rebuild/test_web_api.py tests/rebuild/test_export.py -q` then `.venv/bin/pytest tests/rebuild -q`
Expected: all pass — especially `test_eie_sheet_payload_unchanged_by_details`

- [ ] **Step 6: Commit**

```bash
git add skannonser/publish/rows.py skannonser/web/api.py tests/rebuild/test_web_api.py tests/rebuild/test_export.py
git commit -m "feat(web-api): listing details in /api/listings + derived fields + meta vocabularies"
```

---

### Task 10: Map filter panel — new controls + "inkluder ukjent"

**Files:**
- Modify: `skannonser/web/static/filters.js`
- Modify: `skannonser/web/static/app.js` (`loadUi` merge, lines ~96-133)

**Interfaces:**
- Consumes: `/api/meta` vocabularies + item keys from Task 9.
- Produces: `defaultFilterState(meta)` gains `soveromMin: 0`, `totalprisMax: TOTALPRIS_MAX`, `felleskostMax: FELLESKOST_MAX`, `eieform: ""`, `energiHidden: {}`, `facilitiesRequired: {}`, `includeUnknown: true`; `metricDimmed(item, ui, meta)` honours all of them; `buildMetricFilterUI` renders the new controls. New exported consts `TOTALPRIS_MAX = 10_000_000`, `FELLESKOST_MAX = 15000` (slider AT max = off, matching the existing `TRAVEL_MAX` idiom).

- [ ] **Step 1: Extend `defaultFilterState`** in `filters.js`:

```js
// Details-filter ceilings (slider AT the ceiling = filter off, like TRAVEL_MAX).
export const TOTALPRIS_MAX = 10_000_000;
export const FELLESKOST_MAX = 15000;

export function defaultFilterState(meta) {
  const priceBound = Number((meta.filters && meta.filters.sheets_max_price) || 7500000);
  const travelMax = {};
  (meta.destinations || []).forEach((d) => {
    travelMax[d.key] = TRAVEL_MAX;
  });
  return {
    priceMax: priceBound,
    braIMin: 0,
    travelMax,
    // Listing-details filters (2026-07-23). Null/absent values on an item are
    // "unknown": they PASS every active details filter while includeUnknown
    // is on (default), and fail while it's off -- silently hiding sparse
    // older rows would be worse than showing them.
    soveromMin: 0,          // 0 = off
    totalprisMax: TOTALPRIS_MAX,
    felleskostMax: FELLESKOST_MAX,
    eieform: "",           // "" = any
    energiHidden: {},       // letter -> true (hidden), like boligtypeHidden
    facilitiesRequired: {}, // facility -> true (must have; AND semantics)
    includeUnknown: true,
  };
}
```

- [ ] **Step 2: Extend `metricDimmed`** (append before the final `return false;`):

```js
  const unknownDims = !f.includeUnknown; // does an unknown value fail filters?

  // Min soverom.
  if (f.soveromMin > 0) {
    const soverom = num(item.soverom);
    if (soverom == null) {
      if (unknownDims) return true;
    } else if (soverom < f.soveromMin) {
      return true;
    }
  }

  // Max totalpris.
  if (f.totalprisMax < TOTALPRIS_MAX) {
    const totalpris = num(item.totalpris);
    if (totalpris == null) {
      if (unknownDims) return true;
    } else if (totalpris > f.totalprisMax) {
      return true;
    }
  }

  // Max felleskost/mnd.
  if (f.felleskostMax < FELLESKOST_MAX) {
    const felleskost = num(item.felleskost_mnd);
    if (felleskost == null) {
      if (unknownDims) return true;
    } else if (felleskost > f.felleskostMax) {
      return true;
    }
  }

  // Eieform (categorical; "" = any).
  if (f.eieform) {
    const eieform = item.eieform || null;
    if (eieform == null) {
      if (unknownDims) return true;
    } else if (eieform !== f.eieform) {
      return true;
    }
  }

  // Energimerke: hidden letters dim; unknown grade follows includeUnknown
  // once any letter is hidden.
  const energiHidden = f.energiHidden || {};
  if (Object.keys(energiHidden).length) {
    const letter = item.energimerke || null;
    if (letter == null) {
      if (unknownDims) return true;
    } else if (energiHidden[letter]) {
      return true;
    }
  }

  // Required facilities (AND). Items without facility data (DNB, unparsed)
  // are "unknown" as a whole.
  const required = Object.keys(f.facilitiesRequired || {});
  if (required.length) {
    const has = item.facilities;
    if (!Array.isArray(has) || has.length === 0) {
      if (unknownDims) return true;
    } else if (!required.every((r) => has.includes(r))) {
      return true;
    }
  }
```

- [ ] **Step 3: Extend `buildMetricFilterUI`.** After the existing BRA-i row (keeping money/rooms filters grouped before the travel sliders), add:

```js
  rangeRow(container, {
    label: "Maks totalpris",
    min: 0,
    max: TOTALPRIS_MAX,
    step: 100000,
    value: ui.filters.totalprisMax,
    fmt: (v) => (v >= TOTALPRIS_MAX ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.totalprisMax = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Maks felleskost/mnd",
    min: 0,
    max: FELLESKOST_MAX,
    step: 250,
    value: ui.filters.felleskostMax,
    fmt: (v) => (v >= FELLESKOST_MAX ? "Av" : NOK.format(v) + " kr"),
    onInput: (v) => {
      ui.filters.felleskostMax = v;
      onChange();
    },
  });

  rangeRow(container, {
    label: "Min soverom",
    min: 0,
    max: 6,
    step: 1,
    value: ui.filters.soveromMin,
    fmt: (v) => (v <= 0 ? "Av" : "≥ " + v),
    onInput: (v) => {
      ui.filters.soveromMin = v;
      onChange();
    },
  });
```

then, after the existing dim/sold-dim sliders, the categorical controls + unknown toggle:

```js
  // Eieform select ("" = any).
  const eieWrap = document.createElement("div");
  eieWrap.className = "filter-row";
  const eieLabel = document.createElement("div");
  eieLabel.className = "filter-head";
  eieLabel.textContent = "Eieform";
  const eieSelect = document.createElement("select");
  [["", "Alle"], ...(meta.eieformer || []).map((e) => [e, e])].forEach(([value, label]) => {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = label;
    eieSelect.appendChild(opt);
  });
  eieSelect.value = ui.filters.eieform || "";
  eieSelect.addEventListener("change", () => {
    ui.filters.eieform = eieSelect.value;
    onChange();
  });
  eieWrap.appendChild(eieLabel);
  eieWrap.appendChild(eieSelect);
  container.appendChild(eieWrap);

  // Energimerke checkboxes (checked = visible), one per observed letter.
  if ((meta.energimerker || []).length) {
    const energiWrap = document.createElement("div");
    energiWrap.className = "filter-row energi-row";
    const energiLabel = document.createElement("div");
    energiLabel.className = "filter-head";
    energiLabel.textContent = "Energimerking";
    energiWrap.appendChild(energiLabel);
    (meta.energimerker || []).forEach((letter) => {
      const row = document.createElement("label");
      row.className = "toggle energi-toggle";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = !ui.filters.energiHidden[letter];
      cb.addEventListener("change", () => {
        if (cb.checked) delete ui.filters.energiHidden[letter];
        else ui.filters.energiHidden[letter] = true;
        onChange();
      });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(letter));
      energiWrap.appendChild(row);
    });
    container.appendChild(energiWrap);
  }

  // Required facilities (checked = must have), sorted by frequency from meta.
  if ((meta.facilities || []).length) {
    const facWrap = document.createElement("div");
    facWrap.className = "filter-row facilities-row";
    const facLabel = document.createElement("div");
    facLabel.className = "filter-head";
    facLabel.textContent = "Må ha fasiliteter";
    facWrap.appendChild(facLabel);
    (meta.facilities || []).forEach((f) => {
      const row = document.createElement("label");
      row.className = "toggle facility-toggle";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = Boolean(ui.filters.facilitiesRequired[f.name]);
      cb.addEventListener("change", () => {
        if (cb.checked) ui.filters.facilitiesRequired[f.name] = true;
        else delete ui.filters.facilitiesRequired[f.name];
        onChange();
      });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(f.name + " (" + f.count + ")"));
      facWrap.appendChild(row);
    });
    container.appendChild(facWrap);
  }

  // Unknown-value policy for every details filter above.
  const unkRow = document.createElement("label");
  unkRow.className = "toggle";
  const unkCb = document.createElement("input");
  unkCb.type = "checkbox";
  unkCb.checked = ui.filters.includeUnknown !== false;
  unkCb.addEventListener("change", () => {
    ui.filters.includeUnknown = unkCb.checked;
    onChange();
  });
  unkRow.appendChild(unkCb);
  unkRow.appendChild(document.createTextNode("Inkluder ukjent verdi"));
  container.appendChild(unkRow);
```

- [ ] **Step 4: Merge the new object-valued filter keys in `app.js` `loadUi`.** Inside the `filters:` merge object (next to the existing `travelMax` spread), add:

```js
          energiHidden: { ...((stored.filters || {}).energiHidden || {}) },
          facilitiesRequired: { ...((stored.filters || {}).facilitiesRequired || {}) },
```

(The scalar keys — `soveromMin`, `totalprisMax`, `felleskostMax`, `eieform`, `includeUnknown` — are already covered by the existing `...base.filters, ...(stored.filters || {})` spread.)

- [ ] **Step 5: Verify in the browser.** Start the dev server (via the launch config / `skannonser web` against a details-backfilled dev DB — run `skannonser tools backfill-details --db <devdb>` first if the DB has no details yet). Check: the new sliders/select/checkbox groups render in the "Filtre" panel; narrowing "Maks felleskost/mnd" dims listings; toggling "Inkluder ukjent verdi" off additionally dims DNB/unparsed listings while a details filter is active; no console errors; reload preserves the settings (localStorage round-trip).

- [ ] **Step 6: Commit**

```bash
git add skannonser/web/static/filters.js skannonser/web/static/app.js
git commit -m "feat(web): details filters -- soverom/totalpris/felleskost/eieform/energi/fasiliteter + inkluder-ukjent"
```

---

### Task 11: Table columns

**Files:**
- Modify: `skannonser/web/static/table.js` (`COLUMNS`, `NUMERIC_COLUMNS`, `buildRow`)

**Interfaces:**
- Consumes: item keys from Task 9.
- Produces: eight new sortable columns in the table view.

- [ ] **Step 1: Extend `NUMERIC_COLUMNS`** with `"soverom", "etasje", "totalpris", "felleskost_mnd", "pris_kvm_totalpris", "maanedskost"`.

- [ ] **Step 2: Extend `COLUMNS`.** Insert after the `pris_kvm` entry:

```js
  { key: "totalpris", label: "Totalpris", sortable: true },
  { key: "pris_kvm_totalpris", label: "Total/kvm", sortable: true },
  { key: "felleskost_mnd", label: "Felleskost", sortable: true },
  { key: "maanedskost", label: "Mnd-kost", sortable: true },
```

insert after the `bra_i` entry:

```js
  { key: "soverom", label: "Sov", sortable: true },
  { key: "etasje", label: "Etg", sortable: true },
```

insert after the `boligtype` entry:

```js
  { key: "eieform", label: "Eieform", sortable: true },
```

and after the `byggeaar` entry:

```js
  { key: "energimerke", label: "Energi", sortable: true },
```

- [ ] **Step 3: Format the new cells in `buildRow`.** Extend the existing money case:

```js
      case "pris":
      case "pris_kvm":
      case "totalpris":
      case "pris_kvm_totalpris":
      case "felleskost_mnd":
      case "maanedskost":
      case "sold_price": {
```

and the existing plain-numeric case:

```js
      case "bra_i":
      case "soverom":
      case "etasje":
      case "byggeaar":
      case "brj":
      case "mvv":
      case "mvv_uni": {
```

(`eieform` and `energimerke` need no case — the `default:` text branch renders them.)

- [ ] **Step 4: Verify in the browser.** Open `/table`: new headers render; clicking "Totalpris" sorts numerically with blanks last; sold toggle still works; DNB rows show blanks in the new columns; no console errors. With ~1200 rows and 26 columns, confirm the table still renders instantly and scrolls horizontally inside its container rather than overflowing the page.

- [ ] **Step 5: Commit**

```bash
git add skannonser/web/static/table.js
git commit -m "feat(web): details columns in table view (totalpris/felleskost/soverom/energi/...)"
```

---

### Task 12: Popup + detail rows

**Files:**
- Modify: `skannonser/web/static/popup.js` (`buildPopupContent`, the `dl` block at lines ~83-117)

**Interfaces:**
- Consumes: item keys from Task 9.
- Produces: totalpris/felleskost/energimerke/soverom/eieform rows in the map popup.

- [ ] **Step 1: Add the rows.** In `buildPopupContent`, after the travel-destination rows and before the `addRow(dl, "BRA-i", ...)` line, insert:

```js
  // Listing-details enrichment: the true cost picture + key filters.
  addRow(dl, "Totalpris", fmtPris(item.totalpris));
  addRow(dl, "Felleskost", fmtPris(item.felleskost_mnd) && fmtPris(item.felleskost_mnd) + "/mnd");
  addRow(dl, "Mnd-kost", fmtPris(item.maanedskost) && fmtPris(item.maanedskost) + "/mnd");
```

and after the `addRow(dl, "Boligtype", ...)` line:

```js
  addRow(dl, "Eieform", item.eieform);
  addRow(dl, "Soverom", item.soverom);
  if (item.energimerke) {
    addRow(
      dl,
      "Energi",
      item.energimerke + (item.energifarge ? " (" + item.energifarge + ")" : "")
    );
  }
```

(`addRow` already skips null/empty values, so DNB/unparsed items render exactly as before.)

- [ ] **Step 2: Verify in the browser.** Click an Eie marker whose listing has details: the new rows appear with kr-formatting; click a DNB marker: popup unchanged; no console errors.

- [ ] **Step 3: Commit**

```bash
git add skannonser/web/static/popup.js
git commit -m "feat(web): totalpris/felleskost/energi/soverom/eieform rows in map popup"
```

---

### Task 13: Full verification, production backfill note, docs

**Files:**
- Modify: `README.md` (architecture bullets for `ingest/`, `web/`; common-commands block; migrations note `001`–`005` → `001`–`010`)

- [ ] **Step 1: Run the full suite**

Run: `.venv/bin/pytest tests/rebuild -q`
Expected: all pass, zero warnings

- [ ] **Step 2: End-to-end smoke on a dev copy of the live DB**

```bash
cp main/database/properties.db /tmp/details-smoke.db
.venv/bin/python -m skannonser db migrate --db /tmp/details-smoke.db 2>/dev/null || skannonser db migrate --db /tmp/details-smoke.db
skannonser tools backfill-details --db /tmp/details-smoke.db
skannonser tools backfill-details --db /tmp/details-smoke.db --status
```

Expected: backfill reports `parsed` ≈ 5,800+ with a small `missing_html` remainder; coverage shows `with_totalpris` in the thousands. Then serve `skannonser web --db /tmp/details-smoke.db` and spot-check `/api/listings` (details keys populated), `/api/meta` (vocabularies non-empty), the map filters, and `/table` sorting on Totalpris.

- [ ] **Step 3: Update README.** In the architecture section: add `parse_details.py`/`backfill.py` to the `ingest/finn` bullet and mention the details fields + new filters in the `web/` bullet. In the common-commands block add `skannonser tools backfill-details [--wipe|--status]`. Fix the migrations range in the Development section (`001`–`010`). Add a deploy note under "Follow-ups & standing notes": run `skannonser db migrate` + `skannonser tools backfill-details` on the server after deploying (one-time, ~5,900 local parses, no network).

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: listing-details enrichment -- README architecture/commands/deploy note"
```

---

## Self-review notes (already applied)

- **Spec coverage:** parser (spec §1 → Tasks 1-5), storage (§2 → Task 6), ingest/refresh (§3 → Task 7), backfill (§4 → Task 8), API incl. derived/meta/matrikkel-on-detail/sold-bucket (§5 → Task 9), UI filters/table/popup incl. inkluder-ukjent (§6 → Tasks 10-12), testing incl. sheet-payload-unchanged guard (§7 → woven through + Task 9 Step 1).
- **Sheet-unchanged guarantee** is tested (Task 9), not assumed, since the new SQL columns ride in shared fragments.
- **Monkeypatch-ability** of `parse_details` inside `pipeline.py` requires module-level import (`finn_parse_details.parse_details(...)`), noted in Task 7.
- Tasks 7 and 9 describe *test intent* with partial arrange blocks (`...`) because the correct seeding helpers live in the existing test files and must be copied from there — the assert blocks, which carry the behavioral contract, are complete.
