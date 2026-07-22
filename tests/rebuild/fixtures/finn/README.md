# Finn parser golden fixtures

`<finnkode>.html` + `<finnkode>.expected.json` are golden fixtures for the
Finn ad parser (`test_finn_parse.py`). Each `.expected.json` is the field dict
the **legacy** parser produced for that archived ad page.

**These expectations were frozen from the legacy parser on 2026-07-20.**
The legacy system (`main/`) was deleted in Phase 6, so `generate_expected.py`
is gone and **regeneration is no longer possible**. Treat every
`.expected.json` here as golden: if a fixture must change, update it by hand
with a documented reason — never by re-running a legacy generator.

`result_page1.html` is an archived Finn search-result page used by the crawl
tests (`test_finn_crawl.py`).
