import json

from skannonser.enrich.tilstand_validate import snap_band, stated_bands, strip_stated_costs, validate_estimates
from skannonser.store import connection, migrations


def test_snap_band_outward():
    assert snap_band(15_000, 60_000) == (10_000, 100_000)
    assert snap_band(10_000, 50_000) == (10_000, 50_000)   # already on grid
    assert snap_band(700_000, 2_000_000) == (500_000, 1_000_000)  # ceiling caps at 1M+


def test_stated_bands_covers_the_observed_phrasings():
    text = (
        "Badet er ikke tett. Kostnadsestimat: 200 000 - 500 000,-. "
        "Vinduer med trekarmer. Utbedringskostnader: Under 10 000. "
        "Taket har mose. Estimert prisanslag Kr 100 000 - 300 000."
    )
    assert stated_bands(text) == [(200_000, 500_000), (0, 10_000), (100_000, 300_000)]


def test_stated_bands_ignores_prose_mentions_without_figures():
    # boilerplate: "angir ... kostnadsoverslag for eventuelle oppgraderinger"
    assert stated_bands("Rapporten angir kostnadsoverslag for oppgraderinger.") == []


def test_strip_removes_figures_but_keeps_defect_text():
    text = "Badet er ikke tett. Kostnadsestimat: 200 000 - 500 000. Må utbedres."
    stripped = strip_stated_costs(text)
    assert "200 000" not in stripped
    assert "Badet er ikke tett" in stripped and "Må utbedres" in stripped


AD_TEXT = ("Badet er ikke tett og må renoveres. TG3. "
           "Kostnadsestimat: 200 000 - 500 000. ") * 5

ESTIMATE_RESPONSE = json.dumps({
    "findings": [
        {"tg": 3, "bygningsdel": "vatrom", "tiltak": None, "alvorlighet": "alvorlig",
         "kostnad_lav": 100_000, "kostnad_hoy": 300_000, "kostnad_kilde": "estimat"},
    ],
    "egenerklaering_present": False,
    "egenerklaering": [],
    "tilstandsrapport_dato": None,
    "tilstandsrapport_utsteder": None,
})


def test_validate_pairs_and_scores(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    (tmp_path / "html_extracted").mkdir()
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('1')")
    (tmp_path / "html_extracted" / "1.html").write_text(AD_TEXT)
    conn.commit()

    seen = []
    report = validate_estimates(
        conn, tmp_path, limit=10,
        _call=lambda t: seen.append(t) or ESTIMATE_RESPONSE,
        _input_fn=lambda html: html.strip() or None,
    )
    # the model was shown the STRIPPED text
    assert "200 000" not in seen[0]
    assert report["ads"] == 1
    # AD_TEXT repeats the stated band 5x -> 5 stated bands, model gave 1
    assert report["pairs"] == 1 and report["stated_unmatched"] == 4
    # (100k, 300k) vs stated (200k, 500k): one grid step off on both bounds
    assert report["exact"] == 0 and report["within_one"] == 1
    assert report["model_lower"] == 1
