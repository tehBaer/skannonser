import json
from types import SimpleNamespace

from skannonser.store import connection, migrations
from skannonser.enrich.tilstand import cache_get, content_sha
from skannonser.enrich.tilstand_backfill import classify_tilstand, classify_tilstand_batch

RESPONSE = json.dumps({
    "findings": [
        {"tg": 3, "bygningsdel": "vatrom", "tiltak": None, "alvorlighet": "alvorlig",
         "kostnad_lav": 200_000, "kostnad_hoy": 500_000, "kostnad_kilde": "estimat"},
    ],
    "egenerklaering_present": True,
    "egenerklaering": [],
    "tilstandsrapport_dato": None,
    "tilstandsrapport_utsteder": None,
})

FAKE_INPUT = lambda html: html.strip() or None  # noqa: E731


def _env(tmp_path, ads: dict[str, str]):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    html_dir = tmp_path / "html_extracted"
    html_dir.mkdir()
    for finnkode, text in ads.items():
        conn.execute("INSERT INTO eiendom (finnkode) VALUES (?)", (finnkode,))
        (html_dir / f"{finnkode}.html").write_text(text)
    conn.commit()
    return conn


def test_classifies_and_upserts(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})
    calls = []
    result = classify_tilstand(
        conn, tmp_path, _call=lambda t: calls.append(t) or RESPONSE, _input_fn=FAKE_INPUT
    )
    assert result["called"] == 1 and result["upserted"] == 1
    assert conn.execute("SELECT tg3_count FROM listing_tilstand WHERE finnkode='1'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0] == 1


def test_second_run_replays_from_cache_without_api(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})
    classify_tilstand(conn, tmp_path, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT)

    def explode(text):
        raise AssertionError("API called on cached input")

    result = classify_tilstand(conn, tmp_path, _call=explode, _input_fn=FAKE_INPUT)
    assert result["cached"] == 1 and result["called"] == 0 and result["upserted"] == 1


def test_limit_bounds_api_calls_not_cache_replays(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60, "2": "TG3 b " * 60, "3": "TG3 c " * 60})
    result = classify_tilstand(
        conn, tmp_path, limit=2, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT
    )
    assert result["called"] == 2 and result["limit_skipped"] == 1


def test_cache_only_never_calls(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})

    def explode(text):
        raise AssertionError("cache_only must not call the API")

    result = classify_tilstand(conn, tmp_path, cache_only=True, _call=explode, _input_fn=FAKE_INPUT)
    assert result["uncached_skipped"] == 1 and result["upserted"] == 0


def test_empty_input_and_missing_html_are_counted_not_fatal(tmp_path):
    conn = _env(tmp_path, {"1": "   "})
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('2')")  # no html file
    conn.commit()
    result = classify_tilstand(conn, tmp_path, _call=lambda t: RESPONSE, _input_fn=FAKE_INPUT)
    assert result["empty_input"] == 1 and result["missing_html"] == 1


def test_bad_api_response_is_error_not_cached(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 bad " * 50})
    result = classify_tilstand(conn, tmp_path, _call=lambda t: "not json", _input_fn=FAKE_INPUT)
    assert result["errors"] == 1 and result["upserted"] == 0
    assert cache_get(conn, content_sha(("TG3 bad " * 50).strip())) is None


class FakeBatchClient:
    """Stands in for anthropic.Anthropic(): create -> poll twice -> results."""

    def __init__(self, response_json, stop_reason="end_turn"):
        self.response_json = response_json
        self.stop_reason = stop_reason
        self.submitted = None
        self.polls = 0
        outer = self

        class _Batches:
            def create(self, requests):
                outer.submitted = requests
                return SimpleNamespace(id="b1", processing_status="in_progress")

            def retrieve(self, batch_id):
                outer.polls += 1
                status = "ended" if outer.polls >= 2 else "in_progress"
                return SimpleNamespace(id=batch_id, processing_status=status)

            def results(self, batch_id):
                for req in outer.submitted:
                    yield SimpleNamespace(
                        custom_id=req["custom_id"],
                        result=SimpleNamespace(
                            type="succeeded",
                            message=SimpleNamespace(
                                stop_reason=outer.stop_reason,
                                content=[SimpleNamespace(type="text", text=outer.response_json)],
                            ),
                        ),
                    )

        self.messages = SimpleNamespace(batches=_Batches())


def test_batch_submits_polls_caches_and_derives(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60, "2": "TG3 b " * 60})
    client = FakeBatchClient(RESPONSE)
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["submitted"] == 2 and result["succeeded"] == 2
    assert client.polls >= 2
    assert result["derive_upserted"] == 2
    # requests keyed by content sha, params carry the strict schema
    req = client.submitted[0]
    assert len(req["custom_id"]) == 64
    assert req["params"]["output_config"]["format"]["type"] == "json_schema"
    assert req["params"]["max_tokens"] == 32000


def test_batch_skips_cached_and_dedups_identical_inputs(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 same " * 40, "2": "TG3 same " * 40})
    client = FakeBatchClient(RESPONSE)
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["submitted"] == 1          # identical text -> one request
    assert result["derive_upserted"] == 2    # ...but both ads get rows


def test_batch_failed_result_is_counted_not_cached(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60})
    client = FakeBatchClient("not json")
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["failed"] == 1 and result["succeeded"] == 0
    assert result["derive_upserted"] == 0


def test_batch_max_tokens_result_is_counted_not_cached(tmp_path):
    conn = _env(tmp_path, {"1": "TG3 a " * 60})
    client = FakeBatchClient(RESPONSE, stop_reason="max_tokens")
    result = classify_tilstand_batch(
        conn, tmp_path, _client=client, _sleep=lambda s: None, _input_fn=FAKE_INPUT
    )
    assert result["failed"] == 1 and result["succeeded"] == 0
    assert result["derive_upserted"] == 0
    assert cache_get(conn, content_sha(("TG3 a " * 60).strip())) is None


def test_batch_nothing_to_do(tmp_path):
    conn = _env(tmp_path, {"1": "   "})
    result = classify_tilstand_batch(
        conn, tmp_path, _client=FakeBatchClient(RESPONSE),
        _sleep=lambda s: None, _input_fn=FAKE_INPUT,
    )
    assert result["submitted"] == 0
