from pathlib import Path

from skannonser.verify.parse import verify_parse


def test_verify_parse_reports_identical_on_fixture_corpus():
    result = verify_parse(Path("data/eiendom"), limit=12, allowlist={})
    assert result.total == 12
    assert result.identical + result.allowlisted + len(result.diffs) == result.total
