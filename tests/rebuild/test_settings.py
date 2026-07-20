from pathlib import Path

from skannonser.config.settings import Secrets, get_secrets


def test_defaults(monkeypatch):
    monkeypatch.delenv("SKANNONSER_DB_PATH", raising=False)
    s = Secrets(_env_file=None)
    assert s.db_path == Path("main/database/properties.db")
    assert s.notify_bin == "notify"
    assert s.google_maps_api_key == ""


def test_env_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(tmp_path / "x.db"))
    s = get_secrets()
    assert s.google_maps_api_key == "test-key"
    assert s.db_path == tmp_path / "x.db"
