import pytest

from skannonser.publish.sheets_client import SheetsClient


class _Execute:
    """Mimics a google-api-python-client request object's .execute()."""

    def __init__(self, result):
        self._result = result

    def execute(self):
        return self._result


class FakeValues:
    def __init__(self, calls, get_result=None, update_result=None):
        self.calls = calls
        self._get_result = get_result if get_result is not None else {"values": []}
        self._update_result = update_result if update_result is not None else {"updatedCells": 0}

    def get(self, spreadsheetId, range):
        self.calls.append(("get", spreadsheetId, range))
        return _Execute(dict(self._get_result))

    def clear(self, spreadsheetId, range):
        self.calls.append(("clear", spreadsheetId, range))
        return _Execute({})

    def update(self, spreadsheetId, range, valueInputOption, body):
        self.calls.append(("update", spreadsheetId, range, valueInputOption, body))
        return _Execute(dict(self._update_result))


class FakeSpreadsheets:
    def __init__(self, calls, values_kwargs=None, meta=None):
        self.calls = calls
        self._values = FakeValues(calls, **(values_kwargs or {}))
        self._meta = meta if meta is not None else {"sheets": []}

    def values(self):
        return self._values

    def get(self, spreadsheetId):
        self.calls.append(("meta_get", spreadsheetId))
        return _Execute(dict(self._meta))


class FakeService:
    def __init__(self, values_kwargs=None, meta=None):
        self.calls = []
        self._spreadsheets = FakeSpreadsheets(self.calls, values_kwargs=values_kwargs, meta=meta)

    def spreadsheets(self):
        return self._spreadsheets


def test_read_tab_returns_values():
    service = FakeService(values_kwargs={"get_result": {"values": [["a", "b"]]}})
    client = SheetsClient("SHEET_ID", service=service)

    rows = client.read_tab("Eie")

    assert rows == [["a", "b"]]
    assert service.calls == [("get", "SHEET_ID", "Eie")]


def test_read_tab_empty_when_absent():
    service = FakeService()  # no "values" key path -> default {"values": []}
    client = SheetsClient("SHEET_ID", service=service)

    assert client.read_tab("Eie") == []


def test_rewrite_tab_clears_then_updates_with_user_entered_and_returns_updated_cells():
    service = FakeService(values_kwargs={"update_result": {"updatedCells": 42}})
    client = SheetsClient("SHEET_ID", service=service)

    rows = [["Finnkode", "Adresse"], ['=HYPERLINK("http://x", "123")', "Foo"]]
    updated = client.rewrite_tab("Eie", rows)

    assert updated == 42
    assert service.calls == [
        ("clear", "SHEET_ID", "Eie"),
        ("update", "SHEET_ID", "Eie!A1", "USER_ENTERED", {"values": rows}),
    ]


def test_rewrite_tab_defaults_to_zero_updated_cells_when_missing():
    service = FakeService()
    client = SheetsClient("SHEET_ID", service=service)

    assert client.rewrite_tab("Eie", [["a"]]) == 0


def test_tab_exists_true_and_false():
    service = FakeService(
        meta={"sheets": [{"properties": {"title": "Eie"}}, {"properties": {"title": "Sold"}}]}
    )
    client = SheetsClient("SHEET_ID", service=service)

    assert client.tab_exists("Sold") is True
    assert client.tab_exists("Missing") is False


def test_injected_service_never_triggers_build_service(monkeypatch):
    build_calls = []
    monkeypatch.setattr(
        SheetsClient, "_build_service", lambda self: build_calls.append(True) or object()
    )

    service = FakeService()
    client = SheetsClient("SHEET_ID", service=service)
    client.read_tab("Eie")
    client.rewrite_tab("Eie", [["a"]])
    client.tab_exists("Eie")

    assert build_calls == []


def test_build_service_raises_when_no_service_account_file(monkeypatch, tmp_path):
    from skannonser.config import settings as settings_module

    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(tmp_path / "missing.json"))
    settings_module.get_secrets.cache_clear()
    try:
        client = SheetsClient("SHEET_ID")
        with pytest.raises(RuntimeError, match="service-account"):
            client._build_service()
    finally:
        settings_module.get_secrets.cache_clear()
