"""Google Sheets client: service-account auth, tab read/rewrite/exists.

Ports the credential-resolution and tab read/clear/write behavior of
`main/googleUtils.py` (`get_service_account_credentials`, `get_sheets_service`,
`download_sheet_as_csv`) and the full-tab-rewrite pattern in
`main/sync/helper_sync_to_sheets.py` (`full_sync_eiendom_to_sheets`,
the sold-sheet sync), simplified for headless cron use.

Simplification (within charter — interactive flows are banned in this
rebuild): legacy `get_credentials()` fell back from service-account auth to
an OAuth `InstalledAppFlow` with a local-server login (and a `token.json`
refresh dance) when no service account was configured. The server cron only
ever runs with a service account, so that interactive fallback is dead code
for this use case and is intentionally not ported — `_build_service` raises
a clear `RuntimeError` instead of attempting an interactive login.

`rewrite_tab` clears then updates with `valueInputOption="USER_ENTERED"`,
matching the legacy full-tab-rewrite call sites (not the `"RAW"` ones used
elsewhere for plain coordinate/header syncs):
`main/sync/helper_sync_to_sheets.py:714-719` (sold-sheet sync) and
`:793-798` (`full_sync_eiendom_to_sheets`). Both write a header + data block
where the Finnkode column carries `=HYPERLINK(...)` formulas
(`main/googleUtils.py:117`, `main/sync/helper_sync_to_sheets.py:352-353`);
`USER_ENTERED` is required so Sheets evaluates those as formulas instead of
storing the literal formula text as `"RAW"` would.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from skannonser.config.settings import get_secrets

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsClient:
    """Thin wrapper around the Sheets API v4 `spreadsheets` resource."""

    def __init__(self, spreadsheet_id: str, service: Any = None):
        self.spreadsheet_id = spreadsheet_id
        self._service = service

    @property
    def service(self) -> Any:
        if self._service is None:
            self._service = self._build_service()
        return self._service

    def _build_service(self) -> Any:
        # Imported lazily so callers/tests that inject a fake `service` never
        # need googleapiclient/google-auth importable.
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        sa_path: Path | None = get_secrets().google_service_account_file
        if sa_path is None or not Path(sa_path).exists():
            raise RuntimeError(
                "No Google service-account file configured. Set "
                "GOOGLE_SERVICE_ACCOUNT_FILE (or google_service_account_file "
                "in .env) to a valid service-account JSON key file."
            )
        creds = service_account.Credentials.from_service_account_file(str(sa_path), scopes=SCOPES)
        return build("sheets", "v4", credentials=creds)

    def read_tab(self, tab: str) -> list[list]:
        """Return all values in `tab`, or [] when the tab is empty/absent."""
        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=tab)
            .execute()
        )
        return result.get("values", [])

    def rewrite_tab(self, tab: str, rows: list[list]) -> int:
        """Clear `tab` then write `rows` starting at A1. Returns cells written."""
        service = self.service
        service.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=tab,
        ).execute()
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{tab}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": rows},
            )
            .execute()
        )
        return result.get("updatedCells", 0)

    def tab_exists(self, tab: str) -> bool:
        """Return whether `tab` is a sheet in the spreadsheet's metadata."""
        meta = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
        return tab in titles
