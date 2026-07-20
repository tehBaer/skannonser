from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Secrets(BaseSettings):
    """Secrets and machine-specific paths. Values come from env vars / .env only."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    google_maps_api_key: str = ""
    spreadsheet_id: str = ""
    google_service_account_file: Path | None = None
    notify_bin: str = "notify"
    db_path: Path = Field(
        default=Path("main/database/properties.db"),
        validation_alias="SKANNONSER_DB_PATH",
    )


@lru_cache
def get_secrets() -> Secrets:
    return Secrets()
