"""Loads notification secrets from a gitignored module or environment variables."""
import os

try:
    from main.config import notify_secrets as _secrets
except Exception:
    try:
        from config import notify_secrets as _secrets
    except Exception:
        _secrets = None


def _get(name, default=""):
    if _secrets is not None and hasattr(_secrets, name):
        return getattr(_secrets, name)
    return os.environ.get(name, default)


PUSHOVER_APP_TOKEN = _get("PUSHOVER_APP_TOKEN")
PUSHOVER_USER_KEY = _get("PUSHOVER_USER_KEY")
HEALTHCHECKS_URL = _get("HEALTHCHECKS_URL")
