import pytest

from skannonser.config.settings import get_secrets


@pytest.fixture(autouse=True)
def clear_secrets_cache():
    get_secrets.cache_clear()
    yield
    get_secrets.cache_clear()
