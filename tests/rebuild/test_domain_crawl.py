"""Tests for the `[crawl]` pacing config (`skannonser.config.domain.Crawl`)
-- the polite-access delay ranges wired into the nightly FINN crawl/refresh.
"""

import pytest

from skannonser.config.domain import Crawl, DomainConfig, load_domain


def test_crawl_defaults_are_polite_ranges():
    c = Crawl()
    # Defaults are the deliberately-slow ranges (seconds), not the legacy
    # sub-second pacing.
    assert c.page_delay_min_s == 2.0
    assert c.page_delay_max_s == 8.0
    assert c.fetch_delay_min_s == 1.0
    assert c.fetch_delay_max_s == 5.0
    assert c.listing_delay_min_s == 1.0
    assert c.listing_delay_max_s == 5.0


def test_crawl_rejects_inverted_range():
    with pytest.raises(ValueError):
        Crawl(page_delay_min_s=8.0, page_delay_max_s=2.0)


def test_load_domain_reads_crawl_section():
    cfg = load_domain()
    # config/domain.toml carries an explicit [crawl] section.
    assert cfg.crawl.page_delay_max_s == 8.0
    assert cfg.crawl.fetch_delay_min_s == 1.0


def test_domain_config_defaults_crawl_when_section_absent():
    base = load_domain()
    data = base.model_dump()
    data.pop("crawl")
    cfg = DomainConfig(**data)
    assert cfg.crawl == Crawl()
