from skannonser.config.domain import load_domain
from skannonser.geo import is_point_in_polygon
from skannonser.textnorm import normalize_addr, normalize_pc


def test_oslo_center_inside_polygon_north_sea_outside():
    polygon = load_domain().polygon_points
    assert is_point_in_polygon(59.9139, 10.7522, polygon)       # Oslo center
    assert not is_point_in_polygon(58.0, 3.0, polygon)          # North Sea


def test_normalizers_match_legacy():
    import sys
    sys.path.insert(0, ".")
    from main.extractors.filter_and_load_dnbeiendom_no_buffer import (
        normalize_addr as legacy_addr, normalize_pc as legacy_pc)
    samples = ["  Storgata 1 B, 0155 OSLO ", "Ullevålsveien 3", "", "Bjørnsons gate 2A"]
    for s in samples:
        assert normalize_addr(s) == legacy_addr(s)
    for pc in ["0155", 155, "0155.0", None, ""]:
        assert normalize_pc(pc) == legacy_pc(pc)
