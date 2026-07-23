from skannonser.config.domain import load_domain
from skannonser.geo import is_point_in_polygon
from skannonser.textnorm import normalize_addr, normalize_pc


def test_oslo_center_inside_polygon_north_sea_outside():
    polygon = load_domain().polygon_points
    assert is_point_in_polygon(59.9139, 10.7522, polygon)       # Oslo center
    assert not is_point_in_polygon(58.0, 3.0, polygon)          # North Sea


def test_normalizers_match_legacy():
    # Frozen input->output pairs harvested from the legacy normalizers
    # (main.extractors.filter_and_load_dnbeiendom_no_buffer.normalize_addr /
    # normalize_pc) at deletion, 2026-07-22. Legacy is gone; these literals are
    # the golden contract the ported normalizers must keep matching.
    addr_pairs = [
        ("  Storgata 1 B, 0155 OSLO ", "storgata 1 b 0155 oslo"),
        ("Ullevålsveien 3", "ullevålsveien 3"),
        ("", ""),
        ("Bjørnsons gate 2A", "bjørnsons gate 2a"),
    ]
    for raw, expected in addr_pairs:
        assert normalize_addr(raw) == expected

    pc_pairs = [
        ("0155", "0155"),
        # SANCTIONED DIVERGENCE (2026-07-23): legacy returned "155" here, so a
        # legacy-stripped eiendom postnummer could never match a zero-padded
        # DNB one. normalize_pc now pads short numeric codes to 4 digits
        # (mirrors migration 008's stored-value backfill).
        (155, "0155"),
        ("581", "0581"),
        ("0155.0", "0155"),
        (None, ""),
        ("", ""),
        ("N/A", "N/A"),
    ]
    for raw, expected in pc_pairs:
        assert normalize_pc(raw) == expected
