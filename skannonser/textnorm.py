"""Text normalization utilities for addresses and postcodes."""
import re
import pandas as pd


def normalize_addr(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"[.,©()\"'\\/]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_pc(pc) -> str:
    if pd.isna(pc):
        return ""
    s = str(pc).strip()
    s = re.sub(r"\.0$", "", s)
    # Zero-pad short numeric codes (Norwegian postcodes are always 4 digits) so
    # legacy-stripped values ("581") and preserved ones ("0581") produce the
    # SAME match key. Deliberate divergence from the legacy normalizer, which
    # returned the stripped form verbatim and thus could never match a
    # stripped eiendom row against a padded DNB row (2026-07-23; see
    # migration 008, which fixes the stored values the same way).
    if s.isdigit() and len(s) < 4:
        s = s.zfill(4)
    return s
