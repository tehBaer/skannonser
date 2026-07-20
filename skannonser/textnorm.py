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
    return s
