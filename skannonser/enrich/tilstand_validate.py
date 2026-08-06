"""Stage-1 validation harness (design spec 'Validation: the free ground
truth'). The ~12% of ads with surveyor-stated cost bands are labelled data
for the estimation task: strip the stated figures, let the model estimate
blind, compare. Acceptance gate: >=70% within one band, no direction bias.
Never used in production classification -- there, stated costs stay in.
"""
import re
import sqlite3
from pathlib import Path

from skannonser.enrich.tilstand import GRID, classify_input, classify_one

# The label vocabulary measured over 500 ads (design spec 'Measurements').
_COST_LABEL = re.compile(
    r"(kostnadsestimat|utbedringskostnad\w*|kostnadsoverslag"
    r"|(?:sjablongmessig\w*\s+|estimert\w*\s+)?prisanslag"
    r"|oppgraderingskostnad\w*|estimert\w*\s+kostnad\w*)"
    r"(?:\s+gjelder(?:\s+er)?)?\s*:?\s*",
    re.I,
)
# A band range or an open-ended bound, right after a label.
_COST_VALUE = re.compile(
    r"(?:under|over)\s*(?:kr\.?\s*)?[\d][\d\s.]{2,}"
    r"|(?:kr\.?\s*)?[\d][\d\s.]{2,}\s*(?:-|–|til)\s*(?:kr\.?\s*)?[\d][\d\s.]{2,}",
    re.I,
)


def snap_band(lav: int, hoy: int) -> tuple[int, int]:
    """Outward snap: never narrower than the surveyor said."""
    lo = max((g for g in GRID if g <= lav), default=0)
    hi = min((g for g in GRID if g >= hoy), default=GRID[-1])
    return lo, hi


def _amounts(raw: str) -> list[int]:
    out = []
    for m in re.findall(r"[\d][\d\s.]{2,}", raw):
        digits = re.sub(r"\D", "", m)
        if digits and int(digits) >= 1000:
            out.append(int(digits))
    return out


def _label_value_spans(text: str) -> list[tuple[int, int, str]]:
    """(start, end, value_text) for each stated label+figure occurrence."""
    spans = []
    for m in _COST_LABEL.finditer(text):
        v = _COST_VALUE.match(text[m.end():m.end() + 60])
        if v:
            spans.append((m.start(), m.end() + v.end(), v.group(0)))
    return spans


def stated_bands(text: str) -> list[tuple[int, int]]:
    bands = []
    for _, _, raw in _label_value_spans(text):
        nums = _amounts(raw)
        if not nums:
            continue
        low = raw.lower()
        if len(nums) == 1:
            lav, hoy = ((0, nums[0]) if "under" in low
                        else (nums[0], GRID[-1]) if "over" in low
                        else (nums[0], nums[0]))
        else:
            lav, hoy = nums[0], nums[1]
        if lav <= hoy:
            bands.append(snap_band(lav, hoy))
    return bands


def strip_stated_costs(text: str) -> str:
    out, prev = [], 0
    for a, b, _ in _label_value_spans(text):
        out.append(text[prev:a])
        prev = b
    out.append(text[prev:])
    return "".join(out)


def validate_estimates(
    conn: sqlite3.Connection,
    project_dir: Path,
    *,
    limit: int = 50,
    _call=None,
    _input_fn=None,
) -> dict:
    input_fn = _input_fn or classify_input
    report = {"ads": 0, "attempts": 0, "pairs": 0, "exact": 0, "within_one": 0,
              "model_higher": 0, "model_lower": 0,
              "stated_unmatched": 0, "model_unmatched": 0}
    for (finnkode,) in conn.execute("SELECT finnkode FROM eiendom"):
        # Bounded by ATTEMPTS, not scored ads: `ads` only increments after a
        # classify_one call succeeds, so persistent API failures would
        # otherwise walk the whole corpus making paid calls that never count
        # toward the limit.
        if report["attempts"] >= limit:
            break
        path = Path(project_dir) / "html_extracted" / f"{finnkode}.html"
        if not path.is_file():
            continue
        text = input_fn(path.read_text(encoding="utf-8", errors="replace"))
        if text is None:
            continue
        stated = sorted(stated_bands(text), key=lambda b: b[0] + b[1])
        if not stated:
            continue
        report["attempts"] += 1
        try:
            resp = (classify_one(strip_stated_costs(text), _call=_call)
                    if _call else classify_one(strip_stated_costs(text)))
            model = sorted(
                ((f.kostnad_lav, f.kostnad_hoy) for f in resp.findings
                 if f.kostnad_lav is not None and f.kostnad_hoy is not None
                 and f.kostnad_kilde == "estimat"),
                key=lambda b: b[0] + b[1],
            )
            report["ads"] += 1
            n = min(len(stated), len(model))
            report["stated_unmatched"] += len(stated) - n
            report["model_unmatched"] += len(model) - n
            for (slav, shoy), (mlav, mhoy) in zip(stated[:n], model[:n]):
                report["pairs"] += 1
                if (mlav, mhoy) == (slav, shoy):
                    report["exact"] += 1
                if (abs(GRID.index(mlav) - GRID.index(slav)) <= 1
                        and abs(GRID.index(mhoy) - GRID.index(shoy)) <= 1):
                    report["within_one"] += 1
                mmid, smid = (mlav + mhoy) / 2, (slav + shoy) / 2
                if mmid > smid:
                    report["model_higher"] += 1
                elif mmid < smid:
                    report["model_lower"] += 1
        except Exception:
            continue
    return report
