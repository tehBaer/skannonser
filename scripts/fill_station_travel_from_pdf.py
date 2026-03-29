#!/usr/bin/env python3
"""Fill station travel times to a destination by parsing timetable PDFs.

Primary workflow:
1) Parse local timetable PDFs (default: ./rutetabeller tog/*.pdf)
2) Estimate station->destination minutes per line using weekday departures
   nearest 08:00 from each station
3) Upsert into normalized station tables (stations / station_lines / station_travel)
4) Optionally sync DB export to Google Sheets Stations tab

This script is intentionally conservative: ambiguous rows are skipped rather
than force-written.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import tempfile
import unicodedata
import urllib.request
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

# Allow importing main.* when executed from scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main.database.stations import StationDatabase
from main.sync.sync_stations_to_sheet import sync_stations_to_sheet


TIME_RE = re.compile(r"\b\d{4}\b|\b\d{2}\b")
STATION_PREFIX_RE = re.compile(r"^([A-Za-z .\-_/():+&'\u00c0-\u017f]+?)\s+(\d{4}|\d{2})\b")
LINE_TOKEN_RE = re.compile(r"(?:RE|R|L)\d+[A-Z]?", re.IGNORECASE)
STATION_TOKEN_RE = re.compile(r"^[A-Za-z'\-.\u00c0-\u017f]+$")


@dataclass
class ParsedRow:
    station: str
    times_abs: List[int]
    time_points: List["TimePoint"] = field(default_factory=list)


@dataclass
class TimePoint:
    x0: float
    minutes_abs: int


@dataclass
class Candidate:
    station: str
    line: str
    minutes: int
    departure_abs: int
    arrival_abs: int
    x_delta: float
    source_pdf: str
    page_no: int


def normalize_space(text: str) -> str:
    return " ".join((text or "").strip().split())


def normalize_key(text: str) -> str:
    text = normalize_space(text)
    text = (
        text.replace("æ", "ae")
        .replace("Æ", "AE")
        .replace("ø", "o")
        .replace("Ø", "O")
        .replace("å", "a")
        .replace("Å", "A")
    )
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.casefold()
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def slugify_for_filename(text: str) -> str:
    key = normalize_key(text)
    return key or "destination"


def destination_writes_skoyen_fallback(destination: str) -> bool:
    # The stations table has a dedicated fallback column to_skoyen_min.
    return normalize_key(destination) == "skoyen"


def maybe_deduplicate_station_name(name: str) -> str:
    name = normalize_space(name)
    parts = name.split()
    if len(parts) >= 2 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if parts[:half] == parts[half:]:
            parts = parts[:half]

    # Some PDFs leak a stray single-letter marker into the station label,
    # e.g. "Dal k" or "Lillestrom k". Drop those trailing artifacts.
    while parts and len(parts[-1]) == 1 and parts[-1].islower():
        parts = parts[:-1]

    return " ".join(parts)


def sanitize_station_tokens(tokens: Sequence[str]) -> List[str]:
    parts = [normalize_space(token) for token in tokens if normalize_space(token)]
    if any(any(ch.isdigit() for ch in part) for part in parts):
        return []

    while parts and not STATION_TOKEN_RE.fullmatch(parts[-1]):
        parts.pop()

    while parts and len(parts[-1]) == 1 and parts[-1].islower():
        parts.pop()

    return parts


def is_plausible_station_name(name: str) -> bool:
    key = normalize_key(name)
    return bool(key) and key not in {"tognr"}


def parse_line_ids_from_stem(stem: str) -> List[str]:
    stem_u = stem.upper()
    tokens = [tok.upper() for tok in LINE_TOKEN_RE.findall(stem_u)]
    dedup: List[str] = []
    seen = set()
    for tok in tokens:
        if tok not in seen:
            dedup.append(tok)
            seen.add(tok)
    return dedup


def parse_line_pdf_mapping_value(value: str) -> Tuple[List[str], str]:
    if "=" not in value:
        raise ValueError(f"Invalid --line-pdf mapping (expected LINE=PATH_OR_URL): {value}")
    line_raw, source_raw = value.split("=", 1)
    source = source_raw.strip()
    lines = [ln.strip().upper() for ln in line_raw.split("+") if ln.strip()]
    if not lines or not source:
        raise ValueError(f"Invalid --line-pdf mapping: {value}")
    return lines, source


def read_nonempty_lines(path: Path) -> List[str]:
    items: List[str] = []
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            items.append(line)
    return items


def resolve_sources(args: argparse.Namespace) -> List[Tuple[List[str], Path]]:
    """Resolve all PDF sources into (line_ids, local_pdf_path) tuples."""
    resolved: List[Tuple[List[str], Path]] = []

    # 1) Explicit line mapping from CLI and optional list-file.
    mapping_specs: List[str] = list(args.line_pdf or [])
    if args.line_pdf_list_file:
        mapping_specs.extend(read_nonempty_lines(Path(args.line_pdf_list_file)))

    for spec in mapping_specs:
        lines, source = parse_line_pdf_mapping_value(spec)
        local_path = materialize_source_to_local_pdf(source)
        resolved.append((lines, local_path))

    # 2) Generic sources from --pdf-url and --pdf-list-file.
    generic_sources: List[str] = list(args.pdf_url or [])
    if args.pdf_list_file:
        generic_sources.extend(read_nonempty_lines(Path(args.pdf_list_file)))
    for source in generic_sources:
        local_path = materialize_source_to_local_pdf(source)
        line_ids = parse_line_ids_from_stem(local_path.stem)
        resolved.append((line_ids, local_path))

    # 3) Default local directory fallback when nothing explicit is provided.
    if not resolved:
        for pdf_path in sorted(Path(args.pdf_dir).glob("*.pdf")):
            line_ids = parse_line_ids_from_stem(pdf_path.stem)
            resolved.append((line_ids, pdf_path))

    # Normalize and keep only readable files.
    filtered: List[Tuple[List[str], Path]] = []
    for line_ids, path in resolved:
        if not path.exists() or not path.is_file():
            print(f"WARN: skipping missing PDF source: {path}")
            continue
        if path.suffix.lower() != ".pdf":
            print(f"WARN: skipping non-PDF source: {path}")
            continue
        filtered.append((line_ids, path))

    return filtered


def materialize_source_to_local_pdf(source: str) -> Path:
    src = source.strip()
    if src.lower().startswith("http://") or src.lower().startswith("https://"):
        return download_pdf(src)
    return Path(src)


def download_pdf(url: str) -> Path:
    tmp_dir = Path(tempfile.mkdtemp(prefix="station_travel_pdf_"))
    name = Path(url.split("?", 1)[0]).name or "timetable.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    target = tmp_dir / name
    with urllib.request.urlopen(url, timeout=60) as resp:
        payload = resp.read()
    target.write_bytes(payload)
    return target


def row_from_words(words: Sequence[Dict[str, object]], y_bucket: float) -> str:
    row_words = [w for w in words if round(float(w.get("top", 0.0)) / y_bucket) * y_bucket == y_bucket]
    row_words.sort(key=lambda w: float(w.get("x0", 0.0)))
    return " ".join(str(w.get("text", "")) for w in row_words).strip()


def extract_rows_from_page(page) -> List[str]:
    words = page.extract_words(use_text_flow=False)
    by_bucket: Dict[int, List[Tuple[float, str]]] = {}
    for w in words:
        top = float(w.get("top", 0.0))
        y = int(round(top / 2.0) * 2)
        by_bucket.setdefault(y, []).append((float(w.get("x0", 0.0)), str(w.get("text", ""))))

    rows: List[str] = []
    for y in sorted(by_bucket):
        toks = [t for _, t in sorted(by_bucket[y], key=lambda item: item[0])]
        s = normalize_space(" ".join(toks))
        if s:
            rows.append(s)
    return rows


def extract_positioned_rows_from_page(page) -> List[List[Tuple[float, str]]]:
    words = page.extract_words(use_text_flow=False)
    by_bucket: Dict[int, List[Tuple[float, str]]] = {}
    for w in words:
        top = float(w.get("top", 0.0))
        y = int(round(top / 2.0) * 2)
        by_bucket.setdefault(y, []).append((float(w.get("x0", 0.0)), str(w.get("text", ""))))

    rows: List[List[Tuple[float, str]]] = []
    for y in sorted(by_bucket):
        toks = sorted(by_bucket[y], key=lambda item: item[0])
        if toks:
            rows.append(toks)
    return rows


def trim_duplicate_station_segment(station: str, row_text: str) -> str:
    # Many pages include a duplicated Saturday block on the same horizontal row.
    # Keep the first occurrence block to stay on the weekday segment.
    escaped = re.escape(station)
    matches = list(re.finditer(rf"\b{escaped}\b", row_text, flags=re.IGNORECASE))
    if len(matches) >= 2:
        return row_text[: matches[1].start()].strip()
    return row_text


def parse_row(row_text: str) -> Optional[ParsedRow]:
    m = STATION_PREFIX_RE.search(row_text)
    if not m:
        return None

    station = maybe_deduplicate_station_name(m.group(1))
    station = normalize_space(station)
    if not station or not is_plausible_station_name(station):
        return None

    segment = trim_duplicate_station_segment(station, row_text)
    tokens = TIME_RE.findall(segment)
    if len(tokens) < 4:
        return None

    times_abs = expand_time_tokens(tokens)
    if len(times_abs) < 3:
        return None

    return ParsedRow(station=station, times_abs=times_abs)


def expand_time_tokens_with_positions(tokens: Sequence[Tuple[float, str]]) -> List[TimePoint]:
    out: List[TimePoint] = []
    prev: Optional[int] = None

    for x0, tok in tokens:
        tok = tok.strip()
        if not tok.isdigit():
            continue

        if len(tok) == 4:
            hour = int(tok[:2])
            minute = int(tok[2:])
            if hour > 24 or minute >= 60:
                continue
            if hour == 24:
                hour = 0
            cur = hour * 60 + minute
            if prev is not None:
                while cur <= prev:
                    cur += 24 * 60
        elif len(tok) == 2:
            minute = int(tok)
            if minute >= 60 or prev is None:
                continue
            base_hour = (prev // 60) % 24
            base_day = prev // (24 * 60)
            cur = base_day * 24 * 60 + base_hour * 60 + minute
            while cur <= prev:
                cur += 60
        else:
            continue

        out.append(TimePoint(x0=float(x0), minutes_abs=cur))
        prev = cur

    return out


def parse_row_tokens(row_tokens: Sequence[Tuple[float, str]]) -> Optional[ParsedRow]:
    if not row_tokens:
        return None

    ordered = sorted(row_tokens, key=lambda item: item[0])
    texts = [text for _, text in ordered]
    row_text = normalize_space(" ".join(texts))
    if not row_text:
        return None

    time_start_idx = None
    for i, (_, text) in enumerate(ordered):
        token = text.strip()
        if TIME_RE.fullmatch(token):
            time_start_idx = i
            break
    if time_start_idx is None:
        return None

    station_tokens = sanitize_station_tokens([text for _, text in ordered[:time_start_idx]])
    if not station_tokens:
        return None

    station = maybe_deduplicate_station_name(" ".join(station_tokens))
    station = normalize_space(station)
    if not station or not is_plausible_station_name(station):
        return None

    time_points = expand_time_tokens_with_positions(ordered[time_start_idx:])
    times_abs = [tp.minutes_abs for tp in time_points]
    if len(times_abs) < 3:
        return None

    return ParsedRow(station=station, times_abs=times_abs, time_points=time_points)


def expand_time_tokens(tokens: Sequence[str]) -> List[int]:
    abs_minutes: List[int] = []
    prev: Optional[int] = None

    for tok in tokens:
        tok = tok.strip()
        if not tok.isdigit():
            continue

        if len(tok) == 4:
            hour = int(tok[:2])
            minute = int(tok[2:])
            if hour > 24 or minute >= 60:
                continue
            if hour == 24:
                hour = 0
            cur = hour * 60 + minute
            if prev is not None:
                while cur <= prev:
                    cur += 24 * 60
        elif len(tok) == 2:
            minute = int(tok)
            if minute >= 60:
                continue
            if prev is None:
                continue
            base_hour = (prev // 60) % 24
            base_day = prev // (24 * 60)
            cur = base_day * 24 * 60 + base_hour * 60 + minute
            while cur <= prev:
                cur += 60
        else:
            continue

        abs_minutes.append(cur)
        prev = cur

    return abs_minutes


def nearest_departure_to_target(times_abs: Sequence[int], target_minute: int) -> Optional[int]:
    if not times_abs:
        return None
    return min(times_abs, key=lambda t: abs(t - target_minute))


def arrival_after_departure(arrivals_abs: Sequence[int], dep_abs: int) -> Optional[int]:
    candidates = [t for t in arrivals_abs if t >= dep_abs and t - dep_abs <= 180]
    if not candidates:
        return None
    return min(candidates)


def best_aligned_arrival(dest_points: Sequence[TimePoint], dep_point: TimePoint) -> Optional[Tuple[TimePoint, float]]:
    candidates = [tp for tp in dest_points if tp.minutes_abs >= dep_point.minutes_abs and tp.minutes_abs - dep_point.minutes_abs <= 180]
    if not candidates:
        return None

    chosen = min(
        candidates,
        key=lambda tp: (
            abs(tp.x0 - dep_point.x0),
            tp.minutes_abs - dep_point.minutes_abs,
            tp.minutes_abs,
        ),
    )
    return chosen, abs(chosen.x0 - dep_point.x0)


def compute_candidates_for_page(
    rows: Sequence[ParsedRow],
    line: str,
    source_pdf: Path,
    page_no: int,
    destination_key: str,
    target_departure_minute: int,
) -> List[Candidate]:
    out: List[Candidate] = []
    if not rows:
        return out

    dest_indexes = [i for i, r in enumerate(rows) if normalize_key(r.station) == destination_key]
    if not dest_indexes:
        return out

    for dest_idx in dest_indexes:
        dest_row = rows[dest_idx]
        for i in range(dest_idx):
            src_row = rows[i]
            if normalize_key(src_row.station) == destination_key:
                continue

            best_for_row: Optional[Candidate] = None
            stops_to_destination = dest_idx - i
            min_plausible = max(2, stops_to_destination * 2)

            for dep_point in src_row.time_points:
                aligned = best_aligned_arrival(dest_row.time_points, dep_point)
                if aligned is None:
                    continue
                arr_point, x_delta = aligned
                minutes = arr_point.minutes_abs - dep_point.minutes_abs

                if minutes < min_plausible or minutes > 180:
                    continue

                candidate = Candidate(
                    station=src_row.station,
                    line=line,
                    minutes=minutes,
                    departure_abs=dep_point.minutes_abs,
                    arrival_abs=arr_point.minutes_abs,
                    x_delta=x_delta,
                    source_pdf=source_pdf.name,
                    page_no=page_no,
                )

                if best_for_row is None:
                    best_for_row = candidate
                    continue

                current_score = (
                    round(candidate.x_delta, 3),
                    abs(candidate.departure_abs - target_departure_minute),
                    candidate.minutes,
                    candidate.page_no,
                )
                best_score = (
                    round(best_for_row.x_delta, 3),
                    abs(best_for_row.departure_abs - target_departure_minute),
                    best_for_row.minutes,
                    best_for_row.page_no,
                )
                if current_score < best_score:
                    best_for_row = candidate

            if best_for_row is not None:
                out.append(best_for_row)

    return out


def parse_pdf_candidates(
    pdf_path: Path,
    line: str,
    destination: str,
    target_departure_minute: int,
) -> List[Candidate]:
    try:
        import pdfplumber
    except Exception as exc:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it in your active environment."
        ) from exc

    destination_key = normalize_key(destination)
    all_candidates: List[Candidate] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if destination_key not in normalize_key(text):
                continue

            parsed_rows: List[ParsedRow] = []
            for row_tokens in extract_positioned_rows_from_page(page):
                parsed = parse_row_tokens(row_tokens)
                if not parsed:
                    continue
                parsed_rows.append(parsed)

            all_candidates.extend(
                compute_candidates_for_page(
                    rows=parsed_rows,
                    line=line,
                    source_pdf=pdf_path,
                    page_no=page_no,
                    destination_key=destination_key,
                    target_departure_minute=target_departure_minute,
                )
            )

    return all_candidates


def choose_best_per_station_line(candidates: Sequence[Candidate]) -> Dict[Tuple[str, str], Candidate]:
    grouped: Dict[Tuple[str, str], List[Candidate]] = {}
    for c in candidates:
        key = (c.station, c.line)
        grouped.setdefault(key, []).append(c)

    chosen: Dict[Tuple[str, str], Candidate] = {}
    for key, items in grouped.items():
        # Prefer same-column alignment, then nearest departure to 08:00,
        # then shorter travel as a tiebreaker.
        items_sorted = sorted(
            items,
            key=lambda c: (
                round(c.x_delta, 3),
                abs(c.departure_abs - 8 * 60),
                c.minutes,
                c.page_no,
            ),
        )
        chosen[key] = items_sorted[0]

    return chosen


def build_station_name_lookup(db: StationDatabase) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for station in db.get_all_stations():
        name = str(station.get("name") or "").strip()
        if not name:
            continue
        lookup[normalize_key(name)] = name
    return lookup


def resolve_station_name(parsed_name: str, existing_lookup: Dict[str, str]) -> str:
    key = normalize_key(parsed_name)
    return existing_lookup.get(key, normalize_space(parsed_name))


def write_debug_csvs(
    all_candidates: Sequence[Candidate],
    chosen: Dict[Tuple[str, str], Candidate],
    output_dir: Path,
    destination: str,
) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    dest_slug = slugify_for_filename(destination)
    raw_path = output_dir / f"{dest_slug}_parse_candidates_{stamp}.csv"
    by_line_path = output_dir / f"{dest_slug}_times_by_line_{stamp}.csv"

    with raw_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Line", "Minutes", "DepartureAbs", "ArrivalAbs", "PDF", "Page"])
        for c in all_candidates:
            writer.writerow([c.station, c.line, c.minutes, c.departure_abs, c.arrival_abs, c.source_pdf, c.page_no])

    line_columns = sorted({line for _, line in chosen.keys()})
    stations = sorted({station for station, _ in chosen.keys()})
    matrix: Dict[Tuple[str, str], int] = {(station, line): c.minutes for (station, line), c in chosen.items()}

    with by_line_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", *line_columns])
        for station in stations:
            row = [station]
            for line in line_columns:
                val = matrix.get((station, line), "")
                row.append(val)
            writer.writerow(row)

    return raw_path, by_line_path


def minutes_from_hhmm(hhmm: str) -> int:
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", hhmm.strip())
    if not m:
        raise ValueError(f"Invalid HH:MM time: {hhmm}")
    hour = int(m.group(1))
    minute = int(m.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid HH:MM time: {hhmm}")
    return hour * 60 + minute


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill station travel minutes to destination from timetable PDFs"
    )
    parser.add_argument("--destination", default="Skoyen", help="Destination station name")
    parser.add_argument("--column", default="TO_SKOYEN_MIN", help="Compatibility flag (DB uses to_skoyen_min)")
    parser.add_argument("--stations-sheet", default="Stations", help="Google Sheet tab name for stations")

    parser.add_argument("--pdf-dir", default="rutetabeller tog", help="Local directory with timetable PDFs")
    parser.add_argument("--pdf-url", action="append", default=[], help="PDF URL or local PDF path")
    parser.add_argument("--pdf-list-file", default=None, help="Text file with one PDF URL/path per line")

    parser.add_argument(
        "--line-pdf",
        action="append",
        default=[],
        help="Explicit line mapping: LINE=PDF_URL_OR_PATH (use LINE1+LINE2=... for shared PDF)",
    )
    parser.add_argument(
        "--line-pdf-list-file",
        default=None,
        help="Text file with one LINE=PDF_URL_OR_PATH mapping per line",
    )

    parser.add_argument("--per-line", action="store_true", help="Write per-line rows to station_travel table")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print only; do not write DB")
    parser.add_argument("--no-sync", action="store_true", help="Do not sync Stations sheet after DB write")

    parser.add_argument("--target-time", default="08:00", help="Target departure time HH:MM")
    parser.add_argument("--max-candidates", type=int, default=0, help="Optional cap for debug/testing")
    parser.add_argument("--output-dir", default="tmp", help="Directory for diagnostic CSV outputs")
    parser.add_argument(
        "--allow-new-stations",
        action="store_true",
        help="Allow writing parsed station names that are not already present in stations DB",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_departure = minutes_from_hhmm(args.target_time)

    fallback_writes_enabled = destination_writes_skoyen_fallback(args.destination)

    if args.column.strip().upper() != "TO_SKOYEN_MIN" and fallback_writes_enabled:
        print(
            f"WARN: --column '{args.column}' requested; DB schema stores station fallback in TO_SKOYEN_MIN."
        )
    elif not fallback_writes_enabled:
        print(
            "INFO: destination is not Skoyen; skipping stations.to_skoyen_min fallback updates "
            "and writing only station_travel rows for this destination."
        )

    sources = resolve_sources(args)
    if not sources:
        print("No valid PDF sources found. Provide --pdf-url/--pdf-list-file or ensure --pdf-dir has PDFs.")
        return 1

    print(f"Resolved PDF sources: {len(sources)}")
    for line_ids, path in sources:
        print(f"  - {path} :: lines={','.join(line_ids) if line_ids else '(derived none)'}")

    all_candidates: List[Candidate] = []
    for line_ids, pdf_path in sources:
        effective_lines = line_ids or parse_line_ids_from_stem(pdf_path.stem)
        if not effective_lines:
            print(f"WARN: no line IDs inferred for {pdf_path.name}; skipping")
            continue
        for line in effective_lines:
            try:
                parsed = parse_pdf_candidates(
                    pdf_path=pdf_path,
                    line=line,
                    destination=args.destination,
                    target_departure_minute=target_departure,
                )
            except Exception as exc:
                print(f"WARN: failed parsing {pdf_path.name} for {line}: {exc}")
                continue
            all_candidates.extend(parsed)

    if args.max_candidates > 0:
        all_candidates = all_candidates[: args.max_candidates]

    if not all_candidates:
        print("No station travel candidates parsed from PDFs.")
        return 2

    chosen = choose_best_per_station_line(all_candidates)
    raw_path, by_line_path = write_debug_csvs(
        all_candidates,
        chosen,
        Path(args.output_dir),
        args.destination,
    )

    print(f"Candidates parsed: {len(all_candidates)}")
    print(f"Station+line chosen: {len(chosen)}")
    print(f"Debug CSV: {raw_path}")
    print(f"Line matrix CSV: {by_line_path}")

    db = StationDatabase()
    existing_lookup = build_station_name_lookup(db)

    unresolved_names = 0
    skipped_unknown_stations = 0
    written_station_lines = 0

    # Upsert line-level and collect aggregate fallback per station.
    per_station_minutes: Dict[str, List[int]] = {}

    for (parsed_station, line), cand in sorted(chosen.items()):
        station_name = resolve_station_name(parsed_station, existing_lookup)
        if normalize_key(station_name) not in existing_lookup:
            unresolved_names += 1
            if not args.allow_new_stations:
                skipped_unknown_stations += 1
                continue

        if args.dry_run:
            print(f"DRYRUN line={line:<5} station={station_name:<24} minutes={cand.minutes:>3}")
            per_station_minutes.setdefault(station_name, []).append(cand.minutes)
            continue

        station_id = db.upsert_station(name=station_name)
        per_station_minutes.setdefault(station_name, []).append(cand.minutes)

        if args.per_line:
            station_line_id = db.upsert_station_line(station_id=station_id, line=line)
            db.upsert_station_travel(
                station_line_id=station_line_id,
                destination=args.destination,
                minutes=cand.minutes,
            )
            written_station_lines += 1

    # Station fallback: keep writing only for Skoyen because schema has to_skoyen_min.
    if not args.dry_run and fallback_writes_enabled:
        for station_name, values in per_station_minutes.items():
            if not values:
                continue
            db.upsert_station(name=station_name, to_skoyen_min=min(values))

    print(f"Unresolved station-name matches (new names): {unresolved_names}")
    if skipped_unknown_stations:
        print(f"Skipped unknown stations (use --allow-new-stations to include): {skipped_unknown_stations}")
    if not args.dry_run:
        print(f"DB writes: station_line rows touched={written_station_lines}")
        print(
            f"DB totals now: stations={db.count_stations()} station_lines={db.count_station_lines()} "
            f"station_travel={db.count_station_travel()}"
        )

    if not args.dry_run and not args.no_sync:
        ok = sync_stations_to_sheet(sheet_name=args.stations_sheet, destination=args.destination)
        if not ok:
            print("Station sheet sync failed.")
            return 3

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
