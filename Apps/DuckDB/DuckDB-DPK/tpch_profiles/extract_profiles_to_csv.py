#!/usr/bin/env python3
"""Extract key metrics from DuckDB JSON profiling outputs into a CSV.

This script scans a directory (default: this file's directory) for JSON profiling
files (e.g., query1.json) and writes a single CSV where each row corresponds to
one JSON file.

Fields extracted (top-level JSON keys):
- query_id (derived from filename stem, e.g., query1)
- cpu_time
- query_name
- latency
- rows_returned
- total_bytes_read
- total_bytes_written
- parquet_decompression_time
- parquet_decompression_count
- parquet_decryption_time
- parquet_decryption_count

Note:
The user request listed `total_bytes_read` twice; this script includes both
`total_bytes_read` and `total_bytes_written` since both commonly exist in the
DuckDB profiler output.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class ProfileRow:
    # query_id is taken from the filename (stem), e.g., query1
    query_id: str
    cpu_time: Optional[float]
    query_name: Optional[str]
    latency: Optional[float]
    rows_returned: Optional[int]
    total_bytes_read: Optional[int]
    total_bytes_written: Optional[int]
    parquet_decompression_time: Optional[float]
    parquet_decompression_count: Optional[int]
    parquet_decryption_time: Optional[float]
    parquet_decryption_count: Optional[int]


CSV_FIELDNAMES: List[str] = [
    "query_id",
    "cpu_time",
    "query_name",
    "latency",
    "rows_returned",
    "total_bytes_read",
    "total_bytes_written",
    "parquet_decompression_time",
    "parquet_decompression_count",
    "parquet_decryption_time",
    "parquet_decryption_count",
]


_QUERY_ID_NUMERIC_RE = re.compile(r"(\d+)")


def _normalize_query_name(query_name: Any) -> Optional[str]:
    """Normalize query_name for CSV output.

    DuckDB's `query_name` field often contains embedded newlines and indentation.
    While CSV supports newlines inside quoted fields, it makes the output harder
    to inspect with line-based tools. This function collapses whitespace so each
    query is stored on a single physical line.
    """

    if query_name is None:
        return None
    if not isinstance(query_name, str):
        query_name = str(query_name)

    # Replace all whitespace sequences (including newlines) with a single space.
    normalized = " ".join(query_name.split())
    return normalized


def _coerce_int(value: Any) -> Optional[int]:
    # DuckDB profiler JSON sometimes contains numeric values as ints/floats.
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return int(stripped)
        except ValueError:
            # Some fields might be formatted strings in other profiler variants.
            return None
    return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def load_profile_json(path: Path) -> Dict[str, Any]:
    # Loads a single DuckDB JSON profiling output.
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def extract_row(profile: Dict[str, Any], query_id: str) -> ProfileRow:
    # Extracts the requested top-level fields into a typed row.
    return ProfileRow(
        query_id=query_id,
        cpu_time=_coerce_float(profile.get("cpu_time")),
        query_name=_normalize_query_name(profile.get("query_name")),
        latency=_coerce_float(profile.get("latency")),
        rows_returned=_coerce_int(profile.get("rows_returned")),
        total_bytes_read=_coerce_int(profile.get("total_bytes_read")),
        total_bytes_written=_coerce_int(profile.get("total_bytes_written")),
        parquet_decompression_time=_coerce_float(profile.get("parquet_decompression_time")),
        parquet_decompression_count=_coerce_int(profile.get("parquet_decompression_count")),
        parquet_decryption_time=_coerce_float(profile.get("parquet_decryption_time")),
        parquet_decryption_count=_coerce_int(profile.get("parquet_decryption_count")),
    )


def iter_profile_rows(input_dir: Path) -> Iterable[ProfileRow]:
    # Reads all *.json files under input_dir and yields extracted rows.

    def sort_key(path: Path) -> tuple:
        # Sort first by the first number found in the stem (e.g., query10 -> 10),
        # then by stem as a tie-breaker.
        match = _QUERY_ID_NUMERIC_RE.search(path.stem)
        numeric = int(match.group(1)) if match else 10**18
        return (numeric, path.stem)

    json_paths = sorted(
        (p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"),
        key=sort_key,
    )
    for path in json_paths:
        query_id = path.stem
        profile = load_profile_json(path)
        yield extract_row(profile, query_id=query_id)


def write_csv(rows: Iterable[ProfileRow], output_csv: Path) -> None:
    # Writes rows to a CSV with a stable header order.
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "query_id": row.query_id,
                    "cpu_time": row.cpu_time,
                    "query_name": row.query_name,
                    "latency": row.latency,
                    "rows_returned": row.rows_returned,
                    "total_bytes_read": row.total_bytes_read,
                    "total_bytes_written": row.total_bytes_written,
                    "parquet_decompression_time": row.parquet_decompression_time,
                    "parquet_decompression_count": row.parquet_decompression_count,
                    "parquet_decryption_time": row.parquet_decryption_time,
                    "parquet_decryption_count": row.parquet_decryption_count,
                }
            )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    default_dir = Path(__file__).resolve().parent
    default_out = default_dir / "tpch_profiles_summary.csv"

    parser = argparse.ArgumentParser(
        description="Extract key fields from DuckDB JSON profiling files into a single CSV."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=default_dir,
        help="Directory containing DuckDB JSON profiling files (default: script directory)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=default_out,
        help="Path to write the output CSV (default: <input-dir>/tpch_profiles_summary.csv)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    input_dir: Path = args.input_dir
    output_csv: Path = args.output_csv

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")

    rows = list(iter_profile_rows(input_dir))
    write_csv(rows, output_csv)
    print(f"Wrote {len(rows)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
