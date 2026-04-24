#!/usr/bin/env python3
"""Extract CSV metrics from baseline profiles with pread2-per-table overlay.

Purpose:
- Use all metrics from `--baseline-dir` profiles.
- Override only `pread2_per_table_threads`, `pread2_per_table_bytes`,
  and `pread2_per_table_time` from matching profiles in `--pread2-dir`.

This is useful when parquet decrypt/decompress timings are meaningful in one
profile set (baseline), while pread2 per-table metrics are emitted in another.

Usage:
python tools/utils/extract_profiles_to_csv_mixed_pread2.py \
  --baseline-dir /data/dpk/duckdb-dpk/tpch_eval_baseline_profiles \
  --pread2-dir /data/dpk/duckdb-dpk/tpch_better_string_profiling \
  --output-csv /tmp/tpch_eval_with_pread2_overlay.csv
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
import re
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import extract_profiles_to_csv as base_extract


# Keep sort behavior aligned with the base script (`query10` after `query9`).
_QUERY_ID_NUMERIC_RE = re.compile(r"(\d+)")

# Only these keys are overlaid from --pread2-dir onto --baseline-dir profiles.
_PREAD2_OVERLAY_KEYS: Tuple[str, str, str] = (
    "pread2_per_table_threads",
    "pread2_per_table_bytes",
    "pread2_per_table_time",
)

_BYTES_PER_MIB = 1024.0 * 1024.0


def _sorted_json_paths(input_dir: Path) -> List[Path]:
    """Return JSON paths sorted by first numeric token then stem."""

    def sort_key(path: Path) -> tuple:
        match = _QUERY_ID_NUMERIC_RE.search(path.stem)
        numeric = int(match.group(1)) if match else 10**18
        return (numeric, path.stem)

    return sorted(
        (path for path in input_dir.iterdir() if path.is_file() and path.suffix.lower() == ".json"),
        key=sort_key,
    )


def _build_query_path_index(input_dir: Path) -> Dict[str, Path]:
    """Map query id (filename stem) to JSON path with duplicate detection."""
    index: Dict[str, Path] = {}
    for path in _sorted_json_paths(input_dir):
        query_id = path.stem
        existing = index.get(query_id)
        if existing is not None:
            raise SystemExit(
                f"Duplicate query id '{query_id}' in {input_dir}: '{existing.name}' and '{path.name}'"
            )
        index[query_id] = path
    return index


def _overlay_pread2_metrics(
    baseline_profile: Dict[str, object], pread2_profile: Dict[str, object], query_id: str
) -> None:
    """Overlay only pread2-per-table keys from pread2 profile into baseline profile."""
    missing_keys: List[str] = []
    for key in _PREAD2_OVERLAY_KEYS:
        value = pread2_profile.get(key)
        if value is None:
            missing_keys.append(key)
            continue
        baseline_profile[key] = value

    # Warn when overlay source is incomplete; this directly affects throughput derivation.
    if missing_keys:
        print(
            f"[WARN] query={query_id}: missing overlay keys in pread2 profile: {', '.join(missing_keys)}",
            file=sys.stderr,
        )


def _sum_positive_values(metric_map: Dict[str, float]) -> float:
    """Sum only positive values from a parsed per-table metric map."""
    return sum(value for value in metric_map.values() if value > 0)


def _estimate_effective_concurrency_from_pread2(profile: Dict[str, object], query_id: str) -> Optional[float]:
    """Estimate query-level effective concurrency from pread2 per-table metrics.

    Equation:
      C_eff = sum_t(T_t) / sum_t(T_t / N_t)
    where:
    - T_t = pread2_per_table_time for table t (thread-time ns)
    - N_t = pread2_per_table_threads for table t
    """
    time_map = base_extract._parse_table_metric_map(profile.get("pread2_per_table_time"))
    if not time_map:
        return None
    thread_map = base_extract._parse_table_metric_map(profile.get("pread2_per_table_threads"))

    total_thread_time_ns = 0.0
    total_wall_estimate_ns = 0.0
    for table_name, time_ns in time_map.items():
        if time_ns <= 0:
            continue

        # Fall back to one thread when table-specific thread count is missing.
        thread_count = thread_map.get(table_name, 1.0)
        if thread_count <= 0:
            print(
                f"[WARN] query={query_id}: invalid thread_count={thread_count} for {table_name}; "
                "falling back to 1.0 for effective-concurrency estimate",
                file=sys.stderr,
            )
            thread_count = 1.0

        total_thread_time_ns += time_ns
        total_wall_estimate_ns += time_ns / thread_count

    if total_thread_time_ns <= 0 or total_wall_estimate_ns <= 0:
        return None
    return total_thread_time_ns / total_wall_estimate_ns


def _compute_stage_throughput_pair_mib_s(
    total_bytes: float,
    stage_time_seconds: Optional[float],
    stage_count: Optional[int],
    effective_concurrency: Optional[float],
    query_id: str,
    stage_name: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Compute aggregate/per-thread throughput for one parquet stage.

    `stage_time_seconds` is aggregate thread-time (not wall time):
      per_thread_throughput = total_bytes / stage_time_seconds
      aggregate_throughput = per_thread_throughput * C_eff
    """
    if stage_time_seconds is None:
        return None, None
    if total_bytes <= 0:
        return None, None

    # stage_time==0 is valid when stage_count==0 (inactive stage): keep empty.
    if stage_time_seconds < 0:
        print(
            f"[WARN] query={query_id}: parquet_{stage_name}_time is negative ({stage_time_seconds}); "
            "cannot compute throughput",
            file=sys.stderr,
        )
        return None, None
    if stage_time_seconds == 0:
        if stage_count is not None and stage_count > 0:
            print(
                f"[WARN] query={query_id}: parquet_{stage_name}_time is 0 with "
                f"parquet_{stage_name}_count={stage_count}; cannot compute throughput",
                file=sys.stderr,
            )
        return None, None

    # Per-thread throughput uses aggregate thread-time directly.
    per_thread_mib_s = (total_bytes / stage_time_seconds) / _BYTES_PER_MIB

    if effective_concurrency is None or effective_concurrency <= 0:
        print(
            f"[WARN] query={query_id}: missing/invalid effective concurrency ({effective_concurrency}); "
            f"parquet_{stage_name}_aggregate_throughput left empty",
            file=sys.stderr,
        )
        return None, per_thread_mib_s

    aggregate_mib_s = per_thread_mib_s * effective_concurrency
    return aggregate_mib_s, per_thread_mib_s


def _recompute_parquet_throughputs(
    profile: Dict[str, object], query_id: str
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Recompute throughput columns using thread-time semantics + inferred wall time."""
    bytes_map = base_extract._parse_table_metric_map(profile.get("pread2_per_table_bytes"))
    total_bytes = _sum_positive_values(bytes_map)
    if total_bytes <= 0:
        print(
            f"[WARN] query={query_id}: missing/invalid pread2_per_table_bytes; "
            "parquet throughput columns left empty",
            file=sys.stderr,
        )
        return None, None, None, None

    effective_concurrency = _estimate_effective_concurrency_from_pread2(profile, query_id)

    decrypt_agg_mib_s, decrypt_per_thread_mib_s = _compute_stage_throughput_pair_mib_s(
        total_bytes=total_bytes,
        stage_time_seconds=base_extract._coerce_float(profile.get("parquet_decryption_time")),
        stage_count=base_extract._coerce_int(profile.get("parquet_decryption_count")),
        effective_concurrency=effective_concurrency,
        query_id=query_id,
        stage_name="decryption",
    )
    decompress_agg_mib_s, decompress_per_thread_mib_s = _compute_stage_throughput_pair_mib_s(
        total_bytes=total_bytes,
        stage_time_seconds=base_extract._coerce_float(profile.get("parquet_decompression_time")),
        stage_count=base_extract._coerce_int(profile.get("parquet_decompression_count")),
        effective_concurrency=effective_concurrency,
        query_id=query_id,
        stage_name="decompression",
    )
    return (
        decrypt_agg_mib_s,
        decrypt_per_thread_mib_s,
        decompress_agg_mib_s,
        decompress_per_thread_mib_s,
    )


def iter_merged_profile_rows(baseline_dir: Path, pread2_dir: Path) -> Iterable[base_extract.ProfileRow]:
    """Yield extracted CSV rows after overlaying pread2-per-table metrics by query id."""
    pread2_index = _build_query_path_index(pread2_dir)
    baseline_paths = _sorted_json_paths(baseline_dir)

    missing_pread2_profiles: List[str] = []
    for baseline_path in baseline_paths:
        query_id = baseline_path.stem
        baseline_profile = base_extract.load_profile_json(baseline_path)

        pread2_path = pread2_index.get(query_id)
        if pread2_path is None:
            # Keep baseline row extraction working, but warn since requested overlay is absent.
            missing_pread2_profiles.append(query_id)
        else:
            pread2_profile = base_extract.load_profile_json(pread2_path)
            _overlay_pread2_metrics(baseline_profile, pread2_profile, query_id)

        (
            parquet_decryption_throughput_mb_s,
            parquet_decryption_thread_throughput_mb_s,
            parquet_decompression_throughput_mb_s,
            parquet_decompression_thread_throughput_mb_s,
        ) = _recompute_parquet_throughputs(baseline_profile, query_id)

        extracted_row = base_extract.extract_row(baseline_profile, query_id=query_id)

        # Original behavior kept for reference:
        # yield base_extract.extract_row(baseline_profile, query_id=query_id)
        #
        # Updated behavior:
        # Override throughput fields with corrected equations where parquet_*_time
        # is treated as aggregate thread-time (not wall time).
        yield replace(
            extracted_row,
            parquet_decryption_throughput_mb_s=parquet_decryption_throughput_mb_s,
            parquet_decryption_thread_throughput_mb_s=parquet_decryption_thread_throughput_mb_s,
            parquet_decompression_throughput_mb_s=parquet_decompression_throughput_mb_s,
            parquet_decompression_thread_throughput_mb_s=parquet_decompression_thread_throughput_mb_s,
        )

    if missing_pread2_profiles:
        print(
            "[WARN] Missing matching pread2 profiles for query ids: "
            + ", ".join(missing_pread2_profiles),
            file=sys.stderr,
        )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse CLI args for baseline/pread2 merge extraction."""
    parser = argparse.ArgumentParser(
        description=(
            "Extract CSV metrics from baseline profiles while overriding only "
            "pread2_per_table_* keys from a second profile directory."
        )
    )
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        required=True,
        help="Directory providing all baseline JSON profiling metrics",
    )
    parser.add_argument(
        "--pread2-dir",
        type=Path,
        required=True,
        help="Directory providing pread2_per_table_* metrics to overlay",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help=(
            "Path to write the output CSV "
            "(default: <baseline-dir>/<baseline-dir-name>_with_<pread2-dir-name>.csv)"
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Run merged extraction and write the summary CSV."""
    args = parse_args(argv)
    baseline_dir: Path = args.baseline_dir
    pread2_dir: Path = args.pread2_dir

    if not baseline_dir.exists() or not baseline_dir.is_dir():
        raise SystemExit(f"Baseline directory does not exist or is not a directory: {baseline_dir}")
    if not pread2_dir.exists() or not pread2_dir.is_dir():
        raise SystemExit(f"Pread2 directory does not exist or is not a directory: {pread2_dir}")

    output_csv: Path = args.output_csv or (baseline_dir / f"{baseline_dir.name}_with_{pread2_dir.name}.csv")

    rows = list(iter_merged_profile_rows(baseline_dir=baseline_dir, pread2_dir=pread2_dir))
    base_extract.write_csv(rows, output_csv)
    print(
        f"Wrote {len(rows)} rows to {output_csv} "
        f"(baseline={baseline_dir}, pread2_overlay={pread2_dir})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
