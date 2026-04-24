#!/usr/bin/env python3
"""Extract key metrics from DuckDB JSON profiling outputs into a CSV.

Usage: python extract_profiles_to_csv.py [--input-dir INPUT_DIR] [--output-csv OUTPUT_CSV]
If --output-csv is not provided, the output defaults to <input-dir>/tpch_profiles_summary.csv.

This script scans a directory (default: this file's directory) for JSON profiling
files (e.g., query1.json) and writes a single CSV where each row corresponds to
one JSON file.

Fields extracted (top-level JSON keys):
- query_id (derived from filename stem, e.g., query1)
- cpu_time
- cumulative_optimizer_timing
- query_parsing_optim_time
- string_predicate_cpu_time
- string_pred_wallclock_time
- query_name
- latency
- rows_returned
- total_bytes_read
- total_bytes_written
- parquet_decompression_time
- parquet_decompression_count
- parquet_decryption_time
- parquet_decryption_count
- table_scan_string_constant_comparison_time
- table_scan_string_constant_comparison_count
- table_scan_string_like_operator_time
- table_scan_string_like_operator_count
- string_predicate_read_io_time
- table_scan_string_constant_comparison_read_io_time
- table_scan_string_like_operator_read_io_time
- dds_pread_time
- dds_pread2_time
- pread2_per_table_threads
- pread2_per_table_bytes
- pread2_per_table_time
- dds_pread_total_throughput
- dds_pread_thread_throughput
- dds_pread_p99_latency
- dds_pread_p50_latency
- dds_pread_max_latency
- dds_pread_min_latency
- dds_pread_latency
- dds_pread_call_count
- pread2_throughput_mb_s
- parquet_decryption_throughput_mb_s
- parquet_decryption_thread_throughput_mb_s
- parquet_decompression_throughput_mb_s
- parquet_decompression_thread_throughput_mb_s
- pread_total_throughput
- pread_thread_throughput
- pread_p99_latency
- pread_p50_latency
- pread_max_latency
- pread_min_latency
- pread_latency
- pread_call_count

Compatibility:
- Newer profiling JSONs use `dds_pread_*` keys.
- Older/orig profiling JSONs use `pread_*` keys.
    These are different metric families; this script extracts both sets when
    present and preserves their original key names in the CSV.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class ProfileRow:
    # query_id is taken from the filename (stem), e.g., query1
    query_id: str
    cpu_time: Optional[float]
    # Total time spent in optimizer pipeline (seconds).
    cumulative_optimizer_timing: Optional[float]
    # Combined planning/optimizer phase timing:
    #   planner + all_optimizers
    query_parsing_optim_time: Optional[float]
    # Aggregate CPU time for the requested string predicate categories, computed
    # from direct profiler fields:
    # - table_scan_string_constant_comparison_time
    # - table_scan_string_like_operator_time
    #
    # If direct fields are absent (older profiling JSONs), this value is None.
    string_predicate_cpu_time: Optional[float]
    # Rough wallclock estimate for string predicate evaluation:
    #   (like_time + const_cmp_time) / avg_concurrency
    # where avg_concurrency is bytes-weighted using inferred predicate tables.
    string_pred_wallclock_time: Optional[float]
    # Aggregate read-I/O time for the requested string predicate categories.
    # Computed from direct profiler fields:
    # - table_scan_string_constant_comparison_read_io_time
    # - table_scan_string_like_operator_read_io_time
    #
    # If direct fields are absent (older profiling JSONs), this value is None.
    string_predicate_read_io_time: Optional[float]
    # Direct profiler metrics emitted from DuckDB for table-scan predicate CPU.
    table_scan_string_constant_comparison_time: Optional[float]
    table_scan_string_constant_comparison_count: Optional[int]
    table_scan_string_like_operator_time: Optional[float]
    table_scan_string_like_operator_count: Optional[int]
    # Direct profiler metrics emitted from DuckDB for read-I/O time attributed
    # to table-scan string predicate processing.
    table_scan_string_constant_comparison_read_io_time: Optional[float]
    table_scan_string_like_operator_read_io_time: Optional[float]
    query_name: Optional[str]
    latency: Optional[float]
    rows_returned: Optional[int]
    total_bytes_read: Optional[int]
    total_bytes_written: Optional[int]
    parquet_decompression_time: Optional[float]
    parquet_decompression_count: Optional[int]
    parquet_decryption_time: Optional[float]
    parquet_decryption_count: Optional[int]
    # DDS pread total elapsed I/O time across all pread calls in seconds.
    dds_pread_time: Optional[float]
    # DDS pread2 total elapsed I/O time across all pread2 calls in seconds.
    dds_pread2_time: Optional[float]
    # Per-table pread2 aggregate metrics in "table=value,table=value" form.
    pread2_per_table_threads: Optional[str]
    pread2_per_table_bytes: Optional[str]
    pread2_per_table_time: Optional[str]
    # DDS pread latency stats and call count (top-level keys in profiler JSON)
    dds_pread_total_throughput: Optional[float]
    dds_pread_thread_throughput: Optional[float]
    dds_pread_p99_latency: Optional[float]
    dds_pread_p50_latency: Optional[float]
    dds_pread_max_latency: Optional[float]
    dds_pread_min_latency: Optional[float]
    dds_pread_latency: Optional[float]
    dds_pread_call_count: Optional[int]
    # Estimated pread2 throughput in MB/s derived from per-table pread2 metrics:
    #   total_bytes / sum(table_time_ns / table_threads)
    pread2_throughput_mb_s: Optional[float]
    # Estimated parquet decrypt/decompress throughput in MB/s:
    #   aggregate = total_bytes / stage_time
    #   per-thread = aggregate / bytes_weighted_avg_threads
    # where bytes_weighted_avg_threads is computed from pread2_per_table_*.
    parquet_decryption_throughput_mb_s: Optional[float]
    parquet_decryption_thread_throughput_mb_s: Optional[float]
    parquet_decompression_throughput_mb_s: Optional[float]
    parquet_decompression_thread_throughput_mb_s: Optional[float]
    # Legacy/orig pread metrics (without dds_ prefix)
    pread_total_throughput: Optional[float]
    pread_thread_throughput: Optional[float]
    pread_p99_latency: Optional[float]
    pread_p50_latency: Optional[float]
    pread_max_latency: Optional[float]
    pread_min_latency: Optional[float]
    pread_latency: Optional[float]
    pread_call_count: Optional[int]


CSV_FIELDNAMES: List[str] = [
    "query_id",
    "cpu_time",
    "cumulative_optimizer_timing",
    "query_parsing_optim_time",
    "string_predicate_cpu_time",
    "string_pred_wallclock_time",
    "string_predicate_read_io_time",
    "table_scan_string_constant_comparison_time",
    "table_scan_string_constant_comparison_count",
    "table_scan_string_like_operator_time",
    "table_scan_string_like_operator_count",
    "table_scan_string_constant_comparison_read_io_time",
    "table_scan_string_like_operator_read_io_time",
    "query_name",
    "latency",
    "rows_returned",
    "total_bytes_read",
    "total_bytes_written",
    "parquet_decompression_time",
    "parquet_decompression_count",
    "parquet_decryption_time",
    "parquet_decryption_count",
    "dds_pread_time",
    "dds_pread2_time",
    "pread2_per_table_threads",
    "pread2_per_table_bytes",
    "pread2_per_table_time",
    "dds_pread_total_throughput",
    "dds_pread_thread_throughput",
    "dds_pread_p99_latency",
    "dds_pread_p50_latency",
    "dds_pread_max_latency",
    "dds_pread_min_latency",
    "dds_pread_latency",
    "dds_pread_call_count",
    "pread2_throughput_mb_s",
    "parquet_decryption_throughput_mb_s",
    "parquet_decryption_thread_throughput_mb_s",
    "parquet_decompression_throughput_mb_s",
    "parquet_decompression_thread_throughput_mb_s",
    "pread_total_throughput",
    "pread_thread_throughput",
    "pread_p99_latency",
    "pread_p50_latency",
    "pread_max_latency",
    "pread_min_latency",
    "pread_latency",
    "pread_call_count",
]


_QUERY_ID_NUMERIC_RE = re.compile(r"(\d+)")
_STRING_FUNC_COL_RE = re.compile(
    r"(?i)\b(?:contains|prefix|suffix|starts_with|ends_with|regexp_matches|regexp_full_match)\s*\(\s*"
    r"([a-z_][a-z0-9_]*)\b"
)
_STRING_LIKE_OP_COL_RE = re.compile(r"(?i)\b([a-z_][a-z0-9_]*)\b\s*(?:!~~\*?|~~\*?)\s*'[^']*'")
_STRING_LIKE_KEYWORD_COL_RE = re.compile(
    r"(?i)\b([a-z_][a-z0-9_]*)\b\s+(?:not\s+)?i?like\s*'[^']*'"
)
_STRING_COMPARE_COL_RE = re.compile(
    r"(?i)\b([a-z_][a-z0-9_]*)\b\s*(?:=|!=|<>|<=|>=|<|>)\s*'[^']*'"
    r"(?!\s*::\s*(?:date|time|timestamp|timestamptz|interval)\b)"
)
_STRING_IN_LIST_COL_RE = re.compile(
    r"(?i)\b([a-z_][a-z0-9_]*)\b\s+(?:not\s+)?in\s*\(\s*'[^']*'(?:\s*,\s*'[^']*')*\s*\)"
)
_STRING_BETWEEN_COL_RE = re.compile(
    r"(?i)\b([a-z_][a-z0-9_]*)\b\s+(?:not\s+)?between\s*'[^']*'\s+and\s*'[^']*'"
)
_STRING_SUBSTR_IN_COL_RE = re.compile(
    r"(?i)(?:\"(?:substring|substr)\"|(?:substring|substr))\s*\(\s*\"?([a-z_][a-z0-9_]*)\"?[^)]*\)\s+"
    r"(?:not\s+)?in\s*\(\s*'[^']*'"
)
_COLUMN_PREFIX_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9]*)_[A-Za-z0-9_]+\b")

# TPCH column prefix hints (applied only if table metric keys match TPCH stems).
_TPCH_PREFIX_TO_TABLE = {
    "l": "lineitem",
    "o": "orders",
    "c": "customer",
    "p": "part",
    "ps": "partsupp",
    "s": "supplier",
    "n": "nation",
    "r": "region",
}


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


def _parse_table_metric_map(metric: Any) -> Dict[str, float]:
    """Parse 'table=value,table=value' strings into a numeric map."""
    if metric is None:
        return {}
    if not isinstance(metric, str):
        metric = str(metric)

    parsed: Dict[str, float] = {}
    for raw_entry in metric.split(","):
        entry = raw_entry.strip()
        if not entry or "=" not in entry:
            continue
        table_name, raw_value = entry.split("=", 1)
        table_name = table_name.strip()
        if not table_name:
            continue
        value = _coerce_float(raw_value.strip())
        if value is None:
            continue
        parsed[table_name] = value
    return parsed


def _table_stem(table_metric_key: str) -> str:
    """Return normalized table stem from metric key (e.g., orders.parquet -> orders)."""
    table_key = table_metric_key.strip().lower()
    if table_key.endswith(".parquet"):
        return table_key[: -len(".parquet")]
    if "." in table_key:
        return table_key.split(".", 1)[0]
    return table_key


def _iter_operator_nodes(node: Any) -> Iterable[Dict[str, Any]]:
    """Depth-first traversal over operator nodes in a profiling tree."""
    if not isinstance(node, dict):
        return
    yield node
    children = node.get("children")
    if not isinstance(children, list):
        return
    for child in children:
        if isinstance(child, dict):
            yield from _iter_operator_nodes(child)


def _extract_string_predicate_prefixes(value: Any) -> Set[str]:
    """Extract column prefixes from recognized string-predicate expressions."""
    prefixes: Set[str] = set()
    if value is None:
        return prefixes

    text = value if isinstance(value, str) else str(value)
    if not text:
        return prefixes

    # These patterns capture the column identifier that participates in a
    # string predicate. We intentionally avoid projection-derived hints to keep
    # table inference tied to actual filter expressions.
    for pattern in (
        _STRING_FUNC_COL_RE,
        _STRING_LIKE_OP_COL_RE,
        _STRING_LIKE_KEYWORD_COL_RE,
        _STRING_COMPARE_COL_RE,
        _STRING_IN_LIST_COL_RE,
        _STRING_BETWEEN_COL_RE,
        _STRING_SUBSTR_IN_COL_RE,
    ):
        for match in pattern.finditer(text):
            prefixes.update(_extract_column_prefixes(match.group(1)))

    return prefixes


def _value_contains_string_predicate(value: Any) -> bool:
    """Check whether a filter value contains supported string-predicate forms."""
    return bool(_extract_string_predicate_prefixes(value))


def _extract_column_prefixes(value: Any) -> Set[str]:
    """Extract column prefixes (e.g., o_orderkey -> o) from text/list values."""
    prefixes: Set[str] = set()
    if value is None:
        return prefixes

    values: List[str]
    if isinstance(value, list):
        values = [str(v) for v in value]
    else:
        values = [str(value)]

    for item in values:
        for match in _COLUMN_PREFIX_RE.finditer(item):
            prefix = match.group(1).lower()
            if prefix:
                prefixes.add(prefix)
    return prefixes


def _resolve_prefix_to_tables(prefix: str, table_metric_keys: Set[str]) -> Set[str]:
    """Resolve a column prefix to candidate table keys using TPCH+generic heuristics."""
    candidates: Set[str] = set()
    if not prefix:
        return candidates

    stem_to_key = {_table_stem(key): key for key in table_metric_keys}

    # TPCH-aware mapping when available.
    mapped_stem = _TPCH_PREFIX_TO_TABLE.get(prefix.lower())
    if mapped_stem and mapped_stem in stem_to_key:
        # Use explicit TPCH mapping as authoritative to avoid 'p' -> partsupp
        # over-matching in generic startswith logic.
        return {stem_to_key[mapped_stem]}

    # Prefer exact stem matches before fuzzy prefix matching.
    exact_match = stem_to_key.get(prefix.lower())
    if exact_match:
        return {exact_match}

    # Generic startswith match for non-TPCH names.
    for stem, key in stem_to_key.items():
        if stem.startswith(prefix.lower()):
            candidates.add(key)

    # If unresolved, accept a unique first-letter match.
    if not candidates and len(prefix) >= 1:
        first_letter_matches = [key for stem, key in stem_to_key.items() if stem.startswith(prefix[0].lower())]
        if len(first_letter_matches) == 1:
            candidates.add(first_letter_matches[0])

    return candidates


def _infer_string_predicate_tables(profile: Dict[str, Any], table_metric_keys: Set[str]) -> Set[str]:
    """Infer predicate-source tables by scanning READ_PARQUET operators with string filters."""
    inferred_tables: Set[str] = set()
    if not table_metric_keys:
        return inferred_tables

    roots = profile.get("children")
    if not isinstance(roots, list):
        return inferred_tables

    for root in roots:
        for node in _iter_operator_nodes(root):
            operator_name = str(node.get("operator_name", "")).strip().upper()
            if operator_name != "READ_PARQUET":
                continue

            extra_info = node.get("extra_info")
            if not isinstance(extra_info, dict):
                continue

            filter_values: List[Any] = []
            for key, value in extra_info.items():
                if "filter" in str(key).lower():
                    filter_values.append(value)
            if not filter_values:
                continue
            if not any(_value_contains_string_predicate(value) for value in filter_values):
                continue

            prefixes: Set[str] = set()
            for value in filter_values:
                prefixes.update(_extract_string_predicate_prefixes(value))

            resolved: Set[str] = set()
            for prefix in prefixes:
                resolved.update(_resolve_prefix_to_tables(prefix, table_metric_keys))

            if resolved:
                inferred_tables.update(resolved)
            elif len(table_metric_keys) == 1:
                # Defensive fallback for single-table scans with missing column hints.
                inferred_tables.update(table_metric_keys)

    return inferred_tables


def _compute_weighted_avg_concurrency(
    bytes_map: Dict[str, float], threads_map: Dict[str, float], selected_tables: Set[str]
) -> Optional[float]:
    """Compute bytes-weighted average concurrency for selected tables."""
    weighted_threads = 0.0
    total_bytes = 0.0
    for table in selected_tables:
        bytes_value = bytes_map.get(table)
        if bytes_value is None or bytes_value <= 0:
            continue

        # Fall back to one thread when per-table thread count is missing/invalid.
        thread_count = threads_map.get(table, 1.0)
        if thread_count <= 0:
            thread_count = 1.0

        weighted_threads += bytes_value * thread_count
        total_bytes += bytes_value

    if total_bytes <= 0:
        return None
    return weighted_threads / total_bytes


def _compute_string_pred_wallclock_time(
    profile: Dict[str, Any], query_id: str, string_predicate_cpu_time: Optional[float]
) -> Optional[float]:
    """Estimate string predicate wallclock time using inferred table concurrency."""
    if string_predicate_cpu_time is None:
        return None
    if string_predicate_cpu_time <= 0:
        return 0.0

    bytes_map = _parse_table_metric_map(profile.get("pread2_per_table_bytes"))
    threads_map = _parse_table_metric_map(profile.get("pread2_per_table_threads"))
    if not bytes_map:
        return None

    table_keys = set(bytes_map.keys())
    predicate_tables = _infer_string_predicate_tables(profile, table_keys)

    # Fallback to all scanned tables when predicate-source inference is ambiguous.
    selected_tables = predicate_tables if predicate_tables else table_keys
    if not predicate_tables:
        print(
            f"[WARN] query={query_id}: could not infer string predicate source tables from plan; "
            "falling back to all tables in pread2_per_table_* metrics",
            file=sys.stderr,
        )
    avg_concurrency = _compute_weighted_avg_concurrency(bytes_map, threads_map, selected_tables)
    if avg_concurrency is None or avg_concurrency <= 0:
        return None

    return string_predicate_cpu_time / avg_concurrency


def _compute_pread2_throughput_mb_s(profile: Dict[str, Any]) -> Optional[float]:
    """Estimate pread2 throughput (MB/s) from per-table bytes/time/thread metrics."""
    bytes_map = _parse_table_metric_map(profile.get("pread2_per_table_bytes"))
    time_map = _parse_table_metric_map(profile.get("pread2_per_table_time"))
    threads_map = _parse_table_metric_map(profile.get("pread2_per_table_threads"))

    if not bytes_map or not time_map:
        return None

    total_bytes = 0.0
    total_effective_time_ns = 0.0
    for table_name, bytes_value in bytes_map.items():
        time_ns = time_map.get(table_name)
        if time_ns is None or time_ns <= 0:
            continue

        # Fall back to one thread when the table-specific thread count is missing.
        thread_count = threads_map.get(table_name, 1.0)
        if thread_count <= 0:
            thread_count = 1.0

        total_bytes += bytes_value
        total_effective_time_ns += time_ns / thread_count

    if total_bytes <= 0 or total_effective_time_ns <= 0:
        return None

    bytes_per_second = total_bytes / (total_effective_time_ns / 1e9)
    mb_per_second = bytes_per_second / (1024.0 * 1024.0)
    return mb_per_second


def _compute_parquet_stage_throughputs_mb_s(
    profile: Dict[str, Any], query_id: str, stage_name: str, stage_time_seconds: Optional[float]
) -> Tuple[Optional[float], Optional[float]]:
    """Estimate aggregate/per-thread parquet stage throughput (MB/s).

    Uses `pread2_per_table_bytes` as the processed byte baseline and
    `pread2_per_table_threads` to estimate bytes-weighted average concurrency.
    """
    if stage_time_seconds is None:
        return None, None

    bytes_map = _parse_table_metric_map(profile.get("pread2_per_table_bytes"))
    if not bytes_map:
        return None, None

    total_bytes = sum(value for value in bytes_map.values() if value > 0)
    if total_bytes <= 0:
        return None, None

    # Defensive guard: throughput is undefined with non-positive stage time.
    if stage_time_seconds <= 0:
        print(
            f"[WARN] query={query_id}: cannot compute parquet_{stage_name}_throughput_mb_s "
            f"with non-positive stage_time_seconds={stage_time_seconds}",
            file=sys.stderr,
        )
        return None, None

    aggregate_mb_s = (total_bytes / stage_time_seconds) / (1024.0 * 1024.0)

    threads_map = _parse_table_metric_map(profile.get("pread2_per_table_threads"))
    avg_threads = _compute_weighted_avg_concurrency(bytes_map, threads_map, set(bytes_map.keys()))
    if avg_threads is None or avg_threads <= 0:
        return aggregate_mb_s, None

    per_thread_mb_s = aggregate_mb_s / avg_threads
    return aggregate_mb_s, per_thread_mb_s


def load_profile_json(path: Path) -> Dict[str, Any]:
    # Loads a single DuckDB JSON profiling output.
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def extract_row(profile: Dict[str, Any], query_id: str) -> ProfileRow:
    """Extract requested top-level fields into a typed row.

    Includes DDS pread latency statistics and call count when present in the
    profiler JSON.
    """
    table_scan_string_constant_comparison_time = _coerce_float(
        profile.get("table_scan_string_constant_comparison_time")
    )
    table_scan_string_constant_comparison_count = _coerce_int(
        profile.get("table_scan_string_constant_comparison_count")
    )
    table_scan_string_like_operator_time = _coerce_float(profile.get("table_scan_string_like_operator_time"))
    table_scan_string_like_operator_count = _coerce_int(profile.get("table_scan_string_like_operator_count"))
    table_scan_string_constant_comparison_read_io_time = _coerce_float(
        profile.get("table_scan_string_constant_comparison_read_io_time")
    )
    table_scan_string_like_operator_read_io_time = _coerce_float(
        profile.get("table_scan_string_like_operator_read_io_time")
    )

    # Use only direct profiler metrics for the requested string predicate CPU
    # categories. For older JSONs without these fields, leave values empty.
    if (
        table_scan_string_constant_comparison_time is not None
        or table_scan_string_like_operator_time is not None
    ):
        string_predicate_cpu_time = (table_scan_string_constant_comparison_time or 0.0) + (
            table_scan_string_like_operator_time or 0.0
        )
    else:
        string_predicate_cpu_time = None

    if (
        table_scan_string_constant_comparison_read_io_time is not None
        or table_scan_string_like_operator_read_io_time is not None
    ):
        string_predicate_read_io_time = (table_scan_string_constant_comparison_read_io_time or 0.0) + (
            table_scan_string_like_operator_read_io_time or 0.0
        )
    else:
        string_predicate_read_io_time = None

    planner_time = _coerce_float(profile.get("planner"))
    all_optimizers_time = _coerce_float(profile.get("all_optimizers"))
    # If either side is present, sum with missing side treated as zero.
    if planner_time is not None or all_optimizers_time is not None:
        query_parsing_optim_time = (planner_time or 0.0) + (all_optimizers_time or 0.0)
    else:
        query_parsing_optim_time = None

    parquet_decompression_time = _coerce_float(profile.get("parquet_decompression_time"))
    parquet_decryption_time = _coerce_float(profile.get("parquet_decryption_time"))
    string_pred_wallclock_time = _compute_string_pred_wallclock_time(profile, query_id, string_predicate_cpu_time)
    pread2_throughput_mb_s = _compute_pread2_throughput_mb_s(profile)
    (
        parquet_decryption_throughput_mb_s,
        parquet_decryption_thread_throughput_mb_s,
    ) = _compute_parquet_stage_throughputs_mb_s(profile, query_id, "decryption", parquet_decryption_time)
    (
        parquet_decompression_throughput_mb_s,
        parquet_decompression_thread_throughput_mb_s,
    ) = _compute_parquet_stage_throughputs_mb_s(profile, query_id, "decompression", parquet_decompression_time)

    return ProfileRow(
        query_id=query_id,
        cpu_time=_coerce_float(profile.get("cpu_time")),
        cumulative_optimizer_timing=_coerce_float(profile.get("cumulative_optimizer_timing")),
        query_parsing_optim_time=query_parsing_optim_time,
        string_predicate_cpu_time=string_predicate_cpu_time,
        string_pred_wallclock_time=string_pred_wallclock_time,
        string_predicate_read_io_time=string_predicate_read_io_time,
        table_scan_string_constant_comparison_time=table_scan_string_constant_comparison_time,
        table_scan_string_constant_comparison_count=table_scan_string_constant_comparison_count,
        table_scan_string_like_operator_time=table_scan_string_like_operator_time,
        table_scan_string_like_operator_count=table_scan_string_like_operator_count,
        table_scan_string_constant_comparison_read_io_time=table_scan_string_constant_comparison_read_io_time,
        table_scan_string_like_operator_read_io_time=table_scan_string_like_operator_read_io_time,
        query_name=_normalize_query_name(profile.get("query_name")),
        latency=_coerce_float(profile.get("latency")),
        rows_returned=_coerce_int(profile.get("rows_returned")),
        total_bytes_read=_coerce_int(profile.get("total_bytes_read")),
        total_bytes_written=_coerce_int(profile.get("total_bytes_written")),
        parquet_decompression_time=parquet_decompression_time,
        parquet_decompression_count=_coerce_int(profile.get("parquet_decompression_count")),
        parquet_decryption_time=parquet_decryption_time,
        parquet_decryption_count=_coerce_int(profile.get("parquet_decryption_count")),
        dds_pread_time=_coerce_float(profile.get("dds_pread_time")),
        dds_pread2_time=_coerce_float(profile.get("dds_pread2_time")),
        pread2_per_table_threads=_normalize_query_name(profile.get("pread2_per_table_threads")),
        pread2_per_table_bytes=_normalize_query_name(profile.get("pread2_per_table_bytes")),
        pread2_per_table_time=_normalize_query_name(profile.get("pread2_per_table_time")),
        # DDS pread metrics (latency stats, throughput, and call count)
        # Extracted only from `dds_pread_*` keys.
        dds_pread_total_throughput=_coerce_float(profile.get("dds_pread_total_throughput")),
        dds_pread_thread_throughput=_coerce_float(profile.get("dds_pread_thread_throughput")),
        dds_pread_p99_latency=_coerce_float(profile.get("dds_pread_p99_latency")),
        dds_pread_p50_latency=_coerce_float(profile.get("dds_pread_p50_latency")),
        dds_pread_max_latency=_coerce_float(profile.get("dds_pread_max_latency")),
        dds_pread_min_latency=_coerce_float(profile.get("dds_pread_min_latency")),
        dds_pread_latency=_coerce_float(profile.get("dds_pread_latency")),
        dds_pread_call_count=_coerce_int(profile.get("dds_pread_call_count")),
        pread2_throughput_mb_s=pread2_throughput_mb_s,
        parquet_decryption_throughput_mb_s=parquet_decryption_throughput_mb_s,
        parquet_decryption_thread_throughput_mb_s=parquet_decryption_thread_throughput_mb_s,
        parquet_decompression_throughput_mb_s=parquet_decompression_throughput_mb_s,
        parquet_decompression_thread_throughput_mb_s=parquet_decompression_thread_throughput_mb_s,
        # Legacy/orig pread metrics (without dds_ prefix)
        # Extracted only from `pread_*` keys.
        pread_total_throughput=_coerce_float(profile.get("pread_total_throughput")),
        pread_thread_throughput=_coerce_float(profile.get("pread_thread_throughput")),
        pread_p99_latency=_coerce_float(profile.get("pread_p99_latency")),
        pread_p50_latency=_coerce_float(profile.get("pread_p50_latency")),
        pread_max_latency=_coerce_float(profile.get("pread_max_latency")),
        pread_min_latency=_coerce_float(profile.get("pread_min_latency")),
        pread_latency=_coerce_float(profile.get("pread_latency")),
        pread_call_count=_coerce_int(profile.get("pread_call_count")),
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
                    "cumulative_optimizer_timing": row.cumulative_optimizer_timing,
                    "query_parsing_optim_time": row.query_parsing_optim_time,
                    "string_predicate_cpu_time": row.string_predicate_cpu_time,
                    "string_pred_wallclock_time": row.string_pred_wallclock_time,
                    "string_predicate_read_io_time": row.string_predicate_read_io_time,
                    "table_scan_string_constant_comparison_time": row.table_scan_string_constant_comparison_time,
                    "table_scan_string_constant_comparison_count": row.table_scan_string_constant_comparison_count,
                    "table_scan_string_like_operator_time": row.table_scan_string_like_operator_time,
                    "table_scan_string_like_operator_count": row.table_scan_string_like_operator_count,
                    "table_scan_string_constant_comparison_read_io_time": row.table_scan_string_constant_comparison_read_io_time,
                    "table_scan_string_like_operator_read_io_time": row.table_scan_string_like_operator_read_io_time,
                    "query_name": row.query_name,
                    "latency": row.latency,
                    "rows_returned": row.rows_returned,
                    "total_bytes_read": row.total_bytes_read,
                    "total_bytes_written": row.total_bytes_written,
                    "parquet_decompression_time": row.parquet_decompression_time,
                    "parquet_decompression_count": row.parquet_decompression_count,
                    "parquet_decryption_time": row.parquet_decryption_time,
                    "parquet_decryption_count": row.parquet_decryption_count,
                    "dds_pread_time": row.dds_pread_time,
                    "dds_pread2_time": row.dds_pread2_time,
                    "pread2_per_table_threads": row.pread2_per_table_threads,
                    "pread2_per_table_bytes": row.pread2_per_table_bytes,
                    "pread2_per_table_time": row.pread2_per_table_time,
                    # DDS pread metrics (latency stats, throughput, and call count)
                    "dds_pread_total_throughput": row.dds_pread_total_throughput,
                    "dds_pread_thread_throughput": row.dds_pread_thread_throughput,
                    "dds_pread_p99_latency": row.dds_pread_p99_latency,
                    "dds_pread_p50_latency": row.dds_pread_p50_latency,
                    "dds_pread_max_latency": row.dds_pread_max_latency,
                    "dds_pread_min_latency": row.dds_pread_min_latency,
                    "dds_pread_latency": row.dds_pread_latency,
                    "dds_pread_call_count": row.dds_pread_call_count,
                    "pread2_throughput_mb_s": row.pread2_throughput_mb_s,
                    "parquet_decryption_throughput_mb_s": row.parquet_decryption_throughput_mb_s,
                    "parquet_decryption_thread_throughput_mb_s": row.parquet_decryption_thread_throughput_mb_s,
                    "parquet_decompression_throughput_mb_s": row.parquet_decompression_throughput_mb_s,
                    "parquet_decompression_thread_throughput_mb_s": row.parquet_decompression_thread_throughput_mb_s,
                    # Legacy/orig pread metrics (without dds_ prefix)
                    "pread_total_throughput": row.pread_total_throughput,
                    "pread_thread_throughput": row.pread_thread_throughput,
                    "pread_p99_latency": row.pread_p99_latency,
                    "pread_p50_latency": row.pread_p50_latency,
                    "pread_max_latency": row.pread_max_latency,
                    "pread_min_latency": row.pread_min_latency,
                    "pread_latency": row.pread_latency,
                    "pread_call_count": row.pread_call_count,
                }
            )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments for input/output locations.

    The input directory is now required to ensure profiling JSONs are picked
    from the user-specified location.
    """
    parser = argparse.ArgumentParser(
        description="Extract key fields from DuckDB JSON profiling files into a single CSV."
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing DuckDB JSON profiling files",
    )

    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Path to write the output CSV (default: <input-dir>/<input-dir-name>.csv)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Run the extraction and write the summary CSV.

    If --output-csv is not provided, the output defaults to
    <input-dir>/tpch_profiles_summary.csv.
    """
    args = parse_args(argv)
    input_dir: Path = args.input_dir
    # Default to a CSV named after the input directory (e.g.,
    # <input-dir>/<input-dir-name>.csv) so outputs are easy to associate with the
    # profile set that generated them.
    output_csv: Path = args.output_csv or (input_dir / f"{input_dir.name}.csv")

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")

    rows = list(iter_profile_rows(input_dir))
    write_csv(rows, output_csv)
    print(f"Wrote {len(rows)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
