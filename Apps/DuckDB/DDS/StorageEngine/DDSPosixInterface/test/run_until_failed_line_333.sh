#!/usr/bin/env bash

set -uo pipefail

PATTERN='FAILED at line'
CMD='./build/concurrent_dds_posix_tests'

iter=1
while true; do
  echo "==== iteration ${iter} ($(date -Is)) ===="

  # Always capture output; do not stop on non-zero exit codes.
  output="$(${CMD} 2>&1 || true)"
  printf '%s\n' "$output"

  if grep -Fq "$PATTERN" <<<"$output"; then
    echo "==== matched '${PATTERN}' on iteration ${iter}; stopping ===="
    exit 0
  fi

  iter=$((iter + 1))
done
