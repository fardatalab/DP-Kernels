#!/bin/bash

# Configuration
DUCKDB_BIN="./build/release/duckdb"
DB_FILE="tpch_metadata.db"
#QUERY_DIR="/data/dpk/GPUDB_Profiler/TPC-H/queries"
QUERY_DIR="/data/dbcomm/tpch-queries"
PROFILE_DIR="./tpch_test_perpage_profiles"
SECRET_KEY="01234567890123456789012345678901"
KEY_NAME="tpch_master_key"

# Create profile directory if it doesn't exist
mkdir -p "$PROFILE_DIR"

echo "=== TPC-H Encrypted Parquet Benchmark ==="
echo "Query Directory: $QUERY_DIR"
echo "Profile Directory: $PROFILE_DIR"

# Iterate through all 22 TPC-H queries
for q_nr in {1..22}; do
    #QUERY_FILE="$QUERY_DIR/query${q_nr}.sql"
    QUERY_FILE="$QUERY_DIR/tpch-q${q_nr}.sql"
    PROFILE_FILE="$(pwd)/$PROFILE_DIR/query${q_nr}.json" # Use absolute path
    
    if [ ! -f "$QUERY_FILE" ]; then
        echo "Skip: $QUERY_FILE not found."
        continue
    fi

    echo "------------------------------------------------"
    echo "Running Query $q_nr..."

    # 1. DROP OS CACHES (Cold Start)
    # Requires sudo. If running as non-root, ensure sudo is passwordless or run script with sudo.
    echo "Clearing OS Page Cache..."
    sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    # 2. EXECUTE DUCKDB
    # We pass the commands via -c to ensure clean session close and file flush.
    # We include the query file content directly in the string.
    $DUCKDB_BIN "$DB_FILE" <<EOF
-- Security Setup
PRAGMA add_parquet_key('$KEY_NAME', '$SECRET_KEY');

-- SET threads to 64
SET threads TO 16;

-- Profiling Configuration
SET profiling_output = '$PROFILE_FILE';
SET profiling_mode = 'detailed';
SET enable_profiling = 'json';

-- Load Query Content
.read $QUERY_FILE

-- Force Flush of Profiling JSON
#SET enable_profiling = 'none';
EOF

    echo "Query $q_nr complete. Profile saved to $PROFILE_FILE"
done

echo "------------------------------------------------"
echo "All benchmarks finished."
