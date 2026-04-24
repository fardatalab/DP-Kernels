#!/bin/bash

# Configuration
DUCKDB_BIN="./build/release/duckdb"  # Path to your compiled DuckDB binary
# SF 50 used in the paper
SCALE_FACTOR=40                        # 1 = ~1GB of raw data
OUTPUT_DIR="./tpch_parquet_new_SF${SCALE_FACTOR}" # Output directory for Parquet files
DB_PATH="./tpch_sf${SCALE_FACTOR}.duckdb"
SECRET_KEY="01234567890123456789012345678901" # Must be 32 chars for 256-bit
KEY_NAME="tpch_master_key"

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Defensive check: fail early if DuckDB binary is missing or not executable.
if [[ ! -x "$DUCKDB_BIN" ]]; then
    echo "Error: DuckDB binary not found or not executable at $DUCKDB_BIN"
    exit 1
fi

# List of TPC-H tables
TABLES=("lineitem" "orders" "part" "partsupp" "customer" "supplier" "nation" "region")

echo "Starting TPC-H data generation and Parquet export..."
echo "Scale Factor: $SCALE_FACTOR"
echo "Compression:  GZIP"
echo "Encryption:   AES-GCM (footer_key)"

# Construct the SQL command
# 1. Load extensions (built-in if you followed previous steps)
# 2. Add the encryption key
# 3. Generate the data using dbgen
# 4. Loop through tables and export each to Parquet
SQL_COMMAND="
LOAD tpch;
LOAD parquet;
PRAGMA add_parquet_key('$KEY_NAME', '$SECRET_KEY');
SELECT 'Generating data...' as status;
CALL dbgen(sf=$SCALE_FACTOR);
"

for TABLE in "${TABLES[@]}"; do
    SQL_COMMAND+="
    SELECT 'Exporting $TABLE...' as status;
    COPY $TABLE TO '$OUTPUT_DIR/$TABLE.parquet' (
        FORMAT PARQUET, 
        COMPRESSION 'gzip', 
        ENCRYPTION_CONFIG {footer_key: '$KEY_NAME'}
    );"
done

# Execute DuckDB
# Original behavior used an in-memory database:
# $DUCKDB_BIN :memory: <<EOF
# Updated behavior uses an on-disk database to avoid OOM at higher SF.
$DUCKDB_BIN "$DB_PATH" <<EOF
$SQL_COMMAND
EOF

echo "--------------------------------------------------"
echo "Done! Files generated in $OUTPUT_DIR"
echo "On-disk DuckDB file: $DB_PATH"
ls -lh "$OUTPUT_DIR"
