#!/bin/bash

# Configuration
DUCKDB_BIN="./build/release/duckdb"  # Path to your compiled DuckDB binary
# SF 50 used in the paper
SCALE_FACTOR=10                        # 1 = ~1GB of raw data
OUTPUT_DIR="./tpch_parquet_nonencrypted"
SECRET_KEY="01234567890123456789012345678901" # Must be 32 chars for 256-bit
KEY_NAME="tpch_master_key"

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# List of TPC-H tables
TABLES=("lineitem" "orders" "part" "partsupp" "customer" "supplier" "nation" "region")

echo "Starting TPC-H data generation and Parquet export..."
echo "Scale Factor: $SCALE_FACTOR"
echo "Compression:  GZIP"
# echo "Encryption:   AES-GCM (footer_key)"

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
        COMPRESSION 'gzip' 
    );"
done

# Execute DuckDB
# We use an in-memory database (:memory:) to avoid creating a large .duckdb file
$DUCKDB_BIN :memory: <<EOF
$SQL_COMMAND
EOF

echo "--------------------------------------------------"
echo "Done! Files generated in $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"
