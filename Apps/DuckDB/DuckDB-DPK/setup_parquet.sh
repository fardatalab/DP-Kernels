#!/bin/bash

# Configuration
DUCKDB_BIN="./build/release/duckdb"
DB_FILE="tpch_metadata.db"
PARQUET_DIR="./tpch_parquet_encrypted"
#PARQUET_DIR="./tpch_parquet_test"
KEY_NAME="tpch_master_key"
SECRET_KEY="01234567890123456789012345678901"

# List of TPC-H tables
TABLES=("lineitem" "orders" "part" "partsupp" "customer" "supplier" "nation" "region")

echo "Creating persistent views in $DB_FILE..."

# Build a single SQL string to minimize CLI overhead
SQL_CMD="LOAD parquet; PRAGMA add_parquet_key('$KEY_NAME', '$SECRET_KEY');"

for TABLE in "${TABLES[@]}"; do
    FILE_PATH="$PARQUET_DIR/$TABLE.parquet"
    
    # Check if file exists before creating view
    if [ ! -f "$FILE_PATH" ]; then
        echo "Warning: $FILE_PATH not found. Skipping $TABLE."
        continue
    fi

    # CREATE OR REPLACE ensures you can re-run this if paths change
    SQL_CMD+="CREATE OR REPLACE VIEW $TABLE AS 
              SELECT * FROM read_parquet('$FILE_PATH', encryption_config={footer_key: '$KEY_NAME'});"
    echo "Registered view: $TABLE"
done

# Execute against the persistent DB file
$DUCKDB_BIN "$DB_FILE" <<EOF
$SQL_CMD
EOF

echo "--------------------------------------------------"
echo "Success! Use './duckdb $DB_FILE' to access your tables."
