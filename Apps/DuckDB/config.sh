#!/bin/bash

common_path=/home/ubuntu/DP-Kernel/DuckDB

so_path=${common_path}/DuckDB-API
duckdb_api_path=${comment_path}/DuckDB-API

## Unzip DuckDB's C/C++ API
cd DuckDB-API
if ! [[ -f duckdb.h && -f duckdb.hpp && -f libduckdb.so ]]; then
    unzip libduckdb-linux-aarch64.zip
fi
cd ../..

## Set up LD_LIBRARY_PATH if needed
if ! [[ $LD_LIBRARY_PATH == *"$so_path"* ]]; then
    LD_LIBRARY_PATH=/usr/local/lib
    export LD_LIBRARY_PATH=$so_path:$LD_LIBRARY_PATH
fi
echo "$LD_LIBRARY_PATH"

## Install needed dependencies
sudo apt install nlohmann-json3-dev