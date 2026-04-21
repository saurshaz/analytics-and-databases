#!/bin/bash
# Quick-start script for importing Parquet files into ClickHouse
# Usage: ./import_parquet_quickstart.sh <parquet_file> [--table table_name] [--method 1-4]

set -e

echo "=========================================="
echo "ClickHouse Parquet Import Quick Start"
echo "=========================================="

# Check arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <parquet_file> [--table table_name] [--method 1-4]"
    echo ""
    echo "Examples:"
    echo "  $0 data.parquet"
    echo "  $0 data.parquet --table my_table"
    echo "  $0 data.parquet --table my_table --method 2"
    exit 1
fi

PARQUET_FILE="$1"
TABLE_NAME="${3:-yellow_taxi_trips}"
METHOD="${5:-2}"
HOST="localhost"
PORT="9000"

# Verify file exists
if [ ! -f "$PARQUET_FILE" ]; then
    echo "❌ Error: File not found: $PARQUET_FILE"
    exit 1
fi

# Show file info
FILE_SIZE=$(du -h "$PARQUET_FILE" | cut -f1)
ROW_COUNT=$(python3 -c "import pandas; print(len(pandas.read_parquet('$PARQUET_FILE')))" 2>/dev/null || echo "?")

echo ""
echo "📊 File Information:"
echo "  File: $PARQUET_FILE"
echo "  Size: $FILE_SIZE"
echo "  Rows: $ROW_COUNT"
echo ""
echo "🎯 Import Settings:"
echo "  Method: $METHOD"
echo "  Table: $TABLE_NAME"
echo "  Host: $HOST:$PORT"
echo ""

# Check if ClickHouse is running
echo "🔍 Checking ClickHouse connection..."
if python3 -c "from clickhouse_driver import Client; Client('$HOST', port=$PORT).execute('SELECT 1')" 2>/dev/null; then
    echo "✓ Connected to ClickHouse"
else
    echo "❌ Error: Cannot connect to ClickHouse at $HOST:$PORT"
    echo ""
    echo "Start ClickHouse with:"
    echo "  docker-compose up -d clickhouse"
    exit 1
fi

# Run import
echo ""
echo "⏳ Starting import (Method $METHOD)..."
python3 - "$PARQUET_FILE" "$TABLE_NAME" "$METHOD" "$HOST" "$PORT" << 'PYTHON_SCRIPT'
import sys
from parquet_importer import ParquetImporter
import time

parquet_file = sys.argv[1]
table_name = sys.argv[2]
method = int(sys.argv[3])
host = sys.argv[4]
port = int(sys.argv[5])

importer = ParquetImporter(host=host, port=port)

try:
    start = time.time()
    
    if method == 1:
        importer.import_method_1_pandas_chunks(parquet_file, table_name)
    elif method == 2:
        importer.import_method_2_native_protocol(parquet_file, table_name)
    elif method == 3:
        importer.import_method_3_sql_parquet_format(parquet_file, table_name)
    elif method == 4:
        importer.import_method_4_http_api(parquet_file, table_name)
    
    elapsed = time.time() - start
    importer.validate_import(table_name)
    
    print(f"\n✅ Import completed in {elapsed:.2f}s")
    
except Exception as e:
    print(f"\n❌ Import failed: {e}")
    sys.exit(1)
PYTHON_SCRIPT

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ SUCCESS!"
    echo "=========================================="
    echo ""
    echo "Data imported successfully!"
    echo ""
    echo "Next: Query your data"
    echo "  docker exec nyc_taxi_clickhouse clickhouse-client \\"
    echo "    --query \"SELECT COUNT(*) FROM $TABLE_NAME\""
else
    echo ""
    echo "❌ Import failed!"
    exit $exit_code
fi
