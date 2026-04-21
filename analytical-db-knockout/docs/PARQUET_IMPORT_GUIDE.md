# Parquet Import Guide for ClickHouse

## Quick Start - NYC Taxi Batch Import

### 1. Discover Available Parquet Files
```bash
make parquet-discover
```

### 2. Import All NYC Taxi Parquet Files
```bash
make parquet-import
```

This will:
1. Start ClickHouse if not running
2. Discover all `.parquet` files in `../NYC Yellow Taxi Record 23-24-25/`
3. Batch import to `yellow_taxi_trips` table
4. Validate the import

### 3. Monitor Progress
The import shows:
- File count and total size
- Import progress per file (rows/sec)
- Summary statistics at the end

---

## Installation

### 1. Install Dependencies
```bash
make install
# OR manually:
pip install clickhouse-driver pandas pyarrow requests
```

### 2. Start ClickHouse
```bash
make setup-clickhouse
# OR manually:
docker compose up -d clickhouse
```

---

## Command-Line Usage

### Batch Import from Directory (All Parquet Files)

**Recommended for NYC Taxi Data:**
```bash
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --table yellow_taxi_trips \
  --directory \
  --method 2 \
  --validate
```

**Shorter version using Python:**
```bash
python import_nyc_taxi_batch.py --validate
```

### Single File Import
```bash
python parquet_importer.py taxi_data.parquet \
  --table yellow_taxi_trips \
  --method 2 \
  --validate
```

### Import with Options
```bash
# Dry run (show what would be imported)
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --directory \
  --dry-run

# Continue on errors (skip failed files)
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --directory \
  --skip-errors

# Use different method
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --directory \
  --method 1 \
  --chunk-size 5000
```

---

## Import Methods Comparison

| Method | Speed | Memory | Best For | Command |
|--------|-------|--------|----------|---------|
| **1: Pandas Chunks** | Slow | High | Small files (<1GB) | `--method 1 --chunk-size 10000` |
| **2: Native Protocol** | ⭐⭐⭐ Fast | Medium | **Most use cases (RECOMMENDED)** | `--method 2` |
| **3: SQL Parquet Format** | ⭐⭐⭐⭐ Fastest | Low | Large files + shared volume | `--method 3` |
| **4: HTTP API** | Medium | Medium | Remote/REST integration | `--method 4` |

**For NYC Taxi batch import: Use Method 2** (native protocol) - best balance of speed and reliability.

---

## Method Details

### Method 1: Pandas + Chunks
**When to use:** Small to medium files (<1GB)

```bash
python parquet_importer.py taxi_data.parquet \
  --table yellow_taxi_trips \
  --method 1 \
  --chunk-size 10000
```

**Pros:**
- ✅ Simple, no infrastructure setup
- ✅ Memory-efficient with chunking
- ✅ Good for testing

**Cons:**
- ❌ Slowest method
- ❌ Memory overhead with dataframes

---

### Method 2: Native Protocol ⭐ RECOMMENDED
**When to use:** General purpose, **99% of cases**

```bash
python parquet_importer.py taxi_data.parquet \
  --table yellow_taxi_trips \
  --method 2
```

**Make command:**
```bash
make parquet-import
```

**Pros:**
- ✅ **Best performance** for most scenarios
- ✅ Native ClickHouse protocol optimization
- ✅ Good memory efficiency
- ✅ Works well for batch imports

**Cons:**
- ❌ Requires network access to ClickHouse

---

### Method 3: SQL Parquet Format (FASTEST)
**When to use:** Large files (>10GB), production deployments

**Prerequisites:**
File must be accessible to ClickHouse container:
- In `/var/lib/clickhouse/user_files/`
- Or on S3 via `s3()` function

**Docker Setup:**
```yaml
volumes:
  - ./data:/var/lib/clickhouse/user_files  # Mount your Parquet files here
```

**Usage:**
```bash
cp taxi_data.parquet ./data/
python parquet_importer.py /var/lib/clickhouse/user_files/taxi_data.parquet \
  --table yellow_taxi_trips \
  --method 3
```

**Alternative: Direct SQL**
```sql
INSERT INTO yellow_taxi_trips 
SELECT * FROM file('/var/lib/clickhouse/user_files/taxi_data.parquet', 'Parquet')
```

**For S3 files:**
```sql
INSERT INTO yellow_taxi_trips 
SELECT * FROM s3(
  'https://my-bucket.s3.amazonaws.com/taxi_data.parquet',
  'access_key',
  'secret_key'
)
```

**Pros:**
- ✅ **Fastest method** (0-copy in container)
- ✅ Minimal memory usage
- ✅ Parallel processing

**Cons:**
- ❌ Requires shared volume or S3 access
- ❌ More setup complexity

---

### Method 4: HTTP API
**When to use:** Remote imports, REST integration

```bash
python parquet_importer.py taxi_data.parquet \
  --table yellow_taxi_trips \
  --method 4 \
  --host remote-clickhouse.example.com
```

**Pros:**
- ✅ Works over network
- ✅ REST-friendly

**Cons:**
- ❌ Slower than native protocol
- ❌ Requires HTTP port (8123) open

---

## Batch Import Features

### Discover Parquet Files
```bash
make parquet-discover
```

Shows all `.parquet` files found recursively in the directory:
```
Found 24 Parquet files:
  - ../NYC Yellow Taxi Record 23-24-25/yellow_tripdata_2024-01.parquet (456.2 MB)
  - ../NYC Yellow Taxi Record 23-24-25/yellow_tripdata_2024-02.parquet (402.1 MB)
  - ...
Total size: 12345.6 MB
```

### Import All Files
```bash
make parquet-import
```

Batch imports with:
- ✅ Automatic ClickHouse startup
- ✅ Recursive file discovery
- ✅ Per-file progress tracking
- ✅ Error handling & skip option
- ✅ Final validation & statistics

### Dry-Run Preview
```bash
make parquet-import-dryrun
```

Shows what would be imported without actually importing.

### Skip Errors
```bash
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --directory \
  --skip-errors
```

Continues importing even if some files fail.

---

## Real-World Examples

### Example 1: Import NYC Taxi Data (Batch)

```bash
# Step 1: Start ClickHouse
make setup-clickhouse

# Step 2: Import all Parquet files
make parquet-import

# Step 3: Query the data
docker exec nyc_taxi_clickhouse clickhouse-client \
  --query "SELECT COUNT(*) FROM yellow_taxi_trips"
```

### Example 2: Import with Progress Monitoring

```bash
# Verbose import with per-file progress
python import_nyc_taxi_batch.py --validate

# Output:
# [1/24] Importing yellow_tripdata_2024-01.parquet (456.2 MB)...
# ✓ Imported 2,500,000 rows in 12.34s (202,598 rows/sec)
# [2/24] Importing yellow_tripdata_2024-02.parquet (402.1 MB)...
# ✓ Imported 2,200,000 rows in 10.56s (208,333 rows/sec)
# ...
# 📊 BATCH IMPORT SUMMARY
# Total files: 24
# Successful: 24
# Failed: 0
# Total rows: 60,500,000
# Total time: 234.56s
# ✓ Successfully imported 24 files with 60,500,000 rows
```

### Example 3: Custom Directory

```bash
python import_nyc_taxi_batch.py \
  --directory /path/to/parquet/files \
  --method 2 \
  --validate
```

### Example 4: Handle Errors Gracefully

```bash
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --directory \
  --skip-errors \
  --validate
```

If any file fails, import continues with others. Summary shows which files failed.

### Example 5: Batch Import in Python Script

```python
from parquet_importer import ParquetImporter

importer = ParquetImporter()

# Batch import
result = importer.batch_import(
    '../NYC Yellow Taxi Record 23-24-25',
    'yellow_taxi_trips',
    method=2,
    skip_errors=True
)

# Check results
print(f"Imported {result['successful_files']}/{result['total_files']} files")
print(f"Total rows: {result['total_rows']:,}")
print(f"Time: {result['total_time']:.2f}s")

if result['failed_files'] > 0:
    print("Failed files:")
    for file_path, error in result['failed_file_list']:
        print(f"  - {file_path}: {error}")
```

---

## Performance Tuning

### For Large Batch Imports (>50GB)

**Method 1: Use SQL with Docker Volume**
```yaml
# docker-compose.yml
services:
  clickhouse:
    volumes:
      - ./parquet_data:/var/lib/clickhouse/user_files
```

```bash
# Copy files to volume
cp -r ../NYC\ Yellow\ Taxi\ Record\ 23-24-25/* ./parquet_data/

# Import via SQL (fastest)
docker exec nyc_taxi_clickhouse clickhouse-client \
  --query "INSERT INTO yellow_taxi_trips 
           SELECT * FROM file('*.parquet', 'Parquet')"
```

**Method 2: Optimize ClickHouse Settings**
```python
from clickhouse_driver import Client

client = Client('localhost')

# Increase insert block size for faster imports
client.execute("SET insert_block_size = 104857600")  # 100MB blocks

# Use async insert for throughput
client.execute("SET async_insert = 1")

# Import...
```

### For Many Small Files

```bash
# Parallel import (ClickHouse handles it automatically)
python import_nyc_taxi_batch.py --method 2
```

ClickHouse's native protocol handles parallel inserts automatically.

### For Low-Memory Systems

**Use smaller chunks:**
```bash
python parquet_importer.py ../NYC\ Yellow\ Taxi\ Record\ 23-24-25 \
  --directory \
  --method 1 \
  --chunk-size 1000
```

---

## Troubleshooting

### Issue: "Connection refused"
```
✗ Import failed: Error connection from ('localhost', 9000)
```

**Solution:** Check ClickHouse is running
```bash
docker-compose up -d clickhouse
docker-compose logs clickhouse
```

### Issue: "Table doesn't exist"
```
✗ Import failed: Code: 60, e.displayText() = DB::Exception: Table yellow_taxi_trips doesn't exist
```

**Solution:** Create table first
```sql
CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    VendorID BIGINT,
    tpep_pickup_datetime DateTime('UTC'),
    tpep_dropoff_datetime DateTime('UTC'),
    passenger_count Float64,
    trip_distance Float64,
    RatecodeID Float64,
    store_and_fwd_flag String,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type BIGINT,
    fare_amount Float64,
    extra Float64,
    mta_tax Float64,
    tip_amount Float64,
    tolls_amount Float64,
    improvement_surcharge Float64,
    total_amount Float64,
    congestion_surcharge Float64,
    airport_fee Float64,
    cbd_congestion_fee Float64
) ENGINE = MergeTree()
ORDER BY (tpep_pickup_datetime, VendorID)
PARTITION BY toYYYYMM(tpep_pickup_datetime)
```

### Issue: "Type mismatch" on import
```
✗ Import failed: Code: 53, e.displayText() = DB::Exception: Type mismatch
```

**Solution:** Check Parquet schema matches ClickHouse table
```python
from parquet_importer import ParquetImporter
import pandas as pd

importer = ParquetImporter()
schema = importer.get_table_schema('yellow_taxi_trips')
print("ClickHouse schema:", schema)

# Verify Parquet matches
df = pd.read_parquet('data.parquet')
print("Parquet schema:", df.dtypes)
```

### Issue: "Out of memory" on large files
```
✗ Import failed: MemoryError
```

**Solution:** Use Method 3 (SQL Parquet Format) instead of Method 2

```bash
python parquet_importer.py taxi_data.parquet \
  --table yellow_taxi_trips \
  --method 3
```

### Issue: "No Parquet files found"
```
Found 0 Parquet files in ../NYC Yellow Taxi Record 23-24-25/
```

**Solution:** Verify directory path and permissions
```bash
ls -la "../NYC Yellow Taxi Record 23-24-25/"
find "../NYC Yellow Taxi Record 23-24-25/" -name "*.parquet" | head -5
```

---

## Make Commands Summary

```bash
# Discovery
make parquet-discover          # Find all Parquet files

# Import
make parquet-import            # Import all NYC Taxi Parquet files ⭐
make parquet-import-dryrun     # Preview without importing
make parquet-import-sample     # Create & import sample
make parquet-import-sample-to-ch  # Import sample to ClickHouse

# Testing
make parquet-test              # Run import tests

# Complete workflow
make parquet-setup-all         # Setup + discover + sample + test
```

---

## Advanced Configuration

### Custom Table Definition
```python
from parquet_importer import ParquetImporter

importer = ParquetImporter()

# Get current table schema
schema = importer.get_table_schema('yellow_taxi_trips')
print(schema)

# Validate table
importer.validate_import('yellow_taxi_trips')
```

### Batch Import with Filtering
```python
from pathlib import Path
from parquet_importer import ParquetImporter

importer = ParquetImporter()

# Find specific year/month files
data_dir = Path('../NYC Yellow Taxi Record 23-24-25')
parquet_files = [f for f in data_dir.rglob('*.parquet') if '2024-01' in str(f)]

# Import each
for file in parquet_files:
    importer.import_method_2_native_protocol(str(file), 'yellow_taxi_trips')
```

---

## Resources

- [ClickHouse Parquet Format](https://clickhouse.com/docs/en/sql-reference/formats/parquet)
- [clickhouse-driver Docs](https://github.com/mymarilyn/clickhouse-driver)
- [Pandas Parquet](https://pandas.pydata.org/docs/reference/io.html#parquet)
- [PyArrow Parquet](https://arrow.apache.org/docs/python/parquet.html)

---

## Support & Issues

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs: `docker-compose logs clickhouse`
3. Test connection: `python parquet_importer.py --help`
4. Verify file: `python -c "import pandas; print(len(pandas.read_parquet('file.parquet')))"`
