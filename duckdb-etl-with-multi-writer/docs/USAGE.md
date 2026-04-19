# Usage Guide

## Quick Start

### Setup

```bash
# Create virtual environment
make venv-create

# Activate (Linux/macOS)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Load 2023 Data

```bash
make etl-load-2023
```

This runs the unified ETL pipeline in etl mode for 2023 only:
```python
from src.unified_etl_pipeline import UnifiedETLPipeline

pipeline = UnifiedETLPipeline(mode='etl')
result = pipeline.run(years=[2023])
print(f"Loaded {result['total_rows']:,} rows")
```

Performance: ~45M rows in 16 seconds (2.8M rows/sec)

## Available Modes and Commands

### ETL Mode (Incremental Loading)
```bash
make etl              # All years
make etl-load-2023    # 2023 only
make etl-load-2024    # 2024 only
make etl-load-2025    # 2025 only
make etl-load-all     # Explicitly all years
```

### Partition Mode (Hive Format)
```bash
make partition        # Partition all years
```
Creates `data/processed/year=YYYY/month=MM/day=DD/` structure for 10-100x faster queries.

### Analytics Mode
```bash
make query            # Run all queries
make query-stats      # Table statistics
make query-daily      # Last 7 days summary
make query-vendor     # Vendor performance
```

### Configuration Presets
```bash
make etl-dev          # Development (snappy)
make etl-fast         # Maximum speed (no compression)
make etl-compact      # Maximum compression (gzip)
```

### Test Multi-Writer Coordination

```bash
make test-multiwriter
```

Tests concurrent writers with registry locking:
- 3 parallel workers loading different years
- Lock timeout scenarios
- Error recovery

### Registry Management

```bash
# View active locks and completed runs
make registry-status

# Show performance metrics
make show-metrics

# Clean locks older than 24 hours
make registry-cleanup
```

### Interactive Demo

```bash
make demo
```

Runs `scripts/demo_registry_locking.py` with options for:
- Single-writer loading
- Multi-writer coordination
- Registry audit trail
- Raw registry JSON inspection

## Python API

### Load Data with Lock

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

pipeline = UnifiedETLPipeline(
    mode='etl',
    db_path='nyc_yellow_taxi.duckdb',
    pipeline_id='my_etl'
)

# Load with automatic registry management
result = pipeline.run(
    years=[2023],
    writer_id_prefix='worker'
)

print(f"Loaded {result['total_rows']:,} rows in {result.get('duration_sec', 'N/A')}s")
```

### Multi-Writer Scenario

```python
from src.unified_etl_pipeline import UnifiedETLPipeline
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)

pipeline = UnifiedETLPipeline(mode='etl', timeout=300)

def load_year_safe(year):
    \"\"\"Load a year with registry locking\"\"\"
    try:
        result = pipeline.run(years=[year])
        return {'year': year, 'status': 'success', 'rows': result['total_rows']}
    except Exception as e:
        return {'year': year, 'status': 'failed', 'error': str(e)}

# Load all years in parallel (registry locks coordinate safely)
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(load_year_safe, [2023, 2024, 2025]))

for result in results:
    status_icon = '✅' if result['status'] == 'success' else '❌'
    print(f"{status_icon} {result['year']}: {result['status']}")
```

### Execute SQL Safely

```python
from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL

etl = DuckDBMultiWriterETL(db_path='nyc_yellow_taxi.duckdb')

# All writes are coordinated via registry locking
con = etl.get_connection()

try:
    result = con.execute("""
        SELECT 
            DATE(tpep_pickup_datetime) as day,
            COUNT(*) as trips,
            AVG(total_amount) as avg_fare
        FROM yellow_taxi_trips
        WHERE YEAR(tpep_pickup_datetime) = 2024
        GROUP BY DATE(tpep_pickup_datetime)
        ORDER BY day DESC
        LIMIT 10
    """).fetchall()
    
    for row in result:
        print(f"{row[0]}: {row[1]} trips, ${row[2]:.2f} avg fare")
finally:
    etl.close()
```

### Check Registry Status

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

pipeline = UnifiedETLPipeline(mode='etl')

# Get and display status
status = pipeline.show_status()
```

This shows:
- Active locks
- Completed runs
- Total rows written
- Recent operations

## Configuration

### Customize Timeout

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

# Default 300s, specify custom
pipeline = UnifiedETLPipeline(
    mode='etl',
    db_path='nyc_yellow_taxi.duckdb',
    timeout=600  # 10 minutes for large loads
)

result = pipeline.run(years=[2023])
```

### Custom Registry Location

```python
from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL

etl = DuckDBMultiWriterETL(
    db_path='nyc_yellow_taxi.duckdb',
    registry_dir='/custom/registry/path'
)
```

### Custom Data Directory

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

# Load from alternative data directory
pipeline = UnifiedETLPipeline(
    mode='etl',
    db_path='alternative/path.duckdb',
    data_dir='/mnt/shared/taxi_data/NYC'
)

result = pipeline.run(years=[2023])
print(f"Loaded {result['total_rows']:,} rows")
```

## Analyzing Partitioned Data

After running `make partition`, analyze the structure:

```python
from pathlib import Path

partitioned_dir = Path('data/processed')

# Count partitions
year_dirs = list(partitioned_dir.glob('year=*/'))
print(f"Years: {len(year_dirs)}")

# Check data sizes
for year_dir in year_dirs:
    files = list(year_dir.glob('**/*.parquet'))
    print(f"  {year_dir.name}: {len(files)} parquet files")

# Query from partitions
import duckdb

con = duckdb.connect()
result = con.execute("""
    SELECT 
        YEAR(tpep_pickup_datetime) as year,
        MONTH(tpep_pickup_datetime) as month,
        COUNT(*) as trips
    FROM read_parquet('data/processed/**/*.parquet')
    GROUP BY year, month
""").fetchall()

for row in result[:10]:
    print(f"  {row[0]}-{row[1]:02d}: {row[2]:,} trips")
```

## Performance Tuning

### Increase Lock Timeout

For larger datasets, increase registry lock timeout:

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

pipeline = UnifiedETLPipeline(mode='etl', timeout=900)  # 15 minutes
result = pipeline.run(years=[2023])
```

### Query Optimization

```python
# DuckDB automatically uses partition pruning for performant date range queries
import duckdb

con = duckdb.connect()
# This query automatically reads only relevant partitions
result = con.execute("""
    SELECT COUNT(*) 
    FROM read_parquet('data/processed/**/*.parquet')
    WHERE tpep_pickup_datetime >= '2024-04-01'
      AND tpep_pickup_datetime <  '2024-07-01'
""").fetchall()

print(f"Q2 2024: {result[0][0]:,} trips (with partition pruning)")
```

## Batch Loading

```python
# Load multiple years efficiently
from src.unified_etl_pipeline import UnifiedETLPipeline

etl = DuckDBMultiWriterETL(db_path='taxidb.duckdb')

# Load multiple partitions in sequence
results = etl.parallel_load_partitions_safe(
    partition_paths=[
        'NYC Yellow Taxi Record 23-24-25/2023/*.parquet',
        'NYC Yellow Taxi Record 23-24-25/2024/*.parquet',
    ],
    table_name='yellow_taxi_trips',
    run_id='bulk_load_20260417',
    sequential=True  # Safe coordination
)
```

### Query Caching

```python
import duckdb

con = duckdb.connect('nyc_yellow_taxi.duckdb', read_only=True)

# Repeated queries benefit from Parquet caching
for _ in range(10):
    result = con.execute("""
        SELECT 
            HOUR(tpep_pickup_datetime) as hour,
            COUNT(*) as trips
        FROM yellow_taxi_trips
        GROUP BY HOUR(tpep_pickup_datetime)
    """).fetchall()

con.close()
```

## Maintenance

### Archive and Backup Registry

```bash
# Backup registry before cleanup
cp data/registries/nyc_yellow_taxi_registry.json \
   data/registries/nyc_yellow_taxi_registry.backup.json

# Cleanup old locks (older than 24 hours)
make registry-cleanup
```

### Monitor Disk Usage

```bash
# Check database size
ls -lh nyc_yellow_taxi.duckdb

# Check registry size
wc -l data/registries/*.json

# Check partitioned data size
du -sh data/processed/
```

### Verify Data Integrity

```bash
# Use validate mode
make validate

# Or programmatically
from src.unified_etl_pipeline import UnifiedETLPipeline

pipeline = UnifiedETLPipeline(mode='validate')
result = pipeline.run()
```

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed solutions.

Quick reference:
```bash
# Check registry status
make registry-status

# View full metrics
make show-metrics

# Test basic functionality
make test-etl

# View full registry
cat data/registries/nyc_yellow_taxi_registry.json | jq .

# Reset registry (WARNING: loses history)
rm data/registries/*.json
```

Common issues and solutions:
- **Lock timeout**: Increase timeout or clean old locks
- **Database locked**: Restart connection or remove .duckdb file
- **No data found**: Verify data directory path
- **Permission denied**: Check file permissions on data and registry directories

## Next Steps

1. Load data: `make etl-load-all`
2. Run analytics: `make query-stats`  
3. Partition for fast queries: `make partition`
4. Monitor performance: `make show-metrics`
5. Check registry: `make registry-status`
6. See [ARCHITECTURE.md](../docs/ARCHITECTURE.md) for system design
7. Setup deployment (see DEPLOYMENT guide)
