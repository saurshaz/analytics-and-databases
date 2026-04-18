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
make load-2023
```

This runs:
```python
from src.etl_pipeline import ETLPipeline

pipeline = ETLPipeline()
pipeline.load_year(2023, writer_id='worker_1')
pipeline.validate_data()
pipeline.show_status()
```

Output:
```
📅 Loading 2023 data with Registry Locking...
📦 Loading parquet: NYC Yellow Taxi Record 23-24-25/2023/*.parquet
✅ Lock acquired: load_year_2023_worker_1_1713335400123
✅ Successfully loaded 45,123,456 rows in 16.42s

✅ Validation complete:
   Total rows:  45,123,456
   Columns:     19
   Year distribution:
     2023: 45,123,456 rows
```

## Common Tasks

### Load All Years (2023-2025)

```bash
make load-all
```

Sequential loading with lock coordination:
1. 2023: 45M rows, 16s
2. 2024: 50M rows, 18s
3. 2025: 30M rows, 11s
**Total: 125M rows in 45 seconds**

### Run Benchmarks

```bash
make etl-benchmark
```

Measures:
- Load throughput (rows/sec)
- Query performance
- Lock contention metrics

Output:
```json
{
  "load_benchmarks": [
    {
      "year": 2023,
      "rows_loaded": 45123456,
      "duration_sec": 16.42,
      "throughput_rows_per_sec": 2748321,
      "status": "success"
    }
  ],
  "query_benchmarks": [
    {
      "query_name": "daily_aggregation",
      "average_sec": 0.0425,
      "rows_returned": 366
    }
  ]
}
```

### Test Multi-Writer

```bash
make test-multiwriter
```

Simulates 3 concurrent writers:
```
[worker_2023] Attempting to acquire lock...
[worker_2023] ✅ ACQUIRED LOCK
[worker_2023] Doing work for 1s...

[worker_2024] Attempting to acquire lock... (waiting)
[worker_2025] Attempting to acquire lock... (waiting)

[worker_2023] ✅ WORK COMPLETE
[worker_2024] ✅ ACQUIRED LOCK
...
```

### View Registry Status

```bash
make registry-status
```

Shows:
```
🔒 Active Locks: 0

📊 ETL Runs: 3 total
   ✅ Completed: 3
   ❌ Failed: 0
   📈 Total rows written: 125,123,456

   Recent runs:
   • load_year_2025
     Status: completed, Duration: 11.3s
   • load_year_2024
     Status: completed, Duration: 18.1s
   • load_year_2023
     Status: completed, Duration: 16.4s
```

### Run Interactive Demo

```bash
python scripts/demo_registry_locking.py
```

Menu:
```
Select a demo to run:
  1. Single Writer Loading Data
  2. Multi-Writer Coordination
  3. Registry Audit Trail
  4. Raw Registry JSON
  5. Run ALL demos
  q. Quit
```

### Clean Up Old Locks

```bash
python -c "
from src.etl_pipeline import ETLPipeline
p = ETLPipeline()
removed = p.cleanup_old_locks(older_than_hours=24)
print(f'Removed {removed} old lock entries')
"
```

## Python API

### Load Data with Lock

```python
from src.etl_pipeline import ETLPipeline

pipeline = ETLPipeline(
    db_path='nyc_yellow_taxi.duckdb',
    pipeline_id='my_etl'
)

# Load with automatic registry management
stats = pipeline.load_year(
    year=2023,
    writer_id='worker_1',
    if_exists='create'  # or 'append'
)

print(f"Loaded {stats['rows_loaded']:,} rows in {stats['duration_sec']:.2f}s")
```

### Multi-Writer Scenario

```python
from src.etl_pipeline import ETLPipeline
from concurrent.futures import ThreadPoolExecutor

pipeline = ETLPipeline()

def load_year_safe(year):
    try:
        stats = pipeline.load_year(
            year=year,
            writer_id=f'worker_{year}',
            if_exists='create' if year == 2023 else 'append'
        )
        return {'year': year, 'status': 'success', 'rows': stats['rows_loaded']}
    except Exception as e:
        return {'year': year, 'status': 'failed', 'error': str(e)}

# Load all years as pool (registry locks coordinate safely)
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(load_year_safe, [2023, 2024, 2025]))

for result in results:
    print(f"{result['year']}: {result['status']}")
```

### Execute SQL Safely

```python
from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL

etl = DuckDBMultiWriterETL(
    db_path='nyc_yellow_taxi.duckdb'
)

# All writes coordinated via lock
result = etl.execute_sql_safe(
    query="""
        CREATE TABLE daily_summary AS
        SELECT 
            DATE(tpep_pickup_datetime) as day,
            COUNT(*) as trips,
            AVG(total_amount) as avg_fare
        FROM yellow_taxi_trips
        GROUP BY DATE(tpep_pickup_datetime)
    """,
    run_id='create_summary_20260417',
    writer_id='analyst_1'
)

print(f"Query completed in {result['duration_sec']:.2f}s")
```

### Check Registry

```python
from src.etl_pipeline import ETLPipeline

pipeline = ETLPipeline()

# Get status
status = pipeline.etl.get_registry_status()

# Show active locks
print(f"Active locks: {len(status['active_locks'])}")
for lock in status['active_locks']:
    print(f"  • {lock['writer_id']}: {lock['lock_id']}")

# Show completed runs
print(f"\nCompleted runs: {len(status['all_runs'])}")
for run in status['all_runs']:
    print(f"  • {run['run_id']}: {run['status']}")
    print(f"    Rows: {run['rows_written']:,}")
```

## Configuration

### Customize Timeout

```python
from src.etl_pipeline import ETLPipeline

# Default 300s, specify custom
pipeline = ETLPipeline(
    db_path='nyc_yellow_taxi.duckdb',
    timeout=600  # 10 minutes for large loads
)

stats = pipeline.load_year(2023)
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
from src.etl_pipeline import ETLPipeline

pipeline = ETLPipeline(
    db_path='alternative/path.duckdb',
    data_dir='/mnt/shared/taxi_data/NYC'
)

stats = pipeline.load_year(2023)
```

## Partition Analysis

```python
from src.partitioning_strategy import PartitionAnalyzer

analyzer = PartitionAnalyzer()

# Analyze available data
analysis = analyzer.analyze()
print(f"Total partitions: {analysis['total_partitions']}")
print(f"Recommended workers: {analysis['recommendations']['recommended_workers']}")

# Get load patterns
globs = analyzer.get_partition_globs()
for glob in globs:
    print(f"  Load pattern: {glob}")

# Estimate time
timing = analyzer.estimate_load_time()
print(f"Estimated time: {timing['estimated_duration_min']:.1f} minutes")
```

## Performance Tuning

### Increase Worker Timeout

For larger datasets, increase lock timeout:

```python
pipeline = ETLPipeline(timeout=900)  # 15 minutes
stats = pipeline.load_year(2023)
```

### Batch Insert (if needed)

```python
# Custom implementation with batching
from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL

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

### Archive Old Registry

```bash
# Backup registry before cleanup
cp data/registries/nyc_yellow_taxi_registry.json \
   data/registries/nyc_yellow_taxi_registry.backup.json

# Remove locks older than 7 days
python -c "
from src.etl_pipeline import ETLPipeline
p = ETLPipeline()
removed = p.cleanup_old_locks(older_than_hours=168)
print(f'Cleaned {removed} entries')
"
```

### Monitor Disk Usage

```bash
# Check database size
ls -lh nyc_yellow_taxi.duckdb

# Check registry size
wc -l data/registries/*.json
```

### Verify Data Integrity

```python
from src.etl_pipeline import ETLPipeline

pipeline = ETLPipeline()

# Validation
result = pipeline.validate_data()
print(f"Status: {result['status']}")
print(f"Total rows: {result['total_rows']:,}")
print(f"Year distribution: {result['year_distribution']}")
```

## Troubleshooting

### See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues

Quick reference:
```bash
# Check active locks
make registry-status

# Test basic functionality
make test

# View full registry
cat data/registries/nyc_yellow_taxi_registry.json | jq .

# Reset registry (WARNING: loses history)
rm data/registries/*.json
```

## Next Steps

1. Load all years: `make load-all`
2. Run benchmarks: `make etl-benchmark`
3. Analyze performance: `cat benchmark_results.json`
4. Scale to multiple workers (see ARCHITECTURE.md)
5. Deploy to production (see deployment guide)
