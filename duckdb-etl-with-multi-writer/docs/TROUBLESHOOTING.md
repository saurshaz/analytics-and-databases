# Troubleshooting Guide

## Common Issues and Solutions

### Lock Issues

#### "Lock timeout after X seconds"

**Symptom:**
```
TimeoutError: Failed to acquire lock after 15.2s. 
Writer: worker_2, Run: load_year_2024
```

**Cause:** Another writer is holding the lock longer than expected

**Solution:**

1. **Check who's holding the lock:**
```bash
make registry-status
```

2. **View active locks:**
```python
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
status = p.show_status()
for lock in status['active_locks']:
    print(f"Lock holder: {lock['writer_id']}")
    print(f"Since: {lock['acquired_at']}")
    print(f"Expires: {lock['expires_at']}")
```

3. **Increase timeout if needed:**
```python
pipeline = UnifiedETLPipeline(mode='etl', timeout=600)  # 10 minutes instead of 5
pipeline.run(years=[2023])
```

4. **Kill stuck worker (if crashed):**
```bash
# Find process
ps aux | grep etl_pipeline

# Kill it
kill -9 <pid>

# Locks expire automatically after timeout
```

---

#### "fcntl: Already locked" or "Resource temporarily unavailable"

**Cause:** Race condition in lock file access

**Solution:**
```bash
# Check lock file permissions
ls -la data/registries/*.lock

# Fix if needed
chmod 600 data/registries/*.lock

# Restart operation
make load-2023
```

---

### Registry Issues

#### "JSON decode error in registry file"

**Symptom:**
```
JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

**Cause:** Registry file corrupted or empty

**Solution:**

1. **Backup current registry:**
```bash
cp data/registries/nyc_yellow_taxi_registry.json \
   data/registries/backup_$(date +%s).json
```

2. **Restore from backup (if available):**
```bash
# Find recent backup
ls -ltr data/registries/backup_*.json

# Restore
cp data/registries/backup_<timestamp>.json \
   data/registries/nyc_yellow_taxi_registry.json
```

3. **Or reinitialize (loses history):**
```bash
# Remove registry files
rm data/registries/nyc_yellow_taxi*.json

# Next run creates fresh registry
python -c "
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
p.run(years=[2023])
"
```

---

### Data Loading Issues

#### "Parquet file not found"

**Symptom:**
```
Error: Could not open '/path/to/data/*.parquet', no matching files
```

**Cause:** Data directory path is incorrect

**Solution:**

1. **Verify data exists:**
```bash
ls -la NYC\ Yellow\ Taxi\ Record\ 23-24-25/2023/
```

2. **Check if data_dir is correct:**
```python
from src.partitioning_strategy import PartitionAnalyzer

analyzer = PartitionAnalyzer(
    data_dir='NYC Yellow Taxi Record 23-24-25'  # Verify this path
)
analysis = analyzer.analyze()
print(f"Found {analysis['total_partitions']} partitions")
```

3. **Or use symlink:**
```bash
# Create symlink to data in expected location
ln -s /mnt/external/taxi_data/NYC\ Yellow\ Taxi data/shared

# Then load relative to project
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl', data_dir='data/shared')
```

---

#### "Table 'yellow_taxi_trips' does not exist"

**Symptom:**
```
Catalog Error: Table with name yellow_taxi_trips does not exist!
```

**Cause:** Table hasn't been created yet

**Solution:**

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

p = UnifiedETLPipeline(mode='etl')

# Load data first (creates table)
p.run(years=[2023])

# Now queries work
p.validate_data()
```

---

#### "Rows mismatch: expected 45M, got 40M"

**Symptom:** Loaded fewer rows than expected

**Cause:** 
- Corrupted parquet files
- Incomplete download
- Query filtering

**Solution:**

1. **Verify source files:**
```bash
# Check file sizes
du -sh NYC\ Yellow\ Taxi\ Record\ 23-24-25/2023/*

# Estimate row count
python -c "
import duckdb
con = duckdb.connect()
result = con.execute(
    \"SELECT COUNT(*) FROM read_parquet('NYC.../2023/*.parquet')\"
).fetchall()
print(f'Actual rows in source: {result[0][0]:,}')
"
```

2. **Re-download if corrupted:**
```bash
# Remove incomplete data
rm NYC\ Yellow\ Taxi\ Record\ 23-24-25/2023/*.parquet

# Re-download (follow NYC TLC guide)
```

3. **Reload database:**
```bash
rm nyc_yellow_taxi.duckdb
python -c "
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
p.run(years=[2023])
"
```

---

### Performance Issues

#### "Load is very slow (< 1M rows/sec)"

**Symptom:**
```
Throughput: 450,000 rows/sec (expected: 2,800,000)
```

**Cause:**
- High system load
- Slow disk (HDD instead of SSD)
- Large number of retries (lock contention)

**Solution:**

```python
# Check lock contention
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
metrics = p.show_metrics()

# Count retries in locks
all_locks = status['active_locks'] + \
    [l for r in status['all_runs'] for l in [r.get('lock_id', '')]]

print(f"Lock acquisitions: {len(all_locks)}")
```

If many retries:
1. Use single-writer mode (no concurrency)
2. Increase timeout
3. Use SSD disk

---

#### "Queries are slow"

**Symptom:**
```
Query "SELECT COUNT(*)" takes 30 seconds
```

**Cause:**
- DuckDB not using index
- Memory pressure
- Query not optimal

**Solution:**

```python
import duckdb

con = duckdb.connect('nyc_yellow_taxi.duckdb')

# Use EXPLAIN to see query plan
result = con.execute("""
    EXPLAIN 
    SELECT COUNT(*) FROM yellow_taxi_trips
""").fetchall()

print(result)

# For complex queries, create indexes
con.execute("""
    CREATE INDEX idx_date ON yellow_taxi_trips (tpep_pickup_datetime)
""")
```

---

### Multi-Writer Issues

#### "All writers fail with timeout"

**Symptom:**
```
Worker 1, 2, 3: All timeout after 300s
```

**Cause:** 
- One worker crashed holding lock
- Lock file permissions issue
- Registry corruption

**Solution:**

1. **Check lock expiration:**
```bash
# Each lock has expires_at timestamp
cat data/registries/nyc_yellow_taxi_registry.json | jq '.locks[0].expires_at'

# If in past, force cleanup
python -c "
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
print('Registry locks managed by unified pipeline')
"
```

2. **Verify lock file:**
```bash
# Check permissions
ls -la data/registries/*.lock

# Recreate if missing
touch data/registries/nyc_yellow_taxi_registry.lock
chmod 600 data/registries/nyc_yellow_taxi_registry.lock
```

3. **Check disk space:**
```bash
df -h

# Ensure >10GB free for database
```

---

### Installation Issues

#### "ModuleNotFoundError: No module named 'duckdb'"

**Solution:**
```bash
# Reinstall dependencies
pip install -r requirements.txt

# Or manually
pip install duckdb==0.8.1
```

---

#### "fcntl module not found" (Windows)

**Symptom:** Windows-specific error

**Solution:**
```bash
# Use WSL2 (Windows Subsystem for Linux)
wsl --install

# Or run on Linux/macOS
```

---

### Registry Cleanup

#### "Registry file is very large (>100MB)"

**Cause:** Years of runs accumulated

**Solution:**

```python
from src.unified_etl_pipeline import UnifiedETLPipeline

p = UnifiedETLPipeline(mode='etl')

# Clean up runs older than 30 days
import json
from pathlib import Path

registry_path = Path('data/registries/nyc_yellow_taxi_registry.json')
data = json.load(open(registry_path))

# Keep only recent runs
from datetime import datetime, timedelta
cutoff = datetime.utcnow() - timedelta(days=30)

data['runs'] = [
    r for r in data['runs']
    if datetime.fromisoformat(r.get('started_at', datetime.utcnow().isoformat())) > cutoff
]

# Save cleaned registry
json.dump(data, open(registry_file, 'w'), indent=2)
print("Registry cleaned")
```

---

## Debugging

### Enable Verbose Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)

from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
p.run(years=[2023])  # Will print debug info
```

### Inspect Registry File

```bash
# Pretty-print registry
cat data/registries/nyc_yellow_taxi_registry.json | jq .

# Show only recent runs
cat data/registries/nyc_yellow_taxi_registry.json | jq '.runs[-5:]'

# Show active locks
cat data/registries/nyc_yellow_taxi_registry.json | jq '.locks[] | select(.status=="active")'
```

### Test Lock Mechanism

```python
from src.registry_lock_manager import RegistryLockManager

registry = RegistryLockManager(db_path='test.duckdb')

# Test acquire/release
with registry.acquire_lock('test_run', 'test_worker', timeout=10):
    print("Lock acquired successfully")

print("Lock released")

# Check registry
import json
data = json.load(open(registry.registry_file))
print(f"Total locks: {len(data['locks'])}")
```

---

## Getting Help

### Check Status

```bash
make registry-status
make test
```

### View Logs

```bash
# If using Docker
docker logs etl-worker

# View DuckDB logs
tail -f /var/log/duckdb.log
```

### Collect Debug Info

```bash
echo "=== System ==="
uname -a
df -h

echo "=== Python ==="
python --version
pip show duckdb

echo "=== Files ==="
ls -la data/registries/
ls -lh *.duckdb

echo "=== Registry ==="
cat data/registries/nyc_yellow_taxi_registry.json | jq -c '.locks, .runs | length'
```

---

## Recovery Procedures

### Complete Reset

```bash
# WARNING: This deletes all data and history

# Stop all workers
pkill -f etl_pipeline

# Remove database
rm nyc_yellow_taxi.duckdb

# Remove registry
rm data/registries/*.json data/registries/*.lock

# Reload
python -c "
from src.unified_etl_pipeline import UnifiedETLPipeline
p = UnifiedETLPipeline(mode='etl')
p.run(years=[2023])
"
```

### Partial Recovery

```bash
# Keep database, reset registry
rm data/registries/*.json data/registries/*.lock

# Registry will be recreated on next write
# Existing data in database remains intact
```

---

## Performance Baseline

For reference, expected performance on modern hardware (SSD, 8-core CPU, 16GB RAM):

| Operation | Expected Duration | Notes |
|-----------|------------------|-------|
| Load 2023 (45M rows) | ~15-20s | Single writer |
| Load 2024 (50M rows) | ~18-22s | Single writer |
| Load all years | ~45-55s | Sequential |
| Single query (aggregation) | <100ms | Cached |
| Row count query | ~50-100ms | Full scan |

If your numbers are significantly slower, see "Performance Issues" above.

---

## Still Stuck?

1. Check [ARCHITECTURE.md](ARCHITECTURE.md) for design details
2. Check [USAGE.md](USAGE.md) for examples
3. Review [../blog/BLOG_POST.md](../blog/BLOG_POST.md) for background
4. Run tests: `make test-multiwriter`
5. Check DuckDB docs: https://duckdb.org/docs/
