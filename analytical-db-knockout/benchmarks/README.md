# Benchmark Test Suite

Complete pytest-based benchmark framework for comparing DuckDB and PostgreSQL performance on analytical workloads.

## Quick Start

```bash
# Install dependencies
pip install -r ../requirements.txt

# Run validation tests (query correctness)
pytest -m validation

# Run performance benchmarks
pytest -m benchmark

# Run specific benchmark
pytest benchmarks/test_duckdb_vs_postgres.py -v -s
```

## Test Files

| File | Purpose |
|------|---------|
| `conftest.py` | Pytest fixtures and configuration |
| `benchmark_timing.py` | Timing utilities and result aggregation |
| `benchmark_results.py` | Result persistence (JSON snapshots) |
| `test_duckdb_vs_postgres.py` | Main benchmark: DuckDB vs PostgreSQL comparison |
| `test_duckdb_queries.py` | DuckDB performance profiling |
| `test_query_correctness.py` | Query validation and row count checks |

## Pytest Markers

### `@pytest.mark.benchmark`
Performance comparison tests. Runs queries multiple times and measures execution time.

```bash
pytest -m benchmark               # Run all benchmarks
pytest -m benchmark -v -s        # Verbose with output
```

### `@pytest.mark.validation`
Query correctness tests. Verify queries execute without errors and return results.

```bash
pytest -m validation             # Run validation only
```

## Results Storage

Benchmark results are automatically saved to:
- `results/duckdb_vs_postgres_latest.json` — Main comparison results
- `results/duckdb_results_latest.json` — DuckDB-specific metrics
- `results/postgres_results_latest.json` — PostgreSQL-specific metrics

Example result structure:
```json
{
  "name": "duckdb_vs_postgres",
  "generated_at": "2026-04-18T10:30:45.123456+00:00",
  "duckdb": {
    "1": {
      "status": "PASS",
      "rows": 3380,
      "avg_time": 0.482,
      "error": ""
    }
  },
  "postgres": {
    "1": {
      "status": "PASS",
      "rows": 3380,
      "avg_time": 61.564,
      "error": ""
    }
  },
  "summary": {
    "speedup": {
      "duckdb_vs_postgres": 127.6,
      "factor": "127.6x"
    }
  }
}
```

## Environment Variables

Control database connections via environment variables:

```bash
# PostgreSQL
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=nyc_taxi
export PG_USER=postgres
export PG_PASS=postgres
```

## Running from Root Directory

```bash
# From /home/dev/Desktop/litess/duckdbws/analytical-db-knockout
make install          # Install dependencies
make benchmark        # Run all benchmarks
make validation       # Run validation tests
make test             # Run both
```

## Example Output

```
[Query 1] Daily Revenue & Vendor Growth (LAG Window Function)
  Complexity: Calculate day-over-day vendor revenue growth
  DuckDB ✅: 0.482s (3380 rows)
  PostgreSQL ✅: 61.564s (3380 rows)
  ⚡ Speedup: 127.6x faster in DuckDB

[Query 7] P90 Distance by Vendor/Month (Percentile Aggregation)
  Complexity: 90th percentile trip distance by vendor and month
  DuckDB ✅: 1.561s (100 rows)
  PostgreSQL ✅: 60.939s (100 rows)
  ⚡ Speedup: 39.0x faster in DuckDB

================================================================================
SUMMARY
================================================================================

DUCKDB:
  Total Time: 3.245s
  Avg/Query:  0.162s
  Min Time:   0.010s
  Max Time:   1.823s
  Queries:    20/20

POSTGRES:
  Total Time: 68.392s
  Avg/Query:  3.420s
  Min Time:   0.500s
  Max Time:   68.392s
  Queries:    20/20

🏆 Overall Speedup: 21.1x
```

## Benchmark Methodology

Each query is run with:
- **1 warmup run** (discard results, allows query plan caching)
- **2 measured runs** (average these results)
- **Wall-clock timing** (includes all overhead)
- **Cold database state** (between different query types)

This ensures fair comparison while accounting for JIT compilation and query planning overhead.

## Adding New Queries

1. Add query to `queries.json`:
   ```json
   {
     "id": 21,
     "name": "New Query Name",
     "description": "What this query measures",
     "sql": "SELECT ..."
   }
   ```

2. Rerun benchmarks:
   ```bash
   make benchmark
   ```

## Troubleshooting

### PostgreSQL Connection Failed
```bash
# Ensure PostgreSQL is running
docker compose up -d postgres

# Verify connection
psql -h localhost -U postgres -d nyc_taxi
```

### DuckDB Database Not Found
```bash
# Ensure NYC Taxi data is loaded into DuckDB
# The benchmark expects at least 'yellow_taxi' table
```

### Tests Skipped
Benchmarks gracefully skip if databases are unavailable:
- DuckDB: requires `nyc_yellow_taxi.duckdb` in parent directory
- PostgreSQL: requires running instance on `localhost:5432`

## See Also

- [BLOG_POST.md](../blog/BLOG_POST.md) — Performance analysis and findings
- [ARCHITECTURE.md](../docs/ARCHITECTURE.md) — Technical deep-dive
- [README.md](../README.md) — Project overview
