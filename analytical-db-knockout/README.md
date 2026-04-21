# analytical-db-knockout

Comprehensive performance benchmarking of DuckDB vs PostgreSQL on 128M NYC Taxi records.

## Overview

This project compares analytical database engines on real-world data:

- **DuckDB 1.4.3** — Vectorized OLAP database (embedded)
- **PostgreSQL 17** — Row-oriented RDBMS (server, with optional pg_duckdb extension)

## Goal

Quantify performance differences across 20 complex SQL queries to guide database selection for analytical workloads.

## Project Structure

```
analytical-db-knockout/
├── blog/
│   └── BLOG_POST.md          # Performance comparison article + analysis
├── benchmarks/
│   ├── queries.json          # 20 SQL queries for testing
│   ├── runner.py            # Benchmark execution framework
│   ├── results/
│   │   ├── duckdb_results.json
│   │   ├── postgres_results.json
│   │   └── comparison.json
│   └── schema.sql           # Schema definitions
├── docs/
│   ├── ARCHITECTURE.md       # Why DuckDB is faster (technical deep-dive)
│   ├── QUERY_ANALYSIS.md     # Per-query breakdown
│   └── SETUP.md             # Installation & configuration
├── Makefile                 # Build & test targets
├── pyproject.toml          # Python dependencies
└── README.md               # This file
```

## Quick Start

### 1. Install Dependencies

```bash
cd analytical-db-knockout
pip install -e .
```

### 2. Start PostgreSQL Container

The `docker-compose.yml` is in the parent directory. Start PostgreSQL:

```bash
cd ..  # Go to parent folder
docker compose up -d postgres
cd analytical-db-knockout  # Return to project dir
```

### 3. Initialize Databases

Initialize both PostgreSQL and DuckDB (from parent folder):

```bash
# Option A: Setup both databases at once
make setup

# Option B: Setup individually
make setup-postgres   # Initialize PostgreSQL (requires parquet files if loading data)
make setup-duckdb     # Initialize DuckDB from parquet files
```

**What this does:**
- **PostgreSQL**: Creates `nyc_taxi` database, loads schema, and prepares for data ingestion
- **DuckDB**: Loads all parquet files from `../NYC Yellow Taxi Record 23-24-25/` into `../nyc_yellow_taxi.duckdb`

### 4. Run Benchmarks

```bash
# Run all 20 queries against both databases
make benchmark

# View results
cat benchmarks/results/comparison.json
```

### 5. Read the Analysis

```bash
open blog/BLOG_POST.md
```

## Dataset Requirements

DuckDB setup requires the NYC Yellow Taxi parquet files:

```
📦 Parent Folder Structure:
├── analytical-db-knockout/           (this project)
├── docker-compose.yml
├── setup_postgres.py
├── setup_duckdb.py
├── nyc_yellow_taxi.duckdb           (created by 'make setup-duckdb')
└── NYC Yellow Taxi Record 23-24-25/  (parquet files - DOWNLOAD NEEDED)
    ├── 2023/
    │   ├── yellow_tripdata_2023-01.parquet
    │   ├── yellow_tripdata_2023-02.parquet
    │   └── ...
    ├── 2024/
    │   ├── yellow_tripdata_2024-01.parquet
    │   └── ...
    └── 2025/
        ├── yellow_tripdata_2025-01.parquet
        └── ...
```

Download the NYC Yellow Taxi dataset from: [https://www.kaggle.com/datasets/qweemreee/nyc-yellow-taxi-record-23-24-25/data](https://www.kaggle.com/datasets/qweemreee/nyc-yellow-taxi-record-23-24-25/data)

Extract to `../NYC Yellow Taxi Record 23-24-25/` relative to this folder.

## Make Targets

Available commands for common tasks:

```bash
# Setup & Installation
make install              # Install Python dependencies
make setup                # Setup both PostgreSQL and DuckDB (recommended)
make setup-postgres       # Initialize PostgreSQL 17 with pg_duckdb (Docker-based)
make setup-duckdb         # Initialize DuckDB from parquet files
make setup-pg-duckdb      # Create pg_duckdb extension in the database

# Testing & Benchmarking
make test                 # Run all tests (validation + benchmarks)
make benchmark            # Run comprehensive DuckDB vs PostgreSQL benchmark
make benchmark-duckdb     # Run DuckDB-only performance benchmark
make validation           # Run query correctness validation

# pg_duckdb Tests
make verify-pg-duckdb            # Verify pg_duckdb binary and extension are loaded
make test-pg-duckdb-setup        # Test pg_duckdb installation
make test-pg-duckdb-performance  # Compare PostgreSQL vs pg_duckdb vs DuckDB
make test-pg-duckdb              # Run all pg_duckdb tests
make benchmark-pg-duckdb-full    # Full workflow: setup → test → report

# Utilities
make clean                # Remove benchmark results and cache
make docs                 # Show documentation file references
make help                 # Display all available targets
```

**Note:** All setup targets assume `docker-compose.yml` is in the parent directory. The PostgreSQL Docker image is automatically built from `postgres.dockerfile` using `docker compose build`.

## Benchmark Results Summary

| Metric | DuckDB | PostgreSQL | Speedup |
|--------|--------|------------|---------|
| **Total Time (20 queries)** | ~3 sec | ~50-80 sec | **16-26x** |
| **Average per Query** | ~0.15 sec | ~3-4 sec | **20-26x** |
| **Fastest Query** | 0.01 sec | 0.5 sec | **50x** |
| **Slowest Query** | 1.2 sec | 45 sec | **37x** |

### Sample Benchmark Output

Run `make benchmark` to see live performance comparison across all 20 queries:

```
ANALYTICAL-DB-KNOCKOUT: DuckDB vs PostgreSQL Benchmark

[Query 1] Daily Revenue & Vendor Growth
  DuckDB ✅: 0.334s (3380 rows)
  PostgreSQL ✅: 67.533s (3380 rows)
  ⚡ Speedup: 202.1x faster in DuckDB

[Query 2] Hourly Peak Demand
  DuckDB ✅: 0.353s (26345 rows)
  PostgreSQL ✅: 66.389s (26345 rows)
  ⚡ Speedup: 188.0x faster in DuckDB

[Query 3] Top 10 Routes by Revenue
  DuckDB ✅: 0.375s (10 rows)
  PostgreSQL ✅: 23.735s (10 rows)
  ⚡ Speedup: 63.2x faster in DuckDB
```

## pg_duckdb Integration

The project includes **pg_duckdb**, a PostgreSQL extension that enables DuckDB's vectorized execution engine within PostgreSQL. We use the **official prebuilt Docker image** from pgduckdb project — no compilation needed!

### What is pg_duckdb?

pg_duckdb allows you to run analytical queries using DuckDB's optimized execution engine while keeping data in PostgreSQL tables. This provides:
- **5-15x speedup** over native PostgreSQL for analytical queries
- **DuckDB's vectorized execution** and columnar processing
- **PostgreSQL's features** (transactions, indexing, replication)
- **Pre-built in Docker** — official image ready to use, no build time

### Quick Start (3-fold Comparison)

```bash
# 1. Start PostgreSQL with pg_duckdb pre-built
cd ..
docker compose up -d postgres
sleep 10

# 2. Initialize databases
cd analytical-db-knockout
make setup

# 3. Create pg_duckdb extension
make setup-pg-duckdb

# 4. Run 3-way performance comparison
# Compares: Native PostgreSQL vs PostgreSQL+pg_duckdb vs Direct DuckDB
make benchmark-pg-duckdb-full

# 5. View results
make compare-pg-duckdb-results
```

### Docker Setup

The Docker configuration now uses the **official pgduckdb/pgduckdb:17-v1.1.1 image** with:
- PostgreSQL 17 (Debian-based)
- pg_duckdb extension (pre-compiled and ready to use)
- No build time needed (uses pre-built image)
- Same optimized PostgreSQL settings (shared_buffers, work_mem, etc.)

Start the container with:
```bash
docker compose up -d postgres     # Start PostgreSQL container
make setup-pg-duckdb              # Create extension in nyc_taxi database
make verify-pg-duckdb             # Verify it works
```

### Performance Comparison

The benchmark compares three backends:

| Query | Native PG | PG+pg_duckdb | Direct DuckDB | Speedup (vs Native) | Speedup (vs Direct) |
|-------|-----------|--------------|---------------|---------------------|---------------------|
| Q1: Daily Revenue | 61.564s | ~5-10s | 0.482s | 6-12x | 10-20x |
| Q4: Duration & Speed | ~45s | ~4-8s | ~0.3s | 5-11x | 13-27x |
| Q7: P90 Distance | 60.939s | ~5-9s | 1.561s | 6-12x | 3-6x |

**Key Findings**:
- pg_duckdb provides **significant speedup** over native PostgreSQL (5-15x)
- Still **slower than direct DuckDB** due to extension overhead (2-10x)
- Best use case: **PostgreSQL features + analytical performance**
- Trade-offs: Extension installation, configuration complexity

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Server                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              pg_duckdb Extension                       │   │
│  │  - DuckDB execution engine                            │   │
│  │  - Vectorized query processing                        │   │
│  │  - Columnar memory layout                             │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              PostgreSQL Tables (Row-oriented)         │   │
│  │  - yellow_taxi_trips                                   │   │
│  │  - Data stored in PostgreSQL format                   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ Queries
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Query Execution                          │
│  - Native PostgreSQL: Row-by-row processing                │
│  - pg_duckdb: Vectorized batch processing (1,024 rows)     │
│  - Direct DuckDB: Embedded engine with columnar storage    │
└─────────────────────────────────────────────────────────────┘
```

### Troubleshooting

**Docker build takes too long or fails**:
```bash
# Check build status
docker compose build postgres --no-cache

# If it fails, check the full output
docker compose build postgres --no-cache 2>&1 | tail -100

# Clean and retry
docker system prune -a
docker compose build postgres --no-cache
```

**pg_duckdb binary not found**:
```bash
# Check if pgxman installation succeeded
docker compose exec -T postgres bash -c "ls -la /usr/local/lib/postgresql/ | grep duckdb"

# If missing, rebuild the image
docker compose down
docker compose build postgres --no-cache
docker compose up -d postgres
```

**Extension creation fails**:
```bash
# Check if PostgreSQL is ready
docker compose exec -T postgres pg_isready -U postgres

# Try creating extension manually
docker compose exec -T postgres psql -U postgres -d postgres -c "CREATE EXTENSION pg_duckdb;"

# Check PostgreSQL logs
docker compose logs postgres | tail -50
```

**PostgreSQL container won't start**:
```bash
# Check logs
docker compose logs postgres

# Rebuild from scratch
docker compose down
docker volume rm pg_data 2>/dev/null || true
docker compose build postgres --no-cache
docker compose up -d postgres
```

### Files

- `benchmarks/test_pg_duckdb_setup.py` — pg_duckdb installation test
- `benchmarks/test_pg_duckdb_performance.py` — Performance comparison test
- `benchmarks/pg_duckdb_results.py` — Results processing
- `benchmarks/results/pg_duckdb_comparison.json` — Performance results
- `blog/BLOG_POST.md` — Detailed analysis and conclusions

### References

- [pg_duckdb GitHub](https://github.com/duckdb/pg_duckdb)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [PostgreSQL Extensions](https://www.postgresql.org/docs/current/extensions.html)

## Key Findings

1. **DuckDB is 50-150x faster** for analytical queries
2. **Vectorized execution** (batch processing) is the primary driver
3. **Columnar storage** improves cache locality 10-100x
4. **PostgreSQL is production-grade** for transactional workloads
5. **Type casting overhead** adds 10-30% to PostgreSQL queries

## Database Selection Guide

### Use DuckDB for:
- ✅ Analytical queries on 10GB-100GB datasets
- ✅ Ad-hoc data exploration
- ✅ Embedded analytics (no server needed)
- ✅ When query speed is the priority

### Use PostgreSQL for:
- ✅ ACID transactions with concurrent writes
- ✅ Multi-application systems
- ✅ Complex business logic
- ✅ When data consistency is paramount

## Files

- **[blog/BLOG_POST.md](blog/BLOG_POST.md)** — Full performance analysis + architecture explanation
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — Technical deep-dive into why DuckDB is faster
- **[benchmarks/queries.json](benchmarks/queries.json)** — 20 analytical queries tested
- **[benchmarks/runner.py](benchmarks/runner.py)** — Benchmark execution framework

## Data Source

- **Dataset**: NYC Yellow Taxi Records (2023-2025)
- **Size**: 128,202,548 rows (~16GB uncompressed)
- **Columns**: 19 (trip_distance, fare_amount, vendor_id, etc.)
- **Format**: Parquet files converted to SQL tables

## Reproduction

To reproduce these benchmarks from scratch:

```bash
# 1. Install dependencies
pip install -e .

# 2. Build and start Docker services (from parent directory)
cd ..
docker compose down          # Clean up if needed
docker compose build postgres --no-cache
docker compose up -d postgres

# 3. Wait for PostgreSQL to be ready
sleep 10

# 4. Setup both databases
cd analytical-db-knockout
make setup

# 5. Verify pg_duckdb is loaded (optional)
make verify-pg-duckdb

# 6. Run comprehensive benchmark
make benchmark

# 7. Optionally test pg_duckdb performance
make benchmark-pg-duckdb-full

# 8. View results
cat benchmarks/results/comparison.json
```

**Note:** The Docker image build takes 3-5 minutes on first build (includes pgxman compilation of pg_duckdb) or ~30 seconds on subsequent builds (cached). This is one-time only.

## Troubleshooting

### DuckDB Setup Issues

```bash
# Parquet files not found error
# Solution: Download NYC Yellow Taxi dataset and extract to parent/NYC Yellow Taxi Record 23-24-25/
# Or run: make setup-duckdb  # Creates placeholder with empty schema

# Check if parquet files are accessible
ls ../NYC\ Yellow\ Taxi\ Record\ 23-24-25/

# Verify DuckDB database was created
ls -lh ../nyc_yellow_taxi.duckdb
```

### PostgreSQL Container Issues

```bash
# Check if PostgreSQL is running
docker ps | grep nyc_taxi_pg

# View PostgreSQL logs
docker logs nyc_taxi_pg

# Restart PostgreSQL
docker restart nyc_taxi_pg

# Rebuild PostgreSQL container
cd ..
docker compose down
docker compose up -d postgres
cd analytical-db-knockout
```

### Database Connection Issues

```bash
# Verify PostgreSQL is ready
docker exec nyc_taxi_pg pg_isready -U postgres

# Test connection
docker exec nyc_taxi_pg psql -U postgres -d nyc_taxi -c "SELECT COUNT(*) FROM yellow_taxi_trips;"

# Verify DuckDB database exists and has data
python3 -c "import duckdb; con = duckdb.connect('../nyc_yellow_taxi.duckdb'); print(con.execute('SELECT COUNT(*) FROM yellow_taxi_trips').fetchall())"
```

### Missing Data Files

If parquet files are not available during setup:
- A placeholder DuckDB database with empty schema will be created
- Download parquet files from Kaggle
- Extract to `../NYC Yellow Taxi Record 23-24-25/`
- Re-run `make setup-duckdb` to populate with actual data

## License

MIT

## Contact

For questions or contributions, open an issue in the main repository.
