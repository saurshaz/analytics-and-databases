# Analytics & Databases Research

Central hub for comprehensive database performance analysis and ETL system design with NYC Yellow Taxi data (128M+ records across 2023-2025).

## 📋 Table of Contents

This workspace contains two complementary research projects exploring analytical database performance and production-grade ETL coordination:

### 1. 📊 [Analytical DB Knockout](./analytical-db-knockout/)

**Performance benchmarking and comparison of DuckDB vs PostgreSQL**

A comprehensive study quantifying performance differences across 20 complex SQL queries on real-world analytical workloads. Tests embed vectorized OLAP (DuckDB 0.9.2) against row-oriented RDBMS (PostgreSQL 15) using 128M NYC Taxi records.

Prerequisites : download the dataset into root folder in a folder named [`NYC Yellow Taxi Record 23-24-25`](https://www.kaggle.com/datasets/qweemreee/nyc-yellow-taxi-record-23-24-2) (with parquet files)

**Key Features:**
- 20 complex SQL queries covering aggregations, joins, window functions
- Automated benchmark runner with stable result collection
- Performance comparison analysis with visualizations
- Technical deep-dive into why DuckDB outperforms PostgreSQL for OLAP
- Complete blog post with findings and recommendations

**Quick Start:**

```bash
cd analytical-db-knockout
pip install -e .
make benchmark
```

**Learn More:** [analytical-db-knockout/README.md](./analytical-db-knockout/README.md)

---

### 2. 🔄 [DuckDB ETL with Multi-Writer Coordination](./duckdb-etl-with-multi-writer/)

**Registry locking solution for safe concurrent ETL operations on DuckDB**

A production-grade ETL pipeline demonstrating how to safely coordinate multi-writer access to DuckDB through file-based registry locking. Automatically partitions data, detects schema changes, and maintains an audit trail of all operations.

**Key Features:**
- Registry-based locking mechanism for concurrent writers
- Automatic data partitioning and optimization
- Full ETL pipeline with data loading and transformation
- Multi-writer safety without conflicts
- Complete audit trail in JSON format
- Parallel partition loading

**Quick Start:**
```bash
cd duckdb-etl-with-multi-writer
make venv-install && make etl-run && make test-multiwriter
```

**Learn More:** [duckdb-etl-with-multi-writer/README.md](./duckdb-etl-with-multi-writer/README.md)

---

## 📁 Data

NYC Yellow Taxi data is available in the workspace:
- **CSV source:** `yellow_taxi_trips.csv` (2023-2025 combined)
- **Organized by year:** `NYC Yellow Taxi Record 23-24-25/` directory
- **Pre-loaded databases:**
  - `nyc_yellow_taxi.duckdb` (root)
  
## 🚀 Getting Started

Both projects are independently executable. Start with either:

1. **Performance Analysis First:** If you want to understand benchmarking methodology
   ```bash
   cd analytical-db-knockout && make benchmark
   ```

2. **ETL Pipeline First:** If you want to explore multi-writer coordination
   ```bash
   cd duckdb-etl-with-multi-writer && make etl-run
   ```

## 📚 Documentation

- [Analytical DB Knockout Architecture](./analytical-db-knockout/docs/ARCHITECTURE.md) — Why DuckDB is faster
- [ETL Architecture](./duckdb-etl-with-multi-writer/docs/ARCHITECTURE.md) — Multi-writer coordination design
- [Blog Post: Performance Comparison](./analytical-db-knockout/blog/BLOG_POST.md) — Detailed findings and analysis

## 🔧 Common Tasks

| Task | Command | Project |
|------|---------|---------|
| Run performance benchmarks | `make benchmark` | analytical-db-knockout |
| Test ETL multi-writer safety | `make test-multiwriter` | duckdb-etl-with-multi-writer |
| Generate comparison reports | `make results` | analytical-db-knockout |
| Load all 2023-2025 data | `make etl-run` | duckdb-etl-with-multi-writer |
| View registry status | `make registry-status` | duckdb-etl-with-multi-writer |

## 📖 Project Purposes

### Analytical DB Knockout
- **Goal:** Quantify performance differences for analytical workloads
- **Use Case:** Deciding between DuckDB and PostgreSQL for OLAP queries
- **Outcome:** Data-driven database selection guidance

### DuckDB ETL with Multi-Writer
- **Goal:** Demonstrate safe concurrent access patterns for DuckDB
- **Use Case:** Building production ETL systems with shared database writes
- **Outcome:** Reusable registry locking mechanism and best practices

---

**Last Updated:** April 2026  
**Data:** NYC Yellow Taxi Trip Records (128M+ trips, 2023-2025)
