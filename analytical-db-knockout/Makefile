.PHONY: help install setup setup-postgres setup-duckdb test benchmark benchmark-duckdb validation clean docs test-help setup-pg-duckdb verify-pg-duckdb test-pg-duckdb-setup test-pg-duckdb-performance benchmark-pg-duckdb compare-pg-duckdb-results test-pg-duckdb benchmark-pg-duckdb-full

# ============================================================================
# HELP TARGET
# ============================================================================
# Display available Make targets and their descriptions
# Usage: make help
help:
	@echo "analytical-db-knockout — DuckDB vs PostgreSQL Benchmarking"
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install dependencies"
	@echo "  make setup            Setup both PostgreSQL and DuckDB databases"
	@echo "  make setup-postgres   Initialize PostgreSQL database (Docker)"
	@echo "  make setup-duckdb     Initialize DuckDB database (from parquet files)"
	@echo ""
	@echo "Testing & Benchmarking:"
	@echo "  make test             Run all tests (validation + benchmarks)"
	@echo "  make benchmark        Run comprehensive benchmark suite"
	@echo "  make benchmark-duckdb Run DuckDB-only benchmark"
	@echo "  make validation       Run query correctness validation"
	@echo ""
	@echo "pg_duckdb (PostgreSQL + DuckDB vectorized execution):"
	@echo "  make setup-pg-duckdb              Create pg_duckdb extension in database"
	@echo "  make verify-pg-duckdb             Verify pg_duckdb extension is loaded"
	@echo "  make test-pg-duckdb-setup         Test pg_duckdb installation"
	@echo "  make test-pg-duckdb-performance   3-way comparison: PostgreSQL vs pg_duckdb vs DuckDB"
	@echo "  make test-pg-duckdb               Run all pg_duckdb tests"
	@echo "  make benchmark-pg-duckdb-full     Full workflow: setup → verify → test → report"
	@echo "  make compare-pg-duckdb-results    Display performance comparison results"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean            Remove benchmark results"
	@echo ""
	@echo "Documentation:"
	@echo "  make docs             Show documentation files"

# ============================================================================
# SETUP TARGETS - Initialize Environments
# ============================================================================

# Install all Python dependencies from requirements.txt
# Installs: pytest, duckdb, psycopg2, pandas, etc.
# Usage: make install
install:
	pip3 install -r requirements.txt

# Master setup target: Initializes both PostgreSQL and DuckDB databases
# Prerequisite: docker-compose.yml must be in parent directory
# Creates: ../nyc_yellow_taxi.duckdb and nyc_taxi PostgreSQL database
# Usage: make setup
setup: setup-postgres setup-duckdb
	@echo "\n✅ Both databases are ready!"
	@echo "   Run 'make benchmark' to compare performance"

# Initialize PostgreSQL 17 container with nyc_taxi database
# Features: Uses official pgduckdb/pgduckdb:17-v1.1.1 image with pg_duckdb extension pre-built
# Runs: docker compose up for PostgreSQL service
# Requires: ../docker-compose.yml (in parent directory)
# Runs: ../setup_postgres.py to create schema and prepare database
# Usage: make setup-postgres
setup-postgres:
	@echo "Setting up PostgreSQL 17 with pg_duckdb extension (pre-built in Docker image)..."
	cd .. && docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	sleep 10
	python3 ../setup_postgres.py

# Initialize DuckDB database from parquet files
# Loads: NYC Yellow Taxi parquet files from ../NYC Yellow Taxi Record 23-24-25/
# Creates: ../nyc_yellow_taxi.duckdb with yellow_taxi_trips table
# Falls back to empty schema if parquet files not found
# Usage: make setup-duckdb
setup-duckdb:
	@echo "Setting up DuckDB database..."
	python3 ../setup_duckdb.py

# ============================================================================
# TESTING & BENCHMARKING TARGETS
# ============================================================================

# Run ALL tests: both validation and benchmark tests
# Executes: Correctness validation tests + Full benchmark suite
# Useful for: Complete test coverage before deployment
# Usage: make test
test:
	@echo "Running all tests (validation + benchmarks)...\n"
	python3 -m pytest -m "validation or benchmark" --tb=short

# MAIN BENCHMARK: Compare DuckDB vs PostgreSQL performance
# Requires: Both databases initialized (run 'make setup' first)
# Executes: All 20 analytical queries on both databases
# Output: Prints speedup factors, execution times, and side-by-side comparison
# Saves results to: benchmarks/results/comparison.json
# Usage: make benchmark
benchmark:
	@echo "Running comprehensive DuckDB vs PostgreSQL benchmark...\n"
	python3 -m pytest -m benchmark benchmarks/test_duckdb_vs_postgres.py -v -s

# DuckDB-only performance analysis
# Executes: Profile DuckDB query execution times across all 20 queries
# Useful for: Identifying slow queries or performance regressions in DuckDB
# Output: Performance timing data and statistics
# Usage: make benchmark-duckdb
benchmark-duckdb:
	@echo "Running DuckDB-only performance benchmark...\n"
	python3 -m pytest -m benchmark benchmarks/test_duckdb_queries.py -v -s

# Query correctness validation
# Verifies: All 20 queries execute successfully without errors
# Checks: Row counts, result validity, and schema correctness
# Useful for: Regression testing after configuration changes
# Usage: make validation
validation:
	@echo "Running query correctness validation...\n"
	python3 -m pytest -m validation benchmarks/test_query_correctness.py -v -s

# pg_duckdb Setup and Testing
# ============================================================================
# Note: pg_duckdb is pre-built in the official pgduckdb/pgduckdb Docker image
# No compilation needed - just create the extension in the database

# Setup pg_duckdb extension in PostgreSQL database
# Usage: make setup-pg-duckdb
setup-pg-duckdb:
	@echo "Setting up pg_duckdb extension in PostgreSQL..."
	@cd .. && docker compose exec -T postgres psql -U postgres -d nyc_taxi -c "CREATE EXTENSION IF NOT EXISTS pg_duckdb;" && echo "✅ pg_duckdb extension created in nyc_taxi database"

# Verify pg_duckdb extension is loaded and accessible
# Creates the extension if not exists and verifies it's operational
# Usage: make verify-pg-duckdb
verify-pg-duckdb:
	@echo "Verifying pg_duckdb extension..."
	@cd .. && docker compose exec -T postgres psql -U postgres -d nyc_taxi -c "CREATE EXTENSION IF NOT EXISTS pg_duckdb;" && echo "✅ pg_duckdb extension ready in nyc_taxi database"
	@cd .. && docker compose exec -T postgres psql -U postgres -d nyc_taxi -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_duckdb';" && echo "✅ pg_duckdb extension is loaded"

# Test pg_duckdb installation and setup
# Verifies: Extension is installed, enabled, and can execute queries
# Usage: make test-pg-duckdb-setup
test-pg-duckdb-setup:
	@echo "Testing pg_duckdb setup..."
	@python3 -m pytest benchmarks/test_pg_duckdb_setup.py -v -s

# Test pg_duckdb performance comparison (3-way: Native PG vs PG+pg_duckdb vs DuckDB)
# Compares: PostgreSQL native vs pg_duckdb vs direct DuckDB
# Runs: Top 3 analytical queries (Q1, Q4, Q7) with warm-up and measurement runs
# Output: Execution times, speedup factors, detailed comparison
# Usage: make test-pg-duckdb-performance
test-pg-duckdb-performance:
	@echo "Testing pg_duckdb performance (3-way comparison)..."
	@python3 -m pytest benchmarks/test_pg_duckdb_performance.py -v -s

# Run pg_duckdb benchmark with detailed output
# Executes: Performance comparison tests
# Usage: make benchmark-pg-duckdb
benchmark-pg-duckdb:
	@echo "Running pg_duckdb benchmark..."
	@python3 -m pytest benchmarks/test_pg_duckdb_performance.py -v -s

# Compare pg_duckdb results and generate report
# Generates: Detailed comparison table and summary report
# Output: benchmarks/results/pg_duckdb_report.txt
# Usage: make compare-pg-duckdb-results
compare-pg-duckdb-results:
	@echo "Comparing pg_duckdb results..."
	@python3 benchmarks/pg_duckdb_results.py

# Run all pg_duckdb tests
# Executes: Setup verification and performance comparison tests
# Usage: make test-pg-duckdb
test-pg-duckdb: test-pg-duckdb-setup test-pg-duckdb-performance
	@echo "✅ All pg_duckdb tests completed!"

# Complete workflow for 3-fold performance comparison
# Setup → Verify → Test Performance → Generate Report
# Compares: PostgreSQL native vs PostgreSQL+pg_duckdb vs Direct DuckDB
# Queries: Top 3 analytical queries (Q1, Q4, Q7)
# Usage: make benchmark-pg-duckdb-full
benchmark-pg-duckdb-full: setup-pg-duckdb test-pg-duckdb-performance compare-pg-duckdb-results
	@echo "✅ 3-way benchmark complete! Results saved to benchmarks/results/pg_duckdb_report.txt"

# Note: Combined test targets are defined in individual test commands
# To run pg_duckdb tests specifically: make test-pg-duckdb
# To run all tests: Run individual make targets as needed

# Display pytest marker and test execution help
# Shows: Available pytest markers and example test commands
# Useful for: Running specific tests or understanding test structure
# Usage: make test-help
test-help:
	@echo "Pytest Markers:"
	@echo "  @pytest.mark.benchmark    - Performance comparison tests"
	@echo "  @pytest.mark.validation   - Correctness validation tests"
	@echo ""
	@echo "Run specific tests:"
	@echo "  pytest benchmarks/test_duckdb_vs_postgres.py"
	@echo "  pytest benchmarks/test_duckdb_queries.py -k 'test_all_queries'"
	@echo "  pytest -m benchmark -v"

# ============================================================================
# UTILITY TARGETS - Cleanup and Documentation
# ============================================================================

# Clean up benchmark results and Python cache
# Removes: benchmark results JSON files, pytest cache, __pycache__ directories
# Useful for: Fresh runs, before committing code, freeing disk space
# Usage: make clean
clean:
	rm -rf benchmarks/results/*.json
	rm -rf .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# List and describe all documentation files
# Shows: Location and purpose of each documentation file
# Files: ARCHITECTURE.md (technical deep-dive), BLOG_POST.md (analysis), README.md (overview)
# Usage: make docs
docs:
	@echo "Documentation files:"
	@echo "  - blog/BLOG_POST.md              Full performance analysis"
	@echo "  - docs/ARCHITECTURE.md           Technical deep-dive"
	@echo "  - README.md                      Project overview"
