.PHONY: help install setup setup-postgres setup-duckdb test benchmark benchmark-duckdb validation clean docs test-help

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

# Initialize PostgreSQL container with nyc_taxi database
# Runs: docker compose up for PostgreSQL service
# Requires: ../docker-compose.yml (in parent directory)
# Runs: ../setup_postgres.py to create schema and prepare database
# Usage: make setup-postgres
setup-postgres:
	@echo "Setting up PostgreSQL database..."
	cd .. && docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
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
