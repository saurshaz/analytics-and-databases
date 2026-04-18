.PHONY: help install setup setup-postgres setup-duckdb test benchmark benchmark-duckdb validation clean docs test-help

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

install:
	pip3 install -r requirements.txt

setup: setup-postgres setup-duckdb
	@echo "\n✅ Both databases are ready!"
	@echo "   Run 'make benchmark' to compare performance"

setup-postgres:
	@echo "Setting up PostgreSQL database..."
	cd .. && docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	python3 ../setup_postgres.py

setup-duckdb:
	@echo "Setting up DuckDB database..."
	python3 ../setup_duckdb.py

# ============================================================================
# Test Targets (Using pytest)
# ============================================================================

test:
	@echo "Running all tests (validation + benchmarks)...\n"
	python3 -m pytest -m "validation or benchmark" --tb=short

benchmark:
	@echo "Running comprehensive DuckDB vs PostgreSQL benchmark...\n"
	python3 -m pytest -m benchmark benchmarks/test_duckdb_vs_postgres.py -v -s

benchmark-duckdb:
	@echo "Running DuckDB-only performance benchmark...\n"
	python3 -m pytest -m benchmark benchmarks/test_duckdb_queries.py -v -s

validation:
	@echo "Running query correctness validation...\n"
	python3 -m pytest -m validation benchmarks/test_query_correctness.py -v -s

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
# Utilities
# ============================================================================

clean:
	rm -rf benchmarks/results/*.json
	rm -rf .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

docs:
	@echo "Documentation files:"
	@echo "  - blog/BLOG_POST.md              Full performance analysis"
	@echo "  - docs/ARCHITECTURE.md           Technical deep-dive"
	@echo "  - README.md                      Project overview"
