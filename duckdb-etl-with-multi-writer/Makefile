.PHONY: venv-create venv-install etl-run etl-benchmark etl-parallel etl-load-2023 \
        etl-load-2024 etl-load-2025 etl-dev etl-fast etl-compact etl-query etl-daily \
        etl-status etl-help test-multiwriter test-etl demo registry-status \
        registry-cleanup query-stats query-daily query-vendor query-date-range \
        query-peek explain-plan help clean

VENV := .venv
PYTHON := python3
PIP := python3 -m pip

venv-create:
	python3 -m venv $(VENV)

venv-install: venv-create
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

etl-run:
	@echo "Running ETL pipeline..."
	$(PYTHON) -m src.etl_pipeline

etl-benchmark:
	@echo "Benchmarking ETL performance..."
	$(PYTHON) -m src.benchmark_etl

etl-parallel:
	@echo "Loading partitions in parallel..."
	$(PYTHON) -c "from src.etl_pipeline import ETLPipeline; ETLPipeline().load_all_years()"

etl-load-2023:
	@echo "Loading 2023 data..."
	$(PYTHON) -c "from src.etl_pipeline import ETLPipeline; ETLPipeline().load_year(2023)"

etl-load-2024:
	@echo "Loading 2024 data..."
	$(PYTHON) -c "from src.etl_pipeline import ETLPipeline; ETLPipeline().load_year(2024)"

etl-load-2025:
	@echo "Loading 2025 data..."
	$(PYTHON) -c "from src.etl_pipeline import ETLPipeline; ETLPipeline().load_year(2025)"

test-multiwriter:
	@echo "Testing multi-writer coordination..."
	$(PYTHON) -m pytest tests/test_etl_multiwriter.py -v

test-etl:
	@echo "Testing ETL functionality..."
	$(PYTHON) -m pytest tests/ -v

demo:
	@echo "Running interactive demo..."
	$(PYTHON) scripts/demo_registry_locking.py

registry-status:
	@echo "Registry status:"
	$(PYTHON) -c "from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL; etl = DuckDBMultiWriterETL('nyc_yellow_taxi.duckdb'); status = etl.get_registry_status(); print(f'Active Locks: {len(status[\"active_locks\"])}, Runs: {len(status[\"all_runs\"])}')"

registry-cleanup:
	@echo "Cleaning old registry entries..."
	$(PYTHON) -c "from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL; etl = DuckDBMultiWriterETL('nyc_yellow_taxi.duckdb'); cleaned = etl.cleanup_old_locks(24); print(f'Cleaned {cleaned} entries')"

# ============================================================================
# ⚙️ ETL CONFIGURATION PRESETS
# ============================================================================

etl-dev:
	@echo "🔧 Running ETL pipeline with development config (2 workers, snappy)..."
	$(PYTHON) etl_config.py development

etl-fast:
	@echo "⚡ Running ETL pipeline with fast config (no compression)..."
	$(PYTHON) etl_config.py fast

etl-compact:
	@echo "🗜️  Running ETL pipeline with compact config (gzip compression)..."
	$(PYTHON) etl_config.py compact

etl-status:
	@echo "📊 ETL Configuration Status"
	@test -f etl_config.json && cat etl_config.json || echo "No configuration file (run make etl-dev/fast/compact first)"

etl-query:
	@echo "📈 Running example query on loaded data..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.daily_summary(days=7); print('\n'); print(df.to_string()); opt.close()"

etl-daily:
	@echo "📅 Daily metrics query (last 30 days)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.daily_summary(days=30); print('\n📈 Top 15 Days by Revenue:'); print(df.head(15).to_string()); opt.close()"


query-stats:
	@echo "📊 Table statistics with column discovery..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); print('\n'); stats = opt.get_statistics(); import json; print(json.dumps(stats, indent=2)); opt.close()"

query-peek:
	@echo "👀 Peeking at data (first 5 rows)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.peek_data(limit=5); print(df); opt.close()"

query-daily:
	@echo "📈 Daily summary metrics (last 7 days)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.daily_summary(days=7); print(df.to_string()); opt.close()"

query-vendor:
	@echo "🏢 Vendor performance analysis..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.vendor_performance(); print(df.to_string()); opt.close()"

query-date-range:
	@echo "📅 Query date range (example: Jan 2024)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.query_date_range('2024-01-01', '2024-01-31', ['VendorID', 'tpep_pickup_datetime', 'trip_distance', 'total_amount']); print(f'Found {len(df)} rows'); print(df.head(10).to_string()); opt.close()"

explain-plan:
	@echo "📋 Query execution plan analysis..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); plan = opt.explain_plan('SELECT COUNT(*) FROM yellow_taxi_trips'); print(plan); opt.close()"

help:
	@echo ""
	@echo "📚 NYC Yellow Taxi ETL Pipeline - Makefile Targets"
	@echo "=================================================="
	@echo ""
	@echo "🚀 ETL Loading:"
	@echo "  make etl-run           - Run full ETL pipeline (2023-2025)"
	@echo "  make etl-parallel      - Load all years sequentially"
	@echo "  make etl-load-2023     - Load 2023 data only"
	@echo "  make etl-load-2024     - Load 2024 data only"
	@echo "  make etl-load-2025     - Load 2025 data only"
	@echo "  make etl-benchmark     - Benchmark ETL performance"
	@echo ""
	@echo "⚙️  ETL Configuration Presets:"
	@echo "  make etl-dev           - Development (2 workers, snappy)"
	@echo "  make etl-fast          - Maximum speed (8 workers, no compression)"
	@echo "  make etl-compact       - Maximum compression (4 workers, gzip)"
	@echo "  make etl-status        - Show current configuration"
	@echo ""
	@echo "📊 Analytics & Queries:"
	@echo "  make etl-query         - Sample daily aggregates"
	@echo "  make etl-daily         - Detailed daily metrics (30 days)"
	@echo "  make query-stats       - Show table statistics & columns"
	@echo "  make query-peek        - Preview sample data (5 rows)"
	@echo "  make query-daily       - Daily summary (last 7 days)"
	@echo "  make query-vendor      - Vendor performance analysis"
	@echo "  make query-date-range  - Example date range query"
	@echo "  make explain-plan      - Show query execution plan"
	@echo ""
	@echo "📋 Registry Management:"
	@echo "  make registry-status   - Show active locks & runs"
	@echo "  make registry-cleanup  - Clean old lock entries"
	@echo ""
	@echo "✅ Testing:"
	@echo "  make test-etl          - Run ETL tests"
	@echo "  make test-multiwriter  - Test multi-writer locking"
	@echo ""
	@echo "🛠️  Environment:"
	@echo "  make venv-create       - Create Python virtual environment"
	@echo "  make venv-install      - Install dependencies"
	@echo "  make clean             - Remove pycache and temp files"
	@echo ""

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	@echo "✅ Cleaned"
