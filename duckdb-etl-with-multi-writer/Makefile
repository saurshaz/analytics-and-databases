.PHONY: venv-create venv-install etl partition query validate \
        etl-load-all etl-load-2023 etl-load-2024 etl-load-2025 \
        etl-dev etl-fast etl-compact etl-status test-multiwriter test-etl demo \
        registry-status registry-cleanup query-stats query-peek query-daily query-vendor query-date-range \
        query-explain-plan help clean show-metrics show-partition-structure

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

venv-create:
	python3 -m venv $(VENV)

venv-install: venv-create
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt

# ============================================================================
# 🚀 UNIFIED ETL PIPELINE - MAIN MODES
# ============================================================================

etl:
	@echo "🚀 Running ETL pipeline (standard incremental loading)..."
	$(PYTHON) -m src.unified_etl_pipeline --mode etl

partition:
	@echo "📊 Running partition pipeline (Hive format)..."
	$(PYTHON) -m src.unified_etl_pipeline --mode partition

query:
	@echo "📈 Running query analysis on loaded data..."
	$(PYTHON) -m src.unified_etl_pipeline --mode query

validate:
	@echo "🔍 Validating data quality and schema..."
	$(PYTHON) -m src.unified_etl_pipeline --mode validate

both:
	@echo "🚀 Running ETL pipeline + partition (both modes)..."
	$(PYTHON) -m src.unified_etl_pipeline --mode both

# ============================================================================
# 📅 LOAD SPECIFIC YEARS
# ============================================================================

etl-load-all:
	@echo "📅 Loading all years (2023, 2024, 2025)..."
	$(PYTHON) -m src.unified_etl_pipeline --mode etl --years 2023,2024,2025

etl-load-2023:
	@echo "📅 Loading 2023 data only..."
	$(PYTHON) -m src.unified_etl_pipeline --mode etl --years 2023

etl-load-2024:
	@echo "📅 Loading 2024 data only..."
	$(PYTHON) -m src.unified_etl_pipeline --mode etl --years 2024

etl-load-2025:
	@echo "📅 Loading 2025 data only..."
	$(PYTHON) -m src.unified_etl_pipeline --mode etl --years 2025

# ============================================================================
# ⚙️ ETL CONFIGURATION PRESETS
# ============================================================================

etl-dev:
	@echo "🔧 Running ETL pipeline with development config..."
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

# ============================================================================
# 📊 ANALYTICS & QUERIES
# ============================================================================

query-stats:
	@echo "📊 Table statistics with column discovery..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); print('\n'); stats = opt.get_statistics(); import json; print(json.dumps(stats, indent=2)); opt.close()"

query-peek:
	@echo "👀 Peeking at data (first 5 rows)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.peek_data(limit=5); print(df); opt.close()"

query-daily:
	@echo "📈 Daily summary metrics (last 7 days)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.get_daily_aggregates(7); print(df.to_string()); opt.close()"

query-vendor:
	@echo "🏢 Vendor performance analysis..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.vendor_performance(); print(df.to_string()); opt.close()"

query-date-range:
	@echo "📅 Query date range (example: Jan 2024)..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); df = opt.query_by_date_range('2024-01-01', '2024-01-31', ['VendorID', 'tpep_pickup_datetime', 'trip_distance', 'total_amount']); print(f'Found {len(df)} rows'); print(df.head(10).to_string()); opt.close()"

query-explain-plan:
	@echo "📋 Query execution plan analysis..."
	$(PYTHON) -c "from src.query_optimizer import QueryOptimizer; opt = QueryOptimizer(); plan = opt.explain_plan('SELECT COUNT(*) FROM yellow_taxi_trips'); print(plan); opt.close()"

show-partition-structure:
	@echo "📂 Partitioned data directory structure:"
	@if [ -d data/processed ]; then \
		find data/processed -type d -name "day=*" | head -10; \
		echo ""; \
		echo "✅ Partitioned storage ready"; \
	else \
		echo "❌ No partitioned data found. Run: make partition"; \
	fi

# ============================================================================
# 📋 REGISTRY MANAGEMENT & MONITORING
# ============================================================================

registry-status:
	@echo "📋 Registry Status:"
	$(PYTHON) -m src.unified_etl_pipeline --status

registry-cleanup:
	@echo "🧹 Cleaning old registry entries..."
	$(PYTHON) -c "from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL; etl = DuckDBMultiWriterETL('nyc_yellow_taxi.duckdb'); cleaned = etl.cleanup_old_locks(24); print(f'✅ Cleaned {cleaned} entries')"

show-metrics:
	@echo "📊 Pipeline Metrics & Performance:"
	$(PYTHON) -m src.unified_etl_pipeline --show-metrics

# ============================================================================
# ✅ TESTING
# ============================================================================

test-multiwriter:
	@echo "Testing multi-writer coordination..."
	$(PYTHON) -m pytest tests/test_etl_multiwriter.py -v

test-etl:
	@echo "Testing ETL functionality..."
	$(PYTHON) -m pytest tests/ -v

test-targets:
	@echo "Testing all make targets..."
	$(PYTHON) -m pytest tests/test_make_targets.py -v

demo:
	@echo "Running interactive demo..."
	$(PYTHON) scripts/demo_registry_locking.py

# ============================================================================
# 🛠️ UTILITIES & HELP
# ============================================================================

help:
	@echo ""
	@echo "📚 NYC Yellow Taxi ETL Pipeline - Makefile Targets"
	@echo "===================================================="
	@echo ""
	@echo "🚀 UNIFIED ETL MODES:"
	@echo "  make etl            - Standard incremental loading (DuckDB table)"
	@echo "  make partition      - Hive-partitioned format (year/month/day/)"
	@echo "  make query          - Run analytics on loaded data"
	@echo "  make validate       - Data quality validation"
	@echo "  make both           - ETL + Partition (both modes)"
	@echo ""
	@echo "📅 LOAD SPECIFIC YEARS (etl mode):"
	@echo "  make etl-load-2023     - Load 2023 data only"
	@echo "  make etl-load-2024     - Load 2024 data only"
	@echo "  make etl-load-2025     - Load 2025 data only"
	@echo "  make etl-load-all      - Load all years (2023-2025)"
	@echo ""
	@echo "⚙️  ETL CONFIGURATION PRESETS:"
	@echo "  make etl-dev        - Development (snappy compression)"
	@echo "  make etl-fast       - Maximum speed (no compression)"
	@echo "  make etl-compact    - Maximum compression (gzip)"
	@echo "  make etl-status     - Show current configuration"
	@echo ""
	@echo "📊 ANALYTICS & QUERIES:"
	@echo "  make query-stats       - Table statistics & columns"
	@echo "  make query-peek        - Preview data (first 5 rows)"
	@echo "  make query-daily       - Daily summary (last 7 days)"
	@echo "  make query-vendor      - Vendor performance analysis"
	@echo "  make query-date-range  - Query by date range"
	@echo "  make query-explain-plan - Query execution plan"
	@echo ""
	@echo "📂 PARTITIONED DATA:"
	@echo "  make show-partition-structure - Show partition directory tree"
	@echo ""
	@echo "📋 REGISTRY & MONITORING:"
	@echo "  make registry-status     - Show active locks & runs"
	@echo "  make registry-cleanup    - Clean old lock entries"
	@echo "  make show-metrics        - Pipeline metrics & performance"
	@echo ""
	@echo "✅ TESTING:"
	@echo "  make test-etl           - Run ETL tests"
	@echo "  make test-multiwriter   - Test multi-writer locking"
	@echo "  make test-targets       - Test all make targets"
	@echo "  make demo               - Run interactive demo"
	@echo ""
	@echo "🛠️  ENVIRONMENT:"
	@echo "  make venv-create    - Create Python virtual environment"
	@echo "  make venv-install   - Install dependencies"
	@echo "  make clean          - Remove cache and temp files"
	@echo ""

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	@echo "✅ Cleaned"
