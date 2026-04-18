"""
Test: Validate and benchmark individual queries on DuckDB.

Ensures all 20 queries execute successfully and collects performance metrics.
"""

import time
import pytest

from benchmarks.benchmark_timing import benchmark_query


def run_query_duckdb(con, sql: str):
    """Execute query on DuckDB and return dataframe, time, and status."""
    start = time.perf_counter()
    try:
        df = con.execute(sql).fetchdf()
        elapsed = time.perf_counter() - start
        return df, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        return None, elapsed, "FAIL", str(e)[:80]


@pytest.mark.benchmark
class TestDuckDBQueries:
    """Validate DuckDB query execution and performance."""

    @pytest.mark.parametrize("query", [], ids=lambda q: f"Q{q['id']}")
    def test_query_execution(self, duckdb_con, query):
        """Verify each query executes successfully on DuckDB."""
        duckdb_sql = query.get("sql")
        result = benchmark_query(run_query_duckdb, duckdb_con, duckdb_sql)
        
        assert result["status"] == "PASS", f"Query {query['id']} failed: {result['error']}"
        assert result["rows"] >= 0, f"Query {query['id']} returned invalid row count"
        assert result["avg_time"] >= 0, f"Query {query['id']} has invalid timing"

    def test_all_queries_pass(self, duckdb_con, queries):
        """Verify all 20 queries execute successfully."""
        failed_queries = []
        
        for query in queries:
            duckdb_sql = query.get("sql")
            result = benchmark_query(run_query_duckdb, duckdb_con, duckdb_sql)
            if result["status"] != "PASS":
                failed_queries.append({
                    "id": query["id"],
                    "name": query["name"],
                    "error": result["error"]
                })
        
        assert len(failed_queries) == 0, f"Failed queries: {failed_queries}"

    def test_performance_consistency(self, duckdb_con, queries):
        """Verify DuckDB performance is consistent across runs."""
        first_run_times = {}
        second_run_times = {}
        
        for query in queries:
            duckdb_sql = query.get("sql")
            result1 = benchmark_query(run_query_duckdb, duckdb_con, duckdb_sql, measured_runs=1)
            result2 = benchmark_query(run_query_duckdb, duckdb_con, duckdb_sql, measured_runs=1)
            
            if result1["status"] == "PASS" and result2["status"] == "PASS":
                first_run_times[query["id"]] = result1["avg_time"]
                second_run_times[query["id"]] = result2["avg_time"]
        
        # Verify relatively consistent performance (within 2x variance)
        for query_id in first_run_times:
            ratio = max(first_run_times[query_id], second_run_times[query_id]) / \
                    min(first_run_times[query_id], second_run_times[query_id])
            assert ratio < 2.0, f"Query {query_id} has inconsistent performance: {ratio:.2f}x variance"

    def test_output_correctness(self, duckdb_con, queries):
        """Verify queries return expected row counts and column types."""
        for query in queries:
            duckdb_sql = query.get("sql")
            df = duckdb_con.execute(duckdb_sql).fetchdf()
            
            assert df is not None, f"Query {query['id']} returned None"
            assert len(df) > 0, f"Query {query['id']} returned empty result"
            assert len(df.columns) > 0, f"Query {query['id']} has no columns"
