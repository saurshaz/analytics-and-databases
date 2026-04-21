"""
Test: pg_duckdb performance comparison.

Compares performance of three backends:
1. PostgreSQL native (baseline)
2. PostgreSQL with pg_duckdb enabled
3. Direct DuckDB (reference)

Runs top 3 analytical queries with warm-up and measurement runs.
"""

import json
import time
from pathlib import Path

import duckdb
import psycopg2
import pytest

from benchmarks.benchmark_timing import benchmark_query
from benchmarks.conftest import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS, QUERIES_PATH


def run_query_postgres_native(con, sql: str):
    """Execute query on PostgreSQL native engine."""
    if not con:
        return None, 0, "SKIP", "No connection"
    
    start = time.perf_counter()
    try:
        cur = con.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        elapsed = time.perf_counter() - start
        cur.close()
        return result, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        con.rollback()
        return None, elapsed, "FAIL", str(e)[:80]


def run_query_postgres_pgduckdb(con, sql: str):
    """Execute query on PostgreSQL with pg_duckdb enabled."""
    if not con:
        return None, 0, "SKIP", "No connection"
    
    start = time.perf_counter()
    try:
        cur = con.cursor()
        cur.execute("SET duckdb.force_execution TO true;")
        cur.execute(sql)
        result = cur.fetchall()
        elapsed = time.perf_counter() - start
        cur.close()
        return result, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        con.rollback()
        return None, elapsed, "FAIL", str(e)[:80]


def run_query_duckdb(con, sql: str):
    """Execute query on direct DuckDB."""
    start = time.perf_counter()
    try:
        df = con.execute(sql).fetchdf()
        elapsed = time.perf_counter() - start
        return df, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        return None, elapsed, "FAIL", str(e)[:80]


@pytest.mark.benchmark
class TestPgDuckDBPerformance:
    """Performance comparison: PostgreSQL native vs pg_duckdb vs direct DuckDB."""

    def test_pg_duckdb_performance_comparison(self, pg_con, duckdb_con, queries):
        """Run top 3 queries on all three backends and compare performance."""
        
        # Select top 3 queries for pg_duckdb testing
        pg_duckdb_queries = [
            q for q in queries if q["id"] in [1, 4, 7]
        ]
        
        results = {
            "native_postgres": {},
            "pg_duckdb": {},
            "direct_duckdb": {}
        }
        
        print("\n" + "="*80)
        print("PG_DUCKDB PERFORMANCE COMPARISON")
        print("="*80)
        print("\nComparing three backends:")
        print("  1. PostgreSQL Native (baseline)")
        print("  2. PostgreSQL + pg_duckdb (DuckDB execution engine)")
        print("  3. Direct DuckDB (embedded engine)")
        print("\n" + "-"*80)
        
        for query in pg_duckdb_queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")
            
            print(f"\n[Query {query_id}] {query_name}")
            print(f"  Complexity: {query.get('complexity', 'N/A')}")
            
            # Use dialect-specific SQL
            # - PostgreSQL Native: Use postgres_sql (PostgreSQL-compatible)
            # - PostgreSQL + pg_duckdb: Use pg_duckdb_sql (DuckDB syntax with type safety)
            # - Direct DuckDB: Use base sql (pure DuckDB)
            native_sql = query.get("postgres_sql", query.get("sql"))
            pg_duckdb_sql = query.get("pg_duckdb_sql", query.get("sql"))  # Use pg_duckdb_sql if available
            duckdb_sql = query.get("sql")
            
            # Run PostgreSQL Native
            native_result = benchmark_query(
                run_query_postgres_native,
                pg_con,
                native_sql,
                warmup_runs=1,
                measured_runs=3
            )
            results["native_postgres"][query_id] = native_result
            
            status_emoji = "✅" if native_result["status"] == "PASS" else "❌"
            print(f"  PostgreSQL Native {status_emoji}: {native_result['avg_time']:.3f}s ({native_result['rows']} rows)")
            if native_result["error"]:
                print(f"    Error: {native_result['error']}")
            
            # Run PostgreSQL + pg_duckdb
            pg_duckdb_result = benchmark_query(
                run_query_postgres_pgduckdb,
                pg_con,
                pg_duckdb_sql,
                warmup_runs=1,
                measured_runs=3
            )
            results["pg_duckdb"][query_id] = pg_duckdb_result
            
            status_emoji = "✅" if pg_duckdb_result["status"] == "PASS" else "❌"
            print(f"  PostgreSQL + pg_duckdb {status_emoji}: {pg_duckdb_result['avg_time']:.3f}s ({pg_duckdb_result['rows']} rows)")
            if pg_duckdb_result["error"]:
                print(f"    Error: {pg_duckdb_result['error']}")
            
            # Run Direct DuckDB
            duckdb_result = benchmark_query(
                run_query_duckdb,
                duckdb_con,
                duckdb_sql,
                warmup_runs=1,
                measured_runs=3
            )
            results["direct_duckdb"][query_id] = duckdb_result
            
            status_emoji = "✅" if duckdb_result["status"] == "PASS" else "❌"
            print(f"  Direct DuckDB {status_emoji}: {duckdb_result['avg_time']:.3f}s ({duckdb_result['rows']} rows)")
            if duckdb_result["error"]:
                print(f"    Error: {duckdb_result['error']}")
            
            # Calculate speedup factors
            if native_result["status"] == "PASS" and pg_duckdb_result["status"] == "PASS":
                speedup_vs_native = native_result["avg_time"] / pg_duckdb_result["avg_time"]
                print(f"  Speedup vs Native: {speedup_vs_native:.2f}x")
            else:
                speedup_vs_native = None
                print(f"  Speedup vs Native: N/A (execution failed)")
            
            if pg_duckdb_result["status"] == "PASS" and duckdb_result["status"] == "PASS":
                speedup_vs_direct = pg_duckdb_result["avg_time"] / duckdb_result["avg_time"]
                print(f"  Speedup vs Direct DuckDB: {speedup_vs_direct:.2f}x")
            else:
                speedup_vs_direct = None
                print(f"  Speedup vs Direct DuckDB: N/A (execution failed)")
        
        # Print summary
        print("\n" + "="*80)
        print("PERFORMANCE SUMMARY")
        print("="*80)
        
        summary = {
            "native_postgres": {},
            "pg_duckdb": {},
            "direct_duckdb": {},
            "speedup_vs_native": {},
            "speedup_vs_direct": {}
        }
        
        for query in pg_duckdb_queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")
            
            native_result = results["native_postgres"][query_id]
            pg_duckdb_result = results["pg_duckdb"][query_id]
            duckdb_result = results["direct_duckdb"][query_id]
            
            # Native PostgreSQL summary
            if native_result["status"] == "PASS":
                summary["native_postgres"][query_id] = {
                    "name": query_name,
                    "avg_time": native_result["avg_time"],
                    "rows": native_result["rows"]
                }
            else:
                summary["native_postgres"][query_id] = {
                    "name": query_name,
                    "avg_time": 0,
                    "rows": 0,
                    "error": native_result["error"]
                }
            
            # pg_duckdb summary
            if pg_duckdb_result["status"] == "PASS":
                summary["pg_duckdb"][query_id] = {
                    "name": query_name,
                    "avg_time": pg_duckdb_result["avg_time"],
                    "rows": pg_duckdb_result["rows"]
                }
            else:
                summary["pg_duckdb"][query_id] = {
                    "name": query_name,
                    "avg_time": 0,
                    "rows": 0,
                    "error": pg_duckdb_result["error"]
                }
            
            # Direct DuckDB summary
            if duckdb_result["status"] == "PASS":
                summary["direct_duckdb"][query_id] = {
                    "name": query_name,
                    "avg_time": duckdb_result["avg_time"],
                    "rows": duckdb_result["rows"]
                }
            else:
                summary["direct_duckdb"][query_id] = {
                    "name": query_name,
                    "avg_time": 0,
                    "rows": 0,
                    "error": duckdb_result["error"]
                }
            
            # Speedup calculations
            if native_result["status"] == "PASS" and pg_duckdb_result["status"] == "PASS":
                summary["speedup_vs_native"][query_id] = {
                    "name": query_name,
                    "speedup": native_result["avg_time"] / pg_duckdb_result["avg_time"]
                }
            else:
                summary["speedup_vs_native"][query_id] = {
                    "name": query_name,
                    "speedup": None
                }
            
            if pg_duckdb_result["status"] == "PASS" and duckdb_result["status"] == "PASS":
                summary["speedup_vs_direct"][query_id] = {
                    "name": query_name,
                    "speedup": pg_duckdb_result["avg_time"] / duckdb_result["avg_time"]
                }
            else:
                summary["speedup_vs_direct"][query_id] = {
                    "name": query_name,
                    "speedup": None
                }
        
        # Print summary table
        print("\n| Query | Native PG | PG+pg_duckdb | Direct DuckDB | Speedup (vs Native) | Speedup (vs Direct) |")
        print("|-------|-----------|--------------|---------------|---------------------|---------------------|")
        
        for query in pg_duckdb_queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")
            
            native_time = summary["native_postgres"][query_id]["avg_time"]
            pg_duckdb_time = summary["pg_duckdb"][query_id]["avg_time"]
            duckdb_time = summary["direct_duckdb"][query_id]["avg_time"]
            
            speedup_vs_native = summary["speedup_vs_native"][query_id]["speedup"]
            speedup_vs_direct = summary["speedup_vs_direct"][query_id]["speedup"]
            
            native_str = f"{native_time:.3f}s" if native_time > 0 else "N/A"
            pg_duckdb_str = f"{pg_duckdb_time:.3f}s" if pg_duckdb_time > 0 else "N/A"
            duckdb_str = f"{duckdb_time:.3f}s" if duckdb_time > 0 else "N/A"
            
            speedup_native_str = f"{speedup_vs_native:.2f}x" if speedup_vs_native else "N/A"
            speedup_direct_str = f"{speedup_vs_direct:.2f}x" if speedup_vs_direct else "N/A"
            
            print(f"| {query_id}: {query_name} | {native_str} | {pg_duckdb_str} | {duckdb_str} | {speedup_native_str} | {speedup_direct_str} |")
        
        # Print overall statistics
        print("\n" + "-"*80)
        print("OVERALL STATISTICS")
        print("-"*80)
        
        # Calculate average speedup (only for successfully executed queries on BOTH backends)
        successful_query_ids = [
            qid for qid in summary["native_postgres"].keys()
            if summary["native_postgres"][qid]["avg_time"] > 0 
            and summary["pg_duckdb"][qid]["avg_time"] > 0
        ]
        
        if successful_query_ids:
            avg_speedup_vs_native = sum(
                summary["native_postgres"][qid]["avg_time"] / summary["pg_duckdb"][qid]["avg_time"]
                for qid in successful_query_ids
            ) / len(successful_query_ids)
            print(f"\nAverage Speedup (pg_duckdb vs Native PostgreSQL): {avg_speedup_vs_native:.2f}x")
        else:
            print(f"\nAverage Speedup (pg_duckdb vs Native PostgreSQL): N/A (no successful queries on both backends)")
        
        # Collect valid execution times for comparison
        native_times = [v["avg_time"] for v in summary["native_postgres"].values() if v["avg_time"] > 0]
        pg_duckdb_times = [v["avg_time"] for v in summary["pg_duckdb"].values() if v["avg_time"] > 0]
        duckdb_times = [v["avg_time"] for v in summary["direct_duckdb"].values() if v["avg_time"] > 0]
        
        if pg_duckdb_times and duckdb_times and len(pg_duckdb_times) == len(duckdb_times):
            avg_speedup_vs_direct = sum(
                pg_duckdb_times[i] / duckdb_times[i]
                for i in range(len(pg_duckdb_times))
            ) / len(pg_duckdb_times)
            print(f"Average Speedup (pg_duckdb vs Direct DuckDB): {avg_speedup_vs_direct:.2f}x")
        
        # Total execution time
        total_native = sum(native_times) if native_times else 0
        total_pg_duckdb = sum(pg_duckdb_times) if pg_duckdb_times else 0
        total_duckdb = sum(duckdb_times) if duckdb_times else 0
        
        print(f"\nTotal Execution Time (3 queries):")
        print(f"  Native PostgreSQL: {total_native:.3f}s")
        print(f"  PostgreSQL + pg_duckdb: {total_pg_duckdb:.3f}s")
        print(f"  Direct DuckDB: {total_duckdb:.3f}s")
        
        if total_native > 0 and total_pg_duckdb > 0:
            total_speedup = total_native / total_pg_duckdb
            print(f"\nOverall Speedup (pg_duckdb vs Native): {total_speedup:.2f}x")
        
        if total_pg_duckdb > 0 and total_duckdb > 0:
            total_overhead = total_pg_duckdb / total_duckdb
            print(f"Overall Overhead (pg_duckdb vs Direct DuckDB): {total_overhead:.2f}x")
        
        # Save results
        results_file = Path(__file__).parent / "results" / "pg_duckdb_comparison.json"
        results_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(results_file, "w") as f:
            json.dump({
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "queries": pg_duckdb_queries,
                "results": results,
                "summary": summary
            }, f, indent=2)
        
        print(f"\n✓ Results saved to: {results_file}")
        
        # Assertions
        for query in pg_duckdb_queries:
            query_id = query["id"]
            
            # All queries should pass on all backends
            assert results["native_postgres"][query_id]["status"] == "PASS", \
                f"Query {query_id} failed on PostgreSQL native"
            assert results["pg_duckdb"][query_id]["status"] == "PASS", \
                f"Query {query_id} failed on PostgreSQL + pg_duckdb"
            assert results["direct_duckdb"][query_id]["status"] == "PASS", \
                f"Query {query_id} failed on direct DuckDB"
        
        print("\n✓ All pg_duckdb performance tests passed!")