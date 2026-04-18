"""
Test: DuckDB vs PostgreSQL performance on analytical workloads.

This comprehensive benchmark compares two database engines on 20 analytical queries
using the NYC Yellow Taxi dataset (128M+ rows).

Results demonstrate:
- DuckDB's vectorized execution advantages
- Query optimization impact on PostgreSQL
- Speedup factors across different query complexity levels
"""

import json
import time
from pathlib import Path

import duckdb
import psycopg2
import pytest

from benchmarks.benchmark_results import save_latest_results
from benchmarks.benchmark_timing import benchmark_query, format_run_times, summarize_results


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


def run_query_postgres(con, sql: str):
    """Execute query on PostgreSQL and return dataframe, time, and status."""
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


@pytest.mark.benchmark
class TestDuckDBVsPostgres:
    """Main benchmark: DuckDB vs PostgreSQL on analytical queries."""

    def test_benchmark_all_queries(self, duckdb_con, pg_con, queries):
        """Run all 20 benchmark queries on both databases and compare performance."""
        results = {
            "duckdb": {},
            "postgres": {}
        }
        
        print("\n" + "="*80)
        print("ANALYTICAL-DB-KNOCKOUT: DuckDB vs PostgreSQL Benchmark")
        print("="*80)
        
        for query in queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")
            
            # Get dialect-specific SQL
            duckdb_sql = query.get("sql")
            postgres_sql = query.get("postgres_sql", query.get("sql"))
            
            print(f"\n[Query {query_id}] {query_name}")
            print(f"  Complexity: {query.get('complexity', 'N/A')}")
            
            # Run DuckDB
            duckdb_result = benchmark_query(run_query_duckdb, duckdb_con, duckdb_sql)
            results["duckdb"][query_id] = duckdb_result
            
            status_emoji = "✅" if duckdb_result["status"] == "PASS" else "❌"
            print(f"  DuckDB {status_emoji}: {duckdb_result['avg_time']:.3f}s ({duckdb_result['rows']} rows)")
            if duckdb_result["error"]:
                print(f"    Error: {duckdb_result['error']}")
            
            # Run PostgreSQL
            postgres_result = benchmark_query(run_query_postgres, pg_con, postgres_sql)
            results["postgres"][query_id] = postgres_result
            
            status_emoji = "✅" if postgres_result["status"] == "PASS" else "❌"
            print(f"  PostgreSQL {status_emoji}: {postgres_result['avg_time']:.3f}s ({postgres_result['rows']} rows)")
            if postgres_result["error"]:
                print(f"    Error: {postgres_result['error']}")
            
            # Calculate speedup
            if (duckdb_result["status"] == "PASS" and 
                postgres_result["status"] == "PASS" and
                duckdb_result["avg_time"] > 0):
                speedup = postgres_result["avg_time"] / duckdb_result["avg_time"]
                print(f"  ⚡ Speedup: {speedup:.1f}x faster in DuckDB")
        
        # Summary
        summary = summarize_results(results)
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        for db_name, stats in summary.items():
            if isinstance(stats, dict) and "total_time" in stats:
                print(f"\n{db_name.upper()}:")
                print(f"  Total Time: {stats['total_time']:.3f}s")
                print(f"  Avg/Query:  {stats['avg_time']:.3f}s")
                print(f"  Min Time:   {stats['min_time']:.3f}s")
                print(f"  Max Time:   {stats['max_time']:.3f}s")
                print(f"  Queries:    {stats['queries_run']}/{sum(1 for q in queries)}")
                if stats['queries_failed'] > 0:
                    print(f"  Failed:     {stats['queries_failed']}")
        
        if "speedup" in summary:
            print(f"\n🏆 Overall Speedup: {summary['speedup']['factor']}")
        
        # Save results
        save_latest_results("duckdb_vs_postgres", {
            "duckdb": results["duckdb"],
            "postgres": results["postgres"],
            "summary": summary
        })
        
        # Assertions
        duckdb_pass = sum(1 for r in results["duckdb"].values() if r["status"] == "PASS")
        assert duckdb_pass > 0, "DuckDB: No queries passed"
        
        postgres_pass = sum(1 for r in results["postgres"].values() if r["status"] == "PASS")
        assert postgres_pass > 0, "PostgreSQL: No queries passed"


@pytest.mark.benchmark
class TestIndividualDatabases:
    """Detailed performance analysis for each database."""

    def test_duckdb_execution(self, duckdb_con, queries):
        """Profile DuckDB query execution across all benchmarks."""
        print("\n" + "="*80)
        print("DuckDB Execution Profile")
        print("="*80)
        
        times = []
        for query in queries:
            duckdb_sql = query.get("sql")
            result = benchmark_query(run_query_duckdb, duckdb_con, duckdb_sql)
            if result["status"] == "PASS":
                times.append(result["avg_time"])
                print(f"  Query {query['id']:2d}: {result['avg_time']:8.3f}s ({result['rows']:6d} rows)")
        
        if times:
            print(f"\n  Total: {sum(times):.3f}s | Avg: {sum(times)/len(times):.3f}s | Min: {min(times):.3f}s | Max: {max(times):.3f}s")

    def test_postgres_execution(self, pg_con, queries):
        """Profile PostgreSQL query execution across all benchmarks."""
        if not pg_con:
            pytest.skip("PostgreSQL not available")
        
        print("\n" + "="*80)
        print("PostgreSQL Execution Profile")
        print("="*80)
        
        times = []
        for query in queries:
            postgres_sql = query.get("postgres_sql", query.get("sql"))
            result = benchmark_query(run_query_postgres, pg_con, postgres_sql)
            if result["status"] == "PASS":
                times.append(result["avg_time"])
                print(f"  Query {query['id']:2d}: {result['avg_time']:8.3f}s ({result['rows']:6d} rows)")
        
        if times:
            print(f"\n  Total: {sum(times):.3f}s | Avg: {sum(times)/len(times):.3f}s | Min: {min(times):.3f}s | Max: {max(times):.3f}s")
