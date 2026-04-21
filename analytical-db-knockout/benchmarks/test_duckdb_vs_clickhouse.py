"""
Test: DuckDB vs ClickHouse performance on analytical workloads.

This comprehensive benchmark compares DuckDB (in-process columnar) vs ClickHouse (distributed OLAP)
on 20 analytical queries using the NYC Yellow Taxi dataset (128M+ rows).

Key differences:
- DuckDB: Fast in-process execution, vectorized operations, no network overhead
- ClickHouse: Optimized for massive scale, column compression, network queries
"""

import json
import time
from pathlib import Path

import duckdb
import pytest

try:
    from clickhouse_driver import Client
except ImportError:
    Client = None


def get_clickhouse_query(query_dict: dict) -> str:
    """
    Get ClickHouse-specific query or adapt from PostgreSQL query.
    
    ClickHouse SQL adaptations:
    - DATE_TRUNC → toStartOfMonth() or toDate()
    - EXTRACT(DOW FROM ts) → toDayOfWeek(ts)
    - EXTRACT(HOUR FROM ts) → toHour(ts)
    - PERCENTILE_CONT() → quantile()
    - date_diff() → dateDiff()
    - Cast operators (::type → implicit coercion)
    - Window functions fully supported
    """
    if "clickhouse_sql" in query_dict:
        return query_dict["clickhouse_sql"]
    
    # Start with PostgreSQL syntax (closest to ClickHouse)
    sql = query_dict.get("postgres_sql") or query_dict.get("sql")
    
    # Adapt date functions - handle all variations
    sql = sql.replace(
        "DATE_TRUNC('month', tpep_pickup_datetime)",
        "toStartOfMonth(tpep_pickup_datetime)"
    )
    sql = sql.replace(
        "DATE_TRUNC('month',tpep_pickup_datetime)",
        "toStartOfMonth(tpep_pickup_datetime)"
    )
    
    # Adapt date extraction - toDate for simple DATE()
    sql = sql.replace(
        "DATE(tpep_pickup_datetime)",
        "toDate(tpep_pickup_datetime)"
    )
    
    # Adapt EXTRACT functions
    sql = sql.replace(
        "EXTRACT(DOW FROM tpep_pickup_datetime)",
        "toDayOfWeek(tpep_pickup_datetime)"
    )
    sql = sql.replace(
        "EXTRACT(HOUR FROM tpep_pickup_datetime)::integer",
        "toHour(tpep_pickup_datetime)"
    )
    sql = sql.replace(
        "EXTRACT(HOUR FROM tpep_pickup_datetime)",
        "toHour(tpep_pickup_datetime)"
    )
    
    # Adapt casting operators (PostgreSQL ::type → ClickHouse implicit coercion)
    sql = sql.replace("::integer", "")  # ClickHouse infers from functions
    sql = sql.replace("::numeric", "")  # ClickHouse implicit coercion
    
    # Adapt PERCENTILE_CONT to quantile
    sql = sql.replace(
        "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY trip_distance)",
        "quantile(0.9)(trip_distance)"
    )
    
    # Adapt date difference calculations
    sql = sql.replace(
        "EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60",
        "dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60"
    )
    sql = sql.replace(
        "EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600",
        "dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 3600"
    )
    
    # Adapt NULLIF to ClickHouse nullIf
    sql = sql.replace("NULLIF(", "nullIf(")
    
    return sql


def run_query_duckdb(con, sql: str):
    """Execute query on DuckDB and return results, time, and status."""
    start = time.perf_counter()
    try:
        df = con.execute(sql).fetchdf()
        elapsed = time.perf_counter() - start
        return df, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        return None, elapsed, "FAIL", str(e)[:80]


def run_query_clickhouse(con, sql: str):
    """Execute query on ClickHouse and return results, time, and status."""
    start = time.perf_counter()
    try:
        result = con.execute(sql)
        elapsed = time.perf_counter() - start
        return result, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        error_msg = str(e)[:150]  # More detail for debugging
        return None, elapsed, "FAIL", error_msg


@pytest.mark.benchmark
class TestDuckDBVsClickHouse:
    """Main benchmark: DuckDB vs ClickHouse on analytical queries."""

    def test_benchmark_all_queries(self, duckdb_con, clickhouse_con, queries):
        """Run all 20 benchmark queries on both DuckDB and ClickHouse and compare."""
        results = {
            "duckdb": {},
            "clickhouse": {}
        }
        
        print("\n" + "="*80)
        print("ANALYTICAL-DB-KNOCKOUT: DuckDB vs ClickHouse Benchmark")
        print("="*80)
        
        speedups = []
        comparison_table = []
        
        for query in queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")
            complexity = query.get("complexity", "N/A")
            
            # Get dialect-specific SQL
            duckdb_sql = query.get("sql")
            clickhouse_sql = get_clickhouse_query(query)
            
            print(f"\n[Query {query_id}] {query_name}")
            print(f"  Complexity: {complexity}")
            
            # Warm-up runs
            duckdb_result = None
            clickhouse_result = None
            
            # Run DuckDB (3 measurement runs)
            duckdb_times = []
            for run in range(3):
                df, elapsed, status, error = run_query_duckdb(duckdb_con, duckdb_sql)
                if status == "PASS":
                    duckdb_times.append(elapsed)
                    if run == 0:
                        rows_duckdb = len(df) if df is not None else 0
                        duckdb_result = {"status": status, "rows": rows_duckdb, "error": ""}
            
            if duckdb_times:
                avg_duckdb = sum(duckdb_times) / len(duckdb_times)
                min_duckdb = min(duckdb_times)
                max_duckdb = max(duckdb_times)
                
                results["duckdb"][query_id] = {
                    "title": query_name,
                    "complexity": complexity,
                    "avg_time": avg_duckdb,
                    "min_time": min_duckdb,
                    "max_time": max_duckdb,
                    "rows": rows_duckdb,
                }
                
                print(f"  DuckDB ✅: {avg_duckdb:.3f}s (±{(max_duckdb-min_duckdb)/2:.3f}s) [{rows_duckdb} rows]")
            else:
                avg_duckdb = 0
                print(f"  DuckDB ❌: Failed")
                results["duckdb"][query_id] = {
                    "title": query_name,
                    "complexity": complexity,
                    "status": "FAIL",
                }
            
            # Run ClickHouse (3 measurement runs)
            clickhouse_times = []
            last_error = ""
            for run in range(3):
                result, elapsed, status, error = run_query_clickhouse(clickhouse_con, clickhouse_sql)
                if status == "PASS":
                    clickhouse_times.append(elapsed)
                    if run == 0:
                        rows_clickhouse = len(result) if result else 0
                        clickhouse_result = {"status": status, "rows": rows_clickhouse, "error": ""}
                else:
                    last_error = error
                    if run == 0:  # Only print on first run
                        print(f"  ClickHouse SQL: {clickhouse_sql[:100]}...")
            
            if clickhouse_times:
                avg_clickhouse = sum(clickhouse_times) / len(clickhouse_times)
                min_clickhouse = min(clickhouse_times)
                max_clickhouse = max(clickhouse_times)
                
                results["clickhouse"][query_id] = {
                    "title": query_name,
                    "complexity": complexity,
                    "avg_time": avg_clickhouse,
                    "min_time": min_clickhouse,
                    "max_time": max_clickhouse,
                    "rows": rows_clickhouse,
                }
                
                print(f"  ClickHouse ✅: {avg_clickhouse:.3f}s (±{(max_clickhouse-min_clickhouse)/2:.3f}s) [{rows_clickhouse} rows]")
            else:
                avg_clickhouse = 0
                print(f"  ClickHouse ❌: Failed - {last_error[:80]}")
                results["clickhouse"][query_id] = {
                    "title": query_name,
                    "complexity": complexity,
                    "status": "FAIL",
                    "error": last_error,
                }
            
            # Calculate speedup
            if avg_duckdb > 0 and avg_clickhouse > 0:
                if avg_duckdb > avg_clickhouse:
                    speedup = avg_duckdb / avg_clickhouse
                    winner = "ClickHouse"
                    speedups.append((query_name, speedup, winner))
                    print(f"  🏆 ClickHouse is {speedup:.2f}x faster")
                else:
                    speedup = avg_clickhouse / avg_duckdb
                    winner = "DuckDB"
                    speedups.append((query_name, speedup, winner))
                    print(f"  🏆 DuckDB is {speedup:.2f}x faster")
                
                comparison_table.append({
                    "query_id": query_id,
                    "query_name": query_name,
                    "duckdb_time": avg_duckdb,
                    "clickhouse_time": avg_clickhouse,
                    "speedup": speedup,
                    "winner": winner,
                })
        
        # Summary Report
        print("\n" + "="*80)
        print("PERFORMANCE SUMMARY")
        print("="*80)
        
        if speedups:
            print("\nTop 5 Queries Where DuckDB Wins:")
            duckdb_wins = [(name, speed, winner) for name, speed, winner in speedups if winner == "DuckDB"]
            for name, speedup, _ in sorted(duckdb_wins, key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {speedup:6.2f}x - {name}")
            
            print("\nTop 5 Queries Where ClickHouse Wins:")
            clickhouse_wins = [(name, speed, winner) for name, speed, winner in speedups if winner == "ClickHouse"]
            for name, speedup, _ in sorted(clickhouse_wins, key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {speedup:6.2f}x - {name}")
        
        # Aggregated stats
        duckdb_times = [r["avg_time"] for r in results["duckdb"].values() if "avg_time" in r]
        clickhouse_times = [r["avg_time"] for r in results["clickhouse"].values() if "avg_time" in r]
        
        if duckdb_times and clickhouse_times:
            total_duckdb = sum(duckdb_times)
            total_clickhouse = sum(clickhouse_times)
            avg_duckdb = total_duckdb / len(duckdb_times)
            avg_clickhouse = total_clickhouse / len(clickhouse_times)
            
            print(f"\n📊 Aggregate Statistics:")
            print(f"  DuckDB:")
            print(f"    Total time: {total_duckdb:.3f}s")
            print(f"    Average query: {avg_duckdb:.3f}s")
            print(f"    Queries: {len(duckdb_times)}/20")
            
            print(f"\n  ClickHouse:")
            print(f"    Total time: {total_clickhouse:.3f}s")
            print(f"    Average query: {avg_clickhouse:.3f}s")
            print(f"    Queries: {len(clickhouse_times)}/20")
            
            if total_duckdb > total_clickhouse:
                overall_speedup = total_duckdb / total_clickhouse
                print(f"\n  🏆 ClickHouse is {overall_speedup:.2f}x faster overall")
            else:
                overall_speedup = total_clickhouse / total_duckdb
                print(f"\n  🏆 DuckDB is {overall_speedup:.2f}x faster overall")
        
        print("="*80)
        
        # Save results
        self._save_results(results)
    
    def test_data_consistency(self, duckdb_con, clickhouse_con):
        """Verify both databases have the same data loaded."""
        print("\n" + "="*80)
        print("Data Consistency Check")
        print("="*80)
        
        # DuckDB stats
        duckdb_stats = duckdb_con.execute("""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT VendorID) as vendors,
                MIN(tpep_pickup_datetime) as min_date,
                MAX(tpep_pickup_datetime) as max_date,
                ROUND(AVG(total_amount), 2) as avg_fare,
                ROUND(SUM(total_amount), 2) as total_revenue
            FROM yellow_taxi_trips
        """).fetchall()[0]
        
        # ClickHouse stats
        clickhouse_stats = clickhouse_con.execute("""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT VendorID) as vendors,
                MIN(tpep_pickup_datetime) as min_date,
                MAX(tpep_pickup_datetime) as max_date,
                ROUND(AVG(total_amount), 2) as avg_fare,
                ROUND(SUM(total_amount), 2) as total_revenue
            FROM yellow_taxi_trips
        """)[0]
        
        print(f"\nDuckDB:")
        print(f"  Rows: {duckdb_stats[0]:,}")
        print(f"  Vendors: {duckdb_stats[1]}")
        print(f"  Date range: {duckdb_stats[2]} to {duckdb_stats[3]}")
        print(f"  Average fare: ${duckdb_stats[4]}")
        print(f"  Total revenue: ${duckdb_stats[5]:,.2f}")
        
        print(f"\nClickHouse:")
        print(f"  Rows: {clickhouse_stats[0]:,}")
        print(f"  Vendors: {clickhouse_stats[1]}")
        print(f"  Date range: {clickhouse_stats[2]} to {clickhouse_stats[3]}")
        print(f"  Average fare: ${clickhouse_stats[4]}")
        print(f"  Total revenue: ${clickhouse_stats[5]:,.2f}")
        
        # Verify consistency
        if duckdb_stats[0] == clickhouse_stats[0]:
            print(f"\n✅ Row count matches: {duckdb_stats[0]:,}")
        else:
            print(f"\n⚠️  Row count mismatch: DuckDB={duckdb_stats[0]:,}, ClickHouse={clickhouse_stats[0]:,}")
        
        print("="*80)
    
    @staticmethod
    def _save_results(results):
        """Save benchmark results to JSON."""
        results_file = Path(__file__).parent / "results" / "duckdb_vs_clickhouse_results.json"
        results_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\n✅ Results saved to {results_file}")
        except Exception as e:
            print(f"\n⚠️  Could not save results: {e}")
