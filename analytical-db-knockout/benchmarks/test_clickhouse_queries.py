"""
Test: ClickHouse performance on analytical workloads.

This benchmark compares ClickHouse OLAP engine on 20 analytical queries
using the NYC Yellow Taxi dataset (128M+ rows).

ClickHouse advantages:
- Column-oriented storage for analytical queries
- Vectorized query execution
- Built-in compression for storage efficiency
- Native SQL support with OLAP functions
"""

import json
import time
from pathlib import Path

import pytest


def get_clickhouse_query(query_dict: dict) -> str:
    """
    Get ClickHouse-specific query or generic SQL with ClickHouse adaptations.
    
    ClickHouse SQL dialect adaptations:
    - Uses same standard SQL as PostgreSQL for most queries
    - PERCENTILE_CONT() is supported
    - Window functions fully supported
    - DATE_TRUNC() is NOT available, use toStartOfMonth() instead
    - EXTRACT(DOW FROM ts) works like PostgreSQL
    """
    # Check if there's a dedicated ClickHouse SQL variant
    if "clickhouse_sql" in query_dict:
        return query_dict["clickhouse_sql"]
    
    # Adapt from generic or PostgreSQL query
    sql = query_dict.get("postgres_sql") or query_dict.get("sql")
    
    # ClickHouse adaptations
    # Replace DATE_TRUNC with ClickHouse equivalent
    sql = sql.replace(
        "DATE_TRUNC('month', tpep_pickup_datetime)",
        "toStartOfMonth(tpep_pickup_datetime)"
    )
    sql = sql.replace(
        "DATE_TRUNC('month',tpep_pickup_datetime)",
        "toStartOfMonth(tpep_pickup_datetime)"
    )
    
    # Replace strftime calls if present (SQLite style)
    sql = sql.replace(
        "strftime('%Y-%m-01', tpep_pickup_datetime)",
        "toStartOfMonth(tpep_pickup_datetime)"
    )
    
    return sql


def run_query_clickhouse(con, sql: str):
    """Execute query on ClickHouse and return results, time, and status."""
    start = time.perf_counter()
    try:
        result = con.execute(sql)
        elapsed = time.perf_counter() - start
        return result, elapsed, "PASS", ""
    except Exception as e:
        elapsed = time.perf_counter() - start
        return None, elapsed, "FAIL", str(e)[:80]


@pytest.mark.benchmark
class TestClickHouseQueries:
    """ClickHouse benchmark on analytical queries."""

    def test_clickhouse_query_correctness(self, clickhouse_con, queries):
        """Verify all 20 queries execute successfully without errors."""
        print("\n" + "="*80)
        print("ClickHouse Query Correctness Validation")
        print("="*80)

        passed = 0
        failed = 0
        skipped = 0

        for query in queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")

            sql = get_clickhouse_query(query)

            print(f"\n[Query {query_id}] {query_name}")

            result, elapsed, status, error = run_query_clickhouse(clickhouse_con, sql)

            if status == "PASS":
                rows = len(result) if result else 0
                print(f"  ✅ PASS ({elapsed:.3f}s, {rows} rows)")
                passed += 1
            elif status == "SKIP":
                print(f"  ⊘ SKIP: {error}")
                skipped += 1
            else:
                print(f"  ❌ FAIL ({elapsed:.3f}s): {error}")
                failed += 1

        print("\n" + "="*80)
        print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
        print("="*80)

        assert failed == 0, f"{failed} queries failed"

    def test_benchmark_all_queries(self, clickhouse_con, queries):
        """Run all 20 benchmark queries on ClickHouse and measure performance."""
        results = {}

        print("\n" + "="*80)
        print("ClickHouse Analytical-DB-Knockout Benchmark")
        print("="*80)

        timings = []

        for query in queries:
            query_id = query["id"]
            query_name = query.get("title", f"Query {query_id}")
            complexity = query.get("complexity", "N/A")

            sql = get_clickhouse_query(query)

            print(f"\n[Query {query_id}] {query_name} ({complexity})")

            # Warm-up run (not counted)
            _, _, status, _ = run_query_clickhouse(clickhouse_con, sql)
            if status != "PASS":
                print(f"  ⊘ Skipped (failed)")
                continue

            # Measure multiple runs
            runs = 3
            run_times = []
            for run_num in range(1, runs + 1):
                result, elapsed, status, error = run_query_clickhouse(
                    clickhouse_con, sql
                )

                if status == "PASS":
                    run_times.append(elapsed)
                    print(f"  Run {run_num}: {elapsed:.3f}s", end="")
                    if result:
                        print(f" ({len(result)} rows)")
                    else:
                        print()
                else:
                    print(f"  Run {run_num}: FAILED - {error}")

            if run_times:
                avg_time = sum(run_times) / len(run_times)
                min_time = min(run_times)
                max_time = max(run_times)

                results[query_id] = {
                    "title": query_name,
                    "complexity": complexity,
                    "avg_time": avg_time,
                    "min_time": min_time,
                    "max_time": max_time,
                    "runs": run_times,
                }

                timings.append((query_name, avg_time))

                print(f"  Average: {avg_time:.3f}s (min: {min_time:.3f}s, max: {max_time:.3f}s)")

        # Summary statistics
        print("\n" + "="*80)
        print("Performance Summary")
        print("="*80)

        if timings:
            timings_sorted = sorted(timings, key=lambda x: x[1], reverse=True)
            print("\nTop 5 Slowest Queries:")
            for name, elapsed in timings_sorted[:5]:
                print(f"  {elapsed:7.3f}s - {name}")

            print("\nTop 5 Fastest Queries:")
            for name, elapsed in timings_sorted[-5:]:
                print(f"  {elapsed:7.3f}s - {name}")

            total_time = sum(t for _, t in timings)
            avg_time = total_time / len(timings)
            print(f"\nTotal time for all queries: {total_time:.3f}s")
            print(f"Average query time: {avg_time:.3f}s")

        print("="*80)

        # Save results
        self._save_results(results)

    def test_data_stats(self, clickhouse_con):
        """Verify data is loaded and show statistics."""
        print("\n" + "="*80)
        print("ClickHouse Data Statistics")
        print("="*80)

        # Basic statistics
        stats_query = """
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT VendorID) as vendors,
                COUNT(DISTINCT PULocationID) as pickup_locations,
                COUNT(DISTINCT DOLocationID) as dropoff_locations,
                MIN(tpep_pickup_datetime) as min_date,
                MAX(tpep_pickup_datetime) as max_date,
                ROUND(AVG(total_amount), 2) as avg_fare,
                ROUND(SUM(total_amount), 2) as total_revenue
            FROM yellow_taxi_trips
        """

        result = clickhouse_con.execute(stats_query)
        if result:
            row = result[0]
            print(f"\n  Total Rows: {row[0]:,}")
            print(f"  Vendors: {row[1]}")
            print(f"  Pickup Locations: {row[2]:,}")
            print(f"  Dropoff Locations: {row[3]:,}")
            print(f"  Date Range: {row[4]} to {row[5]}")
            print(f"  Average Fare: ${row[6]}")
            print(f"  Total Revenue: ${row[7]:,.2f}")

        print("="*80)

    def test_sample_query_results(self, clickhouse_con):
        """Execute sample queries and display results."""
        print("\n" + "="*80)
        print("Sample Query Results from ClickHouse")
        print("="*80)

        # Sample 1: Top 5 routes by revenue
        print("\n📊 Top 5 Routes by Revenue:")
        query = """
            SELECT
                PULocationID,
                DOLocationID,
                COUNT(*) as trips,
                ROUND(SUM(total_amount), 2) as revenue,
                ROUND(AVG(total_amount), 2) as avg_fare
            FROM yellow_taxi_trips
            WHERE PULocationID != DOLocationID
            GROUP BY PULocationID, DOLocationID
            ORDER BY revenue DESC
            LIMIT 5
        """
        result = clickhouse_con.execute(query)
        for row in result:
            print(f"  PU: {row[0]:3d} → DO: {row[1]:3d} | {row[2]:6,} trips | ${row[3]:10,.2f} | Avg: ${row[4]:6.2f}")

        # Sample 2: Daily revenue
        print("\n📊 Last 5 Days Revenue:")
        query = """
            SELECT
                toDate(tpep_pickup_datetime) as trip_date,
                COUNT(*) as trips,
                ROUND(SUM(total_amount), 2) as daily_revenue,
                ROUND(AVG(total_amount), 2) as avg_fare
            FROM yellow_taxi_trips
            GROUP BY trip_date
            ORDER BY trip_date DESC
            LIMIT 5
        """
        result = clickhouse_con.execute(query)
        for row in result:
            print(f"  {row[0]} | {row[1]:6,} trips | ${row[2]:10,.2f} | Avg: ${row[3]:6.2f}")

        # Sample 3: Hourly distribution
        print("\n📊 Hourly Trip Distribution (Sample Hour):")
        query = """
            SELECT
                toHour(tpep_pickup_datetime) as hour,
                COUNT(*) as trips,
                ROUND(SUM(total_amount), 2) as revenue
            FROM yellow_taxi_trips
            GROUP BY hour
            ORDER BY hour
            LIMIT 24
        """
        result = clickhouse_con.execute(query)
        for row in result:
            trips = row[1]
            bar = "█" * min(50, trips // 100)
            print(f"  {row[0]:02d}:00 | {bar:50s} | {trips:6,} trips | ${row[2]:8,.2f}")

        print("="*80)

    @staticmethod
    def _save_results(results):
        """Save benchmark results to JSON."""
        results_file = Path(__file__).parent / "results" / "clickhouse_results.json"
        results_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\n✅ Results saved to {results_file}")
        except Exception as e:
            print(f"\n⚠️  Could not save results: {e}")
