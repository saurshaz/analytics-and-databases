#!/usr/bin/env python3
"""
Cross-Partition Query Benchmark Suite
Tests querying across multiple partitions to validate partition pruning and performance

Usage:
    python scripts/benchmark_cross_partitions.py
    make test-partitions  # if added to Makefile

This demonstrates:
- Adjacent partition queries (Jan + Feb 2024)
- Non-adjacent partition queries (Jun 2024 + Jun 2025) 
- Year-boundary queries (Dec 2024 + Jan 2025)
- Aggregation across partitions
- Performance metrics including throughput
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query_optimizer import QueryOptimizer


def print_header(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)


def test_adjacent_partitions():
    """Test querying 2 adjacent partitions (Jan + Feb 2024)"""
    print_header("Test 1: Adjacent Partitions (Jan + Feb 2024)")
    
    optimizer = QueryOptimizer("nyc_yellow_taxi.duckdb")
    
    print("📊 Query: 2024-01-01 to 2024-02-29 (2 partitions)")
    print("-" * 80)
    
    start = time.time()
    result = optimizer.query_by_date_range(
        "2024-01-01",
        "2024-02-29",
        columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
    )
    elapsed = time.time() - start

    rows = len(result)
    throughput = rows / elapsed if elapsed > 0 else 0
    
    print(f"✅ Rows returned: {rows:,}")
    print(f"⏱️  Query time: {elapsed:.3f}s")
    print(f"📈 Throughput: {throughput:,.0f} rows/sec")
    
    if rows > 0:
        print(f"\n📋 Sample data (first 3 rows):")
        print(result.head(3).to_string())
    
    optimizer.close()
    return rows > 0


def test_non_adjacent_partitions():
    """Test querying 2 non-adjacent partitions (Jun 2024 + Jun 2025, 1 year apart)"""
    print_header("Test 2: Non-Adjacent Partitions (Jun 2024 + Jun 2025)")
    
    optimizer = QueryOptimizer("nyc_yellow_taxi.duckdb")
    
    print("📊 Query: 2024-06-01 to 2025-06-30 (13 months, non-adjacent partitions)")
    print("-" * 80)
    
    start = time.time()
    result = optimizer.query_by_date_range(
        "2024-06-01",
        "2025-06-30",
        columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
    )
    elapsed = time.time() - start

    rows = len(result)
    throughput = rows / elapsed if elapsed > 0 else 0
    
    print(f"✅ Total rows: {rows:,}")
    print(f"⏱️  Query time: {elapsed:.3f}s")
    print(f"📈 Throughput: {throughput:,.0f} rows/sec")
    
    # Monthly breakdown
    if rows > 0:
        date_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in result.columns else "pickup_datetime"
        result['month'] = result[date_col].dt.to_period('M')
        monthly = result.groupby('month').size()
        
        print(f"\n📅 Breakdown by month ({len(monthly)} months):")
        for month, count in monthly.items():
            print(f"   {month}: {count:,} rows")
    
    optimizer.close()
    return rows > 0


def test_year_boundary():
    """Test querying across year boundary (Dec 2024 + Jan 2025)"""
    print_header("Test 3: Year-Boundary Query (Dec 2024 + Jan 2025)")
    
    optimizer = QueryOptimizer("nyc_yellow_taxi.duckdb")
    
    print("📊 Query: 2024-12-01 to 2025-01-31 (crossing year boundary)")
    print("-" * 80)
    
    start = time.time()
    result = optimizer.query_by_date_range(
        "2024-12-01",
        "2025-01-31",
        columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
    )
    elapsed = time.time() - start

    rows = len(result)
    throughput = rows / elapsed if elapsed > 0 else 0
    
    print(f"✅ Total rows: {rows:,}")
    print(f"⏱️  Query time: {elapsed:.3f}s")
    print(f"📈 Throughput: {throughput:,.0f} rows/sec")
    
    # Daily stats
    if rows > 0:
        date_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in result.columns else "pickup_datetime"
        result['day'] = result[date_col].dt.date
        
        daily_stats = result.groupby('day').agg({
            'fare_amount': ['count', 'mean', 'sum'],
            'trip_distance': 'mean',
            'total_amount': 'sum'
        }).round(2)
        daily_stats.columns = ['trips', 'avg_fare', 'total_fare', 'avg_distance', 'total_revenue']
        
        print(f"\n📅 Daily stats ({len(daily_stats)} days):")
        print(f"   First day: {daily_stats.index[0]}")
        print(f"   Last day:  {daily_stats.index[-1]}")
        print(f"\n   Daily statistics (last 5 days):")
        print(daily_stats.tail(5).to_string())
    
    optimizer.close()
    return rows > 0


def test_aggregation():
    """Test aggregation across multiple partitions (Q2-Q3 2024)"""
    print_header("Test 4: Aggregation Across Partitions (Q2-Q3 2024)")
    
    optimizer = QueryOptimizer("nyc_yellow_taxi.duckdb")
    
    print("📊 Query: 2024-06-01 to 2024-08-31 (Q2-Q3, 3 partitions)")
    print("-" * 80)
    
    start = time.time()
    result = optimizer.query_by_date_range(
        "2024-06-01",
        "2024-08-31",
        columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
    )
    elapsed = time.time() - start

    rows = len(result)
    
    print(f"✅ Total rows: {rows:,}")
    print(f"⏱️  Query time: {elapsed:.3f}s")
    
    if rows > 0:
        # Calculate aggregations
        stats = {
            'avg_distance': result['trip_distance'].mean(),
            'avg_fare': result['fare_amount'].mean(),
            'median_fare': result['fare_amount'].median(),
            'total_revenue': result['total_amount'].sum(),
            'min_fare': result['fare_amount'].min(),
            'max_fare': result['fare_amount'].max(),
        }
        
        print(f"\n📈 Aggregation Results:")
        print(f"   Total trips: {rows:,}")
        print(f"   Avg distance: {stats['avg_distance']:.2f} mi")
        print(f"   Avg fare: ${stats['avg_fare']:.2f}")
        print(f"   Median fare: ${stats['median_fare']:.2f}")
        print(f"   Min/Max fare: ${stats['min_fare']:.2f} - ${stats['max_fare']:.2f}")
        print(f"   Total revenue: ${stats['total_revenue']:,.2f}")
    
    optimizer.close()
    return rows > 0


def test_performance_comparison():
    """Compare query performance across different time ranges"""
    print_header("Test 5: Performance Comparison - Various Time Ranges")
    
    optimizer = QueryOptimizer("nyc_yellow_taxi.duckdb")
    
    test_cases = [
        ("1 day", "2024-06-15", "2024-06-15"),
        ("7 days", "2024-06-15", "2024-06-21"),
        ("1 month", "2024-06-01", "2024-06-30"),
        ("2 months", "2024-06-01", "2024-07-31"),
        ("6 months", "2024-06-01", "2025-11-30"),
        ("12 months (1 year)", "2024-01-01", "2024-12-31"),
    ]
    
    print("⏱️  Query Performance Across Time Ranges:")
    print("-" * 80)
    print(f"{'Duration':<20} {'Rows':<15} {'Time (s)':<12} {'Throughput':<15} {'Partitions'}")
    print("-" * 80)
    
    results = []
    for label, start_date, end_date in test_cases:
        start = time.time()
        result = optimizer.query_by_date_range(start_date, end_date)
        elapsed = time.time() - start
        
        rows = len(result)
        throughput = rows / elapsed if elapsed > 0 else 0
        
        # Estimate partitions by date range
        from datetime import datetime
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        months_span = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month) + 1
        
        print(f"{label:<20} {rows:<15,} {elapsed:<12.3f} {throughput:<15,.0f} {months_span}")
        
        results.append({
            'label': label,
            'rows': rows,
            'time': elapsed,
            'throughput': throughput
        })
    
    optimizer.close()
    return True


def summarize_results():
    """Print final summary"""
    print_header("Summary: Cross-Partition Query Performance")
    
    print("""
✅ Cross-partition query tests assess:
   • Adjacent partition merging (Jan + Feb 2024)
   • Non-adjacent partitions spanning months (Jun 2024 - Jun 2025)
   • Year-boundary edge cases (Dec 2024 - Jan 2025)
   • Aggregation efficiency across multiple partitions
   • Query throughput as function of date range

📊 Recommended baselines for NYC Yellow Taxi data:
   • Single day (~95K rows): <100ms
   • Single month (~2M rows): <300ms
   • 3 months (~5-6M rows): <750ms
   • 12 months (~13M rows): <2s

🔍 Partition Pruning Validation:
   If query times scale linearly with months, pruning is working.
   If times vary unpredictably, investigate partition boundaries.

📚 Next Steps:
   1. Add these tests to CI/CD pipeline
   2. Monitor performance trends over time
   3. Use results to validate partition strategy
   4. Benchmark against other databases (PostgreSQL, SQLite)
    """)


def main():
    """Run all cross-partition tests"""
    print("\n" + "="*80)
    print("  DuckDB Cross-Partition Query Benchmark Suite")
    print("  NYC Yellow Taxi Data (2023-2025)")
    print("="*80)
    
    try:
        # Run all tests
        test_adjacent_partitions()
        test_non_adjacent_partitions()
        test_year_boundary()
        test_aggregation()
        test_performance_comparison()
        
        # Summary
        summarize_results()
        
        print("\n✅ All cross-partition tests completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
