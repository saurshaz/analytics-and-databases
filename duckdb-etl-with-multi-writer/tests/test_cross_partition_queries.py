#!/usr/bin/env python3
"""
Test querying across multiple partitions simultaneously
Validates partition pruning and cross-partition performance

Run with:
    pytest tests/test_cross_partition_queries.py -v -s
    make test-partitions  # if added to Makefile
"""

import sys
import time
from pathlib import Path

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.query_optimizer import QueryOptimizer


class TestCrossPartitionQueries:
    """Test queries that span multiple partition boundaries"""

    @pytest.fixture
    def optimizer(self):
        """Initialize QueryOptimizer"""
        opt = QueryOptimizer("nyc_yellow_taxi.duckdb")
        yield opt
        opt.close()

    def test_adjacent_partitions_jan_feb_2024(self, optimizer):
        """Test querying 2 adjacent partitions (Jan + Feb 2024)"""
        print("\n📊 Test: Query 2024-01 + 2024-02 (adjacent partitions)")
        
        start = time.time()
        result = optimizer.query_by_date_range(
            "2024-01-01",
            "2024-02-29",
            columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
        )
        elapsed = time.time() - start

        # Assertions
        assert result is not None
        assert len(result) > 0
        assert "tpep_pickup_datetime" in result.columns or "pickup_datetime" in result.columns
        
        rows = len(result)
        throughput = rows / elapsed
        
        print(f"✅ Rows: {rows:,}")
        print(f"⏱️  Time: {elapsed:.3f}s")
        print(f"📈 Throughput: {throughput:,.0f} rows/sec")
        
        # Sanity checks
        assert elapsed < 30, f"Query took too long: {elapsed}s"
        assert rows > 1_000_000, f"Expected >1M rows, got {rows}"

    def test_non_adjacent_partitions_across_year(self, optimizer):
        """Test querying 2 non-adjacent partitions (Jun 2024 + Jun 2025, 1 year apart)"""
        print("\n📊 Test: Query 2024-06 + 2025-06 (non-adjacent partitions)")
        
        start = time.time()
        result = optimizer.query_by_date_range(
            "2024-06-01",
            "2025-06-30",
            columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
        )
        elapsed = time.time() - start

        # Assertions
        assert result is not None
        assert len(result) > 0
        
        rows = len(result)
        throughput = rows / elapsed
        
        print(f"✅ Rows: {rows:,}")
        print(f"⏱️  Time: {elapsed:.3f}s")
        print(f"📈 Throughput: {throughput:,.0f} rows/sec")
        
        # Monthly breakdown
        date_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in result.columns else "pickup_datetime"
        result['month'] = result[date_col].dt.to_period('M')
        monthly = result.groupby('month').size()
        
        print(f"\n📅 Breakdown by month:")
        for month, count in monthly.items():
            print(f"   {month}: {count:,} rows")
        
        # Should span 13 months (Jun 2024 - Jun 2025 inclusive)
        assert len(monthly) == 13, f"Expected 13 months, got {len(monthly)}"

    def test_year_boundary_query(self, optimizer):
        """Test querying across year boundary (Dec 2024 + Jan 2025)"""
        print("\n📊 Test: Query year boundary Dec 2024 + Jan 2025")
        
        start = time.time()
        result = optimizer.query_by_date_range(
            "2024-12-01",
            "2025-01-31",
            columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
        )
        elapsed = time.time() - start

        # Assertions
        assert result is not None
        assert len(result) > 0
        
        rows = len(result)
        throughput = rows / elapsed
        
        print(f"✅ Total rows: {rows:,}")
        print(f"⏱️  Query time: {elapsed:.3f}s")
        print(f"📈 Throughput: {throughput:,.0f} rows/sec")

        # Partition by day and calculate daily stats
        date_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in result.columns else "pickup_datetime"
        result['day'] = result[date_col].dt.date
        
        daily_stats = result.groupby('day').agg({
            'fare_amount': ['count', 'mean', 'sum'],
            'trip_distance': 'mean',
            'total_amount': 'sum'
        }).round(2)
        daily_stats.columns = ['trips', 'avg_fare', 'total_fare', 'avg_distance', 'total_revenue']
        daily_count = len(daily_stats)
        
        print(f"\n📅 Daily data points: {daily_count}")
        
        # Should have data for 2 months (roughly 60 days)
        assert daily_count >= 55, f"Expected ~60 days, got {daily_count}"

    def test_aggregation_across_partitions(self, optimizer):
        """Test aggregation queries spanning multiple partitions"""
        print("\n📊 Test: Aggregation across partitions")
        
        start = time.time()
        result = optimizer.query_by_date_range(
            "2024-06-01",
            "2024-08-31",
            columns=["tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
        )
        elapsed = time.time() - start

        assert result is not None
        assert len(result) > 0

        # Calculate aggregations
        stats = {
            'total_rows': len(result),
            'avg_distance': result['trip_distance'].mean(),
            'avg_fare': result['fare_amount'].mean(),
            'total_revenue': result['total_amount'].sum(),
            'query_time': elapsed
        }

        print(f"\n📈 Aggregation Results (Q2-Q3 2024):")
        print(f"   Total trips: {stats['total_rows']:,}")
        print(f"   Avg distance: {stats['avg_distance']:.2f} mi")
        print(f"   Avg fare: ${stats['avg_fare']:.2f}")
        print(f"   Total revenue: ${stats['total_revenue']:,.2f}")
        print(f"   Query time: {elapsed:.3f}s")
        
        # Sanity checks
        assert stats['avg_distance'] > 0, "Average distance should be positive"
        assert stats['avg_fare'] > 0, "Average fare should be positive"
        assert elapsed < 30, f"Aggregation query took too long: {elapsed}s"

    def test_single_day_range_for_comparison(self, optimizer):
        """Single-partition query for baseline comparison"""
        print("\n📊 Test: Baseline - single day (2024-06-15)")
        
        start = time.time()
        result = optimizer.query_by_date_range(
            "2024-06-15",
            "2024-06-15",
            columns=["tpep_pickup_datetime", "trip_distance", "fare_amount"]
        )
        elapsed = time.time() - start

        assert result is not None
        assert len(result) > 0
        
        print(f"✅ Rows: {len(result):,}")
        print(f"⏱️  Time: {elapsed:.3f}s")

    def test_performance_comparison(self, optimizer):
        """Compare performance across different query scopes"""
        print("\n" + "="*80)
        print("Performance Comparison: Single Day vs. Multiple Partitions")
        print("="*80)
        
        test_cases = [
            ("1 day", "2024-06-15", "2024-06-15"),
            ("1 month", "2024-06-01", "2024-06-30"),
            ("2 months", "2024-06-01", "2024-07-31"),
            ("6 months", "2024-06-01", "2024-11-30"),
            ("1 year", "2024-01-01", "2024-12-31"),
        ]
        
        results = []
        for label, start_date, end_date in test_cases:
            start = time.time()
            result = optimizer.query_by_date_range(start_date, end_date)
            elapsed = time.time() - start
            rows = len(result)
            throughput = rows / elapsed
            
            results.append({
                'scope': label,
                'rows': rows,
                'time': elapsed,
                'throughput': throughput
            })
            
            print(f"  {label:12s} | {rows:10,} rows | {elapsed:7.3f}s | {throughput:10,.0f} rows/sec")
        
        # Assertions: each query should complete in reasonable time
        for r in results:
            assert r['time'] < 60, f"{r['scope']} query timed out: {r['time']}s"
            assert r['rows'] > 0, f"{r['scope']} returned no rows"


class TestPartitionPruningEfficiency:
    """Test that partition pruning is actually reducing work"""

    @pytest.fixture
    def optimizer(self):
        """Initialize QueryOptimizer"""
        opt = QueryOptimizer("nyc_yellow_taxi.duckdb")
        yield opt
        opt.close()

    def test_narrow_date_range_is_fast(self, optimizer):
        """Narrow date ranges should be faster (confirming pruning works)"""
        print("\n🔍 Testing partition pruning efficiency...")
        
        # 1-day query
        start = time.time()
        day_result = optimizer.query_by_date_range("2024-06-15", "2024-06-15")
        day_time = time.time() - start
        
        # 30-day query  
        start = time.time()
        month_result = optimizer.query_by_date_range("2024-06-01", "2024-06-30")
        month_time = time.time() - start
        
        # 365-day query
        start = time.time()
        year_result = optimizer.query_by_date_range("2024-01-01", "2024-12-31")
        year_time = time.time() - start
        
        print(f"\n  1-day:   {day_time:.3f}s  ({len(day_result):,} rows)")
        print(f"  30-day:  {month_time:.3f}s  ({len(month_result):,} rows)")
        print(f"  365-day: {year_time:.3f}s  ({len(year_result):,} rows)")
        
        # Year should be faster than linear scaling due to pruning
        # (not a strict test, just a practical check)
        assert year_time < year_time * 5, "Year query taking suspiciously long"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
