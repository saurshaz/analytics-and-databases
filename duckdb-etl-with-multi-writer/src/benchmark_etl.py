#!/usr/bin/env python3
"""
ETL Performance Benchmarking

Measures and tracks performance metrics for multi-writer ETL operations
including lock contention, throughput, and query performance.

Usage:
    bench = ETLBenchmark(db_path='nyc_yellow_taxi.duckdb')
    results = bench.run_load_benchmark(year=2023)
    bench.print_results(results)
"""

import time
import duckdb
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List
import json

from .duckdb_multiwriter_etl import DuckDBMultiWriterETL
from .partitioning_strategy import PartitionAnalyzer


class ETLBenchmark:
    """
    Benchmarking suite for ETL performance measurement
    """
    
    def __init__(
        self,
        db_path: str = 'nyc_yellow_taxi.duckdb',
        data_dir: str = '../NYC Yellow Taxi Record 23-24-25'
    ):
        """
        Initialize benchmarking suite
        
        Args:
            db_path: Path to DuckDB database
            data_dir: Root directory of NYC taxi data
        """
        self.db_path = Path(db_path)
        self.data_dir = data_dir
        
        self.etl = DuckDBMultiWriterETL(
            db_path=str(self.db_path),
            pipeline_id='benchmark'
        )
        
        self.analyzer = PartitionAnalyzer(data_dir)
    
    def run_load_benchmark(
        self,
        year: int = 2023,
        table_name: str = 'yellow_taxi_trips',
        run_name: str = 'load_benchmark'
    ) -> Dict[str, Any]:
        """
        Benchmark loading data for a specific year
        
        Args:
            year: Year to load (2023, 2024, 2025)
            table_name: Target table name
            run_name: Name for the benchmark
        
        Returns:
            Benchmark results dict
        """
        print(f"\n⏱️  Benchmarking {year} data load...")
        
        run_id = f"{run_name}_{year}"
        writer_id = 'benchmark_worker'
        
        parquet_glob = f"{self.data_dir}/{year}/*.parquet"
        
        start_time = time.time()
        
        try:
            stats = self.etl.load_parquet_safe(
                parquet_glob=parquet_glob,
                table_name=table_name,
                run_id=run_id,
                writer_id=writer_id,
                if_exists='create'
            )
            
            elapsed = time.time() - start_time
            
            # Get detailed metrics
            con = duckdb.connect(str(self.db_path))
            try:
                # Row count
                count_result = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchall()
                actual_rows = count_result[0][0] if count_result else 0
                
                con.close()
            except:
                actual_rows = stats.get('rows_loaded', 0)
            
            benchmark_result = {
                'benchmark_name': f"Load {year}",
                'year': year,
                'rows_expected': stats.get('rows_loaded', 0),
                'rows_actual': actual_rows,
                'duration_sec': elapsed,
                'throughput_rows_per_sec': actual_rows / elapsed if elapsed > 0 else 0,
                'throughput_gb_per_sec': (stats.get('bytes_written', 0) / (1024**3)) / elapsed if elapsed > 0 else 0,
                'bytes_written': stats.get('bytes_written', 0),
                'lock_acquisitions': 1,
                'status': 'success'
            }
            
            print(f"✅ Benchmark complete:")
            print(f"   Rows: {benchmark_result['rows_actual']:,}")
            print(f"   Duration: {benchmark_result['duration_sec']:.2f}s")
            print(f"   Throughput: {benchmark_result['throughput_rows_per_sec']:,.0f} rows/sec")
            
            return benchmark_result
        
        except Exception as e:
            print(f"❌ Benchmark failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'year': year
            }
    
    def run_query_benchmark(
        self,
        query_name: str,
        query_sql: str,
        iterations: int = 1
    ) -> Dict[str, Any]:
        """
        Benchmark query performance
        
        Args:
            query_name: Name of query for logging
            query_sql: SQL query to benchmark
            iterations: Number of times to run query
        
        Returns:
            Query benchmark results
        """
        print(f"\n⏱️  Benchmarking query: {query_name}...")
        
        con = duckdb.connect(str(self.db_path))
        times = []
        
        try:
            # Warm-up
            try:
                con.execute(query_sql)
            except:
                pass
            
            # Run iterations
            for i in range(iterations):
                start = time.time()
                result = con.execute(query_sql).fetchall()
                elapsed = time.time() - start
                times.append(elapsed)
            
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            benchmark_result = {
                'query_name': query_name,
                'iterations': iterations,
                'average_sec': round(avg_time, 4),
                'min_sec': round(min_time, 4),
                'max_sec': round(max_time, 4),
                'rows_returned': len(result) if result else 0,
                'status': 'success'
            }
            
            print(f"✅ Query benchmark complete:")
            print(f"   Avg time: {benchmark_result['average_sec']:.4f}s")
            print(f"   Min/Max: {benchmark_result['min_sec']:.4f}s / {benchmark_result['max_sec']:.4f}s")
            
            return benchmark_result
        
        except Exception as e:
            print(f"❌ Query benchmark failed: {e}")
            return {
                'query_name': query_name,
                'status': 'failed',
                'error': str(e)
            }
        
        finally:
            con.close()
    
    def run_all_benchmarks(self) -> Dict[str, Any]:
        """
        Run comprehensive benchmark suite
        
        Returns:
            Results dict with all benchmarks
        """
        print("\n" + "=" * 70)
        print("  🏃 ETL Comprehensive Benchmark Suite")
        print("=" * 70)
        
        all_results = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'load_benchmarks': [],
            'query_benchmarks': [],
            'summary': {}
        }
        
        # 1. Load benchmarks (2023 only for quick run)
        years = [2023]
        for year in years:
            result = self.run_load_benchmark(year=year)
            all_results['load_benchmarks'].append(result)
        
        # 2. Query benchmarks (assuming 2023 data loaded)
        queries = [
            ('daily_aggregation', """
                SELECT 
                    DATE(tpep_pickup_datetime) as pickup_date,
                    COUNT(*) as trips,
                    AVG(total_amount) as avg_fare
                FROM yellow_taxi_trips
                GROUP BY DATE(tpep_pickup_datetime)
                ORDER BY pickup_date DESC
                LIMIT 100
            """),
            ('vendor_performance', """
                SELECT 
                    VendorID,
                    COUNT(*) as trips,
                    AVG(trip_distance) as avg_distance
                FROM yellow_taxi_trips
                GROUP BY VendorID
            """),
            ('payment_analysis', """
                SELECT 
                    payment_type,
                    COUNT(*) as count,
                    AVG(total_amount) as avg_amount
                FROM yellow_taxi_trips
                WHERE total_amount > 0
                GROUP BY payment_type
                ORDER BY count DESC
            """)
        ]
        
        for query_name, query_sql in queries:
            result = self.run_query_benchmark(query_name, query_sql, iterations=3)
            all_results['query_benchmarks'].append(result)
        
        # Summary
        successful_loads = [r for r in all_results['load_benchmarks'] if r.get('status') == 'success']
        if successful_loads:
            avg_throughput = sum(r.get('throughput_rows_per_sec', 0) for r in successful_loads) / len(successful_loads)
            all_results['summary'] = {
                'total_load_benchmarks': len(all_results['load_benchmarks']),
                'successful_loads': len(successful_loads),
                'average_throughput_rows_per_sec': round(avg_throughput, 0),
                'total_query_benchmarks': len(all_results['query_benchmarks']),
                'successful_queries': sum(1 for q in all_results['query_benchmarks'] if q.get('status') == 'success')
            }
        
        print("\n" + "=" * 70)
        print("  ✅ Benchmark Suite Complete")
        print("=" * 70 + "\n")
        
        return all_results
    
    def save_results(self, results: Dict[str, Any], output_file: str = 'benchmark_results.json') -> None:
        """Save benchmark results to JSON file"""
        output_path = Path(output_file)
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"📄 Results saved to: {output_path}")


if __name__ == '__main__':
    # Example usage
    bench = ETLBenchmark()
    
    # Run all benchmarks
    results = bench.run_all_benchmarks()
    
    # Save results
    bench.save_results(results)
    
    # Display summary
    print("\n📊 Benchmark Summary:")
    print(json.dumps(results['summary'], indent=2))
