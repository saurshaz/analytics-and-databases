#!/usr/bin/env python3
"""
ETL Pipeline Orchestrator

Main orchestration for NYC Yellow Taxi ETL with Registry Locking coordination.
Handles yearly loads, data transformations, and performance tracking.

Usage:
    pipeline = ETLPipeline(db_path='nyc_yellow_taxi.duckdb')
    pipeline.load_year(2023, writer_id='worker_1')
    pipeline.show_status()
"""

import duckdb
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

from .duckdb_multiwriter_etl import DuckDBMultiWriterETL
from .registry_lock_manager import RegistryLockManager


class ETLPipeline:
    """
    Main ETL pipeline coordinator for NYC Yellow Taxi data
    
    Features:
    - Yearly data ingestion with registry locking
    - Data quality validation
    - Query performance benchmarking
    - Registry audit trail
    """
    
    def __init__(
        self,
        db_path: str = 'nyc_yellow_taxi.duckdb',
        data_dir: str = '../NYC Yellow Taxi Record 23-24-25',
        pipeline_id: str = 'taxi_etl_v1',
        timeout: int = 300
    ):
        """
        Initialize ETL pipeline
        
        Args:
            db_path: Path to DuckDB database
            data_dir: Root directory for NYC taxi data
            pipeline_id: Pipeline identifier
            timeout: Lock timeout in seconds
        """
        self.db_path = Path(db_path)
        self.data_dir = Path(data_dir)
        self.pipeline_id = pipeline_id
        self.timeout = timeout
        
        # Initialize multi-writer ETL
        self.etl = DuckDBMultiWriterETL(
            db_path=str(self.db_path),
            pipeline_id=pipeline_id,
            timeout=timeout
        )
        
        # Registry access
        self.registry = self.etl.registry
    
    def load_year(
        self,
        year: int,
        writer_id: str = 'etl_worker',
        if_exists: str = 'create'
    ) -> Dict[str, Any]:
        """
        Load all taxi data for a specific year
        
        Args:
            year: Year to load (2023, 2024, 2025)
            writer_id: Writer identifier for lock
            if_exists: 'create' for new table, 'append' for existing
        
        Returns:
            Statistics dict with rows_loaded, duration, etc
        """
        run_id = f"load_year_{year}"
        
        # Determine parquet glob pattern
        parquet_glob = f"{self.data_dir}/{year}/*.parquet"
        
        print(f"\n📅 Loading {year} data with Registry Locking...")
        
        try:
            stats = self.etl.load_parquet_safe(
                parquet_glob=parquet_glob,
                table_name='yellow_taxi_trips',
                run_id=run_id,
                writer_id=writer_id,
                if_exists=if_exists,
                union_by_name=True
            )
            
            print(f"✅ {year} load complete:")
            print(f"   Rows:     {stats['rows_loaded']:,}")
            print(f"   Duration: {stats['duration_sec']:.2f}s")
            print(f"   Speed:    {stats['rows_loaded']/stats['duration_sec']:,.0f} rows/sec")
            
            return stats
        
        except Exception as e:
            print(f"❌ Failed to load {year}: {e}")
            raise
    
    def load_all_years(
        self,
        years: List[int] = None,
        writer_id_prefix: str = 'worker'
    ) -> List[Dict[str, Any]]:
        """
        Load multiple years of data sequentially with locks
        
        Args:
            years: List of years to load (default: [2023, 2024, 2025])
            writer_id_prefix: Prefix for writer IDs
        
        Returns:
            List of statistics dicts
        """
        years = years or [2023, 2024, 2025]
        all_stats = []
        
        for idx, year in enumerate(years):
            writer_id = f"{writer_id_prefix}_{year}"
            if_exists = 'create' if idx == 0 else 'append'
            
            stats = self.load_year(
                year=year,
                writer_id=writer_id,
                if_exists=if_exists
            )
            all_stats.append(stats)
        
        return all_stats
    
    def validate_data(self) -> Dict[str, Any]:
        """
        Validate loaded data quality
        
        Returns:
            Validation results dict
        """
        con = duckdb.connect(str(self.db_path))
        
        try:
            print("\n🔍 Validating data...")
            
            # Row count
            result = con.execute(
                "SELECT COUNT(*) as count FROM yellow_taxi_trips"
            ).fetchall()
            total_rows = result[0][0] if result else 0
            
            # Column validation
            columns = con.execute(
                "SELECT * FROM yellow_taxi_trips LIMIT 0"
            ).description
            col_count = len(columns)
            
            # Year distribution
            year_dist = con.execute("""
                SELECT 
                    YEAR(tpep_pickup_datetime) as year,
                    COUNT(*) as count
                FROM yellow_taxi_trips
                GROUP BY YEAR(tpep_pickup_datetime)
                ORDER BY year
            """).fetchall()
            
            print(f"✅ Validation complete:")
            print(f"   Total rows:  {total_rows:,}")
            print(f"   Columns:     {col_count}")
            print(f"   Year distribution:")
            for year, count in year_dist:
                print(f"     {year}: {count:,} rows")
            
            return {
                'status': 'valid',
                'total_rows': total_rows,
                'column_count': col_count,
                'year_distribution': dict(year_dist)
            }
        
        finally:
            con.close()
    
    def run_sample_queries(self) -> Dict[str, Any]:
        """
        Run sample analytical queries for benchmarking
        
        Returns:
            Query results and timing
        """
        con = duckdb.connect(str(self.db_path))
        results = {}
        
        try:
            print("\n⏱️  Running sample queries...")
            
            # Query 1: Daily fare average
            q1_start = datetime.now()
            q1_result = con.execute("""
                SELECT 
                    DATE(tpep_pickup_datetime) as pickup_date,
                    COUNT(*) as trips,
                    AVG(total_amount) as avg_fare
                FROM yellow_taxi_trips
                GROUP BY DATE(tpep_pickup_datetime)
                ORDER BY pickup_date DESC
                LIMIT 10
            """).fetchall()
            q1_duration = (datetime.now() - q1_start).total_seconds()
            
            results['daily_aggregation'] = {
                'duration_sec': q1_duration,
                'rows_returned': len(q1_result)
            }
            print(f"   Daily aggregation: {q1_duration:.3f}s")
            
            # Query 2: Vendor performance
            q2_start = datetime.now()
            q2_result = con.execute("""
                SELECT 
                    VendorID,
                    COUNT(*) as trips,
                    AVG(trip_distance) as avg_distance,
                    AVG(total_amount) as avg_fare
                FROM yellow_taxi_trips
                GROUP BY VendorID
            """).fetchall()
            q2_duration = (datetime.now() - q2_start).total_seconds()
            
            results['vendor_performance'] = {
                'duration_sec': q2_duration,
                'rows_returned': len(q2_result)
            }
            print(f"   Vendor analysis: {q2_duration:.3f}s")
            
            # Query 3: Peak hours
            q3_start = datetime.now()
            q3_result = con.execute("""
                SELECT 
                    HOUR(tpep_pickup_datetime) as hour_of_day,
                    COUNT(*) as trip_count,
                    AVG(total_amount) as avg_fare
                FROM yellow_taxi_trips
                GROUP BY HOUR(tpep_pickup_datetime)
                ORDER BY trip_count DESC
            """).fetchall()
            q3_duration = (datetime.now() - q3_start).total_seconds()
            
            results['peak_hours'] = {
                'duration_sec': q3_duration,
                'rows_returned': len(q3_result)
            }
            print(f"   Peak hours: {q3_duration:.3f}s")
            
            print(f"✅ Queries complete")
            return results
        
        finally:
            con.close()
    
    def show_status(self) -> None:
        """Display current pipeline status"""
        print("\n" + "=" * 70)
        print("  ETL Pipeline Status Report")
        print("=" * 70)
        
        status = self.etl.get_registry_status()
        
        # Active locks
        active = status['active_locks']
        print(f"\n🔒 Active Locks: {len(active)}")
        if active:
            for lock in active:
                print(f"   • {lock['writer_id']}: {lock['lock_id']}")
        else:
            print("   (none)")
        
        # ETL runs
        runs = status['all_runs']
        print(f"\n📊 ETL Runs: {len(runs)} total")
        
        if runs:
            # Success/failure summary
            success = sum(1 for r in runs if r['status'] == 'completed')
            failed = sum(1 for r in runs if r['status'] == 'failed')
            
            print(f"   ✅ Completed: {success}")
            print(f"   ❌ Failed: {failed}")
            
            # Total rows
            total_rows = sum(r.get('rows_written', 0) for r in runs)
            print(f"   📈 Total rows written: {total_rows:,}")
            
            # Recent runs
            print(f"\n   Recent runs:")
            for run in runs[-3:]:
                duration = 'pending'
                if run.get('ended_at') and run.get('started_at'):
                    try:
                        from dateutil.parser import parse as parse_iso
                        start = parse_iso(run['started_at'])
                        end = parse_iso(run['ended_at'])
                        
                        # Handle both aware and naive datetimes
                        if start.tzinfo is None and end.tzinfo is None:
                            # Both naive, safe to subtract
                            duration = f"{(end - start).total_seconds():.1f}s"
                        elif start.tzinfo is not None and end.tzinfo is not None:
                            # Both aware, safe to subtract
                            duration = f"{(end - start).total_seconds():.1f}s"
                        else:
                            # Mixed, make both aware
                            from datetime import timezone as dt_timezone
                            if start.tzinfo is None:
                                start = start.replace(tzinfo=dt_timezone.utc)
                            if end.tzinfo is None:
                                end = end.replace(tzinfo=dt_timezone.utc)
                            duration = f"{(end - start).total_seconds():.1f}s"
                    except Exception as e:
                        duration = f"error: {e}"
                
                print(f"   • {run['run_id']}")
                print(f"     Status: {run['status']}, Duration: {duration}")
        
        print("\n" + "=" * 70 + "\n")
    
    def cleanup_old_locks(self, older_than_hours: int = 24) -> int:
        """Clean up old lock entries from registry"""
        return self.etl.cleanup_old_locks(older_than_hours)


if __name__ == '__main__':
    # Example usage
    print("🚀 NYC Yellow Taxi ETL Pipeline\n")
    
    pipeline = ETLPipeline(
        db_path='nyc_yellow_taxi.duckdb',
        pipeline_id='taxi_etl_demo'
    )
    
    # Load 2023 data
    try:
        pipeline.load_year(2023, writer_id='worker_2023')
        
        # Validate
        pipeline.validate_data()
        
        # Run queries
        pipeline.run_sample_queries()
        
        # Show status
        pipeline.show_status()
    
    except Exception as e:
        print(f"Error: {e}")
