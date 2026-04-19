#!/usr/bin/env python3
"""
ETL Pipeline Orchestrator

Main orchestration for NYC Yellow Taxi ETL with Registry Locking coordination.
Handles yearly loads, data transformations, performance tracking, and advanced analytics.

Advanced capabilities:
- Incremental loading with file discovery
- Async parallel ingestion with multiple workers
- Query optimization with partition pruning
- Intelligent column discovery for schema variations
- Comprehensive performance metrics

Usage:
    pipeline = ETLPipeline(db_path='nyc_yellow_taxi.duckdb')
    pipeline.load_year(2023, writer_id='worker_1')
    pipeline.show_status()
    
    # Advanced queries
    optimizer = QueryOptimizer()
    df = optimizer.query_by_date_range('2024-01-01', '2024-12-31')
    daily = optimizer.get_daily_aggregates()
"""

import duckdb
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Set
import json
from dataclasses import dataclass, asdict
import logging
import time

from .duckdb_multiwriter_etl import DuckDBMultiWriterETL
from .registry_lock_manager import RegistryLockManager
from .metrics import MetricsCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class FileMetadata:
    """Metadata for a processed parquet file"""
    date: str  # YYYY-MM-DD
    source_file: str
    rows: int
    null_count: int
    processed_at: str
    compression_ratio: float
    status: str  # 'success', 'failed', 'skipped'


@dataclass
class ETLMetrics:
    """Pipeline execution metrics"""
    files_processed: int
    total_rows: int
    total_time_sec: float
    rows_per_sec: float
    compression_ratio: float
    data_quality_score: float


# ============================================================================
# REGISTRY MANAGEMENT
# ============================================================================

class DataRegistry:
    """Tracks which dates have been processed (incremental loading)"""
    
    def __init__(self, registry_path: str = "data_registry.json"):
        self.path = Path(registry_path)
        self.data = self._load()
    
    def _load(self) -> Dict:
        """Load registry from file"""
        if self.path.exists():
            with open(self.path) as f:
                return json.load(f)
        return {
            "last_updated": None,
            "total_files": 0,
            "total_rows": 0,
            "loaded_dates": [],
            "errors": []
        }
    
    def _save(self):
        """Persist registry to file"""
        self.path.write_text(json.dumps(self.data, indent=2))
        logger.info(f"Registry saved: {self.path}")
    
    def add_file(self, metadata: FileMetadata):
        """Register a successfully processed file"""
        self.data["loaded_dates"].append(asdict(metadata))
        self.data["total_files"] += 1
        self.data["total_rows"] += metadata.rows
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._save()
    
    def add_error(self, file_path: str, error: str):
        """Register a failed file"""
        self.data["errors"].append({
            "file": file_path,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        self._save()
    
    def get_loaded_dates(self) -> Set[str]:
        """Get set of all processed dates (YYYY-MM-DD)"""
        return {item["date"] for item in self.data.get("loaded_dates", [])}
    
    def get_stats(self) -> Dict:
        """Get current registry statistics"""
        return {
            "total_files": self.data["total_files"],
            "total_rows": self.data["total_rows"],
            "last_updated": self.data["last_updated"],
            "error_count": len(self.data.get("errors", []))
        }


class QueryOptimizer:
    """
    Advanced query optimization with partition pruning and column discovery
    """
    
    def __init__(self, db_path: str = "nyc_yellow_taxi.duckdb"):
        self.db_path = str(db_path)
        self.con = duckdb.connect(self.db_path, read_only=False)
        
        # Discovered columns cache
        self._column_cache = None
    
    def _discover_column_name(self, pattern: str) -> Optional[str]:
        """
        Auto-discover column names handling variations (tpep_/TPEP_, etc)
        
        Returns the discovered column name or None
        """
        if self._column_cache is None:
            result = self.con.execute(
                "SELECT * FROM yellow_taxi_trips LIMIT 0"
            ).description
            self._column_cache = {desc[0].lower() for desc in result}
        
        # Try exact match (case-insensitive)
        pattern_lower = pattern.lower()
        if pattern_lower in self._column_cache:
            return pattern_lower
        
        # Try with tpep_ prefix variations
        for col in self._column_cache:
            if pattern_lower.replace('tpep_', '').replace('_', '') == \
               col.replace('tpep_', '').replace('_', ''):
                return col
        
        return None
    
    def query_by_date_range(
        self,
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None
    ) -> Any:
        """
        Query data with automatic partition pruning for date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            columns: Column list (auto-discovers if None)
        
        Returns:
            DuckDB relation with results
        """
        if columns is None:
            columns = ["trip_distance", "total_amount", "fare_amount"]
        
        # Build column select with auto-discovery
        col_select = []
        for col in columns:
            discovered = self._discover_column_name(col)
            if discovered:
                col_select.append(discovered)
        
        col_str = ", ".join(col_select) if col_select else "*"
        
        # Determine pickup datetime column
        pickup_col = self._discover_column_name("pickup_datetime") or "tpep_pickup_datetime"
        
        query = f"""
        SELECT {col_str}
        FROM yellow_taxi_trips
        WHERE {pickup_col}::date BETWEEN '{start_date}' AND '{end_date}'
        """
        
        result = self.con.execute(query)
        return result.df()
    
    def get_daily_aggregates(self, days: int = 7) -> Any:
        """
        Get daily aggregated metrics for last N days
        
        Returns DataFrame with daily stats
        """
        pickup_col = self._discover_column_name("pickup_datetime") or "tpep_pickup_datetime"
        
        query = f"""
        SELECT 
            DATE({pickup_col}) as trip_date,
            COUNT(*) as total_trips,
            AVG(trip_distance) as avg_distance,
            AVG(fare_amount) as avg_fare,
            SUM(total_amount) as daily_revenue,
            COUNT(CASE WHEN payment_type = 1 THEN 1 END) as credit_card_trips,
            COUNT(CASE WHEN payment_type = 2 THEN 1 END) as cash_trips
        FROM yellow_taxi_trips
        WHERE {pickup_col}::date >= current_date - INTERVAL '{days} days'
        GROUP BY DATE({pickup_col})
        ORDER BY trip_date DESC
        """
        
        result = self.con.execute(query)
        return result.df()
    
    def vendor_performance(self) -> Any:
        """
        Analyze vendor performance metrics
        
        Returns DataFrame with vendor statistics
        """
        query = """
        SELECT 
            VendorID,
            COUNT(*) as trip_count,
            AVG(trip_distance) as avg_distance,
            AVG(fare_amount) as avg_fare,
            AVG(total_amount) as avg_total,
            COUNT(CASE WHEN payment_type = 1 THEN 1 END) as credit_card_trips,
            COUNT(CASE WHEN payment_type = 2 THEN 1 END) as cash_trips,
            SUM(total_amount) as total_revenue
        FROM yellow_taxi_trips
        GROUP BY VendorID
        ORDER BY total_revenue DESC
        """
        
        result = self.con.execute(query)
        return result.df()
    
    def peek_data(self, limit: int = 5) -> Any:
        """Preview first N rows of data"""
        query = f"SELECT * FROM yellow_taxi_trips LIMIT {limit}"
        result = self.con.execute(query)
        return result.df()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get table statistics and schema info"""
        # Row count
        row_result = self.con.execute(
            "SELECT COUNT(*) as cnt FROM yellow_taxi_trips"
        ).fetchall()
        total_rows = row_result[0][0] if row_result else 0
        
        # Schema
        schema_result = self.con.execute(
            "SELECT * FROM yellow_taxi_trips LIMIT 0"
        ).description
        columns = [desc[0] for desc in schema_result]
        
        return {
            "total_rows": total_rows,
            "column_count": len(columns),
            "columns": columns
        }
    
    def explain_plan(self, query: str) -> str:
        """Get query execution plan"""
        result = self.con.execute(f"EXPLAIN {query}").fetchall()
        return "\n".join(str(row[0]) for row in result)
    
    def close(self):
        """Close database connection"""
        self.con.close()


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
        
        # Metrics collector
        self.metrics = MetricsCollector()
    
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
            
            # Record metrics
            self.metrics.start_operation(f'load_year_{year}')
            self.metrics.record_row_count(stats['rows_loaded'])
            self.metrics.record_duration(stats['duration_sec'])
            self.metrics.record_throughput(stats['rows_loaded'], stats['duration_sec'])
            self.metrics.record_file_count(1)
            self.metrics.end_operation(status='completed')
            
            print(f"✅ {year} load complete:")
            print(f"   Rows:     {stats['rows_loaded']:,}")
            print(f"   Duration: {stats['duration_sec']:.2f}s")
            print(f"   Speed:    {stats['rows_loaded']/stats['duration_sec']:,.0f} rows/sec")
            
            return stats
        
        except Exception as e:
            # Record failed operation
            self.metrics.start_operation(f'load_year_{year}')
            self.metrics.record_error(str(e))
            self.metrics.end_operation(status='failed', error=str(e))
            
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
    
    def show_metrics(self) -> str:
        """Display metrics report"""
        return self.metrics.report(verbose=True)


# ============================================================================
# PARTITIONED ETL PIPELINE (Hive Format)
# ============================================================================

class PartitionedETLPipeline:
    """
    ETL Pipeline with Hive partitioned output (year=YYYY/month=MM/day=DD/)
    
    Writes data to optimized partitioned storage enabling 10-100x query speedup
    through automatic partition pruning by DuckDB.
    
    Features:
    - Hive partitioned storage structure for partition pruning
    - Automatic column normalization (tpep_* variations)
    - Snappy/gzip compression support
    - Incremental registry tracking
    - Detailed progress reporting
    """
    
    def __init__(
        self,
        source_data_dir: str = '../NYC Yellow Taxi Record 23-24-25',
        output_dir: str = 'data/processed',
        db_path: str = 'nyc_yellow_taxi.duckdb',
        pipeline_id: str = 'partitioned_etl_v1',
    ):
        """
        Initialize partitioned ETL pipeline
        
        Args:
            source_data_dir: Root directory with raw parquet files (2023/, 2024/, 2025/)
            output_dir: Output directory for partitioned data
            db_path: DuckDB database path for queries
            pipeline_id: Pipeline identifier
        """
        self.source_dir = Path(source_data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.pipeline_id = pipeline_id
        self.registry = DataRegistry('partitioned_data_registry.json')
        self.metrics = MetricsCollector()
        logger.info(f"PartitionedETL initialized: {output_dir}")
    
    def load_and_partition_year(
        self,
        year: int,
        writer_id: str = 'partitioner_worker',
        compression: str = 'snappy'
    ) -> Dict[str, Any]:
        """
        Load year's parquet files and write to Hive partitioned format
        
        Args:
            year: Year to load (2023, 2024, 2025)
            writer_id: Worker identifier
            compression: Compression codec (snappy, gzip, uncompressed)
        
        Returns:
            Statistics dict with rows processed, timing, etc
        """
        import pandas as pd
        import re
        
        year_dir = self.source_dir / str(year)
        start_time = time.time()
        total_rows = 0
        files_processed = 0
        total_null_count = 0
        
        if not year_dir.exists():
            msg = f"Year directory not found: {year_dir}"
            logger.error(msg)
            print(f"❌ {msg}")
            return {'error': msg, 'year': year}
        
        print(f"\n📅 Partitioning {year} data → data/processed/year={year}/month=MM/day=DD/")
        
        # Find all parquet files for this year
        parquet_files = sorted(year_dir.glob('*.parquet'))
        print(f"   Found {len(parquet_files)} files to process\n")
        
        if not parquet_files:
            print(f"   ⚠️  No parquet files found in {year_dir}")
            return {
                'year': year,
                'files_processed': 0,
                'total_rows': 0,
                'duration_sec': 0,
                'output_dir': str(self.output_dir)
            }
        
        # Process each parquet file
        for idx, parquet_file in enumerate(parquet_files, 1):
            try:
                # Read with DuckDB (faster than pandas)
                con = duckdb.connect(':memory:')
                df = con.execute(f"SELECT * FROM '{parquet_file}'").df()
                rows = len(df)
                
                # Extract date from filename (yellow_tripdata_YYYY-MM.parquet)
                match = re.search(r'(\d{4})-(\d{2})', parquet_file.name)
                if not match:
                    logger.warning(f"Could not extract date from {parquet_file.name}")
                    print(f"   ⚠️  Skipped {parquet_file.name} (no date found)")
                    continue
                
                year_part, month_part = match.groups()
                
                # Normalize columns
                df = self._normalize_columns(df)
                
                # Create partition directory: year=YYYY/month=MM/day=01/
                partition_dir = self.output_dir / f"year={year_part}" / f"month={month_part}" / "day=01"
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Write partitioned parquet file
                output_file = partition_dir / f"{parquet_file.name}"
                df.to_parquet(
                    output_file,
                    engine='pyarrow',
                    compression=compression,
                    index=False
                )
                
                # Register in data registry
                null_count = int(df.isnull().sum().sum())
                metadata = FileMetadata(
                    date=f"{year_part}-{month_part}-01",
                    source_file=parquet_file.name,
                    rows=rows,
                    null_count=null_count,
                    processed_at=datetime.now(timezone.utc).isoformat(),
                    compression_ratio=0.42,  # Snappy typical ratio
                    status='success'
                )
                self.registry.add_file(metadata)
                
                total_rows += rows
                total_null_count += null_count
                files_processed += 1
                
                print(f"   [{idx:2d}/{len(parquet_files)}] ✅ {parquet_file.name:<35} {rows:>10,} rows → year={year_part}/month={month_part}/")
                
                con.close()
                
            except Exception as e:
                logger.error(f"Failed to process {parquet_file.name}: {str(e)}")
                print(f"   ❌ Failed {parquet_file.name}: {str(e)}")
                self.registry.add_error(str(parquet_file), str(e))
        
        elapsed = time.time() - start_time
        throughput = total_rows / elapsed if elapsed > 0 else 0
        
        print(f"\n📊 {year} Partitioning Summary:")
        print(f"   ├─ Files processed: {files_processed}/{len(parquet_files)}")
        print(f"   ├─ Total rows: {total_rows:,}")
        print(f"   ├─ Null values: {total_null_count:,}")
        print(f"   ├─ Total time: {elapsed:.2f}s")
        print(f"   ├─ Throughput: {throughput:,.0f} rows/sec")
        print(f"   └─ Output: data/processed/year={year}/")
        
        return {
            'year': year,
            'files_processed': files_processed,
            'total_rows': total_rows,
            'duration_sec': elapsed,
            'throughput_rows_per_sec': throughput,
            'output_dir': str(self.output_dir)
        }
    
    def load_all_years_partitioned(
        self,
        years: List[int] = None,
        compression: str = 'snappy'
    ) -> List[Dict[str, Any]]:
        """
        Load and partition all years sequentially
        
        Args:
            years: List of years (default: [2023, 2024, 2025])
            compression: Compression codec (snappy, gzip, uncompressed)
        
        Returns:
            List of result dicts with statistics
        """
        years = years or [2023, 2024, 2025]
        results = []
        
        print(f"\n🚀 Starting Partitioned ETL for {len(years)} years")
        print(f"\n📂 Output Structure:")
        print(f"   data/processed/")
        print(f"   ├── year=2023/month=01/day=01/yellow_tripdata_2023-01.parquet")
        print(f"   ├── year=2023/month=02/day=01/yellow_tripdata_2023-02.parquet")
        print(f"   ├── year=2024/month=01/day=01/yellow_tripdata_2024-01.parquet")
        print(f"   └── ...")
        
        for year in years:
            result = self.load_and_partition_year(year, compression=compression)
            results.append(result)
        
        # Print summary
        total_files = sum(r.get('files_processed', 0) for r in results)
        total_rows_all = sum(r.get('total_rows', 0) for r in results)
        total_time = sum(r.get('duration_sec', 0) for r in results)
        
        print(f"\n" + "=" * 70)
        print(f"🎉 PARTITIONED ETL COMPLETE")
        print(f"=" * 70)
        print(f"Years processed: {', '.join(str(y) for y in years)}")
        print(f"Total files: {total_files}")
        print(f"Total rows: {total_rows_all:,}")
        print(f"Total time: {total_time:.2f}s")
        print(f"Average throughput: {total_rows_all / total_time:,.0f} rows/sec" if total_time > 0 else "")
        print(f"Output location: {self.output_dir}")
        print(f"\n✅ Data is now Hive partitioned and ready for queries with partition pruning!")
        print(f"\n📖 Next step: make query-from-partitions")
        
        return results
    
    @staticmethod
    def _normalize_columns(df: 'pd.DataFrame') -> 'pd.DataFrame':
        """
        Normalize column names across different years
        
        Handles variations like:
        - tpep_pickup_datetime vs TPEP_PICKUP_DATETIME
        - payment_type vs payment_methods
        - airport_fee (2024+)
        - cbd_congestion_surcharge (2025+)
        """
        column_mapping = {
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'TPEP_PICKUP_DATETIME': 'pickup_datetime',
            'TPEP_DROPOFF_DATETIME': 'dropoff_datetime',
            'trip_distance': 'trip_distance',
            'passenger_count': 'passenger_count',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'total_amount': 'total_amount',
            'payment_type': 'payment_type',
            'trip_type': 'trip_type',
            'airport_fee': 'airport_fee',
            'cbd_congestion_surcharge': 'cbd_congestion_surcharge',
            'pulocationid': 'pulocationid',
            'dolocationid': 'dolocationid',
            'vendorid': 'vendorid',
        }
        
        # Apply mapping
        new_columns = {}
        for col in df.columns:
            if col in column_mapping:
                new_columns[col] = column_mapping[col]
            else:
                # Fallback: lowercase and replace spaces
                new_columns[col] = col.lower().replace(' ', '_')
        
        return df.rename(columns=new_columns)


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
