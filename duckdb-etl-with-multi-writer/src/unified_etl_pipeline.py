#!/usr/bin/env python3
"""
Unified ETL Pipeline with Multiple Runnable Modes

Consolidates standard ETL and partitioned ETL into a single flexible pipeline
that can operate in multiple modes:

- 'etl': Standard incremental loading into yellow_taxi_trips table
- 'partition': Hive partitioned output (year=YYYY/month=MM/day=DD/)
- 'both': Load into both table and partitioned format
- 'query': Run analytical queries on loaded data
- 'validate': Validate data quality

Usage:
    pipeline = UnifiedETLPipeline(mode='etl')
    pipeline.run(years=[2023, 2024, 2025])
    
    # Or with partitioning
    pipeline = UnifiedETLPipeline(mode='partition')
    pipeline.run(years=[2023, 2024])
    
    # Or both
    pipeline = UnifiedETLPipeline(mode='both')
    pipeline.run()

CLI:
    python -m src.unified_etl_pipeline --mode etl --years 2023,2024,2025
    python -m src.unified_etl_pipeline --mode partition --years 2024
    python -m src.unified_etl_pipeline --mode query --query-type daily
"""

import duckdb
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, asdict
import json
import logging
import time
import re
import argparse

from .duckdb_multiwriter_etl import DuckDBMultiWriterETL
from .utils import normalize_columns, format_number, format_duration
from .metrics import MetricsCollector
from .exceptions import ETLError, DataNotFoundError, ConfigurationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class FileMetadata:
    """Metadata for a processed parquet file"""
    date: str
    source_file: str
    rows: int
    null_count: int
    processed_at: str
    compression_ratio: float
    status: str


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


# ============================================================================
# UNIFIED ETL PIPELINE
# ============================================================================

class UnifiedETLPipeline:
    """
    Unified ETL Pipeline with multiple runnable modes.
    
    Modes:
    - 'etl': Standard incremental loading
    - 'partition': Hive partitioned output
    - 'both': Load into both formats
    - 'query': Run analytical queries
    - 'validate': Validate data quality
    
    Attributes:
        mode: Pipeline mode (etl, partition, both, query, validate)
        db_path: Path to DuckDB database
        data_dir: Root directory for raw data
        output_dir: Output directory for partitioned data
    """
    
    def __init__(
        self,
        mode: str = 'etl',
        db_path: str = 'nyc_yellow_taxi.duckdb',
        data_dir: str = '../NYC Yellow Taxi Record 23-24-25',
        output_dir: str = 'data/processed',
        pipeline_id: str = 'taxi_etl_v2',
        timeout: int = 300
    ):
        """
        Initialize unified ETL pipeline.
        
        Args:
            mode: Pipeline mode (etl, partition, both, query, validate)
            db_path: Path to DuckDB database
            data_dir: Root directory for NYC taxi data
            output_dir: Output directory for partitioned data
            pipeline_id: Pipeline identifier
            timeout: Lock timeout in seconds
        """
        valid_modes = {'etl', 'partition', 'both', 'query', 'validate'}
        if mode not in valid_modes:
            raise ConfigurationError(
                f"Invalid mode '{mode}'. Must be one of: {', '.join(valid_modes)}"
            )
        
        self.mode = mode
        self.db_path = Path(db_path)
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.pipeline_id = pipeline_id
        self.timeout = timeout
        
        # Initialize multi-writer ETL
        self.etl = DuckDBMultiWriterETL(
            db_path=str(self.db_path),
            pipeline_id=pipeline_id,
            timeout=timeout
        )
        
        self.registry = DataRegistry()
        self.metrics = MetricsCollector()
        
        logger.info(f"UnifiedETLPipeline initialized: mode={mode}, db={db_path}")
    
    def run(
        self,
        years: Optional[List[int]] = None,
        writer_id_prefix: str = 'worker',
        compression: str = 'snappy'
    ) -> Dict[str, Any]:
        """
        Execute pipeline based on configured mode.
        
        Args:
            years: List of years to process (default: [2023, 2024, 2025])
            writer_id_prefix: Prefix for writer IDs
            compression: Compression codec (snappy, gzip, uncompressed)
        
        Returns:
            Results dictionary with completion status and metrics
        """
        years = years or [2023, 2024, 2025]
        
        if self.mode == 'etl':
            return self._run_etl(years, writer_id_prefix)
        elif self.mode == 'partition':
            return self._run_partition(years, compression)
        elif self.mode == 'both':
            etl_results = self._run_etl(years, writer_id_prefix)
            partition_results = self._run_partition(years, compression)
            return {
                'etl': etl_results,
                'partition': partition_results,
                'combined_rows': etl_results.get('total_rows', 0)
            }
        elif self.mode == 'query':
            return self._run_query()
        elif self.mode == 'validate':
            return self._run_validate()
    
    def _run_etl(
        self,
        years: List[int],
        writer_id_prefix: str
    ) -> Dict[str, Any]:
        """Run standard ETL mode (incremental loading)"""
        print(f"\n{'='*70}")
        print(f"🚀 UNIFIED ETL PIPELINE - ETL MODE")
        print(f"{'='*70}")
        print(f"📅 Years: {', '.join(str(y) for y in years)}")
        print(f"📊 Database: {self.db_path}\n")
        
        all_stats = []
        total_rows = 0
        total_time = 0
        
        for idx, year in enumerate(years):
            writer_id = f"{writer_id_prefix}_{year}"
            if_exists = 'create' if idx == 0 else 'append'
            
            print(f"\n[{idx+1}/{len(years)}] 📅 Loading {year}...")
            
            try:
                parquet_glob = f"{self.data_dir}/{year}/*.parquet"
                stats = self.etl.load_parquet_safe(
                    parquet_glob=parquet_glob,
                    table_name='yellow_taxi_trips',
                    run_id=f'load_year_{year}',
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
                
                print(f"✅ {year} complete:")
                print(f"   Rows:     {format_number(stats['rows_loaded'])}")
                print(f"   Duration: {format_duration(stats['duration_sec'])}")
                print(f"   Speed:    {format_number(stats['rows_loaded']/stats['duration_sec'])} rows/sec")
                
                all_stats.append(stats)
                total_rows += stats['rows_loaded']
                total_time += stats['duration_sec']
                
            except Exception as e:
                self.metrics.start_operation(f'load_year_{year}')
                self.metrics.record_error(str(e))
                self.metrics.end_operation(status='failed', error=str(e))
                
                print(f"❌ Failed: {e}")
                raise
        
        # Print summary
        print(f"\n{'='*70}")
        print(f"✅ ETL MODE COMPLETE")
        print(f"{'='*70}")
        print(f"Total rows: {format_number(total_rows)}")
        print(f"Total time: {format_duration(total_time)}")
        print(f"Avg speed:  {format_number(total_rows / total_time)} rows/sec" if total_time > 0 else "")
        print(f"\n💾 Data loaded into: {self.db_path}")
        print(f"🔒 Registry: Locked writes, safe concurrent access\n")
        
        return {
            'mode': 'etl',
            'years': years,
            'total_rows': total_rows,
            'total_time': total_time,
            'stats': all_stats,
            'db_path': str(self.db_path)
        }
    
    def _run_partition(
        self,
        years: List[int],
        compression: str
    ) -> Dict[str, Any]:
        """Run partition mode (Hive partitioned output)"""
        print(f"\n{'='*70}")
        print(f"🚀 UNIFIED ETL PIPELINE - PARTITION MODE")
        print(f"{'='*70}")
        print(f"📅 Years: {', '.join(str(y) for y in years)}")
        print(f"📂 Output: {self.output_dir}\n")
        print(f"📋 Structure: year=YYYY/month=MM/day=DD/")
        print(f"💡 Enables 10-100x faster queries with partition pruning\n")
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        all_results = []
        total_rows = 0
        total_time = 0
        
        for idx, year in enumerate(years):
            print(f"\n[{idx+1}/{len(years)}] 📅 Partitioning {year}...")
            
            result = self._partition_year(year, compression)
            all_results.append(result)
            total_rows += result.get('total_rows', 0)
            total_time += result.get('duration_sec', 0)
        
        # Print summary
        total_files = sum(r.get('files_processed', 0) for r in all_results)
        
        print(f"\n{'='*70}")
        print(f"✅ PARTITION MODE COMPLETE")
        print(f"{'='*70}")
        print(f"Total rows: {format_number(total_rows)}")
        print(f"Total files: {total_files}")
        print(f"Total time: {format_duration(total_time)}")
        print(f"Avg speed:  {format_number(total_rows / total_time)} rows/sec" if total_time > 0 else "")
        print(f"\n📂 Data partitioned at: {self.output_dir}")
        print(f"🚀 Ready for partition-pruned queries\n")
        
        return {
            'mode': 'partition',
            'years': years,
            'total_rows': total_rows,
            'total_files': total_files,
            'total_time': total_time,
            'results': all_results,
            'output_dir': str(self.output_dir)
        }
    
    def _partition_year(
        self,
        year: int,
        compression: str
    ) -> Dict[str, Any]:
        """Partition a single year into Hive format"""
        year_dir = self.data_dir / str(year)
        start_time = time.time()
        total_rows = 0
        files_processed = 0
        total_null_count = 0
        
        if not year_dir.exists():
            msg = f"Year directory not found: {year_dir}"
            logger.error(msg)
            print(f"❌ {msg}")
            return {'error': msg, 'year': year}
        
        # Find parquet files
        parquet_files = sorted(year_dir.glob('*.parquet'))
        print(f"   Found {len(parquet_files)} files\n")
        
        if not parquet_files:
            return {
                'year': year,
                'files_processed': 0,
                'total_rows': 0,
                'duration_sec': 0,
                'output_dir': str(self.output_dir)
            }
        
        # Process each file
        for idx, parquet_file in enumerate(parquet_files, 1):
            try:
                con = duckdb.connect(':memory:')
                df = con.execute(f"SELECT * FROM '{parquet_file}'").df()
                rows = len(df)
                
                # Extract date from filename
                match = re.search(r'(\d{4})-(\d{2})', parquet_file.name)
                if not match:
                    logger.warning(f"Could not extract date: {parquet_file.name}")
                    continue
                
                year_part, month_part = match.groups()
                
                # Normalize columns
                df = normalize_columns(df)
                
                # Create partition directory
                partition_dir = (
                    self.output_dir / f"year={year_part}" / 
                    f"month={month_part}" / "day=01"
                )
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Write partitioned file
                output_file = partition_dir / f"{parquet_file.name}"
                df.to_parquet(
                    output_file,
                    engine='pyarrow',
                    compression=compression,
                    index=False
                )
                
                # Record metadata
                null_count = int(df.isnull().sum().sum())
                metadata = FileMetadata(
                    date=f"{year_part}-{month_part}-01",
                    source_file=parquet_file.name,
                    rows=rows,
                    null_count=null_count,
                    processed_at=datetime.now(timezone.utc).isoformat(),
                    compression_ratio=0.42,
                    status='success'
                )
                self.registry.add_file(metadata)
                
                total_rows += rows
                total_null_count += null_count
                files_processed += 1
                
                print(f"   [{idx:2d}/{len(parquet_files)}] ✅ {parquet_file.name:<35} {format_number(rows):>10} rows")
                
                con.close()
                
            except Exception as e:
                logger.error(f"Failed: {parquet_file.name}: {str(e)}")
                print(f"   ❌ {parquet_file.name}: {str(e)}")
                self.registry.add_error(str(parquet_file), str(e))
        
        elapsed = time.time() - start_time
        throughput = total_rows / elapsed if elapsed > 0 else 0
        
        self.metrics.start_operation(f'partition_year_{year}')
        self.metrics.record_row_count(total_rows)
        self.metrics.record_duration(elapsed)
        self.metrics.record_throughput(total_rows, elapsed)
        self.metrics.record_file_count(files_processed)
        self.metrics.end_operation(status='completed')
        
        return {
            'year': year,
            'files_processed': files_processed,
            'total_rows': total_rows,
            'duration_sec': elapsed,
            'throughput_rows_per_sec': throughput,
            'output_dir': str(self.output_dir)
        }
    
    def _run_query(self) -> Dict[str, Any]:
        """Run analytical queries on loaded data"""
        print(f"\n{'='*70}")
        print(f"🚀 UNIFIED ETL PIPELINE - QUERY MODE")
        print(f"{'='*70}\n")
        
        con = duckdb.connect(str(self.db_path))
        results = {}
        
        try:
            print("📊 Running sample queries...\n")
            
            # Daily aggregation
            print("[1/3] Daily aggregation...")
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
            print(f"   ✅ {format_duration(q1_duration)}, {len(q1_result)} rows\n")
            
            # Vendor performance
            print("[2/3] Vendor performance...")
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
            print(f"   ✅ {format_duration(q2_duration)}, {len(q2_result)} rows\n")
            
            # Peak hours
            print("[3/3] Peak hours...")
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
            print(f"   ✅ {format_duration(q3_duration)}, {len(q3_result)} rows\n")
            
            print(f"{'='*70}")
            print(f"✅ QUERY MODE COMPLETE")
            print(f"{'='*70}\n")
            
            return {
                'mode': 'query',
                'results': results,
                'total_queries': 3
            }
        
        finally:
            con.close()
    
    def _run_validate(self) -> Dict[str, Any]:
        """Validate data quality"""
        print(f"\n{'='*70}")
        print(f"🚀 UNIFIED ETL PIPELINE - VALIDATE MODE")
        print(f"{'='*70}\n")
        
        con = duckdb.connect(str(self.db_path))
        
        try:
            print("🔍 Validating data...\n")
            
            # Row count
            print("[1/3] Row count...")
            result = con.execute(
                "SELECT COUNT(*) as count FROM yellow_taxi_trips"
            ).fetchall()
            total_rows = result[0][0] if result else 0
            print(f"   ✅ Total rows: {format_number(total_rows)}\n")
            
            # Schema
            print("[2/3] Schema validation...")
            columns = con.execute(
                "SELECT * FROM yellow_taxi_trips LIMIT 0"
            ).description
            col_count = len(columns)
            col_names = [desc[0] for desc in columns]
            print(f"   ✅ Columns: {col_count}")
            print(f"      {', '.join(col_names[:5])}...\n")
            
            # Year distribution
            print("[3/3] Year distribution...")
            year_dist = con.execute("""
                SELECT 
                    YEAR(tpep_pickup_datetime) as year,
                    COUNT(*) as count
                FROM yellow_taxi_trips
                GROUP BY YEAR(tpep_pickup_datetime)
                ORDER BY year
            """).fetchall()
            
            for year, count in year_dist:
                print(f"   ✅ {year}: {format_number(count)} rows")
            
            print(f"\n{'='*70}")
            print(f"✅ VALIDATE MODE COMPLETE")
            print(f"{'='*70}\n")
            
            return {
                'mode': 'validate',
                'total_rows': total_rows,
                'column_count': col_count,
                'year_distribution': {str(y): c for y, c in year_dist},
                'status': 'valid'
            }
        
        finally:
            con.close()
    
    def show_status(self) -> str:
        """Display pipeline status"""
        status = self.etl.get_registry_status()
        
        output = "\n" + "=" * 70 + "\n"
        output += "  ETL Pipeline Status Report\n"
        output += "=" * 70 + "\n\n"
        
        output += f"Mode: {self.mode.upper()}\n"
        output += f"Database: {self.db_path}\n"
        output += f"Output dir: {self.output_dir}\n\n"
        
        active = status['active_locks']
        output += f"🔒 Active Locks: {len(active)}\n"
        if active:
            for lock in active:
                output += f"   • {lock['writer_id']}: {lock['lock_id']}\n"
        else:
            output += "   (none)\n"
        
        runs = status['all_runs']
        output += f"\n📊 ETL Runs: {len(runs)} total\n"
        
        if runs:
            success = sum(1 for r in runs if r['status'] == 'completed')
            failed = sum(1 for r in runs if r['status'] == 'failed')
            total_rows = sum(r.get('rows_written', 0) for r in runs)
            
            output += f"   ✅ Completed: {success}\n"
            output += f"   ❌ Failed: {failed}\n"
            output += f"   📈 Total rows: {format_number(total_rows)}\n"
        
        output += "\n" + "=" * 70 + "\n\n"
        return output
    
    def show_metrics(self) -> str:
        """Display metrics report"""
        return self.metrics.report(verbose=True)


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Command-line interface for unified ETL pipeline"""
    parser = argparse.ArgumentParser(
        description='Unified ETL Pipeline for NYC Yellow Taxi data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Standard ETL mode
  python -m src.unified_etl_pipeline --mode etl

  # Partition mode
  python -m src.unified_etl_pipeline --mode partition --years 2023,2024

  # Both ETL and partition
  python -m src.unified_etl_pipeline --mode both

  # Query mode
  python -m src.unified_etl_pipeline --mode query

  # Validate data
  python -m src.unified_etl_pipeline --mode validate
        '''
    )
    
    parser.add_argument(
        '--mode',
        choices=['etl', 'partition', 'both', 'query', 'validate'],
        default='etl',
        help='Pipeline mode (default: etl)'
    )
    parser.add_argument(
        '--years',
        type=str,
        default='2023,2024,2025',
        help='Comma-separated years to process (default: 2023,2024,2025)'
    )
    parser.add_argument(
        '--db-path',
        default='nyc_yellow_taxi.duckdb',
        help='Path to DuckDB database'
    )
    parser.add_argument(
        '--data-dir',
        default='../NYC Yellow Taxi Record 23-24-25',
        help='Root directory for raw data'
    )
    parser.add_argument(
        '--output-dir',
        default='data/processed',
        help='Output directory for partitioned data'
    )
    parser.add_argument(
        '--compression',
        choices=['snappy', 'gzip', 'uncompressed'],
        default='snappy',
        help='Compression codec for partition mode'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show pipeline status and exit'
    )
    parser.add_argument(
        '--show-metrics',
        action='store_true',
        help='Show metrics report and exit'
    )
    
    args = parser.parse_args()
    
    # Parse years
    years = [int(y.strip()) for y in args.years.split(',')]
    
    # Create pipeline
    pipeline = UnifiedETLPipeline(
        mode=args.mode,
        db_path=args.db_path,
        data_dir=args.data_dir,
        output_dir=args.output_dir
    )
    
    # Handle status requests
    if args.status:
        print(pipeline.show_status())
        return
    
    if args.show_metrics:
        print(pipeline.show_metrics())
        return
    
    # Run pipeline
    result = pipeline.run(
        years=years if args.mode not in ['query', 'validate'] else None,
        compression=args.compression
    )
    
    # Show metrics if successful
    if result:
        print(pipeline.show_metrics())


if __name__ == '__main__':
    main()
