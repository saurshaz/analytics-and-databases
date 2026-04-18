#!/usr/bin/env python3
"""
DuckDB Multi-Writer ETL module using Registry Locking

Integrates Registry Locking with DuckDB for safe concurrent ETL operations.
Works with the NYC Yellow Taxi ETL pipeline.

Usage:
    etl = DuckDBMultiWriterETL(
        db_path='nyc_yellow_taxi.duckdb',
        pipeline_id='taxi_etl_v1'
    )
    
    etl.load_parquet_safe(
        parquet_glob='*.parquet',
        table_name='yellow_taxi_trips',
        run_id='run_001',
        writer_id='worker_1'
    )
"""

import duckdb
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import time
from .registry_lock_manager import RegistryLockManager


class DuckDBMultiWriterETL:
    """
    Safe multi-writer DuckDB ETL coordinator using Registry Locking
    
    Features:
    - Atomic table creation/modification
    - Lock-based write coordination
    - Automatic registry updates
    - Audit trail for all operations
    - Works with existing DuckDB queries
    """
    
    def __init__(
        self,
        db_path: str,
        pipeline_id: str = 'default_etl',
        registry_dir: str = 'data/registries',
        timeout: int = 300  # 5 minutes default
    ):
        """
        Initialize multi-writer ETL coordinator
        
        Args:
            db_path: Path to DuckDB database file
            pipeline_id: Pipeline identifier for registry
            registry_dir: Directory for registry files
            timeout: Lock timeout in seconds
        """
        self.db_path = Path(db_path)
        self.pipeline_id = pipeline_id
        self.registry = RegistryLockManager(
            db_path=str(self.db_path),
            registry_dir=registry_dir,
            default_timeout=timeout
        )
        self.timeout = timeout
    
    def load_parquet_safe(
        self,
        parquet_glob: str,
        table_name: str,
        run_id: str,
        writer_id: str,
        if_exists: str = 'append',
        union_by_name: bool = True
    ) -> Dict[str, Any]:
        """
        Safely load parquet files into DuckDB using registry locking.
        
        Args:
            parquet_glob: Glob pattern for parquet files (e.g., '*.parquet')
            table_name: Target table name
            run_id: ETL run identifier
            writer_id: Writer identifier (e.g., 'worker_1')
            if_exists: 'append', 'replace', or 'create'
            union_by_name: Union by column name (for multi-part files)
        
        Returns:
            Dict with operation stats (rows_loaded, bytes_written, duration, etc)
        
        Example:
            stats = etl.load_parquet_safe(
                parquet_glob='NYC Yellow Taxi Record 23-24-25/**/yellow_tripdata_*.parquet',
                table_name='yellow_taxi_trips',
                run_id='daily_load_20260417',
                writer_id='worker_1'
            )
        """
        start_time = datetime.now(timezone.utc)
        start_time_unix = time.time()
        
        # Record run start in registry
        self.registry.record_etl_run(
            run_id=run_id,
            pipeline_id=self.pipeline_id,
            writer_id=writer_id,
            start_time=start_time,
            status='running'
        )
        
        try:
            # Acquire lock and execute transaction
            with self.registry.acquire_lock(
                run_id=run_id,
                writer_id=writer_id,
                timeout=self.timeout
            ):
                # Connect and execute
                con = duckdb.connect(str(self.db_path))

                
                try:
                    # Expand glob pattern
                    base_dir = Path(self.db_path).parent
                    parquet_path = str(base_dir / parquet_glob)
                    
                    print(f"📦 Loading parquet: {parquet_path}")
                    
                    # Execute load using union_by_name for schema flexibility
                    if if_exists == 'replace':
                        con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                    if if_exists == 'create':
                        # Create table with all files (union_by_name handles schema differences)
                        query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} AS
                        SELECT * FROM read_parquet(
                            '{parquet_path}',
                            union_by_name=true,
                            hive_partitioning=false
                        )
                        """
                        con.execute(query)
                    else:  # append mode
                        # Get existing table columns to filter source data
                        existing_cols_result = con.execute(
                            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
                        ).fetchall()
                        
                        if existing_cols_result:
                            # Build column list for INSERT - only select columns that exist in target table
                            existing_cols = [col[0] for col in existing_cols_result]
                            col_list = ', '.join([f'"{col}"' for col in existing_cols])
                            
                            # INSERT using column matching - DuckDB will handle missing columns from parquet
                            query = f"""
                            INSERT INTO {table_name}
                            SELECT {col_list} FROM read_parquet(
                                '{parquet_path}',
                                union_by_name=true,
                                hive_partitioning=false
                            )
                            """
                            con.execute(query)
                        else:
                            # Fallback - create if doesn't exist
                            query = f"""
                            CREATE TABLE IF NOT EXISTS {table_name} AS
                            SELECT * FROM read_parquet(
                                '{parquet_path}',
                                union_by_name=true,
                                hive_partitioning=false
                            )
                            """
                            con.execute(query)
                    
                    # Get row count
                    result = con.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchall()
                    rows_loaded = result[0][0] if result else 0
                    
                    # Estimate bytes written from row count (rough estimate: ~100 bytes per row)
                    bytes_written = rows_loaded * 100
                    
                    con.close()
                    
                    # Update registry with success
                    duration = time.time() - start_time_unix
                    self.registry.update_etl_run(
                        run_id=run_id,
                        status='completed',
                        rows_written=rows_loaded,
                        bytes_written=bytes_written,
                        end_time=datetime.now(timezone.utc)
                    )
                    
                    stats = {
                        'status': 'success',
                        'rows_loaded': rows_loaded,
                        'bytes_written': bytes_written,
                        'duration_sec': duration,
                        'table_name': table_name,
                        'run_id': run_id,
                        'writer_id': writer_id
                    }
                    
                    print(f"✅ Successfully loaded {rows_loaded:,} rows in {duration:.2f}s")
                    return stats
                
                finally:
                    try:
                        con.close()
                    except:
                        pass
        
        except Exception as e:
            # Update registry with failure
            self.registry.update_etl_run(
                run_id=run_id,
                status='failed',
                end_time=datetime.now(timezone.utc)
            )
            
            print(f"❌ ETL failed: {e}")
            raise
    
    def execute_sql_safe(
        self,
        query: str,
        run_id: str,
        writer_id: str,
        query_name: str = 'unnamed'
    ) -> Dict[str, Any]:
        """
        Safely execute SQL in DuckDB using registry locking.
        
        Args:
            query: SQL query to execute
            run_id: ETL run identifier
            writer_id: Writer identifier
            query_name: Name of query (for logging)
        
        Returns:
            Query result and execution stats
        """
        start_time = datetime.now(timezone.utc)
        start_time_unix = time.time()
        
        self.registry.record_etl_run(
            run_id=run_id,
            pipeline_id=self.pipeline_id,
            writer_id=writer_id,
            start_time=start_time,
            status='running',
            metadata={'query_name': query_name}
        )
        
        try:
            with self.registry.acquire_lock(
                run_id=run_id,
                writer_id=writer_id,
                timeout=self.timeout
            ):
                con = duckdb.connect(str(self.db_path))
                
                try:
                    print(f"🔧 Executing: {query_name}")
                    result = con.execute(query).fetchall()
                    
                    duration = time.time() - start_time_unix
                    
                    self.registry.update_etl_run(
                        run_id=run_id,
                        status='completed',
                        end_time=datetime.now(timezone.utc)
                    )
                    
                    print(f"✅ Query completed in {duration:.2f}s")
                    
                    return {
                        'status': 'success',
                        'result': result,
                        'duration_sec': duration,
                        'query_name': query_name
                    }
                
                finally:
                    try:
                        con.close()
                    except:
                        pass
        
        except Exception as e:
            self.registry.update_etl_run(
                run_id=run_id,
                status='failed',
                end_time=datetime.now(timezone.utc)
            )
            
            print(f"❌ Query failed: {e}")
            raise
    
    def parallel_load_partitions_safe(
        self,
        partition_paths: List[str],
        table_name: str,
        run_id: str,
        writer_id_prefix: str = 'worker',
        sequential: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Load multiple partition files with registry locking.
        
        Args:
            partition_paths: List of parquet file paths
            table_name: Target table name
            run_id: Master run ID
            writer_id_prefix: Prefix for writer IDs (e.g., 'worker_1', 'worker_2')
            sequential: If False, attempt parallel loading; if True, load sequentially
        
        Returns:
            List of stats for each partition
        
        Example:
            results = etl.parallel_load_partitions_safe(
                partition_paths=[
                    'NYC Yellow Taxi Record 23-24-25/2023/*.parquet',
                    'NYC Yellow Taxi Record 23-24-25/2024/*.parquet',
                    'NYC Yellow Taxi Record 23-24-25/2025/*.parquet'
                ],
                table_name='yellow_taxi_trips',
                run_id='bulk_load_20260417'
            )
        """
        results = []
        
        if sequential:
            # Load partitions one by one
            for idx, partition_path in enumerate(partition_paths):
                writer_id = f"{writer_id_prefix}_{idx}"
                result = self.load_parquet_safe(
                    parquet_glob=partition_path,
                    table_name=table_name,
                    run_id=run_id,
                    writer_id=writer_id,
                    if_exists='create' if idx == 0 else 'append'
                )
                results.append(result)
        else:
            # Note: For parallel, you'd need threading/multiprocessing
            # For now, we'll load sequentially but with separate lock entries
            for idx, partition_path in enumerate(partition_paths):
                writer_id = f"{writer_id_prefix}_{idx}"
                result = self.load_parquet_safe(
                    parquet_glob=partition_path,
                    table_name=table_name,
                    run_id=f"{run_id}_partition_{idx}",
                    writer_id=writer_id,
                    if_exists='create' if idx == 0 else 'append'
                )
                results.append(result)
        
        return results
    
    def get_registry_status(self) -> Dict[str, Any]:
        """Get current status of all locks and runs"""
        return {
            'active_locks': self.registry.get_active_locks(),
            'all_runs': self.registry.get_all_runs(),
            'db_path': str(self.db_path),
            'pipeline_id': self.pipeline_id
        }
    
    def cleanup_old_locks(self, older_than_hours: int = 24) -> int:
        """Clean up locks older than specified time"""
        seconds = older_than_hours * 3600
        cleaned = self.registry.cleanup_expired_locks(older_than_seconds=seconds)
        print(f"🧹 Cleaned up {cleaned} old lock entries")
        return cleaned


if __name__ == '__main__':
    # Example: NYC Yellow Taxi ETL
    print("🚀 NYC Yellow Taxi ETL with Registry Locking\n")
    
    etl = DuckDBMultiWriterETL(
        db_path='nyc_yellow_taxi.duckdb',
        pipeline_id='taxi_etl_v1',
        timeout=300
    )
    
    # Load all years
    try:
        stats = etl.load_parquet_safe(
            parquet_glob='NYC Yellow Taxi Record 23-24-25/**/yellow_tripdata_*.parquet',
            table_name='yellow_taxi_trips',
            run_id='initial_load_20260417',
            writer_id='worker_1'
        )
        
        print("\n📊 ETL Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Show registry status
        print("\n📋 Registry Status:")
        status = etl.get_registry_status()
        print(f"  Active Locks: {len(status['active_locks'])}")
        print(f"  Completed Runs: {len(status['all_runs'])}")
    
    except Exception as e:
        print(f"Error: {e}")
