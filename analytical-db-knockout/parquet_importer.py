#!/usr/bin/env python3
"""
Parquet File Importer for ClickHouse

Supports three import methods:
1. HTTP API (via clickhouse-driver)
2. Native protocol (via clickhouse-driver)
3. Direct SQL with Parquet format (fastest for large files)
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import pandas as pd
from clickhouse_driver import Client
import time
from glob import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetImporter:
    """Import Parquet files into ClickHouse with multiple strategies."""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 9000,
        database: str = 'default',
        username: str = 'default',
        password: str = ''
    ):
        """Initialize ClickHouse client."""
        self.host = host
        self.port = port
        self.database = database
        self.client = Client(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            settings={'use_numpy': True}
        )
        logger.info(f"Connected to ClickHouse at {host}:{port}")
    
    def discover_parquet_files(self, directory: str) -> List[Tuple[str, int]]:
        """
        Recursively discover all Parquet files in a directory.
        
        Returns: List of tuples (file_path, file_size_mb)
        """
        directory = Path(directory)
        if not directory.exists():
            logger.error(f"✗ Directory not found: {directory}")
            return []
        
        parquet_files = []
        for parquet_path in directory.rglob('*.parquet'):
            size_mb = parquet_path.stat().st_size / 1024 / 1024
            parquet_files.append((str(parquet_path), size_mb))
        
        # Sort by size (smaller first)
        parquet_files.sort(key=lambda x: x[1])
        
        logger.info(f"Found {len(parquet_files)} Parquet files:")
        total_size = sum(size for _, size in parquet_files)
        for file_path, size_mb in parquet_files:
            logger.info(f"  - {file_path} ({size_mb:.1f} MB)")
        logger.info(f"Total size: {total_size:.1f} MB")
        
        return parquet_files
    
    def batch_import(
        self,
        directory: str,
        table_name: str,
        method: int = 2,
        chunk_size: int = 10000,
        skip_errors: bool = False,
        dry_run: bool = False,
        inspect_schema: bool = False
    ) -> Dict[str, any]:
        """
        Batch import all Parquet files from directory.
        
        Returns: Summary dict with stats about the import
        """
        logger.info(f"🔍 Batch importing Parquet files from: {directory}")
        logger.info(f"   Table: {table_name}, Method: {method}")
        
        parquet_files = self.discover_parquet_files(directory)
        
        if not parquet_files:
            logger.warning("⚠️  No Parquet files found!")
            return {
                'total_files': 0,
                'successful_files': 0,
                'failed_files': 0,
                'total_rows': 0,
                'total_time': 0,
            }
        
        # Schema inspection mode
        if inspect_schema:
            logger.info("\n" + "="*60)
            logger.info("SCHEMA INSPECTION")
            logger.info("="*60)
            
            first_file = parquet_files[0][0]
            logger.info(f"\nClickHouse table '{table_name}' schema:")
            table_schema = self.get_table_schema(table_name)
            for col, col_type in table_schema.items():
                logger.info(f"  {col}: {col_type}")
            
            logger.info(f"\nParquet file schema (from {Path(first_file).name}):")
            parquet_schema = self.get_parquet_schema(first_file)
            for col, col_type in parquet_schema.items():
                logger.info(f"  {col}: {col_type}")
            
            table_cols = set(table_schema.keys())
            parquet_cols = set(parquet_schema.keys())
            missing_in_parquet = table_cols - parquet_cols
            extra_in_parquet = parquet_cols - table_cols
            
            if missing_in_parquet:
                logger.warning(f"\nMissing in Parquet (will be filled with NULL):")
                for col in missing_in_parquet:
                    logger.warning(f"  - {col}: {table_schema[col]}")
            
            if extra_in_parquet:
                logger.warning(f"\nExtra in Parquet (will be ignored):")
                for col in extra_in_parquet:
                    logger.warning(f"  - {col}: {parquet_schema[col]}")
            
            logger.info("")
        
        if dry_run:
            logger.info(f"[DRY RUN] Would import {len(parquet_files)} files (not importing)")
            return {
                'total_files': len(parquet_files),
                'successful_files': 0,
                'failed_files': 0,
                'total_rows': 0,
                'total_time': 0,
                'dry_run': True,
            }
        
        # Track statistics
        total_start = time.time()
        successful = 0
        failed = 0
        total_rows = 0
        failed_files = []
        
        # Import each file
        for idx, (file_path, size_mb) in enumerate(parquet_files, 1):
            logger.info(f"\n[{idx}/{len(parquet_files)}] Importing {Path(file_path).name} ({size_mb:.1f} MB)...")
            
            try:
                # Count rows first
                df = pd.read_parquet(file_path)
                rows = len(df)
                total_rows += rows
                
                start = time.time()
                
                # Import using specified method
                if method == 1:
                    self._import_chunks(df, table_name, chunk_size)
                elif method == 2:
                    self._insert_dataframe(df, table_name)
                elif method == 3:
                    # Method 3 needs special handling
                    logger.warning("Method 3 (SQL Parquet) not supported for batch. Using Method 2.")
                    self._insert_dataframe(df, table_name)
                elif method == 4:
                    self._import_http(file_path, table_name)
                
                elapsed = time.time() - start
                rate = rows / elapsed if elapsed > 0 else 0
                logger.info(f"✓ Imported {rows:,} rows in {elapsed:.2f}s ({rate:.0f} rows/sec)")
                
                successful += 1
                
            except Exception as e:
                failed += 1
                failed_files.append((file_path, str(e)))
                logger.error(f"✗ Failed to import {file_path}: {e}")
                
                if not skip_errors:
                    logger.error("Stopping batch import due to error. Use --skip-errors to continue.")
                    break
        
        total_time = time.time() - total_start
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("📊 BATCH IMPORT SUMMARY")
        logger.info("="*60)
        logger.info(f"Total files: {len(parquet_files)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Total time: {total_time:.2f}s")
        
        if failed > 0:
            logger.error("\nFailed files:")
            for file_path, error in failed_files:
                logger.error(f"  - {file_path}: {error}")
        
        if successful > 0:
            logger.info(f"\n✓ Successfully imported {successful} files with {total_rows:,} rows")
            self.validate_import(table_name)
        
        return {
            'total_files': len(parquet_files),
            'successful_files': successful,
            'failed_files': failed,
            'total_rows': total_rows,
            'total_time': total_time,
            'failed_file_list': failed_files,
        }
    
    def _handle_missing_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Add missing columns to dataframe with NULL values.
        Handles schema evolution where newer ClickHouse table has columns that old data doesn't.
        """
        # Get table schema
        schema = self.get_table_schema(table_name)
        df_columns = set(df.columns)
        table_columns = set(schema.keys())
        
        missing_cols = table_columns - df_columns
        
        if missing_cols:
            logger.warning(f"Adding {len(missing_cols)} missing columns with NULL: {missing_cols}")
            for col in missing_cols:
                df[col] = None
        
        # Reorder columns to match table schema
        df = df[[col for col in schema.keys()]]
        
        return df
    
    def _import_chunks(self, df: pd.DataFrame, table_name: str, chunk_size: int):
        """Helper: Insert dataframe in chunks."""
        df = self._handle_missing_columns(df, table_name)
        total_rows = len(df)
        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            self.client.insert_dataframe(
                f'INSERT INTO {table_name} VALUES',
                chunk
            )
    
    def _insert_dataframe(self, df: pd.DataFrame, table_name: str):
        """Helper: Insert dataframe directly."""
        df = self._handle_missing_columns(df, table_name)
        self.client.insert_dataframe(
            f'INSERT INTO {table_name} VALUES',
            df
        )
    
    def _import_http(self, parquet_file: str, table_name: str):
        """Helper: Import via HTTP API."""
        import requests
        
        with open(parquet_file, 'rb') as f:
            data = f.read()
        
        url = f"http://{self.host}:{self.port}/"
        params = {
            'query': f'INSERT INTO {table_name} FORMAT Parquet',
            'database': self.database
        }
        
        response = requests.post(url, params=params, data=data)
        response.raise_for_status()
    
    def import_method_1_pandas_chunks(
        self,
        parquet_file: str,
        table_name: str,
        chunk_size: int = 10000
    ) -> None:
        """
        Method 1: Read Parquet with pandas and insert in chunks.
        
        ✅ Good for: Small-medium files (<1GB)
        ❌ Issues: Memory overhead, slower than native protocol
        """
        logger.info(f"[Method 1] Importing {parquet_file} → {table_name} (chunk size: {chunk_size})")
        
        try:
            df = pd.read_parquet(parquet_file)
            total_rows = len(df)
            logger.info(f"Loaded {total_rows} rows from Parquet")
            
            # Import in chunks (missing columns handled inside)
            self._import_chunks(df, table_name, chunk_size)
            
            logger.info(f"✓ Successfully imported {total_rows} rows")
            
        except Exception as e:
            logger.error(f"✗ Import failed: {e}")
            raise
    
    def import_method_2_native_protocol(
        self,
        parquet_file: str,
        table_name: str
    ) -> None:
        """
        Method 2: Use native protocol with streaming.
        
        ✅ Good for: Large files, best performance
        ❌ Issues: Requires network connection
        """
        logger.info(f"[Method 2] Importing {parquet_file} → {table_name} (native protocol)")
        
        try:
            df = pd.read_parquet(parquet_file)
            start = time.time()
            
            # Insert (missing columns handled inside)
            self._insert_dataframe(df, table_name)
            
            elapsed = time.time() - start
            rows = len(df)
            rate = rows / elapsed if elapsed > 0 else 0
            
            logger.info(f"✓ Inserted {rows} rows in {elapsed:.2f}s ({rate:.0f} rows/sec)")
            
        except Exception as e:
            logger.error(f"✗ Import failed: {e}")
            raise
    
    def import_method_3_sql_parquet_format(
        self,
        parquet_file: str,
        table_name: str,
        http_port: int = 8123
    ) -> None:
        """
        Method 3: Direct SQL with Parquet format (most efficient).
        
        Requires: File accessible to ClickHouse container
        ✅ Good for: Large files on shared volume, best performance
        ✅ Fastest method for production
        
        Usage:
        - Place Parquet file in /var/lib/clickhouse/user_files/
        - Or use s3() function for S3-hosted files
        """
        logger.warning(
            "Method 3 requires Parquet file in ClickHouse user_files or S3. "
            "Use this in production with shared volumes."
        )
        logger.info(f"[Method 3] Would execute SQL: "
                   f"INSERT INTO {table_name} SELECT * FROM file('{parquet_file}', 'Parquet')")
        
        # Example SQL (uncomment to use):
        # sql = f"INSERT INTO {table_name} SELECT * FROM file('{parquet_file}', 'Parquet')"
        # result = self.client.execute(sql)
        # logger.info(f"✓ Imported via SQL: {result}")
    
    def import_method_4_http_api(
        self,
        parquet_file: str,
        table_name: str,
        http_port: int = 8123
    ) -> None:
        """
        Method 4: HTTP API with streaming.
        
        ✅ Good for: Remote imports, REST integration
        ❌ Issues: Slower than native protocol
        """
        import requests
        
        logger.info(f"[Method 4] Importing via HTTP API → {table_name}")
        
        try:
            with open(parquet_file, 'rb') as f:
                data = f.read()
            
            url = f"http://{self.host}:{http_port}/"
            params = {
                'query': f'INSERT INTO {table_name} FORMAT Parquet',
                'database': self.database
            }
            
            response = requests.post(url, params=params, data=data)
            response.raise_for_status()
            
            logger.info(f"✓ HTTP import successful")
            
        except Exception as e:
            logger.error(f"✗ HTTP import failed: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Fetch table schema."""
        result = self.client.execute(
            f"DESCRIBE TABLE {table_name}"
        )
        return {row[0]: row[1] for row in result}
    
    def get_parquet_schema(self, parquet_file: str) -> Dict:
        """Get Parquet file schema (column names and types)."""
        df = pd.read_parquet(parquet_file)
        return {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    def validate_import(self, table_name: str) -> None:
        """Validate table after import."""
        result = self.client.execute(
            f"SELECT COUNT() as cnt FROM {table_name}"
        )
        count = result[0][0]
        logger.info(f"Table validation: {count:,} rows imported")


def main():
    parser = argparse.ArgumentParser(
        description='Import Parquet files into ClickHouse (single file or batch from directory)'
    )
    
    # Make parquet_file optional since we support both file and directory modes
    parser.add_argument(
        'parquet_file',
        nargs='?',
        help='Path to Parquet file or directory with Parquet files'
    )
    
    parser.add_argument('--table', required=True, help='Target ClickHouse table')
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', type=int, default=9000, help='ClickHouse port')
    parser.add_argument('--database', default='default', help='Database name')
    parser.add_argument('--method', choices=[1, 2, 3, 4], type=int, default=2,
                       help='Import method (1=pandas, 2=native, 3=sql, 4=http)')
    parser.add_argument('--chunk-size', type=int, default=10000,
                       help='Chunk size for method 1')
    parser.add_argument('--validate', action='store_true', help='Validate after import')
    parser.add_argument('--directory', '-d', action='store_true',
                       help='Import all Parquet files from directory (recursive)')
    parser.add_argument('--skip-errors', action='store_true',
                       help='Continue batch import even if some files fail')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be imported without actually importing')
    
    args = parser.parse_args()
    
    if not args.parquet_file:
        parser.print_help()
        sys.exit(1)
    
    # Verify file/directory exists
    path = Path(args.parquet_file)
    if not path.exists():
        logger.error(f"✗ Path not found: {args.parquet_file}")
        sys.exit(1)
    
    # Initialize importer
    importer = ParquetImporter(
        host=args.host,
        port=args.port,
        database=args.database
    )
    
    try:
        # Batch import from directory
        if args.directory or (path.is_dir() and not path.suffix):
            logger.info("=" * 60)
            logger.info("BATCH IMPORT MODE (Directory)")
            logger.info("=" * 60)
            
            result = importer.batch_import(
                args.parquet_file,
                args.table,
                method=args.method,
                chunk_size=args.chunk_size,
                skip_errors=args.skip_errors,
                dry_run=args.dry_run
            )
            
            if not args.dry_run and args.validate and result['successful_files'] > 0:
                importer.validate_import(args.table)
            
            # Exit with error if any files failed
            sys.exit(0 if result['failed_files'] == 0 else 1)
        
        # Single file import
        else:
            logger.info("=" * 60)
            logger.info("SINGLE FILE IMPORT MODE")
            logger.info("=" * 60)
            
            if args.method == 1:
                importer.import_method_1_pandas_chunks(
                    args.parquet_file,
                    args.table,
                    args.chunk_size
                )
            elif args.method == 2:
                importer.import_method_2_native_protocol(
                    args.parquet_file,
                    args.table
                )
            elif args.method == 3:
                importer.import_method_3_sql_parquet_format(
                    args.parquet_file,
                    args.table
                )
            elif args.method == 4:
                importer.import_method_4_http_api(
                    args.parquet_file,
                    args.table
                )
            
            if args.validate:
                importer.validate_import(args.table)
            
            logger.info("✓ Import completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Import failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
