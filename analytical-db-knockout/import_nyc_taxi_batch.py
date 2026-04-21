#!/usr/bin/env python3
"""
Batch import NYC Yellow Taxi Parquet files into ClickHouse.

This script discovers all Parquet files in the NYC Yellow Taxi Record 23-24-25
directory and imports them into ClickHouse in the yellow_taxi_trips table.

Usage:
    python import_nyc_taxi_batch.py
    python import_nyc_taxi_batch.py --method 2 --validate
    python import_nyc_taxi_batch.py --dry-run
    python import_nyc_taxi_batch.py --skip-errors
"""

import sys
import argparse
import logging
from pathlib import Path

from parquet_importer import ParquetImporter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default paths (relative to current directory)
NYC_TAXI_DIR = '../NYC Yellow Taxi Record 23-24-25'
CLICKHOUSE_TABLE = 'yellow_taxi_trips'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000


def main():
    parser = argparse.ArgumentParser(
        description='Batch import NYC Yellow Taxi Parquet files to ClickHouse'
    )
    
    parser.add_argument(
        '--directory',
        default=NYC_TAXI_DIR,
        help=f'Path to NYC Taxi data directory (default: {NYC_TAXI_DIR})'
    )
    parser.add_argument(
        '--table',
        default=CLICKHOUSE_TABLE,
        help=f'Target table name (default: {CLICKHOUSE_TABLE})'
    )
    parser.add_argument(
        '--host',
        default=CLICKHOUSE_HOST,
        help=f'ClickHouse host (default: {CLICKHOUSE_HOST})'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=CLICKHOUSE_PORT,
        help=f'ClickHouse port (default: {CLICKHOUSE_PORT})'
    )
    parser.add_argument(
        '--method',
        type=int,
        choices=[1, 2, 3, 4],
        default=2,
        help='Import method: 1=pandas, 2=native (default), 3=sql, 4=http'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate data after import'
    )
    parser.add_argument(
        '--skip-errors',
        action='store_true',
        help='Continue importing even if some files fail'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be imported without actually importing'
    )
    parser.add_argument(
        '--inspect-schema',
        action='store_true',
        help='Show schema comparison between ClickHouse table and Parquet files'
    )
    
    args = parser.parse_args()
    
    # Verify directory exists
    data_dir = Path(args.directory)
    if not data_dir.exists():
        logger.error(f"✗ Directory not found: {data_dir}")
        logger.info(f"  Looked for: {data_dir.absolute()}")
        sys.exit(1)
    
    logger.info("=" * 70)
    logger.info("NYC YELLOW TAXI BATCH IMPORT")
    logger.info("=" * 70)
    logger.info(f"Directory: {data_dir.absolute()}")
    logger.info(f"Table: {args.table}")
    logger.info(f"Method: {args.method}")
    logger.info(f"Host: {args.host}:{args.port}")
    logger.info("")
    
    # Initialize importer
    importer = ParquetImporter(
        host=args.host,
        port=args.port,
        database='default'
    )
    
    try:
        # Run batch import
        result = importer.batch_import(
            str(data_dir),
            args.table,
            method=args.method,
            skip_errors=args.skip_errors,
            dry_run=args.dry_run,
            inspect_schema=args.inspect_schema
        )
        
        # Validate if requested
        if args.validate and not args.dry_run and result['successful_files'] > 0:
            logger.info("\n" + "=" * 70)
            logger.info("Validating imported data...")
            logger.info("=" * 70)
            importer.validate_import(args.table)
        
        # Exit code
        if result['failed_files'] == 0 or args.dry_run:
            logger.info("\n✓ Batch import completed successfully!")
            sys.exit(0)
        else:
            logger.warning(f"\n⚠️  Import completed with {result['failed_files']} file(s) failed")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"\n✗ Batch import failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
