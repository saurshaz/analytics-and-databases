#!/usr/bin/env python3
"""
Tests for Parquet import functionality.
Run with: pytest benchmarks/test_parquet_import.py -v
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import logging
from clickhouse_driver import Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def temp_parquet_file():
    """Create a temporary Parquet file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        temp_path = f.name
    
    # Generate sample data
    df = pd.DataFrame({
        'id': np.arange(1000),
        'value': np.random.randn(1000),
        'category': np.random.choice(['A', 'B', 'C'], 1000),
        'date': pd.date_range('2024-01-01', periods=1000, freq='D'),
    })
    
    df.to_parquet(temp_path, engine='pyarrow', compression='snappy')
    yield temp_path
    
    # Cleanup
    Path(temp_path).unlink()


@pytest.fixture
def clickhouse_client():
    """Get ClickHouse client."""
    try:
        client = Client('localhost')
        client.execute('SELECT 1')
        return client
    except Exception as e:
        pytest.skip(f"ClickHouse not available: {e}")


@pytest.fixture
def test_table(clickhouse_client):
    """Create test table in ClickHouse."""
    # Drop if exists
    clickhouse_client.execute('DROP TABLE IF EXISTS test_parquet_import')
    
    # Create table
    clickhouse_client.execute("""
        CREATE TABLE test_parquet_import (
            id UInt32,
            value Float64,
            category String,
            date Date
        ) ENGINE = MergeTree()
        ORDER BY date
    """)
    
    yield 'test_parquet_import'
    
    # Cleanup
    clickhouse_client.execute('DROP TABLE IF EXISTS test_parquet_import')


def test_parquet_file_creation(temp_parquet_file):
    """Test that Parquet file is created correctly."""
    assert Path(temp_parquet_file).exists()
    assert temp_parquet_file.endswith('.parquet')
    
    # Read and verify
    df = pd.read_parquet(temp_parquet_file)
    assert len(df) == 1000
    assert list(df.columns) == ['id', 'value', 'category', 'date']


def test_clickhouse_connection(clickhouse_client):
    """Test ClickHouse connection."""
    result = clickhouse_client.execute('SELECT 1')
    assert result[0][0] == 1


def test_create_test_table(clickhouse_client, test_table):
    """Test table creation."""
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == 0


def test_import_via_pandas(clickhouse_client, test_table, temp_parquet_file):
    """Test importing Parquet via pandas chunks."""
    logger.info("Testing pandas import method...")
    
    df = pd.read_parquet(temp_parquet_file)
    
    # Insert via clickhouse-driver
    clickhouse_client.insert_dataframe(
        f'INSERT INTO {test_table} VALUES',
        df
    )
    
    # Verify
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == 1000
    logger.info(f"✓ Imported {result[0][0]} rows")


def test_import_chunked(clickhouse_client, test_table, temp_parquet_file):
    """Test chunked import."""
    logger.info("Testing chunked import...")
    
    df = pd.read_parquet(temp_parquet_file)
    chunk_size = 100
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        clickhouse_client.insert_dataframe(
            f'INSERT INTO {test_table} VALUES',
            chunk
        )
    
    # Verify
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == 1000
    logger.info(f"✓ Imported {result[0][0]} rows in chunks")


def test_data_integrity(clickhouse_client, test_table, temp_parquet_file):
    """Test that data is imported correctly."""
    logger.info("Testing data integrity...")
    
    df = pd.read_parquet(temp_parquet_file)
    
    # Import
    clickhouse_client.insert_dataframe(
        f'INSERT INTO {test_table} VALUES',
        df
    )
    
    # Check count
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == len(df)
    
    # Check date range
    result = clickhouse_client.execute(
        f"SELECT min(date), max(date) FROM {test_table}"
    )
    min_date, max_date = result[0]
    assert min_date is not None
    assert max_date is not None
    logger.info(f"✓ Date range: {min_date} to {max_date}")
    
    # Check value stats
    result = clickhouse_client.execute(
        f"SELECT avg(value), count(DISTINCT category) FROM {test_table}"
    )
    avg_value, distinct_categories = result[0]
    assert avg_value is not None
    assert distinct_categories == 3
    logger.info(f"✓ Average value: {avg_value:.2f}, Categories: {distinct_categories}")


def test_import_with_duplicates(clickhouse_client, test_table, temp_parquet_file):
    """Test that multiple imports work correctly."""
    logger.info("Testing multiple imports...")
    
    df = pd.read_parquet(temp_parquet_file)
    
    # Import twice
    clickhouse_client.insert_dataframe(
        f'INSERT INTO {test_table} VALUES',
        df
    )
    clickhouse_client.insert_dataframe(
        f'INSERT INTO {test_table} VALUES',
        df
    )
    
    # Should have 2x rows
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == 2000
    logger.info(f"✓ Multiple imports successful: {result[0][0]} rows")


def test_empty_dataframe(clickhouse_client, test_table):
    """Test importing empty dataframe."""
    logger.info("Testing empty dataframe...")
    
    df = pd.DataFrame({
        'id': pd.Series([], dtype='int32'),
        'value': pd.Series([], dtype='float64'),
        'category': pd.Series([], dtype='object'),
        'date': pd.Series([], dtype='datetime64[ns]'),
    })
    
    clickhouse_client.insert_dataframe(
        f'INSERT INTO {test_table} VALUES',
        df
    )
    
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == 0
    logger.info("✓ Empty import handled correctly")


def test_large_dataframe_chunking(clickhouse_client, test_table):
    """Test importing large dataframe with chunking."""
    logger.info("Testing large dataframe chunking...")
    
    # Create larger dataset
    large_df = pd.DataFrame({
        'id': np.arange(10000),
        'value': np.random.randn(10000),
        'category': np.random.choice(['A', 'B', 'C'], 10000),
        'date': pd.date_range('2024-01-01', periods=10000, freq='H'),
    })
    
    # Import in chunks
    chunk_size = 1000
    for i in range(0, len(large_df), chunk_size):
        chunk = large_df.iloc[i:i + chunk_size]
        clickhouse_client.insert_dataframe(
            f'INSERT INTO {test_table} VALUES',
            chunk
        )
    
    result = clickhouse_client.execute(f"SELECT count() FROM {test_table}")
    assert result[0][0] == 10000
    logger.info(f"✓ Imported large dataset: {result[0][0]} rows")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
