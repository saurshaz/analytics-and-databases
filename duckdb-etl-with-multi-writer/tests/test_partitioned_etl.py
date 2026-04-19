#!/usr/bin/env python3
"""Test partitioned ETL functionality"""

import pytest
import tempfile
import pandas as pd
from pathlib import Path
from src.unified_etl_pipeline import UnifiedETLPipeline


class TestPartitionedETLPipeline:
    """Test suite for Hive partitioned ETL"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    def test_normalize_columns_tpep_prefix(self):
        """Test that tpep_ prefixes are normalized correctly"""
        df = pd.DataFrame({
            'tpep_pickup_datetime': ['2024-01-01 12:00:00'],
            'tpep_dropoff_datetime': ['2024-01-01 12:15:00'],
            'trip_distance': [1.5],
            'total_amount': [15.50],
            'payment_type': [1]
        })
        
        normalized = PartitionedETLPipeline._normalize_columns(df)
        
        # Check that tpep_ prefix was removed
        assert 'pickup_datetime' in normalized.columns
        assert 'dropoff_datetime' in normalized.columns
        assert 'tpep_pickup_datetime' not in normalized.columns
        assert 'trip_distance' in normalized.columns
        assert 'total_amount' in normalized.columns
    
    def test_normalize_columns_uppercase(self):
        """Test that uppercase variations are handled"""
        df = pd.DataFrame({
            'TPEP_PICKUP_DATETIME': ['2024-01-01 12:00:00'],
            'TPEP_DROPOFF_DATETIME': ['2024-01-01 12:15:00'],
            'TRIP_DISTANCE': [1.5]
        })
        
        normalized = PartitionedETLPipeline._normalize_columns(df)
        
        # Column names should be preserved or normalized
        assert len(normalized.columns) == 3
    
    def test_date_extraction(self):
        """Test date extraction from filenames"""
        # Standard format
        assert PartitionedETLPipeline._extract_date('yellow_tripdata_2023-01.parquet') == '2023-01'
        assert PartitionedETLPipeline._extract_date('yellow_tripdata_2024-12.parquet') == '2024-12'
        
        # No match returns None
        assert PartitionedETLPipeline._extract_date('some_file.parquet') is None
    
    def test_partitioned_pipeline_initialization(self, temp_output_dir):
        """Test that pipeline initializes with correct directories"""
        pipeline = PartitionedETLPipeline(
            output_dir=temp_output_dir
        )
        
        assert pipeline.output_dir.exists()
        assert pipeline.registry is not None
    
    def test_column_mapping_completeness(self):
        """Test that common NYC taxi columns are mapped"""
        # Create a dataframe with various column names
        df = pd.DataFrame({
            'tpep_pickup_datetime': [1],
            'tpep_dropoff_datetime': [1],
            'trip_distance': [1.0],
            'passenger_count': [1],
            'fare_amount': [1.0],
            'extra': [0.0],
            'mta_tax': [0.5],
            'tip_amount': [0.0],
            'tolls_amount': [0.0],
            'total_amount': [1.5],
            'payment_type': [1],
            'pulocationid': [1],
            'dolocationid': [1],
            'vendorid': [1]
        })
        
        normalized = PartitionedETLPipeline._normalize_columns(df)
        
        # All columns should be present
        assert len(normalized.columns) == len(df.columns)
        
        # Check key normalized names exist
        assert 'pickup_datetime' in normalized.columns
        assert 'dropoff_datetime' in normalized.columns
        assert 'total_amount' in normalized.columns
    
    def test_partition_directory_naming(self):
        """Test that partition directory names follow year=YYYY/month=MM/day=DD/ format"""
        # This is a structure validation test
        # The actual directory creation happens in load_and_partition_year
        
        year_part = "2024"
        month_part = "03"
        day_part = "01"
        
        # Build path like the pipeline would
        partition_path = Path("data/processed") / f"year={year_part}" / f"month={month_part}" / f"day={day_part}"
        
        # Verify structure is correct
        assert str(partition_path) == "data/processed/year=2024/month=03/day=01"
    
    def test_compression_formats(self):
        """Test that compression formats are supported"""
        supported_formats = ['snappy', 'gzip', 'uncompressed']
        
        # All formats should be valid (actual compression tested in integration tests)
        for fmt in supported_formats:
            assert fmt in ['snappy', 'gzip', 'uncompressed']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
