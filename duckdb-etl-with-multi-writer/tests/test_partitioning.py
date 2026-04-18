#!/usr/bin/env python3
"""
Test partitioning strategy functionality

Run with: pytest tests/test_partitioning.py -v -s
"""

import pytest
from pathlib import Path
import tempfile
import shutil
import json

from src.partitioning_strategy import PartitionAnalyzer


class TestPartitionAnalyzer:
    """Test partition discovery and analysis"""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary data directory with mock parquet files"""
        temp_dir = tempfile.mkdtemp()
        
        # Create mock directory structure
        data_root = Path(temp_dir) / 'NYC Yellow Taxi Record 23-24-25'
        
        for year in [2023, 2024]:
            year_dir = data_root / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            
            # Create mock parquet files
            for month in range(1, 3):  # Only 2 months per year for speed
                parquet_file = year_dir / f'yellow_tripdata_{year:04d}-{month:02d}.parquet'
                parquet_file.touch()
        
        yield str(data_root)
        
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def analyzer(self, temp_data_dir):
        """Create analyzer with test data"""
        return PartitionAnalyzer(data_dir=temp_data_dir)
    
    def test_discover_partitions(self, analyzer):
        """Test partition discovery"""
        partitions = analyzer.discover_partitions()
        
        assert isinstance(partitions, dict)
        assert len(partitions) > 0
    
    def test_discover_partitions_structure(self, analyzer):
        """Test partition structure"""
        partitions = analyzer.discover_partitions()
        
        # Should have entries for years
        partition_keys = list(partitions.keys())
        
        for key in partition_keys:
            # Keys should be strings (year or year/month)
            assert isinstance(key, str)
            # Values should be path lists
            files = partitions[key]
            assert isinstance(files, list)
            assert all(isinstance(f, Path) for f in files)
    
    def test_analyze_returns_analysis(self, analyzer):
        """Test that analyze returns proper structure"""
        analysis = analyzer.analyze()
        
        assert 'status' in analysis
        assert 'total_partitions' in analysis
        assert 'partitions' in analysis
        assert 'recommendations' in analysis
    
    def test_analyze_with_data(self, analyzer):
        """Test analysis with mock data"""
        analysis = analyzer.analyze()
        
        assert analysis['status'] in ('success', 'no_data_found')
        
        if analysis['status'] == 'success':
            assert analysis['total_partitions'] > 0
            assert 'partitions' in analysis
            assert len(analysis['partitions']) > 0
    
    def test_get_partition_globs(self, analyzer):
        """Test glob pattern generation"""
        globs = analyzer.get_partition_globs()
        
        assert isinstance(globs, list)
        
        # Each glob should be a valid pattern
        for glob_pattern in globs:
            assert isinstance(glob_pattern, str)
            assert '*.parquet' in glob_pattern or '.parquet' in glob_pattern
    
    def test_estimate_load_time_structure(self, analyzer):
        """Test load time estimate returns proper structure"""
        estimate = analyzer.estimate_load_time()
        
        assert 'estimated_total_rows' in estimate
        assert 'throughput_rows_per_sec' in estimate
        assert 'estimated_duration_sec' in estimate
        assert 'estimated_duration_min' in estimate
        assert 'notes' in estimate
    
    def test_estimate_load_time_values(self, analyzer):
        """Test load time estimate values are reasonable"""
        estimate = analyzer.estimate_load_time()
        
        assert estimate['estimated_total_rows'] >= 0
        assert estimate['throughput_rows_per_sec'] > 0
        assert estimate['estimated_duration_sec'] >= 0
        assert estimate['estimated_duration_min'] >= 0
    
    def test_estimate_load_time_consistency(self, analyzer):
        """Test consistency between duration estimates"""
        estimate = analyzer.estimate_load_time()
        
        # Minutes should be approx seconds / 60
        calculated_min = estimate['estimated_duration_sec'] / 60
        expected_min = estimate['estimated_duration_min']
        
        # Allow small floating point tolerance
        assert abs(calculated_min - expected_min) < 0.1


class TestPartitioningStrategy:
    """Test partitioning strategy recommendations"""
    
    @pytest.fixture
    def analyzer_no_data(self):
        """Create analyzer with no data"""
        return PartitionAnalyzer(data_dir='/nonexistent')
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary data directory"""
        temp_dir = tempfile.mkdtemp()
        data_root = Path(temp_dir) / 'NYC Yellow Taxi Record 23-24-25'
        
        # Create 3 years of data
        for year in [2023, 2024, 2025]:
            year_dir = data_root / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            
            for month in range(1, 13):
                parquet_file = year_dir / f'yellow_tripdata_{year:04d}-{month:02d}.parquet'
                parquet_file.touch()
        
        yield str(data_root)
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def analyzer_with_data(self, temp_data_dir):
        """Create analyzer with test data"""
        return PartitionAnalyzer(data_dir=temp_data_dir)
    
    def test_strategy_yearly_format(self, analyzer_with_data):
        """Test that strategy handles yearly partitions"""
        analysis = analyzer_with_data.analyze()
        
        if analysis['status'] == 'success':
            # Should have yearly grouping
            assert 'partitions' in analysis
            assert len(analysis['partitions']) > 0
    
    def test_recommendations_present(self, analyzer_with_data):
        """Test that recommendations are present"""
        analysis = analyzer_with_data.analyze()
        
        if analysis['status'] == 'success':
            recs = analysis['recommendations']
            assert 'total_files' in recs
            assert 'total_size_gb' in recs
            assert 'recommended_workers' in recs
            assert 'loading_order' in recs
            assert 'notes' in recs
    
    def test_recommendation_workers_reasonable(self, analyzer_with_data):
        """Test worker recommendation is reasonable"""
        analysis = analyzer_with_data.analyze()
        
        if analysis['status'] == 'success':
            workers = analysis['recommendations']['recommended_workers']
            # Should be between 1 and 4
            assert 1 <= workers <= 4


class TestPartitionAnalyzerEdgeCases:
    """Test edge cases in partition analysis"""
    
    def test_analyzer_with_missing_directory(self):
        """Test analyzer handles missing directory"""
        analyzer = PartitionAnalyzer(data_dir='/totally/fake/path/xyz')
        analysis = analyzer.analyze()
        
        assert analysis['status'] == 'no_data_found'
    
    def test_analyze_handles_malformed_data(self):
        """Test analyzer handles directory with non-parquet files"""
        temp_dir = tempfile.mkdtemp()
        
        try:
            data_root = Path(temp_dir) / 'NYC Yellow Taxi Record 23-24-25'
            year_dir = data_root / '2023'
            year_dir.mkdir(parents=True)
            
            # Create non-parquet files
            (year_dir / 'README.txt').touch()
            (year_dir / 'data.csv').touch()
            
            analyzer = PartitionAnalyzer(data_dir=str(data_root))
            analysis = analyzer.analyze()
            
            # Should still return valid structure even with no parquet files
            assert 'status' in analysis
            assert 'partitions' in analysis
        
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
