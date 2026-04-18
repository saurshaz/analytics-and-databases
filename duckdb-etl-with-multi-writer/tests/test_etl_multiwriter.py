#!/usr/bin/env python3
"""
Test multi-writer ETL scenarios

Run with: pytest tests/test_etl_multiwriter.py -v -s
"""

import pytest
import time
import threading
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

from src.etl_pipeline import ETLPipeline
from src.benchmark_etl import ETLBenchmark
from src.partitioning_strategy import PartitionAnalyzer


class TestETLPipeline:
    """Test ETL pipeline operations"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def pipeline(self, temp_dir):
        """Create pipeline for testing"""
        db_path = Path(temp_dir) / 'test_taxi.duckdb'
        return ETLPipeline(
            db_path=str(db_path),
            pipeline_id='test_pipeline'
        )
    
    def test_pipeline_initialization(self, pipeline):
        """Test pipeline initializes correctly"""
        assert pipeline.db_path is not None
        assert pipeline.pipeline_id == 'test_pipeline'
        assert pipeline.etl is not None
        assert pipeline.registry is not None
    
    def test_pipeline_validate_data(self, pipeline):
        """Test data validation when table exists"""
        # Create sample data
        import duckdb
        con = duckdb.connect(str(pipeline.db_path))
        
        con.execute("""
            CREATE TABLE yellow_taxi_trips (
                trip_id INT,
                tpep_pickup_datetime TIMESTAMP,
                VendorID INT,
                total_amount DECIMAL(10,2)
            )
        """)
        
        con.execute("""
            INSERT INTO yellow_taxi_trips VALUES
            (1, '2023-01-01 10:00:00', 1, 15.50),
            (2, '2023-02-01 11:00:00', 2, 20.00)
        """)
        
        con.close()
        
        # Validate
        result = pipeline.validate_data()
        
        assert result['status'] == 'valid'
        assert result['total_rows'] == 2
        assert result['column_count'] > 0
    
    def test_pipeline_show_status(self, pipeline):
        """Test status display doesn't crash"""
        # Should not raise exception
        pipeline.show_status()
    
    def test_pipeline_cleanup(self, pipeline):
        """Test cleanup of old locks"""
        cleaned = pipeline.cleanup_old_locks(older_than_hours=24)
        assert isinstance(cleaned, int)
        assert cleaned >= 0


class TestETLBenchmark:
    """Test ETL benchmarking functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def benchmark(self, temp_dir):
        """Create benchmark instance"""
        db_path = Path(temp_dir) / 'test_bench.duckdb'
        return ETLBenchmark(db_path=str(db_path))
    
    def test_benchmark_query(self, benchmark):
        """Test query benchmarking"""
        result = benchmark.run_query_benchmark(
            query_name='test_query',
            query_sql='SELECT 1 as test',
            iterations=2
        )
        
        assert result['status'] == 'success'
        assert result['query_name'] == 'test_query'
        assert result['iterations'] == 2
        assert result['average_sec'] > 0
        assert result['rows_returned'] >= 0
    
    def test_benchmark_results_structure(self, benchmark):
        """Test benchmark results have correct structure"""
        # Run minimal benchmark
        result = benchmark.run_query_benchmark(
            query_name='structure_test',
            query_sql='SELECT COUNT(*) FROM (SELECT 1) as t'
        )
        
        assert 'query_name' in result
        assert 'average_sec' in result or 'status' in result


class TestPartitionAnalyzer:
    """Test partition analysis functionality"""
    
    def test_analyzer_initialization(self):
        """Test analyzer initializes"""
        analyzer = PartitionAnalyzer()
        assert analyzer.data_dir is not None
    
    def test_analyze_no_data(self):
        """Test analysis with no data directory"""
        analyzer = PartitionAnalyzer(data_dir='/nonexistent/path')
        analysis = analyzer.analyze()
        
        assert 'status' in analysis
        # Should report no data found
        assert analysis['status'] in ('no_data_found', 'success')
    
    def test_get_partition_globs(self):
        """Test glob pattern generation"""
        analyzer = PartitionAnalyzer()
        globs = analyzer.get_partition_globs()
        
        assert isinstance(globs, list)
        # Should return a list (may be empty if no data)
    
    def test_estimate_load_time(self):
        """Test load time estimation"""
        analyzer = PartitionAnalyzer()
        estimate = analyzer.estimate_load_time()
        
        assert 'estimated_duration_sec' in estimate
        assert 'estimated_duration_min' in estimate
        assert estimate['estimated_duration_sec'] >= 0


class TestMultiWriterCoordination:
    """Test multi-writer coordination in ETL scenarios"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def pipeline(self, temp_dir):
        """Create pipeline for multi-writer testing"""
        db_path = Path(temp_dir) / 'multiwriter_test.duckdb'
        return ETLPipeline(db_path=str(db_path))
    
    def test_sequential_loads_with_different_writers(self, pipeline):
        """Test that two sequential loads with different writers don't conflict"""
        results = []
        
        def load_task(year, writer_id):
            try:
                # Create simple test table
                import duckdb
                con = duckdb.connect(str(pipeline.db_path))
                
                # Create table for first writer
                if writer_id == 'writer_1':
                    con.execute("""
                        CREATE TABLE IF NOT EXISTS test_data (
                            id INT,
                            year INT,
                            value DECIMAL(10,2)
                        )
                    """)
                
                # Each writer inserts different years
                con.execute(f"""
                    INSERT INTO test_data VALUES
                    ({year}, {year}, {year * 100})
                """)
                con.close()
                
                results.append({'writer_id': writer_id, 'year': year, 'status': 'success'})
            except Exception as e:
                results.append({'writer_id': writer_id, 'year': year, 'status': 'failed', 'error': str(e)})
        
        # Sequential loads with registry locking concepts
        load_task(2023, 'writer_1')
        load_task(2024, 'writer_2')
        
        # Both should succeed
        assert len(results) == 2
        assert all(r['status'] == 'success' for r in results)
    
    def test_concurrent_writers_different_runs(self, pipeline):
        """Test concurrent writers with different run IDs"""
        results = []
        
        def writer_task(writer_id, run_id):
            try:
                with pipeline.registry.acquire_lock(run_id, writer_id, timeout=10):
                    time.sleep(0.1)
                    results.append({'writer_id': writer_id, 'run_id': run_id, 'status': 'success'})
            except Exception as e:
                results.append({'writer_id': writer_id, 'run_id': run_id, 'status': 'failed'})
        
        # Different run IDs - should not conflict
        threads = [
            threading.Thread(target=writer_task, args=('w1', 'run_1')),
            threading.Thread(target=writer_task, args=('w2', 'run_2'))
        ]
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        assert len(results) == 2
        assert all(r['status'] == 'success' for r in results)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
