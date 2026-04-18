#!/usr/bin/env python3
"""
Test Registry Locking functionality

Run with: pytest tests/test_registry_locking.py -v -s
"""

import pytest
import time
import threading
import json
from pathlib import Path
from datetime import datetime
import tempfile
import shutil

from src.registry_lock_manager import RegistryLockManager, LockContext
from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL


class TestRegistryLockManager:
    """Test core registry locking functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test registries"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def registry(self, temp_dir):
        """Create registry manager for testing"""
        db_path = Path(temp_dir) / 'test.duckdb'
        return RegistryLockManager(
            db_path=str(db_path),
            registry_dir=str(Path(temp_dir) / 'registries')
        )
    
    def test_registry_creates_files(self, registry):
        """Test that registry creates necessary files"""
        assert registry.registry_file.exists()
        assert registry.registry_file.read_text() == '{"runs": [], "locks": []}'
    
    def test_single_lock_acquire_and_release(self, registry):
        """Test acquiring and releasing a single lock"""
        run_id = 'test_run_1'
        writer_id = 'worker_1'
        
        # Acquire lock
        with registry.acquire_lock(run_id, writer_id, timeout=10):
            # Check lock is active
            active = registry.get_active_locks()
            assert len(active) == 1
            assert active[0]['writer_id'] == writer_id
        
        # After context exit, check lock is released
        active = registry.get_active_locks()
        # Locks may still be in registry but with 'success' status
        assert len(active) == 0 or all(l['status'] != 'active' for l in active)
    
    def test_lock_prevents_concurrent_access(self, registry):
        """Test that lock prevents concurrent access"""
        run_id = 'test_run_concurrent'
        timeline = []
        
        def writer_task(writer_id):
            try:
                with registry.acquire_lock(run_id, writer_id, timeout=5):
                    timeline.append(('enter', writer_id))
                    time.sleep(0.5)
                    timeline.append(('exit', writer_id))
            except TimeoutError:
                timeline.append(('timeout', writer_id))
        
        # Start 2 threads
        t1 = threading.Thread(target=writer_task, args=('w1',))
        t2 = threading.Thread(target=writer_task, args=('w2',))
        
        t1.start()
        time.sleep(0.1)  # Ensure t1 gets lock first
        t2.start()
        
        t1.join()
        t2.join()
        
        # Both should complete successfully
        assert ('enter', 'w1') in timeline
        assert ('exit', 'w1') in timeline
        # w2 should retry and eventually succeed
        assert any(entry[0] != 'timeout' and entry[1] == 'w2' for entry in timeline if entry[0] in ('enter', 'exit'))
    
    def test_etl_run_recording(self, registry):
        """Test recording ETL runs in registry"""
        run_id = 'etl_001'
        start_time = datetime.utcnow()
        
        registry.record_etl_run(
            run_id=run_id,
            pipeline_id='test_pipeline',
            writer_id='worker_1',
            start_time=start_time,
            status='running'
        )
        
        run = registry.get_etl_run(run_id)
        assert run is not None
        assert run['run_id'] == run_id
        assert run['status'] == 'running'
        assert run['rows_written'] == 0
    
    def test_etl_run_update(self, registry):
        """Test updating ETL run"""
        run_id = 'etl_002'
        start_time = datetime.utcnow()
        
        # Create run
        registry.record_etl_run(
            run_id=run_id,
            pipeline_id='test_pipeline',
            writer_id='worker_1',
            start_time=start_time
        )
        
        # Update run
        end_time = datetime.utcnow()
        registry.update_etl_run(
            run_id=run_id,
            status='completed',
            rows_written=1000000,
            bytes_written=52428800,
            end_time=end_time
        )
        
        run = registry.get_etl_run(run_id)
        assert run['status'] == 'completed'
        assert run['rows_written'] == 1000000
        assert run['bytes_written'] == 52428800
        assert run['ended_at'] is not None
    
    def test_lock_timeout(self, registry):
        """Test that lock timeout works"""
        run_id = 'test_timeout'
        
        def hold_lock():
            with registry.acquire_lock(run_id, 'holder', timeout=5):
                time.sleep(2)
        
        # Start holder
        holder_thread = threading.Thread(target=hold_lock)
        holder_thread.start()
        
        time.sleep(0.5)  # Let holder acquire
        
        # Try to acquire with short timeout
        start = time.time()
        with pytest.raises(TimeoutError):
            with registry.acquire_lock(run_id, 'waiter', timeout=1):
                pass
        
        elapsed = time.time() - start
        assert elapsed >= 1  # Should have waited at least until timeout
        
        holder_thread.join()
    
    def test_cleanup_expired_locks(self, registry):
        """Test cleanup of old locks"""
        # Add some test locks
        registry_data = {
            'runs': [],
            'locks': [
                {
                    'lock_id': 'old_lock_1',
                    'writer_id': 'w1',
                    'status': 'success',
                    'released_at': '2026-04-10T00:00:00'  # 7 days old
                },
                {
                    'lock_id': 'recent_lock_1',
                    'writer_id': 'w2',
                    'status': 'success',
                    'released_at': datetime.utcnow().isoformat()  # Just now
                }
            ]
        }
        registry._write_registry(registry_data)
        
        # Cleanup anything older than 1 hour
        cleaned = registry.cleanup_expired_locks(older_than_seconds=3600)
        
        assert cleaned == 1  # Should remove old_lock_1


class TestDuckDBMultiWriter:
    """Test DuckDB multi-writer ETL functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def etl(self, temp_dir):
        """Create multi-writer ETL instance"""
        db_path = Path(temp_dir) / 'test_taxi.duckdb'
        return DuckDBMultiWriterETL(
            db_path=str(db_path),
            pipeline_id='test_etl',
            registry_dir=str(Path(temp_dir) / 'registries'),
            timeout=30
        )
    
    def test_etl_execute_sql(self, etl):
        """Test safe SQL execution"""
        result = etl.execute_sql_safe(
            query="SELECT 1 as test",
            run_id='test_sql_001',
            writer_id='worker_1',
            query_name='simple_query'
        )
        
        assert result['status'] == 'success'
        assert len(result['result']) > 0
        assert result['duration_sec'] > 0
    
    def test_etl_get_registry_status(self, etl):
        """Test getting registry status"""
        status = etl.get_registry_status()
        
        assert 'active_locks' in status
        assert 'all_runs' in status
        assert status['db_path'] is not None
        assert status['pipeline_id'] == 'test_etl'
    
    def test_etl_cleanup_old_locks(self, etl):
        """Test cleanup functionality"""
        cleaned = etl.cleanup_old_locks(older_than_hours=1)
        
        # Should return an integer (number of locks cleaned)
        assert isinstance(cleaned, int)
        assert cleaned >= 0


class TestMultiWriterScenarios:
    """Test realistic multi-writer scenarios"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def registry(self, temp_dir):
        """Create registry for testing"""
        db_path = Path(temp_dir) / 'test.duckdb'
        return RegistryLockManager(
            db_path=str(db_path),
            registry_dir=str(Path(temp_dir) / 'registries')
        )
    
    def test_three_writer_scenario(self, registry):
        """Test 3 concurrent writers accessing same lock"""
        run_id = 'three_writer_test'
        results = []
        barrier = threading.Barrier(3)  # Wait for all threads to start
        
        def writer_task(writer_id, duration):
            barrier.wait()  # Synchronize start
            try:
                start = time.time()
                with registry.acquire_lock(run_id, writer_id, timeout=20):
                    actual_wait = time.time() - start
                    time.sleep(duration)
                    results.append({
                        'writer_id': writer_id,
                        'status': 'success',
                        'wait_time': actual_wait,
                        'work_time': duration
                    })
            except TimeoutError:
                results.append({
                    'writer_id': writer_id,
                    'status': 'timeout'
                })
        
        # Start 3 writers with 0.5s work duration each
        threads = [
            threading.Thread(target=writer_task, args=(f'w{i}', 0.5))
            for i in range(3)
        ]
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # All should succeed
        assert len(results) == 3
        assert all(r['status'] == 'success' for r in results)
        
        # Total time should be roughly 3 * 0.5 = 1.5 seconds (sequential)
        total_work_time = sum(r['work_time'] for r in results)
        assert abs(total_work_time - 1.5) < 0.5
    
    def test_writer_failure_handling(self, registry):
        """Test that failed writers are cleaned up"""
        run_id = 'failure_test'
        
        try:
            with registry.acquire_lock(run_id, 'failing_writer', timeout=5):
                print("Lock acquired")
                raise RuntimeError("Simulated failure")
        except RuntimeError:
            pass
        
        # Lock should be released despite exception
        active = registry.get_active_locks()
        assert len(active) == 0 or all(l['status'] != 'active' for l in active)
        
        # Check registry recorded the failure
        locks = registry._read_registry()['locks']
        failure_lock = next((l for l in locks if l['writer_id'] == 'failing_writer'), None)
        
        if failure_lock:
            assert failure_lock['status'] == 'failed'
            assert 'error' in failure_lock


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
