#!/usr/bin/env python3
"""
Registry Locking Multi-Writer Coordinator

Enables multiple ETL writers to safely coordinate DuckDB writes using 
file-based locking and a JSON registry. No Temporal, no complex orchestration.

Usage:
    registry = RegistryLockManager(db_path='nyc_yellow_taxi.duckdb')
    
    with registry.acquire_lock('etl_run_001', writer_id='worker_1', timeout=30):
        # Safely execute DuckDB transactions
        con.execute("INSERT INTO table ...")
        # Registry updated when lock is released
"""

import json
import fcntl
import time
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from threading import Lock as ThreadLock


@dataclass
class LockEntry:
    """Represents a single lock entry in the registry"""
    lock_id: str
    writer_id: str
    acquired_at: str
    expires_at: str
    timeout_sec: int
    status: str  # 'active', 'released', 'expired', 'failed'
    error: Optional[str] = None


@dataclass
class ETLRunEntry:
    """Represents an ETL run in the registry"""
    run_id: str
    pipeline_id: str
    status: str  # 'pending', 'running', 'completed', 'failed'
    started_at: str
    ended_at: Optional[str] = None
    rows_written: int = 0
    bytes_written: int = 0
    lock_id: Optional[str] = None
    writer_id: Optional[str] = None
    metadata: Dict[str, Any] = None


class RegistryLockManager:
    """
    File-based registry lock manager for coordinating multi-writer DuckDB access.
    
    Features:
    - Atomic lock operations using fcntl
    - JSON registry for audit trail
    - Automatic lock expiration
    - No external services required
    - Thread-safe
    """
    
    def __init__(
        self,
        db_path: str,
        registry_dir: str = 'data/registries',
        default_timeout: int = 60
    ):
        """
        Initialize registry lock manager
        
        Args:
            db_path: Path to DuckDB file (used to derive registry name)
            registry_dir: Directory to store registry files
            default_timeout: Default lock timeout in seconds
        """
        self.db_path = Path(db_path)
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        
        # Get DB name without extension
        db_name = self.db_path.stem
        
        self.registry_file = self.registry_dir / f"{db_name}_registry.json"
        self.lock_file = self.registry_dir / f"{db_name}_registry.lock"
        self.default_timeout = default_timeout
        
        # Thread-safe in-memory registry (backup)
        self._thread_lock = ThreadLock()
        self._registry_cache = None
        
        # Initialize registry if not exists
        self._ensure_registry()
    
    def _ensure_registry(self) -> None:
        """Ensure registry file exists and is valid JSON"""
        if not self.registry_file.exists():
            self._write_registry({'runs': [], 'locks': []})
    
    def _write_registry(self, data: Dict[str, Any]) -> None:
        """Atomically write registry to file with lock"""
        with self._file_lock():
            with open(self.registry_file, 'w') as f:
                json.dump(data, f)
    
    def _read_registry(self) -> Dict[str, Any]:
        """Atomically read registry from file with lock"""
        with self._file_lock():
            try:
                with open(self.registry_file, 'r') as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                return {'runs': [], 'locks': []}
    
    @contextmanager
    def _file_lock(self):
        """Context manager for file-based locking using fcntl"""
        lock_fd = open(self.lock_file, 'w')
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
    
    def acquire_lock(
        self,
        run_id: str,
        writer_id: str,
        timeout: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'LockContext':
        """
        Acquire a lock for a specific ETL run.
        
        Returns a context manager that:
        - Acquires lock on entry
        - Holds lock during computation
        - Releases lock on exit
        
        Args:
            run_id: Unique ETL run identifier
            writer_id: Identifier of the writer (e.g., 'worker_1', 'etl_node_2')
            timeout: Lock timeout in seconds (default: 60)
            metadata: Optional metadata dict to store with lock
        
        Returns:
            LockContext context manager
        
        Example:
            with registry.acquire_lock('run_001', 'worker_1', timeout=120):
                # Write to DuckDB safely
                con.execute("INSERT INTO ...")
        """
        timeout = timeout or self.default_timeout
        return LockContext(
            registry=self,
            run_id=run_id,
            writer_id=writer_id,
            timeout=timeout,
            metadata=metadata or {}
        )
    
    def _try_acquire_lock(
        self,
        run_id: str,
        writer_id: str,
        timeout: int
    ) -> Optional[str]:
        """
        Attempt to acquire a lock. Returns lock_id if successful, None if conflict.
        """
        registry = self._read_registry()
        now = datetime.now(timezone.utc)
        lock_id = f"{run_id}_{writer_id}_{int(time.time() * 1000)}"
        
        # Check for existing active locks
        active_locks = [
            lock for lock in registry.get('locks', [])
            if lock['status'] == 'active'
            and lock['lock_id'] != lock_id
        ]
        
        if active_locks:
            # There are active locks - check if they should expire
            expired = []
            for lock in active_locks:
                try:
                    expires = datetime.fromisoformat(lock['expires_at'])
                    if now > expires:
                        expired.append(lock['lock_id'])
                except (KeyError, ValueError):
                    pass
            
            # Remove expired locks
            if expired:
                registry['locks'] = [
                    l for l in registry['locks'] if l['lock_id'] not in expired
                ]
        
        # Check again if there are still active locks from OTHER writers
        conflict_locks = [
            lock for lock in registry.get('locks', [])
            if lock['status'] == 'active'
            and lock['run_id'] == run_id
            and lock['writer_id'] != writer_id
        ]
        
        if conflict_locks:
            # Can't acquire - another writer has the lock
            return None
        
        # Create lock entry
        lock_entry = {
            'lock_id': lock_id,
            'writer_id': writer_id,
            'run_id': run_id,
            'acquired_at': now.isoformat(),
            'expires_at': (now + timedelta(seconds=timeout)).isoformat(),
            'timeout_sec': timeout,
            'status': 'active'
        }
        
        registry['locks'].append(lock_entry)
        self._write_registry(registry)
        
        return lock_id
    
    def _release_lock(
        self,
        lock_id: str,
        success: bool = True,
        error: Optional[str] = None,
        stats: Optional[Dict[str, Any]] = None
    ) -> None:
        """Release a lock and update registry with results"""
        registry = self._read_registry()
        
        # Find and update lock entry
        for lock in registry['locks']:
            if lock['lock_id'] == lock_id:
                lock['status'] = 'success' if success else 'failed'
                lock['released_at'] = datetime.now(timezone.utc).isoformat()
                if error:
                    lock['error'] = error
                if stats:
                    lock['stats'] = stats
                break
        
        self._write_registry(registry)
    
    def record_etl_run(
        self,
        run_id: str,
        pipeline_id: str,
        writer_id: str,
        start_time: datetime,
        rows_written: int = 0,
        bytes_written: int = 0,
        status: str = 'pending',
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record an ETL run in the registry"""
        registry = self._read_registry()
        
        run_entry = {
            'run_id': run_id,
            'pipeline_id': pipeline_id,
            'writer_id': writer_id,
            'status': status,
            'started_at': start_time.isoformat(),
            'ended_at': None,
            'rows_written': rows_written,
            'bytes_written': bytes_written,
            'metadata': metadata or {}
        }
        
        # Check if run exists
        existing = [r for r in registry.get('runs', []) if r['run_id'] == run_id]
        
        if existing:
            # Update existing
            for run in registry['runs']:
                if run['run_id'] == run_id:
                    run.update(run_entry)
        else:
            # Add new
            registry['runs'].append(run_entry)
        
        self._write_registry(registry)
    
    def update_etl_run(
        self,
        run_id: str,
        status: str = None,
        rows_written: int = None,
        bytes_written: int = None,
        end_time: datetime = None
    ) -> None:
        """Update an existing ETL run record"""
        registry = self._read_registry()
        
        for run in registry.get('runs', []):
            if run['run_id'] == run_id:
                if status:
                    run['status'] = status
                if rows_written is not None:
                    run['rows_written'] = rows_written
                if bytes_written is not None:
                    run['bytes_written'] = bytes_written
                if end_time:
                    run['ended_at'] = end_time.isoformat()
                break
        
        self._write_registry(registry)
    
    def get_active_locks(self) -> List[Dict[str, Any]]:
        """Get all active locks"""
        registry = self._read_registry()
        now = datetime.now(timezone.utc)
        
        active = []
        for lock in registry.get('locks', []):
            if lock['status'] == 'active':
                try:
                    expires = datetime.fromisoformat(lock['expires_at'])
                    if now < expires:
                        active.append(lock)
                except (KeyError, ValueError):
                    pass
        
        return active
    
    def get_etl_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific ETL run record"""
        registry = self._read_registry()
        
        for run in registry.get('runs', []):
            if run['run_id'] == run_id:
                return run
        
        return None
    
    def get_all_runs(self) -> List[Dict[str, Any]]:
        """Get all ETL runs"""
        registry = self._read_registry()
        return registry.get('runs', [])
    
    def cleanup_expired_locks(self, older_than_seconds: int = 3600) -> int:
        """
        Clean up expired lock entries from registry.
        
        Args:
            older_than_seconds: Remove lock entries older than this
        
        Returns:
            Number of locks cleaned up
        """
        registry = self._read_registry()
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=older_than_seconds)
        
        original_count = len(registry.get('locks', []))
        
        registry['locks'] = [
            lock for lock in registry.get('locks', [])
            if (lock.get('status') == 'active' or
                datetime.fromisoformat(lock.get('released_at', datetime.now(timezone.utc).isoformat())).replace(tzinfo=None) > cutoff.replace(tzinfo=None))
        ]
        
        cleaned = original_count - len(registry.get('locks', []))
        
        if cleaned > 0:
            self._write_registry(registry)
        
        return cleaned


class LockContext:
    """Context manager for acquiring and releasing locks"""
    
    def __init__(
        self,
        registry: RegistryLockManager,
        run_id: str,
        writer_id: str,
        timeout: int,
        metadata: Dict[str, Any]
    ):
        self.registry = registry
        self.run_id = run_id
        self.writer_id = writer_id
        self.timeout = timeout
        self.metadata = metadata
        self.lock_id = None
    
    def __enter__(self) -> 'LockContext':
        """Acquire lock on context entry"""
        start_time = time.time()
        retry_count = 0
        max_retries = 5
        retry_delay = 0.5  # Start with 0.5 second
        
        while retry_count < max_retries:
            self.lock_id = self.registry._try_acquire_lock(
                self.run_id,
                self.writer_id,
                self.timeout
            )
            
            if self.lock_id:
                print(f"✅ Lock acquired: {self.lock_id}")
                return self
            
            # Exponential backoff
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 5)  # Cap at 5 seconds
            retry_count += 1
        
        elapsed = time.time() - start_time
        raise TimeoutError(
            f"Failed to acquire lock after {elapsed:.1f}s. "
            f"Writer: {self.writer_id}, Run: {self.run_id}"
        )
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release lock on context exit"""
        if self.lock_id:
            success = exc_type is None
            error = f"{exc_type.__name__}: {exc_val}" if exc_type else None
            
            self.registry._release_lock(
                self.lock_id,
                success=success,
                error=error
            )
            
            status = "✅ Success" if success else "❌ Failed"
            print(f"{status} - Lock released: {self.lock_id}")


if __name__ == '__main__':
    # Example usage
    registry = RegistryLockManager(
        db_path='nyc_yellow_taxi.duckdb',
        registry_dir='data/registries'
    )
    
    # Simulate a multi-writer scenario
    print("\n=== Multi-Writer Registry Locking Example ===\n")
    
    run_id = "etl_run_001"
    
    # Writer 1: Acquire lock and do work
    print("Writer 1 attempting to acquire lock...")
    try:
        with registry.acquire_lock(run_id, 'worker_1', timeout=10):
            print("Writer 1: Performing ETL operations...")
            time.sleep(2)
            registry.update_etl_run(run_id, rows_written=1000000)
            print("Writer 1: ETL complete")
    except TimeoutError as e:
        print(f"Writer 1: {e}")
    
    # Writer 2: Try to acquire same lock (will wait/retry)
    print("\nWriter 2 attempting to acquire lock (will wait)...")
    try:
        with registry.acquire_lock(run_id, 'worker_2', timeout=10):
            print("Writer 2: Performing ETL operations...")
            time.sleep(1)
            print("Writer 2: ETL complete")
    except TimeoutError as e:
        print(f"Writer 2: {e}")
    
    # Show registry contents
    print("\n=== Registry Contents ===\n")
    print("Active Locks:", registry.get_active_locks())
    print("\nETL Runs:", registry.get_all_runs())
