#!/usr/bin/env python3
"""
Quick Start: Registry Locking Multi-Writer Solution

Run this to see it in action:
  python scripts/demo_registry_locking.py

This demonstrates:
1. Single writer loading data
2. Multi-writer coordination
3. Registry audit trail
4. Lock monitoring
"""

import sys
from pathlib import Path
import time
import json
import threading

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL
from src.registry_lock_manager import RegistryLockManager


def print_header(text):
    """Print formatted header"""
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}\n")


def demo_single_writer():
    """Demo 1: Single writer loading data"""
    print_header("DEMO 1: Single Writer Loading Data")
    
    etl = DuckDBMultiWriterETL(
        db_path='nyc_yellow_taxi.duckdb',
        pipeline_id='demo_pipeline',
        timeout=300
    )
    
    print("📦 Loading 2023 NYC Yellow Taxi data with registry locking...")
    print("   (This may take a minute on first run)\n")
    
    try:
        stats = etl.load_parquet_safe(
            parquet_glob='NYC Yellow Taxi Record 23-24-25/2023/*.parquet',
            table_name='demo_2023_trips',
            run_id='demo_single_writer_001',
            writer_id='worker_1'
        )
        
        print(f"\n✅ SUCCESS!")
        print(f"   Rows loaded:     {stats['rows_loaded']:,}")
        print(f"   Bytes written:   {stats['bytes_written']:,}")
        print(f"   Duration:        {stats['duration_sec']:.2f}s")
        print(f"   Throughput:      {stats['rows_loaded']/stats['duration_sec']:,.0f} rows/sec")
        
        return stats
    
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
        return None
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


def demo_multi_writer():
    """Demo 2: Multi-writer coordination"""
    print_header("DEMO 2: Multi-Writer Coordination")
    
    registry = RegistryLockManager(
        db_path='nyc_yellow_taxi.duckdb',
        registry_dir='data/registries'
    )
    
    print("Starting 3 concurrent writers trying to access same lock...\n")
    
    results = []
    lock_events = []
    
    def writer_task(writer_id, duration):
        lock_events.append(f"[{writer_id}] Attempting to acquire lock...")
        
        try:
            with registry.acquire_lock(f'demo_run', writer_id, timeout=30):
                lock_events.append(f"[{writer_id}] ✅ ACQUIRED LOCK")
                lock_events.append(f"[{writer_id}] Doing work for {duration}s...")
                time.sleep(duration)
                lock_events.append(f"[{writer_id}] ✅ WORK COMPLETE")
                results.append({'writer_id': writer_id, 'status': 'success'})
        
        except TimeoutError as e:
            lock_events.append(f"[{writer_id}] ❌ TIMEOUT: {e}")
            results.append({'writer_id': writer_id, 'status': 'timeout'})
    
    # Start 3 threads
    threads = [
        threading.Thread(target=writer_task, args=('worker_2023', 1)),
        threading.Thread(target=writer_task, args=('worker_2024', 1)),
        threading.Thread(target=writer_task, args=('worker_2025', 1)),
    ]
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    
    # Print timeline
    print("Timeline of Events:")
    print("-" * 70)
    for event in lock_events:
        print(f"  {event}")
    
    # Results
    print(f"\n📊 Results:")
    success_count = sum(1 for r in results if r['status'] == 'success')
    print(f"  ✅ Succeeded: {success_count}/3")
    print(f"  ❌ Timeouts:  {len(results) - success_count}/3")
    print(f"\n💡 Note: Writers accessed sequentially (safe but serial)")
    print(f"   This ensures NO data loss or conflicts!")


def demo_registry_audit_trail():
    """Demo 3: Registry audit trail"""
    print_header("DEMO 3: Registry Audit Trail")
    
    etl = DuckDBMultiWriterETL(
        db_path='nyc_yellow_taxi.duckdb',
        pipeline_id='demo_pipeline'
    )
    
    # Get status
    status = etl.get_registry_status()
    
    print("📋 Registry Status Report\n")
    
    # Active locks
    active = status['active_locks']
    print(f"Active Locks: {len(active)}")
    if active:
        for lock in active:
            print(f"  • Lock ID: {lock['lock_id']}")
            print(f"    Writer: {lock['writer_id']}")
            print(f"    Acquired: {lock['acquired_at']}")
            print(f"    Expires: {lock['expires_at']}")
    else:
        print("  (none)")
    
    # Recent runs
    runs = status['all_runs']
    print(f"\nRecent Runs: {len(runs)}")
    
    if runs:
        print("\n  Last 5 runs:")
        for run in runs[-5:]:
            duration = 'N/A'
            if run.get('ended_at'):
                from datetime import datetime
                start = datetime.fromisoformat(run['started_at'])
                end = datetime.fromisoformat(run['ended_at'])
                duration = f"{(end - start).total_seconds():.2f}s"
            
            print(f"\n  • Run ID: {run['run_id']}")
            print(f"    Status: {run['status']}")
            print(f"    Writer: {run.get('writer_id', 'N/A')}")
            print(f"    Rows: {run['rows_written']:,}")
            print(f"    Duration: {duration}")


def demo_registry_json():
    """Demo 4: Show raw registry JSON"""
    print_header("DEMO 4: Raw Registry JSON (Audit Trail)")
    
    registry = RegistryLockManager(
        db_path='nyc_yellow_taxi.duckdb',
        registry_dir='data/registries'
    )
    
    registry_data = registry._read_registry()
    
    print("Registry structure:")
    print(json.dumps(registry_data, indent=2))
    
    print(f"\n💡 This JSON file serves as the complete audit trail")
    print(f"   Location: {registry.registry_file}")


def main():
    """Run all demos"""
    print("\n" + "=" * 70)
    print("  🚀 Registry Locking Multi-Writer Solution - Live Demo")
    print("=" * 70)
    
    print("\nSelect a demo to run:")
    print("  1. Single Writer Loading Data")
    print("  2. Multi-Writer Coordination")
    print("  3. Registry Audit Trail")
    print("  4. Raw Registry JSON")
    print("  5. Run ALL demos")
    print("  q. Quit")
    
    choice = input("\nYour choice: ").strip().lower()
    
    if choice == '1':
        demo_single_writer()
    elif choice == '2':
        demo_multi_writer()
    elif choice == '3':
        demo_registry_audit_trail()
    elif choice == '4':
        demo_registry_json()
    elif choice == '5':
        demo_single_writer()
        demo_multi_writer()
        demo_registry_audit_trail()
        demo_registry_json()
    elif choice == 'q':
        print("\nGoodbye!")
        return
    else:
        print("\nInvalid choice")
        return
    
    print("\n" + "=" * 70)
    print("  ✅ Demo Complete!")
    print("=" * 70 + "\n")


if __name__ == '__main__':
    main()
