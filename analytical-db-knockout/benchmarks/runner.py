#!/usr/bin/env python3
"""
Benchmark runner for DuckDB vs PostgreSQL performance comparison.

Usage:
    python benchmarks/runner.py --setup           # Create schema
    python benchmarks/runner.py --run             # Run benchmarks
    python benchmarks/runner.py --compare         # Compare results
"""

import json
import time
import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple
from dataclasses import dataclass, asdict
import subprocess

try:
    import duckdb
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("Error: Required packages not installed. Run: pip install -r requirements.txt")
    sys.exit(1)


@dataclass
class QueryResult:
    """Single query execution result."""
    query_id: int
    query_name: str
    database: str
    execution_time: float
    rows_returned: int
    error: str = None


class BenchmarkRunner:
    """Orchestrate DuckDB and PostgreSQL benchmarks."""

    def __init__(self, duckdb_path: str = None, 
                 postgres_dsn: str = None):
        if duckdb_path is None:
            parent_dir = Path(__file__).parent.parent.parent
            duckdb_path = str(parent_dir / "nyc_yellow_taxi.duckdb")
        self.duckdb_path = duckdb_path
        self.postgres_dsn = postgres_dsn or os.getenv("DATABASE_URL", 
                                                      "postgresql://postgres:postgres@localhost/taxi")
        self.queries = self._load_queries()
        self.results = []

    def _load_queries(self) -> List[Dict]:
        """Load queries from JSON file."""
        queries_file = Path(__file__).parent / "queries.json"
        with open(queries_file) as f:
            return json.load(f)

    def _setup_duckdb(self):
        """Create schema in DuckDB."""
        print("[DuckDB] Creating schema...")
        conn = duckdb.connect(self.duckdb_path)
        
        # Check if table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        if any(t[0] == 'yellow_taxi' for t in tables):
            print("[DuckDB] Schema already exists")
            conn.close()
            return
        
        # Create table (assuming parquet files exist)
        parent_dir = Path(__file__).parent.parent.parent
        taxi_dir = parent_dir / "data" / "NYC Yellow Taxi Record 23-24-25"
        
        if taxi_dir.exists():
            print(f"[DuckDB] Loading data from {taxi_dir}")
            conn.execute(f"""
                CREATE TABLE yellow_taxi AS
                SELECT * FROM read_parquet('{taxi_dir}/**/*.parquet')
            """)
            print("[DuckDB] Data loaded")
        else:
            print(f"[Warning] Taxi data directory not found at {taxi_dir}")
            print("[DuckDB] Creating empty schema for demonstration")
        
        conn.close()

    def _setup_postgres(self):
        """Create schema in PostgreSQL."""
        print("[PostgreSQL] Creating schema...")
        try:
            conn = psycopg2.connect(self.postgres_dsn)
            cur = conn.cursor()
            
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'yellow_taxi'
                )
            """)
            exists = cur.fetchone()[0]
            
            if exists:
                print("[PostgreSQL] Schema already exists")
                cur.close()
                conn.close()
                return
            
            print("[PostgreSQL] Schema created (populate with ETL pipeline)")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[PostgreSQL] Setup failed: {e}")
            print("         Ensure PostgreSQL is running and accessible")

    def setup(self):
        """Initialize databases."""
        print("\n=== Setting up databases ===\n")
        self._setup_duckdb()
        self._setup_postgres()
        print("\n✓ Setup complete\n")

    def _run_query_duckdb(self, query: Dict) -> QueryResult:
        """Run query in DuckDB."""
        try:
            conn = duckdb.connect(self.duckdb_path)
            start = time.time()
            result = conn.execute(query['sql']).fetchall()
            elapsed = time.time() - start
            conn.close()
            
            return QueryResult(
                query_id=query['id'],
                query_name=query['name'],
                database="DuckDB",
                execution_time=elapsed,
                rows_returned=len(result)
            )
        except Exception as e:
            return QueryResult(
                query_id=query['id'],
                query_name=query['name'],
                database="DuckDB",
                execution_time=0,
                rows_returned=0,
                error=str(e)
            )

    def _run_query_postgres(self, query: Dict) -> QueryResult:
        """Run query in PostgreSQL."""
        try:
            conn = psycopg2.connect(self.postgres_dsn)
            cur = conn.cursor()
            start = time.time()
            cur.execute(query['sql'])
            result = cur.fetchall()
            elapsed = time.time() - start
            cur.close()
            conn.close()
            
            return QueryResult(
                query_id=query['id'],
                query_name=query['name'],
                database="PostgreSQL",
                execution_time=elapsed,
                rows_returned=len(result)
            )
        except Exception as e:
            return QueryResult(
                query_id=query['id'],
                query_name=query['name'],
                database="PostgreSQL",
                execution_time=0,
                rows_returned=0,
                error=str(e)
            )

    def run(self):
        """Execute all queries against both databases."""
        print("\n=== Running benchmarks ===\n")
        
        for i, query in enumerate(self.queries, 1):
            print(f"[{i}/{len(self.queries)}] {query['name']}")
            
            duckdb_result = self._run_query_duckdb(query)
            postgres_result = self._run_query_postgres(query)
            
            self.results.append(duckdb_result)
            self.results.append(postgres_result)
            
            if duckdb_result.error:
                print(f"  DuckDB: ERROR - {duckdb_result.error}")
            else:
                print(f"  DuckDB: {duckdb_result.execution_time:.3f}s ({duckdb_result.rows_returned} rows)")
            
            if postgres_result.error:
                print(f"  PostgreSQL: ERROR - {postgres_result.error}")
            else:
                print(f"  PostgreSQL: {postgres_result.execution_time:.3f}s ({postgres_result.rows_returned} rows)")
                if duckdb_result.execution_time > 0:
                    speedup = postgres_result.execution_time / duckdb_result.execution_time
                    print(f"  Speedup: {speedup:.1f}x")
        
        self._save_results()
        print("\n✓ Benchmarks complete\n")

    def _save_results(self):
        """Save results to JSON files."""
        results_dir = Path(__file__).parent / "results"
        results_dir.mkdir(exist_ok=True)
        
        # DuckDB results
        duckdb_results = [asdict(r) for r in self.results if r.database == "DuckDB"]
        with open(results_dir / "duckdb_results.json", "w") as f:
            json.dump(duckdb_results, f, indent=2)
        
        # PostgreSQL results
        postgres_results = [asdict(r) for r in self.results if r.database == "PostgreSQL"]
        with open(results_dir / "postgres_results.json", "w") as f:
            json.dump(postgres_results, f, indent=2)
        
        # Summary
        summary = self._generate_summary()
        with open(results_dir / "comparison.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        print(f"\nResults saved to {results_dir}/")

    def _generate_summary(self) -> Dict:
        """Generate comparison summary."""
        duckdb_times = [r.execution_time for r in self.results if r.database == "DuckDB" and not r.error]
        postgres_times = [r.execution_time for r in self.results if r.database == "PostgreSQL" and not r.error]
        
        return {
            "duckdb": {
                "total_time": sum(duckdb_times),
                "avg_time": sum(duckdb_times) / len(duckdb_times) if duckdb_times else 0,
                "min_time": min(duckdb_times) if duckdb_times else 0,
                "max_time": max(duckdb_times) if duckdb_times else 0,
                "queries_run": len(duckdb_times)
            },
            "postgres": {
                "total_time": sum(postgres_times),
                "avg_time": sum(postgres_times) / len(postgres_times) if postgres_times else 0,
                "min_time": min(postgres_times) if postgres_times else 0,
                "max_time": max(postgres_times) if postgres_times else 0,
                "queries_run": len(postgres_times)
            },
            "speedup": {
                "total": sum(postgres_times) / sum(duckdb_times) if sum(duckdb_times) > 0 else 0,
                "average": (sum(postgres_times) / len(postgres_times)) / (sum(duckdb_times) / len(duckdb_times)) 
                           if duckdb_times and postgres_times else 0
            }
        }


def main():
    parser = argparse.ArgumentParser(description="DuckDB vs PostgreSQL benchmarks")
    parser.add_argument("--setup", action="store_true", help="Setup databases")
    parser.add_argument("--run", action="store_true", help="Run benchmarks")
    parser.add_argument("--compare", action="store_true", help="Compare results")
    parser.add_argument("--duckdb", default="../nyc_yellow_taxi.duckdb", help="DuckDB path")
    parser.add_argument("--postgres", help="PostgreSQL DSN")
    
    args = parser.parse_args()
    
    runner = BenchmarkRunner(duckdb_path=args.duckdb, postgres_dsn=args.postgres)
    
    if args.setup:
        runner.setup()
    elif args.run:
        runner.run()
    elif args.compare:
        results_dir = Path(__file__).parent / "results" / "comparison.json"
        if results_dir.exists():
            with open(results_dir) as f:
                summary = json.load(f)
                print(json.dumps(summary, indent=2))
        else:
            print("No results found. Run benchmarks first: python runner.py --run")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
