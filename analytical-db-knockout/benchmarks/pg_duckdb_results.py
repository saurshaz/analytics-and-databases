"""
Results processing and reporting for pg_duckdb performance comparison.

Analyzes benchmark results and generates comprehensive reports.
"""

import json
from pathlib import Path
from typing import Dict, List


def load_results(results_file: str = None) -> Dict:
    """Load pg_duckdb benchmark results from JSON file."""
    if results_file is None:
        results_file = Path(__file__).parent / "results" / "pg_duckdb_comparison.json"
    else:
        results_file = Path(results_file)
    
    if not results_file.exists():
        raise FileNotFoundError(f"Results file not found: {results_file}")
    
    with open(results_file, "r") as f:
        return json.load(f)


def calculate_statistics(values: List[float]) -> Dict:
    """Calculate statistics for a list of values."""
    if not values:
        return {
            "count": 0,
            "min": 0,
            "max": 0,
            "avg": 0,
            "median": 0
        }
    
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / len(values),
        "median": sorted(values)[len(values) // 2]
    }


def generate_comparison_table(results: Dict) -> str:
    """Generate a formatted comparison table."""
    queries = results.get("queries", [])
    summary = results.get("summary", {})
    
    if not queries:
        return "No queries found in results"
    
    # Build table
    table = []
    table.append("| Query | Native PG | PG+pg_duckdb | Direct DuckDB | Speedup (vs Native) | Speedup (vs Direct) |")
    table.append("|-------|-----------|--------------|---------------|---------------------|---------------------|")
    
    for query in queries:
        query_id = query["id"]
        query_name = query.get("title", f"Query {query_id}")
        
        native_data = summary["native_postgres"].get(query_id, {})
        pg_duckdb_data = summary["pg_duckdb"].get(query_id, {})
        duckdb_data = summary["direct_duckdb"].get(query_id, {})
        
        native_time = native_data.get("avg_time", 0)
        pg_duckdb_time = pg_duckdb_data.get("avg_time", 0)
        duckdb_time = duckdb_data.get("avg_time", 0)
        
        speedup_vs_native = summary["speedup_vs_native"].get(query_id, {}).get("speedup")
        speedup_vs_direct = summary["speedup_vs_direct"].get(query_id, {}).get("speedup")
        
        native_str = f"{native_time:.3f}s" if native_time > 0 else "N/A"
        pg_duckdb_str = f"{pg_duckdb_time:.3f}s" if pg_duckdb_time > 0 else "N/A"
        duckdb_str = f"{duckdb_time:.3f}s" if duckdb_time > 0 else "N/A"
        
        speedup_native_str = f"{speedup_vs_native:.2f}x" if speedup_vs_native else "N/A"
        speedup_direct_str = f"{speedup_vs_direct:.2f}x" if speedup_vs_direct else "N/A"
        
        table.append(f"| {query_id}: {query_name} | {native_str} | {pg_duckdb_str} | {duckdb_str} | {speedup_native_str} | {speedup_direct_str} |")
    
    return "\n".join(table)


def generate_summary_report(results: Dict) -> str:
    """Generate a comprehensive summary report."""
    queries = results.get("queries", [])
    summary = results.get("summary", {})
    
    if not queries:
        return "No queries found in results"
    
    report = []
    report.append("="*80)
    report.append("PG_DUCKDB PERFORMANCE COMPARISON REPORT")
    report.append("="*80)
    report.append(f"\nGenerated: {results.get('timestamp', 'N/A')}")
    report.append(f"Queries Tested: {len(queries)}")
    report.append(f"Backends Compared: 3 (Native PostgreSQL, PostgreSQL+pg_duckdb, Direct DuckDB)")
    
    # Calculate statistics for each backend
    native_times = []
    pg_duckdb_times = []
    duckdb_times = []
    
    for query in queries:
        query_id = query["id"]
        
        native_time = summary["native_postgres"].get(query_id, {}).get("avg_time", 0)
        pg_duckdb_time = summary["pg_duckdb"].get(query_id, {}).get("avg_time", 0)
        duckdb_time = summary["direct_duckdb"].get(query_id, {}).get("avg_time", 0)
        
        if native_time > 0:
            native_times.append(native_time)
        if pg_duckdb_time > 0:
            pg_duckdb_times.append(pg_duckdb_time)
        if duckdb_time > 0:
            duckdb_times.append(duckdb_time)
    
    # Backend statistics
    report.append("\n" + "-"*80)
    report.append("BACKEND STATISTICS")
    report.append("-"*80)
    
    if native_times:
        native_stats = calculate_statistics(native_times)
        report.append(f"\nNative PostgreSQL:")
        report.append(f"  Average Time: {native_stats['avg']:.3f}s")
        report.append(f"  Min Time: {native_stats['min']:.3f}s")
        report.append(f"  Max Time: {native_stats['max']:.3f}s")
        report.append(f"  Median Time: {native_stats['median']:.3f}s")
        report.append(f"  Total Time: {sum(native_times):.3f}s")
    
    if pg_duckdb_times:
        pg_duckdb_stats = calculate_statistics(pg_duckdb_times)
        report.append(f"\nPostgreSQL + pg_duckdb:")
        report.append(f"  Average Time: {pg_duckdb_stats['avg']:.3f}s")
        report.append(f"  Min Time: {pg_duckdb_stats['min']:.3f}s")
        report.append(f"  Max Time: {pg_duckdb_stats['max']:.3f}s")
        report.append(f"  Median Time: {pg_duckdb_stats['median']:.3f}s")
        report.append(f"  Total Time: {sum(pg_duckdb_times):.3f}s")
    
    if duckdb_times:
        duckdb_stats = calculate_statistics(duckdb_times)
        report.append(f"\nDirect DuckDB:")
        report.append(f"  Average Time: {duckdb_stats['avg']:.3f}s")
        report.append(f"  Min Time: {duckdb_stats['min']:.3f}s")
        report.append(f"  Max Time: {duckdb_stats['max']:.3f}s")
        report.append(f"  Median Time: {duckdb_stats['median']:.3f}s")
        report.append(f"  Total Time: {sum(duckdb_times):.3f}s")
    
    # Speedup statistics
    report.append("\n" + "-"*80)
    report.append("SPEEDUP STATISTICS")
    report.append("-"*80)
    
    if native_times and pg_duckdb_times:
        speedup_vs_native = [
            native_times[i] / pg_duckdb_times[i]
            for i in range(len(native_times))
        ]
        speedup_stats = calculate_statistics(speedup_vs_native)
        report.append(f"\npg_duckdb vs Native PostgreSQL:")
        report.append(f"  Average Speedup: {speedup_stats['avg']:.2f}x")
        report.append(f"  Min Speedup: {speedup_stats['min']:.2f}x")
        report.append(f"  Max Speedup: {speedup_stats['max']:.2f}x")
        report.append(f"  Median Speedup: {speedup_stats['median']:.2f}x")
    
    if pg_duckdb_times and duckdb_times:
        speedup_vs_direct = [
            pg_duckdb_times[i] / duckdb_times[i]
            for i in range(len(pg_duckdb_times))
        ]
        speedup_stats = calculate_statistics(speedup_vs_direct)
        report.append(f"\npg_duckdb vs Direct DuckDB:")
        report.append(f"  Average Speedup: {speedup_stats['avg']:.2f}x")
        report.append(f"  Min Speedup: {speedup_stats['min']:.2f}x")
        report.append(f"  Max Speedup: {speedup_stats['max']:.2f}x")
        report.append(f"  Median Speedup: {speedup_stats['median']:.2f}x")
    
    # Overall comparison
    report.append("\n" + "-"*80)
    report.append("OVERALL COMPARISON")
    report.append("-"*80)
    
    if native_times and pg_duckdb_times:
        total_native = sum(native_times)
        total_pg_duckdb = sum(pg_duckdb_times)
        overall_speedup = total_native / total_pg_duckdb
        report.append(f"\nTotal Execution Time (3 queries):")
        report.append(f"  Native PostgreSQL: {total_native:.3f}s")
        report.append(f"  PostgreSQL + pg_duckdb: {total_pg_duckdb:.3f}s")
        report.append(f"  Overall Speedup: {overall_speedup:.2f}x")
    
    if pg_duckdb_times and duckdb_times:
        total_pg_duckdb = sum(pg_duckdb_times)
        total_duckdb = sum(duckdb_times)
        overall_overhead = total_pg_duckdb / total_duckdb
        report.append(f"\nTotal Execution Time (3 queries):")
        report.append(f"  PostgreSQL + pg_duckdb: {total_pg_duckdb:.3f}s")
        report.append(f"  Direct DuckDB: {total_duckdb:.3f}s")
        report.append(f"  Overall Overhead: {overall_overhead:.2f}x")
    
    # Key findings
    report.append("\n" + "-"*80)
    report.append("KEY FINDINGS")
    report.append("-"*80)
    
    if native_times and pg_duckdb_times:
        avg_speedup = speedup_stats['avg'] if 'speedup_vs_native' in locals() else 0
        if avg_speedup >= 5:
            report.append(f"\n✓ pg_duckdb provides {avg_speedup:.1f}x average speedup over native PostgreSQL")
            report.append("  This is a significant performance improvement for analytical queries.")
        elif avg_speedup >= 2:
            report.append(f"\n✓ pg_duckdb provides {avg_speedup:.1f}x average speedup over native PostgreSQL")
            report.append("  This is a moderate performance improvement.")
        else:
            report.append(f"\n⚠ pg_duckdb provides {avg_speedup:.1f}x average speedup over native PostgreSQL")
            report.append("  Performance improvement is modest.")
    
    if pg_duckdb_times and duckdb_times:
        avg_overhead = speedup_stats['avg'] if 'speedup_vs_direct' in locals() else 0
        if avg_overhead >= 2:
            report.append(f"\n⚠ pg_duckdb has {avg_overhead:.1f}x average overhead compared to direct DuckDB")
            report.append("  This is expected due to extension overhead.")
        elif avg_overhead >= 1.5:
            report.append(f"\n✓ pg_duckdb has {avg_overhead:.1f}x average overhead compared to direct DuckDB")
            report.append("  This is reasonable overhead for the convenience of using PostgreSQL features.")
        else:
            report.append(f"\n✓ pg_duckdb has {avg_overhead:.1f}x average overhead compared to direct DuckDB")
            report.append("  This is excellent overhead, showing minimal performance penalty.")
    
    report.append("\n" + "="*80)
    
    return "\n".join(report)


def main():
    """Main function to generate and display pg_duckdb comparison report."""
    try:
        # Load results
        results = load_results()
        
        # Generate comparison table
        table = generate_comparison_table(results)
        print("\n" + "="*80)
        print("PERFORMANCE COMPARISON TABLE")
        print("="*80)
        print(table)
        
        # Generate summary report
        report = generate_summary_report(results)
        print(report)
        
        # Save report to file
        report_file = Path(__file__).parent / "results" / "pg_duckdb_report.txt"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_file, "w") as f:
            f.write(report)
        
        print(f"\n✓ Report saved to: {report_file}")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease run the pg_duckdb performance test first:")
        print("  pytest benchmarks/test_pg_duckdb_performance.py -v")
        return 1
    except Exception as e:
        print(f"\n❌ Error generating report: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())