"""Shared timing helpers for benchmark-style pytest tests."""

from __future__ import annotations

from statistics import fmean
from typing import Any, Callable


QueryRunner = Callable[[Any, str], tuple[Any, float, str, str]]


def benchmark_query(
    runner: QueryRunner,
    con: Any,
    sql: str,
    *,
    warmup_runs: int = 1,
    measured_runs: int = 2,
) -> dict[str, Any]:
    """Run a warmup pass and then average a fixed number of measured runs."""
    if not sql:
        return {
            "status": "SKIP",
            "rows": 0,
            "avg_time": 0.0,
            "warmup_runs": [],
            "measured_runs": [],
            "error": "No SQL",
        }

    warmup_attempts: list[dict[str, Any]] = []
    for _ in range(warmup_runs):
        df, elapsed, status, error = runner(con, sql)
        warmup_attempts.append(
            {
                "elapsed": elapsed,
                "status": status,
                "error": error,
                "rows": len(df) if df is not None else 0,
            }
        )

    measured_attempts: list[dict[str, Any]] = []
    for _ in range(measured_runs):
        df, elapsed, status, error = runner(con, sql)
        measured_attempts.append(
            {
                "elapsed": elapsed,
                "status": status,
                "error": error,
                "rows": len(df) if df is not None else 0,
            }
        )

    warmup_statuses = {attempt["status"] for attempt in warmup_attempts}
    measured_statuses = {attempt["status"] for attempt in measured_attempts}

    if warmup_statuses == {"SKIP"} and measured_statuses == {"SKIP"}:
        overall_status = "SKIP"
    elif warmup_statuses == {"PASS"} and measured_statuses == {"PASS"}:
        overall_status = "PASS"
    else:
        overall_status = "FAIL"

    avg_time = fmean(attempt["elapsed"] for attempt in measured_attempts) if measured_attempts else 0.0
    rows = measured_attempts[-1]["rows"] if measured_attempts else 0

    error = ""
    if overall_status == "FAIL":
        failing_attempt = next(
            (
                attempt
                for attempt in warmup_attempts + measured_attempts
                if attempt["status"] != "PASS"
            ),
            None,
        )
        if failing_attempt is not None:
            error = failing_attempt["error"]

    return {
        "status": overall_status,
        "rows": rows,
        "avg_time": avg_time,
        "warmup_runs": warmup_attempts,
        "measured_runs": measured_attempts,
        "error": error,
    }


def format_run_times(attempts: list[dict[str, Any]]) -> str:
    """Format run timings for compact display."""
    return ", ".join(f"{attempt['elapsed']:.3f}s" for attempt in attempts) if attempts else "N/A"


def summarize_results(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Summarize benchmark results across databases."""
    summary = {}
    for db_name, db_results in results.items():
        passing_times = [
            r["avg_time"]
            for r in db_results.values()
            if r["status"] == "PASS" and r["avg_time"] > 0
        ]
        if passing_times:
            summary[db_name] = {
                "total_time": sum(passing_times),
                "avg_time": fmean(passing_times),
                "min_time": min(passing_times),
                "max_time": max(passing_times),
                "queries_run": len(passing_times),
                "queries_failed": len([r for r in db_results.values() if r["status"] == "FAIL"]),
            }
    
    # Calculate speedup factors
    if len(summary) >= 2:
        db_names = list(summary.keys())
        if "duckdb" in summary and "postgres" in summary:
            speedup = summary["postgres"]["total_time"] / summary["duckdb"]["total_time"]
            summary["speedup"] = {
                "duckdb_vs_postgres": speedup,
                "factor": f"{speedup:.1f}x"
            }
    
    return summary
