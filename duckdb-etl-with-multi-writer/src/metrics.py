#!/usr/bin/env python3
"""
Metrics collection and reporting for ETL pipeline

Provides standardized metrics collection, formatting, and reporting
across all ETL operations.

Usage:
    metrics = MetricsCollector()
    metrics.record('load_year', rows=1000000, duration=120.5)
    metrics.report()
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import json
import logging
from pathlib import Path
from .utils import format_number, format_duration, calculate_throughput

logger = logging.getLogger(__name__)


@dataclass
class Metric:
    """Single metric entry"""
    name: str
    value: Any
    unit: str = ''
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricsSummary:
    """Summary of metrics for a specific operation"""
    operation: str
    start_time: str
    end_time: Optional[str] = None
    duration_sec: float = 0.0
    metrics: Dict[str, Any] = field(default_factory=dict)
    status: str = 'pending'  # 'pending', 'running', 'completed', 'failed'
    error: Optional[str] = None


class MetricsCollector:
    """
    Centralized metrics collection and reporting
    
    Features:
    - Standardized metric recording
    - Automatic throughput calculation
    - Formatted output (human-readable, JSON, CSV)
    - Metrics persistence
    - Summary statistics
    """
    
    def __init__(self, output_dir: str = 'data/metrics'):
        """
        Initialize metrics collector
        
        Args:
            output_dir: Directory for metrics output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._summaries: List[MetricsSummary] = []
        self._current_summary: Optional[MetricsSummary] = None
    
    def start_operation(self, operation: str) -> None:
        """
        Start tracking metrics for an operation
        
        Args:
            operation: Operation name (e.g., 'load_year', 'partition_year')
        """
        self._current_summary = MetricsSummary(
            operation=operation,
            start_time=datetime.now(timezone.utc).isoformat(),
            status='running'
        )
        self._summaries.append(self._current_summary)
    
    def record_metric(self, name: str, value: Any, unit: str = '', **metadata) -> None:
        """
        Record a single metric
        
        Args:
            name: Metric name
            value: Metric value
            unit: Unit of measurement
            **metadata: Additional metadata
        """
        if self._current_summary is None:
            raise RuntimeError("No operation in progress. Call start_operation() first.")
        
        metric = Metric(
            name=name,
            value=value,
            unit=unit,
            metadata=metadata
        )
        
        self._current_summary.metrics[name] = {
            'value': value,
            'unit': unit,
            'metadata': metadata
        }
    
    def record_row_count(self, count: int) -> None:
        """Record row count metric"""
        self.record_metric('rows', count, 'rows')
    
    def record_duration(self, duration_sec: float) -> None:
        """Record duration metric"""
        self.record_metric('duration', duration_sec, 'seconds')
    
    def record_throughput(self, rows: int, duration_sec: float) -> None:
        """
        Record throughput metric (rows per second)
        
        Args:
            rows: Number of rows processed
            duration_sec: Duration in seconds
        """
        throughput = calculate_throughput(rows, duration_sec)
        self.record_metric('throughput', throughput, 'rows/sec', rows=rows, duration_sec=duration_sec)
    
    def record_file_count(self, count: int) -> None:
        """Record file count metric"""
        self.record_metric('files', count, 'files')
    
    def record_bytes(self, bytes_written: int) -> None:
        """Record bytes written metric"""
        self.record_metric('bytes', bytes_written, 'bytes')
    
    def record_error(self, error: str) -> None:
        """Record error metric"""
        self.record_metric('error', error, 'error')
    
    def end_operation(self, status: str = 'completed', error: Optional[str] = None) -> None:
        """
        End tracking for current operation
        
        Args:
            status: Operation status ('completed', 'failed')
            error: Error message if failed
        """
        if self._current_summary is None:
            raise RuntimeError("No operation in progress.")
        
        self._current_summary.end_time = datetime.now(timezone.utc).isoformat()
        self._current_summary.status = status
        
        if error:
            self._current_summary.error = error
        
        # Calculate duration if both start and end times are available
        if self._current_summary.start_time and self._current_summary.end_time:
            try:
                start = datetime.fromisoformat(self._current_summary.start_time)
                end = datetime.fromisoformat(self._current_summary.end_time)
                self._current_summary.duration_sec = (end - start).total_seconds()
            except (ValueError, TypeError):
                pass
        
        self._current_summary = None
    
    def get_summary(self, operation: Optional[str] = None) -> Optional[MetricsSummary]:
        """
        Get summary for a specific operation
        
        Args:
            operation: Operation name (None for most recent)
        
        Returns:
            MetricsSummary or None
        """
        if operation:
            for summary in reversed(self._summaries):
                if summary.operation == operation:
                    return summary
            return None
        return self._current_summary or self._summaries[-1] if self._summaries else None
    
    def get_all_summaries(self) -> List[MetricsSummary]:
        """Get all operation summaries"""
        return self._summaries.copy()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get overall statistics across all operations
        
        Returns:
            Dictionary with statistics
        """
        if not self._summaries:
            return {'total_operations': 0}
        
        completed = [s for s in self._summaries if s.status == 'completed']
        failed = [s for s in self._summaries if s.status == 'failed']
        
        total_rows = sum(
            s.metrics.get('rows', {}).get('value', 0)
            for s in completed
        )
        
        total_duration = sum(
            s.duration_sec
            for s in completed
        )
        
        avg_throughput = total_rows / total_duration if total_duration > 0 else 0
        
        return {
            'total_operations': len(self._summaries),
            'completed': len(completed),
            'failed': len(failed),
            'total_rows': total_rows,
            'total_duration_sec': total_duration,
            'avg_throughput_rows_per_sec': avg_throughput
        }
    
    def report(self, verbose: bool = False) -> str:
        """
        Generate human-readable report
        
        Args:
            verbose: Include detailed metrics
        
        Returns:
            Formatted report string
        """
        if not self._summaries:
            return "No metrics collected yet."
        
        lines = []
        lines.append("=" * 70)
        lines.append("  ETL Metrics Report")
        lines.append("=" * 70)
        
        # Overall statistics
        stats = self.get_statistics()
        lines.append(f"\n📊 Overall Statistics:")
        lines.append(f"   Total operations: {stats['total_operations']}")
        lines.append(f"   Completed: {stats['completed']}")
        lines.append(f"   Failed: {stats['failed']}")
        lines.append(f"   Total rows processed: {format_number(stats['total_rows'])}")
        lines.append(f"   Total duration: {format_duration(stats['total_duration_sec'])}")
        lines.append(f"   Average throughput: {format_number(stats['avg_throughput_rows_per_sec'])} rows/sec")
        
        # Per-operation summaries
        lines.append(f"\n📋 Operation Summaries:")
        for summary in self._summaries:
            status_icon = "✅" if summary.status == 'completed' else "❌"
            duration_str = format_duration(summary.duration_sec)
            
            lines.append(f"\n   {status_icon} {summary.operation}")
            lines.append(f"      Duration: {duration_str}")
            
            if verbose and summary.metrics:
                lines.append(f"      Metrics:")
                for name, data in summary.metrics.items():
                    value = data['value']
                    unit = data['unit']
                    formatted = format_number(value) if isinstance(value, int) else f"{value:.2f}"
                    lines.append(f"        • {name}: {formatted} {unit}")
        
        lines.append("\n" + "=" * 70 + "\n")
        
        return "\n".join(lines)
    
    def save_json(self, filename: Optional[str] = None) -> Path:
        """
        Save metrics to JSON file
        
        Args:
            filename: Output filename (default: metrics_<timestamp>.json)
        
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            filename = f"metrics_{timestamp}.json"
        
        output_path = self.output_dir / filename
        
        data = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'statistics': self.get_statistics(),
            'summaries': [asdict(s) for s in self._summaries]
        }
        
        output_path.write_text(json.dumps(data, indent=2))
        logger.info(f"Metrics saved to {output_path}")
        
        return output_path
    
    def save_csv(self, filename: Optional[str] = None) -> Path:
        """
        Save metrics to CSV file
        
        Args:
            filename: Output filename (default: metrics_<timestamp>.csv)
        
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            filename = f"metrics_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        # Write CSV header
        with open(output_path, 'w') as f:
            f.write("operation,status,duration_sec,rows,throughput,bytes,files\n")
            
            for summary in self._summaries:
                status = summary.status
                duration = summary.duration_sec
                rows = summary.metrics.get('rows', {}).get('value', 0)
                throughput = summary.metrics.get('throughput', {}).get('value', 0)
                bytes_written = summary.metrics.get('bytes', {}).get('value', 0)
                files = summary.metrics.get('files', {}).get('value', 0)
                
                f.write(f"{summary.operation},{status},{duration},{rows},{throughput},{bytes_written},{files}\n")
        
        logger.info(f"Metrics saved to {output_path}")
        
        return output_path


class MetricsReporter:
    """
    Convenience class for reporting metrics in ETL operations
    
    Provides a simple interface for common ETL operations:
    - load_year
    - partition_year
    - query_execution
    """
    
    def __init__(self, collector: Optional[MetricsCollector] = None):
        """
        Initialize reporter
        
        Args:
            collector: MetricsCollector instance (creates new if None)
        """
        self.collector = collector or MetricsCollector()
    
    def load_year(
        self,
        year: int,
        rows: int,
        duration_sec: float,
        files_processed: int,
        writer_id: str
    ) -> None:
        """
        Record metrics for year load operation
        
        Args:
            year: Year loaded
            rows: Number of rows loaded
            duration_sec: Duration in seconds
            files_processed: Number of files processed
            writer_id: Writer identifier
        """
        self.collector.start_operation(f'load_year_{year}')
        self.collector.record_row_count(rows)
        self.collector.record_duration(duration_sec)
        self.collector.record_throughput(rows, duration_sec)
        self.collector.record_file_count(files_processed)
        self.collector.record_metric('writer_id', writer_id, 'id')
        self.collector.end_operation(status='completed')
    
    def partition_year(
        self,
        year: int,
        rows: int,
        duration_sec: float,
        files_processed: int,
        output_dir: str
    ) -> None:
        """
        Record metrics for year partitioning operation
        
        Args:
            year: Year partitioned
            rows: Number of rows processed
            duration_sec: Duration in seconds
            files_processed: Number of files processed
            output_dir: Output directory
        """
        self.collector.start_operation(f'partition_year_{year}')
        self.collector.record_row_count(rows)
        self.collector.record_duration(duration_sec)
        self.collector.record_throughput(rows, duration_sec)
        self.collector.record_file_count(files_processed)
        self.collector.record_metric('output_dir', output_dir, 'path')
        self.collector.end_operation(status='completed')
    
    def query_execution(
        self,
        query_name: str,
        duration_sec: float,
        rows_returned: int,
        query_type: str = 'select'
    ) -> None:
        """
        Record metrics for query execution
        
        Args:
            query_name: Query name
            duration_sec: Duration in seconds
            rows_returned: Number of rows returned
            query_type: Type of query ('select', 'insert', 'update', 'delete')
        """
        self.collector.start_operation(f'query_{query_name}')
        self.collector.record_duration(duration_sec)
        self.collector.record_row_count(rows_returned)
        self.collector.record_metric('query_type', query_type, 'type')
        self.collector.end_operation(status='completed')
    
    def report(self) -> str:
        """Generate and return report"""
        return self.collector.report()
    
    def save(self, format: str = 'json') -> Path:
        """
        Save metrics report
        
        Args:
            format: Output format ('json' or 'csv')
        
        Returns:
            Path to saved file
        """
        if format == 'json':
            return self.collector.save_json()
        elif format == 'csv':
            return self.collector.save_csv()
        else:
            raise ValueError(f"Unsupported format: {format}")