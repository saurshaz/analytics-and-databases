# DuckDB ETL with Multi-Writer Components
from .registry_lock_manager import RegistryLockManager, LockContext, LockEntry, ETLRunEntry
from .duckdb_multiwriter_etl import DuckDBMultiWriterETL
from .query_optimizer import QueryOptimizer
from .unified_etl_pipeline import (
    UnifiedETLPipeline,
    DataRegistry,
    FileMetadata,
    ETLMetrics
)
from .utils import (
    normalize_column_name,
    normalize_columns,
    discover_column_name,
    extract_date_from_filename,
    ensure_directory_exists,
    safe_int,
    safe_float,
    safe_str,
    setup_logging,
    get_logger,
    calculate_throughput,
    format_number,
    format_duration
)
from .metrics import MetricsCollector, MetricsReporter
from .exceptions import (
    ETLError,
    DataValidationError,
    DataNotFoundError,
    LockAcquisitionError,
    LockReleaseError,
    RegistryError,
    DatabaseConnectionError,
    QueryExecutionError,
    PartitioningError,
    ConfigurationError,
    FileProcessingError
)

__all__ = [
    # Registry Locking
    'RegistryLockManager',
    'LockContext',
    'LockEntry',
    'ETLRunEntry',
    # ETL Operations & Data Models
    'DuckDBMultiWriterETL',
    'UnifiedETLPipeline',
    'DataRegistry',
    'FileMetadata',
    'ETLMetrics',
    # Query Optimization
    'QueryOptimizer',
    # Utilities
    'normalize_column_name',
    'normalize_columns',
    'discover_column_name',
    'extract_date_from_filename',
    'ensure_directory_exists',
    'safe_int',
    'safe_float',
    'safe_str',
    'setup_logging',
    'get_logger',
    'calculate_throughput',
    'format_number',
    'format_duration',
    # Metrics
    'MetricsCollector',
    'MetricsReporter',
    # Exceptions
    'ETLError',
    'DataValidationError',
    'DataNotFoundError',
    'LockAcquisitionError',
    'LockReleaseError',
    'RegistryError',
    'DatabaseConnectionError',
    'QueryExecutionError',
    'PartitioningError',
    'ConfigurationError',
    'FileProcessingError'
]
