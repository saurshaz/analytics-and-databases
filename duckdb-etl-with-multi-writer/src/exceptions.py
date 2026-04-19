#!/usr/bin/env python3
"""
Custom exceptions for ETL pipeline

Provides domain-specific exceptions for better error handling and debugging.

Usage:
    from src.exceptions import ETLError, LockAcquisitionError, DataValidationError

    try:
        pipeline.load_year(2023)
    except ETLError as e:
        logger.error(f"ETL failed: {e}")
    except LockAcquisitionError as e:
        logger.error(f"Lock conflict: {e}")
"""

from typing import Optional, Any, Dict


class ETLError(Exception):
    """
    Base exception for all ETL-related errors
    
    All custom ETL exceptions inherit from this base class.
    """
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize ETL error
        
        Args:
            message: Error message
            details: Additional error details
        """
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class DataValidationError(ETLError):
    """
    Raised when data validation fails
    
    Examples:
        - Invalid data format
        - Missing required columns
        - Data quality issues
    """
    def __init__(self, message: str, column: Optional[str] = None, row: Optional[int] = None):
        """
        Initialize data validation error
        
        Args:
            message: Error message
            column: Column name that failed validation
            row: Row number that failed validation
        """
        details = {}
        if column:
            details['column'] = column
        if row:
            details['row'] = row
        
        super().__init__(message, details)


class DataNotFoundError(ETLError):
    """
    Raised when required data is not found
    
    Examples:
        - Parquet files not found
        - Table doesn't exist
        - Registry file missing
    """
    def __init__(self, message: str, path: Optional[str] = None):
        """
        Initialize data not found error
        
        Args:
            message: Error message
            path: Path that was not found
        """
        details = {}
        if path:
            details['path'] = path
        
        super().__init__(message, details)


class LockAcquisitionError(ETLError):
    """
    Raised when lock acquisition fails
    
    Examples:
        - Lock timeout
        - Lock conflict with another writer
        - Registry file locked
    """
    def __init__(
        self,
        message: str,
        run_id: Optional[str] = None,
        writer_id: Optional[str] = None,
        timeout: Optional[float] = None
    ):
        """
        Initialize lock acquisition error
        
        Args:
            message: Error message
            run_id: ETL run identifier
            writer_id: Writer identifier
            timeout: Timeout value
        """
        details = {}
        if run_id:
            details['run_id'] = run_id
        if writer_id:
            details['writer_id'] = writer_id
        if timeout:
            details['timeout'] = timeout
        
        super().__init__(message, details)


class LockReleaseError(ETLError):
    """
    Raised when lock release fails
    
    Examples:
        - Registry write failure
        - Lock file corruption
    """
    def __init__(self, message: str, lock_id: Optional[str] = None):
        """
        Initialize lock release error
        
        Args:
            message: Error message
            lock_id: Lock identifier
        """
        details = {}
        if lock_id:
            details['lock_id'] = lock_id
        
        super().__init__(message, details)


class RegistryError(ETLError):
    """
    Raised when registry operations fail
    
    Examples:
        - Registry file corruption
        - Invalid JSON in registry
        - Registry permission denied
    """
    def __init__(self, message: str, registry_path: Optional[str] = None):
        """
        Initialize registry error
        
        Args:
            message: Error message
            registry_path: Registry file path
        """
        details = {}
        if registry_path:
            details['registry_path'] = registry_path
        
        super().__init__(message, details)


class DatabaseConnectionError(ETLError):
    """
    Raised when database connection fails
    
    Examples:
        - DuckDB file not found
        - Database locked
        - Connection timeout
    """
    def __init__(self, message: str, db_path: Optional[str] = None):
        """
        Initialize database connection error
        
        Args:
            message: Error message
            db_path: Database file path
        """
        details = {}
        if db_path:
            details['db_path'] = db_path
        
        super().__init__(message, details)


class QueryExecutionError(ETLError):
    """
    Raised when query execution fails
    
    Examples:
        - Invalid SQL syntax
        - Query timeout
        - Permission denied
    """
    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        query_type: Optional[str] = None
    ):
        """
        Initialize query execution error
        
        Args:
            message: Error message
            query: SQL query that failed
            query_type: Type of query
        """
        details = {}
        if query:
            details['query'] = query
        if query_type:
            details['query_type'] = query_type
        
        super().__init__(message, details)


class PartitioningError(ETLError):
    """
    Raised when partitioning operations fail
    
    Examples:
        - Invalid partition structure
        - File format error
        - Output directory permission denied
    """
    def __init__(self, message: str, partition_key: Optional[str] = None):
        """
        Initialize partitioning error
        
        Args:
            message: Error message
            partition_key: Partition key that failed
        """
        details = {}
        if partition_key:
            details['partition_key'] = partition_key
        
        super().__init__(message, details)


class ConfigurationError(ETLError):
    """
    Raised when configuration is invalid
    
    Examples:
        - Missing required configuration
        - Invalid configuration values
        - Unsupported configuration option
    """
    def __init__(self, message: str, config_key: Optional[str] = None):
        """
        Initialize configuration error
        
        Args:
            message: Error message
            config_key: Configuration key that is invalid
        """
        details = {}
        if config_key:
            details['config_key'] = config_key
        
        super().__init__(message, details)


class FileProcessingError(ETLError):
    """
    Raised when file processing fails
    
    Examples:
        - Parquet file corruption
        - File read/write error
        - Unsupported file format
    """
    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        file_type: Optional[str] = None
    ):
        """
        Initialize file processing error
        
        Args:
            message: Error message
            file_path: File path that failed
            file_type: Type of file
        """
        details = {}
        if file_path:
            details['file_path'] = file_path
        if file_type:
            details['file_type'] = file_type
        
        super().__init__(message, details)