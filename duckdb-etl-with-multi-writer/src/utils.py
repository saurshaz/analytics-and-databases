#!/usr/bin/env python3
"""
Shared utilities for ETL pipeline

Provides common utilities used across multiple modules:
- Column name normalization
- Data type conversion
- File path helpers
- Logging configuration
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# COLUMN NORMALIZATION
# ============================================================================

COLUMN_MAPPING = {
    'tpep_pickup_datetime': 'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'TPEP_PICKUP_DATETIME': 'pickup_datetime',
    'TPEP_DROPOFF_DATETIME': 'dropoff_datetime',
    'trip_distance': 'trip_distance',
    'passenger_count': 'passenger_count',
    'fare_amount': 'fare_amount',
    'extra': 'extra',
    'mta_tax': 'mta_tax',
    'tip_amount': 'tip_amount',
    'tolls_amount': 'tolls_amount',
    'total_amount': 'total_amount',
    'payment_type': 'payment_type',
    'trip_type': 'trip_type',
    'airport_fee': 'airport_fee',
    'cbd_congestion_surcharge': 'cbd_congestion_surcharge',
    'pulocationid': 'pulocationid',
    'dolocationid': 'dolocationid',
    'vendorid': 'vendorid',
}


def normalize_column_name(column: str) -> str:
    """
    Normalize a column name to standard format
    
    Handles variations like:
    - tpep_pickup_datetime vs TPEP_PICKUP_DATETIME
    - payment_type vs payment_methods
    - airport_fee (2024+)
    - cbd_congestion_surcharge (2025+)
    
    Args:
        column: Original column name
    
    Returns:
        Normalized column name
    """
    # Try exact match (case-insensitive)
    column_lower = column.lower()
    if column_lower in COLUMN_MAPPING:
        return COLUMN_MAPPING[column_lower]
    
    # Try with tpep_ prefix variations
    for original, normalized in COLUMN_MAPPING.items():
        if column_lower.replace('tpep_', '').replace('_', '') == \
           original.replace('tpep_', '').replace('_', ''):
            return normalized
    
    # Fallback: lowercase and replace spaces
    return column.lower().replace(' ', '_')


def normalize_columns(df: Any) -> Any:
    """
    Normalize all column names in a DataFrame
    
    Args:
        df: DataFrame with columns to normalize
    
    Returns:
        DataFrame with normalized column names
    """
    if hasattr(df, 'columns'):
        # Pandas DataFrame
        new_columns = {}
        for col in df.columns:
            new_columns[col] = normalize_column_name(col)
        return df.rename(columns=new_columns)
    else:
        # DuckDB relation
        # Return as-is for now, normalization can be applied during query
        return df


def discover_column_name(pattern: str, available_columns: set) -> Optional[str]:
    """
    Auto-discover column name handling variations
    
    Args:
        pattern: Expected column name pattern
        available_columns: Set of available column names
    
    Returns:
        Discovered column name or None
    """
    # Try exact match (case-insensitive)
    pattern_lower = pattern.lower()
    if pattern_lower in available_columns:
        return pattern_lower
    
    # Try with tpep_ prefix variations
    for col in available_columns:
        if pattern_lower.replace('tpep_', '').replace('_', '') == \
           col.replace('tpep_', '').replace('_', ''):
            return col
    
    return None


# ============================================================================
# FILE PATH HELPERS
# ============================================================================

def extract_date_from_filename(filename: str) -> Optional[tuple]:
    """
    Extract year and month from parquet filename
    
    Expected format: yellow_tripdata_YYYY-MM.parquet
    
    Args:
        filename: Parquet filename
    
    Returns:
        Tuple of (year, month) or None if not found
    """
    match = re.search(r'(\d{4})-(\d{2})', filename)
    if match:
        return match.groups()
    return None


def ensure_directory_exists(path: str) -> Path:
    """
    Ensure directory exists, create if needed
    
    Args:
        path: Directory path
    
    Returns:
        Path object for the directory
    """
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


# ============================================================================
# DATA TYPE HELPERS
# ============================================================================

def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_str(value: Any, default: str = '') -> str:
    """Safely convert value to string"""
    try:
        return str(value)
    except (ValueError, TypeError):
        return default


# ============================================================================
# LOGGING HELPERS
# ============================================================================

def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> None:
    """
    Configure logging for ETL pipeline
    
    Args:
        level: Logging level (default: INFO)
        format_string: Custom format string (default: standard)
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('etl_pipeline.log')
        ]
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name
    
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# ============================================================================
# METRIC HELPERS
# ============================================================================

def calculate_throughput(rows: int, duration_sec: float) -> float:
    """
    Calculate rows per second throughput
    
    Args:
        rows: Number of rows processed
        duration_sec: Duration in seconds
    
    Returns:
        Throughput in rows per second
    """
    if duration_sec <= 0:
        return 0.0
    return rows / duration_sec


def format_number(num: int, precision: int = 0) -> str:
    """
    Format number with commas
    
    Args:
        num: Number to format
        precision: Decimal precision (default: 0)
    
    Returns:
        Formatted string
    """
    if precision == 0:
        return f"{num:,}"
    else:
        return f"{num:,.{precision}f}"


def format_duration(seconds: float, precision: int = 2) -> str:
    """
    Format duration in human-readable format
    
    Args:
        seconds: Duration in seconds
        precision: Decimal precision (default: 2)
    
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.{precision}}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.{precision}}m"
    else:
        hours = seconds / 3600
        return f"{hours:.{precision}}h"