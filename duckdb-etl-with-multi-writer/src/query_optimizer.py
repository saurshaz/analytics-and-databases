#!/usr/bin/env python3
"""
Query Optimizer with Partition Pruning and Column Discovery

Provides optimized query execution on partitioned data with:
- Automatic partition pruning (year/month/day)
- Column name discovery and normalization
- Predicate pushdown
- Query planning analysis

Usage:
    optimizer = QueryOptimizer(db_path='nyc_yellow_taxi.duckdb')
    result = optimizer.query_date_range('2024-01-01', '2024-12-31', ['trip_distance', 'fare_amount'])
    print(result)
"""

import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryOptimizer:
    """
    Optimizes DuckDB queries with partition pruning and column discovery
    """
    
    def __init__(self, db_path: str = 'nyc_yellow_taxi.duckdb'):
        """
        Initialize query optimizer
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self._col_cache = {}
    
    def _discover_column_name(
        self,
        desired_name: str,
        available_cols: List[str]
    ) -> Optional[str]:
        """
        Find actual column name matching desired name
        
        Handles common variations:
        - tpep_pickup_datetime -> pickup_datetime
        - Direct matches
        - Partial matches by suffix
        """
        if desired_name in available_cols:
            return desired_name
        
        # Try tpep_ prefix
        if f"tpep_{desired_name}" in available_cols:
            return f"tpep_{desired_name}"
        
        # Try suffix matching
        for col in available_cols:
            if col.endswith(desired_name) or col.endswith(f"_{desired_name}"):
                return col
        
        # Try case-insensitive
        desired_lower = desired_name.lower()
        for col in available_cols:
            if col.lower() == desired_lower:
                return col
        
        return None
    
    def get_table_schema(self, table_name: str = 'yellow_taxi_trips') -> Dict[str, str]:
        """
        Get schema for a table
        
        Returns:
            Dict mapping column names to types
        """
        try:
            result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            schema = {}
            for col_name, col_type, *_ in result:
                schema[col_name] = col_type
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return {}
    
    def get_available_columns(self, table_name: str = 'yellow_taxi_trips') -> List[str]:
        """Get list of available columns from table"""
        try:
            schema = self.get_table_schema(table_name)
            return list(schema.keys())
        except Exception as e:
            logger.warning(f"Could not get columns: {e}")
            return []
    
    def query_date_range(
        self,
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None,
        table_name: str = 'yellow_taxi_trips'
    ) -> Any:
        """
        Query with date range using partition pruning
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            columns: Specific columns to select (None = all)
            table_name: Target table name
        
        Returns:
            Query results as DataFrame
        """
        available_cols = self.get_available_columns(table_name)
        
        # Discover pickup datetime column
        pickup_col = self._discover_column_name('tpep_pickup_datetime', available_cols)
        if not pickup_col:
            pickup_col = self._discover_column_name('pickup_datetime', available_cols)
        
        if not pickup_col:
            raise ValueError(
                f"Could not find pickup datetime column. Available: {available_cols}"
            )
        
        # Build column list with predicate pushdown
        col_select = ", ".join(columns) if columns else "*"
        
        query = f"""
            SELECT {col_select}
            FROM {table_name}
            WHERE {pickup_col} >= '{start_date}'
              AND {pickup_col} < timestamp '{end_date}' + INTERVAL 1 DAY
            ORDER BY {pickup_col}
        """
        
        logger.info(f"📊 Query: Date range {start_date} to {end_date}")
        logger.info(f"   Columns: {col_select if col_select != '*' else 'all'}")
        
        start_time = datetime.now()
        try:
            result = self.conn.execute(query).fetchall()
            elapsed = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Query returned {len(result):,} rows in {elapsed:.3f}s")
            return self.conn.execute(query).df()
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def daily_summary(
        self,
        table_name: str = 'yellow_taxi_trips',
        days: int = 30
    ) -> Any:
        """
        Generate daily summary metrics (last N days)
        
        Args:
            table_name: Target table
            days: Number of days to summarize
        
        Returns:
            DataFrame with daily aggregates
        """
        available_cols = self.get_available_columns(table_name)
        
        # Discover columns
        pickup_col = self._discover_column_name('tpep_pickup_datetime', available_cols) or \
                     self._discover_column_name('pickup_datetime', available_cols)
        trip_dist_col = self._discover_column_name('trip_distance', available_cols)
        fare_col = self._discover_column_name('fare_amount', available_cols)
        total_col = self._discover_column_name('total_amount', available_cols)
        payment_col = self._discover_column_name('payment_type', available_cols)
        passenger_col = self._discover_column_name('passenger_count', available_cols)
        
        if not all([pickup_col, trip_dist_col, fare_col, total_col]):
            missing = []
            if not pickup_col: missing.append('pickup_datetime')
            if not trip_dist_col: missing.append('trip_distance')
            if not fare_col: missing.append('fare_amount')
            if not total_col: missing.append('total_amount')
            raise ValueError(f"Missing columns: {missing}")
        
        # Build optionally included columns
        optional_cols = []
        if payment_col:
            optional_cols.append(f"COUNT(*) FILTER (WHERE {payment_col} = 1) as credit_card_trips")
            optional_cols.append(f"COUNT(*) FILTER (WHERE {payment_col} = 2) as cash_trips")
        if passenger_col:
            optional_cols.append(f"AVG({passenger_col}) as avg_passengers")
        
        optional_select = ",\n                " + (",\n                ".join(optional_cols)) if optional_cols else ""
        
        query = f"""
            SELECT 
                CAST({pickup_col} AS DATE) as trip_date,
                COUNT(*) as total_trips,
                AVG({trip_dist_col}) as avg_distance,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {trip_dist_col}) as median_distance,
                AVG({fare_col}) as avg_fare,
                SUM({total_col}) as daily_revenue,
                MIN({total_col}) as min_fare,
                MAX({total_col}) as max_fare{optional_select}
            FROM {table_name}
            WHERE {pickup_col} >= CURRENT_DATE - INTERVAL {days} DAY
            GROUP BY CAST({pickup_col} AS DATE)
            ORDER BY trip_date DESC
        """
        
        logger.info(f"📈 Daily summary: Last {days} days")
        
        start_time = datetime.now()
        try:
            result = self.conn.execute(query).df()
            elapsed = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Summary computed: {len(result)} days in {elapsed:.3f}s")
            return result
        except Exception as e:
            logger.error(f"Summary query failed: {e}")
            raise
    
    def vendor_performance(
        self,
        table_name: str = 'yellow_taxi_trips'
    ) -> Any:
        """
        Analyze performance by vendor
        
        Returns:
            DataFrame with vendor metrics
        """
        available_cols = self.get_available_columns(table_name)
        
        # Discover columns
        vendor_col = 'VendorID'
        trip_dist_col = self._discover_column_name('trip_distance', available_cols)
        fare_col = self._discover_column_name('fare_amount', available_cols)
        total_col = self._discover_column_name('total_amount', available_cols)
        passenger_col = self._discover_column_name('passenger_count', available_cols)
        pickup_col = self._discover_column_name('tpep_pickup_datetime', available_cols) or \
                     self._discover_column_name('pickup_datetime', available_cols)
        
        if not all([trip_dist_col, fare_col, total_col]):
            raise ValueError("Missing required columns for vendor analysis")
        
        optional_select = ""
        if passenger_col:
            optional_select += f",\n                AVG({passenger_col}) as avg_passengers"
        
        query = f"""
            SELECT 
                {vendor_col} as vendor_id,
                COUNT(*) as total_trips,
                ROUND(AVG({trip_dist_col}), 2) as avg_distance,
                ROUND(AVG({fare_col}), 2) as avg_fare,
                ROUND(SUM({total_col}) / COUNT(*), 2) as avg_total,
                ROUND(SUM({total_col}), 2) as total_revenue{optional_select}
            FROM {table_name}
            GROUP BY {vendor_col}
            ORDER BY total_revenue DESC
        """
        
        logger.info(f"📊 Vendor performance analysis")
        
        start_time = datetime.now()
        try:
            result = self.conn.execute(query).df()
            elapsed = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Analysis complete: {len(result)} vendors in {elapsed:.3f}s")
            return result
        except Exception as e:
            logger.error(f"Vendor analysis failed: {e}")
            raise
    
    def peek_data(
        self,
        table_name: str = 'yellow_taxi_trips',
        limit: int = 5
    ) -> Any:
        """
        Peek at raw data
        
        Args:
            table_name: Target table
            limit: Number of rows to return
        
        Returns:
            DataFrame with first N rows
        """
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        logger.info(f"👀 Peeking at {limit} rows from {table_name}")
        
        try:
            result = self.conn.execute(query).df()
            logger.info(f"✅ Fetched {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"Peek failed: {e}")
            raise
    
    def get_statistics(
        self,
        table_name: str = 'yellow_taxi_trips'
    ) -> Dict[str, Any]:
        """
        Get table statistics for performance tuning
        
        Returns:
            Dict with row counts, data types, etc.
        """
        try:
            # Row count
            count_result = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()
            row_count = count_result[0] if count_result else 0
            
            # Schema
            schema = self.get_table_schema(table_name)
            
            stats = {
                'table_name': table_name,
                'total_rows': row_count,
                'column_count': len(schema),
                'columns': schema,
            }
            
            logger.info(f"📊 Table statistics for {table_name}:")
            logger.info(f"   Rows: {row_count:,}")
            logger.info(f"   Columns: {len(schema)}")
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}
    
    def explain_plan(
        self,
        sql: str
    ) -> str:
        """
        Show query execution plan for optimization
        
        Args:
            sql: SQL query to analyze
        
        Returns:
            Query plan as formatted string
        """
        try:
            result = self.conn.execute(f"EXPLAIN {sql}").fetchall()
            plan = "\n".join(str(row[0]) for row in result)
            logger.info(f"📋 Query plan:\n{plan}")
            return plan
        except Exception as e:
            logger.error(f"Failed to explain query: {e}")
            return str(e)
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


if __name__ == '__main__':
    # Example usage
    print("🔍 Query Optimizer Demo\n")
    
    optimizer = QueryOptimizer(db_path='nyc_yellow_taxi.duckdb')
    
    # Get statistics
    stats = optimizer.get_statistics()
    print(f"\n📊 Statistics: {stats['total_rows']:,} rows in {stats['column_count']} columns\n")
    
    # Peek at data
    print("👀 Sample data:")
    sample = optimizer.peek_data(limit=3)
    print(sample)
    
    # Daily summary
    print("\n📈 Daily summary (last 7 days):")
    try:
        daily = optimizer.daily_summary(days=7)
        print(daily)
    except Exception as e:
        print(f"Error: {e}")
    
    # Vendor performance
    print("\n🏢 Vendor performance:")
    try:
        vendors = optimizer.vendor_performance()
        print(vendors)
    except Exception as e:
        print(f"Error: {e}")
    
    optimizer.close()
