#!/usr/bin/env python3
"""
Setup DuckDB database by loading NYC Yellow Taxi parquet files.
Loads all parquet files from NYC Yellow Taxi Record 23-24-25 folder.
"""

import os
import sys
from pathlib import Path
import duckdb

# Configuration
PARENT_DIR = Path(__file__).parent
DB_PATH = PARENT_DIR / "nyc_yellow_taxi.duckdb"
NYC_TAXI_DIR = PARENT_DIR / "NYC Yellow Taxi Record 23-24-25"

# Parquet file pattern
PARQUET_PATTERN = str(NYC_TAXI_DIR / "**" / "*.parquet")



def check_data_availability():
    """Verify NYC taxi data folder exists and contains parquet files."""
    if not NYC_TAXI_DIR.exists():
        print(f"❌ NYC Yellow Taxi dataset not found at {NYC_TAXI_DIR}")
        print(f"   Please download from: https://www.kaggle.com/datasets/qweemreee/nyc-yellow-taxi-record-23-24-25")
        return False
    
    # Count parquet files
    parquet_files = list(NYC_TAXI_DIR.glob("**/*.parquet"))
    if not parquet_files:
        print(f"❌ No parquet files found in {NYC_TAXI_DIR}")
        return False
    
    print(f"📦 Found {len(parquet_files)} parquet files")
    return True


def setup_duckdb():
    """Create DuckDB database and load parquet files."""
    try:
        # Remove existing database if present
        if DB_PATH.exists():
            print(f"♻️  Removing existing database at {DB_PATH}")
            DB_PATH.unlink()
        
        # Connect to DuckDB
        print(f"🔗 Connecting to DuckDB: {DB_PATH}")
        con = duckdb.connect(str(DB_PATH))
        
        # Load all parquet files into yellow_taxi_trips table
        print(f"📥 Loading parquet files from {NYC_TAXI_DIR}")
        print(f"   Pattern: {PARQUET_PATTERN}")
        
        try:
            # Create table from all parquet files using glob pattern
            con.execute(f"""
                CREATE TABLE yellow_taxi_trips AS
                SELECT * FROM read_parquet('{PARQUET_PATTERN}', union_by_name=true, hive_partitioning=false)
            """)
            
            # Get row count
            result = con.execute("SELECT COUNT(*) as row_count FROM yellow_taxi_trips").fetchall()
            row_count = result[0][0]
            
            # Get column info
            columns = con.execute("DESCRIBE yellow_taxi_trips").fetchall()
            col_count = len(columns)
            
            print(f"✅ Database created successfully")
            print(f"   Rows: {row_count:,}")
            print(f"   Columns: {col_count}")
            
            # Display schema
            print(f"\n📋 Table Schema:")
            for col_name, col_type, null, key, default, extra in columns:
                print(f"   - {col_name}: {col_type}")
            
            # Get database file size
            db_size_mb = DB_PATH.stat().st_size / (1024 * 1024)
            print(f"\n💾 Database size: {db_size_mb:.2f} MB")
            
            con.close()
            return True
        
        except Exception as e:
            print(f"❌ Failed to load parquet files: {e}")
            con.close()
            # Remove partial database
            if DB_PATH.exists():
                DB_PATH.unlink()
            return False
    
    except Exception as e:
        print(f"❌ DuckDB setup failed: {e}")
        return False


def verify_database():
    """Verify database is properly set up."""
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        
        # Check table exists
        result = con.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name = 'yellow_taxi_trips'
        """).fetchall()
        
        if not result:
            print("❌ Table 'yellow_taxi_trips' not found")
            con.close()
            return False
        
        # Check row count
        count_result = con.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchall()
        row_count = count_result[0][0]
        
        if row_count == 0:
            print("❌ Table is empty")
            con.close()
            return False
        
        print(f"✅ Verification passed: {row_count:,} rows")
        con.close()
        return True
    
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False


def create_placeholder_schema():
    """Create an empty schema placeholder."""
    try:
        con = duckdb.connect(str(DB_PATH))
        con.execute("""
            CREATE TABLE yellow_taxi_trips (
                VendorID INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE,
                RatecodeID INTEGER,
                store_and_fwd_flag VARCHAR,
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                total_amount DOUBLE,
                congestion_surcharge DOUBLE,
                airport_fee DOUBLE,
                extra_field INTEGER
            )
        """)
        con.close()
        return True
    except Exception as e:
        print(f"❌ Failed to create placeholder schema: {e}")
        return False


def main():
    """Main setup flow."""
    print("=" * 60)
    print("DuckDB NYC Yellow Taxi Dataset Setup")
    print("=" * 60)
    print()
    
    # Check data availability
    if not check_data_availability():
        print("\n⚠️  Data loading skipped. You can download later and run again.")
        # Create empty database anyway for placeholder
        if not DB_PATH.exists():
            DB_PATH.unlink() if DB_PATH.exists() else None
            if create_placeholder_schema():
                print(f"✅ Empty database created at {DB_PATH}")
                print(f"   Run again after placing parquet files in {NYC_TAXI_DIR}")
                return True
        return False
    
    # Setup database
    print()
    if not setup_duckdb():
        sys.exit(1)
    
    # Verify
    print()
    if not verify_database():
        print("\n⚠️  Setup completed but verification failed")
        sys.exit(1)
    
    print()
    print("=" * 60)
    print("✅ DuckDB setup complete!")
    print(f"   Database: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
