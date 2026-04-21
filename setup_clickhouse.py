#!/usr/bin/env python3
"""
Setup script for ClickHouse NYC Taxi database.

Loads NYC Yellow Taxi data into ClickHouse with proper schema and partitioning.
Supports loading from:
- Parquet files (NYC Yellow Taxi Record 23-24-25/)
- CSV file (yellow_taxi_trips.csv)
- DuckDB database (nyc_yellow_taxi.duckdb)

ClickHouse optimizations:
- MergeTree engine for fast aggregations
- Column-oriented storage for analytical queries
- Partition by month for efficient data pruning
- Primary key on (pickup_datetime, VendorID)
"""

import os
import sys
from pathlib import Path
import time

import pandas as pd
from clickhouse_driver import Client

# Configuration
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "default")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
# ClickHouse default user typically has no password - use None if not set
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS") or None

# Paths
ROOT_DIR = Path(__file__).resolve().parent
TAXI_DIR = ROOT_DIR / "NYC Yellow Taxi Record 23-24-25"
SCHEMA_FILE = ROOT_DIR / "analytical-db-knockout" / "schema_clickhouse.sql"
DB_FILE = ROOT_DIR / "nyc_yellow_taxi.duckdb"
CSV_FILE = ROOT_DIR / "yellow_taxi_trips.csv"

BATCH_SIZE = 100000  # Increased from 50000 for faster loading


def connect_clickhouse():
    """Create ClickHouse client connection."""
    try:
        # Build connection kwargs
        conn_kwargs = {
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "database": CLICKHOUSE_DB,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASS if CLICKHOUSE_PASS else "",
            "connect_timeout": 10,
        }
        
        client = Client(**conn_kwargs)
        # Test connection
        client.execute("SELECT 1")
        print(f"✅ Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        print(f"   Make sure ClickHouse is running at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        sys.exit(1)


def create_schema(client):
    """Create ClickHouse schema from SQL file."""
    if not SCHEMA_FILE.exists():
        print(f"❌ Schema file not found: {SCHEMA_FILE}")
        sys.exit(1)

    with open(SCHEMA_FILE, "r") as f:
        schema_sql = f.read()

    # Split by semicolons and execute each statement
    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
    for stmt in statements:
        try:
            print(f"  Executing: {stmt[:60]}...")
            client.execute(stmt)
        except Exception as e:
            # Ignore "already exists" errors
            if "already exists" not in str(e):
                print(f"❌ Error: {e}")
            else:
                print(f"  (Table already exists)")

    print("✅ Schema created successfully")


def load_from_duckdb(client):
    """Load data from DuckDB database."""
    if not DB_FILE.exists():
        print(f"⚠️  DuckDB file not found: {DB_FILE}")
        return False

    try:
        import duckdb

        print(f"\n📦 Loading data from DuckDB: {DB_FILE}")
        
        # First, truncate the table to ensure clean slate
        try:
            client.execute("TRUNCATE TABLE yellow_taxi_trips")
            print("  Cleared existing data from ClickHouse table")
        except:
            pass  # Table might not exist yet
        
        duckdb_conn = duckdb.connect(str(DB_FILE), read_only=True)

        # Get total row count
        count_result = duckdb_conn.execute(
            "SELECT COUNT(*) as cnt FROM yellow_taxi_trips"
        ).fetchall()
        total_rows = count_result[0][0] if count_result else 0

        if total_rows == 0:
            print("⚠️  DuckDB table is empty")
            duckdb_conn.close()
            return False

        print(f"  Total rows in DuckDB: {total_rows:,}")

        # Load in batches with progress tracking
        offset = 0
        batch_num = 0
        rows_inserted = 0
        start_time = time.time()

        while offset < total_rows:
            batch_num += 1
            query = f"""
                SELECT 
                    VendorID,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    RatecodeID,
                    store_and_fwd_flag,
                    PULocationID,
                    DOLocationID,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    airport_fee,
                    cbd_congestion_fee
                FROM yellow_taxi_trips
                ORDER BY tpep_pickup_datetime
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """

            df = duckdb_conn.execute(query).df()

            if df.empty:
                break

            # Convert store_and_fwd_flag to string if needed
            if 'store_and_fwd_flag' in df.columns:
                df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N').astype(str)

            # Insert into ClickHouse
            client.insert_dataframe(
                "INSERT INTO yellow_taxi_trips VALUES",
                df,
                settings={"use_numpy": True},
            )

            rows_inserted += len(df)
            progress = min(rows_inserted, total_rows)
            percent = (progress / total_rows) * 100 if total_rows > 0 else 0
            elapsed = time.time() - start_time
            rows_per_sec = rows_inserted / elapsed if elapsed > 0 else 0
            eta_secs = (total_rows - rows_inserted) / rows_per_sec if rows_per_sec > 0 else 0

            print(f"  Batch {batch_num}: {rows_inserted:,}/{total_rows:,} rows ({percent:.1f}%) - {rows_per_sec:,.0f} rows/sec - ETA: {eta_secs/60:.1f} min")

            offset += BATCH_SIZE

        duckdb_conn.close()
        
        elapsed = time.time() - start_time
        print(f"✅ Successfully loaded {rows_inserted:,} rows from DuckDB in {elapsed/60:.1f} minutes")
        return rows_inserted > 0

    except ImportError:
        print("⚠️  duckdb module not available")
        return False
    except Exception as e:
        print(f"❌ Error loading from DuckDB: {e}")
        import traceback
        traceback.print_exc()
        return False


def load_from_csv(client):
    """Load data from CSV file."""
    if not CSV_FILE.exists():
        print(f"⚠️  CSV file not found: {CSV_FILE}")
        return False

    try:
        print(f"\n📦 Loading data from CSV: {CSV_FILE}")

        # Read CSV in chunks
        reader = pd.read_csv(CSV_FILE, chunksize=BATCH_SIZE)
        batch_num = 0
        total_rows = 0

        for chunk in reader:
            batch_num += 1

            # Rename columns to match ClickHouse schema
            chunk = chunk.rename(
                columns={
                    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
                    "tpep_pickup_datetime": "tpep_pickup_datetime",
                }
            )

            # Type conversions for ClickHouse
            chunk["VendorID"] = chunk["VendorID"].astype("float64")
            chunk["passenger_count"] = chunk["passenger_count"].astype("float64")
            chunk["RatecodeID"] = chunk["RatecodeID"].astype("float64")
            chunk["PULocationID"] = chunk["PULocationID"].astype("int64")
            chunk["DOLocationID"] = chunk["DOLocationID"].astype("int64")
            chunk["payment_type"] = chunk["payment_type"].astype("int64")
            chunk["trip_distance"] = chunk["trip_distance"].astype("float64")
            chunk["fare_amount"] = chunk["fare_amount"].astype("float64")
            chunk["extra"] = chunk["extra"].astype("float64")
            chunk["mta_tax"] = chunk["mta_tax"].astype("float64")
            chunk["tip_amount"] = chunk["tip_amount"].astype("float64")
            chunk["tolls_amount"] = chunk["tolls_amount"].astype("float64")
            chunk["total_amount"] = chunk["total_amount"].astype("float64")
            chunk["improvement_surcharge"] = chunk["improvement_surcharge"].astype("float64")
            chunk["congestion_surcharge"] = chunk["congestion_surcharge"].astype("float64")
            chunk["airport_fee"] = chunk["airport_fee"].astype("float64") if "airport_fee" in chunk.columns else 0.0
            chunk["cbd_congestion_fee"] = chunk["cbd_congestion_fee"].astype("float64") if "cbd_congestion_fee" in chunk.columns else 0.0

            # Insert into ClickHouse
            client.insert_dataframe(
                "INSERT INTO yellow_taxi_trips VALUES",
                chunk,
                settings={"use_numpy": True},
            )

            total_rows += len(chunk)
            print(f"  Batch {batch_num}: Loaded {total_rows:,} rows")

        print(f"✅ Successfully loaded {total_rows:,} rows from CSV")
        return total_rows > 0

    except Exception as e:
        print(f"❌ Error loading from CSV: {e}")
        return False


def load_from_parquet(client):
    """Load data from Parquet files."""
    if not TAXI_DIR.exists():
        print(f"⚠️  Taxi data directory not found: {TAXI_DIR}")
        return False

    try:
        import duckdb

        parquet_files = list(TAXI_DIR.glob("**/*.parquet"))
        if not parquet_files:
            print(f"⚠️  No parquet files found in {TAXI_DIR}")
            return False

        print(f"\n📦 Loading data from {len(parquet_files)} Parquet files")

        # Use DuckDB to read and transform parquet
        duckdb_conn = duckdb.connect()

        total_rows = 0
        for idx, parquet_file in enumerate(parquet_files, 1):
            print(f"  File {idx}/{len(parquet_files)}: {parquet_file.name}")

            query = f"""
                SELECT 
                    VendorID,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    RatecodeID,
                    store_and_fwd_flag,
                    PULocationID,
                    DOLocationID,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    airport_fee,
                    cbd_congestion_fee
                FROM read_parquet('{parquet_file}')
            """

            df = duckdb_conn.execute(query).df()

            # Insert into ClickHouse in batches
            for batch_start in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[batch_start : batch_start + BATCH_SIZE]
                
                # Ensure all required columns exist (fill with 0 if missing)
                for col in ["improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]:
                    if col not in batch.columns:
                        batch[col] = 0.0
                
                client.insert_dataframe(
                    "INSERT INTO yellow_taxi_trips VALUES",
                    batch,
                    settings={"use_numpy": True},
                )

            total_rows += len(df)
            print(f"    Inserted {len(df):,} rows")

        duckdb_conn.close()
        print(f"✅ Successfully loaded {total_rows:,} rows from Parquet")
        return total_rows > 0

    except ImportError:
        print("⚠️  duckdb module not available for reading parquet")
        return False
    except Exception as e:
        print(f"❌ Error loading from Parquet: {e}")
        return False


def verify_data(client):
    """Verify data was loaded correctly."""
    try:
        result = client.execute(
            "SELECT COUNT(*) as row_count, COUNT(DISTINCT VendorID) as vendors, "
            "MIN(tpep_pickup_datetime) as min_date, MAX(tpep_pickup_datetime) as max_date "
            "FROM yellow_taxi_trips"
        )

        row_count, vendors, min_date, max_date = result[0]
        print(f"\n📊 Data Verification:")
        print(f"   Total rows: {row_count:,}")
        print(f"   Vendors: {vendors}")
        print(f"   Date range: {min_date} to {max_date}")

        if row_count > 0:
            print("✅ Data loaded successfully!")
            return True
        else:
            print("❌ No data found in table")
            return False

    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        return False


def main():
    """Main setup routine."""
    print("=" * 70)
    print("ClickHouse NYC Taxi Database Setup")
    print("=" * 70)

    # Connect to ClickHouse
    client = connect_clickhouse()

    # Create schema
    print("\n🔨 Creating ClickHouse schema...")
    create_schema(client)

    # Try loading data from various sources
    print("\n📥 Loading data...")
    data_loaded = False

    # Priority order: DuckDB > Parquet > CSV
    if not data_loaded:
        data_loaded = load_from_duckdb(client)
    if not data_loaded:
        data_loaded = load_from_parquet(client)
    if not data_loaded:
        data_loaded = load_from_csv(client)

    if not data_loaded:
        print("⚠️  Could not load data from any source")
        print("   Available sources:")
        print(f"   - DuckDB: {DB_FILE}")
        print(f"   - Parquet: {TAXI_DIR}")
        print(f"   - CSV: {CSV_FILE}")

    # Verify data
    print("\n" + "=" * 70)
    verify_data(client)
    print("=" * 70)


if __name__ == "__main__":
    main()
