#!/usr/bin/env python3
"""
Setup PostgreSQL database with NYC Yellow Taxi schema and data.
"""

import os
import sys
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configuration
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASS = "postgres"
DB_NAME = "nyc_taxi"

# Paths
SCHEMA_PATH = "/home/dev/code/analytics-and_databases/analytical-db-knockout/schema_postgres.sql"
CSV_PATH = "/home/dev/code/analytics-and_databases/yellow_taxi_trips.csv"

def wait_for_postgres(max_attempts=30):
    """Wait for PostgreSQL to be ready."""
    print("Waiting for PostgreSQL to be ready...")
    for attempt in range(max_attempts):
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASS,
                dbname="postgres"
            )
            conn.close()
            print("✓ PostgreSQL is ready")
            return True
        except psycopg2.OperationalError:
            print(f"  Attempt {attempt + 1}/{max_attempts}: Waiting for PostgreSQL...")
            time.sleep(1)
    return False

def create_database():
    """Create the nyc_taxi database (idempotent - skips if exists with data)."""
    print("\nCreating database...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if database already exists
        cur.execute(f"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}');")
        db_exists = cur.fetchone()[0]
        
        if db_exists:
            print(f"  ℹ️  Database '{DB_NAME}' already exists")
            
            # Connect to the database to check if it has data
            cur.close()
            conn.close()
            
            try:
                db_conn = psycopg2.connect(
                    host=PG_HOST,
                    port=PG_PORT,
                    user=PG_USER,
                    password=PG_PASS,
                    dbname=DB_NAME
                )
                db_cur = db_conn.cursor()
                
                # Check if table exists and has data
                db_cur.execute("""
                    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
                    WHERE table_name='yellow_taxi_trips');
                """)
                table_exists = db_cur.fetchone()[0]
                
                if table_exists:
                    db_cur.execute("SELECT COUNT(*) FROM yellow_taxi_trips;")
                    row_count = db_cur.fetchone()[0]
                    if row_count > 0:
                        print(f"  ✅ Table has {row_count:,} rows (skipping recreation)")
                        db_cur.close()
                        db_conn.close()
                        return True
                
                db_cur.close()
                db_conn.close()
            except:
                pass
            
            # If we get here, drop and recreate
            print(f"  ♻️  Recreating database...")
            cur = conn.cursor()
            cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
            print("  - Dropped existing database")
        
        # Create new database
        cur.execute(f"CREATE DATABASE {DB_NAME};")
        print(f"✓ Database '{DB_NAME}' created")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Error creating database: {e}")
        return False

def create_schema():
    """Create the table schema (idempotent - skips if table already exists)."""
    print("\nCreating schema...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname=DB_NAME
        )
        cur = conn.cursor()
        
        # Check if table already exists
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_name='yellow_taxi_trips'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            print("  ℹ️  Table 'yellow_taxi_trips' already exists (skipping schema creation)")
            cur.close()
            conn.close()
            return True
        
        # Read and execute schema SQL
        with open(SCHEMA_PATH, 'r') as f:
            schema_sql = f.read()
        
        cur.execute(schema_sql)
        conn.commit()
        print("✓ Schema created")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Error creating schema: {e}")
        return False

def load_csv_data():
    """Load CSV data into the table (idempotent - skips if data already exists)."""
    print("\nLoading CSV data...")
    if not os.path.exists(CSV_PATH):
        print(f"✗ CSV file not found: {CSV_PATH}")
        return False
    
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname=DB_NAME
        )
        cur = conn.cursor()
        
        # Check if data already exists
        cur.execute("SELECT COUNT(*) FROM yellow_taxi_trips;")
        existing_count = cur.fetchone()[0]
        
        if existing_count > 0:
            print(f"  ⚠️  Table already contains {existing_count:,} rows")
            print(f"  ℹ️  Skipping data load (idempotent - no duplicates)")
            cur.close()
            conn.close()
            return True
        
        print(f"  - Loading from {CSV_PATH}")
        with open(CSV_PATH, 'r') as f:
            cur.copy_expert(
                "COPY yellow_taxi_trips FROM STDIN WITH CSV HEADER",
                f
            )
        
        conn.commit()
        
        # Get row count
        cur.execute("SELECT COUNT(*) FROM yellow_taxi_trips;")
        count = cur.fetchone()[0]
        print(f"✓ Loaded {count:,} rows")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        return False

def verify_setup():
    """Verify the database is set up correctly."""
    print("\nVerifying setup...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname=DB_NAME
        )
        cur = conn.cursor()
        
        # Check table exists
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_name='yellow_taxi_trips'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        # Get row count
        cur.execute("SELECT COUNT(*) FROM yellow_taxi_trips;")
        row_count = cur.fetchone()[0]
        
        # Get table columns
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name='yellow_taxi_trips' 
            ORDER BY ordinal_position;
        """)
        columns = [col[0] for col in cur.fetchall()]
        
        print(f"  - Table exists: {table_exists}")
        print(f"  - Row count: {row_count:,}")
        print(f"  - Columns: {len(columns)}")
        
        if table_exists and row_count > 0:
            print("\n✓ PostgreSQL setup complete!")
            return True
        else:
            print("\n✗ Setup verification failed")
            return False
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"✗ Error verifying setup: {e}")
        return False

def main():
    """Main setup workflow."""
    print("=" * 60)
    print("PostgreSQL NYC Yellow Taxi Setup")
    print("=" * 60)
    
    if not wait_for_postgres():
        print("\n✗ PostgreSQL is not available")
        sys.exit(1)
    
    if not create_database():
        sys.exit(1)
    
    if not create_schema():
        sys.exit(1)
    
    if not load_csv_data():
        sys.exit(1)
    
    verify_setup()

if __name__ == "__main__":
    main()
