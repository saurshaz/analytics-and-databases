#!/usr/bin/env python3
"""
Example scripts for importing Parquet files into ClickHouse.
Run with: python examples_parquet_import.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


def create_sample_taxi_parquet():
    """Generate sample NYC taxi Parquet file for testing."""
    print("Creating sample taxi data...")
    
    np.random.seed(42)
    n_rows = 100000
    
    # Generate realistic taxi data
    base_date = datetime(2024, 1, 1)
    pickup_times = [
        base_date + timedelta(minutes=int(np.random.uniform(0, 1440)))
        for _ in range(n_rows)
    ]
    
    df = pd.DataFrame({
        'VendorID': np.random.choice([1, 2], n_rows),
        'tpep_pickup_datetime': pickup_times,
        'tpep_dropoff_datetime': [
            t + timedelta(minutes=np.random.randint(5, 60))
            for t in pickup_times
        ],
        'passenger_count': np.random.choice([1, 2, 3, 4, 5, 6], n_rows, p=[0.7, 0.15, 0.08, 0.04, 0.02, 0.01]),
        'trip_distance': np.random.uniform(0.1, 50, n_rows),
        'RatecodeID': np.random.choice([1, 2, 3, 4, 5], n_rows),
        'store_and_fwd_flag': np.random.choice(['Y', 'N'], n_rows),
        'PULocationID': np.random.randint(1, 263, n_rows),
        'DOLocationID': np.random.randint(1, 263, n_rows),
        'payment_type': np.random.choice([1, 2, 3, 4], n_rows),
        'fare_amount': np.random.uniform(2.5, 150, n_rows),
        'extra': np.random.choice([0, 0.5, 1], n_rows),
        'mta_tax': np.full(n_rows, 0.5),
        'tip_amount': np.random.uniform(0, 50, n_rows),
        'tolls_amount': np.random.choice([0, 0, 0, 5.76, 6.5], n_rows),
        'improvement_surcharge': np.full(n_rows, 0.3),
        'total_amount': np.random.uniform(5, 200, n_rows),
        'congestion_surcharge': np.random.choice([0, 2.5], n_rows),
        'airport_fee': np.random.choice([0, 1.25], n_rows),
        'cbd_congestion_fee': np.random.choice([0, 0, 0, 0.75], n_rows),
    })
    
    # Save to Parquet
    output_file = 'sample_taxi_100k.parquet'
    df.to_parquet(output_file, engine='pyarrow', compression='snappy')
    print(f"✓ Created {output_file} ({n_rows} rows, {Path(output_file).stat().st_size / 1024 / 1024:.1f} MB)")
    
    return output_file


def example_basic_import():
    """Example 1: Basic import with default settings."""
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Import (Method 2 - Native Protocol)")
    print("="*60)
    print("""
# Usage:
python parquet_importer.py sample_taxi_100k.parquet \\
  --table yellow_taxi_trips \\
  --method 2 \\
  --validate

# This will:
1. Connect to ClickHouse on localhost:9000
2. Import all rows from Parquet file
3. Validate the import completed successfully
    """)


def example_batch_import():
    """Example 2: Batch import multiple files."""
    print("\n" + "="*60)
    print("EXAMPLE 2: Batch Import Multiple Files")
    print("="*60)
    print("""
#!/bin/bash
# Save as: batch_import.sh

for file in data/*.parquet; do
  echo "Importing $file..."
  python parquet_importer.py "$file" \\
    --table yellow_taxi_trips \\
    --method 2 \\
    --validate
done

# Run:
chmod +x batch_import.sh
./batch_import.sh
    """)


def example_pandas_conversion():
    """Example 3: Convert CSV to Parquet then import."""
    print("\n" + "="*60)
    print("EXAMPLE 3: CSV → Parquet → ClickHouse")
    print("="*60)
    print("""
import pandas as pd
from parquet_importer import ParquetImporter

# Convert CSV to Parquet
print("Converting CSV to Parquet...")
df = pd.read_csv('yellow_tripdata_2024-01.csv')
df.to_parquet('taxi_2024_01.parquet', engine='pyarrow', compression='snappy')

# Import to ClickHouse
print("Importing to ClickHouse...")
importer = ParquetImporter(host='localhost', port=9000)
importer.import_method_2_native_protocol(
    'taxi_2024_01.parquet',
    'yellow_taxi_trips'
)

# Validate
importer.validate_import('yellow_taxi_trips')
print("✓ Import complete!")
    """)


def example_parallel_import():
    """Example 4: Parallel import with Python."""
    print("\n" + "="*60)
    print("EXAMPLE 4: Parallel Import (Multiple Files)")
    print("="*60)
    print("""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from parquet_importer import ParquetImporter

def import_file(parquet_file):
    importer = ParquetImporter()
    importer.import_method_2_native_protocol(
        str(parquet_file),
        'yellow_taxi_trips'
    )
    print(f"✓ Imported {parquet_file.name}")

# Import all Parquet files in parallel (4 workers)
parquet_files = list(Path('data').glob('*.parquet'))
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(import_file, parquet_files)

print(f"✓ Imported {len(parquet_files)} files")
    """)


def example_large_file_import():
    """Example 5: Large file import with Method 3."""
    print("\n" + "="*60)
    print("EXAMPLE 5: Large File Import (Method 3 - SQL Parquet)")
    print("="*60)
    print("""
# For files >10GB, use Method 3 (fastest)

# 1. Copy file to ClickHouse user_files directory
docker cp large_dataset.parquet nyc_taxi_clickhouse:/var/lib/clickhouse/user_files/

# 2. Import via SQL
docker exec nyc_taxi_clickhouse clickhouse-client \\
  --query "INSERT INTO yellow_taxi_trips \\
           SELECT * FROM file('/var/lib/clickhouse/user_files/large_dataset.parquet', 'Parquet')"

# Or use Python:
from clickhouse_driver import Client
client = Client('localhost')
client.execute("INSERT INTO yellow_taxi_trips SELECT * FROM file('large_dataset.parquet', 'Parquet')")
    """)


def example_docker_volume_mount():
    """Example 6: Using Docker volume for efficient import."""
    print("\n" + "="*60)
    print("EXAMPLE 6: Docker Volume Mount (Production)")
    print("="*60)
    print("""
# Update docker-compose.yml:
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - ./data:/var/lib/clickhouse/user_files  # Mount Parquet files here
      - clickhouse_data:/var/lib/clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"

# Place Parquet files in ./data/ directory on host:
cp taxi_data.parquet ./data/

# Then import:
docker-compose up -d clickhouse
docker exec nyc_taxi_clickhouse clickhouse-client \\
  --query "INSERT INTO yellow_taxi_trips \\
           SELECT * FROM file('taxi_data.parquet', 'Parquet')"
    """)


def example_validation():
    """Example 7: Validate import and check data quality."""
    print("\n" + "="*60)
    print("EXAMPLE 7: Validation & Data Quality Checks")
    print("="*60)
    print("""
from clickhouse_driver import Client

client = Client('localhost')

# Check row count
count = client.execute("SELECT COUNT() FROM yellow_taxi_trips")[0][0]
print(f"Total rows: {count:,}")

# Check date range
date_range = client.execute(
    "SELECT min(tpep_pickup_datetime), max(tpep_pickup_datetime) "
    "FROM yellow_taxi_trips"
)[0]
print(f"Date range: {date_range[0]} to {date_range[1]}")

# Check data quality
stats = client.execute("""
    SELECT
        COUNT() as total_rows,
        COUNT(DISTINCT VendorID) as vendors,
        COUNT(DISTINCT PULocationID) as pickup_locations,
        COUNT(DISTINCT DOLocationID) as dropoff_locations,
        avg(fare_amount) as avg_fare,
        avg(trip_distance) as avg_distance,
        percentile(fare_amount, 0.95) as p95_fare
    FROM yellow_taxi_trips
""")[0]

print(f"Statistics:")
print(f"  Total rows: {stats[0]:,}")
print(f"  Vendors: {stats[1]}")
print(f"  Pickup locations: {stats[2]}")
print(f"  Dropoff locations: {stats[3]}")
print(f"  Avg fare: ${stats[4]:.2f}")
print(f"  Avg distance: {stats[5]:.2f} miles")
print(f"  95th percentile fare: ${stats[6]:.2f}")
    """)


def example_performance_monitoring():
    """Example 8: Monitor import performance."""
    print("\n" + "="*60)
    print("EXAMPLE 8: Performance Monitoring")
    print("="*60)
    print("""
import time
from parquet_importer import ParquetImporter
from pathlib import Path

file_path = 'large_dataset.parquet'
file_size_mb = Path(file_path).stat().st_size / 1024 / 1024

importer = ParquetImporter()

start = time.time()
importer.import_method_2_native_protocol(file_path, 'yellow_taxi_trips')
elapsed = time.time() - start

throughput = file_size_mb / elapsed

print(f"Performance:")
print(f"  File size: {file_size_mb:.1f} MB")
print(f"  Time: {elapsed:.2f} seconds")
print(f"  Throughput: {throughput:.1f} MB/s")

# Check ClickHouse system metrics
client = importer.client
metrics = client.execute(
    "SELECT * FROM system.query_log "
    "WHERE query_start_time > now() - INTERVAL 1 MINUTE "
    "ORDER BY query_start_time DESC LIMIT 1"
)
    """)


if __name__ == '__main__':
    print("\n" + "="*60)
    print("PARQUET IMPORT EXAMPLES FOR CLICKHOUSE")
    print("="*60)
    
    # Create sample data
    sample_file = create_sample_taxi_parquet()
    print(f"\n✓ Sample file ready: {sample_file}")
    
    # Show examples
    example_basic_import()
    example_batch_import()
    example_pandas_conversion()
    example_parallel_import()
    example_large_file_import()
    example_docker_volume_mount()
    example_validation()
    example_performance_monitoring()
    
    print("\n" + "="*60)
    print("READY TO USE!")
    print("="*60)
    print(f"""
Next steps:

1. Start ClickHouse:
   docker-compose up -d clickhouse

2. Import sample data:
   python parquet_importer.py {sample_file} \\
     --table yellow_taxi_trips \\
     --method 2 \\
     --validate

3. Check the data:
   docker exec nyc_taxi_clickhouse clickhouse-client \\
     --query "SELECT count() FROM yellow_taxi_trips"
    """)
