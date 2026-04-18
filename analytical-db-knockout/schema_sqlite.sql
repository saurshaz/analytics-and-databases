-- SQLite Schema for NYC Yellow Taxi Data
-- Adapted from PostgreSQL schema for SQLite compatibility

CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    VendorID              INTEGER,
    tpep_pickup_datetime  TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count       REAL,
    trip_distance         REAL,
    RatecodeID            REAL,
    store_and_fwd_flag    TEXT,
    PULocationID          INTEGER,
    DOLocationID          INTEGER,
    payment_type          INTEGER,
    fare_amount           REAL,
    extra                 REAL,
    mta_tax               REAL,
    tip_amount            REAL,
    tolls_amount          REAL,
    improvement_surcharge REAL,
    total_amount          REAL,
    congestion_surcharge  REAL,
    airport_fee           REAL,
    cbd_congestion_fee    REAL
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_yellow_taxi_trips_vendor_id ON yellow_taxi_trips(VendorID);
CREATE INDEX IF NOT EXISTS idx_yellow_taxi_trips_pu_location_id ON yellow_taxi_trips(PULocationID);
CREATE INDEX IF NOT EXISTS idx_yellow_taxi_trips_do_location_id ON yellow_taxi_trips(DOLocationID);
CREATE INDEX IF NOT EXISTS idx_yellow_taxi_trips_pickup_datetime ON yellow_taxi_trips(tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_yellow_taxi_trips_dropoff_datetime ON yellow_taxi_trips(tpep_dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_yellow_taxi_trips_payment_type ON yellow_taxi_trips(payment_type);