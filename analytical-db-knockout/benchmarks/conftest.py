"""Pytest configuration and shared fixtures for analytical-db-knockout benchmarks."""

import json
import os
from pathlib import Path

import duckdb
import psycopg2
import pytest


ROOT_DIR = Path(__file__).resolve().parent.parent
PARENT_DIR = ROOT_DIR.parent
DB_PATH = str(PARENT_DIR / "nyc_yellow_taxi.duckdb")
QUERIES_PATH = str(ROOT_DIR / "benchmarks" / "queries.json")

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "nyc_taxi")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASS = os.environ.get("PG_PASS", "postgres")


@pytest.fixture(scope="session")
def duckdb_con():
    """DuckDB connection for analytical queries."""
    if not os.path.exists(DB_PATH):
        pytest.skip(f"DuckDB database not found at {DB_PATH}")
    
    conn = duckdb.connect(DB_PATH, read_only=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def pg_con():
    """PostgreSQL connection for comparison queries."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            connect_timeout=5
        )
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL connection failed: {e}")


@pytest.fixture(scope="session")
def queries():
    """Load benchmark queries from JSON."""
    if not os.path.exists(QUERIES_PATH):
        pytest.skip(f"Queries file not found at {QUERIES_PATH}")
    
    with open(QUERIES_PATH, "r") as f:
        data = json.load(f)
        # Handle both new format (with metadata) and old format (direct array)
        if isinstance(data, dict) and "queries" in data:
            return data["queries"]
        elif isinstance(data, list):
            return data
        else:
            pytest.skip(f"Unexpected queries format in {QUERIES_PATH}")
