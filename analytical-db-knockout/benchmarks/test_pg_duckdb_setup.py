"""
Test: pg_duckdb extension setup and installation.

Ensures pg_duckdb extension is installed, enabled, and ready for use.
Tests installation via multiple methods (pgxman, apt, dnf) with platform detection.
"""

import subprocess
import sys
import time
from pathlib import Path

import psycopg2
import pytest

from benchmarks.conftest import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS


def check_pg_duckdb_installed():
    """Check if pg_duckdb extension is already installed."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            connect_timeout=5
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM pg_extension WHERE extname = 'pg_duckdb'")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        return False


def install_pg_duckdb_via_pgxman():
    """Install pg_duckdb using pgxman package manager."""
    try:
        # Check if pgxman is installed
        result = subprocess.run(
            ["pgxman", "--version"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode != 0:
            return False, "pgxman not installed"

        # Install pg_duckdb for PostgreSQL 15
        result = subprocess.run(
            ["pgxman", "install", "--pg", "15", "pg_duckdb"],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            return True, "pgxman installation successful"
        else:
            return False, f"pgxman install failed: {result.stderr}"
    except subprocess.TimeoutExpired:
        return False, "pgxman installation timed out"
    except FileNotFoundError:
        return False, "pgxman not found"
    except Exception as e:
        return False, f"pgxman installation error: {str(e)}"


def install_pg_duckdb_via_apt():
    """Install pg_duckdb using apt package manager (Ubuntu/Debian)."""
    try:
        # Check if apt is available
        result = subprocess.run(
            ["apt-get", "update"],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode != 0:
            return False, "apt-get update failed"

        # Install pg_duckdb for PostgreSQL 15
        result = subprocess.run(
            ["apt-get", "install", "-y", "postgresql-15-pg-duckdb"],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            return True, "apt installation successful"
        else:
            return False, f"apt install failed: {result.stderr}"
    except subprocess.TimeoutExpired:
        return False, "apt installation timed out"
    except FileNotFoundError:
        return False, "apt-get not found"
    except Exception as e:
        return False, f"apt installation error: {str(e)}"


def install_pg_duckdb_via_dnf():
    """Install pg_duckdb using dnf package manager (Fedora/RHEL)."""
    try:
        # Check if dnf is available
        result = subprocess.run(
            ["dnf", "search", "pg_duckdb"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode != 0:
            return False, "dnf search failed"

        # Install pg_duckdb for PostgreSQL 15
        result = subprocess.run(
            ["dnf", "install", "-y", "postgresql-15-pg-duckdb"],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            return True, "dnf installation successful"
        else:
            return False, f"dnf install failed: {result.stderr}"
    except subprocess.TimeoutExpired:
        return False, "dnf installation timed out"
    except FileNotFoundError:
        return False, "dnf not found"
    except Exception as e:
        return False, f"dnf installation error: {str(e)}"


def install_pg_duckdb():
    """Install pg_duckdb extension using available package manager."""
    # Try pgxman first (Linux)
    if sys.platform == "linux":
        success, message = install_pg_duckdb_via_pgxman()
        if success:
            return success, message
        
        # Try apt (Ubuntu/Debian)
        success, message = install_pg_duckdb_via_apt()
        if success:
            return success, message
        
        # Try dnf (Fedora/RHEL)
        success, message = install_pg_duckdb_via_dnf()
        if success:
            return success, message
    
    return False, f"Unsupported platform: {sys.platform}"


def setup_pg_duckdb_extension():
    """Setup pg_duckdb extension in PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            connect_timeout=5
        )
        cur = conn.cursor()
        
        # Check if extension already exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_extension 
                WHERE extname = 'pg_duckdb'
            )
        """)
        exists = cur.fetchone()[0]
        
        if exists:
            cur.close()
            conn.close()
            return True, "pg_duckdb already installed"
        
        # Create extension
        cur.execute("CREATE EXTENSION pg_duckdb;")
        conn.commit()
        cur.close()
        conn.close()
        
        # Enable DuckDB execution for all queries
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            connect_timeout=5
        )
        cur = conn.cursor()
        cur.execute("SET duckdb.force_execution TO true;")
        conn.commit()
        cur.close()
        conn.close()
        
        return True, "pg_duckdb extension created and enabled"
    except Exception as e:
        return False, f"pg_duckdb setup failed: {str(e)}"


@pytest.mark.benchmark
class TestPgDuckDBSetup:
    """Test pg_duckdb extension installation and setup."""

    def test_pg_duckdb_installed(self):
        """Verify pg_duckdb extension is installed."""
        is_installed = check_pg_duckdb_installed()
        assert is_installed, "pg_duckdb extension is not installed"

    def test_pg_duckdb_extension_exists(self):
        """Verify pg_duckdb extension is registered in PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS,
                connect_timeout=5
            )
            cur = conn.cursor()
            cur.execute("SELECT * FROM pg_extension WHERE extname = 'pg_duckdb'")
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            assert result is not None, "pg_duckdb extension not found in pg_extension"
            assert result[1] == "pg_duckdb", "Extension name mismatch"
        except Exception as e:
            pytest.fail(f"Failed to check pg_duckdb extension: {e}")

    def test_duckdb_force_execution_enabled(self):
        """Verify duckdb.force_execution is set to true."""
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS,
                connect_timeout=5
            )
            cur = conn.cursor()
            cur.execute("SHOW duckdb.force_execution;")
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            assert result is not None, "duckdb.force_execution setting not found"
            assert result[0] == "true", f"duckdb.force_execution is '{result[0]}', expected 'true'"
        except Exception as e:
            pytest.fail(f"Failed to check duckdb.force_execution: {e}")

    def test_pg_duckdb_query_execution(self):
        """Verify pg_duckdb can execute a simple query."""
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS,
                connect_timeout=5
            )
            cur = conn.cursor()
            
            # Test simple query using pg_duckdb
            cur.execute("SELECT COUNT(*) FROM yellow_taxi_trips WHERE total_amount > 0")
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            assert result is not None, "Query execution failed"
            assert result[0] > 0, "No rows returned"
        except Exception as e:
            pytest.fail(f"pg_duckdb query execution failed: {e}")

    def test_pg_duckdb_extension_version(self):
        """Verify pg_duckdb extension has a version."""
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS,
                connect_timeout=5
            )
            cur = conn.cursor()
            
            # Get extension version
            cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_duckdb'")
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            assert result is not None, "Extension version not found"
            assert result[0] is not None, "Extension version is NULL"
            assert len(result[0]) > 0, "Extension version is empty"
        except Exception as e:
            pytest.fail(f"Failed to get pg_duckdb version: {e}")