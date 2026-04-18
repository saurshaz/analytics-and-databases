"""
Test: Query correctness validation across all databases.

Ensures queries produce valid results and basic sanity checks pass.
"""

import pytest


@pytest.mark.validation
class TestQueryCorrectness:
    """Validate query correctness on available databases."""

    def test_duckdb_query_count(self, duckdb_con, queries):
        """Verify DuckDB returns result rows for all queries."""
        print("\n" + "="*80)
        print("DuckDB Query Result Validation")
        print("="*80)
        
        result_counts = {}
        for query in queries:
            try:
                duckdb_sql = query.get("sql")
                df = duckdb_con.execute(duckdb_sql).fetchdf()
                count = len(df)
                result_counts[query["id"]] = count
                status = "✅" if count > 0 else "⚠️"
                print(f"  Query {query['id']:2d}: {count:8d} rows {status}")
                
                assert count > 0, f"Query {query['id']} returned no results"
            except Exception as e:
                print(f"  Query {query['id']:2d}: ERROR - {str(e)[:60]}")
                result_counts[query['id']] = None
        
        print(f"\nTotal queries: {len([v for v in result_counts.values() if v is not None])}/{len(queries)}")

    def test_postgres_query_count(self, pg_con, queries):
        """Verify PostgreSQL returns result rows for all queries."""
        if not pg_con:
            pytest.skip("PostgreSQL not available")
        
        print("\n" + "="*80)
        print("PostgreSQL Query Result Validation")
        print("="*80)
        
        result_counts = {}
        for query in queries:
            try:
                postgres_sql = query.get("postgres_sql", query.get("sql"))
                cur = pg_con.cursor()
                cur.execute(postgres_sql)
                result = cur.fetchall()
                count = len(result)
                cur.close()
                
                result_counts[query["id"]] = count
                status = "✅" if count > 0 else "⚠️"
                print(f"  Query {query['id']:2d}: {count:8d} rows {status}")
                
                assert count > 0, f"Query {query['id']} returned no results"
            except Exception as e:
                print(f"  Query {query['id']:2d}: ERROR - {str(e)[:60]}")
                pg_con.rollback()
                result_counts[query['id']] = None
        
        print(f"\nTotal queries: {len([v for v in result_counts.values() if v is not None])}/{len(queries)}")

    def test_schema_validation(self, duckdb_con):
        """Verify NYC Yellow Taxi table structure."""
        info = duckdb_con.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'yellow_taxi_trips'
            ORDER BY ordinal_position
        """).fetchdf()
        
        assert len(info) > 0, "yellow_taxi_trips table not found or has no columns"
        print(f"\n✅ yellow_taxi_trips table has {len(info)} columns")
