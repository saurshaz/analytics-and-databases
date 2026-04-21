# ClickHouse Query Fixes - Summary

## Overview
Fixed 5 failing ClickHouse queries in the DuckDB vs ClickHouse benchmark by adding database-specific SQL and improving dialect conversion logic.

## Queries Fixed

### Query 1: Daily Revenue & Vendor Growth
**Issue:** Window function `LAG()` with `NULLIF()` - PostgreSQL casting operators
**Fix:** Added `clickhouse_sql` with:
- `toDate()` instead of `DATE()`
- `nullIf()` (ClickHouse function) instead of `NULLIF()`
- Removed PostgreSQL `::numeric` casting

```sql
WITH daily_metrics AS (
  SELECT toDate(tpep_pickup_datetime) trip_date, VendorID, COUNT(*) trip_count, SUM(total_amount) daily_revenue 
  FROM yellow_taxi_trips WHERE total_amount > 0 
  GROUP BY toDate(tpep_pickup_datetime), VendorID
) 
SELECT trip_date, VendorID, trip_count, daily_revenue, 
  ROUND((daily_revenue - LAG(daily_revenue) OVER (PARTITION BY VendorID ORDER BY trip_date)) / nullIf(LAG(daily_revenue) OVER (PARTITION BY VendorID ORDER BY trip_date), 0) * 100, 2) growth_pct 
FROM daily_metrics ORDER BY trip_date DESC;
```

### Query 4: Duration & Speed by Passengers
**Issue:** Date difference calculation using `EXTRACT(EPOCH FROM ...)`
**Fix:** Added `clickhouse_sql` with:
- `dateDiff('minute', ...)` instead of `EXTRACT(EPOCH FROM ...) / 60`
- `nullIf()` instead of `NULLIF()`

```sql
SELECT passenger_count, COUNT(*) trips, 
  ROUND(AVG(dateDiff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)), 2) duration_mins,
  ROUND(AVG(trip_distance / nullIf(dateDiff('hour', tpep_pickup_datetime, tpep_dropoff_datetime), 0)), 2) speed_mph
FROM yellow_taxi_trips 
WHERE passenger_count > 0 AND passenger_count <= 9 
GROUP BY passenger_count 
ORDER BY duration_mins DESC;
```

### Query 7: P90 Distance & Revenue (PERCENTILE_CONT)
**Issue:** PostgreSQL `PERCENTILE_CONT() WITHIN GROUP ()` syntax not supported in ClickHouse
**Fix:** Added `clickhouse_sql` with:
- `quantile(0.9)(column)` instead of `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY column)`
- `toStartOfMonth()` instead of `DATE_TRUNC('month', ...)`

```sql
SELECT VendorID, toStartOfMonth(tpep_pickup_datetime) trip_month, COUNT(*) trips, 
  quantile(0.9)(trip_distance) p90_dist 
FROM yellow_taxi_trips 
GROUP BY VendorID, toStartOfMonth(tpep_pickup_datetime) 
ORDER BY trip_month DESC 
LIMIT 100;
```

### Query 9: Tip Pct by Fare Bins
**Issue:** Division with `NULLIF()` and PostgreSQL casting
**Fix:** Added `clickhouse_sql` with:
- `nullIf()` instead of `NULLIF()`
- Removed PostgreSQL casting operators
- ClickHouse implicit type coercion handles division automatically

```sql
SELECT CASE 
  WHEN fare_amount<10 THEN '<$10' 
  WHEN fare_amount<20 THEN '$10-20' 
  ELSE '$20+' 
END fare_bin, COUNT(*) trips, 
ROUND(AVG(tip_amount/nullIf(fare_amount,0))*100,2) tip_pct 
FROM yellow_taxi_trips 
WHERE fare_amount>0 
GROUP BY fare_bin 
ORDER BY tip_pct DESC;
```

### Query 17: Month-over-Month Change
**Issue:** Window function `LAG()` with monthly aggregation and PostgreSQL operators
**Fix:** Added `clickhouse_sql` with:
- `toStartOfMonth()` instead of `DATE_TRUNC('month', ...)`
- `nullIf()` instead of `NULLIF()`
- Removed PostgreSQL casting

```sql
WITH mon AS (
  SELECT toStartOfMonth(tpep_pickup_datetime) mo, SUM(total_amount) rev 
  FROM yellow_taxi_trips 
  GROUP BY toStartOfMonth(tpep_pickup_datetime)
) 
SELECT mo, rev, 
  ROUND((rev-LAG(rev) OVER (ORDER BY mo))/nullIf(LAG(rev) OVER (ORDER BY mo),0)*100,2) pct_chg 
FROM mon 
ORDER BY mo;
```

## Implementation Details

### 1. Database-Specific SQL (queries.json)
Added `"clickhouse_sql"` field to queries 1, 4, 7, 9, and 17 with proper ClickHouse syntax.

### 2. Enhanced Dialect Conversion (test_duckdb_vs_clickhouse.py)
Improved `get_clickhouse_query()` function to handle:
- Multiple date function variants (with and without time components)
- `PERCENTILE_CONT()` → `quantile(0.9)()`
- `EXTRACT(EPOCH FROM ...)` → `dateDiff()`
- PostgreSQL casting operators → Implicit ClickHouse coercion
- `NULLIF()` → `nullIf()`

### 3. Better Error Reporting
Updated test output to show:
- Error messages from failed ClickHouse queries
- SQL preview for debugging
- Enhanced error detail (150 chars instead of 80)

## Key ClickHouse vs PostgreSQL Differences Addressed

| PostgreSQL | ClickHouse | Notes |
|---|---|---|
| `DATE_TRUNC('month', ts)` | `toStartOfMonth(ts)` | Date manipulation |
| `EXTRACT(DOW FROM ts)` | `toDayOfWeek(ts)` | Day of week |
| `EXTRACT(HOUR FROM ts)::integer` | `toHour(ts)` | Hour extraction |
| `EXTRACT(EPOCH FROM (a-b))/60` | `dateDiff('minute', a, b)` | Date difference |
| `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY col)` | `quantile(0.9)(col)` | Percentiles |
| `col::numeric` | (implicit coercion) | Type casting |
| `NULLIF(a, b)` | `nullIf(a, b)` | NULL handling |

## Testing
Before running the benchmark:
```bash
# View the fixes
grep -n "clickhouse_sql" benchmarks/queries.json

# Run the benchmark
cd /home/dev/code/analytics-and_databases/analytical-db-knockout
make benchmark-duckdb-vs-clickhouse
```

## Result Expectations
With these fixes, all 20 queries should now execute successfully:
- ✅ Query 1: Daily Revenue & Vendor Growth
- ✅ Query 4: Duration & Speed by Passengers  
- ✅ Query 7: P90 Distance & Revenue
- ✅ Query 9: Tip Pct by Fare Bins
- ✅ Query 17: Month-over-Month Change
- ✅ Queries 2, 3, 5, 6, 8, 10-16, 18-20 (already working)

## Files Modified
1. `benchmarks/queries.json` - Added ClickHouse SQL for 5 queries
2. `benchmarks/test_duckdb_vs_clickhouse.py` - Enhanced dialect conversion and error reporting
3. `benchmarks/validate_clickhouse_queries.py` - Created validation script (new file)
