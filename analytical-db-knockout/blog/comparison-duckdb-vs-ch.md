
## ANALYTICAL-DB-KNOCKOUT: DuckDB vs ClickHouse Benchmark
----

Project code at https://github.com/saurshaz/analytics-and-databases 
----
[Query 1] Daily Revenue & Vendor Growth
- Complexity: High
- DuckDB ✅: 0.851s (±0.507s) [3380 rows]
- ClickHouse ✅: 0.571s (±0.049s) [3380 rows]
- 🏆 ClickHouse is 1.49x faster

[Query 2] Hourly Peak Demand
- Complexity: Medium
- DuckDB ✅: 0.556s (±0.044s) [26345 rows]
- ClickHouse ✅: 0.434s (±0.004s) [26345 rows]
- 🏆 ClickHouse is 1.28x faster

[Query 3] Top 10 Routes by Revenue
- Complexity: Medium
- DuckDB ✅: 0.603s (±0.096s) [10 rows]
- ClickHouse ✅: 0.748s (±0.055s) [10 rows]
- 🏆 DuckDB is 1.24x faster

[Query 4] Duration & Speed by Passengers
- Complexity: High
- DuckDB ✅: 1.203s (±0.140s) [9 rows]
- ClickHouse ✅: 0.837s (±0.014s) [9 rows]
- 🏆 ClickHouse is 1.44x faster

[Query 5] Payment Type & Tips
- Complexity: Medium
- DuckDB ✅: 0.311s (±0.090s) [2 rows]
- ClickHouse ✅: 0.580s (±0.034s) [2 rows]
- 🏆 DuckDB is 1.87x faster

[Query 6] Monthly Revenue Trends
- Complexity: High
- DuckDB ✅: 1.058s (±0.034s) [46 rows]
- ClickHouse ✅: 0.354s (±0.017s) [46 rows]
- 🏆 ClickHouse is 2.99x faster

[Query 7] P90 Distance & Revenue
- Complexity: High
- DuckDB ✅: 2.109s (±0.043s) [100 rows]
- ClickHouse ✅: 0.641s (±0.015s) [100 rows]
- 🏆 ClickHouse is 3.29x faster

[Query 8] Weekend vs Weekday
- Complexity: Medium
- DuckDB ✅: 0.357s (±0.006s) [2 rows]
- ClickHouse ✅: 0.503s (±0.041s) [2 rows]
- 🏆 DuckDB is 1.41x faster

[Query 9] Tip Pct by Fare Bins
- Complexity: High
- DuckDB ✅: 1.138s (±0.042s) [3 rows]
- ClickHouse ✅: 0.851s (±0.027s) [3 rows]
- 🏆 ClickHouse is 1.34x faster

[Query 10] Top Routes
- Complexity: Medium
- DuckDB ✅: 0.557s (±0.040s) [20 rows]
- ClickHouse ✅: 0.726s (±0.025s) [20 rows]
- 🏆 DuckDB is 1.30x faster

[Query 11] Vendor Performance
- Complexity: Medium
- DuckDB ✅: 0.257s (±0.007s) [4 rows]
- ClickHouse ✅: 0.522s (±0.031s) [4 rows]
- 🏆 DuckDB is 2.03x faster

[Query 12] Fee Impact
- Complexity: Medium
- DuckDB ✅: 0.656s (±0.039s) [2 rows]
- ClickHouse ✅: 0.523s (±0.034s) [2 rows]
- 🏆 ClickHouse is 1.25x faster

[Query 13] Revenue Quintiles
- Complexity: High
- DuckDB ✅: 2.310s (±0.111s) [5 rows]
- ClickHouse ✅: 4.175s (±0.225s) [5 rows]
- 🏆 DuckDB is 1.81x faster

[Query 14] Heatmap 24x7
- Complexity: High
- DuckDB ✅: 0.369s (±0.006s) [168 rows]
- ClickHouse ✅: 0.223s (±0.009s) [168 rows]
- 🏆 ClickHouse is 1.66x faster

[Query 15] Airport Trips
- Complexity: Medium
- DuckDB ✅: 0.522s (±0.030s) [2 rows]
- ClickHouse ✅: 0.393s (±0.017s) [2 rows]
- 🏆 ClickHouse is 1.33x faster

[Query 16] Rate Codes
- Complexity: Medium
- DuckDB ✅: 0.426s (±0.053s) [7 rows]
- ClickHouse ✅: 0.278s (±0.018s) [8 rows]
- 🏆 ClickHouse is 1.53x faster

[Query 17] Month-over-Month Change
- Complexity: High
- DuckDB ✅: 1.045s (±0.038s) [46 rows]
- ClickHouse ✅: 0.344s (±0.028s) [46 rows]
- 🏆 ClickHouse is 3.04x faster

[Query 18] Revenue per Mile
- Complexity: High
- DuckDB ✅: 1.296s (±0.012s) [127 rows]
- ClickHouse ✅: 0.771s (±0.022s) [127 rows]
- 🏆 ClickHouse is 1.68x faster

[Query 19] Cumulative Revenue
- Complexity: High
- DuckDB ✅: 0.400s (±0.005s) [3380 rows]
- ClickHouse ✅: 0.526s (±0.024s) [3380 rows]
- 🏆 DuckDB is 1.32x faster

[Query 20] Top 5% vs Rest
- Complexity: High
- DuckDB ✅: 2.393s (±0.125s) [2 rows]
- ClickHouse ✅: 5.275s (±0.279s) [2 rows]
- 🏆 DuckDB is 2.20x faster


----
#### PERFORMANCE SUMMARY
----

##### Top 5 Queries Where DuckDB Wins:
  - 2.20x - Top 5% vs Rest
  - 2.03x - Vendor Performance
  - 1.87x - Payment Type & Tips
  - 1.81x - Revenue Quintiles
  - 1.41x - Weekend vs Weekday

##### Top 5 Queries Where ClickHouse Wins:
  - 3.29x - P90 Distance & Revenue
  - 3.04x - Month-over-Month Change
  - 2.99x - Monthly Revenue Trends
  - 1.68x - Revenue per Mile
  - 1.66x - Heatmap 24x7

#### 📊 Aggregate Statistics:
##### DuckDB:
  - Total time: 18.418s
  - Average query: 0.921s
  - Queries: 20/20

##### ClickHouse:
  - Total time: 19.276s
  - Average query: 0.964s
  - Queries: 20/20

- 🏆 DuckDB is 1.05x faster overall
----