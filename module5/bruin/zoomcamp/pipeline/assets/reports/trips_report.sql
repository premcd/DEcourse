/* @bruin

name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips
materialization:
  type: table

  
columns:
  - name: pickup_datetime
    type: timestamp
    description: when the trip started
    primary_key: true
  - name: dropoff_datetime
    type: DATE
    description: when the trip ended
    primary_key: true
  - name: passenger_count
    type: BIGINT
    description: number of passengers
    checks:
      - name: non_negative


@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

-- agregate trips by date, taxi_type, payment_type

WITH filtered AS (
  SELECT
    pickup_datetime,
    taxi_type,
    payment_type_name,
    fare_amount
  FROM staging.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
),
aggregated AS (
  SELECT
    CAST(date_trunc('day', pickup_datetime) AS date) AS trip_date,
    taxi_type,
    COALESCE(payment_type_name, 'UNKNOWN') AS payment_type_name,
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_fare_amount,
    AVG(fare_amount) AS avg_fare_amount
  FROM filtered
  GROUP BY
    CAST(date_trunc('day', pickup_datetime) AS date),
    taxi_type,
    COALESCE(payment_type_name, 'UNKNOWN')
)
SELECT *
FROM aggregated;