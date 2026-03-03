/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
#name: TODO_SET_ASSET_NAME
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
#type: TODO

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
#depends:
#  - TODO_DEP_1
#  - TODO_DEP_2

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
#materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  #type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  #strategy: TODO
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  #incremental_key: TODO_SET_INCREMENTAL_KEY
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  #time_granularity: TODO_SET_GRANULARITY

# TODO: Define output columns, mark primary keys, and add a few checks.
#columns:
#  - name: TODO_pk1
#    type: TODO
#    description: TODO
#    primary_key: true
#    nullable: false
#    checks:
#      - name: not_null
#  - name: TODO_metric
#    type: TODO
#    description: TODO
#    checks:
#      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
#custom_checks:
#  - name: TODO_custom_check_name
#    description: TODO
#    query: |
#      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
#      SELECT 0
#    value: 0
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
#  strategy: time_interval
#  incremental_key: pickup_datetime
#  time_granularity: day

#custom_checks:
#  - name: no_duplicates
#    description: no duplicates in the pickup_datetime column
#    query: |
#      SELECT COUNT(*)
#      FROM staging.trips
#      GROUP BY pickup_datetime
#      HAVING COUNT(*) > 1
#    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.


WITH base AS (
  SELECT
    -- Normalize timestamps and important fields from ingestion
    t.tpep_pickup_datetime  AS pickup_datetime,
    t.tpep_dropoff_datetime AS dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.payment_type          AS payment_type_id,
    t.taxi_type,
    t.extracted_at,
    ROW_NUMBER() OVER (
      PARTITION BY
        t.tpep_pickup_datetime,
        t.tpep_dropoff_datetime,
        t.payment_type,
        t.taxi_type,
        t.fare_amount,
        t.trip_distance,
        t.passenger_count
      ORDER BY
        t.extracted_at DESC
    ) AS rn
  FROM ingestion.trips AS t
),
deduped AS (
  SELECT *
  FROM base
  WHERE rn = 1
),
enriched AS (
  SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.passenger_count,
    d.trip_distance,
    d.fare_amount,
    d.payment_type_id,
    l.payment_type_name,
    d.taxi_type
  FROM deduped d
  LEFT JOIN ingestion.payment_lookup l
    ON d.payment_type_id = l.payment_type_id
)
SELECT *
FROM enriched;