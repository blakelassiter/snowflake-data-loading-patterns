-- ============================================================================
-- External Tables: Materialized Views
-- ============================================================================
-- Materialized views over external tables cache frequently-queried data
-- in Snowflake storage, dramatically improving query performance while
-- keeping the source data external.

-- ----------------------------------------------------------------------------
-- Basic materialized view over external table
-- ----------------------------------------------------------------------------

-- Create materialized view for recent/hot data
CREATE OR REPLACE MATERIALIZED VIEW lake.events_recent_mv
AS
SELECT 
    event_id,
    event_type,
    user_id,
    event_timestamp,
    payload
FROM lake.events_partitioned
WHERE year = 2025 AND month >= 1;

-- Query the MV (uses cached data, fast)
SELECT * FROM lake.events_recent_mv
WHERE event_type = 'purchase'
LIMIT 100;

-- Query the external table directly (reads from S3, slower)
SELECT * FROM lake.events_partitioned
WHERE year = 2025 AND month >= 1 AND event_type = 'purchase'
LIMIT 100;

-- ----------------------------------------------------------------------------
-- Aggregated materialized view
-- ----------------------------------------------------------------------------

-- Pre-compute aggregations for dashboard queries
CREATE OR REPLACE MATERIALIZED VIEW lake.events_daily_summary_mv
AS
SELECT 
    year,
    month,
    day,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM lake.events_partitioned
WHERE year >= 2024
GROUP BY year, month, day, event_type;

-- Dashboard query hits MV (milliseconds)
SELECT 
    year, month, day,
    SUM(event_count) as total_events,
    SUM(unique_users) as total_users
FROM lake.events_daily_summary_mv
WHERE year = 2025
GROUP BY year, month, day
ORDER BY year, month, day;

-- ----------------------------------------------------------------------------
-- Filtered materialized view (hot data pattern)
-- ----------------------------------------------------------------------------

-- Cache only the most recent 30 days
CREATE OR REPLACE MATERIALIZED VIEW lake.events_hot_mv
AS
SELECT *
FROM lake.events_partitioned
WHERE event_date >= DATEADD('day', -30, CURRENT_DATE());

-- Note: The MV refreshes automatically when base data changes
-- but the filter (30 days) is evaluated at refresh time, not query time

-- ----------------------------------------------------------------------------
-- Materialized view with transformations
-- ----------------------------------------------------------------------------

-- Flatten nested JSON during materialization
CREATE OR REPLACE MATERIALIZED VIEW lake.events_flattened_mv
AS
SELECT 
    event_id,
    event_type,
    user_id,
    event_timestamp,
    payload:device:type::STRING as device_type,
    payload:device:os::STRING as device_os,
    payload:location:country::STRING as country,
    payload:location:city::STRING as city
FROM lake.events_partitioned
WHERE year >= 2024;

-- Query flattened data efficiently
SELECT 
    country,
    device_type,
    COUNT(*) as events
FROM lake.events_flattened_mv
GROUP BY country, device_type
ORDER BY events DESC;

-- ----------------------------------------------------------------------------
-- Refresh behavior
-- ----------------------------------------------------------------------------

-- Check MV refresh status
SHOW MATERIALIZED VIEWS LIKE 'events_recent_mv';

-- Manual refresh (if needed)
ALTER MATERIALIZED VIEW lake.events_recent_mv REFRESH;

-- Suspend automatic refresh (for maintenance)
ALTER MATERIALIZED VIEW lake.events_recent_mv SUSPEND;

-- Resume automatic refresh
ALTER MATERIALIZED VIEW lake.events_recent_mv RESUME;

-- ----------------------------------------------------------------------------
-- Cost considerations
-- ----------------------------------------------------------------------------

/*
Materialized views add costs:

1. Storage: MV data is stored in Snowflake
   - Charged at standard Snowflake rates
   - Size depends on data volume and compression

2. Refresh compute: Warehouse time for refreshes
   - Automatic refresh uses background compute
   - Charged per-second of compute

3. When MVs make sense:
   - Query frequency is high
   - Query patterns are predictable (same filters/aggregations)
   - External table scans are expensive (large data, complex joins)
   - Data changes infrequently relative to query frequency

4. When to skip MVs:
   - Ad-hoc queries with varying filters
   - Data changes frequently (high refresh cost)
   - External table queries are already fast enough
   - Storage cost exceeds query cost savings
*/

-- ----------------------------------------------------------------------------
-- Check MV storage and refresh costs
-- ----------------------------------------------------------------------------

-- Storage used by MVs
SELECT 
    table_name,
    bytes / 1e9 as gb_stored
FROM information_schema.tables
WHERE table_type = 'MATERIALIZED VIEW'
  AND table_schema = 'LAKE';

-- Refresh history (Account Usage)
SELECT 
    materialized_view_name,
    DATE_TRUNC('day', start_time) as day,
    COUNT(*) as refresh_count,
    SUM(credits_used) as total_credits
FROM snowflake.account_usage.materialized_view_refresh_history
WHERE start_time > dateadd('day', -30, current_date())
GROUP BY materialized_view_name, day
ORDER BY day DESC;

-- ----------------------------------------------------------------------------
-- Layered caching strategy
-- ----------------------------------------------------------------------------

-- Pattern: External table → MV for recent data → native table for hot data

-- Layer 1: External table (all historical data)
-- lake.events_partitioned covers years of data, cold storage cost

-- Layer 2: MV for recent data (last year)
CREATE OR REPLACE MATERIALIZED VIEW lake.events_year_mv
AS
SELECT * FROM lake.events_partitioned
WHERE year >= YEAR(CURRENT_DATE()) - 1;

-- Layer 3: Native table for hot data (loaded daily)
CREATE OR REPLACE TABLE raw.events_current AS
SELECT * FROM lake.events_partitioned
WHERE event_date >= DATEADD('day', -7, CURRENT_DATE());

-- Query routing (application layer decides):
-- Last 7 days: raw.events_current (fastest)
-- Last year: lake.events_year_mv (fast)
-- Historical: lake.events_partitioned (slowest but complete)

-- ----------------------------------------------------------------------------
-- Limitations
-- ----------------------------------------------------------------------------

/*
Materialized views over external tables have limitations:

1. No joins in MV definition
   - MV must query single external table
   - Join in consuming queries instead

2. Limited function support
   - Some functions not allowed in MV definitions
   - Check documentation for current restrictions

3. Refresh latency
   - Not real-time; refresh takes time
   - Queries may see stale data between refreshes

4. Can't use MVs when:
   - Need real-time data (use Snowpipe instead)
   - Query patterns are unpredictable
   - External table already meets performance needs
*/
