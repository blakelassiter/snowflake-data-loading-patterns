-- ============================================================================
-- Testing: Data Quality Checks
-- ============================================================================
-- Validate data values during and after loading.

-- ----------------------------------------------------------------------------
-- Null checks
-- ----------------------------------------------------------------------------

-- Check required fields for nulls
SELECT 
    COUNT(*) as total_rows,
    COUNT_IF(event_id IS NULL) as null_event_id,
    COUNT_IF(event_type IS NULL) as null_event_type,
    COUNT_IF(event_timestamp IS NULL) as null_timestamp,
    COUNT_IF(user_id IS NULL) as null_user_id
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- Null percentage threshold check
WITH null_stats AS (
    SELECT 
        COUNT(*) as total,
        COUNT_IF(customer_id IS NULL) as null_customer_id,
        COUNT_IF(email IS NULL) as null_email,
        COUNT_IF(created_at IS NULL) as null_created_at
    FROM raw.customers
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    'customer_id' as column_name,
    null_customer_id as null_count,
    ROUND(null_customer_id * 100.0 / total, 2) as null_pct,
    CASE WHEN null_customer_id > 0 THEN 'FAIL - Required field' ELSE 'PASS' END as status
FROM null_stats
UNION ALL
SELECT 
    'email',
    null_email,
    ROUND(null_email * 100.0 / total, 2),
    CASE WHEN null_email * 100.0 / total > 5 THEN 'WARN - >5% null' ELSE 'PASS' END
FROM null_stats
UNION ALL
SELECT 
    'created_at',
    null_created_at,
    ROUND(null_created_at * 100.0 / total, 2),
    CASE WHEN null_created_at > 0 THEN 'FAIL - Required field' ELSE 'PASS' END
FROM null_stats;

-- ----------------------------------------------------------------------------
-- Uniqueness checks
-- ----------------------------------------------------------------------------

-- Primary key uniqueness
SELECT 
    event_id,
    COUNT(*) as occurrence_count
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 20;

-- Uniqueness summary
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT event_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT event_id) as duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT event_id) THEN 'PASS'
        ELSE 'FAIL - Duplicates found'
    END as status
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- ----------------------------------------------------------------------------
-- Range and boundary checks
-- ----------------------------------------------------------------------------

-- Numeric range validation
SELECT 
    COUNT(*) as total_rows,
    COUNT_IF(order_total < 0) as negative_totals,
    COUNT_IF(order_total > 1000000) as suspiciously_large,
    COUNT_IF(quantity <= 0) as invalid_quantities,
    MIN(order_total) as min_total,
    MAX(order_total) as max_total,
    AVG(order_total) as avg_total
FROM raw.orders
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- Date range validation
SELECT 
    COUNT(*) as total_rows,
    COUNT_IF(event_timestamp < '2020-01-01') as too_old,
    COUNT_IF(event_timestamp > CURRENT_TIMESTAMP()) as future_dates,
    MIN(event_timestamp) as earliest,
    MAX(event_timestamp) as latest
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- ----------------------------------------------------------------------------
-- Format validation
-- ----------------------------------------------------------------------------

-- Email format
SELECT 
    COUNT(*) as total,
    COUNT_IF(email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') as valid_emails,
    COUNT_IF(NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') as invalid_emails
FROM raw.customers
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
  AND email IS NOT NULL;

-- Phone format
SELECT 
    COUNT(*) as total,
    COUNT_IF(REGEXP_REPLACE(phone, '[^0-9]', '') RLIKE '^[0-9]{10,11}$') as valid_phones,
    COUNT_IF(NOT REGEXP_REPLACE(phone, '[^0-9]', '') RLIKE '^[0-9]{10,11}$') as invalid_phones
FROM raw.customers
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
  AND phone IS NOT NULL;

-- UUID format
SELECT 
    COUNT(*) as total,
    COUNT_IF(id RLIKE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') as valid_uuids,
    COUNT_IF(NOT id RLIKE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') as invalid_uuids
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- ----------------------------------------------------------------------------
-- Referential integrity checks
-- ----------------------------------------------------------------------------

-- Foreign key existence
SELECT 
    COUNT(*) as orphan_orders
FROM raw.orders o
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
  AND NOT EXISTS (
    SELECT 1 FROM raw.customers c WHERE c.customer_id = o.customer_id
);

-- Referential integrity summary
WITH integrity_check AS (
    SELECT 
        o.order_id,
        o.customer_id,
        CASE WHEN c.customer_id IS NOT NULL THEN 'VALID' ELSE 'ORPHAN' END as customer_status
    FROM raw.orders o
    LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
    WHERE o.loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    customer_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
FROM integrity_check
GROUP BY customer_status;

-- ----------------------------------------------------------------------------
-- Enumeration/allowed values checks
-- ----------------------------------------------------------------------------

-- Check for unexpected status values
SELECT 
    status,
    COUNT(*) as count
FROM raw.orders
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY status
ORDER BY count DESC;

-- Validate against allowed values
WITH allowed_statuses AS (
    SELECT 'pending' as status UNION ALL
    SELECT 'processing' UNION ALL
    SELECT 'shipped' UNION ALL
    SELECT 'delivered' UNION ALL
    SELECT 'cancelled'
)
SELECT 
    o.status,
    COUNT(*) as count,
    CASE WHEN a.status IS NOT NULL THEN 'VALID' ELSE 'INVALID' END as validation
FROM raw.orders o
LEFT JOIN allowed_statuses a ON LOWER(o.status) = a.status
WHERE o.loaded_at > dateadd('hour', -1, current_timestamp())
GROUP BY o.status, validation
ORDER BY count DESC;

-- ----------------------------------------------------------------------------
-- Statistical anomaly detection
-- ----------------------------------------------------------------------------

-- Compare batch to historical distribution
WITH current_batch AS (
    SELECT 
        AVG(order_total) as avg_total,
        STDDEV(order_total) as stddev_total,
        COUNT(*) as row_count
    FROM raw.orders
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
),
historical AS (
    SELECT 
        AVG(order_total) as avg_total,
        STDDEV(order_total) as stddev_total
    FROM raw.orders
    WHERE loaded_at BETWEEN dateadd('day', -30, current_timestamp()) 
                        AND dateadd('hour', -1, current_timestamp())
)
SELECT 
    c.avg_total as current_avg,
    h.avg_total as historical_avg,
    ABS(c.avg_total - h.avg_total) / NULLIF(h.stddev_total, 0) as z_score,
    CASE 
        WHEN ABS(c.avg_total - h.avg_total) / NULLIF(h.stddev_total, 0) > 3 THEN 'ANOMALY - >3 std devs'
        WHEN ABS(c.avg_total - h.avg_total) / NULLIF(h.stddev_total, 0) > 2 THEN 'WARN - >2 std devs'
        ELSE 'NORMAL'
    END as status
FROM current_batch c, historical h;

-- ----------------------------------------------------------------------------
-- Comprehensive data quality report
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW ops.data_quality_report AS
WITH base AS (
    SELECT * FROM raw.events
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    'raw.events' as table_name,
    CURRENT_TIMESTAMP() as check_time,
    COUNT(*) as total_rows,
    
    -- Completeness
    ROUND(COUNT_IF(event_id IS NOT NULL) * 100.0 / COUNT(*), 2) as event_id_completeness,
    ROUND(COUNT_IF(event_type IS NOT NULL) * 100.0 / COUNT(*), 2) as event_type_completeness,
    ROUND(COUNT_IF(event_timestamp IS NOT NULL) * 100.0 / COUNT(*), 2) as timestamp_completeness,
    
    -- Uniqueness
    COUNT(DISTINCT event_id) as unique_event_ids,
    COUNT(*) - COUNT(DISTINCT event_id) as duplicate_count,
    
    -- Validity
    COUNT_IF(event_timestamp > CURRENT_TIMESTAMP()) as future_timestamps,
    COUNT_IF(event_timestamp < '2020-01-01') as ancient_timestamps,
    
    -- Freshness
    MAX(event_timestamp) as latest_event,
    DATEDIFF('minute', MAX(event_timestamp), CURRENT_TIMESTAMP()) as minutes_behind
FROM base;

-- Query the report
SELECT * FROM ops.data_quality_report;

-- ----------------------------------------------------------------------------
-- Data quality score
-- ----------------------------------------------------------------------------

-- Calculate overall quality score
WITH metrics AS (
    SELECT 
        COUNT(*) as total,
        COUNT_IF(event_id IS NOT NULL) as has_id,
        COUNT_IF(event_type IS NOT NULL) as has_type,
        COUNT_IF(event_timestamp IS NOT NULL) as has_timestamp,
        COUNT_IF(event_timestamp <= CURRENT_TIMESTAMP()) as valid_timestamp,
        COUNT(DISTINCT event_id) as unique_ids
    FROM raw.events
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    ROUND((
        (has_id * 1.0 / total) * 25 +           -- 25% weight: ID completeness
        (has_type * 1.0 / total) * 20 +         -- 20% weight: Type completeness
        (has_timestamp * 1.0 / total) * 20 +    -- 20% weight: Timestamp completeness
        (valid_timestamp * 1.0 / total) * 15 +  -- 15% weight: Valid timestamps
        (unique_ids * 1.0 / total) * 20         -- 20% weight: Uniqueness
    ), 1) as quality_score,
    CASE 
        WHEN quality_score >= 95 THEN 'EXCELLENT'
        WHEN quality_score >= 85 THEN 'GOOD'
        WHEN quality_score >= 70 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as quality_grade
FROM metrics;

-- ----------------------------------------------------------------------------
-- Quality check automation
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.run_quality_checks(
    table_name STRING,
    hours_back INTEGER DEFAULT 1
)
RETURNS TABLE (check_name STRING, result STRING, status STRING)
LANGUAGE SQL
AS
$$
BEGIN
    RETURN TABLE(
        SELECT 'row_count' as check_name, 
               COUNT(*)::STRING as result,
               CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status
        FROM IDENTIFIER(:table_name)
        WHERE loaded_at > dateadd('hour', -:hours_back, current_timestamp())
    );
END;
$$;

-- Usage
-- CALL ops.run_quality_checks('raw.events', 1);
