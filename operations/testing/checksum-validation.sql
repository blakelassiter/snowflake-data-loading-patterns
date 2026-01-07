-- ============================================================================
-- Testing: Checksum Validation
-- ============================================================================
-- Hash-based verification to ensure data integrity during loading.

-- ----------------------------------------------------------------------------
-- Basic row-level hashing
-- ----------------------------------------------------------------------------

-- Compute hash of key columns for comparison
SELECT 
    event_id,
    MD5(
        COALESCE(event_id, '') || '|' ||
        COALESCE(event_type, '') || '|' ||
        COALESCE(TO_CHAR(event_timestamp), '')
    ) as row_hash
FROM raw.events
WHERE loaded_at > dateadd('hour', -1, current_timestamp())
LIMIT 10;

-- ----------------------------------------------------------------------------
-- Aggregate checksum for batch validation
-- ----------------------------------------------------------------------------

-- Hash all rows loaded in a batch, then aggregate
WITH row_hashes AS (
    SELECT 
        MD5(
            COALESCE(event_id, '') || '|' ||
            COALESCE(event_type, '') || '|' ||
            COALESCE(user_id, '') || '|' ||
            COALESCE(TO_CHAR(event_timestamp), '')
        ) as row_hash
    FROM raw.events
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    COUNT(*) as row_count,
    MD5(LISTAGG(row_hash, '') WITHIN GROUP (ORDER BY row_hash)) as batch_checksum
FROM row_hashes;

-- Compare to expected checksum from source system
-- If source provides: expected_checksum = 'abc123...'
-- CASE WHEN batch_checksum = 'abc123...' THEN 'PASS' ELSE 'FAIL' END

-- ----------------------------------------------------------------------------
-- Numeric column checksums (faster for large tables)
-- ----------------------------------------------------------------------------

-- Sum-based checksum for numeric columns
SELECT 
    COUNT(*) as row_count,
    SUM(HASH(event_id)) as id_checksum,
    SUM(COALESCE(order_total, 0)) as total_checksum,
    SUM(COALESCE(quantity, 0)) as quantity_checksum
FROM raw.orders
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- Compare against source-provided checksums
WITH loaded_checksums AS (
    SELECT 
        COUNT(*) as row_count,
        SUM(COALESCE(order_total, 0)) as total_sum
    FROM raw.orders
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
),
expected AS (
    SELECT 50000 as row_count, 2500000.00 as total_sum  -- From manifest
)
SELECT 
    l.row_count as loaded_rows,
    e.row_count as expected_rows,
    l.total_sum as loaded_total,
    e.total_sum as expected_total,
    CASE 
        WHEN l.row_count = e.row_count AND l.total_sum = e.total_sum THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM loaded_checksums l, expected e;

-- ----------------------------------------------------------------------------
-- Source file checksum comparison
-- ----------------------------------------------------------------------------

-- If source provides per-file checksums in a manifest
CREATE OR REPLACE TEMPORARY TABLE file_checksums AS
SELECT 
    $1:file_name::STRING as file_name,
    $1:row_count::INTEGER as expected_rows,
    $1:checksum::STRING as expected_checksum
FROM @external_stage/manifests/checksums.json
(FILE_FORMAT => 'json_format');

-- Compute checksums from staged files
WITH staged_checksums AS (
    SELECT 
        METADATA$FILENAME as file_name,
        COUNT(*) as row_count,
        MD5(LISTAGG(
            MD5(TO_CHAR($1)), ''
        ) WITHIN GROUP (ORDER BY METADATA$FILE_ROW_NUMBER)) as computed_checksum
    FROM @external_stage/data/
    GROUP BY file_name
)
SELECT 
    e.file_name,
    e.expected_rows,
    s.row_count as actual_rows,
    e.expected_checksum,
    s.computed_checksum,
    CASE 
        WHEN e.expected_checksum = s.computed_checksum THEN 'PASS'
        ELSE 'FAIL - Checksum mismatch'
    END as status
FROM file_checksums e
JOIN staged_checksums s ON e.file_name = s.file_name;

-- ----------------------------------------------------------------------------
-- Delta/incremental checksum validation
-- ----------------------------------------------------------------------------

-- For incremental loads, verify only new/changed records
WITH source_records AS (
    SELECT 
        $1:id::STRING as id,
        $1:updated_at::TIMESTAMP_NTZ as updated_at,
        MD5(TO_CHAR($1)) as source_hash
    FROM @external_stage/incremental/
),
target_records AS (
    SELECT 
        id,
        updated_at,
        MD5(
            COALESCE(id, '') || '|' ||
            COALESCE(name, '') || '|' ||
            COALESCE(TO_CHAR(value), '')
        ) as target_hash
    FROM raw.dimension_table
)
SELECT 
    s.id,
    s.source_hash,
    t.target_hash,
    CASE 
        WHEN s.source_hash = t.target_hash THEN 'MATCH'
        WHEN t.id IS NULL THEN 'NEW - Not in target'
        ELSE 'MISMATCH'
    END as status
FROM source_records s
LEFT JOIN target_records t ON s.id = t.id
WHERE s.source_hash != t.target_hash OR t.id IS NULL;

-- ----------------------------------------------------------------------------
-- Checksum for specific column integrity
-- ----------------------------------------------------------------------------

-- Verify PII columns weren't corrupted during load
SELECT 
    COUNT(*) as total_rows,
    COUNT_IF(LENGTH(email) > 0 AND email LIKE '%@%.%') as valid_emails,
    COUNT_IF(LENGTH(phone) = 10 OR LENGTH(phone) = 11) as valid_phones,
    COUNT_IF(LENGTH(ssn_hash) = 64) as valid_ssn_hashes  -- SHA256 = 64 chars
FROM raw.customers
WHERE loaded_at > dateadd('hour', -1, current_timestamp());

-- ----------------------------------------------------------------------------
-- Checksum procedure for automation
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.validate_checksum(
    table_name STRING,
    key_columns STRING,  -- Comma-separated
    expected_checksum STRING,
    hours_back INTEGER DEFAULT 1
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    computed_checksum STRING;
    sql_stmt STRING;
BEGIN
    -- Build dynamic SQL to compute checksum
    -- Note: This is simplified; production would need proper column handling
    
    sql_stmt := '
        SELECT MD5(LISTAGG(row_hash, '''') WITHIN GROUP (ORDER BY row_hash))
        FROM (
            SELECT MD5(' || :key_columns || ') as row_hash
            FROM ' || :table_name || '
            WHERE loaded_at > dateadd(''hour'', -' || :hours_back || ', current_timestamp())
        )';
    
    EXECUTE IMMEDIATE sql_stmt INTO computed_checksum;
    
    IF (computed_checksum = :expected_checksum) THEN
        RETURN 'PASS: Checksums match';
    ELSE
        RETURN 'FAIL: Expected ' || :expected_checksum || ' but got ' || computed_checksum;
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- Checksum audit trail
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ops.checksum_log (
    check_id INTEGER AUTOINCREMENT,
    check_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name STRING,
    load_batch STRING,
    expected_checksum STRING,
    computed_checksum STRING,
    row_count INTEGER,
    status STRING
);

-- Log checksum validation
INSERT INTO ops.checksum_log (
    table_name, 
    load_batch, 
    expected_checksum, 
    computed_checksum, 
    row_count, 
    status
)
WITH computed AS (
    SELECT 
        COUNT(*) as row_count,
        MD5(LISTAGG(MD5(event_id || event_type), '') 
            WITHIN GROUP (ORDER BY event_id)) as checksum
    FROM raw.events
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
)
SELECT 
    'raw.events',
    'batch_2025_01_15_001',
    'expected_abc123',  -- From manifest
    checksum,
    row_count,
    CASE WHEN checksum = 'expected_abc123' THEN 'PASS' ELSE 'FAIL' END
FROM computed;

-- Review checksum history
SELECT * FROM ops.checksum_log
WHERE check_time > dateadd('day', -7, current_timestamp())
ORDER BY check_time DESC;

-- ----------------------------------------------------------------------------
-- Performance considerations
-- ----------------------------------------------------------------------------

/*
Checksum computation can be expensive on large tables:

1. Hash individual rows then aggregate (more accurate, slower)
2. Sum numeric columns (faster, less comprehensive)
3. Sample-based validation (fastest, statistical confidence)

For very large loads:
- Compute checksums during COPY INTO transformation
- Store row hash as a column
- Validate aggregates rather than full checksums
*/

-- Sample-based validation for large tables
SELECT 
    COUNT(*) as sample_size,
    MD5(LISTAGG(row_hash, '') WITHIN GROUP (ORDER BY row_hash)) as sample_checksum
FROM (
    SELECT MD5(event_id || event_type) as row_hash
    FROM raw.events SAMPLE (1)  -- 1% sample
    WHERE loaded_at > dateadd('hour', -1, current_timestamp())
);
