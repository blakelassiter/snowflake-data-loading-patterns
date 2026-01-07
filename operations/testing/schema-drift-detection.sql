-- ============================================================================
-- Testing: Schema Drift Detection
-- ============================================================================
-- Detect unexpected changes in source data structure before they break loads.

-- ----------------------------------------------------------------------------
-- Compare inferred schema to target table
-- ----------------------------------------------------------------------------

-- Detect new, missing, or changed columns
WITH source_schema AS (
    SELECT 
        UPPER(COLUMN_NAME) as column_name,
        TYPE as data_type,
        NULLABLE,
        ORDER_ID as ordinal_position
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@external_stage/data/',
            FILE_FORMAT => 'parquet_format'
        )
    )
),
target_schema AS (
    SELECT 
        UPPER(column_name) as column_name,
        data_type,
        is_nullable = 'YES' as nullable,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = 'RAW' AND table_name = 'EVENTS'
)
SELECT 
    COALESCE(s.column_name, t.column_name) as column_name,
    s.data_type as source_type,
    t.data_type as target_type,
    s.nullable as source_nullable,
    t.nullable as target_nullable,
    CASE 
        WHEN s.column_name IS NULL THEN 'REMOVED - Column no longer in source'
        WHEN t.column_name IS NULL THEN 'NEW - Column added to source'
        WHEN s.data_type != t.data_type THEN 'TYPE_CHANGE'
        WHEN s.nullable != t.nullable THEN 'NULLABLE_CHANGE'
        ELSE 'OK'
    END as drift_status
FROM source_schema s
FULL OUTER JOIN target_schema t ON s.column_name = t.column_name
ORDER BY COALESCE(s.ordinal_position, t.ordinal_position);

-- Filter to only show drifts
-- WHERE drift_status != 'OK'

-- ----------------------------------------------------------------------------
-- Track schema over time
-- ----------------------------------------------------------------------------

-- Store schema snapshots for comparison
CREATE TABLE IF NOT EXISTS ops.schema_snapshots (
    snapshot_id INTEGER AUTOINCREMENT,
    snapshot_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_location STRING,
    column_name STRING,
    data_type STRING,
    nullable BOOLEAN,
    ordinal_position INTEGER
);

-- Capture current schema
INSERT INTO ops.schema_snapshots (source_location, column_name, data_type, nullable, ordinal_position)
SELECT 
    '@external_stage/data/',
    COLUMN_NAME,
    TYPE,
    NULLABLE,
    ORDER_ID
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@external_stage/data/',
        FILE_FORMAT => 'parquet_format'
    )
);

-- Compare to previous snapshot
WITH current_schema AS (
    SELECT column_name, data_type, nullable
    FROM ops.schema_snapshots
    WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM ops.schema_snapshots)
),
previous_schema AS (
    SELECT column_name, data_type, nullable
    FROM ops.schema_snapshots
    WHERE snapshot_time = (
        SELECT MAX(snapshot_time) 
        FROM ops.schema_snapshots 
        WHERE snapshot_time < (SELECT MAX(snapshot_time) FROM ops.schema_snapshots)
    )
)
SELECT 
    COALESCE(c.column_name, p.column_name) as column_name,
    p.data_type as previous_type,
    c.data_type as current_type,
    CASE 
        WHEN p.column_name IS NULL THEN 'NEW'
        WHEN c.column_name IS NULL THEN 'REMOVED'
        WHEN p.data_type != c.data_type THEN 'TYPE_CHANGED'
        ELSE 'UNCHANGED'
    END as change_type
FROM current_schema c
FULL OUTER JOIN previous_schema p ON c.column_name = p.column_name
WHERE c.column_name IS NULL 
   OR p.column_name IS NULL 
   OR p.data_type != c.data_type;

-- ----------------------------------------------------------------------------
-- JSON schema drift detection
-- ----------------------------------------------------------------------------

-- For JSON data, detect changes in structure
WITH json_keys AS (
    SELECT DISTINCT 
        f.key as column_name,
        TYPEOF(f.value) as data_type
    FROM @external_stage/json_data/,
    LATERAL FLATTEN(input => $1) f
    LIMIT 10000  -- Sample for performance
)
SELECT 
    column_name,
    data_type,
    COUNT(*) as occurrence_count
FROM json_keys
GROUP BY column_name, data_type
ORDER BY column_name;

-- Detect nested structure changes
WITH nested_paths AS (
    SELECT DISTINCT 
        f.path as json_path,
        TYPEOF(f.value) as value_type
    FROM @external_stage/json_data/,
    LATERAL FLATTEN(input => $1, recursive => TRUE) f
    LIMIT 100000
)
SELECT 
    json_path,
    LISTAGG(DISTINCT value_type, ', ') as value_types,
    COUNT(*) as occurrences
FROM nested_paths
GROUP BY json_path
ORDER BY json_path;

-- ----------------------------------------------------------------------------
-- Alert on schema changes
-- ----------------------------------------------------------------------------

CREATE OR REPLACE ALERT ops.alert_schema_drift
    WAREHOUSE = ops_wh
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Daily at 6 AM
    IF (EXISTS (
        WITH source_schema AS (
            SELECT UPPER(COLUMN_NAME) as column_name, TYPE as data_type
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '@external_stage/data/',
                    FILE_FORMAT => 'parquet_format'
                )
            )
        ),
        target_schema AS (
            SELECT UPPER(column_name) as column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'RAW' AND table_name = 'EVENTS'
        )
        SELECT 1
        FROM source_schema s
        FULL OUTER JOIN target_schema t ON s.column_name = t.column_name
        WHERE s.column_name IS NULL 
           OR t.column_name IS NULL 
           OR s.data_type != t.data_type
    ))
    THEN
        CALL system$send_email(
            'data_alerts',
            'data-team@company.com',
            'Schema Drift Detected',
            'Source schema for raw.events has changed. Review before next load.'
        );

ALTER ALERT ops.alert_schema_drift RESUME;

-- ----------------------------------------------------------------------------
-- Automated schema evolution
-- ----------------------------------------------------------------------------

-- For additive changes (new columns), automatically update target table
CREATE OR REPLACE PROCEDURE ops.handle_schema_drift(
    source_location STRING,
    target_table STRING,
    file_format STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    new_columns RESULTSET;
    col_name STRING;
    col_type STRING;
    alter_stmt STRING;
    changes_made INTEGER DEFAULT 0;
BEGIN
    -- Find new columns
    new_columns := (
        WITH source_schema AS (
            SELECT UPPER(COLUMN_NAME) as column_name, TYPE as data_type
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => :source_location,
                    FILE_FORMAT => :file_format
                )
            )
        ),
        target_schema AS (
            SELECT UPPER(column_name) as column_name
            FROM information_schema.columns
            WHERE table_name = UPPER(SPLIT_PART(:target_table, '.', -1))
        )
        SELECT s.column_name, s.data_type
        FROM source_schema s
        LEFT JOIN target_schema t ON s.column_name = t.column_name
        WHERE t.column_name IS NULL
    );
    
    -- Add new columns
    FOR record IN new_columns DO
        col_name := record.column_name;
        col_type := record.data_type;
        alter_stmt := 'ALTER TABLE ' || :target_table || ' ADD COLUMN ' || col_name || ' ' || col_type;
        
        EXECUTE IMMEDIATE alter_stmt;
        changes_made := changes_made + 1;
    END FOR;
    
    RETURN 'Added ' || changes_made || ' new columns';
END;
$$;

-- Usage (be careful with automatic schema changes!)
-- CALL ops.handle_schema_drift('@stage/data/', 'raw.events', 'parquet_format');

-- ----------------------------------------------------------------------------
-- Schema validation before load
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ops.validate_schema_before_load(
    source_location STRING,
    target_table STRING,
    file_format STRING,
    allow_new_columns BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    drift_count INTEGER;
    new_column_count INTEGER;
BEGIN
    -- Check for breaking changes (removed columns, type changes)
    SELECT COUNT(*) INTO drift_count
    FROM (
        WITH source_schema AS (
            SELECT UPPER(COLUMN_NAME) as column_name, TYPE as data_type
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => :source_location,
                    FILE_FORMAT => :file_format
                )
            )
        ),
        target_schema AS (
            SELECT UPPER(column_name) as column_name, data_type
            FROM information_schema.columns
            WHERE table_name = UPPER(SPLIT_PART(:target_table, '.', -1))
        )
        SELECT 1
        FROM target_schema t
        LEFT JOIN source_schema s ON t.column_name = s.column_name
        WHERE s.column_name IS NULL  -- Required column missing from source
           OR s.data_type != t.data_type  -- Type mismatch
    );
    
    IF (drift_count > 0) THEN
        RETURN 'FAIL: ' || drift_count || ' breaking schema changes detected';
    END IF;
    
    -- Check for new columns
    SELECT COUNT(*) INTO new_column_count
    FROM (
        WITH source_schema AS (
            SELECT UPPER(COLUMN_NAME) as column_name
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => :source_location,
                    FILE_FORMAT => :file_format
                )
            )
        ),
        target_schema AS (
            SELECT UPPER(column_name) as column_name
            FROM information_schema.columns
            WHERE table_name = UPPER(SPLIT_PART(:target_table, '.', -1))
        )
        SELECT 1
        FROM source_schema s
        LEFT JOIN target_schema t ON s.column_name = t.column_name
        WHERE t.column_name IS NULL
    );
    
    IF (new_column_count > 0 AND NOT :allow_new_columns) THEN
        RETURN 'WARN: ' || new_column_count || ' new columns in source (not blocking)';
    END IF;
    
    RETURN 'PASS: Schema compatible';
END;
$$;

-- Usage
-- CALL ops.validate_schema_before_load('@stage/data/', 'raw.events', 'parquet_format', FALSE);

-- ----------------------------------------------------------------------------
-- Schema drift log
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ops.schema_drift_log (
    drift_id INTEGER AUTOINCREMENT,
    detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_location STRING,
    target_table STRING,
    column_name STRING,
    change_type STRING,  -- NEW, REMOVED, TYPE_CHANGED
    old_value STRING,
    new_value STRING,
    action_taken STRING,
    resolved_at TIMESTAMP_NTZ
);

-- Log detected drifts
INSERT INTO ops.schema_drift_log (
    source_location, target_table, column_name, change_type, old_value, new_value
)
SELECT 
    '@external_stage/data/',
    'raw.events',
    COALESCE(s.column_name, t.column_name),
    CASE 
        WHEN s.column_name IS NULL THEN 'REMOVED'
        WHEN t.column_name IS NULL THEN 'NEW'
        ELSE 'TYPE_CHANGED'
    END,
    t.data_type,
    s.data_type
FROM (
    SELECT UPPER(COLUMN_NAME) as column_name, TYPE as data_type
    FROM TABLE(INFER_SCHEMA(LOCATION => '@external_stage/data/', FILE_FORMAT => 'parquet_format'))
) s
FULL OUTER JOIN (
    SELECT UPPER(column_name) as column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'RAW' AND table_name = 'EVENTS'
) t ON s.column_name = t.column_name
WHERE s.column_name IS NULL OR t.column_name IS NULL OR s.data_type != t.data_type;
