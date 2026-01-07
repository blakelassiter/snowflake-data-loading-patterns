-- ============================================================================
-- COPY INTO: Basic Usage Patterns
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Basic load from external stage
-- ----------------------------------------------------------------------------

COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Load with common options
-- ----------------------------------------------------------------------------

COPY INTO target_schema.target_table
FROM @stage_name/path/
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*[.]json'
ON_ERROR = 'CONTINUE'           -- Also: SKIP_FILE, ABORT_STATEMENT
-- VALIDATION_MODE = 'RETURN_ERRORS'  -- Uncomment to test before loading
-- PURGE = TRUE                  -- Uncomment to delete files after successful load
-- FORCE = TRUE                  -- Uncomment to reload previously loaded files
;

-- ----------------------------------------------------------------------------
-- Load specific files (not pattern matching)
-- ----------------------------------------------------------------------------

COPY INTO raw.orders
FROM @external_stage/orders/
FILES = ('orders_2025_01_01.parquet', 'orders_2025_01_02.parquet')
FILE_FORMAT = (TYPE = 'PARQUET');

-- ----------------------------------------------------------------------------
-- Load from internal stage
-- ----------------------------------------------------------------------------

-- First upload files to internal stage
PUT file:///local/path/data.csv @~;

-- Then load from user stage
COPY INTO raw.uploads
FROM @~
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- ----------------------------------------------------------------------------
-- Load from table stage
-- ----------------------------------------------------------------------------

-- Every table has an implicit stage: @%table_name
COPY INTO raw.customers
FROM @%customers
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|');

-- ----------------------------------------------------------------------------
-- CSV with options
-- ----------------------------------------------------------------------------

COPY INTO raw.legacy_data
FROM @external_stage/legacy/
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = '\\'
)
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- JSON load
-- ----------------------------------------------------------------------------

COPY INTO raw.api_responses
FROM @external_stage/api/
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE    -- If file contains JSON array at root
)
ON_ERROR = 'SKIP_FILE';

-- ----------------------------------------------------------------------------
-- Parquet with metadata columns
-- ----------------------------------------------------------------------------

-- Snowflake can capture file-level metadata during load
COPY INTO raw.events (
    event_id,
    event_type,
    event_timestamp,
    payload,
    _loaded_at,
    _source_file
)
FROM (
    SELECT 
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:payload::VARIANT,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @external_stage/events/
)
FILE_FORMAT = (TYPE = 'PARQUET');

-- ----------------------------------------------------------------------------
-- Load with size limit
-- ----------------------------------------------------------------------------

-- Limit bytes loaded per statement (useful for testing or incremental loads)
COPY INTO raw.large_dataset
FROM @external_stage/large/
FILE_FORMAT = (TYPE = 'PARQUET')
SIZE_LIMIT = 1000000000;  -- 1GB limit per COPY INTO execution
