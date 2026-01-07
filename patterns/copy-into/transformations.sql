-- ============================================================================
-- COPY INTO: Transformations During Load
-- ============================================================================
-- Transform data as it loads to reduce the need for separate staging steps.
-- Use a SELECT statement in the FROM clause to apply functions, reorder 
-- columns, or filter rows.

-- ----------------------------------------------------------------------------
-- Basic column transformation
-- ----------------------------------------------------------------------------

COPY INTO target_schema.target_table
FROM (
    SELECT 
        $1:field1::STRING as field1,
        $1:field2::INTEGER as field2,
        $1:timestamp::TIMESTAMP_NTZ as event_timestamp,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @stage_name/path/
)
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*[.]json'
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Reorder and rename columns from CSV
-- ----------------------------------------------------------------------------

-- Source CSV has columns: id, first_name, last_name, email, created_date
-- Target table expects: customer_id, full_name, email_address, created_at

COPY INTO raw.customers
FROM (
    SELECT 
        $1 as customer_id,
        $2 || ' ' || $3 as full_name,
        $4 as email_address,
        TO_TIMESTAMP($5, 'YYYY-MM-DD') as created_at
    FROM @external_stage/customers/
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- ----------------------------------------------------------------------------
-- Filter rows during load
-- ----------------------------------------------------------------------------

COPY INTO raw.valid_orders
FROM (
    SELECT 
        $1:order_id::STRING,
        $1:customer_id::STRING,
        $1:order_total::NUMBER(12,2),
        $1:order_date::DATE
    FROM @external_stage/orders/
    WHERE $1:order_total::NUMBER(12,2) > 0  -- Skip zero/negative orders
      AND $1:order_id IS NOT NULL           -- Skip records missing key field
)
FILE_FORMAT = (TYPE = 'JSON');

-- ----------------------------------------------------------------------------
-- Flatten nested JSON during load
-- ----------------------------------------------------------------------------

-- Source JSON structure:
-- {"user": {"id": "123", "name": "Alice"}, "events": [{"type": "click"}, {"type": "view"}]}

COPY INTO raw.user_events
FROM (
    SELECT 
        $1:user.id::STRING as user_id,
        $1:user.name::STRING as user_name,
        f.value:type::STRING as event_type,
        METADATA$FILENAME as source_file,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @external_stage/user_data/,
    LATERAL FLATTEN(input => $1:events) f
)
FILE_FORMAT = (TYPE = 'JSON');

-- ----------------------------------------------------------------------------
-- Apply business logic during load
-- ----------------------------------------------------------------------------

COPY INTO analytics.orders_enriched
FROM (
    SELECT 
        $1:order_id::STRING as order_id,
        $1:customer_id::STRING as customer_id,
        $1:order_total::NUMBER(12,2) as order_total,
        $1:order_date::DATE as order_date,
        -- Derive fiscal quarter
        CASE 
            WHEN MONTH($1:order_date::DATE) IN (1,2,3) THEN 'Q1'
            WHEN MONTH($1:order_date::DATE) IN (4,5,6) THEN 'Q2'
            WHEN MONTH($1:order_date::DATE) IN (7,8,9) THEN 'Q3'
            ELSE 'Q4'
        END as fiscal_quarter,
        -- Categorize order size
        CASE 
            WHEN $1:order_total::NUMBER(12,2) < 100 THEN 'small'
            WHEN $1:order_total::NUMBER(12,2) < 1000 THEN 'medium'
            ELSE 'large'
        END as order_size_category,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @external_stage/orders/
)
FILE_FORMAT = (TYPE = 'PARQUET');

-- ----------------------------------------------------------------------------
-- Handle multiple date formats
-- ----------------------------------------------------------------------------

COPY INTO raw.mixed_dates
FROM (
    SELECT 
        $1:id::STRING as id,
        -- Try multiple date formats
        COALESCE(
            TRY_TO_TIMESTAMP($1:date_field::STRING, 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP($1:date_field::STRING, 'MM/DD/YYYY'),
            TRY_TO_TIMESTAMP($1:date_field::STRING, 'DD-MON-YYYY')
        ) as parsed_timestamp
    FROM @external_stage/mixed/
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- Add audit columns
-- ----------------------------------------------------------------------------

COPY INTO raw.audited_data
FROM (
    SELECT 
        $1:* as raw_data,                    -- Keep original as VARIANT
        METADATA$FILENAME as _source_file,
        METADATA$FILE_ROW_NUMBER as _source_row,
        CURRENT_TIMESTAMP() as _loaded_at,
        CURRENT_USER() as _loaded_by
    FROM @external_stage/data/
)
FILE_FORMAT = (TYPE = 'JSON');

-- ----------------------------------------------------------------------------
-- Match by column name (Parquet/ORC/Avro)
-- ----------------------------------------------------------------------------

-- When source and target column names match, let Snowflake align automatically
-- instead of relying on positional ordering

COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;  -- Also: CASE_SENSITIVE, NONE
