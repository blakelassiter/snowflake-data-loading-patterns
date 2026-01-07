-- ============================================================================
-- COPY INTO: Schema Detection (INFER_SCHEMA)
-- ============================================================================
-- When loading JSON, Parquet, Avro or ORC files, INFER_SCHEMA reads staged
-- files and returns column definitions. CREATE TABLE...USING TEMPLATE builds
-- a table from that output. COPY INTO with MATCH_BY_COLUMN_NAME loads data
-- by column name rather than position.

-- ----------------------------------------------------------------------------
-- Basic schema inference from Parquet
-- ----------------------------------------------------------------------------

-- First, create a file format
CREATE OR REPLACE FILE FORMAT my_parquet_format
    TYPE = 'PARQUET';

-- Infer schema from staged files
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@my_stage/data/',
        FILE_FORMAT => 'my_parquet_format'
    )
);

-- Returns columns:
-- COLUMN_NAME    - Detected column name
-- TYPE           - Snowflake data type
-- NULLABLE       - Whether column can be null
-- EXPRESSION     - Expression to extract column from file
-- FILENAMES      - Files used for inference
-- ORDER_ID       - Column position

-- ----------------------------------------------------------------------------
-- Create table from inferred schema
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE my_table
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_stage/data/',
            FILE_FORMAT => 'my_parquet_format'
        )
    )
);

-- Verify the created schema
DESC TABLE my_table;

-- ----------------------------------------------------------------------------
-- Load data using column name matching
-- ----------------------------------------------------------------------------

-- After creating table from inferred schema, load with MATCH_BY_COLUMN_NAME
COPY INTO my_table
FROM @my_stage/data/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Options for MATCH_BY_COLUMN_NAME:
-- CASE_SENSITIVE      - Exact column name match
-- CASE_INSENSITIVE    - Case-insensitive match
-- NONE                - Positional matching (default)

-- ----------------------------------------------------------------------------
-- JSON schema inference
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT my_json_format
    TYPE = 'JSON';

-- Infer schema - works well for consistent JSON structures
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@my_stage/json_data/',
        FILE_FORMAT => 'my_json_format'
    )
);

-- Create table and load
CREATE OR REPLACE TABLE json_data
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_stage/json_data/',
            FILE_FORMAT => 'my_json_format'
        )
    )
);

COPY INTO json_data
FROM @my_stage/json_data/
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ----------------------------------------------------------------------------
-- Control inference behavior
-- ----------------------------------------------------------------------------

-- Limit files sampled (default: 100 files, max: 1000)
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@my_stage/data/',
        FILE_FORMAT => 'my_parquet_format',
        FILES => 'sample_file_1.parquet'  -- Or specific file list
    )
);

-- For large directories, specifying files speeds up inference

-- ----------------------------------------------------------------------------
-- Handle schema evolution
-- ----------------------------------------------------------------------------

-- When new columns appear in source files:

-- Step 1: Detect current schema from new files
CREATE OR REPLACE TEMPORARY TABLE new_schema AS
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@my_stage/data/2025/',
        FILE_FORMAT => 'my_parquet_format'
    )
);

-- Step 2: Compare with existing table
SELECT 
    n.COLUMN_NAME as new_column,
    n.TYPE as new_type
FROM new_schema n
LEFT JOIN information_schema.columns c
    ON c.table_name = 'MY_TABLE'
    AND UPPER(c.column_name) = UPPER(n.COLUMN_NAME)
WHERE c.column_name IS NULL;

-- Step 3: Add missing columns
-- ALTER TABLE my_table ADD COLUMN new_column_name VARCHAR;

-- ----------------------------------------------------------------------------
-- Complete workflow example
-- ----------------------------------------------------------------------------

-- Scenario: New Parquet files arrived, schema unknown

-- 1. Create file format
CREATE OR REPLACE FILE FORMAT ingest_parquet
    TYPE = 'PARQUET'
    USE_VECTORIZED_SCANNER = TRUE;

-- 2. Preview schema
SELECT 
    COLUMN_NAME,
    TYPE,
    NULLABLE
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@raw_stage/new_dataset/',
        FILE_FORMAT => 'ingest_parquet'
    )
)
ORDER BY ORDER_ID;

-- 3. Create target table
CREATE OR REPLACE TABLE raw.new_dataset
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@raw_stage/new_dataset/',
            FILE_FORMAT => 'ingest_parquet'
        )
    )
);

-- 4. Add audit columns
ALTER TABLE raw.new_dataset ADD COLUMN _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE raw.new_dataset ADD COLUMN _source_file STRING;

-- 5. Load with audit columns populated
COPY INTO raw.new_dataset
FROM (
    SELECT 
        *,
        CURRENT_TIMESTAMP() as _loaded_at,
        METADATA$FILENAME as _source_file
    FROM @raw_stage/new_dataset/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ----------------------------------------------------------------------------
-- Limitations
-- ----------------------------------------------------------------------------

-- INFER_SCHEMA works with: Parquet, Avro, ORC, JSON
-- Does NOT work with: CSV (no embedded schema)

-- For CSV, define schema manually or use a sample to generate DDL:
-- 1. Load sample to VARIANT column
-- 2. Inspect structure
-- 3. Create typed table

CREATE OR REPLACE TABLE csv_staging (
    raw_line VARIANT
);

-- Then create typed table based on inspection
