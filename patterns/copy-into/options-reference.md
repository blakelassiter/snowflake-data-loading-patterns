# COPY INTO Options Reference

Complete reference for COPY INTO parameters. See [Snowflake documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table) for the authoritative source.

## Copy Options

### ON_ERROR

Controls behavior when errors occur during load.

| Value | Behavior |
|-------|----------|
| `CONTINUE` | Skip rows with errors, load valid rows |
| `SKIP_FILE` | Skip entire file if any row has error |
| `SKIP_FILE_<n>` | Skip file if error count exceeds n |
| `SKIP_FILE_<n>%` | Skip file if error percentage exceeds n% |
| `ABORT_STATEMENT` | Stop load on first error |

Default: `ABORT_STATEMENT`

### VALIDATION_MODE

Validate data without loading. Mutually exclusive with `ON_ERROR`.

| Value | Behavior |
|-------|----------|
| `RETURN_ERRORS` | Return first error per file |
| `RETURN_ALL_ERRORS` | Return all errors |
| `RETURN_<n>_ROWS` | Validate and return first n rows |

### SIZE_LIMIT

Maximum bytes to load per COPY INTO statement. Useful for incremental loads or testing.

```sql
SIZE_LIMIT = 1000000000  -- 1GB limit
```

### PURGE

Delete files from stage after successful load.

```sql
PURGE = TRUE  -- Default: FALSE
```

Applies only to files that loaded successfully. Failed files remain in stage.

### RETURN_FAILED_ONLY

When `ON_ERROR = CONTINUE`, only return rows for files with errors in the result.

```sql
RETURN_FAILED_ONLY = TRUE  -- Default: FALSE
```

### MATCH_BY_COLUMN_NAME

Align columns by name instead of position. Works with Parquet, Avro, ORC.

| Value | Behavior |
|-------|----------|
| `CASE_SENSITIVE` | Exact name match |
| `CASE_INSENSITIVE` | Case-insensitive match |
| `NONE` | Positional matching (default) |

### ENFORCE_LENGTH

Truncate strings that exceed target column length instead of raising error.

```sql
ENFORCE_LENGTH = FALSE  -- Truncate (default: TRUE raises error)
```

### TRUNCATECOLUMNS

Alias for `ENFORCE_LENGTH = FALSE`.

```sql
TRUNCATECOLUMNS = TRUE  -- Truncate instead of error
```

### FORCE

Reload files even if previously loaded (within 64-day tracking window).

```sql
FORCE = TRUE  -- Default: FALSE
```

### LOAD_UNCERTAIN_FILES

Load files with uncertain load status (e.g., load history unavailable).

```sql
LOAD_UNCERTAIN_FILES = TRUE  -- Default: FALSE
```

## File Selection

### FILES

Explicit list of file names to load.

```sql
FILES = ('file1.parquet', 'file2.parquet')
```

### PATTERN

Regex pattern to match file names.

```sql
PATTERN = '.*[.]parquet'              -- All Parquet files
PATTERN = '2025-01-.*[.]json'         -- January 2025 JSON files
PATTERN = '.*/partition=us/.*'        -- Files in us partition
```

## File Format Options

Specify inline or reference a named file format.

### Inline

```sql
FILE_FORMAT = (TYPE = 'PARQUET')
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = '|')
```

### Named Format

```sql
FILE_FORMAT = my_format
FILE_FORMAT = (FORMAT_NAME = 'my_schema.my_format')
```

## CSV-Specific Options

| Option | Default | Description |
|--------|---------|-------------|
| `FIELD_DELIMITER` | `,` | Column separator |
| `RECORD_DELIMITER` | `\n` | Row separator |
| `SKIP_HEADER` | 0 | Header rows to skip |
| `FIELD_OPTIONALLY_ENCLOSED_BY` | `NONE` | Quote character |
| `NULL_IF` | `\\N` | Values to treat as NULL |
| `EMPTY_FIELD_AS_NULL` | `FALSE` | Treat empty strings as NULL |
| `ESCAPE` | `NONE` | Escape character for enclosed fields |
| `ESCAPE_UNENCLOSED_FIELD` | `\\` | Escape character for unenclosed fields |
| `DATE_FORMAT` | `AUTO` | Date parsing format |
| `TIME_FORMAT` | `AUTO` | Time parsing format |
| `TIMESTAMP_FORMAT` | `AUTO` | Timestamp parsing format |

## JSON-Specific Options

| Option | Default | Description |
|--------|---------|-------------|
| `STRIP_OUTER_ARRAY` | `FALSE` | Remove root array wrapper |
| `STRIP_NULL_VALUES` | `FALSE` | Remove null-valued keys |
| `ALLOW_DUPLICATE` | `FALSE` | Allow duplicate keys |

## Parquet-Specific Options

| Option | Default | Description |
|--------|---------|-------------|
| `USE_VECTORIZED_SCANNER` | `TRUE` | Use optimized scanner |
| `BINARY_AS_TEXT` | `TRUE` | Load binary as string |

## Metadata Columns

Available when using SELECT in FROM clause:

| Column | Description |
|--------|-------------|
| `METADATA$FILENAME` | Full path of source file |
| `METADATA$FILE_ROW_NUMBER` | Row number within file |
| `METADATA$FILE_CONTENT_KEY` | Unique file identifier |
| `METADATA$FILE_LAST_MODIFIED` | File modification timestamp |
| `METADATA$START_SCAN_TIME` | When Snowflake started reading file |

```sql
COPY INTO target
FROM (
    SELECT 
        $1:field::STRING,
        METADATA$FILENAME as source_file,
        METADATA$FILE_ROW_NUMBER as file_row
    FROM @stage/
)
FILE_FORMAT = (TYPE = 'JSON');
```

## Iceberg-Specific Options

For loading into Iceberg tables:

| Option | Description |
|--------|-------------|
| `LOAD_MODE = FULL_INGEST` | Read and rewrite as Iceberg Parquet |
| `LOAD_MODE = ADD_FILES_COPY` | Copy compatible Parquet directly |

## Common Patterns

### Production batch load

```sql
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE'
PURGE = TRUE;
```

### Test before loading

```sql
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';
```

### Reload specific files

```sql
COPY INTO raw.events
FROM @external_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
FILES = ('problem_file.parquet')
FORCE = TRUE;
```

### Load with audit trail

```sql
COPY INTO raw.events
FROM (
    SELECT 
        $1:*,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()
    FROM @stage/
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';
```
