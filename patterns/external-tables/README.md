# External Tables

[External tables](https://docs.snowflake.com/en/user-guide/tables-external-intro) let you query data directly in cloud storage without copying it into Snowflake. The data stays in S3, GCS or Azure - Snowflake reads it on demand.

## When to Use

External tables fit scenarios where:

- You have massive historical data but only query recent subsets
- Regulatory requirements mandate data stays in specific storage locations
- Storage cost reduction matters more than query speed
- Data is already in columnar format (Parquet, ORC)
- You need to query data before deciding whether to load it

## When to Skip

External tables are not a replacement for loading:

- Query performance is significantly slower than native tables
- No clustering, search optimization or other Snowflake features
- Each query reads from remote storage (network latency)
- Complex transformations are expensive

For frequently-queried data, load it into Snowflake.

## Architecture

External tables are metadata definitions that point to cloud storage. When you query, Snowflake:

1. Reads file metadata (locations, partitions)
2. Pushes filters to skip irrelevant files
3. Reads required data from cloud storage
4. Processes and returns results

No data is stored in Snowflake - you pay only for compute during queries.

## Cost Model

**Storage:** Zero Snowflake storage cost - data stays in your cloud storage.

**Compute:** Warehouse time for queries. Typically higher than native tables for equivalent queries due to remote reads.

**Cloud egress:** Data transfer from storage to Snowflake compute. Can be significant for large scans.

External tables make sense when storage savings exceed the compute and egress premium.

## Files in This Directory

- [setup.sql](setup.sql) - Basic external table configuration
- [partitioning.sql](partitioning.sql) - Partition pruning for performance
- [materialized-views.sql](materialized-views.sql) - Caching hot data

## Related

- [Decision framework](../../decision-framework/) - When external tables vs loading
- [Snowflake docs: External Tables](https://docs.snowflake.com/en/user-guide/tables-external-intro)
