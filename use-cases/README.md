# Use Cases

Step-by-step recipes for common data loading scenarios. Each guide combines patterns from across the repository into complete solutions.

## Index

| Use Case | Loading Method | Complexity |
|----------|----------------|------------|
| [Batch Migration](batch-migration.md) | COPY INTO | Low |
| [Incremental Loading](incremental-loading.md) | COPY INTO / Snowpipe | Medium |
| [CDC Pipelines](cdc-pipelines.md) | Snowpipe / Streaming | Medium-High |

## Choosing a Recipe

**Starting from scratch with historical data?**
→ [Batch Migration](batch-migration.md)

**Adding daily/hourly updates to existing tables?**
→ [Incremental Loading](incremental-loading.md)

**Capturing changes from operational databases?**
→ [CDC Pipelines](cdc-pipelines.md)

## Recipe Structure

Each guide includes:

1. **Scenario** - When this pattern applies
2. **Architecture** - High-level data flow
3. **Setup** - Required configuration
4. **Implementation** - Step-by-step SQL
5. **Monitoring** - How to track health
6. **Troubleshooting** - Common issues

## Building Blocks

These recipes combine patterns from:

- [Loading patterns](../patterns/) - Core methods
- [Operations](../operations/) - Monitoring, error handling, testing
- [Decision framework](../decision-framework/) - Choosing approaches

## Related

For dbt transformation patterns after data loads, see [dbt-snowflake-optimization](https://github.com/blakelassiter/dbt-snowflake-optimization).
