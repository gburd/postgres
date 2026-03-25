# PostgreSQL UNDO Examples

This directory contains practical examples demonstrating the UNDO subsystem
and transactional file operations (FILEOPS).

## Prerequisites

1. Enable UNDO at server level (requires restart):
   ```
   enable_undo = on
   ```

2. Adjust retention settings (optional):
   ```
   undo_retention_time = 3600000   # 1 hour in milliseconds
   undo_worker_naptime = 60000     # 1 minute
   ```

## Examples

- **01-basic-undo-setup.sql**: Setting up UNDO and basic recovery
- **02-undo-rollback.sql**: Transaction rollback with UNDO records
- **03-undo-subtransactions.sql**: SAVEPOINT and subtransaction rollback
- **04-transactional-fileops.sql**: Crash-safe table creation/deletion
- **05-undo-monitoring.sql**: Monitoring UNDO subsystem usage

## Running Examples

```bash
psql -d testdb -f examples/01-basic-undo-setup.sql
psql -d testdb -f examples/02-undo-rollback.sql
...
```

## Notes

- UNDO logging is opt-in per table via `WITH (enable_undo = on)`
- FILEOPS is enabled by default (`enable_transactional_fileops = on`)
- System catalogs cannot enable UNDO
- Performance overhead when UNDO enabled: ~15-25% on write-heavy workloads
