# PostgreSQL UNDO Examples

This directory contains practical examples demonstrating the UNDO subsystem
and transactional file operations (FILEOPS).

## Prerequisites

Tables opt into UNDO by using the `flux` access method:

    CREATE TABLE my_table (...) USING flux;

UNDO is always-on infrastructure -- there is no GUC to enable or disable it
globally.  Table access methods opt in via the `am_supports_undo` callback.

Optional retention tuning (postgresql.conf):

    undo_retention_time = 3600000   # 1 hour in milliseconds
    undo_worker_naptime = 60000     # 1 minute

## Examples

- **01-basic-undo-setup.sql**: Creating UNDO-enabled tables and monitoring
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

- UNDO is always-on; tables opt in via `USING flux`
- FILEOPS (transactional file operations) is always-on for all tables
- System catalogs never use UNDO
