# RECNO Error Recovery Procedures

## Overview

This document provides diagnostic and recovery procedures for corruption scenarios in RECNO tables. While RECNO is designed to be robust, rare hardware failures, software bugs, or operator errors can lead to data inconsistencies. This guide helps database administrators detect and recover from these issues.

**Target audience**: Database administrators, site reliability engineers, PostgreSQL support personnel

**Prerequisites**: Familiarity with PostgreSQL administration, basic understanding of RECNO architecture

---

## Quick Reference

| Issue | Symptoms | Quick Fix | Section |
|-------|----------|-----------|---------|
| Overflow chain breakage | Error: "invalid overflow record offset" or "overflow chain exceeded maximum length" | Salvage readable rows, VACUUM FULL | [Section 1](#1-overflow-page-chain-breakage) |
| Compression metadata corruption | Error: "invalid delta compression tag" or decompression size mismatch | Disable compression, VACUUM FULL | [Section 2](#2-compression-metadata-corruption) |
| FSM inconsistencies | Bloated table despite VACUUM; "failed to allocate page for tuple insertion" | VACUUM or VACUUM FULL | [Section 3](#3-fsm-inconsistencies) |
| VM inconsistencies | Slow index-only scans, excessive heap fetches | VACUUM to rebuild VM | [Section 4](#4-vm-inconsistencies) |
| WAL replay failure | Startup fails with PANIC in recno redo | Point-in-time recovery to before corruption | [Section 5](#5-wal-replay-failures) |
| MVCC state corruption | "RECNO MVCC not initialized" or "maximum number of RECNO transactions exceeded" | Restart PostgreSQL | [Section 6](#6-mvcc-state-corruption) |
| Clock/HLC anomalies | "HLC drift exceeds max_offset" or FATAL clock errors | Fix NTP, adjust recno_max_clock_offset_ms | [Section 7](#7-clock-and-hlc-anomalies) |
| Index corruption | Index scan returns wrong rows; amcheck reports errors | REINDEX TABLE | [Section 8](#8-index-corruption-specific-to-recno) |

---

## 1. Overflow Page Chain Breakage

### Symptoms

Error messages from `recno_overflow.c`:

- `ERROR: invalid overflow record offset <N> on block <N>`
- `ERROR: overflow record at (<block>,<offset>) is not a normal item`
- `ERROR: expected overflow record at (<block>,<offset>), found magic 0x<HEX>`
- `ERROR: overflow chain exceeded maximum length`
- `ERROR: incomplete overflow read: expected <N> bytes, got <N>`
- `ERROR: could not allocate space for overflow record`

Behavioral symptoms:
- `SELECT` on specific rows fails, while other rows in the same table remain accessible
- Query returns incomplete data for large TEXT/BYTEA/JSONB columns
- The error always references the same block/offset for the same row

### Cause

Overflow page chains store large attributes across multiple linked pages within
the same relation.  Each overflow record carries a magic number (`0xDEAD0F10`)
and a continuation pointer (`or_next_block` / `or_next_offset`).  Chain breakage
can occur due to:

- Incomplete WAL replay after a crash during overflow chain construction
- Storage-level bit flip in an overflow record's continuation pointer or magic number
- A bug in VACUUM that frees an overflow page still referenced by a live tuple
- Interrupted `VACUUM FULL` or table rewrite
- Disk corruption affecting overflow page data

### Detection

#### Method 1: Query-based detection
```sql
-- Attempt to read all rows with large columns
SELECT id, length(large_column) FROM recno_table;

-- If specific rows fail, note their IDs
```

#### Method 2: pg_waldump inspection
```bash
# Check for overflow-related WAL records around the time of failure
pg_waldump -p $PGDATA/pg_wal -r RECNO | grep OVERFLOW
```

#### Method 3: Pageinspect (expert use only)
```sql
-- Check specific page for overflow pointers
SELECT * FROM recno_page_items(get_raw_page('recno_table', <page_num>));

-- Look for invalid overflow offsets (outside table bounds)
```

### Recovery Procedure

#### Option 1: REINDEX (preserves data, rebuilds indexes)

If the corruption is limited to overflow linkage metadata and the actual data is intact:

1. **Identify affected tuples**:
   ```sql
   -- Create a test query to find broken rows
   DO $$
   DECLARE
       r RECORD;
   BEGIN
       FOR r IN SELECT id FROM recno_table LOOP
           BEGIN
               PERFORM large_column FROM recno_table WHERE id = r.id;
           EXCEPTION WHEN OTHERS THEN
               RAISE NOTICE 'Broken row: id=%', r.id;
           END;
       END LOOP;
   END $$;
   ```

2. **Rebuild indexes** (may help if corruption is in index entries):
   ```sql
   REINDEX TABLE recno_table;
   ```

3. **If REINDEX doesn't fix it, proceed to Option 2**.

#### Option 2: Rebuild affected tuples

For limited corruption affecting specific rows:

1. **Export good data**:
   ```sql
   -- Export rows that can be read successfully
   COPY (
       SELECT * FROM recno_table WHERE id NOT IN (
           -- List of broken row IDs identified above
           <bad_ids>
       )
   ) TO '/tmp/good_rows.csv' CSV HEADER;
   ```

2. **Drop and recreate table**:
   ```sql
   BEGIN;
   DROP TABLE recno_table;
   CREATE TABLE recno_table (
       -- Same schema
   ) USING recno;
   COMMIT;
   ```

3. **Reload data**:
   ```sql
   COPY recno_table FROM '/tmp/good_rows.csv' CSV HEADER;
   ```

4. **Restore indexes and constraints**:
   ```sql
   -- Recreate all indexes and constraints
   ```

#### Option 3: Point-in-time recovery (PITR)

If corruption is widespread or critical data is affected:

1. **Stop PostgreSQL**:
   ```bash
   pg_ctl stop -m fast
   ```

2. **Restore from base backup**:
   ```bash
   # Restore last known good backup
   rm -rf $PGDATA
   tar -xzf /backups/base_backup_<date>.tar.gz -C $PGDATA
   ```

3. **Configure recovery target** (`postgresql.conf` or `recovery.conf` depending on version):
   ```
   restore_command = 'cp /archivedir/%f %p'
   recovery_target_time = '<timestamp before corruption>'
   ```

4. **Start PostgreSQL**:
   ```bash
   pg_ctl start
   ```

5. **Verify data integrity**:
   ```sql
   SELECT COUNT(*) FROM recno_table;
   -- Verify expected row count
   ```

### Prevention

- **Regular backups**: Use `pg_basebackup` daily
- **WAL archiving**: Archive WAL files for PITR
- **Monitoring**: Set up alerts for overflow-related errors
- **Hardware**: Use ECC RAM and enterprise storage with checksums
- **Testing**: Test recovery procedures regularly

---

## 2. Compression Metadata Corruption

### Symptoms

Error messages from `recno_compress.c`:

- `ERROR: invalid delta compression tag: 0x<HEX>`
- `ERROR: compressed data corruption: decompressed size <N> does not match expected <N>`

Behavioral symptoms:
- `SELECT` on specific columns fails with decompression errors
- The same row may be readable if you exclude the corrupted column
- Other columns in the same row remain accessible

**Note on current implementation**: As of March 2026, the compression algorithms
are stub implementations that perform `memcpy` without actual compression.  This
means compression metadata corruption is unlikely in practice until real LZ4/ZSTD
algorithms are connected.  The framework and error paths exist and will become
relevant when actual compression is enabled.

### Cause

Compression metadata corruption can result from:
- Bit flips in memory or on disk affecting the 8-byte compression header
  (algorithm ID, level, original size, compressed size)
- Mismatch between the recorded algorithm and the actual compressed bytes
- Incomplete write during an in-place update of a compressed attribute
- Partial page writes during crash (mitigated by `full_page_writes = on`)

### Detection

#### Method 1: Query-based detection
```sql
-- Test decompression by reading compressed columns
SELECT id, large_text_column FROM recno_table;

-- If specific rows fail, identify them
```

#### Method 2: Enable detailed logging
```sql
-- In postgresql.conf or via ALTER SYSTEM:
SET log_min_messages = DEBUG1;
SET recno_log_compression = on;  -- If this GUC exists

-- Retry query, check logs for compression-related errors
```

#### Method 3: Pageinspect
```sql
-- Check tuple infomask for compression flags
SELECT lp, t_infomask, t_infomask2
FROM recno_page_items(get_raw_page('recno_table', <page_num>));

-- Look for unusual infomask combinations
```

### Recovery Procedure

#### Option 1: Disable compression and rebuild

If compression is causing persistent issues:

1. **Disable compression**:
   ```sql
   ALTER SYSTEM SET recno_enable_compression = off;
   SELECT pg_reload_conf();
   ```

2. **Rebuild table** (forces decompression):
   ```sql
   BEGIN;
   CREATE TABLE recno_table_new (LIKE recno_table) USING recno;
   INSERT INTO recno_table_new SELECT * FROM recno_table;
   -- If this fails on corrupted rows, skip them manually
   DROP TABLE recno_table;
   ALTER TABLE recno_table_new RENAME TO recno_table;
   -- Recreate indexes
   COMMIT;
   ```

#### Option 2: Fix specific corrupted tuples

For isolated corruption:

1. **Identify corrupted tuples**:
   ```sql
   -- Same loop-based approach as overflow corruption
   ```

2. **Delete and re-insert affected rows**:
   ```sql
   BEGIN;
   DELETE FROM recno_table WHERE id = <corrupted_id>;
   INSERT INTO recno_table VALUES (<correct_data>);
   -- Repeat for each corrupted row
   COMMIT;
   ```

3. **If original data is lost**, restore from application-level backups or reconstruct manually.

#### Option 3: Restore from backup

If corruption is widespread:

1. **Follow PITR procedure** from Section 1, Option 3.

### Prevention

- **Test compression**: Before enabling in production, test with realistic data
- **Hardware ECC RAM**: Reduces bit flips
- **Checksums**: Enable data checksums (`initdb -k` or `pg_checksums --enable`)
- **Gradual rollout**: Enable compression on non-critical tables first
- **Monitoring**: Log compression ratios and errors

---

## 3. FSM Inconsistencies

### Symptoms

Error messages from `recno_operations.c`:

- `ERROR: RECNO failed to allocate page for tuple insertion`
- `ERROR: RECNO failed to allocate page for tuple insertion after retry`
- `ERROR: RECNO page still has insufficient space after FSM update`

These indicate the FSM directed RECNO to a page without enough space, and even
after FSM correction and retry the relation could not find a suitable page.

Behavioral symptoms:
- Table size grows despite frequent `UPDATE`/`DELETE` and `VACUUM`
- `VACUUM` runs but doesn't reclaim space
- New rows inserted at end of table instead of filling gaps
- `recno_fsm_stats('table')` shows unexpected space category distributions

### Cause

Free Space Map (FSM) tracks available space on each page using PostgreSQL's
standard FSM with RECNO's additional 5-category classification (FULL, TIGHT,
MEDIUM, LOOSE, EMPTY).  Inconsistencies can arise from:
- Crash during FSM update (FSM is not WAL-logged by default)
- Stale FSM entries after interrupted defragmentation
- Concurrent access race during page space classification
- Corruption of `.fsm` file on disk

### Detection

#### Method 1: Compare reported vs. actual free space
```sql
-- Check FSM stats
SELECT * FROM recno_fsm_stats('recno_table');

-- Compare with actual table size
SELECT pg_size_pretty(pg_relation_size('recno_table')) AS current_size,
       pg_size_pretty(pg_relation_size('recno_table', 'main')) AS main_fork,
       pg_size_pretty(pg_relation_size('recno_table', 'fsm')) AS fsm_fork;

-- If FSM is empty (0 bytes) but table has deleted rows, FSM is broken
```

#### Method 2: Manual page inspection
```sql
-- Check free space on a sample of pages
SELECT blkno, freespace
FROM generate_series(0, pg_relation_size('recno_table') / 8192 - 1) AS blkno,
     LATERAL (SELECT pg_freespace('recno_table', blkno) AS freespace) f
WHERE freespace > 1000  -- Should have free space
LIMIT 100;

-- If many pages have free space but FSM shows none, FSM is stale
```

### Recovery Procedure

#### Option 1: VACUUM with FSM rebuild

1. **Run VACUUM VERBOSE**:
   ```sql
   VACUUM VERBOSE recno_table;
   ```

2. **Check output** for messages like:
   ```
   INFO: "recno_table": found 1000 removable, 5000 nonremovable row versions
   INFO: "recno_table": truncated from 10000 to 9000 pages
   ```

3. **If VACUUM doesn't help**, proceed to Option 2.

#### Option 2: Rebuild FSM manually

1. **Truncate FSM fork** (PostgreSQL 13+):
   ```sql
   SELECT pg_truncate_visibility_map('recno_table');  -- Clears VM (if exists)

   -- No direct FSM truncate function, use pg_prewarm to force FSM rebuild:
   VACUUM recno_table;  -- Should rebuild FSM
   ```

2. **Alternative: pg_surgery extension** (PostgreSQL 14+):
   ```sql
   CREATE EXTENSION pg_surgery;

   -- Force FSM rebuild by rewriting pages
   -- (No direct FSM surgery function; use VACUUM FULL instead)
   ```

#### Option 3: VACUUM FULL (rebuilds entire table)

1. **WARNING**: `VACUUM FULL` requires exclusive lock and rewrites the entire table.

2. **Run VACUUM FULL**:
   ```sql
   VACUUM FULL recno_table;
   ```

3. **Verify**:
   ```sql
   SELECT * FROM recno_fsm_stats('recno_table');
   ```

#### Option 4: Rebuild table (last resort)

If FSM corruption persists:

1. **Create new table**:
   ```sql
   BEGIN;
   CREATE TABLE recno_table_new (LIKE recno_table) USING recno;
   INSERT INTO recno_table_new SELECT * FROM recno_table;
   DROP TABLE recno_table;
   ALTER TABLE recno_table_new RENAME TO recno_table;
   -- Recreate indexes and constraints
   COMMIT;
   ```

### Prevention

- **Regular VACUUM**: Schedule autovacuum or periodic manual VACUUM
- **Monitor FSM health**: Check `recno_fsm_stats()` periodically
- **Avoid manual .fsm file edits**: Never directly modify FSM files
- **Crash recovery**: Ensure PostgreSQL has time to complete crash recovery after unclean shutdown

---

## 4. VM Inconsistencies

### Symptoms

- Index-only scans are slow despite recent VACUUM
- `EXPLAIN` shows "Heap Fetches" > 0 for index-only scan on freshly VACUUMed table
- `SELECT * FROM recno_vm_stats('table')` shows no all-visible pages despite stable data
- VACUUM repeatedly tries to set VM bits but fails

### Cause

Visibility Map (VM) inconsistencies can result from:
- Incomplete VACUUM
- Corruption of `.vm` file
- Bug in VM update logic
- Concurrent DML clearing VM bits without proper WAL logging

### Detection

#### Method 1: Check VM statistics
```sql
-- Check VM coverage
SELECT * FROM recno_vm_stats('recno_table');

-- Compare with table size
SELECT relpages FROM pg_class WHERE relname = 'recno_table';

-- If all tuples are visible but VM shows 0 all-visible pages, VM is stale
```

#### Method 2: Test index-only scan performance
```sql
-- Create covering index
CREATE INDEX ON recno_table (id) INCLUDE (value);

-- Test index-only scan
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, value FROM recno_table WHERE id > 0;

-- Check "Heap Fetches" line in output
-- Should be 0 for fully visible table; >0 indicates VM issues
```

### Recovery Procedure

#### Option 1: Rebuild VM with VACUUM

1. **Run VACUUM VERBOSE**:
   ```sql
   VACUUM VERBOSE recno_table;
   ```

2. **Check output** for VM-related messages:
   ```
   INFO: "recno_table": marked 5000 pages as all-visible
   ```

3. **Verify**:
   ```sql
   SELECT * FROM recno_vm_stats('recno_table');
   ```

#### Option 2: Truncate and rebuild VM

1. **Truncate VM fork** (PostgreSQL 13+):
   ```sql
   SELECT pg_truncate_visibility_map('recno_table');
   ```

2. **Run VACUUM** to rebuild:
   ```sql
   VACUUM recno_table;
   ```

3. **Verify**:
   ```sql
   SELECT * FROM recno_vm_stats('recno_table');
   ```

#### Option 3: VACUUM FULL (extreme case)

If VM corruption persists after truncation:

1. **Run VACUUM FULL**:
   ```sql
   VACUUM FULL recno_table;
   ```

2. **This rebuilds table, FSM, and VM from scratch**.

### Prevention

- **Regular VACUUM**: Ensures VM stays up-to-date
- **Monitor index-only scan performance**: Alert on unexpected heap fetches
- **Avoid manual .vm file edits**: Never directly modify VM files
- **Test VM updates**: After major DML operations, check VM stats

---

## 5. WAL Replay Failures

### Symptoms

- PostgreSQL fails to start after crash
- Error message: `FATAL: invalid recno WAL record: <details>`
- Error message: `PANIC: recno redo failed: <details>`
- Standby server reports WAL replay errors

### Cause

WAL replay failures indicate corruption in WAL records or bugs in REDO logic:
- Corrupted WAL file (disk error)
- Bug in RECNO WAL logging function
- Bug in RECNO REDO function
- Incorrect manual WAL manipulation

### Detection

#### Method 1: Check PostgreSQL logs

Look for errors during startup:
```
LOG: database system was interrupted; last known up at <timestamp>
LOG: database system was not properly shut down; automatic recovery in progress
LOG: redo starts at <LSN>
FATAL: invalid recno WAL record at <LSN>: <error details>
```

#### Method 2: Inspect WAL with pg_waldump

```bash
# Dump WAL records around failure point
pg_waldump -p $PGDATA/pg_wal -s <start_LSN> -e <end_LSN> -r RECNO

# Look for unusual or malformed records
```

### Recovery Procedure

#### Option 1: Skip corrupted WAL record (data loss!)

**WARNING**: This causes data loss. Only use if you have a backup and need to get the database online quickly.

1. **Identify the LSN** of the corrupted record from error message.

2. **Set recovery target BEFORE corruption** in `postgresql.conf`:
   ```
   recovery_target_lsn = '<LSN before corruption>'
   recovery_target_action = 'promote'
   ```

3. **Start PostgreSQL**:
   ```bash
   pg_ctl start
   ```

4. **Database will recover up to the target LSN, losing any changes after**.

#### Option 2: Point-in-time recovery to before corruption

1. **Restore from last base backup**:
   ```bash
   rm -rf $PGDATA
   tar -xzf /backups/base_backup_<date>.tar.gz -C $PGDATA
   ```

2. **Configure recovery to stop before corruption**:
   ```
   restore_command = 'cp /archivedir/%f %p'
   recovery_target_lsn = '<LSN before corruption>'
   ```

3. **Start PostgreSQL**:
   ```bash
   pg_ctl start
   ```

#### Option 3: Fix WAL record (expert use only)

If corruption is understood and fixable:

1. **Backup original WAL**:
   ```bash
   cp $PGDATA/pg_wal/<segment> /backup/pg_wal_<segment>.backup
   ```

2. **Use pg_waldump to identify exact record**:
   ```bash
   pg_waldump -p $PGDATA/pg_wal -s <LSN> -e <LSN + 100> -r RECNO
   ```

3. **Manually edit WAL file** (requires deep PostgreSQL expertise):
   - Use hex editor to correct record
   - Recalculate CRC
   - **Extreme risk of further corruption**

4. **Start PostgreSQL** and monitor carefully.

#### Option 4: Report bug and restore from backup

If corruption appears to be a RECNO bug:

1. **Collect diagnostic data**:
   ```bash
   pg_waldump -p $PGDATA/pg_wal -r RECNO > /tmp/recno_wal_dump.txt
   pg_controldata $PGDATA > /tmp/pg_controldata.txt
   ```

2. **Report bug** to RECNO maintainers with:
   - Error message from PostgreSQL logs
   - WAL dump around corrupted record
   - Steps to reproduce (if known)

3. **Restore from backup** (Option 2 above) while bug is being fixed.

### Prevention

- **WAL archiving**: Archive WAL files to durable storage
- **Hardware redundancy**: Use RAID for WAL disks
- **Checksums**: Enable data checksums (`initdb -k`)
- **Regular backups**: Daily base backups + WAL archiving
- **Test recovery**: Practice PITR regularly

---

## 6. MVCC State Corruption

### Symptoms

Error messages from `recno_mvcc.c`:

- `ERROR: RECNO MVCC not initialized`
- `ERROR: maximum number of RECNO transactions (<N>) exceeded`

Behavioral symptoms:
- All transactions on RECNO tables fail with "MVCC not initialized"
- Transactions hang waiting for timestamp generation
- Visibility anomalies (rows visible/invisible when they should not be)

### Cause

RECNO's MVCC uses shared memory structures (`RecnoMvccShmemData`) to track
active transactions and generate monotonic commit timestamps.  Corruption can
occur from:

- Shared memory corruption (hardware failure, OOM killer terminating backends)
- Incorrect timestamp monotonicity after a large system clock adjustment
- Transaction state array overflow when concurrent transactions exceed
  `recno_max_transactions`
- Server crash during commit timestamp assignment

### Detection

```sql
-- Check MVCC state
SELECT * FROM recno_mvcc_stats();

-- Check for stuck transactions
SELECT pid, state, query_start, now() - query_start AS duration
FROM pg_stat_activity
WHERE state = 'active'
  AND now() - query_start > interval '1 hour';

-- Check current RECNO configuration
SHOW recno_max_transactions;
SHOW recno_enable_serializable;
```

### Recovery Procedure

#### Option 1: Restart PostgreSQL

MVCC shared memory state is re-initialized on server startup:
```bash
pg_ctl restart -D $PGDATA
```
This is safe because MVCC state is transient; committed data is
preserved in WAL and data files.  All active transactions are aborted
on restart, but committed data is unaffected.

#### Option 2: Increase transaction slots

If the error is "maximum number of RECNO transactions exceeded":
```sql
ALTER SYSTEM SET recno_max_transactions = 2000;
-- Requires restart to take effect
```
Set this to at least 2x your expected peak concurrent transactions.

#### Option 3: Check timestamp monotonicity

After a restart following a large clock adjustment:
```sql
-- Check that the MVCC global timestamp is reasonable
SELECT * FROM recno_mvcc_stats();
-- The global_commit_ts should be close to current wall-clock time
```

### Prevention

- Set `recno_max_transactions` conservatively (2x expected peak concurrency)
- Avoid large system clock adjustments while PostgreSQL is running;
  use NTP's slew mode (`makestep 0.1 3` in chrony)
- Monitor MVCC statistics regularly with `recno_mvcc_stats()`

### Data Loss Assessment

- **No data loss**: MVCC state corruption prevents new transactions
  but does not affect already-committed data
- Restart always recovers full functionality

---

## 7. Clock and HLC Anomalies

### Symptoms

Error messages from `recno_hlc.c`:

- `WARNING: HLC drift exceeds max_offset: hlc_physical=<N>, wall_clock=<N>, drift=<N> ms`

Error messages from `recno_clock.c`:

- `FATAL: RECNO: clock error bound <N> ms exceeds 80% of maximum <N> ms`
- `FATAL: RECNO: NTP synchronization lost for 10 minutes`
- `WARNING: RECNO: clock error bound <N> ms exceeds 50% of maximum <N> ms`
- `WARNING: RECNO: NTP synchronization may be lost (no update for 5 minutes)`

Error messages from `recno_xlog.c`:

- `WARNING: RECNO: waited 1 second for HLC to advance`

Behavioral symptoms:
- Server shuts down with FATAL clock error (when `recno_fatal_on_clock_drift = on`)
- HLC timestamps drift from wall-clock time
- Replication lag increases due to uncertainty waiting on standbys

### Cause

When HLC mode is enabled (`recno_use_hlc = on`), RECNO depends on clock
accuracy for causal ordering.  Anomalies can occur from:

- NTP step adjustment moving the system clock backward
- Clock drift exceeding `recno_max_clock_offset_ms`
- Loss of NTP synchronization
- VM migration to a host with a different clock
- AWS ClockBound daemon becoming unavailable

### Detection

```sql
-- Check HLC state and current settings
SELECT * FROM recno_mvcc_stats();
SHOW recno_max_clock_offset_ms;
SHOW recno_use_hlc;
SHOW recno_uncertainty_wait;
SHOW recno_fatal_on_clock_drift;
```

System-level checks:
```bash
# Check NTP synchronization
chronyc tracking
chronyc sources -v

# Check system clock
timedatectl status

# If using AWS ClockBound
clockbound-cli now
```

### Recovery Procedure

#### Option 1: Fix clock synchronization and restart

```bash
# Fix NTP
sudo systemctl restart chrony
chronyc makestep  # force immediate sync (use only as last resort)

# Restart PostgreSQL
pg_ctl restart -D $PGDATA
```

#### Option 2: Increase clock offset tolerance

If clocks are synchronized within acceptable bounds but RECNO's
threshold is too tight:

```sql
ALTER SYSTEM SET recno_max_clock_offset_ms = 500;
SELECT pg_reload_conf();  -- takes effect immediately (no restart needed)
```

#### Option 3: Disable fatal clock drift behavior

For environments where clock drift is expected (development, testing):

```sql
ALTER SYSTEM SET recno_fatal_on_clock_drift = off;
SELECT pg_reload_conf();
```

This allows RECNO to continue operating with degraded clock quality,
emitting warnings instead of shutting down.

#### Option 4: Switch to default timestamp mode

If HLC issues persist, disable HLC mode entirely:

```sql
ALTER SYSTEM SET recno_use_hlc = off;
-- Requires restart
```

Existing HLC timestamps in tuples remain readable; new commits use
plain monotonic timestamps.

### Prevention

- Use chrony or NTP with multiple upstream servers
- Prefer `makestep 0.1 3` in chrony to limit step adjustments
- On AWS, use the Amazon Time Sync Service and ClockBound daemon
- Monitor clock drift with `chronyc tracking` and set up alerting
- Set `recno_max_clock_offset_ms` to at least 2x observed maximum skew
- In production, keep `recno_fatal_on_clock_drift = on` to catch drift early

### Data Loss Assessment

- **No data loss**: Clock anomalies affect timestamp ordering, not data integrity
- HLC timestamps may drift from wall-clock, but causal ordering is preserved
  within a single node
- After clock correction and restart, HLC converges back to wall-clock

---

## 8. Index Corruption Specific to RECNO

### Symptoms

- Index scan returns different results than sequential scan
- `pg_amcheck` reports inconsistencies
- Error messages during index operations referencing invalid TIDs
- REINDEX fixes the issue but it recurs

### Cause

RECNO's in-place update design means that TIDs generally remain stable (unlike
heap, where HOT redirections change tuple locations).  However, index corruption
specific to RECNO can occur when:

- A cross-page update (when a tuple grows and cannot be updated in place)
  moves a tuple to a new page but the index update is interrupted by a crash
- VACUUM removes a tuple that is still referenced by an index entry
- An overflow page is reclaimed while an index still references the parent tuple

Because RECNO does not implement HOT (Heap-Only Tuples) -- it doesn't need to,
since in-place updates keep the TID stable -- every non-in-place update creates
new index entries.  This means index bloat may accumulate faster than with heap
for updates that change tuple size.

### Detection

#### Method 1: pg_amcheck
```bash
# Check all indexes on a RECNO table
pg_amcheck --install-missing --table my_table mydb
```

#### Method 2: Compare index scan vs sequential scan
```sql
-- Force sequential scan
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM my_table WHERE indexed_column = 'value';

-- Force index scan
RESET enable_indexscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;
SELECT count(*) FROM my_table WHERE indexed_column = 'value';
RESET enable_seqscan;

-- If counts differ, the index is inconsistent
```

#### Method 3: Check for dead index entries
```sql
-- Check index bloat
SELECT
  indexrelname,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname = 'my_table';
```

### Recovery Procedure

#### Option 1: REINDEX (non-blocking)

```sql
-- Rebuild all indexes without blocking reads
REINDEX TABLE CONCURRENTLY my_table;
```

#### Option 2: REINDEX (blocking, faster)

```sql
-- Rebuild all indexes (blocks concurrent access)
REINDEX TABLE my_table;
```

#### Option 3: Drop and recreate specific index

If a specific index is corrupted:
```sql
DROP INDEX CONCURRENTLY my_index;
CREATE INDEX CONCURRENTLY my_index ON my_table (column);
```

### Prevention

- Schedule regular `REINDEX CONCURRENTLY` for frequently updated RECNO tables
  (RECNO's lack of HOT means index entries accumulate for size-changing updates):
  ```sql
  -- Weekly reindex for high-update tables
  SELECT cron.schedule('reindex-recno', '0 2 * * 0',
    $$REINDEX TABLE CONCURRENTLY my_table;$$);
  ```
- Run `VACUUM` regularly to remove dead index entries
- Monitor index bloat using `pg_stat_user_indexes`

### Data Loss Assessment

- **No data loss**: Index corruption affects query results via index scans,
  but sequential scans always return correct data
- REINDEX always fully recovers index consistency

---

## 9. General Diagnostic Tools

### pg_waldump
```bash
# Inspect all RECNO WAL records
pg_waldump -p $PGDATA/pg_wal -r RECNO

# Inspect specific LSN range
pg_waldump -p $PGDATA/pg_wal -s <start_LSN> -e <end_LSN>

# Count RECNO operations by type
pg_waldump -p $PGDATA/pg_wal -r RECNO -z
```

### pageinspect extension
```sql
CREATE EXTENSION pageinspect;

-- Inspect RECNO page header
SELECT * FROM page_header(get_raw_page('recno_table', 0));

-- Inspect individual tuples on a page
SELECT * FROM heap_page_items(get_raw_page('recno_table', 0));
```

### RECNO-specific monitoring functions
```sql
-- Check compression statistics
SELECT * FROM recno_compression_stats('recno_table');

-- Check FSM health
SELECT * FROM recno_fsm_stats('recno_table');

-- Check MVCC state
SELECT * FROM recno_mvcc_stats();

-- Check overflow page usage
SELECT * FROM recno_overflow_stats('recno_table');
```

### Full table integrity scan
```sql
-- Force sequential scan to read every tuple and overflow chain
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM my_table;
RESET enable_indexscan;
RESET enable_bitmapscan;
-- If this succeeds, all tuple data and overflow chains are readable
```

### System logs
```bash
# Check PostgreSQL logs for RECNO errors
tail -f $PGDATA/log/postgresql-*.log | grep -iE 'recno|overflow|hlc|dvv'

# Check system logs for hardware/disk errors
dmesg | grep -i "i/o error"
journalctl -u postgresql -p err
```

---

## 10. Recovery Decision Matrix

| Symptom | First Action | If That Fails | Data Loss Risk |
|---------|-------------|---------------|----------------|
| Single row unreadable (overflow) | Skip row, salvage data | Restore from backup | Low (one row) |
| Column decompression error | Disable compression, VACUUM FULL | Restore from backup | Low (one column) |
| INSERT/UPDATE "failed to allocate" | VACUUM | VACUUM FULL | None |
| Server won't start (WAL replay) | Check WAL archive, PITR | pg_resetwal (last resort) | Medium |
| "MVCC not initialized" | Restart PostgreSQL | Check shared_preload_libraries | None |
| "maximum transactions exceeded" | Restart, increase recno_max_transactions | Check for long transactions | None |
| Clock drift FATAL | Fix NTP, restart | Increase offset tolerance | None |
| HLC drift WARNING | Check NTP, monitor | Reduce max_clock_offset_ms | None |
| Index returns wrong results | REINDEX TABLE CONCURRENTLY | REINDEX TABLE | None |
| Widespread corruption | Stop immediately, restore from backup | Contact support | Depends on backup |

---

## 11. Post-Recovery Checklist

After recovering from any corruption:

- [ ] Verify data integrity: Run application-level consistency checks
- [ ] Check table row counts: `SELECT COUNT(*) FROM <table>`
- [ ] Test critical queries: Ensure business logic works correctly
- [ ] Run ANALYZE: `ANALYZE <table>` to update planner statistics
- [ ] Rebuild indexes: `REINDEX TABLE <table>`
- [ ] Check related tables: Repeat diagnostics on tables with foreign key relationships
- [ ] Review PostgreSQL logs: Look for additional errors or warnings
- [ ] Document the incident: Record what happened, root cause, and resolution
- [ ] Improve monitoring: Add alerts to detect similar issues earlier
- [ ] Test backups: Verify backups are restorable and complete
- [ ] Review hardware health: Check SMART data, dmesg for disk errors

---

## 12. Emergency Contacts and Support

If you encounter a RECNO-related corruption issue not covered in this document:

1. **Check PostgreSQL mailing lists**: Search pgsql-general and pgsql-hackers
2. **Report a bug**: Include error messages, pg_waldump output, and steps to reproduce
3. **Commercial support**: Contact your PostgreSQL support vendor
4. **Community help**: Ask on the PostgreSQL Slack or IRC channels

**Include in your report**:
- PostgreSQL version and RECNO build information
- Exact error messages from PostgreSQL logs
- Output of `pg_controldata $PGDATA`
- WAL dump around corrupted record (`pg_waldump`)
- Steps to reproduce (if known)
- Hardware details (especially storage type and ECC RAM status)

---

## 13. Prevention Best Practices Summary

1. **Hardware**:
   - Use ECC RAM
   - Use enterprise storage with checksums
   - Enable data checksums in PostgreSQL (`initdb --data-checksums`)
   - Use battery-backed RAID controllers

2. **Backups**:
   - Daily base backups with `pg_basebackup`
   - Continuous WAL archiving (`archive_mode = on`)
   - Test restore procedures monthly
   - Store backups on separate hardware

3. **Monitoring**:
   - Alert on RECNO-specific errors in logs (overflow, compression, HLC)
   - Monitor `recno_mvcc_stats()`, `recno_fsm_stats()`, `recno_compression_stats()`
   - Track table and index bloat
   - Monitor clock synchronization quality (`chronyc tracking`)

4. **Maintenance**:
   - Configure autovacuum or schedule regular VACUUM
   - Run ANALYZE after bulk data changes
   - Schedule periodic REINDEX CONCURRENTLY for high-update tables
   - Run periodic full-table scans as corruption canaries

5. **Configuration**:
   - `fsync = on` (never disable in production)
   - `full_page_writes = on` (default; prevents partial page writes)
   - `wal_level = replica` or `logical`
   - `data_checksums` enabled at initdb time
   - `recno_max_transactions` set to 2x expected peak concurrency

6. **Testing**:
   - Test recovery procedures regularly
   - Test failover and standby promotion with RECNO tables
   - Perform load testing before production deployment
   - Test crash recovery scenarios (controlled `kill -9` during operations)

---

*Document version: 2.0 (March 2026)*
*Author: RECNO Development Team*
*Status: For PostgreSQL Mailing List Review*
