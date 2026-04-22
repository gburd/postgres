# RECNO Testing Guide

This document describes how to run existing tests, add new tests, and
benchmark the RECNO storage access method.


## Test File Inventory

| File | Type | Coverage |
|------|------|----------|
| `src/test/regress/sql/recno.sql` | Regression | Full DML, data types, overflow, defrag, indexes, FK |
| `src/test/regress/sql/recno_performance.sql` | Performance | Comparison benchmarks with heap |
| `src/test/regress/sql/recno_undo_redo.sql` | Recovery | Undo/redo WAL replay scenarios |
| `src/backend/access/recno/test_recno_locks.sql` | Functional | Locking, isolation levels, rollback |


## Running Tests

### Prerequisites

Build and install PostgreSQL with RECNO support:

```bash
# Meson build
meson setup build
cd build && ninja && ninja install

# Or Make build
./configure
make -j$(nproc) && make install

# Initialize a test cluster
initdb -D /tmp/recno_test_data
pg_ctl -D /tmp/recno_test_data -l /tmp/recno_test.log start
createdb recno_testdb
```

### Regression Tests

```bash
# Run via installcheck (uses running server)
make installcheck EXTRA_TESTS=recno
make installcheck EXTRA_TESTS=recno_performance
make installcheck EXTRA_TESTS=recno_undo_redo

# Run via psql (for interactive debugging)
psql -d recno_testdb -f src/test/regress/sql/recno.sql
psql -d recno_testdb -f src/test/regress/sql/recno_performance.sql
psql -d recno_testdb -f src/test/regress/sql/recno_undo_redo.sql
psql -d recno_testdb -f src/backend/access/recno/test_recno_locks.sql
```

### Running with Verbose Output

The current implementation includes `elog(WARNING, ...)` diagnostic messages
in insert, update, delete, and multi_insert paths. These are visible by
default and provide operation-level tracing:

```
WARNING:  RECNO single insert #1 starting
WARNING:  RECNO insert: added tuple at block=0, offnum=1, maxoff_after=1
```

For additional debug output:

```sql
SET client_min_messages = 'debug1';
SET log_min_messages = 'debug1';
```


## Test Coverage Summary

### recno.sql Coverage

| Feature | Operations Tested |
|---------|-------------------|
| Table creation | `CREATE TABLE ... USING recno` |
| Basic DML | INSERT, SELECT, UPDATE, DELETE |
| Bulk insert | `INSERT ... SELECT generate_series` (100 rows) |
| Large data | INSERT with `repeat('X', 10000)` for overflow |
| Compression | INSERT with `repeat(...)` text for compression |
| Data types | 17 data types (bool, int2, int4, int8, float4, float8, numeric, char, varchar, text, bytea, date, time, timestamp, json, jsonb) |
| NULL values | INSERT DEFAULT VALUES (all nulls) |
| Indexes | CREATE INDEX, EXPLAIN showing index usage |
| VACUUM | VACUUM, VACUUM ANALYZE |
| Defragmentation | DELETE every-other-row then INSERT to trigger defrag |
| Constraints | CHECK constraints, FOREIGN KEY |
| Joins | JOIN between RECNO tables |
| Serializable | `BEGIN ISOLATION LEVEL SERIALIZABLE` + `FOR UPDATE` |
| Truncation | TRUNCATE, verify empty |
| Statistics | Query pg_stats, pg_class for RECNO tables |

### test_recno_locks.sql Coverage

| Feature | Operations Tested |
|---------|-------------------|
| Isolation levels | READ COMMITTED, REPEATABLE READ, SERIALIZABLE |
| Rollback | INSERT + UPDATE then ROLLBACK, verify state |
| Locking | UPDATE with implicit row locks |
| Concurrent access | Multiple sessions with conflicting updates |
| Deadlock detection | Circular lock dependencies |

## Adding New Tests

### Adding Regression Tests

1. Create your test SQL file:

```sql
-- src/test/regress/sql/recno_myfeature.sql
-- Test description and purpose

CREATE TABLE test_myfeature (...) USING recno;

-- Test cases with expected behavior
INSERT INTO test_myfeature ...;
SELECT * FROM test_myfeature WHERE ...;

-- Cleanup
DROP TABLE test_myfeature;
```

2. Generate expected output:

```bash
psql -d testdb -f src/test/regress/sql/recno_myfeature.sql > \
  src/test/regress/expected/recno_myfeature.out
```

3. Add to test schedule (src/test/regress/parallel_schedule):

```
# RECNO tests
test: recno recno_myfeature
```

### Adding Isolation Tests

For concurrency testing, create isolation specs:

```
# src/test/isolation/specs/recno-myfeature.spec

setup
{
  CREATE TABLE test_table (id int, data text) USING recno;
  INSERT INTO test_table VALUES (1, 'initial');
}

teardown
{
  DROP TABLE test_table;
}

session "s1"
setup { BEGIN; }
step "s1_update" { UPDATE test_table SET data = 'session1' WHERE id = 1; }
step "s1_commit" { COMMIT; }

session "s2"
setup { BEGIN; }
step "s2_update" { UPDATE test_table SET data = 'session2' WHERE id = 1; }
step "s2_commit" { COMMIT; }

permutation "s1_update" "s2_update" "s1_commit" "s2_commit"
```

Run with:
```bash
make installcheck-isolation EXTRA_TESTS=recno-myfeature
```

### Adding TAP Tests

For complex scenarios requiring programmatic control:

```perl
# src/test/modules/recno/t/006_myfeature.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->start;

$node->safe_psql('postgres', q{
    CREATE TABLE test_recno (id int, data text) USING recno;
    INSERT INTO test_recno VALUES (1, 'test');
});

# Test specific behavior
my $result = $node->safe_psql('postgres',
    "SELECT * FROM test_recno WHERE id = 1");
is($result, "1|test", "Data retrieved correctly");

# Test crash recovery
$node->stop('immediate');
$node->start;
$result = $node->safe_psql('postgres',
    "SELECT * FROM test_recno WHERE id = 1");
is($result, "1|test", "Data survived crash");

done_testing();
```

## Performance Testing

### Basic Performance Comparison

```sql
-- Create test tables
CREATE TABLE heap_test (id serial, data text, value numeric);
CREATE TABLE recno_test (id serial, data text, value numeric) USING recno;

-- Insert test data
INSERT INTO heap_test (data, value)
  SELECT md5(random()::text), random() * 1000
  FROM generate_series(1, 100000);

INSERT INTO recno_test (data, value)
  SELECT md5(random()::text), random() * 1000
  FROM generate_series(1, 100000);

-- Compare update performance
\timing on

-- Heap updates
BEGIN;
UPDATE heap_test SET value = value * 1.1 WHERE id % 10 = 0;
COMMIT;

-- RECNO updates (should be faster due to in-place)
BEGIN;
UPDATE recno_test SET value = value * 1.1 WHERE id % 10 = 0;
COMMIT;

-- Compare table bloat
SELECT
  'heap' as storage,
  pg_size_pretty(pg_relation_size('heap_test')) as size,
  (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'heap_test') as dead_tuples
UNION ALL
SELECT
  'recno' as storage,
  pg_size_pretty(pg_relation_size('recno_test')) as size,
  (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'recno_test') as dead_tuples;
```

### pgbench Custom Scripts

Create recno_bench.sql:

```sql
\set aid random(1, 100000)
\set delta random(-100, 100)
BEGIN;
UPDATE recno_accounts SET balance = balance + :delta WHERE aid = :aid;
SELECT balance FROM recno_accounts WHERE aid = :aid;
COMMIT;
```

Run benchmark:

```bash
# Initialize
createdb benchdb
psql -d benchdb -c "CREATE TABLE recno_accounts (aid int primary key, balance numeric) USING recno"
psql -d benchdb -c "INSERT INTO recno_accounts SELECT i, 1000 FROM generate_series(1, 100000) i"

# Run benchmark
pgbench -c 10 -j 2 -T 60 -f recno_bench.sql benchdb

# Compare with heap
psql -d benchdb -c "CREATE TABLE heap_accounts AS SELECT * FROM recno_accounts"
# Modify script to use heap_accounts and rerun
```

## Code Coverage Measurement

### Setup Coverage Build

```bash
# Configure with coverage
./configure --enable-coverage CFLAGS="-O0"
make clean
make -j$(nproc)
make install

# Initialize coverage
lcov --directory . --zerocounters
```

### Run Tests with Coverage

```bash
# Run all RECNO tests
make installcheck EXTRA_TESTS="recno recno_performance recno_undo_redo"
make installcheck-isolation

# Generate coverage report
lcov --directory . --capture --output-file coverage.info
lcov --remove coverage.info '/usr/*' --output-file coverage.info
genhtml coverage.info --output-directory coverage-report

# View report
firefox coverage-report/index.html
```

### Target Coverage Areas

Focus coverage testing on:

1. **Core Operations** (>95% target)
   - Insert, update, delete paths
   - In-place vs out-of-place updates
   - Tuple visibility checks

2. **Advanced Features** (>90% target)
   - Overflow page operations
   - Compression/decompression
   - Free space management
   - Defragmentation

3. **Error Paths** (>80% target)
   - Lock timeouts
   - Space exhaustion
   - Invalid TIDs
   - Corrupted data handling

4. **WAL and Recovery** (>90% target)
   - All WAL record types
   - Crash recovery scenarios
   - Partial page writes

## Debugging Tests

### Enable Detailed Logging

```sql
-- In test session
SET client_min_messages = 'debug5';
SET log_statement = 'all';
SET log_min_messages = 'debug5';
SET trace_recovery_messages = 'on';

-- RECNO-specific debug
SET recno_debug_level = 2;  -- If implemented
```

### GDB Debugging

```bash
# Attach to backend
psql -d testdb
SELECT pg_backend_pid();  -- Note PID

# In another terminal
gdb -p <PID>

# Set breakpoints
(gdb) break recno_tuple_insert
(gdb) break RecnoTupleVisible
(gdb) continue

# Trigger breakpoint from psql session
INSERT INTO test_table VALUES (1, 'test');
```

### Common Test Failures

1. **Visibility Issues**
   ```sql
   -- Debug with explicit timestamp checks
   SELECT t.*, t.xmin, t.xmax,
          (t.tableoid::regclass)::text as table_name
   FROM test_table t;
   ```

2. **Lock Timeouts**
   ```sql
   -- Check lock status
   SELECT * FROM pg_locks WHERE relation = 'test_table'::regclass;
   ```

3. **Space Management**
   ```sql
   -- Check FSM accuracy
   SELECT * FROM pg_freespace('test_table');
   SELECT * FROM recno_fsm_stats('test_table');
   ```

## Continuous Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/recno-tests.yml
name: RECNO Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Build PostgreSQL with RECNO
      run: |
        ./configure --enable-debug --enable-cassert
        make -j$(nproc)
        make install

    - name: Run Tests
      run: |
        make installcheck EXTRA_TESTS="recno recno_performance"
        make installcheck-isolation

    - name: Upload Test Results
      if: failure()
      uses: actions/upload-artifact@v2
      with:
        name: regression-diffs
        path: src/test/regress/regression.diffs
```

## Test Data Generation

### Large Dataset Testing

```python
#!/usr/bin/env python3
# generate_test_data.py

import psycopg2
import random
import string
from datetime import datetime, timedelta

conn = psycopg2.connect("dbname=testdb")
cur = conn.cursor()

# Create test table
cur.execute("""
    CREATE TABLE IF NOT EXISTS large_test (
        id SERIAL PRIMARY KEY,
        user_id INT,
        timestamp TIMESTAMPTZ,
        event_type TEXT,
        payload JSONB,
        description TEXT
    ) USING recno
""")

# Generate 1M rows
batch_size = 10000
for batch in range(100):
    data = []
    for i in range(batch_size):
        user_id = random.randint(1, 10000)
        timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
        event_type = random.choice(['login', 'purchase', 'view', 'click'])
        payload = {
            'session_id': ''.join(random.choices(string.hexdigits, k=32)),
            'value': random.uniform(0, 1000)
        }
        description = ''.join(random.choices(string.ascii_letters, k=random.randint(10, 1000)))

        data.append((user_id, timestamp, event_type, json.dumps(payload), description))

    cur.executemany("""
        INSERT INTO large_test (user_id, timestamp, event_type, payload, description)
        VALUES (%s, %s, %s, %s, %s)
    """, data)
    conn.commit()
    print(f"Inserted batch {batch + 1}/100")

cur.close()
conn.close()
```

### Missing Test Coverage

The following areas are not yet covered by tests:

- **Crash recovery**: No TAP tests that kill the server mid-operation
  and verify recovery via WAL replay
- **Concurrency**: No isolation tests using `isolationtester` that verify
  correct behavior under concurrent access
- **Overflow chain integrity**: No test that verifies overflow data
  survives crash + recovery
- **Compression effectiveness**: No test that verifies compressed data
  round-trips correctly (since LZ4/ZSTD compression are stubs; delta
  and dictionary compression have real implementations)
- **Defragmentation correctness**: No test that verifies TID stability
  or offset mapping correctness after defrag
- **Error paths**: No tests for out-of-space, corrupted pages, invalid
  overflow chains
- **Parallel scan**: Not implemented, no tests
- **Replication**: No tests for streaming replication of RECNO WAL records
- **HLC/DVV**: No tests for HLC timestamp ordering, DVV generation,
  uncertainty interval behavior, or TSC calibration
- **Custom slot operations**: No tests specifically verifying
  RecnoTupleTableSlot deforming correctness for edge cases (wide
  tuples, many nulls, mixed fixed/variable-length attributes)
- **Cross-page defragmentation**: No test for the XLOG_RECNO_CROSS_PAGE_DEFRAG
  WAL record type and its replay


## Adding New Tests

### Regression Tests

Add new SQL test files to `src/test/regress/sql/`:

```sql
-- src/test/regress/sql/recno_new_feature.sql

-- Setup
CREATE TABLE recno_feature_test (
    id SERIAL PRIMARY KEY,
    data TEXT
) USING recno;

-- Test the feature
INSERT INTO recno_feature_test (data) VALUES ('test');
-- ... operations ...

-- Verify results
SELECT * FROM recno_feature_test ORDER BY id;

-- Cleanup
DROP TABLE recno_feature_test;
```

Generate expected output:

```bash
psql -d recno_testdb -f src/test/regress/sql/recno_new_feature.sql \
    > src/test/regress/expected/recno_new_feature.out 2>&1
```

Register the test in `src/test/regress/parallel_schedule` or run via
`EXTRA_TESTS`.

### Isolation Tests

For concurrency testing, create an isolation test spec in
`src/test/isolation/specs/`:

```
# src/test/isolation/specs/recno_concurrent_update.spec

setup
{
    CREATE TABLE recno_iso_test (id int, val int) USING recno;
    INSERT INTO recno_iso_test VALUES (1, 100);
}

teardown
{
    DROP TABLE recno_iso_test;
}

session s1
setup { BEGIN; }
step s1_update { UPDATE recno_iso_test SET val = 200 WHERE id = 1; }
step s1_commit { COMMIT; }

session s2
setup { BEGIN; }
step s2_update { UPDATE recno_iso_test SET val = 300 WHERE id = 1; }
step s2_commit { COMMIT; }

# Test: concurrent updates should serialize
permutation s1_update s2_update s1_commit s2_commit
```

Run with:

```bash
make -C src/test/isolation check EXTRA_TESTS=recno_concurrent_update
```

### TAP Tests

For recovery testing, create a TAP test in `src/test/recovery/t/`:

```perl
# src/test/recovery/t/042_recno_recovery.pl
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->start;

# Create RECNO table and insert data
$node->safe_psql('postgres', q{
    CREATE TABLE recno_recovery (id int, data text) USING recno;
    INSERT INTO recno_recovery VALUES (1, 'before crash');
});

# Force a checkpoint
$node->safe_psql('postgres', 'CHECKPOINT');

# Insert more data (will be in WAL but not checkpointed)
$node->safe_psql('postgres', q{
    INSERT INTO recno_recovery VALUES (2, 'after checkpoint');
});

# Crash the server
$node->stop('immediate');

# Restart and verify recovery
$node->start;

my $result = $node->safe_psql('postgres',
    'SELECT count(*) FROM recno_recovery');
is($result, '2', 'All rows recovered after crash');

$node->stop;
done_testing();
```


## Performance Benchmarking

### Quick Comparison with Heap

The `recno_performance.sql` test includes basic comparisons. For more
thorough benchmarking:

```sql
-- Create matched tables
CREATE TABLE bench_heap (id serial, val int, data text) USING heap;
CREATE TABLE bench_recno (id serial, val int, data text) USING recno;

-- Measure insert throughput
\timing on

INSERT INTO bench_heap (val, data)
SELECT i, repeat('x', 100) FROM generate_series(1, 100000) i;

INSERT INTO bench_recno (val, data)
SELECT i, repeat('x', 100) FROM generate_series(1, 100000) i;

-- Measure update throughput (in-place candidate)
UPDATE bench_heap SET val = val + 1;
UPDATE bench_recno SET val = val + 1;

-- Measure update throughput (size change)
UPDATE bench_heap SET data = repeat('y', 200);
UPDATE bench_recno SET data = repeat('y', 200);

-- Measure sequential scan
SELECT count(*) FROM bench_heap;
SELECT count(*) FROM bench_recno;

-- Compare storage size
SELECT pg_relation_size('bench_heap') as heap_bytes,
       pg_relation_size('bench_recno') as recno_bytes;

-- Cleanup
DROP TABLE bench_heap, bench_recno;
```

### pgbench Custom Script

Create a custom pgbench script for RECNO workload testing:

```sql
-- pgbench_recno_init.sql
CREATE TABLE pgbench_recno (
    aid int NOT NULL,
    abalance int,
    filler text
) USING recno;

INSERT INTO pgbench_recno (aid, abalance, filler)
SELECT i, 0, repeat('x', 80)
FROM generate_series(1, 100000) i;
```

```sql
-- pgbench_recno_update.sql (custom script)
\set aid random(1, 100000)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_recno SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_recno WHERE aid = :aid;
END;
```

Run with:

```bash
psql -f pgbench_recno_init.sql
pgbench -f pgbench_recno_update.sql -c 4 -j 2 -T 60 recno_testdb
```

### Metrics to Track

When benchmarking RECNO, measure these metrics:

| Metric | How to Measure |
|--------|---------------|
| Insert TPS | pgbench or \timing |
| Update TPS (in-place) | Update same-size values |
| Update TPS (grow) | Update to larger values |
| Sequential scan time | `EXPLAIN ANALYZE SELECT count(*)` |
| Table size after updates | `pg_relation_size()` |
| Table size after deletes | `pg_relation_size()` |
| WAL volume | `pg_stat_wal` before/after |
| Buffer hits/reads | `pg_stat_user_tables` |

### Monitoring During Tests

```sql
-- Reset statistics
SELECT pg_stat_reset();

-- Run workload...

-- Check table I/O
SELECT relname, seq_scan, seq_tup_read, idx_scan,
       n_tup_ins, n_tup_upd, n_tup_del,
       n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname LIKE 'recno%' OR relname LIKE 'bench%';

-- Check I/O timing (requires track_io_timing = on)
SELECT relname, heap_blks_read, heap_blks_hit
FROM pg_statio_user_tables
WHERE relname LIKE 'recno%' OR relname LIKE 'bench%';
```


## Testing HLC/DVV Functionality

HLC and DVV can be tested by enabling the GUC parameters and verifying
causal ordering:

```sql
-- Verify HLC is enabled
SHOW recno_use_hlc;  -- should be 'on'
SHOW recno_node_id;  -- should be '0' (default)

-- Create a table and insert data
CREATE TABLE hlc_test (id int, val text) USING recno;
INSERT INTO hlc_test VALUES (1, 'first');
INSERT INTO hlc_test VALUES (2, 'second');

-- Verify that commit timestamps are monotonically increasing
-- (requires internal access to tuple headers; use debug logging)
SET client_min_messages = 'debug1';
SELECT * FROM hlc_test;

-- Test with different node IDs (simulating multi-node)
SET recno_node_id = 1;
INSERT INTO hlc_test VALUES (3, 'from node 1');
SET recno_node_id = 0;  -- reset

-- Test uncertainty interval
SET recno_max_clock_offset_ms = 500;
-- Operations during the uncertainty window may block reads
-- until the window passes (when recno_uncertainty_wait = on)

-- Cleanup
DROP TABLE hlc_test;
```

### Testing Slot Operations

The custom `RecnoTupleTableSlot` can be exercised with:

```sql
CREATE TABLE slot_test (
    a int,
    b text,
    c float8,
    d bytea,
    e bool,
    f timestamptz
) USING recno;

-- Test null handling
INSERT INTO slot_test (a) VALUES (1);
INSERT INTO slot_test DEFAULT VALUES;

-- Test variable-width attributes
INSERT INTO slot_test VALUES (2, repeat('x', 5000), 3.14, '\xDEADBEEF', true, now());

-- Test incremental deforming (accessing attributes in different orders)
SELECT a FROM slot_test;
SELECT f, e, d FROM slot_test;
SELECT * FROM slot_test;

-- Test materialization (subqueries force materialization)
SELECT * FROM slot_test WHERE a IN (SELECT a FROM slot_test WHERE b IS NOT NULL);

DROP TABLE slot_test;
```


## Debugging Test Failures

### Common Failure Modes

1. **"RECNO failed to allocate page for tuple insertion"**
   - The FSM could not find or create a page with enough space.
   - Check if the relation's underlying storage is accessible.
   - Verify that `RelationGetSmgr()` succeeded.

2. **"failed to add RECNO tuple to page"**
   - `RecnoPageAddTuple()` returned `InvalidOffsetNumber`.
   - The page's item pointer array is full or the page has no space.
   - This is a PANIC inside a critical section -- check WAL replay.

3. **Incorrect scan results (missing or extra rows)**
   - Check `RecnoTupleVisible()` logic: is the snapshot timestamp correct?
   - Verify that `t_commit_ts` was set correctly during insert.
   - Check for missing `RECNO_TUPLE_DELETED` flag on deleted tuples.

4. **Crash during WAL replay**
   - Check `recno_redo()` for the specific XLOG record type.
   - Verify buffer initialization: `XLogInitBufferForRedo` vs `ReadBuffer`.
   - Check that all registered buffers match the expected page state.

### Examining Page Contents

If `pageinspect` is extended for RECNO (not yet done), you can examine
raw page contents:

```sql
-- Until RECNO-specific pageinspect functions exist, use raw page
-- examination via the existing functions
SELECT * FROM page_header(get_raw_page('my_recno_table', 0));
SELECT * FROM heap_page_items(get_raw_page('my_recno_table', 0));
-- Note: heap_page_items will show raw data but interpret headers
-- incorrectly since RECNO has a different tuple header format.
```

For accurate page examination, use GDB:

```gdb
# Attach to backend PID
attach <pid>

# Read a specific page
p *BufferGetPage(ReadBuffer(rel, 0))

# Examine tuple at offset 1
set $page = BufferGetPage(ReadBuffer(rel, 0))
set $itemid = PageGetItemId($page, 1)
p *(RecnoTupleHeader *)PageGetItem($page, $itemid)
```
