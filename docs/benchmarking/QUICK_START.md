# Orvos Performance Testing - Quick Start

**Prerequisites**: PostgreSQL built and installed, `pg-aliases.sh` sourced

---

## Step 1: Setup and Start PostgreSQL

```bash
# First time only - setup build if not done
pg-setup

# Build PostgreSQL with your changes
pg-build

# Install to $PG_INSTALL_DIR
pg-install

# Initialize PostgreSQL data directory
pg-init

# Start PostgreSQL in background
pg-start-bg

# Verify it's running
pg-status
```

---

## Step 2: Create Benchmark Database

```bash
# Create database for benchmarking
createdb bench_heap
createdb bench_orvos

# Initialize pgbench schema with scale factor 100 (~1.5GB)
# This ensures database is larger than shared_buffers (256MB)
pgbench -i -s 100 bench_heap

# For Orvos, we need to create tables using orvos access method
# First initialize with default (heap), then we'll convert
pgbench -i -s 100 bench_orvos
```

---

## Step 3: Convert Orvos Tables

```bash
# Connect to bench_orvos and convert tables to use orvos
psql bench_orvos <<'EOF'
-- Drop and recreate pgbench tables using orvos
BEGIN;

-- Save data
CREATE TEMP TABLE pgbench_accounts_temp AS SELECT * FROM pgbench_accounts;
CREATE TEMP TABLE pgbench_branches_temp AS SELECT * FROM pgbench_branches;
CREATE TEMP TABLE pgbench_tellers_temp AS SELECT * FROM pgbench_tellers;
CREATE TEMP TABLE pgbench_history_temp AS SELECT * FROM pgbench_history;

-- Recreate with orvos
DROP TABLE pgbench_accounts CASCADE;
DROP TABLE pgbench_branches CASCADE;
DROP TABLE pgbench_tellers CASCADE;
DROP TABLE pgbench_history CASCADE;

CREATE TABLE pgbench_accounts (
    aid int NOT NULL,
    bid int,
    abalance int,
    filler char(84),
    PRIMARY KEY (aid)
) USING orvos;

CREATE TABLE pgbench_branches (
    bid int NOT NULL,
    bbalance int,
    filler char(88),
    PRIMARY KEY (bid)
) USING orvos;

CREATE TABLE pgbench_tellers (
    tid int NOT NULL,
    bid int,
    tbalance int,
    filler char(84),
    PRIMARY KEY (tid)
) USING orvos;

CREATE TABLE pgbench_history (
    tid int,
    bid int,
    aid int,
    delta int,
    mtime timestamp,
    filler char(22)
) USING orvos;

-- Restore data
INSERT INTO pgbench_accounts SELECT * FROM pgbench_accounts_temp;
INSERT INTO pgbench_branches SELECT * FROM pgbench_branches_temp;
INSERT INTO pgbench_tellers SELECT * FROM pgbench_tellers_temp;
INSERT INTO pgbench_history SELECT * FROM pgbench_history_temp;

-- Recreate indexes
CREATE INDEX ON pgbench_accounts(bid);
CREATE INDEX ON pgbench_tellers(bid);

-- Analyze for query planning
ANALYZE;

COMMIT;
EOF
```

---

## Step 4: Configure PostgreSQL for Benchmarking

Edit `$PGDATA/postgresql.conf` or use `ALTER SYSTEM`:

```bash
psql -d bench_heap <<'EOF'
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '128MB';
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET max_wal_size = '2GB';
EOF

# Restart PostgreSQL for settings to take effect
pg-stop
sleep 2
pg-start-bg
```

---

## Step 5: Run Benchmarks

### OLTP Benchmark (TPC-B like)

```bash
# Warmup (discard results)
echo "=== Warmup heap ==="
pgbench -c 8 -T 30 bench_heap > /dev/null

echo "=== Warmup orvos ==="
pgbench -c 8 -T 30 bench_orvos > /dev/null

# Run actual benchmarks (3 runs each)
echo "=== Benchmarking heap ==="
for i in 1 2 3; do
  echo "Run $i/3..."
  pgbench -c 8 -T 60 bench_heap > heap_run_${i}.log
  sleep 10
done

echo "=== Benchmarking orvos ==="
for i in 1 2 3; do
  echo "Run $i/3..."
  pgbench -c 8 -T 60 bench_orvos > orvos_run_${i}.log
  sleep 10
done

# Analyze results
echo ""
echo "=== HEAP RESULTS ==="
grep "tps =" heap_run_*.log | awk '{print $3}'

echo ""
echo "=== ORVOS RESULTS ==="
grep "tps =" orvos_run_*.log | awk '{print $3}'
```

### OLAP Benchmark (SELECT only)

```bash
# Warmup
pgbench -c 8 -T 30 -S bench_heap > /dev/null
pgbench -c 8 -T 30 -S bench_orvos > /dev/null

# Run benchmarks
for i in 1 2 3; do
  pgbench -c 8 -T 60 -S bench_heap > heap_select_${i}.log
  sleep 10
done

for i in 1 2 3; do
  pgbench -c 8 -T 60 -S bench_orvos > orvos_select_${i}.log
  sleep 10
done

# Results
echo "=== HEAP SELECT RESULTS ==="
grep "tps =" heap_select_*.log | awk '{print $3}'

echo "=== ORVOS SELECT RESULTS ==="
grep "tps =" orvos_select_*.log | awk '{print $3}'
```

---

## Step 6: Test Column Projection

Create a wide table to test column projection benefits:

```bash
psql bench_heap <<'EOF'
-- Create 30-column table
CREATE TABLE wide_test_heap (
    c1 int PRIMARY KEY,
    c2 text, c3 text, c4 text, c5 text, c6 text,
    c7 text, c8 text, c9 text, c10 text, c11 text,
    c12 text, c13 text, c14 text, c15 text, c16 text,
    c17 text, c18 text, c19 text, c20 text, c21 text,
    c22 text, c23 text, c24 text, c25 text, c26 text,
    c27 text, c28 text, c29 text, c30 text
);

-- Populate with data
INSERT INTO wide_test_heap
SELECT i, repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100)
FROM generate_series(1, 100000) i;
EOF

psql bench_orvos <<'EOF'
-- Same for orvos
CREATE TABLE wide_test_orvos (
    c1 int PRIMARY KEY,
    c2 text, c3 text, c4 text, c5 text, c6 text,
    c7 text, c8 text, c9 text, c10 text, c11 text,
    c12 text, c13 text, c14 text, c15 text, c16 text,
    c17 text, c18 text, c19 text, c20 text, c21 text,
    c22 text, c23 text, c24 text, c25 text, c26 text,
    c27 text, c28 text, c29 text, c30 text
) USING orvos;

INSERT INTO wide_test_orvos
SELECT i, repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100), repeat('x', 100),
       repeat('x', 100), repeat('x', 100)
FROM generate_series(1, 100000) i;
EOF

# Test single column query (should show Orvos advantage)
echo "=== Testing Column Projection ==="
psql bench_heap -c '\timing' -c 'SELECT c1 FROM wide_test_heap WHERE c1 < 50000;'
psql bench_orvos -c '\timing' -c 'SELECT c1 FROM wide_test_orvos WHERE c1 < 50000;'
```

---

## Step 7: Test VACUUM Performance

```bash
# Heap VACUUM test
psql bench_heap <<'EOF'
\timing
DELETE FROM pgbench_accounts WHERE aid % 2 = 0;
VACUUM pgbench_accounts;
EOF

# Orvos VACUUM test
psql bench_orvos <<'EOF'
\timing
DELETE FROM pgbench_accounts WHERE aid % 2 = 0;
VACUUM pgbench_accounts;
EOF
```

---

## Step 8: Test CREATE INDEX Performance

```bash
# Drop existing indexes
psql bench_heap -c 'DROP INDEX IF EXISTS pgbench_accounts_bid_idx;'
psql bench_orvos -c 'DROP INDEX IF EXISTS pgbench_accounts_bid_idx;'

# Time index creation
echo "=== Heap CREATE INDEX ==="
psql bench_heap -c '\timing' -c 'CREATE INDEX pgbench_accounts_bid_idx ON pgbench_accounts(bid);'

echo "=== Orvos CREATE INDEX ==="
psql bench_orvos -c '\timing' -c 'CREATE INDEX pgbench_accounts_bid_idx ON pgbench_accounts(bid);'

# Test on wide table (should show bigger difference)
psql bench_heap -c '\timing' -c 'CREATE INDEX wide_test_heap_c1_idx ON wide_test_heap(c1);'
psql bench_orvos -c '\timing' -c 'CREATE INDEX wide_test_orvos_c1_idx ON wide_test_orvos(c1);'
```

---

## Step 9: Profile with perf (Linux)

```bash
# Get PostgreSQL backend PID
PG_PID=$(psql bench_orvos -c "SELECT pg_backend_pid();" -t | xargs)

# Start perf recording in background
perf record -F 99 -g -p $PG_PID -o orvos_perf.data -- sleep 60 &

# Run workload while perf records
pgbench -c 8 -T 60 bench_orvos

# Generate report
perf report -i orvos_perf.data

# Generate flame graph (if FlameGraph tools installed)
perf script -i orvos_perf.data | stackcollapse-perf.pl | flamegraph.pl > orvos_flame.svg
```

---

## Expected Results

Based on Phase 2 analysis:

| Workload | Expected Orvos vs Heap |
|----------|----------------------|
| OLTP (TPC-B) | 0.9-1.0x (comparable) |
| OLAP (SELECT only) | 1.2-1.5x (faster) |
| Wide table (1 col) | 2-3x (much faster) |
| CREATE INDEX (wide) | 1.5-2x (faster) |
| VACUUM | 2-3x (much faster) |
| Storage size | 0.5-0.7x (30-50% smaller) |

---

## Troubleshooting

**PostgreSQL won't start:**
```bash
pg-stop
rm -rf $PGDATA
pg-init
pg-start-bg
```

**Out of memory during benchmark:**
- Reduce scale factor: `pgbench -i -s 50 bench_db`
- Reduce client count: `pgbench -c 4 -T 60 bench_db`

**High variance in results (CV > 5%):**
- Ensure system is idle (close browsers, etc.)
- Run longer benchmarks: `pgbench -c 8 -T 120 bench_db`
- Do more warmup: `pgbench -c 8 -T 60 bench_db > /dev/null`

---

## Full Automated Script

Save as `run_benchmarks.sh`:

```bash
#!/bin/bash
set -euo pipefail

echo "=== Setting up benchmark databases ==="
pg-stop 2>/dev/null || true
sleep 2
pg-init
pg-start-bg
sleep 5

# Create databases
createdb bench_heap
createdb bench_orvos

# Initialize
pgbench -i -s 100 bench_heap
pgbench -i -s 100 bench_orvos

# TODO: Convert orvos tables (add SQL from Step 3 above)

echo "=== Running OLTP benchmarks ==="
# Warmup
pgbench -c 8 -T 30 bench_heap > /dev/null
pgbench -c 8 -T 30 bench_orvos > /dev/null

# Benchmark
for i in 1 2 3; do
  pgbench -c 8 -T 60 bench_heap > heap_run_${i}.log
  pgbench -c 8 -T 60 bench_orvos > orvos_run_${i}.log
done

echo "=== Results ==="
echo "HEAP:"
grep "tps =" heap_run_*.log | awk '{print $3}'
echo "ORVOS:"
grep "tps =" orvos_run_*.log | awk '{print $3}'
```

---

**See also**:
- `docs/benchmarking/BENCHMARKING_METHODOLOGY.md` - Detailed methodology
- `docs/PHASE2_EXECUTIVE_SUMMARY.md` - Expected performance improvements
