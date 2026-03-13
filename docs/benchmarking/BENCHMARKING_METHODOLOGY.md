# Orvos Benchmarking Methodology

**Purpose**: Guide for benchmarking Orvos vs Heap performance
**Based on**: Andres Freund's methodology and PostgreSQL best practices

---

## Key Principles

### 1. Always Warmup

**Why**: First run populates caches, JIT compiles code
**How**: Run benchmark for 30s, discard results, then run actual test

```sql
-- Warmup
pgbench -c 8 -T 30 bench_db > /dev/null 2>&1

-- Actual benchmark
pgbench -c 8 -T 60 bench_db
```

---

### 2. Multiple Runs with Statistics

**Why**: Single run can vary 10-30% due to system noise
**How**: Run 3-5 times, report median and standard deviation

```bash
for run in 1 2 3; do
  pgbench -c 8 -T 60 bench_db > run_${run}.log
done

# Calculate median, stddev, coefficient of variation
```

---

### 3. Appropriate Scale Factor

**Why**: Scale too small tests cache, not storage
**Rule**: Scale factor database size should exceed shared_buffers

```bash
# shared_buffers = 256MB
# Minimum scale factor: ~30 (300MB database)
# Recommended scale factor: 100 (1.5GB database)

pgbench -i -s 100 bench_db
```

---

### 4. Report All Parameters

**Always include**:
- Hardware: CPU model, core count, RAM, storage type
- PostgreSQL version: Git commit hash
- Build flags: Optimization level, assertions, debug
- Configuration: shared_buffers, work_mem, maintenance_work_mem
- Test parameters: Scale, clients, duration, test type
- Results: All TPS values, not just one run

---

### 5. Stable System

**Before benchmarking**:
```bash
# Disable CPU frequency scaling
sudo cpupower frequency-set -g performance

# Drop caches (if testing cold cache)
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# Close unnecessary applications
```

---

## Workload Types

### OLTP (TPC-B Like)

**Purpose**: Mixed read/write transactions
**Command**: `pgbench -b tpcb-like`
**Expected**: Orvos comparable to heap (UNDO overhead)

---

### OLAP (SELECT Only)

**Purpose**: Read-heavy analytical queries
**Command**: `pgbench -S`
**Expected**: Orvos faster (compression, column projection)

---

### Wide Table Column Projection

**Purpose**: Test column projection benefit
**Setup**:
```sql
CREATE TABLE wide_test (
  c1 int, c2 text, ..., c30 text
) USING orvos;

-- Test single column scan
SELECT c1 FROM wide_test;
```
**Expected**: Orvos 2-3x faster

---

### CREATE INDEX

**Purpose**: Test column projection during index build
**Setup**:
```sql
CREATE INDEX idx_c1 ON wide_test(c1);
```
**Expected**: Orvos 30-60% faster on wide tables

---

### VACUUM

**Purpose**: Test batch undo operations
**Setup**:
```sql
DELETE FROM test WHERE id % 2 = 0;
VACUUM test;
```
**Expected**: Orvos 2-3x faster

---

## Profiling

### Linux perf

```bash
# Record CPU samples
perf record -F 99 -g -p $PG_PID -- sleep 60

# View report
perf report -i perf.data

# Generate flame graph
perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg
```

### Expected Patterns

**Heap**:
- `heapgettup_pagemode` (15-25%)
- `_bt_compare` (10-15%)
- `heap_page_prune` (5-10%)

**Orvos**:
- `lz4_decompress` (10-20%)
- `ovbt_*` functions (15-25%)
- `orvosam_getnextslot` (10-20%)

---

## Common Pitfalls

### ❌ No warmup
- First run is always slower (cold caches)

### ❌ Single run
- Results can vary 10-30% between runs

### ❌ Too short duration
- Runs < 60s may not be representative

### ❌ Wrong scale
- Scale < shared_buffers tests cache, not storage

### ❌ No system info
- Can't reproduce results without hardware details

### ❌ Ignoring variance
- High stddev means unreliable results

---

## Success Criteria

**Good benchmark**:
- ✅ Warmup run completed
- ✅ 3-5 runs executed
- ✅ Coefficient of variation < 5%
- ✅ All parameters documented
- ✅ System information recorded

**Reliable results**:
- Median within 10% of mean
- Standard deviation < 10% of mean
- Consistent across runs

---

## Quick Start

```bash
# 1. Configure PostgreSQL
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB

# 2. Initialize database
createdb bench_db
pgbench -i -s 100 bench_db

# 3. Warmup
pgbench -c 8 -T 30 bench_db > /dev/null

# 4. Run benchmark (3 times)
for i in 1 2 3; do
  pgbench -c 8 -T 60 bench_db > run_$i.log
  sleep 10
done

# 5. Analyze results
grep "tps =" run_*.log | awk '{print $3}'
```

---

## References

- PostgreSQL pgbench documentation
- Andres Freund's benchmarking posts on pgsql-hackers
- Brendan Gregg's flame graph methodology

---

**Created**: 2026-03-04
**Status**: Best practices guide
