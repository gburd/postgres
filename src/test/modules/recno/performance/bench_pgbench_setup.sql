--
-- bench_pgbench_setup.sql
--
-- Creates tables for pgbench concurrent workload testing.
-- Both HEAP and RECNO versions are created side by side.
--

DROP TABLE IF EXISTS pgbench_heap_accounts CASCADE;
DROP TABLE IF EXISTS pgbench_heap_tellers CASCADE;
DROP TABLE IF EXISTS pgbench_heap_branches CASCADE;

DROP TABLE IF EXISTS pgbench_recno_accounts CASCADE;
DROP TABLE IF EXISTS pgbench_recno_tellers CASCADE;
DROP TABLE IF EXISTS pgbench_recno_branches CASCADE;

-- HEAP version
CREATE TABLE pgbench_heap_branches (
    bid     INT4 PRIMARY KEY,
    bbalance INT4,
    filler  TEXT
) USING heap;

CREATE TABLE pgbench_heap_tellers (
    tid     INT4 PRIMARY KEY,
    bid     INT4,
    tbalance INT4,
    filler  TEXT
) USING heap;

CREATE TABLE pgbench_heap_accounts (
    aid     INT4 PRIMARY KEY,
    bid     INT4,
    abalance INT4,
    filler  TEXT
) USING heap;

-- RECNO version
CREATE TABLE pgbench_recno_branches (
    bid     INT4 PRIMARY KEY,
    bbalance INT4,
    filler  TEXT
) USING recno;

CREATE TABLE pgbench_recno_tellers (
    tid     INT4 PRIMARY KEY,
    bid     INT4,
    tbalance INT4,
    filler  TEXT
) USING recno;

CREATE TABLE pgbench_recno_accounts (
    aid     INT4 PRIMARY KEY,
    bid     INT4,
    abalance INT4,
    filler  TEXT
) USING recno;

-- Populate (10 branches, 100 tellers, 100K accounts)
INSERT INTO pgbench_heap_branches
SELECT i, 0, repeat('x', 84)
FROM generate_series(1, 10) i;

INSERT INTO pgbench_heap_tellers
SELECT i, (i - 1) / 10 + 1, 0, repeat('x', 84)
FROM generate_series(1, 100) i;

INSERT INTO pgbench_heap_accounts
SELECT i, (i - 1) / 10000 + 1, 0, repeat('x', 84)
FROM generate_series(1, 100000) i;

INSERT INTO pgbench_recno_branches
SELECT i, 0, repeat('x', 84)
FROM generate_series(1, 10) i;

INSERT INTO pgbench_recno_tellers
SELECT i, (i - 1) / 10 + 1, 0, repeat('x', 84)
FROM generate_series(1, 100) i;

INSERT INTO pgbench_recno_accounts
SELECT i, (i - 1) / 10000 + 1, 0, repeat('x', 84)
FROM generate_series(1, 100000) i;

ANALYZE pgbench_heap_branches;
ANALYZE pgbench_heap_tellers;
ANALYZE pgbench_heap_accounts;
ANALYZE pgbench_recno_branches;
ANALYZE pgbench_recno_tellers;
ANALYZE pgbench_recno_accounts;
