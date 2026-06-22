#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Benchmark RECNO VACUUM performance.
# Tests VACUUM, VACUUM FULL, and autovacuum behavior.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(gettimeofday tv_interval usleep);
use Getopt::Long;

my $scale = 10000;  # Number of initial rows
my $verbose = 0;

GetOptions(
    'scale=i' => \$scale,
    'verbose' => \$verbose
) or die "Usage: $0 [--scale=N] [--verbose]\n";

# Initialize cluster with aggressive autovacuum for testing
my $node = PostgreSQL::Test::Cluster->new('recno_vacuum');
$node->init;

# Configure autovacuum for testing
$node->append_conf('postgresql.conf', 'autovacuum = on');
$node->append_conf('postgresql.conf', 'autovacuum_naptime = 10s');
$node->append_conf('postgresql.conf', 'autovacuum_vacuum_threshold = 50');
$node->append_conf('postgresql.conf', 'autovacuum_vacuum_scale_factor = 0.1');
$node->append_conf('postgresql.conf', 'log_autovacuum_min_duration = 0');
$node->start;

print "VACUUM Performance Test\n";
print "=" x 50 . "\n";
print "Scale: $scale rows\n\n";

# ============================================================
# Setup: Create and populate test tables
# ============================================================

$node->safe_psql('postgres', qq{
    -- RECNO table
    CREATE TABLE recno_vacuum_test (
        id int PRIMARY KEY,
        val int,
        data text,
        updated_at timestamp DEFAULT now()
    ) USING recno;

    -- Heap table for comparison
    CREATE TABLE heap_vacuum_test (
        id int PRIMARY KEY,
        val int,
        data text,
        updated_at timestamp DEFAULT now()
    ) USING heap;

    -- Create indexes
    CREATE INDEX recno_vacuum_val_idx ON recno_vacuum_test(val);
    CREATE INDEX heap_vacuum_val_idx ON heap_vacuum_test(val);
});

# Insert initial data
print "Inserting initial data...\n";
$node->safe_psql('postgres', qq{
    INSERT INTO recno_vacuum_test
    SELECT i, i % 1000, 'initial_' || i, now()
    FROM generate_series(1, $scale) i;

    INSERT INTO heap_vacuum_test
    SELECT i, i % 1000, 'initial_' || i, now()
    FROM generate_series(1, $scale) i;
});

# ============================================================
# Test 1: VACUUM after bulk DELETE
# ============================================================

print "\nTest 1: VACUUM after bulk DELETE\n";
print "-" x 30 . "\n";

# Delete 30% of rows
$node->safe_psql('postgres', qq{
    DELETE FROM recno_vacuum_test WHERE id % 3 = 0;
    DELETE FROM heap_vacuum_test WHERE id % 3 = 0;
});

# Get dead tuple count before VACUUM
my $recno_dead_before = $node->safe_psql('postgres',
    "SELECT n_dead_tup FROM pg_stat_user_tables WHERE tablename = 'recno_vacuum_test'");
my $heap_dead_before = $node->safe_psql('postgres',
    "SELECT n_dead_tup FROM pg_stat_user_tables WHERE tablename = 'heap_vacuum_test'");

printf "Dead tuples before VACUUM:\n";
printf "  RECNO: %d\n", $recno_dead_before;
printf "  Heap:  %d\n", $heap_dead_before;

# VACUUM RECNO
my $t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM VERBOSE recno_vacuum_test');
my $recno_vacuum_time = tv_interval($t0);

# VACUUM Heap
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM VERBOSE heap_vacuum_test');
my $heap_vacuum_time = tv_interval($t0);

printf "VACUUM time:\n";
printf "  RECNO: %.3f seconds\n", $recno_vacuum_time;
printf "  Heap:  %.3f seconds\n", $heap_vacuum_time;
printf "  Ratio: %.2fx\n\n", $heap_vacuum_time / $recno_vacuum_time;

# ============================================================
# Test 2: VACUUM after bulk UPDATE
# ============================================================

print "Test 2: VACUUM after bulk UPDATE\n";
print "-" x 30 . "\n";

# Update 50% of rows
$node->safe_psql('postgres', qq{
    UPDATE recno_vacuum_test
    SET data = 'updated_' || id, updated_at = now()
    WHERE id % 2 = 0;

    UPDATE heap_vacuum_test
    SET data = 'updated_' || id, updated_at = now()
    WHERE id % 2 = 0;
});

# VACUUM RECNO
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM VERBOSE recno_vacuum_test');
my $recno_vacuum_update_time = tv_interval($t0);

# VACUUM Heap
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM VERBOSE heap_vacuum_test');
my $heap_vacuum_update_time = tv_interval($t0);

printf "VACUUM after UPDATE:\n";
printf "  RECNO: %.3f seconds\n", $recno_vacuum_update_time;
printf "  Heap:  %.3f seconds\n", $heap_vacuum_update_time;
printf "  Ratio: %.2fx\n\n", $heap_vacuum_update_time / $recno_vacuum_update_time;

# ============================================================
# Test 3: VACUUM FULL performance
# ============================================================

print "Test 3: VACUUM FULL performance\n";
print "-" x 30 . "\n";

# Get size before VACUUM FULL
my $recno_size_before = $node->safe_psql('postgres',
    "SELECT pg_relation_size('recno_vacuum_test')");
my $heap_size_before = $node->safe_psql('postgres',
    "SELECT pg_relation_size('heap_vacuum_test')");

# VACUUM FULL RECNO
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM FULL VERBOSE recno_vacuum_test');
my $recno_vacuum_full_time = tv_interval($t0);

# VACUUM FULL Heap
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM FULL VERBOSE heap_vacuum_test');
my $heap_vacuum_full_time = tv_interval($t0);

# Get size after VACUUM FULL
my $recno_size_after = $node->safe_psql('postgres',
    "SELECT pg_relation_size('recno_vacuum_test')");
my $heap_size_after = $node->safe_psql('postgres',
    "SELECT pg_relation_size('heap_vacuum_test')");

printf "VACUUM FULL time:\n";
printf "  RECNO: %.3f seconds\n", $recno_vacuum_full_time;
printf "  Heap:  %.3f seconds\n", $heap_vacuum_full_time;
printf "  Ratio: %.2fx\n\n", $heap_vacuum_full_time / $recno_vacuum_full_time;

printf "Space reclaimed:\n";
printf "  RECNO: %d bytes (%.1f%%)\n",
    $recno_size_before - $recno_size_after,
    (($recno_size_before - $recno_size_after) / $recno_size_before) * 100;
printf "  Heap:  %d bytes (%.1f%%)\n\n",
    $heap_size_before - $heap_size_after,
    (($heap_size_before - $heap_size_after) / $heap_size_before) * 100;

# ============================================================
# Test 4: HOT updates and VACUUM
# ============================================================

print "Test 4: Non-indexed column updates and VACUUM\n";
print "-" x 30 . "\n";

# Create tables with indexed and non-indexed columns
$node->safe_psql('postgres', qq{
    CREATE TABLE recno_nonidx_vacuum (
        id int PRIMARY KEY,
        indexed_col int,
        non_indexed text
    ) USING recno;

    CREATE TABLE heap_nonidx_vacuum (
        id int PRIMARY KEY,
        indexed_col int,
        non_indexed text
    ) USING heap;

    CREATE INDEX ON recno_nonidx_vacuum(indexed_col);
    CREATE INDEX ON heap_nonidx_vacuum(indexed_col);

    INSERT INTO recno_nonidx_vacuum
    SELECT i, i, 'initial'
    FROM generate_series(1, 1000) i;

    INSERT INTO heap_nonidx_vacuum
    SELECT i, i, 'initial'
    FROM generate_series(1, 1000) i;
});

# Perform non-indexed column updates
$node->safe_psql('postgres', qq{
    UPDATE recno_nonidx_vacuum SET non_indexed = 'updated';
    UPDATE heap_nonidx_vacuum SET non_indexed = 'updated';
});

# Check update statistics
my $recno_update_stats = $node->safe_psql('postgres', qq{
    SELECT n_tup_upd
    FROM pg_stat_user_tables
    WHERE tablename = 'recno_nonidx_vacuum';
});

my $heap_update_stats = $node->safe_psql('postgres', qq{
    SELECT n_tup_upd
    FROM pg_stat_user_tables
    WHERE tablename = 'heap_nonidx_vacuum';
});

printf "Non-indexed updates:\n";
printf "  RECNO: %d\n", $recno_update_stats;
printf "  Heap:  %d\n", $heap_update_stats;

# VACUUM after updates
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM recno_nonidx_vacuum');
my $recno_nonidx_vacuum_time = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM heap_nonidx_vacuum');
my $heap_nonidx_vacuum_time = tv_interval($t0);

printf "VACUUM after non-indexed updates:\n";
printf "  RECNO: %.3f seconds\n", $recno_nonidx_vacuum_time;
printf "  Heap:  %.3f seconds\n\n", $heap_nonidx_vacuum_time;

# ============================================================
# Test 5: Visibility map and VACUUM skip
# ============================================================

print "Test 5: Visibility map and VACUUM skip\n";
print "-" x 30 . "\n";

# Create tables with all-visible pages
$node->safe_psql('postgres', qq{
    CREATE TABLE recno_vm_vacuum (
        id int PRIMARY KEY,
        val int
    ) USING recno;

    CREATE TABLE heap_vm_vacuum (
        id int PRIMARY KEY,
        val int
    ) USING heap;

    INSERT INTO recno_vm_vacuum SELECT i, i FROM generate_series(1, 10000) i;
    INSERT INTO heap_vm_vacuum SELECT i, i FROM generate_series(1, 10000) i;

    VACUUM recno_vm_vacuum;
    VACUUM heap_vm_vacuum;
});

# Update only a few rows to dirty specific pages
$node->safe_psql('postgres', qq{
    UPDATE recno_vm_vacuum SET val = val + 1 WHERE id IN (100, 5000, 9000);
    UPDATE heap_vm_vacuum SET val = val + 1 WHERE id IN (100, 5000, 9000);
});

# VACUUM should skip most pages
$t0 = [gettimeofday];
my $recno_vm_output = $node->safe_psql('postgres', 'VACUUM VERBOSE recno_vm_vacuum');
my $recno_vm_vacuum_time = tv_interval($t0);

$t0 = [gettimeofday];
my $heap_vm_output = $node->safe_psql('postgres', 'VACUUM VERBOSE heap_vm_vacuum');
my $heap_vm_vacuum_time = tv_interval($t0);

printf "VACUUM with visibility map:\n";
printf "  RECNO: %.3f seconds\n", $recno_vm_vacuum_time;
printf "  Heap:  %.3f seconds\n\n", $heap_vm_vacuum_time;

if ($verbose) {
    print "RECNO VACUUM output:\n$recno_vm_output\n\n";
    print "Heap VACUUM output:\n$heap_vm_output\n\n";
}

# ============================================================
# Test 6: Autovacuum behavior
# ============================================================

print "Test 6: Autovacuum behavior\n";
print "-" x 30 . "\n";

# Create tables for autovacuum testing
$node->safe_psql('postgres', qq{
    CREATE TABLE recno_autovac (
        id int PRIMARY KEY,
        val int
    ) USING recno;

    CREATE TABLE heap_autovac (
        id int PRIMARY KEY,
        val int
    ) USING heap;

    -- Set aggressive autovacuum parameters
    ALTER TABLE recno_autovac SET (autovacuum_vacuum_threshold = 50);
    ALTER TABLE heap_autovac SET (autovacuum_vacuum_threshold = 50);
});

# Generate dead tuples to trigger autovacuum
print "Generating workload for autovacuum...\n";
for (my $i = 0; $i < 10; $i++) {
    $node->safe_psql('postgres', qq{
        INSERT INTO recno_autovac SELECT i, i FROM generate_series(1, 100) i
        ON CONFLICT (id) DO UPDATE SET val = recno_autovac.val + 1;

        INSERT INTO heap_autovac SELECT i, i FROM generate_series(1, 100) i
        ON CONFLICT (id) DO UPDATE SET val = heap_autovac.val + 1;
    });
    usleep(100000);  # 100ms between batches
}

# Wait for autovacuum to run
sleep(15);

# Check autovacuum statistics
my $recno_autovac_count = $node->safe_psql('postgres', qq{
    SELECT autovacuum_count
    FROM pg_stat_user_tables
    WHERE tablename = 'recno_autovac';
});

my $heap_autovac_count = $node->safe_psql('postgres', qq{
    SELECT autovacuum_count
    FROM pg_stat_user_tables
    WHERE tablename = 'heap_autovac';
});

printf "Autovacuum runs:\n";
printf "  RECNO: %d\n", $recno_autovac_count;
printf "  Heap:  %d\n\n", $heap_autovac_count;

# ============================================================
# Test 7: VACUUM FREEZE performance
# ============================================================

print "Test 7: VACUUM FREEZE performance\n";
print "-" x 30 . "\n";

# VACUUM FREEZE RECNO
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM FREEZE VERBOSE recno_vacuum_test');
my $recno_freeze_time = tv_interval($t0);

# VACUUM FREEZE Heap
$t0 = [gettimeofday];
$node->safe_psql('postgres', 'VACUUM FREEZE VERBOSE heap_vacuum_test');
my $heap_freeze_time = tv_interval($t0);

printf "VACUUM FREEZE time:\n";
printf "  RECNO: %.3f seconds\n", $recno_freeze_time;
printf "  Heap:  %.3f seconds\n", $heap_freeze_time;
printf "  Ratio: %.2fx\n\n", $heap_freeze_time / $recno_freeze_time;

# ============================================================
# Summary
# ============================================================

print "=" x 50 . "\n";
print "Summary: VACUUM Performance\n";
print "=" x 50 . "\n";

my $total_recno = $recno_vacuum_time + $recno_vacuum_update_time +
                  $recno_vacuum_full_time + $recno_freeze_time;
my $total_heap = $heap_vacuum_time + $heap_vacuum_update_time +
                 $heap_vacuum_full_time + $heap_freeze_time;

printf "Total RECNO VACUUM time: %.3f seconds\n", $total_recno;
printf "Total Heap VACUUM time:  %.3f seconds\n", $total_heap;
printf "Overall performance ratio: %.2fx\n", $total_heap / $total_recno;

if ($total_recno < $total_heap) {
    print "\nResult: RECNO VACUUM is FASTER than Heap\n";
} elsif ($total_recno > $total_heap * 1.1) {
    print "\nResult: RECNO VACUUM is SLOWER than Heap\n";
} else {
    print "\nResult: RECNO and Heap have SIMILAR VACUUM performance\n";
}

# Cleanup
$node->safe_psql('postgres', q{
    DROP TABLE IF EXISTS recno_vacuum_test, heap_vacuum_test;
    DROP TABLE IF EXISTS recno_nonidx_vacuum, heap_nonidx_vacuum;
    DROP TABLE IF EXISTS recno_vm_vacuum, heap_vm_vacuum;
    DROP TABLE IF EXISTS recno_autovac, heap_autovac;
});

$node->stop;
exit 0;