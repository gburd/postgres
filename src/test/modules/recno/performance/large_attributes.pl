#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Benchmark RECNO performance with large attributes (overflow pages).
# Compares RECNO vs heap for storing and accessing large text/bytea values.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(gettimeofday tv_interval);
use Getopt::Long;

my $scale = 100;  # Number of rows to test
my $size = 8192;  # Size of large attributes in bytes
my $verbose = 0;

GetOptions(
    'scale=i' => \$scale,
    'size=i' => \$size,
    'verbose' => \$verbose
) or die "Usage: $0 [--scale=N] [--size=N] [--verbose]\n";

# Initialize cluster
my $node = PostgreSQL::Test::Cluster->new('recno_large_attrs');
$node->init;
$node->append_conf('postgresql.conf', 'shared_buffers = 256MB');
$node->append_conf('postgresql.conf', 'work_mem = 64MB');
$node->start;

print "Large Attribute Performance Test\n";
print "=" x 50 . "\n";
print "Scale: $scale rows\n";
print "Attribute size: $size bytes\n\n";

# Create test tables
$node->safe_psql('postgres', qq{
    -- RECNO table with large attributes
    CREATE TABLE recno_large (
        id int PRIMARY KEY,
        small_data text,
        large_data text,
        binary_data bytea
    ) USING recno;

    -- Heap table for comparison
    CREATE TABLE heap_large (
        id int PRIMARY KEY,
        small_data text,
        large_data text,
        binary_data bytea
    ) USING heap;
});

# Generate large test data
my $large_text = 'X' x $size;
my $binary_data = '\x' . ('FF' x ($size / 2));

# ============================================================
# Test 1: Bulk INSERT of large attributes
# ============================================================

print "Test 1: Bulk INSERT Performance\n";
print "-" x 30 . "\n";

# RECNO insert
my $t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO recno_large
    SELECT i,
           'small_' || i,
           '$large_text' || i,
           '$binary_data'::bytea
    FROM generate_series(1, $scale) i;
});
my $recno_insert_time = tv_interval($t0);

# Heap insert
$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO heap_large
    SELECT i,
           'small_' || i,
           '$large_text' || i,
           '$binary_data'::bytea
    FROM generate_series(1, $scale) i;
});
my $heap_insert_time = tv_interval($t0);

printf "RECNO INSERT: %.3f seconds\n", $recno_insert_time;
printf "Heap INSERT:  %.3f seconds\n", $heap_insert_time;
printf "Ratio: %.2fx\n\n", $heap_insert_time / $recno_insert_time;

# ============================================================
# Test 2: Sequential scan of large attributes
# ============================================================

print "Test 2: Sequential Scan Performance\n";
print "-" x 30 . "\n";

# RECNO scan
$node->safe_psql('postgres', 'VACUUM ANALYZE recno_large');
$t0 = [gettimeofday];
my $recno_count = $node->safe_psql('postgres',
    'SELECT COUNT(*), SUM(length(large_data)) FROM recno_large');
my $recno_scan_time = tv_interval($t0);

# Heap scan
$node->safe_psql('postgres', 'VACUUM ANALYZE heap_large');
$t0 = [gettimeofday];
my $heap_count = $node->safe_psql('postgres',
    'SELECT COUNT(*), SUM(length(large_data)) FROM heap_large');
my $heap_scan_time = tv_interval($t0);

printf "RECNO scan: %.3f seconds\n", $recno_scan_time;
printf "Heap scan:  %.3f seconds\n", $heap_scan_time;
printf "Ratio: %.2fx\n\n", $heap_scan_time / $recno_scan_time;

# ============================================================
# Test 3: Point queries for large attributes
# ============================================================

print "Test 3: Point Query Performance\n";
print "-" x 30 . "\n";

my $queries = 100;
my @test_ids = map { int(rand($scale)) + 1 } (1..$queries);

# RECNO point queries
$t0 = [gettimeofday];
foreach my $id (@test_ids) {
    $node->safe_psql('postgres',
        "SELECT length(large_data) FROM recno_large WHERE id = $id");
}
my $recno_point_time = tv_interval($t0);

# Heap point queries
$t0 = [gettimeofday];
foreach my $id (@test_ids) {
    $node->safe_psql('postgres',
        "SELECT length(large_data) FROM heap_large WHERE id = $id");
}
my $heap_point_time = tv_interval($t0);

printf "RECNO point queries: %.3f seconds (%d queries)\n",
    $recno_point_time, $queries;
printf "Heap point queries:  %.3f seconds (%d queries)\n",
    $heap_point_time, $queries;
printf "Ratio: %.2fx\n\n", $heap_point_time / $recno_point_time;

# ============================================================
# Test 4: UPDATE of large attributes
# ============================================================

print "Test 4: UPDATE Performance\n";
print "-" x 30 . "\n";

my $new_large_text = 'Y' x $size;

# RECNO update
$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    UPDATE recno_large
    SET large_data = '$new_large_text' || id
    WHERE id <= $scale / 2;
});
my $recno_update_time = tv_interval($t0);

# Heap update
$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    UPDATE heap_large
    SET large_data = '$new_large_text' || id
    WHERE id <= $scale / 2;
});
my $heap_update_time = tv_interval($t0);

printf "RECNO UPDATE: %.3f seconds\n", $recno_update_time;
printf "Heap UPDATE:  %.3f seconds\n", $heap_update_time;
printf "Ratio: %.2fx\n\n", $heap_update_time / $recno_update_time;

# ============================================================
# Test 5: Storage efficiency
# ============================================================

print "Test 5: Storage Efficiency\n";
print "-" x 30 . "\n";

my $recno_size = $node->safe_psql('postgres',
    "SELECT pg_total_relation_size('recno_large')");
my $heap_size = $node->safe_psql('postgres',
    "SELECT pg_total_relation_size('heap_large')");

printf "RECNO total size: %s bytes\n", $recno_size;
printf "Heap total size:  %s bytes\n", $heap_size;
printf "RECNO overhead: %.1f%%\n\n",
    (($recno_size - $heap_size) / $heap_size) * 100;

# ============================================================
# Test 6: TOAST vs Overflow performance
# ============================================================

print "Test 6: TOAST vs Overflow Page Performance\n";
print "-" x 30 . "\n";

# Create tables with very large attributes (force overflow/TOAST)
my $huge_size = 32768;  # 32KB
my $huge_text = 'Z' x $huge_size;

$node->safe_psql('postgres', qq{
    CREATE TABLE recno_huge (id int PRIMARY KEY, data text) USING recno;
    CREATE TABLE heap_huge (id int PRIMARY KEY, data text) USING heap;
});

# Insert huge attributes
$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO recno_huge
    SELECT i, '$huge_text' || i
    FROM generate_series(1, 10) i;
});
my $recno_huge_time = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO heap_huge
    SELECT i, '$huge_text' || i
    FROM generate_series(1, 10) i;
});
my $heap_huge_time = tv_interval($t0);

printf "RECNO huge insert: %.3f seconds\n", $recno_huge_time;
printf "Heap huge insert:  %.3f seconds\n", $heap_huge_time;
printf "Ratio: %.2fx\n\n", $heap_huge_time / $recno_huge_time;

# ============================================================
# Test 7: Overflow page chain performance
# ============================================================

if ($verbose) {
    print "Test 7: Overflow Chain Statistics\n";
    print "-" x 30 . "\n";

    # Get overflow statistics for RECNO
    my $overflow_stats = $node->safe_psql('postgres', qq{
        SELECT
            relname,
            relpages,
            reltuples,
            pg_size_pretty(pg_relation_size(oid)) as size
        FROM pg_class
        WHERE relname IN ('recno_large', 'recno_huge')
        ORDER BY relname;
    });

    print "Overflow statistics:\n$overflow_stats\n\n";
}

# ============================================================
# Summary
# ============================================================

print "=" x 50 . "\n";
print "Summary: Large Attribute Performance\n";
print "=" x 50 . "\n";

my $total_recno = $recno_insert_time + $recno_scan_time +
                  $recno_point_time + $recno_update_time;
my $total_heap = $heap_insert_time + $heap_scan_time +
                 $heap_point_time + $heap_update_time;

printf "Total RECNO time: %.3f seconds\n", $total_recno;
printf "Total Heap time:  %.3f seconds\n", $total_heap;
printf "Overall performance ratio: %.2fx\n", $total_heap / $total_recno;

if ($total_recno < $total_heap) {
    print "\nResult: RECNO is FASTER for large attributes\n";
} elsif ($total_recno > $total_heap * 1.1) {
    print "\nResult: RECNO is SLOWER for large attributes\n";
} else {
    print "\nResult: RECNO and Heap have SIMILAR performance\n";
}

# Cleanup
$node->safe_psql('postgres', 'DROP TABLE recno_large, heap_large, recno_huge, heap_huge');
$node->stop;

exit 0;