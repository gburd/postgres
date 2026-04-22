#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Benchmark RECNO compression effectiveness.
# Tests compression ratio, performance impact, and different data patterns.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(gettimeofday tv_interval);
use Getopt::Long;

my $scale = 1000;  # Number of rows per test
my $verbose = 0;

GetOptions(
    'scale=i' => \$scale,
    'verbose' => \$verbose
) or die "Usage: $0 [--scale=N] [--verbose]\n";

# Initialize cluster
my $node = PostgreSQL::Test::Cluster->new('recno_compression');
$node->init;

# Enable compression settings
$node->append_conf('postgresql.conf', 'recno.compression_level = 6');
$node->append_conf('postgresql.conf', 'recno.compression_threshold = 256');
$node->start;

print "Compression Effectiveness Test\n";
print "=" x 50 . "\n";
print "Scale: $scale rows per test\n\n";

# ============================================================
# Test 1: Highly compressible data (repeated patterns)
# ============================================================

print "Test 1: Highly Compressible Data\n";
print "-" x 30 . "\n";

$node->safe_psql('postgres', qq{
    -- RECNO table with compression
    CREATE TABLE recno_compress_high (
        id int PRIMARY KEY,
        data text
    ) USING recno;

    -- Heap table for comparison
    CREATE TABLE heap_compress_high (
        id int PRIMARY KEY,
        data text
    ) USING heap;
});

# Insert highly compressible data (repeated pattern)
my $repeated_data = 'AAAAAAAAAA' x 100;  # 1000 bytes of repeated 'A'

my $t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO recno_compress_high
    SELECT i, '$repeated_data' || (i % 10)
    FROM generate_series(1, $scale) i;
});
my $recno_insert_time = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO heap_compress_high
    SELECT i, '$repeated_data' || (i % 10)
    FROM generate_series(1, $scale) i;
});
my $heap_insert_time = tv_interval($t0);

# Measure storage size
my $recno_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('recno_compress_high')");
my $heap_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('heap_compress_high')");

printf "Highly compressible data:\n";
printf "  RECNO size: %d bytes (insert: %.3fs)\n", $recno_size, $recno_insert_time;
printf "  Heap size:  %d bytes (insert: %.3fs)\n", $heap_size, $heap_insert_time;
printf "  Compression ratio: %.2f:1\n", $heap_size / $recno_size;
printf "  Space saved: %.1f%%\n\n", (1 - $recno_size/$heap_size) * 100;

# ============================================================
# Test 2: Random data (poorly compressible)
# ============================================================

print "Test 2: Random Data (Poorly Compressible)\n";
print "-" x 30 . "\n";

$node->safe_psql('postgres', qq{
    CREATE TABLE recno_compress_random (
        id int PRIMARY KEY,
        data text
    ) USING recno;

    CREATE TABLE heap_compress_random (
        id int PRIMARY KEY,
        data text
    ) USING heap;
});

# Insert random data
$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO recno_compress_random
    SELECT i, md5(random()::text) || md5(random()::text) || md5(random()::text)
    FROM generate_series(1, $scale) i;
});
my $recno_random_insert = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO heap_compress_random
    SELECT i, md5(random()::text) || md5(random()::text) || md5(random()::text)
    FROM generate_series(1, $scale) i;
});
my $heap_random_insert = tv_interval($t0);

$recno_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('recno_compress_random')");
$heap_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('heap_compress_random')");

printf "Random data:\n";
printf "  RECNO size: %d bytes (insert: %.3fs)\n", $recno_size, $recno_random_insert;
printf "  Heap size:  %d bytes (insert: %.3fs)\n", $heap_size, $heap_random_insert;
printf "  Compression ratio: %.2f:1\n", $heap_size / $recno_size;
printf "  Space saved: %.1f%%\n\n", (1 - $recno_size/$heap_size) * 100;

# ============================================================
# Test 3: JSON data (moderate compressibility)
# ============================================================

print "Test 3: JSON Data\n";
print "-" x 30 . "\n";

$node->safe_psql('postgres', qq{
    CREATE TABLE recno_compress_json (
        id int PRIMARY KEY,
        data jsonb
    ) USING recno;

    CREATE TABLE heap_compress_json (
        id int PRIMARY KEY,
        data jsonb
    ) USING heap;
});

# Insert JSON data with repeated structure
$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO recno_compress_json
    SELECT i, jsonb_build_object(
        'id', i,
        'name', 'User_' || i,
        'email', 'user' || i || '\@example.com',
        'created', now(),
        'active', true,
        'settings', jsonb_build_object(
            'theme', 'default',
            'language', 'en',
            'notifications', true
        )
    )
    FROM generate_series(1, $scale) i;
});
my $recno_json_insert = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO heap_compress_json
    SELECT i, jsonb_build_object(
        'id', i,
        'name', 'User_' || i,
        'email', 'user' || i || '\@example.com',
        'created', now(),
        'active', true,
        'settings', jsonb_build_object(
            'theme', 'default',
            'language', 'en',
            'notifications', true
        )
    )
    FROM generate_series(1, $scale) i;
});
my $heap_json_insert = tv_interval($t0);

$recno_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('recno_compress_json')");
$heap_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('heap_compress_json')");

printf "JSON data:\n";
printf "  RECNO size: %d bytes (insert: %.3fs)\n", $recno_size, $recno_json_insert;
printf "  Heap size:  %d bytes (insert: %.3fs)\n", $heap_size, $heap_json_insert;
printf "  Compression ratio: %.2f:1\n", $heap_size / $recno_size;
printf "  Space saved: %.1f%%\n\n", (1 - $recno_size/$heap_size) * 100;

# ============================================================
# Test 4: Mixed data types
# ============================================================

print "Test 4: Mixed Data Types\n";
print "-" x 30 . "\n";

$node->safe_psql('postgres', qq{
    CREATE TABLE recno_compress_mixed (
        id int PRIMARY KEY,
        int_col int,
        bigint_col bigint,
        text_col text,
        timestamp_col timestamp,
        bool_col boolean,
        array_col int[]
    ) USING recno;

    CREATE TABLE heap_compress_mixed (
        id int PRIMARY KEY,
        int_col int,
        bigint_col bigint,
        text_col text,
        timestamp_col timestamp,
        bool_col boolean,
        array_col int[]
    ) USING heap;
});

$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO recno_compress_mixed
    SELECT
        i,
        i * 10,
        i::bigint * 1000000,
        repeat('text', i % 10),
        now() + (i || ' seconds')::interval,
        i % 2 = 0,
        ARRAY[i, i+1, i+2]
    FROM generate_series(1, $scale) i;
});
my $recno_mixed_insert = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres', qq{
    INSERT INTO heap_compress_mixed
    SELECT
        i,
        i * 10,
        i::bigint * 1000000,
        repeat('text', i % 10),
        now() + (i || ' seconds')::interval,
        i % 2 = 0,
        ARRAY[i, i+1, i+2]
    FROM generate_series(1, $scale) i;
});
my $heap_mixed_insert = tv_interval($t0);

$recno_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('recno_compress_mixed')");
$heap_size = $node->safe_psql('postgres',
    "SELECT pg_relation_size('heap_compress_mixed')");

printf "Mixed data types:\n";
printf "  RECNO size: %d bytes (insert: %.3fs)\n", $recno_size, $recno_mixed_insert;
printf "  Heap size:  %d bytes (insert: %.3fs)\n", $heap_size, $heap_mixed_insert;
printf "  Compression ratio: %.2f:1\n", $heap_size / $recno_size;
printf "  Space saved: %.1f%%\n\n", (1 - $recno_size/$heap_size) * 100;

# ============================================================
# Test 5: Query performance on compressed data
# ============================================================

print "Test 5: Query Performance on Compressed Data\n";
print "-" x 30 . "\n";

# Sequential scan
$t0 = [gettimeofday];
$node->safe_psql('postgres',
    'SELECT COUNT(*), AVG(length(data)) FROM recno_compress_high');
my $recno_scan_compressed = tv_interval($t0);

$t0 = [gettimeofday];
$node->safe_psql('postgres',
    'SELECT COUNT(*), AVG(length(data)) FROM heap_compress_high');
my $heap_scan_compressed = tv_interval($t0);

printf "Sequential scan (compressed data):\n";
printf "  RECNO: %.3f seconds\n", $recno_scan_compressed;
printf "  Heap:  %.3f seconds\n", $heap_scan_compressed;
printf "  Decompression overhead: %.1f%%\n\n",
    (($recno_scan_compressed - $heap_scan_compressed) / $heap_scan_compressed) * 100;

# Point queries
my $queries = 100;
$t0 = [gettimeofday];
for (my $i = 1; $i <= $queries; $i++) {
    my $id = int(rand($scale)) + 1;
    $node->safe_psql('postgres',
        "SELECT data FROM recno_compress_high WHERE id = $id");
}
my $recno_point_compressed = tv_interval($t0);

$t0 = [gettimeofday];
for (my $i = 1; $i <= $queries; $i++) {
    my $id = int(rand($scale)) + 1;
    $node->safe_psql('postgres',
        "SELECT data FROM heap_compress_high WHERE id = $id");
}
my $heap_point_compressed = tv_interval($t0);

printf "Point queries (%d queries):\n", $queries;
printf "  RECNO: %.3f seconds\n", $recno_point_compressed;
printf "  Heap:  %.3f seconds\n", $heap_point_compressed;
printf "  Decompression overhead: %.1f%%\n\n",
    (($recno_point_compressed - $heap_point_compressed) / $heap_point_compressed) * 100;

# ============================================================
# Test 6: Compression with different thresholds
# ============================================================

if ($verbose) {
    print "Test 6: Compression Threshold Analysis\n";
    print "-" x 30 . "\n";

    my @thresholds = (128, 256, 512, 1024);

    foreach my $threshold (@thresholds) {
        $node->safe_psql('postgres',
            "SET recno.compression_threshold = $threshold");

        $node->safe_psql('postgres', qq{
            CREATE TABLE recno_thresh_$threshold (
                id int PRIMARY KEY,
                small text,
                medium text,
                large text
            ) USING recno;
        });

        # Insert data of varying sizes
        $node->safe_psql('postgres', qq{
            INSERT INTO recno_thresh_$threshold
            SELECT
                i,
                repeat('S', 50),  -- Below all thresholds
                repeat('M', 300), -- Above some thresholds
                repeat('L', 2000) -- Above all thresholds
            FROM generate_series(1, 100) i;
        });

        my $size = $node->safe_psql('postgres',
            "SELECT pg_relation_size('recno_thresh_$threshold')");

        printf "Threshold %d bytes: table size = %d bytes\n",
            $threshold, $size;
    }
    print "\n";
}

# ============================================================
# Summary
# ============================================================

print "=" x 50 . "\n";
print "Summary: Compression Effectiveness\n";
print "=" x 50 . "\n";

# Calculate average compression ratio
my $total_recno_size = 0;
my $total_heap_size = 0;

foreach my $table ('high', 'random', 'json', 'mixed') {
    my $r = $node->safe_psql('postgres',
        "SELECT pg_relation_size('recno_compress_$table')");
    my $h = $node->safe_psql('postgres',
        "SELECT pg_relation_size('heap_compress_$table')");
    $total_recno_size += $r;
    $total_heap_size += $h;
}

printf "Total RECNO size: %d bytes\n", $total_recno_size;
printf "Total Heap size:  %d bytes\n", $total_heap_size;
printf "Overall compression ratio: %.2f:1\n", $total_heap_size / $total_recno_size;
printf "Overall space saved: %.1f%%\n", (1 - $total_recno_size/$total_heap_size) * 100;

if ($total_recno_size < $total_heap_size * 0.8) {
    print "\nResult: RECNO compression is HIGHLY EFFECTIVE (>20% savings)\n";
} elsif ($total_recno_size < $total_heap_size) {
    print "\nResult: RECNO compression is MODERATELY EFFECTIVE\n";
} else {
    print "\nResult: RECNO compression is NOT EFFECTIVE for this workload\n";
}

# Cleanup
$node->safe_psql('postgres', q{
    DROP TABLE IF EXISTS recno_compress_high, heap_compress_high;
    DROP TABLE IF EXISTS recno_compress_random, heap_compress_random;
    DROP TABLE IF EXISTS recno_compress_json, heap_compress_json;
    DROP TABLE IF EXISTS recno_compress_mixed, heap_compress_mixed;
});

if ($verbose) {
    foreach my $threshold (128, 256, 512, 1024) {
        $node->safe_psql('postgres',
            "DROP TABLE IF EXISTS recno_thresh_$threshold");
    }
}

$node->stop;
exit 0;