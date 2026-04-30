# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Test WAL compression for per-relation UNDO operations.
#
# This test verifies that the wal_compression GUC works correctly for
# per-relation UNDO WAL records. Full Page Images (FPIs) logged by
# XLOG_RELUNDO_INIT and XLOG_RELUNDO_INSERT are compressed automatically
# by XLogCompressBackupBlock() when wal_compression is enabled.
#
# The test measures WAL growth with compression off vs. lz4, and confirms
# that compression reduces WAL size for per-relation UNDO workloads.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ------------------------------------------------------------------
# Helper: get current WAL LSN as a numeric value for comparison
# ------------------------------------------------------------------
sub get_wal_lsn
{
	my ($node) = @_;
	return $node->safe_psql("postgres",
		"SELECT pg_current_wal_lsn()");
}

# Convert an LSN string (e.g., "0/1A3B4C0") to a numeric byte offset
sub lsn_to_bytes
{
	my ($lsn) = @_;
	my ($hi, $lo) = split('/', $lsn);
	return hex($hi) * (2**32) + hex($lo);
}

# ------------------------------------------------------------------
# Test: WAL compression off vs lz4 for per-relation UNDO
# ------------------------------------------------------------------

# Start with wal_compression = off
my $node = PostgreSQL::Test::Cluster->new('relundo_walcomp');
$node->init;
$node->append_conf(
	"postgresql.conf", qq(
autovacuum = off
log_min_messages = warning
shared_preload_libraries = ''
wal_compression = off
full_page_writes = on
));
$node->start;

# Install extension
$node->safe_psql("postgres", "CREATE EXTENSION test_undo_tam");

# ================================================================
# Phase 1: Measure WAL growth with wal_compression = off
# ================================================================

# Force a checkpoint so subsequent writes produce FPIs
$node->safe_psql("postgres", "CHECKPOINT");

my $lsn_before_nocomp = get_wal_lsn($node);

# Create table and insert rows -- each INSERT generates WAL with UNDO records
# The CHECKPOINT above ensures the first modification to each page will
# produce a full page image (FPI).
$node->safe_psql("postgres", qq(
CREATE TABLE relundo_nocomp (id int, data text) USING test_undo_tam;
INSERT INTO relundo_nocomp
  SELECT g, repeat('x', 200) FROM generate_series(1, 500) g;
));

my $lsn_after_nocomp = get_wal_lsn($node);

my $wal_bytes_nocomp =
	lsn_to_bytes($lsn_after_nocomp) - lsn_to_bytes($lsn_before_nocomp);

ok($wal_bytes_nocomp > 0,
	"WAL generated with wal_compression=off: $wal_bytes_nocomp bytes");

# Verify data integrity
my $count_nocomp = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_nocomp");
is($count_nocomp, '500', 'all 500 rows present with compression off');

# Verify UNDO chain integrity
my $undo_count_nocomp = $node->safe_psql("postgres",
	"SELECT count(*) FROM test_undo_tam_dump_chain('relundo_nocomp')");
is($undo_count_nocomp, '500',
	'500 UNDO records present with compression off');

# ================================================================
# Phase 2: Measure WAL growth with wal_compression = lz4
# ================================================================

# Enable lz4 compression
$node->safe_psql("postgres", "ALTER SYSTEM SET wal_compression = 'lz4'");
$node->reload;

# Force checkpoint to reset FPI tracking
$node->safe_psql("postgres", "CHECKPOINT");

my $lsn_before_lz4 = get_wal_lsn($node);

# Create a new table with the same workload
$node->safe_psql("postgres", qq(
CREATE TABLE relundo_lz4 (id int, data text) USING test_undo_tam;
INSERT INTO relundo_lz4
  SELECT g, repeat('x', 200) FROM generate_series(1, 500) g;
));

my $lsn_after_lz4 = get_wal_lsn($node);

my $wal_bytes_lz4 =
	lsn_to_bytes($lsn_after_lz4) - lsn_to_bytes($lsn_before_lz4);

ok($wal_bytes_lz4 > 0,
	"WAL generated with wal_compression=lz4: $wal_bytes_lz4 bytes");

# Verify data integrity
my $count_lz4 = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_lz4");
is($count_lz4, '500', 'all 500 rows present with lz4 compression');

# Verify UNDO chain integrity
my $undo_count_lz4 = $node->safe_psql("postgres",
	"SELECT count(*) FROM test_undo_tam_dump_chain('relundo_lz4')");
is($undo_count_lz4, '500',
	'500 UNDO records present with lz4 compression');

# ================================================================
# Phase 3: Compare WAL sizes
# ================================================================

# LZ4 should produce less WAL than uncompressed
ok($wal_bytes_lz4 < $wal_bytes_nocomp,
	"lz4 compression reduces WAL size " .
	"(off=$wal_bytes_nocomp, lz4=$wal_bytes_lz4)");

# Calculate compression ratio
my $ratio = 0;
if ($wal_bytes_nocomp > 0)
{
	$ratio = 100.0 * (1.0 - $wal_bytes_lz4 / $wal_bytes_nocomp);
}

# Log the compression ratio for documentation purposes
diag("WAL compression results for per-relation UNDO:");
diag("  wal_compression=off:  $wal_bytes_nocomp bytes");
diag("  wal_compression=lz4:  $wal_bytes_lz4 bytes");
diag(sprintf("  WAL size reduction:   %.1f%%", $ratio));

# We expect at least some compression (conservatively, >5%)
# FPI compression on UNDO pages with repetitive data should achieve much more
ok($ratio > 5.0,
	sprintf("WAL size reduction is meaningful: %.1f%%", $ratio));

# ================================================================
# Phase 4: Crash recovery with compressed WAL
# ================================================================

# Insert more data with compression enabled, then crash
$node->safe_psql("postgres", qq(
CREATE TABLE relundo_crash_lz4 (id int, data text) USING test_undo_tam;
INSERT INTO relundo_crash_lz4
  SELECT g, repeat('y', 100) FROM generate_series(1, 100) g;
CHECKPOINT;
));

$node->stop('immediate');
$node->start;

# Table should be accessible after crash recovery with compressed WAL
my $crash_count = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_crash_lz4");
ok(defined $crash_count,
	'per-relation UNDO table accessible after crash with lz4 WAL');

# New inserts should still work
$node->safe_psql("postgres",
	"INSERT INTO relundo_crash_lz4 VALUES (999, 'post_crash')");
my $post_crash = $node->safe_psql("postgres",
	"SELECT count(*) FROM relundo_crash_lz4 WHERE id = 999");
is($post_crash, '1', 'INSERT works after crash recovery with lz4 WAL');

# ================================================================
# Phase 5: Verify ZSTD compression (if available)
# ================================================================

# Try to set zstd -- this may fail if not compiled in, which is OK
my ($ret, $stdout, $stderr) = $node->psql("postgres",
	"ALTER SYSTEM SET wal_compression = 'zstd'");

if ($ret == 0)
{
	$node->reload;
	$node->safe_psql("postgres", "CHECKPOINT");

	my $lsn_before_zstd = get_wal_lsn($node);

	$node->safe_psql("postgres", qq(
	CREATE TABLE relundo_zstd (id int, data text) USING test_undo_tam;
	INSERT INTO relundo_zstd
	  SELECT g, repeat('x', 200) FROM generate_series(1, 500) g;
	));

	my $lsn_after_zstd = get_wal_lsn($node);
	my $wal_bytes_zstd =
		lsn_to_bytes($lsn_after_zstd) - lsn_to_bytes($lsn_before_zstd);

	ok($wal_bytes_zstd < $wal_bytes_nocomp,
		"zstd compression also reduces WAL " .
		"(off=$wal_bytes_nocomp, zstd=$wal_bytes_zstd)");

	my $zstd_ratio = 0;
	if ($wal_bytes_nocomp > 0)
	{
		$zstd_ratio = 100.0 * (1.0 - $wal_bytes_zstd / $wal_bytes_nocomp);
	}
	diag(sprintf("  wal_compression=zstd: $wal_bytes_zstd bytes (%.1f%% reduction)",
		$zstd_ratio));
}
else
{
	diag("zstd not available, skipping zstd compression test");
	pass('zstd test skipped (not available)');
}

# ================================================================
# Phase 6: Verify PGLZ compression
# ================================================================

$node->safe_psql("postgres",
	"ALTER SYSTEM SET wal_compression = 'pglz'");
$node->reload;
$node->safe_psql("postgres", "CHECKPOINT");

my $lsn_before_pglz = get_wal_lsn($node);

$node->safe_psql("postgres", qq(
CREATE TABLE relundo_pglz (id int, data text) USING test_undo_tam;
INSERT INTO relundo_pglz
  SELECT g, repeat('x', 200) FROM generate_series(1, 500) g;
));

my $lsn_after_pglz = get_wal_lsn($node);
my $wal_bytes_pglz =
	lsn_to_bytes($lsn_after_pglz) - lsn_to_bytes($lsn_before_pglz);

ok($wal_bytes_pglz < $wal_bytes_nocomp,
	"pglz compression also reduces WAL " .
	"(off=$wal_bytes_nocomp, pglz=$wal_bytes_pglz)");

my $pglz_ratio = 0;
if ($wal_bytes_nocomp > 0)
{
	$pglz_ratio = 100.0 * (1.0 - $wal_bytes_pglz / $wal_bytes_nocomp);
}
diag(sprintf("  wal_compression=pglz: $wal_bytes_pglz bytes (%.1f%% reduction)",
	$pglz_ratio));

# Print summary
diag("");
diag("=== WAL Compression Summary for Per-Relation UNDO ===");
diag("Workload: 500 rows x 200 bytes each, test_undo_tam");
diag(sprintf("  off:  %d bytes (baseline)", $wal_bytes_nocomp));
diag(sprintf("  pglz: %d bytes (%.1f%% reduction)", $wal_bytes_pglz, $pglz_ratio));
diag(sprintf("  lz4:  %d bytes (%.1f%% reduction)", $wal_bytes_lz4, $ratio));

# Cleanup
$node->stop;

done_testing();
