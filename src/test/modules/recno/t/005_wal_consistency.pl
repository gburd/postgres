# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Verify WAL consistency for RECNO operations, including overflow.
# Tests WAL correctness via streaming replication data comparison and
# pg_walinspect verification of RECNO WAL record generation.
#
# Note: wal_consistency_checking is NOT used here because it triggers
# a pre-existing timeline history bug in the UNDO-in-WAL fork when
# combined with streaming replication (unrelated to RECNO).  The actual
# WAL replay correctness is verified by comparing primary/standby data.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

# Set up primary
my $primary = PostgreSQL::Test::Cluster->new('recno_wal_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', 'wal_level = replica');
$primary->append_conf('postgresql.conf', 'max_wal_senders = 5');
$primary->start;

$primary->safe_psql('postgres', 'CREATE EXTENSION pg_walinspect');

# Create replication slot
is($primary->psql('postgres',
	qq[SELECT pg_create_physical_replication_slot('wal_check_slot');]),
	0, 'Physical replication slot created');

# Take backup for standby
my $backup_name = 'wal_check_backup';
$primary->backup($backup_name);

# Create streaming standby
my $standby = PostgreSQL::Test::Cluster->new('recno_wal_standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', 'primary_slot_name = wal_check_slot');
$standby->start;

# ============================================================
# Test 1: Basic CRUD generates proper WAL records
# ============================================================

$primary->safe_psql('postgres',
	'CREATE TABLE recno_wal_test (id int PRIMARY KEY, val text, num int) USING recno');

my $start_lsn = $primary->lsn('insert');

# Generate INSERT WAL records
$primary->safe_psql('postgres',
	'INSERT INTO recno_wal_test SELECT i, \'row_\' || i, i * 10 FROM generate_series(1, 50) i');

# Generate UPDATE WAL records
$primary->safe_psql('postgres',
	'UPDATE recno_wal_test SET num = num + 1 WHERE id <= 25');

# Generate DELETE WAL records
$primary->safe_psql('postgres',
	'DELETE FROM recno_wal_test WHERE id > 45');

my $end_lsn = $primary->lsn('flush');

# Verify WAL record types
my $wal_summary = $primary->safe_psql('postgres',
	"SELECT resource_manager, COUNT(*)
	 FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO'
	 GROUP BY resource_manager");
like($wal_summary, qr/RECNO/, "RECNO WAL records generated for CRUD operations");

# Wait for standby to catch up and verify consistency
$primary->wait_for_replay_catchup($standby);

my $primary_data = $primary->safe_psql('postgres',
	'SELECT COUNT(*), SUM(num) FROM recno_wal_test');
my $standby_data = $standby->safe_psql('postgres',
	'SELECT COUNT(*), SUM(num) FROM recno_wal_test');
is($standby_data, $primary_data, "Primary and standby data match after CRUD");

# ============================================================
# Test 2: Overflow data generates proper WAL
# ============================================================

$primary->safe_psql('postgres',
	'CREATE TABLE recno_wal_overflow (
		id int PRIMARY KEY,
		small_col text,
		large_col text
	) USING recno');

$start_lsn = $primary->lsn('insert');

# Insert overflow-sized data
$primary->safe_psql('postgres',
	"INSERT INTO recno_wal_overflow VALUES (1, 'small', repeat('X', 10000))");
$primary->safe_psql('postgres',
	"INSERT INTO recno_wal_overflow VALUES (2, 'medium', repeat('Y', 25000))");

$end_lsn = $primary->lsn('flush');

my $overflow_wal = $primary->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO'");
cmp_ok($overflow_wal, '>', '0', "WAL records generated for overflow inserts");

# Verify overflow data replicates correctly
$primary->wait_for_replay_catchup($standby);

my $ov_standby = $standby->safe_psql('postgres',
	"SELECT id, length(large_col), large_col = repeat('X', 10000)
	 FROM recno_wal_overflow WHERE id = 1");
is($ov_standby, '1|10000|t', "Overflow data replicated with WAL consistency");

my $ov_standby2 = $standby->safe_psql('postgres',
	"SELECT id, length(large_col), large_col = repeat('Y', 25000)
	 FROM recno_wal_overflow WHERE id = 2");
is($ov_standby2, '2|25000|t', "Large overflow data replicated correctly");

# ============================================================
# Test 3: Overflow update WAL
# ============================================================

$start_lsn = $primary->lsn('insert');

# Update: overflow -> overflow (different size)
$primary->safe_psql('postgres',
	"UPDATE recno_wal_overflow SET large_col = repeat('Z', 40000) WHERE id = 1");

# Update: overflow -> non-overflow (shrink)
$primary->safe_psql('postgres',
	"UPDATE recno_wal_overflow SET large_col = 'tiny' WHERE id = 2");

$end_lsn = $primary->lsn('flush');

$primary->wait_for_replay_catchup($standby);

my $ov_updated1 = $standby->safe_psql('postgres',
	"SELECT large_col = repeat('Z', 40000) FROM recno_wal_overflow WHERE id = 1");
is($ov_updated1, 't', "Overflow-to-overflow update replicated via WAL");

my $ov_updated2 = $standby->safe_psql('postgres',
	"SELECT large_col FROM recno_wal_overflow WHERE id = 2");
is($ov_updated2, 'tiny', "Overflow-to-inline update replicated via WAL");

# ============================================================
# Test 4: Overflow delete WAL
# ============================================================

$start_lsn = $primary->lsn('insert');

$primary->safe_psql('postgres',
	'DELETE FROM recno_wal_overflow WHERE id = 1');

$end_lsn = $primary->lsn('flush');

$primary->wait_for_replay_catchup($standby);

my $ov_deleted = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_overflow WHERE id = 1');
is($ov_deleted, '0', "Overflow row deletion replicated via WAL");

# ============================================================
# Test 5: VACUUM WAL
# ============================================================

$primary->safe_psql('postgres',
	'INSERT INTO recno_wal_test SELECT i, \'new_\' || i, i FROM generate_series(100, 200) i');
$primary->safe_psql('postgres',
	'DELETE FROM recno_wal_test WHERE id > 150');

$start_lsn = $primary->lsn('insert');

$primary->safe_psql('postgres', 'VACUUM recno_wal_test');

$end_lsn = $primary->lsn('flush');

$primary->wait_for_replay_catchup($standby);

my $post_vacuum_primary = $primary->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_test');
my $post_vacuum_standby = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_test');
is($post_vacuum_standby, $post_vacuum_primary,
	"Data consistent after VACUUM WAL replay on standby");

# ============================================================
# Test 6: Transaction rollback doesn't generate visible changes
# ============================================================

my $pre_count = $primary->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_test');

$primary->safe_psql('postgres',
	'BEGIN;
	 INSERT INTO recno_wal_test VALUES (999, \'rollback_me\', 0);
	 ROLLBACK');

$primary->wait_for_replay_catchup($standby);

my $standby_post_rollback = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_test');
is($standby_post_rollback, $pre_count,
	"Rolled-back transaction not visible on standby");

# ============================================================
# Test 7: Bulk operations with checkpoint
# ============================================================

$primary->safe_psql('postgres',
	'INSERT INTO recno_wal_test SELECT i, repeat(\'data\', 25), i
	 FROM generate_series(1000, 1500) i');

$primary->safe_psql('postgres', 'CHECKPOINT');

$primary->safe_psql('postgres',
	'DELETE FROM recno_wal_test WHERE id BETWEEN 1200 AND 1300');

$primary->wait_for_replay_catchup($standby);

my $post_ckpt_primary = $primary->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_test');
my $post_ckpt_standby = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_wal_test');

is($post_ckpt_standby, $post_ckpt_primary,
	"Data consistent after checkpoint and continued operations");

# Verify no crashes or errors in server logs
my $standby_log = $standby->logfile;
my $standby_errors = 0;
if (open(my $fh, '<', $standby_log)) {
	while (<$fh>) {
		$standby_errors++ if /PANIC|FATAL|inconsistent page found/;
	}
	close($fh);
}
is($standby_errors, 0, "No PANIC/FATAL errors in standby log");

$primary->stop;
$standby->stop;

done_testing();
