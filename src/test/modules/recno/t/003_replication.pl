# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Verify streaming replication with RECNO tables.
#
# Tests:
#   - Primary setup with RECNO tables
#   - Streaming replica creation and initial sync
#   - INSERT/UPDATE/DELETE replication
#   - Bulk DML replication
#   - Index DDL replication
#   - VACUUM replication consistency
#   - Standby crash and recovery
#   - Overflow data replication

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

# ============================================================
# Setup: Primary with RECNO table + streaming standby
# ============================================================

my $primary = PostgreSQL::Test::Cluster->new('recno_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', 'wal_level = replica');
$primary->append_conf('postgresql.conf', 'max_wal_senders = 5');
$primary->start;

# Create physical replication slot
is($primary->psql('postgres',
	qq[SELECT pg_create_physical_replication_slot('standby_slot');]),
	0, 'Physical replication slot created');

# Create RECNO table on primary and load initial data
$primary->safe_psql('postgres',
	"CREATE TABLE recno_repl (id int PRIMARY KEY, val text, ts timestamp) USING recno;
	 INSERT INTO recno_repl SELECT i, 'primary_' || i, now()
	 FROM generate_series(1, 100) i");

# Take base backup for standby
my $backup_name = 'recno_backup';
$primary->backup($backup_name);

# Create streaming standby
my $standby = PostgreSQL::Test::Cluster->new('recno_standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', 'primary_slot_name = standby_slot');
$standby->start;

# Wait for standby to finish initial sync
$primary->wait_for_replay_catchup($standby);

# ============================================================
# Test 1: Initial data replicated
# ============================================================

my $primary_count = $primary->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl');
my $standby_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl');
is($standby_count, $primary_count, "Initial data replicated to standby");
is($standby_count, '100', "Standby has all 100 rows");

# ============================================================
# Test 2: INSERT replication
# ============================================================

$primary->safe_psql('postgres',
	"INSERT INTO recno_repl VALUES (101, 'replicated_insert', now())");
$primary->wait_for_replay_catchup($standby);

my $replicated = $standby->safe_psql('postgres',
	'SELECT val FROM recno_repl WHERE id = 101');
is($replicated, 'replicated_insert', "INSERT replicated to standby");

# ============================================================
# Test 3: UPDATE replication
# ============================================================

$primary->safe_psql('postgres',
	"UPDATE recno_repl SET val = 'updated_value' WHERE id = 50");
$primary->wait_for_replay_catchup($standby);

my $updated = $standby->safe_psql('postgres',
	'SELECT val FROM recno_repl WHERE id = 50');
is($updated, 'updated_value', "UPDATE replicated to standby");

# ============================================================
# Test 4: DELETE replication
# ============================================================

$primary->safe_psql('postgres',
	'DELETE FROM recno_repl WHERE id > 95 AND id <= 100');
$primary->wait_for_replay_catchup($standby);

my $deleted_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl WHERE id > 95');
is($deleted_count, '1', "DELETE replicated (only id=101 remains above 95)");

# ============================================================
# Test 5: Bulk DML replication (transaction)
# ============================================================

$primary->safe_psql('postgres',
	"BEGIN;
	 INSERT INTO recno_repl SELECT i, 'bulk_' || i, now()
	 FROM generate_series(200, 300) i;
	 UPDATE recno_repl SET val = val || '_modified' WHERE id BETWEEN 10 AND 20;
	 DELETE FROM recno_repl WHERE id BETWEEN 30 AND 35;
	 COMMIT");
$primary->wait_for_replay_catchup($standby);

my $bulk_count = $standby->safe_psql('postgres',
	"SELECT COUNT(*) FROM recno_repl WHERE val LIKE 'bulk_%'");
is($bulk_count, '101', "Bulk INSERT replicated correctly");

my $modified_count = $standby->safe_psql('postgres',
	"SELECT COUNT(*) FROM recno_repl WHERE val LIKE '%_modified'");
is($modified_count, '11', "Bulk UPDATE replicated correctly");

my $deleted_range = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl WHERE id BETWEEN 30 AND 35');
is($deleted_range, '0', "Bulk DELETE replicated correctly");

# ============================================================
# Test 6: Index DDL replication
# ============================================================

$primary->safe_psql('postgres',
	'CREATE INDEX recno_repl_val_idx ON recno_repl(val)');
$primary->wait_for_replay_catchup($standby);

my $index_exists = $standby->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_indexes
	 WHERE tablename = 'recno_repl' AND indexname = 'recno_repl_val_idx'");
is($index_exists, '1', "Index creation replicated to standby");

# ============================================================
# Test 7: VACUUM replication consistency
# ============================================================

$primary->safe_psql('postgres', 'VACUUM recno_repl');
$primary->wait_for_replay_catchup($standby);

$primary_count = $primary->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl');
$standby_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl');
is($standby_count, $primary_count, "Data consistent after VACUUM replication");

# ============================================================
# Test 8: Standby crash and recovery
# ============================================================

# Crash the standby
$standby->stop('immediate');
$standby->start;
$primary->wait_for_replay_catchup($standby);

$standby_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl');
is($standby_count, $primary_count, "Standby data correct after crash recovery");

# Verify specific values after standby crash
my $post_crash = $standby->safe_psql('postgres',
	'SELECT val FROM recno_repl WHERE id = 101');
is($post_crash, 'replicated_insert',
	"Specific row correct on standby after crash");

# ============================================================
# Test 9: Large data replication
# ============================================================

$primary->safe_psql('postgres',
	"INSERT INTO recno_repl SELECT i, repeat('x', 1000), now()
	 FROM generate_series(1000, 2000) i");
$primary->wait_for_replay_catchup($standby);

my $large_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl WHERE id >= 1000');
is($large_count, '1001', "Large data replication works");

# ============================================================
# Test 10: Overflow data replication
# ============================================================

$primary->safe_psql('postgres',
	'CREATE TABLE recno_overflow_repl (
		id int PRIMARY KEY,
		small_col text,
		large_col text
	) USING recno');

# Insert overflow-sized data on primary
$primary->safe_psql('postgres',
	"INSERT INTO recno_overflow_repl VALUES (1, 'small', repeat('A', 10000))");
$primary->safe_psql('postgres',
	"INSERT INTO recno_overflow_repl VALUES (2, 'another', repeat('B', 50000))");

$primary->wait_for_replay_catchup($standby);

# Verify overflow data on standby
my $ov_standby_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_overflow_repl');
is($ov_standby_count, '2', "Overflow rows replicated to standby");

my $ov_len = $standby->safe_psql('postgres',
	'SELECT length(large_col) FROM recno_overflow_repl WHERE id = 2');
is($ov_len, '50000', "Overflow column length matches on standby");

my $ov_content = $standby->safe_psql('postgres',
	"SELECT large_col = repeat('B', 50000) FROM recno_overflow_repl WHERE id = 2");
is($ov_content, 't', "Overflow column content matches on standby");

# Update overflow data on primary
$primary->safe_psql('postgres',
	"UPDATE recno_overflow_repl SET large_col = repeat('C', 30000) WHERE id = 1");
$primary->wait_for_replay_catchup($standby);

my $ov_updated = $standby->safe_psql('postgres',
	"SELECT large_col = repeat('C', 30000) FROM recno_overflow_repl WHERE id = 1");
is($ov_updated, 't', "Updated overflow data replicated correctly");

# Delete overflow row on primary
$primary->safe_psql('postgres',
	'DELETE FROM recno_overflow_repl WHERE id = 2');
$primary->wait_for_replay_catchup($standby);

my $ov_deleted = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_overflow_repl WHERE id = 2');
is($ov_deleted, '0', "Overflow row deletion replicated correctly");

# ============================================================
# Test 11: Additional DML replication verification
# ============================================================
#
# Verify that RECNO WAL records are generated and replayed correctly
# for various DML operations and that data remains consistent.

# Enable WAL inspection on primary
$primary->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS pg_walinspect');

my $start_lsn = $primary->lsn('insert');

# Perform DML operations
$primary->safe_psql('postgres',
	"INSERT INTO recno_repl VALUES (3001, 'test_1', now())");
$primary->safe_psql('postgres',
	"UPDATE recno_repl SET val = 'updated' WHERE id = 3001");
$primary->safe_psql('postgres',
	"INSERT INTO recno_repl VALUES (3002, 'test_2', now())");

my $end_lsn = $primary->lsn('flush');

# Verify RECNO WAL records were generated
my $recno_wal = $primary->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_get_wal_records_info('$start_lsn', '$end_lsn')
	 WHERE resource_manager = 'RECNO'");
cmp_ok($recno_wal, '>', '0', "RECNO WAL records generated for DML");

# Wait for standby to replay all WAL
$primary->wait_for_replay_catchup($standby);

# Verify data consistency on standby
my $val1 = $standby->safe_psql('postgres',
	'SELECT val FROM recno_repl WHERE id = 3001');
is($val1, 'updated', "UPDATE replayed correctly on standby");

my $val2 = $standby->safe_psql('postgres',
	'SELECT val FROM recno_repl WHERE id = 3002');
is($val2, 'test_2', "INSERT replayed correctly on standby");

# Verify data is fully consistent between primary and standby
my $primary_data = $primary->safe_psql('postgres',
	'SELECT id, val FROM recno_repl WHERE id >= 3001 ORDER BY id');
my $standby_data = $standby->safe_psql('postgres',
	'SELECT id, val FROM recno_repl WHERE id >= 3001 ORDER BY id');
is($standby_data, $primary_data,
	"Primary and standby data identical after WAL replay");

# Test rapid successive operations in a transaction
$primary->safe_psql('postgres',
	"BEGIN;
	 INSERT INTO recno_repl VALUES (3010, 'rapid_1', now());
	 INSERT INTO recno_repl VALUES (3011, 'rapid_2', now());
	 INSERT INTO recno_repl VALUES (3012, 'rapid_3', now());
	 UPDATE recno_repl SET val = 'rapid_1_upd' WHERE id = 3010;
	 DELETE FROM recno_repl WHERE id = 3012;
	 COMMIT");
$primary->wait_for_replay_catchup($standby);

my $rapid_count = $standby->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_repl WHERE id BETWEEN 3010 AND 3012');
is($rapid_count, '2', "Rapid operations replayed correctly (2 surviving rows)");

my $rapid_val = $standby->safe_psql('postgres',
	'SELECT val FROM recno_repl WHERE id = 3010');
is($rapid_val, 'rapid_1_upd',
	"Rapid UPDATE within transaction replayed correctly");

# ============================================================
# Test 12: Full data consistency check
# ============================================================

# Final consistency: compare full table counts and checksums
my $final_primary = $primary->safe_psql('postgres',
	'SELECT COUNT(*), SUM(id) FROM recno_repl');
my $final_standby = $standby->safe_psql('postgres',
	'SELECT COUNT(*), SUM(id) FROM recno_repl');
is($final_standby, $final_primary,
	"Final data fully consistent between primary and standby");

# Check no WAL consistency errors in primary log (if wal_consistency_checking
# was enabled, it would surface here)
my $primary_log = $primary->logfile;
my $wal_errors = 0;
if (open(my $fh, '<', $primary_log))
{
	while (<$fh>)
	{
		$wal_errors++ if /inconsistent page found/;
	}
	close($fh);
}
is($wal_errors, 0, "No WAL consistency errors in primary log");

$primary->stop;
$standby->stop;

done_testing();
