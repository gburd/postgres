
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test PREPARE TRANSACTION + crash recovery for UNDO-enabled tables.
#
# xl_xact_prepare stores the UNDO chain head LSN(s) for each persistence
# level in its last_batch_lsn[] array.  This test verifies that after a
# server crash with a prepared transaction pending on an UNDO-enabled
# table, crash recovery correctly restores the prepared-transaction state
# and that both COMMIT PREPARED and ROLLBACK PREPARED work correctly
# using the preserved UNDO chain.
#
# Scenarios:
#   1. PREPARE INSERT  + crash + ROLLBACK PREPARED  -> inserted rows absent
#   2. PREPARE DELETE  + crash + COMMIT PREPARED    -> deleted rows absent
#   3. PREPARE INSERT  + crash + COMMIT PREPARED    -> inserted rows present
#   4. Multiple prepared transactions survive crash -> mixed commit/rollback

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('two_phase_undo');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
enable_undo = on
max_prepared_transactions = 10
wal_level = replica
autovacuum = off
});
$node->start;

# -------------------------------------------------------------------
# Scenario 1: PREPARE INSERT + crash + ROLLBACK PREPARED
#
# Verify that after crash recovery, ROLLBACK PREPARED correctly
# removes rows that were inserted in the prepared transaction.
# -------------------------------------------------------------------

$node->safe_psql('postgres', q{
CREATE TABLE tbl_insert (id int PRIMARY KEY, val text) WITH (enable_undo = on);
INSERT INTO tbl_insert SELECT g, 'base_' || g FROM generate_series(1, 100) g;
});

$node->safe_psql('postgres', q{
BEGIN;
INSERT INTO tbl_insert SELECT g, 'prep_' || g FROM generate_series(101, 200) g;
PREPARE TRANSACTION 'undo_2pc_insert_rollback';
});

# Prepared-transaction rows are not yet committed; MVCC hides them.
my $s1_before = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_insert});
is($s1_before, '100',
	'scenario 1: only 100 base rows visible before crash (prepared INSERT hidden)');

# Crash with prepared transaction pending.
$node->stop('immediate');
$node->start;

# The prepared transaction must survive crash recovery.
my $s1_prep = $node->safe_psql('postgres',
	q{SELECT gid FROM pg_prepared_xacts WHERE gid = 'undo_2pc_insert_rollback'});
is($s1_prep, 'undo_2pc_insert_rollback',
	'scenario 1: prepared transaction survives crash recovery');

$node->safe_psql('postgres',
	q{ROLLBACK PREPARED 'undo_2pc_insert_rollback'});

my $s1_after = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_insert});
is($s1_after, '100',
	'scenario 1: only 100 base rows remain after ROLLBACK PREPARED');

my $s1_max = $node->safe_psql('postgres',
	q{SELECT max(id) FROM tbl_insert});
is($s1_max, '100',
	'scenario 1: max id is 100; prepared INSERT rows removed by ROLLBACK PREPARED');

# -------------------------------------------------------------------
# Scenario 2: PREPARE DELETE + crash + COMMIT PREPARED
#
# Verify that after crash recovery, COMMIT PREPARED makes the DELETE
# permanent so the deleted rows are absent.
# -------------------------------------------------------------------

$node->safe_psql('postgres', q{
CREATE TABLE tbl_delete (id int PRIMARY KEY, val text) WITH (enable_undo = on);
INSERT INTO tbl_delete SELECT g, 'row_' || g FROM generate_series(1, 100) g;
});

$node->safe_psql('postgres', q{
BEGIN;
DELETE FROM tbl_delete WHERE id <= 50;
PREPARE TRANSACTION 'undo_2pc_delete_commit';
});

# Prepared DELETE not committed; all 100 rows still visible (MVCC).
my $s2_before = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_delete});
is($s2_before, '100',
	'scenario 2: 100 rows visible before crash (prepared DELETE not committed)');

$node->stop('immediate');
$node->start;

my $s2_prep = $node->safe_psql('postgres',
	q{SELECT gid FROM pg_prepared_xacts WHERE gid = 'undo_2pc_delete_commit'});
is($s2_prep, 'undo_2pc_delete_commit',
	'scenario 2: prepared DELETE transaction survives crash');

$node->safe_psql('postgres',
	q{COMMIT PREPARED 'undo_2pc_delete_commit'});

my $s2_after = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_delete});
is($s2_after, '50',
	'scenario 2: 50 rows remain after COMMIT PREPARED DELETE');

my $s2_min = $node->safe_psql('postgres',
	q{SELECT min(id) FROM tbl_delete});
is($s2_min, '51',
	'scenario 2: min id is 51; rows 1-50 deleted by COMMIT PREPARED');

# -------------------------------------------------------------------
# Scenario 3: PREPARE INSERT + crash + COMMIT PREPARED
#
# Verify that after crash recovery, COMMIT PREPARED makes the prepared
# INSERT visible.
# -------------------------------------------------------------------

$node->safe_psql('postgres', q{
CREATE TABLE tbl_insert_commit (id int PRIMARY KEY, val text)
    WITH (enable_undo = on);
});

$node->safe_psql('postgres', q{
BEGIN;
INSERT INTO tbl_insert_commit
    SELECT g, 'val_' || g FROM generate_series(1, 200) g;
PREPARE TRANSACTION 'undo_2pc_insert_commit';
});

my $s3_before = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_insert_commit});
is($s3_before, '0',
	'scenario 3: table empty before crash (prepared INSERT not committed)');

$node->stop('immediate');
$node->start;

my $s3_prep = $node->safe_psql('postgres',
	q{SELECT gid FROM pg_prepared_xacts WHERE gid = 'undo_2pc_insert_commit'});
is($s3_prep, 'undo_2pc_insert_commit',
	'scenario 3: prepared INSERT transaction survives crash');

$node->safe_psql('postgres',
	q{COMMIT PREPARED 'undo_2pc_insert_commit'});

my $s3_after = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_insert_commit});
is($s3_after, '200',
	'scenario 3: 200 rows visible after COMMIT PREPARED INSERT');

my $s3_sum = $node->safe_psql('postgres',
	q{SELECT sum(id) FROM tbl_insert_commit});
is($s3_sum, '20100',
	'scenario 3: sum of ids correct after COMMIT PREPARED (1+...+200=20100)');

# -------------------------------------------------------------------
# Scenario 4: Multiple prepared transactions survive crash
#
# Prepare two transactions that modify the same UNDO-enabled table,
# crash, verify both survive, then commit one and roll back the other.
# The committed UPDATE must be visible; the rolled-back UPDATE must not.
# -------------------------------------------------------------------

$node->safe_psql('postgres', q{
CREATE TABLE tbl_multi (id int PRIMARY KEY, val text) WITH (enable_undo = on);
INSERT INTO tbl_multi SELECT g, 'orig_' || g FROM generate_series(1, 100) g;
});

$node->safe_psql('postgres', q{
BEGIN;
UPDATE tbl_multi SET val = 'upd1_' || id WHERE id BETWEEN 1 AND 25;
PREPARE TRANSACTION 'undo_2pc_multi_1';
});

$node->safe_psql('postgres', q{
BEGIN;
UPDATE tbl_multi SET val = 'upd2_' || id WHERE id BETWEEN 26 AND 50;
PREPARE TRANSACTION 'undo_2pc_multi_2';
});

$node->stop('immediate');
$node->start;

my $s4_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_prepared_xacts
	  WHERE gid IN ('undo_2pc_multi_1', 'undo_2pc_multi_2')});
is($s4_count, '2',
	'scenario 4: both prepared transactions survive crash');

# Commit the first, roll back the second.
$node->safe_psql('postgres', q{COMMIT PREPARED 'undo_2pc_multi_1'});
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'undo_2pc_multi_2'});

# Verify pg_prepared_xacts is clear.
my $s4_remaining = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_prepared_xacts});
is($s4_remaining, '0',
	'scenario 4: no prepared transactions remain after resolution');

# Rows 1-25: committed UPDATE -> should have upd1_ values.
my $s4_upd1 = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_multi
	  WHERE id BETWEEN 1 AND 25 AND val LIKE 'upd1_%'});
is($s4_upd1, '25',
	'scenario 4: committed UPDATE visible (rows 1-25 have upd1_ values)');

# Rows 26-50: rolled-back UPDATE -> should have original orig_ values.
my $s4_orig = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_multi
	  WHERE id BETWEEN 26 AND 50 AND val LIKE 'orig_%'});
is($s4_orig, '25',
	'scenario 4: rolled-back UPDATE invisible (rows 26-50 restored to orig_)');

# Rows 51-100: untouched by either prepared transaction.
my $s4_untouched = $node->safe_psql('postgres',
	q{SELECT count(*) FROM tbl_multi
	  WHERE id BETWEEN 51 AND 100 AND val LIKE 'orig_%'});
is($s4_untouched, '50',
	'scenario 4: untouched rows (51-100) unaffected by prepared-txn resolution');

$node->stop;

done_testing();
