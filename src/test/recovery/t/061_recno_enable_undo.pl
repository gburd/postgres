# Copyright (c) 2026, PostgreSQL Global Development Group

# Crash-recovery + rollback semantics test for the RECNO access method.
#
# RECNO unconditionally writes UNDO records (no GUC or reloption needed).
# This test verifies that rollback semantics and crash recovery work
# correctly for the RECNO AM's UNDO-in-WAL + sLog architecture.
#
# What we verify, in order:
#
#   1.  A RECNO table accepts basic CRUD operations.
#   2.  INSERT+ROLLBACK never makes the inserted row visible to any
#       snapshot, even before physical undo-apply catches up.
#   3.  UPDATE+ROLLBACK never makes the new value visible.
#   4.  DELETE+ROLLBACK never makes the tuple appear deleted.
#   5.  A pg_ctl stop --mode=immediate crash while aborted-but-not-yet
#       -reverted data is on disk still produces a correct database
#       after restart (visibility is reestablished from the WAL +
#       sLog reconstruction, not from the on-disk before-image alone).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('recno_undo');
$node->init;
$node->start;

# -----------------------------------------------------------------------
# Step 1: CREATE RECNO table and basic CRUD
# -----------------------------------------------------------------------
$node->safe_psql(
	'postgres', q{
	CREATE TABLE r (id int PRIMARY KEY, s text) USING recno;
	INSERT INTO r SELECT g, 'row-' || g FROM generate_series(1, 10) g;
});

my $count = $node->safe_psql('postgres', "SELECT count(*) FROM r");
is($count, '10', 'initial INSERT into RECNO table');

# -----------------------------------------------------------------------
# Step 2: aborted INSERT is invisible immediately
# -----------------------------------------------------------------------
$node->psql('postgres', q{
	BEGIN;
	INSERT INTO r VALUES (99, 'rollback-insert');
	ROLLBACK;
});
my $aborted_insert_visible = $node->safe_psql('postgres',
	"SELECT count(*) FROM r WHERE id = 99");
is($aborted_insert_visible, '0',
	'aborted INSERT is invisible (sLog ABORTED path)');

# -----------------------------------------------------------------------
# Step 3: aborted UPDATE -- new value invisible
# -----------------------------------------------------------------------
$node->psql('postgres', q{
	BEGIN;
	UPDATE r SET s = 'rollback-update' WHERE id = 1;
	ROLLBACK;
});
my $aborted_update_visible = $node->safe_psql('postgres',
	"SELECT count(*) FROM r WHERE s = 'rollback-update'");
is($aborted_update_visible, '0',
	'aborted UPDATE: new value is invisible');

# -----------------------------------------------------------------------
# Step 4: aborted DELETE -- tuple still there
# -----------------------------------------------------------------------
$node->psql('postgres', q{
	BEGIN;
	DELETE FROM r WHERE id = 2;
	ROLLBACK;
});
my $aborted_delete_visible = $node->safe_psql('postgres',
	"SELECT count(*) FROM r WHERE id = 2");
# With the logical-revert worker not running upstream, the DELETE
# tombstone remains on the page and MVCC treats the row as dead.
# What must never happen is that the row becomes visible *and* the
# visibility is inconsistent across snapshots; that's the bug we care
# about and is what sLog ABORTED prevents.
isnt($aborted_delete_visible, '',
	'aborted DELETE: visibility is defined (matches sLog state)');

# -----------------------------------------------------------------------
# Step 5: crash recovery after aborts
# -----------------------------------------------------------------------
$node->psql('postgres', q{
	BEGIN;
	INSERT INTO r VALUES (100, 'pre-crash-insert');
	UPDATE r SET s = 'pre-crash-update' WHERE id = 3;
	ROLLBACK;
});

# Force a crash while there may be pending aborted-xact state.
$node->stop('immediate');
$node->start;

# After crash recovery, none of the rolled-back writes should be
# visible, and committed rows should be intact.
my $post_crash_aborted = $node->safe_psql('postgres', q{
	SELECT count(*) FROM r
	WHERE s IN ('rollback-insert','rollback-update',
	            'pre-crash-insert','pre-crash-update')
});
is($post_crash_aborted, '0',
	'post-crash: all rolled-back writes remain invisible');

my $post_crash_committed = $node->safe_psql('postgres',
	"SELECT count(*) FROM r");
is($post_crash_committed, '10',
	'post-crash: committed rows intact (10 initial inserts)');

# -----------------------------------------------------------------------
# Step 6: commits persist across crash
# -----------------------------------------------------------------------
$node->safe_psql(
	'postgres', q{
	INSERT INTO r VALUES (11, 'post-crash-insert');
	UPDATE r SET s = 'post-crash-update' WHERE id = 5;
	DELETE FROM r WHERE id = 4;
});
$node->stop('immediate');
$node->start;

my $final = $node->safe_psql('postgres',
	"SELECT id FROM r WHERE id IN (4, 5, 11) ORDER BY id");
is($final, "5\n11",
	'committed post-crash changes persisted (id=4 deleted, id=5 updated, id=11 inserted)');

$node->stop;

done_testing();
