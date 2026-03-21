
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test that UNDO-enabled table rollback is correctly observed on a
# streaming standby.
#
# With the current heap-based storage, rollback on the primary works
# via PostgreSQL's standard MVCC mechanism (CLOG marks the transaction
# as aborted).  WAL replay on the standby processes the same CLOG
# updates, so the standby should observe the correct post-rollback state.
#
# Scenarios tested:
#   1. INSERT then ROLLBACK - standby should see no new rows.
#   2. DELETE then ROLLBACK - standby should see all original rows.
#   3. UPDATE then ROLLBACK - standby should see original values.
#   4. Committed data interleaved with rollbacks.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary node with streaming replication support.
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', q{
enable_undo = on
autovacuum = off
});
$node_primary->start;

# Create UNDO-enabled table and insert base data on primary.
$node_primary->safe_psql('postgres', q{
CREATE TABLE standby_test (id int PRIMARY KEY, val text) WITH (enable_undo = on);
INSERT INTO standby_test SELECT g, 'base ' || g FROM generate_series(1, 20) g;
});

# Take a backup and create a streaming standby.
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

# Wait for the standby to catch up with the initial data.
$node_primary->wait_for_replay_catchup($node_standby);

# Verify initial state on standby.
my $standby_count = $node_standby->safe_psql('postgres',
	q{SELECT count(*) FROM standby_test});
is($standby_count, '20', 'standby has initial 20 rows');

# ---- Test 1: INSERT then ROLLBACK ----
# The rolled-back inserts should not appear on the standby.

$node_primary->safe_psql('postgres', q{
BEGIN;
INSERT INTO standby_test SELECT g, 'phantom ' || g FROM generate_series(100, 109) g;
ROLLBACK;
});

$node_primary->wait_for_replay_catchup($node_standby);

my $count_after_insert_rollback = $node_standby->safe_psql('postgres',
	q{SELECT count(*) FROM standby_test});
is($count_after_insert_rollback, '20',
	'standby: no phantom rows after INSERT rollback');

# ---- Test 2: DELETE then ROLLBACK ----
# All rows should remain on the standby after the DELETE is rolled back.

$node_primary->safe_psql('postgres', q{
BEGIN;
DELETE FROM standby_test WHERE id <= 10;
ROLLBACK;
});

$node_primary->wait_for_replay_catchup($node_standby);

my $count_after_delete_rollback = $node_standby->safe_psql('postgres',
	q{SELECT count(*) FROM standby_test});
is($count_after_delete_rollback, '20',
	'standby: all rows present after DELETE rollback');

# Check specific row content to verify tuple data restoration.
my $val_check = $node_standby->safe_psql('postgres',
	q{SELECT val FROM standby_test WHERE id = 5});
is($val_check, 'base 5',
	'standby: tuple content intact after DELETE rollback');

# ---- Test 3: UPDATE then ROLLBACK ----
# The original values should be preserved on the standby.

$node_primary->safe_psql('postgres', q{
BEGIN;
UPDATE standby_test SET val = 'modified ' || id WHERE id <= 10;
ROLLBACK;
});

$node_primary->wait_for_replay_catchup($node_standby);

my $count_after_update_rollback = $node_standby->safe_psql('postgres',
	q{SELECT count(*) FROM standby_test});
is($count_after_update_rollback, '20',
	'standby: row count unchanged after UPDATE rollback');

my $val_after_update_rollback = $node_standby->safe_psql('postgres',
	q{SELECT val FROM standby_test WHERE id = 3});
is($val_after_update_rollback, 'base 3',
	'standby: original value restored after UPDATE rollback');

# Verify no rows have 'modified' prefix.
my $modified_count = $node_standby->safe_psql('postgres',
	q{SELECT count(*) FROM standby_test WHERE val LIKE 'modified%'});
is($modified_count, '0',
	'standby: no modified values remain after UPDATE rollback');

# ---- Test 4: Committed data + rollback interleaving ----
# Verify that committed changes on the primary propagate correctly even
# when interleaved with rollbacks on UNDO-enabled tables.

$node_primary->safe_psql('postgres', q{
INSERT INTO standby_test VALUES (21, 'committed row');
});

$node_primary->safe_psql('postgres', q{
BEGIN;
DELETE FROM standby_test WHERE id = 21;
ROLLBACK;
});

$node_primary->wait_for_replay_catchup($node_standby);

my $committed_row = $node_standby->safe_psql('postgres',
	q{SELECT val FROM standby_test WHERE id = 21});
is($committed_row, 'committed row',
	'standby: committed row preserved despite subsequent DELETE rollback');

my $final_count = $node_standby->safe_psql('postgres',
	q{SELECT count(*) FROM standby_test});
is($final_count, '21',
	'standby: correct final row count (20 original + 1 committed)');

# Clean shutdown.
$node_standby->stop;
$node_primary->stop;

done_testing();
