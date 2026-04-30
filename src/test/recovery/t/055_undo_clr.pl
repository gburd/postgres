
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test that UNDO WAL records are properly generated for tables with
# enable_undo=on and that rollback works correctly.
#
# This test verifies:
#   1. XLOG_UNDO_BATCH WAL records are generated when DML modifies
#      an UNDO-enabled table.
#   2. Transaction rollback correctly restores data (via MVCC).
#   3. UNDO records are written to the WAL even though physical UNDO
#      application is not needed for standard heap rollback.
#
# We use pg_waldump to inspect the WAL and confirm the presence of
# Undo/BATCH entries after DML operations.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
enable_undo = on
wal_level = replica
autovacuum = off
});
$node->start;

# Record the WAL insert position before any UNDO activity.
my $start_lsn = $node->safe_psql('postgres',
	q{SELECT pg_current_wal_insert_lsn()});

# Create a table with UNDO logging enabled.
$node->safe_psql('postgres',
	q{CREATE TABLE undo_clr_test (id int, val text) WITH (enable_undo = on)});

# Insert some data and commit, so there is data to operate on.
$node->safe_psql('postgres',
	q{INSERT INTO undo_clr_test SELECT g, 'row ' || g FROM generate_series(1, 10) g});

# Record LSN after the committed inserts.
my $after_insert_lsn = $node->safe_psql('postgres',
	q{SELECT pg_current_wal_insert_lsn()});

# Execute a transaction that modifies the UNDO-enabled table and then
# rolls back.  The DML should generate UNDO BATCH WAL records, and
# the rollback should correctly restore data via MVCC.
my $before_rollback_lsn = $node->safe_psql('postgres',
	q{SELECT pg_current_wal_insert_lsn()});

$node->safe_psql('postgres', q{
BEGIN;
DELETE FROM undo_clr_test WHERE id <= 5;
ROLLBACK;
});

# Record the LSN after the rollback so we can bound our pg_waldump search.
my $end_lsn = $node->safe_psql('postgres',
	q{SELECT pg_current_wal_insert_lsn()});

# Force a WAL switch to ensure all records are on disk.
$node->safe_psql('postgres', q{SELECT pg_switch_wal()});

# Use pg_waldump to examine WAL between the start and end LSNs.
# Filter for the Undo resource manager to find BATCH entries that
# were generated during the INSERT operations.
my ($stdout, $stderr);
IPC::Run::run [
	'pg_waldump',
	'--start' => $start_lsn,
	'--end' => $end_lsn,
	'--rmgr' => 'Undo',
	'--path' => $node->data_dir . '/pg_wal/',
  ],
  '>' => \$stdout,
  '2>' => \$stderr;

# Check that UNDO BATCH records were generated during DML.
my @batch_lines = grep { /BATCH/ } split(/\n/, $stdout);

ok(@batch_lines > 0,
	'pg_waldump shows Undo/BATCH records during DML on undo-enabled table');

# Verify that the table data is correct after rollback: all 10 rows
# should be present since the DELETE was rolled back.
my $row_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM undo_clr_test});
is($row_count, '10', 'all rows restored after ROLLBACK');

# Test INSERT rollback works correctly too.
$node->safe_psql('postgres', q{
BEGIN;
INSERT INTO undo_clr_test SELECT g, 'new ' || g FROM generate_series(100, 104) g;
ROLLBACK;
});

# Verify the inserted rows did not persist.
my $row_count2 = $node->safe_psql('postgres',
	q{SELECT count(*) FROM undo_clr_test});
is($row_count2, '10', 'no extra rows after INSERT rollback');

# Test UPDATE rollback restores original values.
$node->safe_psql('postgres', q{
BEGIN;
UPDATE undo_clr_test SET val = 'modified' WHERE id <= 5;
ROLLBACK;
});

my $val_check = $node->safe_psql('postgres',
	q{SELECT val FROM undo_clr_test WHERE id = 3});
is($val_check, 'row 3', 'original value restored after UPDATE rollback');

$node->stop;

done_testing();
