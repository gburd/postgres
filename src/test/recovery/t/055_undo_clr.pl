
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test that per-relation UNDO WAL records are properly generated for
# RECNO tables.
#
# This test verifies:
#   1. RelUndo WAL records (INIT, INSERT) are generated when DML
#      modifies a RECNO table (which uses per-relation UNDO).
#   2. The UNDO infrastructure is properly initialized for RECNO tables.
#
# Note: Full transaction rollback (physical UNDO apply) for RECNO is
# still being developed.  The MVCC visibility check after ROLLBACK
# does not yet properly handle all cases.

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
log_min_messages = warning
});
$node->start;

# Check if RECNO AM is available
my $has_recno = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_am WHERE amname = 'recno'});
if ($has_recno eq '0')
{
	plan skip_all => 'recno access method not available';
}

# Record the WAL insert position before any UNDO activity.
my $start_lsn = $node->safe_psql('postgres',
	q{SELECT pg_current_wal_insert_lsn()});

# Create a RECNO table (which automatically gets a RELUNDO fork).
$node->safe_psql('postgres',
	q{CREATE TABLE undo_clr_test (id int, val text) USING recno});

# Insert some data and commit.
$node->safe_psql('postgres',
	q{INSERT INTO undo_clr_test SELECT g, 'row ' || g FROM generate_series(1, 10) g});

# Verify data was inserted correctly.
my $row_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM undo_clr_test});
is($row_count, '10', 'inserted 10 rows into RECNO table');

# Record LSN after the committed inserts.
my $end_lsn = $node->safe_psql('postgres',
	q{SELECT pg_current_wal_insert_lsn()});

# Force a WAL switch to ensure all records are on disk.
$node->safe_psql('postgres', q{SELECT pg_switch_wal()});

# Use pg_waldump to examine WAL between the start and end LSNs.
my ($stdout, $stderr);
IPC::Run::run [
	'pg_waldump',
	'--start' => $start_lsn,
	'--end' => $end_lsn,
	'--rmgr' => 'RelUndo',
	'--path' => $node->data_dir . '/pg_wal/',
  ],
  '>' => \$stdout,
  '2>' => \$stderr;

# Check that RelUndo WAL records were generated.
my @relundo_lines = split(/\n/, $stdout);

ok(@relundo_lines > 0,
	'pg_waldump shows RelUndo WAL records during DML on RECNO table');

# Check specifically for INIT record (from table creation).
my @init_lines = grep { /INIT/ } @relundo_lines;
ok(@init_lines > 0,
	'RelUndo INIT record found (from RELUNDO fork creation)');

# Check for INSERT records (from DML operations).
my @insert_lines = grep { /INSERT/ } @relundo_lines;
ok(@insert_lines > 0,
	'RelUndo INSERT records found (from UNDO logging of DML)');

$node->stop;

done_testing();
