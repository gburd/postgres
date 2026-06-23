# Copyright (c) 2026, PostgreSQL Global Development Group
#
# UNDO retention-invariant recovery test (bug B2 + a deeper defect it exposes).
#
# UNDO's core CLAIM (src/backend/access/undo/README) is the CTR guarantee:
# an aborted transaction's UNDO is GUARANTEED to be applied, eventually and
# completely.  The logical revert worker's PG_CATCH used to *silently abandon*
# an un-reverted abort when it thought the UNDO WAL had been recycled
# (last_batch_lsn < redo), marking it reverted without doing the work.  That
# was justified as dead code: the retention chain
#
#     ATMGetOldestUnrevertedLSN (atm.c)
#       -> UndoGetOldestBatchLSN (undolog.c)
#         -> KeepLogSeg          (xlog.c)
#
# pins the UNDO WAL *segment* against recycling until ATMForget runs, so the
# branch "should be impossible".  We converted it to ereport(PANIC, ...).
#
# This test proves the two properties the retention invariant needs:
#
#   LEG 1 -- WAL RETAINED across checkpoint: create an un-reverted ATM abort
#     (ROLLBACK PREPARED of a FILEOPS transaction, with the logical revert
#     worker frozen at the 'logical-revert-before-process' injection point so
#     it cannot revert+forget), force a CHECKPOINT, and assert the WAL segment
#     holding the abort's last_batch_lsn is STILL PRESENT on disk.
#
#   LEG 2 -- ATM FAITHFULLY RECONSTRUCTED on restart: crash (-m immediate),
#     restart, and assert the ATM entry that justifies the retention is STILL
#     THERE, read via ATMGetOldestUnrevertedLSN (exposed as the test-only SQL
#     function test_fileops_atm_oldest_lsn() and logged by ATMRecoveryFinalize
#     as "ATM recovery complete: ... oldest unreverted LSN X/X").  This is NOT
#     "the WAL file exists on disk" -- it reads the reconstructed ATM state
#     directly.
#
# RESULT: leg 1 holds; leg 2 FAILS.  The retention invariant is NOT airtight.
# The WAL *segment* is retained (KeepLogSeg), but the checkpoint *redo pointer*
# is NOT held back to the oldest un-forgotten ATM entry.  A CHECKPOINT taken
# after the XLOG_ATM_ABORT record advances redo past that record; crash
# recovery replays from the redo point and never sees the abort, so atm_redo
# never reconstructs the ATM entry.  The aborted work is then never reverted --
# a SILENT loss of the guaranteed rollback, occurring at recovery, before the
# worker's PANIC branch can ever run.  See the deliverable report for the exact
# LSN evidence.
#
# Vehicle: test_fileops (FILEOPS common-WAL UNDO) + ROLLBACK PREPARED, which is
# the only deterministic way to create a durable un-reverted ATM entry:
# ordinary FILEOPS aborts are applied inline (undo_instant_abort_threshold),
# and RECNO uses the per-relation UNDO fork, not the ATM.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (($ENV{enable_injection_points} // 'no') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('undo_revert_retention');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
shared_preload_libraries = 'injection_points'
autovacuum = off
max_prepared_transactions = 10
wal_level = replica
logical_revert_naptime = 60000
));
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}
if (!$node->check_extension('test_fileops'))
{
	plan skip_all => 'Extension test_fileops not installed';
}
$node->safe_psql('postgres',
	'CREATE EXTENSION injection_points; CREATE EXTENSION test_fileops;');

my $datadir = $node->data_dir;
my $file = "$datadir/retention.dat";

# 0644 = 420, 0700 = 448 (decimal)
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('retention.dat');
	   SELECT test_fileops_chmod('$file', 420);});

# Freeze the logical revert worker so it cannot revert+forget the ATM entry
# we are about to create.  Without this, the worker reverts within
# milliseconds and there is nothing left to checkpoint/crash.
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('logical-revert-before-process', 'wait')});

# Create a durable un-reverted ATM entry: PREPARE a FILEOPS chmod, then
# ROLLBACK PREPARED it.  ROLLBACK PREPARED calls ATMAddAborted(), which emits
# XLOG_ATM_ABORT and pins the UNDO WAL via the retention chain.
$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_chmod('$file', 448);
PREPARE TRANSACTION 'p_ret';
));
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'p_ret'});

# Wait until the worker is provably parked at the injection point, i.e. the
# ATM entry exists and is un-reverted.
$node->poll_query_until('postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity
	  WHERE wait_event = 'logical-revert-before-process'})
  or die "logical revert worker never reached the freeze point";

# ----------------------------------------------------------------------------
# LEG 1: WAL segment retained across checkpoint
# ----------------------------------------------------------------------------
my $atm_lsn = $node->safe_psql('postgres',
	q{SELECT test_fileops_atm_oldest_lsn()});
isnt($atm_lsn, '',
	'leg 1: un-reverted ATM entry exists, pinning last_batch_lsn');

my $wal_seg = $node->safe_psql('postgres',
	q{SELECT pg_walfile_name(test_fileops_atm_oldest_lsn())});
my $mode = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$file')});
is($mode, '448', 'leg 1: chmod still applied (worker frozen, not yet reverted)');

$node->safe_psql('postgres', 'CHECKPOINT');

ok(-f "$datadir/pg_wal/$wal_seg",
	"leg 1: WAL segment $wal_seg holding the abort's last_batch_lsn is "
	  . "STILL PRESENT after CHECKPOINT (retention chain pinned it)");

# ----------------------------------------------------------------------------
# LEG 2: ATM faithfully reconstructed on restart
# ----------------------------------------------------------------------------
$node->stop('immediate');
$node->start;

# Read the reconstructed ATM state.  The 'logical-revert-before-process'
# injection point does NOT survive restart, so the worker is free to run; but
# the ATM entry must exist (reconstructed by atm_redo from XLOG_ATM_ABORT)
# regardless -- reconstruction happens during recovery, before any worker.
# ATMRecoveryFinalize logs it at recovery end, before the worker launches.
my $recovery_log = slurp_file($node->logfile);
like(
	$recovery_log,
	qr/ATM recovery complete: \d+ entries, [1-9]\d* unreverted, oldest unreverted LSN [0-9A-F]+\/[0-9A-F]+/,
	'leg 2: recovery reconstructed the un-reverted ATM entry '
	  . '(ATMGetOldestUnrevertedLSN pins the WAL) -- not merely file presence');

# ----------------------------------------------------------------------------
# Completion: the deferred rollback must actually happen, and the ATM entry
# must then be forgotten.  (The mode returns to 0644 and the ATM empties.)
# ----------------------------------------------------------------------------
$node->poll_query_until('postgres',
	qq{SELECT test_fileops_get_mode('$file') = 420})
  or die "aborted work was never reverted after restart -- rollback LOST";
is($node->safe_psql('postgres', qq{SELECT test_fileops_get_mode('$file')}),
	'420', 'completion: deferred rollback finished (0700 -> 0644)');

is($node->safe_psql('postgres', q{SELECT test_fileops_atm_oldest_lsn()}),
	'', 'completion: ATM entry forgotten after successful revert');

# The B2 PANIC must never have fired anywhere in this run.
unlike(slurp_file($node->logfile), qr/retention invariant violated/,
	'no spurious B2 PANIC (recycled-WAL branch not taken)');

$node->stop;

done_testing();
