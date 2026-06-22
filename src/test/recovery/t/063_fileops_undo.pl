# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Test UNDO rollback of FileOps operations during normal transaction abort.
#
# Unlike 065_undo_adversarial_crash.pl (which crashes mid-abort and relies on
# crash recovery), this test verifies that the FILEOPS UNDO apply callbacks
# reverse filesystem state during ordinary top-level ABORT and subtransaction
# ROLLBACK TO SAVEPOINT, with no crash involved.
#
# FILEOPS uses the undo-in-common-WAL mechanism (UNDO batches written via
# InsertXactUndoData, replayed/reversed by the generic UNDO infrastructure).
# RECNO uses the per-relation buffer-manager UNDO fork (RM_RELUNDO).  The
# final two tests place BOTH mechanisms inside a single transaction to prove
# the dual-mode UNDO design reverses (and commits) them together.
#
# Vehicle: the test_fileops extension, whose SQL wrappers call FileOps* C
# functions directly and are legal inside a transaction block (unlike
# CREATE TABLESPACE, which cannot run inside BEGIN).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('fileops_undo');
$node->init;
$node->append_conf(
	"postgresql.conf", qq(
autovacuum = off
log_min_messages = debug2
));
$node->start;

if (!$node->check_extension('test_fileops'))
{
	plan skip_all => 'Extension test_fileops not installed';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_fileops');

my $datadir = $node->data_dir;

# ================================================================
# Test 1: FileOpsTruncate + ABORT restores original size
# ================================================================

$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('undo1.dat')});
my $result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/undo1.dat')});
is($result, '1024', 'Test 1: initial file size is 1024');

$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_truncate('$datadir/undo1.dat', 0);
ABORT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/undo1.dat')});
is($result, '1024', 'Test 1: truncate reversed by UNDO after ABORT');

# ================================================================
# Test 2: FileOpsTruncate + COMMIT persists
# ================================================================

$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_truncate('$datadir/undo1.dat', 256);
COMMIT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/undo1.dat')});
is($result, '256', 'Test 2: committed truncate persists');

# ================================================================
# Test 3: FileOpsChmod + ABORT restores original mode
# ================================================================

my $orig_mode = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/undo1.dat')});

$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_chmod('$datadir/undo1.dat', 384);  -- 0600
ABORT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/undo1.dat')});
is($result, $orig_mode, 'Test 3: chmod reversed by UNDO after ABORT');

# ================================================================
# Test 4: Subtransaction ROLLBACK TO restores size, COMMIT keeps it
# ================================================================

# File is 256 bytes from Test 2.  Truncate inside a savepoint, roll the
# savepoint back, then commit the outer transaction.  Size must be 256.
$node->safe_psql('postgres', qq(
BEGIN;
SAVEPOINT sp1;
SELECT test_fileops_truncate('$datadir/undo1.dat', 128);
ROLLBACK TO sp1;
COMMIT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/undo1.dat')});
is($result, '256', 'Test 4: subtransaction truncate reversed by ROLLBACK TO');

# ================================================================
# Test 5: Nested savepoints - outer op commits, inner op rolls back
# ================================================================

# Outer truncate to 64 (committed); inner truncate to 16 rolled back.
$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_truncate('$datadir/undo1.dat', 64);
SAVEPOINT sp1;
SELECT test_fileops_truncate('$datadir/undo1.dat', 16);
ROLLBACK TO sp1;
COMMIT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/undo1.dat')});
is($result, '64', 'Test 5: outer truncate persists, inner reversed');

# ================================================================
# Test 6: Multiple FileOps in one aborted txn - all reversed
# ================================================================

# File is 64 bytes; capture mode.  Truncate AND chmod in one txn, abort.
my $mode_before = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/undo1.dat')});

$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_truncate('$datadir/undo1.dat', 8);
SELECT test_fileops_chmod('$datadir/undo1.dat', 384);  -- 0600
ABORT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/undo1.dat')});
is($result, '64', 'Test 6: truncate reversed (multi-op abort)');
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_get_mode('$datadir/undo1.dat')});
is($result, $mode_before, 'Test 6: chmod reversed (multi-op abort)');

# ================================================================
# Test 7: MIXED MODE - RECNO (per-relation fork) + FILEOPS (undo-in-WAL)
#         in one aborted transaction.  Both mechanisms must reverse.
# ================================================================

$node->safe_psql('postgres', q{
CREATE TABLE mixed_r (id int PRIMARY KEY, s text) USING recno;
INSERT INTO mixed_r VALUES (1, 'base');
});

$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('mixed.dat')});
my $mixed_size_before = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/mixed.dat')});

$node->safe_psql('postgres', qq(
BEGIN;
INSERT INTO mixed_r VALUES (2, 'rollback-row');
UPDATE mixed_r SET s = 'rollback-update' WHERE id = 1;
SELECT test_fileops_truncate('$datadir/mixed.dat', 0);
ABORT;
));

# RECNO per-relation fork UNDO: inserted row invisible, update reversed.
$result = $node->safe_psql('postgres',
	"SELECT count(*) FROM mixed_r WHERE id = 2");
is($result, '0', 'Test 7: RECNO aborted INSERT invisible (per-rel fork)');
$result = $node->safe_psql('postgres',
	"SELECT s FROM mixed_r WHERE id = 1");
is($result, 'base', 'Test 7: RECNO aborted UPDATE reversed (per-rel fork)');

# FILEOPS undo-in-WAL: truncate reversed.
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/mixed.dat')});
is($result, $mixed_size_before, 'Test 7: FILEOPS truncate reversed (undo-in-WAL)');

# ================================================================
# Test 8: MIXED MODE - both mechanisms COMMIT together
# ================================================================

$node->safe_psql('postgres', qq(
BEGIN;
INSERT INTO mixed_r VALUES (3, 'committed-row');
SELECT test_fileops_truncate('$datadir/mixed.dat', 32);
COMMIT;
));

$result = $node->safe_psql('postgres',
	"SELECT s FROM mixed_r WHERE id = 3");
is($result, 'committed-row', 'Test 8: RECNO committed INSERT visible');
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_size('$datadir/mixed.dat')});
is($result, '32', 'Test 8: FILEOPS committed truncate persists');

# ================================================================
# Test 9: FileOpsLink + ABORT removes the new link
# ================================================================

# Hard-link an existing file inside an aborted txn; the new path must be
# gone after UNDO reverses the link.
$node->safe_psql('postgres',
	qq{SELECT test_fileops_create_tempfile('link_src.dat')});

$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_link('$datadir/link_src.dat', '$datadir/link_dst.dat');
ABORT;
));

$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_exists('$datadir/link_dst.dat')});
is($result, 'f', 'Test 9: link reversed by UNDO after ABORT');

# COMMIT keeps the link.
$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_link('$datadir/link_src.dat', '$datadir/link_dst.dat');
COMMIT;
));
$result = $node->safe_psql('postgres',
	qq{SELECT test_fileops_file_exists('$datadir/link_dst.dat')});
is($result, 't', 'Test 9: committed link persists');

# ================================================================
# Test 10: FileOpsSetXattr + ABORT restores prior xattr state
# ================================================================

# Setting a previously-absent xattr inside an aborted txn must leave the
# attribute absent after UNDO.  Skip gracefully if the filesystem does not
# support xattrs (setxattr returns false on ENOTSUP/EPERM).
my $xattr_ok = $node->safe_psql('postgres',
	qq{SELECT test_fileops_setxattr('$datadir/undo1.dat', 'user.recno_probe', 'committed')});

SKIP:
{
	skip 'filesystem does not support xattrs', 4 if $xattr_ok ne 't';

	# Probe set+committed above; confirm it is present and committed.
	$result = $node->safe_psql('postgres',
		qq{SELECT test_fileops_getxattr('$datadir/undo1.dat', 'user.recno_probe')});
	is($result, 'committed', 'Test 10: baseline xattr committed');

	# Overwrite the existing xattr inside an aborted txn -> original restored.
	$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_setxattr('$datadir/undo1.dat', 'user.recno_probe', 'rolledback');
ABORT;
));
	$result = $node->safe_psql('postgres',
		qq{SELECT test_fileops_getxattr('$datadir/undo1.dat', 'user.recno_probe')});
	is($result, 'committed',
		'Test 10: setxattr overwrite reversed to original value');

	# Set a brand-new xattr inside an aborted txn -> attribute absent again.
	$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_setxattr('$datadir/undo1.dat', 'user.recno_new', 'transient');
ABORT;
));
	$result = $node->safe_psql('postgres',
		qq{SELECT test_fileops_getxattr('$datadir/undo1.dat', 'user.recno_new')});
	is($result, '', 'Test 10: setxattr of new attr reversed (attr absent)');

	# ============================================================
	# Test 11: FileOpsRemoveXattr + ABORT restores the attribute
	# ============================================================

	# Remove the committed xattr inside an aborted txn -> value restored.
	$node->safe_psql('postgres', qq(
BEGIN;
SELECT test_fileops_removexattr('$datadir/undo1.dat', 'user.recno_probe');
ABORT;
));
	$result = $node->safe_psql('postgres',
		qq{SELECT test_fileops_getxattr('$datadir/undo1.dat', 'user.recno_probe')});
	is($result, 'committed', 'Test 11: removexattr reversed by UNDO after ABORT');
}

$node->stop;
done_testing();
