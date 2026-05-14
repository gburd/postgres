# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Test UNDO rollback of FileOps operations.
#
# These tests verify that UNDO correctly reverses immediate-execution
# FileOps when a transaction is aborted or a subtransaction is rolled back.
#
# Gap 1: UNDO rollback of FileOpsMkdir/FileOpsSymlink via CREATE TABLESPACE
# Gap 4: Subtransaction rollback of FileOps UNDO
#
# Unlike 054_fileops_recovery.pl (which tests WAL redo after crash), this
# test verifies that the UNDO apply callbacks properly clean up filesystem
# state during normal transaction abort.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Path qw(rmtree);

my $node = PostgreSQL::Test::Cluster->new('fileops_undo');
$node->init;
$node->append_conf(
	"postgresql.conf", qq(
autovacuum = off
log_min_messages = debug2
));
$node->start;

my $pgdata = $node->data_dir;

# ================================================================
# Test 1: CREATE TABLESPACE + ROLLBACK - UNDO removes directory
# ================================================================

# With in-place tablespaces, CREATE TABLESPACE creates a directory
# under pg_tblspc/.  On ROLLBACK, the UNDO callback should rmdir it.

$node->safe_psql("postgres", qq(
SET allow_in_place_tablespaces = on;
));

# Get the tablespace OID that would be assigned (by checking pg_tablespace after)
my $result = $node->psql("postgres", qq(
BEGIN;
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE undo_test_ts LOCATION '';
ROLLBACK;
));

# After ROLLBACK, the tablespace should not exist in the catalog
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_test_ts'");
is($result, '0', 'tablespace catalog entry removed after ROLLBACK');

# Check that no new directories were left behind in pg_tblspc/
# (In-place tablespaces create dirs under pg_tblspc/<oid>)
my @tblspc_entries = glob("$pgdata/pg_tblspc/*");
# Filter out any pre-existing entries (there should be none in a fresh cluster)
my @unexpected = grep { -d $_ } @tblspc_entries;
is(scalar(@unexpected), 0,
	'no tablespace directories left after ROLLBACK (UNDO cleaned up)');

# ================================================================
# Test 2: CREATE TABLESPACE + COMMIT - directory persists
# ================================================================

$node->safe_psql("postgres", qq(
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE undo_commit_ts LOCATION '';
));

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_commit_ts'");
is($result, '1', 'tablespace exists after COMMIT');

# There should be a directory in pg_tblspc/
@tblspc_entries = glob("$pgdata/pg_tblspc/*");
my @committed_dirs = grep { -d $_ } @tblspc_entries;
cmp_ok(scalar(@committed_dirs), '>=', 1,
	'tablespace directory exists after COMMIT');

# Clean up
$node->safe_psql("postgres", "DROP TABLESPACE undo_commit_ts");

# ================================================================
# Test 3: CREATE TABLESPACE in subtransaction with ROLLBACK TO
# ================================================================

$node->psql("postgres", qq(
BEGIN;
SET allow_in_place_tablespaces = on;
SAVEPOINT sp1;
CREATE TABLESPACE undo_sp_ts LOCATION '';
ROLLBACK TO sp1;
COMMIT;
));

# After ROLLBACK TO + COMMIT, tablespace should not exist
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_sp_ts'");
is($result, '0', 'tablespace removed after ROLLBACK TO SAVEPOINT');

# No leftover directories
@tblspc_entries = glob("$pgdata/pg_tblspc/*");
@unexpected = grep { -d $_ } @tblspc_entries;
is(scalar(@unexpected), 0,
	'no tablespace directories left after subtransaction rollback');

# ================================================================
# Test 4: Nested subtransactions with CREATE TABLESPACE
# ================================================================

$node->psql("postgres", qq(
BEGIN;
SET allow_in_place_tablespaces = on;

-- Outer tablespace (will be committed)
CREATE TABLESPACE undo_outer_ts LOCATION '';

SAVEPOINT sp1;
-- Inner tablespace (will be rolled back)
CREATE TABLESPACE undo_inner_ts LOCATION '';
ROLLBACK TO sp1;

COMMIT;
));

# Outer tablespace should exist, inner should not
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_outer_ts'");
is($result, '1', 'outer tablespace persists after commit');

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_inner_ts'");
is($result, '0', 'inner tablespace removed after ROLLBACK TO');

# Clean up
$node->safe_psql("postgres", "DROP TABLESPACE undo_outer_ts");

# ================================================================
# Test 5: CREATE TABLE in tablespace + ROLLBACK
# ================================================================

$node->psql("postgres", qq(
BEGIN;
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE undo_table_ts LOCATION '';
CREATE TABLE undo_tbl (id int) TABLESPACE undo_table_ts;
INSERT INTO undo_tbl VALUES (1);
ROLLBACK;
));

# Both table and tablespace should be gone
$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_table_ts'");
is($result, '0', 'tablespace with table removed after ROLLBACK');

my ($ret, $stdout, $stderr) = $node->psql("postgres",
	"SELECT * FROM undo_tbl");
isnt($ret, 0, 'table in rolled-back tablespace does not exist');

# ================================================================
# Test 6: Multiple savepoints - partial rollback
# ================================================================

$node->psql("postgres", qq(
BEGIN;
SET allow_in_place_tablespaces = on;

CREATE TABLESPACE undo_multi_ts1 LOCATION '';

SAVEPOINT sp1;
CREATE TABLESPACE undo_multi_ts2 LOCATION '';

SAVEPOINT sp2;
CREATE TABLESPACE undo_multi_ts3 LOCATION '';
ROLLBACK TO sp2;  -- removes ts3

COMMIT;  -- ts1 and ts2 persist
));

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_multi_ts1'");
is($result, '1', 'ts1 persists (not rolled back)');

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_multi_ts2'");
is($result, '1', 'ts2 persists (savepoint committed)');

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_tablespace WHERE spcname = 'undo_multi_ts3'");
is($result, '0', 'ts3 removed (rolled back at sp2)');

# Clean up
$node->safe_psql("postgres", qq(
DROP TABLESPACE undo_multi_ts1;
DROP TABLESPACE undo_multi_ts2;
));

# ================================================================
# Test 7: CREATE DATABASE + ROLLBACK
# (FileOpsMkdir + FileOpsCreate in CreateDirAndVersionFile)
# ================================================================

$node->psql("postgres", qq(
BEGIN;
CREATE DATABASE undo_test_db;
ROLLBACK;
));

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_database WHERE datname = 'undo_test_db'");
is($result, '0', 'database removed after ROLLBACK');

# ================================================================
# Test 8: CREATE DATABASE in subtransaction + ROLLBACK TO
# ================================================================

$node->psql("postgres", qq(
BEGIN;
SAVEPOINT sp1;
CREATE DATABASE undo_sp_db;
ROLLBACK TO sp1;
COMMIT;
));

$result = $node->safe_psql("postgres",
	"SELECT count(*) FROM pg_database WHERE datname = 'undo_sp_db'");
is($result, '0', 'database removed after subtransaction ROLLBACK TO');

# ================================================================
# Test 9: Verify no filesystem debris after all rollback tests
# ================================================================

# All tablespaces should be cleaned up
@tblspc_entries = glob("$pgdata/pg_tblspc/*");
@unexpected = grep { -d $_ } @tblspc_entries;
is(scalar(@unexpected), 0,
	'no orphaned tablespace directories remain');

# ================================================================
# Done
# ================================================================

$node->stop;
done_testing();
