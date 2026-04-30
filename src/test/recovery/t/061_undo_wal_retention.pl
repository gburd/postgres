# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test WAL retention for cluster-wide UNDO operations.
#
# This test verifies that the UNDO WAL retention mechanism prevents WAL
# recycling while a transaction has unprocessed UNDO data.
#
# Scenario:
#   1. Start a cluster with enable_undo=on
#   2. Create an UNDO-enabled table
#   3. Begin a transaction that generates UNDO batches but does NOT commit
#   4. Force a checkpoint (twice) -- WAL should NOT be recycled
#   5. Verify the WAL segment containing the UNDO batch still exists
#   6. Commit, checkpoint, verify data integrity

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('undo_wal_retention');
$node->init;
$node->append_conf('postgresql.conf', qq{
enable_undo = on
shared_buffers = 64MB
});
$node->start;

# Create an UNDO-enabled table.
$node->safe_psql('postgres',
	"CREATE TABLE t (id int) WITH (enable_undo = on)");

# Record the WAL segment before the long-running transaction.
my $lsn_before = $node->safe_psql('postgres',
	"SELECT pg_current_wal_lsn()");
my $seg_before = $node->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_current_wal_lsn())");

# Start a background connection: begin a transaction that generates UNDO
# batches but does NOT commit.
my $bgconn = $node->background_psql('postgres');
$bgconn->query_safe("BEGIN");
$bgconn->query_safe("INSERT INTO t SELECT generate_series(1,1000)");

# Force WAL flush and double checkpoint -- WAL should NOT be recycled
# because the open transaction holds UNDO data.
$node->safe_psql('postgres', "SELECT pg_switch_wal()");
$node->safe_psql('postgres', "CHECKPOINT");
$node->safe_psql('postgres', "CHECKPOINT");

# The WAL segment containing the UNDO batch should still exist on disk.
my $seg_after_checkpoint = $node->safe_psql('postgres',
	"SELECT pg_walfile_name('$lsn_before'::pg_lsn)");
my $wal_dir = $node->data_dir . '/pg_wal';
ok(-f "$wal_dir/$seg_after_checkpoint",
	"WAL segment containing UNDO batch is retained during open transaction");

# Commit the transaction and let the background connection finish.
$bgconn->query_safe("COMMIT");
$bgconn->quit;

# Checkpoint again to allow recycling.
$node->safe_psql('postgres', "CHECKPOINT");
$node->safe_psql('postgres', "SELECT pg_switch_wal()");
$node->safe_psql('postgres', "CHECKPOINT");

# Verify the server is still running and data is correct.
my $count = $node->safe_psql('postgres', "SELECT count(*) FROM t");
is($count, '1000', "table has correct row count after commit");

$node->stop;

done_testing();
