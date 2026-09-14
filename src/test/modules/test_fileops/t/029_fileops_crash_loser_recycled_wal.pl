# Copyright (c) 2026, PostgreSQL Global Development Group

# A crash loser's UNDO batches can lie in WAL that a standby has already
# removed at a restartpoint by the time it replays the end of the primary's
# crash recovery.  The primary logs the rollback with the UNDO records it
# applied (XLOG_UNDO_ROLLBACK), so the standby does not need the batches.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf',
	"min_wal_size = 32MB\nmax_wal_size = 32MB\ncheckpoint_timeout = 1h\n");
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_fileops');
$primary->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'b', has_streaming => 1);
$standby->start;

my $bg = $primary->background_psql('postgres');
$bg->query_safe("BEGIN; SELECT test_fileops_create('old-loser.dat', 384);");
my $seg = $primary->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_insert_lsn())');

# Move the standby's restartpoints past the loser's batch, so that it can
# remove the segment.  The primary keeps it for the open transaction.
$primary->safe_psql('postgres', 'CREATE TABLE t (a int)');
for (1 .. 6)
{
	$primary->safe_psql('postgres',
		'INSERT INTO t SELECT generate_series(1, 1000);'
		  . ' SELECT pg_switch_wal(); CHECKPOINT;');
	$primary->wait_for_catchup($standby);
	$standby->safe_psql('postgres', 'CHECKPOINT');
}
my $sdir = $standby->data_dir;
ok(!-e "$sdir/pg_wal/$seg", "standby removed $seg");
ok(-e $standby->data_dir . '/old-loser.dat', 'standby has the loser file');

$primary->stop('immediate');
eval { $bg->quit; };
$primary->start;
$primary->wait_for_catchup($standby);
ok(!-e $primary->data_dir . '/old-loser.dat', 'primary rolled back the loser');
ok(!-e "$sdir/old-loser.dat", 'standby rolled back the loser');

$standby->stop;
$primary->stop;
done_testing();
