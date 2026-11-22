# Copyright (c) 2026, PostgreSQL Global Development Group

# A threaded standby whose replay is paused must still promote.  The startup
# fiber waits for the pause to end on a condition variable, and that wait's
# CHECK_FOR_INTERRUPTS() drains the backend's interrupt mailbox, so the
# promote request has to be applied there and not only in the startup loop.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{
multithreaded = on
carrier_loops = 2
});
$primary->start;
$primary->safe_psql('postgres', 'CREATE TABLE t AS SELECT 1 AS a');
$primary->backup('backup');

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
$standby->start;
$primary->wait_for_replay_catchup($standby);

is($standby->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'standby runs threaded');

$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$primary->safe_psql('postgres', 'INSERT INTO t VALUES (2)');
ok( $standby->poll_query_until(
		'postgres', "SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'replay is paused');

$standby->promote;
is($standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'), 'f',
	'paused standby promoted');

$standby->stop;
$primary->stop;
done_testing();
