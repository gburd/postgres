# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Concurrency test for DROP BUFFER POOL quiescence.
#
# Proves the PROCSIGNAL_BARRIER_BUFPOOL_DETACH quiescence in
# DestroyDynamicBufferPool: while a DROP BUFFER POOL is paused mid-flight
# (at the bufpool-destroy-before-quiesce injection point), other backends
# remain attached to the pool's DSM.  When the DROP is released it emits the
# detach barrier and waits for every backend to drop its references before
# detaching/unpinning the DSM.  The server must not crash and must end in a
# consistent state.
#
# Without the barrier, a backend still mapped to (or mid-allocation against)
# the pool's DSM when the DROP detaches/unpins it would SIGSEGV/SIGBUS.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (($ENV{enable_injection_points} // 'no') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_bp_lru'");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# Create a user buffer pool and a table routed to it, generate traffic so
# backends attach to the pool DSM, then release the table's dependency so the
# pool itself can be dropped.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL lru_pool HANDLER lru_pool_handler SIZE '8388608';
CREATE TABLE t_on_pool (id int primary key, pad text) WITH (buffer_pool = 'lru_pool');
INSERT INTO t_on_pool SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g;
SQL

# A long-lived worker session that has touched the pool (so it is attached to
# the pool DSM) and will still be alive across the teardown.
my $worker = $node->background_psql('postgres');
is($worker->query_safe('SELECT count(*) FROM t_on_pool;'),
	'2000', 'worker reads from pooled table');

# Release the table dependency so DROP BUFFER POOL is permitted.
$node->safe_psql('postgres', 'ALTER TABLE t_on_pool RESET (buffer_pool);');

# 1) Arm the injection point so the pool teardown pauses before quiescence.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufpool-destroy-before-quiesce', 'wait');"
);

# 2) Start DROP BUFFER POOL in the background; it parks at the injection point.
my $dropper = $node->background_psql('postgres');
$dropper->query_until(qr/start_drop/,
	"\\echo start_drop\nDROP BUFFER POOL lru_pool;\n");

$node->wait_for_event('client backend', 'bufpool-destroy-before-quiesce');
pass('DROP BUFFER POOL parked at quiescence injection point');

# 3) While teardown is parked, drive more buffer traffic from the worker.
#    (Now reading from the default pool since the table was reset, but the
#    worker is still attached to the lru_pool DSM from earlier.)
$worker->query_safe("SELECT count(*) FROM t_on_pool;");

# 4) Release the teardown -> emits PROCSIGNAL_BARRIER_BUFPOOL_DETACH and waits
#    for the worker (and all backends) to detach before tearing down the DSM.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufpool-destroy-before-quiesce');");

# 5) Cluster survives the barrier-quiesced teardown.
ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'server alive after barrier-quiesced pool teardown');

# 6) Worker session still usable: no use-after-detach corruption.
is($worker->query_safe('SELECT 42;'), '42',
	'concurrent worker survived pool teardown');

$worker->quit;
$dropper->quit;

# 7) Clean restart proves shared state was not corrupted.
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1',
	'clean restart after teardown');

$node->stop;
done_testing();
