# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Isolation/injection test (plan R5, scenario 3): algorithm SWAP racing with
# readers, with precise interleaving control.
#
# ALTER BUFFER POOL name SET (handler '...') is a barrier-quiesced
# destroy-and-recreate under the new routine (SwapDynamicBufferPoolAlgorithm ->
# DestroyDynamicBufferPool -> CreateDynamicBufferPool).  The dangerous window
# is between "old strategy state about to be freed" and "new algorithm's fresh
# state initialized": a reader mid-get_victim against the old strategy_data
# there is a use-after-free.  PROCSIGNAL_BARRIER_BUFPOOL_DETACH closes it by
# forcing every backend to drop the pool first.
#
# 006_algorithm_swap already proves the swap with a reader that is IDLE between
# queries.  This test PARKS the swap mid-teardown at the
# bufpool-destroy-before-quiesce injection point and drives a reader against
# the pool WHILE it is parked -- the exact race a missing/broken barrier would
# lose.  The reader must survive the old algorithm's strategy state being freed
# and the new one initialized underneath it.
#
# Only lru_pool_handler ships in this extension, so the swap is lru->lru; that
# still exercises the entire barrier-quiesced destroy+recreate swap path (old
# strategy freed, new strategy initialized) which is what the race concerns.

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

$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL sw_pool HANDLER lru_pool_handler SIZE '16777216';
CREATE TABLE t_sw (id int primary key, pad text) WITH (buffer_pool = 'sw_pool');
INSERT INTO t_sw SELECT g, repeat('s', 200) FROM generate_series(1, 10000) g;
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_sw;'),
	'10000', 'pool usable before swap');

# A long-lived reader attached to the pool's strategy state; stays alive
# across the whole swap.
my $reader = $node->background_psql('postgres');
is($reader->query_safe('SELECT count(*) FROM t_sw;'), '10000',
	'concurrent reader attached to pool before swap');

# Park the swap's teardown before quiescence.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufpool-destroy-before-quiesce', 'wait');"
);

# Start the algorithm swap in the background; it parks mid-teardown, BEFORE
# the old strategy state is freed and the new one is created.
my $swapper = $node->background_psql('postgres');
$swapper->query_until(qr/start_swap/,
	"\\echo start_swap\nALTER BUFFER POOL sw_pool SET (handler 'lru_pool_handler');\n"
);

$node->wait_for_event('client backend', 'bufpool-destroy-before-quiesce');
pass('SWAP parked at teardown injection point (before quiescence)');

# While the swap is parked (old strategy state still live), drive the reader
# against the pool -- the get_victim path runs against the about-to-be-freed
# strategy_data.  The barrier must force this backend to detach before the
# swap frees it.
$reader->query_safe('SELECT count(*) FROM t_sw WHERE id % 7 = 0;');

# Release the teardown -> barrier fires, reader detaches, old strategy freed,
# new algorithm initialized fresh.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufpool-destroy-before-quiesce');");

$swapper->query_safe('SELECT 1;');
ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'server alive after barrier-quiesced swap');

# Reader survived the strategy state being freed and reinitialized.
is($reader->query_safe('SELECT 77;'), '77',
	'concurrent reader survived algorithm swap teardown+recreate');

$node->safe_psql('postgres',
	"SELECT injection_points_detach('bufpool-destroy-before-quiesce');");

# Catalog reflects the (re-set) handler.
is( $node->safe_psql('postgres',
		"SELECT bphandler::regproc::text FROM pg_bufferpool WHERE bpname = 'sw_pool';"
	),
	'lru_pool_handler', 'catalog handler present after swap');

# Pool works after the swap; cold cache repopulates.
is($reader->query_safe('SELECT count(*) FROM t_sw;'), '10000',
	'pool usable after swap (reader)');
$reader->quit;
$swapper->quit;

# A second swap under several concurrent readers (no injection park), then a
# rejected swap to an unknown handler must leave the pool intact.
my @bg;
for my $c (1 .. 4)
{
	my $h = $node->background_psql('postgres');
	$h->query_safe('SELECT count(*) FROM t_sw;');
	push @bg, $h;
}
$node->safe_psql('postgres',
	"ALTER BUFFER POOL sw_pool SET (handler 'lru_pool_handler');");
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_sw;'),
	'10000', 'pool correct after swap under concurrent readers');
$_->query_safe('SELECT 1;') for @bg;
$_->quit for @bg;

my ($ret, $out, $err) = $node->psql('postgres',
	"ALTER BUFFER POOL sw_pool SET (handler 'no_such_handler');");
isnt($ret, 0, 'unknown handler rejected');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_sw;'),
	'10000', 'pool intact after rejected swap');

# Clean up + restart proves no shared-memory corruption.
$node->safe_psql('postgres', 'DROP TABLE t_sw; DROP BUFFER POOL sw_pool;');
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1',
	'clean restart after swaps');

$node->stop;
done_testing();
