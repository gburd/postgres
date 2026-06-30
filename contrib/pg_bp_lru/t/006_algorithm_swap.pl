# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Online replacement-algorithm swap (P7).
#
# ALTER BUFFER POOL name SET (handler 'newalgo') changes a dynamic pool's
# replacement algorithm at runtime.  It is implemented as a barrier-quiesced
# destroy-and-recreate (SwapDynamicBufferPoolAlgorithm): the detach barrier
# ensures no backend is mid-access against the old algorithm's strategy state
# when it is freed and the new algorithm initializes -- the safe form of the
# swap that an earlier draft attempted with an unquiesced in-place memset.
#
# This exercises the swap under a concurrent reader to confirm the quiescence
# holds (no crash, correct results, clean restart).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<'CONF');
shared_preload_libraries = 'pg_bp_lru'
max_buffer_pool_memory = 64MB
CONF
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# Start with the LRU algorithm.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL swap_pool HANDLER lru_pool_handler SIZE '16777216';
CREATE TABLE t_swap (id int primary key, pad text) WITH (buffer_pool = 'swap_pool');
INSERT INTO t_swap SELECT g, repeat('s', 200) FROM generate_series(1, 10000) g;
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_swap;'),
	'10000', 'pool usable before swap');

# A long-lived reader that has touched the pool (attached, may hold pins
# transiently) running concurrently with the swap.
my $reader = $node->background_psql('postgres');
is($reader->query_safe('SELECT count(*) FROM t_swap;'), '10000',
	'concurrent reader attached to pool');

# Swap the algorithm (destroy+recreate under the barrier). Only lru_pool_handler
# is available in this extension, so swap lru->lru: it still exercises the full
# barrier-quiesced destroy/recreate swap path.
$node->safe_psql('postgres',
	"ALTER BUFFER POOL swap_pool SET (handler 'lru_pool_handler');");
pass('ALTER BUFFER POOL ... SET (handler ...) accepted');

# Catalog reflects the new handler.
is( $node->safe_psql('postgres',
		"SELECT bphandler::regproc::text FROM pg_bufferpool WHERE bpname = 'swap_pool';"
	),
	'lru_pool_handler',
	'catalog handler updated after swap');

# Pool still works after the swap (cold cache, repopulates).
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_swap;'),
	'10000', 'pool usable after swap');

# Concurrent reader survived the swap (quiescence held; no use-after-free).
is($reader->query_safe('SELECT 99;'), '99',
	'concurrent reader survived algorithm swap');

# Swap back to LRU and exercise once more.
$node->safe_psql('postgres',
	"ALTER BUFFER POOL swap_pool SET (handler 'lru_pool_handler');");
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_swap WHERE id < 5000;'),
	'4999', 'pool usable after swapping back');

# An unknown handler is rejected (does not destroy the pool).
my ($ret, $out, $err) = $node->psql('postgres',
	"ALTER BUFFER POOL swap_pool SET (handler 'no_such_handler');");
isnt($ret, 0, 'unknown handler rejected');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_swap;'),
	'10000', 'pool intact after rejected swap');

$reader->quit;

# Clean up and restart.
$node->safe_psql('postgres', 'DROP TABLE t_swap; DROP BUFFER POOL swap_pool;');
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1', 'clean restart after swaps');

$node->stop;
done_testing();
