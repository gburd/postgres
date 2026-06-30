# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# NUMA interleaving of buffer pool memory (P5).
#
# When buffer_pool_numa is on, a reservation-backed pool's memory
# is interleaved across NUMA nodes on multi-node systems.  The placement is
# algorithm-agnostic (it acts on the pool's committed memory, not the eviction
# policy), gated on USE_LIBNUMA + numa_available() + more than one node.  On a
# single-node or non-NUMA host the interleave is a no-op, so this test only
# asserts that enabling it is harmless and pools still work; the multi-node
# distribution itself requires NUMA hardware to observe.

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
buffer_pool_numa = on
CONF
$node->start;

is($node->safe_psql('postgres', 'SHOW buffer_pool_numa;'),
	'on', 'buffer_pool_numa GUC accepted');

$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# A reservation-backed pool created with interleaving enabled must work
# (interleave applied on commit; no-op on single-node systems).
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL numa_pool HANDLER lru_pool_handler SIZE '16777216';
CREATE TABLE t_numa (id int primary key, pad text) WITH (buffer_pool = 'numa_pool');
INSERT INTO t_numa SELECT g, repeat('n', 300) FROM generate_series(1, 20000) g;
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_numa;'),
	'20000', 'reservation pool with NUMA interleave is usable');

# Read/write churn to ensure interleaved pages are exercised.
$node->safe_psql('postgres', 'UPDATE t_numa SET pad = repeat(\'m\', 300) WHERE id % 2 = 0;');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_numa WHERE pad LIKE \'m%\';'),
	'10000', 'writes through NUMA-interleaved pool');

# Drop + recreate (re-commits + re-interleaves the freed extent).
$node->safe_psql('postgres', 'DROP TABLE t_numa; DROP BUFFER POOL numa_pool;');
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL numa_pool2 HANDLER lru_pool_handler SIZE '8388608';
CREATE TABLE t_numa2 (id int) WITH (buffer_pool = 'numa_pool2');
INSERT INTO t_numa2 SELECT generate_series(1, 500);
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_numa2;'),
	'500', 'recreated NUMA-interleaved pool works');
$node->safe_psql('postgres', 'DROP TABLE t_numa2; DROP BUFFER POOL numa_pool2;');

$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1', 'clean restart');

$node->stop;
done_testing();
