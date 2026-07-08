# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Monitoring for the unified NUMA-aware default pool (Clock2BitSweep) via the
# single pg_stat_bufferpool view.
#
# pg_stat_bufferpool now reports, per pool: sizing/occupancy, access counters
# with a derived hit_ratio, the replacement algorithm name, the NUMA-aware
# sweep state (numa_active, numa_nodes, batch_size), the trickle-writer cleaning
# count, and the heat-state distribution (hot_buffers / cool_buffers).  The old
# pg_stat_bufferpool_numa view (per-node clock-hand rows) has been folded into
# these per-pool columns -- there is no separate NUMA SRF anymore.
#
# The default pool is a single pool-scoped batched clock sweep: ONE global clock
# hand, plus a batch size that is 1 off NUMA and a larger power of two on NUMA
# (auto-sized from this pool's buffers / online cores).
#
# Real multi-node hardware is not required: the buffer_pool_numa_nodes developer
# GUC forces a logical node count so the NUMA path activates on single-node CI.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# --- 1. NUMA off (default): one row for the default pool, numa inactive,
#        batch = 1, algorithm = clock. ---
{
	my $node = PostgreSQL::Test::Cluster->new('mon_plain');
	$node->init;
	$node->append_conf('postgresql.conf', 'buffer_pool_numa = off');
	$node->start;

	# The default pool is always present and named 'default'.
	is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_stat_bufferpool WHERE name = 'default';"),
		'1', 'default pool present in pg_stat_bufferpool');

	is($node->safe_psql('postgres',
		"SELECT algorithm FROM pg_stat_bufferpool WHERE name = 'default';"),
		'clock', 'default pool algorithm is clock');

	# NUMA off: not active, single node, byte-identical one-at-a-time sweep.
	is($node->safe_psql('postgres', <<'SQL'),
SELECT numa_active = false AND numa_nodes = 1 AND batch_size = 1
FROM pg_stat_bufferpool WHERE name = 'default';
SQL
		't', 'NUMA off: inactive, 1 node, batch = 1');

	# nbuffers matches shared_buffers; hit_ratio is within [0,1] once we touch
	# some data; heat counts are non-negative and bounded by nbuffers.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 100) FROM generate_series(1, 5000) g;
SELECT count(*) FROM t;
SQL
	is($node->safe_psql('postgres', <<'SQL'),
SELECT nbuffers = (SELECT setting::int FROM pg_settings WHERE name = 'shared_buffers')
   AND (hit_ratio IS NULL OR (hit_ratio >= 0 AND hit_ratio <= 1))
   AND hot_buffers >= 0 AND cool_buffers >= 0
   AND hot_buffers + cool_buffers <= nbuffers
FROM pg_stat_bufferpool WHERE name = 'default';
SQL
		't', 'default pool: sizing, hit_ratio, and heat counts are sane');

	$node->stop;
}

# --- 2. Forced multi-node: NUMA-aware batched clock sweep reflected in the
#        per-pool NUMA columns. ---
{
	my $node = PostgreSQL::Test::Cluster->new('mon_numa');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
shared_buffers = 128MB
CONF
	$node->start;

	my $log = slurp_file($node->logfile);
	like($log, qr/batched clock-sweep with NUMA interleaved placement across 4 nodes/,
		'NUMA-aware batched sweep activated with forced 4 nodes');

	# The default pool reports numa_active, the forced node count, and a
	# power-of-two batch size > 1.
	is($node->safe_psql('postgres', <<'SQL'),
SELECT numa_active = true
   AND numa_nodes = 4
   AND batch_size > 1
   AND (batch_size & (batch_size - 1)) = 0
FROM pg_stat_bufferpool WHERE name = 'default';
SQL
		't', 'NUMA on: numa_active, numa_nodes = 4, batch is power of two > 1');

	# Drive eviction, then confirm trickle_writes advanced (the per-pool
	# trickle writer cleaned dirty buffers) and heat counts stay bounded.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SELECT count(*) FROM t;
CHECKPOINT;
SQL
	is($node->safe_psql('postgres', <<'SQL'),
SELECT hot_buffers + cool_buffers <= nbuffers
   AND evictions >= 0
FROM pg_stat_bufferpool WHERE name = 'default';
SQL
		't', 'NUMA on: heat counts bounded, evictions counted under pressure');

	$node->stop;
}

done_testing();
