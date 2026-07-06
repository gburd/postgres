# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Monitoring for the NUMA-partitioned / striped-cooling default pool (R4).
#
# pg_stat_bufferpool predates the NUMA work and shows nothing about node
# partitioning or per-stripe clock hands.  pg_stat_bufferpool_numa (backed by
# pg_stat_get_bufferpool_numa()) exposes, per NUMA node (and per stripe when
# cooling is on): the buffers owned by that range, the clock-hand position,
# and completed clock passes.
#
# Real multi-node hardware is not required: the buffer_pool_numa_nodes
# developer GUC forces a logical node count so the per-node/per-stripe rows
# are produced on single-node CI.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# --- 1. NUMA off (default): exactly one synthetic node-0/stripe-0 row. ---
{
	my $node = PostgreSQL::Test::Cluster->new('mon_plain');
	$node->init;
	$node->append_conf('postgresql.conf', 'buffer_pool_numa = off');
	$node->start;

	is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_bufferpool_numa;'),
		'1', 'NUMA off: single row in pg_stat_bufferpool_numa');

	is($node->safe_psql('postgres',
		'SELECT node, stripe FROM pg_stat_bufferpool_numa;'),
		'0|0', 'NUMA off: row is node 0 / stripe 0');

	# nbuffers must match shared_buffers (whole pool on the one node), and
	# the hand must be inside the pool.
	is($node->safe_psql('postgres', <<'SQL'),
SELECT nbuffers = (SELECT setting::int FROM pg_settings WHERE name = 'shared_buffers')
   AND clock_hand >= 0 AND clock_hand < nbuffers
FROM pg_stat_bufferpool_numa;
SQL
		't', 'NUMA off: nbuffers = shared_buffers, hand within pool');

	$node->stop;
}

# --- 2. Forced multi-node, cooling off: plain clock + NUMA placement. ---
#     With cooling off, buffer_pool_numa only interleaves placement; victim
#     selection stays the unmodified global clock, so the view reports the
#     single global-hand row (the plain-clock shape).
{
	my $node = PostgreSQL::Test::Cluster->new('mon_numa');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_cooling = off
buffer_pool_numa_nodes = 4
shared_buffers = 128MB
CONF
	$node->start;

	my $log = slurp_file($node->logfile);
	like($log, qr/clock-sweep replacement algorithm with NUMA interleaved placement across 4 nodes/,
		'NUMA placement (cooling off) activated with forced 4 nodes');

	# Plain clock: a single global-hand row.
	is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_bufferpool_numa;'),
		'1', 'cooling off: single global-hand row');
	is($node->safe_psql('postgres', <<'SQL'),
SELECT nbuffers = (SELECT setting::int FROM pg_settings WHERE name = 'shared_buffers')
FROM pg_stat_bufferpool_numa;
SQL
		't', 'cooling off: nbuffers = shared_buffers');

	# Drive eviction, then confirm the hand advanced or a pass completed.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SELECT count(*) FROM t;
SQL
	is($node->safe_psql('postgres',
		'SELECT clock_hand > 0 OR complete_passes > 0 FROM pg_stat_bufferpool_numa;'),
		't', 'cooling off: sweep advanced under eviction pressure');

	$node->stop;
}

# --- 3. Forced multi-node, cooling on: one row for the batched global hand. ---
{
	my $node = PostgreSQL::Test::Cluster->new('mon_cooling');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_cooling = on
buffer_pool_numa_nodes = 4
shared_buffers = 128MB
CONF
	$node->start;
	my $log = slurp_file($node->logfile);
	like($log, qr/batched global clock sweep with blind-atomic cooling/,
		'batched cooling sweep activated');

	# The batched sweep has ONE global hand over the whole pool, so the view
	# reports a single row; the stripe column carries the batch size.
	is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_bufferpool_numa;'),
		'1', 'cooling on: single row for the global batched hand');
	is($node->safe_psql('postgres',
		'SELECT stripe > 0 FROM pg_stat_bufferpool_numa;'),
		't', 'cooling on: stripe column reports batch size (> 0)');

	# Drive eviction, then the global hand must have advanced.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SELECT count(*) FROM t;
SQL
	is($node->safe_psql('postgres',
		'SELECT clock_hand > 0 OR complete_passes > 0 FROM pg_stat_bufferpool_numa;'),
		't', 'cooling on: global hand advanced under pressure');

	$node->stop;
}

done_testing();
