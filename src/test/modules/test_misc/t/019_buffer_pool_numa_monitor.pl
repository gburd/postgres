# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Monitoring for the unified NUMA-aware default pool (Clock2BitSweep).
#
# pg_stat_bufferpool predates the NUMA work and shows nothing about node
# placement or the clock hand.  pg_stat_bufferpool_numa (backed by
# pg_stat_get_bufferpool_numa()) exposes, per NUMA node: the buffers placed on
# that node's range, the clock-hand position, completed clock passes, and the
# per-pool clock-sweep batch size (the "stripe" column).
#
# The default pool is now a single pool-scoped batched clock sweep: ONE global
# clock hand, plus a batch size that is 1 off NUMA and a larger power of two on
# NUMA (auto-sized from this pool's buffers / online cores).  There is no longer
# a separate node-partitioned or "cooling" victim routine.
#
# Real multi-node hardware is not required: the buffer_pool_numa_nodes
# developer GUC forces a logical node count so the per-node rows are produced
# on single-node CI.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# --- 1. NUMA off (default): exactly one synthetic node-0 row, batch = 1. ---
{
	my $node = PostgreSQL::Test::Cluster->new('mon_plain');
	$node->init;
	$node->append_conf('postgresql.conf', 'buffer_pool_numa = off');
	$node->start;

	is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_bufferpool_numa;'),
		'1', 'NUMA off: single row in pg_stat_bufferpool_numa');

	is($node->safe_psql('postgres',
		'SELECT node FROM pg_stat_bufferpool_numa;'),
		'0', 'NUMA off: row is node 0');

	# Off NUMA the batch size (stripe) is 1: byte-identical to the classic
	# one-at-a-time clock sweep.
	is($node->safe_psql('postgres',
		'SELECT stripe FROM pg_stat_bufferpool_numa;'),
		'1', 'NUMA off: batch size (stripe) is 1');

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

# --- 2. Forced multi-node: NUMA-aware batched clock sweep. ---
#     With buffer_pool_numa on, buffers are interleaved across nodes and the
#     single clock hand claims a NUMA-sized batch per fetch_add.  The monitor
#     reports one row per node (that node's range) with the batch as stripe.
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

	# One row per node.
	is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_bufferpool_numa;'),
		'4', 'NUMA on: one row per node');

	# The batch (stripe) is a power of two > 1 on NUMA, reported in every row.
	is($node->safe_psql('postgres',
		'SELECT bool_and(stripe > 1) FROM pg_stat_bufferpool_numa;'),
		't', 'NUMA on: stripe column reports batch size (> 1)');

	# Drive eviction, then confirm the hand advanced or a pass completed.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SELECT count(*) FROM t;
SQL
	is($node->safe_psql('postgres',
		'SELECT bool_or(clock_hand > 0 OR complete_passes > 0) FROM pg_stat_bufferpool_numa;'),
		't', 'NUMA on: sweep advanced under eviction pressure');

	$node->stop;
}

done_testing();
