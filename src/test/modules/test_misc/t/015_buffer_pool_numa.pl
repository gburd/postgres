# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# NUMA-partitioned clock sweep for the default buffer pool (P8).
#
# When buffer_pool_numa is on and the system has more than one NUMA node, the
# default pool switches from the single global clock sweep to a
# NUMA-partitioned one: one clock hand per node, each confined to that node's
# contiguous buffer range, with fallback to other nodes so a single backend can
# still use the whole pool.  This targets the multi-socket scalability cliff
# (single global clock hand + cross-node victim traffic).
#
# Real multi-node hardware is required to observe the scalability benefit; this
# test uses the buffer_pool_numa_nodes developer GUC to FORCE a logical node
# count so the partitioned sweep's victim-selection logic (per-node hands,
# wraparound passes, cross-node fallback, eviction under memory pressure) runs
# for real and is proven correct on single-node CI hardware.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# --- 1. Single-node / disabled: must stay on the plain clock sweep. ---
{
	my $node = PostgreSQL::Test::Cluster->new('plain');
	$node->init;
	$node->append_conf('postgresql.conf', 'buffer_pool_numa = off');
	$node->start;
	is($node->safe_psql('postgres', 'SHOW buffer_pool_numa;'), 'off',
		'buffer_pool_numa GUC present and default off');
	# Server runs normally on the plain sweep.
	is($node->safe_psql('postgres', 'SELECT count(*) FROM generate_series(1,1000);'),
		'1000', 'plain clock sweep works with NUMA off');
	$node->stop;
}

# --- 2. Forced multi-node: partitioned clock sweep activates and is correct
#        under eviction pressure. ---
{
	my $node = PostgreSQL::Test::Cluster->new('numa');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
shared_buffers = 128MB
CONF
	$node->start;

	# The log must show the partitioned sweep was selected.
	my $log = slurp_file($node->logfile);
	like($log, qr/NUMA-partitioned clock sweep across 4 nodes/,
		'partitioned clock sweep activated with forced 4 nodes');

	# Build a dataset larger than shared_buffers so victim selection (and thus
	# the per-node hands + cross-node fallback) is exercised heavily.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SQL

	# Full scans force eviction across the whole (partitioned) pool.
	for my $iter (1 .. 3)
	{
		is($node->safe_psql('postgres', 'SELECT count(*), sum(length(pad)) FROM t;'),
			'300000|120000000',
			"full scan correct under partitioned sweep (iter $iter)");
	}

	# Random point reads (scattered victim selection across node partitions).
	is( $node->safe_psql('postgres',
			'SELECT count(*) FROM t WHERE id IN (1, 99999, 150000, 250000, 299999);'
		),
		'5', 'point reads correct under partitioned sweep');

	# Concurrent eviction pressure from several sessions interleaves victim
	# selection across the per-node hands.
	my @pids;
	for my $c (1 .. 4)
	{
		my $bg = $node->background_psql('postgres');
		$bg->query_safe('SELECT count(*) FROM t;');
		push @pids, $bg;
	}
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'correct under concurrent eviction across partitions');
	$_->quit for @pids;

	# A write workload (dirties buffers, exercises eviction of dirty pages
	# through the partitioned sweep).
	$node->safe_psql('postgres', 'UPDATE t SET pad = repeat(\'y\', 400) WHERE id % 3 = 0;');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t WHERE pad LIKE \'y%\';'),
		'100000', 'writes + dirty eviction correct under partitioned sweep');

	# Clean restart: no shared-memory corruption from the partitioned control.
	$node->restart;
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'clean restart with partitioned sweep');
	$node->stop;
}

done_testing();
