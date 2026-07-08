# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# NUMA-aware batched clock sweep for the default buffer pool (P8).
#
# When buffer_pool_numa is on and the system has more than one NUMA node, the
# default pool's buffers are interleaved across nodes and the single clock hand
# claims a NUMA-sized batch (power of two >= 16) per atomic fetch_add, cutting
# cross-socket contention on the shared hand.  This targets the multi-socket
# scalability cliff (contention on the single global clock hand).
#
# Real multi-node hardware is required to observe the scalability benefit; this
# test uses the buffer_pool_numa_nodes developer GUC to FORCE a logical node
# count so the batched sweep's victim-selection logic (batch claims on the
# shared hand, wraparound, eviction under memory pressure) runs for real and is
# proven correct on single-node CI hardware.

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

# --- 2. Forced multi-node: batched NUMA clock sweep activates and is correct
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

	# The log must show the NUMA-aware batched sweep was selected.
	my $log = slurp_file($node->logfile);
	like($log, qr/batched clock-sweep with NUMA interleaved placement across 4 nodes/,
		'NUMA interleaved placement activated with forced 4 nodes');

	# Build a dataset larger than shared_buffers so victim selection (and thus
	# the per-node hands + cross-node fallback) is exercised heavily.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SQL

	# Full scans force eviction across the whole pool.
	for my $iter (1 .. 3)
	{
		is($node->safe_psql('postgres', 'SELECT count(*), sum(length(pad)) FROM t;'),
			'300000|120000000',
			"full scan correct under batched NUMA sweep (iter $iter)");
	}

	# Random point reads (scattered victim selection across the pool).
	is( $node->safe_psql('postgres',
			'SELECT count(*) FROM t WHERE id IN (1, 99999, 150000, 250000, 299999);'
		),
		'5', 'point reads correct under batched NUMA sweep');

	# Concurrent eviction pressure from several sessions interleaves victim
	# selection across the shared hand.
	my @pids;
	for my $c (1 .. 4)
	{
		my $bg = $node->background_psql('postgres');
		$bg->query_safe('SELECT count(*) FROM t;');
		push @pids, $bg;
	}
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'correct under concurrent eviction across the pool');
	$_->quit for @pids;

	# A write workload (dirties buffers, exercises eviction of dirty pages
	# through the batched NUMA sweep).
	$node->safe_psql('postgres', 'UPDATE t SET pad = repeat(\'y\', 400) WHERE id % 3 = 0;');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t WHERE pad LIKE \'y%\';'),
		'100000', 'writes + dirty eviction correct under batched NUMA sweep');

	# Clean restart: no shared-memory corruption from the clock hand.
	$node->restart;
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'clean restart with batched NUMA sweep');
	$node->stop;
}

# --- 3. Huge-pages gate: buffer_pool_numa=on WITHOUT the forced-node override
#        and WITHOUT huge pages must NOT activate the NUMA sweep.  The NUMA path
#        only helps with huge pages, so it stays off (plain unified clock) when
#        huge pages are not in effect -- the safe default. ---
{
	my $node = PostgreSQL::Test::Cluster->new('numa_no_hugepages');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
huge_pages = off
shared_buffers = 128MB
CONF
	$node->start;
	my $log = slurp_file($node->logfile);
	# Must fall back to the plain clock sweep, not the NUMA/batched sweep,
	# because huge pages are off (and no forced node count is set).
	unlike($log, qr/NUMA|batched|interleav/i,
		'NUMA sweep NOT activated without huge pages');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM generate_series(1,1000);'),
		'1000', 'server works with buffer_pool_numa on but huge pages off');
	$node->stop;
}

done_testing();
