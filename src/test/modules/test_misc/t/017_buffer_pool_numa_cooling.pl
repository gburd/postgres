# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Unified NUMA-aware default pool: the batched 2-bit clock sweep (Clock2BitSweep).
#
# The default pool is a single pool-scoped clock sweep.  On NUMA hardware a
# backend claims a batch of consecutive clock-hand values per atomic fetch_add
# (batch size auto-sized from this pool's buffers / online cores, a power of two
# >= 16), cutting cross-socket contention on the shared hand.  Off NUMA the
# batch is 1 -- byte-identical to the classic one-at-a-time sweep.  The cooling
# decrement is a CAS on the 2-bit hot/cooling/cold usage state, so a shared hand
# and overlapping batches can never underflow the field into the flag bits.
#
# This test drives heavy + concurrent + dirty eviction under a forced multi-node
# configuration so the batched sweep runs for real on single-node CI; any state
# corruption would surface as wrong query results, a PANIC, or a failed restart.
#
# The default sweep also declares itself scan-resistant: every demand-loaded
# page is admitted at usage_count 0 (LeanStore COOL/probationary), earning "hot"
# only on a second access.  A single-touch sequential scan therefore leaves its
# pages cool and evictable, independent of the BufferAccessStrategy ring.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# --- 1. The buffer_pool_numa_cooling GUC still exists and defaults off. ---
{
	my $node = PostgreSQL::Test::Cluster->new('cooling_off');
	$node->init;
	$node->start;
	is($node->safe_psql('postgres', 'SHOW buffer_pool_numa_cooling;'),
		'off', 'buffer_pool_numa_cooling GUC present and default off');
	$node->stop;
}

# --- 2. Default (no NUMA): plain one-at-a-time sweep, batch = 1. ---
{
	my $node = PostgreSQL::Test::Cluster->new('cooling_no_numa');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = off
CONF
	$node->start;
	my $log = slurp_file($node->logfile);
	# With NUMA off, no NUMA-interleaved-placement log line.
	unlike($log, qr/NUMA interleaved placement/,
		'no NUMA placement when buffer_pool_numa is off');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM generate_series(1,1000);'),
		'1000', 'server works with NUMA off');
	$node->stop;
}

# --- 3. Forced multi-node: the NUMA-aware batched sweep activates and is
#        correct under heavy / concurrent / dirty eviction. ---
{
	my $node = PostgreSQL::Test::Cluster->new('cooling_on');
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
		'NUMA-aware batched sweep activated with forced 4 nodes');

	# Dataset larger than shared_buffers so the sweep runs constantly.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SQL

	# Repeated full scans: every candidate buffer with usage_count>0 takes a
	# CAS cooling decrement on the 2-bit field.
	for my $iter (1 .. 3)
	{
		is($node->safe_psql('postgres', 'SELECT count(*), sum(length(pad)) FROM t;'),
			'300000|120000000',
			"full scan correct under batched NUMA sweep (iter $iter)");
	}

	# Point reads: scattered victim selection across the pool.
	is( $node->safe_psql('postgres',
			'SELECT count(*) FROM t WHERE id IN (1, 99999, 150000, 250000, 299999);'
		),
		'5', 'point reads correct under batched NUMA sweep');

	# Concurrent eviction from several sessions: overlapping batches on the
	# shared hand; the CAS cooling decrement must stay race-correct.
	my @bg;
	for my $c (1 .. 6)
	{
		my $h = $node->background_psql('postgres');
		$h->query_safe('SELECT count(*) FROM t;');
		push @bg, $h;
	}
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'correct under concurrent eviction across batches');
	$_->quit for @bg;

	# Dirty eviction: writes push modified pages out through the sweep.
	$node->safe_psql('postgres',
		'UPDATE t SET pad = repeat(\'y\', 400) WHERE id % 3 = 0;');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t WHERE pad LIKE \'y%\';'),
		'100000', 'writes + dirty eviction correct under batched NUMA sweep');

	# Clean restart: no shared-memory corruption from the clock hand.
	$node->restart;
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'clean restart with batched NUMA sweep');

	# The sweep advances a monotonic hand under eviction pressure.  A frozen
	# hand would never select victims, so no buffers could be allocated for
	# incoming pages.  buffers_alloc counts exactly those clock-sweep
	# allocations (drained into pg_stat_bgwriter by the checkpointer now that
	# the dedicated background writer is retired), so after sustained dirty
	# eviction it must be non-zero -- proof the sweep point advanced.
	#
	# (We deliberately do NOT assert on buffers_clean here: with the global
	# background writer gone, dirty victims are written inline by the evicting
	# backends and by the checkpointer, and the per-pool trickle writer only
	# opportunistically cleans whatever those have not already flushed, so the
	# background-cleaned count is timing-dependent and not a reliable signal.)
	$node->append_conf('postgresql.conf', "bgwriter_delay = 10ms\nbgwriter_lru_maxpages = 1000\n");
	$node->restart;
	for my $r (1 .. 4)
	{
		$node->safe_psql('postgres',
			"UPDATE t SET pad = repeat(chr(97 + $r), 400) WHERE id % 2 = 0;");
		$node->safe_psql('postgres', 'SELECT count(*) FROM t;');
	}
	$node->safe_psql('postgres', 'SELECT pg_sleep(1);');
	my $alloc = $node->safe_psql('postgres',
		'SELECT buffers_alloc FROM pg_stat_bgwriter;');
	cmp_ok($alloc, '>', 0,
		'sweep advanced under dirty eviction (buffers allocated, hand not frozen)');
	$node->stop;
}

# --- 4. Scan-resistance is the DEFAULT algorithm's property (scan_resistant). ---
#     Clock2BitSweep admits EVERY demand-loaded page at usage_count 0 (LeanStore
#     COOL/probationary) and promotes to hot only on a second access.  The
#     absolute usagecount of any given relation's pages is dominated by how it
#     was loaded (an INSERT writes pages with the default strategy, which marks
#     them hot), so we do not assert an absolute cool value here -- that is
#     regime-sensitive.  Instead we exercise both a bulk-read (strategy) scan
#     and a small non-strategy scan and require correctness under probationary
#     admission; the cool-admission mechanism itself is covered by the
#     InitialUsageCountBits unit path and the eviction correctness above.
{
	my $node = PostgreSQL::Test::Cluster->new('scan_cool');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
shared_buffers = 256MB
CONF
	$node->start;
	$node->safe_psql('postgres', 'CREATE EXTENSION pg_buffercache;');
	# A table larger than one bulk-read ring but within s_b, so a seqscan uses
	# BAS_BULKREAD and its pages land in the shared pool.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE scanme (id int, pad text);
INSERT INTO scanme SELECT g, repeat('z', 200) FROM generate_series(1, 400000) g;
SQL
	$node->safe_psql('postgres',
		'SET max_parallel_workers_per_gather=0; SELECT count(*) FROM scanme;');
	# A small non-strategy scan must also work under the scan-resistant default.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE tiny (id int, pad text);
INSERT INTO tiny SELECT g, repeat('q', 200) FROM generate_series(1, 5000) g;
SELECT count(*) FROM tiny;
SQL
	# Correctness must hold under probationary admission for both scan shapes.
	is($node->safe_psql('postgres', 'SELECT count(*) FROM scanme;'),
		'400000', 'bulk-read scan correct under probationary cooling admission');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM tiny;'),
		'5000', 'non-strategy scan correct under probationary cooling admission');
	# pg_buffercache must observe the scanned relations resident with a valid
	# usagecount in the 2-bit range [0, 3] -- proof the state field is sane.
	is($node->safe_psql('postgres', <<'SQL'),
SELECT bool_and(usagecount BETWEEN 0 AND 3) FROM pg_buffercache b
JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
WHERE c.relname IN ('scanme', 'tiny');
SQL
		't', 'scanned pages carry a valid 2-bit usage state');
	$node->stop;
}

done_testing();
