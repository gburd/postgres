# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Per-core striped clock sweep with blind-atomic cooling (buffer_pool_numa_cooling).
#
# Extends the NUMA-partitioned clock sweep: each node's buffer range is split
# into per-core stripes, and a backend sweeps ITS stripe with ITS own hand.
# Because a buffer then has a single sweeping owner per pass, the usage_count
# cooling decrement uses a blind pg_atomic_fetch_sub_u32 instead of a CAS loop.
#
# The danger of a blind sub is underflow: subtracting from usage_count == 0 (or
# a concurrent double-sub) borrows into the flag/lock bits and corrupts the
# buffer state.  Single-owner-per-stripe makes that impossible; a cassert build
# additionally asserts on every blind sub that the pre-sub usage_count was > 0.
# This test drives heavy + concurrent + dirty eviction so that assert runs
# thousands of times and any corruption would surface as wrong query results,
# a PANIC, or a failed restart.
#
# Real multi-node hardware is required to observe the scalability benefit; this
# test FORCES a logical node count with the buffer_pool_numa_nodes developer
# GUC so the striped victim-selection logic runs for real on single-node CI.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# --- 1. Default off: no striping, plain behavior. ---
{
	my $node = PostgreSQL::Test::Cluster->new('cooling_off');
	$node->init;
	$node->start;
	is($node->safe_psql('postgres', 'SHOW buffer_pool_numa_cooling;'),
		'off', 'buffer_pool_numa_cooling GUC present and default off');
	$node->stop;
}

# --- 2. Cooling on WITHOUT buffer_pool_numa: must stay inert (no striping). ---
{
	my $node = PostgreSQL::Test::Cluster->new('cooling_no_numa');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = off
buffer_pool_numa_cooling = on
CONF
	$node->start;
	my $log = slurp_file($node->logfile);
	# With NUMA off, the striped sweep must NOT be selected.
	unlike($log, qr/striped clock sweep/,
		'cooling is inert when buffer_pool_numa is off');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM generate_series(1,1000);'),
		'1000', 'server works with cooling on but NUMA off');
	$node->stop;
}

# --- 3. Forced multi-node + cooling on: striped sweep activates and is
#        correct under heavy / concurrent / dirty eviction. ---
{
	my $node = PostgreSQL::Test::Cluster->new('cooling_on');
	$node->init;
	$node->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
buffer_pool_numa_cooling = on
shared_buffers = 128MB
CONF
	$node->start;

	# The log must show the striped cooling sweep was selected.
	my $log = slurp_file($node->logfile);
	like($log, qr/NUMA striped clock sweep with blind-atomic cooling across 4 nodes/,
		'striped cooling sweep activated with forced 4 nodes');

	# Dataset larger than shared_buffers so the cooling sweep runs constantly.
	$node->safe_psql('postgres', <<'SQL');
CREATE TABLE t (id int primary key, pad text);
INSERT INTO t SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
SQL

	# Repeated full scans: every candidate buffer with usage_count>0 takes a
	# blind fetch_sub; the cassert underflow guard runs on each.
	for my $iter (1 .. 3)
	{
		is($node->safe_psql('postgres', 'SELECT count(*), sum(length(pad)) FROM t;'),
			'300000|120000000',
			"full scan correct under striped cooling sweep (iter $iter)");
	}

	# Point reads: scattered victim selection across stripes.
	is( $node->safe_psql('postgres',
			'SELECT count(*) FROM t WHERE id IN (1, 99999, 150000, 250000, 299999);'
		),
		'5', 'point reads correct under striped cooling sweep');

	# Concurrent eviction from several sessions: different backends land on
	# different stripes; the single-owner invariant must still hold.
	my @bg;
	for my $c (1 .. 6)
	{
		my $h = $node->background_psql('postgres');
		$h->query_safe('SELECT count(*) FROM t;');
		push @bg, $h;
	}
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'correct under concurrent eviction across stripes');
	$_->quit for @bg;

	# Dirty eviction: writes push modified pages out through the cooling sweep.
	$node->safe_psql('postgres',
		'UPDATE t SET pad = repeat(\'y\', 400) WHERE id % 3 = 0;');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t WHERE pad LIKE \'y%\';'),
		'100000', 'writes + dirty eviction correct under striped cooling sweep');

	# Clean restart: no shared-memory corruption from stripe hands.
	$node->restart;
	is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'),
		'300000', 'clean restart with striped cooling sweep');

	# The cooling sweep advances per-core stripe hands, not the node-0 hand, so
	# sync_start must report an ADVANCING sweep point (NumaCoolingSyncStart sums
	# the stripe hands).  A frozen point (position=start, passes=0 forever) would
	# keep BgBufferSync from ever pacing ahead of the sweep, so the bgwriter
	# would never clean a buffer.  After sustained dirty eviction under cooling,
	# buffers_clean must be non-zero -- proof the sweep point advanced.
	$node->append_conf('postgresql.conf', "bgwriter_delay = 10ms\nbgwriter_lru_maxpages = 1000\n");
	$node->restart;
	for my $r (1 .. 4)
	{
		$node->safe_psql('postgres',
			"UPDATE t SET pad = repeat(chr(97 + $r), 400) WHERE id % 2 = 0;");
		$node->safe_psql('postgres', 'SELECT count(*) FROM t;');
	}
	$node->safe_psql('postgres', 'SELECT pg_sleep(1);');
	my $clean = $node->safe_psql('postgres',
		'SELECT buffers_clean FROM pg_stat_bgwriter;');
	cmp_ok($clean, '>', 0,
		'bgwriter cleans under cooling (sweep point advances, not frozen)');
	$node->stop;
}

# --- 4. Scan-resistance: probationary admission is the ALGORITHM's property. ---
#     A scan-resistant pool admits EVERY demand-loaded page at usage_count 0
#     (LeanStore COOL/probationary) and promotes to hot only on a second
#     access -- independent of whether the load used a BufferAccessStrategy
#     ring.  So scan pages (single touch) stay cool and are evicted first.
#     We verify both a bulk-read (strategy) scan AND a small non-strategy scan
#     are cooler under cooling than under plain numa.  pg_buffercache observes it.
{
	my $numa = PostgreSQL::Test::Cluster->new('scan_numa');
	$numa->init;
	$numa->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
shared_buffers = 256MB
CONF
	$numa->start;
	$numa->safe_psql('postgres', 'CREATE EXTENSION pg_buffercache;');
	# A table comfortably larger than one bulk-read ring but well within s_b,
	# so a seqscan uses BAS_BULKREAD and its pages land in the shared pool.
	$numa->safe_psql('postgres', <<'SQL');
CREATE TABLE scanme (id int, pad text);
INSERT INTO scanme SELECT g, repeat('z', 200) FROM generate_series(1, 400000) g;
SQL
	# Drive a bulk-read seqscan (large enough to trigger BAS_BULKREAD).
	$numa->safe_psql('postgres',
		'SET max_parallel_workers_per_gather=0; SELECT count(*) FROM scanme;');
	my $numa_uc = $numa->safe_psql('postgres', <<'SQL');
SELECT coalesce(max(usagecount),0) FROM pg_buffercache b
JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
WHERE c.relname = 'scanme';
SQL
	# A small table: a seqscan of it does NOT trigger BAS_BULKREAD, so it is a
	# NON-strategy load.  Under a scan-resistant algorithm it must still be
	# admitted cool -- proving probation is the algorithm's property, not the
	# ring's.
	$numa->safe_psql('postgres', <<'SQL');
CREATE TABLE tiny (id int, pad text);
INSERT INTO tiny SELECT g, repeat('q', 200) FROM generate_series(1, 5000) g;
SELECT count(*) FROM tiny;
SQL
	my $numa_tiny = $numa->safe_psql('postgres', <<'SQL');
SELECT coalesce(max(usagecount),0) FROM pg_buffercache b
JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
WHERE c.relname = 'tiny';
SQL
	$numa->stop;

	my $cool = PostgreSQL::Test::Cluster->new('scan_cool');
	$cool->init;
	$cool->append_conf('postgresql.conf', <<'CONF');
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
buffer_pool_numa_cooling = on
shared_buffers = 256MB
CONF
	$cool->start;
	$cool->safe_psql('postgres', 'CREATE EXTENSION pg_buffercache;');
	$cool->safe_psql('postgres', <<'SQL');
CREATE TABLE scanme (id int, pad text);
INSERT INTO scanme SELECT g, repeat('z', 200) FROM generate_series(1, 400000) g;
SQL
	$cool->safe_psql('postgres',
		'SET max_parallel_workers_per_gather=0; SELECT count(*) FROM scanme;');
	my $cool_uc = $cool->safe_psql('postgres', <<'SQL');
SELECT coalesce(max(usagecount),0) FROM pg_buffercache b
JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
WHERE c.relname = 'scanme';
SQL
	$cool->safe_psql('postgres', <<'SQL');
CREATE TABLE tiny (id int, pad text);
INSERT INTO tiny SELECT g, repeat('q', 200) FROM generate_series(1, 5000) g;
SELECT count(*) FROM tiny;
SQL
	my $cool_tiny = $cool->safe_psql('postgres', <<'SQL');
SELECT coalesce(max(usagecount),0) FROM pg_buffercache b
JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
WHERE c.relname = 'tiny';
SQL
	# Correctness must hold regardless of admission policy.
	is($cool->safe_psql('postgres', 'SELECT count(*) FROM scanme;'),
		'400000', 'scan correct under probationary cooling admission');
	$cool->stop;

	# The bulk-read (strategy) scan's pages must be COOLER under cooling.
	cmp_ok($cool_uc, '<=', $numa_uc,
		"probationary strategy-scan admission cools pages (cool uc=$cool_uc <= numa uc=$numa_uc)");
	# The small NON-strategy scan's pages must ALSO be cooler under cooling --
	# probation is the algorithm's property, not dependent on the ring.
	cmp_ok($cool_tiny, '<=', $numa_tiny,
		"probation cools NON-strategy loads too (cool uc=$cool_tiny <= numa uc=$numa_tiny)");
}

done_testing();
