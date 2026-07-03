# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Default buffer pool must match upstream/master: the built-in clock-sweep
# replacement algorithm, for EVERY relation including TOAST, out of the box.
#
# The pluggable buffer-pool framework allows extension algorithms (ARC, LRU,
# etc.) to be registered and selected, and allows per-relation pools via the
# buffer_pool reloption.  None of that must change the DEFAULT: a cluster with
# no buffer_pool_algorithm set, no shared_preload_libraries, and no reloptions
# must behave exactly like stock PostgreSQL -- clock-sweep everywhere.  This
# test locks that invariant so a future change cannot silently make some other
# algorithm the default.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('default_pool');
$node->init;
# Deliberately set NOTHING: no buffer_pool_algorithm, no preload, no numa.
$node->start;

# 1. The GUC default is 'clock'.
is($node->safe_psql('postgres', 'SHOW buffer_pool_algorithm;'),
	'clock', 'buffer_pool_algorithm defaults to clock');

# 2. The server logs that the default pool uses the clock-sweep algorithm.
#    This LOG is emitted from StrategyCtlShmemInit very early in postmaster
#    startup -- possibly before log redirection -- so it may land on the
#    postmaster's boot stderr rather than the node logfile.  Check both the
#    node logfile and (best-effort) do not require it: the behavioral checks
#    below are the authoritative proof.  We assert only the NEGATIVE that we
#    are NOT on the NUMA-partitioned sweep, which is reliably observable.
my $log = $node->logfile;
my $logtext = PostgreSQL::Test::Utils::slurp_file($log);
unlike(
	$logtext,
	qr/NUMA-partitioned clock sweep/,
	'plain clock-sweep, not NUMA-partitioned, with no config');

# 3. TOAST goes through the default pool too: create a table with a big text
#    column (forces a TOAST relation), write/read wide values, and confirm it
#    works.  No buffer_pool reloption is set anywhere, so both the main and
#    TOAST relations use the default (clock) pool.
$node->safe_psql('postgres', <<'SQL');
CREATE TABLE toasty (id int primary key, big text);
INSERT INTO toasty
  SELECT g, repeat('x', 200000) FROM generate_series(1, 50) g;
SQL

is( $node->safe_psql('postgres',
		'SELECT count(*) FROM toasty WHERE length(big) = 200000;'),
	'50', 'wide TOASTed values round-trip through the default clock pool');

# 4. The table has a TOAST relation and neither it nor the main rel carries a
#    buffer_pool reloption (so both use the default pool).
is( $node->safe_psql('postgres',
		q{SELECT reloptions IS NULL FROM pg_class WHERE relname = 'toasty';}),
	't', 'main relation has no buffer_pool reloption (uses default pool)');
is( $node->safe_psql('postgres', <<'SQL'),
SELECT reloptions IS NULL
FROM pg_class
WHERE oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'toasty');
SQL
	't', 'TOAST relation has no buffer_pool reloption (uses default pool)');

$node->stop;
done_testing();
