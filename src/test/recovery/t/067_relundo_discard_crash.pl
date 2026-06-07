# Copyright (c) 2026, PostgreSQL Global Development Group

# Crash-recovery test for per-relation UNDO fork discard (RelUndoDiscard).
#
# RECNO writes a before-image UNDO record into the relation's "relundo" fork
# for every in-place UPDATE/DELETE.  VACUUM discards fork pages whose records
# are all older than the cluster-wide removable horizon by splicing the
# discardable run directly onto the metapage's free list, fully
# WAL-logged via XLOG_RELUNDO_DISCARD (metapage + run-tail + new-live-tail).
#
# What we verify, in order:
#
#   1.  A RECNO table that takes many committed in-place UPDATEs grows its
#       relundo fork across multiple pages.
#   2.  VACUUM, with no concurrent reader pinning an old snapshot, discards
#       the now-unneeded fork pages (the fork stops growing without bound).
#   3.  An immediate crash right after the discard still produces a correct,
#       consistent database after restart: committed data is intact and the
#       fork chain / free list survive replay (no PANIC in
#       relundo_redo_discard).
#   4.  Rollback still works after a post-discard crash: a fresh aborted
#       UPDATE must restore the prior committed value (the fork's live chain
#       was not corrupted by the splice + replay).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('relundo_discard');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
autovacuum = off
));
$node->start;

# -----------------------------------------------------------------------
# Step 1: build a RECNO table and grow its relundo fork with many
# committed in-place UPDATEs.  Each UPDATE writes a before-image UNDO
# record into the fork; thousands of them push the fork past one page.
# -----------------------------------------------------------------------
$node->safe_psql(
	'postgres', q{
	CREATE TABLE r (id int PRIMARY KEY, n bigint, pad text) USING recno;
	INSERT INTO r SELECT g, 0, repeat('x', 200)
	  FROM generate_series(1, 200) g;
});

# Drive a large number of committed UPDATEs to accumulate fork pages.
$node->safe_psql(
	'postgres', q{
	DO $$
	BEGIN
	  FOR i IN 1..50 LOOP
	    UPDATE r SET n = n + 1;
	  END LOOP;
	END $$;
});

my $fork_before = $node->safe_psql('postgres',
	"SELECT pg_relation_size('r', 'relundo')");
ok($fork_before > 0, "relundo fork is non-empty after many UPDATEs ($fork_before bytes)");

# -----------------------------------------------------------------------
# Step 2: VACUUM discards fork pages older than the removable horizon.
# With no concurrent old snapshot, every accumulated before-image is past
# the horizon, so the discardable run covers (nearly) the whole chain.
# -----------------------------------------------------------------------
$node->safe_psql('postgres', "VACUUM r");

my $committed_after_vacuum = $node->safe_psql('postgres', "SELECT count(*) FROM r");
is($committed_after_vacuum, '200', 'committed rows intact after VACUUM discard');

# The fork must reach a flat plateau, not merely be "bounded".  Each
# UPDATE+VACUUM cycle discards the prior cycle's before-image pages and
# returns them to the fork's free list, where the next cycle reuses them
# instead of extending the fork.  So once the working set is allocated, the
# fork size must stop growing entirely.  Run several identical cycles and
# require the size to be exactly stable across the last few -- any per-cycle
# growth means pages are not being reclaimed/reused (the bloat regression
# this machinery exists to prevent).
my @sizes;
for my $cycle (1 .. 4)
{
	$node->safe_psql(
		'postgres', q{
		DO $$
		BEGIN
		  FOR i IN 1..50 LOOP
		    UPDATE r SET n = n + 1;
		  END LOOP;
		END $$;
	});
	$node->safe_psql('postgres', "VACUUM r");
	push @sizes,
	  $node->safe_psql('postgres', "SELECT pg_relation_size('r', 'relundo')");
}

# The last three cycles must be byte-for-byte identical: a stable plateau
# proving pages are discarded and reused rather than newly extended.
is($sizes[3], $sizes[1],
	"relundo fork at a flat plateau across UPDATE+VACUUM cycles "
	  . "(sizes: @sizes bytes)");
is($sizes[2], $sizes[1],
	"relundo fork stops growing after first reclaim cycle");

my $fork_after = $sizes[3];

# -----------------------------------------------------------------------
# Step 3: crash immediately after discard, then restart.  This exercises
# replay of XLOG_RELUNDO_DISCARD against an on-disk metapage / fork chain.
# -----------------------------------------------------------------------
$node->stop('immediate');
$node->start;

my $post_crash = $node->safe_psql('postgres', "SELECT count(*) FROM r");
is($post_crash, '200', 'post-crash: committed rows intact after discard replay');

my $post_crash_sum = $node->safe_psql('postgres', "SELECT sum(n) FROM r");
is($post_crash_sum, '50000',
	'post-crash: UPDATE values intact (200 rows * 250 increments)');

# -----------------------------------------------------------------------
# Step 4: rollback still works after the post-discard crash.  If the splice
# or its replay corrupted the live fork chain, before-image restore would
# fail and the aborted value would leak (or the row would vanish).
# -----------------------------------------------------------------------
$node->psql(
	'postgres', q{
	BEGIN;
	UPDATE r SET n = -999 WHERE id = 1;
	ROLLBACK;
});
my $rolled_back = $node->safe_psql('postgres',
	"SELECT n FROM r WHERE id = 1");
is($rolled_back, '250',
	'post-discard rollback restores committed value (fork live chain intact)');

# A fresh VACUUM after the crash must also succeed (chain still walkable).
$node->safe_psql('postgres', "VACUUM r");
my $final = $node->safe_psql('postgres', "SELECT count(*), sum(n) FROM r");
is($final, "200|50000", 'post-crash VACUUM succeeds; data unchanged');

$node->stop;

done_testing();
