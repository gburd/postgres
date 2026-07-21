# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase C/D forced-migration correctness gate.
#
# Phase D spawns regular client-backend fibers migratable (work-stealable)
# across carrier loops; Phase C published each fiber's own current-work roots
# via xtc_proc userdata and armed the assert-only seam cross-check
# (XtcPgVerifyCurrentWorkIsSelf) + affine park-boundary tripwire.  This test is
# the deterministic local correctness gate for that flip.  It boots the threaded
# runtime with only two carrier loops and drives several concurrent CPU-bound
# sessions under load imbalance, then asserts the things that ARE provable and
# deterministic:
#
#   1. client backends actually spawn migratable=1 (the flip is LIVE) -- parsed
#      from the spawn diagnostic in the server log;
#   2. every session computes its OWN correct result under concurrent load --
#      a cross-fiber root leak (a stolen/interleaved fiber reading a sibling's
#      session/execution root: its temp table, CurrentPgExecution memory
#      context) would corrupt at least one session's deterministic accumulator;
#   3. no crash / assertion fired -- under a cassert build the Phase-C seam
#      cross-check and the affine park-boundary tripwire are ARMED, so a wrong
#      root repoint or an affine span parking would TRAP here;
#   4. the server stays usable afterward, with no crash signatures in the log.
#
# STEAL OBSERVATION IS BEST-EFFORT, NOT A GATE.  libxtc's work-steal heuristic
# only steals when a loop's run queue is fully drained AND it has no pending
# timer; libxtc forces that condition in its own migration proof
# (test/sim/test_sim_migratable.c, INV1/INV5) with a DETERMINISTIC-SIMULATOR
# pessimal-placement knob that a live (non-DST) PG run cannot use.  So an
# observed nonzero steal count under a natural local workload is timing-
# dependent and is NOT asserted here (it is logged for information).  The real
# steal-under-load proof is the EC2 A/B benchmark, where sustained many-session
# load naturally drives the idle-loop steal window.  The migratable=1 spawn
# proof + libxtc's DST proof of the migrated-resume path together establish that
# migration is live and safe; this test proves PG's roots stay fiber-correct
# under concurrent load with the tripwires armed.
#
# WORKLOAD NOTES:
#  - Connections are established SERIALLY, then the CPU phase runs concurrently
#    on the already-connected backends.  A simultaneous connection storm trips
#    a SEPARATE, pre-existing concurrent-startup fragility (client_encoding GUC
#    lookup, ThreadedGUCMutex) that reproduces PINNED too and is unrelated to
#    fiber migration.  Migration matters during query EXECUTION, so serial
#    connect loses no coverage.
#  - No GUC writes on the hot path (same pre-existing-fragility reason).
#    Session identity/correctness is DATA-based (a per-session temp table),
#    which exercises the migration root-repoint (CurrentPgExecution, session
#    temp namespace).

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Force a small carrier pool so runnable fibers must rebalance across loops.
$ENV{PG_XTC_CARRIER_LOOPS} = '2';

my $node = PostgreSQL::Test::Cluster->new('phase_cd_forced_migration');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 0
autovacuum = off
io_method = sync
summarize_wal = off
max_connections = 40
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'),
	'on', 'forced-migration gate starts threaded runtime');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

my $log_start = -s $node->logfile;

my $NSESS = 12;
my $ITERS = 120;

# Even-index sessions burn a long CPU loop, odd-index sessions a short one, to
# push load imbalance across the two loops (round-robin spawn places even sids
# on one loop) -- a best-effort nudge toward the steal window, not a
# correctness requirement.
sub iters_for { my ($sid) = @_; return ($sid % 2 == 0) ? $ITERS : 4; }

sub expected_for
{
	my ($sid) = @_;
	my $base = $sid * 1000;
	my $iters = iters_for($sid);
	my $sum = 0;
	$sum += ($base + $_) for (1 .. $iters);
	return $sum;
}

# Open the sessions one at a time (safe startup), each with its own temp
# accumulator.  Serial connect avoids the pre-existing concurrent-startup
# fragility; the migration-relevant concurrency comes during query execution.
my @sessions;
for my $sid (0 .. $NSESS - 1)
{
	my $s = $node->background_psql('postgres', timeout => 180);
	$s->query_safe(
		"CREATE TEMP TABLE mig_t(acc bigint); INSERT INTO mig_t VALUES (0);",
		verbose => 0);
	push @sessions, $s;
}

# Fire the CPU-bound accumulation on ALL sessions WITHOUT waiting.  Each
# iteration is its own statement (CPU burn folded into an accumulator update),
# so every iteration ends in a socket round-trip: the backend fiber PARKS on the
# client read, then WAKES and is re-enqueued on its loop's stealable deque.  The
# even/odd iteration split biases the load imbalance a steal needs.  A trailing
# sentinel row marks each session done.
my $sentinel = 'MIG_DONE_ROW';
for my $sid (0 .. $NSESS - 1)
{
	my $base = $sid * 1000;
	my $iters = iters_for($sid);
	my $sql = '';
	for my $i (1 .. $iters)
	{
		my $add = $base + $i;
		$sql .=
			"UPDATE mig_t SET acc = acc + $add"
			. " + (SELECT count(*) FROM (SELECT md5(g::text)"
			. " FROM generate_series(1, 3000) g) x) * 0;\n";
	}
	$sql .= "SELECT '$sentinel=' || acc FROM mig_t;\n";
	$sessions[$sid]->{stdin} .= $sql;
	$sessions[$sid]->{run}->pump_nb();
}

# Pump all sessions concurrently until each has emitted its sentinel row.
my $deadline = time() + 170;
my %done;
while (keys %done < $NSESS)
{
	die "forced-migration gate timed out" if time() > $deadline;
	for my $sid (0 .. $NSESS - 1)
	{
		next if $done{$sid};
		eval { $sessions[$sid]->{run}->pump_nb(); };
		$done{$sid} = 1 if $sessions[$sid]->{stdout} =~ /\Q$sentinel\E=\d+/;
	}
}

# (2) Correctness: each session's accumulator equals its OWN deterministic sum.
my $all_correct = 1;
for my $sid (0 .. $NSESS - 1)
{
	my $want = expected_for($sid);
	my $out = $sessions[$sid]->{stdout};
	unless ($out =~ /\Q$sentinel\E=(\d+)/ && "$1" eq "$want")
	{
		my $got = ($out =~ /\Q$sentinel\E=(\d+)/) ? $1 : '(none)';
		diag("session $sid: acc=$got, want $want (cross-fiber root leak?)");
		$all_correct = 0;
	}
}
ok($all_correct,
	'every migratable session computed its own correct result under concurrent load (no cross-fiber root leak)');

$_->quit for @sessions;

# (1) The flip is LIVE: client backends spawned migratable=1.
my $log = slurp_file($node->logfile, $log_start);
my $migratable_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=1/g;
ok($migratable_spawns > 0,
	"client-backend fibers spawned migratable=1 (Phase D flip is live; count=$migratable_spawns)")
  || diag(
	"no migratable=1 spawns in log -- Phase D flip not reaching the spawn site");

# Best-effort observability ONLY (see header): log the steal count, do not gate
# on it -- a natural local steal is timing-dependent; the EC2 A/B benchmark is
# the real steal-under-load proof.
my $steals = $node->safe_psql('postgres',
	'SELECT test_backend_runtime_carrier_total_steals();');
note(
	"carrier total_steals=$steals (best-effort: steals are opportunistic; the "
	  . "real steal-under-load proof is the EC2 A/B benchmark, not this local gate)"
);

# (4) Server usable after the migration workload.
is($node->safe_psql('postgres', 'SELECT 42;'),
	'42', 'server usable after forced-migration workload');

# (3) No crash / assertion / corruption signatures -- under cassert this means
# the Phase-C seam cross-check and the affine park-boundary tripwire stayed
# silent, i.e. no wrong root repoint and no affine span parked.
unlike(
	$log,
	qr/PANIC|Assert|assertion|segmentation|was terminated by signal|TRAP:|FailedAssertion|server process .* was terminated/,
	'no crash / assertion / corruption signatures (Phase-C cross-check + affine tripwire silent)');

$node->stop('fast');

done_testing();
