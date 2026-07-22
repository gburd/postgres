# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase C/D correctness gate + GUC-amutex cross-fiber leak regression.
#
# STATUS (Phase D HOLD, 2026-07-21): live migration is DISABLED.  The reported
# GUC-amutex cross-fiber root leak is fixed (guc.c ThreadedGUCLock now seam-
# wraps the amutex exactly like xtc_pg_wait_fd), but validating that fix found a
# SECOND, independent unseamed leak on the migratable path -- a protocol-read-
# park cross-carrier PANIC under concurrent contended GUC, reproduced on the
# PRISTINE pre-seam binary too, so pre-existing.  Rather than ship a tree that
# is UNSAFE with migration live, xtc_carrier_migratable() returns false (all
# fibers PINNED) until the remaining wait-boundary audit completes.  Client
# backends therefore run PINNED here; this test still exercises the contended
# GUC path with the seam + cross-checks ARMED and asserts the pinned/safe state.
#
# When migration is re-enabled (after the protocol-read-park and any sibling
# parks are seam-wrapped), restore assertion (1) below to require migratable=1
# and re-arm the steal expectation.
#
# WHAT THIS TEST DRIVES:
#   1. Client backends spawn PINNED (migratable=0) -- the Phase D HOLD state.
#   2. CONTENDED GUC WRITES under concurrent load: many sessions hammer SET
#      search_path / SET work_mem, all serializing on the single process-wide
#      GUC amutex.  Under contention the amutex PARKS fibers -- the seam this
#      test regression-guards.  (Pinned, so no steal; the seam's save/restore
#      still runs and its cross-checks are armed.)
#   3. Per-session correctness: every session reads back its OWN just-SET
#      search_path AND its own deterministic accumulator.  A cross-fiber root
#      leak (a fiber reading a sibling's session/execution root) shows up as a
#      wrong schema name or wrong accumulator on at least one session.
#   4. No crash / assertion: under cassert the seam cross-checks
#      (XtcPgVerifyCurrentWorkIsSelf on the GUC-amutex seam restore,
#      XtcPgVerifySnapshotIsSelf on its save) and the affine park-boundary
#      tripwire are ARMED, so a wrong root repoint would TRAP.
#   5. The server stays usable afterward, with no crash signatures.
#
# STEAL COUNT: not applicable while migration is disabled (fibers are pinned,
# so total_steals is definitionally 0).  It is logged for information; when
# migration is re-enabled it becomes best-effort (a deterministic local steal
# needs libxtc's not-yet-landed Phase E wake-nudge -- __xtc_exec_try_steal only
# runs when a peer loop's run queue is drained AND it has no pending timer, and
# an idle peer already blocked in xtc_io_poll(-1) is not nudged when fresh
# stealable work lands on a busy peer -- and piling backends on one loop trips
# unrelated multi-fiber-on-one-loop races).  libxtc proves the migrated-resume
# path in its own deterministic simulator (test/sim/test_sim_migratable.c).
#
# WORKLOAD NOTES:
#  - Connections are established SERIALLY, then the workload runs concurrently
#    on the already-connected backends.  A simultaneous connection storm used to
#    trip a SEPARATE, pre-existing concurrent-STARTUP corruption (the backend
#    fiber startup window ran MemoryContextInit + early GUC init against the
#    SHARED per-OS-thread early_execution_fallback / early_session_fallback,
#    which a sibling fiber clobbered across the GUC-amutex park).  That is now
#    fixed (PreInstallPgThreadBackendRuntimeState makes the window per-fiber);
#    015_phase_c_concurrent_startup_storm is the dedicated regression gate for
#    it.  This test keeps serial connect because its focus is the contended
#    GUC-write execution phase, not startup concurrency.

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Small carrier pool (two loops).  Backends are PINNED (migration disabled --
# see STATUS above), so placement is round-robin and no steal occurs; the loop
# count only keeps the threaded runtime multi-loop so the contended-GUC path is
# exercised realistically.  We do NOT set the test-only PG_XTC_FORCE_LOOP hook:
# concentrating many concurrently-alive client backends on one loop trips
# unrelated pre-existing multi-fiber-on-one-loop races, and while pinned there
# is nothing to steal anyway.
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
	'on', 'threaded runtime up (Phase D HOLD: migration pinned)');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

my $log_start = -s $node->logfile;

my $NSESS = 12;
my $ITERS = 40;

sub expected_for
{
	my ($sid) = @_;
	my $base = $sid * 1000;
	my $sum = 0;
	$sum += ($base + $_) for (1 .. $ITERS);
	return $sum;
}

# Distinct per-session schema name so a leaked per-session GUC root (another
# fiber's search_path) is observable in the readback.
sub schema_for { my ($sid) = @_; return "mig_s$sid"; }

# Open sessions one at a time (safe startup -- see header), each with its own
# temp accumulator and schema.
my @sessions;
for my $sid (0 .. $NSESS - 1)
{
	my $s = $node->background_psql('postgres', timeout => 180);
	my $schema = schema_for($sid);
	$s->query_safe(
		"CREATE SCHEMA $schema;"
		  . "CREATE TEMP TABLE mig_t(acc bigint); INSERT INTO mig_t VALUES (0);",
		verbose => 0);
	push @sessions, $s;
}

# Fire the workload on ALL sessions WITHOUT waiting, so they run concurrently.
# Each iteration:
#   - SET search_path / SET work_mem -> takes the CONTENDED GUC amutex (12
#     sessions hammering it => the lock parks fibers: the leak path);
#   - an accumulator UPDATE ending in a socket round-trip -> the fiber PARKS on
#     the client read, WAKES, and is re-enqueued on its loop's stealable deque.
# A trailing sentinel row reports the accumulator AND the live search_path so a
# cross-session GUC leak is directly observable.
my $sentinel = 'MIG_DONE_ROW';
for my $sid (0 .. $NSESS - 1)
{
	my $base = $sid * 1000;
	my $schema = schema_for($sid);
	my $wm = 1024 + $sid;    # distinct per-session work_mem (kB)
	my $sql = '';
	for my $i (1 .. $ITERS)
	{
		my $add = $base + $i;
		$sql .= "SET search_path = $schema, pg_catalog;\n";
		$sql .= "SET work_mem = '${wm}kB';\n";
		$sql .=
			"UPDATE mig_t SET acc = acc + $add"
			. " + (SELECT count(*) FROM (SELECT md5(g::text)"
			. " FROM generate_series(1, 500) g) x) * 0;\n";
	}
	$sql .=
		"SELECT '$sentinel=' || (SELECT acc FROM mig_t) || ':'"
	  . " || split_part(current_setting('search_path'), ',', 1);\n";
	$sessions[$sid]->{stdin} .= $sql;
	$sessions[$sid]->{run}->pump_nb();
}

# Pump all sessions concurrently until each has emitted its sentinel row.
my $deadline = time() + 170;
my %done;
while (keys %done < $NSESS)
{
	die "contended-GUC gate timed out" if time() > $deadline;
	for my $sid (0 .. $NSESS - 1)
	{
		next if $done{$sid};
		eval { $sessions[$sid]->{run}->pump_nb(); };
		$done{$sid} = 1 if $sessions[$sid]->{stdout} =~ /\Q$sentinel\E=\d+:/;
	}
}

# (3) Correctness: each session's accumulator equals its OWN deterministic sum
# AND its live search_path is its OWN schema (not a sibling's -- the GUC leak).
my $all_correct = 1;
for my $sid (0 .. $NSESS - 1)
{
	my $want = expected_for($sid);
	my $want_schema = schema_for($sid);
	my $out = $sessions[$sid]->{stdout};
	if ($out =~ /\Q$sentinel\E=(\d+):(\w+)/)
	{
		my ($acc, $sp) = ($1, $2);
		if ("$acc" ne "$want")
		{
			diag("session $sid: acc=$acc, want $want (cross-fiber root leak?)");
			$all_correct = 0;
		}
		if ("$sp" ne "$want_schema")
		{
			diag(
				"session $sid: search_path=$sp, want $want_schema (cross-session GUC leak!)"
			);
			$all_correct = 0;
		}
	}
	else
	{
		diag("session $sid: no sentinel row");
		$all_correct = 0;
	}
}
ok($all_correct,
	'every session computed its own result AND read back its own search_path under contended GUC writes (no cross-fiber root leak)'
);

$_->quit for @sessions;

# (1) Migration is currently DISABLED (Phase D HOLD): a second, independent
# unseamed leak on the migratable path (a protocol-read-park cross-carrier
# PANIC under concurrent contended GUC, pre-existing) keeps migration off until
# the wait-boundary audit completes.  The GUC-amutex seam is in and correct;
# client backends therefore spawn migratable=0 (pinned = safe).  Assert exactly
# that -- if a future change re-enables migration before the remaining parks are
# seamed, this flips and the test must be revisited alongside the re-enable.
my $log = slurp_file($node->logfile, $log_start);
my $migratable_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=1/g;
my $pinned_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=0/g;
ok($migratable_spawns == 0 && $pinned_spawns > 0,
	"client-backend fibers spawned PINNED (migratable=0; Phase D HOLD -- migration disabled pending protocol-read-park audit; migratable=$migratable_spawns pinned=$pinned_spawns)"
) || diag(
	"unexpected migratable spawn count -- migration may have been re-enabled without seaming the remaining parks");

# (2) Work-steal observability: BEST-EFFORT (see header).  A local steal needs
# libxtc's Phase-E wake-nudge to reliably fire, which has not landed; the seam
# correctness above is proven by the armed cross-checks under contended GUC, and
# libxtc's own DST proves the migrated-resume path.  Log the count; do NOT gate.
my $steals = $node->safe_psql('postgres',
	'SELECT test_backend_runtime_carrier_total_steals();');
note(
	"carrier total_steals=$steals (0 expected: fibers are pinned while migration "
	  . "is on Phase D HOLD; becomes best-effort when migration is re-enabled -- a "
	  . "deterministic local steal needs libxtc's not-yet-landed Phase E wake-nudge "
	  . "and cannot be forced safely on the kernel-poller scheduler; libxtc's DST "
	  . "migrated-resume proof + the EC2 A/B benchmark are the real evidence)");

# (5) Server usable after the workload.
is($node->safe_psql('postgres', 'SELECT 42;'),
	'42', 'server usable after contended-GUC workload');

# (4) No crash / assertion / corruption signatures -- under cassert this means
# the seam cross-checks (XtcPgVerifyCurrentWorkIsSelf on the GUC-amutex seam
# restore, XtcPgVerifySnapshotIsSelf on its save) and the affine park-boundary
# tripwire stayed silent, i.e. no wrong root repoint.
unlike(
	$log,
	qr/PANIC|Assert|assertion|segmentation|was terminated by signal|TRAP:|FailedAssertion|server process .* was terminated/,
	'no crash / assertion / corruption signatures (seam cross-checks + affine tripwire silent)'
);

$node->stop('fast');

done_testing();
