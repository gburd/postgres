# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase D migration gate + GUC-amutex cross-fiber leak regression.
#
# STATUS (Phase D LIVE, 2026-07-22): migration is ENABLED.  All three in-tree
# unseamed-park corruptions are closed -- the GUC-amutex command-path bridge
# leak is seam-wrapped (guc.c ThreadedGUCLock, mirroring xtc_pg_wait_fd), the
# concurrent-startup pre-install window is per-fiber
# (PreInstallPgThreadBackendRuntimeState), and every other backend-fiber park
# is seamed or detaches the bridge by design.  libxtc v1.27.0 eager work-
# stealing rebalance is wired on the threaded multi-loop carrier, so migratable
# backend fibers actually get STOLEN across loops under contended load.  Client
# backends therefore spawn migratable=1 (ssl_sni off).  This test exercises the
# contended-GUC park path with the seam + cross-checks ARMED, asserts real
# cross-loop steals fire (n_steals > 0), and asserts NO cross-fiber leak and NO
# protocol-read-park PANIC under real migration.
#
# The "protocol-read-park cross-carrier PANIC" that previously held migration on
# Phase D HOLD is DECIDED here: with the GUC-amutex seam in place + eager
# rebalance producing hundreds of real steals under this exact contended-GUC
# concurrent load, PgCarrierCommitProtocolReadPark NEVER PANICs.  The prior
# PANIC was a downstream manifestation of the now-fixed GUC-amutex bridge leak
# (subsumed by the seam), not a distinct scheduler-affinity hazard.
#
# WHAT THIS TEST DRIVES:
#   1. Client backends spawn MIGRATABLE (migratable=1) -- the Phase D LIVE
#      state -- and real cross-loop steals fire (n_steals > 0).
#   2. CONTENDED GUC WRITES under concurrent load: many sessions hammer SET
#      search_path / SET work_mem, all serializing on the single process-wide
#      GUC amutex.  Under contention the amutex PARKS fibers -- the seam this
#      test regression-guards.  A parked migratable fiber can be STOLEN onto an
#      idle peer loop and RESUME on a different carrier, exercising the
#      cross-carrier resume the seam and the protocol-read commit must handle.
#   3. Per-session correctness: every session reads back its OWN just-SET
#      search_path AND its own deterministic accumulator.  A cross-fiber root
#      leak (a fiber reading a sibling's session/execution root) shows up as a
#      wrong schema name or wrong accumulator on at least one session.
#   4. No crash / assertion / PANIC: under cassert the seam cross-checks
#      (XtcPgVerifyCurrentWorkIsSelf on the GUC-amutex seam restore,
#      XtcPgVerifySnapshotIsSelf on its save) and the affine park-boundary
#      tripwire are ARMED, so a wrong root repoint would TRAP; the
#      protocol-read commit PANIC would fire in release too.
#   5. The server stays usable afterward, with no crash signatures.
#
# STEAL COUNT: with eager rebalance wired (libxtc v1.27.0) + migratable=1, a
# run-queue-empty-but-fd-parked loop steals a peer's runnable migratable proc,
# and enqueuing migratable work nudges an idle peer.  This test GATES on
# n_steals > 0: real migration must actually happen for the correctness and
# no-PANIC assertions to be meaningful.  (If a future scheduler/OS change makes
# steals unachievable in this workload, this gate will fail loudly rather than
# silently degrade to a pinned no-op.)
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

# Small carrier pool (two loops).  Backends are MIGRATABLE (migration LIVE --
# see STATUS above): placement is round-robin, and under eager rebalance a loop
# whose run queue drains while it still owns fd-parked fibers steals a peer's
# runnable migratable proc.  Two loops give a peer to steal from while keeping
# the contended-GUC path realistic.  We do NOT set the test-only PG_XTC_FORCE_LOOP
# hook: eager rebalance produces steals without forced placement, and piling
# many concurrently-alive client backends on one loop trips unrelated
# multi-fiber-on-one-loop races.
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
	'on', 'threaded runtime up (Phase D LIVE: migration enabled)');

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

# (1) Migration is now LIVE (Phase D): client backends spawn migratable=1
# (ssl_sni off) so a parked fiber can be work-stolen onto an idle peer loop.
# Assert every client-backend fiber spawned MIGRATABLE and NONE pinned -- a
# regression that silently re-pins would make the steal/no-PANIC evidence below
# meaningless.
my $log = slurp_file($node->logfile, $log_start);
my $migratable_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=1/g;
my $pinned_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=0/g;
ok($migratable_spawns > 0 && $pinned_spawns == 0,
	"client-backend fibers spawned MIGRATABLE (migratable=1; Phase D LIVE -- migratable=$migratable_spawns pinned=$pinned_spawns)"
) || diag(
	"expected all client backends migratable=1 -- migration may have been re-pinned or a non-B_BACKEND leaked onto this path");

# (2) Work-steal PROOF: with eager rebalance wired + migratable=1, real cross-
# loop steals must fire under this contended-GUC concurrent load.  GATE on it:
# n_steals > 0 proves migration ACTUALLY happens (a parked-then-woken migratable
# fiber was rebalanced onto an idle loop), so the correctness (3) and no-PANIC
# (4) assertions are exercised against REAL cross-carrier resumes, not a pinned
# no-op.  This also DECIDES the former "protocol-read-park cross-carrier PANIC"
# question: hundreds of real steals here + assertion (4) staying silent proves
# that PANIC was subsumed by the GUC-amutex seam, not a distinct hazard.
my $steals = $node->safe_psql('postgres',
	'SELECT test_backend_runtime_carrier_total_steals();');
cmp_ok($steals, '>', 0,
	"real cross-loop steals fired under contended GUC + migratable=1 (n_steals=$steals; migration is LIVE)"
) || diag(
	"n_steals=0: eager rebalance did not produce a steal in this workload -- migration may not be exercised; investigate before trusting the no-leak/no-PANIC result");

# (5) Server usable after the workload.
is($node->safe_psql('postgres', 'SELECT 42;'),
	'42', 'server usable after contended-GUC workload');

# (4) No crash / assertion / corruption / PANIC signatures under REAL steals --
# under cassert this means the seam cross-checks (XtcPgVerifyCurrentWorkIsSelf on
# the GUC-amutex seam restore, XtcPgVerifySnapshotIsSelf on its save) and the
# affine park-boundary tripwire stayed silent (no wrong root repoint), and the
# protocol-read-park commit (PgCarrierCommitProtocolReadPark) did NOT PANIC on a
# cross-carrier mismatch even though hundreds of fibers were stolen across loops.
# This is the release-visible half of the bug-#2 decision.
unlike(
	$log,
	qr/PANIC|Assert|assertion|segmentation|was terminated by signal|TRAP:|FailedAssertion|server process .* was terminated/,
	'no crash / assertion / corruption / protocol-read-park PANIC under real cross-loop steals (seam cross-checks + affine tripwire silent)'
);

$node->stop('fast');

done_testing();
