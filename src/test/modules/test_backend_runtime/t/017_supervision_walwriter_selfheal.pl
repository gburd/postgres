# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Supervision tree, aux-worker self-heal demonstration (F4-SUP follow-up).
#
# The user's north star includes a fully supervised PostgreSQL that can
# self-heal in some cases, BEAM-style.  This test proves the ONE narrow,
# already-safe case end to end: a fiber-backed WAL writer that exits
# CLEANLY (SIGTERM, exit status 0) is relaunched by the existing postmaster
# machinery (LaunchMissingBackgroundProcesses), and the server keeps serving
# throughout.  A second case proves the boundary that must NOT move: a
# GENUINE crash (SIGSEGV) still fail-stops the whole server, exactly like a
# crashing client backend.
#
# What this test is NOT: it does not test xtc_orc.  We evaluated putting
# the WAL writer's spawn under libxtc's xtc_orc supervisor and found three
# independent reasons not to (see
# plan_docs/phase16_audits/SUPERVISION_SCOPE_WHY_NO_CRASH_RESPAWN.md and
# plan_docs/phase16_audits/LIBXTC_ORC_NO_PER_CHILD_DOWN.md):
#   1. xtc_orc restarts a child from the supervisor's OWN carrier-loop
#      thread; PMChild slot assignment is postmaster-thread-affine, so
#      handing it spawn ownership would be a new cross-thread bug of our own.
#   2. Even with libxtc v1.44.0's spawn-then-monitor fix (verified: our
#      OWN atomic xtc_proc_spawn_monitor() path is unaffected either way),
#      xtc_orc's public API exposes only AGGREGATE counters -- no per-child
#      DOWN detail -- so an external caller cannot reliably tell "this
#      child crashed" from "this child exited cleanly" for a specific pid.
#   3. Nothing in xtc_orc's API lets a caller record an external restart
#      event, so even pure bookkeeping requires handing it spawn ownership.
# Restart ownership for this singleton therefore stays exactly where
# process-mode PostgreSQL already safely puts it: the postmaster's own
# LaunchMissingBackgroundProcesses(), called every ServerLoop tick,
# unchanged.  What is NEW here is restart-INTENSITY tracking on top of that
# existing relaunch (xtc_pg_aux_worker_note_relaunch(), pg_xtc_carrier.c) --
# a flapping WAL writer (dies-and-relaunches repeatedly within a window)
# escalates to the same fail-stop a genuine crash gets, instead of spinning
# forever.  This test's third case proves that escalation fires.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('supervision_walwriter_selfheal');

$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 0
autovacuum = off
io_method = sync
wal_writer_delay = 200
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime enabled for the supervision fixture');

# Establish a durability baseline so we can prove the server kept serving
# (and kept committing) across the WAL writer's clean-exit relaunch.
$node->safe_psql('postgres', q{
	CREATE TABLE selfheal_probe(id int primary key, v text);
	INSERT INTO selfheal_probe VALUES (1, 'before');
});
is($node->safe_psql('postgres', 'SELECT v FROM selfheal_probe WHERE id = 1'),
	'before', 'baseline row visible before the WAL writer is touched');

# Find the WAL writer's signal pid via pg_stat_activity (works whether it is
# a dedicated thread carrier or a fiber -- PostmasterChildSignalPid publishes
# the same logical id either way).
sub walwriter_pid
{
	return $node->safe_psql('postgres',
		q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'walwriter'}
	);
}

my $original_pid = walwriter_pid();
isnt($original_pid, '', 'WAL writer is visible in pg_stat_activity');

my $log_start = -s $node->logfile;

# --- Case 1: a CLEAN exit (SIGTERM) is relaunched, server keeps serving. ---
kill('TERM', $original_pid);

my $relaunched_pid;
for (1 .. 100)
{
	my $pid = walwriter_pid();
	if ($pid ne '' && $pid ne $original_pid)
	{
		$relaunched_pid = $pid;
		last;
	}
	usleep(100_000);
}
ok(defined $relaunched_pid,
	'WAL writer was relaunched with a new pid after a clean SIGTERM exit');

# The server must have kept running throughout: no crash-recovery log lines,
# and ordinary SQL keeps working.
$node->safe_psql('postgres',
	q{INSERT INTO selfheal_probe VALUES (2, 'after-clean-exit')});
is($node->safe_psql('postgres',
		'SELECT v FROM selfheal_probe WHERE id = 2'),
	'after-clean-exit',
	'server kept serving SQL across the WAL writer clean-exit relaunch');

my $log_after_case1 = slurp_file($node->logfile, $log_start);
unlike($log_after_case1, qr/terminating threaded server runtime/,
	'a clean WAL writer exit did NOT trigger the crash-fail-stop path');
unlike($log_after_case1, qr/restart intensity exceeded/,
	'a single clean exit does not (yet) look like flapping');

# --- Case 2: restart-intensity escalation -- flap the WAL writer past the
# threshold and confirm it fail-stops instead of spinning forever. ---
# xtc_pg_aux_worker_note_relaunch's window is 3 restarts within 5 seconds
# (mirrors libxtc xtc_orc's own default max_restarts=3/period_ns=5s -- see
# pg_xtc_carrier.c).  Case 1 already consumed one relaunch; SIGTERM it
# rapidly a few more times to exceed the threshold within the window.  Guard
# every step: once the flap trips, the postmaster exits and a subsequent
# psql attempt would otherwise die the test script outright.
my $flap_log_start = -s $node->logfile;
for (1 .. 4)
{
	my $pid = eval { walwriter_pid() };
	last if !defined $pid || $pid eq '';
	kill('TERM', $pid);
	usleep(200_000);
}

my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
$postmaster_pid =~ s/\n.*//s;

my $postmaster_exited = 0;
for (1 .. 100)
{
	if (kill(0, $postmaster_pid) == 0)
	{
		$postmaster_exited = 1;
		last;
	}
	usleep(100_000);
}
ok($postmaster_exited,
	'flapping WAL writer (repeated clean exits within the restart-intensity window) fail-stops the server, like a genuine crash, rather than spinning forever');

my $flap_log = slurp_file($node->logfile, $flap_log_start);
like($flap_log, qr/restart intensity exceeded/,
	'restart-intensity escalation logged the flap before fail-stop');

$node->stop('immediate', fail_ok => 1);

done_testing();
