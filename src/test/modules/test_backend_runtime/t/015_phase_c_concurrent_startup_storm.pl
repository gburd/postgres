# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase C concurrent-startup corruption regression gate.
#
# Regression guard for a PRE-EXISTING concurrent-startup DATA CORRUPTION in the
# thread-per-session fiber runtime, live at migratable=0 (migration OFF) and
# independent of work-stealing / migration:
#
#   The backend-fiber startup window used to run MemoryContextInit() and the
#   early GUC init (InitializeThreadedSessionGUCOptions / read_nondefault_
#   variables) BEFORE the six-root current-work bridge was installed.  With
#   CurrentPgExecution/CurrentPgSession == NULL in that window, TopMemoryContext
#   and the early session-GUC table resolved to the SHARED per-OS-thread
#   early_execution_fallback / early_session_fallback.  The early GUC init takes
#   the process-wide GUC amutex, which PARKS the fiber under concurrent startup;
#   while parked, a sibling backend fiber time-sharing the SAME carrier OS thread
#   ran MemoryContextInit() and clobbered that shared fallback -- tripping
#   Assert("TopMemoryContext == NULL") (cassert) or, in release, a fiber
#   SIGSEGV that the supervisor reports as a GENUINE-CRASH and "terminating
#   threaded server runtime".
#
#   The crash was ON-LOOP cooperative fiber INTERLEAVING (reproduced with a
#   SINGLE carrier loop too), NOT work-stealing / cross-loop / migration.
#   Historic rate on the pristine pre-fix binary: N=2 -> 0/5, N=3 -> ~4-5/5,
#   N>=4 -> 5/5 (2 loops); single loop crashed at N~6.
#
# The fix makes the whole pre-install startup window PER-FIBER: it installs the
# fiber-owned logical roots as current (PreInstallPgThreadBackendRuntimeState,
# mirroring the pooled-protocol path's PgCarrierAttachBackend) BEFORE the
# per-backend startup, so MemoryContextInit() and the early GUC init write into
# fiber-owned storage that rides the GUC-amutex park with the fiber; the sibling
# fiber sees its OWN (NULL) TopMemoryContext.  No shared fallback is touched in
# the window, so there is nothing to clobber.
#
# WHAT THIS TEST DRIVES: many client backends CONNECT SIMULTANEOUSLY (a
# connection storm), the exact concurrency the bug needs -- each fresh backend
# fiber runs its startup window (MemoryContextInit + early GUC init) while
# siblings do the same on the same small carrier-loop pool.  A regression
# reintroduces the shared-fallback clobber and shows up as a crash signature in
# the server log and/or a lost/failed connection.
#
# migratable is now 1 (migration re-enabled): client backends spawn migratable
# so the concurrent-connect storm exercises the pre-install window BOTH under
# on-loop cooperative interleaving AND under real cross-loop steals.  The
# per-fiber-window fix must hold under migration too; assertion (5) below now
# requires migratable=1, and the storm-safety assertions (2-4) prove no
# pre-install-window corruption regresses once fibers can be stolen.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use POSIX ();

# Two carrier loops keeps the runtime multi-loop while concentrating enough
# concurrently-starting fibers per loop to interleave their startup windows.
$ENV{PG_XTC_CARRIER_LOOPS} = '2';

my $node = PostgreSQL::Test::Cluster->new('phase_c_concurrent_startup_storm');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 0
autovacuum = off
io_method = sync
summarize_wal = off
max_connections = 100
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'),
	'on', 'threaded runtime up (migration ON; concurrent-startup storm gate)');

my $connstr = $node->connstr('postgres');
my $psql = $ENV{PG_REGRESS_PSQL} || $ENV{PSQL} || 'psql';

# Fire N client backends that connect AT THE SAME TIME (a connection storm) --
# the exact concurrency the startup-window bug needs.  fork() all N children
# first WITHOUT exec, then release them together so their connect()/accept()
# and backend-fiber startup windows overlap as tightly as possible on the small
# carrier-loop pool.  Each child runs only a trivial query: the corruption is
# in backend STARTUP, so the query body is irrelevant; a 0 exit proves the
# backend completed its startup window + first query without the shared-
# fallback clobber killing it.  (IPC::Run::start in a loop staggers the
# connects too much to reproduce the on-loop interleaving; a true simultaneous
# fork/exec storm is what trips the pristine bug.)
sub storm
{
	my ($n) = @_;
	pipe(my $rgate, my $wgate) or die "pipe: $!";
	my @pids;
	for my $i (1 .. $n)
	{
		my $pid = fork();
		die "fork: $!" unless defined $pid;
		if ($pid == 0)
		{
			# Child: block until the parent closes the gate, so all children
			# race into connect() at once.
			close $wgate;
			my $buf;
			sysread($rgate, $buf, 1);
			close $rgate;
			open(STDOUT, '>', '/dev/null');
			open(STDERR, '>', '/dev/null');
			exec($psql, '--no-psqlrc', '--quiet', '--no-align',
				'--tuples-only', '--dbname' => $connstr,
				'--command' => 'SELECT 1;')
			  or POSIX::_exit(127);
		}
		push @pids, $pid;
	}
	close $rgate;
	close $wgate;    # release all children simultaneously
	my $ok = 0;
	for my $pid (@pids)
	{
		waitpid($pid, 0);
		$ok++ if $? == 0;
	}
	return ($ok, scalar @pids);
}

# Run the storm at escalating widths, several passes each, on a SINGLE loop so
# the concurrently-starting fibers interleave their startup windows across the
# early GUC-amutex park (the pristine bug crashes deterministically single-loop
# after ~2 interleaved fibers; the fixed binary completes every backend).  Any
# crashing storm escalates to a supervisor GENUINE-CRASH that terminates the
# whole runtime, so every later connect fails too -- either way $ok != $total trips.
my $log_start = -s $node->logfile;
my $all_ok = 1;
for my $n (4, 8, 16, 24, 32)
{
	for my $pass (1 .. 4)
	{
		my ($ok, $total) = storm($n);
		if ($ok != $total)
		{
			diag("storm N=$n pass=$pass: only $ok/$total backends completed startup");
			$all_ok = 0;
		}
	}
}
ok($all_ok,
	'every backend in the concurrent-connect storm completed startup + first query (no pre-install-window corruption)'
);

# The server must still be usable after the storm -- a startup-window clobber
# escalates to a supervisor GENUINE-CRASH that terminates the whole runtime.
# Use a non-fatal probe so a regressed (crashed) server still reports a clean
# FAIL here instead of aborting the remaining assertions.
my ($rc, $stdout, $stderr) = $node->psql('postgres', 'SELECT 42;');
is($stdout, '42', 'server usable after concurrent-startup storm')
  or diag("psql rc=$rc stderr=$stderr");

# No crash / assertion / corruption signatures.  Under cassert this catches the
# Assert("TopMemoryContext == NULL") directly; in release it catches the fiber
# SIGSEGV / supervisor GENUINE-CRASH / runtime-termination signatures.
my $log = slurp_file($node->logfile, $log_start);
unlike(
	$log,
	qr/TopMemoryContext == NULL|TRAP:|FailedAssertion|PANIC|terminating threaded server runtime|GENUINE-CRASH|was terminated by signal|segmentation/,
	'no crash / assertion / corruption signatures during concurrent-startup storm'
);

# Confirm the storm spawned fibers MIGRATABLE (migratable=1): migration is now
# re-enabled (xtc_carrier_migratable returns true for client backends), so the
# per-fiber-window fix must hold while fibers can be stolen across loops.  The
# storm-safety assertions above (no lost connect, server usable, no crash
# signatures) prove the pre-install window stays corruption-free under
# migration.  A regression to all-pinned here would silently weaken the gate.
my $migratable_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=1/g;
my $pinned_spawns = () = $log =~ /spawned backend fiber pid=.*migratable=0/g;
ok($migratable_spawns > 0,
	"storm backends spawned MIGRATABLE (migration re-enabled -- migratable=$migratable_spawns pinned=$pinned_spawns)"
);

# Best-effort shutdown: if the storm crashed the runtime the postmaster is
# already gone, so do not let teardown mask the real assertion failures above.
eval { $node->stop('fast'); };

done_testing();
