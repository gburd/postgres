# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase 19 Increment 2(e): crash behavior of a process-fallback backend.
#
# A process-fallback backend (used for a session that cannot run on a
# shared-address-space carrier) is a real, isolated, forked+exec'd process, so
# its crash does NOT corrupt the postmaster's/carriers' address space.  BUT --
# like ANY backend, process-mode included -- a crashing backend may have left
# *shared memory* (buffers, locks, ...) inconsistent.  Process mode responds to
# that by SIGQUIT'ing all backends and reinitializing shared memory; under
# multithreaded=on the postmaster cannot safely do that while carrier threads
# run inside its own process, so the correct, deliberate policy is the same
# FAIL-STOP as a carrier-fiber crash (010): the crash is contained, committed
# data survives, and an external supervisor restarts a clean postmaster.
#
# This test pins that contract for the process-fallback route.  The address-space
# isolation of the fallback process is still valuable (an unsafe extension cannot
# scribble on sibling fibers' stacks), but it does not change the shared-memory
# crash-recovery policy, which remains fail-stop.
#
# Requires the fork+exec route (xtc_force_process_fallback=on + sysv).  Note the
# config ORDER: the fallback/sysv GUCs must precede multithreaded=on (a known
# config-application order sensitivity, see the Phase 19 design doc, Bug A).

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('phase19_process_fallback_crash');
$node->init;
# Order matters: fallback + sysv BEFORE multithreaded (Bug A workaround).
$node->append_conf(
	'postgresql.conf', qq(
xtc_force_process_fallback = on
shared_memory_type = sysv
multithreaded = on
));
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime active');

# If the fork+exec route did not engage (config-application order bug, or a
# build without the fallback), skip rather than mis-report.
if ($node->safe_psql('postgres', 'SHOW xtc_force_process_fallback') ne 'on'
	or $node->safe_psql('postgres', 'SHOW shared_memory_type') ne 'sysv')
{
	$node->stop;
	plan skip_all =>
	  'process-fallback route not engaged (xtc_force_process_fallback + sysv did not co-apply)';
}
is($node->safe_psql('postgres', 'SHOW xtc_force_process_fallback'),
	'on', 'process-fallback route forced');

$node->safe_psql('postgres',
	'CREATE EXTENSION test_backend_runtime_threaded;');

# Durable, committed data that must survive across the crash + restart.
$node->safe_psql('postgres',
	'CREATE TABLE crash_survive(id int); INSERT INTO crash_survive SELECT generate_series(1, 500);');

# Crash a process-fallback backend.
my ($ret, $out, $err) = ('', '', '');
$ret = $node->psql('postgres',
	'SELECT test_backend_runtime_crash_current_backend();',
	stderr => \$err);
isnt($ret, 0, 'crashing backend returns a client-visible failure');
like($err,
	qr/server closed the connection|terminating connection|connection to server/,
	'client observes the process-fallback backend crash');

# Fail-stop: the postmaster terminates after the crash (shared-memory safety;
# in-process reinit is unsafe with live carriers).  Poll until the server stops
# accepting connections.
my $down = 0;
for (1 .. 120)
{
	my ($r) = $node->psql('postgres', 'SELECT 1;');
	if ($r != 0) { $down = 1; last; }
	sleep 1;
}
ok($down, 'server fail-stops after a process-fallback backend crash');

# External restart (clear the fail-stop's stale lock files, as an operator would)
# and confirm clean recovery + committed data survived.
$node->{_pid} = undef;
unlink $node->data_dir . '/postmaster.pid';
unlink glob($node->host . '/.s.PGSQL.*.lock');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM crash_survive;'),
	'500', 'committed data survives the process-fallback crash + restart');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'server usable after restart');

$node->stop;
done_testing();
