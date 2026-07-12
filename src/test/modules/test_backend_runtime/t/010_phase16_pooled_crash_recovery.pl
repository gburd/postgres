# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase 16 hardening: define and lock in the behavior of a backend crash
# (SIGSEGV) under the pooled protocol scheduler.
#
# In a shared-process threaded runtime one fiber's memory corruption cannot be
# safely isolated the way a separate process crash can, so the current,
# deliberate policy is FAIL-STOP: a genuine backend-fiber crash is contained on
# the carrier, escalated to the postmaster, and the whole server is terminated
# (see "terminating threaded server runtime after backend fiber crash" in
# postmaster.c).  This test pins that contract: the crash must be observed by
# the client and must bring the server down cleanly (no hang, no silent
# corruption of the on-disk state), and committed data written before the crash
# must still be present after a manual restart.
#
# Known hardening gaps recorded alongside this test (not yet addressed):
#  - the crash is only consumed on the postmaster's next wait-loop wakeup, so
#    escalation latency can be tens of seconds (~71 s observed);
#  - restart_after_crash is NOT honored for threaded crashes (fail-stop exits
#    the postmaster rather than cycling crash recovery).
# Both are follow-ups for a later hardening pass; the safety contract validated
# here is "crash => clean fail-stop + durable committed data", which holds.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('phase16_pooled_crash_recovery');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 3
autovacuum = off
io_method = sync
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION test_backend_runtime_threaded;');

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'pooled crash test runs threaded');
isnt($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '0',
	'pooled crash test runs pooled');

# Durable, committed data that must survive across the crash + restart.
$node->safe_psql('postgres', q{
	CREATE TABLE crash_survive(id int primary key, v text);
	INSERT INTO crash_survive SELECT g, 'row-'||g FROM generate_series(1, 500) g;
	CHECKPOINT;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM crash_survive;'),
	'500', 'committed rows present before crash');

# Crash one pooled session with a SIGSEGV.  The connection dies.
my ($ret, $out, $err) = $node->psql('postgres',
	'SELECT test_backend_runtime_crash_current_backend();');
isnt($ret, 0, 'crashing SELECT returns a client-visible failure');
like($err, qr/server closed the connection|terminating connection|connection to server/,
	'client observes the backend crash');

# Fail-stop: the postmaster terminates after the fiber crash.  Poll until the
# server is no longer accepting connections (bounded so we do not hang the
# suite indefinitely on the known escalation latency).
my $down = 0;
for (1 .. 120)
{
	my ($r) = $node->psql('postgres', 'SELECT 1;');
	if ($r != 0) { $down = 1; last; }
	sleep 1;
}
ok($down, 'server fail-stops (stops accepting connections) after the pooled backend crash');

# Bring the postmaster's bookkeeping in line, then restart and confirm the
# on-disk state recovered cleanly and committed data survived.
#
# The fail-stop is a fast _exit(2) with no cleanup (correct: we don't touch
# possibly-corrupt state), so the crashed postmaster leaves its stale lock files
# behind -- postmaster.pid in the data dir and the .s.PGSQL.<port>.lock socket
# lock in the socket dir.  An external supervisor/operator would clear them
# before restart; do the same here, or $node->start refuses with "lock file ...
# already exists".
$node->{_pid} = undef;    # postmaster already exited; avoid a stop() on a dead pid
unlink $node->data_dir . '/postmaster.pid';
unlink glob($node->host . '/.s.PGSQL.*.lock');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM crash_survive;'),
	'500', 'committed rows survive crash + restart with clean recovery');
$node->safe_psql('postgres',
	"INSERT INTO crash_survive VALUES (1001, 'after-restart');");
is($node->safe_psql('postgres',
		"SELECT v FROM crash_survive WHERE id = 1001;"),
	'after-restart', 'server fully usable after restart');

$node->stop('fast');
done_testing();
