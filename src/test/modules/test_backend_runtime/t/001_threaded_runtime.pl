# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('threaded_runtime');

sub start_psql_script
{
	my ($sql, $timeout) = @_;
	my $stdin = $sql;
	my $stdout = '';
	my $stderr = '';
	my $timer = IPC::Run::timer($timeout);
	my @cmd = (
		'psql',
		'--no-psqlrc',
		'--no-align',
		'--tuples-only',
		'--quiet',
		'--dbname' => $node->connstr('postgres'),
		'--file' => '-');
	my $run = IPC::Run::start(\@cmd, '<', \$stdin, '>', \$stdout, '2>',
		\$stderr, $timer);

	return {
		run => $run,
		timer => $timer,
		stdout => \$stdout,
		stderr => \$stderr,
	};
}

$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
autovacuum = on
autovacuum_naptime = '1s'
autovacuum_vacuum_threshold = 0
autovacuum_vacuum_scale_factor = 0
autovacuum_vacuum_insert_threshold = 0
autovacuum_vacuum_insert_scale_factor = 0
autovacuum_analyze_threshold = 0
autovacuum_analyze_scale_factor = 0
log_autovacuum_min_duration = 0
log_autoanalyze_min_duration = 0
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime enabled');

$node->safe_psql(
	'postgres',
	q{
CREATE TABLE threaded_runtime_stress(id int primary key, payload text);
INSERT INTO threaded_runtime_stress
SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g;
});
$node->safe_psql(
	'postgres',
	q{
UPDATE threaded_runtime_stress SET payload = payload || 'y';
DELETE FROM threaded_runtime_stress WHERE id = 1;
SELECT pg_stat_force_next_flush();
});
pass('threaded DDL and primary-key index build completed');

my @sessions;
my %signal_pids;
for my $i (1 .. 5)
{
	my $session = $node->background_psql('postgres', timeout => 20);
	my $pid = $session->query_safe('SELECT pg_backend_pid();',
		verbose => 0);
	$signal_pids{$pid} = 1;
	push @sessions, $session;
}
is(scalar(keys %signal_pids), 5,
	'concurrent threaded sessions have distinct SQL-visible backend ids');

foreach my $session (@sessions)
{
	$session->quit;
}

my $cancel_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n", 30);
ok( pump_until($cancel_psql->{run}, $cancel_psql->{timer},
		$cancel_psql->{stdout}, qr/^\d+\s*$/m),
	'active threaded backend reported logical backend id');
my ($cancel_pid) = ${ $cancel_psql->{stdout} } =~ /^(\d+)\s*$/m;
is($node->safe_psql('postgres', "SELECT pg_cancel_backend($cancel_pid);"),
	't', 'cancel request accepted for active threaded backend');
ok( pump_until($cancel_psql->{run}, $cancel_psql->{timer},
		$cancel_psql->{stderr}, qr/canceling statement due to user request/),
	'active threaded backend observed query cancel');
eval { $cancel_psql->{run}->finish; };

my $victim = $node->background_psql('postgres', on_error_stop => 0,
	timeout => 20);
my $victim_pid = $victim->query_safe('SELECT pg_backend_pid();',
	verbose => 0);
is($node->safe_psql('postgres',
		"SELECT pg_terminate_backend($victim_pid, 5000);"),
	't', 'terminate request accepted for idle threaded backend');
$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $victim_pid);",
	't') || die "timed out waiting for terminated threaded backend";
pass('idle threaded backend left pg_stat_activity after terminate');
eval { $victim->{run}->finish; };

$node->psql(
	'postgres',
	'SELECT 1 / 0;',
	on_error_stop => 1,
	stdout => \my $error_stdout,
	stderr => \my $error_stderr);
like($error_stderr, qr/division by zero/,
	'threaded backend reported SQL ERROR');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after SQL ERROR');

my ($abort_ret, $abort_stdout, $abort_stderr) = $node->psql(
	'postgres',
	'BEGIN; SELECT pg_advisory_xact_lock(987655); SELECT 1 / 0;',
	on_error_stop => 1);
isnt($abort_ret, 0, 'threaded transaction abort fixture failed as expected');
like($abort_stderr, qr/division by zero/,
	'threaded transaction abort fixture reported SQL ERROR');
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"),
	'0', 'threaded transaction abort released advisory locks');

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE FUNCTION threaded_plpgsql_add(a int, b int)
RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RETURN a + b;
END
$$;
});
is($node->safe_psql('postgres', 'SELECT threaded_plpgsql_add(20, 22);'),
	'42', 'PL/pgSQL runs in threaded runtime');

my ($load_ret, $load_stdout, $load_stderr) =
  $node->psql('postgres', "LOAD 'test_backend_runtime';",
	on_error_stop => 1);
isnt($load_ret, 0,
	'process-only test module is rejected in threaded runtime');
like($load_stderr, qr/backend model mismatch/,
	'process-only module rejection reports backend model mismatch');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after process-only module rejection');

my $abandoned = $node->background_psql('postgres',
	on_error_stop => 0, timeout => 20);
$abandoned->query_safe('BEGIN; SELECT pg_advisory_lock(987654);',
	verbose => 0);
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"),
	'1', 'advisory lock is visible before client abandon');
$abandoned->{run}->kill_kill;
eval { $abandoned->{run}->finish; };

for (1 .. 100)
{
	last
	  if $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;") eq '0';
	usleep(100_000);
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted;"),
	'0', 'abandoned threaded backend released advisory lock');

my $reconnect_ok = 1;
for my $i (1 .. 30)
{
	my $result = eval {
		$node->safe_psql('postgres', "SELECT $i;");
	};

	if (!defined $result || $result ne "$i")
	{
		diag("reconnect iteration $i returned "
		  . (defined $result ? qq{"$result"} : 'undef'));
		$reconnect_ok = 0;
		last;
	}
}
ok($reconnect_ok, 'repeated threaded connect/disconnect loop completed');

is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after Gate D smoke');

unlike(
	slurp_file($node->logfile),
	qr/PANIC|segmentation|unsupported byval|could not find tuple|server process .* was terminated|was terminated by signal/,
	'server log has no threaded-runtime crash/corruption signatures');

$node->stop('fast');

done_testing();
