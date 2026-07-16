# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $repo_root = abs_path("$FindBin::Bin/../../../../..");
my $node = PostgreSQL::Test::Cluster->new('threaded_runtime');
# Both thread-backed and pooled-logical carriers reclaim TopMemoryContext at
# exit and log it through the postmaster; count both wordings so the
# reclamation-accounting assertions do not undercount pooled-logical exits.
my $top_reclaim_re =
  qr/(?:thread-backed child|pooled logical backend) \d+ reclaimed [1-9]\d* bytes from TopMemoryContext at exit/;

sub install_contrib_extensions
{
	my @contrib_dirs =
	  qw(hstore pg_trgm btree_gist pageinspect pg_plan_advice);
	my $gmake = $ENV{GMAKE} || 'gmake';

	# Under a meson build the contrib extensions and plsample are already
	# installed into the test install tree (meson installs them), so the
	# runtime gmake step is both redundant and unavailable (there are no
	# configured contrib Makefiles).  Detect that case by looking for an
	# extension control file in the install's sharedir and skip the build.
	# The make-based path (gmake check-threaded) has no such pre-install,
	# so fall back to building the extensions with gmake.
	my $sharedir = `pg_config --sharedir 2>/dev/null`;
	chomp $sharedir if defined $sharedir;
	if (   defined $sharedir
		&& $sharedir ne ''
		&& -f "$sharedir/extension/hstore.control")
	{
		note("contrib extensions already installed in $sharedir; "
			. "skipping runtime gmake build");
		return;
	}

	foreach my $dir (@contrib_dirs)
	{
		system_or_bail(
			$gmake, '-C', "$repo_root/contrib/$dir",
			"DESTDIR=$repo_root/tmp_install", 'install');
	}

	system_or_bail(
		$gmake, '-C', "$repo_root/src/test/modules/plsample",
		"DESTDIR=$repo_root/tmp_install", 'install');
}

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

sub postmaster_child_count
{
	my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
	$postmaster_pid =~ s/\n.*//s;

	my $stdout = '';
	my $stderr = '';
	my $result = IPC::Run::run [ 'ps', '-Ao', 'ppid=' ], '>', \$stdout, '2>',
	  \$stderr;
	die "could not count postmaster children: $stderr" unless $result;

	my $count = 0;
	foreach my $ppid (split /\n/, $stdout)
	{
		$ppid =~ s/^\s+|\s+$//g;
		$count++ if $ppid eq $postmaster_pid;
	}
	return $count;
}

sub postmaster_child_command_count
{
	my ($pattern) = @_;
	my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
	$postmaster_pid =~ s/\n.*//s;

	my $stdout = '';
	my $stderr = '';
	my $result = IPC::Run::run [ 'ps', '-Ao', 'ppid=,command=' ], '>',
	  \$stdout, '2>', \$stderr;
	die "could not inspect postmaster children: $stderr" unless $result;

	my $count = 0;
	foreach my $line (split /\n/, $stdout)
	{
		$line =~ s/^\s+//;
		my ($ppid, $command) = split /\s+/, $line, 2;
		next unless defined $command;
		$count++ if $ppid eq $postmaster_pid && $command =~ $pattern;
	}
	return $count;
}

sub wait_for_pids_to_leave_pg_stat_activity
{
	my ($pids, $label) = @_;
	my $pid_list = join(',', map { int($_) } @$pids);

	$node->poll_query_until(
		'postgres',
		"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid IN ($pid_list));",
		't') || die "timed out waiting for $label";
	pass($label);
}

sub wait_for_advisory_lock_count
{
	my ($low, $high, $expected, $label) = @_;

	for (1 .. 100)
	{
		last
		  if $node->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN $low AND $high;") eq $expected;
		usleep(100_000);
	}
	is($node->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN $low AND $high;"),
		$expected, $label);
}

sub reclaimed_top_memory_context_log_count
{
	my ($log) = @_;

	return scalar(() = $log =~ /$top_reclaim_re/g);
}

sub wait_for_reclaimed_top_memory_context_logs
{
	my ($minimum, $label) = @_;
	my $log;
	my $count = 0;

	for (1 .. 100)
	{
		$log = slurp_file($node->logfile);
		$count = reclaimed_top_memory_context_log_count($log);
		last if $count >= $minimum;
		usleep(100_000);
	}

	ok($count >= $minimum, $label)
	  || diag("observed $count reclaimed TopMemoryContext log entries, expected at least $minimum");

	return $log;
}

install_contrib_extensions();

$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 0
autovacuum = on
autovacuum_naptime = '1h'
io_method = worker
io_min_workers = 2
io_max_workers = 4
io_worker_launch_interval = 0
io_worker_idle_timeout = '60s'
log_min_messages = debug1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime enabled');

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION test_backend_runtime_threaded;
SELECT test_backend_runtime_request_autovacuum_worker();
});
is($node->safe_psql(
		'postgres',
		q{
SELECT test_backend_runtime_custom_guc_value();
SELECT test_backend_runtime_custom_guc_init_count() >= 1;
}),
	"default\nt",
	'threaded extension DDL loads module and initializes custom GUC');

for (1 .. 50)
{
	last
	  if slurp_file($node->logfile) =~
	  qr/autovacuum worker started without a worker entry/;
	usleep(100_000);
}

like(slurp_file($node->logfile),
	qr/autovacuum worker started without a worker entry/,
	'deterministic autovacuum worker thread reached worker main');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 8
	  if $^O eq 'MSWin32';

	$node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'autovacuum launcher';},
		't') || die "timed out waiting for autovacuum launcher";
	is($node->safe_psql(
			'postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum launcher';}),
		'1', 'autovacuum launcher is visible as a logical threaded backend');
	is(postmaster_child_command_count(qr/autovacuum launcher/), 0,
		'autovacuum launcher did not fork a postmaster child process');

	# Under multithreaded=on this runtime deliberately does NOT start io
	# workers: the xtc carrier handles AIO in-fiber (see "never start io
	# workers under multithreaded=on").  Assert there are zero io workers and
	# that raising io_min_workers does not spawn any.
	is($node->safe_psql(
			'postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'io worker';}),
		'0',
		'threaded runtime starts with no io workers (AIO handled in-fiber)');
	is(postmaster_child_command_count(qr/io worker|ioworker/), 0,
		'no io worker was forked as a postmaster child');

	my $children_before = postmaster_child_count();
	is($children_before, 0,
		'threaded runtime has no postmaster child processes after startup');

	$node->safe_psql('postgres', q{ALTER SYSTEM SET io_min_workers = 3});
	$node->safe_psql('postgres', q{SELECT pg_reload_conf()});

	# Give the reload a moment; still expect zero io workers under threaded.
	usleep(500_000);
	is($node->safe_psql(
			'postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'io worker';}),
		'0',
		'raising io_min_workers does not start io workers under multithreaded=on');
}

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

is($node->safe_psql(
		'postgres',
		q{
CREATE EXTENSION hstore;
CREATE EXTENSION pg_trgm;
CREATE EXTENSION btree_gist;
CREATE EXTENSION pageinspect;
SELECT ('"a"=>"b"'::hstore -> 'a') || '|' ||
       (similarity('thread', 'threads') > 0)::text || '|' ||
       ((bt_metap('threaded_runtime_stress_pkey')).level >= 0)::text;
CREATE TABLE threaded_btree_gist(id int, EXCLUDE USING gist (id WITH =));
INSERT INTO threaded_btree_gist VALUES (1), (2);
SELECT count(*) FROM threaded_btree_gist;
DROP TABLE threaded_btree_gist;
}),
	"b|true|true\n2",
	'threaded runtime loads and exercises representative contrib extensions');

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

my $fatal_psql = start_psql_script(
	"SELECT pg_backend_pid();\nSELECT test_backend_runtime_emit_fatal();\n",
	30);
ok( pump_until($fatal_psql->{run}, $fatal_psql->{timer},
		$fatal_psql->{stdout}, qr/^\d+\s*$/m),
	'FATAL threaded backend reported logical backend id');
my ($fatal_pid) = ${ $fatal_psql->{stdout} } =~ /^(\d+)\s*$/m;
ok( pump_until($fatal_psql->{run}, $fatal_psql->{timer},
		$fatal_psql->{stderr}, qr/test_backend_runtime requested FATAL/),
	'threaded backend reported test FATAL');
eval { $fatal_psql->{run}->finish; };
$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $fatal_pid);",
	't') || die "timed out waiting for FATAL threaded backend";
pass('FATAL threaded backend left pg_stat_activity');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after backend FATAL');

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

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION plsample;
CREATE FUNCTION threaded_plsample_echo(a text)
RETURNS text
AS $$threaded plsample ok$$ LANGUAGE plsample;
});
is($node->safe_psql('postgres',
		"SELECT threaded_plsample_echo('argument');"),
	'threaded plsample ok', 'PL/Sample runs in threaded runtime');

$node->safe_psql(
	'postgres',
	q{
CREATE TABLE threaded_gate_e2_smoke(id int);
INSERT INTO threaded_gate_e2_smoke VALUES (1);
DROP TABLE threaded_gate_e2_smoke;
});
pass('threaded runtime supports basic catalog-writing table DDL');

$node->safe_psql(
	'postgres',
	q{
ALTER DATABASE postgres SET work_mem = '3MB';
CREATE ROLE threaded_guc_role LOGIN;
ALTER ROLE threaded_guc_role SET statement_timeout = '7s';
ALTER ROLE threaded_guc_role SET default_statistics_target = 77;
});
my $threaded_guc_role_connstr =
  $node->connstr('postgres') . " user=threaded_guc_role";
is($node->safe_psql(
		'postgres',
		q{
SHOW work_mem;
SHOW statement_timeout;
SHOW default_statistics_target;
},
		connstr => $threaded_guc_role_connstr),
	"3MB\n7s\n77",
	'threaded runtime applies database and role GUC defaults');
is($node->safe_psql(
		'postgres',
		'SHOW lock_timeout;',
		connstr => $threaded_guc_role_connstr . " options='-c lock_timeout=8s'"),
	'8s',
	'threaded runtime applies startup packet GUC options');
is($node->safe_psql(
		'postgres',
		q{
SET work_mem = '4MB';
SHOW work_mem;
BEGIN;
SET LOCAL work_mem = '5MB';
SHOW work_mem;
ROLLBACK;
SHOW work_mem;
RESET work_mem;
SHOW work_mem;
},
		connstr => $threaded_guc_role_connstr),
	"4MB\n5MB\n4MB\n3MB",
	'threaded runtime preserves built-in GUC SET LOCAL rollback and RESET stack semantics');
is($node->safe_psql(
		'postgres',
		q{
BEGIN;
SET LOCAL statement_timeout = '9s';
SHOW statement_timeout;
COMMIT;
SHOW statement_timeout;
},
		connstr => $threaded_guc_role_connstr),
	"9s\n7s",
	'threaded runtime restores role GUC default after SET LOCAL commit');
is($node->safe_psql(
		'postgres',
		q{
SET lock_timeout = '9s';
SHOW lock_timeout;
RESET lock_timeout;
SHOW lock_timeout;
},
		connstr => $threaded_guc_role_connstr . " options='-c lock_timeout=8s'"),
	"9s\n8s",
	'threaded runtime restores startup packet GUC source on RESET');

is($node->safe_psql(
		'postgres',
		q{
SET test_backend_runtime_threaded.custom_guc = 'session one';
LOAD 'test_backend_runtime_threaded';
SHOW test_backend_runtime_threaded.custom_guc;
}),
	'session one',
	'threaded custom GUC placeholder converts during first module load');
is($node->safe_psql(
		'postgres',
		q{
SET test_backend_runtime_threaded.custom_guc = 'session two';
LOAD 'test_backend_runtime_threaded';
SHOW test_backend_runtime_threaded.custom_guc;
}),
	'session two',
	'threaded custom GUC placeholder converts when loaded module is reused in another session');
is($node->safe_psql(
		'postgres',
		q{
LOAD 'test_backend_runtime_threaded';
SHOW test_backend_runtime_threaded.custom_guc;
}),
	'default',
	'threaded custom GUC initializes to default in a later session');
is($node->safe_psql(
		'postgres',
		q{
LOAD 'test_backend_runtime_threaded';
SET test_backend_runtime_threaded.custom_guc = 'changed';
SHOW test_backend_runtime_threaded.custom_guc;
BEGIN;
SET LOCAL test_backend_runtime_threaded.custom_guc = 'local';
SHOW test_backend_runtime_threaded.custom_guc;
COMMIT;
SHOW test_backend_runtime_threaded.custom_guc;
RESET test_backend_runtime_threaded.custom_guc;
SHOW test_backend_runtime_threaded.custom_guc;
}),
	"changed\nlocal\nchanged\ndefault",
	'threaded custom GUC preserves SET LOCAL and RESET stack semantics');

my @guc_stress_scripts;
for my $worker (1 .. 4)
{
	my $script = qq{\\set ON_ERROR_STOP on
LOAD 'test_backend_runtime_threaded';
BEGIN;
DO \$stress\$
DECLARE
  i int;
BEGIN
  FOR i IN 1..25 LOOP
    PERFORM set_config('work_mem', (4 + (i % 4))::text || 'MB', false);
    PERFORM set_config('default_statistics_target', (100 + i)::text, false);
    PERFORM set_config('lock_timeout', (2000 + i)::text || 'ms', false);
    PERFORM set_config('search_path', 'pg_catalog, public', false);
    PERFORM set_config('bytea_output',
                       CASE WHEN i % 2 = 0 THEN 'hex' ELSE 'escape' END,
                       false);
    PERFORM set_config('IntervalStyle',
                       CASE WHEN i % 2 = 0 THEN 'postgres' ELSE 'iso_8601' END,
                       false);
    PERFORM set_config('wal_consistency_checking',
                       CASE WHEN i % 2 = 0 THEN 'all' ELSE '' END,
                       false);
    PERFORM set_config('test_backend_runtime_threaded.custom_guc',
                       'stress-$worker-' || i::text,
                       false);
  END LOOP;
END
\$stress\$;
SET LOCAL work_mem = '16MB';
SET LOCAL test_backend_runtime_threaded.custom_guc = 'local-$worker';
SELECT 'local-$worker:' || current_setting('work_mem') || ':' ||
       current_setting('test_backend_runtime_threaded.custom_guc');
COMMIT;
SELECT 'done-$worker:' || current_setting('work_mem') || ':' ||
       current_setting('default_statistics_target') || ':' ||
       current_setting('lock_timeout') || ':' ||
       current_setting('test_backend_runtime_threaded.custom_guc');
};
	push @guc_stress_scripts,
	  {
		worker => $worker,
		psql => start_psql_script($script, 30),
	  };
}

foreach my $stress (@guc_stress_scripts)
{
	my $worker = $stress->{worker};
	my $psql = $stress->{psql};

	ok( pump_until(
			$psql->{run},
			$psql->{timer},
			$psql->{stdout},
			qr/done-$worker:5MB:125:2025ms:stress-$worker-25/),
		"threaded GUC stress worker $worker completed");
	eval { $psql->{run}->finish; };
	is(${ $psql->{stderr} }, '',
		"threaded GUC stress worker $worker completed without stderr");
	like(${ $psql->{stdout} },
		qr/local-$worker:16MB:local-$worker/,
		"threaded GUC stress worker $worker saw transaction-local values");
}

my ($load_ret, $load_stdout, $load_stderr) =
  $node->psql('postgres', "LOAD 'test_backend_runtime';",
	on_error_stop => 1);
isnt($load_ret, 0,
	'process-only test module is rejected in threaded runtime');
like($load_stderr, qr/backend model mismatch/,
	'process-only module rejection reports backend model mismatch');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after process-only module rejection');

is($node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_rejects_process_bgworker();'),
	't', 'process-model background worker is rejected in threaded runtime');
like(
	slurp_file($node->logfile),
	qr/background worker "test_backend_runtime process bgworker" is not supported in threaded mode/,
	'process-model background worker rejection is logged explicitly');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after process-model bgworker rejection');

like(
	$node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_launch_thread_bgworker();'),
	qr/^\d+$/,
	'thread-model background worker starts and stops in threaded runtime');
like(
	slurp_file($node->logfile),
	qr/starting background worker thread carrier/,
	'thread-model background worker used a thread carrier');

is($node->safe_psql(
		'postgres',
		'SELECT test_backend_runtime_restart_thread_bgworker();'),
	't', 'restartable thread-model background worker restarted and stopped');
like(
	slurp_file($node->logfile),
	qr/test_backend_runtime restart bgworker run 2/,
	'restartable thread-model background worker reached its second run');

is($node->safe_psql(
		'postgres',
		q{
SET debug_parallel_query = on;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
CREATE TEMP TABLE threaded_parallel AS
SELECT i, i % 10 AS g FROM generate_series(1, 20000) AS i;
ALTER TABLE threaded_parallel SET (parallel_workers = 4);
ANALYZE threaded_parallel;
SELECT sum(i)::text || '|' || count(*)::text
FROM threaded_parallel
WHERE g >= 0;
}),
	'200010000|20000',
	'parallel query returns expected result in threaded runtime');
like(
	slurp_file($node->logfile),
	qr/starting background worker thread carrier "parallel worker/,
	'parallel query used background worker thread carriers');

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

my @abandoned_stress;
for my $i (1 .. 4)
{
	my $session = $node->background_psql('postgres',
		on_error_stop => 0, timeout => 20);
	$session->query_safe(
		"BEGIN; SELECT pg_advisory_lock(988000 + $i); CREATE TEMP TABLE threaded_abandoned_$i(id int); INSERT INTO threaded_abandoned_$i VALUES ($i);",
		verbose => 0);
	push @abandoned_stress, $session;
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN 988001 AND 988004;"),
	'4', 'concurrent abandoned-client stress acquired advisory locks');
foreach my $session (@abandoned_stress)
{
	$session->{run}->kill_kill;
	eval { $session->{run}->finish; };
}
for (1 .. 100)
{
	last
	  if $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN 988001 AND 988004;") eq '0';
	usleep(100_000);
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN 988001 AND 988004;"),
	'0', 'concurrent abandoned threaded backends released advisory locks');

my @terminate_stress;
my @terminate_pids;
for my $i (1 .. 4)
{
	my $session = $node->background_psql('postgres',
		on_error_stop => 0, timeout => 20);
	my $pid = $session->query_safe('SELECT pg_backend_pid();',
		verbose => 0);
	push @terminate_stress, $session;
	push @terminate_pids, $pid;
}
is(scalar(@terminate_pids), 4,
	'concurrent termination stress started threaded backends');
is($node->safe_psql(
		'postgres',
		"SELECT bool_and(pg_terminate_backend(pid, 5000)) FROM unnest(ARRAY["
		  . join(',', @terminate_pids)
		  . "]) AS p(pid);"),
	't', 'concurrent termination stress accepted terminate requests');
my $terminate_pid_list = join(',', @terminate_pids);
$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid IN ($terminate_pid_list));",
	't') || die "timed out waiting for concurrently terminated threaded backends";
foreach my $session (@terminate_stress)
{
	eval { $session->{run}->finish; };
}
pass('concurrently terminated threaded backends left pg_stat_activity');

my @mixed_fatal_stress;
my @mixed_abandoned_stress;
my @mixed_terminate_stress;
my @mixed_terminate_pids;
for my $i (1 .. 4)
{
	my $fatal = start_psql_script(
		"SELECT pg_backend_pid();\nSELECT test_backend_runtime_emit_fatal();\n",
		30);
	push @mixed_fatal_stress, $fatal;

	my $abandoned_session = $node->background_psql('postgres',
		on_error_stop => 0, timeout => 20);
	$abandoned_session->query_safe(
		"BEGIN; SELECT pg_advisory_lock(989000 + $i); CREATE TEMP TABLE threaded_mixed_abandoned_$i(id int); INSERT INTO threaded_mixed_abandoned_$i VALUES ($i);",
		verbose => 0);
	push @mixed_abandoned_stress, $abandoned_session;

	my $terminate_session = $node->background_psql('postgres',
		on_error_stop => 0, timeout => 20);
	push @mixed_terminate_stress, $terminate_session;
	push @mixed_terminate_pids,
	  $terminate_session->query_safe('SELECT pg_backend_pid();',
		verbose => 0);
}

my @mixed_fatal_pids;
foreach my $fatal (@mixed_fatal_stress)
{
	ok( pump_until($fatal->{run}, $fatal->{timer},
			$fatal->{stdout}, qr/^\d+\s*$/m),
		'mixed teardown stress FATAL backend reported logical backend id');
	my ($pid) = ${ $fatal->{stdout} } =~ /^(\d+)\s*$/m;
	push @mixed_fatal_pids, $pid;
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN 989001 AND 989004;"),
	'4', 'mixed teardown stress acquired abandoned-client advisory locks');
is($node->safe_psql(
		'postgres',
		"SELECT bool_and(pg_terminate_backend(pid, 5000)) FROM unnest(ARRAY["
		  . join(',', @mixed_terminate_pids)
		  . "]) AS p(pid);"),
	't', 'mixed teardown stress accepted terminate requests');

foreach my $session (@mixed_abandoned_stress)
{
	$session->{run}->kill_kill;
	eval { $session->{run}->finish; };
}
foreach my $fatal (@mixed_fatal_stress)
{
	ok( pump_until($fatal->{run}, $fatal->{timer},
			$fatal->{stderr}, qr/test_backend_runtime requested FATAL/),
		'mixed teardown stress FATAL backend reported test FATAL');
	eval { $fatal->{run}->finish; };
}
foreach my $session (@mixed_terminate_stress)
{
	eval { $session->{run}->finish; };
}

my $mixed_pid_list = join(',', @mixed_fatal_pids, @mixed_terminate_pids);
$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid IN ($mixed_pid_list));",
	't') || die "timed out waiting for mixed teardown threaded backends";
pass('mixed teardown stress FATAL and terminated backends left pg_stat_activity');

for (1 .. 100)
{
	last
	  if $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN 989001 AND 989004;") eq '0';
	usleep(100_000);
}
is($node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid BETWEEN 989001 AND 989004;"),
	'0', 'mixed teardown stress abandoned backends released advisory locks');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after mixed teardown stress');

my $pmchild_reap_children_before =
  $^O eq 'MSWin32' ? undef : postmaster_child_count();
my $pmchild_reap_reclaim_logs_before =
  reclaimed_top_memory_context_log_count(slurp_file($node->logfile));
my $pmchild_reap_expected_reclaims = 3 * (3 + 3 + 2);
for my $cycle (1 .. 3)
{
	my @reap_abandoned_stress;
	my @reap_abandoned_pids;
	my @reap_terminate_stress;
	my @reap_terminate_pids;
	my @reap_fatal_stress;
	my @reap_fatal_pids;
	my $lock_base = 990000 + $cycle * 100;
	my $lock_low = $lock_base + 1;
	my $lock_high = $lock_base + 3;

	for my $i (1 .. 3)
	{
		my $session = $node->background_psql('postgres',
			on_error_stop => 0, timeout => 30);
		my $pid = $session->query_safe('SELECT pg_backend_pid();',
			verbose => 0);

		$session->query_safe(
			"BEGIN; SELECT pg_advisory_lock($lock_base + $i); CREATE TEMP TABLE threaded_reap_abandoned_${cycle}_${i}(id int); INSERT INTO threaded_reap_abandoned_${cycle}_${i} VALUES ($i);",
			verbose => 0);
		push @reap_abandoned_stress, $session;
		push @reap_abandoned_pids, $pid;
	}

	for my $i (1 .. 3)
	{
		my $session = start_psql_script(
			"SELECT pg_backend_pid();\nSELECT pg_sleep(30);\n",
			45);

		ok(pump_until($session->{run}, $session->{timer},
				$session->{stdout}, qr/^\d+\s*$/m),
			"PMChild reaping stress cycle $cycle terminate backend reported logical backend id");
		my ($pid) = ${ $session->{stdout} } =~ /^(\d+)\s*$/m;
		push @reap_terminate_stress, $session;
		push @reap_terminate_pids, $pid;
	}

	for my $i (1 .. 2)
	{
		my $fatal = start_psql_script(
			"SELECT pg_backend_pid();\nSELECT test_backend_runtime_emit_fatal();\n",
			45);

		ok(pump_until($fatal->{run}, $fatal->{timer},
				$fatal->{stdout}, qr/^\d+\s*$/m),
			"PMChild reaping stress cycle $cycle FATAL backend reported logical backend id");
		my ($pid) = ${ $fatal->{stdout} } =~ /^(\d+)\s*$/m;
		push @reap_fatal_stress, $fatal;
		push @reap_fatal_pids, $pid;
	}

	wait_for_advisory_lock_count($lock_low, $lock_high, '3',
		"PMChild reaping stress cycle $cycle acquired abandoned-client advisory locks");
	is($node->safe_psql(
			'postgres',
			"SELECT bool_and(pg_terminate_backend(pid, 5000)) FROM unnest(ARRAY["
			  . join(',', @reap_terminate_pids)
			  . "]) AS p(pid);"),
		't',
		"PMChild reaping stress cycle $cycle accepted active terminate requests");

	foreach my $session (@reap_abandoned_stress)
	{
		$session->{run}->kill_kill;
		eval { $session->{run}->finish; };
	}
	foreach my $fatal (@reap_fatal_stress)
	{
		ok(pump_until($fatal->{run}, $fatal->{timer},
				$fatal->{stderr}, qr/test_backend_runtime requested FATAL/),
			"PMChild reaping stress cycle $cycle FATAL backend reported test FATAL");
		eval { $fatal->{run}->finish; };
	}
	foreach my $session (@reap_terminate_stress)
	{
		eval { $session->{run}->finish; };
	}

	wait_for_pids_to_leave_pg_stat_activity(
		[ @reap_abandoned_pids, @reap_terminate_pids, @reap_fatal_pids ],
		"PMChild reaping stress cycle $cycle cleaned up all logical backends");
	wait_for_advisory_lock_count($lock_low, $lock_high, '0',
		"PMChild reaping stress cycle $cycle released abandoned-client advisory locks");
	is($node->safe_psql('postgres', 'SELECT 42;'), '42',
		"threaded server remains usable after PMChild reaping stress cycle $cycle");
}

wait_for_reclaimed_top_memory_context_logs(
	$pmchild_reap_reclaim_logs_before + $pmchild_reap_expected_reclaims,
	'PMChild reaping stress logged reclaimed TopMemoryContext accounting for repeated exits');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 1
	  if $^O eq 'MSWin32';

	is(postmaster_child_count(), $pmchild_reap_children_before,
		'PMChild reaping stress did not leak postmaster child processes');
}

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

$node->safe_psql('postgres', 'DROP EXTENSION test_backend_runtime_threaded;');
is($node->safe_psql(
		'postgres',
		q{SELECT NOT EXISTS (
	SELECT 1 FROM pg_extension WHERE extname = 'test_backend_runtime_threaded'
);}),
	't', 'threaded extension DDL drops thread-compatible extension');
is($node->safe_psql('postgres', 'SELECT 42;'), '42',
	'threaded server remains usable after threaded extension drop');

is($node->safe_psql(
		'postgres',
		q{
LOAD 'pg_plan_advice';
SET pg_plan_advice.advice = 'SEQ_SCAN(threaded_runtime_stress)';
SELECT current_setting('pg_plan_advice.advice') =
       'SEQ_SCAN(threaded_runtime_stress)';
}),
	't', 'threaded runtime loads pg_plan_advice module state');

SKIP:
{
	skip 'postmaster child counting smoke is Unix-specific', 1
	  if $^O eq 'MSWin32';

	is(postmaster_child_count(), 0,
		'threaded runtime still has no postmaster child processes after worker activity');
}

my $final_log = wait_for_reclaimed_top_memory_context_logs(1,
	'server log records reclaimed TopMemoryContext accounting');

unlike(
	$final_log,
	qr/PANIC|segmentation|unsupported byval|could not find tuple|server process .* was terminated|was terminated by signal|retained \d+ bytes in TopMemoryContext at exit/,
	'server log has no threaded-runtime crash/corruption or retained TopMemoryContext signatures');

$node->stop('fast');

done_testing();
