# Copyright (c) 2026, PostgreSQL Global Development Group

# A later sentinel launch proves the postmaster has finished the target's
# launch path while the target remains before entry-point lookup.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'startup publication coverage requires injection points'
  unless ($ENV{enable_injection_points} // '') eq 'yes';

$ENV{PG_XTC_CARRIER_LOOPS} = '1';

for my $lane ('process', 'fiber', 'pooled')
{
	my $mode = $lane eq 'process' ? 'off' : 'on';
	my $pool = $lane eq 'pooled' ? 2 : 0;
	my $node = PostgreSQL::Test::Cluster->new("startup_publication_$lane");
	$node->init;
	$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = $pool
io_method = sync
summarize_wal = off
autovacuum = off
max_connections = 20
max_worker_processes = 8
max_logical_replication_workers = 0
log_min_messages = debug1
statement_timeout = 0
});
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION test_backend_runtime_threaded');
	is($node->safe_psql('postgres', 'SHOW multithreaded'),
		$mode, "$lane: backend mode");
	is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'),
		"$pool", "$lane: protocol carrier setting");
	if ($mode eq 'on')
	{
		my $pool_label = $lane eq 'pooled'
		  ? 'worker-fiber pool; client backends on pooled carriers'
		  : 'thread-per-session backend pool';
		like(slurp_file($node->logfile),
			qr/xtc: carrier scheduler thread up \(1 loop, 1 supervisor, \Q$pool_label\E\)/,
			"$lane: one worker fiber loop");
	}

	my $fixture_failed = 0;
	for my $case ('normal', 'missing', 'clean', 'terminate')
	{
		my ($stdout, $stderr, $timed_out);
		my $ret = $node->psql('postgres',
			"SELECT test_backend_runtime_bgworker_startup_publication('$case');",
			stdout => \$stdout, stderr => \$stderr,
			timeout => 60, timed_out => \$timed_out);
		my $completed = !$timed_out && defined($ret) && $ret == 0;
		ok($completed, "$lane/$case: observations returned after cleanup")
		  or diag("stdout=$stdout, stderr=$stderr");
		if (!$completed)
		{
			# No further calls into a node whose fixture cleanup failed.
			$fixture_failed = 1;
			last;
		}
		my %observed = map { split /=/, $_, 2 } split /;/, $stdout;
		is($observed{carrier}, $mode eq 'off' ? 'process' : 'fiber',
			"$lane/$case: actual worker carrier");
		is($observed{held}, $mode eq 'off' ? 'STARTED' : 'NOT_YET_STARTED',
			"$lane/$case: held startup visibility");
		is($observed{held_pid}, $mode eq 'off' ? '1' : '0',
			"$lane/$case: published PID while held");
		is($observed{startup}, $case eq 'normal' ? 'STARTED' : 'STOPPED',
			"$lane/$case: startup result");
		is($observed{startup_pid}, $case eq 'normal' ? '1' : '0',
			"$lane/$case: startup PID");
		is($observed{main}, $case eq 'normal' ? '1' : '0',
			"$lane/$case: entry-point acknowledgment");
		is($observed{stopped}, 'STOPPED', "$lane/$case: shutdown result");
		if ($case eq 'missing')
		{
			like(slurp_file($node->logfile),
				qr/could not find function "test_backend_runtime_missing_startup_main"/,
				"$lane: missing entry fails during lookup");
		}
		is($node->safe_psql('postgres', 'SELECT 1'), '1',
			"$lane/$case: server remains usable");
	}

	my $driver;
	for my $action ('cancel', 'terminate')
	{
		last if $fixture_failed;
		my $label = "$lane/driver_$action";
		my $completed = eval {
			$driver = $node->background_psql('postgres', timeout => 10,
				on_error_stop => 0, extra_params => ['-v', 'VERBOSITY=verbose']);
			$driver->set_query_timer_restart;
			$driver->query_safe('SET client_min_messages = notice');
			my $pid = $driver->query_safe('SELECT pg_backend_pid()');
			$driver->{timeout}->start;
			$driver->{stdin} .=
			  "SELECT test_backend_runtime_bgworker_startup_publication('driver_hold');\n";
			ok(pump_until($driver->{run}, $driver->{timeout},
				\$driver->{stderr}, qr/startup driver ready: \Q$pid\E\r?\n/),
				"$label: target held and later sentinel started") or die "driver not ready";

			# The later sentinel launch excludes earlier fixtures' unregister logs.
			my $offset = -s $node->logfile;
			$driver->{timeout}->start;
			my $signal = $action eq 'cancel'
			  ? "pg_cancel_backend($pid)" : "pg_terminate_backend($pid, 5000)";
			is($node->safe_psql('postgres', "SELECT $signal", timeout => 10),
				't', "$label: SQL signal accepted") or die "signal failed";
			my $event = $action eq 'cancel'
			  ? qr/ERROR:  57014: canceling statement due to user request/
			  : qr/FATAL:  57P01: terminating connection due to administrator command/;
			ok(pump_until($driver->{run}, $driver->{timeout},
				\$driver->{stderr}, $event) && !$driver->{timeout}->is_expired,
				"$label: requested interrupt received within 10 seconds")
			  or die "interrupt not received";
			if ($action eq 'cancel')
			{
				is($driver->query(q{\echo :SQLSTATE}), '57014',
					"$label: canceled query SQLSTATE") or die "wrong SQLSTATE";
				$driver->{stderr} = '';
				is($driver->query_safe('SELECT 1'), '1',
					"$label: same session recovers") or die "session did not recover";
				$driver->quit;
			}
			else
			{
				# Noninteractive psql exits on connection loss; do not query it.
				$driver->{run}->finish;
			}
			pass("$label: driver psql reaped");
			$driver = undef;
			local $PostgreSQL::Test::Utils::timeout_default = 10;
			for my $worker ('target', 'sentinel')
			{
				$node->wait_for_log(
					qr/DEBUG:  unregistering background worker "test_backend_runtime startup $worker"/,
					$offset);
				pass("$label: $worker registration slot reclaimed");
			}
			die "startup fixture cleanup timed out"
			  if slurp_file($node->logfile, $offset) =~ /startup test cleanup timed out/;
			my $result = $node->safe_psql('postgres',
				"SELECT test_backend_runtime_bgworker_startup_publication('normal')",
				timeout => 30);
			like($result, qr/;startup=STARTED;startup_pid=1;main=1;stopped=STOPPED;/,
				"$label: same injection name reattaches and worker starts and stops")
			  or die "reattachment fixture failed";
			1;
		};
		if (!$completed)
		{
			my $error = $@;
			fail("$label: cleanup scenario completed");
			diag($error);
			diag($driver->{stderr}) if defined $driver;
			last;
		}
	}

	my $stopped = $node->stop('fast', timeout => 10, fail_ok => 1);
	ok($stopped, "$lane: clean fast shutdown");
	$node->stop('immediate', timeout => 5) unless $stopped;
	if (defined $driver)
	{
		# Reap a failed scenario only after stopping its node.
		eval {
			$driver->{timeout}->start;
			$driver->{run}->pump_nb;
			$driver->{stdin} .= "\\q\n" if $driver->{run}->pumpable;
			$driver->{run}->finish;
		};
		diag($@) if $@;
	}
	unlike(slurp_file($node->logfile),
		qr/startup (?:gate|driver hold|test cleanup) timed out|retained .*TopMemoryContext|retained runtime-owned roots|PANIC:|segmentation fault|TRAP:|was terminated by signal/i,
		"$lane: no fixture timeouts, retained roots or crash diagnostics");
}

done_testing();
