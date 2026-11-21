# Copyright (c) 2026, PostgreSQL Global Development Group

# DROP must stop an attached apply worker even before ProcSignalInit.
# This fixture selects a startup window; it does not fabricate a failed send.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC usleep);

plan skip_all => 'subscription startup coverage requires injection points'
  unless ($ENV{enable_injection_points} // '') eq 'yes';
$ENV{PG_XTC_CARRIER_LOOPS} = '4';

# Polling budgets exclude the final query's separate three-second client bound.
sub await_sql
{
	my ($session, $sql, $seconds) = @_;
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + $seconds;
	do
	{
		return 1 if $session->query_safe($sql) eq 't';
		usleep(20_000);
	} while (clock_gettime(CLOCK_MONOTONIC) < $deadline);
	return 0;
}

for my $case ('fiber_early', 'fiber_late', 'process_early', 'fiber_release')
{
	my $mode = $case eq 'process_early' ? 'off' : 'on';
	my $node = PostgreSQL::Test::Cluster->new("subscription_$case");
	$node->init;
	$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
io_method = sync
summarize_wal = off
autovacuum = off
max_connections = 20
max_worker_processes = 12
max_logical_replication_workers = 4
max_wal_senders = 0
wal_level = logical
log_min_messages = debug1
statement_timeout = 0
});
	$node->start;
	my ($driver, $keeper);
	my $completed = eval {
		$node->safe_psql('postgres', 'CREATE EXTENSION test_backend_runtime_threaded');
		$driver = $node->background_psql('postgres', timeout => 10, on_error_stop => 0,
			extra_params => ['-v', 'VERBOSITY=verbose']);
		$keeper = $node->background_psql('postgres', timeout => 3);
		$driver->set_query_timer_restart;
		$keeper->set_query_timer_restart;
		$keeper->query_safe("SET statement_timeout = '2s'");
		my $pid = $driver->query_safe('SELECT pg_backend_pid()');
		is($driver->query_safe('SHOW multithreaded'), $mode, "$case: backend mode");
		if ($mode eq 'on')
		{
			like(slurp_file($node->logfile), qr/xtc: carrier scheduler thread up \(4 loops,/,
				"$case: four worker carrier loops");
		}
		my $create_state = $driver->query(q{
CREATE SUBSCRIPTION startup_sub CONNECTION 'dbname=subscription_startup_nonexistent connect_timeout=1'
PUBLICATION mypub WITH (connect=false, enabled=false, create_slot=false, copy_data=false);
\echo :SQLSTATE
});
		is($create_state, '00000', "$case: disconnected CREATE succeeds")
		  or die 'CREATE failed';
		like($driver->{stderr}, qr/\Apsql:<stdin>:[0-9]+: WARNING:  01000: subscription was created, but is not connected\r?\nHINT:  To initiate replication, you must manually create the replication slot, enable the subscription, and alter the subscription to refresh publications\.\r?\nLOCATION:  CreateSubscription, subscriptioncmds\.c:[0-9]+(?:\r?\n)?\z/,
			"$case: only expected disconnected CREATE warning")
		  or die 'unexpected CREATE diagnostic';
		$driver->{stderr} = '';
		my $oid = $driver->query_safe("SELECT oid FROM pg_subscription WHERE subname = 'startup_sub'");
		my $handle = $keeper->query_safe("SELECT test_backend_runtime_subscription_arm($oid)");
		my $status = "test_backend_runtime_subscription_gate($handle, 'status')";
		$driver->query_safe('ALTER SUBSCRIPTION startup_sub ENABLE');
		ok(await_sql($keeper, "SELECT split_part($status, '|', 1) = '1'", 10),
			"$case: actual apply worker entered pre-auth gate") or die 'gate entry missing';
		my @s = split /\|/, $keeper->query_safe("SELECT $status");
		my ($worker, $generation) = @s[4, 5];
		is($s[8], $mode eq 'off' ? 'process' : 'fiber', "$case: actual worker carrier");
		is($s[9], '1', "$case: registered worker matches generation, PGPROC and backend identity");
		note("$case: handle=$handle subscription=$oid worker=$worker generation=$generation procno=$s[6] backend_id=$s[7]");
		my $lock = qq{SELECT EXISTS (
SELECT FROM pg_locks w JOIN pg_locks d USING (locktype, database, classid, objid, objsubid)
WHERE w.locktype='object' AND w.classid='pg_subscription'::regclass
AND w.database=0 AND w.objid=$oid AND w.objsubid=0
AND w.pid=$worker AND NOT w.granted AND w.mode='AccessShareLock'
AND d.pid=$pid AND d.granted AND d.mode='AccessExclusiveLock')};
		my $done = "SELECT state = 'idle in transaction' AND query = 'DROP SUBSCRIPTION startup_sub;' FROM pg_stat_activity WHERE pid=$pid";
		my $gone = "SELECT split_part($status, '|', 11) = '0' AND split_part($status, '|', 3) = '1'";
		my $offset = -s $node->logfile;
		if ($case eq 'fiber_release')
		{
			# Commit DISABLE before release: no publisher connection is attempted.
			$driver->query_safe('ALTER SUBSCRIPTION startup_sub DISABLE');
			$keeper->query_safe("SELECT test_backend_runtime_subscription_gate($handle, 'release')");
			ok(await_sql($keeper, $gone, 10), "$case: released startup exits without DROP") or die 'worker still live';
			$driver->query_safe('ALTER SUBSCRIPTION startup_sub SET (slot_name=NONE)');
			$driver->query_safe('DROP SUBSCRIPTION startup_sub');
		}
		else
		{
			$driver->query_safe('BEGIN; ALTER SUBSCRIPTION startup_sub DISABLE; ALTER SUBSCRIPTION startup_sub SET (slot_name=NONE)');
			is($keeper->query_safe(qq{SELECT count(*) FROM pg_locks WHERE pid=$pid
AND locktype='object' AND classid='pg_subscription'::regclass AND objid=$oid
AND database=0 AND granted AND mode='AccessExclusiveLock'}), '1', "$case: driver holds subscription lock");
			if ($case eq 'fiber_late')
			{
				$keeper->query_safe("SELECT test_backend_runtime_subscription_gate($handle, 'release')");
				ok(await_sql($keeper, $lock, 10), "$case: worker reaches driver lock before DROP") or die 'late lock edge missing';
				is((split /\|/, $keeper->query_safe("SELECT $status"))[9], '1', "$case: same worker before late send");
			}
			my $drop_output = $driver->query_until(qr/drop_start\r?\n/, "\\echo drop_start\nDROP SUBSCRIPTION startup_sub;\n\\echo drop_result :SQLSTATE\n");
			if ($case ne 'fiber_late')
			{
				ok(await_sql($keeper, qq{SELECT query = 'DROP SUBSCRIPTION startup_sub;' AND (wait_event='BgworkerShutdown' OR state='idle in transaction')
FROM pg_stat_activity WHERE pid=$pid}, 5), "$case: DROP reaches stop wait or completes") or die 'DROP did not reach stop';
				if ($case eq 'process_early')
				{
					ok(await_sql($keeper, $done, 5), "$case: native termination completes while gate remains closed")
					  or die 'fork termination failed';
					ok(await_sql($keeper, "SELECT split_part($status, '|', 3) = '1' AND split_part($status, '|', 11) = '0' AND split_part($status, '|', 2) = '0'", 5),
						"$case: terminated callback and worker gone before release") or die 'fork callback still live';
				}
				$keeper->query_safe("SELECT test_backend_runtime_subscription_gate($handle, 'release')");
			}
			if ($case eq 'fiber_early')
			{
				my $edge_or_done = "SELECT ($lock) OR ($done)";
				ok(await_sql($keeper, $edge_or_done, 5),
					"$case: released worker reaches lock edge or DROP completes") or die 'post-release progress missing';
			}
			my $dropped = await_sql($keeper, $done, 5);
			ok($dropped, "$case: DROP completes in five-second polling budget without cleanup intervention");
			if (!$dropped)
			{
				# This is evidence, not an expected-error pass or an ESRCH oracle.
				is($keeper->query_safe($lock), 't', "$case: stalled worker waits on this driver's exact subscription lock");
				is($keeper->query_safe("SELECT wait_event FROM pg_stat_activity WHERE pid=$pid"), 'BgworkerShutdown', "$case: stalled DROP waits for worker shutdown");
				my @now = split /\|/, $keeper->query_safe("SELECT $status");
				is($now[9], '1', "$case: stalled generation and PGPROC still match");
				is($now[2], '1', "$case: worker has left injection gate");
				diag($keeper->query_safe("SELECT pid, state, wait_event_type, wait_event, query FROM pg_stat_activity WHERE pid IN ($pid,$worker)"));
				diag($keeper->query_safe("SELECT locktype,database,classid,objid,objsubid,pid,mode,granted FROM pg_locks WHERE pid IN ($pid,$worker)"));
				is($keeper->query_safe("SELECT pg_cancel_backend($pid)"), 't', "$case: cleanup cancel accepted");
			}
			$drop_output .= $driver->query_until(qr/drop_result [0-9A-Z]{5}\r?\n/, '')
			  unless $drop_output =~ /drop_result [0-9A-Z]{5}\r?\n/;
			like($drop_output, $dropped ? qr/drop_result 00000/ : qr/drop_result 57014/,
				"$case: DROP SQLSTATE distinguishes success from cleanup cancel");
			# query() consumes both stream separators; query_until() only drains stdout.
			my $state = $driver->query(q{\echo :SQLSTATE});
			is($state, $dropped ? '00000' : '57014', "$case: SQLSTATE after two-stream barrier");
			if (!$dropped)
			{
				like($driver->{stderr}, qr/ERROR:  57014: canceling statement due to user request/,
					"$case: expected cancellation error drained before rollback");
				$driver->{stderr} = '';
			}
			$driver->query_safe($dropped ? 'COMMIT' : 'ROLLBACK');
			$driver->query_safe('ALTER SUBSCRIPTION startup_sub DISABLE') unless $dropped;
			ok(await_sql($keeper, $gone, 10), "$case: worker slots no longer in use") or die 'worker cleanup failed';
			if (!$dropped)
			{
				$driver->query_safe('ALTER SUBSCRIPTION startup_sub SET (slot_name=NONE)');
				$driver->query_safe('DROP SUBSCRIPTION startup_sub');
			}
		}
		my @final = split /\|/, $keeper->query_safe("SELECT $status");
		is($final[2], '1', "$case: callback detached its worker mapping");
		is($final[3], '0', "$case: no autonomous gate timeout");
		{
			local $PostgreSQL::Test::Utils::timeout_default = 10;
			$node->wait_for_log(qr/unregistering background worker "logical replication apply worker for subscription $oid"/, $offset);
		}
		pass("$case: background worker registration released");
		is($driver->query_safe('SELECT 1'), '1', "$case: original driver usable");
		is($keeper->query_safe('SELECT 1'), '1', "$case: original keeper usable");
		$keeper->query_safe("SELECT test_backend_runtime_subscription_gate($handle, 'close')");
		$driver->quit;
		$driver = undef;
		$keeper->quit;
		$keeper = undef;
		1;
	};
	if (!$completed)
	{
		fail("$case: fixture handshakes and cleanup complete");
		diag($@);
	}
	my $stopped = $node->stop('fast', timeout => 10, fail_ok => 1);
	ok($stopped, "$case: bounded fast shutdown");
	$node->stop('immediate', timeout => 5) unless $stopped;
	for my $session ($driver, $keeper)
	{
		next unless defined $session;
		eval { $session->{timeout}->start; $session->{run}->finish; };
		diag($@) if $@;
	}
	unlike(slurp_file($node->logfile), qr/subscription gate timed out|retained runtime-owned roots|PANIC:|segmentation fault|TRAP:/i,
		"$case: no gate timeout, retained roots or crash");
}
done_testing();
