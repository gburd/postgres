# Copyright (c) 2026, PostgreSQL Global Development Group

# A database move sends an SMGR barrier to sleeping workers.  With one fiber
# loop, SetLatch must wake a sibling fiber, not the setter's own self-pipe.
# Keep a separate no-setter deadline control: a lost wake alone does not
# explain a worker remaining asleep beyond its finite WaitLatch timeout.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

$ENV{PG_XTC_CARRIER_LOOPS} = '1';

for my $mode ('off', 'on')
{
	my $node = PostgreSQL::Test::Cluster->new("same_loop_latch_$mode");
	$node->init;
	$node->append_conf(
		'postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = 0
io_method = sync
summarize_wal = off
max_connections = 20
max_worker_processes = 8
max_logical_replication_workers = 4
});
	$node->start;

	is($node->safe_psql('postgres', 'SHOW multithreaded'),
		$mode, "$mode: requested backend model is active");
	is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'),
		'0', "$mode: stackless protocol pool is disabled");
	if ($mode eq 'on')
	{
		like(slurp_file($node->logfile),
			qr/xtc: carrier scheduler thread up \(1 loop, 1 supervisor, thread-per-session backend pool\)/,
			'fiber: exactly one carrier loop');
	}

	ok($node->poll_query_until('postgres', q{
		SELECT count(*) = 3 FROM pg_stat_activity
		WHERE backend_type IN
			('checkpointer', 'background writer', 'logical replication launcher')
	}), "$mode: barrier participants are running");
	ok($node->poll_query_until('postgres', q{
		SELECT count(*) = 1 FROM pg_stat_activity
		WHERE backend_type = 'logical replication launcher'
		  AND wait_event = 'LogicalLauncherMain'
	}), "$mode: logical launcher is waiting before the barrier");

	# pg_sleep uses WaitLatch with a finite timeout, without a test setter.
	# Do not arm statement_timeout here: it could rescue a broken sleep timer.
	my ($stdout, $stderr, $timed_out);
	my $start = clock_gettime(CLOCK_MONOTONIC);
	my $ret = $node->psql('postgres', 'SELECT pg_sleep(1); SELECT 1;',
		stdout => \$stdout, stderr => \$stderr,
		timeout => 5, timed_out => \$timed_out);
	my $elapsed = clock_gettime(CLOCK_MONOTONIC) - $start;
	ok(!$timed_out && defined($ret) && $ret == 0 && $stdout =~ /1$/
		&& $elapsed >= 1 && $elapsed < 5,
		"$mode: one-second no-setter deadline completes")
	  or diag("elapsed=$elapsed, stdout=$stdout, stderr=$stderr");

	my $tablespace = PostgreSQL::Test::Utils::tempdir;
	$tablespace =~ s/'/''/g;
	$ret = $node->psql('postgres', qq{
		SET statement_timeout = '10s';
		CREATE TABLESPACE regress_tblspace LOCATION '$tablespace';
		CREATE DATABASE regression_utf8 TEMPLATE template0 ENCODING 'UTF8';
		ALTER DATABASE regression_utf8 SET TABLESPACE regress_tblspace;
		SELECT dattablespace = (SELECT oid FROM pg_tablespace
			WHERE spcname = 'regress_tblspace')
		FROM pg_database WHERE datname = 'regression_utf8';
	}, stdout => \$stdout, stderr => \$stderr,
		timeout => 15, timed_out => \$timed_out);
	ok(!$timed_out && defined($ret) && $ret == 0 && $stdout eq 't',
		"$mode: database move wakes barrier participants before timeout")
	  or diag("stdout=$stdout, stderr=$stderr");

	# A failing wake must not turn test cleanup into a three-minute worker nap.
	if (!$node->stop('fast', timeout => 5, fail_ok => 1))
	{
		$node->stop('immediate', timeout => 5);
	}
}

done_testing();
