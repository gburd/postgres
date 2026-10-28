# Copyright (c) 2026, PostgreSQL Global Development Group

# An empty parallel index build reaches the attachment wait before its worker
# attaches.  On one fiber loop, polling a STARTED worker without waiting can
# prevent the worker from running at all.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

$ENV{PG_XTC_CARRIER_LOOPS} = '1';

for my $mode ('off', 'on')
{
	my $node = PostgreSQL::Test::Cluster->new("parallel_attach_$mode");
	$node->init;
	$node->append_conf(
		'postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = 0
io_method = sync
summarize_wal = off
max_connections = 20
max_worker_processes = 8
max_parallel_workers = 4
max_parallel_maintenance_workers = 4
max_logical_replication_workers = 4
log_min_messages = debug1
statement_timeout = 0
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

	# Keep the empty-table expression-index case from btree_index.sql.  Use an
	# external deadline: a busy fiber can also prevent statement_timeout from
	# being delivered on the same loop.
	my ($stdout, $stderr, $timed_out);
	my $ret = $node->psql('postgres', q{
		CREATE TABLE btree_test_expr (n int);
		CREATE FUNCTION btree_test_func() RETURNS int LANGUAGE sql IMMUTABLE RETURN 0;
		BEGIN;
		SET LOCAL min_parallel_table_scan_size = 0;
		SET LOCAL max_parallel_maintenance_workers = 4;
		CREATE INDEX btree_test_expr_idx ON btree_test_expr USING btree (btree_test_func());
		COMMIT;
		DROP TABLE btree_test_expr;
		DROP FUNCTION btree_test_func();
		SELECT 1;
	}, stdout => \$stdout, stderr => \$stderr,
		timeout => 15, timed_out => \$timed_out);
	ok(!$timed_out && defined($ret) && $ret == 0 && $stdout eq '1',
		"$mode: parallel expression index completes before deadline")
	  or diag("stdout=$stdout, stderr=$stderr");

	# Completion alone could be a serial fallback when no worker is available.
	my $carrier = $mode eq 'on' ? 'thread carrier' : 'process';
	like(slurp_file($node->logfile),
		qr/starting background worker $carrier "parallel worker for PID \d+"/,
		"$mode: index build launched a parallel worker");

	# A stuck attach wait must not leave cleanup waiting for the busy fiber.
	if (!$node->stop('fast', timeout => 5, fail_ok => 1))
	{
		$node->stop('immediate', timeout => 5);
	}
}

done_testing();
