# Copyright (c) 2026, PostgreSQL Global Development Group

# Dead-end clients must receive their startup rejection, not run a worker's
# main_fn(NULL, 0).  Reverting the dead-end copy/dispatch in launch_backend.c
# makes the threaded lane assert in BackendMain (or crash without assertions).
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

$ENV{PG_XTC_CARRIER_LOOPS} = '2';

for my $mode ('off', 'on')
{
	my $node = PostgreSQL::Test::Cluster->new("dead_end_$mode");
	$node->init;
	$node->append_conf(
		'postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = 0
max_connections = 4
max_wal_senders = 0
reserved_connections = 0
superuser_reserved_connections = 0
autovacuum = off
io_method = sync
ssl = off
authentication_timeout = 120
log_min_messages = debug2
});
	$node->start;

	SKIP:
	{
		skip 'raw connections are unavailable', 7
		  unless $node->raw_connect_works;

		my $sibling = $node->background_psql('postgres');
		is($sibling->query_safe('SELECT 41'), '41',
			"$mode: sibling connected before exhaustion");

		# The client child pool holds 2 * (max_connections + max_wal_senders)
		# entries.  Keep seven more clients waiting for startup, plus the live
		# sibling, to exhaust child slots without exhausting PGPROCs.  Negotiate
		# SSL serially to avoid overflowing the listen backlog.
		my @sockets;
		for (1 .. 7)
		{
			my $sock = $node->raw_connect;
			$sock->send(pack('Nnn', 8, 1234, 5679));
			my $reply = '';
			{
				local $SIG{ALRM} = sub { die "SSL negotiation timed out\n"; };
				alarm $PostgreSQL::Test::Utils::timeout_default;
				$sock->recv($reply, 1);
				alarm 0;
			}
			die "expected SSL rejection, got '$reply'" unless $reply eq 'N';
			push @sockets, $sock;
		}

		my $log_start = -s $node->logfile;
		# Unlike an SSLRequest alone, psql sends a complete startup packet.
		$node->connect_fails(
			'dbname=postgres sslmode=disable connect_timeout=10',
			"$mode: exhausted child slots reject startup",
			expected_stderr => qr/FATAL:  sorry, too many clients already/);
		$node->wait_for_log(qr/releasing dead-end backend/, $log_start);
		like(slurp_file($node->logfile, $log_start),
			qr/allocating dead-end child/,
			"$mode: rejection used a dead-end child, not the PGPROC limit");
		is($sibling->query_safe('SELECT 42'), '42',
			"$mode: sibling survives dead-end rejection");

		$_->close for @sockets;
		$sibling->quit;
		ok($node->poll_query_until('postgres', 'SELECT 43', '43'),
			"$mode: new connection works after releasing child slots");
		unlike(slurp_file($node->logfile, $log_start),
			qr/TRAP:|PANIC:|terminating threaded server runtime|GENUINE-CRASH/,
			"$mode: no runtime crash");
	}
	ok($node->stop('fast'), "$mode: fast shutdown completes");
}

done_testing();
