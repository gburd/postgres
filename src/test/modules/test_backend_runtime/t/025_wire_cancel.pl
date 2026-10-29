# Copyright (c) 2026, PostgreSQL Global Development Group

# Wire cancellation must authenticate BackendKeyData, not an OS PID alone.
# Keep keys opaque and in memory; never include packets in TAP diagnostics.
use strict;
use warnings FATAL => 'all';

use IO::Select;
use Socket qw(SHUT_WR);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'Unix sockets required' unless $use_unix_sockets;

sub now { return clock_gettime(CLOCK_MONOTONIC); }

# Exact I/O with one deadline for the whole frame, not a timeout per byte.
sub read_exact
{
	my ($sock, $length, $deadline) = @_;
	my $data = '';
	while (length($data) < $length)
	{
		my $left = $deadline - now();
		die "wire read deadline\n"
		  unless $left > 0 && IO::Select->new($sock)->can_read($left);
		my $n = sysread($sock, my $part, $length - length($data));
		die "wire read failed or unexpected EOF\n" unless defined($n) && $n > 0;
		$data .= $part;
	}
	return $data;
}

sub write_packet
{
	my ($sock, $packet) = @_;
	my $deadline = now() + 3;
	my $offset = 0;
	while ($offset < length($packet))
	{
		my $left = $deadline - now();
		die "wire write deadline\n"
		  unless $left > 0 && IO::Select->new($sock)->can_write($left);
		my $n = syswrite($sock, $packet, length($packet) - $offset, $offset);
		die "wire write failed\n" unless defined($n) && $n > 0;
		$offset += $n;
	}
}

# Only startup and single-column simple-query responses are needed here.
# Consume full frames through ReadyForQuery, including ErrorResponse.
sub response
{
	my ($session) = @_;
	my $sock = $session->{sock};
	my $deadline = now() + 8;
	my $result = { states => [], rows => [] };
	for (1 .. 1000)
	{
		my ($type, $length) = unpack('aN', read_exact($sock, 5, $deadline));
		die "invalid response length\n" unless $length >= 4 && $length <= 65536;
		my $body = read_exact($sock, $length - 4, $deadline);
		if ($type eq 'K')
		{
			die "invalid BackendKeyData\n" unless length($body) >= 8;
			$session->{pid} = unpack('N', substr($body, 0, 4));
			$session->{key} = substr($body, 4);
		}
		elsif ($type eq 'R')
		{
			die "expected trust authentication\n" unless $body eq pack('N', 0);
		}
		elsif ($type eq 'E')
		{
			my @codes = $body =~ /(?:^|\0)C([0-9A-Z]{5})\0/g;
			die "missing SQLSTATE\n" unless @codes == 1;
			push @{ $result->{states} }, $codes[0];
		}
		elsif ($type eq 'D')
		{
			die "expected one text column\n"
			  unless length($body) >= 6 && unpack('n', $body) == 1;
			my $size = unpack('N', substr($body, 2, 4));
			die "invalid text column length\n" unless $size == length($body) - 6;
			push @{ $result->{rows} }, substr($body, 6);
		}
		elsif ($type eq 'Z')
		{
			die "invalid ReadyForQuery\n" unless $body =~ /^[ITE]$/;
			$result->{ready} = $body;
			return $result;
		}
		elsif ($type !~ /^[SNTCI]$/)
		{
			die "unexpected response type\n";
		}
	}
	die "too many response messages\n";
}

sub query
{
	my ($session, $sql) = @_;
	write_packet($session->{sock}, 'Q' . pack('N', length($sql) + 5) . $sql . "\0");
}

sub scalar_query
{
	my ($session, $sql) = @_;
	query($session, $sql);
	my $r = response($session);
	die "scalar query failed\n" if @{ $r->{states} } || @{ $r->{rows} } != 1;
	return $r->{rows}[0];
}

sub connect_session
{
	my ($node, $minor, $user, $sock) = @_;
	my $session = { sock => $sock // $node->raw_connect };
	my $body = pack('N', 196608 + $minor)
	  . "user\0$user\0database\0postgres\0\0";
	write_packet($session->{sock}, pack('N', length($body) + 4) . $body);
	my $r = response($session);
	die "startup failed\n"
	  if @{ $r->{states} } || !defined($session->{key}) || $r->{ready} ne 'I';
	is(length($session->{key}), $minor == 0 ? 4 : 32,
		"protocol 3.$minor: advertised key length");
	is(scalar_query($session, 'SELECT pg_backend_pid()'), $session->{pid},
		'wire identity matches SQL identity');
	return $session;
}

sub cancel_packet
{
	my ($pid, $key) = @_;
	return pack('NNN', 12 + length($key), 80877102, $pid) . $key;
}

# Read to EOF, which confirms the receiver finished processing this packet.
# Malformed requests may send an error; their bytes must not enter diagnostics.
sub cancel_closed
{
	my ($sock) = @_;
	shutdown($sock, SHUT_WR) or die "cancel shutdown failed\n";
	my $deadline = now() + 3;
	my $total = 0;
	while (1)
	{
		my $left = $deadline - now();
		die "cancel close deadline\n"
		  unless $left > 0 && IO::Select->new($sock)->can_read($left);
		my $n = sysread($sock, my $ignored, 4096);
		die "cancel read failed\n" unless defined $n;
		last if $n == 0;
		$total += $n;
		die "excess cancel response\n" if $total > 65536;
	}
	close($sock);
}

sub send_cancel
{
	my ($node, $packet) = @_;
	my $sock = $node->raw_connect;
	write_packet($sock, $packet);
	cancel_closed($sock);
}

sub wait_sql
{
	my ($observer, $sql) = @_;
	my $deadline = now() + 3;
	while (now() < $deadline)
	{
		return if scalar_query($observer, $sql) eq 't';
		sleep 0.01;
	}
	die "activity synchronization deadline\n";
}

sub sleeping
{
	my ($session, $observer) = @_;
	query($session, 'SELECT pg_sleep(5)');
	wait_sql($observer, "SELECT EXISTS (SELECT FROM pg_stat_activity
		WHERE pid = $session->{pid} AND wait_event = 'PgSleep')");
}

sub not_canceled
{
	my ($session, $label) = @_;
	ok(!IO::Select->new($session->{sock})->can_read(0.1), $label);
}

sub canceled_and_usable
{
	my ($session, $label) = @_;
	my $r = response($session);
	is_deeply($r->{states}, ['57014'], "$label: query canceled");
	is($r->{ready}, 'I', "$label: ready after error");
	is(scalar_query($session, 'SELECT 42'), '42', "$label: session usable");
}

# Each unfinished startup creates new-connection demand and pins a carrier.
sub startup_holders
{
	my ($node, $count) = @_;
	my @held;
	for (1 .. $count)
	{
		my $sock = $node->raw_connect;
		write_packet($sock, pack('Nnn', 8, 1234, 5679));
		is(read_exact($sock, 1, now() + 3), 'N', 'SSL rejected before startup');
		push @held, $sock;
	}
	return @held;
}

# Unfinished startup pins a native pooled carrier.  Do not run this fixture
# with pooled4: the fifth SSL refusal would wait for a carrier held by us.
sub dead_end_cancel
{
	my ($node, $observer, $user, $lane, $a, $holders) = @_;

	# TAP018's child-pool saturation fixture, with a wire cancel instead of
	# ordinary startup: two live clients plus six pre-startup connections.
	$a //= connect_session($node, 2, $user);
	my @held = defined($holders) ? @$holders : startup_holders($node, 6);
	if ($lane eq 'pooled8-dead-end')
	{
		# Six startup holders plus the sleeping target and querying observer
		# require eight native pooled carriers, not eight libxtc loops.
		wait_sql($observer, q{SELECT value = 8
			FROM pg_stat_xtc_runtime WHERE counter = 'carriers_started'});
		my $started = scalar_query($observer, q{SELECT value
			FROM pg_stat_xtc_runtime WHERE counter = 'carriers_started'});
		is($started, '8', 'eight native pooled carriers actually started');
		die "pooled8 carrier capacity not established\n" unless $started eq '8';
	}
	sleeping($a, $observer);
	my $log_start = -s $node->logfile;
	my $bad = $a->{key};
	substr($bad, 0, 1) = chr(ord(substr($bad, 0, 1)) ^ 1);
	send_cancel($node, cancel_packet($a->{pid}, $bad));
	not_canceled($a, 'dead-end receiver rejects wrong key');
	send_cancel($node, cancel_packet($a->{pid}, $a->{key}));
	canceled_and_usable($a, 'dead-end receiver correct pair');
	like(slurp_file($node->logfile, $log_start), qr/allocating dead-end child/,
		'cancel receiver used exhausted child-pool route');
	is(scalar_query($observer, 'SELECT 44'), '44', 'observer remains usable');
	close($_) for @held;
	close($a->{sock});
}

for my $lane ('fork', 'fiber1', 'fiber2', 'pooled4', 'fallback')
{
	subtest $lane => sub {
		my $mode = $lane eq 'fork' ? 'off' : 'on';
		my $pooled = $lane =~ /^(pooled4|fallback)$/ ? 4 : 0;
		my $loops = $lane eq 'fiber2' ? 2 : 1;
		local $ENV{PG_XTC_CARRIER_LOOPS} = $loops;
		my $node = PostgreSQL::Test::Cluster->new("wire_cancel_$lane");
		$node->init;
		# Match TAP013's order for the existing fork+exec/sysv fixture.
		$node->append_conf('postgresql.conf', qq{
xtc_force_process_fallback = on
shared_memory_type = sysv
}) if $lane eq 'fallback';
		$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = $pooled
max_connections = 4
max_wal_senders = 0
reserved_connections = 0
superuser_reserved_connections = 0
autovacuum = off
summarize_wal = off
io_method = sync
ssl = off
authentication_timeout = 120
log_min_messages = debug2
});
		$node->start;
		die "raw connections unavailable\n" unless $node->raw_connect_works;
		my $user = $node->safe_psql('postgres', 'SELECT current_user');
		my $observer = connect_session($node, 0, $user);
		is(scalar_query($observer, 'SHOW multithreaded'), $mode, 'backend model');
		is(scalar_query($observer, 'SHOW pooled_protocol_carriers'), $pooled,
			'configured pool size');
		if ($lane =~ /^fiber/)
		{
			like(slurp_file($node->logfile),
				qr/xtc: carrier scheduler thread up \($loops loops?, $loops supervisors?, thread-per-session backend pool\)/,
				'actual fiber loop count');
		}
		if ($lane eq 'fallback' || $lane eq 'pooled4')
		{
			query($observer, 'CREATE EXTENSION test_backend_runtime_threaded');
			is_deeply(response($observer)->{states}, [], 'runtime fixture loaded');
			my $model = scalar_query($observer,
				'SELECT test_backend_runtime_model_snapshot()');
			like($model, $lane eq 'fallback' ? qr/^process\|process\|/
				: qr/^pooled_protocol\|pooled-protocol-affine\|4\|1$/,
				'actual target runtime');
		}

		for my $minor (0, 2)
		{
			my $label = "protocol 3.$minor";
			my $a = connect_session($node, $minor, $user);
			my $b = connect_session($node, $minor, $user);
			my ($pid, $key) = ($a->{pid}, $a->{key});
			my $first = $key;
			my $last = $key;
			substr($first, 0, 1) = chr(ord(substr($first, 0, 1)) ^ 1);
			substr($last, -1, 1) = chr(ord(substr($last, -1, 1)) ^ 1);
			my @invalid = (
				['wrong first byte', cancel_packet($pid, $first)],
				['wrong last byte', cancel_packet($pid, $last)],
				['other session key', cancel_packet($pid, $b->{key})],
				['short key', cancel_packet($pid, substr($key, 0, length($key) - 1))],
				['long key', cancel_packet($pid, $key . "x")],
				['one byte key', cancel_packet($pid, substr($key, 0, 1))],
				['256 byte key', cancel_packet($pid, $key . ('x' x (256 - length($key))))],
				['empty key', cancel_packet($pid, '')],
				['oversize key', cancel_packet($pid, 'x' x 257)],
				['truncated body', substr(cancel_packet($pid, $key), 0, -1)],
				['missing PID', pack('NN', 8, 80877102)],
				['partial PID', pack('NN', 10, 80877102) . "\0\0"],
				['partial length', "\0\0"],
				['truncated code', pack('N', 8) . "\x04\xd2"],
				['invalid outer length', pack('N', 4)],
				['oversize outer length', pack('N', 100000)],
				['zero PID', cancel_packet(0, $key)],
				['high-bit PID', cancel_packet(0x80000000, $key)],
				['all-bits PID', cancel_packet(0xffffffff, $key)],
				['nonexistent PID', cancel_packet(0x7fffffff, $key)]);
			# Small batches leave ample room inside the finite sleep even on
			# fork+exec.  The unfixed server finishes each sleep rather than hangs.
			while (@invalid)
			{
				sleeping($a, $observer);
				for my $case (splice(@invalid, 0, 4))
				{
					send_cancel($node, $case->[1]);
					not_canceled($a, "$label: $case->[0] rejected");
				}
				send_cancel($node, cancel_packet($pid, $key));
				canceled_and_usable($a, "$label: correct pair after invalid requests");
			}
			is(scalar_query($b, 'SELECT 43'), '43', "$label: sibling usable");

			# Cross-pair rejection must protect both active sessions.
			sleeping($a, $observer);
			sleeping($b, $observer);
			send_cancel($node, cancel_packet($b->{pid}, $key));
			not_canceled($a, "$label: cross-pair leaves key owner running");
			not_canceled($b, "$label: cross-pair leaves PID owner running");
			for my $s ($a, $b)
			{
				send_cancel($node, cancel_packet($s->{pid}, $s->{key}));
				canceled_and_usable($s, "$label: active pair");
			}
			close($b->{sock});
			wait_sql($observer, "SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $b->{pid})");

			# pg_stat_get_backend_idset returns actual ProcNumbers, not wire IDs.
			my $slot_sql = q{SELECT s FROM pg_stat_get_backend_idset() s
				WHERE pg_stat_get_backend_pid(s) = pg_backend_pid()};
			my $slot = scalar_query($a, $slot_sql);
			close($a->{sock});
			wait_sql($observer, "SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $pid)");
			# The normal PGPROC freelist is FIFO.  Observe reuse rather than
			# assuming the first reconnect obtains the retired slot.
			my $reused;
			for (1 .. 8)
			{
				$a = connect_session($node, $minor, $user);
				$reused = scalar_query($a, $slot_sql) eq $slot;
				last if $reused;
				close($a->{sock});
				wait_sql($observer, "SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $a->{pid})");
			}
			ok($reused, "$label: actual PGPROC slot reused");
			die "PGPROC reuse fixture failed\n" unless $reused;
			isnt($a->{pid}, $pid, "$label: slot reuse is not wire identity reuse");
			sleeping($a, $observer);
			send_cancel($node, cancel_packet($pid, $key));
			not_canceled($a, "$label: disconnected pair replay rejected");
			send_cancel($node, cancel_packet($a->{pid}, $key));
			not_canceled($a, "$label: old key with successor identity rejected");
			send_cancel($node, cancel_packet($a->{pid}, $a->{key}));
			canceled_and_usable($a, "$label: successor pair");
			close($a->{sock});
			wait_sql($observer, "SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $a->{pid})");
		}

		# Queue independent receiver connections before waiting for their closes.
		# This exercises overlap, not a deterministic teardown race schedule.
		for my $round (1 .. 3)
		{
			my $a = connect_session($node, 2, $user);
			my $b = connect_session($node, 2, $user);
			my $exiting = connect_session($node, 2, $user);
			sleeping($a, $observer);
			sleeping($b, $observer);
			query($exiting, 'SELECT pg_sleep(0.2)');
			my @receivers;
			for my $packet (cancel_packet($a->{pid}, $a->{key}),
				cancel_packet($b->{pid}, $a->{key}),
				cancel_packet($exiting->{pid}, $exiting->{key}))
			{
				my $sock = $node->raw_connect;
				write_packet($sock, $packet);
				push @receivers, $sock;
			}
			close($exiting->{sock});
			cancel_closed($_) for @receivers;
			not_canceled($b, "overlap $round: wrong pair rejected");
			canceled_and_usable($a, "overlap $round");
			send_cancel($node, cancel_packet($b->{pid}, $b->{key}));
			canceled_and_usable($b, "overlap $round: sibling");
			close($a->{sock});
			close($b->{sock});
			wait_sql($observer, q{SELECT count(*) = 1 FROM pg_stat_activity
				WHERE backend_type = 'client backend'});
		}

		dead_end_cancel($node, $observer, $user, $lane) unless $lane eq 'pooled4';
		close($observer->{sock});
		unlike(slurp_file($node->logfile),
			qr/TRAP:|PANIC:|terminating threaded server runtime|GENUINE-CRASH/,
			'no crash during authentication and teardown');
		ok($node->stop('fast', timeout => 10, fail_ok => 1), 'fast shutdown')
		  or $node->stop('immediate', timeout => 5);
		done_testing();
	};
}

subtest 'pooled8-dead-end' => sub {
	my ($ncpus, $cpu_error);
	IPC::Run::run(['getconf', '_NPROCESSORS_ONLN'],
		'>' => \$ncpus, '2>' => \$cpu_error)
	  or die "could not determine online CPU count: $cpu_error";
	$ncpus =~ /^\s*([1-9][0-9]*)\s*$/
	  or die "invalid online CPU count\n";
	$ncpus = $1;
	plan skip_all => 'pooled8 dead-end coverage requires eight online CPUs'
	  if $ncpus < 8;

	local $ENV{PG_XTC_CARRIER_LOOPS} = 1;
	my $node = PostgreSQL::Test::Cluster->new('wire_cancel_pooled8_dead_end');
	$node->init(extra => ['--username=postgres']);
	$node->append_conf('postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 8
max_connections = 4
max_wal_senders = 0
reserved_connections = 0
superuser_reserved_connections = 0
autovacuum = off
summarize_wal = off
io_method = sync
ssl = off
authentication_timeout = 120
log_min_messages = debug2
});
	$node->start;
	# Grow via eight new startups, not resumes of already parked sessions:
	# resume-growth does not include the carriers pinned by startup holders.
	my @held = startup_holders($node, 8);
	my $observer = connect_session($node, 0, 'postgres', shift @held);
	my $a = connect_session($node, 2, 'postgres', shift @held);
	is(scalar_query($observer, 'SHOW multithreaded'), 'on', 'threaded mode');
	is(scalar_query($observer, 'SHOW pooled_protocol_carriers'), '8',
		'eight pooled carriers configured');
	dead_end_cancel($node, $observer, 'postgres', 'pooled8-dead-end', $a, \@held);
	close($observer->{sock});
	unlike(slurp_file($node->logfile),
		qr/TRAP:|PANIC:|terminating threaded server runtime|GENUINE-CRASH/,
		'no crash during pooled8 dead-end cancellation');
	ok($node->stop('fast', timeout => 10, fail_ok => 1), 'fast shutdown')
	  or $node->stop('immediate', timeout => 5);
	done_testing();
};

done_testing();
